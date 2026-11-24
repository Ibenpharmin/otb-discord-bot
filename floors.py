"""Floor-price helpers for the OTB Discord bot (OpenSea).

Data source: OpenSea API v2 (free tier). The API key comes ONLY from the
OPENSEA_API_KEY env var (see .env.example) — never hardcoded. All data is
read-only public market data: collection stats (floor) + best listings.

Mirrors prices.py style: fetchers take an aiohttp session, return None on
failure, and never raise into the caller. `aiohttp` is imported lazily so
the pure helpers (alert math, formatting, config/state IO) stay importable
— and testable — without it.
"""

import asyncio
import json
import logging
import os
import time
from pathlib import Path

log = logging.getLogger("otb-floors")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

OPENSEA_API_BASE = "https://api.opensea.io/api/v2"

# Alert defaults — overridable globally via env, per-collection via
# collections.json ("move_threshold_pct" / "below_threshold_pct").
DEFAULT_MOVE_THRESHOLD_PCT = 10.0
DEFAULT_BELOW_THRESHOLD_PCT = 5.0

_CACHE_TTL = 60  # seconds — !floor spam must not hammer the free tier
_CACHE: dict = {}

_MAX_RETRIES = 3  # on HTTP 429 (rate limited) — then give up until next poll


def opensea_api_key() -> str | None:
    """The key lives in env only. Empty/unset = floor features disabled."""
    key = os.getenv("OPENSEA_API_KEY", "").strip()
    return key or None


def _env_float(name: str, default: float) -> float:
    try:
        return float(os.getenv(name, "") or default)
    except ValueError:
        return default


def default_move_threshold_pct() -> float:
    return _env_float("FLOOR_MOVE_THRESHOLD_PCT", DEFAULT_MOVE_THRESHOLD_PCT)


def default_below_threshold_pct() -> float:
    return _env_float("FLOOR_BELOW_THRESHOLD_PCT", DEFAULT_BELOW_THRESHOLD_PCT)


# ---------------------------------------------------------------------------
# Config + state files
# ---------------------------------------------------------------------------

def load_collections(path: Path) -> list[dict]:
    """Read collections.json fresh every call, so edits need no restart.

    Expected shape: {"collections": [ {slug, name, chain, currency, ...} ]}
    Entries without a slug are skipped. Returns [] on any read error.
    """
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        log.warning("collections.json unreadable (%s) — floor alerts idle", exc)
        return []
    entries = data.get("collections", []) if isinstance(data, dict) else data
    out = []
    for entry in entries if isinstance(entries, list) else []:
        if not isinstance(entry, dict) or not entry.get("slug"):
            continue
        out.append(
            {
                "slug": str(entry["slug"]),
                "name": str(entry.get("name") or entry["slug"]),
                "chain": str(entry.get("chain") or "ethereum"),
                "currency": str(entry.get("currency") or "ETH"),
                "move_threshold_pct": _as_float(
                    entry.get("move_threshold_pct"), default_move_threshold_pct()
                ),
                "below_threshold_pct": _as_float(
                    entry.get("below_threshold_pct"), default_below_threshold_pct()
                ),
            }
        )
    return out


def _as_float(value, default: float) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def load_floor_state(path: Path) -> dict:
    """{slug: {"floor": float, "alerted_deals": [deal_id, ...]}} — {} if none."""
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def save_floor_state(path: Path, state: dict) -> None:
    try:
        Path(path).write_text(json.dumps(state, indent=2), encoding="utf-8")
    except OSError as exc:
        log.warning("Could not write floor state (%s)", exc)


# ---------------------------------------------------------------------------
# Cache (same pattern as prices.py)
# ---------------------------------------------------------------------------

def _cache_get(key: str):
    entry = _CACHE.get(key)
    if entry and time.time() - entry[0] < _CACHE_TTL:
        return entry[1]
    return None


def _cache_set(key: str, value) -> None:
    _CACHE[key] = (time.time(), value)


# ---------------------------------------------------------------------------
# Fetchers (async — need aiohttp + network + OPENSEA_API_KEY)
# ---------------------------------------------------------------------------

async def _get_json(session, url: str, params: dict | None = None):
    """GET JSON with the env API key. Backs off on 429 (Retry-After first,
    else exponential 2s/4s/8s), up to _MAX_RETRIES attempts. None on failure.
    """
    key = opensea_api_key()
    if not key:
        return None
    headers = {"accept": "application/json", "X-API-KEY": key}
    for attempt in range(_MAX_RETRIES):
        try:
            async with session.get(
                url, params=params, headers=headers, timeout=15
            ) as resp:
                if resp.status == 429:
                    wait = _retry_after(resp.headers.get("Retry-After"), attempt)
                    log.warning(
                        "OpenSea rate limited (429) on %s — backing off %ss",
                        url, wait,
                    )
                    await asyncio.sleep(wait)
                    continue
                if resp.status != 200:
                    log.warning("OpenSea %s for %s", resp.status, url)
                    return None
                return await resp.json()
        except Exception:
            log.exception("OpenSea fetch failed for %s", url)
            return None
    log.warning("OpenSea still rate limited after retries: %s", url)
    return None


def _retry_after(header_value, attempt: int) -> float:
    try:
        return min(float(header_value), 60.0)
    except (TypeError, ValueError):
        return min(2.0 * (2 ** attempt), 30.0)


async def fetch_floor_price(session, slug: str):
    """Current floor price (native currency units) or None on failure."""
    cache_key = f"floor:{slug}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    data = await _get_json(session, f"{OPENSEA_API_BASE}/collections/{slug}/stats")
    if not data:
        return None
    try:
        floor = float(data["total"]["floor_price"])
    except (KeyError, TypeError, ValueError):
        log.warning("OpenSea stats for %s had no usable floor_price", slug)
        return None
    _cache_set(cache_key, floor)
    return floor


async def fetch_best_listings(session, slug: str, limit: int = 5):
    """Best (cheapest) active listings: [{price, token_id}] or None.

    Prices are converted from the API's smallest-unit string using the
    listing's own decimals. token_id is best-effort (None if the payload
    shape differs) — it only feeds alert de-duplication.
    """
    cache_key = f"listings:{slug}:{limit}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    data = await _get_json(
        session,
        f"{OPENSEA_API_BASE}/listings/collection/{slug}/best",
        params={"limit": limit},
    )
    if not data:
        return None
    out = []
    for listing in data.get("listings", []):
        try:
            current = listing["price"]["current"]
            price = int(current["value"]) / (10 ** int(current["decimals"]))
        except (KeyError, TypeError, ValueError):
            continue
        token_id = None
        try:
            offer = listing["protocol_data"]["parameters"]["offer"][0]
            token_id = str(offer["identifierOrCriteria"])
        except (KeyError, IndexError, TypeError):
            pass
        out.append({"price": float(price), "token_id": token_id})
    _cache_set(cache_key, out)
    return out


# ---------------------------------------------------------------------------
# Alert math (pure — unit-tested)
# ---------------------------------------------------------------------------

def floor_move_pct(previous, current):
    """Signed % change, or None if there is no usable baseline."""
    if previous is None or current is None or previous <= 0:
        return None
    return (current - previous) / previous * 100.0


def is_move_alert(previous, current, threshold_pct: float) -> bool:
    pct = floor_move_pct(previous, current)
    return pct is not None and abs(pct) >= threshold_pct


def below_floor_deals(listings, floor, threshold_pct: float):
    """Listings priced >= threshold_pct BELOW the current floor.

    The cheapest listing normally IS the floor (0% below) — only a listing
    undercutting the stats floor by the threshold counts as a deal, which
    happens when the stats floor lags a fresh low listing.
    """
    if not listings or floor is None or floor <= 0:
        return []
    cutoff = floor * (1 - threshold_pct / 100.0)
    deals = []
    for listing in listings:
        if listing["price"] < cutoff:
            deals.append(
                {
                    **listing,
                    "pct_below": (floor - listing["price"]) / floor * 100.0,
                    "deal_id": f"{listing.get('token_id') or '?'}:{listing['price']}",
                }
            )
    return deals


# ---------------------------------------------------------------------------
# Formatting (pure — unit-tested)
# ---------------------------------------------------------------------------

def format_amount(value: float) -> str:
    """Compact native-currency amount: 1.25 -> '1.25', 0.000420 -> '0.00042'."""
    if value >= 0.01:
        text = f"{value:,.4f}"
    else:
        text = f"{value:.8f}"
    return text.rstrip("0").rstrip(".") if "." in text else text


def format_floor_line(entry: dict, floor) -> str:
    if floor is None:
        return f"**{entry['name']}** — floor unavailable right now"
    return f"**{entry['name']}** — floor: {format_amount(floor)} {entry['currency']}"


def format_move_alert(entry: dict, previous: float, current: float) -> str:
    pct = floor_move_pct(previous, current)
    arrow = "📈" if pct >= 0 else "📉"
    return (
        f"{arrow} **Floor move — {entry['name']}**\n"
        f"Floor: {format_amount(previous)} → {format_amount(current)} "
        f"{entry['currency']} ({pct:+.1f}%)\n"
        f"https://opensea.io/collection/{entry['slug']}"
    )


def format_deal_alert(entry: dict, floor: float, deal: dict) -> str:
    return (
        f"🎯 **Below-floor listing — {entry['name']}**\n"
        f"Listed: {format_amount(deal['price'])} {entry['currency']} "
        f"({deal['pct_below']:.1f}% under the {format_amount(floor)} "
        f"{entry['currency']} floor)\n"
        f"https://opensea.io/collection/{entry['slug']}"
    )
