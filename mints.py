# ----------------------------------------------------------------
# mints.py — live NFT mint tracker for the OTB Discord bot
# ----------------------------------------------------------------
"""Mint-tracker helpers for the OTB Discord bot (OpenSea + Robinhood Chain).

Two routes, one poll cycle:

- Route 1 — OpenSea events: the v2 API's `mint` event type per watched
  collection (Ethereum, Base, Robinhood, etc.). Poll every 2 minutes.
  Reuses the existing OPENSEA_API_KEY env var — no new key, no new cost.
- Route 2 — Robinhood Chain direct watching: polls the public RPC for
  mint transfers (ERC-721 `Transfer` events from the zero address) on a
  contract address. Poll every 1 minute. No key at all. Catches a
  brand-new Sawmill contract the moment it mints, before OpenSea
  indexes it.

Mirrors floors.py style: fetchers take the bot's aiohttp session, return
None on failure, and never raise into the caller. `aiohttp` is never
imported here, so the pure helpers (parsing, dedupe, formatting,
config/state IO) stay importable — and testable — without it.

Self-contained by design: this module does NOT import floors.py,
prices.py, or bot.py. Those files arrived as an email attachment and
cannot execute on the build VM, so the small shared helpers
(`_get_json` 429 backoff, cache) are duplicated here in the floors
pattern rather than shared. One deliberate duplication, documented.

Data is read-only public data on both routes: OpenSea events and
public RPC logs. No keys, no signing, no transactions — ever.
"""

import asyncio
import json
import logging
import os
import time
from pathlib import Path

log = logging.getLogger("otb-mints")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

OPENSEA_API_BASE = "https://api.opensea.io/api/v2"

# Robinhood Chain mainnet (chain ID 4663, Arbitrum Orbit, ETH gas).
# Public endpoint is free but rate-limited — fine for one small poll a
# minute. A dedicated provider URL can be dropped in via env instead.
DEFAULT_ROBINHOOD_RPC_URL = "https://rpc.mainnet.chain.robinhood.com"
ROBINHOOD_EXPLORER = "https://robinhoodchain.blockscout.com"

# keccak256("Transfer(address,address,uint256)") — ERC-721 mint is a
# Transfer whose `from` is the zero address.
TRANSFER_TOPIC = (
    "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
)
ZERO_ADDRESS_TOPIC = "0x" + "00" * 32

_CACHE_TTL = 60  # seconds — same as floors.py / prices.py
_CACHE: dict = {}

_MAX_RETRIES = 3  # on HTTP 429 (rate limited) — then give up until next poll

_MAX_SEEN = 200  # cap the per-collection dedupe list in the state file
_MAX_RECENT = 10  # cap the per-collection recent-mints list (!mint reads it)
_MAX_BLOCK_RANGE = 1000  # blocks per RPC poll (1 min ≈ 240 blocks here)


def opensea_api_key() -> str | None:
    """The key lives in env only. Empty/unset = OpenSea route idle."""
    key = os.getenv("OPENSEA_API_KEY", "").strip()
    return key or None


def robinhood_rpc_url() -> str:
    """Public RPC by default; a provider URL wins if set in env."""
    return os.getenv("ROBINHOOD_CHAIN_RPC_URL", "").strip() or DEFAULT_ROBINHOOD_RPC_URL


# ---------------------------------------------------------------------------
# Config + state files
# ---------------------------------------------------------------------------

def load_mints(path: Path) -> list[dict]:
    """Read mints.json fresh every call, so edits need no restart.

    Expected shape: {"mints": [ {slug, name, chain, contract} ]}
    Entries without a slug are skipped. `contract` is optional: null or
    missing means the RPC route is skipped for that entry (OpenSea route
    still runs). A contract that does not look like an EVM address is
    treated as missing, with a warning. Returns [] on any read error.
    """
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        log.warning("mints.json unreadable (%s) — mint alerts idle", exc)
        return []
    entries = data.get("mints", []) if isinstance(data, dict) else data
    out = []
    for entry in entries if isinstance(entries, list) else []:
        if not isinstance(entry, dict) or not entry.get("slug"):
            continue
        contract = _clean_contract(entry.get("contract"))
        if entry.get("contract") and not contract:
            log.warning(
                "mints.json: %s has an unusable contract value — RPC route off",
                entry["slug"],
            )
        out.append(
            {
                "slug": str(entry["slug"]),
                "name": str(entry.get("name") or entry["slug"]),
                "chain": str(entry.get("chain") or "robinhood"),
                "contract": contract,
            }
        )
    return out


def _clean_contract(value) -> str | None:
    """Lowercased 0x + 40 hex chars, or None."""
    if not isinstance(value, str):
        return None
    value = value.strip().lower()
    if len(value) != 42 or not value.startswith("0x"):
        return None
    try:
        int(value[2:], 16)
    except ValueError:
        return None
    return value


def load_mint_state(path: Path) -> dict:
    """{slug: {"seen": [key, ...], "last_block": int|None,
    "recent": [mint, ...], "baselined": bool}} — {} if none."""
    try:
        data = json.loads(Path(path).read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except (OSError, json.JSONDecodeError):
        return {}


def save_mint_state(path: Path, state: dict) -> None:
    try:
        Path(path).write_text(json.dumps(state, indent=2), encoding="utf-8")
    except OSError as exc:
        log.warning("Could not write mint state (%s)", exc)


# ---------------------------------------------------------------------------
# Cache (same pattern as floors.py / prices.py)
# ---------------------------------------------------------------------------

def _cache_get(key: str):
    entry = _CACHE.get(key)
    if entry and time.time() - entry[0] < _CACHE_TTL:
        return entry[1]
    return None


def _cache_set(key: str, value) -> None:
    _CACHE[key] = (time.time(), value)


# ---------------------------------------------------------------------------
# HTTP helpers (async — need aiohttp + network)
# ---------------------------------------------------------------------------

async def _get_json(session, url: str, params: dict | None = None):
    """GET JSON with the env API key. Backs off on 429 (Retry-After first,
    else exponential 2s/4s/8s), up to _MAX_RETRIES attempts. None on failure.

    Duplicated from the floors.py pattern on purpose: mints.py must not
    import floors.py (attachment code cannot execute on the build VM).
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


async def _rpc_call(session, method: str, params):
    """One JSON-RPC call to the Robinhood Chain public RPC. Returns the
    `result` field, or None on any failure (HTTP error, JSON-RPC error,
    timeout). No retry — the next poll (1 min away) retries it."""
    payload = {"jsonrpc": "2.0", "id": 1, "method": method, "params": params}
    try:
        async with session.post(
            robinhood_rpc_url(), json=payload, timeout=15
        ) as resp:
            if resp.status != 200:
                log.warning("Robinhood RPC HTTP %s on %s", resp.status, method)
                return None
            data = await resp.json()
    except Exception:
        log.exception("Robinhood RPC call failed (%s)", method)
        return None
    if not isinstance(data, dict) or "error" in data:
        log.warning("Robinhood RPC error on %s: %s", method, data.get("error"))
        return None
    return data.get("result")


# ---------------------------------------------------------------------------
# Fetchers (async — need aiohttp + network)
# ---------------------------------------------------------------------------

async def fetch_opensea_mints(session, slug: str, limit: int = 20):
    """Recent mint events for a collection: [mint, ...] or None on failure.

    A mint record: {"token_id", "to", "tx", "ts", "source", "key"}.
    `key` is the dedupe id; `to` may be None; `ts` is a unix timestamp
    or None. The API returns newest first; order is preserved.
    """
    cache_key = f"mints-os:{slug}:{limit}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    data = await _get_json(
        session,
        f"{OPENSEA_API_BASE}/events/collection/{slug}",
        params={"event_type": "mint", "limit": limit},
    )
    if data is None:
        return None
    mints = parse_opensea_events(data)
    _cache_set(cache_key, mints)
    return mints


async def fetch_latest_block(session):
    """Current Robinhood Chain head block number, or None on failure."""
    result = await _rpc_call(session, "eth_blockNumber", [])
    if not isinstance(result, str):
        return None
    try:
        return int(result, 16)
    except ValueError:
        log.warning("Robinhood RPC returned an unparsable block number")
        return None


async def fetch_rpc_mints(session, contract: str, from_block: int, to_block: int):
    """Mint transfers for a contract over [from_block, to_block].

    Returns [mint, ...] (possibly empty) or None on failure. A mint
    record here carries "block" instead of "ts". from_block > to_block
    returns [] without calling the node.
    """
    if from_block > to_block:
        return []
    result = await _rpc_call(
        session,
        "eth_getLogs",
        [
            {
                "fromBlock": hex(from_block),
                "toBlock": hex(to_block),
                "address": contract,
                "topics": [TRANSFER_TOPIC, ZERO_ADDRESS_TOPIC],
            }
        ],
    )
    if result is None:
        return None
    if not isinstance(result, list):
        log.warning("Robinhood RPC getLogs returned a non-list result")
        return None
    return parse_rpc_logs(result)


# ---------------------------------------------------------------------------
# Parsing (pure — unit-tested)
# ---------------------------------------------------------------------------

def parse_opensea_events(data: dict) -> list[dict]:
    """Parse a v2 events payload into mint records.

    Only `event_type == "mint"` entries count; anything malformed is
    skipped, never raised. Newest first, as the API returns them.
    """
    mints = []
    events = data.get("asset_events", []) if isinstance(data, dict) else []
    for event in events if isinstance(events, list) else []:
        if not isinstance(event, dict) or event.get("event_type") != "mint":
            continue
        nft = event.get("nft") or {}
        token_id = nft.get("identifier")
        if token_id is None:
            continue
        tx = event.get("transaction") or ""
        ts = event.get("event_timestamp")
        try:
            ts = int(ts) if ts is not None else None
        except (TypeError, ValueError):
            ts = None
        to_account = event.get("to_account") or {}
        key = f"os:{tx}:{token_id}" if tx else f"os:{ts}:{token_id}"
        mints.append(
            {
                "token_id": str(token_id),
                "to": to_account.get("address"),
                "tx": tx,
                "ts": ts,
                "source": "opensea",
                "key": key,
            }
        )
    return mints


def parse_rpc_logs(logs: list) -> list[dict]:
    """Parse eth_getLogs results into mint records.

    A log is a mint when topics[0] is the Transfer signature and
    topics[1] is the zero address (mint = transfer FROM nowhere).
    topics[2] is the minter, topics[3] is the ERC-721 token id.
    Anything malformed is skipped, never raised.
    """
    mints = []
    for entry in logs if isinstance(logs, list) else []:
        if not isinstance(entry, dict):
            continue
        topics = entry.get("topics") or []
        if len(topics) < 4:
            continue
        if topics[0] != TRANSFER_TOPIC or topics[1] != ZERO_ADDRESS_TOPIC:
            continue
        try:
            token_id = int(topics[3], 16)
            to = "0x" + topics[2][-40:]
            int(to[2:], 16)  # minter must be a real address
        except (TypeError, ValueError, IndexError):
            continue
        tx = entry.get("transactionHash") or ""
        try:
            block = int(entry.get("blockNumber"), 16)
        except (TypeError, ValueError):
            block = None
        mints.append(
            {
                "token_id": str(token_id),
                "to": to,
                "tx": tx,
                "block": block,
                "source": "rpc",
                "key": f"rpc:{tx}:{token_id}" if tx else f"rpc:{block}:{token_id}",
            }
        )
    return mints


def new_mints(mints: list, seen_keys) -> list[dict]:
    """Mints whose dedupe key is not already in `seen_keys`."""
    seen = set(seen_keys or [])
    return [m for m in mints if m.get("key") not in seen]


# ---------------------------------------------------------------------------
# Poll cycle (async — driven by the bot loop, tested without the bot)
# ---------------------------------------------------------------------------

async def poll_collection(session, entry: dict, state: dict, send,
                          *, opensea_due: bool) -> bool:
    """One watched collection, one poll tick.

    `send` is an async callable taking the alert text (in production:
    the alert channel's `.send`). Returns True if the persisted state
    changed. First sighting only sets baselines — a fresh deploy never
    fires a mint alert for old mints.

    Route 1 (OpenSea) runs only when `opensea_due` is True and a key is
    set. Route 2 (RPC) runs every tick for entries with a contract.
    """
    slug = entry["slug"]
    record = state.setdefault(
        slug, {"seen": [], "last_block": None, "recent": [], "baselined": False}
    )
    seen_list = record.get("seen") or []
    seen = set(seen_list)
    baselined = bool(record.get("baselined"))
    changed = False
    fresh: list[dict] = []

    def _remember(mints: list) -> None:
        nonlocal changed
        for mint in mints:
            if mint["key"] not in seen:
                seen.add(mint["key"])
                seen_list.append(mint["key"])
                changed = True
                fresh.append(mint)

    # Route 1 — OpenSea mint events (keyed, 2-minute cadence).
    if opensea_due:
        if opensea_api_key():
            mints = await fetch_opensea_mints(session, slug)
            if mints is not None:
                _remember(mints)
        else:
            log.debug("No OPENSEA_API_KEY — OpenSea mint route idle for %s", slug)

    # Route 2 — Robinhood Chain RPC mint transfers (keyless, every tick).
    contract = entry.get("contract")
    if contract:
        latest = await fetch_latest_block(session)
        if latest is not None:
            last_block = record.get("last_block")
            if last_block is None:
                # First sighting: anchor at the head, don't scan history.
                record["last_block"] = latest
                changed = True
                log.info("RPC baseline for %s: block %s", slug, latest)
            else:
                from_block = max(last_block + 1, latest - _MAX_BLOCK_RANGE + 1)
                if from_block > last_block + 1:
                    log.warning(
                        "RPC gap for %s: %s blocks behind, scanning the last %s",
                        slug, latest - last_block, _MAX_BLOCK_RANGE,
                    )
                mints = await fetch_rpc_mints(session, contract, from_block, latest)
                if mints is not None:
                    _remember(mints)
                    record["last_block"] = latest
                    changed = True

    if len(seen_list) > _MAX_SEEN:  # keep the state file bounded
        record["seen"] = seen_list[-_MAX_SEEN:]
        changed = True
    else:
        record["seen"] = seen_list

    if not baselined:
        record["baselined"] = True
        changed = True
        log.info("Mint baseline for %s: %s known mint(s) recorded silently",
                 slug, len(seen_list))
        return changed

    for mint in fresh:
        await send(format_mint_alert(entry, mint))
        log.info("Mint alert for %s: token %s (%s)",
                 slug, mint["token_id"], mint["source"])
    if fresh:
        recent = record.get("recent") or []
        recent.extend(fresh)
        record["recent"] = recent[-_MAX_RECENT:]
        changed = True
    return changed


# ---------------------------------------------------------------------------
# Formatting (pure — unit-tested)
# ---------------------------------------------------------------------------

def short_address(address) -> str:
    """0x123456…abcd, or 'unknown' when there is no usable address."""
    if not isinstance(address, str) or len(address) < 10:
        return "unknown"
    return f"{address[:8]}…{address[-4:]}"


def format_mint_alert(entry: dict, mint: dict) -> str:
    source = "OpenSea" if mint.get("source") == "opensea" else "Robinhood Chain"
    if mint.get("source") == "rpc" and entry.get("contract"):
        link = (
            f"{ROBINHOOD_EXPLORER}/token/{entry['contract']}"
            f"/instance/{mint['token_id']}"
        )
    else:
        link = f"https://opensea.io/collection/{entry['slug']}"
    return (
        f"🌱 **New mint — {entry['name']}**\n"
        f"Token #{mint['token_id']} → {short_address(mint.get('to'))} "
        f"(via {source})\n"
        f"{link}"
    )


def format_mint_line(mint: dict) -> str:
    source = "OpenSea" if mint.get("source") == "opensea" else "RPC"
    return f"• #{mint['token_id']} → {short_address(mint.get('to'))} ({source})"


def format_watch_line(entry: dict, state_record: dict | None) -> str:
    routes = ["OpenSea"]
    if entry.get("contract"):
        routes.append("RPC")
    else:
        routes.append("RPC off — no contract set")
    recent = (state_record or {}).get("recent") or []
    line = f"**{entry['name']}** ({entry['slug']}) — {' + '.join(routes)}"
    if recent:
        line += f" — last mint: #{recent[-1]['token_id']}"
    return line
