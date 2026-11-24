"""Market-data helpers for the OTB Discord bot.

Free APIs only, no keys:
- CoinGecko (simple/price) for common coins.
- DexScreener (token endpoint) for CASTER (Base) and PLANK (Robinhood Chain),
  which CoinGecko may not list.
- Alternative.me for the Fear & Greed Index.

`aiohttp` is imported lazily inside the fetch functions so the pure helpers
(format_price, format_change, resolve_coin) stay importable without it.
"""

import time

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

COINGECKO_SIMPLE_PRICE = "https://api.coingecko.com/api/v3/simple/price"
DEXSCREENER_TOKEN = "https://api.dexscreener.com/latest/dex/tokens/{}"
FNG_URL = "https://api.alternative.me/fng/?limit=2"

# Symbol / nickname -> CoinGecko id. Anything not listed here is tried
# verbatim as a CoinGecko id (so "dogecoin", "pepe", "hyperliquid" all work).
COIN_MAP = {
    "btc": "bitcoin",
    "bitcoin": "bitcoin",
    "eth": "ethereum",
    "ethereum": "ethereum",
    "sol": "solana",
    "solana": "solana",
    "doge": "dogecoin",
    "dogecoin": "dogecoin",
    "xrp": "ripple",
    "ripple": "ripple",
    "ada": "cardano",
    "cardano": "cardano",
    "avax": "avalanche-2",
    "avalanche": "avalanche-2",
    "link": "chainlink",
    "chainlink": "chainlink",
    "dot": "polkadot",
    "polkadot": "polkadot",
    "ltc": "litecoin",
    "litecoin": "litecoin",
    "bch": "bitcoin-cash",
    "near": "near",
    "sui": "sui",
    "apt": "aptos",
    "aptos": "aptos",
    "arb": "arbitrum",
    "arbitrum": "arbitrum",
    "op": "optimism",
    "optimism": "optimism",
    "pepe": "pepe",
    "shib": "shiba-inu",
    "usdc": "usd-coin",
    "usdt": "tether",
    "dai": "dai",
    "wbtc": "wrapped-bitcoin",
    "hype": "hyperliquid",
    "hyperliquid": "hyperliquid",
    "tao": "bittensor",
    "bittensor": "bittensor",
    "render": "render-token",
    "virtual": "virtual-protocol",
    "kas": "kaspa",
    "kaspa": "kaspa",
    "trx": "tron",
    "tron": "tron",
    "ton": "the-open-network",
    "toncoin": "the-open-network",
    "cro": "crypto-com-chain",
    "atom": "cosmos",
    "cosmos": "cosmos",
    "algo": "algorand",
    "algorand": "algorand",
    "fil": "filecoin",
    "filecoin": "filecoin",
    "vet": "vechain",
    "xlm": "stellar",
    "stellar": "stellar",
    "hbar": "hedera-hashgraph",
    "hedera": "hedera-hashgraph",
    "matic": "matic-network",
    "pol": "matic-network",
    "ena": "ethena",
    "pendle": "pendle",
}

# Community tokens, priced via DexScreener (not reliably on CoinGecko).
# name -> (dexscreener chainId, contract address, display label)
DEX_TOKENS = {
    "caster": (
        "base",
        "0x09Aa909Eea859f712f2Ae3dd1872671D2363f6f4",
        "CASTER",
    ),
    "plank": (
        "robinhood",
        "0x69420eaf0eBF43E08F621B014f25cEfDfA7e2DDc",
        "PLANK",
    ),
}

# Tiny in-memory cache so a burst of !price calls (and the ticker job)
# doesn't hammer the free CoinGecko tier. {key: (timestamp, result)}
_CACHE: dict = {}
_CACHE_TTL = 60  # seconds


# ---------------------------------------------------------------------------
# Formatting helpers (pure — no network)
# ---------------------------------------------------------------------------

def format_price(price: float) -> str:
    """Human-readable price. Big prices get commas; dust gets sig figs."""
    if price is None:
        return "n/a"
    if price >= 1000:
        return f"${price:,.2f}"
    if price >= 1:
        return f"${price:,.2f}"
    if price >= 0.01:
        return f"${price:,.4f}"
    # Sub-cent: significant figures, so $0.0000000008486 stays readable.
    return f"${price:.10g}"


def format_change(change: float | None) -> str:
    """24h change as an arrow + signed percent, e.g. '▲ 2.41%'."""
    if change is None:
        return ""
    arrow = "▲" if change >= 0 else "▼"
    return f"{arrow} {change:+.2f}%"


def resolve_coin(query: str) -> str:
    """Map user input to a CoinGecko id (or return it verbatim as an id)."""
    return COIN_MAP.get(query.strip().lower(), query.strip().lower())


# ---------------------------------------------------------------------------
# Fetchers (async — need aiohttp + network)
# ---------------------------------------------------------------------------

def _cache_get(key: str):
    entry = _CACHE.get(key)
    if entry and time.time() - entry[0] < _CACHE_TTL:
        return entry[1]
    return None


def _cache_set(key: str, value) -> None:
    _CACHE[key] = (time.time(), value)


async def fetch_coingecko_price(session, coin_id: str):
    """Return (price_usd, change_24h) or None on failure."""
    cache_key = f"cg:{coin_id}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    try:
        params = {
            "ids": coin_id,
            "vs_currencies": "usd",
            "include_24hr_change": "true",
        }
        async with session.get(
            COINGECKO_SIMPLE_PRICE, params=params, timeout=10
        ) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
    except Exception:
        return None
    row = data.get(coin_id)
    if not row or row.get("usd") is None:
        return None
    result = (float(row["usd"]), row.get("usd_24h_change"))
    _cache_set(cache_key, result)
    return result


async def fetch_dexscreener_price(session, chain_id: str, address: str):
    """Return (price_usd, change_24h) for a token contract, or None.

    Picks the pair on the requested chain with the deepest USD liquidity,
    so a stray low-liquidity pool doesn't set the price.
    """
    cache_key = f"dex:{chain_id}:{address.lower()}"
    cached = _cache_get(cache_key)
    if cached is not None:
        return cached
    try:
        async with session.get(
            DEXSCREENER_TOKEN.format(address), timeout=10
        ) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
    except Exception:
        return None
    pairs = [
        p for p in data.get("pairs", [])
        if p.get("chainId") == chain_id and p.get("priceUsd")
    ]
    if not pairs:
        return None
    best = max(pairs, key=lambda p: (p.get("liquidity") or {}).get("usd") or 0)
    change = (best.get("priceChange") or {}).get("h24")
    result = (float(best["priceUsd"]), change)
    _cache_set(cache_key, result)
    return result


async def fetch_price(session, query: str):
    """One entry point for !price and the ticker job.

    Returns (label, price_usd, change_24h) or None if unknown/unreachable.
    """
    key = query.strip().lower()
    if key in DEX_TOKENS:
        chain_id, address, label = DEX_TOKENS[key]
        got = await fetch_dexscreener_price(session, chain_id, address)
        if got is None:
            return None
        price, change = got
        return (label, price, change)
    coin_id = resolve_coin(query)
    got = await fetch_coingecko_price(session, coin_id)
    if got is None:
        return None
    price, change = got
    return (coin_id.upper(), price, change)


async def fetch_fear_greed(session):
    """Return (today_value, today_label, yesterday_value) or None.

    Alternative.me free endpoint, no key. limit=2 gives today + yesterday.
    """
    try:
        async with session.get(FNG_URL, timeout=10) as resp:
            if resp.status != 200:
                return None
            data = await resp.json()
    except Exception:
        return None
    rows = data.get("data") or []
    if not rows:
        return None
    today = rows[0]
    yesterday = rows[1] if len(rows) > 1 else None
    return (
        int(today["value"]),
        today.get("value_classification", ""),
        int(yesterday["value"]) if yesterday else None,
    )
