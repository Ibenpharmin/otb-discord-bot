# OTB Discord Bot — The Algorithmic Trap House

The Discord bot for Iben's community server. Price tickers, Fear & Greed, NFT floor alerts, live mint alerts, and OTB RADIO.

## What it does

| Feature | How |
|---|---|
| `!price <coin>` | Live price + 24h change (BTC, ETH, SOL, CASTER, PLANK, ...) |
| `!scan` / `!wrap` / `!dome` | Latest digest / ITZ A WRAP / OFF THE DOME links |
| `!radio` | What's playing on OTB RADIO |
| `!fear` | Live Fear & Greed Index |
| `!floor [collection]` | Live NFT floor price(s) being tracked |
| `!mint [collection]` | Recent mints + watched collections |
| Daily 9am PT | Fear & Greed post to #tickers |
| Every 6h | Ticker post to #tickers |
| Floor alerts | OpenSea floor moves + below-floor deals to #floor-alerts |
| Mint alerts | Live NFT mints (OpenSea + Robinhood Chain RPC) to #mint-alerts |
| Radio | Loops local tracks in OTB RADIO voice channel |

## Files

| File | What it is |
|---|---|
| `bot.py` | The bot: commands, scheduled jobs, radio, DAVE voice fix |
| `prices.py` | Price + Fear & Greed fetching (CoinGecko, DexScreener) |
| `floors.py` | Floor-price watching (OpenSea v2) |
| `mints.py` | Mint watching (OpenSea v2 events + Robinhood Chain RPC) |
| `collections.json` | Floor watch list (OpenSea slugs) |
| `mints.json` | Mint watch list (OpenSea slugs + contract addresses) |
| `links.json` | Latest scan/wrap/dome URLs (edit live, no restart) |
| `test_floors.py` / `test_mints.py` | Fixture test suites (stdlib only, no keys, no network) |
| `requirements.txt` | discord.py[voice], aiohttp, python-dotenv |

## Setup

```bash
pip install -r requirements.txt
cp .env.example .env   # fill in DISCORD_TOKEN + OPENSEA_API_KEY
python bot.py
```

The token lives ONLY in `.env` (gitignored). Never commit it.

## Deploy (Oracle)

The bot runs on an Oracle Cloud E2 Micro (San Jose). Deploy via OCI Run Command — SSH from Merlin's VM is blocked by the egress proxy. See the deploy notes in the OTB workspace. Restarts must `export PATH="/var/lib/ocarun/bin:$PATH"` first (ffmpeg lives there, not on the default PATH).

## DAVE voice fix (2026-11-21)

discord.py 2.7.1 never calls the davey session methods that switch the MLS keys on. `bot.py` monkey-patches the missing `prepare_transition()` / `execute_transition()` calls. Do NOT remove this on upgrade unless upstream wires it. Verified live: without the fix, DAVE clients hear silence.

## For agents (Sage / They / Magnus)

This repo is the shared source of truth for the bot. Clone it, don't ask for zip attachments — email attachments can't execute on agent VMs. Quest handoffs reference this repo by path. When you build a new module, mirror the existing style (fetchers take the bot's aiohttp session, return None on failure, never raise into the caller; pure helpers stay importable without aiohttp; fixture tests in the `test_*.py` pattern).

## License

Private — OTB internal. Not for redistribution.
