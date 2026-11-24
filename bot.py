"""OTB Discord bot for The Algorithmic Trap House.

Commands (prefix "!"): !price !scan !wrap !dome !radio !fear !floor !mint
Scheduled: daily Fear & Greed post + 6-hour ticker post to #tickers;
floor alerts (OpenSea) to the floor-alerts channel when configured;
mint alerts (OpenSea + Robinhood Chain RPC) to the mint-alerts channel
when configured.
Radio: loops the project's own tracks/ files in the OTB RADIO voice channel.

Token comes ONLY from the DISCORD_TOKEN env var (see .env.example).
Never put a token in code or in this repo.
"""

import asyncio
import json
import logging
import os
import random
from datetime import time as dtime
from pathlib import Path
from zoneinfo import ZoneInfo

import discord
from discord.ext import commands, tasks
from dotenv import load_dotenv

from prices import (
    DEX_TOKENS,
    fetch_fear_greed,
    fetch_price,
    format_change,
    format_price,
)

from floors import (
    below_floor_deals,
    fetch_best_listings,
    fetch_floor_price,
    format_deal_alert,
    format_floor_line,
    format_move_alert,
    is_move_alert,
    load_collections,
    load_floor_state,
    opensea_api_key,
    save_floor_state,
)

from mints import (
    format_mint_line,
    format_watch_line,
    load_mint_state,
    load_mints,
    poll_collection,
    save_mint_state,
)

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

BASE_DIR = Path(__file__).resolve().parent
LINKS_FILE = BASE_DIR / "links.json"
COLLECTIONS_FILE = BASE_DIR / "collections.json"
FLOOR_STATE_FILE = BASE_DIR / "floor_state.json"
MINTS_FILE = BASE_DIR / "mints.json"
MINT_STATE_FILE = BASE_DIR / "mint_state.json"
TRACKS_DIR = BASE_DIR / "tracks"
AUDIO_EXTS = {".mp3", ".wav", ".ogg", ".m4a", ".flac"}

PT = ZoneInfo("America/Los_Angeles")
TICKERS_CHANNEL_NAME = "tickers"
RADIO_CHANNEL_NAME = "OTB RADIO"

# Tickers job: these four, every 6 hours.
TICKER_COINS = ["btc", "eth", "caster", "plank"]

# Daily Fear & Greed post time (PT).
FNG_POST_TIME = dtime(hour=9, minute=0, tzinfo=PT)

load_dotenv(BASE_DIR / ".env")

# Floor-alert poll interval (minutes). Quest bounds: 15–30 min on the
# OpenSea free tier — values outside that range are clamped into it.
try:
    FLOOR_POLL_MINUTES = int(os.getenv("FLOOR_POLL_MINUTES", "20"))
except ValueError:
    FLOOR_POLL_MINUTES = 20
FLOOR_POLL_MINUTES = max(15, min(30, FLOOR_POLL_MINUTES))

# Floor alerts channel: an explicit channel ID wins (FLOOR_ALERTS_CHANNEL_ID);
# otherwise fall back to a channel NAME (house style — lookups by name).
FLOOR_ALERTS_CHANNEL_NAME = os.getenv("FLOOR_ALERTS_CHANNEL_NAME", "floor-alerts")

# Mint-alert poll interval (minutes): one loop, 1-minute ticks; the RPC
# route runs every tick, the OpenSea route every MINT_OPENSEA_EVERY-th
# tick (default 2, i.e. every 2 minutes). Clamped 1–5 / 1–10.
try:
    MINT_POLL_MINUTES = int(os.getenv("MINT_POLL_MINUTES", "1"))
except ValueError:
    MINT_POLL_MINUTES = 1
MINT_POLL_MINUTES = max(1, min(5, MINT_POLL_MINUTES))

try:
    MINT_OPENSEA_EVERY = int(os.getenv("MINT_OPENSEA_EVERY", "2"))
except ValueError:
    MINT_OPENSEA_EVERY = 2
MINT_OPENSEA_EVERY = max(1, min(10, MINT_OPENSEA_EVERY))

# Mint alerts channel: MINT_ALERTS_CHANNEL_ID (env) wins; otherwise fall
# back to a channel NAME (same pattern as the floor alerts).
MINT_ALERTS_CHANNEL_NAME = os.getenv("MINT_ALERTS_CHANNEL_NAME", "mint-alerts")

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
log = logging.getLogger("otb-bot")


# DAVE TRANSITION FIX (2026-11-21) — discord.py 2.7.1 never calls the davey
# session methods that switch the MLS keys on: it processes commits/welcomes
# and does the transition signalling, but never calls
# dave_session.prepare_transition() or dave_session.execute_transition().
# Result: the session stays ready=False forever and the bot's audio goes out
# without the MLS layer, so DAVE clients play silence. These wrappers add the
# missing calls at the exact points the DAVE protocol requires them.
# Revisit on discord.py upgrade — remove if upstream wires these calls.
import struct as _struct
from discord.gateway import DiscordVoiceWebSocket as _DiscordVoiceWebSocket
from discord.voice_state import VoiceConnectionState as _VoiceConnectionState

_orig_voice_received_message = _DiscordVoiceWebSocket.received_message


async def _patched_voice_received_message(self, msg):
    op = msg.get("op")
    data = msg.get("d") or {}
    if op == _DiscordVoiceWebSocket.DAVE_PREPARE_TRANSITION:
        ds = self._connection.dave_session
        if ds is not None:
            try:
                ds.prepare_transition(data["transition_id"], data["protocol_version"])
                log.debug("DAVE fix: prepared transition %s", data["transition_id"])
            except Exception:
                log.exception("DAVE fix: prepare_transition failed")
    await _orig_voice_received_message(self, msg)


_DiscordVoiceWebSocket.received_message = _patched_voice_received_message

_orig_voice_received_binary_message = _DiscordVoiceWebSocket.received_binary_message


async def _patched_voice_received_binary_message(self, msg):
    await _orig_voice_received_binary_message(self, msg)
    op = msg[2] if len(msg) > 2 else None
    if op in (_DiscordVoiceWebSocket.MLS_ANNOUNCE_COMMIT_TRANSITION,
              _DiscordVoiceWebSocket.MLS_WELCOME):
        ds = self._connection.dave_session
        if ds is None:
            return
        tid = _struct.unpack_from(">H", msg, 3)[0]
        try:
            ds.prepare_transition(tid, self._connection.dave_protocol_version)
            log.debug("DAVE fix: prepared transition %s after commit/welcome", tid)
        except Exception:
            log.exception("DAVE fix: prepare_transition after commit failed")
        if tid == 0:
            # Transition 0 is the initial group formation: no ready-ack
            # round-trip, and the server may never send prepare/execute for
            # it — execute immediately so the session becomes ready.
            try:
                ok = ds.execute_transition(0)
                log.debug("DAVE fix: execute_transition(0) -> %s", ok)
            except Exception:
                log.exception("DAVE fix: execute_transition(0) failed")


_DiscordVoiceWebSocket.received_binary_message = _patched_voice_received_binary_message

_orig_execute_transition = _VoiceConnectionState._execute_transition


async def _patched_execute_transition(self, transition_id):
    ds = self.dave_session
    if ds is not None:
        try:
            ok = ds.execute_transition(transition_id)
            log.debug("DAVE fix: execute_transition(%s) -> %s", transition_id, ok)
        except Exception:
            log.exception("DAVE fix: execute_transition failed")
    await _orig_execute_transition(self, transition_id)


_VoiceConnectionState._execute_transition = _patched_execute_transition


def load_links() -> dict:
    """Read links.json fresh every time, so edits need no restart."""
    defaults = {
        "latest_scan_url": "https://x.com/ontheblokkchain",
        "latest_wrap_url": "https://open.spotify.com/episode/7lZKhZHC9hypJl1qih0hi6",
        "latest_dome_url": "https://open.spotify.com/show/3aZa1P4NQ6KA9VZv7ls6Y2",
    }
    try:
        data = json.loads(LINKS_FILE.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        log.warning("links.json unreadable (%s) — using defaults", exc)
        return defaults
    for key, val in defaults.items():
        data.setdefault(key, val)
    return data


# ---------------------------------------------------------------------------
# Bot
# ---------------------------------------------------------------------------

class OTBBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        # Required for prefix commands + member lookups. Both must also be
        # flipped on in the Developer Portal (Bot > Privileged Gateway Intents).
        intents.message_content = True
        intents.members = True
        # Proxy-aware: if the host sets HTTPS_PROXY (e.g. Merlin's VM, which
        # routes all egress through an HTTP proxy), discord.py must be told
        # explicitly — aiohttp ignores proxy env vars by default and a direct
        # connect fails with an SSL error. No proxy env = no-op (Google VM).
        proxy = os.getenv("HTTPS_PROXY") or os.getenv("https_proxy")
        super().__init__(command_prefix="!", intents=intents, help_command=None, proxy=proxy)
        self.session = None  # aiohttp.ClientSession, created in setup_hook
        self.radio_enabled = os.getenv("OTB_RADIO_ENABLED", "true").lower() != "false"
        self.radio_vc: discord.VoiceClient | None = None
        self.current_track: str | None = None
        self._radio_task: asyncio.Task | None = None
        self._mint_tick = 0  # counts mint-alert loop ticks (OpenSea cadence)

    # -- lifecycle -----------------------------------------------------------

    async def setup_hook(self):
        import aiohttp

        # trust_env=True lets the price-fetch session use the host's proxy
        # env vars when present (Merlin's VM); no-op on a direct-egress host.
        self.session = aiohttp.ClientSession(trust_env=True)
        self.fear_greed_daily.start()
        self.tickers_post.start()
        self.floor_alerts.start()
        self.mint_alerts.start()
        log.info(
            "Scheduled jobs started (F&G daily 9am PT, tickers every 6h, "
            "floor alerts every %s min, mint alerts every %s min)",
            FLOOR_POLL_MINUTES, MINT_POLL_MINUTES,
        )

    async def on_ready(self):
        log.info("Logged in as %s (%s)", self.user, self.user.id)
        log.info("Guilds: %s", ", ".join(g.name for g in self.guilds))
        if self.radio_enabled:
            self._radio_task = asyncio.create_task(self.radio_loop())
        else:
            log.info("Radio disabled via OTB_RADIO_ENABLED=false")

    async def close(self):
        if self._radio_task:
            self._radio_task.cancel()
        if self.radio_vc:
            await self.radio_vc.disconnect(force=True)
        if self.session:
            await self.session.close()
        await super().close()

    async def on_command_error(self, ctx, error):
        if isinstance(error, commands.MissingRequiredArgument):
            await ctx.send(f"Usage: `!{ctx.command.name} {ctx.command.signature}`")
        elif isinstance(error, commands.CommandNotFound):
            pass  # ignore typos silently
        else:
            log.exception("Command error in %s", ctx.command)
            await ctx.send("Something broke on my end — try again in a bit.")

    # -- channel helpers (lookups by NAME, never hardcoded IDs) --------------

    def find_text_channel(self, name: str):
        for guild in self.guilds:
            channel = discord.utils.get(guild.text_channels, name=name)
            if channel:
                return channel
        return None

    def find_voice_channel(self, name: str):
        for guild in self.guilds:
            channel = discord.utils.get(guild.voice_channels, name=name)
            if channel:
                return channel
        return None

    # -- scheduled jobs ------------------------------------------------------

    @tasks.loop(time=FNG_POST_TIME)
    async def fear_greed_daily(self):
        await self.wait_until_ready()
        channel = self.find_text_channel(TICKERS_CHANNEL_NAME)
        if not channel:
            log.warning("No #%s channel found — skipping F&G post", TICKERS_CHANNEL_NAME)
            return
        result = await fetch_fear_greed(self.session)
        if result is None:
            log.warning("F&G fetch failed — skipping daily post")
            return
        value, label, yesterday = result
        msg = f"📊 **Fear & Greed: {value} — {label}**"
        if yesterday is not None:
            msg += f"\nYesterday: {yesterday}"
        await channel.send(msg)
        log.info("Posted daily Fear & Greed: %s (%s)", value, label)

    @tasks.loop(hours=6)
    async def tickers_post(self):
        await self.wait_until_ready()
        channel = self.find_text_channel(TICKERS_CHANNEL_NAME)
        if not channel:
            log.warning("No #%s channel found — skipping tickers post", TICKERS_CHANNEL_NAME)
            return
        lines = []
        for coin in TICKER_COINS:
            got = await fetch_price(self.session, coin)
            if got is None:
                lines.append(f"{coin.upper()}: n/a")
                continue
            label, price, change = got
            lines.append(f"{label}: {format_price(price)} {format_change(change)}".strip())
        await channel.send("📈 **Tickers**\n" + "\n".join(lines))
        log.info("Posted 6h tickers")

    # -- floor alerts --------------------------------------------------------

    def find_floor_channel(self):
        """FLOOR_ALERTS_CHANNEL_ID (env) wins; else the channel NAME."""
        channel_id = os.getenv("FLOOR_ALERTS_CHANNEL_ID", "").strip()
        if channel_id:
            try:
                channel = self.get_channel(int(channel_id))
            except ValueError:
                log.warning("FLOOR_ALERTS_CHANNEL_ID is not a number: %r", channel_id)
                channel = None
            if channel:
                return channel
        return self.find_text_channel(FLOOR_ALERTS_CHANNEL_NAME)

    @tasks.loop(minutes=FLOOR_POLL_MINUTES)
    async def floor_alerts(self):
        await self.wait_until_ready()
        if not opensea_api_key():
            log.warning("OPENSEA_API_KEY not set — floor alerts idle")
            return
        collections = load_collections(COLLECTIONS_FILE)
        if not collections:
            return
        channel = self.find_floor_channel()
        if not channel:
            log.warning("No floor-alerts channel found — skipping floor poll")
            return
        state = load_floor_state(FLOOR_STATE_FILE)
        changed = False
        for entry in collections:
            try:
                changed = (
                    await self._poll_collection(entry, channel, state) or changed
                )
            except Exception:
                log.exception("Floor poll failed for %s", entry["slug"])
            await asyncio.sleep(1)  # free-tier courtesy between collections
        if changed:
            save_floor_state(FLOOR_STATE_FILE, state)

    async def _poll_collection(self, entry, channel, state) -> bool:
        """One collection: baseline / move alert / below-floor deals.

        Returns True if the persisted state changed. First sighting only
        sets the baseline — a fresh deploy never fires a move alert.
        """
        slug = entry["slug"]
        floor = await fetch_floor_price(self.session, slug)
        if floor is None:
            return False
        record = state.setdefault(slug, {"floor": None, "alerted_deals": []})
        previous = record.get("floor")
        changed = False
        if previous is None:
            record["floor"] = floor
            changed = True
            log.info("Floor baseline for %s: %s %s", slug, floor, entry["currency"])
        else:
            if is_move_alert(previous, floor, entry["move_threshold_pct"]):
                await channel.send(format_move_alert(entry, previous, floor))
                log.info(
                    "Floor move alert for %s: %s -> %s", slug, previous, floor
                )
            if floor != previous:
                record["floor"] = floor
                changed = True
        listings = await fetch_best_listings(self.session, slug)
        deals = below_floor_deals(listings, floor, entry["below_threshold_pct"])
        alerted = record.setdefault("alerted_deals", [])
        for deal in deals:
            if deal["deal_id"] in alerted:
                continue  # same listing already pinged — don't spam the poll
            await channel.send(format_deal_alert(entry, floor, deal))
            alerted.append(deal["deal_id"])
            changed = True
            log.info("Below-floor alert for %s: %s", slug, deal["deal_id"])
        if len(alerted) > 100:  # keep the state file bounded
            record["alerted_deals"] = alerted[-100:]
            changed = True
        return changed

    # -- mint alerts ---------------------------------------------------------

    def find_mint_channel(self):
        """MINT_ALERTS_CHANNEL_ID (env) wins; else the channel NAME."""
        channel_id = os.getenv("MINT_ALERTS_CHANNEL_ID", "").strip()
        if channel_id:
            try:
                channel = self.get_channel(int(channel_id))
            except ValueError:
                log.warning("MINT_ALERTS_CHANNEL_ID is not a number: %r", channel_id)
                channel = None
            if channel:
                return channel
        return self.find_text_channel(MINT_ALERTS_CHANNEL_NAME)

    @tasks.loop(minutes=MINT_POLL_MINUTES)
    async def mint_alerts(self):
        await self.wait_until_ready()
        entries = load_mints(MINTS_FILE)
        if not entries:
            return
        channel = self.find_mint_channel()
        if not channel:
            log.warning("No mint-alerts channel found — skipping mint poll")
            return
        state = load_mint_state(MINT_STATE_FILE)
        self._mint_tick += 1
        # One loop, two cadences: the RPC route runs every tick, the
        # OpenSea route every MINT_OPENSEA_EVERY-th tick (default: 2 min).
        opensea_due = (self._mint_tick - 1) % MINT_OPENSEA_EVERY == 0
        changed = False
        for entry in entries:
            try:
                changed = (
                    await poll_collection(
                        self.session, entry, state, channel.send,
                        opensea_due=opensea_due,
                    )
                    or changed
                )
            except Exception:
                log.exception("Mint poll failed for %s", entry["slug"])
            await asyncio.sleep(1)  # free-tier courtesy between collections
        if changed:
            save_mint_state(MINT_STATE_FILE, state)

    # -- radio ---------------------------------------------------------------

    def playlist(self):
        """Own tracks only — local files in tracks/. Sorted, deterministic."""
        if not TRACKS_DIR.is_dir():
            return []
        return sorted(
            p for p in TRACKS_DIR.iterdir()
            if p.is_file() and p.suffix.lower() in AUDIO_EXTS
        )

    async def radio_loop(self):
        """Join OTB RADIO and loop the local playlist forever.

        Never streams from YouTube / SoundCloud / Spotify — local files only.
        Reconnects on drop; backs off and retries instead of crashing.
        """
        await self.wait_until_ready()
        while True:
            try:
                tracks = self.playlist()
                if not tracks:
                    log.warning("No tracks in %s — radio idle, retrying in 10 min",
                                TRACKS_DIR)
                    await asyncio.sleep(600)
                    continue
                if self.radio_vc is None or not self.radio_vc.is_connected():
                    channel = self.find_voice_channel(RADIO_CHANNEL_NAME)
                    if channel is None:
                        log.warning("No voice channel named %r — radio idle",
                                    RADIO_CHANNEL_NAME)
                        await asyncio.sleep(600)
                        continue
                    self.radio_vc = await channel.connect(timeout=30, reconnect=True)
                    log.info("Radio joined #%s", RADIO_CHANNEL_NAME)
                # Shuffle the order each cycle so the loop doesn't feel canned.
                order = tracks[:]
                random.shuffle(order)
                for track in order:
                    if self.radio_vc is None or not self.radio_vc.is_connected():
                        break  # outer loop reconnects
                    self.current_track = track.stem.replace("_", " ").replace("-", " ")
                    source = discord.FFmpegPCMAudio(str(track), options="-vn")
                    self.radio_vc.play(source)
                    log.info("Radio now playing: %s", track.name)
                    while self.radio_vc.is_playing() or self.radio_vc.is_paused():
                        await asyncio.sleep(1)
                    self.current_track = None
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Radio loop error — retrying in 60s")
                self.current_track = None
                await asyncio.sleep(60)


# ---------------------------------------------------------------------------
# Commands
# ---------------------------------------------------------------------------

bot = OTBBot()


@bot.command(name="price")
async def price_cmd(ctx, coin: str):
    """!price <coin> — live price + 24h change."""
    got = await fetch_price(bot.session, coin)
    if got is None:
        await ctx.send(
            f"Couldn't pull `{coin}` — check the name, or try btc, eth, sol, "
            f"caster, plank."
        )
        return
    label, price, change = got
    await ctx.send(f"**{label}** — {format_price(price)} {format_change(change)}".strip())


@bot.command(name="scan")
async def scan_cmd(ctx):
    """!scan — latest daily digest link."""
    await ctx.send(f"📰 Latest scan:\n{load_links()['latest_scan_url']}")


@bot.command(name="wrap")
async def wrap_cmd(ctx):
    """!wrap — latest ITZ A WRAP episode link."""
    await ctx.send(f"🎙️ Latest ITZ A WRAP:\n{load_links()['latest_wrap_url']}")


@bot.command(name="dome")
async def dome_cmd(ctx):
    """!dome — latest OFF THE DOME link."""
    await ctx.send(f"🏛️ Latest OFF THE DOME:\n{load_links()['latest_dome_url']}")


@bot.command(name="radio")
async def radio_cmd(ctx):
    """!radio — what's playing on OTB RADIO."""
    if bot.current_track:
        await ctx.send(f"🎧 Now playing on OTB RADIO: **{bot.current_track}**")
    else:
        await ctx.send("Radio is off")


@bot.command(name="fear")
async def fear_cmd(ctx):
    """!fear — live Fear & Greed Index."""
    result = await fetch_fear_greed(bot.session)
    if result is None:
        await ctx.send("Couldn't pull the Fear & Greed Index right now — try again in a bit.")
        return
    value, label, yesterday = result
    msg = f"📊 **Fear & Greed: {value} — {label}**"
    if yesterday is not None:
        msg += f"\nYesterday: {yesterday}"
    await ctx.send(msg)


@bot.command(name="floor")
async def floor_cmd(ctx, *, collection: str = None):
    """!floor [collection] — live floor for one (or all) tracked collections."""
    if not opensea_api_key():
        await ctx.send("Floor data isn't configured yet (no OpenSea API key set).")
        return
    collections = load_collections(COLLECTIONS_FILE)
    if not collections:
        await ctx.send("No collections are being tracked yet.")
        return
    if collection:
        query = collection.strip().lower()
        match = next(
            (
                e
                for e in collections
                if query in (e["slug"].lower(), e["name"].lower())
            ),
            None,
        )
        if match is None:
            names = ", ".join(e["name"] for e in collections)
            await ctx.send(f"`{collection}` isn't tracked. Tracking: {names}")
            return
        collections = [match]
    lines = []
    for entry in collections:
        floor = await fetch_floor_price(bot.session, entry["slug"])
        lines.append(format_floor_line(entry, floor))
    await ctx.send("🏷️ **Floors**\n" + "\n".join(lines))


@bot.command(name="mint")
async def mint_cmd(ctx, *, collection: str = None):
    """!mint [collection] — recent mints + watch list (from state, no network)."""
    entries = load_mints(MINTS_FILE)
    if not entries:
        await ctx.send("No collections are being watched for mints yet.")
        return
    if collection:
        query = collection.strip().lower()
        match = next(
            (
                e
                for e in entries
                if query in (e["slug"].lower(), e["name"].lower())
            ),
            None,
        )
        if match is None:
            names = ", ".join(e["name"] for e in entries)
            await ctx.send(f"`{collection}` isn't watched. Watching: {names}")
            return
        entries = [match]
    state = load_mint_state(MINT_STATE_FILE)
    lines = []
    for entry in entries:
        record = state.get(entry["slug"]) or {}
        lines.append(format_watch_line(entry, record))
        recent = record.get("recent") or []
        for mint in reversed(recent[-5:]):
            lines.append(format_mint_line(mint))
        if not recent:
            lines.append("  No mints recorded yet.")
    await ctx.send("🌱 **Mint tracker**\n" + "\n".join(lines))


@bot.command(name="help")
async def help_cmd(ctx):
    """!help — what this bot does."""
    await ctx.send(
        "**OTB Bot — The Algorithmic Trap House**\n"
        "`!price <coin>` — live price + 24h change (btc, eth, sol, caster, plank...)\n"
        "`!scan` — latest daily digest\n"
        "`!wrap` — latest ITZ A WRAP episode\n"
        "`!dome` — latest OFF THE DOME\n"
        "`!radio` — what's playing on OTB RADIO\n"
        "`!fear` — Fear & Greed Index\n"
        "`!floor [collection]` — live NFT floor price(s) being tracked\n"
        "`!mint [collection]` — recent mints + watched collections"
    )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main():
    # Opus (voice/radio): the Oracle box has no system libopus — it's
    # extracted to ~/lib/opus, which find_library can't see. Load by path.
    opus_path = Path.home() / "lib/opus/usr/lib/x86_64-linux-gnu/libopus.so.0"
    if opus_path.exists() and not discord.opus.is_loaded():
        discord.opus.load_opus(str(opus_path))
        log.info("loaded libopus from %s", opus_path)
    token = os.getenv("DISCORD_TOKEN")
    if not token:
        raise SystemExit(
            "DISCORD_TOKEN is not set.\n"
            "Copy .env.example to .env, put your bot token in it, and run again.\n"
            "The token is NEVER stored in code — env var only."
        )
    bot.run(token)


if __name__ == "__main__":
    main()
