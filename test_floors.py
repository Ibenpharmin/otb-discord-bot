"""Fixture tests for the floor-alerts build. Run: python test_floors.py"""
import asyncio
import json
import os
import sys
import tempfile
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).parent))
os.environ.pop("OPENSEA_API_KEY", None)

import floors

PASS = 0
FAIL = 0


def check(name, cond):
    global PASS, FAIL
    if cond:
        PASS += 1
        print(f" PASS {name}")
    else:
        FAIL += 1
        print(f" FAIL {name}")


# --- config loading ---------------------------------------------------------
with tempfile.TemporaryDirectory() as td:
    p = Path(td) / "collections.json"
    p.write_text(json.dumps({"collections": [
        {"slug": "alpha", "name": "Alpha", "chain": "ethereum", "currency": "ETH"},
        {"name": "no slug here"},
        {"slug": "beta", "move_threshold_pct": 25},
    ]}))
    cols = floors.load_collections(p)
    check("collections: 2 valid entries", len(cols) == 2)
    check("collections: default thresholds", cols[0]["move_threshold_pct"] == 10.0
          and cols[0]["below_threshold_pct"] == 5.0)
    check("collections: per-entry override", cols[1]["move_threshold_pct"] == 25.0)
    check("collections: missing file -> []",
          floors.load_collections(Path(td) / "nope.json") == [])
    sp = Path(td) / "state.json"
    floors.save_floor_state(sp, {"alpha": {"floor": 1.5, "alerted_deals": ["7:1.2"]}})
    check("state round-trip",
          floors.load_floor_state(sp)["alpha"]["floor"] == 1.5)

# --- alert math ---------------------------------------------------------------
check("move: +12% alerts at 10", floors.is_move_alert(1.0, 1.12, 10.0))
check("move: +5% silent at 10", not floors.is_move_alert(1.0, 1.05, 10.0))
check("move: -15% alerts at 10", floors.is_move_alert(2.0, 1.7, 10.0))
check("move: no baseline no alert", not floors.is_move_alert(None, 1.0, 10.0))
check("move: zero baseline no alert", not floors.is_move_alert(0, 1.0, 10.0))
check("move pct value", abs(floors.floor_move_pct(2.0, 1.7) - (-15.0)) < 1e-9)

deals = floors.below_floor_deals(
    [{"price": 1.0, "token_id": "1"}, {"price": 0.92, "token_id": "2"},
     {"price": 0.97, "token_id": "3"}],
    floor=1.0, threshold_pct=5.0)
check("deals: only the 8%-below listing", len(deals) == 1
      and deals[0]["token_id"] == "2" and abs(deals[0]["pct_below"] - 8.0) < 1e-9)
check("deals: at-floor listing is not a deal",
      floors.below_floor_deals([{"price": 1.0, "token_id": "1"}], 1.0, 5.0) == [])
check("deals: no listings -> []", floors.below_floor_deals(None, 1.0, 5.0) == [])

# --- formatting ---------------------------------------------------------------
check("fmt 1.25", floors.format_amount(1.25) == "1.25")
check("fmt 0.00042", floors.format_amount(0.00042) == "0.00042")
check("fmt 12", floors.format_amount(12.0) == "12")
entry = {"slug": "alpha", "name": "Alpha", "chain": "ethereum", "currency": "ETH",
         "move_threshold_pct": 10.0, "below_threshold_pct": 5.0}
check("move alert text", "Floor move" in floors.format_move_alert(entry, 1.0, 1.12)
      and "+12.0%" in floors.format_move_alert(entry, 1.0, 1.12))
check("deal alert text", "Below-floor" in floors.format_deal_alert(
    entry, 1.0, {"price": 0.92, "token_id": "2", "pct_below": 8.0, "deal_id": "2:0.92"}))

# --- fetchers with a fake session ----------------------------------------------
class FakeResp:
    def __init__(self, status, payload=None, headers=None):
        self.status = status
        self._payload = payload
        self.headers = headers or {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *a):
        return False

    async def json(self):
        return self._payload


class FakeSession:
    def __init__(self, script):
        self.script = list(script)
        self.calls = 0

    def get(self, url, params=None, headers=None, timeout=None):
        self.calls += 1
        return self.script.pop(0)


async def fetch_tests():
    stats = {"total": {"floor_price": 0.42, "volume": 100.0}}
    s = FakeSession([FakeResp(200, stats)])
    check("fetch floor parses stats", await floors.fetch_floor_price(s, "alpha") == 0.42)

    listings = {"listings": [
        {"price": {"current": {"value": "420000000000000000", "decimals": 18,
                                "currency": "ETH"}},
         "protocol_data": {"parameters": {"offer": [
             {"identifierOrCriteria": "777"}]}}},
    ]}
    s = FakeSession([FakeResp(200, listings)])
    got = await floors.fetch_best_listings(s, "beta")
    check("fetch listings parses wei + token",
          got == [{"price": 0.42, "token_id": "777"}])

    async def nosleep(_):
        return None
    with mock.patch.object(asyncio, "sleep", nosleep):
        s = FakeSession([FakeResp(429, headers={"Retry-After": "1"}),
                         FakeResp(429), FakeResp(200, stats)])
        floors._CACHE.clear()
        check("429 backoff then success",
              await floors.fetch_floor_price(s, "gamma") == 0.42 and s.calls == 3)
        s = FakeSession([FakeResp(429), FakeResp(429), FakeResp(429)])
        floors._CACHE.clear()
        check("429 exhausted -> None",
              await floors.fetch_floor_price(s, "delta") is None and s.calls == 3)
        s = FakeSession([FakeResp(500)])
        floors._CACHE.clear()
        check("500 -> None, no retry",
              await floors.fetch_floor_price(s, "eps") is None and s.calls == 1)


os.environ["OPENSEA_API_KEY"] = "test-key-not-real"
asyncio.run(fetch_tests())

# no key -> no HTTP at all
os.environ.pop("OPENSEA_API_KEY", None)
floors._CACHE.clear()
s = FakeSession([])
check("no API key -> None, zero calls",
      asyncio.run(floors.fetch_floor_price(s, "alpha")) is None and s.calls == 0)

# --- _poll_collection integration (real OTBBot, scripted fetchers) -------------
os.environ["OPENSEA_API_KEY"] = "test-key-not-real"
import bot as botmod


class FakeChannel:
    def __init__(self):
        self.sent = []

    async def send(self, msg):
        self.sent.append(msg)


async def poll_tests():
    bot = botmod.bot
    channel = FakeChannel()
    state = {}
    floors_seq = [1.0, 1.12, 1.12, 1.12]
    listings_seq = [[], [], [{"price": 1.03, "token_id": "9"}],
                    [{"price": 1.03, "token_id": "9"}]]

    async def fake_floor(session, slug):
        return floors_seq.pop(0)

    async def fake_listings(session, slug, limit=5):
        return listings_seq.pop(0)

    with mock.patch.object(botmod, "fetch_floor_price", fake_floor), \
         mock.patch.object(botmod, "fetch_best_listings", fake_listings):
        c1 = await bot._poll_collection(entry, channel, state)
        check("poll 1: baseline set, no sends", c1 and channel.sent == []
              and state["alpha"]["floor"] == 1.0)
        await bot._poll_collection(entry, channel, state)
        check("poll 2: +12% move alert sent", len(channel.sent) == 1
              and "Floor move" in channel.sent[0])
        await bot._poll_collection(entry, channel, state)
        check("poll 3: below-floor deal alert sent", len(channel.sent) == 2
              and "Below-floor" in channel.sent[1])
        await bot._poll_collection(entry, channel, state)
        check("poll 4: same deal deduped, no new send", len(channel.sent) == 2)


asyncio.run(poll_tests())
check("floor loop interval in quest bounds",
      botmod.FLOOR_POLL_MINUTES == 20 and 15 <= botmod.FLOOR_POLL_MINUTES <= 30)

print(f"\n{PASS} PASS / {FAIL} FAIL")
sys.exit(1 if FAIL else 0)
