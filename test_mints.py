"""Fixture tests for mints.py — no live keys, no network, stdlib only.

Mirrors the test_floors.py pattern: a plain script with PASS/FAIL
counters, FakeResp/FakeSession doubles, and asyncio.sleep patched for
the 429-backoff checks. Run it: `python test_mints.py` (exit 0 = all
pass). The poll-cycle tests drive mints.poll_collection directly —
the same cycle the bot loop wraps — so the full flow is covered
without importing bot.py (attachment code cannot execute here).
"""

import asyncio
import json
import os
import sys
import tempfile
from pathlib import Path
from unittest import mock

sys.path.insert(0, str(Path(__file__).parent))

import mints  # noqa: E402  (local module under test)

PASS = 0
FAIL = 0


def check(name, condition, extra=""):
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  PASS  {name}")
    else:
        FAIL += 1
        print(f"  FAIL  {name} {extra}")


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

TRANSFER = mints.TRANSFER_TOPIC
ZERO = mints.ZERO_ADDRESS_TOPIC
TO_TOPIC = "0x" + "00" * 12 + "aa" * 20
OTHER_TOPIC = "0x" + "00" * 12 + "bb" * 20
CONTRACT = "0x" + "11" * 20

OS_EVENTS = {
    "asset_events": [
        {
            "event_type": "mint",
            "event_timestamp": 1795000000,
            "transaction": "0xabc123",
            "nft": {"identifier": "42", "name": "Plank #42"},
            "to_account": {"address": "0x" + "aa" * 20},
        },
        {
            "event_type": "sale",  # not a mint — must be skipped
            "event_timestamp": 1795000100,
            "transaction": "0xdef456",
            "nft": {"identifier": "7"},
            "to_account": {"address": "0x" + "bb" * 20},
        },
        {
            "event_type": "mint",
            "event_timestamp": 1795000200,
            "transaction": "0xabc999",
            "nft": {"identifier": "43"},
            "to_account": {"address": "0x" + "cc" * 20},
        },
        {"event_type": "mint"},  # malformed — no nft, must be skipped
        "not-a-dict",  # malformed — must be skipped
    ]
}

RPC_LOGS = [
    {  # a mint: Transfer from the zero address
        "address": CONTRACT,
        "blockNumber": "0x1001",
        "transactionHash": "0xmint1",
        "topics": [TRANSFER, ZERO, TO_TOPIC, "0x" + "00" * 31 + "2a"],
    },
    {  # a plain transfer — not a mint, must be skipped
        "address": CONTRACT,
        "blockNumber": "0x1001",
        "transactionHash": "0xmove1",
        "topics": [TRANSFER, OTHER_TOPIC, TO_TOPIC, "0x" + "00" * 31 + "07"],
    },
    {  # malformed — short topics, must be skipped
        "address": CONTRACT,
        "blockNumber": "0x1001",
        "transactionHash": "0xbad1",
        "topics": [TRANSFER, ZERO],
    },
]


# ---------------------------------------------------------------------------
# Doubles
# ---------------------------------------------------------------------------

class FakeResp:
    def __init__(self, status=200, payload=None, headers=None):
        self.status = status
        self._payload = payload if payload is not None else {}
        self.headers = headers or {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    async def json(self):
        return self._payload


class FakeSession:
    """GET/POST doubles. Handlers receive the call and return a FakeResp."""

    def __init__(self, get_handler=None, post_handler=None):
        self.get_calls = []
        self.post_calls = []
        self.get_handler = get_handler or (lambda url, params: FakeResp(404))
        self.post_handler = post_handler or (lambda url, payload: FakeResp(404))

    def get(self, url, params=None, headers=None, timeout=None):
        self.get_calls.append((url, params))
        return self.get_handler(url, params)

    def post(self, url, json=None, timeout=None):
        self.post_calls.append((url, json))
        return self.post_handler(url, json)


def make_rpc_session(fixture):
    """A FakeSession serving OpenSea GETs + RPC POSTs from `fixture`:
    {"os_events": [...], "head": int, "logs": [...]}."""

    def get_handler(url, params):
        if "/events/collection/" in url:
            return FakeResp(200, {"asset_events": fixture["os_events"]})
        return FakeResp(404)

    def post_handler(url, payload):
        method = payload.get("method") if isinstance(payload, dict) else None
        if method == "eth_blockNumber":
            return FakeResp(200, {"jsonrpc": "2.0", "id": 1,
                                  "result": hex(fixture["head"])})
        if method == "eth_getLogs":
            return FakeResp(200, {"jsonrpc": "2.0", "id": 1,
                                  "result": fixture["logs"]})
        return FakeResp(200, {"jsonrpc": "2.0", "id": 1, "error": {"code": -32601}})

    return FakeSession(get_handler, post_handler)


# ---------------------------------------------------------------------------
# Pure parsing / formatting
# ---------------------------------------------------------------------------

def test_parse_opensea_events():
    print("\nparse_opensea_events")
    out = mints.parse_opensea_events(OS_EVENTS)
    check("counts only mint events", len(out) == 2, f"got {len(out)}")
    check("first token id", out[0]["token_id"] == "42")
    check("first to address", out[0]["to"] == "0x" + "aa" * 20)
    check("first tx", out[0]["tx"] == "0xabc123")
    check("first ts", out[0]["ts"] == 1795000000)
    check("first source", out[0]["source"] == "opensea")
    check("first dedupe key", out[0]["key"] == "os:0xabc123:42")
    check("second token id", out[1]["token_id"] == "43")
    check("empty payload -> []", mints.parse_opensea_events({}) == [])
    check("non-dict payload -> []", mints.parse_opensea_events(None) == [])


def test_parse_rpc_logs():
    print("\nparse_rpc_logs")
    out = mints.parse_rpc_logs(RPC_LOGS)
    check("counts only zero-address transfers", len(out) == 1, f"got {len(out)}")
    check("token id hex -> decimal", out[0]["token_id"] == "42")
    check("to address from topic[2]", out[0]["to"] == "0x" + "aa" * 20)
    check("tx hash", out[0]["tx"] == "0xmint1")
    check("block number", out[0]["block"] == 0x1001)
    check("source", out[0]["source"] == "rpc")
    check("dedupe key", out[0]["key"] == "rpc:0xmint1:42")
    check("empty logs -> []", mints.parse_rpc_logs([]) == [])
    check("bad hex token skipped",
          mints.parse_rpc_logs([{"topics": [TRANSFER, ZERO, TO_TOPIC, "zz"]}]) == [])


def test_new_mints():
    print("\nnew_mints")
    seen = {"os:0xabc123:42"}
    out = mints.new_mints(mints.parse_opensea_events(OS_EVENTS), seen)
    check("filters already-seen", len(out) == 1 and out[0]["token_id"] == "43")
    check("empty seen keeps all",
          len(mints.new_mints(mints.parse_opensea_events(OS_EVENTS), [])) == 2)


def test_formatting():
    print("\nformatting")
    entry = {"name": "RobinWood Plank", "slug": "robinwoodplanks42069",
             "contract": CONTRACT}
    rpc_mint = {"token_id": "42", "to": "0x" + "aa" * 20, "source": "rpc"}
    os_mint = {"token_id": "7", "to": None, "source": "opensea"}
    alert = mints.format_mint_alert(entry, rpc_mint)
    check("rpc alert names collection", "RobinWood Plank" in alert)
    check("rpc alert token", "Token #42" in alert)
    check("rpc alert short address", "0xaaaaaa…aaaa" in alert)
    check("rpc alert links to Blockscout",
          f"{mints.ROBINHOOD_EXPLORER}/token/{CONTRACT}/instance/42" in alert)
    os_alert = mints.format_mint_alert(entry, os_mint)
    check("opensea alert links to collection",
          "https://opensea.io/collection/robinwoodplanks42069" in os_alert)
    check("missing to -> unknown", "unknown" in os_alert)
    check("short_address None", mints.short_address(None) == "unknown")
    check("short_address short", mints.short_address("0x12") == "unknown")
    line = mints.format_mint_line(rpc_mint)
    check("mint line", line == "• #42 → 0xaaaaaa…aaaa (RPC)", line)
    watch = mints.format_watch_line(entry, {"recent": [rpc_mint]})
    check("watch line with contract + recent",
          "OpenSea + RPC" in watch and "last mint: #42" in watch, watch)
    no_contract = {"name": "Paper Planks", "slug": "paper-planks",
                   "contract": None}
    watch2 = mints.format_watch_line(no_contract, None)
    check("watch line without contract", "RPC off — no contract set" in watch2)


def test_contract_cleaning():
    print("\n_clean_contract")
    check("valid lowercase", mints._clean_contract(CONTRACT) == CONTRACT)
    check("uppercase normalized",
          mints._clean_contract(CONTRACT.upper()) == CONTRACT)
    check("missing 0x rejected", mints._clean_contract("11" * 20) is None)
    check("short rejected", mints._clean_contract("0x1234") is None)
    check("non-hex rejected", mints._clean_contract("0x" + "zz" * 20) is None)
    check("non-str rejected", mints._clean_contract(12345) is None)
    check("None rejected", mints._clean_contract(None) is None)


# ---------------------------------------------------------------------------
# Config + state IO
# ---------------------------------------------------------------------------

def test_config_io():
    print("\nconfig + state IO")
    with tempfile.TemporaryDirectory() as tmp:
        tmp = Path(tmp)
        mints_file = tmp / "mints.json"
        mints_file.write_text(json.dumps({
            "mints": [
                {"slug": "robinwoodplanks42069", "name": "RobinWood Plank",
                 "chain": "robinhood", "contract": CONTRACT},
                {"slug": "paper-planks", "name": "Paper Planks",
                 "chain": "robinhood", "contract": None},
                {"slug": "bad-addr", "contract": "not-an-address"},
                {"name": "no slug at all"},
            ]
        }))
        entries = mints.load_mints(mints_file)
        check("loads valid entries", len(entries) == 3, f"got {len(entries)}")
        check("contract kept", entries[0]["contract"] == CONTRACT)
        check("null contract stays null", entries[1]["contract"] is None)
        check("bad contract -> None", entries[2]["contract"] is None)
        check("chain default", entries[2]["chain"] == "robinhood")
        check("missing file -> []", mints.load_mints(tmp / "nope.json") == [])
        mints_file.write_text("{not json")
        check("invalid json -> []", mints.load_mints(mints_file) == [])

        state_file = tmp / "mint_state.json"
        check("missing state -> {}", mints.load_mint_state(state_file) == {})
        state = {"robinwoodplanks42069": {"seen": ["a"], "last_block": 5,
                                          "recent": [], "baselined": True}}
        mints.save_mint_state(state_file, state)
        check("state round-trips", mints.load_mint_state(state_file) == state)


# ---------------------------------------------------------------------------
# Async fetchers (fake session)
# ---------------------------------------------------------------------------

async def _test_get_json_429():
    print("\n_get_json 429 backoff")
    os.environ["OPENSEA_API_KEY"] = "test-key"
    calls = {"n": 0}

    def flaky(url, params):
        calls["n"] += 1
        if calls["n"] == 1:
            return FakeResp(429, {}, {"Retry-After": "1"})
        return FakeResp(200, {"ok": True})

    with mock.patch("mints.asyncio.sleep", new=mock.AsyncMock()) as sleep:
        session = FakeSession(get_handler=flaky)
        result = await mints._get_json(session, "https://x.test/events")
        check("429 then 200 returns payload", result == {"ok": True})
        check("slept once on 429", sleep.await_count == 1)
        check("retried once", calls["n"] == 2)

    def always_429(url, params):
        return FakeResp(429, {}, {})

    with mock.patch("mints.asyncio.sleep", new=mock.AsyncMock()) as sleep:
        session = FakeSession(get_handler=always_429)
        result = await mints._get_json(session, "https://x.test/events")
        check("persistent 429 -> None", result is None)
        check("retried 3 times", sleep.await_count == 3)

    session = FakeSession(get_handler=lambda u, p: FakeResp(500))
    check("500 -> None", await mints._get_json(session, "https://x.test/") is None)

    def boom(url, params):
        raise RuntimeError("network down")

    session = FakeSession(get_handler=boom)
    check("exception -> None", await mints._get_json(session, "https://x.test/") is None)

    os.environ.pop("OPENSEA_API_KEY", None)
    session = FakeSession(get_handler=lambda u, p: FakeResp(200, {"ok": True}))
    check("no key -> None, no call",
          await mints._get_json(session, "https://x.test/") is None
          and not session.get_calls)


async def _test_fetchers():
    print("\nfetchers")
    os.environ["OPENSEA_API_KEY"] = "test-key"
    mints._CACHE.clear()
    fixture = {"os_events": OS_EVENTS["asset_events"], "head": 0x1001,
               "logs": RPC_LOGS}
    session = make_rpc_session(fixture)

    first = await mints.fetch_opensea_mints(session, "robinwoodplanks42069")
    second = await mints.fetch_opensea_mints(session, "robinwoodplanks42069")
    check("fetch_opensea_mints parses", len(first) == 2)
    check("cache hit returns same object", first is second)
    check("cache avoids second GET", len(session.get_calls) == 1)

    check("fetch_latest_block parses hex",
          await mints.fetch_latest_block(session) == 0x1001)

    out = await mints.fetch_rpc_mints(session, CONTRACT, 0x1001, 0x1001)
    check("fetch_rpc_mints parses", len(out) == 1 and out[0]["token_id"] == "42")
    calls_before = len(session.post_calls)
    check("from > to returns [] without calling",
          await mints.fetch_rpc_mints(session, CONTRACT, 0x1002, 0x1001) == []
          and len(session.post_calls) == calls_before)

    err_session = FakeSession(
        post_handler=lambda u, p: FakeResp(200, {"jsonrpc": "2.0", "id": 1,
                                                 "error": {"code": -32000}}))
    check("rpc error -> None", await mints.fetch_latest_block(err_session) is None)
    os.environ.pop("OPENSEA_API_KEY", None)


# ---------------------------------------------------------------------------
# Poll cycle (the bot loop's core, driven without the bot)
# ---------------------------------------------------------------------------

async def _test_poll_cycle():
    print("\npoll_collection")
    os.environ["OPENSEA_API_KEY"] = "test-key"
    mints._CACHE.clear()
    entry = {"slug": "robinwoodplanks42069", "name": "RobinWood Plank",
             "chain": "robinhood", "contract": CONTRACT}
    fixture = {"os_events": OS_EVENTS["asset_events"], "head": 0x1000,
               "logs": []}
    session = make_rpc_session(fixture)
    state = {}
    sent = []

    async def send(text):
        sent.append(text)

    # Poll 1 — first sighting: baselines silently, no alerts.
    changed = await mints.poll_collection(session, entry, state, send,
                                          opensea_due=True)
    rec = state["robinwoodplanks42069"]
    check("poll 1 changes state", changed)
    check("poll 1 sends nothing (baseline)", sent == [])
    check("poll 1 baselined", rec["baselined"] is True)
    check("poll 1 records OpenSea mints as seen",
          len(rec["seen"]) == 2, f"seen={rec['seen']}")
    check("poll 1 anchors RPC head", rec["last_block"] == 0x1000)
    check("poll 1 recent empty", rec["recent"] == [])

    # Poll 2 — same data: nothing new, nothing sent.
    changed = await mints.poll_collection(session, entry, state, send,
                                          opensea_due=True)
    check("poll 2 sends nothing (no new mints)", sent == [])

    # Poll 3 — new block with an RPC mint + a new OpenSea mint.
    fixture["head"] = 0x1001
    fixture["logs"] = RPC_LOGS
    fixture["os_events"] = OS_EVENTS["asset_events"] + [{
        "event_type": "mint",
        "event_timestamp": 1795000300,
        "transaction": "0xabc777",
        "nft": {"identifier": "44"},
        "to_account": {"address": "0x" + "dd" * 20},
    }]
    mints._CACHE.clear()  # the fixture changed; the bot's 60s cache is bypassed
    changed = await mints.poll_collection(session, entry, state, send,
                                          opensea_due=True)
    check("poll 3 sends two alerts", len(sent) == 2, f"sent={len(sent)}")
    check("poll 3 alert mentions token 44", any("#44" in s for s in sent))
    check("poll 3 alert mentions token 42", any("#42" in s for s in sent))
    check("poll 3 advances last_block", rec["last_block"] == 0x1001)
    check("poll 3 recent holds both", len(rec["recent"]) == 2)
    check("poll 3 seen grows", len(rec["seen"]) == 4, f"seen={len(rec['seen'])}")

    # Poll 4 — duplicates only: nothing sent.
    mints._CACHE.clear()
    await mints.poll_collection(session, entry, state, send, opensea_due=True)
    check("poll 4 dedupes (no repeat alerts)", len(sent) == 2)

    # opensea_due=False skips the OpenSea route entirely.
    session2 = make_rpc_session(fixture)
    mints._CACHE.clear()
    await mints.poll_collection(session2, entry, state, send, opensea_due=False)
    check("opensea_due=False makes no GET calls", session2.get_calls == [])

    # Entry without a contract skips the RPC route entirely.
    no_contract = {"slug": "paper-planks", "name": "Paper Planks",
                   "chain": "robinhood", "contract": None}
    session3 = make_rpc_session(fixture)
    mints._CACHE.clear()
    await mints.poll_collection(session3, no_contract, state, send,
                                opensea_due=True)
    check("no contract makes no RPC calls", session3.post_calls == [])
    check("no contract still baselines",
          state["paper-planks"]["baselined"] is True)

    # Seen list is capped at 200 in the persisted state.
    state["robinwoodplanks42069"]["seen"] = [f"k{i}" for i in range(250)]
    mints._CACHE.clear()
    await mints.poll_collection(session, entry, state, send, opensea_due=True)
    check("seen capped at 200",
          len(state["robinwoodplanks42069"]["seen"]) == 200)

    # RPC gap: head jumps past the 1000-block cap — scan window is capped.
    fixture["head"] = 0x1001 + 5000
    fixture["logs"] = []
    session4 = make_rpc_session(fixture)
    mints._CACHE.clear()
    await mints.poll_collection(session4, entry, state, send, opensea_due=False)
    getlogs = [c for c in session4.post_calls
               if c[1].get("method") == "eth_getLogs"]
    from_block = int(getlogs[0][1]["params"][0]["fromBlock"], 16)
    check("gap scan capped at 1000 blocks",
          fixture["head"] - from_block == 999,
          f"window={fixture['head'] - from_block}")
    os.environ.pop("OPENSEA_API_KEY", None)


def main():
    print("=== test_mints.py ===")
    test_parse_opensea_events()
    test_parse_rpc_logs()
    test_new_mints()
    test_formatting()
    test_contract_cleaning()
    test_config_io()
    asyncio.run(_test_get_json_429())
    asyncio.run(_test_fetchers())
    asyncio.run(_test_poll_cycle())
    print(f"\n{PASS} passed, {FAIL} failed")
    return 1 if FAIL else 0


if __name__ == "__main__":
    sys.exit(main())
