import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import httpx
import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
os.environ.setdefault("CACHE_DIR", "/tmp/terminal_test_cache_unit")
import app as A  # noqa: E402

MOCK, APP = 8765, 8766


# ------------------------------------------------------------------ unit: calculations
def test_parse_quote_doc_sample():
    v = {"ohlc": {"open": 53.4, "high": 53.8, "low": 51.75, "close": 52.05}, "instrument_token": "NSE_EQ|INE848E01016",
         "last_price": 52.04999923706055, "volume": 24123697, "average_price": 52.56, "oi": 0, "net_change": -1.0500000000000043,
         "last_trade_time": "1697624972130"}
    q = A.parse_quote(v)
    assert abs(q["prev"] - 53.1) < 1e-6 and abs(q["pct"] - (-1.05 / 53.1 * 100)) < 1e-5 and q["vwap"] == 52.56


def test_candles_sorted_ascending_from_newest_first():
    data = {"candles": [["2025-01-12T15:15:00+05:30", 2305.3, 2307.05, 2301, 2304.65, 559982, 0],
                        ["2025-01-12T14:45:00+05:30", 2309.1, 2310.75, 2305.25, 2305.3, 740124, 0]]}
    c = A._candles(data)
    assert c[0][0] < c[1][0] and c[1][4] == 2304.65


def _row(k, c_oi, p_oi, c_iv=20, p_iv=21):
    return {"strike_price": k, "underlying_spot_price": 100,
            "call_options": {"market_data": {"oi": c_oi, "prev_oi": c_oi - 10, "volume": 5}, "option_greeks": {"iv": c_iv}},
            "put_options": {"market_data": {"oi": p_oi, "prev_oi": p_oi - 5, "volume": 7}, "option_greeks": {"iv": p_iv}}}


def test_chain_metrics_max_pain_pcr_atm():
    rows = [_row(90, 10, 300), _row(95, 50, 200), _row(100, 100, 100), _row(105, 300, 20), _row(110, 400, 10)]
    m = A.chain_metrics(rows, 101)
    # writers' payout is smallest at 100 for this book (calls above, puts below)
    payout = lambda K: sum(r["call_options"]["market_data"]["oi"] * max(0, K - r["strike_price"]) +
                           r["put_options"]["market_data"]["oi"] * max(0, r["strike_price"] - K) for r in rows)
    assert m["max_pain"] == min((r["strike_price"] for r in rows), key=payout) == 100
    assert abs(m["pcr_oi"] - 630 / 860) < 1e-9 and m["atm"] == 100 and m["atm_iv"] == 20.5
    assert m["call_oi_chg"] == 50 and m["put_oi_chg"] == 25 and sum(p["atm"] for p in m["profile"]) == 1


def test_rsi_and_buildup():
    assert A.rsi(list(range(1, 40))) == 100.0
    assert A.rsi([10, 9, 8, 7, 6, 5, 4, 3, 2, 1, 0.5, 0.4, 0.3, 0.2, 0.1, 0.05]) < 1
    flat = [100 + (1 if i % 2 else -1) for i in range(60)]
    assert 40 < A.rsi(flat) < 60
    assert A.buildup(1, 2) == "LONG BUILD-UP" and A.buildup(-1, 2) == "SHORT BUILD-UP"
    assert A.buildup(1, -2) == "SHORT COVERING" and A.buildup(-1, -2) == "LONG UNWINDING"


def test_lexicon_fallback_sentiment():
    s = A.sentiment(["HDFC Bank shares jump after strong profit growth", "Company faces penalty and probe, shares fall"])
    assert s[0] > 0 > s[1]


# ------------------------------------------------------------------ integration: real server against replica services
def _wait(port, t=40):
    end = time.time() + t
    while time.time() < end:
        try:
            socket.create_connection(("127.0.0.1", port), 0.5).close()
            return
        except OSError:
            time.sleep(0.3)
    raise RuntimeError(f"port {port} not up")


@pytest.fixture(scope="module")
def servers(tmp_path_factory):
    cache = tmp_path_factory.mktemp("cache")
    mock = subprocess.Popen([sys.executable, "-m", "uvicorn", "mock_services:app", "--port", str(MOCK), "--log-level", "warning"],
                            cwd=ROOT / "tests")
    _wait(MOCK)
    env = {**os.environ, "UPSTOX_ANALYTICS_TOKEN": "test-token", "ACCESS_CODE": "gim2026", "UPSTOX_BASE": f"http://127.0.0.1:{MOCK}",
           "INSTRUMENTS_URL": f"http://127.0.0.1:{MOCK}/instr/{{}}.json.gz", "NSE_ARCHIVES": f"http://127.0.0.1:{MOCK}/nse",
           "NIFTY_CSV": f"http://127.0.0.1:{MOCK}/nifty.csv", "NEWS_RSS": f"http://127.0.0.1:{MOCK}/rss", "CACHE_DIR": str(cache)}
    srv = subprocess.Popen([sys.executable, "-m", "uvicorn", "app:app", "--port", str(APP), "--log-level", "warning"], cwd=ROOT, env=env)
    _wait(APP)
    c = httpx.Client(base_url=f"http://127.0.0.1:{APP}", timeout=30)
    for _ in range(80):                                                         # wait for the background boot
        r = c.post("/login", data={"code": "gim2026"})
        cookie = r.cookies.get("tv_access")
        s = c.get("/api/snapshot", cookies={"tv_access": cookie}).json()
        if not s.get("loading") and s.get("health", {}).get("delivery_sessions"):
            break
        time.sleep(0.5)
    yield c, cookie
    srv.terminate()
    mock.terminate()


def test_access_gate(servers):
    c, cookie = servers
    assert c.get("/api/snapshot").status_code == 401
    assert "access code" in c.get("/").text.lower()
    assert c.post("/login", data={"code": "wrong"}).status_code == 401
    assert c.get("/healthz").json() == {"ok": True}
    assert "PREDICTIVE ANALYTICS TERMINAL" in c.get("/", cookies={"tv_access": cookie}).text


def test_snapshot_live_shape(servers):
    c, ck = servers
    s = c.get("/api/snapshot", cookies={"tv_access": ck}).json()
    assert s["source"] == "UPSTOX" and s["phase"] in ("OPEN", "PRE-OPEN", "CLOSED")
    assert len(s["stocks"]) == 50 and len(s["indices"]) == 3
    assert [x["name"] for x in s["commodities"]] == ["CRUDE", "NAT GAS", "GOLD", "SILVER", "COPPER", "ALUMINIUM"]   # mini contracts excluded
    assert sum(len(g["stocks"]) for g in s["groups"]) == 50 and len(s["leaders"]) == len(s["laggards"]) == 5
    assert s["breadth"]["up"] + s["breadth"]["down"] <= 50 and "_quotes" not in s
    assert s["health"]["mode"] == "LIVE" and s["health"]["token"] == "valid" and s["health"]["delivery_sessions"] == 20
    errs = {k: v for k, v in s["health"]["errors"].items() if k != "nlp"}     # FinBERT is not installed in the test sandbox
    assert s["health"]["fii"] and not errs, errs
    assert all(i["spark"] or i["name"] == "BANK NIFTY" for i in s["indices"])      # Bank Nifty exercises the fallback path


@pytest.mark.parametrize("tf", ["1m", "5m", "5d", "3m", "1y"])
def test_stock_detail_every_timeframe(servers, tf):
    c, ck = servers
    d = c.get(f"/api/stock/HDFCBANK?tf={tf}", cookies={"tv_access": ck}).json()
    assert d["tf"] == tf and len(d["candles"]) > 3
    assert all(d["candles"][i][0] < d["candles"][i + 1][0] for i in range(len(d["candles"]) - 1))
    if tf == "5d":
        import datetime as _dt
        assert len({_dt.datetime.fromtimestamp(x[0] / 1000).date() for x in d["candles"]}) <= 5
    assert len(d["chain"]["profile"]) == 9 and d["chain"]["max_pain"] and 0 < d["chain"]["pcr_oi"] < 3
    assert d["expiry"] and d["dte"] == 19
    f = d["fut"]
    assert f["buildup"] in ("LONG BUILD-UP", "SHORT BUILD-UP", "SHORT COVERING", "LONG UNWINDING") and f["basis_ann"] > 0
    assert 0 < f["rollover"] < 100 and len(d["deliv"]) == 20 and abs(d["fii"]["long_share"] - 0.3) < 1e-9
    assert len(d["signals"]) == 12 and {s["tone"] for s in d["signals"]} <= {"bull", "bear", "neutral"}
    L = d["levels"]
    assert L["lo52"] <= d["quote"]["ltp"] <= L["hi52"] or L["hi52"] > 0


def test_stock_without_options(servers):
    c, ck = servers
    d = c.get("/api/stock/ETERNAL?tf=5m", cookies={"tv_access": ck}).json()
    assert d["expiry"] is None and d["chain"] == {} and d["fut"] == {} and len(d["signals"]) == 12
    assert c.get("/api/stock/NOPE", cookies={"tv_access": ck}).status_code == 404


def test_news_filter_dedup_and_scores(servers):
    c, ck = servers
    j = c.get("/api/news/HDFCBANK", cookies={"tv_access": ck}).json()
    titles = [x["title"] for x in j["items"]]
    assert titles == ["HDFC Bank shares jump after strong Q2 profit growth", "HDFC Bank faces RBI penalty over KYC lapses"]
    assert j["items"][0]["score"] > 0 > j["items"][1]["score"] and j["items"][0]["source"] == "Moneycontrol"


def test_rejected_token_is_reported(servers):
    c, ck = servers
    httpx.post(f"http://127.0.0.1:{MOCK}/_control", json={"fail_token": True})
    try:
        time.sleep(25)                                                           # let the snapshot cache expire (closed-market TTL)
        s = c.get("/api/snapshot", cookies={"tv_access": ck}).json()
        assert s.get("stale") and s["health"]["upstox_ok"] is False and "rejected" in s["health"]["token"]
    finally:
        httpx.post(f"http://127.0.0.1:{MOCK}/_control", json={"fail_token": False})


def test_finbert_scores_from_pipeline_output(monkeypatch):
    # documented output of a transformers text-classification pipeline with top_k=None, for a list input
    fake = lambda texts: [[{"label": "positive", "score": 0.9}, {"label": "negative", "score": 0.05}, {"label": "neutral", "score": 0.05}],
                          [{"label": "neutral", "score": 0.6}, {"label": "negative", "score": 0.3}, {"label": "positive", "score": 0.1}]][:len(texts)]
    monkeypatch.setattr(A, "_finbert", fake)
    assert A.sentiment(["a", "b"]) == [0.85, -0.2]
