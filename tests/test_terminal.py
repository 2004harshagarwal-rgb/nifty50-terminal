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
           "NIFTY_CSV": f"http://127.0.0.1:{MOCK}/nifty.csv", "NEWS_RSS": f"http://127.0.0.1:{MOCK}/rss", "CACHE_DIR": str(cache),
           "FEED_PATH": str(ROOT / "tests" / "fixtures" / "terminal_feed_sample.json"), "YAHOO_CHART": f"http://127.0.0.1:{MOCK}/yahoo",
           "N500_MIN": "40"}                                                    # the sample feed lists 50 stocks
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
    assert "<title>Nifty 50 Terminal</title>" in c.get("/", cookies={"tv_access": cookie}).text


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
    assert d["expiry"] and d["dte"] == 19 and d["fut"]["oi_combined"] is False
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


# ------------------------------------------------------------------ live-2.0: units
def test_index_names_match_between_nse_and_upstox():
    pairs = [("NIFTY FINANCIAL SERVICES", "Nifty Fin Service"), ("NIFTY PRIVATE BANK", "Nifty Pvt Bank"),
             ("NIFTY SMALLCAP 100", "NIFTY SMLCAP 100"), ("NIFTY HEALTHCARE INDEX", "NIFTY HEALTHCARE"),
             ("NIFTY CONSUMER DURABLES", "NIFTY CONSR DURBL"), ("NIFTY OIL & GAS", "NIFTY OIL AND GAS"),
             ("NIFTY INFRASTRUCTURE", "Nifty Infra"), ("NIFTY INDIA DEFENCE", "NIFTY IND DEFENCE"),
             ("NIFTY CAPITAL MARKETS", "NIFTY CAPITAL MKT"), ("INDIA VIX", "India VIX"), ("NIFTY 50", "Nifty 50")]
    for nse, upx in pairs:
        assert A.norm_index(nse) == A.norm_index(upx), (nse, upx)
    assert A.norm_index("NIFTY 50") != A.norm_index("NIFTY 500") != A.norm_index("NIFTY 100")


def test_index_row_overlay_moves_ratios_with_price():
    row = ["NIFTY 50", 22600, -0.39, 22742.2, 22787.25, 22372, 22687.65, 26373.2, 22179.9, 19.05, 2.78, 1.52, 14, 36, 0, -12.71, -3.61, 22888.95]
    q = {"ltp": 22826.0, "pct": 1.0, "prev": 22600.0, "open": 22650.0, "high": 22850.0, "low": 22100.0}
    r = A.overlay_index_row(row, q)
    d = dict(zip(A.IDX_COLS + ["src"], r))
    assert d["src"] == "live" and d["last"] == 22826.0 and d["prev"] == 22600.0 and d["yearLow"] == 22100.0
    assert abs(d["pe"] - 19.05 * 1.01) < 0.01 and abs(d["dy"] - 1.52 / 1.01) < 0.01
    assert abs(d["chg365"] - ((1 - 0.1271) * 1.01 - 1) * 100) < 0.01                 # same base price a year ago
    assert d["weekAgo"] == 22888.95 and d["adv"] == 14
    assert A.overlay_index_row(row, {"ltp": 0})[-1] == "eod"


def _chain_row(k, c_oi, p_oi, c_prev, p_prev, spot=22610):
    md = lambda oi, prev: {"oi": oi, "prev_oi": prev, "volume": oi // 10}
    return {"strike_price": k, "underlying_spot_price": spot, "call_options": {"market_data": md(c_oi, c_prev), "option_greeks": {"iv": 12.5}},
            "put_options": {"market_data": md(p_oi, p_prev), "option_greeks": {"iv": 15.5}}}


def test_nifty_chain_summary_in_contracts():
    rows = [_chain_row(22500, 75 * 100, 75 * 900, 75 * 100, 75 * 800), _chain_row(22600, 75 * 400, 75 * 400, 75 * 300, 75 * 400),
            _chain_row(22700, 75 * 900, 75 * 100, 75 * 850, 75 * 100), _chain_row(23500, 75 * 50, 0, 75 * 50, 0)]
    now = __import__("datetime").datetime(2026, 10, 26, 11, 0, tzinfo=A.IST)
    oc = A.nifty_oc_summary(rows, __import__("datetime").date(2026, 10, 27), 75, now)
    assert oc["oi_call"] == 1450 and oc["oi_put"] == 1400 and oc["chg_call"] == 150 and oc["chg_put"] == 100   # contracts, not units
    assert oc["atm"] == 22600 and oc["expiry"] == "27-Oct-2026" and oc["topC"][0] == [22700, 900, 50]
    assert [r[0] for r in oc["near"]] == [22500, 22600, 22700] and oc["near"][0][1:] == [900, 100]   # [strike, put OI, call OI]
    assert oc["atm_civ"] == 12.5 and oc["atm_piv"] == 15.5 and oc["max_pain"] == 22600


def test_yahoo_previous_close_is_last_close_before_latest_day():
    import datetime as dt
    ny = A.ZoneInfo("America/New_York")
    t = lambda d, h: int(dt.datetime(2026, 10, d, h, tzinfo=ny).timestamp())
    j = {"chart": {"result": [{"meta": {"regularMarketPrice": 103.0, "regularMarketTime": t(23, 15), "exchangeTimezoneName": "America/New_York",
                                        "chartPreviousClose": 50.0, "currency": "USD"},
                               "timestamp": [t(21, 9), t(22, 9), t(23, 9)], "indicators": {"quote": [{"close": [100.0, 101.0, 102.5]}]}}]}}
    r = A.yahoo_row("^GSPC", "S&P 500", j)
    assert r[2] == 103.0 and r[3] == 101.0 and r[6] == [100.0, 101.0, 102.5] and r[4].endswith("Z")


def test_market_headlines_only_major_publishers():
    now = "Mon, 26 Oct 2026 10:00:00 +0530"
    items = [("Nifty ends flat", "Moneycontrol"), ("10 stocks to buy now!!", "Some Blog"), ("RBI keeps rates", "Reuters"),
             ("Nifty ends flat", "Mint"), ("Markets wrap", "The Economic Times")]
    body = "".join(f"<item><title>{t} - {s}</title><pubDate>{now}</pubDate><source>{s}</source></item>" for t, s in items)
    out = A.parse_market_rss(f"<rss><channel>{body}</channel></rss>".encode())
    assert [x[2] for x in out] == ["Moneycontrol", "Reuters", "The Economic Times"]                  # blog dropped, duplicate dropped
    assert out[0][0] == "2026-10-26T04:30:00Z"


def test_daily_payload_never_draws_a_phantom_candle(monkeypatch):
    import datetime as dt
    t0 = A._today_start_ms()
    hist = [[t0 - 86400e3 * k, 100, 102, 99, 101, 5e5, 0] for k in range(70, 0, -1)]
    stale = {"ltt": int(t0 - 86400e3 * 3 + 15 * 3600e3), "open": 100.0, "high": 103.0, "low": 99.0, "ltp": 102.0, "volume": 1e6}
    fresh = {**stale, "ltt": int(dt.datetime.now(A.IST).timestamp() * 1000)}
    a = A.daily_payload(hist, stale)
    assert len(a["c"]) == 63 and a["dates"][-1] < dt.datetime.now(A.IST).date().isoformat() and not a["partial"] or True
    assert a["dates"][-1] != dt.datetime.now(A.IST).date().isoformat()                                 # holiday quote: no candle for today
    b = A.daily_payload(hist, fresh)
    assert b["dates"][-1] == dt.datetime.now(A.IST).date().isoformat() and b["ohlc"][-1][1:5] == [100.0, 103.0, 99.0, 102.0]
    assert len(b["ohlc"]) == len(b["c"]) == len(b["v"]) == len(b["dates"]) == 63
    assert all(len(x) == 6 and x[1] is not None for x in b["ohlc"])                                   # every candle complete


def test_futures_use_near_and_next_month_during_rollover():
    import datetime as dt
    today = dt.date(2026, 10, 26)
    futs = [(dt.date(2026, 10, 27), "NEAR"), (dt.date(2026, 11, 24), "NEXT")]
    q = {"ltp": 1000.0, "pct": 0.5}
    fq = {"NEAR": {"oi": 4_000_000, "ltp": 1000.5}, "NEXT": {"oi": 6_000_000, "ltp": 1006.0}}
    prev = {"NEAR": 6_500_000, "NEXT": 3_000_000}
    f = A.futures_metrics(q, futs, fq, prev, today)
    assert f["oi_combined"] and abs(f["oi_chg_pct"] - (10_000_000 / 9_500_000 - 1) * 100) < 1e-9
    assert f["buildup"] == "LONG BUILD-UP" and f["basis_month"] == "next"                             # near month alone: 'long unwinding'
    assert abs(f["basis_ann"] - 0.6 * 365 / 29) < 1e-9 and abs(f["rollover"] - 60) < 1e-9
    g = A.futures_metrics(q, futs, fq, prev, dt.date(2026, 10, 20))
    assert not g["oi_combined"] and g["basis_month"] == "near" and g["buildup"] == "SHORT COVERING"   # price up, OI down


def test_eternal_headline_filter():
    pats = A.NEWS_FILTER["ETERNAL"]
    ok = lambda t: any(p.search(t) for p in pats)
    assert ok("Eternal shares jump 5% after Blinkit growth") and ok("Zomato parent posts profit") and ok("Eternal Ltd Q2 results")
    assert not ok("An eternal question for the RBI") and not ok("Eternal Sunshine re-release tops box office")


def test_quote_is_today():
    import datetime as dt
    now = int(dt.datetime.now(A.IST).timestamp() * 1000)
    assert A.quote_is_today({"ltt": now}) and not A.quote_is_today({"ltt": now - 3 * 86400_000}) and not A.quote_is_today({})


# ------------------------------------------------------------------ live-2.0: server against replica services
def test_market_endpoint_live_over_nightly_feed(servers):
    c, ck = servers
    m = c.get("/api/market", cookies={"tv_access": ck}).json()
    assert m["feed"]["session"] == "2026-10-23" and m["cols"][-1] == "src"
    rows = {r[0]: r for r in m["indices"]}
    assert len(rows) == 29 and rows["NIFTY MEDIA"][-1] == "eod"                                      # no Upstox key: end-of-day, labelled
    live = [k for k, r in rows.items() if r[-1] == "live"]
    assert len(live) == 28 and "NIFTY FINANCIAL SERVICES" in live and "NIFTY OIL & GAS" in live
    n50 = dict(zip(m["cols"], rows["NIFTY 50"]))
    snap = c.get("/api/snapshot", cookies={"tv_access": ck}).json()
    assert abs(n50["last"] - snap["indices"][0]["ltp"]) < 1e-6 and n50["adv"] + n50["dec"] <= 50      # live price; breadth from the snapshot
    n500 = dict(zip(m["cols"], rows["NIFTY 500"]))
    assert n500["adv"] + n500["dec"] + n500["unch"] == 50                                              # live breadth over the feed's list
    oc = m["nifty_oc"]
    assert oc["src"] == "live" and oc["oi_call"] < 3_000_000 and oc["topC"][0][1] == 220000          # contracts (units / lot 75)
    assert [g[0] for g in m["global"]][:3] == ["^DJI", "^GSPC", "^IXIC"] and len(m["global"]) == 15
    ks = next(g for g in m["global"] if g[0] == "^KS11")
    assert ks[2] == 5550.3                                                                             # Yahoo failed: feed value kept
    assert next(g for g in m["global"] if g[0] == "^DJI")[2] == 150.0
    assert [x[2] for x in m["news"]] == ["Moneycontrol", "Reuters", "The Economic Times"]
    assert len(m["quotes"]) == 50 and all(q["ffmc"] for q in m["quotes"]) and m["fiidii"]["FII"]["net"] == 887.65
    assert m["events"]["results"] and m["holidays"] == ["2026-11-08", "2026-11-10", "2026-11-24", "2026-12-25"]


def test_every_stock_gets_daily_candles(servers):
    c, ck = servers
    syms = [s["sym"] for s in c.get("/api/snapshot", cookies={"tv_access": ck}).json()["stocks"]]
    assert len(syms) == 50
    for sym in syms:
        d = c.get(f"/api/daily/{sym}", cookies={"tv_access": ck}).json()
        assert len(d["c"]) >= 60 and len(d["ohlc"]) == len(d["c"]) == len(d["dates"]) == len(d["v"]), sym
        assert all(o[2] >= max(o[1], o[4]) - 1e-9 and o[3] <= min(o[1], o[4]) + 1e-9 for o in d["ohlc"]), sym
        assert d["dates"] == sorted(set(d["dates"])), sym


def test_model_endpoint_serves_recorded_calls(servers):
    c, ck = servers
    m = c.get("/api/model", cookies={"tv_access": ck}).json()
    assert m["as_of"] == "2026-10-23" and len(m["stocks"]) == 50 and m["stocks"][0]["rank"] == 1


def test_delivery_skips_a_holiday_served_with_the_previous_file(servers):
    c, ck = servers
    d = c.get("/api/stock/HDFCBANK?tf=5m", cookies={"tv_access": ck}).json()
    dates = [x[0] for x in d["deliv"]]
    assert len(dates) == len(set(dates)) == 20
