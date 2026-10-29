"""Offline stand-ins for every outside service, returning payloads in the formats documented by each provider
(Upstox v2/v3 market-quote, historical-candle, option-chain and market-status; Upstox instrument JSON; NSE archives;
niftyindices constituent CSV; Google News RSS). Used only by the tests."""
import gzip
import json
import random
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, PlainTextResponse, Response

IST = ZoneInfo("Asia/Kolkata")
SYMS = ["HDFCBANK", "ICICIBANK", "SBIN", "KOTAKBANK", "AXISBANK", "BAJFINANCE", "BAJAJFINSV", "JIOFIN", "HDFCLIFE", "SBILIFE", "BSE",
        "SHRIRAMFIN", "TCS", "INFY", "HCLTECH", "TECHM", "RELIANCE", "ONGC", "NTPC", "POWERGRID", "COALINDIA", "HINDUNILVR", "ITC",
        "NESTLEIND", "TATACONSUM", "ASIANPAINT", "TITAN", "TRENT", "ETERNAL", "MARUTI", "M&M", "TMPV", "BAJAJ-AUTO", "EICHERMOT",
        "SUNPHARMA", "CIPLA", "DRREDDY", "APOLLOHOSP", "MAXHEALTH", "TATASTEEL", "JSWSTEEL", "HINDALCO", "ULTRACEMCO", "GRASIM", "LT",
        "ADANIENT", "ADANIPORTS", "BEL", "INDIGO", "BHARTIARTL"]
ISIN = {s: f"INE{i:03d}A01010" for i, s in enumerate(SYMS)}
HOLIDAY = (datetime.now(IST) - timedelta(days=8)).date()
while HOLIDAY.weekday() >= 5:
    HOLIDAY -= timedelta(days=1)
PRICE = {s: 200 + (i * 137) % 4800 for i, s in enumerate(SYMS)}
STATE = {"calls": [], "fail_token": False}
# Upstox short names for the indices in the nightly feed (NSE's full names differ; 'Nifty Media' left out on purpose)
UPSTOX_INDEX_NAMES = ["Nifty 50", "Nifty Next 50", "Nifty Bank", "Nifty Fin Service", "Nifty 100", "Nifty 500", "NIFTY MIDCAP 100",
                      "NIFTY SMLCAP 100", "India VIX", "NIFTY MIDCAP 150", "Nifty Auto", "Nifty FMCG", "Nifty IT", "Nifty Metal",
                      "Nifty Pharma", "Nifty PSU Bank", "Nifty Pvt Bank", "Nifty Realty", "NIFTY HEALTHCARE", "NIFTY CONSR DURBL",
                      "NIFTY OIL AND GAS", "Nifty Commodities", "Nifty CPSE", "Nifty Energy", "Nifty Infra", "Nifty PSE",
                      "NIFTY IND DEFENCE", "NIFTY CAPITAL MKT"]
app = FastAPI()


def _exp_ms(d):
    return int(datetime(d.year, d.month, d.day, 23, 59, 59, tzinfo=IST).timestamp() * 1000)


@app.middleware("http")
async def record(request: Request, call_next):
    STATE["calls"].append(str(request.url))
    if request.url.path.startswith(("/v2/", "/v3/")):
        if STATE["fail_token"]:
            return JSONResponse({"status": "error", "errors": [{"errorCode": "UDAPI100050", "message": "Invalid token"}]}, status_code=401)
        if request.headers.get("authorization") != "Bearer test-token":
            return JSONResponse({"status": "error"}, status_code=401)
    return await call_next(request)


# ------------------------------------------------------------------ instruments (Upstox JSON format)
@app.get("/instr/{exch}.json.gz")
def instruments(exch: str):
    today = datetime.now(IST).date()
    rows = []
    if exch == "NSE":
        exp1, exp2 = today + timedelta(days=19), today + timedelta(days=47)
        for i, s in enumerate(SYMS):
            rows.append({"segment": "NSE_EQ", "name": s + " LTD", "exchange": "NSE", "isin": ISIN[s], "instrument_type": "EQ",
                         "instrument_key": f"NSE_EQ|{ISIN[s]}", "lot_size": 1, "trading_symbol": s, "short_name": s.title()})
            if s in ("ETERNAL", "JIOFIN"):
                continue                                                     # stocks without F&O in this fixture
            for j, e in enumerate((exp1, exp2)):
                rows.append({"segment": "NSE_FO", "exchange": "NSE", "expiry": _exp_ms(e), "instrument_type": "FUT", "underlying_symbol": s,
                             "instrument_key": f"NSE_FO|F{i}{j}", "underlying_type": "EQUITY", "trading_symbol": f"{s} FUT", "strike_price": 0.0})
            for k in range(-3, 4):
                for t in ("CE", "PE"):
                    rows.append({"segment": "NSE_FO", "exchange": "NSE", "expiry": _exp_ms(exp1), "instrument_type": t, "underlying_symbol": s,
                                 "instrument_key": f"NSE_FO|O{i}{k}{t}", "underlying_type": "EQUITY", "strike_price": PRICE[s] + 20 * k})
        for nm in UPSTOX_INDEX_NAMES:                                           # index keys use Upstox's short names
            rows.append({"segment": "NSE_INDEX", "name": nm, "exchange": "NSE", "instrument_type": "INDEX",
                         "instrument_key": f"NSE_INDEX|{nm}", "trading_symbol": nm.upper()})
        for e in (today + timedelta(days=2), today + timedelta(days=9)):              # Nifty index options, lot 75
            for t in ("CE", "PE"):
                rows.append({"segment": "NSE_FO", "exchange": "NSE", "expiry": _exp_ms(e), "instrument_type": t, "underlying_symbol": "NIFTY",
                             "underlying_key": "NSE_INDEX|Nifty 50", "instrument_key": f"NSE_FO|N{e:%m%d}{t}", "underlying_type": "INDEX",
                             "strike_price": 25000.0, "lot_size": 75})
        rows.append({"segment": "NSE_FO", "exchange": "NSE", "expiry": _exp_ms(today - timedelta(days=3)), "instrument_type": "CE",
                     "underlying_symbol": "HDFCBANK", "instrument_key": "NSE_FO|OLD", "underlying_type": "EQUITY", "strike_price": 1})
    else:
        for u in ("CRUDEOIL", "NATURALGAS", "GOLD", "SILVER", "COPPER", "ALUMINIUM", "GOLDM", "CRUDEOILM"):
            for j, e in enumerate((today + timedelta(days=12), today + timedelta(days=40))):
                rows.append({"segment": "MCX_FO", "exchange": "MCX", "expiry": _exp_ms(e), "instrument_type": "FUT", "underlying_symbol": u,
                             "name": u, "instrument_key": f"MCX_FO|{u}{j}", "underlying_type": "COM", "trading_symbol": f"{u} FUT"})
    return Response(gzip.compress(json.dumps(rows).encode()), media_type="application/gzip")


def _quote_price(key: str) -> float:
    if key.startswith("NSE_FO|F"):                                           # futures trade near their underlying
        return PRICE[SYMS[int(key.split("|F")[1][:-1])]]
    if key.startswith("NSE_EQ|"):
        s = next(k for k, v in ISIN.items() if v == key.split("|")[1])
        return PRICE[s]
    return {"NSE_INDEX|Nifty 50": 25010.5, "NSE_INDEX|Nifty Bank": 56110.2, "NSE_INDEX|India VIX": 11.9}.get(key, 1000.0 + len(key) * 10)


# ------------------------------------------------------------------ Upstox REST (documented response formats)
@app.get("/v2/market-quote/quotes")
def quotes(instrument_key: str):
    data = {}
    for key in instrument_key.split(","):
        p = _quote_price(key)
        under = f"NSE_EQ|{ISIN[SYMS[int(key.split('|F')[1][:-1])]]}" if key.startswith("NSE_FO|F") else key
        r = random.Random(under)                                              # a future moves with its stock
        chg = round(p * r.uniform(-0.025, 0.025), 2)
        sym = key.split("|")[1]
        is_fut = key.startswith("NSE_FO|F")
        data[f"{key.split('|')[0]}:{sym}"] = {
            "ohlc": {"open": p * 0.995, "high": p * 1.012, "low": p * 0.985, "close": p}, "depth": {"buy": [], "sell": []},
            "timestamp": datetime.now(IST).isoformat(), "instrument_token": key, "symbol": sym,
            "last_price": p + chg + (p * 0.004 if is_fut else 0), "volume": 1234567, "average_price": p * 1.001,
            "oi": (5_000_000 if key.endswith("0") else 900_000) if is_fut else 0, "net_change": chg,
            "total_buy_quantity": 1, "total_sell_quantity": 1, "lower_circuit_limit": p * 0.8, "upper_circuit_limit": p * 1.2,
            "last_trade_time": str(int(datetime.now().timestamp() * 1000)), "oi_day_high": 0, "oi_day_low": 0}
    return {"status": "success", "data": data}


def _gen(n, start, step, base, seed):
    r, p, out = random.Random(seed), base, []
    for i in range(n):
        o = p
        c = o * (1 + r.gauss(0, 0.003))
        ts = (start + i * step).isoformat()
        out.append([ts, round(o, 2), round(max(o, c) * 1.001, 2), round(min(o, c) * 0.999, 2), round(c, 2), int(r.uniform(1e4, 9e4)),
                    int(4_800_000 + i * 1000)])
        p = c
    return out[::-1]                                                          # Upstox returns newest first


@app.get("/v3/historical-candle/intraday/{key}/{unit}/{n}")
def intraday(key: str, unit: str, n: int):
    now = datetime.now(IST)
    start = now.replace(hour=9, minute=15, second=0, microsecond=0)
    if now.weekday() >= 5 or now < start or "Nifty Bank" in key:            # no session today -> empty (tests the fallback)
        return {"status": "success", "data": {"candles": []}}
    count = max(1, min(int((min(now, now.replace(hour=15, minute=30)) - start).total_seconds() // (60 * n)), 375 // n))
    return {"status": "success", "data": {"candles": _gen(count, start, timedelta(minutes=n), _quote_price(key), key + unit + str(n))}}


@app.get("/v3/historical-candle/{key}/{unit}/{n}/{to_date}/{from_date}")
def historical(key: str, unit: str, n: int, to_date: str, from_date: str):
    to_d = datetime.fromisoformat(to_date).replace(tzinfo=IST)
    fr_d = datetime.fromisoformat(from_date).replace(tzinfo=IST)
    days = [fr_d + timedelta(days=i) for i in range((to_d - fr_d).days + 1)]
    days = [d for d in days if d.weekday() < 5 and d.date() < datetime.now(IST).date()]
    base = _quote_price(key)
    if unit == "days":
        rows = _gen(len(days), days[0], timedelta(days=1), base * 0.9, key)
        rows = rows[::-1]
        for r, d in zip(rows, days):
            r[0] = d.replace(hour=0, minute=0).isoformat()
        return {"status": "success", "data": {"candles": rows[::-1]}}
    out = []
    for d in days[-6:]:
        out += _gen(375 // n, d.replace(hour=9, minute=15), timedelta(minutes=n), base, key + str(d.date()))[::-1]
    return {"status": "success", "data": {"candles": out[::-1]}}


@app.get("/v2/option/chain")
def chain(instrument_key: str, expiry_date: str):
    spot = _quote_price(instrument_key)
    unit = 75 if instrument_key == "NSE_INDEX|Nifty 50" else 1
    rows = []
    for k in range(-6, 7):
        strike = round(spot / 20) * 20 + 20 * k
        rows.append({"expiry": expiry_date, "pcr": 1.1, "strike_price": strike, "underlying_key": instrument_key, "underlying_spot_price": spot,
                     "call_options": {"instrument_key": "NSE_FO|1", "market_data": {"ltp": 10, "volume": (1000 + 100 * k) * unit, "oi": (100000 + 20000 * max(k, 0)) * unit,
                                                                                      "close_price": 9, "prev_oi": 90000 * unit},
                                      "option_greeks": {"vega": 1, "theta": -1, "gamma": 0, "delta": 0.5, "iv": 22.0 + 0.2 * k, "pop": 50}},
                     "put_options": {"instrument_key": "NSE_FO|2", "market_data": {"ltp": 10, "volume": (1200 - 100 * k) * unit, "oi": (110000 + 25000 * max(-k, 0)) * unit,
                                                                                     "close_price": 9, "prev_oi": 95000 * unit},
                                     "option_greeks": {"vega": 1, "theta": -1, "gamma": 0, "delta": -0.5, "iv": 23.0 - 0.3 * k, "pop": 50}}})
    return {"status": "success", "data": rows}


@app.get("/v2/market/status/{exch}")
def mstatus(exch: str):
    return {"status": "success", "data": {"exchange": exch, "status": "NORMAL_OPEN", "last_updated": 1}}


# ------------------------------------------------------------------ niftyindices, NSE archives, Google News
@app.get("/nifty.csv", response_class=PlainTextResponse)
def nifty():
    lines = ["Company Name,Industry,Symbol,Series,ISIN Code"]
    for s in SYMS:
        lines.append(f"{s.title()} Ltd.,Financial Services,{s},EQ,{ISIN[s]}")
    return "\n".join(lines)


@app.get("/nse/products/content/{fname}", response_class=PlainTextResponse)
def bhav(fname: str):
    d = datetime.strptime(fname.split("_")[-1][:8], "%d%m%Y")
    if d.weekday() >= 5:
        return PlainTextResponse("not found", status_code=404)
    if d.date() == HOLIDAY:
        d = d - timedelta(days=1 if d.weekday() else 3)                             # previous session's content
    lines = ["SYMBOL, SERIES, DATE1, PREV_CLOSE, OPEN_PRICE, HIGH_PRICE, LOW_PRICE, LAST_PRICE, CLOSE_PRICE, AVG_PRICE, TTL_TRD_QNTY, "
             "TURNOVER_LACS, NO_OF_TRADES, DELIV_QTY, DELIV_PER"]
    for i, s in enumerate(SYMS):
        pct = 40 + (i * 7 + d.day * 3) % 30
        lines.append(f"{s}, EQ, {d:%d-%b-%Y}, 100, 100, 101, 99, 100, 100, 100, 1000, 1, 10, 500, {pct:.2f}")
    return "\n".join(lines)


@app.get("/nse/content/nsccl/{fname}", response_class=PlainTextResponse)
def part(fname: str):
    return ('"Participant wise Open Interest (no. of contracts) in Equity Derivatives as on Oct 08, 2026"\n'
            "Client Type,Future Index Long,Future Index Short,Future Stock Long,Future Stock Short\n"
            "Client,100,200,1,1\nDII,50,60,1,1\nFII,30000,70000,1,1\nPro,10,10,1,1\nTOTAL,1,1,1,1\n")


MARKET_ITEMS = [("Sensex, Nifty end higher as banks gain", "Moneycontrol", 1), ("Rupee steadies ahead of Fed decision", "Reuters", 2),
                ("Nifty outlook: what to expect tomorrow", "Random Blog", 2),                         # not a major publisher: dropped
                ("FPIs turn net buyers in October", "The Economic Times", 3), ("Sensex, Nifty end higher as banks gain", "Mint", 4)]


@app.get("/rss")
def rss(q: str = ""):
    now = datetime.now(IST)
    if "Sensex" in q:
        body = "".join(f"<item><title>{t} - {src}</title><pubDate>{(now - timedelta(hours=h)).strftime('%a, %d %b %Y %H:%M:%S %z')}</pubDate>"
                       f"<source url='x'>{src}</source></item>" for t, src, h in MARKET_ITEMS)
        return Response(f"<?xml version='1.0'?><rss><channel>{body}</channel></rss>", media_type="application/xml")
    items = [("HDFC Bank shares jump after strong Q2 profit growth", "Moneycontrol", 1),
             ("HDFC Bank faces RBI penalty over KYC lapses", "Economic Times", 3),
             ("Gail Smith obituary - Daily Herald", "Daily Herald", 2),                      # irrelevant: must be dropped
             ("HDFC Bank shares jump after strong Q2 profit growth", "Business Standard", 4)]  # duplicate: must be dropped
    body = "".join(f"<item><title>{t} - {src}</title><pubDate>{(now - timedelta(hours=h)).strftime('%a, %d %b %Y %H:%M:%S %z')}</pubDate>"
                   f"<source url='x'>{src}</source></item>" for t, src, h in items)
    return Response(f"<?xml version='1.0'?><rss><channel>{body}</channel></rss>", media_type="application/xml")


@app.post("/_control")
async def control(request: Request):
    STATE.update(await request.json())
    return {"ok": True}


@app.get("/yahoo/{sym}")
def yahoo(sym: str):
    if sym == "^KS11":
        return JSONResponse({"chart": {"result": None, "error": {"code": "Not Found"}}}, status_code=404)   # falls back to the feed
    now = datetime.now(IST)
    days = [now - timedelta(days=i) for i in range(30, 0, -1)]
    days = [d for d in days if d.weekday() < 5]
    closes = [100 + i for i in range(len(days))]
    return {"chart": {"result": [{"meta": {"currency": "USD", "regularMarketPrice": 150.0, "regularMarketTime": int(now.timestamp()),
                                           "exchangeTimezoneName": "America/New_York", "chartPreviousClose": 90.0},
                                  "timestamp": [int(d.replace(hour=16).timestamp()) for d in days],
                                  "indicators": {"quote": [{"close": closes}]}}], "error": None}}
