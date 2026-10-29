"""
Nifty 50 Predictive Analytics Terminal - live web server (academic demonstration).

Market data : Upstox API, read-only Analytics Token held on the server as a secret (never sent to the browser):
              stocks, indices, MCX, candles, option chains (stocks and Nifty).
EOD data    : NSE public archives (delivery %, participant open interest).
Nightly feed: one JSON file built after each close by the research pipeline (Colab): NSE index statistics
              (P/E, P/B, yield, breadth, 30/365-day change), stock valuation (P/E, sector P/E, market cap, free
              float, 52-week range, industry), FII/DII cash flows, results and macro calendar, trading holidays and
              the frozen model's recorded calls. Live prices are laid over it during the session.
Global      : Yahoo Finance chart data (feed snapshot as fallback).
News        : Google News RSS (credible financial publishers only for market headlines), scored with FinBERT when
              it can load, otherwise a finance word list.

Environment variables (Render):
  UPSTOX_ANALYTICS_TOKEN  the Upstox Analytics Token (without it the page runs in clearly labelled DEMO mode)
  ACCESS_CODE             optional passcode that viewers must enter (recommended)
  FEED_URL                where the nightly feed is published (optional; FEED_PATH, a file in this repo, otherwise)
  FEED_TOKEN              read token for FEED_URL when it is private (optional)
"""
from __future__ import annotations

import asyncio
import gzip
import hashlib
import hmac
import json
import math
import os
import random
import re
import threading
import time
from collections import defaultdict
from datetime import date, datetime, timedelta
from email.utils import parsedate_to_datetime
from pathlib import Path
from urllib.parse import parse_qs, quote, quote_plus
from xml.etree import ElementTree as ET
from zoneinfo import ZoneInfo

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, RedirectResponse

VERSION = "live-2.0 (2026-10-26)"
IST = ZoneInfo("Asia/Kolkata")
HERE = Path(__file__).resolve().parent
TOKEN = (os.environ.get("UPSTOX_ANALYTICS_TOKEN") or os.environ.get("UPSTOX_TOKEN") or "").strip()
ACCESS_CODE = os.environ.get("ACCESS_CODE", "").strip()
DEMO = os.environ.get("DEMO", "").strip() == "1" or not TOKEN
UPSTOX = os.environ.get("UPSTOX_BASE", "https://api.upstox.com").rstrip("/")
INSTR_URL = os.environ.get("INSTRUMENTS_URL", "https://assets.upstox.com/market-quote/instruments/exchange/{}.json.gz")
NSE_ARCH = [u.rstrip("/") for u in os.environ.get(
    "NSE_ARCHIVES", "https://nsearchives.nseindia.com,https://archives.nseindia.com").split(",") if u.strip()]
NIFTY_CSV = os.environ.get("NIFTY_CSV", "https://www.niftyindices.com/IndexConstituent/ind_nifty50list.csv")
NEWS_RSS = os.environ.get("NEWS_RSS", "https://news.google.com/rss/search")
CACHE_DIR = Path(os.environ.get("CACHE_DIR", "/tmp/terminal_cache"))
CACHE_DIR.mkdir(parents=True, exist_ok=True)
FEED_PATH = Path(os.environ.get("FEED_PATH") or (HERE / "feed" / "terminal_feed.json"))
FEED_URL = os.environ.get("FEED_URL", "").strip()
FEED_TOKEN = os.environ.get("FEED_TOKEN", "").strip()
FEED_SCHEMA = "terminal-feed/1"
YAHOO = os.environ.get("YAHOO_CHART", "https://query1.finance.yahoo.com/v8/finance/chart").rstrip("/")
BROWSER = {"User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) "
                         "Chrome/124.0 Safari/537.36", "Accept": "*/*", "Accept-Language": "en-US,en;q=0.9"}

# ----------------------------------------------------------------------------------------------------- universe
GROUPS = ["FINANCIALS", "IT", "ENERGY & UTILITIES", "CONSUMER", "AUTOS", "HEALTHCARE", "MATERIALS", "INDUSTRIALS & SERVICES"]
FALLBACK = {  # used if the official constituent list cannot be fetched; group = display group on the market map
    "HDFCBANK": ("HDFC Bank", "FINANCIALS"), "ICICIBANK": ("ICICI Bank", "FINANCIALS"), "SBIN": ("State Bank of India", "FINANCIALS"),
    "KOTAKBANK": ("Kotak Mahindra Bank", "FINANCIALS"), "AXISBANK": ("Axis Bank", "FINANCIALS"),
    "BAJFINANCE": ("Bajaj Finance", "FINANCIALS"), "BAJAJFINSV": ("Bajaj Finserv", "FINANCIALS"),
    "JIOFIN": ("Jio Financial Services", "FINANCIALS"), "HDFCLIFE": ("HDFC Life Insurance", "FINANCIALS"),
    "SBILIFE": ("SBI Life Insurance", "FINANCIALS"), "BSE": ("BSE", "FINANCIALS"), "SHRIRAMFIN": ("Shriram Finance", "FINANCIALS"),
    "TCS": ("Tata Consultancy Services", "IT"), "INFY": ("Infosys", "IT"), "HCLTECH": ("HCL Technologies", "IT"),
    "TECHM": ("Tech Mahindra", "IT"),
    "RELIANCE": ("Reliance Industries", "ENERGY & UTILITIES"), "ONGC": ("Oil & Natural Gas Corporation", "ENERGY & UTILITIES"),
    "NTPC": ("NTPC", "ENERGY & UTILITIES"), "POWERGRID": ("Power Grid Corporation of India", "ENERGY & UTILITIES"),
    "COALINDIA": ("Coal India", "ENERGY & UTILITIES"),
    "HINDUNILVR": ("Hindustan Unilever", "CONSUMER"), "ITC": ("ITC", "CONSUMER"), "NESTLEIND": ("Nestle India", "CONSUMER"),
    "TATACONSUM": ("Tata Consumer Products", "CONSUMER"), "ASIANPAINT": ("Asian Paints", "CONSUMER"),
    "TITAN": ("Titan Company", "CONSUMER"), "TRENT": ("Trent", "CONSUMER"), "ETERNAL": ("Eternal", "CONSUMER"),
    "MARUTI": ("Maruti Suzuki India", "AUTOS"), "M&M": ("Mahindra & Mahindra", "AUTOS"),
    "TMPV": ("Tata Motors Passenger Vehicles", "AUTOS"), "BAJAJ-AUTO": ("Bajaj Auto", "AUTOS"), "EICHERMOT": ("Eicher Motors", "AUTOS"),
    "SUNPHARMA": ("Sun Pharmaceutical Industries", "HEALTHCARE"), "CIPLA": ("Cipla", "HEALTHCARE"),
    "DRREDDY": ("Dr. Reddy's Laboratories", "HEALTHCARE"), "APOLLOHOSP": ("Apollo Hospitals Enterprise", "HEALTHCARE"),
    "MAXHEALTH": ("Max Healthcare Institute", "HEALTHCARE"),
    "TATASTEEL": ("Tata Steel", "MATERIALS"), "JSWSTEEL": ("JSW Steel", "MATERIALS"), "HINDALCO": ("Hindalco Industries", "MATERIALS"),
    "ULTRACEMCO": ("UltraTech Cement", "MATERIALS"), "GRASIM": ("Grasim Industries", "MATERIALS"),
    "LT": ("Larsen & Toubro", "INDUSTRIALS & SERVICES"), "ADANIENT": ("Adani Enterprises", "INDUSTRIALS & SERVICES"),
    "ADANIPORTS": ("Adani Ports and SEZ", "INDUSTRIALS & SERVICES"), "BEL": ("Bharat Electronics", "INDUSTRIALS & SERVICES"),
    "INDIGO": ("InterGlobe Aviation (IndiGo)", "INDUSTRIALS & SERVICES"), "BHARTIARTL": ("Bharti Airtel", "INDUSTRIALS & SERVICES"),
}
INDUSTRY_GROUP = [  # official 'Industry' column -> display group (for any new constituent)
    ("Financial", "FINANCIALS"), ("Information Technology", "IT"), ("Oil", "ENERGY & UTILITIES"), ("Power", "ENERGY & UTILITIES"),
    ("Consumable Fuels", "ENERGY & UTILITIES"), ("Fast Moving", "CONSUMER"), ("Consumer", "CONSUMER"), ("Automobile", "AUTOS"),
    ("Healthcare", "HEALTHCARE"), ("Metals", "MATERIALS"), ("Construction Materials", "MATERIALS"), ("Chemicals", "MATERIALS"),
]
NEWS_ALIASES = {  # how the press names a company (used for the news search and to check each headline is about it)
    "SBIN": ["State Bank of India", "SBI"], "LT": ["Larsen & Toubro", "L&T"], "M&M": ["Mahindra & Mahindra", "M&M"],
    "TMPV": ["Tata Motors"], "ETERNAL": ["Eternal Ltd", "Eternal shares", "Zomato", "Blinkit"], "INDIGO": ["IndiGo", "InterGlobe"],
    "HINDUNILVR": ["Hindustan Unilever", "HUL"], "BAJAJ-AUTO": ["Bajaj Auto"], "BEL": ["Bharat Electronics", "BEL"],
    "BSE": ["BSE shares", "BSE Ltd"], "JIOFIN": ["Jio Financial"], "MAXHEALTH": ["Max Healthcare"], "RELIANCE": ["Reliance Industries", "RIL"],
    "TCS": ["TCS", "Tata Consultancy"], "INFY": ["Infosys"], "HCLTECH": ["HCLTech", "HCL Tech"], "TECHM": ["Tech Mahindra"],
    "ITC": ["ITC"], "TITAN": ["Titan"], "TRENT": ["Trent", "Zudio"], "ONGC": ["ONGC"], "NTPC": ["NTPC"], "POWERGRID": ["Power Grid"],
    "COALINDIA": ["Coal India"], "ADANIPORTS": ["Adani Ports"], "ADANIENT": ["Adani Enterprises"], "BHARTIARTL": ["Bharti Airtel", "Airtel"],
    "SUNPHARMA": ["Sun Pharma"], "DRREDDY": ["Dr Reddy", "Dr. Reddy"], "APOLLOHOSP": ["Apollo Hospitals"], "NESTLEIND": ["Nestle India"],
    "TATACONSUM": ["Tata Consumer"], "ASIANPAINT": ["Asian Paints"], "SHRIRAMFIN": ["Shriram Finance"], "ULTRACEMCO": ["UltraTech"],
    "HDFCLIFE": ["HDFC Life"], "SBILIFE": ["SBI Life"], "BAJFINANCE": ["Bajaj Finance"], "BAJAJFINSV": ["Bajaj Finserv"],
    "KOTAKBANK": ["Kotak Mahindra Bank", "Kotak Bank"], "MARUTI": ["Maruti Suzuki", "Maruti"], "EICHERMOT": ["Eicher Motors", "Royal Enfield"],
}
NEWS_FILTER = {  # headline must match one of these (a bare 'Eternal' is usually not about the company)
    "ETERNAL": [re.compile(r"\bEternal\b(?=\s+(?:Ltd|Limited|shares?|stock|share price|Q[1-4]|results?|and Swiggy|CEO))", re.I),
                re.compile(r"\b(?:Zomato|Blinkit)\b", re.I)],
}
INDICES = {"NIFTY 50": "NSE_INDEX|Nifty 50", "BANK NIFTY": "NSE_INDEX|Nifty Bank", "INDIA VIX": "NSE_INDEX|India VIX"}
MCX = [("CRUDEOIL", "CRUDE"), ("NATURALGAS", "NAT GAS"), ("GOLD", "GOLD"), ("SILVER", "SILVER"), ("COPPER", "COPPER"),
       ("ALUMINIUM", "ALUMINIUM")]
COMMODITY_LINK = {  # stock -> (commodity, +1 if the company gains when it rises, -1 if it is a cost)
    "RELIANCE": ("CRUDEOIL", 1), "ONGC": ("CRUDEOIL", 1), "COALINDIA": ("NATURALGAS", 1), "NTPC": ("NATURALGAS", -1),
    "ASIANPAINT": ("CRUDEOIL", -1), "INDIGO": ("CRUDEOIL", -1), "TITAN": ("GOLD", -1), "HINDALCO": ("ALUMINIUM", 1),
    "MARUTI": ("CRUDEOIL", -1), "M&M": ("CRUDEOIL", -1), "TMPV": ("CRUDEOIL", -1), "BAJAJ-AUTO": ("CRUDEOIL", -1),
    "EICHERMOT": ("CRUDEOIL", -1), "TATASTEEL": ("COPPER", 1), "JSWSTEEL": ("COPPER", 1), "GRASIM": ("CRUDEOIL", -1),
    "ULTRACEMCO": ("CRUDEOIL", -1), "HINDUNILVR": ("CRUDEOIL", -1), "ADANIENT": ("NATURALGAS", 1),
}
MODEL_GROUPS = [  # the information groups the prediction model uses (shown while the model is in training)
    ("Price & technicals", "~170 factors: momentum, reversal, volatility, volume, trend", "TECHNICAL"),
    ("Options & futures", "implied vol, skew, put-call ratio, max pain, OI build-up, basis", "F&O"),
    ("Delivery & flows", "delivery %, FII/DII positioning, average trade size", "FLOW"),
    ("Block, bulk & short deals", "3,229 block and 2,096 bulk deals, short selling", "DEALS"),
    ("Exchange filings", "~1.1 lakh NSE announcements, results dates, insider trades", "FILINGS"),
    ("Company news (FinBERT)", "6.9 lakh headlines, 7 channels, timed to the session", "NEWS"),
    ("Group & competitor news", "subsidiaries, promoters, rivals of each company", "NEWS"),
    ("Macro & global themes", "44 themes: RBI, Fed, crude, China, Japan, tariffs...", "MACRO"),
    ("Global markets", "US, Europe, Asia indices, ADRs, dollar, yields, commodities", "MARKET"),
    ("Attention", "Wikipedia page views for every company", "ATTN"),
]

STATE = {"universe": None, "instr": None, "instr_date": None, "deliv": {}, "deliv_days": 0, "part": None,
         "errors": {}, "upstox_ok": None, "upstox_latency_ms": None, "token": "not set" if not TOKEN else "unverified",
         "nlp": "loading", "ready": False, "boot_msg": "starting", "feed": None, "feed_src": None, "feed_mtime": None}


def log(msg: str) -> None:
    print(f"{datetime.now(IST):%H:%M:%S} {msg}", flush=True)


def _group_for(industry: str) -> str:
    for k, g in INDUSTRY_GROUP:
        if k.lower() in str(industry).lower():
            return g
    return "INDUSTRIALS & SERVICES"


def load_universe() -> list[dict]:
    """Current Nifty 50 (official list; embedded list as fallback). key = Upstox instrument key."""
    rows = []
    try:
        r = httpx.get(NIFTY_CSV, headers=BROWSER, timeout=20, follow_redirects=True)
        r.raise_for_status()
        lines = [x for x in r.text.splitlines() if x.strip()]
        head = [h.strip() for h in lines[0].split(",")]
        for ln in lines[1:]:
            v = dict(zip(head, [x.strip() for x in ln.split(",")]))
            sym = v.get("Symbol")
            if not sym:
                continue
            name, grp = FALLBACK.get(sym, (re.sub(r"\s+(Ltd\.?|Limited)$", "", v.get("Company Name", sym)), _group_for(v.get("Industry", ""))))
            rows.append({"sym": sym, "name": name, "group": grp, "isin": v.get("ISIN Code", "")})
    except Exception as e:  # noqa: BLE001
        STATE["errors"]["universe"] = f"official list unavailable ({type(e).__name__}); embedded list used"
    if len(rows) < 45:
        rows = [{"sym": s, "name": n, "group": g, "isin": ""} for s, (n, g) in FALLBACK.items()]
    return rows


def _expiry(ms) -> date | None:
    try:
        return datetime.fromtimestamp(int(ms) / 1000, IST).date()
    except Exception:  # noqa: BLE001
        return None


def load_instruments() -> dict:
    """Upstox instrument master (public file, refreshed daily): equity keys, F&O expiries and futures, MCX futures."""
    today = datetime.now(IST).date()
    out = {"eq": {}, "name": {}, "opt_exp": defaultdict(set), "fut": defaultdict(list), "mcx": defaultdict(list),
           "idx": {}, "idx_opt": defaultdict(set), "idx_lot": {}}
    import ijson
    for exch in ("NSE", "MCX"):
        f = CACHE_DIR / f"instr_{exch}_{today:%Y%m%d}.json.gz"
        if not f.exists():
            tmp = f.with_suffix(".part")
            with httpx.stream("GET", INSTR_URL.format(exch), timeout=180, follow_redirects=True) as r:
                r.raise_for_status()
                with open(tmp, "wb") as fh:
                    for chunk in r.iter_bytes(1 << 20):
                        fh.write(chunk)
            tmp.replace(f)
        with gzip.open(f, "rb") as fh:
            data = ijson.items(fh, "item", use_float=True)          # one record at a time: small memory
            _ingest(data, out, today)
    for d in (out["fut"], out["mcx"]):
        for k in d:
            d[k].sort()
    return out


def _ingest(data, out: dict, today: date) -> None:
    if True:
        for x in data:
            seg, typ = x.get("segment"), x.get("instrument_type")
            if seg == "NSE_EQ" and typ == "EQ":
                out["eq"][x.get("trading_symbol")] = x.get("instrument_key")
                out["name"][x.get("trading_symbol")] = x.get("short_name") or x.get("name")
            elif seg == "NSE_FO" and x.get("underlying_type") == "EQUITY":
                u, exp = x.get("underlying_symbol"), _expiry(x.get("expiry"))
                if exp is None or exp < today:
                    continue
                if typ in ("CE", "PE"):
                    out["opt_exp"][u].add(exp)
                elif typ == "FUT":
                    out["fut"][u].append((exp, x.get("instrument_key")))
            elif seg == "NSE_INDEX":
                nm, k = x.get("name") or x.get("trading_symbol"), x.get("instrument_key")
                if nm and k:
                    out["idx"][nm] = k
            elif seg == "NSE_FO" and x.get("underlying_type") == "INDEX" and typ in ("CE", "PE") and (
                    x.get("underlying_symbol") == "NIFTY" or x.get("underlying_key") == INDICES["NIFTY 50"]):
                exp = _expiry(x.get("expiry"))
                if exp and exp >= today:
                    out["idx_opt"]["NIFTY"].add(exp)
                    if x.get("lot_size"):
                        out["idx_lot"].setdefault(exp, int(x["lot_size"]))
            elif seg == "MCX_FO" and typ == "FUT":
                u, exp = (x.get("underlying_symbol") or x.get("name") or "").upper(), _expiry(x.get("expiry"))
                if exp and exp >= today and u in dict(MCX):
                    out["mcx"][u].append((exp, x.get("instrument_key"), x.get("trading_symbol")))


def instrument_key(row: dict) -> str | None:
    ins = STATE["instr"] or {}
    return (ins.get("eq") or {}).get(row["sym"]) or (f"NSE_EQ|{row['isin']}" if row.get("isin") else None)


# ----------------------------------------------------------------------------------------------------- NSE archives
def _nse_get(path: str) -> str | None:
    for base in NSE_ARCH:
        try:
            r = httpx.get(f"{base}/{path}", headers={**BROWSER, "Referer": "https://www.nseindia.com/"}, timeout=20)
            if r.status_code == 200 and len(r.content) > 200:
                return r.content.decode("utf-8", "ignore")
        except Exception:  # noqa: BLE001
            continue
    return None


def load_delivery(n_sessions: int = 20) -> tuple[dict, int]:
    """Delivery % for the last n sessions from NSE's full bhavcopy (end-of-day, public)."""
    out, got, d, seen = defaultdict(list), 0, datetime.now(IST).date(), set()
    for _ in range(40):
        if got >= n_sessions:
            break
        if d.weekday() < 5:
            f = CACHE_DIR / f"deliv_{d:%Y%m%d}.csv"
            txt = f.read_text() if f.exists() else _nse_get(f"products/content/sec_bhavdata_full_{d:%d%m%Y}.csv")
            if txt:
                f.write_text(txt)
                lines = txt.splitlines()
                head = [h.strip().upper() for h in lines[0].split(",")]
                rows, file_day = [], None
                for ln in lines[1:]:
                    v = dict(zip(head, [x.strip() for x in ln.split(",")]))
                    if v.get("SERIES") == "EQ":
                        try:
                            file_day = file_day or datetime.strptime(v.get("DATE1", ""), "%d-%b-%Y").date()
                        except ValueError:
                            pass
                        try:
                            pct = float(v.get("DELIV_PER", "nan"))
                        except ValueError:
                            pct = float("nan")
                        rows.append((v.get("SYMBOL"), pct))
                day = file_day or d
                if day.isoformat() not in seen:              # same session served under a holiday's name: skip
                    seen.add(day.isoformat())
                    for sym, pct in rows:
                        out[sym].append((day.isoformat(), pct))
                    got += 1
        d -= timedelta(days=1)
    for k in out:
        out[k].sort()
    return dict(out), got


def load_participant_oi() -> dict | None:
    """FII index-futures long/short (NSE participant-wise open interest, end-of-day)."""
    d = datetime.now(IST).date()
    for _ in range(10):
        if d.weekday() < 5:
            txt = _nse_get(f"content/nsccl/fao_participant_oi_{d:%d%m%Y}.csv")
            if txt:
                lines = [x for x in txt.splitlines() if x.strip()]
                hi = next(i for i, x in enumerate(lines) if "Client Type" in x)
                head = [h.strip().strip('"') for h in lines[hi].split(",")]
                for ln in lines[hi + 1:]:
                    v = dict(zip(head, [x.strip().strip('"') for x in ln.split(",")]))
                    if v.get("Client Type", "").upper() == "FII":
                        lo, sh = float(v["Future Index Long"]), float(v["Future Index Short"])
                        return {"date": d.isoformat(), "long": lo, "short": sh, "long_share": lo / (lo + sh) if lo + sh else None}
        d -= timedelta(days=1)
    return None


# ----------------------------------------------------------------------------------------------------- NLP
_finbert = None
LEX_POS = set("""beat beats surge surges soar soars jump jumps rally rallies gain gains rise rises record upgrade upgrades upgraded
outperform outperforms strong growth profit profits boost boosts wins win bags order orders expansion approval approves approved
dividend buyback positive bullish higher robust recovery rebound rebounds""".split())
LEX_NEG = set("""fall falls fell drop drops plunge plunges slump slumps crash crashes loss losses downgrade downgrades downgraded miss misses
weak weaker decline declines probe penalty fine fined fraud raid raids lawsuit ban banned default cut cuts negative bearish lower
slowdown warning resigns resignation exit exits concern concerns pressure""".split())


def _load_finbert() -> None:
    global _finbert
    try:
        from transformers import pipeline
        _finbert = pipeline("text-classification", model="ProsusAI/finbert", top_k=None, truncation=True)
        STATE["nlp"] = "FinBERT"
    except ModuleNotFoundError:
        STATE["nlp"] = "finance word list (FinBERT in research pipeline)"
    except Exception as e:  # noqa: BLE001
        STATE["nlp"] = "finance word list"
        STATE["errors"]["nlp"] = f"FinBERT could not load ({type(e).__name__}); headlines scored with a finance word list"


def sentiment(texts: list[str]) -> list[float]:
    """p(positive) - p(negative) per headline, in [-1, 1]."""
    if not texts:
        return []
    if _finbert is not None:
        res = _finbert(texts)
        out = []
        for r in res:
            p = {d["label"].lower(): d["score"] for d in r}
            out.append(round(p.get("positive", 0) - p.get("negative", 0), 3))
        return out
    out = []
    for t in texts:
        w = re.findall(r"[a-z]+", t.lower())
        pos, neg = sum(x in LEX_POS for x in w), sum(x in LEX_NEG for x in w)
        out.append(round((pos - neg) / max(pos + neg, 1) * 0.6, 3) if pos + neg else 0.0)
    return out


# ----------------------------------------------------------------------------------------------------- caching
_cache: dict = {}
_locks: dict = defaultdict(asyncio.Lock)


async def cached(key: str, ttl: float, fn):
    hit = _cache.get(key)
    if hit and time.time() - hit[0] < ttl:
        return hit[1]
    async with _locks[key]:
        hit = _cache.get(key)
        if hit and time.time() - hit[0] < ttl:
            return hit[1]
        val = await fn()
        _cache[key] = (time.time(), val)
        return val


# ----------------------------------------------------------------------------------------------------- Upstox
class UpstoxError(Exception):
    pass


_client: httpx.AsyncClient | None = None


async def upx(path: str, params: dict | None = None, quiet: bool = False):
    t0 = time.time()
    r = await _client.get(UPSTOX + path, params=params,
                          headers={"Authorization": f"Bearer {TOKEN}", "Accept": "application/json"}, timeout=15)
    STATE["upstox_latency_ms"] = int((time.time() - t0) * 1000)
    if r.status_code == 401:
        STATE["token"], STATE["upstox_ok"] = "rejected (401) - generate a new Analytics Token", False
        raise UpstoxError("token rejected")
    if r.status_code == 429:
        raise UpstoxError("rate limited")
    if r.status_code >= 400:
        if not quiet:
            STATE["errors"]["upstox"] = f"HTTP {r.status_code}: {r.text[:160]}"
        raise UpstoxError(f"HTTP {r.status_code}")
    j = r.json()
    if j.get("status") != "success":
        raise UpstoxError(str(j.get("errors"))[:160])
    STATE["upstox_ok"], STATE["token"] = True, "valid"
    STATE["errors"].pop("upstox", None)
    return j.get("data")


def _k(key: str) -> str:
    return quote(key, safe="")


def parse_quote(v: dict) -> dict:
    ltp = float(v.get("last_price") or 0)
    chg = float(v.get("net_change") or 0)
    prev = ltp - chg
    o = v.get("ohlc") or {}
    ltt = v.get("last_trade_time")
    try:
        t = datetime.fromtimestamp(int(ltt) / 1000, IST).strftime("%H:%M:%S") if ltt else None
    except Exception:  # noqa: BLE001
        t = None
    try:
        ltt_ms = int(ltt) if ltt else None
    except (TypeError, ValueError):
        ltt_ms = None
    return {"ltp": ltp, "chg": chg, "pct": (chg / prev * 100) if prev else 0.0, "prev": prev, "ltt": ltt_ms,
            "open": o.get("open"), "high": o.get("high"), "low": o.get("low"), "vwap": v.get("average_price") or None,
            "volume": v.get("volume") or 0, "oi": v.get("oi") or 0, "time": t,
            "upper": v.get("upper_circuit_limit"), "lower": v.get("lower_circuit_limit")}


async def quotes(keys: list[str]) -> dict:
    """{instrument_key: parsed quote}; up to 500 keys per call."""
    out = {}
    for i in range(0, len(keys), 450):
        data = await upx("/v2/market-quote/quotes", {"instrument_key": ",".join(keys[i:i + 450])})
        for v in (data or {}).values():
            out[v.get("instrument_token")] = parse_quote(v)
    return out


def _candles(data) -> list[list]:
    """Upstox candles [ts, o, h, l, c, vol, oi] newest-first -> ascending, with ts as IST epoch ms."""
    rows = []
    for c in (data or {}).get("candles", []):
        try:
            ts = datetime.fromisoformat(c[0]).timestamp() * 1000
            rows.append([ts, float(c[1]), float(c[2]), float(c[3]), float(c[4]), float(c[5] or 0), float(c[6] or 0)])
        except Exception:  # noqa: BLE001
            continue
    rows.sort(key=lambda r: r[0])
    return rows


async def intraday(key: str, unit: str, n: int) -> list:
    return _candles(await upx(f"/v3/historical-candle/intraday/{_k(key)}/{unit}/{n}"))


async def history(key: str, unit: str, n: int, start: date, end: date) -> list:
    return _candles(await upx(f"/v3/historical-candle/{_k(key)}/{unit}/{n}/{end:%Y-%m-%d}/{start:%Y-%m-%d}"))


async def session_candles(key: str, n: int) -> list:
    """Today's intraday candles; on a holiday/weekend or before the open, the last session's candles."""
    rows = await intraday(key, "minutes", n)
    if rows:
        return rows
    today = datetime.now(IST).date()
    hist = await history(key, "minutes", n, today - timedelta(days=10), today)
    if not hist:
        return []
    last_day = datetime.fromtimestamp(hist[-1][0] / 1000, IST).date()
    return [r for r in hist if datetime.fromtimestamp(r[0] / 1000, IST).date() == last_day]


async def daily_1y(key: str) -> list:
    today = datetime.now(IST).date()
    return await cached(f"d1y:{key}:{today}", 3600, lambda: history(key, "days", 1, today - timedelta(days=400), today))


# ----------------------------------------------------------------------------------------------------- analytics
def chain_metrics(rows: list[dict], spot: float | None) -> dict:
    """Option-chain summary: OI profile around the money, PCR, max pain, ATM IV, skew."""
    strikes = []
    for r in rows:
        c, p = r.get("call_options") or {}, r.get("put_options") or {}
        cm, pm = c.get("market_data") or {}, p.get("market_data") or {}
        cg, pg = c.get("option_greeks") or {}, p.get("option_greeks") or {}
        strikes.append({"k": float(r["strike_price"]), "c_oi": float(cm.get("oi") or 0), "p_oi": float(pm.get("oi") or 0),
                        "c_prev": float(cm.get("prev_oi") or 0), "p_prev": float(pm.get("prev_oi") or 0),
                        "c_vol": float(cm.get("volume") or 0), "p_vol": float(pm.get("volume") or 0),
                        "c_iv": float(cg.get("iv") or 0), "p_iv": float(pg.get("iv") or 0)})
        spot = spot or r.get("underlying_spot_price")
    strikes.sort(key=lambda s: s["k"])
    if not strikes or not spot:
        return {}
    c_oi, p_oi = sum(s["c_oi"] for s in strikes), sum(s["p_oi"] for s in strikes)
    c_vol, p_vol = sum(s["c_vol"] for s in strikes), sum(s["p_vol"] for s in strikes)
    pain = min(strikes, key=lambda K: sum(s["c_oi"] * max(0.0, K["k"] - s["k"]) + s["p_oi"] * max(0.0, s["k"] - K["k"]) for s in strikes))["k"]
    i_atm = min(range(len(strikes)), key=lambda i: abs(strikes[i]["k"] - spot))
    atm = strikes[i_atm]
    ivs = [x for x in (atm["c_iv"], atm["p_iv"]) if x > 0]
    near = lambda target, field: next((s[field] for s in sorted(strikes, key=lambda s: abs(s["k"] - target)) if s[field] > 0), None)
    put_otm, call_otm = near(spot * 0.95, "p_iv"), near(spot * 1.05, "c_iv")
    lo, hi = max(0, i_atm - 4), min(len(strikes), i_atm + 5)
    return {"spot": spot, "pcr_oi": p_oi / c_oi if c_oi else None, "pcr_vol": p_vol / c_vol if c_vol else None,
            "max_pain": pain, "atm": atm["k"], "atm_iv": sum(ivs) / len(ivs) if ivs else None,
            "skew": (put_otm - call_otm) if put_otm and call_otm else None,
            "call_oi_chg": sum(s["c_oi"] - s["c_prev"] for s in strikes), "put_oi_chg": sum(s["p_oi"] - s["p_prev"] for s in strikes),
            "profile": [{"k": s["k"], "c_oi": s["c_oi"], "p_oi": s["p_oi"], "c_chg": s["c_oi"] - s["c_prev"],
                         "p_chg": s["p_oi"] - s["p_prev"], "atm": s is atm} for s in strikes[lo:hi]]}


async def _prev_oi(key: str, today: date) -> float | None:
    """Open interest at the previous close (daily futures candles carry OI)."""
    fd = await cached(f"futd:{key}:{today}", 3600, lambda: history(key, "days", 1, today - timedelta(days=10), today))
    return next((c[6] for c in reversed(fd) if c[0] < _today_start_ms() and c[6]), None)


def futures_metrics(q: dict, futs: list, fq: dict, prev_oi: dict, today: date, roll_days: int = 2) -> dict:
    """Open-interest change, price/OI build-up, annualised basis and rollover. In the last `roll_days` days before
    the near-month expiry, positions move to the next month, so the near month alone shows a false 'long unwinding':
    OI change then uses near + next month combined, and the basis uses the next month."""
    near_exp, near = futs[0][0], futs[0][1]
    nxt_exp, nxt = (futs[1][0], futs[1][1]) if len(futs) > 1 else (None, None)
    nq, xq = fq.get(near) or {}, (fq.get(nxt) or {}) if nxt else {}
    near_dte = (near_exp - today).days
    rolling = bool(nxt) and near_dte <= roll_days
    out = {"near_dte": near_dte, "oi_combined": rolling, "basis_month": "next" if rolling and xq.get("ltp") else "near"}
    if rolling:
        now_oi = (nq.get("oi") or 0) + (xq.get("oi") or 0)
        before = (prev_oi.get(near) or 0) + (prev_oi.get(nxt) or 0)
    else:
        now_oi, before = nq.get("oi") or 0, prev_oi.get(near) or 0
    if now_oi and before:
        out["oi_chg_pct"] = (now_oi / before - 1) * 100
        out["buildup"] = buildup(q["pct"], out["oi_chg_pct"])
    bq, bexp = (xq, nxt_exp) if out["basis_month"] == "next" else (nq, near_exp)
    if bq.get("ltp") and q.get("ltp"):
        out["basis_ann"] = (bq["ltp"] / q["ltp"] - 1) * 365 / max((bexp - today).days, 1) * 100
    if nxt and xq.get("oi") and nq.get("oi"):
        out["rollover"] = xq["oi"] / (xq["oi"] + nq["oi"]) * 100
    return out


def rsi(closes: list[float], n: int = 14) -> float | None:
    if len(closes) <= n:
        return None
    gains = [max(0.0, closes[i] - closes[i - 1]) for i in range(1, len(closes))]
    losses = [max(0.0, closes[i - 1] - closes[i]) for i in range(1, len(closes))]
    ag, al = sum(gains[:n]) / n, sum(losses[:n]) / n
    for g, l in zip(gains[n:], losses[n:]):
        ag, al = (ag * (n - 1) + g) / n, (al * (n - 1) + l) / n
    return 100.0 if al == 0 else 100 - 100 / (1 + ag / al)


def buildup(price_chg: float, oi_chg: float) -> str:
    if price_chg >= 0 and oi_chg >= 0:
        return "LONG BUILD-UP"
    if price_chg < 0 and oi_chg >= 0:
        return "SHORT BUILD-UP"
    if price_chg >= 0:
        return "SHORT COVERING"
    return "LONG UNWINDING"


def sma(xs: list[float], n: int) -> float | None:
    return sum(xs[-n:]) / n if len(xs) >= n else None


def signals(sym: str, q: dict, daily: list, chain: dict, deliv: list, fut: dict, news_avg, sector_rel, mcx: dict, part) -> list:
    """Twelve rule-based live indicators (descriptive facts, not the model's prediction)."""
    out = []
    closes = [r[4] for r in daily if r[0] < _today_start_ms()] + [q["ltp"]]
    s50, s200 = sma(closes, 50), sma(closes, 200)
    if s50 and s200:
        if q["ltp"] > s50 > s200:
            out.append(("TREND", "▲ above 50/200 DMA", "bull"))
        elif q["ltp"] < s50 < s200:
            out.append(("TREND", "▼ below 50/200 DMA", "bear"))
        else:
            out.append(("TREND", f"mixed · {'>' if q['ltp'] > s50 else '<'} 50 DMA", "neutral"))
    else:
        out.append(("TREND", "—", "neutral"))
    r = rsi(closes[-120:])
    out.append(("RSI 14", f"{r:.0f} · {'overbought' if r > 70 else 'oversold' if r < 30 else 'neutral'}" if r else "—",
                "bear" if r and r > 70 else "bull" if r and r < 30 else "neutral"))
    link = COMMODITY_LINK.get(sym)
    if link and mcx.get(link[0]):
        c = mcx[link[0]]
        tone = "bull" if c["pct"] * link[1] > 0.3 else "bear" if c["pct"] * link[1] < -0.3 else "neutral"
        out.append(("COMMODITY LINK", f"{dict(MCX)[link[0]].title()} {c['pct']:+.1f}% · {'tailwind' if link[1] > 0 else 'cost'}", tone))
    else:
        out.append(("COMMODITY LINK", "no direct link", "neutral"))
    prev = [r_ for r_ in daily if r_[0] < _today_start_ms()]
    if len(prev) >= 20:
        hi60, lo60 = max(r_[2] for r_ in prev[-60:]), min(r_[3] for r_ in prev[-60:])
        if q["ltp"] >= hi60 * 0.985:
            out.append(("SUPPORT/RES", "near 60D high", "bear" if q["ltp"] < hi60 else "bull"))
        elif q["ltp"] <= lo60 * 1.015:
            out.append(("SUPPORT/RES", "near 60D low", "bull" if q["ltp"] > lo60 else "bear"))
        else:
            out.append(("SUPPORT/RES", f"mid-range ({(q['ltp'] - lo60) / (hi60 - lo60) * 100:.0f}%)", "neutral"))
    else:
        out.append(("SUPPORT/RES", "—", "neutral"))
    dv = [p for _, p in deliv if p == p]
    if len(dv) >= 10:
        mu = sum(dv[:-1]) / len(dv[:-1])
        sd = (sum((x - mu) ** 2 for x in dv[:-1]) / len(dv[:-1])) ** 0.5 or 1
        z = (dv[-1] - mu) / sd
        lab = "accumulation" if z > 1 and q["pct"] >= 0 else "distribution" if z > 1 else "normal"
        out.append(("DELIVERY", f"{lab} · z {z:+.1f}", "bull" if lab == "accumulation" else "bear" if lab == "distribution" else "neutral"))
    else:
        out.append(("DELIVERY", "—", "neutral"))
    vols = [r_[5] for r_ in prev[-20:]]
    if vols and sum(vols):
        ratio = q["volume"] / (sum(vols) / len(vols))
        out.append(("VOLUME", f"{ratio:.1f}× 20D avg", "bull" if ratio > 1.5 and q["pct"] > 0 else "bear" if ratio > 1.5 else "neutral"))
    else:
        out.append(("VOLUME", "—", "neutral"))
    pcr = chain.get("pcr_oi") if chain else None
    out.append(("PCR (OI)", f"{pcr:.2f} · {'put support' if pcr > 1.2 else 'call heavy' if pcr < 0.7 else 'balanced'}" if pcr else "—",
                "bull" if pcr and pcr > 1.2 else "bear" if pcr and pcr < 0.7 else "neutral"))
    if fut.get("buildup"):
        out.append(("OI BUILD-UP", fut["buildup"].lower(), "bull" if fut["buildup"] in ("LONG BUILD-UP", "SHORT COVERING") else "bear"))
    else:
        out.append(("OI BUILD-UP", "—", "neutral"))
    iv = chain.get("atm_iv") if chain else None
    rets = [math.log(closes[i] / closes[i - 1]) for i in range(max(1, len(closes) - 250), len(closes)) if closes[i - 1] > 0]
    rv = (sum(x * x for x in rets) / len(rets)) ** 0.5 * math.sqrt(252) * 100 if len(rets) > 20 else None
    if iv and rv:
        out.append(("ATM IV", f"{iv:.1f}% · {'rich' if iv > 1.3 * rv else 'cheap' if iv < 0.8 * rv else 'fair'} (RV {rv:.0f})", "neutral"))
    else:
        out.append(("ATM IV", f"{iv:.1f}%" if iv else "—", "neutral"))
    if news_avg is not None:
        out.append(("NEWS", f"{news_avg:+.2f} {'FinBERT' if _finbert is not None else 'word list'} · {'positive' if news_avg > 0.15 else 'negative' if news_avg < -0.15 else 'neutral'}",
                    "bull" if news_avg > 0.15 else "bear" if news_avg < -0.15 else "neutral"))
    else:
        out.append(("NEWS", "no recent headlines", "neutral"))
    if sector_rel is not None:
        out.append(("SECTOR", f"{'leading' if sector_rel > 0.25 else 'lagging' if sector_rel < -0.25 else 'in line'} {sector_rel:+.2f} pp",
                    "bull" if sector_rel > 0.25 else "bear" if sector_rel < -0.25 else "neutral"))
    else:
        out.append(("SECTOR", "—", "neutral"))
    if part and part.get("long_share") is not None:
        ls = part["long_share"]
        out.append(("FII INDEX FUT", f"{'net long' if ls > 0.55 else 'net short' if ls < 0.45 else 'balanced'} ({ls * 100:.0f}% L)",
                    "bull" if ls > 0.55 else "bear" if ls < 0.45 else "neutral"))
    else:
        out.append(("FII INDEX FUT", "—", "neutral"))
    return [{"name": a, "value": b, "tone": c} for a, b, c in out]


def quote_is_today(q: dict | None) -> bool:
    """True when the quote's last trade happened today (IST). On weekends, holidays and before the open Upstox keeps
    the last session's prices, which must not be drawn as a candle for today."""
    t = (q or {}).get("ltt")
    if not t:
        return False
    return datetime.fromtimestamp(t / 1000, IST).date() == datetime.now(IST).date()


def _today_start_ms() -> float:
    d = datetime.now(IST).replace(hour=0, minute=0, second=0, microsecond=0)
    return d.timestamp() * 1000


def market_phase_by_clock() -> str:
    n = datetime.now(IST)
    if n.weekday() >= 5:
        return "CLOSED"
    m = n.hour * 60 + n.minute
    return "PRE-OPEN" if 540 <= m < 555 else "OPEN" if 555 <= m < 930 else "CLOSED"


# ----------------------------------------------------------------------------------------------------- demo data
def _rng(*parts) -> random.Random:
    return random.Random(hashlib.sha1("|".join(map(str, parts)).encode()).hexdigest())


def demo_quote(key: str, base: float | None = None) -> dict:
    r = _rng(key, datetime.now(IST).date())
    base = base or r.uniform(150, 9000)
    drift = math.sin(time.time() / 600 + r.random() * 6) * 0.02 + r.uniform(-0.01, 0.01)
    ltp = round(base * (1 + drift), 2)
    prev = base
    return {"ltp": ltp, "chg": ltp - prev, "pct": (ltp - prev) / prev * 100, "prev": prev, "open": round(base * 1.002, 2),
            "high": round(max(ltp, base) * 1.008, 2), "low": round(min(ltp, base) * 0.992, 2), "vwap": round((ltp + base) / 2, 2),
            "volume": int(r.uniform(1e5, 2e7)), "oi": 0, "time": datetime.now(IST).strftime("%H:%M:%S"), "upper": None, "lower": None}


def demo_candles(seed, n: int, start_ms: float, step_ms: float, base: float) -> list:
    r = _rng(seed)
    out, p = [], base
    for i in range(n):
        o = p
        c = o * (1 + r.gauss(0, 0.0025))
        out.append([start_ms + i * step_ms, o, max(o, c) * (1 + abs(r.gauss(0, 0.001))), min(o, c) * (1 - abs(r.gauss(0, 0.001))), c,
                    r.uniform(5e3, 2e5), 0])
        p = c
    return out


# ----------------------------------------------------------------------------------------------------- app
from contextlib import asynccontextmanager


@asynccontextmanager
async def lifespan(_app):
    global _client
    _client = httpx.AsyncClient(follow_redirects=True)
    threading.Thread(target=_boot, daemon=True).start()
    threading.Thread(target=_load_finbert, daemon=True).start()
    threading.Thread(target=_daily_refresh, daemon=True).start()
    threading.Thread(target=_feed_loop, daemon=True).start()
    yield
    await _client.aclose()


app = FastAPI(title="Nifty 50 Terminal", docs_url=None, redoc_url=None, openapi_url=None, lifespan=lifespan)


def _boot() -> None:
    try:
        STATE["boot_msg"] = "loading Nifty 50 constituents"
        STATE["universe"] = load_universe()
        if not DEMO:
            STATE["boot_msg"] = "loading Upstox instrument master"
            try:
                STATE["instr"] = load_instruments()
                STATE["instr_date"] = datetime.now(IST).date().isoformat()
            except Exception as e:  # noqa: BLE001
                STATE["errors"]["instruments"] = f"{type(e).__name__}: {str(e)[:120]}"
        STATE["ready"] = True
        STATE["boot_msg"] = "ready"
        log(f"universe {len(STATE['universe'])} stocks; demo={DEMO}")
        STATE["deliv"], STATE["deliv_days"] = load_delivery()
        STATE["part"] = load_participant_oi()
        if not STATE["deliv_days"]:
            STATE["errors"]["nse_archives"] = "NSE archives not reachable from this server (delivery / FII data shown as —)"
    except Exception as e:  # noqa: BLE001
        STATE["errors"]["boot"] = f"{type(e).__name__}: {str(e)[:160]}"
        STATE["ready"] = bool(STATE["universe"])


def _daily_refresh() -> None:
    while True:
        time.sleep(1800)
        today = datetime.now(IST).date().isoformat()
        if STATE["instr_date"] != today and not DEMO:
            try:
                STATE["instr"], STATE["instr_date"] = load_instruments(), today
            except Exception:  # noqa: BLE001
                pass
        try:
            d, n = load_delivery()
            if n:
                STATE["deliv"], STATE["deliv_days"] = d, n
            STATE["part"] = load_participant_oi() or STATE["part"]
        except Exception:  # noqa: BLE001
            pass


# ---- access gate
def _sig() -> str:
    return hmac.new(b"nifty50-terminal", ACCESS_CODE.encode(), hashlib.sha256).hexdigest()


LOGIN = """<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>Nifty 50 Terminal · Access</title><link href="https://fonts.googleapis.com/css2?family=IBM+Plex+Mono:wght@400;600&display=swap" rel="stylesheet">
<style>body{margin:0;height:100vh;display:grid;place-items:center;background:#070B11;color:#E8EEF4;font-family:'IBM Plex Mono',monospace}
form{background:#0B1219;border:1px solid #1C2733;padding:32px 36px;width:340px}h1{font-size:14px;letter-spacing:.12em;margin:0 0 4px}
p{color:#7F8D9C;font-size:11px;margin:0 0 20px;line-height:1.6}input{width:100%;box-sizing:border-box;background:#070B11;border:1px solid #22303F;
color:#E8EEF4;padding:10px;font:inherit;margin-bottom:12px}button{width:100%;background:#22C38E;color:#04110C;border:0;padding:10px;font:600 12px 'IBM Plex Mono';
letter-spacing:.1em;cursor:pointer}.e{color:#F0525E;font-size:11px;margin-bottom:10px}</style></head><body>
<form method="post" action="login"><h1>NIFTY 50 · PREDICTIVE ANALYTICS TERMINAL</h1><p>Academic demonstration (GIM, PGDM-BIFS).<br>Enter the access code you were given.</p>
__ERR__<input name="code" type="password" placeholder="access code" autofocus><button>OPEN TERMINAL</button></form></body></html>"""


@app.middleware("http")
async def gate(request: Request, call_next):
    open_paths = ("/login", "/healthz")
    if ACCESS_CODE and request.url.path not in open_paths and not hmac.compare_digest(request.cookies.get("tv_access", ""), _sig()):
        if request.url.path.startswith("/api/"):
            return JSONResponse({"error": "access code required"}, status_code=401)
        return HTMLResponse(LOGIN.replace("__ERR__", ""))
    return await call_next(request)


@app.post("/login")
async def login(request: Request):
    code = parse_qs((await request.body()).decode()).get("code", [""])[0].strip()
    if ACCESS_CODE and hmac.compare_digest(code, ACCESS_CODE):
        resp = RedirectResponse("./", status_code=303)
        resp.set_cookie("tv_access", _sig(), max_age=30 * 86400, httponly=True, secure=True, samesite="none")
        return resp
    return HTMLResponse(LOGIN.replace("__ERR__", '<div class="e">Incorrect access code.</div>'), status_code=401)


@app.get("/healthz")
async def healthz():
    return {"ok": True}


@app.get("/")
async def index():
    return FileResponse(HERE / "index.html", headers={"Cache-Control": "no-cache"})


# ---- data endpoints
def _universe() -> list[dict]:
    return STATE["universe"] or []


async def _snapshot() -> dict:
    uni = _universe()
    phase = market_phase_by_clock()
    if DEMO:
        q = {r["sym"]: demo_quote(r["sym"]) for r in uni}
        idx = {n: demo_quote(n, b) for n, b in (("NIFTY 50", 25000), ("BANK NIFTY", 56000), ("INDIA VIX", 12.5))}
        com = {u: demo_quote(u, b) for u, b in (("CRUDEOIL", 5600), ("NATURALGAS", 290), ("GOLD", 118000), ("SILVER", 142000),
                                                 ("COPPER", 905), ("ALUMINIUM", 252))}
        spark = {n: [p[4] for p in demo_candles(n + str(datetime.now(IST).date()), 75, 0, 1, idx[n]["prev"])] for n in idx}
        source = "DEMO"
    else:
        keys = {r["sym"]: instrument_key(r) for r in uni}
        mcx_keys = {u: v[0][1] for u, v in ((STATE["instr"] or {}).get("mcx") or {}).items() if v}
        allk = [k for k in keys.values() if k] + list(INDICES.values()) + list(mcx_keys.values())
        qq = await quotes(allk)
        q = {s: qq[k] for s, k in keys.items() if k in qq}
        idx = {n: qq[k] for n, k in INDICES.items() if k in qq}
        com = {u: qq[k] for u, k in mcx_keys.items() if k in qq}

        async def sparks():
            res = {}
            for n, k in INDICES.items():
                try:
                    res[n] = [c[4] for c in await session_candles(k, 5)]
                except Exception:  # noqa: BLE001
                    res[n] = []
            return res
        spark = await cached("sparks", 60, sparks)
        try:
            st = await cached("mstatus", 30, lambda: upx("/v2/market/status/NSE", quiet=True))
            s = str((st or {}).get("status", "")).upper()
            phase = "OPEN" if "NORMAL_OPEN" in s else "PRE-OPEN" if "PRE_OPEN" in s else "CLOSED" if s else phase
        except Exception:  # noqa: BLE001
            pass
        source = "UPSTOX"
    stocks = []
    for r in uni:
        if r["sym"] in q:
            x = q[r["sym"]]
            day = datetime.fromtimestamp(x["ltt"] / 1000, IST).date().isoformat() if x.get("ltt") else None
            stocks.append({"sym": r["sym"], "name": r["name"], "group": r["group"], "ltp": x["ltp"], "pct": x["pct"], "chg": x["chg"],
                           "high": x.get("high"), "low": x.get("low"), "day": day})
    nifty_pct = idx.get("NIFTY 50", {}).get("pct", 0.0)
    groups = []
    for g in GROUPS:
        mem = [s for s in stocks if s["group"] == g]
        if mem:
            groups.append({"name": g, "avg": sum(s["pct"] for s in mem) / len(mem), "stocks": [s["sym"] for s in mem]})
    rel = sorted(stocks, key=lambda s: s["pct"] - nifty_pct)
    up, dn = sum(s["pct"] > 0 for s in stocks), sum(s["pct"] < 0 for s in stocks)
    return {"phase": phase, "source": source, "time": datetime.now(IST).strftime("%H:%M:%S"),
            "indices": [{"name": n, **{k: idx[n][k] for k in ("ltp", "pct", "chg")}, "spark": spark.get(n, [])} for n in INDICES if n in idx],
            "commodities": [{"name": lab, "ltp": com[u]["ltp"], "pct": com[u]["pct"]} for u, lab in MCX if u in com],
            "stocks": stocks, "groups": groups, "breadth": {"up": up, "down": dn},
            "leaders": [{"sym": s["sym"], "pct": s["pct"], "rel": s["pct"] - nifty_pct} for s in rel[::-1][:5]],
            "laggards": [{"sym": s["sym"], "pct": s["pct"], "rel": s["pct"] - nifty_pct} for s in rel[:5]],
            "_quotes": q, "_mcx": com}


def _health() -> dict:
    return {"version": VERSION, "mode": "DEMO" if DEMO else "LIVE", "token": STATE["token"], "upstox_ok": STATE["upstox_ok"],
            "latency_ms": STATE["upstox_latency_ms"], "instruments": STATE["instr_date"], "stocks": len(_universe()),
            "delivery_sessions": STATE["deliv_days"], "fii": (STATE["part"] or {}).get("date"), "nlp": STATE["nlp"],
            "errors": dict(STATE["errors"]), "boot": STATE["boot_msg"], "feed": _feed_meta()}


@app.get("/api/snapshot")
async def api_snapshot():
    if not STATE["ready"]:
        return {"loading": True, "boot": STATE["boot_msg"], "health": _health()}
    ttl = 2.5 if market_phase_by_clock() != "CLOSED" else 20
    try:
        snap = await cached("snapshot", ttl, _snapshot)
    except Exception as e:  # noqa: BLE001
        STATE["upstox_ok"] = False
        STATE["errors"]["snapshot"] = f"{type(e).__name__}: {str(e)[:120]}"
        hit = _cache.get("snapshot")
        if not hit:
            return JSONResponse({"error": STATE["errors"]["snapshot"], "health": _health()}, status_code=503)
        snap = hit[1]
        snap = {**snap, "stale": True}
    out = {k: v for k, v in snap.items() if not k.startswith("_")}
    out["health"] = _health()
    out["model_groups"] = [{"name": a, "desc": b, "tag": c} for a, b, c in MODEL_GROUPS]
    return out


async def _news(sym: str) -> list:
    row = next((r for r in _universe() if r["sym"] == sym), None)
    if not row:
        return []
    names = NEWS_ALIASES.get(sym, [re.sub(r"\s*\(.*?\)", "", row["name"])])
    q = " OR ".join(f'"{n}"' for n in names) + " when:3d"
    url = f"{NEWS_RSS}?q={quote_plus(q)}&hl=en-IN&gl=IN&ceid=IN:en"
    r = await _client.get(url, headers=BROWSER, timeout=15)
    root = ET.fromstring(r.content)
    items = []
    pats = NEWS_FILTER.get(sym) or [re.compile(r"\b" + re.escape(n) + r"\b", 0 if (n.isupper() and len(n) <= 4) else re.I)
                                    for n in names]
    for it in root.iter("item"):
        title = (it.findtext("title") or "").strip()
        src = (it.findtext("source") or "").strip()
        if src and title.endswith(" - " + src):
            title = title[: -len(src) - 3]
        if not any(p.search(title) for p in pats):
            continue
        try:
            ts = parsedate_to_datetime(it.findtext("pubDate")).astimezone(IST)
        except Exception:  # noqa: BLE001
            continue
        items.append({"title": title, "source": src, "ts": ts})
    items.sort(key=lambda x: x["ts"], reverse=True)
    seen, uniq = set(), []
    for x in items:
        k = re.sub(r"[^a-z0-9]", "", x["title"].lower())[:60]
        if k not in seen:
            seen.add(k)
            uniq.append(x)
    uniq = uniq[:6]
    scores = await asyncio.get_running_loop().run_in_executor(None, sentiment, [x["title"] for x in uniq])
    now = datetime.now(IST)
    return [{"title": x["title"], "source": x["source"], "score": s,
             "when": x["ts"].strftime("%H:%M") if x["ts"].date() == now.date() else x["ts"].strftime("%d %b"),
             "age_h": (now - x["ts"]).total_seconds() / 3600} for x, s in zip(uniq, scores)]


@app.get("/api/news/{sym}")
async def api_news(sym: str):
    if not STATE["ready"]:
        return {"items": []}
    try:
        items = await cached(f"news:{sym}", 600, lambda: _news(sym))
        return {"items": items, "nlp": STATE["nlp"]}
    except Exception as e:  # noqa: BLE001
        return {"items": [], "nlp": STATE["nlp"], "error": f"news feed unavailable ({type(e).__name__})"}


TF = {"1m": ("minutes", 1), "5m": ("minutes", 5), "5d": ("minutes", 30), "3m": ("days", 1), "1y": ("days", 1)}


async def _candles_for(key: str, tf: str, q: dict) -> list:
    unit, n = TF[tf]
    if tf in ("1m", "5m"):
        return await session_candles(key, n)
    today = datetime.now(IST).date()
    if tf == "5d":
        hist = await cached(f"h30:{key}:{today}", 900, lambda: history(key, "minutes", 30, today - timedelta(days=12), today - timedelta(days=1)))
        try:
            todays = await intraday(key, "minutes", 30)
        except Exception:  # noqa: BLE001
            todays = []
        rows = hist + todays
        days_ = sorted({datetime.fromtimestamp(r[0] / 1000, IST).date() for r in rows})[-5:]
        return [r for r in rows if datetime.fromtimestamp(r[0] / 1000, IST).date() in days_]
    d = [r for r in await daily_1y(key) if r[0] < _today_start_ms()]
    if quote_is_today(q) and q.get("open"):
        d = d + [[_today_start_ms(), q["open"], q["high"], q["low"], q["ltp"], q["volume"], 0]]
    return d[-63:] if tf == "3m" else d[-250:]


@app.get("/api/stock/{sym}")
async def api_stock(sym: str, tf: str = "5m"):
    if not STATE["ready"]:
        return JSONResponse({"loading": True}, status_code=503)
    tf = tf if tf in TF else "5m"
    row = next((r for r in _universe() if r["sym"] == sym), None)
    if not row:
        return JSONResponse({"error": "unknown symbol"}, status_code=404)
    snap = await cached("snapshot", 3, _snapshot)
    q = snap["_quotes"].get(sym)
    if q is None:
        return JSONResponse({"error": "no quote"}, status_code=503)
    today = datetime.now(IST).date()
    expiry, dte = None, None
    if DEMO:
        start = datetime.now(IST).replace(hour=9, minute=15, second=0, microsecond=0).timestamp() * 1000
        unit, n = TF[tf]
        step = {"1m": 60e3, "5m": 300e3, "5d": 1800e3}.get(tf, 86400e3)
        count = {"1m": 375, "5m": 75, "5d": 65, "3m": 63, "1y": 250}[tf]
        candles = demo_candles(f"{sym}{tf}{today}", count, start - (0 if tf in ("1m", "5m") else step * count), step, q["prev"])
        daily = demo_candles(f"{sym}d{today}", 260, _today_start_ms() - 260 * 86400e3, 86400e3, q["prev"])
        r = _rng(sym, "chain")
        atm = round(q["ltp"] / 20) * 20
        chain = {"spot": q["ltp"], "pcr_oi": r.uniform(0.6, 1.4), "pcr_vol": r.uniform(0.5, 1.3), "max_pain": atm, "atm": atm,
                 "atm_iv": r.uniform(18, 35), "skew": r.uniform(-1, 4), "call_oi_chg": r.uniform(-5e5, 5e5), "put_oi_chg": r.uniform(-5e5, 5e5),
                 "profile": [{"k": atm + 20 * i, "c_oi": r.uniform(1e5, 9e5) * (1 + max(0, i) / 3), "p_oi": r.uniform(1e5, 9e5) * (1 + max(0, -i) / 3),
                              "c_chg": r.uniform(-1e5, 2e5), "p_chg": r.uniform(-1e5, 2e5), "atm": i == 0} for i in range(-4, 5)]}
        fut = {"oi_chg_pct": r.uniform(-4, 4), "basis_ann": r.uniform(4, 9), "rollover": r.uniform(5, 60), "buildup": None}
        fut["buildup"] = buildup(q["pct"], fut["oi_chg_pct"])
        expiry, dte = (today + timedelta(days=20)).isoformat(), 20
        deliv = [((today - timedelta(days=28 - i)).isoformat(), r.uniform(35, 65)) for i in range(20)]
    else:
        key = instrument_key(row)
        ins = STATE["instr"] or {}
        exps = sorted(ins.get("opt_exp", {}).get(sym, []))
        futs = ins.get("fut", {}).get(sym, [])
        if exps:
            expiry, dte = exps[0].isoformat(), (exps[0] - today).days

        async def get_chain():
            if not expiry:
                return {}
            rows = await upx("/v2/option/chain", {"instrument_key": key, "expiry_date": expiry})
            return chain_metrics(rows or [], q["ltp"])

        async def get_fut():
            if not futs:
                return {}
            near, nxt = futs[0][1], (futs[1][1] if len(futs) > 1 else None)
            fq = await quotes([k for k in (near, nxt) if k])
            return futures_metrics(q, futs, fq, {k: await _prev_oi(k, today) for k in (near, nxt) if k}, today)
        res = await asyncio.gather(_candles_for(key, tf, q), daily_1y(key), cached(f"chain:{sym}:{expiry}", 30, get_chain),
                                   cached(f"fut:{sym}", 15, get_fut), return_exceptions=True)
        candles, daily, chain, fut = [x if not isinstance(x, Exception) else ([] if i < 2 else {}) for i, x in enumerate(res)]
        for i, x in enumerate(res):
            if isinstance(x, Exception):
                STATE["errors"][["candles", "daily", "option chain", "futures"][i]] = f"{type(x).__name__}: {str(x)[:100]}"
        deliv = STATE["deliv"].get(sym, [])
    prev_days = [r_ for r_ in daily if r_[0] < _today_start_ms()]
    levels = {"prev": q["prev"]}
    if prev_days:
        H, L, C = prev_days[-1][2], prev_days[-1][3], prev_days[-1][4]
        P = (H + L + C) / 3
        levels.update({"pivot_r1": 2 * P - L, "pivot_s1": 2 * P - H})
        last250 = prev_days[-250:]
        levels.update({"hi52": max([r_[2] for r_ in last250] + [q.get("high") or 0]),
                       "lo52": min([r_[3] for r_ in last250] + [q.get("low") or q["ltp"]]),
                       "hi60": max(r_[2] for r_ in prev_days[-60:]), "lo60": min(r_[3] for r_ in prev_days[-60:])})
        vols = [r_[5] for r_ in prev_days[-20:]]
        levels["avg_vol20"] = sum(vols) / len(vols) if vols else None
    news = (_cache.get(f"news:{sym}") or (0, None))[1]
    recent = [x["score"] for x in (news or []) if x.get("age_h", 99) <= 48]
    news_avg = sum(recent) / len(recent) if recent else None
    grp = next((g for g in snap["groups"] if sym in g["stocks"]), None)
    sector_rel = q["pct"] - grp["avg"] if grp else None
    sig = signals(sym, q, daily, chain, deliv, fut, news_avg, sector_rel, snap["_mcx"], STATE["part"])
    return {"sym": sym, "name": row["name"], "group": row["group"], "quote": q, "tf": tf, "candles": candles, "levels": levels,
            "chain": chain, "expiry": expiry, "dte": dte, "fut": fut, "deliv": deliv[-20:], "fii": STATE["part"],
            "signals": sig, "health": _health()}


# ===================================================================================================== live-2.0
# ---- nightly feed (built in Colab after each close; see the module docstring)
IDX_COLS = ["index", "last", "pct", "open", "high", "low", "prev", "yearHigh", "yearLow", "pe", "pb", "dy", "adv", "dec",
            "unch", "chg365", "chg30", "weekAgo"]


def _num(x):
    try:
        v = float(str(x).replace(",", ""))
        return v if math.isfinite(v) else None
    except (TypeError, ValueError):
        return None


def load_feed() -> None:
    """Read the nightly feed from FEED_URL (if set) or FEED_PATH. Keeps the last good copy on any failure and never
    goes back to an older build."""
    try:
        if FEED_URL:
            h = dict(BROWSER)
            if FEED_TOKEN:
                h["Authorization"] = f"Bearer {FEED_TOKEN}"
            r = httpx.get(FEED_URL, headers=h, timeout=30, follow_redirects=True)
            r.raise_for_status()
            j, src = r.json(), "url"
        elif FEED_PATH.exists():
            m = FEED_PATH.stat().st_mtime
            if m == STATE["feed_mtime"] and STATE["feed"]:
                return
            j, src = json.loads(FEED_PATH.read_text(encoding="utf-8")), "file"
            STATE["feed_mtime"] = m
        else:
            STATE["errors"]["feed"] = "nightly data not connected yet"
            return
        if not (isinstance(j, dict) and j.get("schema") == FEED_SCHEMA and j.get("built_at")):
            raise ValueError("not a terminal feed")
        old = STATE["feed"]
        if old and str(old.get("built_at", "")) > str(j["built_at"]):
            return
        STATE["feed"], STATE["feed_src"] = j, src
        STATE["errors"].pop("feed", None)
        _cache.pop("market", None)
    except Exception as e:  # noqa: BLE001
        STATE["errors"]["feed"] = f"nightly data unavailable ({type(e).__name__})"


def _feed_loop() -> None:
    while True:
        load_feed()
        time.sleep(600)


def _feed_meta() -> dict:
    f = STATE["feed"] or {}
    return {"built_at": f.get("built_at"), "session": f.get("session"), "src": STATE["feed_src"],
            "code_version": f.get("code_version")}


# ---- index names: NSE's full names (feed) vs Upstox's short names (instrument master)
_IDX_ABBR = [("FINANCIAL SERVICES", "FIN SERVICE"), ("PRIVATE BANK", "PVT BANK"), ("SMALLCAP", "SMLCAP"),
             ("CONSUMER DURABLES", "CONSR DURBL"), ("INFRASTRUCTURE", "INFRA"), ("SERVICES SECTOR", "SERV SECTOR"),
             ("INDIA DEFENCE", "IND DEFENCE"), ("CAPITAL MARKETS", "CAPITAL MKT"), ("HEALTHCARE INDEX", "HEALTHCARE"),
             ("INDIA CONSUMPTION", "CONSUMPTION"), ("MIDCAP SELECT", "MID SELECT"), ("&", " AND ")]


def norm_index(name: str) -> str:
    s = " ".join(str(name).upper().replace("&", " & ").split())
    for full, ab in _IDX_ABBR:
        s = s.replace(full, ab)
    return re.sub(r"[^A-Z0-9]", "", s)


def index_key(name: str) -> str | None:
    if name in ("NIFTY 50", "NIFTY BANK", "INDIA VIX"):
        return {"NIFTY 50": INDICES["NIFTY 50"], "NIFTY BANK": INDICES["BANK NIFTY"], "INDIA VIX": INDICES["INDIA VIX"]}[name]
    idx = (STATE["instr"] or {}).get("idx") or {}
    m = STATE.setdefault("_idx_norm", {})
    if len(m) != len(idx):
        m.clear()
        m.update({norm_index(k): v for k, v in idx.items()})
    return m.get(norm_index(name))


def overlay_index_row(row: list, q: dict) -> list:
    """End-of-day index row (IDX_COLS order) + live quote -> live row. Valuation ratios move with the price
    (earnings and book value do not change intraday); 30/365-day changes keep their base price."""
    d = dict(zip(IDX_COLS, row))
    last0, ltp = _num(d.get("last")), q.get("ltp")
    if not ltp:
        return list(row[:len(IDX_COLS)]) + ["eod"]
    d.update(last=ltp, pct=round(q["pct"], 2), prev=q["prev"], open=q.get("open") or d.get("open"),
             high=q.get("high") or d.get("high"), low=q.get("low") or d.get("low"))
    if q.get("high") and _num(d.get("yearHigh")):
        d["yearHigh"] = max(_num(d["yearHigh"]), q["high"])
    if q.get("low") and _num(d.get("yearLow")):
        d["yearLow"] = min(_num(d["yearLow"]), q["low"])
    if last0:
        f = ltp / last0
        for k in ("chg365", "chg30"):
            if _num(d.get(k)) is not None:
                d[k] = round(((1 + _num(d[k]) / 100) * f - 1) * 100, 2)
        for k in ("pe", "pb"):
            if _num(d.get(k)):
                d[k] = round(_num(d[k]) * f, 2)
        if _num(d.get("dy")):
            d["dy"] = round(_num(d["dy"]) / f, 2)
    return [d.get(c) for c in IDX_COLS] + ["live"]


# ---- Nifty option chain (Upstox reports open interest and volume in units; NSE and this page use contracts)
def nifty_oc_summary(rows: list, expiry: date, lot: int | None, now: datetime) -> dict | None:
    st = []
    div = float(lot or 1)
    spot = None
    for r in rows or []:
        c, p = r.get("call_options") or {}, r.get("put_options") or {}
        cm, pm = c.get("market_data") or {}, p.get("market_data") or {}
        cg, pg = c.get("option_greeks") or {}, p.get("option_greeks") or {}
        spot = spot or r.get("underlying_spot_price")
        st.append({"k": float(r["strike_price"]), "c_oi": (cm.get("oi") or 0) / div, "p_oi": (pm.get("oi") or 0) / div,
                   "c_prev": (cm.get("prev_oi") or 0) / div, "p_prev": (pm.get("prev_oi") or 0) / div,
                   "c_vol": (cm.get("volume") or 0) / div, "p_vol": (pm.get("volume") or 0) / div,
                   "c_iv": cg.get("iv") or 0, "p_iv": pg.get("iv") or 0})
    st.sort(key=lambda x: x["k"])
    if not st or not spot:
        return None
    c_oi, p_oi = sum(x["c_oi"] for x in st), sum(x["p_oi"] for x in st)
    c_vol, p_vol = sum(x["c_vol"] for x in st), sum(x["p_vol"] for x in st)
    pain = min(st, key=lambda K: sum(x["c_oi"] * max(0.0, K["k"] - x["k"]) + x["p_oi"] * max(0.0, x["k"] - K["k"]) for x in st))["k"]
    atm = min(st, key=lambda x: abs(x["k"] - spot))
    top = lambda side: [[x["k"], round(x[f"{side}_oi"]), round(x[f"{side}_oi"] - x[f"{side}_prev"])]
                        for x in sorted(st, key=lambda x: -x[f"{side}_oi"])[:3]]
    return {"ts": now.strftime("%d-%b-%Y %H:%M:%S"), "expiry": expiry.strftime("%d-%b-%Y"), "spot": spot, "src": "live",
            "pcr_oi": round(p_oi / c_oi, 3) if c_oi else None, "pcr_vol": round(p_vol / c_vol, 3) if c_vol else None,
            "oi_call": round(c_oi), "oi_put": round(p_oi), "chg_call": round(sum(x["c_oi"] - x["c_prev"] for x in st)),
            "chg_put": round(sum(x["p_oi"] - x["p_prev"] for x in st)), "max_pain": pain, "atm": atm["k"],
            "atm_civ": atm["c_iv"], "atm_piv": atm["p_iv"], "topC": top("c"), "topP": top("p"),
            "near": [[x["k"], round(x["p_oi"]), round(x["c_oi"])] for x in st if abs(x["k"] - atm["k"]) <= 600]}


async def _nifty_oc() -> dict | None:
    ins = STATE["instr"] or {}
    now = datetime.now(IST)
    exps = sorted(e for e in ins.get("idx_opt", {}).get("NIFTY", ())
                  if e > now.date() or (e == now.date() and now.hour * 60 + now.minute < 930))
    if not exps:
        return None
    rows = await upx("/v2/option/chain", {"instrument_key": INDICES["NIFTY 50"], "expiry_date": exps[0].isoformat()})
    return nifty_oc_summary(rows or [], exps[0], (ins.get("idx_lot") or {}).get(exps[0]), now)


# ---- global markets (Yahoo Finance chart data)
GLOBAL = [("^DJI", "Dow Jones"), ("^GSPC", "S&P 500"), ("^IXIC", "Nasdaq"), ("^FTSE", "FTSE 100"), ("^GDAXI", "DAX"),
          ("^N225", "Nikkei 225"), ("^HSI", "Hang Seng"), ("000001.SS", "Shanghai"), ("^KS11", "Kospi"),
          ("^TNX", "US 10Y yield"), ("DX-Y.NYB", "Dollar index"), ("INR=X", "USD/INR"), ("BZ=F", "Brent crude"),
          ("GC=F", "Gold"), ("ES=F", "S&P futures")]


def yahoo_row(sym: str, name: str, j: dict) -> list | None:
    """[sym, name, last, previous close, last time (ISO), currency, last 22 closes]. The previous close is the last
    daily close before the day of the latest price (Yahoo's own field differs between endpoints)."""
    x = j["chart"]["result"][0]
    m, ts = x["meta"], x.get("timestamp") or []
    closes = (x.get("indicators", {}).get("quote") or [{}])[0].get("close") or []
    pairs = [(t, c) for t, c in zip(ts, closes) if c is not None]
    if not pairs:
        return None
    tz = ZoneInfo(m.get("exchangeTimezoneName") or "UTC")
    day = lambda t: datetime.fromtimestamp(t, tz).date()
    last, t_last = m.get("regularMarketPrice") or pairs[-1][1], m.get("regularMarketTime") or pairs[-1][0]
    prior = [c for t, c in pairs if day(t) < day(t_last)]
    prev = prior[-1] if prior else m.get("chartPreviousClose")
    if not prev:
        return None
    return [sym, name, float(last), float(prev), datetime.fromtimestamp(t_last, ZoneInfo("UTC")).isoformat().replace("+00:00", "Z"),
            m.get("currency"), [round(float(c), 4) for _, c in pairs][-22:]]


async def _global() -> list:
    async def one(sym, name):
        r = await _client.get(f"{YAHOO}/{quote(sym, safe='')}", params={"range": "2mo", "interval": "1d"}, headers=BROWSER, timeout=12)
        r.raise_for_status()
        return yahoo_row(sym, name, r.json())
    res = await asyncio.gather(*(one(s, n) for s, n in GLOBAL), return_exceptions=True)
    live = {x[0]: x for x in res if isinstance(x, list)}
    feed = {x[0]: x for x in (((STATE["feed"] or {}).get("global") or {}).get("g") or [])}
    return [live.get(s) or feed.get(s) for s, _ in GLOBAL if live.get(s) or feed.get(s)]


# ---- market headlines (major financial publishers only)
CREDIBLE = ("economic times", "et markets", "et now", "moneycontrol", "business standard", "mint", "livemint", "businessline",
            "hindu businessline", "reuters", "bloomberg", "cnbc", "ndtv profit", "financial express", "business today",
            "the hindu", "fortune india", "the economist", "financial times", "wall street journal", "press trust of india",
            "pib", "reserve bank of india", "nse", "bse")
MARKET_Q = '(Sensex OR Nifty OR "Dalal Street" OR "stock market" OR RBI OR FPI OR "rupee") when:2d'


def credible(source: str) -> bool:
    s = (source or "").lower()
    return any(c == s or s.startswith(c) or s.startswith("the " + c) or c in s.split(" - ")[0] for c in CREDIBLE)


def parse_market_rss(content: bytes, limit: int = 15) -> list:
    root = ET.fromstring(content)
    items, seen = [], set()
    for it in root.iter("item"):
        title = (it.findtext("title") or "").strip()
        src = (it.findtext("source") or "").strip()
        if src and title.endswith(" - " + src):
            title = title[: -len(src) - 3]
        if not credible(src):
            continue
        try:
            ts = parsedate_to_datetime(it.findtext("pubDate"))
        except Exception:  # noqa: BLE001
            continue
        k = re.sub(r"[^a-z0-9]", "", title.lower())[:60]
        if k in seen:
            continue
        seen.add(k)
        items.append([ts.astimezone(ZoneInfo("UTC")).isoformat().replace("+00:00", "Z"), title, src])
    items.sort(key=lambda x: x[0], reverse=True)
    return items[:limit]


async def _market_news() -> list:
    r = await _client.get(f"{NEWS_RSS}?q={quote_plus(MARKET_Q)}&hl=en-IN&gl=IN&ceid=IN:en", headers=BROWSER, timeout=15)
    r.raise_for_status()
    return parse_market_rss(r.content)


# ---- Nifty 500 breadth (live, from the constituent list in the feed)
N500_MIN = int(os.environ.get("N500_MIN", "400"))      # fewer live quotes than this: keep the end-of-day count
async def _n500_breadth() -> list | None:
    syms = (STATE["feed"] or {}).get("n500") or []
    eq = (STATE["instr"] or {}).get("eq") or {}
    keys = [eq[s] for s in syms if s in eq]
    if len(keys) < N500_MIN:
        return None
    qq = await quotes(keys)
    pcts = [x["pct"] for x in qq.values() if x.get("ltp") and quote_is_today(x)]
    if len(pcts) < N500_MIN:
        return None
    return [sum(p > 0 for p in pcts), sum(p < 0 for p in pcts), sum(p == 0 for p in pcts)]


async def _market() -> dict:
    feed = STATE["feed"] or {}
    ind = feed.get("indices") or {}
    cols = ind.get("cols") or IDX_COLS
    rows = [[r[cols.index(c)] if c in cols and cols.index(c) < len(r) else None for c in IDX_COLS] for r in ind.get("idx") or []]
    out = {"feed": _feed_meta(), "ts": ind.get("ts"), "cols": IDX_COLS + ["src"], "fiidii": feed.get("fiidii"),
           "events": feed.get("events"), "holidays": feed.get("holidays"), "nifty_oc": feed.get("nifty_oc"),
           "news": feed.get("news") or [], "global": ((feed.get("global") or {}).get("g") or [])}
    q = feed.get("quotes") or {}
    qc = q.get("cols") or []
    out["quotes"] = [{**dict(zip(qc, r)), "ffmc": dict(zip(qc, r)).get("ffmcCr")} for r in q.get("q") or []]
    out["fii_long"] = (STATE["part"] or {}).get("long_share", feed.get("fii_long"))
    if DEMO:
        out["indices"] = [r + ["eod"] for r in rows]
        return out
    keys = {r[0]: index_key(r[0]) for r in rows}
    tasks = [quotes([k for k in set(keys.values()) if k]), _nifty_oc(), _global(), _market_news(), _n500_breadth(),
             cached("snapshot", 3, _snapshot)]
    iq, oc, gl, news, b500, snap = await asyncio.gather(*tasks, return_exceptions=True)
    for name, x in (("index quotes", iq), ("nifty options", oc), ("global markets", gl), ("market news", news), ("breadth", b500)):
        if isinstance(x, Exception):
            STATE["errors"][f"market: {name}"] = f"{type(x).__name__}: {str(x)[:100]}"
        else:
            STATE["errors"].pop(f"market: {name}", None)
    iq = iq if isinstance(iq, dict) else {}
    live_rows = []
    for r in rows:
        qq = iq.get(keys.get(r[0]))
        live_rows.append(overlay_index_row(r, qq) if qq and quote_is_today(qq) else r + ["eod"])
    if isinstance(snap, dict):
        b = snap.get("breadth") or {}
        for r in live_rows:
            if r[0] == "NIFTY 50" and r[-1] == "live":
                r[12], r[13], r[14] = b.get("up"), b.get("down"), max(0, len(snap.get("stocks") or []) - b.get("up", 0) - b.get("down", 0))
    if isinstance(b500, list):
        for r in live_rows:
            if r[0] == "NIFTY 500":
                r[12], r[13], r[14] = b500
    out["indices"] = live_rows
    if any(r[-1] == "live" for r in live_rows):
        out["ts"] = datetime.now(IST).strftime("%d-%b-%Y %H:%M")
    if isinstance(oc, dict):
        out["nifty_oc"] = oc
    if isinstance(gl, list) and gl:
        out["global"] = gl
    if isinstance(news, list) and news:
        out["news"] = news
    return out


@app.get("/api/market")
async def api_market():
    if not STATE["ready"]:
        return JSONResponse({"loading": True}, status_code=503)
    ttl = 30 if market_phase_by_clock() != "CLOSED" else 300
    try:
        return await cached("market", ttl, _market)
    except Exception as e:  # noqa: BLE001
        STATE["errors"]["market"] = f"{type(e).__name__}: {str(e)[:120]}"
        hit = _cache.get("market")
        return hit[1] if hit else JSONResponse({"error": STATE["errors"]["market"]}, status_code=503)


# ---- daily candles for the stock page (3 months; every stock gets open/high/low/close)
def daily_payload(rows: list, q: dict | None, n: int = 63) -> dict:
    """Completed sessions from the daily history plus today's session from the live quote (only when the quote is
    from today). dates/c/v (volume in lakh shares) and ohlc [ms, o, h, l, c, volume] of the same length."""
    t0 = _today_start_ms()
    d = [r for r in rows if r[0] < t0]
    partial = False
    if q and quote_is_today(q) and q.get("open"):
        o, c = q["open"], q["ltp"]
        d.append([t0, o, max(q.get("high") or c, o, c), min(q.get("low") or c, o, c), c, q.get("volume") or 0, 0])
        partial = market_phase_by_clock() != "CLOSED"
    d = d[-n:]
    return {"dates": [datetime.fromtimestamp(r[0] / 1000, IST).date().isoformat() for r in d], "c": [r[4] for r in d],
            "v": [round((r[5] or 0) / 1e5, 4) for r in d], "ohlc": [[r[0], r[1], r[2], r[3], r[4], r[5]] for r in d],
            "partial": partial}


@app.get("/api/daily/{sym}")
async def api_daily(sym: str):
    if not STATE["ready"]:
        return JSONResponse({"loading": True}, status_code=503)
    row = next((r for r in _universe() if r["sym"] == sym), None)
    if not row:
        return JSONResponse({"error": "unknown symbol"}, status_code=404)
    snap = await cached("snapshot", 3, _snapshot)
    q = snap["_quotes"].get(sym)
    if DEMO:
        rows = demo_candles(f"{sym}d{datetime.now(IST).date()}", 80, _today_start_ms() - 80 * 86400e3, 86400e3, (q or {}).get("prev") or 1000)
    else:
        try:
            rows = await daily_1y(instrument_key(row))
        except Exception as e:  # noqa: BLE001
            return JSONResponse({"error": f"daily candles unavailable ({type(e).__name__})"}, status_code=503)
    return {"sym": sym, **daily_payload(rows, q)}


@app.get("/api/model")
async def api_model():
    m = (STATE["feed"] or {}).get("model")
    if not m or not m.get("stocks"):
        return {"stocks": None, "note": "The model's calls appear here after the first recorded day."}
    return m
