"""
Mini Bloomberg Terminal - Web Edition
=====================================

Same data engine as the Excel version (prices, news, FII/DII, economic calendar,
OI / PCR) but the output is a live web dashboard instead of an Excel workbook.

Run from Anaconda PowerShell:
    conda activate base
    python mini_bloomberg_web.py

It starts a small web server and opens http://127.0.0.1:8050 in your browser.
Keep the PowerShell window open - closing it (or Ctrl+C) stops the terminal.

Running inside a Jupyter notebook also works: paste the whole file into one cell and run
it. The server then runs in the background and the cell finishes straight away. To stop
it, run  stop_terminal()  in another cell (re-running the cell restarts it cleanly).

Options:
    python mini_bloomberg_web.py --port 9000        use another port
    python mini_bloomberg_web.py --no-browser       do not auto-open the browser
    python mini_bloomberg_web.py --host 0.0.0.0     also open it from your phone / another
                                                    PC on the same Wi-Fi (http://<this-pc-ip>:8050)

Needs (all already in your Excel version, plus Flask which ships with Anaconda):
    flask pandas requests yfinance beautifulsoup4 lxml tradingview-screener

Refresh schedule (edit INTERVALS below to change):
  - prices        every 5 seconds
  - news          every 10 seconds
  - OI / PCR      every 5 seconds
  - FII / DII     every 60 seconds
  - calendar      full rebuild hourly, actuals every 5 minutes

The server fetches data in background threads and keeps the latest copy in memory.
The browser only reads that copy, so many tabs / devices do not multiply the load
on the data sources.
"""

from __future__ import annotations

import argparse
import hmac
import logging
import math
import os
import socket
import sys
import threading
import time
import warnings
import webbrowser
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import date, datetime, timedelta, timezone
from typing import Any

import pandas as pd
import requests
import yfinance as yf
from bs4 import BeautifulSoup
from flask import Flask, Response, jsonify, request
from tradingview_screener import Query, col
from werkzeug.serving import make_server

warnings.filterwarnings("ignore")

logging.basicConfig(level=logging.INFO, format="%(asctime)s | %(message)s", datefmt="%H:%M:%S")
log = logging.getLogger("terminal")
logging.getLogger("werkzeug").setLevel(logging.WARNING)
logging.getLogger("yfinance").setLevel(logging.CRITICAL)

UNAVAILABLE_MESSAGE = "[!] DATA TEMPORARILY UNAVAILABLE"

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "en-US,en;q=0.9",
    "Accept": "application/json, text/plain, */*",
}

MONTH_NUM_TO_CODE = {
    1: "F",
    2: "G",
    3: "H",
    4: "J",
    5: "K",
    6: "M",
    7: "N",
    8: "Q",
    9: "U",
    10: "V",
    11: "X",
    12: "Z",
}

MARKET_QUERY_GROUPS = ("futures", "india", "forex", "cfd")

# yfinance now returns ^TNX in percent (e.g. 4.52). Older versions returned yield x 10.
# "is_yield" auto-corrects the old style (anything above 25 is divided by 10).
PRICE_STATIC_FALLBACKS = {
    "DXY": {"yf_symbol": "DX-Y.NYB", "scale": 1.0},
    "US10Y": {"yf_symbol": "^TNX", "scale": 1.0, "is_yield": True},
}

WATCHLIST_CONFIG = [
    {
        "label": "COMEX Gold Near",
        "kind": "contract",
        "root": "GC",
        "exchange_hint": "COMEX",
        "valid_months": list(range(1, 13)),
        "offset": 0,
    },
    {
        "label": "COMEX Gold Far",
        "kind": "contract",
        "root": "GC",
        "exchange_hint": "COMEX",
        "valid_months": list(range(1, 13)),
        "offset": 1,
    },
    {
        "label": "COMEX Silver Near",
        "kind": "contract",
        "root": "SI",
        "exchange_hint": "COMEX",
        "valid_months": list(range(1, 13)),
        "offset": 0,
    },
    {
        "label": "COMEX Silver Far",
        "kind": "contract",
        "root": "SI",
        "exchange_hint": "COMEX",
        "valid_months": list(range(1, 13)),
        "offset": 1,
    },
    {
        "label": "LME Copper Near",
        "kind": "contract",
        "root": "CA",
        "exchange_hint": "LME",
        "valid_months": list(range(1, 13)),
        "offset": 0,
    },
    {
        "label": "LME Copper Far",
        "kind": "contract",
        "root": "CA",
        "exchange_hint": "LME",
        "valid_months": list(range(1, 13)),
        "offset": 1,
    },
    {
        "label": "MCX Gold Near",
        "kind": "contract",
        "root": "GOLD",
        "exchange_hint": "MCX",
        "valid_months": [2, 4, 6, 8, 10, 12],
        "offset": 0,
    },
    {
        "label": "MCX Gold Far",
        "kind": "contract",
        "root": "GOLD",
        "exchange_hint": "MCX",
        "valid_months": [2, 4, 6, 8, 10, 12],
        "offset": 1,
    },
    {
        "label": "MCX Silver Near",
        "kind": "contract",
        "root": "SILVER",
        "exchange_hint": "MCX",
        "valid_months": [3, 5, 7, 9, 12],
        "offset": 0,
    },
    {
        "label": "MCX Silver Far",
        "kind": "contract",
        "root": "SILVER",
        "exchange_hint": "MCX",
        "valid_months": [3, 5, 7, 9, 12],
        "offset": 1,
    },
    {
        "label": "MCX Copper Near",
        "kind": "contract",
        "root": "COPPER",
        "exchange_hint": "MCX",
        "valid_months": list(range(1, 13)),
        "offset": 0,
    },
    {
        "label": "MCX Copper Far",
        "kind": "contract",
        "root": "COPPER",
        "exchange_hint": "MCX",
        "valid_months": list(range(1, 13)),
        "offset": 1,
    },
    {
        "label": "USDINR Near",
        "kind": "contract",
        "root": "USDINR",
        "exchange_hint": ("MSEI", "NSE"),
        "valid_months": list(range(1, 13)),
        "offset": 0,
    },
    {
        "label": "USDINR Far",
        "kind": "contract",
        "root": "USDINR",
        "exchange_hint": ("MSEI", "NSE"),
        "valid_months": list(range(1, 13)),
        "offset": 1,
    },
    {
        "label": "Dollar Index",
        "kind": "static",
        "symbol": "DXY",
        "exchange_hint": "TVC",
    },
    {
        "label": "US 10Y Yield",
        "kind": "static",
        "symbol": "US10Y",
        "exchange_hint": "TVC",
    },
]


# ----------------------------------------------------------------------------
# Small helpers
# ----------------------------------------------------------------------------
def to_naive(dt: Any) -> datetime:
    if dt is None:
        return datetime.utcnow()
    if isinstance(dt, datetime):
        if dt.tzinfo is not None:
            dt = dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
    return datetime.utcnow()


def parse_pub_date(s: str) -> datetime:
    for fmt in (
        "%a, %d %b %Y %H:%M:%S %Z",
        "%a, %d %b %Y %H:%M:%S %z",
        "%Y-%m-%dT%H:%M:%S%z",
        "%Y-%m-%dT%H:%M:%SZ",
        "%Y-%m-%dT%H:%M:%S",
    ):
        try:
            return to_naive(datetime.strptime((s or "").strip(), fmt))
        except Exception:
            continue
    return datetime.utcnow()


def to_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        text = str(value).replace(",", "").replace("₹", "").strip()
        if text in ("", "-", "N/A", "None"):
            return None
        return float(text)
    except Exception:
        return None


# ----------------------------------------------------------------------------
# Refresh scheduler timing (same logic as the Excel version)
# ----------------------------------------------------------------------------
_next_run = {
    "prices": 0.0,
    "news": 0.0,
    "oi": 0.0,
    "fii_attempt": 0.0,
    "calendar": 0.0,
    "calendar_actuals": 0.0,
}

INTERVALS = {
    "prices": 5,
    "news": 10,
    "oi": 5,
    "fii_attempt": 60,
    "calendar": 3600,
    "calendar_actuals": 300,
}

# On a web host (Render sets RENDER=true) the machine is small and the data sources can block a
# server that asks too often, so refresh more gently. On your own PC the fast timings stay.
HOSTED = bool(os.environ.get("RENDER")) or os.environ.get("KOTAK_DESK_HOSTED") == "1"
if HOSTED:
    INTERVALS.update({"prices": 10, "news": 30, "oi": 120, "fii_attempt": 120, "calendar_actuals": 600})

# A source that does not answer must not leave the page on "waiting" forever.
TASK_TIMEOUT = {"prices": 90, "news": 60, "oi": 120, "fii_attempt": 150, "calendar": 180, "calendar_actuals": 120}
MAX_STUCK_PER_TASK = 3
STARTED_AT = time.time()


def is_due(module: str) -> bool:
    return time.time() >= _next_run.get(module, 0.0)


def mark_done(module: str) -> None:
    interval = INTERVALS.get(module, 60)
    now = time.time()
    previous_due = _next_run.get(module, 0.0)
    if previous_due <= 0:
        _next_run[module] = now + interval
        return
    next_due = previous_due
    while next_due <= now:
        next_due += interval
    _next_run[module] = next_due


def force_all_modules_due() -> None:
    for key in _next_run:
        _next_run[key] = 0.0


# ----------------------------------------------------------------------------
# Prices
# ----------------------------------------------------------------------------
def generate_contract_symbols(root: str, valid_months: list[int], years_forward: int = 2) -> list[str]:
    today = datetime.now()
    symbols: list[str] = []
    for year in range(today.year, today.year + years_forward + 1):
        for month in valid_months:
            if year == today.year and month < today.month:
                continue
            symbols.append(f"{root}{MONTH_NUM_TO_CODE[month]}{year}")
    return symbols


def build_watchlist_specs() -> list[dict[str, Any]]:
    specs: list[dict[str, Any]] = []
    for config in WATCHLIST_CONFIG:
        spec = dict(config)
        if spec["kind"] == "contract":
            generated = generate_contract_symbols(
                root=spec["root"],
                valid_months=spec["valid_months"],
                years_forward=2,
            )
            offset = int(spec["offset"])
            spec["candidates"] = generated[offset:offset + 4]
        else:
            spec["candidates"] = [spec["symbol"]]
        specs.append(spec)
    return specs


def fetch_yfinance_snapshot(yf_symbol: str, scale: float = 1.0) -> tuple[float, float | None]:
    ticker = yf.Ticker(yf_symbol)
    price = None
    change_pct = None

    try:
        fast_info = getattr(ticker, "fast_info", None)
        if fast_info:
            price = fast_info.get("lastPrice") or fast_info.get("last_price")
            previous = fast_info.get("previousClose") or fast_info.get("previous_close")
            if price is not None and previous not in (None, 0):
                change_pct = ((float(price) - float(previous)) / float(previous)) * 100.0
    except Exception:
        pass

    if price is None:
        history = ticker.history(period="2d", interval="1d")
        if history.empty:
            raise ValueError(f"No yfinance data for {yf_symbol}")
        price = history["Close"].iloc[-1]
        if len(history) >= 2:
            previous = history["Close"].iloc[-2]
            if previous not in (None, 0):
                change_pct = ((float(price) - float(previous)) / float(previous)) * 100.0

    return round(float(price) * scale, 4), round(float(change_pct), 4) if change_pct is not None else None


def fetch_tradingview_rows(symbols: list[str]) -> pd.DataFrame:
    data = (
        Query()
        .set_markets(*MARKET_QUERY_GROUPS)
        .select("name", "description", "close", "change", "exchange")
        .where(col("name").isin(symbols))
        .get_scanner_data()
    )
    return data[1].copy()


def filter_exchange_matches(raw_df: pd.DataFrame, exchange_hint: Any) -> pd.DataFrame:
    if raw_df.empty or not exchange_hint:
        return raw_df
    if isinstance(exchange_hint, (list, tuple, set)):
        mask = pd.Series(False, index=raw_df.index)
        for hint in exchange_hint:
            mask = mask | raw_df["exchange"].astype(str).str.contains(str(hint), case=False, na=False)
        return raw_df[mask]
    return raw_df[
        raw_df["exchange"].astype(str).str.contains(str(exchange_hint), case=False, na=False)
    ]


def make_watchlist_row_from_match(spec: dict[str, Any], row: pd.Series, fallback_ticker: str) -> dict[str, Any]:
    return {
        "Watchlist": spec["label"],
        "Ticker": row.get("name", fallback_ticker),
        "Description": row.get("description", spec["label"]),
        "Price": row.get("close"),
        "Chg %": row.get("change"),
        "Exchange": row.get("exchange", spec.get("exchange_hint", "")),
        "Timestamp": datetime.now().strftime("%H:%M:%S"),
        "Status": "OK",
    }


def make_missing_watchlist_row(spec: dict[str, Any]) -> dict[str, Any]:
    return {
        "Watchlist": spec["label"],
        "Ticker": spec["candidates"][0],
        "Description": spec["label"],
        "Price": None,
        "Chg %": None,
        "Exchange": spec.get("exchange_hint", ""),
        "Timestamp": datetime.now().strftime("%H:%M:%S"),
        "Status": "Missing",
    }


def resolve_watchlist_row(raw_df: pd.DataFrame, spec: dict[str, Any]) -> dict[str, Any]:
    filtered = filter_exchange_matches(raw_df, spec.get("exchange_hint"))
    for candidate in spec["candidates"]:
        match = filtered[filtered["name"] == candidate] if not filtered.empty else pd.DataFrame()
        if match.empty:
            continue
        return make_watchlist_row_from_match(spec, match.iloc[0], candidate)

    if spec["kind"] == "static" and spec["symbol"] in PRICE_STATIC_FALLBACKS:
        fallback = PRICE_STATIC_FALLBACKS[spec["symbol"]]
        try:
            price, change_pct = fetch_yfinance_snapshot(
                yf_symbol=str(fallback["yf_symbol"]),
                scale=float(fallback["scale"]),
            )
            if fallback.get("is_yield") and price > 25:
                price = round(price / 10.0, 4)
            return {
                "Watchlist": spec["label"],
                "Ticker": spec["symbol"],
                "Description": spec["label"],
                "Price": price,
                "Chg %": change_pct,
                "Exchange": "yfinance",
                "Timestamp": datetime.now().strftime("%H:%M:%S"),
                "Status": "OK",
            }
        except Exception:
            pass

    return make_missing_watchlist_row(spec)


def contract_group_key(spec: dict[str, Any]) -> tuple[Any, ...]:
    exchange_hint = spec.get("exchange_hint")
    if isinstance(exchange_hint, (list, tuple, set)):
        normalized_hint = tuple(str(item) for item in exchange_hint)
    else:
        normalized_hint = (str(exchange_hint),)
    return (
        spec.get("root"),
        tuple(spec.get("valid_months", [])),
        normalized_hint,
    )


def resolve_contract_group_rows(raw_df: pd.DataFrame, specs: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    if not specs:
        return {}

    anchor = specs[0]
    full_candidates = generate_contract_symbols(
        root=str(anchor["root"]),
        valid_months=list(anchor["valid_months"]),
        years_forward=2,
    )
    filtered = filter_exchange_matches(raw_df, anchor.get("exchange_hint"))
    available_rows: list[tuple[str, pd.Series]] = []
    seen_symbols: set[str] = set()

    for candidate in full_candidates:
        match = filtered[filtered["name"] == candidate] if not filtered.empty else pd.DataFrame()
        if match.empty or candidate in seen_symbols:
            continue
        seen_symbols.add(candidate)
        available_rows.append((candidate, match.iloc[0]))

    resolved: dict[str, dict[str, Any]] = {}
    for spec in sorted(specs, key=lambda item: int(item.get("offset", 0))):
        offset = int(spec.get("offset", 0))
        if offset < len(available_rows):
            candidate, row = available_rows[offset]
            resolved[spec["label"]] = make_watchlist_row_from_match(spec, row, candidate)
        else:
            resolved[spec["label"]] = make_missing_watchlist_row(spec)
    return resolved


def fetch_market_watchlist() -> pd.DataFrame | None:
    specs = build_watchlist_specs()
    all_symbols = sorted({symbol for spec in specs for symbol in spec["candidates"]})
    raw_df = pd.DataFrame()
    try:
        raw_df = fetch_tradingview_rows(all_symbols)
    except Exception as exc:
        log.warning("PRICES | TradingView request failed: %s", str(exc)[:120])

    resolved_rows: dict[str, dict[str, Any]] = {}
    contract_groups: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    for spec in specs:
        if spec["kind"] == "contract":
            contract_groups.setdefault(contract_group_key(spec), []).append(spec)
        else:
            resolved_rows[spec["label"]] = resolve_watchlist_row(raw_df, spec)

    for grouped_specs in contract_groups.values():
        resolved_rows.update(resolve_contract_group_rows(raw_df, grouped_specs))

    rows = [resolved_rows.get(spec["label"], make_missing_watchlist_row(spec)) for spec in specs]
    data = pd.DataFrame(rows)
    if data.empty:
        return None
    return data[
        ["Watchlist", "Ticker", "Description", "Price", "Chg %", "Exchange", "Timestamp", "Status"]
    ]


# ----------------------------------------------------------------------------
# News
# ----------------------------------------------------------------------------
BREAKING_KEYWORDS = [
    "rate cut",
    "rate hike",
    "emergency",
    "war",
    "attack",
    "sanctions",
    "opec cut",
    "opec hike",
    "fed pivot",
    "rbi rate",
    "surprise",
    "shock",
    "crash",
    "collapse",
    "default",
    "ban",
    "nuclear",
    "ceasefire",
    "escalat",
    "missile",
    "explosion",
    "tariff hike",
    "gold record",
    "silver record",
    "crude spike",
    "oil ban",
]


def _is_breaking(headline: str) -> bool:
    return any(keyword in headline.lower() for keyword in BREAKING_KEYWORDS)


_news_store: list[dict[str, Any]] = []
_seen_headlines: set[str] = set()

NEWS_FEEDS = [
    ("TOI", "https://timesofindia.indiatimes.com/rssfeeds/1898055.cms"),
    ("Mint", "https://www.livemint.com/rss/markets"),
    ("Mint", "https://www.livemint.com/rss/economy"),
    ("Reuters", "https://feeds.reuters.com/reuters/businessNews"),
    ("Reuters", "https://feeds.reuters.com/reuters/companyNews"),
    ("Guardian", "https://www.theguardian.com/business/rss"),
    (
        "GNews",
        "https://news.google.com/rss/search?q=MCX+gold+silver+copper+USDINR+DXY+US10Y&hl=en-IN&gl=IN&ceid=IN:en",
    ),
]

COMMODITY_KW = [
    "gold",
    "silver",
    "copper",
    "mcx",
    "comex",
    "lme",
    "usdinr",
    "rupee",
    "dollar",
    "dxy",
    "us 10y",
    "treasury",
    "yield",
    "rbi",
    "fed",
    "fomc",
    "inflation",
    "cpi",
    "rate",
    "bullion",
    "metal",
    "tariff",
    "trade",
]


def fetch_feed_items(source: str, url: str) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    try:
        resp = requests.get(url, headers=HEADERS, timeout=5)
        soup = BeautifulSoup(resp.content, "xml")
        for item in soup.find_all("item"):
            headline = (item.title.text or "").strip()
            if not headline:
                continue
            if not any(keyword in headline.lower() for keyword in COMMODITY_KW):
                continue
            link = ""
            if item.link:
                link = item.link.text.strip()
            elif item.find("link"):
                link = item.find("link").get_text().strip()
            dt_obj = parse_pub_date(item.pubDate.text if item.pubDate else "")
            items.append(
                {
                    "Source": source,
                    "Time": dt_obj.strftime("%d-%m-%Y %H:%M"),
                    "Headline": headline,
                    "Link": link,
                    "dt_obj": dt_obj,
                    "Breaking": "BREAKING" if _is_breaking(headline) else "",
                }
            )
    except Exception:
        return []
    return items


def fetch_commodity_news() -> list[dict[str, Any]]:
    new_items: list[dict[str, Any]] = []

    with ThreadPoolExecutor(max_workers=min(6, len(NEWS_FEEDS))) as executor:
        futures = [executor.submit(fetch_feed_items, source, url) for source, url in NEWS_FEEDS]
        for future in as_completed(futures):
            try:
                items = future.result()
            except Exception:
                continue
            for entry in items:
                key = entry["Headline"][:120]
                if key in _seen_headlines:
                    continue
                _seen_headlines.add(key)
                new_items.append(entry)

    _news_store.extend(new_items)
    _news_store.sort(key=lambda item: item["dt_obj"], reverse=True)
    del _news_store[300:]
    return list(_news_store)


# ----------------------------------------------------------------------------
# FII / DII
# ----------------------------------------------------------------------------
def _make_nse_session() -> requests.Session:
    session = requests.Session()
    session.headers.update(
        {
            **HEADERS,
            "Referer": "https://www.nseindia.com/",
            "Origin": "https://www.nseindia.com",
        }
    )
    try:
        session.get("https://www.nseindia.com", timeout=12)
        time.sleep(1.0)
        session.get("https://www.nseindia.com/market-data/live-equity-market", timeout=10)
        time.sleep(0.5)
    except Exception:
        pass
    return session


def _fetch_nse_fii_api(session: requests.Session) -> list[dict[str, Any]]:
    resp = session.get(
        "https://www.nseindia.com/api/fiidiiTradeReact",
        headers={**HEADERS, "Referer": "https://www.nseindia.com/"},
        timeout=15,
    )
    resp.raise_for_status()
    payload = resp.json()
    if not isinstance(payload, list) or not payload:
        raise ValueError("NSE FII API returned empty data")

    rows: list[dict[str, Any]] = []
    for entry in payload:
        rows.append(
            {
                "Date": entry.get("date", ""),
                "Category": entry.get("category", ""),
                "Buy (Cr)": entry.get("buyValue", 0),
                "Sell (Cr)": entry.get("sellValue", 0),
                "Net (Cr)": entry.get("netValue", 0),
            }
        )
    return rows


def _fetch_moneycontrol_fii() -> list[dict[str, Any]]:
    resp = requests.get(
        "https://www.moneycontrol.com/stocks/marketstats/fii_dii_activity/index.php",
        headers=HEADERS,
        timeout=12,
    )
    soup = BeautifulSoup(resp.content, "html.parser")
    table = soup.find("table", {"class": "tbldata14"})
    rows: list[dict[str, Any]] = []
    if not table:
        return rows

    for tr in table.find_all("tr")[1:15]:
        cells = [td.get_text(strip=True) for td in tr.find_all("td")]
        if len(cells) < 5:
            continue
        rows.append(
            {
                "Date": cells[0],
                "Category": cells[1],
                "Buy (Cr)": cells[2],
                "Sell (Cr)": cells[3],
                "Net (Cr)": cells[4],
            }
        )
    return rows


def fetch_fii_dii_flow() -> list[dict[str, Any]]:
    try:
        session = _make_nse_session()
        rows = _fetch_nse_fii_api(session)
        if rows:
            return rows
    except Exception:
        log.warning("FII/DII | NSE source unavailable, trying MoneyControl")

    try:
        rows = _fetch_moneycontrol_fii()
        if rows:
            return rows
    except Exception:
        log.warning("FII/DII | MoneyControl source unavailable")

    return [
        {
            "Date": datetime.now().strftime("%d-%m-%Y"),
            "Category": "Data unavailable",
            "Buy (Cr)": "-",
            "Sell (Cr)": "-",
            "Net (Cr)": "-",
        }
    ]


# ----------------------------------------------------------------------------
# Economic calendar
# ----------------------------------------------------------------------------
CAL_KW = [
    "gdp",
    "cpi",
    "inflation",
    "interest rate",
    "fed",
    "fomc",
    "rbi",
    "mpc",
    "non-farm",
    "nfp",
    "unemployment",
    "jobless",
    "pmi",
    "retail sales",
    "industrial production",
    "trade balance",
    "pce",
    "adp",
    "housing",
    "consumer confidence",
    "manufacturing",
    "services",
    "central bank",
    "monetary policy",
    "repo rate",
    "payroll",
    "jobs",
    "employment",
    "michigan",
    "philadelphia",
    "building permits",
    "wpi",
    "iip",
    "core",
    "balance of trade",
    "current account",
    "budget",
    "nonfarm",
]


def _cal_ok(text: str) -> bool:
    return any(keyword in text.lower() for keyword in CAL_KW)


def get_week_bounds() -> tuple[date, date, date, date, date, date]:
    today = datetime.now().date()
    monday = today - timedelta(days=today.weekday())
    return (
        monday - timedelta(days=7),
        monday - timedelta(days=1),
        monday,
        monday + timedelta(days=6),
        monday + timedelta(days=7),
        monday + timedelta(days=13),
    )


_calendar_cache: dict[str, list[dict[str, Any]]] = {"last": [], "this": [], "next": []}


def _fetch_ff(suffix: str) -> list[dict[str, Any]]:
    url = f"https://nfs.faireconomy.media/ff_calendar_{suffix}.json"
    try:
        response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=12)
        response.raise_for_status()
        events: list[dict[str, Any]] = []
        for entry in response.json():
            title = (entry.get("title", "") or "").strip()
            country = (entry.get("country", "") or "").strip()
            impact = (entry.get("impact", "") or "").strip()
            if country not in ("USD", "INR"):
                continue
            if impact not in ("High", "Medium"):
                continue
            try:
                dt_obj = to_naive(datetime.fromisoformat(entry.get("date", "")))
            except Exception:
                dt_obj = datetime.utcnow()
            try:
                t_str = (
                    datetime.strptime((entry.get("time", "") or "").strip(), "%I:%M%p").strftime("%H:%M")
                    + " EST"
                )
            except Exception:
                t_str = entry.get("time", "") or "All Day"

            events.append(
                {
                    "Date": dt_obj.strftime("%d-%m-%Y"),
                    "Time": t_str,
                    "Country": "USD" if country == "USD" else "INR",
                    "Event": title,
                    "Impact": "HIGH" if impact == "High" else "MEDIUM",
                    "Previous": (entry.get("previous", "") or "-").strip() or "-",
                    "Forecast": (entry.get("estimate", "") or "-").strip() or "-",
                    "Actual": (entry.get("actual", "") or "-").strip() or "-",
                    "Link": "https://www.forexfactory.com/calendar",
                    "_dt": dt_obj,
                }
            )
        return events
    except Exception:
        return []


def _fetch_investing_range(date_from: date, date_to: date) -> list[dict[str, Any]]:
    events: list[dict[str, Any]] = []
    try:
        headers = {
            **HEADERS,
            "X-Requested-With": "XMLHttpRequest",
            "Referer": "https://in.investing.com/economic-calendar/",
        }
        response = requests.post(
            "https://in.investing.com/economic-calendar/Service/getCalendarFilteredData",
            headers=headers,
            data={
                "country[]": ["5", "72"],
                "importance[]": ["2", "3"],
                "timeZone": "20",
                "currentTab": "custom",
                "limit_from": "0",
                "dateFrom": date_from.strftime("%Y-%m-%d"),
                "dateTo": date_to.strftime("%Y-%m-%d"),
            },
            timeout=15,
        )
        soup = BeautifulSoup(response.json().get("data", ""), "html.parser")
        for row in soup.find_all("tr", class_="js-event-item"):
            try:
                dt_obj = to_naive(datetime.strptime(row.get("data-event-datetime", ""), "%Y/%m/%d %H:%M:%S"))
                name_td = row.find("td", class_="event")
                name = name_td.get_text(strip=True) if name_td else ""
                if not name or not _cal_ok(name):
                    continue
                bulls = row.find_all("i", class_="grayFullBullishIcon")
                impact = "HIGH" if len(bulls) >= 3 else "MEDIUM"
                actual = "-"
                forecast = "-"
                previous = "-"
                country = ""
                for td in row.find_all("td"):
                    cls = " ".join(td.get("class", []))
                    val = td.get_text(strip=True) or "-"
                    if "actual" in cls:
                        actual = val
                    if "forecast" in cls:
                        forecast = val
                    if "prev" in cls:
                        previous = val
                    if "flagCur" in cls and td.get_text(strip=True):
                        country = td.get_text(strip=True)
                if not country:
                    low = name.lower()
                    country = "INR" if any(token in low for token in ["india", "rbi", "wpi", "iip", "rupee"]) else "USD"
                link_tag = name_td.find("a") if name_td else None
                link = (
                    "https://in.investing.com" + link_tag["href"]
                    if link_tag and link_tag.get("href")
                    else "https://in.investing.com/economic-calendar/"
                )
                events.append(
                    {
                        "Date": dt_obj.strftime("%d-%m-%Y"),
                        "Time": dt_obj.strftime("%H:%M") + " IST",
                        "Country": country,
                        "Event": name,
                        "Impact": impact,
                        "Previous": previous,
                        "Forecast": forecast,
                        "Actual": actual,
                        "Link": link,
                        "_dt": dt_obj,
                    }
                )
            except Exception:
                continue
    except Exception:
        return events
    return events


def _bucket(events: list[dict[str, Any]], start_date: date, end_date: date) -> list[dict[str, Any]]:
    seen: set[str] = set()
    output: list[dict[str, Any]] = []
    for event in events:
        current_date = event["_dt"].date()
        key = f"{event['Date']}_{event['Event'][:30]}"
        if start_date <= current_date <= end_date and key not in seen:
            seen.add(key)
            output.append(event)
    output.sort(key=lambda item: item["_dt"])
    return output


def _merge_calendar_events(*event_groups: list[dict[str, Any]]) -> list[dict[str, Any]]:
    merged: list[dict[str, Any]] = []
    for group in event_groups:
        merged.extend(group)
    seen: dict[str, dict[str, Any]] = {}
    for event in sorted(merged, key=lambda item: item.get("_dt", datetime.utcnow())):
        key = f"{event.get('Date', '')}_{str(event.get('Event', ''))[:40]}"
        current = seen.get(key)
        if current is None:
            seen[key] = event
            continue
        current_actual = current.get("Actual", "-")
        new_actual = event.get("Actual", "-")
        if current_actual in ("-", "", "N/A") and new_actual not in ("-", "", "N/A"):
            seen[key] = event
    return sorted(seen.values(), key=lambda item: item.get("_dt", datetime.utcnow()))


def _has_pending_actuals(bucket_key: str) -> bool:
    now = datetime.utcnow()
    for event in _calendar_cache.get(bucket_key, []):
        if event.get("Actual", "-") in ("-", "", "N/A") and event["_dt"] < now:
            return True
    return False


def fetch_economic_calendar(actuals_only: bool = False) -> dict[str, list[dict[str, Any]]]:
    global _calendar_cache
    lw0, lw1, tw0, tw1, nw0, nw1 = get_week_bounds()

    if not actuals_only:
        ff_this = _fetch_ff("thisweek")
        ff_next = _fetch_ff("nextweek")
        inv_last = _fetch_investing_range(lw0, lw1)
        inv_this = _fetch_investing_range(tw0, tw1)
        inv_next = _fetch_investing_range(nw0, nw1)

        this_events = _merge_calendar_events(_bucket(ff_this, tw0, tw1), _bucket(inv_this, tw0, tw1))
        next_events = _merge_calendar_events(_bucket(ff_next, nw0, nw1), _bucket(inv_next, nw0, nw1))
        if not next_events:
            next_events = _merge_calendar_events(_bucket(ff_this, nw0, nw1), _bucket(inv_next, nw0, nw1))

        _calendar_cache = {
            "last": _bucket(inv_last, lw0, lw1),
            "this": this_events,
            "next": next_events,
        }
        return _calendar_cache

    needs_last = _has_pending_actuals("last")
    needs_this = _has_pending_actuals("this")
    if not (needs_last or needs_this):
        return _calendar_cache

    fresh = _fetch_investing_range(lw0, tw1)
    fresh_last = _bucket(fresh, lw0, lw1)
    fresh_this = _bucket(fresh, tw0, tw1)
    ff_this = _bucket(_fetch_ff("thisweek"), tw0, tw1)

    def overlay_actuals(base: list[dict[str, Any]], updates: list[dict[str, Any]]) -> list[dict[str, Any]]:
        update_map = {
            item["Event"][:35]: item
            for item in updates
            if item.get("Actual", "-") not in ("-", "", "N/A")
        }
        for event in base:
            key = event["Event"][:35]
            if event.get("Actual", "-") in ("-", "", "N/A") and key in update_map:
                event["Actual"] = update_map[key]["Actual"]
                event["Previous"] = update_map[key].get("Previous", event["Previous"])
        return base

    if needs_last and fresh_last:
        _calendar_cache["last"] = overlay_actuals(_calendar_cache["last"], fresh_last)
    if needs_this:
        merged = overlay_actuals(_calendar_cache["this"], fresh_this)
        _calendar_cache["this"] = overlay_actuals(merged, ff_this)
    return _calendar_cache


# ----------------------------------------------------------------------------
# OI / PCR
# ----------------------------------------------------------------------------
def _calc_oi(call_oi: dict[Any, Any], put_oi: dict[Any, Any], label: str, source: str) -> list[dict[str, Any]]:
    total_calls = sum(call_oi.values())
    total_puts = sum(put_oi.values())
    pcr = round(total_puts / total_calls, 2) if total_calls else 0

    all_strikes = sorted(set(call_oi) | set(put_oi))
    pain_map: dict[Any, float] = {}
    for strike in all_strikes:
        loss = sum(max(0, strike - key) * value for key, value in call_oi.items())
        loss += sum(max(0, key - strike) * value for key, value in put_oi.items())
        pain_map[strike] = loss
    max_pain = min(pain_map, key=pain_map.get) if pain_map else "N/A"
    bias = "Bullish" if pcr > 1.2 else "Bearish" if pcr < 0.8 else "Neutral"

    rows = [
        {
            "type": "summary",
            "label": f"{label} [{source}]",
            "total_call_oi": total_calls,
            "total_put_oi": total_puts,
            "pcr": pcr,
            "max_pain": max_pain,
            "sentiment": bias,
        }
    ]

    top_strikes = sorted(
        all_strikes,
        key=lambda strike: call_oi.get(strike, 0) + put_oi.get(strike, 0),
        reverse=True,
    )[:8]
    for strike in sorted(top_strikes):
        rows.append(
            {
                "type": "strike",
                "strike": strike,
                "call_oi": call_oi.get(strike, 0),
                "put_oi": put_oi.get(strike, 0),
                "pcr": round(put_oi.get(strike, 0) / max(call_oi.get(strike, 1), 1), 2),
                "max_pain": "MAX PAIN" if strike == max_pain else "",
            }
        )
    return rows


def _mcx_opstra(symbol: str) -> tuple[dict[Any, Any], dict[Any, Any]]:
    response = requests.get(
        f"https://opstra.definedge.com/api/openinterest/optionchain/{symbol}",
        headers={
            "User-Agent": "Mozilla/5.0",
            "Referer": "https://opstra.definedge.com",
            "Accept": "application/json",
        },
        timeout=12,
    )
    payload = response.json()
    records = payload.get("data", payload.get("records", []))
    call_oi: dict[Any, Any] = {}
    put_oi: dict[Any, Any] = {}
    for record in records:
        strike = record.get("strike_price", record.get("strikePrice", 0))
        call_oi[strike] = record.get("call_oi", record.get("CE", {}).get("openInterest", 0))
        put_oi[strike] = record.get("put_oi", record.get("PE", {}).get("openInterest", 0))
    if not call_oi and not put_oi:
        raise ValueError("Empty Opstra response")
    return call_oi, put_oi


def _mcx_nse(symbol: str) -> tuple[dict[Any, Any], dict[Any, Any]]:
    session = requests.Session()
    session.get("https://www.nseindia.com", headers=HEADERS, timeout=10)
    time.sleep(1)
    response = session.get(
        f"https://www.nseindia.com/api/option-chain-commodities?symbol={symbol}",
        headers={
            **HEADERS,
            "Referer": "https://www.nseindia.com/market-data/option-chain",
            "X-Requested-With": "XMLHttpRequest",
        },
        timeout=15,
    )
    response.raise_for_status()
    records = response.json().get("records", {}).get("data", [])
    if not records:
        raise ValueError("NSE commodity option chain empty")
    call_oi: dict[Any, Any] = {}
    put_oi: dict[Any, Any] = {}
    for record in records:
        strike = record.get("strikePrice", 0)
        if "CE" in record:
            call_oi[strike] = record["CE"].get("openInterest", 0)
        if "PE" in record:
            put_oi[strike] = record["PE"].get("openInterest", 0)
    return call_oi, put_oi


def fetch_mcx_oi() -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for nse_symbol, opstra_symbol, label in [
        ("GOLDM", "GOLD", "Gold"),
        ("SILVERM", "SILVER", "Silver"),
        ("COPPER", "COPPER", "Copper"),
    ]:
        completed = False
        for func, source, arg in [
            (_mcx_opstra, "Opstra", opstra_symbol),
            (_mcx_nse, "NSE", nse_symbol),
        ]:
            if completed:
                break
            try:
                call_oi, put_oi = func(arg)
                results += _calc_oi(call_oi, put_oi, label, source)
                completed = True
            except Exception:
                pass
        if not completed:
            results.append(
                {
                    "type": "summary",
                    "label": f"{label} [Unavailable]",
                    "total_call_oi": "N/A",
                    "total_put_oi": "N/A",
                    "pcr": "N/A",
                    "max_pain": "N/A",
                    "sentiment": "OI unavailable",
                }
            )
    return results


def fetch_comex_oi() -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for symbol, label in [
        ("GC=F", "COMEX Gold"),
        ("SI=F", "COMEX Silver"),
    ]:
        try:
            ticker = yf.Ticker(symbol)
            expiries = ticker.options
            if not expiries:
                raise ValueError(f"No options chain for {symbol}")
            chain = ticker.option_chain(expiries[0])
            if chain.calls.empty and chain.puts.empty:
                raise ValueError("Empty option chain")
            call_oi = dict(
                zip(
                    chain.calls["strike"].tolist(),
                    chain.calls["openInterest"].fillna(0).astype(int).tolist(),
                )
            )
            put_oi = dict(
                zip(
                    chain.puts["strike"].tolist(),
                    chain.puts["openInterest"].fillna(0).astype(int).tolist(),
                )
            )
            results += _calc_oi(call_oi, put_oi, label, f"yfinance/{expiries[0]}")
        except Exception as exc:
            results.append(
                {
                    "type": "summary",
                    "label": f"{label} [Unavailable]",
                    "total_call_oi": "N/A",
                    "total_put_oi": "N/A",
                    "pcr": "N/A",
                    "max_pain": "N/A",
                    "sentiment": str(exc)[:55],
                }
            )
    return results


def fetch_all_oi() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = [{"type": "header", "label": "MCX OI / PCR"}]
    rows += fetch_mcx_oi()
    rows += [{"type": "header", "label": "COMEX OI / PCR"}]
    rows += fetch_comex_oi()
    return rows


# ----------------------------------------------------------------------------
# Shared store: background threads write, the web server reads
# ----------------------------------------------------------------------------
MODULE_INTERVAL = {"prices": 5, "news": 10, "oi": 5, "fii": 60, "calendar": 300}
if HOSTED:
    MODULE_INTERVAL.update({"prices": 10, "news": 30, "oi": 120, "fii": 120, "calendar": 600})
MODULE_LABEL = {
    "prices": "PRICES",
    "news": "NEWS",
    "oi": "OI / PCR",
    "fii": "FII / DII",
    "calendar": "ECO CALENDAR",
}
TASK_MODULE = {
    "prices": "prices",
    "news": "news",
    "oi": "oi",
    "fii_attempt": "fii",
    "calendar": "calendar",
    "calendar_actuals": "calendar",
}


def clean(obj: Any) -> Any:
    """Turn pandas / numpy / datetime values into plain JSON-safe Python values."""
    if isinstance(obj, dict):
        return {str(k): clean(v) for k, v in obj.items() if k not in ("dt_obj", "_dt")}
    if isinstance(obj, (list, tuple)):
        return [clean(v) for v in obj]
    if obj is None or isinstance(obj, (str, bool)):
        return obj
    try:
        if pd.isna(obj):
            return None
    except (TypeError, ValueError):
        pass
    if isinstance(obj, (datetime, date)):
        return obj.isoformat()
    if hasattr(obj, "item"):
        try:
            obj = obj.item()
        except Exception:
            return str(obj)
    if isinstance(obj, float):
        return None if (math.isnan(obj) or math.isinf(obj)) else obj
    if isinstance(obj, int):
        return obj
    return str(obj)


class Store:
    def __init__(self, names: list[str]) -> None:
        self._lock = threading.Lock()
        self._s: dict[str, dict[str, Any]] = {
            name: {"data": None, "updated": None, "error": "", "version": 0} for name in names
        }

    def peek(self, name: str) -> Any:
        with self._lock:
            return self._s[name]["data"]

    def set(self, name: str, data: Any) -> None:
        with self._lock:
            state = self._s[name]
            first = state["data"] is None
            state.update(data=data, updated=time.time(), error="", version=state["version"] + 1)
        if first:
            log.info("%s | first data received", MODULE_LABEL[name])

    def fail(self, name: str, message: str) -> None:
        with self._lock:
            state = self._s[name]
            changed = state["error"] != message
            if changed:
                state.update(error=message, version=state["version"] + 1)
        if changed:
            log.warning("%s | refresh failed: %s", MODULE_LABEL[name], message)

    def brief(self, name: str) -> dict[str, Any]:
        with self._lock:
            state = self._s[name]
            return {"has_data": state["data"] is not None, "error": state["error"], "updated": state["updated"]}

    def view(self, name: str, since: int | None) -> dict[str, Any]:
        with self._lock:
            state = dict(self._s[name])
        out: dict[str, Any] = {
            "updated": state["updated"],
            "error": state["error"],
            "version": state["version"],
            "interval": MODULE_INTERVAL[name],
            "server_time": time.time(),
        }
        if since is not None and since == state["version"]:
            out["unchanged"] = True
        else:
            out["data"] = state["data"]
        return out


store = Store(list(MODULE_INTERVAL))


def prices_payload(df: pd.DataFrame | None) -> list[dict[str, Any]]:
    if df is None or df.empty:
        raise ValueError("Price watchlist is empty")
    if not pd.to_numeric(df["Price"], errors="coerce").notna().any():
        raise ValueError("No prices returned by the data sources")
    return clean(df.to_dict(orient="records"))


def news_payload(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not items:
        raise ValueError("No news headlines returned by the feeds")
    out = []
    for item in items:
        entry = {k: v for k, v in item.items() if k != "dt_obj"}
        entry["Iso"] = item["dt_obj"].isoformat() + "Z"  # dt_obj is naive UTC
        out.append(entry)
    return clean(out)


def fii_payload(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not rows or all(r.get("Category") == "Data unavailable" for r in rows):
        raise ValueError("NSE and MoneyControl both unreachable")
    out = []
    for r in rows:
        row = dict(r)
        for key in ("Buy (Cr)", "Sell (Cr)", "Net (Cr)"):
            number = to_float(r.get(key))
            row[key] = number if number is not None else r.get(key)
        out.append(row)
    return clean(out)


def calendar_payload(buckets: dict[str, list[dict[str, Any]]]) -> dict[str, Any]:
    if not any(buckets.get(k) for k in ("last", "this", "next")):
        raise ValueError("Calendar sources returned no events")
    return clean({k: list(buckets.get(k, [])) for k in ("last", "this", "next")})


def _oi_has_real_data(rows: list[dict[str, Any]] | None) -> bool:
    return any(
        r.get("type") == "summary" and r.get("pcr") not in ("N/A", None) for r in (rows or [])
    )


def oi_payload(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    # If every source fails this round but we already hold real OI numbers,
    # keep showing those instead of replacing them with N/A cards.
    if not _oi_has_real_data(rows) and _oi_has_real_data(store.peek("oi")):
        raise ValueError("OI sources unreachable")
    return clean(rows)


def submit_task(executor: ThreadPoolExecutor, task: str):
    if task == "prices":
        return executor.submit(fetch_market_watchlist)
    if task == "news":
        return executor.submit(fetch_commodity_news)
    if task == "oi":
        return executor.submit(fetch_all_oi)
    if task == "fii_attempt":
        return executor.submit(fetch_fii_dii_flow)
    if task == "calendar":
        return executor.submit(fetch_economic_calendar, False)
    return executor.submit(fetch_economic_calendar, True)


def finish_task(task: str, future) -> None:
    module = TASK_MODULE[task]
    try:
        result = future.result()
        if module == "prices":
            payload = prices_payload(result)
        elif module == "news":
            payload = news_payload(result)
        elif module == "oi":
            payload = oi_payload(result)
        elif module == "fii":
            payload = fii_payload(result)
        else:
            payload = calendar_payload(result)
        store.set(module, payload)
    except Exception as exc:
        store.fail(module, (str(exc) or type(exc).__name__)[:200])
    finally:
        mark_done(task)
        if task == "calendar":
            mark_done("calendar_actuals")


TASK_INFO: dict[str, dict[str, Any]] = {
    task: {"running_since": None, "last_duration": None, "stuck": 0} for task in TASK_MODULE
}


def scheduler_loop(stop: threading.Event) -> None:
    force_all_modules_due()
    pending: dict[str, tuple[Any, float]] = {}  # task -> (future, start time)
    stuck: dict[str, list[Any]] = {task: [] for task in TASK_MODULE}  # given up on, still running
    executor = ThreadPoolExecutor(max_workers=24)

    def start(task: str) -> None:
        now = time.time()
        pending[task] = (submit_task(executor, task), now)
        TASK_INFO[task]["running_since"] = now

    while not stop.is_set():
        try:
            now = time.time()

            # A request we gave up on may still finish later: use its data if it does.
            for task in stuck:
                for future in [f for f in stuck[task] if f.done()]:
                    stuck[task].remove(future)
                    finish_task(task, future)
                TASK_INFO[task]["stuck"] = len(stuck[task])

            def can_start(task: str) -> bool:
                return task not in pending and len(stuck[task]) < MAX_STUCK_PER_TASK

            for task in ("prices", "news", "oi", "fii_attempt"):
                if is_due(task):
                    if can_start(task):
                        start(task)
                    elif task not in pending:
                        store.fail(TASK_MODULE[task], "Data source is not responding - waiting for earlier requests to finish")

            calendar_busy = "calendar" in pending or "calendar_actuals" in pending
            if not calendar_busy:
                if is_due("calendar") and can_start("calendar"):
                    start("calendar")
                elif is_due("calendar_actuals") and can_start("calendar_actuals"):
                    start("calendar_actuals")

            for task in list(pending):
                future, started = pending[task]
                if future.done():
                    pending.pop(task)
                    TASK_INFO[task].update(running_since=None, last_duration=round(now - started, 1))
                    finish_task(task, future)
                elif now - started > TASK_TIMEOUT[task]:
                    pending.pop(task)
                    stuck[task].append(future)
                    TASK_INFO[task].update(running_since=None, stuck=len(stuck[task]))
                    store.fail(
                        TASK_MODULE[task],
                        f"Timed out after {int(now - started)}s - the data source is not responding",
                    )
                    mark_done(task)
                    if task == "calendar":
                        mark_done("calendar_actuals")
        except RuntimeError as exc:
            if "shutdown" in str(exc):
                break  # the program is exiting; nothing more to refresh
            log.exception("scheduler error")
        except Exception:
            log.exception("scheduler error")
        stop.wait(0.5)


# ----------------------------------------------------------------------------
# Web server
# ----------------------------------------------------------------------------
app = Flask(__name__)

# Optional login for a public website. Set TERMINAL_PASSWORD (and optionally TERMINAL_USER)
# in the host's environment settings. If TERMINAL_PASSWORD is empty the site is open to anyone,
# which is what you want when running it on your own PC.
AUTH_USER = os.environ.get("TERMINAL_USER", "friend")
AUTH_PASS = os.environ.get("TERMINAL_PASSWORD", "")


@app.before_request
def require_login():
    if not AUTH_PASS or request.path == "/healthz":
        return None
    auth = request.authorization
    if auth and auth.username is not None and auth.password is not None:
        user_ok = hmac.compare_digest(auth.username.encode(), AUTH_USER.encode())
        pass_ok = hmac.compare_digest(auth.password.encode(), AUTH_PASS.encode())
        if user_ok and pass_ok:
            return None
    return Response(
        "Login required.",
        status=401,
        headers={"WWW-Authenticate": 'Basic realm="Kotak Desk market terminal"'},
    )


@app.route("/healthz")
def healthz() -> Response:
    return Response("ok", mimetype="text/plain")


_bg: dict[str, Any] = {"stop": None}


def start_background_refresh() -> None:
    """Start the data-refresh thread once. Used by wsgi.py when a web host runs the app."""
    if _bg["stop"] is not None:
        return
    _bg["stop"] = threading.Event()
    threading.Thread(target=scheduler_loop, args=(_bg["stop"],), daemon=True, name="scheduler").start()
    log.info("background refresh started")


@app.route("/")
def index() -> Response:
    resp = Response(INDEX_HTML, mimetype="text/html")
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.route("/api/status")
def api_status():
    """What the server is doing right now. The page shows this while it waits for data."""
    now = time.time()
    tasks = {}
    for task, info in TASK_INFO.items():
        since = info["running_since"]
        tasks[task] = {
            "running_for": round(now - since, 1) if since else None,
            "last_duration": info["last_duration"],
            "stuck": info["stuck"],
        }
    peak_mb = None
    if sys.platform == "linux":
        try:
            import resource

            peak_mb = round(resource.getrusage(resource.RUSAGE_SELF).ru_maxrss / 1024, 1)
        except Exception:
            pass
    refresher = next((t for t in threading.enumerate() if t.name == "scheduler"), None)
    resp = jsonify(
        uptime=round(now - STARTED_AT, 1),
        scheduler_alive=bool(refresher and refresher.is_alive()),
        hosted=HOSTED,
        peak_memory_mb=peak_mb,
        tasks=tasks,
        modules={name: store.brief(name) for name in MODULE_INTERVAL},
        versions={"python": sys.version.split()[0], "pandas": pd.__version__, "yfinance": getattr(yf, "__version__", None)},
    )
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.route("/api/<name>")
def api_module(name: str):
    if name not in MODULE_INTERVAL:
        return jsonify(error="unknown module"), 404
    since = request.args.get("v", type=int)
    resp = jsonify(store.view(name, since))
    resp.headers["Cache-Control"] = "no-store"
    return resp


INDEX_HTML = r"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Kotak Desk - Market Terminal</title>
<style>
:root{
  --bg:#000; --panel:#0e0e10; --panel2:#151518; --line:#232327; --line2:#34343a;
  --txt:#ebe8e2; --dim:#8c8c96; --amber:#ffb000; --amber-d:#9a6b00;
  --up:#2bd67b; --up-bg:rgba(43,214,123,.13);
  --dn:#ff5c5c; --dn-bg:rgba(255,92,92,.14);
  --warn-bg:rgba(255,176,0,.12); --brk:#d93636; --link:#6db3ff;
  --mono:"Cascadia Mono","JetBrains Mono",Consolas,"SF Mono",Menlo,monospace;
  --sans:"Segoe UI",system-ui,-apple-system,Roboto,Arial,sans-serif;
}
*{box-sizing:border-box}
html,body{height:100%;margin:0}
body{background:var(--bg);color:var(--txt);font:13px/1.4 var(--sans);display:flex;flex-direction:column;overflow:hidden}
a{color:var(--link);text-decoration:none}
a:hover{text-decoration:underline}
button{font-family:inherit}
button:focus-visible,input:focus-visible,a:focus-visible{outline:2px solid var(--amber);outline-offset:-2px}

.top{display:flex;align-items:center;gap:18px;padding:8px 14px;border-bottom:2px solid var(--amber);flex:none;flex-wrap:wrap}
.brand{display:flex;align-items:baseline;gap:10px}
.logo{font:700 18px var(--mono);letter-spacing:.12em;color:var(--amber)}
.sub{font:12px var(--mono);color:var(--dim)}
.chips{display:flex;gap:6px;flex-wrap:wrap;margin-left:auto}
.chip{font:11px var(--mono);padding:3px 8px;border:1px solid var(--line2);border-radius:3px;color:var(--dim);display:flex;gap:6px;align-items:center;white-space:nowrap}
.chip b{color:var(--txt);font-weight:600}
.dot{width:7px;height:7px;border-radius:50%;background:var(--dim)}
.chip.live .dot{background:var(--up);box-shadow:0 0 6px var(--up)}
.chip.stale .dot{background:var(--amber)}
.chip.nodata .dot{background:var(--dn)}
.clock{font:600 14px var(--mono);min-width:158px;text-align:right}

.banner{padding:7px 14px;font:600 13px var(--mono);display:flex;gap:12px;align-items:center;flex:none}
.banner[hidden]{display:none}
#banner{background:var(--brk);color:#fff}
#offline{background:var(--amber);color:#000}
.banner .tag{background:#fff;color:var(--brk);padding:1px 8px;border-radius:2px;font-size:11px}

.tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:1px;background:var(--line);border-bottom:1px solid var(--line);flex:none}
.tile{background:var(--panel);padding:8px 12px}
.tile .l{font:11px var(--mono);color:var(--dim)}
.tile .p{font:600 20px var(--mono);margin-top:2px;font-variant-numeric:tabular-nums}
.tile .c{font:12px var(--mono)}
.tile.pos .c,.tile.pos .p{color:var(--up)}
.tile.neg .c,.tile.neg .p{color:var(--dn)}
.tile.flat .c{color:var(--dim)}

.tabs{display:flex;border-bottom:1px solid var(--line);flex:none;overflow-x:auto}
.tab-btn{appearance:none;background:none;border:0;color:var(--dim);font:600 12px var(--mono);padding:10px 16px;cursor:pointer;border-bottom:2px solid transparent;white-space:nowrap}
.tab-btn .k{color:var(--amber-d);margin-right:7px}
.tab-btn:hover{color:var(--txt)}
.tab-btn.active{color:var(--amber);border-bottom-color:var(--amber);background:var(--panel)}

main{flex:1;min-height:0;position:relative}
.pane{display:none;flex-direction:column;height:100%}
.pane.active{display:flex}
.scroll{flex:1;overflow:auto;min-height:0}
.toolbar{display:flex;gap:12px;align-items:center;padding:8px 12px;border-bottom:1px solid var(--line);background:var(--panel);flex:none;flex-wrap:wrap}
.toolbar input[type=search]{background:#000;border:1px solid var(--line2);color:var(--txt);padding:5px 9px;font:12px var(--mono);min-width:240px;border-radius:3px}
.seg{display:flex;border:1px solid var(--line2);border-radius:3px;overflow:hidden}
.seg button{background:#000;color:var(--dim);border:0;padding:5px 12px;font:600 11px var(--mono);cursor:pointer}
.seg button.on{background:var(--amber);color:#000}
.meta{margin-left:auto;font:11px var(--mono);color:var(--dim)}

table{border-collapse:collapse;width:100%}
th,td{padding:7px 10px;text-align:center;border-bottom:1px solid var(--line);border-right:1px solid #18181b;white-space:nowrap}
th{position:sticky;top:0;background:var(--panel2);color:var(--amber);font:600 11px var(--mono);z-index:1;border-bottom:1px solid var(--amber-d)}
td.num{font-family:var(--mono);font-variant-numeric:tabular-nums}
td.hl{white-space:normal;min-width:340px;max-width:760px}
tbody tr:hover td:not(.pos):not(.neg):not(.flat){background:#161619}
td.pos{background:var(--up-bg);color:var(--up)}
td.neg{background:var(--dn-bg);color:var(--dn)}
td.flat{background:var(--warn-bg)}
tr.today td:first-child{box-shadow:inset 3px 0 0 var(--amber)}
tr.fresh td{animation:fresh 4s ease-out}
@keyframes fresh{from{background:rgba(255,176,0,.28)}}
@keyframes fup{from{background:rgba(43,214,123,.6)}}
@keyframes fdn{from{background:rgba(255,92,92,.6)}}
td.fup{animation:fup 1.1s ease-out}
td.fdn{animation:fdn 1.1s ease-out}
@media (prefers-reduced-motion:reduce){td.fup,td.fdn,tr.fresh td{animation:none}}

.badge{display:inline-block;padding:2px 8px;border-radius:2px;font:700 10px var(--mono)}
.badge.brk{background:var(--brk);color:#fff}
.badge.high{background:var(--dn-bg);color:var(--dn);border:1px solid rgba(255,92,92,.4)}
.badge.med{background:var(--warn-bg);color:var(--amber);border:1px solid rgba(255,176,0,.35)}
.bad{color:var(--dn)}

.foot{flex:none;padding:5px 14px;border-top:1px solid var(--line);font:11px var(--mono);color:var(--dim);background:var(--panel)}
.empty{padding:70px 20px;text-align:center;color:var(--dim);font:13px var(--mono)}
.empty b{color:var(--dn);display:block;font-size:14px;margin-bottom:8px}
.empty small{display:block;margin-top:12px;font-size:12px}

.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(240px,1fr));gap:12px;padding:12px}
.card{background:var(--panel);border:1px solid var(--line);border-radius:4px;padding:12px 14px}
.card .cl{font:600 13px var(--mono);color:var(--amber)}
.card .cd{font:11px var(--mono);color:var(--dim);margin-bottom:6px}
.card .cv{font:600 26px var(--mono);font-variant-numeric:tabular-nums}
.card .cv small{font-size:12px;color:var(--dim);margin-left:6px}
.card.pos .cv{color:var(--up)}
.card.neg .cv{color:var(--dn)}
.card .cs{font:12px var(--mono);color:var(--dim);margin-top:4px}

.oi-sec{padding:6px 12px 14px}
.oi-sec h2{font:700 13px var(--mono);color:var(--amber);margin:14px 0 8px;padding-bottom:6px;border-bottom:1px solid var(--line2)}
.oi-grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(420px,1fr));gap:12px}
.oi-card{background:var(--panel);border:1px solid var(--line);border-radius:4px;overflow:hidden}
.oi-card .oh{padding:8px 12px;font:600 13px var(--mono);background:var(--panel2);border-bottom:1px solid var(--line)}
.oi-card .oh.na{color:var(--dim)}
.oi-stats{display:grid;grid-template-columns:repeat(3,1fr);gap:1px;background:var(--line)}
.oi-stats div{background:var(--panel);padding:8px 10px;text-align:center}
.oi-stats .sl{font:11px var(--mono);color:var(--dim)}
.oi-stats .sv{font:600 15px var(--mono);margin-top:2px;font-variant-numeric:tabular-nums}
.oi-stats .sv.pos{color:var(--up)}
.oi-stats .sv.neg{color:var(--dn)}
.oi-stats .sv.flat{color:var(--amber)}
.oi-note{padding:8px 12px;font:12px var(--mono);color:var(--dim);text-align:center;border-top:1px solid var(--line)}
.oi-card table th{position:static}
.oi-card tr.mp td{background:var(--warn-bg)}

@media (max-width:700px){
  .tiles{display:flex;overflow-x:auto}
  .tile{min-width:150px}
  .oi-grid{grid-template-columns:1fr}
  .chips{margin-left:0}
}
</style>
</head>
<body>
<header class="top">
  <div class="brand"><span class="logo">KOTAK DESK</span><span class="sub">Market terminal</span></div>
  <div class="chips" id="chips" aria-label="Data feed status"></div>
  <div class="clock" id="clock"></div>
</header>
<div class="banner" id="banner" hidden></div>
<div class="banner" id="offline" hidden>Lost connection to the terminal server - retrying. If this stays, the server may be restarting or stopped.</div>
<section class="tiles" id="tiles" aria-label="Key prices"></section>
<nav class="tabs" id="tabs">
  <button class="tab-btn active" data-tab="prices"><span class="k">1</span>Live prices</button>
  <button class="tab-btn" data-tab="news"><span class="k">2</span>News</button>
  <button class="tab-btn" data-tab="fii"><span class="k">3</span>FII / DII flow</button>
  <button class="tab-btn" data-tab="calendar"><span class="k">4</span>Eco calendar</button>
  <button class="tab-btn" data-tab="oi"><span class="k">5</span>MCX / COMEX OI &amp; PCR</button>
</nav>
<main>
  <section class="pane active" id="pane-prices"><div class="scroll" id="prices-view"></div></section>

  <section class="pane" id="pane-news">
    <div class="toolbar">
      <input type="search" id="news-q" placeholder="Filter headlines or source" aria-label="Filter headlines">
      <div class="seg" id="news-seg">
        <button class="on" data-v="all">All</button>
        <button data-v="brk">Breaking only</button>
      </div>
      <span class="meta" id="news-meta"></span>
    </div>
    <div class="scroll" id="news-view"></div>
  </section>

  <section class="pane" id="pane-fii"><div class="scroll" id="fii-view"></div></section>

  <section class="pane" id="pane-calendar">
    <div class="toolbar">
      <div class="seg" id="cal-week">
        <button data-w="last">Last week</button>
        <button class="on" data-w="this">This week</button>
        <button data-w="next">Next week</button>
      </div>
      <div class="seg" id="cal-impact">
        <button class="on" data-i="all">All impact</button>
        <button data-i="high">High only</button>
      </div>
      <span class="meta" id="cal-meta"></span>
    </div>
    <div class="scroll" id="calendar-view"></div>
  </section>

  <section class="pane" id="pane-oi"><div class="scroll" id="oi-view"></div></section>
</main>
<footer class="foot">Data comes from public sources and can be delayed, incomplete or wrong. For information only, not investment advice.</footer>

<script>
(() => {
'use strict';
const $ = (s, r = document) => r.querySelector(s);
const $$ = (s, r = document) => Array.from(r.querySelectorAll(s));
const pad = n => String(n).padStart(2, '0');
const esc = s => String(s == null ? '' : s).replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c]));
const isNum = v => typeof v === 'number' && isFinite(v);
const fmt = (v, d = 2) => isNum(v) ? v.toFixed(d) : (v == null || v === '' ? '-' : String(v));
const fmtChg = v => isNum(v) ? (v > 0 ? '+' : '') + v.toFixed(2) : '-';
const signCls = v => !isNum(v) ? 'flat' : v > 0 ? 'pos' : v < 0 ? 'neg' : 'flat';
const safeUrl = u => /^https?:\/\//i.test(u || '') ? u : '';
const hhmmss = ts => { const d = new Date(ts * 1000); return pad(d.getHours()) + ':' + pad(d.getMinutes()) + ':' + pad(d.getSeconds()); };
const fmtLocal = iso => { const d = new Date(iso); if (isNaN(d)) return ''; return pad(d.getDate()) + '-' + pad(d.getMonth() + 1) + '-' + d.getFullYear() + ' ' + pad(d.getHours()) + ':' + pad(d.getMinutes()); };

const MOD = { prices: 'PRICES', news: 'NEWS', fii: 'FII/DII', calendar: 'CALENDAR', oi: 'OI/PCR' };
const TABS = ['prices', 'news', 'fii', 'calendar', 'oi'];
const S = {};
let skew = 0, failures = 0;

/* What the server is doing, shown while a pane has no data yet (filled in by pollStatus). */
let statusInfo = null, statusFailed = false;
const TASKS_OF = { prices: ['prices'], news: ['news'], fii: ['fii_attempt'], calendar: ['calendar', 'calendar_actuals'], oi: ['oi'] };
const fmtDur = s => s < 90 ? Math.round(s) + 's' : Math.round(s / 60) + ' min';
function waitDetail(key) {
  if (statusFailed) return '<small>The server is not answering yet. It may be starting up or restarting - this page keeps trying.</small>';
  if (!statusInfo) return '';
  const bits = ['Server running for ' + fmtDur(statusInfo.uptime) + '.'];
  if (!statusInfo.scheduler_alive) {
    bits.push('The data refresh process is not running.');
  } else {
    const running = (TASKS_OF[key] || []).map(k => statusInfo.tasks[k]).find(t => t && t.running_for != null);
    bits.push(running ? 'This data has been loading for ' + fmtDur(running.running_for) + '.' : 'This data is queued for its first load.');
  }
  return '<small>' + esc(bits.join(' ')) + '</small>';
}
function emptyMsg(m, key) {
  if (m && m.error) return '<div class="empty"><b>[!] DATA TEMPORARILY UNAVAILABLE</b>' + esc(m.error) + '<br>Retrying automatically.</div>';
  return '<div class="empty">Waiting for the first data...' + waitDetail(key) + '</div>';
}
function staleNote(m) {
  return (m && m.data && m.error) ? '<div class="empty" style="padding:8px"><span class="bad">Showing last good data - latest refresh failed: ' + esc(m.error) + '</span></div>' : '';
}

/* ---------- Prices ---------- */
const lastPrice = {};
const TILE_KEYS = ['COMEX Gold Near', 'COMEX Silver Near', 'MCX Gold Near', 'MCX Silver Near', 'LME Copper Near', 'USDINR Near', 'Dollar Index', 'US 10Y Yield'];

function renderTiles(rows) {
  const by = {};
  (rows || []).forEach(r => { by[r.Watchlist] = r; });
  $('#tiles').innerHTML = TILE_KEYS.map(k => {
    const r = by[k];
    const p = r ? r.Price : null, c = r ? r['Chg %'] : null;
    return '<div class="tile ' + signCls(c) + '"><div class="l">' + esc(k) + '</div><div class="p">' + fmt(p) + '</div><div class="c">' + (isNum(c) ? (c > 0 ? '▲ ' : c < 0 ? '▼ ' : '') + fmtChg(c) + '%' : '-') + '</div></div>';
  }).join('');
}

function renderPrices() {
  const m = S.prices, view = $('#prices-view');
  if (!m || !m.data) { view.innerHTML = emptyMsg(m, 'prices'); renderTiles([]); return; }
  const rows = m.data;
  let html = staleNote(m) + '<table><thead><tr><th>Watchlist</th><th>Ticker</th><th>Description</th><th>Price</th><th>Chg %</th><th>Exchange</th><th>Timestamp</th><th>Status</th></tr></thead><tbody>';
  rows.forEach(r => {
    const p = r.Price, c = r['Chg %'], cls = signCls(c);
    const prev = lastPrice[r.Watchlist];
    let fl = '';
    if (isNum(prev) && isNum(p) && p !== prev) fl = p > prev ? ' fup' : ' fdn';
    html += '<tr><td>' + esc(r.Watchlist) + '</td><td class="num">' + esc(r.Ticker) + '</td><td>' + esc(r.Description) + '</td>'
      + '<td class="num ' + cls + fl + '">' + fmt(p) + '</td><td class="num ' + cls + '">' + fmtChg(c) + '</td>'
      + '<td>' + esc(r.Exchange) + '</td><td class="num">' + esc(r.Timestamp) + '</td>'
      + '<td class="' + (r.Status === 'OK' ? '' : 'bad') + '">' + esc(r.Status) + '</td></tr>';
    if (isNum(p)) lastPrice[r.Watchlist] = p;
  });
  view.innerHTML = html + '</tbody></table>';
  renderTiles(rows);
}

/* ---------- News ---------- */
let newsQ = '', newsBrk = false, newsInit = false, freshKeys = new Set();
const seenNews = new Set();
let bannerTimer = null;

function showBanner(text) {
  const b = $('#banner');
  b.innerHTML = '<span class="tag">BREAKING</span><span>' + esc(text) + '</span>';
  b.hidden = false;
  clearTimeout(bannerTimer);
  bannerTimer = setTimeout(() => { b.hidden = true; }, 30000);
}

function onNews() {
  const m = S.news;
  freshKeys = new Set();
  if (m && m.data) {
    const fresh = [];
    m.data.forEach(n => {
      const k = (n.Headline || '').slice(0, 120);
      if (!seenNews.has(k)) { seenNews.add(k); if (newsInit) { fresh.push(n); freshKeys.add(k); } }
    });
    newsInit = true;
    const brk = fresh.filter(n => n.Breaking);
    if (brk.length) showBanner(brk[0].Headline);
  }
  renderNews();
}

function renderNews() {
  const m = S.news, view = $('#news-view');
  if (!m || !m.data) { view.innerHTML = emptyMsg(m, 'news'); $('#news-meta').textContent = ''; return; }
  const q = newsQ.trim().toLowerCase();
  const rows = m.data.filter(n => (!newsBrk || n.Breaking) && (!q || ((n.Headline || '') + ' ' + (n.Source || '')).toLowerCase().includes(q)));
  $('#news-meta').textContent = rows.length + ' of ' + m.data.length + ' headlines';
  let html = staleNote(m) + '<table><thead><tr><th>Time</th><th>Source</th><th>Breaking</th><th>Headline</th><th>Link</th></tr></thead><tbody>';
  rows.forEach(n => {
    const k = (n.Headline || '').slice(0, 120);
    const url = safeUrl(n.Link);
    html += '<tr' + (freshKeys.has(k) ? ' class="fresh"' : '') + '><td class="num">' + esc(fmtLocal(n.Iso) || n.Time) + '</td><td>' + esc(n.Source) + '</td>'
      + '<td>' + (n.Breaking ? '<span class="badge brk">BREAKING</span>' : '') + '</td>'
      + '<td class="hl">' + esc(n.Headline) + '</td>'
      + '<td>' + (url ? '<a href="' + esc(url) + '" target="_blank" rel="noopener noreferrer">Read more</a>' : 'No link') + '</td></tr>';
  });
  view.innerHTML = html + '</tbody></table>';
}

/* ---------- FII / DII ---------- */
function renderFii() {
  const m = S.fii, view = $('#fii-view');
  if (!m || !m.data) { view.innerHTML = emptyMsg(m, 'fii'); return; }
  const rows = m.data;
  const cards = rows.filter(r => isNum(r['Net (Cr)'])).map(r => {
    const n = r['Net (Cr)'];
    return '<div class="card ' + signCls(n) + '"><div class="cl">' + esc(r.Category) + '</div><div class="cd">' + esc(r.Date) + '</div>'
      + '<div class="cv">' + fmtChg(n) + '<small>Cr net</small></div>'
      + '<div class="cs">Buy ' + fmt(r['Buy (Cr)']) + ' / Sell ' + fmt(r['Sell (Cr)']) + '</div></div>';
  }).join('');
  const upd = m.updated ? hhmmss(m.updated) : '-';
  let html = staleNote(m) + (cards ? '<div class="cards">' + cards + '</div>' : '')
    + '<table><thead><tr><th>Date</th><th>Category</th><th>Buy value (Cr)</th><th>Sell value (Cr)</th><th>Net value (Cr)</th><th>Updated</th></tr></thead><tbody>';
  rows.forEach(r => {
    html += '<tr><td>' + esc(r.Date) + '</td><td>' + esc(r.Category) + '</td><td class="num">' + fmt(r['Buy (Cr)']) + '</td><td class="num">' + fmt(r['Sell (Cr)']) + '</td>'
      + '<td class="num ' + (isNum(r['Net (Cr)']) ? signCls(r['Net (Cr)']) : 'flat') + '">' + (isNum(r['Net (Cr)']) ? fmt(r['Net (Cr)']) : esc(r['Net (Cr)'])) + '</td><td class="num">' + upd + '</td></tr>';
  });
  view.innerHTML = html + '</tbody></table>';
}

/* ---------- Calendar ---------- */
let calWeek = 'this', calImpact = 'all';
const num = v => { if (v == null) return NaN; const s = String(v).replace(/[%,KMBT\s]/gi, ''); if (s === '' || s === '-') return NaN; return parseFloat(s); };
function renderCalendar() {
  const m = S.calendar, view = $('#calendar-view');
  if (!m || !m.data) { view.innerHTML = emptyMsg(m, 'calendar'); $('#cal-meta').textContent = ''; return; }
  const t = new Date(), today = pad(t.getDate()) + '-' + pad(t.getMonth() + 1) + '-' + t.getFullYear();
  const all = m.data[calWeek] || [];
  const rows = all.filter(e => calImpact === 'all' || e.Impact === 'HIGH');
  $('#cal-meta').textContent = rows.length + ' events';
  if (!rows.length) { view.innerHTML = staleNote(m) + '<div class="empty">No events for this selection.</div>'; return; }
  let html = staleNote(m) + '<table><thead><tr><th>Date</th><th>Time</th><th>Country</th><th>Event</th><th>Impact</th><th>Previous</th><th>Forecast</th><th>Actual</th><th>Link</th></tr></thead><tbody>';
  rows.forEach(e => {
    let aCls = '';
    const a = num(e.Actual), f = num(e.Forecast);
    if (!isNaN(a) && !isNaN(f)) aCls = a > f ? 'pos' : a < f ? 'neg' : '';
    const url = safeUrl(e.Link);
    html += '<tr' + (e.Date === today ? ' class="today"' : '') + '><td class="num">' + esc(e.Date) + '</td><td class="num">' + esc(e.Time) + '</td><td>' + esc(e.Country) + '</td>'
      + '<td class="hl">' + esc(e.Event) + '</td>'
      + '<td><span class="badge ' + (e.Impact === 'HIGH' ? 'high' : 'med') + '">' + esc(e.Impact) + '</span></td>'
      + '<td class="num">' + esc(e.Previous) + '</td><td class="num">' + esc(e.Forecast) + '</td><td class="num ' + aCls + '">' + esc(e.Actual) + '</td>'
      + '<td>' + (url ? '<a href="' + esc(url) + '" target="_blank" rel="noopener noreferrer">Link</a>' : '') + '</td></tr>';
  });
  view.innerHTML = html + '</tbody></table>';
}

/* ---------- OI / PCR ---------- */
function renderOi() {
  const m = S.oi, view = $('#oi-view');
  if (!m || !m.data) { view.innerHTML = emptyMsg(m, 'oi'); return; }
  const sections = []; let sec = null, card = null;
  m.data.forEach(it => {
    if (it.type === 'header') { sec = { label: it.label, cards: [] }; sections.push(sec); card = null; }
    else if (it.type === 'summary') { if (!sec) { sec = { label: 'OI / PCR', cards: [] }; sections.push(sec); } card = Object.assign({ strikes: [] }, it); sec.cards.push(card); }
    else if (it.type === 'strike' && card) { card.strikes.push(it); }
  });
  const upd = m.updated ? hhmmss(m.updated) : '-';
  let html = staleNote(m);
  sections.forEach(s => {
    html += '<div class="oi-sec"><h2>' + esc(s.label) + '</h2><div class="oi-grid">';
    s.cards.forEach(c => {
      const na = !isNum(c.pcr);
      const pcrCls = na ? 'flat' : c.pcr > 1.2 ? 'pos' : c.pcr < 0.8 ? 'neg' : 'flat';
      html += '<div class="oi-card"><div class="oh' + (na ? ' na' : '') + '">' + esc(c.label) + '</div><div class="oi-stats">'
        + '<div><div class="sl">Total call OI</div><div class="sv">' + esc(c.total_call_oi) + '</div></div>'
        + '<div><div class="sl">Total put OI</div><div class="sv">' + esc(c.total_put_oi) + '</div></div>'
        + '<div><div class="sl">PCR</div><div class="sv ' + pcrCls + '">' + esc(na ? c.pcr : c.pcr.toFixed(2)) + '</div></div>'
        + '<div><div class="sl">Max pain</div><div class="sv">' + esc(c.max_pain) + '</div></div>'
        + '<div><div class="sl">Bias</div><div class="sv">' + esc(c.sentiment) + '</div></div>'
        + '<div><div class="sl">Updated</div><div class="sv">' + upd + '</div></div></div>';
      if (c.strikes.length) {
        html += '<table><thead><tr><th>Strike</th><th>Call OI</th><th>Put OI</th><th>Strike PCR</th><th>Note</th></tr></thead><tbody>';
        c.strikes.forEach(k => {
          html += '<tr' + (k.max_pain ? ' class="mp"' : '') + '><td class="num">' + esc(k.strike) + '</td><td class="num">' + esc(k.call_oi) + '</td><td class="num">' + esc(k.put_oi) + '</td><td class="num">' + fmt(k.pcr) + '</td><td>' + esc(k.max_pain) + '</td></tr>';
        });
        html += '</tbody></table>';
      } else if (na) {
        html += '<div class="oi-note">No strike data - source did not return an option chain.</div>';
      }
      html += '</div>';
    });
    html += '</div></div>';
  });
  view.innerHTML = html;
}

/* ---------- Status chips, clock ---------- */
function renderChips() {
  const now = Date.now() / 1000 + skew;
  $('#chips').innerHTML = Object.keys(MOD).map(k => {
    const m = S[k];
    let cls = 'nodata', txt = '--';
    if (m && m.updated) {
      const age = Math.max(0, Math.round(now - m.updated));
      cls = (age > (m.interval || 10) * 3 + 5 || m.error) ? 'stale' : 'live';
      txt = age < 100 ? age + 's' : Math.round(age / 60) + 'm';
    }
    return '<div class="chip ' + cls + '" title="' + esc((m && m.error) || '') + '"><span class="dot"></span>' + MOD[k] + ' <b>' + txt + '</b></div>';
  }).join('');
}
function tickClock() {
  const d = new Date();
  $('#clock').textContent = pad(d.getDate()) + '-' + pad(d.getMonth() + 1) + '-' + d.getFullYear() + ' ' + pad(d.getHours()) + ':' + pad(d.getMinutes()) + ':' + pad(d.getSeconds());
  renderChips();
}

/* ---------- Polling ---------- */
const RENDER = { prices: renderPrices, news: onNews, fii: renderFii, calendar: renderCalendar, oi: renderOi };
const RERENDER = { prices: renderPrices, news: renderNews, fii: renderFii, calendar: renderCalendar, oi: renderOi };

/* A request that never gets an answer counts as a failure, so the offline banner can appear. */
async function getJson(url, ms) {
  const ctl = new AbortController();
  const timer = setTimeout(() => ctl.abort(), ms);
  try {
    const r = await fetch(url, { cache: 'no-store', signal: ctl.signal });
    if (!r.ok) throw new Error('HTTP ' + r.status);
    return await r.json();
  } finally {
    clearTimeout(timer);
  }
}

const isWaiting = k => { const m = S[k]; return !m || (!m.data && !m.error); };
async function pollStatus() {
  if (Object.keys(MOD).some(isWaiting)) {
    try { statusInfo = await getJson('/api/status', 10000); statusFailed = false; } catch (e) { statusFailed = true; }
    Object.keys(MOD).filter(isWaiting).forEach(k => RERENDER[k]());
  }
  setTimeout(pollStatus, 3000);
}

async function poll(name) {
  try {
    const prev = S[name];
    const j = await getJson('/api/' + name + '?v=' + (prev ? prev.version : -1), 20000);
    skew = j.server_time - Date.now() / 1000;
    failures = 0;
    $('#offline').hidden = true;
    S[name] = { updated: j.updated, error: j.error, interval: j.interval, version: j.version, data: j.unchanged ? (prev ? prev.data : null) : j.data };
    if (!j.unchanged || !prev) RENDER[name]();
  } catch (e) {
    failures++;
    if (failures >= 3) $('#offline').hidden = false;
  } finally {
    setTimeout(() => poll(name), 2000);
  }
}

/* ---------- Tabs and controls ---------- */
function setTab(t) {
  if (TABS.indexOf(t) < 0) t = 'prices';
  TABS.forEach(x => {
    $('#pane-' + x).classList.toggle('active', x === t);
    $('[data-tab="' + x + '"]').classList.toggle('active', x === t);
  });
  try { history.replaceState(null, '', '#' + t); } catch (e) {}
}
$('#tabs').addEventListener('click', e => { const b = e.target.closest('[data-tab]'); if (b) setTab(b.dataset.tab); });
document.addEventListener('keydown', e => {
  if (e.target.tagName === 'INPUT' || e.ctrlKey || e.metaKey || e.altKey) return;
  const i = parseInt(e.key, 10);
  if (i >= 1 && i <= TABS.length) setTab(TABS[i - 1]);
});
function segHandler(sel, attr, apply) {
  $(sel).addEventListener('click', e => {
    const b = e.target.closest('button'); if (!b) return;
    $$('button', $(sel)).forEach(x => x.classList.toggle('on', x === b));
    apply(b.dataset[attr]);
  });
}
segHandler('#news-seg', 'v', v => { newsBrk = v === 'brk'; renderNews(); });
segHandler('#cal-week', 'w', v => { calWeek = v; renderCalendar(); });
segHandler('#cal-impact', 'i', v => { calImpact = v; renderCalendar(); });
$('#news-q').addEventListener('input', e => { newsQ = e.target.value; renderNews(); });

setTab((location.hash || '').slice(1));
Object.keys(RENDER).forEach(k => RENDER[k]());
renderTiles([]);
tickClock();
setInterval(tickClock, 1000);
Object.keys(RENDER).forEach(poll);
pollStatus();
})();
</script>
</body>
</html>
"""


# ----------------------------------------------------------------------------
# Start-up
# ----------------------------------------------------------------------------
def lan_ip() -> str | None:
    probe = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        probe.connect(("8.8.8.8", 80))
        return probe.getsockname()[0]
    except Exception:
        return None
    finally:
        probe.close()


_RUNNING_ATTR = "_kotak_desk_running"  # remembers a running terminal so a re-run can replace it


def port_in_use(port: int) -> bool:
    # Windows lets two servers bind the same port silently, so test by connecting.
    try:
        with socket.create_connection(("127.0.0.1", port), timeout=0.4):
            return True
    except OSError:
        return False


def stop_terminal() -> None:
    """Stop a terminal that was started in the background (notebook use)."""
    running = getattr(sys, _RUNNING_ATTR, None)
    if running:
        running["stop"]()
        print("Terminal stopped.")
    else:
        print("No terminal is running.")


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Mini Bloomberg Terminal - web edition")
    parser.add_argument("--host", default="127.0.0.1", help="use 0.0.0.0 to open it from other devices on your network")
    parser.add_argument("--port", type=int, default=8050)
    parser.add_argument("--no-browser", action="store_true", help="do not open the browser automatically")
    # parse_known_args: Jupyter passes its own "-f kernel-xxxx.json" argument, which must be ignored.
    args, _ignored = parser.parse_known_args(argv)

    in_notebook = "ipykernel" in sys.modules

    # Re-running a notebook cell: shut the previous copy down first so the port is free again.
    previous = getattr(sys, _RUNNING_ATTR, None)
    if previous:
        previous["stop"]()
        time.sleep(0.8)

    if port_in_use(args.port):
        print(f"Port {args.port} is already in use by another program. "
              f"Close it, or start with a different port, e.g. --port {args.port + 1}")
        return
    try:
        server = make_server(args.host, args.port, app, threaded=True)
    except OSError as exc:
        print(f"Could not start the web server on port {args.port}: {exc}")
        return

    url = f"http://127.0.0.1:{args.port}"
    print("=" * 60)
    print("  KOTAK DESK | Market terminal (web edition)")
    print("=" * 60)
    print(f"  Open in browser : {url}")
    if args.host == "0.0.0.0":
        ip = lan_ip()
        if ip:
            print(f"  On your network : http://{ip}:{args.port}")
    print("  Refresh         : prices 5s | news 10s | OI/PCR 5s | FII/DII 60s | calendar 5m")
    if in_notebook:
        print("  Stop            : run  stop_terminal()  in a notebook cell")
    else:
        print("  Stop            : Ctrl+C in this window")
    print("=" * 60)

    stop = threading.Event()
    threading.Thread(target=scheduler_loop, args=(stop,), daemon=True, name="scheduler").start()

    def shutdown() -> None:
        stop.set()
        try:
            server.shutdown()
            server.server_close()
        except Exception:
            pass
        if getattr(sys, _RUNNING_ATTR, None) is handle:
            setattr(sys, _RUNNING_ATTR, None)

    handle = {"stop": shutdown}
    setattr(sys, _RUNNING_ATTR, handle)

    if not args.no_browser:
        threading.Timer(1.5, lambda: webbrowser.open(url)).start()

    if in_notebook:
        # Background mode: the cell finishes immediately and the notebook stays usable.
        threading.Thread(target=server.serve_forever, daemon=True, name="web-server").start()
        return

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        shutdown()
        print("\nTerminal stopped.")
        os._exit(0)


if __name__ == "__main__":
    main()
