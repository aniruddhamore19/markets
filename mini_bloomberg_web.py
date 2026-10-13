"""
Mini Bloomberg Terminal - Web Edition (Render Optimized)
========================================================
- Geoblock handling & fail-fast timeouts for cloud hosting (Render).
- YFinance continuous futures fallbacks for GC=F, SI=F, HG=F, INR=X.
- Lightweight thread concurrency tailored for 0.1 vCPU / 512MB RAM containers.
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
from concurrent.futures import ThreadPoolExecutor, as_completed, wait as wait_futures
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
    1: "F", 2: "G", 3: "H", 4: "J", 5: "K", 6: "M",
    7: "N", 8: "Q", 9: "U", 10: "V", 11: "X", 12: "Z",
}

MARKET_QUERY_GROUPS = ("futures", "india", "forex", "cfd")

PRICE_STATIC_FALLBACKS = {
    "DXY": {"yf_symbol": "DX-Y.NYB", "scale": 1.0},
    "US10Y": {"yf_symbol": "^TNX", "scale": 1.0, "is_yield": True},
}

YF_FUTURES_FALLBACK = {
    "COMEX Gold Near": "GC=F",
    "COMEX Gold Far": "GC=F",
    "COMEX Silver Near": "SI=F",
    "COMEX Silver Far": "SI=F",
    "LME Copper Near": "HG=F",
    "LME Copper Far": "HG=F",
    "MCX Gold Near": "GC=F",
    "MCX Gold Far": "GC=F",
    "MCX Silver Near": "SI=F",
    "MCX Silver Far": "SI=F",
    "MCX Copper Near": "HG=F",
    "MCX Copper Far": "HG=F",
    "USDINR Near": "INR=X",
    "USDINR Far": "INR=X",
}

WATCHLIST_CONFIG = [
    {"label": "COMEX Gold Near", "kind": "contract", "root": "GC", "exchange_hint": "COMEX", "valid_months": list(range(1, 13)), "offset": 0},
    {"label": "COMEX Gold Far", "kind": "contract", "root": "GC", "exchange_hint": "COMEX", "valid_months": list(range(1, 13)), "offset": 1},
    {"label": "COMEX Silver Near", "kind": "contract", "root": "SI", "exchange_hint": "COMEX", "valid_months": list(range(1, 13)), "offset": 0},
    {"label": "COMEX Silver Far", "kind": "contract", "root": "SI", "exchange_hint": "COMEX", "valid_months": list(range(1, 13)), "offset": 1},
    {"label": "LME Copper Near", "kind": "contract", "root": "CA", "exchange_hint": "LME", "valid_months": list(range(1, 13)), "offset": 0},
    {"label": "LME Copper Far", "kind": "contract", "root": "CA", "exchange_hint": "LME", "valid_months": list(range(1, 13)), "offset": 1},
    {"label": "MCX Gold Near", "kind": "contract", "root": "GOLD", "exchange_hint": "MCX", "valid_months": [2, 4, 6, 8, 10, 12], "offset": 0},
    {"label": "MCX Gold Far", "kind": "contract", "root": "GOLD", "exchange_hint": "MCX", "valid_months": [2, 4, 6, 8, 10, 12], "offset": 1},
    {"label": "MCX Silver Near", "kind": "contract", "root": "SILVER", "exchange_hint": "MCX", "valid_months": [3, 5, 7, 9, 12], "offset": 0},
    {"label": "MCX Silver Far", "kind": "contract", "root": "SILVER", "exchange_hint": "MCX", "valid_months": [3, 5, 7, 9, 12], "offset": 1},
    {"label": "MCX Copper Near", "kind": "contract", "root": "COPPER", "exchange_hint": "MCX", "valid_months": list(range(1, 13)), "offset": 0},
    {"label": "MCX Copper Far", "kind": "contract", "root": "COPPER", "exchange_hint": "MCX", "valid_months": list(range(1, 13)), "offset": 1},
    {"label": "USDINR Near", "kind": "contract", "root": "USDINR", "exchange_hint": ("MSEI", "NSE"), "valid_months": list(range(1, 13)), "offset": 0},
    {"label": "USDINR Far", "kind": "contract", "root": "USDINR", "exchange_hint": ("MSEI", "NSE"), "valid_months": list(range(1, 13)), "offset": 1},
    {"label": "Dollar Index", "kind": "static", "symbol": "DXY", "exchange_hint": "TVC"},
    {"label": "US 10Y Yield", "kind": "static", "symbol": "US10Y", "exchange_hint": "TVC"},
]


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

HOSTED = bool(os.environ.get("RENDER")) or os.environ.get("KOTAK_DESK_HOSTED") == "1"
if HOSTED:
    INTERVALS.update({"prices": 15, "news": 30, "oi": 60, "fii_attempt": 120, "calendar_actuals": 600})

TASK_TIMEOUT = {"prices": 30, "news": 20, "oi": 30, "fii_attempt": 20, "calendar": 30, "calendar_actuals": 20}
MAX_STUCK_PER_TASK = 2
STARTED_AT = time.time()
FIRST_RUN_DELAY = {"prices": 0, "news": 2, "fii_attempt": 5, "oi": 8, "calendar": 10, "calendar_actuals": 10}


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


TV_DEADLINE = 10
YF_DEADLINE = 10

_notes_lock = threading.Lock()
_prices_notes: list[str] = []
_fetch_pool = ThreadPoolExecutor(max_workers=4, thread_name_prefix="fetch")
_inflight: dict[str, Any] = {}


def add_price_note(text: str) -> None:
    with _notes_lock:
        if text not in _prices_notes:
            _prices_notes.append(text)


def short_error(exc: BaseException) -> str:
    text = f"{type(exc).__name__}: {exc}".strip().rstrip(":")
    return " ".join(text.split())[:140]


def submit_guarded(key: str, func, *args):
    previous = _inflight.get(key)
    if previous is not None and not previous.done():
        return None
    future = _fetch_pool.submit(func, *args)
    _inflight[key] = future
    return future


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
        try:
            history = ticker.history(period="2d", interval="1d")
            if not history.empty:
                price = history["Close"].iloc[-1]
                if len(history) >= 2:
                    previous = history["Close"].iloc[-2]
                    if previous not in (None, 0):
                        change_pct = ((float(price) - float(previous)) / float(previous)) * 100.0
        except Exception:
            pass

    if price is None:
        raise ValueError(f"No yfinance data for {yf_symbol}")

    return round(float(price) * scale, 4), round(float(change_pct), 4) if change_pct is not None else None


def fetch_tradingview_rows(symbols: list[str]) -> pd.DataFrame:
    try:
        data = (
            Query()
            .set_markets(*MARKET_QUERY_GROUPS)
            .select("name", "description", "close", "change", "exchange")
            .where(col("name").isin(symbols))
            .get_scanner_data()
        )
        return data[1].copy()
    except Exception:
        return pd.DataFrame()


def filter_exchange_matches(raw_df: pd.DataFrame, exchange_hint: Any) -> pd.DataFrame:
    if raw_df.empty or not exchange_hint:
        return raw_df
    if isinstance(exchange_hint, (list, tuple, set)):
        mask = pd.Series(False, index=raw_df.index)
        for hint in exchange_hint:
            mask = mask | raw_df["exchange"].astype(str).str.contains(str(hint), case=False, na=False)
        return raw_df[mask]
    return raw_df[raw_df["exchange"].astype(str).str.contains(str(exchange_hint), case=False, na=False)]


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
        if not match.empty:
            return make_watchlist_row_from_match(spec, match.iloc[0], candidate)

    # Fallback for DXY & US10Y
    if spec["kind"] == "static" and spec["symbol"] in PRICE_STATIC_FALLBACKS:
        fallback = PRICE_STATIC_FALLBACKS[spec["symbol"]]
        try:
            price, change_pct = fetch_yfinance_snapshot(
                yf_symbol=str(fallback["yf_symbol"]),
                scale=float(fallback["scale"]),
            )
            if fallback.get("is_yield") and price and price > 25:
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
        except Exception as exc:
            add_price_note(f"Yahoo Finance {fallback['yf_symbol']}: {short_error(exc)}")

    # Reliable fallback for Commodities & FX
    if spec["label"] in YF_FUTURES_FALLBACK:
        sym = YF_FUTURES_FALLBACK[spec["label"]]
        try:
            price, change_pct = fetch_yfinance_snapshot(sym)
            return {
                "Watchlist": spec["label"],
                "Ticker": sym,
                "Description": spec["label"],
                "Price": price,
                "Chg %": change_pct,
                "Exchange": "yfinance (fallback)",
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
    return (spec.get("root"), tuple(spec.get("valid_months", [])), normalized_hint)


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
            resolved[spec["label"]] = resolve_watchlist_row(raw_df, spec)
    return resolved


def fetch_market_watchlist() -> pd.DataFrame | None:
    with _notes_lock:
        _prices_notes.clear()
    specs = build_watchlist_specs()
    all_symbols = sorted({symbol for spec in specs for symbol in spec["candidates"]})
    raw_df = pd.DataFrame()
    tv_future = submit_guarded("tradingview", fetch_tradingview_rows, all_symbols)
    if tv_future is not None:
        done, _ = wait_futures([tv_future], timeout=TV_DEADLINE)
        if done:
            try:
                raw_df = tv_future.result()
            except Exception:
                pass

    resolved_rows: dict[str, dict[str, Any]] = {}
    contract_groups: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    static_specs: list[dict[str, Any]] = []
    for spec in specs:
        if spec["kind"] == "contract":
            contract_groups.setdefault(contract_group_key(spec), []).append(spec)
        else:
            static_specs.append(spec)

    for spec in static_specs:
        resolved_rows[spec["label"]] = resolve_watchlist_row(raw_df, spec)

    for grouped_specs in contract_groups.values():
        resolved_rows.update(resolve_contract_group_rows(raw_df, grouped_specs))

    rows = [resolved_rows.get(spec["label"], make_missing_watchlist_row(spec)) for spec in specs]
    data = pd.DataFrame(rows)
    if data.empty:
        return None
    return data[["Watchlist", "Ticker", "Description", "Price", "Chg %", "Exchange", "Timestamp", "Status"]]


# ----------------------------------------------------------------------------
# News
# ----------------------------------------------------------------------------
BREAKING_KEYWORDS = [
    "rate cut", "rate hike", "emergency", "war", "attack", "sanctions", "opec cut",
    "opec hike", "fed pivot", "rbi rate", "surprise", "shock", "crash", "collapse",
    "default", "ban", "nuclear", "ceasefire", "escalat", "missile", "explosion",
    "tariff hike", "gold record", "silver record", "crude spike", "oil ban",
]

def _is_breaking(headline: str) -> bool:
    return any(keyword in headline.lower() for keyword in BREAKING_KEYWORDS)


_news_store: list[dict[str, Any]] = []
_seen_headlines: set[str] = set()

NEWS_FEEDS = [
    ("Mint", "https://www.livemint.com/rss/markets"),
    ("Guardian", "https://www.theguardian.com/business/rss"),
    (
        "GNews",
        "https://news.google.com/rss/search?q=MCX+gold+silver+copper+USDINR+DXY+US10Y&hl=en-IN&gl=IN&ceid=IN:en",
    ),
]

COMMODITY_KW = [
    "gold", "silver", "copper", "mcx", "comex", "lme", "usdinr", "rupee", "dollar",
    "dxy", "us 10y", "treasury", "yield", "rbi", "fed", "fomc", "inflation",
    "cpi", "rate", "bullion", "metal", "tariff", "trade",
]


def fetch_feed_items(source: str, url: str) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    try:
        resp = requests.get(url, headers=HEADERS, timeout=4)
        if resp.status_code != 200:
            return []
        soup = BeautifulSoup(resp.content, "html.parser")
        for item in soup.find_all("item")[:15]:
            title_tag = item.find("title")
            headline = title_tag.get_text(strip=True) if title_tag else ""
            if not headline or not any(k in headline.lower() for k in COMMODITY_KW):
                continue

            link_tag = item.find("link")
            link = link_tag.get_text(strip=True) if link_tag else ""

            pub_tag = item.find("pubdate")
            dt_obj = parse_pub_date(pub_tag.get_text(strip=True) if pub_tag else "")

            items.append({
                "Source": source,
                "Time": dt_obj.strftime("%d-%m-%Y %H:%M"),
                "Headline": headline,
                "Link": link,
                "dt_obj": dt_obj,
                "Breaking": "BREAKING" if _is_breaking(headline) else "",
            })
    except Exception:
        return []
    return items


def fetch_commodity_news() -> list[dict[str, Any]]:
    new_items: list[dict[str, Any]] = []
    with ThreadPoolExecutor(max_workers=3) as executor:
        futures = [executor.submit(fetch_feed_items, source, url) for source, url in NEWS_FEEDS]
        for future in as_completed(futures):
            try:
                for entry in future.result():
                    key = entry["Headline"][:120]
                    if key in _seen_headlines:
                        continue
                    _seen_headlines.add(key)
                    new_items.append(entry)
            except Exception:
                continue

    _news_store.extend(new_items)
    _news_store.sort(key=lambda item: item["dt_obj"], reverse=True)
    del _news_store[300:]
    return list(_news_store)


# ----------------------------------------------------------------------------
# FII / DII
# ----------------------------------------------------------------------------
def _fetch_moneycontrol_fii() -> list[dict[str, Any]]:
    resp = requests.get(
        "https://www.moneycontrol.com/stocks/marketstats/fii_dii_activity/index.php",
        headers=HEADERS,
        timeout=6,
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
        rows.append({
            "Date": cells[0],
            "Category": cells[1],
            "Buy (Cr)": cells[2],
            "Sell (Cr)": cells[3],
            "Net (Cr)": cells[4],
        })
    return rows


def fetch_fii_dii_flow() -> list[dict[str, Any]]:
    try:
        rows = _fetch_moneycontrol_fii()
        if rows:
            return rows
    except Exception:
        pass

    return [
        {
            "Date": datetime.now().strftime("%d-%m-%Y"),
            "Category": "FII / DII (NSE/MoneyControl throttled cloud IP)",
            "Buy (Cr)": "-",
            "Sell (Cr)": "-",
            "Net (Cr)": "-",
        }
    ]


# ----------------------------------------------------------------------------
# Economic calendar
# ----------------------------------------------------------------------------
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
        response = requests.get(url, headers={"User-Agent": "Mozilla/5.0"}, timeout=6)
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
                t_str = datetime.strptime((entry.get("time", "") or "").strip(), "%I:%M%p").strftime("%H:%M") + " EST"
            except Exception:
                t_str = entry.get("time", "") or "All Day"

            events.append({
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
            })
        return events
    except Exception:
        return []


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


def fetch_economic_calendar(actuals_only: bool = False) -> dict[str, list[dict[str, Any]]]:
    global _calendar_cache
    lw0, lw1, tw0, tw1, nw0, nw1 = get_week_bounds()
    ff_this = _fetch_ff("thisweek")
    ff_next = _fetch_ff("nextweek")

    _calendar_cache = {
        "last": [],
        "this": _bucket(ff_this, tw0, tw1),
        "next": _bucket(ff_next, nw0, nw1),
    }
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

    rows = [{
        "type": "summary",
        "label": f"{label} [{source}]",
        "total_call_oi": total_calls,
        "total_put_oi": total_puts,
        "pcr": pcr,
        "max_pain": max_pain,
        "sentiment": bias,
    }]

    top_strikes = sorted(
        all_strikes,
        key=lambda strike: call_oi.get(strike, 0) + put_oi.get(strike, 0),
        reverse=True,
    )[:8]
    for strike in sorted(top_strikes):
        rows.append({
            "type": "strike",
            "strike": strike,
            "call_oi": call_oi.get(strike, 0),
            "put_oi": put_oi.get(strike, 0),
            "pcr": round(put_oi.get(strike, 0) / max(call_oi.get(strike, 1), 1), 2),
            "max_pain": "MAX PAIN" if strike == max_pain else "",
        })
    return rows


def fetch_comex_oi() -> list[dict[str, Any]]:
    results: list[dict[str, Any]] = []
    for symbol, label in [("GC=F", "COMEX Gold"), ("SI=F", "COMEX Silver")]:
        try:
            ticker = yf.Ticker(symbol)
            expiries = ticker.options
            if not expiries:
                raise ValueError(f"No options chain for {symbol}")
            chain = ticker.option_chain(expiries[0])
            if chain.calls.empty and chain.puts.empty:
                raise ValueError("Empty option chain")
            call_oi = dict(zip(chain.calls["strike"].tolist(), chain.calls["openInterest"].fillna(0).astype(int).tolist()))
            put_oi = dict(zip(chain.puts["strike"].tolist(), chain.puts["openInterest"].fillna(0).astype(int).tolist()))
            results += _calc_oi(call_oi, put_oi, label, f"yfinance/{expiries[0]}")
        except Exception as exc:
            results.append({
                "type": "summary",
                "label": f"{label} [Unavailable]",
                "total_call_oi": "N/A",
                "total_put_oi": "N/A",
                "pcr": "N/A",
                "max_pain": "N/A",
                "sentiment": str(exc)[:55],
            })
    return results


def fetch_all_oi() -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = [{"type": "header", "label": "COMEX OI / PCR"}]
    rows += fetch_comex_oi()
    return rows


# ----------------------------------------------------------------------------
# Shared store & Background Loop
# ----------------------------------------------------------------------------
MODULE_INTERVAL = {"prices": 10, "news": 30, "oi": 60, "fii": 60, "calendar": 300}
MODULE_LABEL = {"prices": "PRICES", "news": "NEWS", "oi": "OI / PCR", "fii": "FII / DII", "calendar": "ECO CALENDAR"}
TASK_MODULE = {"prices": "prices", "news": "news", "oi": "oi", "fii_attempt": "fii", "calendar": "calendar", "calendar_actuals": "calendar"}


def clean(obj: Any) -> Any:
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
            return obj.item()
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
    return clean(df.to_dict(orient="records"))


def news_payload(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not items:
        raise ValueError("No news headlines returned by feeds")
    out = []
    for item in items:
        entry = {k: v for k, v in item.items() if k != "dt_obj"}
        entry["Iso"] = item["dt_obj"].isoformat() + "Z"
        out.append(entry)
    return clean(out)


def fii_payload(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not rows:
        raise ValueError("FII data unavailable")
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
        raise ValueError("Calendar returned no events")
    return clean({k: list(buckets.get(k, [])) for k in ("last", "this", "next")})


def oi_payload(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
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
        store.fail(module, (str(exc) or type(exc).__name__)[:400])
    finally:
        mark_done(task)
        if task == "calendar":
            mark_done("calendar_actuals")


TASK_INFO: dict[str, dict[str, Any]] = {
    task: {"running_since": None, "last_duration": None, "stuck": 0} for task in TASK_MODULE
}


def scheduler_loop(stop: threading.Event) -> None:
    force_all_modules_due()
    if HOSTED:
        begin = time.time()
        for key, delay in FIRST_RUN_DELAY.items():
            _next_run[key] = begin + delay
    for info in TASK_INFO.values():
        info.update(running_since=None, stuck=0)
    pending: dict[str, tuple[Any, float]] = {}
    stuck: dict[str, list[Any]] = {task: [] for task in TASK_MODULE}
    executor = ThreadPoolExecutor(max_workers=4)

    def start(task: str) -> None:
        now = time.time()
        pending[task] = (submit_task(executor, task), now)
        TASK_INFO[task]["running_since"] = now

    while not stop.is_set():
        try:
            now = time.time()
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
                    store.fail(TASK_MODULE[task], f"Timed out after {int(now - started)}s")
                    mark_done(task)
        except Exception:
            pass
        stop.wait(0.5)


# ----------------------------------------------------------------------------
# Flask App Setup
# ----------------------------------------------------------------------------
app = Flask(__name__)
AUTH_USER = os.environ.get("TERMINAL_USER", "friend")
AUTH_PASS = os.environ.get("TERMINAL_PASSWORD", "")


@app.before_request
def keep_refresh_running():
    if _bg["auto"]:
        start_background_refresh()
    return None


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
    return Response("Login required.", status=401, headers={"WWW-Authenticate": 'Basic realm="Kotak Desk"'})


@app.route("/healthz")
def healthz() -> Response:
    return Response("ok", mimetype="text/plain")


_bg: dict[str, Any] = {"thread": None, "pid": None, "auto": False, "last_start": 0.0, "starts": 0, "error": ""}
_bg_lock = threading.Lock()


def _scheduler_main(stop: threading.Event) -> None:
    try:
        scheduler_loop(stop)
    except BaseException as exc:
        _bg["error"] = f"{type(exc).__name__}: {exc}"[:300]


def _refresh_alive() -> bool:
    thread = _bg["thread"]
    return thread is not None and thread.is_alive() and _bg["pid"] == os.getpid()


def start_background_refresh() -> None:
    _bg["auto"] = True
    if _refresh_alive():
        return
    with _bg_lock:
        if _refresh_alive():
            return
        thread = threading.Thread(target=_scheduler_main, args=(threading.Event(),), daemon=True, name="scheduler")
        _bg.update(thread=thread, pid=os.getpid(), last_start=time.time(), starts=_bg["starts"] + 1)
        thread.start()


@app.route("/")
def index() -> Response:
    resp = Response(INDEX_HTML, mimetype="text/html")
    resp.headers["Cache-Control"] = "no-store"
    return resp


@app.route("/api/status")
def api_status():
    now = time.time()
    tasks = {}
    for task, info in TASK_INFO.items():
        since = info["running_since"]
        tasks[task] = {
            "running_for": round(now - since, 1) if since else None,
            "last_duration": info["last_duration"],
            "stuck": info["stuck"],
        }
    refresher = next((t for t in threading.enumerate() if t.name == "scheduler"), None)
    return jsonify(
        uptime=round(now - STARTED_AT, 1),
        scheduler_alive=bool(refresher and refresher.is_alive()),
        tasks=tasks,
        modules={name: store.brief(name) for name in MODULE_INTERVAL},
    )


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
td.hl{white-space:normal;min-width:340px;max-width:760px;text-align:left}
tbody tr:hover td:not(.pos):not(.neg):not(.flat){background:#161619}
td.pos{background:var(--up-bg);color:var(--up)}
td.neg{background:var(--dn-bg);color:var(--dn)}
td.flat{background:var(--warn-bg)}
.badge{display:inline-block;padding:2px 8px;border-radius:2px;font:700 10px var(--mono)}
.badge.brk{background:var(--brk);color:#fff}
.badge.high{background:var(--dn-bg);color:var(--dn);border:1px solid rgba(255,92,92,.4)}
.badge.med{background:var(--warn-bg);color:var(--amber);border:1px solid rgba(255,176,0,.35)}
.bad{color:var(--dn)}
.foot{flex:none;padding:5px 14px;border-top:1px solid var(--line);font:11px var(--mono);color:var(--dim);background:var(--panel)}
.empty{padding:70px 20px;text-align:center;color:var(--dim);font:13px var(--mono)}
.empty b{color:var(--dn);display:block;font-size:14px;margin-bottom:8px}
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
.oi-stats{display:grid;grid-template-columns:repeat(3,1fr);gap:1px;background:var(--line)}
.oi-stats div{background:var(--panel);padding:8px 10px;text-align:center}
.oi-stats .sl{font:11px var(--mono);color:var(--dim)}
.oi-stats .sv{font:600 15px var(--mono);margin-top:2px;font-variant-numeric:tabular-nums}
.oi-stats .sv.pos{color:var(--up)}
.oi-stats .sv.neg{color:var(--dn)}
.oi-stats .sv.flat{color:var(--amber)}
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
  <div class="chips" id="chips"></div>
  <div class="clock" id="clock"></div>
</header>
<section class="tiles" id="tiles"></section>
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
      <input type="search" id="news-q" placeholder="Filter headlines or source">
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
<footer class="foot">Data comes from public sources. For information only.</footer>
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

const MOD = { prices: 'PRICES', news: 'NEWS', fii: 'FII/DII', calendar: 'CALENDAR', oi: 'OI/PCR' };
const TABS = ['prices', 'news', 'fii', 'calendar', 'oi'];
const S = {};
let skew = 0;

function emptyMsg(m) {
  if (m && m.error) return '<div class="empty"><b>[!] DATA TEMPORARILY UNAVAILABLE</b>' + esc(m.error) + '<br>Retrying automatically.</div>';
  return '<div class="empty">Loading market data...</div>';
}

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
  if (!m || !m.data) { view.innerHTML = emptyMsg(m); renderTiles([]); return; }
  const rows = m.data;
  let html = '<table><thead><tr><th>Watchlist</th><th>Ticker</th><th>Description</th><th>Price</th><th>Chg %</th><th>Exchange</th><th>Timestamp</th><th>Status</th></tr></thead><tbody>';
  rows.forEach(r => {
    const p = r.Price, c = r['Chg %'], cls = signCls(c);
    html += '<tr><td>' + esc(r.Watchlist) + '</td><td class="num">' + esc(r.Ticker) + '</td><td>' + esc(r.Description) + '</td>'
      + '<td class="num ' + cls + '">' + fmt(p) + '</td><td class="num ' + cls + '">' + fmtChg(c) + '</td>'
      + '<td>' + esc(r.Exchange) + '</td><td class="num">' + esc(r.Timestamp) + '</td>'
      + '<td class="' + (r.Status === 'OK' ? '' : 'bad') + '">' + esc(r.Status) + '</td></tr>';
  });
  view.innerHTML = html + '</tbody></table>';
  renderTiles(rows);
}

let newsQ = '', newsBrk = false;
function renderNews() {
  const m = S.news, view = $('#news-view');
  if (!m || !m.data) { view.innerHTML = emptyMsg(m); $('#news-meta').textContent = ''; return; }
  const q = newsQ.trim().toLowerCase();
  const rows = m.data.filter(n => (!newsBrk || n.Breaking) && (!q || ((n.Headline || '') + ' ' + (n.Source || '')).toLowerCase().includes(q)));
  $('#news-meta').textContent = rows.length + ' of ' + m.data.length + ' headlines';
  let html = '<table><thead><tr><th>Time</th><th>Source</th><th>Breaking</th><th>Headline</th><th>Link</th></tr></thead><tbody>';
  rows.forEach(n => {
    const url = safeUrl(n.Link);
    html += '<tr><td class="num">' + esc(n.Time) + '</td><td>' + esc(n.Source) + '</td>'
      + '<td>' + (n.Breaking ? '<span class="badge brk">BREAKING</span>' : '') + '</td>'
      + '<td class="hl">' + esc(n.Headline) + '</td>'
      + '<td>' + (url ? '<a href="' + esc(url) + '" target="_blank" rel="noopener noreferrer">Read</a>' : 'No link') + '</td></tr>';
  });
  view.innerHTML = html + '</tbody></table>';
}

function renderFii() {
  const m = S.fii, view = $('#fii-view');
  if (!m || !m.data) { view.innerHTML = emptyMsg(m); return; }
  const rows = m.data;
  const cards = rows.filter(r => isNum(r['Net (Cr)'])).map(r => {
    const n = r['Net (Cr)'];
    return '<div class="card ' + signCls(n) + '"><div class="cl">' + esc(r.Category) + '</div><div class="cd">' + esc(r.Date) + '</div>'
      + '<div class="cv">' + fmtChg(n) + '<small>Cr net</small></div>'
      + '<div class="cs">Buy ' + fmt(r['Buy (Cr)']) + ' / Sell ' + fmt(r['Sell (Cr)']) + '</div></div>';
  }).join('');
  let html = (cards ? '<div class="cards">' + cards + '</div>' : '')
    + '<table><thead><tr><th>Date</th><th>Category</th><th>Buy value (Cr)</th><th>Sell value (Cr)</th><th>Net value (Cr)</th></tr></thead><tbody>';
  rows.forEach(r => {
    html += '<tr><td>' + esc(r.Date) + '</td><td>' + esc(r.Category) + '</td><td class="num">' + fmt(r['Buy (Cr)']) + '</td><td class="num">' + fmt(r['Sell (Cr)']) + '</td>'
      + '<td class="num ' + (isNum(r['Net (Cr)']) ? signCls(r['Net (Cr)']) : 'flat') + '">' + (isNum(r['Net (Cr)']) ? fmt(r['Net (Cr)']) : esc(r['Net (Cr)'])) + '</td></tr>';
  });
  view.innerHTML = html + '</tbody></table>';
}

let calWeek = 'this', calImpact = 'all';
function renderCalendar() {
  const m = S.calendar, view = $('#calendar-view');
  if (!m || !m.data) { view.innerHTML = emptyMsg(m); $('#cal-meta').textContent = ''; return; }
  const all = m.data[calWeek] || [];
  const rows = all.filter(e => calImpact === 'all' || e.Impact === 'HIGH');
  $('#cal-meta').textContent = rows.length + ' events';
  if (!rows.length) { view.innerHTML = '<div class="empty">No events for this selection.</div>'; return; }
  let html = '<table><thead><tr><th>Date</th><th>Time</th><th>Country</th><th>Event</th><th>Impact</th><th>Previous</th><th>Forecast</th><th>Actual</th><th>Link</th></tr></thead><tbody>';
  rows.forEach(e => {
    const url = safeUrl(e.Link);
    html += '<tr><td class="num">' + esc(e.Date) + '</td><td class="num">' + esc(e.Time) + '</td><td>' + esc(e.Country) + '</td>'
      + '<td class="hl">' + esc(e.Event) + '</td>'
      + '<td><span class="badge ' + (e.Impact === 'HIGH' ? 'high' : 'med') + '">' + esc(e.Impact) + '</span></td>'
      + '<td class="num">' + esc(e.Previous) + '</td><td class="num">' + esc(e.Forecast) + '</td><td class="num">' + esc(e.Actual) + '</td>'
      + '<td>' + (url ? '<a href="' + esc(url) + '" target="_blank" rel="noopener noreferrer">Link</a>' : '') + '</td></tr>';
  });
  view.innerHTML = html + '</tbody></table>';
}

function renderOi() {
  const m = S.oi, view = $('#oi-view');
  if (!m || !m.data) { view.innerHTML = emptyMsg(m); return; }
  const sections = []; let sec = null, card = null;
  m.data.forEach(it => {
    if (it.type === 'header') { sec = { label: it.label, cards: [] }; sections.push(sec); card = null; }
    else if (it.type === 'summary') { if (!sec) { sec = { label: 'OI / PCR', cards: [] }; sections.push(sec); } card = Object.assign({ strikes: [] }, it); sec.cards.push(card); }
    else if (it.type === 'strike' && card) { card.strikes.push(it); }
  });
  let html = '';
  sections.forEach(s => {
    html += '<div class="oi-sec"><h2>' + esc(s.label) + '</h2><div class="oi-grid">';
    s.cards.forEach(c => {
      const na = !isNum(c.pcr);
      const pcrCls = na ? 'flat' : c.pcr > 1.2 ? 'pos' : c.pcr < 0.8 ? 'neg' : 'flat';
      html += '<div class="oi-card"><div class="oh">' + esc(c.label) + '</div><div class="oi-stats">'
        + '<div><div class="sl">Total call OI</div><div class="sv">' + esc(c.total_call_oi) + '</div></div>'
        + '<div><div class="sl">Total put OI</div><div class="sv">' + esc(c.total_put_oi) + '</div></div>'
        + '<div><div class="sl">PCR</div><div class="sv ' + pcrCls + '">' + esc(na ? c.pcr : c.pcr.toFixed(2)) + '</div></div>'
        + '<div><div class="sl">Max pain</div><div class="sv">' + esc(c.max_pain) + '</div></div>'
        + '<div><div class="sl">Bias</div><div class="sv">' + esc(c.sentiment) + '</div></div>'
        + '<div><div class="sl">Updated</div><div class="sv">' + (m.updated ? hhmmss(m.updated) : '-') + '</div></div></div>';
      if (c.strikes.length) {
        html += '<table><thead><tr><th>Strike</th><th>Call OI</th><th>Put OI</th><th>PCR</th><th>Note</th></tr></thead><tbody>';
        c.strikes.forEach(k => {
          html += '<tr><td class="num">' + esc(k.strike) + '</td><td class="num">' + esc(k.call_oi) + '</td><td class="num">' + esc(k.put_oi) + '</td><td class="num">' + fmt(k.pcr) + '</td><td>' + esc(k.max_pain) + '</td></tr>';
        });
        html += '</tbody></table>';
      }
      html += '</div>';
    });
    html += '</div></div>';
  });
  view.innerHTML = html;
}

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
    return '<div class="chip ' + cls + '"><span class="dot"></span>' + MOD[k] + ' <b>' + txt + '</b></div>';
  }).join('');
}

function tickClock() {
  const d = new Date();
  $('#clock').textContent = pad(d.getDate()) + '-' + pad(d.getMonth() + 1) + '-' + d.getFullYear() + ' ' + pad(d.getHours()) + ':' + pad(d.getMinutes()) + ':' + pad(d.getSeconds());
  renderChips();
}

const RENDER = { prices: renderPrices, news: renderNews, fii: renderFii, calendar: renderCalendar, oi: renderOi };

async function poll(name) {
  try {
    const prev = S[name];
    const r = await fetch('/api/' + name + '?v=' + (prev ? prev.version : -1), { cache: 'no-store' });
    if (!r.ok) return;
    const j = await r.json();
    skew = j.server_time - Date.now() / 1000;
    S[name] = { updated: j.updated, error: j.error, interval: j.interval, version: j.version, data: j.unchanged ? (prev ? prev.data : null) : j.data };
    if (!j.unchanged || !prev) RENDER[name]();
  } catch (e) {
  } finally {
    setTimeout(() => poll(name), 3000);
  }
}

function setTab(t) {
  if (TABS.indexOf(t) < 0) t = 'prices';
  TABS.forEach(x => {
    $('#pane-' + x).classList.toggle('active', x === t);
    $('[data-tab="' + x + '"]').classList.toggle('active', x === t);
  });
  try { history.replaceState(null, '', '#' + t); } catch (e) {}
}
$('#tabs').addEventListener('click', e => { const b = e.target.closest('[data-tab]'); if (b) setTab(b.dataset.tab); });
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
})();
</script>
</body>
</html>
"""


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Mini Bloomberg Terminal - web edition")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8050)
    parser.add_argument("--no-browser", action="store_true")
    args, _ = parser.parse_known_args(argv)

    server = make_server(args.host, args.port, app, threaded=True)
    stop = threading.Event()
    threading.Thread(target=scheduler_loop, args=(stop,), daemon=True, name="scheduler").start()

    if not args.no_browser:
        threading.Timer(1.5, lambda: webbrowser.open(f"http://127.0.0.1:{args.port}")).start()

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        stop.set()
        server.shutdown()
        server.server_close()
        os._exit(0)


if __name__ == "__main__":
    main()
