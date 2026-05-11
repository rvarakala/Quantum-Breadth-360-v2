"""
Global Markets — overview card backend
Fetches GIFT NIFTY, S&P 500, Nasdaq, Nikkei 225, Hang Seng.

Data sources:
- GIFT NIFTY: investing.com scrape (code: 17940)
- Others: yfinance (^GSPC, ^IXIC, ^N225, ^HSI)

Cache: in-memory dict, 5-min TTL during active market sessions,
       30-min TTL outside.
"""
import logging
import time
import re
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any, Optional

import requests

logger = logging.getLogger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))

# In-memory cache: { key: { "data": {...}, "fetched_at": epoch_seconds } }
_cache: Dict[str, Dict[str, Any]] = {}

# Tickers config — order controls display order in UI
MARKETS_CONFIG = [
    {"key": "GIFT_NIFTY", "label": "GIFT NIFTY", "source": "investing", "weight": 2},
    {"key": "SP500",      "label": "S&P 500",    "source": "yfinance", "ticker": "^GSPC",  "weight": 1},
    {"key": "NASDAQ",     "label": "Nasdaq",     "source": "yfinance", "ticker": "^IXIC",  "weight": 1},
    {"key": "NIKKEI",     "label": "Nikkei 225", "source": "yfinance", "ticker": "^N225",  "weight": 1},
    {"key": "HSI",        "label": "Hang Seng",  "source": "yfinance", "ticker": "^HSI",   "weight": 1},
]


def _is_active_session() -> bool:
    """True if any major market is likely active (so we use short cache)."""
    now_ist = datetime.now(IST)
    h = now_ist.hour + now_ist.minute / 60
    # Asian/Indian/GIFT hours: ~06:00-23:30 IST covers Tokyo, HK, India, US pre-market
    return 6 <= h <= 23.5


def _cache_ttl() -> int:
    return 300 if _is_active_session() else 1800  # 5 min active, 30 min otherwise


def _get_cached(key: str) -> Optional[Dict[str, Any]]:
    entry = _cache.get(key)
    if not entry:
        return None
    age = time.time() - entry["fetched_at"]
    if age < _cache_ttl():
        return entry["data"]
    return None


def _set_cache(key: str, data: Dict[str, Any]) -> None:
    _cache[key] = {"data": data, "fetched_at": time.time()}


# Short-TTL cache for negative results: stored with a timestamp 60 seconds
# in the past so it expires from the normal _get_cached lookup quickly.
# This means: incomplete payload → next request retries within 60s instead
# of waiting the full 5–30 min TTL.
NEGATIVE_CACHE_TTL = 60

def _set_cache_short(key: str, data: Dict[str, Any]) -> None:
    # Trick: backdate fetched_at so age = ttl - 60s, giving us a 60s effective TTL
    fake_age = max(0, _cache_ttl() - NEGATIVE_CACHE_TTL)
    _cache[key] = {"data": data, "fetched_at": time.time() - fake_age}


def _fetch_yfinance(ticker: str) -> Optional[Dict[str, Any]]:
    """Fetch last price + previous close via yfinance. Returns None on failure.

    Uses attribute access on fast_info (yfinance 0.2.x removed dict-like .get()).
    Falls through to history() whenever fast_info yields incomplete data.
    """
    try:
        import yfinance as yf
        t = yf.Ticker(ticker)
        price, prev_close = None, None

        # Try fast_info via attribute access — robust across 0.2.x versions
        try:
            fi = t.fast_info
            _last = getattr(fi, "last_price", None)
            _prev = getattr(fi, "previous_close", None)
            if _last is not None:
                price = float(_last)
            if _prev is not None:
                prev_close = float(_prev)
        except Exception as e:
            logger.debug(f"fast_info unavailable for {ticker}: {e}")

        # Fall back to history() if either value missing or zero
        if not price or not prev_close:
            hist = t.history(period="5d", interval="1d")
            if hist.empty or len(hist) < 2:
                logger.warning(f"yfinance history empty source=yfinance ticker={ticker}")
                return None
            price = float(hist["Close"].iloc[-1])
            prev_close = float(hist["Close"].iloc[-2])

        if not price or not prev_close or prev_close == 0:
            return None

        change = price - prev_close
        change_pct = (change / prev_close) * 100
        return {
            "price": round(price, 2),
            "prev_close": round(prev_close, 2),
            "change": round(change, 2),
            "change_pct": round(change_pct, 2),
            "status": "OK",
            "source": "yfinance",
        }
    except Exception as e:
        logger.warning(f"fetch failed source=yfinance ticker={ticker} error={e}")
        return None


# yfinance ticker → Stooq symbol mapping. Stooq has different conventions:
#   ^GSPC (S&P 500) → ^spx
#   ^IXIC (Nasdaq)  → ^ndq
#   ^N225 (Nikkei)  → ^nkx
#   ^HSI  (Hang Seng) → ^hsi
#   ^NSEI (Nifty 50) → ^nsei (works too — for GIFT NIFTY fallback chain)
_STOOQ_MAP = {
    "^GSPC": "^spx",
    "^IXIC": "^ndq",
    "^N225": "^nkx",
    "^HSI":  "^hsi",
    "^NSEI": "^nsei",
}


def _fetch_stooq(ticker: str) -> Optional[Dict[str, Any]]:
    """Fallback: fetch last 2 daily closes from Stooq CSV. Free, no auth, no
    rate limits. Returns None on failure (caller falls through to next source).

    Endpoint: https://stooq.com/q/d/l/?s=<symbol>&i=d
    Returns CSV: Date,Open,High,Low,Close,Volume
    We need the last 2 rows to compute change vs previous close.

    Note: Stooq blocks data-center IPs (AWS/GCP/Azure). On residential or
    business ISPs it works reliably. If you see 403 in logs, fall-through
    to Yahoo direct chart API will handle it.
    """
    stooq_symbol = _STOOQ_MAP.get(ticker)
    if not stooq_symbol:
        logger.debug(f"no Stooq mapping for {ticker}")
        return None
    url = f"https://stooq.com/q/d/l/?s={stooq_symbol}&i=d"
    try:
        r = requests.get(url, timeout=8, headers={
            "User-Agent": "Mozilla/5.0 QB360/1.0",
        })
        if r.status_code != 200:
            logger.warning(f"fetch failed source=stooq ticker={ticker} http={r.status_code}")
            return None
        text = r.text.strip()
        # Stooq returns "No data" or empty body when symbol is invalid
        if not text or text.lower().startswith("no data") or "<html" in text.lower():
            logger.warning(f"fetch failed source=stooq ticker={ticker} reason=no_data")
            return None
        lines = text.splitlines()
        if len(lines) < 3:   # header + at least 2 data rows
            logger.warning(f"fetch failed source=stooq ticker={ticker} reason=insufficient_rows")
            return None
        # Header: Date,Open,High,Low,Close,Volume
        # Take last 2 data rows (latest is last line)
        last_row = lines[-1].split(",")
        prev_row = lines[-2].split(",")
        if len(last_row) < 5 or len(prev_row) < 5:
            logger.warning(f"fetch failed source=stooq ticker={ticker} reason=malformed_csv")
            return None
        price = float(last_row[4])
        prev_close = float(prev_row[4])
        if not price or not prev_close or prev_close == 0:
            return None
        change = price - prev_close
        change_pct = (change / prev_close) * 100
        return {
            "price": round(price, 2),
            "prev_close": round(prev_close, 2),
            "change": round(change, 2),
            "change_pct": round(change_pct, 2),
            "status": "OK",
            "source": "stooq",
        }
    except Exception as e:
        logger.warning(f"fetch failed source=stooq ticker={ticker} error={e}")
        return None


def _fetch_yahoo_direct(ticker: str) -> Optional[Dict[str, Any]]:
    """Third-tier fallback: call Yahoo Finance's chart JSON API directly,
    bypassing the yfinance Python wrapper entirely.

    Why this works when yfinance fails:
      - yfinance maintains its own session state, cookies, and crumb tokens
        that occasionally break or get stuck mid-day. The raw chart API
        has no such state — every call is independent.
      - Useful when yfinance's fast_info or .history() returns empty due to
        the wrapper, not Yahoo's actual data.

    Endpoint returns JSON with timestamps and close prices for the last N
    intervals. We need the last 2 daily closes.
    """
    url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
    params = {"range": "5d", "interval": "1d"}
    try:
        r = requests.get(url, params=params, timeout=8, headers={
            "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) "
                           "Chrome/120.0.0.0 Safari/537.36"),
        })
        if r.status_code != 200:
            logger.warning(f"fetch failed source=yahoo_direct ticker={ticker} http={r.status_code}")
            return None
        data = r.json()
        chart = data.get("chart", {})
        if chart.get("error"):
            logger.warning(f"fetch failed source=yahoo_direct ticker={ticker} api_error={chart['error']}")
            return None
        results = chart.get("result", [])
        if not results:
            return None
        result = results[0]
        closes = result.get("indicators", {}).get("quote", [{}])[0].get("close", [])
        # Strip None values that Yahoo sometimes returns for incomplete bars
        valid = [c for c in closes if c is not None]
        if len(valid) < 2:
            logger.warning(f"fetch failed source=yahoo_direct ticker={ticker} reason=insufficient_closes")
            return None
        price = float(valid[-1])
        prev_close = float(valid[-2])
        if not price or not prev_close or prev_close == 0:
            return None
        change = price - prev_close
        change_pct = (change / prev_close) * 100
        return {
            "price": round(price, 2),
            "prev_close": round(prev_close, 2),
            "change": round(change, 2),
            "change_pct": round(change_pct, 2),
            "status": "OK",
            "source": "yahoo_direct",
        }
    except Exception as e:
        logger.warning(f"fetch failed source=yahoo_direct ticker={ticker} error={e}")
        return None


def _fetch_with_fallback(ticker: str) -> Optional[Dict[str, Any]]:
    """
    Try yfinance → Stooq → Yahoo direct chart API. Returns the first
    successful payload, or None if all three sources fail.

    Three independent sources give high resilience:
      - yfinance: well-tested library, can hit rate limits or wrapper bugs
      - Stooq: simple CSV, no auth, blocks data-center IPs
      - Yahoo direct: bypasses yfinance wrapper, works when yfinance is stuck
    """
    data = _fetch_yfinance(ticker)
    if data:
        return data
    logger.info(f"[global-markets] yfinance miss for {ticker} — trying Stooq")
    data = _fetch_stooq(ticker)
    if data:
        logger.info(f"[global-markets] ✅ Stooq served {ticker}")
        return data
    logger.info(f"[global-markets] Stooq miss for {ticker} — trying Yahoo direct")
    data = _fetch_yahoo_direct(ticker)
    if data:
        logger.info(f"[global-markets] ✅ Yahoo direct served {ticker}")
        return data
    logger.warning(f"[global-markets] ❌ all 3 sources failed for {ticker}")
    return None


def _fetch_gift_nifty() -> Optional[Dict[str, Any]]:
    """
    Scrape GIFT NIFTY from investing.com.
    Primary URL: https://www.investing.com/indices/s-p-cnx-nifty-futures
    Fallback: use yfinance ^NSEI (spot NIFTY) if scrape fails.
    """
    try:
        url = "https://www.investing.com/indices/s-p-cnx-nifty-futures"
        headers = {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "en-US,en;q=0.9",
        }
        r = requests.get(url, headers=headers, timeout=8)
        if r.status_code != 200:
            raise RuntimeError(f"HTTP {r.status_code}")
        html = r.text
        # investing.com embeds price in data-test="instrument-price-last"
        price_match = re.search(
            r'data-test="instrument-price-last"[^>]*>([0-9,]+\.?[0-9]*)',
            html
        )
        change_match = re.search(
            r'data-test="instrument-price-change"[^>]*>([+\-]?[0-9,]+\.?[0-9]*)',
            html
        )
        pct_match = re.search(
            r'data-test="instrument-price-change-percent"[^>]*>\(?([+\-]?[0-9,]+\.?[0-9]*)',
            html
        )
        if not price_match:
            raise RuntimeError("price regex miss")
        price = float(price_match.group(1).replace(",", ""))
        change = float(change_match.group(1).replace(",", "")) if change_match else 0.0
        change_pct = float(pct_match.group(1).replace(",", "")) if pct_match else 0.0
        prev_close = round(price - change, 2)
        return {
            "price": round(price, 2),
            "prev_close": prev_close,
            "change": round(change, 2),
            "change_pct": round(change_pct, 2),
            "status": "OK",
        }
    except Exception as e:
        logger.warning(f"GIFT NIFTY scrape failed: {e} — falling back to ^NSEI via yfinance/Stooq")
        # Fallback: spot NIFTY from yfinance, then Stooq
        fallback = _fetch_with_fallback("^NSEI")
        if fallback:
            fallback["status"] = "FALLBACK_SPOT"
        return fallback


def _compute_tone(markets: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Weighted green/red score → RISK_ON / MIXED / RISK_OFF."""
    score = 0
    for m in markets:
        if m.get("status") not in ("OK", "FALLBACK_SPOT"):
            continue
        weight = m.get("weight", 1)
        pct = m.get("change_pct", 0)
        if pct > 0.15:
            score += weight
        elif pct < -0.15:
            score -= weight
    if score >= 3:
        tone = "RISK_ON"
    elif score <= -3:
        tone = "RISK_OFF"
    else:
        tone = "MIXED"
    return {"tone": tone, "tone_score": score}


def get_global_markets() -> Dict[str, Any]:
    """
    Main entry. Returns dict with tone + per-market details.
    Individual fetch failures degrade gracefully through yfinance → Stooq.

    Negative-caching guard:
      If the resulting payload is incomplete (any market UNAVAILABLE), cache
      it for only 60s instead of the normal 5–30 min. This prevents a single
      transient outage from poisoning the cache for half an hour.
    """
    cached = _get_cached("global_markets_payload")
    if cached:
        return cached

    markets = []
    for cfg in MARKETS_CONFIG:
        entry = {
            "key": cfg["key"],
            "label": cfg["label"],
            "weight": cfg["weight"],
            "status": "UNAVAILABLE",
            "price": None, "change": None, "change_pct": None,
        }
        try:
            if cfg["source"] == "investing":
                data = _fetch_gift_nifty()
            else:
                # Two-tier fetch: yfinance → Stooq
                data = _fetch_with_fallback(cfg["ticker"])
            if data:
                entry.update(data)
        except Exception as e:
            logger.error(f"market {cfg['key']} fetch error: {e}")
        markets.append(entry)

    tone_info = _compute_tone(markets)
    payload = {
        "tone": tone_info["tone"],
        "tone_score": tone_info["tone_score"],
        "updated_at": datetime.now(IST).isoformat(),
        "markets": markets,
    }

    # Adaptive cache TTL — only commit failed payload to short cache,
    # so the next request retries quickly. Complete payloads use normal TTL.
    has_failures = any(m.get("status") == "UNAVAILABLE" for m in markets)
    if has_failures:
        _set_cache_short("global_markets_payload", payload)
        failed_keys = [m["key"] for m in markets if m["status"] == "UNAVAILABLE"]
        logger.warning(f"[global-markets] incomplete payload — {failed_keys} unavailable, short cache (60s)")
    else:
        _set_cache("global_markets_payload", payload)

    return payload
