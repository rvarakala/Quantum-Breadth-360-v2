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
                logger.warning(f"yfinance history empty for {ticker}")
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
        }
    except Exception as e:
        logger.warning(f"yfinance fetch failed for {ticker}: {e}")
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
        logger.warning(f"GIFT NIFTY scrape failed: {e} — falling back to ^NSEI")
        # Fallback: spot NIFTY from yfinance
        fallback = _fetch_yfinance("^NSEI")
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
    Individual fetch failures degrade gracefully.
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
                data = _fetch_yfinance(cfg["ticker"])
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
    _set_cache("global_markets_payload", payload)
    return payload
