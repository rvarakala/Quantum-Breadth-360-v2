"""
Earnings Calendar — Overview Row 3 Card 4

Lists Indian companies reporting earnings in the next 7 trading days.
Primary source: NSE corporate event calendar.
Fallback:       Moneycontrol earnings calendar HTML scrape.

Cache: 4-hour SQLite cache (earnings_calendar_cache table).
       Date-keyed so the same day's calendar is reused across requests.

Returns:
{
  "as_of": "2026-04-20",
  "items": [
    {"date": "2026-04-21", "ticker": "TCS", "company": "Tata Consultancy",
     "type": "Q4 Results", "time": "Post-market", "source": "nse"},
    ...
  ],
  "count": 12,
  "next_session_count": 3,
  "fetched_at": "2026-04-20T15:30:00+05:30",
  "cache_age_hr": 0.2
}
"""
import logging
import time
import json
import sqlite3
import re
from datetime import datetime, timezone, timedelta, date
from pathlib import Path
from typing import Dict, List, Any, Optional

import requests

logger = logging.getLogger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))
DB_PATH = Path(__file__).parent / "breadth_data.db"

CACHE_TABLE = "earnings_calendar_cache"
CACHE_TTL_HOURS = 4
LOOKAHEAD_DAYS = 7

# Lazy-loaded set of NIFTY 500 tickers for universe filtering
_universe_cache: Optional[set] = None


def _ensure_cache_table():
    """Create the cache table if it doesn't exist. Idempotent."""
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {CACHE_TABLE} (
            cache_key  TEXT PRIMARY KEY,
            payload    TEXT,
            fetched_at TEXT
        )
    """)
    conn.commit()
    conn.close()


def _get_cached(key: str) -> Optional[Dict[str, Any]]:
    """Return cached payload if younger than CACHE_TTL_HOURS, else None."""
    _ensure_cache_table()
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    row = conn.execute(
        f"SELECT payload, fetched_at FROM {CACHE_TABLE} WHERE cache_key=?",
        (key,)
    ).fetchone()
    conn.close()
    if not row:
        return None
    try:
        fetched = datetime.fromisoformat(row[1])
        age_hr = (datetime.now(timezone.utc) - fetched).total_seconds() / 3600
        if age_hr >= CACHE_TTL_HOURS:
            return None
        payload = json.loads(row[0])
        payload["cache_age_hr"] = round(age_hr, 2)
        return payload
    except Exception as e:
        logger.warning(f"[earnings] cache parse failed: {e}")
        return None


def _set_cached(key: str, payload: Dict[str, Any]):
    _ensure_cache_table()
    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=10)
        conn.execute(
            f"INSERT OR REPLACE INTO {CACHE_TABLE} (cache_key, payload, fetched_at) VALUES (?, ?, ?)",
            (key, json.dumps(payload), datetime.now(timezone.utc).isoformat())
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"[earnings] cache write failed: {e}")


def _load_universe() -> set:
    """Load NIFTY 500 ticker set for universe filtering. Cached."""
    global _universe_cache
    if _universe_cache is not None:
        return _universe_cache
    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=10)
        rows = conn.execute(
            "SELECT ticker FROM ticker_universe WHERE market='India'"
        ).fetchall()
        conn.close()
        _universe_cache = {r[0].upper().replace(".NS", "") for r in rows if r[0]}
        logger.info(f"[earnings] universe loaded: {len(_universe_cache)} NSE tickers")
    except Exception as e:
        logger.warning(f"[earnings] universe load failed: {e}")
        _universe_cache = set()
    return _universe_cache


def _is_results_event(subject: str, purpose: str) -> bool:
    """True if the event looks like a quarterly/annual results announcement."""
    text = f"{subject} {purpose}".lower()
    keywords = ["financial result", "quarterly result", "annual result",
                "audited result", "unaudited result", "q1 result", "q2 result",
                "q3 result", "q4 result", "results"]
    return any(k in text for k in keywords)


def _detect_quarter_label(subject: str, purpose: str) -> str:
    """Try to extract a quarter label like 'Q4 Results' from the text."""
    text = f"{subject} {purpose}"
    m = re.search(r"\bQ([1-4])\b", text, re.IGNORECASE)
    if m:
        return f"Q{m.group(1)} Results"
    if "annual" in text.lower():
        return "Annual Results"
    if "audited" in text.lower():
        return "Audited Results"
    return "Results"


def _fetch_nse_calendar(from_date: date, to_date: date) -> List[Dict[str, Any]]:
    """
    Primary source: NSE corporate event calendar.

    Endpoint: https://www.nseindia.com/api/event-calendar?index=equities
    Returns JSON array of {symbol, company, purpose, bm_date, ...}

    NSE Cloudflare can block this. We use the same pattern jugaad-data uses:
    session cookie + browser-like headers. If it 403s, caller falls back to
    Moneycontrol scrape.
    """
    url = "https://www.nseindia.com/api/event-calendar"
    headers = {
        "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                       "AppleWebKit/537.36 (KHTML, like Gecko) "
                       "Chrome/120.0.0.0 Safari/537.36"),
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nseindia.com/companies-listing/corporate-filings-event-calendar",
    }
    try:
        sess = requests.Session()
        sess.headers.update(headers)
        # Warm cookies by hitting the main page first (NSE Cloudflare trick)
        sess.get("https://www.nseindia.com", timeout=5)
        r = sess.get(url, params={"index": "equities"}, timeout=10)
        if r.status_code != 200:
            logger.warning(f"[earnings] NSE event-calendar http={r.status_code}")
            return []
        data = r.json()
        if not isinstance(data, list):
            logger.warning(f"[earnings] NSE event-calendar unexpected shape: {type(data)}")
            return []

        items = []
        universe = _load_universe()
        for row in data:
            try:
                ticker = (row.get("symbol") or "").upper().strip()
                if not ticker:
                    continue
                if universe and ticker not in universe:
                    continue   # filter to NIFTY 500 only

                # Parse the event date — NSE uses 'bm_date' (board meeting date)
                # in DD-MMM-YYYY format, e.g. "21-Apr-2026"
                date_str = row.get("bm_date") or row.get("eventDate") or ""
                if not date_str:
                    continue
                try:
                    event_dt = datetime.strptime(date_str, "%d-%b-%Y").date()
                except ValueError:
                    try:
                        event_dt = datetime.strptime(date_str, "%Y-%m-%d").date()
                    except ValueError:
                        continue

                if event_dt < from_date or event_dt > to_date:
                    continue

                subject = row.get("subject", "") or ""
                purpose = row.get("purpose", "") or ""
                if not _is_results_event(subject, purpose):
                    continue

                items.append({
                    "date":    event_dt.isoformat(),
                    "ticker":  ticker,
                    "company": row.get("companyName") or row.get("company") or ticker,
                    "type":    _detect_quarter_label(subject, purpose),
                    "time":    "TBA",   # NSE doesn't publish pre/post-market timing
                    "source":  "nse",
                })
            except Exception as e:
                logger.debug(f"[earnings] NSE row parse error: {e}")
                continue

        logger.info(f"[earnings] NSE: {len(items)} results events in {from_date} → {to_date}")
        return items
    except Exception as e:
        logger.warning(f"[earnings] NSE event-calendar failed: {e}")
        return []


def _fetch_moneycontrol_calendar(from_date: date, to_date: date) -> List[Dict[str, Any]]:
    """
    Fallback: scrape Moneycontrol earnings calendar HTML.

    Page: https://www.moneycontrol.com/stocks/marketstats/earnings_calendar.php
    Layout has a table of date | company | sector | event_type.

    Parses with regex (no BS4 dependency required). If the HTML structure
    changes, this gracefully returns empty list and we keep NSE results
    only (or nothing if NSE also failed).
    """
    url = "https://www.moneycontrol.com/stocks/marketstats/earnings_calendar.php"
    try:
        r = requests.get(url, timeout=10, headers={
            "User-Agent": ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                           "AppleWebKit/537.36 (KHTML, like Gecko) "
                           "Chrome/120.0.0.0 Safari/537.36"),
            "Accept-Language": "en-US,en;q=0.9",
        })
        if r.status_code != 200:
            logger.warning(f"[earnings] Moneycontrol http={r.status_code}")
            return []
        html = r.text

        # Each row is approximately:
        #   <tr><td>21-Apr-2026</td><td><a href="...">TCS</a></td>
        #       <td>IT</td><td>Q4 FY26 Earnings</td></tr>
        # Robust regex: capture date, anchor text (ticker), and event type.
        pattern = re.compile(
            r"<tr[^>]*>\s*<td[^>]*>\s*(\d{1,2}[- ][A-Za-z]{3}[- ]\d{4})\s*</td>"
            r"\s*<td[^>]*>\s*(?:<a[^>]*>)?\s*([A-Z][A-Z0-9 &.\-]{1,40})\s*(?:</a>)?\s*</td>"
            r"[\s\S]*?<td[^>]*>([^<]{3,80})</td>",
            re.IGNORECASE,
        )

        items = []
        universe = _load_universe()
        for m in pattern.finditer(html):
            try:
                date_str = m.group(1).strip()
                company  = m.group(2).strip()
                event    = m.group(3).strip()
                try:
                    event_dt = datetime.strptime(date_str.replace("-", " "), "%d %b %Y").date()
                except ValueError:
                    continue
                if event_dt < from_date or event_dt > to_date:
                    continue
                # MC uses company name, not ticker — try to match against universe
                ticker_guess = company.split()[0].upper()
                if universe and ticker_guess not in universe:
                    # Allow it through but flag as unverified — MC names like
                    # "Tata Consultancy Services" need fuzzy match to TCS
                    ticker_guess = ""
                items.append({
                    "date":    event_dt.isoformat(),
                    "ticker":  ticker_guess,
                    "company": company,
                    "type":    event[:40],
                    "time":    "TBA",
                    "source":  "moneycontrol",
                })
            except Exception:
                continue
        logger.info(f"[earnings] Moneycontrol: {len(items)} events parsed")
        return items
    except Exception as e:
        logger.warning(f"[earnings] Moneycontrol fallback failed: {e}")
        return []


def _dedupe(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Dedupe by (date, ticker). Prefer NSE source over Moneycontrol."""
    by_key: Dict[tuple, Dict[str, Any]] = {}
    priority = {"nse": 0, "moneycontrol": 1}
    for it in items:
        key = (it.get("date"), it.get("ticker", "").upper())
        if not key[0] or not key[1]:
            # Items without ticker — dedup by (date, company)
            key = (it.get("date"), (it.get("company", "")).upper())
        existing = by_key.get(key)
        if not existing:
            by_key[key] = it
        else:
            # Keep higher-priority source
            if priority.get(it["source"], 9) < priority.get(existing["source"], 9):
                by_key[key] = it
    return list(by_key.values())


def get_earnings_calendar() -> Dict[str, Any]:
    """Main entry. Cached for 4 hours per IST date."""
    today_ist = datetime.now(IST).date()
    cache_key = f"earnings_{today_ist.isoformat()}"

    cached = _get_cached(cache_key)
    if cached:
        logger.info(f"[earnings] served from cache (age {cached.get('cache_age_hr', 0)}hr)")
        return cached

    from_date = today_ist
    to_date   = today_ist + timedelta(days=LOOKAHEAD_DAYS)

    # 1. Try NSE primary
    items = _fetch_nse_calendar(from_date, to_date)
    nse_count = len(items)

    # 2. Fallback to Moneycontrol if NSE returned nothing or very little
    if nse_count < 3:
        logger.info(f"[earnings] NSE returned {nse_count} items — adding Moneycontrol fallback")
        mc_items = _fetch_moneycontrol_calendar(from_date, to_date)
        items.extend(mc_items)
        items = _dedupe(items)

    # Sort by date ascending, then ticker
    items.sort(key=lambda x: (x.get("date", ""), x.get("ticker", "")))

    # Count items on the very next trading day (or today if today is a trading day)
    next_session_str = (today_ist + timedelta(days=1)).isoformat()
    next_session_count = sum(1 for it in items if it.get("date") == next_session_str)

    payload = {
        "as_of": today_ist.isoformat(),
        "items": items,
        "count": len(items),
        "next_session_count": next_session_count,
        "fetched_at": datetime.now(IST).isoformat(),
        "cache_age_hr": 0,
    }
    _set_cached(cache_key, payload)
    return payload
