"""
NSE Data Adapter — Cloudflare-Bypass Layer
==========================================
Replaces direct NSE API calls (which Cloudflare blocks) with two
reliable free sources:

SOURCE 1 — jugaad-data (NSE Archives / Bhavcopy)
  ✅ OHLCV historical (full NSE bhavcopy, 2020+)
  ✅ FII/DII institutional flow (NSE archives CSV)
  ✅ Insider trades (NSE PIT bhavcopy — CM-PIT-DISCLOSURE)
  ✅ No Cloudflare — hits nsearchives.nseindia.com (CDN, no bot protection)

SOURCE 2 — Indian Stock Market API (Koyeb-hosted yfinance wrapper)
  ✅ Fundamentals: PE, EPS, ROE, Market Cap, Sector per ticker
  ✅ Batch up to ~50 tickers per call
  ✅ No auth, no rate-limit issues for moderate use
  ❌ No historical OHLCV bars (current day only)
  ❌ No Insider / FII data

FALLBACK — yfinance .NS suffix
  ✅ Historical OHLCV (2Y+) — works perfectly from local machine
  ✅ Quarterly financials
  ❌ No Insider / FII data

Usage in other modules:
    from nse_data_adapter import (
        fetch_ohlcv_bhavcopy,       # OHLCV via jugaad bhavcopy
        fetch_fiidii_jugaad,         # FII/DII via jugaad archives
        fetch_insider_jugaad,        # Insider via jugaad PIT disclosure
        fetch_fundamentals_batch,    # Fundamentals via Indian Stock API
        fetch_fundamental_single,    # Single ticker fundamentals
    )
"""

import sqlite3
import logging
import time
import io
import csv
import os
import json
import httpx
from datetime import datetime, date, timedelta, timezone
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)
DB_PATH = Path(__file__).parent / "breadth_data.db"

# ── Indian Stock Market API (fundamentals) ────────────────────────────────────
INDIAN_API_BASE = "https://military-jobye-haiqstudios-14f59639.koyeb.app"
INDIAN_API_TIMEOUT = 15
INDIAN_API_BATCH_SIZE = 30   # safe batch size per call


# ══════════════════════════════════════════════════════════════════════════════
# SOURCE 1 — jugaad-data helpers
# ══════════════════════════════════════════════════════════════════════════════

def _get_jugaad_archives():
    """Return NSEArchives instance (lazy import)."""
    try:
        from jugaad_data.nse.archives import NSEArchives
        return NSEArchives()
    except ImportError:
        raise RuntimeError("jugaad-data not installed. Run: pip install jugaad-data")


def _trading_dates_range(start: date, end: date) -> list:
    """Generate weekday dates from start to end (Mon-Fri only, no holiday filter)."""
    dates = []
    cur = start
    while cur <= end:
        if cur.weekday() < 5:   # Mon=0 … Fri=4
            dates.append(cur)
        cur += timedelta(days=1)
    return dates


# ══════════════════════════════════════════════════════════════════════════════
# OHLCV via Bhavcopy
# ══════════════════════════════════════════════════════════════════════════════

def fetch_ohlcv_bhavcopy(
    symbols: list,
    start: date = None,
    end: date = None,
    days_back: int = 365,
) -> dict:
    """
    Fetch OHLCV data for given symbols from NSE bhavcopy archives.
    Returns {symbol: [{date, open, high, low, close, volume}, ...]}

    Uses nsearchives.nseindia.com — NOT blocked by Cloudflare.
    Handles both old format (pre-Jul 2024) and new UDiff format automatically.

    Args:
        symbols:   List of NSE symbols (e.g. ['RELIANCE', 'TCS'])
        start:     Start date (defaults to today - days_back)
        end:       End date (defaults to today)
        days_back: Days of history if start/end not provided
    """
    if not end:
        end = date.today()
    if not start:
        start = end - timedelta(days=days_back)

    symbols_set = {s.upper().strip() for s in symbols}
    result = {s: [] for s in symbols_set}

    try:
        nse = _get_jugaad_archives()
    except RuntimeError as e:
        logger.error(str(e))
        return result

    trading_days = _trading_dates_range(start, end)
    logger.info(f"Fetching bhavcopy for {len(symbols_set)} symbols across "
                f"{len(trading_days)} trading days ({start} → {end})")

    errors = 0
    for dt in trading_days:
        try:
            raw = nse.full_bhavcopy_raw(dt)
            if not raw or len(raw) < 100:
                continue

            reader = csv.DictReader(io.StringIO(raw))
            # Normalise column names — old format vs UDiff have different cases
            for row in reader:
                # Normalise keys: strip whitespace, uppercase
                row = {k.strip().upper(): v.strip() for k, v in row.items()}

                # Symbol field varies: SYMBOL or TckrSymb
                sym = (row.get("SYMBOL") or row.get("TCKRSYMB") or "").upper().strip()
                if sym not in symbols_set:
                    continue

                # Series filter — EQ only
                series = (row.get("SERIES") or row.get("SRS") or "").strip().upper()
                if series and series not in ("EQ", "BE", "BZ", ""):
                    continue

                # Parse OHLCV — field names differ between formats
                def _f(*keys):
                    for k in keys:
                        v = row.get(k.upper())
                        if v is not None:
                            try:
                                return float(str(v).replace(",", "").strip())
                            except:
                                pass
                    return None

                o = _f("OPEN", "OPENPRIC")
                h = _f("HIGH", "HIPRIC")
                lo = _f("LOW", "LOPRIC")
                c = _f("CLOSE", "CLSPRIC", "LASTPRIC")
                vol = _f("TOTTRDQTY", "VOLUME", "TOTALTRADEDQUANTITY")

                if c is None:
                    continue

                result[sym].append({
                    "date":   dt.isoformat(),
                    "open":   o or c,
                    "high":   h or c,
                    "low":    lo or c,
                    "close":  c,
                    "volume": int(vol) if vol else 0,
                })

            time.sleep(0.1)   # polite delay

        except Exception as e:
            errors += 1
            logger.debug(f"Bhavcopy error for {dt}: {e}")
            if errors > 10:
                logger.warning("Too many bhavcopy errors — aborting early")
                break

    # Sort each symbol oldest → newest
    for sym in result:
        result[sym].sort(key=lambda x: x["date"])

    total_bars = sum(len(v) for v in result.values())
    logger.info(f"Bhavcopy fetch complete: {total_bars} bars for {len(symbols_set)} symbols")
    return result


def store_ohlcv_bhavcopy(symbols: list, days_back: int = 365, market: str = "india") -> dict:
    """
    Fetch bhavcopy OHLCV and store directly into the ohlcv table.
    Returns summary dict.
    """
    data = fetch_ohlcv_bhavcopy(symbols, days_back=days_back)

    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    stored = 0
    now = datetime.now(timezone.utc).isoformat()

    for symbol, bars in data.items():
        for bar in bars:
            try:
                conn.execute("""
                    INSERT OR IGNORE INTO ohlcv
                    (symbol, date, open, high, low, close, volume, market, fetched_at)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    symbol, bar["date"],
                    bar["open"], bar["high"], bar["low"], bar["close"], bar["volume"],
                    market, now,
                ))
                stored += 1
            except Exception as e:
                logger.debug(f"OHLCV insert error {symbol} {bar['date']}: {e}")

    conn.commit()
    conn.close()

    total_bars = sum(len(v) for v in data.values())
    logger.info(f"Stored {stored}/{total_bars} OHLCV bars via bhavcopy")
    return {
        "status": "ok",
        "symbols": len(data),
        "bars_fetched": total_bars,
        "bars_stored": stored,
    }


# ══════════════════════════════════════════════════════════════════════════════
# FII/DII via jugaad Archives
# NSE publishes FII/DII data in their daily reports (CM-FII-STATS or similar)
# Fallback: parse from the investor-wise trading CSV on nsearchives
# ══════════════════════════════════════════════════════════════════════════════

def fetch_fiidii_jugaad(days_back: int = 30) -> dict:
    """
    Fetch FII/DII data using jugaad-data NSEArchives.
    Hits nsearchives.nseindia.com — no Cloudflare protection.

    Returns {status, entries, data: [{date, fii_buy, fii_sell, fii_net, dii_buy, ...}]}
    """
    try:
        nse = _get_jugaad_archives()
    except RuntimeError as e:
        return {"status": "error", "message": str(e), "entries": 0}

    end = date.today()
    start = end - timedelta(days=days_back)
    trading_days = _trading_dates_range(start, end)

    # FII/DII is published in NSE's "investor-wise" bhavcopy section
    # The archive URL pattern: /content/fo/fii{DD}{MMM}{YYYY}.csv — but that's F&O
    # For cash market FII/DII, use the NSE daily reports API via jugaad
    results = []

    try:
        # Try the daily reports API first (current + previous day)
        daily = nse.daily_reports
        reports = daily.get_daily_reports("CM")

        # Look for FII stats file
        fii_keys = [k for item_list in [reports.get("CurrentDay", []),
                                         reports.get("PreviousDay", [])]
                    for k in [item_list] if isinstance(item_list, list)
                    for item in item_list
                    for k in [item.get("fileKey", "")] if "FII" in k.upper()]

        if fii_keys:
            logger.info(f"Found FII report keys: {fii_keys}")
    except Exception as e:
        logger.debug(f"Daily reports probe failed: {e}")

    # Primary method: scrape NSE archives investor-wise data
    # URL: https://nsearchives.nseindia.com/content/equities/fii{dd}{MMM}{yyyy}.csv
    import requests

    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/134.0.6998.166 Safari/537.36",
        "Accept-Encoding": "gzip, deflate",
        "DNT": "1",
    })

    base = "https://nsearchives.nseindia.com"
    stored_count = 0

    for dt in reversed(trading_days[-30:]):   # last 30 trading days max
        dd  = dt.strftime("%d")
        MMM = dt.strftime("%b").upper()
        yyyy = str(dt.year)

        # NSE archives FII/DII CSV URL
        url = f"{base}/content/fo/fii{dd}{MMM}{yyyy}.csv"
        try:
            r = session.get(url, timeout=8)
            if r.status_code != 200:
                continue

            text = r.text
            if not text or len(text) < 50:
                continue

            entry = _parse_fiidii_csv(text, dt.isoformat())
            if entry:
                results.append(entry)
                stored_count += 1

        except Exception as e:
            logger.debug(f"FII/DII archive fetch failed {dt}: {e}")

        time.sleep(0.15)

    if not results:
        return {
            "status": "no_data",
            "message": "NSE archives returned no FII/DII data. "
                       "Try manual Force Sync or CSV import.",
            "entries": 0,
        }

    # Store to DB
    _store_fiidii_to_db(results)

    return {
        "status": "ok",
        "entries": stored_count,
        "latest_date": max(r["date"] for r in results),
        "source": "jugaad/nsearchives",
        "data": results,
    }


def _parse_fiidii_csv(text: str, iso_date: str) -> Optional[dict]:
    """Parse NSE FII/DII CSV from nsearchives."""
    try:
        reader = csv.DictReader(io.StringIO(text))
        fii_buy = fii_sell = fii_net = 0.0
        dii_buy = dii_sell = dii_net = 0.0

        for row in reader:
            row = {k.strip().upper(): v.strip() for k, v in row.items() if k}
            cat = (row.get("CATEGORY") or row.get("CLIENT TYPE") or "").upper()

            def _cr(k):
                v = row.get(k) or row.get(k.replace(" ", "_")) or "0"
                try:
                    return float(str(v).replace(",", "").strip() or "0")
                except:
                    return 0.0

            if "FII" in cat or "FPI" in cat:
                fii_buy  = _cr("BUY VALUE") or _cr("PURCHASE VALUE")
                fii_sell = _cr("SELL VALUE") or _cr("SALES VALUE")
                fii_net  = _cr("NET VALUE") or (fii_buy - fii_sell)
            elif "DII" in cat or "MF" in cat or "DOMESTIC" in cat:
                dii_buy  = _cr("BUY VALUE") or _cr("PURCHASE VALUE")
                dii_sell = _cr("SELL VALUE") or _cr("SALES VALUE")
                dii_net  = _cr("NET VALUE") or (dii_buy - dii_sell)

        if fii_buy == 0 and dii_buy == 0:
            return None

        return {
            "date": iso_date,
            "fii_buy": round(fii_buy, 2),
            "fii_sell": round(fii_sell, 2),
            "fii_net": round(fii_net, 2),
            "dii_buy": round(dii_buy, 2),
            "dii_sell": round(dii_sell, 2),
            "dii_net": round(dii_net, 2),
        }
    except Exception as e:
        logger.debug(f"FII/DII CSV parse error: {e}")
        return None


def _store_fiidii_to_db(entries: list):
    """Store FII/DII entries to fiidii table."""
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS fiidii (
            date TEXT PRIMARY KEY,
            fii_buy REAL DEFAULT 0, fii_sell REAL DEFAULT 0, fii_net REAL DEFAULT 0,
            dii_buy REAL DEFAULT 0, dii_sell REAL DEFAULT 0, dii_net REAL DEFAULT 0,
            fetched_at TEXT
        )
    """)
    now = datetime.now(timezone.utc).isoformat()
    for e in entries:
        conn.execute("""
            INSERT OR REPLACE INTO fiidii
            (date, fii_buy, fii_sell, fii_net, dii_buy, dii_sell, dii_net, fetched_at)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """, (e["date"], e["fii_buy"], e["fii_sell"], e["fii_net"],
              e["dii_buy"], e["dii_sell"], e["dii_net"], now))
    conn.commit()
    conn.close()


# ══════════════════════════════════════════════════════════════════════════════
# INSIDER via jugaad / NSE PIT Disclosure Archive
# NSE publishes PIT disclosures as downloadable CSVs on nsearchives
# ══════════════════════════════════════════════════════════════════════════════

def fetch_insider_jugaad(days_back: int = 30) -> dict:
    """
    Fetch NSE PIT insider trading disclosures from nsearchives.
    URL pattern: https://nsearchives.nseindia.com/corporate/xbrl/pit/
    Also tries the NSE daily reports CM-PIT-DISCLOSURE file key.

    Returns {status, fetched, stored}
    """
    try:
        nse = _get_jugaad_archives()
    except RuntimeError as e:
        return {"status": "error", "message": str(e), "fetched": 0}

    import requests
    session = requests.Session()
    session.headers.update({
        "User-Agent": "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36 "
                      "(KHTML, like Gecko) Chrome/134.0.6998.166 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,*/*",
        "Accept-Encoding": "gzip, deflate",
        "Referer": "https://www.nseindia.com/companies-listing/corporate-filings-insider-trading",
        "DNT": "1",
    })

    base = "https://nsearchives.nseindia.com"
    all_trades = []
    errors = 0

    end = date.today()
    start = end - timedelta(days=days_back)
    trading_days = _trading_dates_range(start, end)

    for dt in reversed(trading_days):
        dd   = dt.strftime("%d")
        mm   = dt.strftime("%m")
        MMM  = dt.strftime("%b").upper()
        yyyy = str(dt.year)

        # NSE PIT archive URL patterns (NSE has changed these over time)
        urls = [
            f"{base}/corporate/xbrl/pit/pit_{dd}{MMM}{yyyy}.csv",
            f"{base}/corporate/pit/pit_{dd}{mm}{yyyy}.csv",
            f"{base}/content/equities/pit{dd}{MMM}{yyyy}.csv",
        ]

        fetched_for_day = False
        for url in urls:
            try:
                r = session.get(url, timeout=10)
                if r.status_code == 200 and len(r.text) > 200:
                    trades = _parse_insider_pit_csv(r.text)
                    if trades:
                        all_trades.extend(trades)
                        fetched_for_day = True
                        logger.debug(f"Fetched {len(trades)} PIT records for {dt} via archives")
                        break
            except Exception as e:
                logger.debug(f"PIT URL {url} failed: {e}")

        if not fetched_for_day:
            errors += 1

        time.sleep(0.15)

    # Also try the daily reports API (CM-PIT-DISCLOSURE key) for today/yesterday
    try:
        daily = nse.daily_reports
        for file_key in ["CM-PIT-DISCLOSURE", "CM-INSIDER-TRADING"]:
            try:
                content = daily.download_file(file_key, segment="CM")
                if content:
                    # Save to temp file and parse
                    import tempfile, zipfile
                    try:
                        fp = io.BytesIO(content)
                        with zipfile.ZipFile(fp) as zf:
                            with zf.open(zf.namelist()[0]) as f:
                                text = f.read().decode("utf-8")
                    except:
                        text = content.decode("utf-8", errors="replace")

                    trades = _parse_insider_pit_csv(text)
                    if trades:
                        all_trades.extend(trades)
                        logger.info(f"Got {len(trades)} PIT trades via daily reports {file_key}")
                        break
            except Exception as e:
                logger.debug(f"Daily reports {file_key} failed: {e}")
    except Exception as e:
        logger.debug(f"Daily reports probe failed: {e}")

    if not all_trades:
        return {
            "status": "no_data",
            "message": "No PIT disclosure data found in NSE archives. "
                       "Use manual CSV import from NSE website as fallback.",
            "fetched": 0,
            "stored": 0,
        }

    # Deduplicate
    seen = set()
    unique_trades = []
    for t in all_trades:
        key = (t["symbol"], t.get("insider_name", ""), t.get("transaction_date", ""),
                t.get("transaction_type", ""), t.get("securities_count", 0))
        if key not in seen:
            seen.add(key)
            unique_trades.append(t)

    stored = _store_insider_trades(unique_trades)

    return {
        "status": "ok",
        "fetched": len(unique_trades),
        "stored": stored,
        "source": "jugaad/nsearchives",
        "days_covered": days_back,
    }


def _parse_insider_pit_csv(text: str) -> list:
    """
    Parse NSE PIT disclosure CSV from nsearchives.
    NSE PIT CSV columns (29 cols):
    SYMBOL, COMPANY, REGULATION, NAME OF ACQUIRER, CATEGORY OF PERSON,
    TYPE OF SECURITY (PRIOR), NO. OF SECURITY (PRIOR), % SHAREHOLDING (PRIOR),
    TYPE OF SECURITY (ACQUIRED), NO. OF SECURITIES (ACQUIRED), VALUE OF SECURITY (ACQUIRED),
    ACQUISITION/DISPOSAL TRANSACTION TYPE, ...DATE OF ACQUISITION FROM...
    MODE OF ACQUISITION, EXCHANGE, REMARK, BROADCASTE DATE AND TIME
    """
    trades = []
    try:
        # Strip BOM if present
        text = text.lstrip("\ufeff")
        reader = csv.reader(io.StringIO(text))
        raw_headers = next(reader, None)
        if not raw_headers:
            return []

        headers = [h.strip().replace("\n", "").upper() for h in raw_headers]

        def _get(row_dict, *key_parts):
            for part in key_parts:
                for k, v in row_dict.items():
                    if part.upper() in k:
                        return (v or "").strip()
            return ""

        def _num(s):
            if not s:
                return 0.0
            try:
                return float(str(s).replace(",", "").strip() or "0")
            except:
                return 0.0

        def _norm_date(ds):
            if not ds:
                return ""
            ds = ds.strip()
            if " " in ds and ":" in ds:
                ds = ds.split(" ")[0]
            for fmt in ("%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y"):
                try:
                    return datetime.strptime(ds, fmt).strftime("%Y-%m-%d")
                except:
                    continue
            return ds

        for row in reader:
            if len(row) < 10:
                continue
            row_dict = {}
            for i, h in enumerate(headers):
                row_dict[h] = row[i].strip() if i < len(row) else ""

            symbol = _get(row_dict, "SYMBOL")
            if not symbol or symbol == "-":
                continue

            company   = _get(row_dict, "COMPANY")
            insider   = _get(row_dict, "NAME OF THE ACQUIRER", "ACQUIRER/DISPOSER", "NAME OF ACQUIRER")
            category  = _get(row_dict, "CATEGORY OF PERSON")

            tx_raw    = _get(row_dict, "ACQUISITION/DISPOSAL TRANSACTION TYPE", "TRANSACTION TYPE")
            tx_lower  = tx_raw.lower()
            if "buy" in tx_lower or "acqui" in tx_lower:
                tx_type = "Buy"
            elif "sell" in tx_lower or "sale" in tx_lower or "dispos" in tx_lower:
                tx_type = "Sell"
            elif "pledge" in tx_lower:
                tx_type = "Pledge"
            elif "revoke" in tx_lower or "invocation" in tx_lower:
                tx_type = "Revoke"
            else:
                tx_type = tx_raw or "Unknown"

            sec_count = _num(_get(row_dict, "NO. OF SECURITIES (ACQUIRED", "NO. OF SECURITIES"))
            sec_value = _num(_get(row_dict, "VALUE OF SECURITY (ACQUIRED", "VALUE OF SECURITY"))

            tx_date   = _get(row_dict, "DATE OF ALLOTMENT/ACQUISITION FROM", "ACQUISITION FROM")
            mode      = _get(row_dict, "MODE OF ACQUISITION", "MODE")
            exchange  = _get(row_dict, "EXCHANGE") or "NSE"
            broadcast = _get(row_dict, "BROADCASTE DATE", "BROADCAST DATE")

            trades.append({
                "symbol":           symbol.upper(),
                "company":          company,
                "insider_name":     insider,
                "category":         category,
                "transaction_type": tx_type,
                "securities_count": sec_count,
                "securities_value": sec_value,
                "transaction_date": _norm_date(tx_date),
                "mode":             mode if mode != "-" else "",
                "exchange":         exchange if exchange != "-" else "NSE",
                "broadcast_date":   _norm_date(broadcast.split(" ")[0] if broadcast else ""),
                "remarks":          _get(row_dict, "REMARK"),
            })

    except Exception as e:
        logger.error(f"PIT CSV parse error: {e}")

    return trades


def _store_insider_trades(trades: list) -> int:
    """Store parsed insider trades to DB. Returns count inserted."""
    if not trades:
        return 0

    conn = sqlite3.connect(str(DB_PATH), timeout=15)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS insider_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            company TEXT,
            insider_name TEXT,
            category TEXT,
            transaction_type TEXT,
            securities_count REAL,
            securities_value REAL,
            transaction_date TEXT,
            mode TEXT,
            exchange TEXT DEFAULT 'NSE',
            broadcast_date TEXT,
            remarks TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, insider_name, transaction_date, transaction_type, securities_count)
        )
    """)
    count = 0
    for t in trades:
        try:
            conn.execute("""
                INSERT OR IGNORE INTO insider_trades
                (symbol, company, insider_name, category, transaction_type,
                 securities_count, securities_value, transaction_date, mode,
                 exchange, broadcast_date, remarks)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                t["symbol"], t["company"], t["insider_name"],
                t["category"], t["transaction_type"],
                t["securities_count"], t["securities_value"],
                t["transaction_date"], t["mode"], t["exchange"],
                t["broadcast_date"], t.get("remarks", ""),
            ))
            count += 1
        except Exception as e:
            logger.debug(f"Insider insert error: {e}")
    conn.commit()
    conn.close()
    return count


# ══════════════════════════════════════════════════════════════════════════════
# SOURCE 2 — Indian Stock Market API (Fundamentals)
# Base URL: https://military-jobye-haiqstudios-14f59639.koyeb.app
# ══════════════════════════════════════════════════════════════════════════════

def _indian_api_get(path: str, params: dict = None, timeout: int = INDIAN_API_TIMEOUT) -> Optional[dict]:
    """Make a GET request to the Indian Stock Market API."""
    url = INDIAN_API_BASE + path
    try:
        with httpx.Client(timeout=timeout, follow_redirects=True) as client:
            r = client.get(url, params=params or {})
        if r.status_code == 200:
            return r.json()
        logger.warning(f"Indian API {path} returned {r.status_code}")
    except Exception as e:
        logger.warning(f"Indian API request failed: {e}")
    return None


def fetch_fundamental_single(symbol: str) -> Optional[dict]:
    """
    Fetch fundamentals for one ticker from Indian Stock Market API.
    Returns normalised dict matching tv_fundamentals schema.
    """
    data = _indian_api_get("/stock", {"symbol": symbol + ".NS", "res": "num"})
    if not data or data.get("status") != "success":
        return None

    d = data.get("data", {})
    return _normalise_indian_api_record(symbol.upper(), d,
                                        data.get("exchange", "NSE"))


def fetch_fundamentals_batch(symbols: list) -> dict:
    """
    Fetch fundamentals for a batch of tickers from Indian Stock Market API.
    Automatically chunks into INDIAN_API_BATCH_SIZE groups.
    Returns {SYMBOL: normalised_dict, ...}

    Also stores results into tv_fundamentals SQLite table.
    """
    symbols = [s.upper().strip() for s in symbols if s]
    result = {}
    chunks = [symbols[i:i + INDIAN_API_BATCH_SIZE]
              for i in range(0, len(symbols), INDIAN_API_BATCH_SIZE)]

    logger.info(f"Fetching Indian API fundamentals: {len(symbols)} tickers "
                f"in {len(chunks)} batches")

    for i, chunk in enumerate(chunks):
        symbols_str = ",".join(s + ".NS" for s in chunk)
        data = _indian_api_get("/stock/list", {"symbols": symbols_str, "res": "num"})

        if not data or data.get("status") != "success":
            logger.warning(f"Indian API batch {i+1} failed")
            time.sleep(1)
            continue

        for stock in data.get("stocks", []):
            sym = stock.get("symbol", "").upper().strip()
            if not sym:
                continue
            entry = _normalise_indian_api_record(sym, stock, stock.get("exchange", "NSE"))
            if entry:
                result[sym] = entry

        logger.info(f"Batch {i+1}/{len(chunks)}: got {len(data.get('stocks',[]))} records")

        if i < len(chunks) - 1:
            time.sleep(0.5)   # polite delay between batches

    # Store to DB
    if result:
        _store_fundamentals_to_db(result)

    return result


def _normalise_indian_api_record(symbol: str, d: dict, exchange: str = "NSE") -> Optional[dict]:
    """
    Normalise Indian Stock Market API response to tv_fundamentals schema.
    Both /stock and /stock/list return same field names.
    """
    def _f(key, default=None):
        v = d.get(key)
        if v is None:
            return default
        try:
            import math
            f = float(v)
            return None if (math.isnan(f) or math.isinf(f)) else f
        except:
            return default

    pe     = _f("pe_ratio")
    eps    = _f("earnings_per_share")
    mktcap = _f("market_cap")
    bv     = _f("book_value")
    div    = _f("dividend_yield")
    sector = str(d.get("sector") or "").strip()
    name   = str(d.get("company_name") or symbol).strip()

    # P/B = price / book_value
    price  = _f("last_price")
    pb     = round(price / bv, 2) if price and bv and bv > 0 else None

    # ROE proxy: EPS / Book Value * 100 (when not provided directly)
    roe    = round(eps / bv * 100, 1) if eps and bv and bv > 0 else None

    if pe is None and eps is None and mktcap is None:
        return None   # completely empty — skip

    return {
        "pe_ratio":         pe,
        "pb_ratio":         pb,
        "roe":              roe,
        "roa":              None,       # not provided by this API
        "gross_margin":     None,
        "operating_margin": None,
        "net_margin":       None,
        "debt_to_equity":   None,       # not provided
        "current_ratio":    None,
        "eps_ttm":          eps,
        "eps_growth_ttm":   None,
        "revenue_ttm":      None,
        "revenue_growth":   None,
        "market_cap":       mktcap,
        "company_name":     name,
        "sector":           sector,
        "industry":         str(d.get("industry") or "").strip(),
        "dividend_yield":   div,
        "book_value":       bv,
        "last_price":       price,
        "source":           "indian_api",
    }


def _store_fundamentals_to_db(data: dict):
    """
    Store fundamentals from Indian API into tv_fundamentals table.
    Only fills fields not already populated by TradingView batch
    (i.e. uses INSERT OR IGNORE for existing rows, UPDATE for missing fields).
    """
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    now = datetime.now(timezone.utc).isoformat()
    updated = 0

    for ticker, entry in data.items():
        try:
            # Check if row exists from TradingView (prefer TV data for ratios)
            existing = conn.execute(
                "SELECT pe_ratio, roe, fetched_at FROM tv_fundamentals WHERE ticker = ?",
                (ticker,)
            ).fetchone()

            if existing is None:
                # No existing row — full insert
                conn.execute("""
                    INSERT OR IGNORE INTO tv_fundamentals
                    (ticker, pe_ratio, pb_ratio, roe, roa, gross_margin, operating_margin,
                     net_margin, debt_to_equity, current_ratio, eps_ttm, eps_growth_ttm,
                     revenue_ttm, revenue_growth, market_cap, company_name, sector, industry,
                     fetched_at)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                """, (
                    ticker,
                    entry["pe_ratio"],   entry["pb_ratio"],
                    entry["roe"],        entry["roa"],
                    entry["gross_margin"], entry["operating_margin"],
                    entry["net_margin"], entry["debt_to_equity"],
                    entry["current_ratio"],
                    entry["eps_ttm"],    entry["eps_growth_ttm"],
                    entry["revenue_ttm"], entry["revenue_growth"],
                    entry["market_cap"],
                    entry["company_name"], entry["sector"], entry["industry"],
                    now,
                ))
            else:
                # Existing row — only fill in NULL fields (don't overwrite TV data)
                conn.execute("""
                    UPDATE tv_fundamentals SET
                        pe_ratio    = COALESCE(pe_ratio, ?),
                        eps_ttm     = COALESCE(eps_ttm, ?),
                        market_cap  = COALESCE(market_cap, ?),
                        company_name = CASE WHEN company_name IS NULL OR company_name = ''
                                       THEN ? ELSE company_name END,
                        sector      = CASE WHEN sector IS NULL OR sector = ''
                                     THEN ? ELSE sector END
                    WHERE ticker = ?
                """, (
                    entry["pe_ratio"], entry["eps_ttm"], entry["market_cap"],
                    entry["company_name"], entry["sector"],
                    ticker,
                ))
            updated += 1
        except Exception as e:
            logger.debug(f"Fundamentals DB store error {ticker}: {e}")

    conn.commit()
    conn.close()
    logger.info(f"Indian API fundamentals stored/updated: {updated} tickers")


# ══════════════════════════════════════════════════════════════════════════════
# COMBINED SYNC FUNCTIONS — Called from main.py
# ══════════════════════════════════════════════════════════════════════════════

def sync_fiidii_with_fallback(days_back: int = 30) -> dict:
    """
    Try NSE direct API first (existing fiidii.py), fall back to jugaad archives.
    Returns combined status dict.
    """
    # Try jugaad first (more reliable)
    logger.info("Attempting FII/DII sync via jugaad/nsearchives...")
    result = fetch_fiidii_jugaad(days_back=days_back)

    if result.get("status") == "ok" and result.get("entries", 0) > 0:
        return result

    # Fallback: existing NSE direct API (may work sometimes)
    logger.info("jugaad FII/DII failed — trying direct NSE API...")
    try:
        from fiidii import fetch_fiidii_from_nse
        nse_result = fetch_fiidii_from_nse()
        if isinstance(nse_result, dict) and "error" not in nse_result:
            return {**nse_result, "source": "nse_direct_api"}
    except Exception as e:
        logger.warning(f"Direct NSE FII/DII also failed: {e}")

    return {
        "status": "no_data",
        "message": "Both jugaad archives and direct NSE API failed for FII/DII. "
                   "Check your network connection.",
        "entries": 0,
    }


def sync_insider_with_fallback(days_back: int = 30) -> dict:
    """
    Try jugaad nsearchives PIT first, fall back to direct NSE API.
    Returns combined status dict.
    """
    logger.info("Attempting Insider sync via jugaad/nsearchives PIT...")
    result = fetch_insider_jugaad(days_back=days_back)

    if result.get("status") == "ok" and result.get("fetched", 0) > 0:
        return result

    # Fallback: existing sync_insider_data (NSE direct API)
    logger.info("jugaad Insider failed — trying direct NSE API...")
    try:
        from insider import sync_insider_data
        nse_result = sync_insider_data(days_back=days_back)
        if nse_result.get("status") == "ok":
            return {**nse_result, "source": "nse_direct_api"}
    except Exception as e:
        logger.warning(f"Direct NSE Insider also failed: {e}")

    return {
        "status": "no_data",
        "message": "Both jugaad PIT archives and direct NSE API failed for Insider. "
                   "Use manual CSV import from NSE website.",
        "fetched": 0,
        "stored": 0,
    }


def sync_fundamentals_indian_api(symbols: list = None, market: str = "india") -> dict:
    """
    Sync fundamentals for all symbols in OHLCV table using Indian Stock Market API.
    Falls back gracefully if API is unreachable.
    """
    if not symbols:
        # Pull all unique symbols from OHLCV table
        try:
            conn = sqlite3.connect(str(DB_PATH), timeout=10)
            rows = conn.execute(
                "SELECT DISTINCT ticker FROM ohlcv WHERE market = ? LIMIT 2000",
                (market,)
            ).fetchall()
            conn.close()
            symbols = [r[0] for r in rows]
        except Exception as e:
            return {"status": "error", "message": str(e)}

    if not symbols:
        return {"status": "no_data", "message": "No symbols found in OHLCV table"}

    logger.info(f"Indian API fundamentals sync: {len(symbols)} symbols")
    result = fetch_fundamentals_batch(symbols)

    return {
        "status": "ok" if result else "no_data",
        "tickers_synced": len(result),
        "tickers_requested": len(symbols),
        "source": "indian_stock_api",
    }


# ══════════════════════════════════════════════════════════════════════════════
# HEALTH CHECK — Test all sources
# ══════════════════════════════════════════════════════════════════════════════

def health_check() -> dict:
    """
    Test connectivity to all data sources.
    Returns {source: {reachable, latency_ms, note}}
    """
    results = {}

    # Test 1: Indian Stock Market API
    t0 = time.time()
    try:
        data = _indian_api_get("/", timeout=5)
        ms = round((time.time() - t0) * 1000)
        results["indian_stock_api"] = {
            "reachable": data is not None,
            "latency_ms": ms,
            "version": data.get("version") if data else None,
            "note": "Fundamentals: PE, EPS, Market Cap, Sector",
        }
    except Exception as e:
        results["indian_stock_api"] = {"reachable": False, "error": str(e)}

    # Test 2: NSE Archives (jugaad backbone)
    t0 = time.time()
    try:
        import requests
        r = requests.get(
            "https://nsearchives.nseindia.com/",
            timeout=5,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        ms = round((time.time() - t0) * 1000)
        results["nse_archives"] = {
            "reachable": r.status_code in (200, 301, 302, 403),
            "latency_ms": ms,
            "status_code": r.status_code,
            "note": "OHLCV bhavcopy, FII/DII, Insider PIT",
        }
    except Exception as e:
        results["nse_archives"] = {"reachable": False, "error": str(e)}

    # Test 3: jugaad-data installed
    try:
        import jugaad_data
        results["jugaad_data"] = {
            "reachable": True,
            "version": getattr(jugaad_data, "__version__", "installed"),
            "note": "Python library for NSE archives",
        }
    except ImportError:
        results["jugaad_data"] = {
            "reachable": False,
            "note": "Run: pip install jugaad-data",
        }

    # Test 4: Direct NSE API (likely blocked)
    t0 = time.time()
    try:
        import requests
        r = requests.get(
            "https://www.nseindia.com/api/marketStatus",
            timeout=5,
            headers={"User-Agent": "Mozilla/5.0"},
        )
        ms = round((time.time() - t0) * 1000)
        results["nse_direct_api"] = {
            "reachable": r.status_code == 200,
            "latency_ms": ms,
            "status_code": r.status_code,
            "note": "Direct NSE API (may be Cloudflare-blocked)",
        }
    except Exception as e:
        results["nse_direct_api"] = {
            "reachable": False,
            "error": str(e),
            "note": "Cloudflare blocking likely",
        }

    return results
