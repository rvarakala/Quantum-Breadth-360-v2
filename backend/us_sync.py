"""
US Market Sync — Russell 3000 Universe
Downloads OHLCV data for US stocks via yfinance in fast batches.
Universe sourced from iShares IWV (Russell 3000 ETF) holdings CSV.

Speed optimizations:
- Bulk yfinance downloads (50 tickers per batch)
- Parallel batches (3 concurrent via ThreadPoolExecutor)
- Bulk SQL INSERT (not per-ticker)
- Same OHLCV table as India (market='US')
"""

import sqlite3
import logging
import time
import csv
import os
from pathlib import Path
from datetime import datetime, timedelta, date
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import yfinance as yf

logger = logging.getLogger(__name__)
DB_PATH = Path(__file__).parent / "breadth_data.db"

# ═══════════════════════════════════════════════════════════════════════════════
# UNIVERSE — Import from iShares IWV CSV
# ═══════════════════════════════════════════════════════════════════════════════

def import_iwv_holdings_csv(csv_path: str) -> dict:
    """
    Import Russell 3000 universe from iShares IWV holdings CSV.
    
    iShares CSV format:
    - First 9 rows are metadata (fund name, date, shares outstanding, etc.)
    - Row 10: Header row with columns:
      Ticker, Name, Sector, Asset Class, Market Value, Weight (%),
      Notional Value, Quantity, Price, Location, Exchange, Currency, etc.
    - Data rows follow (equity + non-equity)
    
    Stores tickers in ticker_universe and sector_map tables with market='US'.
    Returns: {tickers: int, sectors: int, skipped: int}
    """
    # Known iShares → yfinance ticker fixes
    TICKER_FIXES = {
        "BRKB": "BRK-B", "BFB": "BF-B", "BFA": "BF-A",
        "LLYVA": "LLYVA", "LLYVK": "LLYVK",
    }
    
    tickers = []
    sector_data = {}
    skipped = 0
    
    try:
        with open(csv_path, "r", encoding="utf-8-sig") as f:
            raw_lines = f.readlines()
        
        # Find the header row (contains "Ticker")
        header_idx = None
        for i, line in enumerate(raw_lines):
            if "Ticker" in line and "Name" in line and "Sector" in line:
                header_idx = i
                break
        
        if header_idx is None:
            return {"error": "Could not find header row with 'Ticker' column", "tickers": 0}
        
        # Parse from header row
        reader = csv.DictReader(raw_lines[header_idx:])
        reader.fieldnames = [h.strip() for h in reader.fieldnames]
        
        logger.info(f"IWV CSV headers: {reader.fieldnames[:8]}")
        
        seen = set()
        for row in reader:
            ticker = (row.get("Ticker", "") or "").strip().strip('"')
            asset_class = (row.get("Asset Class", "") or "").strip().strip('"')
            sector = (row.get("Sector", "") or "").strip().strip('"')
            name = (row.get("Name", "") or "").strip().strip('"')
            exchange = (row.get("Exchange", "") or "").strip().strip('"')
            
            # Only equities
            if asset_class != "Equity":
                skipped += 1
                continue
            
            # Valid ticker check
            if not ticker or ticker == "-" or len(ticker) > 8:
                skipped += 1
                continue
            
            # Skip non-alpha tickers (cash proxies like P5N994)
            if any(c.isdigit() for c in ticker) and not any(c == '-' for c in ticker):
                skipped += 1
                continue
            
            # Apply ticker fixes (iShares strips dots/hyphens)
            ticker = TICKER_FIXES.get(ticker, ticker)
            
            # Deduplicate
            if ticker in seen:
                continue
            seen.add(ticker)
            
            tickers.append(ticker)
            if sector and sector != "-":
                sector_data[ticker] = {
                    "sector": sector,
                    "name": name,
                    "exchange": exchange,
                }
        
        logger.info(f"Parsed {len(tickers)} US equity tickers, {len(sector_data)} with sectors, {skipped} skipped")
        
    except Exception as e:
        logger.error(f"Failed to parse IWV CSV: {e}")
        import traceback
        traceback.print_exc()
        return {"error": str(e), "tickers": 0}
    
    if not tickers:
        return {"error": "No equity tickers found in CSV", "tickers": 0}
    
    # Store in DB
    conn = sqlite3.connect(str(DB_PATH), timeout=15)
    
    # 1. Ticker universe
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ticker_universe (
            ticker TEXT, market TEXT, added_at TEXT,
            PRIMARY KEY (ticker, market)
        )
    """)
    conn.execute("DELETE FROM ticker_universe WHERE market='US'")
    conn.executemany(
        "INSERT OR REPLACE INTO ticker_universe (ticker, market, added_at) VALUES (?, 'US', ?)",
        [(t, datetime.utcnow().isoformat()) for t in tickers]
    )
    
    # 2. Sector map (merge, don't overwrite India sectors)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sector_map (
            ticker TEXT PRIMARY KEY, sector TEXT, industry TEXT, name TEXT
        )
    """)
    for ticker, info in sector_data.items():
        conn.execute(
            "INSERT OR REPLACE INTO sector_map (ticker, sector, name) VALUES (?, ?, ?)",
            (ticker, info["sector"], info.get("name", ""))
        )
    
    conn.commit()
    conn.close()
    
    # Collect sector distribution
    sector_counts = {}
    for info in sector_data.values():
        s = info["sector"]
        sector_counts[s] = sector_counts.get(s, 0) + 1
    
    logger.info(f"✅ Imported {len(tickers)} US tickers across {len(sector_counts)} GICS sectors")
    
    return {
        "tickers": len(tickers),
        "sectors": len(sector_counts),
        "sector_distribution": sector_counts,
        "skipped": skipped,
        "sample": tickers[:15],
    }


# ═══════════════════════════════════════════════════════════════════════════════
# OHLCV SYNC — Bulk yfinance download
# ═══════════════════════════════════════════════════════════════════════════════

def _get_us_tickers() -> list:
    """Get the Russell 3000 ticker list from DB."""
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    rows = conn.execute(
        "SELECT ticker FROM ticker_universe WHERE market='US' ORDER BY ticker"
    ).fetchall()
    conn.close()
    return [r[0] for r in rows]


def _download_batch(tickers: list, period: str = "2y") -> dict:
    """
    Download OHLCV for a batch of tickers via yfinance.
    Returns {ticker: DataFrame} dict.
    """
    if not tickers:
        return {}
    
    try:
        ticker_str = " ".join(tickers)
        df = yf.download(
            ticker_str,
            period=period,
            interval="1d",
            auto_adjust=True,
            progress=False,
            timeout=30,
            group_by="ticker",
        )
        
        if df is None or df.empty:
            return {}
        
        result = {}
        
        if len(tickers) == 1:
            # Single ticker — df is flat
            t = tickers[0]
            if "Close" in df.columns and not df["Close"].dropna().empty:
                result[t] = df[["Open", "High", "Low", "Close", "Volume"]].dropna()
        else:
            # Multi-ticker — df has MultiIndex columns
            for t in tickers:
                try:
                    if t in df.columns.get_level_values(0):
                        tdf = df[t][["Open", "High", "Low", "Close", "Volume"]].dropna()
                        if len(tdf) >= 10:
                            result[t] = tdf
                except Exception:
                    continue
        
        return result
    
    except Exception as e:
        logger.warning(f"Batch download failed ({len(tickers)} tickers): {e}")
        return {}


def _store_ohlcv_bulk(ticker_data: dict, market: str = "US"):
    """
    Bulk store OHLCV data to SQLite.
    Uses executemany for speed — one INSERT per row, not per ticker.
    """
    if not ticker_data:
        return 0
    
    conn = sqlite3.connect(str(DB_PATH), timeout=30)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS ohlcv (
            ticker TEXT, market TEXT, date TEXT,
            open REAL, high REAL, low REAL, close REAL, volume REAL,
            PRIMARY KEY (ticker, market, date)
        )
    """)
    
    rows = []
    for ticker, df in ticker_data.items():
        for idx, row in df.iterrows():
            date_str = idx.strftime("%Y-%m-%d") if hasattr(idx, 'strftime') else str(idx)[:10]
            rows.append((
                ticker, market, date_str,
                float(row.get("Open", 0) or 0),
                float(row.get("High", 0) or 0),
                float(row.get("Low", 0) or 0),
                float(row.get("Close", 0) or 0),
                float(row.get("Volume", 0) or 0),
            ))
    
    if rows:
        conn.executemany(
            "INSERT OR REPLACE INTO ohlcv (ticker, market, date, open, high, low, close, volume) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            rows
        )
        conn.commit()
    
    conn.close()
    return len(rows)


def sync_us_ohlcv(period: str = "2y", batch_size: int = 50, max_workers: int = 3,
                   progress_callback=None) -> dict:
    """
    Sync OHLCV for all Russell 3000 tickers.
    
    Args:
        period: yfinance period string ("2y", "1mo", etc.)
        batch_size: tickers per yfinance call (50 is optimal)
        max_workers: concurrent batch downloads
        progress_callback: fn(message: str) called with status updates
    
    Returns: {total_tickers, synced, rows, elapsed, errors}
    """
    tickers = _get_us_tickers()
    if not tickers:
        return {"error": "No US tickers in DB. Import IWV CSV first.", "synced": 0}
    
    t0 = time.time()
    total = len(tickers)
    synced = 0
    total_rows = 0
    errors = 0
    
    # Split into batches
    batches = [tickers[i:i+batch_size] for i in range(0, total, batch_size)]
    logger.info(f"US OHLCV sync: {total} tickers in {len(batches)} batches of {batch_size} (period={period})")
    
    if progress_callback:
        progress_callback(f"Starting sync: {total} tickers in {len(batches)} batches...")
    
    # Process batches with parallel workers
    batch_num = 0
    
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit batches in waves to avoid overwhelming yfinance
        for wave_start in range(0, len(batches), max_workers * 2):
            wave_batches = batches[wave_start:wave_start + max_workers * 2]
            futures = {
                executor.submit(_download_batch, batch, period): batch
                for batch in wave_batches
            }
            
            for future in as_completed(futures):
                batch_num += 1
                batch = futures[future]
                try:
                    result = future.result()
                    if result:
                        rows_stored = _store_ohlcv_bulk(result, market="US")
                        synced += len(result)
                        total_rows += rows_stored
                    else:
                        errors += len(batch)
                except Exception as e:
                    logger.warning(f"Batch {batch_num} error: {e}")
                    errors += len(batch)
                
                msg = f"Batch {batch_num}/{len(batches)} — {synced}/{total} tickers synced ({total_rows:,} rows)"
                logger.info(msg)
                if progress_callback:
                    progress_callback(msg)
            
            # Small pause between waves to avoid rate limiting
            if wave_start + max_workers * 2 < len(batches):
                time.sleep(1)
    
    elapsed = round(time.time() - t0, 1)
    result = {
        "total_tickers": total,
        "synced": synced,
        "rows": total_rows,
        "errors": errors,
        "elapsed_seconds": elapsed,
        "period": period,
        "message": f"Synced {synced}/{total} US tickers ({total_rows:,} rows) in {elapsed}s",
    }
    logger.info(f"✅ US OHLCV sync complete: {result['message']}")
    return result


def sync_us_daily() -> dict:
    """Quick daily sync — last 5 trading days only."""
    return sync_us_ohlcv(period="5d", batch_size=50, max_workers=3)


# ═══════════════════════════════════════════════════════════════════════════════
# FULL US SETUP — One-click: import universe + download OHLCV + fundamentals
# ═══════════════════════════════════════════════════════════════════════════════

def full_us_setup(csv_path: str = None, progress_callback=None) -> dict:
    """
    Complete US market setup:
    1. Import IWV CSV (if provided)
    2. Download 2-year OHLCV for all tickers
    3. Sync TV fundamentals for US market
    
    Returns combined status dict.
    """
    results = {}
    
    # Step 1: Import universe
    if csv_path and os.path.exists(csv_path):
        if progress_callback:
            progress_callback("Step 1/3: Importing IWV holdings CSV...")
        results["universe"] = import_iwv_holdings_csv(csv_path)
    else:
        # Check if universe already exists
        tickers = _get_us_tickers()
        if not tickers:
            return {"error": "No US tickers. Upload IWV holdings CSV first."}
        results["universe"] = {"tickers": len(tickers), "message": "Using existing universe"}
    
    # Step 2: OHLCV sync
    if progress_callback:
        progress_callback("Step 2/3: Downloading OHLCV data (this takes 30-45 min)...")
    results["ohlcv"] = sync_us_ohlcv(period="2y", progress_callback=progress_callback)
    
    # Step 3: TV Fundamentals
    if progress_callback:
        progress_callback("Step 3/3: Syncing TV fundamentals for US market...")
    try:
        from tv_fundamentals import fetch_batch_fundamentals
        fund_result = fetch_batch_fundamentals(market="america")
        results["fundamentals"] = {"synced": len(fund_result) if fund_result else 0}
    except Exception as e:
        results["fundamentals"] = {"error": str(e)}
    
    return results
