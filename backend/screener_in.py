"""
screener_in.py — Screener.in fundamental data via screener-scraper-pro (npm)
Calls Node.js bridge script, caches results in SQLite.
Used for: quarterly results, balance sheet, cash flow, shareholding, CAGRs.
"""

import subprocess, json, logging, sqlite3, time
from pathlib import Path
from datetime import datetime, timezone, timedelta

logger = logging.getLogger(__name__)
DB_PATH = str(Path(__file__).parent / "breadth_data.db")
BRIDGE_PATH = str(Path(__file__).parent / "screener_bridge.mjs")
CACHE_TTL_HOURS = 24


def ensure_screener_table():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS screener_fundamentals (
            ticker TEXT PRIMARY KEY,
            data_json TEXT,
            summary_json TEXT,
            fetched_at TEXT,
            source TEXT DEFAULT 'screener.in'
        )
    """)
    conn.commit()
    conn.close()


def fetch_screener_data(ticker: str, force: bool = False) -> dict:
    """
    Fetch comprehensive fundamental data from screener.in via Node.js bridge.
    Caches for 24h.
    """
    ticker = ticker.upper().strip().replace(".NS", "").replace(".BO", "")
    if not ticker:
        return {"error": "No ticker provided"}

    if not force:
        cached = _get_cached(ticker)
        if cached:
            return cached

    logger.info(f"Fetching screener.in data for {ticker}")
    try:
        result = subprocess.run(
            ["node", BRIDGE_PATH, ticker],
            capture_output=True, text=True, timeout=30,
            cwd=str(Path(__file__).parent.parent)
        )

        if result.returncode != 0:
            err = result.stderr.strip() or "Bridge script failed"
            logger.warning(f"Screener bridge error for {ticker}: {err}")
            return {"error": err, "ticker": ticker}

        data = json.loads(result.stdout.strip())

        if data.get("status") == "error":
            logger.warning(f"Screener.in error for {ticker}: {data.get('error')}")
            return data

        _store_cached(ticker, data)
        return data

    except subprocess.TimeoutExpired:
        return {"error": "Timeout (>30s)", "ticker": ticker}
    except json.JSONDecodeError as e:
        return {"error": f"Invalid JSON: {e}", "ticker": ticker}
    except FileNotFoundError:
        return {"error": "Node.js not found", "ticker": ticker}
    except Exception as e:
        return {"error": str(e), "ticker": ticker}


def get_screener_summary(ticker: str) -> dict:
    data = fetch_screener_data(ticker)
    if data.get("error"):
        return data
    return data.get("summary", {})


def get_screener_quarters(ticker: str) -> dict:
    data = fetch_screener_data(ticker)
    if data.get("error"):
        return data
    return data.get("quarters", {})


def get_screener_shareholding(ticker: str) -> dict:
    data = fetch_screener_data(ticker)
    if data.get("error"):
        return data
    return data.get("shareholding", {})


def get_screener_financials(ticker: str) -> dict:
    data = fetch_screener_data(ticker)
    if data.get("error"):
        return data
    return {
        "profitLoss": data.get("profitLoss"),
        "balanceSheet": data.get("balanceSheet"),
        "cashFlow": data.get("cashFlow"),
        "ratios": data.get("ratios"),
    }


def get_screener_cache_stats() -> dict:
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        total = conn.execute("SELECT COUNT(*) FROM screener_fundamentals").fetchone()[0]
        latest = conn.execute("SELECT MAX(fetched_at) FROM screener_fundamentals").fetchone()[0]
        conn.close()
        return {"cached_tickers": total, "latest_fetch": latest}
    except:
        return {"cached_tickers": 0, "latest_fetch": None}


# ── Cache ─────────────────────────────────────────────────────────────────────

def _get_cached(ticker: str) -> dict:
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        row = conn.execute(
            "SELECT data_json, fetched_at FROM screener_fundamentals WHERE ticker=?",
            (ticker,)
        ).fetchone()
        conn.close()
        if not row:
            return None
        fetched_at = row[1]
        if fetched_at:
            age = (datetime.now(timezone.utc) - datetime.fromisoformat(fetched_at)).total_seconds() / 3600
            if age > CACHE_TTL_HOURS:
                return None
        data = json.loads(row[0])
        data["_cached"] = True
        data["_fetched_at"] = fetched_at
        return data
    except:
        return None


def _store_cached(ticker: str, data: dict):
    try:
        summary = data.get("summary", {})
        conn = sqlite3.connect(DB_PATH, timeout=10)
        conn.execute("""
            INSERT OR REPLACE INTO screener_fundamentals (ticker, data_json, summary_json, fetched_at)
            VALUES (?, ?, ?, ?)
        """, (ticker, json.dumps(data), json.dumps(summary),
              datetime.now(timezone.utc).isoformat()))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.debug(f"Cache write error for {ticker}: {e}")
