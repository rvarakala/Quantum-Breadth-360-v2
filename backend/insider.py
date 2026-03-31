"""
Insider Trading Module — NSE PIT (Prohibition of Insider Trading) Data
Fetches insider trading disclosures from NSE, stores in SQLite, computes
buy conviction scores and cluster detection.

Data source: NSE Corporate Filings - PIT Reg 7(2)
API: https://www.nseindia.com/api/corporates-pit
"""
import sqlite3, os, json, logging, time
from datetime import datetime, timedelta, date
from pathlib import Path

logger = logging.getLogger(__name__)
DB = os.path.join(os.path.dirname(__file__), "breadth_data.db")


# ── Database setup ────────────────────────────────────────────────────────────

def _ensure_tables():
    conn = sqlite3.connect(DB, timeout=10)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS insider_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            symbol TEXT NOT NULL,
            company TEXT,
            insider_name TEXT,
            category TEXT,          -- Promoter/Director/KMP/Promoter Group/Immediate Relative
            transaction_type TEXT,  -- Buy/Sell/Pledge/Revoke
            securities_count REAL,
            securities_value REAL,
            transaction_date TEXT,
            mode TEXT,              -- Market Purchase/Sale, Off-market, etc.
            exchange TEXT DEFAULT 'NSE',
            broadcast_date TEXT,
            remarks TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(symbol, insider_name, transaction_date, transaction_type, securities_count)
        )
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_insider_symbol ON insider_trades(symbol)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_insider_date ON insider_trades(transaction_date)
    """)
    conn.execute("""
        CREATE INDEX IF NOT EXISTS idx_insider_type ON insider_trades(transaction_type)
    """)
    conn.commit()
    conn.close()


# ── NSE PIT API fetcher ──────────────────────────────────────────────────────

def fetch_insider_data_nse(from_date: str = None, to_date: str = None) -> list:
    """
    Fetch insider trading data from NSE PIT API.
    Dates in DD-MM-YYYY format.
    Returns list of parsed trade dicts.

    NOTE: This requires running from a machine with direct internet access
    (not behind Cloudflare-blocking proxies). The NSE website requires
    a browser-like session with cookies.
    """
    import httpx

    if not from_date:
        from_date = (date.today() - timedelta(days=7)).strftime("%d-%m-%Y")
    if not to_date:
        to_date = date.today().strftime("%d-%m-%Y")

    url = "https://www.nseindia.com/api/corporates-pit"
    params = {"index": "equities", "from_date": from_date, "to_date": to_date}
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nseindia.com/companies-listing/corporate-filings-insider-trading",
    }

    try:
        client = httpx.Client(follow_redirects=True, timeout=20)
        # Step 1: Get session cookies from main page
        r0 = client.get("https://www.nseindia.com/", headers={"User-Agent": headers["User-Agent"]})
        logger.info(f"NSE session: {r0.status_code}, cookies: {len(r0.cookies)}")
        time.sleep(0.5)

        # Step 2: Hit the API
        r = client.get(url, params=params, headers=headers)
        client.close()

        if r.status_code != 200:
            logger.warning(f"NSE PIT API returned {r.status_code}")
            return []

        data = r.json()
        if isinstance(data, dict):
            # NSE may wrap in {"data": [...]}
            data = data.get("data", data.get("records", []))
        if not isinstance(data, list):
            logger.warning(f"Unexpected NSE PIT response type: {type(data)}")
            return []

        trades = []
        if data:
            # Log first record's keys for debugging field names
            logger.info(f"NSE PIT sample keys: {list(data[0].keys()) if data else 'empty'}")
            logger.info(f"NSE PIT sample record: {data[0] if data else 'empty'}")
        for item in data:
            trade = _parse_nse_pit_record(item)
            if trade:
                trades.append(trade)

        logger.info(f"Fetched {len(trades)} insider trades from NSE ({from_date} to {to_date})")
        return trades

    except Exception as e:
        logger.error(f"NSE PIT fetch error: {e}")
        return []


def _parse_nse_pit_record(item: dict) -> dict:
    """Parse a single NSE PIT API record into our standard format."""
    try:
        symbol = item.get("symbol", item.get("Symbol", "")).strip()
        if not symbol:
            return None

        # NSE uses various key formats — handle both
        company = item.get("company", item.get("Company", "")).strip()
        insider = item.get("acqName", item.get("Acquirer/Disposer", "")).strip()
        category = item.get("personCategory", item.get("Category of Person", "")).strip()

        # Transaction type
        tx_type_raw = item.get("tkdAcqDispType", item.get("TypeOfSecurity", "")).strip()
        if "acqui" in tx_type_raw.lower() or "buy" in tx_type_raw.lower() or "purchase" in tx_type_raw.lower():
            tx_type = "Buy"
        elif "dispos" in tx_type_raw.lower() or "sell" in tx_type_raw.lower() or "sale" in tx_type_raw.lower():
            tx_type = "Sell"
        elif "pledge" in tx_type_raw.lower():
            tx_type = "Pledge"
        elif "revoke" in tx_type_raw.lower() or "invocation" in tx_type_raw.lower():
            tx_type = "Revoke"
        else:
            tx_type = tx_type_raw or "Unknown"

        # Securities count and value — NSE uses various field names
        sec_count = _safe_num(
            item.get("secAcq", 
            item.get("securitiesAcquired",
            item.get("securitiesValue",
            item.get("noOfShareAcq",
            item.get("No. of shares",
            item.get("befAcqSharesNo",
            item.get("secVal", 0)))))))
        )
        sec_value = _safe_num(
            item.get("secVal",
            item.get("securitiesTransacted",
            item.get("afterAcqSharesPer",
            item.get("totAcqShare",
            item.get("Value",
            item.get("acquiredValue", 0))))))
        )
        
        # If both are 0, try alternate field combinations
        if sec_count == 0 and sec_value == 0:
            # Try all numeric fields to find the right ones
            for k, v in item.items():
                val = _safe_num(v)
                if val > 0:
                    kl = k.lower()
                    if ('share' in kl or 'secacq' in kl or 'qty' in kl or 'quantity' in kl) and 'per' not in kl:
                        if sec_count == 0: sec_count = val
                    elif ('val' in kl or 'amount' in kl or 'worth' in kl) and 'per' not in kl and 'date' not in kl:
                        if sec_value == 0: sec_value = val

        # If count > value, they might be swapped in NSE's response
        if sec_count > 0 and sec_value > 0 and sec_count > sec_value * 10:
            sec_count, sec_value = sec_value, sec_count

        tx_date = item.get("date", item.get("Date of Allotment/Acquisition",
                  item.get("acqfromDt", ""))).strip()
        mode = item.get("tdpTransactionType", item.get("Mode of Acquisition", "")).strip()
        broadcast = item.get("brdcstDt", item.get("Date of Broadcast", "")).strip()

        return {
            "symbol": symbol,
            "company": company,
            "insider_name": insider,
            "category": category,
            "transaction_type": tx_type,
            "securities_count": sec_count,
            "securities_value": sec_value,
            "transaction_date": _normalize_date(tx_date),
            "mode": mode,
            "broadcast_date": _normalize_date(broadcast),
            "exchange": "NSE",
        }
    except Exception as e:
        logger.debug(f"Failed to parse PIT record: {e}")
        return None


def _safe_num(val):
    """Convert to float, handling commas, None, empty strings."""
    if val is None:
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    try:
        return float(str(val).replace(",", "").replace(" ", "").strip() or "0")
    except:
        return 0.0


def _normalize_date(ds: str) -> str:
    """Normalize various date formats to YYYY-MM-DD."""
    if not ds:
        return ""
    for fmt in ("%d-%b-%Y", "%d-%m-%Y", "%Y-%m-%d", "%d/%m/%Y", "%b %d, %Y"):
        try:
            return datetime.strptime(ds.strip(), fmt).strftime("%Y-%m-%d")
        except:
            continue
    return ds.strip()


# ── CSV Import (fallback / manual upload) ─────────────────────────────────────

def import_insider_csv(csv_path: str) -> int:
    """
    Import insider trades from NSE PIT CSV download.
    Handles NSE's quirky format: headers with embedded \\n, 29 columns.
    Returns count of records imported.
    """
    import csv

    _ensure_tables()
    conn = sqlite3.connect(DB, timeout=10)
    count = 0

    try:
        with open(csv_path, "r", encoding="utf-8-sig") as f:
            reader = csv.reader(f)
            headers_raw = next(reader)
            # NSE headers have embedded \n — strip all whitespace
            headers = [h.strip().replace("\n", "").strip() for h in headers_raw]
            logger.info(f"CSV headers ({len(headers)}): {headers[:10]}...")

            for row in reader:
                if len(row) < 10:
                    continue
                # Build dict with cleaned headers
                row_dict = {}
                for i, h in enumerate(headers):
                    row_dict[h] = row[i].strip() if i < len(row) else ""

                trade = _parse_nse_csv_row(row_dict)
                if trade:
                    try:
                        conn.execute("""
                            INSERT OR IGNORE INTO insider_trades
                            (symbol, company, insider_name, category, transaction_type,
                             securities_count, securities_value, transaction_date, mode,
                             exchange, broadcast_date, remarks)
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """, (
                            trade["symbol"], trade["company"], trade["insider_name"],
                            trade["category"], trade["transaction_type"],
                            trade["securities_count"], trade["securities_value"],
                            trade["transaction_date"], trade["mode"],
                            trade.get("exchange", "NSE"), trade.get("broadcast_date", ""),
                            trade.get("remarks", ""),
                        ))
                        count += 1
                    except Exception as e:
                        logger.debug(f"Insert error: {e}")
        conn.commit()
    except Exception as e:
        logger.error(f"CSV import error: {e}")
        import traceback
        traceback.print_exc()
    finally:
        conn.close()

    logger.info(f"Imported {count} insider trades from CSV")
    return count


def _parse_nse_csv_row(row: dict) -> dict:
    """
    Parse a single row from NSE PIT CSV download.

    NSE columns (29 total):
    [0]  SYMBOL
    [1]  COMPANY
    [2]  REGULATION
    [3]  NAME OF THE ACQUIRER/DISPOSER
    [4]  CATEGORY OF PERSON
    [5]  TYPE OF SECURITY (PRIOR)
    [6]  NO. OF SECURITY (PRIOR)
    [7]  % SHAREHOLDING (PRIOR)
    [8]  TYPE OF SECURITY (ACQUIRED/DISPLOSED)
    [9]  NO. OF SECURITIES (ACQUIRED/DISPLOSED)
    [10] VALUE OF SECURITY (ACQUIRED/DISPLOSED)
    [11] ACQUISITION/DISPOSAL TRANSACTION TYPE
    [12] TYPE OF SECURITY (POST)
    [13] NO. OF SECURITY (POST)
    [14] % POST
    [15] DATE OF ALLOTMENT/ACQUISITION FROM
    [16] DATE OF ALLOTMENT/ACQUISITION TO
    [17] DATE OF INITMATION TO COMPANY
    [18] MODE OF ACQUISITION
    [25] EXCHANGE
    [26] REMARK
    [27] BROADCASTE DATE AND TIME
    """
    # Helper: find value by partial key match (handles slight variations)
    def _get(key_part):
        for k, v in row.items():
            if key_part.upper() in k.upper():
                return v.strip() if v else ""
        return ""

    symbol = _get("SYMBOL")
    if not symbol or symbol == "-":
        return None

    company = _get("COMPANY")
    insider = _get("NAME OF THE ACQUIRER") or _get("ACQUIRER/DISPOSER")
    category = _get("CATEGORY OF PERSON")

    # Transaction type
    tx_raw = _get("ACQUISITION/DISPOSAL TRANSACTION TYPE")
    if not tx_raw:
        tx_raw = _get("TRANSACTION TYPE")
    tx_lower = tx_raw.lower()
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

    # Securities count and value
    sec_count = _safe_num(_get("NO. OF SECURITIES (ACQUIRED"))
    if sec_count == 0:
        sec_count = _safe_num(_get("NO. OF SECURITIES"))
    sec_value = _safe_num(_get("VALUE OF SECURITY (ACQUIRED"))
    if sec_value == 0:
        sec_value = _safe_num(_get("VALUE OF SECURITY"))

    # Dates
    tx_date = _get("DATE OF ALLOTMENT/ACQUISITION FROM")
    if not tx_date:
        tx_date = _get("ACQUISITION FROM")
    mode = _get("MODE OF ACQUISITION") or _get("MODE")
    exchange = _get("EXCHANGE") or "NSE"
    broadcast = _get("BROADCASTE DATE") or _get("BROADCAST DATE")
    remark = _get("REMARK")
    if remark == "-":
        remark = ""

    return {
        "symbol": symbol,
        "company": company,
        "insider_name": insider,
        "category": category,
        "transaction_type": tx_type,
        "securities_count": sec_count,
        "securities_value": sec_value,
        "transaction_date": _normalize_date(tx_date),
        "mode": mode if mode != "-" else "",
        "exchange": exchange if exchange != "-" else "NSE",
        "broadcast_date": _normalize_date(broadcast.split(" ")[0] if broadcast else ""),
        "remarks": remark,
    }


# ── Store trades to DB ────────────────────────────────────────────────────────

def store_trades(trades: list) -> int:
    """Store a list of trade dicts to SQLite. Returns count inserted."""
    _ensure_tables()
    conn = sqlite3.connect(DB, timeout=10)
    count = 0
    for t in trades:
        try:
            conn.execute("""
                INSERT OR IGNORE INTO insider_trades
                (symbol, company, insider_name, category, transaction_type,
                 securities_count, securities_value, transaction_date, mode,
                 exchange, broadcast_date)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                t["symbol"], t["company"], t["insider_name"],
                t["category"], t["transaction_type"],
                t["securities_count"], t["securities_value"],
                t["transaction_date"], t["mode"],
                t.get("exchange", "NSE"), t.get("broadcast_date", ""),
            ))
            count += 1
        except:
            pass
    conn.commit()
    conn.close()
    logger.info(f"Stored {count} insider trades")
    return count


# ── Query trades ──────────────────────────────────────────────────────────────

def get_insider_trades(
    days: int = 90,
    tx_type: str = None,       # "Buy", "Sell", or None for all
    category: str = None,      # "Promoter", "Director", etc.
    symbol: str = None,
    min_value: float = 0,
    limit: int = 200,
) -> list:
    """Query insider trades from DB with optional filters."""
    _ensure_tables()
    conn = sqlite3.connect(DB, timeout=10)
    conn.row_factory = sqlite3.Row

    where = ["transaction_date >= ?"]
    params = [(date.today() - timedelta(days=days)).isoformat()]

    if tx_type:
        where.append("transaction_type = ?")
        params.append(tx_type)
    if category:
        where.append("category LIKE ?")
        params.append(f"%{category}%")
    if symbol:
        where.append("symbol = ?")
        params.append(symbol.upper())
    if min_value > 0:
        where.append("securities_value >= ?")
        params.append(min_value)

    sql = f"""
        SELECT * FROM insider_trades
        WHERE {' AND '.join(where)}
        ORDER BY transaction_date DESC, securities_value DESC
        LIMIT ?
    """
    params.append(limit)

    rows = conn.execute(sql, params).fetchall()
    conn.close()

    trades = [dict(r) for r in rows]

    # Compute derived fields
    for t in trades:
        t["price_approx"] = round(t["securities_value"] / t["securities_count"], 2) \
            if t["securities_count"] > 0 else 0
        t["value_fmt"] = _fmt_value(t["securities_value"])

    return trades


def get_insider_summary(days: int = 90) -> dict:
    """Get aggregate insider activity stats."""
    _ensure_tables()
    conn = sqlite3.connect(DB, timeout=10)
    cutoff = (date.today() - timedelta(days=days)).isoformat()

    total = conn.execute(
        "SELECT COUNT(*) FROM insider_trades WHERE transaction_date >= ?", (cutoff,)
    ).fetchone()[0]

    buys = conn.execute(
        "SELECT COUNT(*), COALESCE(SUM(securities_value),0) FROM insider_trades "
        "WHERE transaction_date >= ? AND transaction_type='Buy'", (cutoff,)
    ).fetchone()

    sells = conn.execute(
        "SELECT COUNT(*), COALESCE(SUM(securities_value),0) FROM insider_trades "
        "WHERE transaction_date >= ? AND transaction_type='Sell'", (cutoff,)
    ).fetchone()

    # Top buy by value
    top_buy = conn.execute(
        "SELECT symbol FROM insider_trades WHERE transaction_date >= ? AND transaction_type='Buy' "
        "ORDER BY securities_value DESC LIMIT 1", (cutoff,)
    ).fetchone()

    # Top sell by value
    top_sell = conn.execute(
        "SELECT symbol FROM insider_trades WHERE transaction_date >= ? AND transaction_type='Sell' "
        "ORDER BY securities_value DESC LIMIT 1", (cutoff,)
    ).fetchone()

    # Cluster detection: symbols with 3+ insider buys in the period
    clusters = conn.execute("""
        SELECT symbol, COUNT(DISTINCT insider_name) as insiders, SUM(securities_value) as total_value
        FROM insider_trades
        WHERE transaction_date >= ? AND transaction_type='Buy'
        GROUP BY symbol
        HAVING COUNT(DISTINCT insider_name) >= 2
        ORDER BY insiders DESC, total_value DESC
        LIMIT 10
    """, (cutoff,)).fetchall()

    # Last refresh
    last_date = conn.execute(
        "SELECT MAX(transaction_date) FROM insider_trades"
    ).fetchone()[0]

    conn.close()

    return {
        "total_transactions": total,
        "buys_count": buys[0], "buys_value": buys[1],
        "sells_count": sells[0], "sells_value": sells[1],
        "top_buy_symbol": top_buy[0] if top_buy else "—",
        "top_sell_symbol": top_sell[0] if top_sell else "—",
        "clusters": [
            {"symbol": c[0], "insiders": c[1], "total_value": c[2]}
            for c in clusters
        ],
        "last_data_date": last_date or "—",
        "days": days,
    }


def compute_buy_score(trade: dict) -> int:
    """
    Compute a buy conviction score (0-100) for an insider trade.
    Higher = more conviction signal.

    Factors:
    - Category weight (Promoter > Director > KMP > Others)
    - Transaction value (higher = more conviction)
    - Mode (market purchase > off-market)
    - Cluster bonus (multiple insiders buying same stock)
    """
    score = 50  # base

    # Category boost
    cat = (trade.get("category", "") or "").lower()
    if "promoter" in cat and "group" not in cat:
        score += 20
    elif "promoter group" in cat:
        score += 15
    elif "director" in cat:
        score += 12
    elif "kmp" in cat or "key managerial" in cat:
        score += 10
    elif "designated" in cat:
        score += 5

    # Value boost (log scale)
    val = trade.get("securities_value", 0)
    if val >= 50_00_00_000:      # 50 Cr+
        score += 20
    elif val >= 10_00_00_000:    # 10 Cr+
        score += 15
    elif val >= 1_00_00_000:     # 1 Cr+
        score += 10
    elif val >= 10_00_000:       # 10L+
        score += 5

    # Market purchase is stronger signal
    mode = (trade.get("mode", "") or "").lower()
    if "market" in mode and "purchase" in mode:
        score += 5

    return min(100, max(0, score))


def _fmt_value(val: float) -> str:
    """Format value in Indian notation (L/Cr)."""
    if val >= 1_00_00_000:
        return f"₹{val/1_00_00_000:.1f}Cr"
    elif val >= 1_00_000:
        return f"₹{val/1_00_000:.1f}L"
    elif val >= 1000:
        return f"₹{val/1000:.0f}K"
    else:
        return f"₹{val:.0f}"


# ── Sync function (called from main.py) ───────────────────────────────────────

def sync_insider_data(days_back: int = 30) -> dict:
    """
    Fetch latest insider trades from NSE and store in DB.
    Returns status dict.
    """
    _ensure_tables()
    from_date = (date.today() - timedelta(days=days_back)).strftime("%d-%m-%Y")
    to_date = date.today().strftime("%d-%m-%Y")

    try:
        trades = fetch_insider_data_nse(from_date, to_date)
        if trades:
            count = store_trades(trades)
            return {"status": "ok", "fetched": len(trades), "stored": count,
                    "from": from_date, "to": to_date}
        else:
            return {"status": "no_data", "message": "No trades returned from NSE API. "
                    "Try manual CSV import if behind Cloudflare.",
                    "from": from_date, "to": to_date}
    except Exception as e:
        return {"status": "error", "message": str(e)}
