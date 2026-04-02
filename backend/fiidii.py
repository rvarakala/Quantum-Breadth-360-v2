"""
FII/DII Institutional Money Flow — India Market

Data sources:
1. NSE API fiidiiTradeReact — returns latest 1 day (auto-syncs daily)
2. CSV Import — bulk seed from any source (Trendlyne, StockEdge, Mr. Chartist)
   Expected CSV columns: Date, FII_Buy, FII_Sell, FII_Net, DII_Buy, DII_Sell, DII_Net
   Date formats accepted: YYYY-MM-DD, DD-MM-YYYY, DD-Mon-YYYY, DD/MM/YYYY

The DB accumulates data daily via auto-sync. For instant 60-day backfill,
use the CSV import feature on the FII/DII tab.
"""

import sqlite3, logging, time, io, csv
from pathlib import Path
from datetime import datetime, timedelta, timezone, date

logger = logging.getLogger(__name__)
DB_PATH = Path(__file__).parent / "breadth_data.db"


def _ensure_fiidii_table():
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS fiidii (
            date TEXT PRIMARY KEY,
            fii_buy REAL DEFAULT 0, fii_sell REAL DEFAULT 0, fii_net REAL DEFAULT 0,
            dii_buy REAL DEFAULT 0, dii_sell REAL DEFAULT 0, dii_net REAL DEFAULT 0,
            fetched_at TEXT
        )
    """)
    # Clean up any future dates (NSE API sometimes returns bad dates like 2026-12-31)
    today = date.today().isoformat()
    deleted = conn.execute("DELETE FROM fiidii WHERE date > ?", (today,)).rowcount
    if deleted:
        logger.info(f"🗑 Cleaned {deleted} future-dated FII/DII entries")
    conn.commit()
    conn.close()


def fetch_fiidii_from_nse(days_back: int = 60) -> dict:
    """Fetch FII/DII from NSE API (returns latest 1 day) + accumulate in DB."""
    _ensure_fiidii_table()
    stored = 0

    try:
        import httpx
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                           "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
            "Accept": "application/json, text/plain, */*",
            "Accept-Language": "en-US,en;q=0.9",
            "Referer": "https://www.nseindia.com/reports/fii-dii",
        }
        client = httpx.Client(follow_redirects=True, timeout=20)
        r0 = client.get("https://www.nseindia.com/", headers={"User-Agent": headers["User-Agent"]})
        logger.info(f"NSE session: {r0.status_code}, cookies: {len(r0.cookies)}")
        time.sleep(0.5)

        r = client.get("https://www.nseindia.com/api/fiidiiTradeReact", headers=headers)
        client.close()

        if r.status_code != 200:
            return {"status": "error", "message": f"NSE returned {r.status_code}", "entries": 0}

        data = r.json()
        if not isinstance(data, list):
            return {"status": "error", "message": "Bad format", "entries": 0}

        conn = sqlite3.connect(str(DB_PATH), timeout=10)
        now = datetime.now(timezone.utc).isoformat()
        date_data = {}

        for entry in data:
            cat = entry.get("category", "").strip()
            date_str = entry.get("date", "").strip()
            if not date_str:
                continue
            try:
                dt = datetime.strptime(date_str, "%d-%b-%Y")
                iso = dt.strftime("%Y-%m-%d")
                # Reject future dates (NSE API sometimes returns wrong dates)
                if dt.date() > date.today():
                    logger.warning(f"FII/DII: rejecting future date {iso} from NSE")
                    continue
            except:
                continue

            if iso not in date_data:
                date_data[iso] = {}

            def _p(v):
                try: return float(str(v).replace(",", "").strip() or "0")
                except: return 0.0

            if "FII" in cat or "FPI" in cat:
                date_data[iso].update({"fii_buy": _p(entry.get("buyValue")),
                    "fii_sell": _p(entry.get("sellValue")), "fii_net": _p(entry.get("netValue"))})
            elif "DII" in cat:
                date_data[iso].update({"dii_buy": _p(entry.get("buyValue")),
                    "dii_sell": _p(entry.get("sellValue")), "dii_net": _p(entry.get("netValue"))})

        for d, v in date_data.items():
            conn.execute("""INSERT OR REPLACE INTO fiidii
                (date, fii_buy, fii_sell, fii_net, dii_buy, dii_sell, dii_net, fetched_at)
                VALUES (?,?,?,?,?,?,?,?)""",
                (d, v.get("fii_buy", 0), v.get("fii_sell", 0), v.get("fii_net", 0),
                 v.get("dii_buy", 0), v.get("dii_sell", 0), v.get("dii_net", 0), now))
            stored += 1

        conn.commit()
        conn.close()
        if stored:
            logger.info(f"✅ NSE API: stored {stored} FII/DII entries")

    except Exception as e:
        logger.warning(f"FII/DII NSE API: {e}")

    # Report DB status
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    count = conn.execute("SELECT COUNT(*) FROM fiidii").fetchone()[0]
    latest = conn.execute("SELECT MAX(date) FROM fiidii").fetchone()[0]
    conn.close()

    msg = f"Synced {stored} entries from NSE. Total in DB: {count} days."
    if count < 10:
        msg += " Import CSV for 60-day history."

    return {"status": "ok", "entries": stored, "total_in_db": count,
            "latest_date": latest, "message": msg}


def import_fiidii_csv(file_content: str) -> dict:
    """
    Import FII/DII historical data from CSV.
    Accepts flexible column names and date formats.
    Returns: {status, imported, skipped, errors}
    """
    _ensure_fiidii_table()
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    now = datetime.now(timezone.utc).isoformat()

    reader = csv.DictReader(io.StringIO(file_content))
    imported = skipped = errors = 0

    # Normalize headers
    if not reader.fieldnames:
        return {"status": "error", "message": "Empty CSV", "imported": 0}

    headers = {h.strip().upper(): h.strip() for h in reader.fieldnames}
    logger.info(f"FII/DII CSV headers: {list(headers.keys())}")

    def _find_col(*candidates):
        for c in candidates:
            for h_upper, h_orig in headers.items():
                if c.upper() in h_upper:
                    return h_orig
        return None

    date_col = _find_col("DATE")
    fii_buy_col = _find_col("FII_BUY", "FII BUY", "FPI_BUY", "FPI BUY")
    fii_sell_col = _find_col("FII_SELL", "FII SELL", "FPI_SELL", "FPI SELL")
    fii_net_col = _find_col("FII_NET", "FII NET", "FPI_NET", "FPI NET")
    dii_buy_col = _find_col("DII_BUY", "DII BUY")
    dii_sell_col = _find_col("DII_SELL", "DII SELL")
    dii_net_col = _find_col("DII_NET", "DII NET")

    if not date_col:
        return {"status": "error", "message": "No 'Date' column found", "imported": 0}
    if not fii_net_col and not fii_buy_col:
        return {"status": "error", "message": "No FII columns found", "imported": 0}

    for row in reader:
        try:
            raw_date = row.get(date_col, "").strip()
            iso = _parse_date(raw_date)
            if not iso:
                errors += 1
                continue

            def _v(col):
                if not col: return 0.0
                try: return float(row.get(col, "0").replace(",", "").strip() or "0")
                except: return 0.0

            fb = _v(fii_buy_col)
            fs = _v(fii_sell_col)
            fn_csv = _v(fii_net_col)
            db = _v(dii_buy_col)
            ds = _v(dii_sell_col)
            dn_csv = _v(dii_net_col)

            # Always compute Net from Buy - Sell (CSV Net column may have wrong signs)
            if fb > 0 and fs > 0:
                fn = fb - fs
            else:
                fn = fn_csv
            if db > 0 and ds > 0:
                dn = db - ds
            else:
                dn = dn_csv

            if fn == 0 and dn == 0 and fb == 0 and db == 0:
                skipped += 1
                continue

            conn.execute("""INSERT OR REPLACE INTO fiidii
                (date, fii_buy, fii_sell, fii_net, dii_buy, dii_sell, dii_net, fetched_at)
                VALUES (?,?,?,?,?,?,?,?)""",
                (iso, round(fb, 2), round(fs, 2), round(fn, 2),
                 round(db, 2), round(ds, 2), round(dn, 2), now))
            imported += 1

        except Exception as e:
            errors += 1

    conn.commit()
    count = conn.execute("SELECT COUNT(*) FROM fiidii").fetchone()[0]
    conn.close()

    logger.info(f"FII/DII CSV import: {imported} imported, {skipped} skipped, {errors} errors. Total: {count}")
    return {"status": "ok", "imported": imported, "skipped": skipped,
            "errors": errors, "total_in_db": count}


def _parse_date(s: str) -> str:
    """Parse various date formats to YYYY-MM-DD."""
    if not s:
        return None
    # Try M/D/YYYY first (US format: 1/2/2026 = Jan 2, not Feb 1)
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y", "%d-%b-%Y", "%d %b %Y",
                "%d-%B-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(s.strip(), fmt).strftime("%Y-%m-%d")
        except:
            continue
    return None


def get_fiidii_summary(days: int = 60) -> dict:
    """Get FII/DII summary — latest session + streaks + cumulative + history."""
    _ensure_fiidii_table()
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM fiidii ORDER BY date DESC LIMIT ?", (days,)).fetchall()
    conn.close()

    if not rows:
        return {"error": "No FII/DII data. Click Force Sync or Import CSV."}

    latest = dict(rows[0])
    fii_streak = _streak([r["fii_net"] for r in rows])
    dii_streak = _streak([r["dii_net"] for r in rows])

    fii_5d = sum(r["fii_net"] for r in rows[:5])
    dii_5d = sum(r["dii_net"] for r in rows[:5])
    fii_20d = sum(r["fii_net"] for r in rows[:20])
    dii_20d = sum(r["dii_net"] for r in rows[:20])

    net_liq = latest["fii_net"] + latest["dii_net"]
    total_act = abs(latest["fii_net"]) + abs(latest["dii_net"])
    fii_pct = round(abs(latest["fii_net"]) / total_act * 100) if total_act > 0 else 50

    fn, dn = latest["fii_net"], latest["dii_net"]
    if fn < -500 and dn > 500: sent, scol = "AGGRESSIVE SELLING", "#ef4444"
    elif fn > 500 and dn > 0: sent, scol = "STRONG BUYING", "#22c55e"
    elif fn > 0: sent, scol = "FII BUYING", "#4ade80"
    elif fn < 0 and dn > 0: sent, scol = "DII SUPPORT", "#f59e0b"
    else: sent, scol = "NEUTRAL", "#64748b"

    history = [{"date": r["date"], "fii_net": round(r["fii_net"], 2),
                "dii_net": round(r["dii_net"], 2), "net": round(r["fii_net"] + r["dii_net"], 2)}
               for r in reversed(rows)]

    n5 = min(5, len(rows))
    return {
        "latest": {
            "date": latest["date"],
            "fii_buy": round(latest["fii_buy"], 2), "fii_sell": round(latest["fii_sell"], 2),
            "fii_net": round(fn, 2),
            "dii_buy": round(latest["dii_buy"], 2), "dii_sell": round(latest["dii_sell"], 2),
            "dii_net": round(dn, 2),
            "net_liquidity": round(net_liq, 2),
            "fii_pct": fii_pct, "dii_pct": 100 - fii_pct,
        },
        "streaks": {"fii": fii_streak, "dii": dii_streak},
        "cumulative": {
            "fii_5d": round(fii_5d, 2), "dii_5d": round(dii_5d, 2),
            "fii_20d": round(fii_20d, 2), "dii_20d": round(dii_20d, 2),
            "fii_5d_avg": round(fii_5d / n5, 2) if n5 else 0,
            "dii_5d_avg": round(dii_5d / n5, 2) if n5 else 0,
        },
        "sentiment": sent, "sentiment_color": scol,
        "history": history, "days": len(rows),
    }


def _streak(vals):
    if not vals: return {"days": 0, "direction": "Neutral", "total": 0}
    d = "Buying" if vals[0] >= 0 else "Selling"
    days = total = 0
    for v in vals:
        if (d == "Buying" and v >= 0) or (d == "Selling" and v < 0):
            days += 1; total += v
        else: break
    return {"days": days, "direction": d, "total": round(total, 2)}
