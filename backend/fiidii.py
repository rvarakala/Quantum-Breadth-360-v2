"""
FII/DII Institutional Money Flow — India Market

Data sources (tried in order):
1. jugaad-data NSELive.fii_dii() — current day (no Cloudflare issues from local)
2. NSE API fiidiiTradeReact — recent days (needs cookies)
3. NSE Archives CSV — day-by-day historical backfill from nsearchives.nseindia.com

The archives CSV approach is the most reliable for historical data.
URL pattern: https://archives.nseindia.com/content/fo/fii_stats_{DD}-{Mon}-{YYYY}.xls
Alt URL:     https://nsearchives.nseindia.com/content/fo/fii_stats_{DD}-{Mon}-{YYYY}.xls
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
    conn.commit()
    conn.close()


def fetch_fiidii_from_nse(days_back: int = 60) -> dict:
    """
    Fetch FII/DII data — multiple methods:
    1. Try jugaad adapter (nse_data_adapter.py)
    2. Try NSE live API (fiidiiTradeReact)
    3. Backfill missing dates from NSE archives day-by-day
    """
    _ensure_fiidii_table()
    total_stored = 0

    # Method 1: jugaad adapter
    try:
        from nse_data_adapter import fetch_fiidii_jugaad
        result = fetch_fiidii_jugaad(days_back=days_back)
        if result.get("status") == "ok" and result.get("entries", 0) > 0:
            logger.info(f"✅ FII/DII via jugaad: {result['entries']} entries")
            total_stored += result["entries"]
    except Exception as e:
        logger.info(f"Jugaad FII/DII: {e}")

    # Method 2: NSE live API
    try:
        stored = _fetch_from_nse_api()
        if stored > 0:
            total_stored += stored
    except Exception as e:
        logger.info(f"NSE API FII/DII: {e}")

    # Method 3: Backfill missing dates from archives
    try:
        logger.info(f"Starting archives backfill for {days_back} days...")
        filled = _backfill_from_archives(days_back)
        if filled > 0:
            total_stored += filled
            logger.info(f"✅ Archives backfill: {filled} new entries")
        else:
            logger.info("Archives backfill: 0 entries found")
    except Exception as e:
        logger.warning(f"Archives backfill error: {e}")
        import traceback; traceback.print_exc()

    # Check total in DB
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    count = conn.execute("SELECT COUNT(*) FROM fiidii").fetchone()[0]
    latest = conn.execute("SELECT MAX(date) FROM fiidii").fetchone()[0]
    conn.close()

    return {
        "status": "ok" if count > 0 else "no_data",
        "entries": total_stored,
        "total_in_db": count,
        "latest_date": latest,
        "message": f"Synced {total_stored} new entries. Total in DB: {count} days."
    }


def _fetch_from_nse_api() -> int:
    """Try NSE live API — returns recent days."""
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
        return 0

    data = r.json()
    if not isinstance(data, list):
        return 0

    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    now = datetime.now(timezone.utc).isoformat()
    stored = 0
    date_data = {}

    for entry in data:
        cat = entry.get("category", "").strip()
        date_str = entry.get("date", "").strip()
        if not date_str: continue
        try:
            dt = datetime.strptime(date_str, "%d-%b-%Y")
            iso_date = dt.strftime("%Y-%m-%d")
        except: continue

        if iso_date not in date_data:
            date_data[iso_date] = {}

        def _p(v):
            try: return float(str(v).replace(",", "").strip() or "0")
            except: return 0.0

        if "FII" in cat or "FPI" in cat:
            date_data[iso_date].update({"fii_buy": _p(entry.get("buyValue")),
                "fii_sell": _p(entry.get("sellValue")), "fii_net": _p(entry.get("netValue"))})
        elif "DII" in cat:
            date_data[iso_date].update({"dii_buy": _p(entry.get("buyValue")),
                "dii_sell": _p(entry.get("sellValue")), "dii_net": _p(entry.get("netValue"))})

    for d, v in date_data.items():
        conn.execute("""INSERT OR REPLACE INTO fiidii
            (date, fii_buy, fii_sell, fii_net, dii_buy, dii_sell, dii_net, fetched_at)
            VALUES (?,?,?,?,?,?,?,?)""",
            (d, v.get("fii_buy",0), v.get("fii_sell",0), v.get("fii_net",0),
             v.get("dii_buy",0), v.get("dii_sell",0), v.get("dii_net",0), now))
        stored += 1

    conn.commit()
    conn.close()
    if stored: logger.info(f"NSE API: stored {stored} FII/DII entries")
    return stored


def _backfill_from_archives(days_back: int = 60) -> int:
    """Backfill missing dates from NSE archives day-by-day using httpx."""
    import httpx

    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    existing = set(r[0] for r in conn.execute("SELECT date FROM fiidii").fetchall())
    conn.close()

    client = httpx.Client(
        headers={"User-Agent": "Mozilla/5.0 (Windows NT 11.0; Win64; x64) AppleWebKit/537.36"},
        follow_redirects=True, timeout=10,
    )

    today = date.today()
    filled = 0
    tried = 0
    errors = 0

    for i in range(days_back):
        dt = today - timedelta(days=i)
        if dt.weekday() >= 5:  # Skip weekends
            continue
        iso = dt.isoformat()
        if iso in existing:
            continue

        tried += 1
        dd = dt.strftime("%d")
        mon_cap = dt.strftime("%b").capitalize()  # "Mar"
        mon_up = dt.strftime("%b").upper()  # "MAR"
        yyyy = str(dt.year)

        # Try multiple URL patterns — NSE uses inconsistent formats
        urls = [
            f"https://archives.nseindia.com/content/fo/fii_stats_{dd}-{mon_cap}-{yyyy}.xls",
            f"https://nsearchives.nseindia.com/content/fo/fii_stats_{dd}-{mon_cap}-{yyyy}.xls",
            f"https://archives.nseindia.com/content/fo/fii{dd}{mon_up}{yyyy}.csv",
            f"https://nsearchives.nseindia.com/content/fo/fii{dd}{mon_up}{yyyy}.csv",
        ]

        found = False
        for url in urls:
            try:
                r = client.get(url)
                if r.status_code == 200 and len(r.text) > 50:
                    entry = _parse_archive_data(r.text, iso)
                    if entry:
                        _store_single(entry)
                        filled += 1
                        existing.add(iso)
                        found = True
                        if filled <= 3:
                            logger.info(f"  Archive hit: {iso} from {url.split('/')[-1]}")
                        break
            except Exception as e:
                errors += 1
                continue

        if not found and tried <= 3:
            logger.debug(f"  Archive miss: {iso} (tried {len(urls)} URLs)")

        time.sleep(0.12)

    client.close()
    logger.info(f"Archives backfill done: {filled} found, {tried} tried, {errors} errors")
    return filled


def _parse_archive_data(text: str, iso_date: str) -> dict:
    """Parse NSE archive FII/DII data (CSV or XLS-as-text)."""
    try:
        fii_buy = fii_sell = fii_net = 0.0
        dii_buy = dii_sell = dii_net = 0.0

        # Try CSV parsing
        reader = csv.reader(io.StringIO(text))
        for row in reader:
            if len(row) < 4:
                continue
            cat = row[0].strip().upper()

            def _v(idx):
                try: return float(row[idx].replace(",", "").strip())
                except: return 0.0

            if "FII" in cat or "FPI" in cat:
                fii_buy = _v(1) if len(row) > 1 else 0
                fii_sell = _v(2) if len(row) > 2 else 0
                fii_net = _v(3) if len(row) > 3 else (fii_buy - fii_sell)
            elif "DII" in cat or "DOMESTIC" in cat:
                dii_buy = _v(1) if len(row) > 1 else 0
                dii_sell = _v(2) if len(row) > 2 else 0
                dii_net = _v(3) if len(row) > 3 else (dii_buy - dii_sell)

        if fii_buy == 0 and dii_buy == 0:
            return None

        return {
            "date": iso_date,
            "fii_buy": round(fii_buy, 2), "fii_sell": round(fii_sell, 2),
            "fii_net": round(fii_net, 2),
            "dii_buy": round(dii_buy, 2), "dii_sell": round(dii_sell, 2),
            "dii_net": round(dii_net, 2),
        }
    except:
        return None


def _store_single(entry: dict):
    """Store a single FII/DII entry."""
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    now = datetime.now(timezone.utc).isoformat()
    conn.execute("""INSERT OR REPLACE INTO fiidii
        (date, fii_buy, fii_sell, fii_net, dii_buy, dii_sell, dii_net, fetched_at)
        VALUES (?,?,?,?,?,?,?,?)""",
        (entry["date"], entry["fii_buy"], entry["fii_sell"], entry["fii_net"],
         entry["dii_buy"], entry["dii_sell"], entry["dii_net"], now))
    conn.commit()
    conn.close()


def get_fiidii_summary(days: int = 60) -> dict:
    """Get FII/DII summary — latest session + streaks + cumulative + history."""
    _ensure_fiidii_table()
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM fiidii ORDER BY date DESC LIMIT ?", (days,)).fetchall()
    conn.close()

    if not rows:
        return {"error": "No FII/DII data. Click Force Sync to fetch from NSE."}

    latest = dict(rows[0])
    fii_streak = _compute_streak([r["fii_net"] for r in rows])
    dii_streak = _compute_streak([r["dii_net"] for r in rows])

    fii_5d = sum(r["fii_net"] for r in rows[:5])
    dii_5d = sum(r["dii_net"] for r in rows[:5])
    fii_20d = sum(r["fii_net"] for r in rows[:20])
    dii_20d = sum(r["dii_net"] for r in rows[:20])

    net_liq = latest.get("fii_net", 0) + latest.get("dii_net", 0)
    total_act = abs(latest.get("fii_net", 0)) + abs(latest.get("dii_net", 0))
    fii_pct = round(abs(latest.get("fii_net", 0)) / total_act * 100) if total_act > 0 else 50

    fn, dn = latest.get("fii_net", 0), latest.get("dii_net", 0)
    if fn < -500 and dn > 500:
        sentiment, scol = "AGGRESSIVE SELLING", "#ef4444"
    elif fn > 500 and dn > 0:
        sentiment, scol = "STRONG BUYING", "#22c55e"
    elif fn > 0:
        sentiment, scol = "FII BUYING", "#4ade80"
    elif fn < 0 and dn > 0:
        sentiment, scol = "DII SUPPORT", "#f59e0b"
    else:
        sentiment, scol = "NEUTRAL", "#64748b"

    history = [{"date": r["date"], "fii_net": round(r["fii_net"], 2),
                "dii_net": round(r["dii_net"], 2), "net": round(r["fii_net"] + r["dii_net"], 2)}
               for r in reversed(rows)]

    n = min(5, len(rows))
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
            "fii_5d_avg": round(fii_5d / n, 2) if n else 0,
            "dii_5d_avg": round(dii_5d / n, 2) if n else 0,
        },
        "sentiment": sentiment, "sentiment_color": scol,
        "history": history, "days": len(rows),
    }


def _compute_streak(vals):
    if not vals: return {"days": 0, "direction": "Neutral", "total": 0}
    d = "Buying" if vals[0] >= 0 else "Selling"
    days = total = 0
    for v in vals:
        if (d == "Buying" and v >= 0) or (d == "Selling" and v < 0):
            days += 1; total += v
        else: break
    return {"days": days, "direction": d, "total": round(total, 2)}
