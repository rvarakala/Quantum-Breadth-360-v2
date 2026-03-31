"""
FII/DII Institutional Money Flow — India Market
Primary: nsearchives.nseindia.com via nse_data_adapter (no Cloudflare)
Fallback: NSE API fiidiiTradeReact (needs cookies)
"""

import sqlite3, logging, time
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
    Fetch FII/DII data — tries jugaad adapter first (reliable),
    falls back to NSE API (needs cookies).
    """
    _ensure_fiidii_table()

    # Method 1: jugaad adapter — hits nsearchives (no Cloudflare)
    try:
        from nse_data_adapter import fetch_fiidii_jugaad
        result = fetch_fiidii_jugaad(days_back=days_back)
        if result.get("status") == "ok" and result.get("entries", 0) > 0:
            logger.info(f"✅ FII/DII via jugaad: {result['entries']} entries (latest: {result.get('latest_date')})")
            return result
        logger.info(f"Jugaad FII/DII: {result.get('message', 'no data')} — trying NSE API...")
    except Exception as e:
        logger.warning(f"Jugaad FII/DII failed: {e} — trying NSE API fallback...")

    # Method 2: NSE API (needs session cookies)
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
            return {"status": "error", "message": "Unexpected format", "entries": 0}

        logger.info(f"NSE API returned {len(data)} FII/DII entries")
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
                iso_date = dt.strftime("%Y-%m-%d")
            except:
                continue

            if iso_date not in date_data:
                date_data[iso_date] = {}

            def _pcr(v):
                try: return float(str(v).replace(",", "").strip() or "0")
                except: return 0.0

            buy = _pcr(entry.get("buyValue", "0"))
            sell = _pcr(entry.get("sellValue", "0"))
            net = _pcr(entry.get("netValue", "0"))

            if "FII" in cat or "FPI" in cat:
                date_data[iso_date].update({"fii_buy": buy, "fii_sell": sell, "fii_net": net})
            elif "DII" in cat:
                date_data[iso_date].update({"dii_buy": buy, "dii_sell": sell, "dii_net": net})

        stored = 0
        latest_date = None
        for d, vals in date_data.items():
            conn.execute("""
                INSERT OR REPLACE INTO fiidii
                (date, fii_buy, fii_sell, fii_net, dii_buy, dii_sell, dii_net, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (d, vals.get("fii_buy", 0), vals.get("fii_sell", 0), vals.get("fii_net", 0),
                  vals.get("dii_buy", 0), vals.get("dii_sell", 0), vals.get("dii_net", 0), now))
            stored += 1
            if not latest_date or d > latest_date:
                latest_date = d

        conn.commit()
        conn.close()
        logger.info(f"✅ FII/DII via NSE API: {stored} entries (latest: {latest_date})")
        return {"status": "ok", "entries": stored, "latest_date": latest_date}

    except Exception as e:
        logger.error(f"FII/DII NSE API fallback failed: {e}")
        return {"status": "error", "message": str(e), "entries": 0}


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

    net_liquidity = latest.get("fii_net", 0) + latest.get("dii_net", 0)
    total_act = abs(latest.get("fii_net", 0)) + abs(latest.get("dii_net", 0))
    fii_pct = round(abs(latest.get("fii_net", 0)) / total_act * 100) if total_act > 0 else 50

    fn = latest.get("fii_net", 0)
    dn = latest.get("dii_net", 0)
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

    return {
        "latest": {
            "date": latest["date"],
            "fii_buy": round(latest["fii_buy"], 2), "fii_sell": round(latest["fii_sell"], 2),
            "fii_net": round(latest["fii_net"], 2),
            "dii_buy": round(latest["dii_buy"], 2), "dii_sell": round(latest["dii_sell"], 2),
            "dii_net": round(latest["dii_net"], 2),
            "net_liquidity": round(net_liquidity, 2),
            "fii_pct": fii_pct, "dii_pct": 100 - fii_pct,
        },
        "streaks": {"fii": fii_streak, "dii": dii_streak},
        "cumulative": {
            "fii_5d": round(fii_5d, 2), "dii_5d": round(dii_5d, 2),
            "fii_20d": round(fii_20d, 2), "dii_20d": round(dii_20d, 2),
            "fii_5d_avg": round(fii_5d / min(5, len(rows)), 2) if rows else 0,
            "dii_5d_avg": round(dii_5d / min(5, len(rows)), 2) if rows else 0,
        },
        "sentiment": sentiment, "sentiment_color": scol,
        "history": history, "days": len(rows),
    }


def _compute_streak(net_values):
    if not net_values:
        return {"days": 0, "direction": "Neutral", "total": 0}
    direction = "Buying" if net_values[0] >= 0 else "Selling"
    days = total = 0
    for v in net_values:
        if (direction == "Buying" and v >= 0) or (direction == "Selling" and v < 0):
            days += 1; total += v
        else:
            break
    return {"days": days, "direction": direction, "total": round(total, 2)}
