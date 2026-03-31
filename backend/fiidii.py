"""
FII/DII Institutional Money Flow — India Market
Fetches daily FII/DII cash market data from NSE.
Uses same proven session pattern as insider.py.
"""

import sqlite3, logging, time, httpx
from pathlib import Path
from datetime import datetime, timedelta, timezone

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


def _parse_cr(val) -> float:
    if not val: return 0.0
    try: return float(str(val).replace(",", "").strip())
    except: return 0.0


def fetch_fiidii_from_nse() -> dict:
    """Fetch FII/DII data from NSE — uses same session pattern as insider.py."""
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                       "(KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        "Accept": "application/json, text/plain, */*",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nseindia.com/reports/fii-dii",
    }
    try:
        client = httpx.Client(follow_redirects=True, timeout=20)
        r0 = client.get("https://www.nseindia.com/", headers={"User-Agent": headers["User-Agent"]})
        logger.info(f"NSE session: {r0.status_code}, cookies: {len(r0.cookies)}")
        time.sleep(0.5)

        r = client.get("https://www.nseindia.com/api/fiidiiTradeReact", headers=headers)
        client.close()

        if r.status_code != 200:
            return {"error": f"NSE returned {r.status_code}", "entries": 0}

        data = r.json()
        if not isinstance(data, list):
            return {"error": "Unexpected response format", "entries": 0}

        logger.info(f"Fetched {len(data)} FII/DII entries from NSE")
        _ensure_fiidii_table()
        conn = sqlite3.connect(str(DB_PATH), timeout=10)

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

            buy = _parse_cr(entry.get("buyValue", "0"))
            sell = _parse_cr(entry.get("sellValue", "0"))
            net = _parse_cr(entry.get("netValue", "0"))

            if "FII" in cat or "FPI" in cat:
                date_data[iso_date].update({"fii_buy": buy, "fii_sell": sell, "fii_net": net})
            elif "DII" in cat:
                date_data[iso_date].update({"dii_buy": buy, "dii_sell": sell, "dii_net": net})

        now = datetime.now(timezone.utc).isoformat()
        stored = 0
        latest_date = None
        for date, vals in date_data.items():
            conn.execute("""
                INSERT OR REPLACE INTO fiidii
                (date, fii_buy, fii_sell, fii_net, dii_buy, dii_sell, dii_net, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (date, vals.get("fii_buy",0), vals.get("fii_sell",0), vals.get("fii_net",0),
                  vals.get("dii_buy",0), vals.get("dii_sell",0), vals.get("dii_net",0), now))
            stored += 1
            if not latest_date or date > latest_date: latest_date = date

        conn.commit()
        conn.close()
        logger.info(f"Stored {stored} FII/DII entries (latest: {latest_date})")
        return {"entries": stored, "latest_date": latest_date}

    except Exception as e:
        logger.error(f"FII/DII fetch failed: {e}")
        import traceback; traceback.print_exc()
        return {"error": str(e), "entries": 0}


def get_fiidii_summary(days: int = 60) -> dict:
    """Get FII/DII summary — latest session + streaks + cumulative + history."""
    _ensure_fiidii_table()
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM fiidii ORDER BY date DESC LIMIT ?", (days,)).fetchall()
    conn.close()

    if not rows:
        return {"error": "No FII/DII data. Click Force Sync to fetch from NSE.", "data": []}

    latest = dict(rows[0])
    fii_streak = _compute_streak([r["fii_net"] for r in rows])
    dii_streak = _compute_streak([r["dii_net"] for r in rows])

    fii_5d = sum(r["fii_net"] for r in rows[:5])
    dii_5d = sum(r["dii_net"] for r in rows[:5])
    fii_20d = sum(r["fii_net"] for r in rows[:20])
    dii_20d = sum(r["dii_net"] for r in rows[:20])

    net_liquidity = latest.get("fii_net", 0) + latest.get("dii_net", 0)
    total_activity = abs(latest.get("fii_net", 0)) + abs(latest.get("dii_net", 0))
    fii_pct = round(abs(latest.get("fii_net", 0)) / total_activity * 100) if total_activity > 0 else 50

    fii_net = latest.get("fii_net", 0)
    dii_net = latest.get("dii_net", 0)
    if fii_net < -500 and dii_net > 500:
        sentiment, sentiment_color = "AGGRESSIVE SELLING", "#ef4444"
    elif fii_net > 500 and dii_net > 0:
        sentiment, sentiment_color = "STRONG BUYING", "#22c55e"
    elif fii_net > 0:
        sentiment, sentiment_color = "FII BUYING", "#4ade80"
    elif fii_net < 0 and dii_net > 0:
        sentiment, sentiment_color = "DII SUPPORT", "#f59e0b"
    else:
        sentiment, sentiment_color = "NEUTRAL", "#64748b"

    history = [{"date": r["date"], "fii_net": round(r["fii_net"], 2),
                "dii_net": round(r["dii_net"], 2), "net": round(r["fii_net"] + r["dii_net"], 2)}
               for r in reversed(rows)]

    fii_5d_avg = round(fii_5d / min(5, len(rows)), 2) if rows else 0
    dii_5d_avg = round(dii_5d / min(5, len(rows)), 2) if rows else 0

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
            "fii_5d_avg": fii_5d_avg, "dii_5d_avg": dii_5d_avg,
        },
        "sentiment": sentiment, "sentiment_color": sentiment_color,
        "history": history, "days": len(rows),
    }


def _compute_streak(net_values: list) -> dict:
    if not net_values:
        return {"days": 0, "direction": "Neutral", "total": 0}
    direction = "Buying" if net_values[0] >= 0 else "Selling"
    streak_days = 0
    streak_total = 0
    for val in net_values:
        if (direction == "Buying" and val >= 0) or (direction == "Selling" and val < 0):
            streak_days += 1
            streak_total += val
        else:
            break
    return {"days": streak_days, "direction": direction, "total": round(streak_total, 2)}
