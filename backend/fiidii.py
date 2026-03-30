"""
FII/DII Institutional Money Flow — India Market
Fetches daily FII (Foreign) and DII (Domestic) buy/sell data from NSE.
Stores historical data in SQLite for trend analysis.

API: https://www.nseindia.com/api/fiidiiTradeReact
Returns JSON array with daily FII/DII cash market activity.

Key metrics computed:
- FII Net (Buy - Sell) — positive = foreign buying
- DII Net (Buy - Sell) — positive = domestic buying  
- Net Liquidity Injection (FII Net + DII Net)
- FII/DII Streak (consecutive days buying or selling)
- 5-day cumulative, 20-day cumulative
- FII NIFTY500 Ownership % (from breadth data)
"""

import sqlite3, logging, json, time
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Optional

logger = logging.getLogger(__name__)
DB_PATH = Path(__file__).parent / "breadth_data.db"


def _ensure_fiidii_table():
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS fiidii (
            date TEXT PRIMARY KEY,
            fii_buy REAL DEFAULT 0,
            fii_sell REAL DEFAULT 0,
            fii_net REAL DEFAULT 0,
            dii_buy REAL DEFAULT 0,
            dii_sell REAL DEFAULT 0,
            dii_net REAL DEFAULT 0,
            fetched_at TEXT
        )
    """)
    conn.commit()
    conn.close()


def fetch_fiidii_from_nse() -> dict:
    """
    Fetch FII/DII daily data from NSE API.
    Returns: {entries: int, latest_date: str, data: [...]}
    
    NSE API returns JSON array like:
    [
      {
        "category": "FII/FPI *",
        "date": "28-Mar-2026",
        "buyValue": "12345.67",
        "sellValue": "23456.78",
        "netValue": "-11111.11"
      },
      {
        "category": "DII **",
        "date": "28-Mar-2026",
        ...
      }
    ]
    """
    import httpx
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Accept": "application/json",
        "Accept-Language": "en-US,en;q=0.9",
        "Referer": "https://www.nseindia.com/reports/fii-dii",
    }
    
    client = httpx.Client(headers=headers, follow_redirects=True, timeout=15)
    
    try:
        # Get session cookies first
        r = client.get("https://www.nseindia.com/")
        logger.info(f"NSE session: {r.status_code}, cookies: {len(client.cookies)}")
        
        # Fetch FII/DII data
        r = client.get("https://www.nseindia.com/api/fiidiiTradeReact")
        if r.status_code != 200:
            return {"error": f"NSE API returned {r.status_code}", "entries": 0}
        
        data = r.json()
        if not isinstance(data, list):
            return {"error": "Unexpected API response format", "entries": 0}
        
        logger.info(f"Fetched {len(data)} FII/DII entries from NSE")
        
        # Parse and store
        _ensure_fiidii_table()
        conn = sqlite3.connect(str(DB_PATH), timeout=10)
        
        stored = 0
        latest_date = None
        
        # Group entries by date (each date has FII + DII rows)
        date_data = {}
        for entry in data:
            cat = entry.get("category", "").strip()
            date_str = entry.get("date", "").strip()
            
            if not date_str:
                continue
            
            # Parse NSE date format "28-Mar-2026" → "2026-03-28"
            try:
                dt = datetime.strptime(date_str, "%d-%b-%Y")
                iso_date = dt.strftime("%Y-%m-%d")
            except:
                continue
            
            if iso_date not in date_data:
                date_data[iso_date] = {}
            
            # Parse values (in Crores, stored as-is)
            buy = _parse_cr(entry.get("buyValue", "0"))
            sell = _parse_cr(entry.get("sellValue", "0"))
            net = _parse_cr(entry.get("netValue", "0"))
            
            if "FII" in cat or "FPI" in cat:
                date_data[iso_date]["fii_buy"] = buy
                date_data[iso_date]["fii_sell"] = sell
                date_data[iso_date]["fii_net"] = net
            elif "DII" in cat:
                date_data[iso_date]["dii_buy"] = buy
                date_data[iso_date]["dii_sell"] = sell
                date_data[iso_date]["dii_net"] = net
        
        # Store in DB
        now = datetime.now(timezone.utc).isoformat()
        for date, vals in date_data.items():
            conn.execute("""
                INSERT OR REPLACE INTO fiidii 
                (date, fii_buy, fii_sell, fii_net, dii_buy, dii_sell, dii_net, fetched_at)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                date,
                vals.get("fii_buy", 0), vals.get("fii_sell", 0), vals.get("fii_net", 0),
                vals.get("dii_buy", 0), vals.get("dii_sell", 0), vals.get("dii_net", 0),
                now,
            ))
            stored += 1
            if not latest_date or date > latest_date:
                latest_date = date
        
        conn.commit()
        conn.close()
        
        logger.info(f"Stored {stored} FII/DII daily entries (latest: {latest_date})")
        return {"entries": stored, "latest_date": latest_date}
    
    except Exception as e:
        logger.error(f"FII/DII fetch failed: {e}")
        return {"error": str(e), "entries": 0}
    finally:
        client.close()


def _parse_cr(val) -> float:
    """Parse NSE value string (e.g., '12,345.67' or '-1,234.56') to float in Cr."""
    if not val:
        return 0.0
    try:
        return float(str(val).replace(",", "").strip())
    except:
        return 0.0


def get_fiidii_summary(days: int = 30) -> dict:
    """
    Get FII/DII summary for the dashboard.
    Returns latest session data + streaks + cumulative flows.
    """
    _ensure_fiidii_table()
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.row_factory = sqlite3.Row
    
    rows = conn.execute(
        "SELECT * FROM fiidii ORDER BY date DESC LIMIT ?", (days,)
    ).fetchall()
    conn.close()
    
    if not rows:
        return {"error": "No FII/DII data. Click Sync to fetch from NSE.", "data": []}
    
    latest = dict(rows[0])
    
    # Compute streaks
    fii_streak = _compute_streak([r["fii_net"] for r in rows])
    dii_streak = _compute_streak([r["dii_net"] for r in rows])
    
    # Cumulative flows
    fii_5d = sum(r["fii_net"] for r in rows[:5])
    dii_5d = sum(r["dii_net"] for r in rows[:5])
    fii_20d = sum(r["fii_net"] for r in rows[:20])
    dii_20d = sum(r["dii_net"] for r in rows[:20])
    
    # Net liquidity
    net_liquidity = latest.get("fii_net", 0) + latest.get("dii_net", 0)
    
    # FII vs DII balance (as percentage)
    total_activity = abs(latest.get("fii_net", 0)) + abs(latest.get("dii_net", 0))
    fii_pct = round(abs(latest.get("fii_net", 0)) / total_activity * 100) if total_activity > 0 else 50
    
    # Sentiment
    if latest.get("fii_net", 0) < -500 and latest.get("dii_net", 0) > 500:
        sentiment = "AGGRESSIVE SELLING"
        sentiment_color = "#ef4444"
    elif latest.get("fii_net", 0) > 500 and latest.get("dii_net", 0) > 0:
        sentiment = "STRONG BUYING"
        sentiment_color = "#22c55e"
    elif latest.get("fii_net", 0) > 0:
        sentiment = "FII BUYING"
        sentiment_color = "#4ade80"
    elif latest.get("fii_net", 0) < 0 and latest.get("dii_net", 0) > 0:
        sentiment = "DII SUPPORT"
        sentiment_color = "#f59e0b"
    else:
        sentiment = "NEUTRAL"
        sentiment_color = "#64748b"
    
    # Daily history for chart
    history = [{
        "date": r["date"],
        "fii_net": round(r["fii_net"], 2),
        "dii_net": round(r["dii_net"], 2),
        "net": round(r["fii_net"] + r["dii_net"], 2),
    } for r in reversed(rows)]
    
    return {
        "latest": {
            "date": latest["date"],
            "fii_buy": round(latest["fii_buy"], 2),
            "fii_sell": round(latest["fii_sell"], 2),
            "fii_net": round(latest["fii_net"], 2),
            "dii_buy": round(latest["dii_buy"], 2),
            "dii_sell": round(latest["dii_sell"], 2),
            "dii_net": round(latest["dii_net"], 2),
            "net_liquidity": round(net_liquidity, 2),
            "fii_pct": fii_pct,
            "dii_pct": 100 - fii_pct,
        },
        "streaks": {
            "fii": fii_streak,
            "dii": dii_streak,
        },
        "cumulative": {
            "fii_5d": round(fii_5d, 2),
            "dii_5d": round(dii_5d, 2),
            "fii_20d": round(fii_20d, 2),
            "dii_20d": round(dii_20d, 2),
        },
        "sentiment": sentiment,
        "sentiment_color": sentiment_color,
        "history": history,
        "days": len(rows),
    }


def _compute_streak(net_values: list) -> dict:
    """
    Compute buying/selling streak from most recent values.
    net_values[0] = most recent day.
    Returns: {days: int, direction: 'Buying'/'Selling', total: float}
    """
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
    
    return {
        "days": streak_days,
        "direction": direction,
        "total": round(streak_total, 2),
    }
