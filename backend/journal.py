"""
journal.py — Trading Journal (Edgewonk-style)
- Trade log with R-Multiple calculation
- Psychology/emotion tracking per trade
- Setup classification (VCP, Breakout, SVRO, etc.)
- Performance analytics (win rate, expectancy, by setup, by regime)
- SQLite persistence
"""

import sqlite3, logging, json
from pathlib import Path
from datetime import datetime, timezone, date
from typing import Optional

logger = logging.getLogger(__name__)
DB_PATH = str(Path(__file__).parent / "breadth_data.db")


def _ensure_journal_tables():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS journal_trades (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            ticker TEXT NOT NULL,
            direction TEXT DEFAULT 'Long',       -- Long / Short
            setup_type TEXT DEFAULT '',           -- VCP, Breakout, Pullback, SVRO, MeanRev, PocketPivot, Other
            timeframe TEXT DEFAULT 'Swing',       -- Intraday, Swing, Positional
            regime TEXT DEFAULT '',               -- Bullish, Neutral, Distribution, Bearish (Q-BRAM at entry)

            entry_date TEXT NOT NULL,
            entry_price REAL NOT NULL,
            stop_loss REAL,
            target REAL,
            quantity REAL DEFAULT 0,
            position_size_pct REAL DEFAULT 0,     -- % of capital

            exit_date TEXT,
            exit_price REAL,
            status TEXT DEFAULT 'Open',           -- Open, Closed, StoppedOut

            -- Computed fields (updated on save)
            pnl_amount REAL DEFAULT 0,
            pnl_pct REAL DEFAULT 0,
            r_multiple REAL DEFAULT 0,
            holding_days INTEGER DEFAULT 0,

            -- Psychology
            pre_emotion TEXT DEFAULT '',          -- Confident, FOMO, Revenge, Bored, Patient, Fearful
            post_review TEXT DEFAULT '',           -- FollowedPlan, ExitedEarly, EnteredEarly, MovedStop, Oversized, Chased, NoSetup
            discipline_score INTEGER DEFAULT 0,   -- 1-10
            notes TEXT DEFAULT '',

            -- Metadata
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS journal_settings (
            key TEXT PRIMARY KEY,
            value TEXT
        );

        CREATE INDEX IF NOT EXISTS idx_journal_ticker ON journal_trades(ticker);
        CREATE INDEX IF NOT EXISTS idx_journal_status ON journal_trades(status);
        CREATE INDEX IF NOT EXISTS idx_journal_entry_date ON journal_trades(entry_date);
    """)
    conn.commit()
    conn.close()


def _compute_trade_fields(trade: dict) -> dict:
    """Compute R-Multiple, P&L, holding days from trade data."""
    entry = trade.get("entry_price", 0)
    exit_p = trade.get("exit_price")
    stop = trade.get("stop_loss")
    qty = trade.get("quantity", 0)
    direction = trade.get("direction", "Long")

    # P&L
    if exit_p and entry > 0:
        if direction == "Long":
            pnl_pct = (exit_p - entry) / entry * 100
        else:
            pnl_pct = (entry - exit_p) / entry * 100
        pnl_amount = pnl_pct / 100 * entry * qty
    else:
        pnl_pct = 0
        pnl_amount = 0

    # R-Multiple
    r_multiple = 0
    if stop and entry > 0 and exit_p:
        risk = abs(entry - stop)
        if risk > 0:
            if direction == "Long":
                reward = exit_p - entry
            else:
                reward = entry - exit_p
            r_multiple = round(reward / risk, 2)

    # Holding days
    holding_days = 0
    entry_date = trade.get("entry_date")
    exit_date = trade.get("exit_date")
    if entry_date and exit_date:
        try:
            d1 = datetime.strptime(entry_date[:10], "%Y-%m-%d")
            d2 = datetime.strptime(exit_date[:10], "%Y-%m-%d")
            holding_days = (d2 - d1).days
        except:
            pass
    elif entry_date:
        try:
            d1 = datetime.strptime(entry_date[:10], "%Y-%m-%d")
            holding_days = (datetime.now() - d1).days
        except:
            pass

    return {
        "pnl_amount": round(pnl_amount, 2),
        "pnl_pct": round(pnl_pct, 2),
        "r_multiple": r_multiple,
        "holding_days": holding_days,
    }


def add_trade(trade: dict) -> dict:
    """Add a new trade to the journal."""
    _ensure_journal_tables()
    computed = _compute_trade_fields(trade)

    conn = sqlite3.connect(DB_PATH, timeout=10)
    now = datetime.now(timezone.utc).isoformat()
    cur = conn.execute("""
        INSERT INTO journal_trades (
            ticker, direction, setup_type, timeframe, regime,
            entry_date, entry_price, stop_loss, target, quantity, position_size_pct,
            exit_date, exit_price, status,
            pnl_amount, pnl_pct, r_multiple, holding_days,
            pre_emotion, post_review, discipline_score, notes,
            created_at, updated_at
        ) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
    """, (
        trade.get("ticker", "").upper().strip(),
        trade.get("direction", "Long"),
        trade.get("setup_type", ""),
        trade.get("timeframe", "Swing"),
        trade.get("regime", ""),
        trade.get("entry_date", ""),
        trade.get("entry_price", 0),
        trade.get("stop_loss"),
        trade.get("target"),
        trade.get("quantity", 0),
        trade.get("position_size_pct", 0),
        trade.get("exit_date"),
        trade.get("exit_price"),
        trade.get("status", "Open"),
        computed["pnl_amount"], computed["pnl_pct"],
        computed["r_multiple"], computed["holding_days"],
        trade.get("pre_emotion", ""),
        trade.get("post_review", ""),
        trade.get("discipline_score", 0),
        trade.get("notes", ""),
        now, now,
    ))
    conn.commit()
    trade_id = cur.lastrowid
    conn.close()
    logger.info(f"Journal: added trade #{trade_id} {trade.get('ticker')} {trade.get('direction')}")
    return {"status": "ok", "id": trade_id}


def update_trade(trade_id: int, updates: dict) -> dict:
    """Update an existing trade."""
    _ensure_journal_tables()
    conn = sqlite3.connect(DB_PATH, timeout=10)

    existing = conn.execute("SELECT * FROM journal_trades WHERE id=?", (trade_id,)).fetchone()
    if not existing:
        conn.close()
        return {"error": "Trade not found"}

    # Merge existing + updates
    cols = [d[0] for d in conn.execute("PRAGMA table_info(journal_trades)").fetchall()]
    trade = dict(zip(cols, existing))
    for k, v in updates.items():
        if k in trade and k not in ("id", "created_at"):
            trade[k] = v

    # Recompute
    computed = _compute_trade_fields(trade)
    trade.update(computed)
    trade["updated_at"] = datetime.now(timezone.utc).isoformat()

    # Update DB
    set_parts = []
    params = []
    for k in ("ticker", "direction", "setup_type", "timeframe", "regime",
              "entry_date", "entry_price", "stop_loss", "target", "quantity", "position_size_pct",
              "exit_date", "exit_price", "status",
              "pnl_amount", "pnl_pct", "r_multiple", "holding_days",
              "pre_emotion", "post_review", "discipline_score", "notes", "updated_at"):
        set_parts.append(f"{k}=?")
        params.append(trade.get(k))
    params.append(trade_id)

    conn.execute(f"UPDATE journal_trades SET {', '.join(set_parts)} WHERE id=?", params)
    conn.commit()
    conn.close()
    return {"status": "ok", "id": trade_id}


def delete_trade(trade_id: int) -> dict:
    _ensure_journal_tables()
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("DELETE FROM journal_trades WHERE id=?", (trade_id,))
    conn.commit()
    conn.close()
    return {"status": "ok"}


def get_trades(status: str = "all", limit: int = 200) -> list:
    """Get trades, newest first."""
    _ensure_journal_tables()
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    if status == "all":
        rows = conn.execute("SELECT * FROM journal_trades ORDER BY entry_date DESC LIMIT ?", (limit,)).fetchall()
    else:
        rows = conn.execute("SELECT * FROM journal_trades WHERE status=? ORDER BY entry_date DESC LIMIT ?", (status, limit)).fetchall()
    conn.close()

    trades = [dict(r) for r in rows]

    # Enrich open trades with live price
    _enrich_live_prices(trades)

    return trades


def _enrich_live_prices(trades: list):
    """For open trades, fetch latest price from OHLCV and compute live P&L."""
    open_tickers = set(t["ticker"] for t in trades if t["status"] == "Open")
    if not open_tickers:
        return

    conn = sqlite3.connect(DB_PATH, timeout=10)
    live_prices = {}
    for ticker in open_tickers:
        row = conn.execute(
            "SELECT close FROM ohlcv WHERE ticker=? ORDER BY date DESC LIMIT 1", (ticker,)
        ).fetchone()
        if row:
            live_prices[ticker] = row[0]
    conn.close()

    for t in trades:
        if t["status"] == "Open" and t["ticker"] in live_prices:
            ltp = live_prices[t["ticker"]]
            t["live_price"] = ltp
            entry = t["entry_price"]
            if entry > 0:
                if t["direction"] == "Long":
                    t["live_pnl_pct"] = round((ltp - entry) / entry * 100, 2)
                else:
                    t["live_pnl_pct"] = round((entry - ltp) / entry * 100, 2)
                t["live_pnl_amount"] = round(t["live_pnl_pct"] / 100 * entry * (t["quantity"] or 0), 2)
                # Live R-Multiple
                if t.get("stop_loss") and abs(entry - t["stop_loss"]) > 0:
                    risk = abs(entry - t["stop_loss"])
                    reward = (ltp - entry) if t["direction"] == "Long" else (entry - ltp)
                    t["live_r_multiple"] = round(reward / risk, 2)


def get_analytics() -> dict:
    """Compute journal performance analytics."""
    _ensure_journal_tables()
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    all_trades = conn.execute("SELECT * FROM journal_trades ORDER BY entry_date").fetchall()
    conn.close()

    trades = [dict(r) for r in all_trades]
    closed = [t for t in trades if t["status"] in ("Closed", "StoppedOut")]
    opens = [t for t in trades if t["status"] == "Open"]

    if not trades:
        return {"total": 0, "message": "No trades yet"}

    # Basic stats
    total = len(trades)
    total_closed = len(closed)
    winners = [t for t in closed if t["pnl_pct"] > 0]
    losers = [t for t in closed if t["pnl_pct"] < 0]
    breakeven = [t for t in closed if t["pnl_pct"] == 0]

    win_rate = round(len(winners) / total_closed * 100, 1) if total_closed > 0 else 0
    avg_winner = round(sum(t["pnl_pct"] for t in winners) / len(winners), 2) if winners else 0
    avg_loser = round(sum(t["pnl_pct"] for t in losers) / len(losers), 2) if losers else 0
    avg_r = round(sum(t["r_multiple"] for t in closed) / total_closed, 2) if total_closed else 0
    total_pnl = round(sum(t["pnl_amount"] for t in closed), 2)
    avg_hold = round(sum(t["holding_days"] for t in closed) / total_closed, 1) if total_closed else 0

    # Expectancy = (Win% × Avg Win) + (Loss% × Avg Loss)
    expectancy = round((win_rate/100 * avg_winner) + ((1 - win_rate/100) * avg_loser), 2) if total_closed else 0

    # Profit factor = gross profit / gross loss
    gross_profit = sum(t["pnl_amount"] for t in winners)
    gross_loss = abs(sum(t["pnl_amount"] for t in losers))
    profit_factor = round(gross_profit / gross_loss, 2) if gross_loss > 0 else float('inf') if gross_profit > 0 else 0

    # R-Multiple distribution
    r_values = [t["r_multiple"] for t in closed if t["r_multiple"] != 0]

    # By setup type
    by_setup = {}
    for t in closed:
        s = t.get("setup_type") or "Unknown"
        if s not in by_setup:
            by_setup[s] = {"trades": 0, "wins": 0, "total_r": 0, "total_pnl": 0}
        by_setup[s]["trades"] += 1
        if t["pnl_pct"] > 0:
            by_setup[s]["wins"] += 1
        by_setup[s]["total_r"] += t["r_multiple"]
        by_setup[s]["total_pnl"] += t["pnl_amount"]
    for s in by_setup:
        n = by_setup[s]["trades"]
        by_setup[s]["win_rate"] = round(by_setup[s]["wins"] / n * 100, 1) if n else 0
        by_setup[s]["avg_r"] = round(by_setup[s]["total_r"] / n, 2) if n else 0

    # By regime
    by_regime = {}
    for t in closed:
        r = t.get("regime") or "Unknown"
        if r not in by_regime:
            by_regime[r] = {"trades": 0, "wins": 0, "total_r": 0}
        by_regime[r]["trades"] += 1
        if t["pnl_pct"] > 0:
            by_regime[r]["wins"] += 1
        by_regime[r]["total_r"] += t["r_multiple"]
    for r in by_regime:
        n = by_regime[r]["trades"]
        by_regime[r]["win_rate"] = round(by_regime[r]["wins"] / n * 100, 1) if n else 0
        by_regime[r]["avg_r"] = round(by_regime[r]["total_r"] / n, 2) if n else 0

    # By emotion
    by_emotion = {}
    for t in closed:
        e = t.get("pre_emotion") or "None"
        if e not in by_emotion:
            by_emotion[e] = {"trades": 0, "wins": 0, "total_r": 0}
        by_emotion[e]["trades"] += 1
        if t["pnl_pct"] > 0:
            by_emotion[e]["wins"] += 1
        by_emotion[e]["total_r"] += t["r_multiple"]
    for e in by_emotion:
        n = by_emotion[e]["trades"]
        by_emotion[e]["win_rate"] = round(by_emotion[e]["wins"] / n * 100, 1) if n else 0
        by_emotion[e]["avg_r"] = round(by_emotion[e]["total_r"] / n, 2) if n else 0

    # Equity curve (cumulative P&L over closed trades by date)
    equity_curve = []
    cum_pnl = 0
    for t in sorted(closed, key=lambda x: x.get("exit_date") or x.get("entry_date") or ""):
        cum_pnl += t["pnl_amount"]
        equity_curve.append({
            "date": t.get("exit_date") or t.get("entry_date"),
            "pnl": round(cum_pnl, 2),
            "ticker": t["ticker"],
            "r": t["r_multiple"],
        })

    # Streaks
    current_streak = 0
    max_win_streak = 0
    max_loss_streak = 0
    streak = 0
    for t in sorted(closed, key=lambda x: x.get("exit_date") or ""):
        if t["pnl_pct"] > 0:
            streak = streak + 1 if streak > 0 else 1
            max_win_streak = max(max_win_streak, streak)
        elif t["pnl_pct"] < 0:
            streak = streak - 1 if streak < 0 else -1
            max_loss_streak = max(max_loss_streak, abs(streak))
        else:
            streak = 0
    current_streak = streak

    # Mistake frequency
    mistakes = {}
    for t in closed:
        for m in (t.get("post_review") or "").split(","):
            m = m.strip()
            if m and m != "FollowedPlan":
                mistakes[m] = mistakes.get(m, 0) + 1

    return {
        "total": total,
        "open": len(opens),
        "closed": total_closed,
        "winners": len(winners),
        "losers": len(losers),
        "win_rate": win_rate,
        "avg_winner": avg_winner,
        "avg_loser": avg_loser,
        "avg_r": avg_r,
        "expectancy": expectancy,
        "profit_factor": profit_factor,
        "total_pnl": total_pnl,
        "avg_holding_days": avg_hold,
        "r_values": r_values,
        "by_setup": by_setup,
        "by_regime": by_regime,
        "by_emotion": by_emotion,
        "equity_curve": equity_curve,
        "current_streak": current_streak,
        "max_win_streak": max_win_streak,
        "max_loss_streak": max_loss_streak,
        "mistakes": mistakes,
    }


def get_settings() -> dict:
    _ensure_journal_tables()
    conn = sqlite3.connect(DB_PATH, timeout=10)
    rows = conn.execute("SELECT key, value FROM journal_settings").fetchall()
    conn.close()
    settings = {}
    for k, v in rows:
        try:
            settings[k] = json.loads(v)
        except:
            settings[k] = v
    # Defaults
    settings.setdefault("starting_capital", 1000000)
    settings.setdefault("max_risk_per_trade", 1.0)
    settings.setdefault("default_setup", "VCP")
    return settings


def save_settings(settings: dict) -> dict:
    _ensure_journal_tables()
    conn = sqlite3.connect(DB_PATH, timeout=10)
    for k, v in settings.items():
        conn.execute("INSERT OR REPLACE INTO journal_settings (key, value) VALUES (?,?)",
                     (k, json.dumps(v)))
    conn.commit()
    conn.close()
    return {"status": "ok"}
