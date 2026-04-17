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
            direction TEXT DEFAULT 'Long',
            setup_type TEXT DEFAULT '',
            timeframe TEXT DEFAULT 'Swing',
            regime TEXT DEFAULT '',
            broker TEXT DEFAULT '',
            market_type TEXT DEFAULT 'Stocks',

            entry_date TEXT NOT NULL,
            entry_time TEXT DEFAULT '',
            entry_price REAL NOT NULL,
            stop_loss REAL,
            target REAL,
            quantity REAL DEFAULT 0,
            position_size_pct REAL DEFAULT 0,
            fees REAL DEFAULT 0,
            risk_amount REAL DEFAULT 0,

            exit_date TEXT,
            exit_time TEXT DEFAULT '',
            exit_price REAL,
            status TEXT DEFAULT 'Open',

            pnl_amount REAL DEFAULT 0,
            pnl_pct REAL DEFAULT 0,
            r_multiple REAL DEFAULT 0,
            holding_days INTEGER DEFAULT 0,
            mae REAL DEFAULT 0,
            mfe REAL DEFAULT 0,

            -- Psychology sliders (1-10)
            psych_confidence INTEGER DEFAULT 0,
            psych_focus INTEGER DEFAULT 0,
            psych_stress INTEGER DEFAULT 0,
            psych_patience INTEGER DEFAULT 0,
            psych_fomo INTEGER DEFAULT 0,
            psych_revenge INTEGER DEFAULT 0,
            psych_sleep INTEGER DEFAULT 0,
            psych_energy INTEGER DEFAULT 0,

            -- Legacy single field kept for compat
            pre_emotion TEXT DEFAULT '',
            discipline_score INTEGER DEFAULT 0,

            -- Post-trade
            post_review TEXT DEFAULT '',
            followed_plan INTEGER DEFAULT 1,
            trade_grade TEXT DEFAULT '',
            would_repeat INTEGER DEFAULT 1,
            post_notes TEXT DEFAULT '',
            notes TEXT DEFAULT '',

            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS journal_settings (
            key TEXT PRIMARY KEY,
            value TEXT
        );

        CREATE TABLE IF NOT EXISTS journal_accounts (
            id   INTEGER PRIMARY KEY AUTOINCREMENT,
            name TEXT NOT NULL,
            broker TEXT DEFAULT '',
            currency TEXT DEFAULT 'INR',
            starting_capital REAL DEFAULT 1000000,
            color TEXT DEFAULT '#06b6d4',
            notes TEXT DEFAULT '',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        CREATE INDEX IF NOT EXISTS idx_journal_ticker ON journal_trades(ticker);
        CREATE INDEX IF NOT EXISTS idx_journal_status ON journal_trades(status);
        CREATE INDEX IF NOT EXISTS idx_journal_entry_date ON journal_trades(entry_date);
    """)
    conn.commit()

    # Add new columns to existing DBs (ALTER TABLE is idempotent via try/except)
    new_cols = [
        ("account_id", "INTEGER DEFAULT 1"),
        ("strategy_name", "TEXT DEFAULT ''"),
        ("broker", "TEXT DEFAULT ''"),
        ("market_type", "TEXT DEFAULT 'Stocks'"),
        ("entry_time", "TEXT DEFAULT ''"),
        ("exit_time", "TEXT DEFAULT ''"),
        ("fees", "REAL DEFAULT 0"),
        ("risk_amount", "REAL DEFAULT 0"),
        ("mae", "REAL DEFAULT 0"),
        ("mfe", "REAL DEFAULT 0"),
        ("psych_confidence", "INTEGER DEFAULT 0"),
        ("psych_focus", "INTEGER DEFAULT 0"),
        ("psych_stress", "INTEGER DEFAULT 0"),
        ("psych_patience", "INTEGER DEFAULT 0"),
        ("psych_fomo", "INTEGER DEFAULT 0"),
        ("psych_revenge", "INTEGER DEFAULT 0"),
        ("psych_sleep", "INTEGER DEFAULT 0"),
        ("psych_energy", "INTEGER DEFAULT 0"),
        ("followed_plan", "INTEGER DEFAULT 1"),
        ("trade_grade", "TEXT DEFAULT ''"),
        ("would_repeat", "INTEGER DEFAULT 1"),
        ("post_notes", "TEXT DEFAULT ''"),
    ]
    for col, defn in new_cols:
        try:
            conn.execute(f"ALTER TABLE journal_trades ADD COLUMN {col} {defn}")
            conn.commit()
        except Exception:
            pass

    # Create account_id index AFTER the column is guaranteed to exist
    try:
        conn.execute("CREATE INDEX IF NOT EXISTS idx_journal_account ON journal_trades(account_id)")
        conn.commit()
    except Exception:
        pass

    # Seed default Account 1 if no accounts exist
    count = conn.execute("SELECT COUNT(*) FROM journal_accounts").fetchone()[0]
    if count == 0:
        conn.execute("""
            INSERT INTO journal_accounts (id, name, broker, currency, starting_capital, color)
            VALUES (1, 'Account 1', '', 'INR', 1000000, '#06b6d4')
        """)
        conn.commit()

    # Migrate existing trades to account_id=1 if they have NULL
    conn.execute("UPDATE journal_trades SET account_id=1 WHERE account_id IS NULL")
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
            account_id,
            ticker, direction, setup_type, strategy_name, timeframe, regime,
            broker, market_type,
            entry_date, entry_time, entry_price, stop_loss, target,
            quantity, position_size_pct, fees, risk_amount,
            exit_date, exit_time, exit_price, status,
            pnl_amount, pnl_pct, r_multiple, holding_days,
            mae, mfe,
            pre_emotion, post_review, discipline_score, notes, post_notes,
            followed_plan, trade_grade, would_repeat,
            psych_confidence, psych_focus, psych_stress, psych_patience,
            psych_fomo, psych_revenge, psych_sleep, psych_energy,
            created_at, updated_at
        ) VALUES (
            ?,
            ?,?,?,?,?,?,
            ?,?,
            ?,?,?,?,?,
            ?,?,?,?,
            ?,?,?,?,
            ?,?,?,?,
            ?,?,
            ?,?,?,?,?,
            ?,?,?,
            ?,?,?,?,
            ?,?,?,?,
            ?,?
        )
    """, (
        int(trade.get("account_id", 1) or 1),
        trade.get("ticker", "").upper().strip(),
        trade.get("direction", "Long"),
        trade.get("setup_type", ""),
        trade.get("strategy_name", ""),
        trade.get("timeframe", "Swing"),
        trade.get("regime", ""),
        trade.get("broker", ""),
        trade.get("market_type", "Stocks"),
        trade.get("entry_date", ""),
        trade.get("entry_time", ""),
        trade.get("entry_price", 0),
        trade.get("stop_loss"),
        trade.get("target"),
        trade.get("quantity", 0),
        trade.get("position_size_pct", 0),
        trade.get("fees", 0),
        trade.get("risk_amount", 0),
        trade.get("exit_date"),
        trade.get("exit_time", ""),
        trade.get("exit_price"),
        trade.get("status", "Open"),
        computed["pnl_amount"], computed["pnl_pct"],
        computed["r_multiple"], computed["holding_days"],
        trade.get("mae", 0),
        trade.get("mfe", 0),
        trade.get("pre_emotion", ""),
        trade.get("post_review", ""),
        trade.get("discipline_score", 0),
        trade.get("notes", ""),
        trade.get("post_notes", ""),
        trade.get("followed_plan", 1),
        trade.get("trade_grade", ""),
        trade.get("would_repeat", 1),
        trade.get("psych_confidence", 0),
        trade.get("psych_focus", 0),
        trade.get("psych_stress", 0),
        trade.get("psych_patience", 0),
        trade.get("psych_fomo", 0),
        trade.get("psych_revenge", 0),
        trade.get("psych_sleep", 0),
        trade.get("psych_energy", 0),
        now, now,
    ))
    conn.commit()
    trade_id = cur.lastrowid
    conn.close()
    logger.info(f"Journal: added trade #{trade_id} {trade.get('ticker')} {trade.get('direction')}")
    return {"status": "ok", "id": trade_id}


def update_trade(trade_id: int, updates: dict) -> dict:
    """Update an existing trade — writes ALL columns dynamically via PRAGMA."""
    _ensure_journal_tables()
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row  # named access — safe regardless of column order/count

    existing = conn.execute("SELECT * FROM journal_trades WHERE id=?", (trade_id,)).fetchone()
    if not existing:
        conn.close()
        return {"error": "Trade not found"}

    # Get live column list from DB — PRAGMA table_info columns: (cid, name, type, notnull, dflt, pk)
    all_cols = [d[1] for d in conn.execute("PRAGMA table_info(journal_trades)").fetchall()]

    # Build trade dict from named row (avoids zip misalignment on old rows with fewer columns)
    trade = {col: existing[col] if col in existing.keys() else None for col in all_cols}

    # Apply incoming updates (only valid column names, never overwrite id/created_at)
    for k, v in updates.items():
        if k in all_cols and k not in ("id", "created_at"):
            trade[k] = v

    # Recompute derived fields (P&L, R-Multiple, holding days)
    computed = _compute_trade_fields(trade)
    trade.update(computed)
    trade["updated_at"] = datetime.now(timezone.utc).isoformat()

    # Build SET clause from ALL writable columns (skip id + created_at)
    skip = {"id", "created_at"}
    set_parts = []
    params = []
    for k in all_cols:
        if k in skip:
            continue
        set_parts.append(f"{k}=?")
        params.append(trade.get(k))
    params.append(trade_id)

    conn.execute(f"UPDATE journal_trades SET {', '.join(set_parts)} WHERE id=?", params)
    conn.commit()
    conn.close()
    logger.info(f"Journal: updated trade #{trade_id}")
    return {"status": "ok", "id": trade_id}


def delete_trade(trade_id: int) -> dict:
    _ensure_journal_tables()
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("DELETE FROM journal_trades WHERE id=?", (trade_id,))
    conn.commit()
    conn.close()
    return {"status": "ok"}


def get_trades(status: str = "all", limit: int = 200, account_id: int = None) -> list:
    """Get trades, newest first. Optionally filtered by account_id (None = all accounts)."""
    _ensure_journal_tables()
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row

    if account_id is None:
        # All accounts
        if status == "all":
            rows = conn.execute("SELECT * FROM journal_trades ORDER BY entry_date DESC LIMIT ?", (limit,)).fetchall()
        else:
            rows = conn.execute("SELECT * FROM journal_trades WHERE status=? ORDER BY entry_date DESC LIMIT ?", (status, limit)).fetchall()
    else:
        if status == "all":
            rows = conn.execute("SELECT * FROM journal_trades WHERE account_id=? ORDER BY entry_date DESC LIMIT ?", (account_id, limit)).fetchall()
        else:
            rows = conn.execute("SELECT * FROM journal_trades WHERE account_id=? AND status=? ORDER BY entry_date DESC LIMIT ?", (account_id, status, limit)).fetchall()
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


def get_analytics(account_id: int = None) -> dict:
    """Compute journal performance analytics, optionally scoped to an account."""
    _ensure_journal_tables()
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    if account_id is None:
        all_trades = conn.execute("SELECT * FROM journal_trades ORDER BY entry_date").fetchall()
    else:
        all_trades = conn.execute("SELECT * FROM journal_trades WHERE account_id=? ORDER BY entry_date", (account_id,)).fetchall()
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
    settings.setdefault("max_daily_loss_pct", 2.0)
    settings.setdefault("max_weekly_drawdown_pct", 5.0)
    settings.setdefault("max_trades_per_day", 5)
    settings.setdefault("max_consecutive_losses", 3)
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


def get_tilt_score(trades: list) -> dict:
    """
    Compute Tilt Meter™ score (0-100) from recent trade patterns + psychology.
    0-30: Calm | 31-60: Warning | 61-80: Tilted | 81-100: Dangerous
    """
    score = 0
    factors = []

    # Look at last 10 trades
    recent = sorted(trades, key=lambda t: t.get("entry_date",""), reverse=True)[:10]
    closed_recent = [t for t in recent if t["status"] in ("Closed","StoppedOut")]

    if not recent:
        return {"score": 0, "level": "Calm", "color": "#22c55e", "factors": []}

    # Factor 1: consecutive losses (max 30 pts)
    streak = 0
    for t in closed_recent:
        if t.get("pnl_pct", 0) < 0:
            streak += 1
        else:
            break
    if streak >= 2:
        pts = min(30, streak * 12)
        score += pts
        factors.append(f"{streak} consecutive losses (+{pts})")

    # Factor 2: revenge emotion flag (max 20 pts)
    revenge_trades = [t for t in recent if t.get("pre_emotion") == "Revenge" or t.get("psych_revenge", 0) >= 7]
    if revenge_trades:
        pts = min(20, len(revenge_trades) * 10)
        score += pts
        factors.append(f"Revenge emotion detected (+{pts})")

    # Factor 3: FOMO trades (max 15 pts)
    fomo_trades = [t for t in recent if t.get("pre_emotion") == "FOMO" or t.get("psych_fomo", 0) >= 7]
    if fomo_trades:
        pts = min(15, len(fomo_trades) * 8)
        score += pts
        factors.append(f"FOMO entries detected (+{pts})")

    # Factor 4: overtrading (more than 3 trades same day, max 15 pts)
    from collections import Counter
    date_counts = Counter(t.get("entry_date","")[:10] for t in recent)
    overtrade_days = {d: c for d, c in date_counts.items() if c > 3}
    if overtrade_days:
        pts = min(15, len(overtrade_days) * 8)
        score += pts
        factors.append(f"Overtrading detected ({max(overtrade_days.values())} trades/day) (+{pts})")

    # Factor 5: low discipline score (max 10 pts)
    disc_scores = [t.get("discipline_score", 0) for t in recent if t.get("discipline_score")]
    if disc_scores:
        avg_disc = sum(disc_scores) / len(disc_scores)
        if avg_disc < 5:
            pts = min(10, int((5 - avg_disc) * 3))
            score += pts
            factors.append(f"Low discipline avg ({avg_disc:.1f}/10) (+{pts})")

    # Factor 6: low stress/focus from psych sliders (max 10 pts)
    stress_vals = [t.get("psych_stress", 0) for t in recent if t.get("psych_stress", 0) > 0]
    if stress_vals and sum(stress_vals)/len(stress_vals) >= 7:
        pts = 10
        score += pts
        factors.append(f"High stress detected (+{pts})")

    score = min(100, score)

    if score <= 30:
        level, color = "Calm", "#22c55e"
    elif score <= 60:
        level, color = "Warning", "#f59e0b"
    elif score <= 80:
        level, color = "Tilted", "#ef4444"
    else:
        level, color = "Dangerous", "#dc2626"

    return {"score": score, "level": level, "color": color, "factors": factors}


def get_drawdown_series(trades: list) -> dict:
    """Compute max drawdown and drawdown curve from closed trades."""
    closed = sorted(
        [t for t in trades if t["status"] in ("Closed","StoppedOut")],
        key=lambda t: t.get("exit_date") or t.get("entry_date") or ""
    )
    if not closed:
        return {"max_drawdown_pct": 0, "current_drawdown_pct": 0, "curve": []}

    peak = 0
    cum = 0
    max_dd = 0
    curve = []
    for t in closed:
        cum += t.get("pnl_amount", 0)
        if cum > peak:
            peak = cum
        dd = (cum - peak) / peak * 100 if peak > 0 else 0
        max_dd = min(max_dd, dd)
        curve.append({"date": t.get("exit_date") or t.get("entry_date"), "drawdown": round(dd, 2)})

    current_dd = curve[-1]["drawdown"] if curve else 0
    return {
        "max_drawdown_pct": round(max_dd, 2),
        "current_drawdown_pct": round(current_dd, 2),
        "curve": curve
    }


def get_monthly_pnl(trades: list) -> list:
    """Group closed trades P&L by YYYY-MM."""
    from collections import defaultdict
    closed = [t for t in trades if t["status"] in ("Closed","StoppedOut")]
    monthly: dict = defaultdict(float)
    for t in closed:
        d = (t.get("exit_date") or t.get("entry_date") or "")[:7]
        if d:
            monthly[d] += t.get("pnl_amount", 0)
    return [{"month": k, "pnl": round(v, 2)} for k, v in sorted(monthly.items())]


def get_time_of_day_stats(trades: list) -> list:
    """Group closed trades by entry hour (0-23)."""
    from collections import defaultdict
    closed = [t for t in trades if t["status"] in ("Closed","StoppedOut")]
    hourly: dict = defaultdict(lambda: {"trades": 0, "wins": 0, "total_pnl": 0.0})
    for t in closed:
        et = t.get("entry_time","") or ""
        try:
            hour = int(et[:2]) if len(et) >= 2 else -1
        except:
            hour = -1
        if hour >= 0:
            hourly[hour]["trades"] += 1
            if t.get("pnl_pct", 0) > 0:
                hourly[hour]["wins"] += 1
            hourly[hour]["total_pnl"] += t.get("pnl_amount", 0)
    result = []
    for h in sorted(hourly.keys()):
        d = hourly[h]
        result.append({
            "hour": h,
            "label": f"{h:02d}:00",
            "trades": d["trades"],
            "win_rate": round(d["wins"]/d["trades"]*100, 1) if d["trades"] else 0,
            "total_pnl": round(d["total_pnl"], 2),
        })
    return result


def get_ai_insights(analytics: dict, trades: list) -> list:
    """Rule-based AI coaching insights from trade patterns."""
    insights = []
    if analytics.get("total", 0) < 5:
        return [{"type": "info", "icon": "💡", "text": "Log at least 5 trades to unlock AI coaching insights."}]

    by_emotion = analytics.get("by_emotion", {})
    by_setup = analytics.get("by_setup", {})
    mistakes = analytics.get("mistakes", {})
    win_rate = analytics.get("win_rate", 0)
    avg_r = analytics.get("avg_r", 0)
    profit_factor = analytics.get("profit_factor", 0)
    max_loss_streak = analytics.get("max_loss_streak", 0)
    closed = [t for t in trades if t["status"] in ("Closed","StoppedOut")]

    # Emotion → performance
    for emo, d in by_emotion.items():
        if emo in ("FOMO","Revenge") and d.get("trades", 0) >= 2 and d.get("win_rate", 50) < 40:
            insights.append({"type": "danger", "icon": "🚨",
                "text": f"Your {emo} trades win only {d['win_rate']}% of the time. Avoid trading when feeling {emo.lower()}."})
        if emo == "Patient" and d.get("trades", 0) >= 2 and d.get("win_rate", 0) > 60:
            insights.append({"type": "positive", "icon": "✅",
                "text": f"You win {d['win_rate']}% when feeling Patient. Prioritise patience before entry."})

    # Best/worst setup
    if by_setup:
        best = max(by_setup.items(), key=lambda x: x[1].get("avg_r", 0))
        worst = min(by_setup.items(), key=lambda x: x[1].get("avg_r", 99))
        if best[1].get("trades", 0) >= 3:
            insights.append({"type": "positive", "icon": "⚡",
                "text": f"Your best setup is {best[0]} with {best[1]['avg_r']}R avg. Focus here."})
        if worst[1].get("trades", 0) >= 3 and worst[1].get("avg_r", 0) < 0:
            insights.append({"type": "warning", "icon": "⚠",
                "text": f"{worst[0]} setups are losing you {abs(worst[1]['avg_r'])}R on average. Consider dropping this setup."})

    # Overtrading detection
    from collections import Counter
    date_counts = Counter(t.get("entry_date","")[:10] for t in closed)
    overtrade_days = {d: c for d, c in date_counts.items() if c > 3}
    if len(overtrade_days) >= 2:
        insights.append({"type": "warning", "icon": "📊",
            "text": f"You overtrade on {len(overtrade_days)} days (4+ trades). Overtrading often reduces profitability."})

    # Mistake patterns
    if mistakes.get("MovedStop", 0) >= 3:
        insights.append({"type": "danger", "icon": "🛑",
            "text": f"You've moved your stop loss {mistakes['MovedStop']} times. This is your biggest risk leak."})
    if mistakes.get("ExitedEarly", 0) >= 3:
        insights.append({"type": "warning", "icon": "✂",
            "text": f"Early exits detected {mistakes['ExitedEarly']} times. You may be leaving significant R on the table."})
    if mistakes.get("Oversized", 0) >= 2:
        insights.append({"type": "danger", "icon": "📦",
            "text": f"Oversizing detected {mistakes['Oversized']} times — this is a direct drawdown risk."})

    # Loss streak warning
    if max_loss_streak >= 4:
        insights.append({"type": "danger", "icon": "🔴",
            "text": f"Max losing streak: {max_loss_streak}. After 3 losses, consider reducing size or taking a break."})

    # Profit factor
    if profit_factor < 1.0 and analytics.get("closed", 0) >= 10:
        insights.append({"type": "danger", "icon": "📉",
            "text": f"Profit factor is {profit_factor} (below 1.0). Losses exceed gains. Review your exit strategy."})
    elif profit_factor >= 2.0:
        insights.append({"type": "positive", "icon": "🏆",
            "text": f"Excellent profit factor of {profit_factor}. Your winners are significantly larger than losers."})

    # Sleep impact on performance
    sleep_trades = [(t.get("psych_sleep", 0), t.get("pnl_pct", 0)) for t in closed if t.get("psych_sleep", 0) > 0]
    if len(sleep_trades) >= 5:
        poor_sleep = [(s, p) for s, p in sleep_trades if s <= 4]
        good_sleep = [(s, p) for s, p in sleep_trades if s >= 7]
        if poor_sleep and good_sleep:
            poor_wr = sum(1 for _, p in poor_sleep if p > 0) / len(poor_sleep) * 100
            good_wr = sum(1 for _, p in good_sleep if p > 0) / len(good_sleep) * 100
            if good_wr - poor_wr > 15:
                insights.append({"type": "warning", "icon": "😴",
                    "text": f"Poor sleep reduces your win rate by {good_wr-poor_wr:.0f}%. Prioritise sleep before trading days."})

    if not insights:
        insights.append({"type": "info", "icon": "💡",
            "text": "No major issues detected. Keep logging trades consistently for deeper pattern detection."})

    return insights[:8]  # cap at 8 insights


# ──────────────────────────────────────────────────────────────────────────────
# LLM-POWERED AI COACH (Groq)
# ──────────────────────────────────────────────────────────────────────────────

# In-memory cache — keyed on a digest of analytics snapshot.
# Invalidates whenever user logs new trades because cache key embeds trade count
# and aggregate stats.
_llm_coach_cache: dict = {}
_LLM_COACH_TTL = 7200  # 2 hours

# How many trades are required before we bother calling the LLM at all —
# below this, rule-based insights are the right answer anyway.
_LLM_MIN_TRADES = 10


_LLM_SYSTEM_PROMPT = """You are a seasoned trading coach reviewing a systematic swing trader's journal.
You understand Indian equity markets (NSE, NIFTY 500) and the Minervini/O'Neil/Qullamaggie/Weinstein methodology framework.

Your style:
- Blunt, evidence-based, no sycophancy
- Cite specific numbers from the data provided
- Name what's leaking edge and what's working
- No generic advice ("trust the process", "stay disciplined") — those are useless
- Actionable: every point should suggest a concrete change the trader can make

Output EXACTLY a JSON array (no markdown, no commentary, no code fences) of 3-4 insight objects.
Each object has three keys:
- "type": one of "danger" | "warning" | "positive" | "info"
- "icon": a single emoji matching the severity (🚨 🚩 ✅ 🏆 ⚠ 💡 🎯 🔥 🧠 etc.)
- "text": 1-2 sentence coaching point referencing a specific number from the data

Example of a GOOD insight:
{"type":"danger","icon":"🚨","text":"Your FOMO trades win 28% (12 of 43) vs 62% (89 of 144) for planned entries — that's a 34 percentage point gap. Add a hard rule: if a stock has already moved >2% from your ideal entry, skip it."}

Example of a BAD insight (too generic):
{"type":"info","icon":"💡","text":"Remember to stay disciplined and trust your process."}

If data is thin or mixed, say so honestly rather than inventing patterns."""


def _build_llm_context(analytics: dict, trades: list) -> str:
    """Compress the trader's data into a token-efficient structured snapshot."""
    from collections import Counter

    closed = [t for t in trades if t.get("status") in ("Closed", "StoppedOut")]

    # Top / bottom setups by expectancy
    setup_lines = []
    by_setup = analytics.get("by_setup", {})
    ranked = sorted(
        by_setup.items(),
        key=lambda x: x[1].get("avg_r", 0),
        reverse=True,
    )
    for name, s in ranked[:3]:
        if s.get("trades", 0) >= 2:
            setup_lines.append(
                f"  {name}: {s.get('trades',0)} trades, "
                f"{s.get('win_rate',0)}% WR, {s.get('avg_r',0)}R avg"
            )
    for name, s in ranked[-2:]:
        if s.get("trades", 0) >= 2 and name not in [r[0] for r in ranked[:3]]:
            setup_lines.append(
                f"  {name}: {s.get('trades',0)} trades, "
                f"{s.get('win_rate',0)}% WR, {s.get('avg_r',0)}R avg (underperformer)"
            )
    setups_block = "\n".join(setup_lines) if setup_lines else "  (insufficient setup data)"

    # Emotion breakdown
    emotion_lines = []
    by_emotion = analytics.get("by_emotion", {})
    for emo, s in by_emotion.items():
        if s.get("trades", 0) >= 2:
            emotion_lines.append(
                f"  {emo}: {s.get('trades',0)} trades, {s.get('win_rate',0)}% WR"
            )
    emotion_block = "\n".join(emotion_lines) if emotion_lines else "  (no emotion data)"

    # Mistakes
    mistakes = analytics.get("mistakes", {})
    mistake_lines = [
        f"  {m}: {c} occurrences" for m, c in sorted(mistakes.items(), key=lambda x: -x[1])[:5]
    ]
    mistake_block = "\n".join(mistake_lines) if mistake_lines else "  (none logged)"

    # Psychology correlation — sleep & confidence
    psych_lines = []
    sleep_trades = [
        (t.get("psych_sleep", 0), t.get("pnl_pct", 0))
        for t in closed if t.get("psych_sleep", 0) > 0
    ]
    if len(sleep_trades) >= 5:
        low = [p for s, p in sleep_trades if s <= 4]
        high = [p for s, p in sleep_trades if s >= 7]
        if low and high:
            low_wr = sum(1 for p in low if p > 0) / len(low) * 100
            high_wr = sum(1 for p in high if p > 0) / len(high) * 100
            psych_lines.append(
                f"  Poor sleep (≤4): {len(low)} trades, {low_wr:.0f}% WR"
            )
            psych_lines.append(
                f"  Good sleep (≥7): {len(high)} trades, {high_wr:.0f}% WR"
            )

    # Recent overtrading
    date_counts = Counter(t.get("entry_date", "")[:10] for t in closed)
    overtrade_days = sum(1 for c in date_counts.values() if c > 3)

    return f"""
TRADING JOURNAL SNAPSHOT
========================
Total trades: {analytics.get('total', 0)} ({analytics.get('closed', 0)} closed)
Win rate: {analytics.get('win_rate', 0)}%
Avg R per trade: {analytics.get('avg_r', 0)}
Profit factor: {analytics.get('profit_factor', 0)}
Max losing streak: {analytics.get('max_loss_streak', 0)}
Overtrading days (4+ trades/day): {overtrade_days}
Total P&L: ₹{analytics.get('total_pnl', 0):,.0f}

SETUP PERFORMANCE (by avg R)
{setups_block}

EMOTION vs WIN RATE
{emotion_block}

MISTAKES LOGGED
{mistake_block}
{('SLEEP CORRELATION' + chr(10) + chr(10).join(psych_lines)) if psych_lines else ''}
""".strip()


def _parse_llm_response(raw: str) -> list:
    """Extract JSON array from LLM output, tolerating stray markdown/commentary."""
    import json, re

    # Strip markdown code fences if present
    text = raw.strip()
    text = re.sub(r"^```(?:json)?\s*", "", text)
    text = re.sub(r"\s*```$", "", text)

    # Find the first JSON array in the response
    match = re.search(r"\[\s*\{.*?\}\s*\]", text, flags=re.DOTALL)
    if not match:
        raise ValueError("No JSON array found in LLM response")

    parsed = json.loads(match.group(0))
    if not isinstance(parsed, list):
        raise ValueError("LLM output is not a list")

    # Validate and normalise shape
    out = []
    for item in parsed[:4]:
        if not isinstance(item, dict):
            continue
        t = str(item.get("type", "info")).lower()
        if t not in ("danger", "warning", "positive", "info"):
            t = "info"
        out.append({
            "type": t,
            "icon": str(item.get("icon", "💡"))[:4] or "💡",
            "text": str(item.get("text", "")).strip()[:400],
            "source": "llm",
        })
    if not out:
        raise ValueError("No valid insights extracted from LLM output")
    return out


def get_ai_insights_llm(analytics: dict, trades: list) -> list:
    """
    Combined coaching insights: rule-based baseline + LLM-generated coaching.

    Graceful degradation:
    - No Groq API key → rule-based only
    - Fewer than _LLM_MIN_TRADES closed trades → rule-based only
    - LLM error or parse failure → rule-based only
    - Never raises; the user always sees SOMETHING useful.
    """
    # Always start with rule-based (fast, deterministic, always works)
    base = get_ai_insights(analytics, trades)

    # Only augment with LLM if trader has enough data
    if analytics.get("closed", 0) < _LLM_MIN_TRADES:
        return base

    # Try to get LLM coaching on top
    try:
        from ai_insights import _get_api_key, _call_groq_with_fallback
    except Exception as e:
        logger.debug(f"AI Coach: ai_insights module unavailable ({e})")
        return base

    api_key = _get_api_key()
    if not api_key:
        # No key configured — rule-based is the best we can do
        return base

    # Cache key: bucket stats so minor changes don't bust the cache,
    # but new trades always do. 2h TTL safeguards against staleness.
    import time
    cache_key = (
        analytics.get("closed", 0),
        round(analytics.get("win_rate", 0) or 0),
        round((analytics.get("avg_r", 0) or 0) * 10),
        round((analytics.get("profit_factor", 0) or 0) * 10),
    )
    now = time.time()
    cached = _llm_coach_cache.get(cache_key)
    if cached and (now - cached["ts"] < _LLM_COACH_TTL):
        return base + cached["insights"]

    try:
        ctx = _build_llm_context(analytics, trades)
        raw = _call_groq_with_fallback(
            prompt=ctx,
            system=_LLM_SYSTEM_PROMPT,
            api_key=api_key,
            max_tokens=800,
        )
        llm_insights = _parse_llm_response(raw)
        _llm_coach_cache[cache_key] = {"ts": now, "insights": llm_insights}
        logger.info(f"✅ AI Coach LLM: generated {len(llm_insights)} insights "
                    f"(closed={analytics.get('closed', 0)})")
        return base + llm_insights
    except Exception as e:
        # Log and fall back silently — never break the user's view
        logger.warning(f"AI Coach LLM call failed: {e}")
        return base


def get_day_of_week_stats(trades: list) -> list:
    """Group closed trades by day of week."""
    from collections import defaultdict
    closed = [t for t in trades if t["status"] in ("Closed","StoppedOut")]
    days_map = {0:"Mon",1:"Tue",2:"Wed",3:"Thu",4:"Fri",5:"Sat",6:"Sun"}
    dow: dict = defaultdict(lambda: {"trades":0,"wins":0,"total_pnl":0.0})
    for t in closed:
        ed = t.get("entry_date","") or ""
        try:
            from datetime import datetime
            d = datetime.strptime(ed[:10], "%Y-%m-%d").weekday()
            dow[d]["trades"] += 1
            if t.get("pnl_pct",0) > 0: dow[d]["wins"] += 1
            dow[d]["total_pnl"] += t.get("pnl_amount",0)
        except: pass
    result = []
    for d in range(7):
        if d in dow:
            n = dow[d]["trades"]
            result.append({
                "day": days_map[d], "trades": n,
                "win_rate": round(dow[d]["wins"]/n*100,1) if n else 0,
                "total_pnl": round(dow[d]["total_pnl"],2),
            })
    return result


def check_risk_rules(trades: list, settings: dict) -> dict:
    """
    Check if user has breached any risk rules today/this week.
    Returns alerts list and lock_trading flag.
    """
    from datetime import datetime, timedelta, timezone
    alerts = []
    lock_trading = False

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    week_start = (datetime.now(timezone.utc) - timedelta(days=datetime.now(timezone.utc).weekday())).strftime("%Y-%m-%d")

    closed = [t for t in trades if t["status"] in ("Closed","StoppedOut")]
    starting_capital = settings.get("starting_capital", 1000000)
    max_daily_loss_pct = settings.get("max_daily_loss_pct", 2.0)
    max_weekly_dd_pct = settings.get("max_weekly_drawdown_pct", 5.0)
    max_trades_day = settings.get("max_trades_per_day", 5)
    max_consec = settings.get("max_consecutive_losses", 3)
    max_risk_pct = settings.get("max_risk_per_trade", 1.0)

    # Daily loss check
    today_closed = [t for t in closed if (t.get("exit_date") or "")[:10] == today]
    today_pnl = sum(t.get("pnl_amount",0) for t in today_closed)
    today_pnl_pct = (today_pnl / starting_capital * 100) if starting_capital > 0 else 0
    if today_pnl_pct <= -max_daily_loss_pct:
        alerts.append({
            "type":"danger","icon":"🚨",
            "rule":"Max Daily Loss",
            "msg":f"Daily loss {today_pnl_pct:.2f}% exceeds limit of -{max_daily_loss_pct}%. Consider stopping for today.",
        })
        lock_trading = True

    # Max trades per day
    today_all = [t for t in trades if (t.get("entry_date") or "")[:10] == today]
    if len(today_all) >= max_trades_day:
        alerts.append({
            "type":"warning","icon":"⚠",
            "rule":"Max Trades/Day",
            "msg":f"{len(today_all)} trades today — at or above your limit of {max_trades_day}.",
        })

    # Weekly drawdown
    week_closed = [t for t in closed if (t.get("exit_date") or "")[:10] >= week_start]
    week_pnl = sum(t.get("pnl_amount",0) for t in week_closed)
    week_pnl_pct = (week_pnl / starting_capital * 100) if starting_capital > 0 else 0
    if week_pnl_pct <= -max_weekly_dd_pct:
        alerts.append({
            "type":"danger","icon":"🔴",
            "rule":"Weekly Drawdown",
            "msg":f"Weekly drawdown {week_pnl_pct:.2f}% exceeds -{max_weekly_dd_pct}% limit.",
        })
        lock_trading = True

    # Consecutive losses
    streak = 0
    for t in sorted(closed, key=lambda x: x.get("exit_date") or "", reverse=True)[:10]:
        if t.get("pnl_pct",0) < 0: streak += 1
        else: break
    if streak >= max_consec:
        alerts.append({
            "type":"warning","icon":"🛑",
            "rule":"Consecutive Losses",
            "msg":f"{streak} consecutive losses. Rule limit is {max_consec}. Take a break before next trade.",
        })

    return {"alerts": alerts, "lock_trading": lock_trading,
            "today_pnl": round(today_pnl,2), "today_pnl_pct": round(today_pnl_pct,2),
            "today_trades": len(today_all), "week_pnl_pct": round(week_pnl_pct,2),
            "consecutive_losses": streak}


def get_gamification(trades: list) -> dict:
    """Compute gamification badges and streaks."""
    closed = sorted([t for t in trades if t["status"] in ("Closed","StoppedOut")],
                    key=lambda t: t.get("exit_date") or "")

    badges = []

    # Discipline streak — consecutive trades with discipline >= 7
    disc_streak = 0
    for t in reversed(closed):
        if t.get("discipline_score",0) >= 7: disc_streak += 1
        else: break
    if disc_streak >= 5:
        badges.append({"id":"discipline_5","icon":"⭐","label":f"{disc_streak}-Trade Discipline Streak","color":"#f59e0b"})
    if disc_streak >= 10:
        badges.append({"id":"discipline_10","icon":"🏆","label":"10 Disciplined Trades","color":"#f59e0b"})

    # Win streak
    win_streak = 0
    for t in reversed(closed):
        if t.get("pnl_pct",0) > 0: win_streak += 1
        else: break
    if win_streak >= 3:
        badges.append({"id":"win_streak_3","icon":"🔥","label":f"{win_streak}-Trade Win Streak","color":"#22c55e"})
    if win_streak >= 7:
        badges.append({"id":"win_streak_7","icon":"⚡","label":"7 Wins in a Row!","color":"#22c55e"})

    # Green week — net positive P&L this week
    from datetime import datetime, timedelta, timezone
    week_start = (datetime.now(timezone.utc) - timedelta(days=datetime.now(timezone.utc).weekday())).strftime("%Y-%m-%d")
    week_closed = [t for t in closed if (t.get("exit_date") or "")[:10] >= week_start]
    if week_closed and sum(t.get("pnl_amount",0) for t in week_closed) > 0:
        badges.append({"id":"green_week","icon":"🟢","label":"Green Week","color":"#22c55e"})

    # Tilt-free week (no revenge/FOMO trades this week)
    tilt_free = all(
        t.get("pre_emotion","") not in ("Revenge","FOMO") and t.get("psych_revenge",0) < 7
        for t in week_closed
    )
    if week_closed and tilt_free:
        badges.append({"id":"tilt_free","icon":"🧘","label":"Tilt-Free Week","color":"#06b6d4"})

    # Plan follower — 5 consecutive trades with followed_plan=1
    plan_streak = 0
    for t in reversed(closed):
        if t.get("followed_plan",1) == 1: plan_streak += 1
        else: break
    if plan_streak >= 5:
        badges.append({"id":"plan_5","icon":"✅","label":f"Followed Plan: {plan_streak} in a Row","color":"#a855f7"})

    # Total trades milestone
    if len(trades) >= 50:
        badges.append({"id":"trades_50","icon":"📊","label":"50 Trades Logged","color":"#94a3b8"})
    if len(trades) >= 100:
        badges.append({"id":"trades_100","icon":"💯","label":"100 Trades!","color":"#f59e0b"})

    return {
        "badges": badges,
        "win_streak": win_streak,
        "disc_streak": disc_streak,
        "plan_streak": plan_streak,
    }


def parse_csv_import(content: str, broker: str = "generic") -> list:
    """
    Parse broker CSV exports into standardised trade dicts.
    Supports: zerodha, ibkr, mt5, generic
    Returns list of trade dicts ready for add_trade().
    """
    import csv, io
    reader = csv.DictReader(io.StringIO(content.strip()))
    rows = list(reader)
    if not rows:
        return []

    trades = []
    headers = [h.strip().lower() for h in (rows[0].keys() if rows else [])]

    for row in rows:
        r = {k.strip().lower(): v.strip() for k, v in row.items()}
        try:
            # Zerodha Console format
            if broker == "zerodha" or "symbol" in headers and "buy value" in headers:
                ticker  = r.get("symbol","").replace(".NS","").replace("-EQ","").upper()
                qty     = abs(float(r.get("quantity",r.get("qty","0")) or 0))
                entry   = float(r.get("buy price",r.get("avg. buy price","0")) or 0)
                exit_p  = float(r.get("sell price",r.get("avg. sell price","0")) or 0)
                pnl     = float(r.get("p&l",r.get("realised profit","0")) or 0)
                date    = r.get("trade date",r.get("date",""))[:10]
            # IBKR Activity Statement
            elif broker == "ibkr" or "symbol" in headers and "t. price" in headers:
                ticker  = r.get("symbol","").upper()
                qty     = abs(float(r.get("quantity","0") or 0))
                entry   = float(r.get("t. price","0") or 0)
                exit_p  = None
                pnl     = float(r.get("realized p/l","0") or 0)
                date    = (r.get("date/time","") or "")[:10]
            # MT5 History
            elif broker == "mt5" or "type" in headers and "profit" in headers:
                ticker  = r.get("symbol","").upper()
                qty     = abs(float(r.get("volume","0") or 0))
                entry   = float(r.get("price","0") or 0)
                exit_p  = float(r.get("s / l","0") or 0) or None
                pnl     = float(r.get("profit","0") or 0)
                date    = (r.get("time","") or "")[:10]
            # Generic / M360 Demo — reads all columns
            else:
                def _f(val, fallback=0.0):
                    try: return float(val) if val and val.strip() else fallback
                    except: return fallback
                def _i(val, fallback=0):
                    try: return int(float(val)) if val and val.strip() else fallback
                    except: return fallback

                ticker  = (r.get("ticker","") or r.get("symbol","") or r.get("instrument","")).upper()
                qty     = abs(_f(r.get("quantity") or r.get("qty","0")))
                entry   = _f(r.get("entry") or r.get("entry price") or r.get("buy","0"))
                exit_p  = _f(r.get("exit") or r.get("exit price") or r.get("sell","0")) or None
                pnl     = _f(r.get("pnl") or r.get("profit") or r.get("p&l","0"))
                date    = (r.get("date","") or r.get("entry_date","") or r.get("trade date",""))[:10]
                direction = r.get("direction","Long") or "Long"
                stop_loss = _f(r.get("stop_loss") or r.get("stop") or r.get("stoploss"), None)
                target    = _f(r.get("target") or r.get("tp") or r.get("take profit"), None)
                setup_type= r.get("setup_type","") or r.get("setup","")
                timeframe = r.get("timeframe","") or r.get("tf","")
                broker_nm = r.get("broker","") or broker.title()
                risk_amt  = _f(r.get("risk_amount") or r.get("risk","0"))
                fees      = _f(r.get("fees") or r.get("commission","0"))
                r_mult    = _f(r.get("r_multiple") or r.get("r_mult") or r.get("r","0"))
                hold_days = _i(r.get("holding_days") or r.get("hold_days","0"))
                p_conf    = _i(r.get("psych_confidence") or r.get("confidence","0"))
                p_focus   = _i(r.get("psych_focus") or r.get("focus","0"))
                p_stress  = _i(r.get("psych_stress") or r.get("stress","0"))
                p_pat     = _i(r.get("psych_patience") or r.get("patience","0"))
                p_fomo    = _i(r.get("psych_fomo") or r.get("fomo","0"))
                p_rev     = _i(r.get("psych_revenge") or r.get("revenge","0"))
                p_sleep   = _i(r.get("psych_sleep") or r.get("sleep","0"))
                p_energy  = _i(r.get("psych_energy") or r.get("energy","0"))
                grade     = r.get("trade_grade","") or r.get("grade","")
                fol_plan  = _i(r.get("followed_plan","1"))
                w_repeat  = _i(r.get("would_repeat","1"))
                notes_val = r.get("notes","") or f"Imported from {broker.upper()} CSV"
                mae       = _f(r.get("mae","0"))
                mfe       = _f(r.get("mfe","0"))

            if not ticker or not entry: continue
            direction = direction if direction in ("Long","Short") else "Long"
            status    = "Closed" if exit_p else "Open"
            trades.append({
                "ticker":           ticker,
                "direction":        direction,
                "entry_date":       date,
                "entry_price":      entry,
                "exit_price":       exit_p,
                "quantity":         qty,
                "status":           status,
                "stop_loss":        stop_loss,
                "target":           target,
                "setup_type":       setup_type,
                "timeframe":        timeframe,
                "broker":           broker_nm,
                "risk_amount":      risk_amt,
                "fees":             fees,
                "r_multiple":       r_mult,
                "holding_days":     hold_days,
                "psych_confidence": p_conf,
                "psych_focus":      p_focus,
                "psych_stress":     p_stress,
                "psych_patience":   p_pat,
                "psych_fomo":       p_fomo,
                "psych_revenge":    p_rev,
                "psych_sleep":      p_sleep,
                "psych_energy":     p_energy,
                "trade_grade":      grade,
                "followed_plan":    fol_plan,
                "would_repeat":     w_repeat,
                "notes":            notes_val,
                "mae":              mae,
                "mfe":              mfe,
            })
        except Exception:
            continue

    return trades


# ── Account Management ────────────────────────────────────────────────────────

ACCOUNT_COLORS = ['#06b6d4','#a855f7','#22c55e','#f59e0b','#ef4444','#3b82f6','#ec4899','#14b8a6']

def list_accounts() -> list:
    """Return all trading accounts ordered by id."""
    _ensure_journal_tables()
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM journal_accounts ORDER BY id").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def create_account(name: str, broker: str = "", currency: str = "INR",
                   starting_capital: float = 1000000, color: str = "") -> dict:
    _ensure_journal_tables()
    if not name or not name.strip():
        return {"error": "Account name is required"}
    conn = sqlite3.connect(DB_PATH, timeout=10)
    # Auto-pick color if not provided
    if not color:
        count = conn.execute("SELECT COUNT(*) FROM journal_accounts").fetchone()[0]
        color = ACCOUNT_COLORS[count % len(ACCOUNT_COLORS)]
    from datetime import datetime, timezone
    cur = conn.execute(
        "INSERT INTO journal_accounts (name, broker, currency, starting_capital, color, created_at) VALUES (?,?,?,?,?,?)",
        (name.strip(), broker.strip(), currency, starting_capital, color,
         datetime.now(timezone.utc).isoformat())
    )
    acct_id = cur.lastrowid
    conn.commit()
    conn.close()
    return {"status": "ok", "id": acct_id, "name": name.strip(), "color": color}


def update_account(acct_id: int, updates: dict) -> dict:
    _ensure_journal_tables()
    conn = sqlite3.connect(DB_PATH, timeout=10)
    acct = conn.execute("SELECT id FROM journal_accounts WHERE id=?", (acct_id,)).fetchone()
    if not acct:
        conn.close()
        return {"error": "Account not found"}
    allowed = {"name", "broker", "currency", "starting_capital", "color", "notes"}
    parts, params = [], []
    for k, v in updates.items():
        if k in allowed:
            parts.append(f"{k}=?")
            params.append(v)
    if parts:
        params.append(acct_id)
        conn.execute(f"UPDATE journal_accounts SET {', '.join(parts)} WHERE id=?", params)
        conn.commit()
    conn.close()
    return {"status": "ok"}


def delete_account(acct_id: int) -> dict:
    _ensure_journal_tables()
    conn = sqlite3.connect(DB_PATH, timeout=10)
    # Protect Account 1
    if acct_id == 1:
        conn.close()
        return {"error": "Cannot delete the default account"}
    trade_count = conn.execute(
        "SELECT COUNT(*) FROM journal_trades WHERE account_id=?", (acct_id,)
    ).fetchone()[0]
    if trade_count > 0:
        conn.close()
        return {"error": f"Account has {trade_count} trade(s). Move or delete them first."}
    conn.execute("DELETE FROM journal_accounts WHERE id=?", (acct_id,))
    conn.commit()
    conn.close()
    return {"status": "ok"}


def get_account_summary() -> list:
    """Return each account with its aggregate stats."""
    _ensure_journal_tables()
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    accounts = conn.execute("SELECT * FROM journal_accounts ORDER BY id").fetchall()
    result = []
    for a in accounts:
        aid = a["id"]
        trades = conn.execute(
            "SELECT status, pnl_amount, pnl_pct FROM journal_trades WHERE account_id=?", (aid,)
        ).fetchall()
        total      = len(trades)
        closed     = [t for t in trades if t["status"] in ("Closed","StoppedOut")]
        open_count = sum(1 for t in trades if t["status"] == "Open")
        winners    = [t for t in closed if t["pnl_pct"] > 0]
        win_rate   = round(len(winners)/len(closed)*100, 1) if closed else 0
        total_pnl  = round(sum(t["pnl_amount"] for t in closed), 2)
        row = dict(a)
        row.update({"total_trades": total, "open_trades": open_count,
                    "closed_trades": len(closed), "win_rate": win_rate,
                    "total_pnl": total_pnl})
        result.append(row)
    conn.close()
    return result


def get_calendar_data(month: str, account_id: int = None) -> list:
    """
    Return per-day trading summary for a given month (YYYY-MM).
    Each day: date, net_pnl, trade_count, wins, losses, tickers[], setup_types[]
    """
    _ensure_journal_tables()
    try:
        year, mon = map(int, month.split("-"))
    except Exception:
        return []

    import calendar
    _, last_day = calendar.monthrange(year, mon)
    start = f"{year:04d}-{mon:02d}-01"
    end   = f"{year:04d}-{mon:02d}-{last_day:02d}"

    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row

    acct_clause = f"AND account_id = {account_id}" if account_id else ""
    rows = conn.execute(f"""
        SELECT
            entry_date,
            pnl_amount,
            status,
            ticker,
            setup_type,
            direction,
            r_multiple,
            entry_time,
            exit_price
        FROM journal_trades
        WHERE entry_date BETWEEN ? AND ?
          AND status IN ('Closed','StoppedOut')
          {acct_clause}
        ORDER BY entry_date, id
    """, (start, end)).fetchall()
    conn.close()

    from collections import defaultdict
    days = defaultdict(lambda: {
        "date": "", "net_pnl": 0.0, "trade_count": 0,
        "wins": 0, "losses": 0, "tickers": [], "setups": [], "trades": []
    })

    for r in rows:
        d = r["entry_date"][:10]
        day = days[d]
        day["date"] = d
        day["net_pnl"]      = round(day["net_pnl"] + (r["pnl_amount"] or 0), 2)
        day["trade_count"] += 1
        if (r["pnl_amount"] or 0) > 0:
            day["wins"] += 1
        else:
            day["losses"] += 1
        if r["ticker"] and r["ticker"] not in day["tickers"]:
            day["tickers"].append(r["ticker"])
        st = (r["setup_type"] or "").strip()
        if st and st not in day["setups"]:
            day["setups"].append(st)
        day["trades"].append({
            "time":      (r["entry_time"] or "")[:5],
            "ticker":    r["ticker"],
            "direction": r["direction"],
            "r_multiple": round(r["r_multiple"] or 0, 2),
            "pnl":       round(r["pnl_amount"] or 0, 2),
            "setup":     st,
        })

    return sorted(days.values(), key=lambda x: x["date"])


def get_strategy_leaderboard(account_id: int = None) -> list:
    """
    Rank strategies by expectancy.
    Returns list of {strategy, trades, wins, losses, win_rate, avg_r,
                     expectancy, profit_factor, net_pnl}
    """
    _ensure_journal_tables()
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row

    acct_clause = f"AND account_id = {account_id}" if account_id else ""
    rows = conn.execute(f"""
        SELECT
            COALESCE(NULLIF(TRIM(setup_type),''), 'Untagged') AS strategy,
            pnl_amount,
            r_multiple,
            status
        FROM journal_trades
        WHERE status IN ('Closed','StoppedOut')
          {acct_clause}
    """).fetchall()
    conn.close()

    from collections import defaultdict
    buckets = defaultdict(lambda: {"trades": 0, "wins": 0, "losses": 0,
                                   "r_vals": [], "pnl_vals": []})
    for r in rows:
        s = r["strategy"]
        b = buckets[s]
        b["trades"] += 1
        pnl = r["pnl_amount"] or 0
        rm  = r["r_multiple"] or 0
        b["pnl_vals"].append(pnl)
        b["r_vals"].append(rm)
        if pnl > 0:
            b["wins"] += 1
        else:
            b["losses"] += 1

    result = []
    for strategy, b in buckets.items():
        n       = b["trades"]
        wins    = b["wins"]
        losses  = b["losses"]
        win_r   = round(wins / n * 100, 1) if n else 0
        avg_r   = round(sum(b["r_vals"]) / n, 2) if n else 0
        net_pnl = round(sum(b["pnl_vals"]), 2)
        winners_r = [r for r in b["r_vals"] if r > 0]
        losers_r  = [abs(r) for r in b["r_vals"] if r <= 0]
        avg_win_r = sum(winners_r) / len(winners_r) if winners_r else 0
        avg_los_r = sum(losers_r)  / len(losers_r)  if losers_r  else 1
        pf        = round(sum(p for p in b["pnl_vals"] if p > 0) /
                          max(abs(sum(p for p in b["pnl_vals"] if p < 0)), 0.01), 2)
        expectancy = round((win_r/100) * avg_win_r - (losses/n) * avg_los_r, 3) if n else 0
        result.append({
            "strategy":      strategy,
            "trades":        n,
            "wins":          wins,
            "losses":        losses,
            "win_rate":      win_r,
            "avg_r":         avg_r,
            "expectancy":    expectancy,
            "profit_factor": pf,
            "net_pnl":       net_pnl,
        })

    result.sort(key=lambda x: x["expectancy"], reverse=True)
    return result


def get_calendar_months(account_id: int = None) -> list:
    """Return list of YYYY-MM strings that have at least one closed trade."""
    _ensure_journal_tables()
    conn = sqlite3.connect(DB_PATH, timeout=10)
    acct_clause = f"AND account_id = {account_id}" if account_id else ""
    rows = conn.execute(f"""
        SELECT DISTINCT substr(entry_date, 1, 7) AS month
        FROM journal_trades
        WHERE status IN ('Closed','StoppedOut')
          AND entry_date IS NOT NULL
          AND entry_date != ''
          {acct_clause}
        ORDER BY month DESC
    """).fetchall()
    conn.close()
    return [r[0] for r in rows if r[0]]
