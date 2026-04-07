"""
control_center.py — Admin Control Center Backend
- RBAC (Super Admin, Analyst, Support, Finance)
- Audit Logs
- System Monitoring (pipeline status, DB stats)
- Configuration Panel (editable thresholds/feature flags)
- Revenue Analytics (MRR/ARR/churn/ARPU)
- User Management (enhanced)
"""

import sqlite3, logging, json, os, time
from pathlib import Path
from datetime import datetime, timedelta, timezone
from collections import defaultdict

logger = logging.getLogger(__name__)
DB_PATH = str(Path(__file__).parent / "breadth_data.db")

# ══════════════════════════════════════════════════════════════════════════════
# RBAC — Roles & Permissions
# ══════════════════════════════════════════════════════════════════════════════

ROLES = {
    "super_admin": {
        "name": "Super Admin",
        "permissions": ["view", "edit", "delete", "config", "users", "finance", "system", "audit"],
        "description": "Full access to everything",
    },
    "analyst": {
        "name": "Analyst",
        "permissions": ["view", "system", "audit"],
        "description": "View analytics, system status, audit logs",
    },
    "support": {
        "name": "Support",
        "permissions": ["view", "edit", "users"],
        "description": "Manage users, view data, no config/finance",
    },
    "finance": {
        "name": "Finance",
        "permissions": ["view", "finance"],
        "description": "View revenue analytics, user subscriptions",
    },
}


def ensure_cc_tables():
    """Create all control center tables."""
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.executescript("""
        -- Admin roles (extends users table)
        CREATE TABLE IF NOT EXISTS admin_roles (
            user_id INTEGER PRIMARY KEY,
            role TEXT NOT NULL DEFAULT 'analyst',
            granted_by INTEGER,
            granted_at TEXT DEFAULT CURRENT_TIMESTAMP,
            FOREIGN KEY (user_id) REFERENCES users(id)
        );

        -- Audit logs
        CREATE TABLE IF NOT EXISTS audit_logs (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            actor_id INTEGER,
            actor_email TEXT,
            actor_role TEXT,
            action TEXT NOT NULL,
            category TEXT DEFAULT 'system',
            target_type TEXT,
            target_id TEXT,
            metadata TEXT,
            ip_address TEXT,
            timestamp TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_audit_ts ON audit_logs(timestamp);
        CREATE INDEX IF NOT EXISTS idx_audit_actor ON audit_logs(actor_email);
        CREATE INDEX IF NOT EXISTS idx_audit_action ON audit_logs(action);

        -- System config (editable without redeployment)
        CREATE TABLE IF NOT EXISTS system_config (
            key TEXT PRIMARY KEY,
            value TEXT,
            category TEXT DEFAULT 'general',
            description TEXT,
            updated_by TEXT,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        );

        -- Revenue events (subscription changes)
        CREATE TABLE IF NOT EXISTS revenue_events (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            event_type TEXT NOT NULL,
            tier_from TEXT,
            tier_to TEXT,
            amount REAL DEFAULT 0,
            currency TEXT DEFAULT 'INR',
            period TEXT,
            metadata TEXT,
            timestamp TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_rev_ts ON revenue_events(timestamp);

        -- Pipeline status (tracked per sync)
        CREATE TABLE IF NOT EXISTS pipeline_status (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            pipeline TEXT NOT NULL,
            status TEXT NOT NULL,
            records_processed INTEGER DEFAULT 0,
            error_message TEXT,
            started_at TEXT,
            completed_at TEXT,
            duration_seconds REAL DEFAULT 0
        );
        CREATE INDEX IF NOT EXISTS idx_pipe_pipeline ON pipeline_status(pipeline);

        -- Feature flags
        CREATE TABLE IF NOT EXISTS feature_flags (
            key TEXT PRIMARY KEY,
            enabled INTEGER DEFAULT 1,
            target TEXT DEFAULT 'all',
            description TEXT,
            updated_by TEXT,
            updated_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()

    # Seed default configs if empty
    existing = conn.execute("SELECT COUNT(*) FROM system_config").fetchone()[0]
    if existing == 0:
        defaults = [
            ("qbram_expansion_threshold", "80", "qbram", "Score threshold for EXPANSION regime"),
            ("qbram_accumulation_threshold", "60", "qbram", "Score threshold for ACCUMULATION regime"),
            ("qbram_transition_threshold", "40", "qbram", "Score threshold for TRANSITION regime"),
            ("qbram_distribution_threshold", "20", "qbram", "Score threshold for DISTRIBUTION regime"),
            ("iv_volume_multiplier", "2.0", "signals", "IV signal: volume must be N× average"),
            ("iv_close_position", "0.75", "signals", "IV signal: close must be in top N% of range"),
            ("ppv_lookback", "10", "signals", "PPV: lookback days for down-volume comparison"),
            ("bs_gap_pct", "3.0", "signals", "Bull Snort: minimum gap-up percentage"),
            ("bs_volume_multiplier", "1.5", "signals", "Bull Snort: volume must be N× average"),
            ("breakout_lookback", "30", "signals", "Breakout: N-bar high lookback"),
            ("alpha_cluster_momentum_max", "20", "alpha", "Alpha score: max momentum cluster points"),
            ("alpha_cluster_flow_max", "20", "alpha", "Alpha score: max flow cluster points"),
            ("alpha_cluster_institutional_max", "20", "alpha", "Alpha score: max institutional points"),
            ("alpha_cluster_fundamental_max", "20", "alpha", "Alpha score: max fundamental points"),
            ("alpha_cluster_regime_max", "20", "alpha", "Alpha score: max regime alignment points"),
            ("auto_refresh_interval_min", "15", "system", "Frontend auto-refresh interval (minutes)"),
            ("ohlcv_stale_days", "1", "system", "OHLCV data considered stale after N days"),
            ("rs_cache_ttl_hours", "4", "system", "RS rankings cache TTL (hours)"),
            ("max_thread_workers", "8", "system", "Thread pool max workers"),
        ]
        conn.executemany(
            "INSERT OR IGNORE INTO system_config (key, value, category, description) VALUES (?,?,?,?)",
            defaults
        )
        conn.commit()

    # Seed default feature flags
    ff_count = conn.execute("SELECT COUNT(*) FROM feature_flags").fetchone()[0]
    if ff_count == 0:
        flags = [
            ("smart_money_tab", 1, "all", "Smart Money Tracker tab"),
            ("journal_tab", 1, "all", "Trading Journal tab"),
            ("smart_chart_tab", 1, "all", "Smart Chart tab"),
            ("stockbee_monitor", 1, "all", "Stockbee Market Monitor"),
            ("ai_intelligence", 1, "all", "AI Intelligence card on Overview"),
            ("alpha_composite_score", 1, "all", "Alpha Composite Score in Smart Money"),
            ("auto_ohlcv_sync", 1, "all", "Auto-sync OHLCV on startup"),
            ("auto_insider_sync", 1, "all", "Auto-sync insider data on startup"),
        ]
        conn.executemany(
            "INSERT OR IGNORE INTO feature_flags (key, enabled, target, description) VALUES (?,?,?,?)",
            flags
        )
        conn.commit()

    conn.close()
    logger.info("✅ Control Center tables ready")


# ══════════════════════════════════════════════════════════════════════════════
# RBAC
# ══════════════════════════════════════════════════════════════════════════════

def get_admin_role(user_id: int) -> dict:
    conn = sqlite3.connect(DB_PATH, timeout=10)
    row = conn.execute("SELECT role FROM admin_roles WHERE user_id=?", (user_id,)).fetchone()
    conn.close()
    if row:
        role = row[0]
        return {"role": role, **ROLES.get(role, {})}
    return None


def set_admin_role(user_id: int, role: str, granted_by: int = None) -> dict:
    if role not in ROLES:
        return {"error": f"Invalid role: {role}"}
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("""
        INSERT OR REPLACE INTO admin_roles (user_id, role, granted_by, granted_at)
        VALUES (?, ?, ?, ?)
    """, (user_id, role, granted_by, datetime.now(timezone.utc).isoformat()))
    conn.commit()
    conn.close()
    return {"status": "ok", "role": role}


def check_permission(user_id: int, permission: str) -> bool:
    role_data = get_admin_role(user_id)
    if not role_data:
        return False
    return permission in role_data.get("permissions", [])


# ══════════════════════════════════════════════════════════════════════════════
# AUDIT LOGS
# ══════════════════════════════════════════════════════════════════════════════

def log_audit(actor_id: int, actor_email: str, actor_role: str, action: str,
              category: str = "system", target_type: str = None, target_id: str = None,
              metadata: dict = None, ip_address: str = None):
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        conn.execute("""
            INSERT INTO audit_logs (actor_id, actor_email, actor_role, action, category,
                                     target_type, target_id, metadata, ip_address, timestamp)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, (actor_id, actor_email, actor_role, action, category,
              target_type, target_id, json.dumps(metadata) if metadata else None,
              ip_address, datetime.now(timezone.utc).isoformat()))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.debug(f"Audit log failed: {e}")


def get_audit_logs(limit: int = 100, category: str = None, actor: str = None, days: int = 30) -> list:
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()
    query = "SELECT * FROM audit_logs WHERE timestamp >= ?"
    params = [cutoff]
    if category:
        query += " AND category = ?"
        params.append(category)
    if actor:
        query += " AND actor_email LIKE ?"
        params.append(f"%{actor}%")
    query += " ORDER BY timestamp DESC LIMIT ?"
    params.append(limit)
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


# ══════════════════════════════════════════════════════════════════════════════
# SYSTEM MONITORING
# ══════════════════════════════════════════════════════════════════════════════

def get_system_status() -> dict:
    """Get comprehensive system health overview."""
    conn = sqlite3.connect(DB_PATH, timeout=10)
    status = {}

    # DB stats
    tables = conn.execute("""
        SELECT name FROM sqlite_master WHERE type='table' ORDER BY name
    """).fetchall()
    db_stats = {}
    for (tbl,) in tables:
        try:
            count = conn.execute(f"SELECT COUNT(*) FROM [{tbl}]").fetchone()[0]
            db_stats[tbl] = count
        except:
            db_stats[tbl] = -1
    status["db_tables"] = db_stats
    status["db_size_mb"] = round(os.path.getsize(DB_PATH) / 1024 / 1024, 2) if os.path.exists(DB_PATH) else 0

    # Pipeline freshness
    pipelines = {}
    for pipeline in ["ohlcv", "insider", "fiidii", "tv_fundamentals", "rs_rankings", "fvalue"]:
        try:
            if pipeline == "ohlcv":
                row = conn.execute("SELECT MAX(date) FROM ohlcv WHERE market='India'").fetchone()
                pipelines[pipeline] = {"last_date": row[0] if row else None, "status": "ok" if row and row[0] else "no_data"}
            elif pipeline == "insider":
                row = conn.execute("SELECT MAX(created_at), COUNT(*) FROM insider_trades").fetchone()
                pipelines[pipeline] = {"last_sync": row[0], "total": row[1], "status": "ok" if row[1] > 0 else "no_data"}
            elif pipeline == "fiidii":
                row = conn.execute("SELECT MAX(date), COUNT(*) FROM fiidii").fetchone()
                pipelines[pipeline] = {"last_date": row[0], "total": row[1], "status": "ok" if row[1] > 0 else "no_data"}
            elif pipeline == "tv_fundamentals":
                row = conn.execute("SELECT MAX(fetched_at), COUNT(*) FROM tv_fundamentals").fetchone()
                pipelines[pipeline] = {"last_sync": row[0], "total": row[1], "status": "ok" if row[1] > 0 else "no_data"}
            elif pipeline == "fvalue":
                row = conn.execute("SELECT COUNT(*) FROM tv_fundamentals WHERE eps_ttm IS NOT NULL AND eps_ttm > 0").fetchone()
                pipelines[pipeline] = {"graded": row[0], "status": "ok" if row[0] > 0 else "no_data"}
            else:
                pipelines[pipeline] = {"status": "unknown"}
        except Exception as e:
            pipelines[pipeline] = {"status": "error", "error": str(e)}

    status["pipelines"] = pipelines

    # Recent pipeline runs
    try:
        runs = conn.execute("""
            SELECT pipeline, status, records_processed, error_message, started_at, completed_at, duration_seconds
            FROM pipeline_status ORDER BY id DESC LIMIT 20
        """).fetchall()
        status["recent_runs"] = [{"pipeline": r[0], "status": r[1], "records": r[2], "error": r[3],
                                   "started": r[4], "completed": r[5], "duration": r[6]} for r in runs]
    except:
        status["recent_runs"] = []

    # Error count (last 24h from audit logs)
    try:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        row = conn.execute("SELECT COUNT(*) FROM audit_logs WHERE category='error' AND timestamp >= ?", (cutoff,)).fetchone()
        status["errors_24h"] = row[0] if row else 0
    except:
        status["errors_24h"] = 0

    conn.close()
    return status


# ══════════════════════════════════════════════════════════════════════════════
# CONFIGURATION PANEL
# ══════════════════════════════════════════════════════════════════════════════

def get_all_configs() -> list:
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM system_config ORDER BY category, key").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def update_config(key: str, value: str, updated_by: str = "admin") -> dict:
    conn = sqlite3.connect(DB_PATH, timeout=10)
    existing = conn.execute("SELECT key FROM system_config WHERE key=?", (key,)).fetchone()
    if not existing:
        conn.close()
        return {"error": "Config key not found"}
    conn.execute("UPDATE system_config SET value=?, updated_by=?, updated_at=? WHERE key=?",
                 (value, updated_by, datetime.now(timezone.utc).isoformat(), key))
    conn.commit()
    conn.close()
    return {"status": "ok", "key": key, "value": value}


def get_config_value(key: str, default=None):
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        row = conn.execute("SELECT value FROM system_config WHERE key=?", (key,)).fetchone()
        conn.close()
        return row[0] if row else default
    except:
        return default


def get_feature_flags() -> list:
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    rows = conn.execute("SELECT * FROM feature_flags ORDER BY key").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def toggle_feature_flag(key: str, enabled: bool, updated_by: str = "admin") -> dict:
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("UPDATE feature_flags SET enabled=?, updated_by=?, updated_at=? WHERE key=?",
                 (1 if enabled else 0, updated_by, datetime.now(timezone.utc).isoformat(), key))
    conn.commit()
    conn.close()
    return {"status": "ok", "key": key, "enabled": enabled}


# ══════════════════════════════════════════════════════════════════════════════
# REVENUE ANALYTICS
# ══════════════════════════════════════════════════════════════════════════════

def log_revenue_event(user_id: int, event_type: str, tier_from: str = None,
                       tier_to: str = None, amount: float = 0, metadata: dict = None):
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("""
        INSERT INTO revenue_events (user_id, event_type, tier_from, tier_to, amount, metadata, timestamp)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (user_id, event_type, tier_from, tier_to, amount,
          json.dumps(metadata) if metadata else None, datetime.now(timezone.utc).isoformat()))
    conn.commit()
    conn.close()


def get_revenue_analytics() -> dict:
    conn = sqlite3.connect(DB_PATH, timeout=10)

    # Current subscriber counts
    pro_count = conn.execute("SELECT COUNT(*) FROM users WHERE tier='pro' AND status='active'").fetchone()[0]
    explorer_count = conn.execute("SELECT COUNT(*) FROM users WHERE tier='explorer' AND status='active'").fetchone()[0]
    total_users = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    held_count = conn.execute("SELECT COUNT(*) FROM users WHERE status='held'").fetchone()[0]

    # MRR / ARR
    mrr = pro_count * 299
    arr = mrr * 12

    # ARPU
    arpu = round(mrr / max(total_users, 1), 2)

    # Churn (users who went from pro → explorer or held in last 30 days)
    cutoff = (datetime.now(timezone.utc) - timedelta(days=30)).isoformat()
    churn_events = conn.execute("""
        SELECT COUNT(*) FROM revenue_events
        WHERE event_type IN ('downgrade', 'churn') AND timestamp >= ?
    """, (cutoff,)).fetchone()[0]
    churn_rate = round(churn_events / max(pro_count, 1) * 100, 1)

    # Conversion rate (explorer → pro)
    conversion_events = conn.execute("""
        SELECT COUNT(*) FROM revenue_events
        WHERE event_type = 'upgrade' AND timestamp >= ?
    """, (cutoff,)).fetchone()[0]
    conversion_rate = round(conversion_events / max(explorer_count, 1) * 100, 1)

    # Monthly revenue trend (last 6 months)
    monthly_trend = []
    for i in range(6):
        month_start = (datetime.now(timezone.utc).replace(day=1) - timedelta(days=30 * i)).strftime("%Y-%m-01")
        month_end = (datetime.now(timezone.utc).replace(day=1) - timedelta(days=30 * (i - 1))).strftime("%Y-%m-01")
        rev = conn.execute("""
            SELECT COALESCE(SUM(amount), 0) FROM revenue_events
            WHERE event_type IN ('payment', 'upgrade') AND timestamp >= ? AND timestamp < ?
        """, (month_start, month_end)).fetchone()[0]
        monthly_trend.append({"month": month_start[:7], "revenue": round(rev, 2)})
    monthly_trend.reverse()

    # Recent events
    recent = conn.execute("""
        SELECT r.*, u.email, u.name FROM revenue_events r
        LEFT JOIN users u ON r.user_id = u.id
        ORDER BY r.timestamp DESC LIMIT 20
    """).fetchall()
    recent_events = [{"id": r[0], "user_id": r[1], "event_type": r[2], "tier_from": r[3],
                       "tier_to": r[4], "amount": r[5], "timestamp": r[9],
                       "email": r[10], "name": r[11]} for r in recent]

    conn.close()

    return {
        "summary": {
            "total_users": total_users,
            "pro_subscribers": pro_count,
            "explorers": explorer_count,
            "held": held_count,
            "mrr": mrr,
            "arr": arr,
            "arpu": arpu,
            "churn_rate": churn_rate,
            "conversion_rate": conversion_rate,
        },
        "monthly_trend": monthly_trend,
        "recent_events": recent_events,
    }


# ══════════════════════════════════════════════════════════════════════════════
# ENHANCED USER MANAGEMENT
# ══════════════════════════════════════════════════════════════════════════════

def get_users_detailed(search: str = None, tier: str = None, status: str = None,
                        limit: int = 100, offset: int = 0) -> dict:
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row

    query = "SELECT u.*, ar.role as admin_role FROM users u LEFT JOIN admin_roles ar ON u.id = ar.user_id WHERE 1=1"
    params = []

    if search:
        query += " AND (u.email LIKE ? OR u.name LIKE ?)"
        params.extend([f"%{search}%", f"%{search}%"])
    if tier:
        query += " AND u.tier = ?"
        params.append(tier)
    if status:
        query += " AND u.status = ?"
        params.append(status)

    # Count total
    count_query = query.replace("SELECT u.*, ar.role as admin_role FROM users u LEFT JOIN admin_roles ar ON u.id = ar.user_id", "SELECT COUNT(*) FROM users u LEFT JOIN admin_roles ar ON u.id = ar.user_id")
    total = conn.execute(count_query, params).fetchone()[0]

    query += " ORDER BY u.id DESC LIMIT ? OFFSET ?"
    params.extend([limit, offset])
    rows = conn.execute(query, params).fetchall()
    conn.close()

    return {"users": [dict(r) for r in rows], "total": total, "limit": limit, "offset": offset}


def log_pipeline_run(pipeline: str, status: str, records: int = 0, error: str = None,
                      started_at: str = None, duration: float = 0):
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        conn.execute("""
            INSERT INTO pipeline_status (pipeline, status, records_processed, error_message,
                                          started_at, completed_at, duration_seconds)
            VALUES (?, ?, ?, ?, ?, ?, ?)
        """, (pipeline, status, records, error, started_at,
              datetime.now(timezone.utc).isoformat(), duration))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.debug(f"Pipeline log failed: {e}")
