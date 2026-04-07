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

    # Seed demo users if only admin exists
    _seed_demo_users()


def _seed_demo_users():
    """Seed demo users for testing the Control Center."""
    conn = sqlite3.connect(DB_PATH, timeout=10)
    user_count = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    if user_count > 2:
        conn.close()
        return  # Already has users

    import bcrypt
    now = datetime.now(timezone.utc).isoformat()
    demo_users = [
        ("rahul.sharma@gmail.com", "Rahul Sharma", "pro", "active", -45),
        ("priya.patel@outlook.com", "Priya Patel", "pro", "active", -30),
        ("amit.verma@yahoo.com", "Amit Verma", "pro", "active", -60),
        ("sneha.reddy@gmail.com", "Sneha Reddy", "explorer", "active", -20),
        ("vikram.singh@hotmail.com", "Vikram Singh", "explorer", "active", -15),
        ("anjali.gupta@gmail.com", "Anjali Gupta", "pro", "held", -90),
        ("rohit.kumar@outlook.com", "Rohit Kumar", "explorer", "active", -10),
        ("neha.joshi@gmail.com", "Neha Joshi", "pro", "active", -75),
        ("arjun.nair@yahoo.com", "Arjun Nair", "explorer", "held", -50),
        ("kavita.iyer@gmail.com", "Kavita Iyer", "pro", "active", -35),
        ("suresh.menon@hotmail.com", "Suresh Menon", "explorer", "active", -5),
        ("deepika.rao@gmail.com", "Deepika Rao", "pro", "active", -25),
        ("manish.agarwal@outlook.com", "Manish Agarwal", "explorer", "active", -40),
        ("pooja.desai@gmail.com", "Pooja Desai", "pro", "held", -55),
        ("rajesh.pillai@yahoo.com", "Rajesh Pillai", "explorer", "active", -8),
    ]

    pw_hash = bcrypt.hashpw("demo2026".encode(), bcrypt.gensalt()).decode()
    for email, name, tier, status, days_ago in demo_users:
        created = (datetime.now(timezone.utc) + timedelta(days=days_ago)).isoformat()
        last_login = (datetime.now(timezone.utc) + timedelta(days=days_ago + 3)).isoformat() if status == "active" else None
        try:
            conn.execute("""
                INSERT OR IGNORE INTO users (email, name, password_hash, tier, status, created_at, last_login)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (email, name, pw_hash, tier, status, created, last_login))
        except:
            pass

    conn.commit()

    # Seed some revenue events for demo
    pro_users = conn.execute("SELECT id, email, tier, created_at FROM users WHERE tier='pro'").fetchall()
    for uid, email, tier, created in pro_users:
        try:
            conn.execute("""
                INSERT OR IGNORE INTO revenue_events (user_id, event_type, tier_from, tier_to, amount, timestamp)
                VALUES (?, 'upgrade', 'explorer', 'pro', 299, ?)
            """, (uid, created))
        except:
            pass

    conn.commit()
    conn.close()
    logger.info(f"✅ Seeded {len(demo_users)} demo users")


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


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 2: ALERTS SYSTEM
# ══════════════════════════════════════════════════════════════════════════════

def ensure_phase2_tables():
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.executescript("""
        CREATE TABLE IF NOT EXISTS alerts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            severity TEXT NOT NULL DEFAULT 'warning',
            category TEXT DEFAULT 'system',
            title TEXT NOT NULL,
            message TEXT,
            source TEXT,
            acknowledged INTEGER DEFAULT 0,
            acknowledged_by TEXT,
            acknowledged_at TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_alerts_sev ON alerts(severity);
        CREATE INDEX IF NOT EXISTS idx_alerts_ack ON alerts(acknowledged);

        CREATE TABLE IF NOT EXISTS api_keys (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            key_hash TEXT NOT NULL,
            key_prefix TEXT NOT NULL,
            name TEXT DEFAULT '',
            permissions TEXT DEFAULT 'read',
            rate_limit INTEGER DEFAULT 100,
            calls_today INTEGER DEFAULT 0,
            last_used TEXT,
            status TEXT DEFAULT 'active',
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            expires_at TEXT
        );

        CREATE TABLE IF NOT EXISTS api_usage_log (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            api_key_prefix TEXT,
            endpoint TEXT,
            method TEXT,
            status_code INTEGER,
            response_time_ms REAL,
            ip_address TEXT,
            timestamp TEXT DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX IF NOT EXISTS idx_api_usage_ts ON api_usage_log(timestamp);

        -- Phase 3: User sessions / behavior
        CREATE TABLE IF NOT EXISTS user_sessions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER,
            session_token TEXT,
            ip_address TEXT,
            user_agent TEXT,
            device_type TEXT,
            started_at TEXT DEFAULT CURRENT_TIMESTAMP,
            last_active TEXT,
            page_views INTEGER DEFAULT 0,
            features_used TEXT DEFAULT '[]',
            ended_at TEXT
        );
        CREATE INDEX IF NOT EXISTS idx_sessions_user ON user_sessions(user_id);

        -- Phase 3: Announcements / Communication
        CREATE TABLE IF NOT EXISTS announcements (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            title TEXT NOT NULL,
            message TEXT NOT NULL,
            type TEXT DEFAULT 'info',
            target TEXT DEFAULT 'all',
            priority INTEGER DEFAULT 0,
            active INTEGER DEFAULT 1,
            created_by TEXT,
            created_at TEXT DEFAULT CURRENT_TIMESTAMP,
            expires_at TEXT
        );
    """)
    conn.commit()
    conn.close()


# ── Alerts ────────────────────────────────────────────────────────────────────

def create_alert(severity: str, title: str, message: str = "",
                  category: str = "system", source: str = ""):
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("""
        INSERT INTO alerts (severity, category, title, message, source, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (severity, category, title, message, source,
          datetime.now(timezone.utc).isoformat()))
    conn.commit()
    conn.close()


def get_alerts(acknowledged: bool = False, severity: str = None, limit: int = 50) -> list:
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    query = "SELECT * FROM alerts WHERE acknowledged = ?"
    params = [1 if acknowledged else 0]
    if severity:
        query += " AND severity = ?"
        params.append(severity)
    query += " ORDER BY created_at DESC LIMIT ?"
    params.append(limit)
    rows = conn.execute(query, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def acknowledge_alert(alert_id: int, acknowledged_by: str = "admin") -> dict:
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("UPDATE alerts SET acknowledged=1, acknowledged_by=?, acknowledged_at=? WHERE id=?",
                 (acknowledged_by, datetime.now(timezone.utc).isoformat(), alert_id))
    conn.commit()
    conn.close()
    return {"status": "ok"}


def check_system_alerts():
    """Auto-generate alerts based on system health checks."""
    alerts_created = 0
    conn = sqlite3.connect(DB_PATH, timeout=10)
    try:
        # Check OHLCV freshness
        row = conn.execute("SELECT MAX(date) FROM ohlcv WHERE market='India'").fetchone()
        if row and row[0]:
            from datetime import date as _date
            last = _date.fromisoformat(row[0])
            days_old = (_date.today() - last).days
            if days_old > 3:
                create_alert("critical", "OHLCV Data Stale",
                             f"Last OHLCV date is {row[0]} ({days_old} days old). Run Data Import.",
                             "data", "auto_check")
                alerts_created += 1

        # Check TV Fundamentals freshness
        row = conn.execute("SELECT MAX(fetched_at) FROM tv_fundamentals").fetchone()
        if row and row[0]:
            try:
                age_h = (datetime.now(timezone.utc) - datetime.fromisoformat(row[0])).total_seconds() / 3600
                if age_h > 48:
                    create_alert("warning", "TV Fundamentals Stale",
                                 f"Last sync was {int(age_h)} hours ago. Auto-sync may have failed.",
                                 "data", "auto_check")
                    alerts_created += 1
            except:
                pass

        # Check DB size
        db_size = os.path.getsize(DB_PATH) / 1024 / 1024
        if db_size > 500:
            create_alert("warning", "Database Size Warning",
                         f"DB is {db_size:.0f} MB. Consider archiving old data.",
                         "system", "auto_check")
            alerts_created += 1

        # Check for errors in last 24h
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=24)).isoformat()
        err_count = conn.execute(
            "SELECT COUNT(*) FROM audit_logs WHERE category='error' AND timestamp >= ?",
            (cutoff,)
        ).fetchone()[0]
        if err_count > 10:
            create_alert("critical", f"{err_count} Errors in Last 24h",
                         "Check audit logs for details.", "system", "auto_check")
            alerts_created += 1

    except Exception as e:
        logger.debug(f"Alert check failed: {e}")
    finally:
        conn.close()

    return alerts_created


# ── API Key Management ────────────────────────────────────────────────────────

def generate_api_key(user_id: int, name: str = "", permissions: str = "read",
                      rate_limit: int = 100) -> dict:
    import hashlib, secrets
    raw_key = f"qb360_{secrets.token_urlsafe(32)}"
    key_hash = hashlib.sha256(raw_key.encode()).hexdigest()
    prefix = raw_key[:12]

    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("""
        INSERT INTO api_keys (user_id, key_hash, key_prefix, name, permissions, rate_limit, created_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (user_id, key_hash, prefix, name, permissions, rate_limit,
          datetime.now(timezone.utc).isoformat()))
    conn.commit()
    conn.close()
    return {"key": raw_key, "prefix": prefix, "name": name}


def list_api_keys(user_id: int = None) -> list:
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    if user_id:
        rows = conn.execute("SELECT id, user_id, key_prefix, name, permissions, rate_limit, calls_today, last_used, status, created_at FROM api_keys WHERE user_id=?", (user_id,)).fetchall()
    else:
        rows = conn.execute("SELECT id, user_id, key_prefix, name, permissions, rate_limit, calls_today, last_used, status, created_at FROM api_keys ORDER BY id DESC").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def revoke_api_key(key_id: int) -> dict:
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("UPDATE api_keys SET status='revoked' WHERE id=?", (key_id,))
    conn.commit()
    conn.close()
    return {"status": "ok"}


# ══════════════════════════════════════════════════════════════════════════════
# PHASE 3: USER BEHAVIOR INTELLIGENCE
# ══════════════════════════════════════════════════════════════════════════════

def log_session(user_id: int, ip: str = "", user_agent: str = "", device: str = ""):
    conn = sqlite3.connect(DB_PATH, timeout=10)
    token = f"sess_{int(time.time())}_{user_id}"
    conn.execute("""
        INSERT INTO user_sessions (user_id, session_token, ip_address, user_agent, device_type, started_at, last_active)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (user_id, token, ip, user_agent, device,
          datetime.now(timezone.utc).isoformat(), datetime.now(timezone.utc).isoformat()))
    conn.commit()
    conn.close()
    return token


def get_behavior_analytics(days: int = 30) -> dict:
    conn = sqlite3.connect(DB_PATH, timeout=10)
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

    # DAU (distinct users with sessions per day)
    dau_rows = conn.execute("""
        SELECT DATE(started_at) as day, COUNT(DISTINCT user_id) as users
        FROM user_sessions WHERE started_at >= ?
        GROUP BY DATE(started_at) ORDER BY day
    """, (cutoff,)).fetchall()
    dau = [{"date": r[0], "users": r[1]} for r in dau_rows]

    # MAU
    mau = conn.execute("""
        SELECT COUNT(DISTINCT user_id) FROM user_sessions WHERE started_at >= ?
    """, (cutoff,)).fetchone()[0]

    # Total sessions
    total_sessions = conn.execute(
        "SELECT COUNT(*) FROM user_sessions WHERE started_at >= ?", (cutoff,)
    ).fetchone()[0]

    # Avg sessions per user
    avg_sessions = round(total_sessions / max(mau, 1), 1)

    # Device breakdown
    devices = conn.execute("""
        SELECT device_type, COUNT(*) as cnt FROM user_sessions
        WHERE started_at >= ? AND device_type != ''
        GROUP BY device_type ORDER BY cnt DESC
    """, (cutoff,)).fetchall()

    # Top users by session count
    top_users = conn.execute("""
        SELECT s.user_id, u.email, u.name, COUNT(*) as sessions,
               MAX(s.last_active) as last_active
        FROM user_sessions s LEFT JOIN users u ON s.user_id = u.id
        WHERE s.started_at >= ?
        GROUP BY s.user_id ORDER BY sessions DESC LIMIT 10
    """, (cutoff,)).fetchall()

    # Suspicious: same user from multiple IPs
    multi_ip = conn.execute("""
        SELECT user_id, COUNT(DISTINCT ip_address) as ips
        FROM user_sessions WHERE started_at >= ?
        GROUP BY user_id HAVING ips > 5
    """, (cutoff,)).fetchall()

    conn.close()

    return {
        "dau": dau,
        "mau": mau,
        "total_sessions": total_sessions,
        "avg_sessions_per_user": avg_sessions,
        "devices": [{"type": d[0] or "Unknown", "count": d[1]} for d in devices],
        "top_users": [{"user_id": u[0], "email": u[1], "name": u[2],
                        "sessions": u[3], "last_active": u[4]} for u in top_users],
        "suspicious_multi_ip": [{"user_id": m[0], "ip_count": m[1]} for m in multi_ip],
    }


# ── Revenue Deep Analytics ────────────────────────────────────────────────────

def get_revenue_deep(days: int = 180) -> dict:
    conn = sqlite3.connect(DB_PATH, timeout=10)
    cutoff = (datetime.now(timezone.utc) - timedelta(days=days)).isoformat()

    # ARPU trend (monthly)
    arpu_trend = []
    for i in range(6):
        month_start = (datetime.now(timezone.utc).replace(day=1) - timedelta(days=30 * i)).strftime("%Y-%m-01")
        month_end = (datetime.now(timezone.utc).replace(day=1) - timedelta(days=30 * (i - 1))).strftime("%Y-%m-01")
        total_users_month = conn.execute(
            "SELECT COUNT(*) FROM users WHERE created_at < ?", (month_end,)
        ).fetchone()[0]
        rev = conn.execute("""
            SELECT COALESCE(SUM(amount), 0) FROM revenue_events
            WHERE event_type IN ('payment', 'upgrade') AND timestamp >= ? AND timestamp < ?
        """, (month_start, month_end)).fetchone()[0]
        arpu = round(rev / max(total_users_month, 1), 2)
        arpu_trend.append({"month": month_start[:7], "arpu": arpu, "users": total_users_month, "revenue": round(rev, 2)})
    arpu_trend.reverse()

    # Cohort analysis (users by signup month → retention)
    cohorts = []
    for i in range(6):
        cohort_month = (datetime.now(timezone.utc).replace(day=1) - timedelta(days=30 * i)).strftime("%Y-%m")
        signed_up = conn.execute(
            "SELECT COUNT(*) FROM users WHERE created_at LIKE ?", (f"{cohort_month}%",)
        ).fetchone()[0]
        still_active = conn.execute(
            "SELECT COUNT(*) FROM users WHERE created_at LIKE ? AND status='active'",
            (f"{cohort_month}%",)
        ).fetchone()[0]
        still_pro = conn.execute(
            "SELECT COUNT(*) FROM users WHERE created_at LIKE ? AND tier='pro' AND status='active'",
            (f"{cohort_month}%",)
        ).fetchone()[0]
        cohorts.append({
            "month": cohort_month,
            "signed_up": signed_up,
            "still_active": still_active,
            "still_pro": still_pro,
            "retention_pct": round(still_active / max(signed_up, 1) * 100, 1),
        })
    cohorts.reverse()

    # LTV estimate
    avg_monthly_rev = sum(a["revenue"] for a in arpu_trend) / max(len(arpu_trend), 1)
    avg_lifespan_months = 12  # assumption for now
    ltv = round(avg_monthly_rev / max(conn.execute("SELECT COUNT(*) FROM users WHERE tier='pro'").fetchone()[0], 1) * avg_lifespan_months, 2)

    conn.close()
    return {
        "arpu_trend": arpu_trend,
        "cohorts": cohorts,
        "estimated_ltv": ltv,
    }


# ── Communication Center ─────────────────────────────────────────────────────

def create_announcement(title: str, message: str, ann_type: str = "info",
                         target: str = "all", priority: int = 0,
                         created_by: str = "admin", expires_at: str = None) -> dict:
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("""
        INSERT INTO announcements (title, message, type, target, priority, created_by, created_at, expires_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
    """, (title, message, ann_type, target, priority, created_by,
          datetime.now(timezone.utc).isoformat(), expires_at))
    conn.commit()
    conn.close()
    return {"status": "ok"}


def get_announcements(active_only: bool = True) -> list:
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    if active_only:
        rows = conn.execute("""
            SELECT * FROM announcements WHERE active=1
            AND (expires_at IS NULL OR expires_at > ?)
            ORDER BY priority DESC, created_at DESC
        """, (datetime.now(timezone.utc).isoformat(),)).fetchall()
    else:
        rows = conn.execute("SELECT * FROM announcements ORDER BY created_at DESC LIMIT 50").fetchall()
    conn.close()
    return [dict(r) for r in rows]


def toggle_announcement(ann_id: int, active: bool) -> dict:
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.execute("UPDATE announcements SET active=? WHERE id=?", (1 if active else 0, ann_id))
    conn.commit()
    conn.close()
    return {"status": "ok"}
