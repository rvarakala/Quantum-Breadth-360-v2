"""
Authentication & User Management
- BCrypt password hashing
- JWT session tokens
- Subscription tiers: explorer (free), pro (₹299/mo)
- Admin functions: add, remove, hold, upgrade
"""

import sqlite3, logging, os, secrets
from pathlib import Path
from datetime import datetime, timedelta, timezone
from typing import Optional

import bcrypt
import jwt

logger = logging.getLogger(__name__)
DB_PATH = Path(__file__).parent / "breadth_data.db"

JWT_SECRET = os.environ.get("JWT_SECRET", "qbram-360-quantum-breadth-stable-secret-2026")
JWT_ALGO = "HS256"
JWT_EXPIRY_HOURS = 72  # 3 days

# Subscription tiers
TIERS = {
    "explorer": {
        "name": "Explorer",
        "price_monthly": 0,
        "price_annual": 0,
        "tabs": ["overview", "breadth", "sectors"],
    },
    "pro": {
        "name": "Pro",
        "price_monthly": 299,
        "price_annual": 2870,  # 239/mo × 12 = 2868 ≈ 2870
        "tabs": ["overview", "breadth", "sectors", "scanner", "leaders", "fiidii",
                 "fvalue", "insider", "stockbee", "smart-metrics", "charts-tab",
                 "smart-screener", "watchlist", "importer", "compare", "peep",
                 "smart-chart", "smart-money"],
    },
    "admin": {
        "name": "Admin",
        "price_monthly": 0,
        "price_annual": 0,
        "tabs": "__all__",
    },
}


def ensure_auth_tables():
    """Create users table if not exists, seed admin account."""
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            email TEXT UNIQUE NOT NULL,
            name TEXT NOT NULL DEFAULT '',
            password_hash TEXT NOT NULL,
            tier TEXT NOT NULL DEFAULT 'explorer',
            status TEXT NOT NULL DEFAULT 'active',
            created_at TEXT NOT NULL,
            last_login TEXT,
            subscription_end TEXT
        )
    """)
    conn.commit()

    # Seed admin if not exists, or fix tier/password if exists
    admin = conn.execute("SELECT id, tier FROM users WHERE email = ?", ("admin@quantumtrade.pro",)).fetchone()
    if not admin:
        pw_hash = bcrypt.hashpw("QTP@admin2026".encode(), bcrypt.gensalt()).decode()
        conn.execute("""
            INSERT INTO users (email, name, password_hash, tier, status, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
        """, ("admin@quantumtrade.pro", "Admin", pw_hash, "admin", "active",
              datetime.now(timezone.utc).isoformat()))
        conn.commit()
        logger.info("✅ Admin account seeded: admin@quantumtrade.pro")
    else:
        # Always ensure admin tier + reset password to known value
        pw_hash = bcrypt.hashpw("QTP@admin2026".encode(), bcrypt.gensalt()).decode()
        conn.execute("UPDATE users SET tier = 'admin', password_hash = ?, status = 'active' WHERE email = ?",
                     (pw_hash, "admin@quantumtrade.pro"))
        conn.commit()
        if admin[1] != "admin":
            logger.info("✅ Admin account fixed: tier upgraded to admin")

    conn.close()


def register_user(email: str, name: str, password: str) -> dict:
    """Register a new user (default: explorer tier)."""
    email = email.strip().lower()
    name = name.strip()

    if not email or "@" not in email:
        return {"error": "Invalid email"}
    if len(password) < 6:
        return {"error": "Password must be at least 6 characters"}
    if not name:
        return {"error": "Name is required"}

    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    existing = conn.execute("SELECT id FROM users WHERE email = ?", (email,)).fetchone()
    if existing:
        conn.close()
        return {"error": "Email already registered"}

    pw_hash = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
    now = datetime.now(timezone.utc).isoformat()

    conn.execute("""
        INSERT INTO users (email, name, password_hash, tier, status, created_at)
        VALUES (?, ?, ?, ?, ?, ?)
    """, (email, name, pw_hash, "explorer", "active", now))
    conn.commit()
    user_id = conn.execute("SELECT id FROM users WHERE email = ?", (email,)).fetchone()[0]
    conn.close()

    token = _generate_token(user_id, email, "explorer")
    logger.info(f"New user registered: {email} (explorer)")
    return {"status": "ok", "token": token, "user": {"id": user_id, "email": email, "name": name, "tier": "explorer"}}


def login_user(email: str, password: str) -> dict:
    """Authenticate user, return JWT token."""
    email = email.strip().lower()

    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    row = conn.execute(
        "SELECT id, email, name, password_hash, tier, status FROM users WHERE email = ?",
        (email,)
    ).fetchone()

    if not row:
        conn.close()
        return {"error": "Invalid email or password"}

    uid, uemail, uname, pw_hash, tier, status = row

    if status == "held":
        conn.close()
        return {"error": "Account suspended. Contact support."}
    if status == "banned":
        conn.close()
        return {"error": "Account banned."}

    if not bcrypt.checkpw(password.encode(), pw_hash.encode()):
        conn.close()
        return {"error": "Invalid email or password"}

    # Update last login
    conn.execute("UPDATE users SET last_login = ? WHERE id = ?",
                 (datetime.now(timezone.utc).isoformat(), uid))
    conn.commit()
    conn.close()

    token = _generate_token(uid, uemail, tier)
    return {"status": "ok", "token": token, "user": {"id": uid, "email": uemail, "name": uname, "tier": tier}}


def verify_token(token: str) -> Optional[dict]:
    """Verify JWT token, return user info or None."""
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGO])
        return {
            "id": payload["uid"],
            "email": payload["email"],
            "tier": payload["tier"],
            "exp": payload["exp"],
        }
    except jwt.ExpiredSignatureError:
        return None
    except jwt.InvalidTokenError:
        return None


def get_user_by_id(user_id: int) -> Optional[dict]:
    """Get user details by ID."""
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
    conn.close()
    if not row:
        return None
    return dict(row)


def check_tab_access(tier: str, tab: str) -> bool:
    """Check if a tier has access to a specific tab."""
    t = TIERS.get(tier, TIERS["explorer"])
    if t["tabs"] == "__all__":
        return True
    return tab in t["tabs"]


# ── Admin functions ───────────────────────────────────────────────────────────

def admin_list_users() -> list:
    """List all users for admin panel."""
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT id, email, name, tier, status, created_at, last_login, subscription_end FROM users ORDER BY id"
    ).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def admin_update_user(user_id: int, tier: str = None, status: str = None) -> dict:
    """Update user tier or status."""
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    user = conn.execute("SELECT id, email FROM users WHERE id = ?", (user_id,)).fetchone()
    if not user:
        conn.close()
        return {"error": "User not found"}

    updates = []
    params = []
    if tier and tier in TIERS:
        updates.append("tier = ?")
        params.append(tier)
    if status and status in ("active", "held", "banned"):
        updates.append("status = ?")
        params.append(status)

    if updates:
        params.append(user_id)
        conn.execute(f"UPDATE users SET {', '.join(updates)} WHERE id = ?", params)
        conn.commit()

    conn.close()
    logger.info(f"Admin updated user {user_id}: tier={tier}, status={status}")
    return {"status": "ok", "user_id": user_id}


def admin_delete_user(user_id: int) -> dict:
    """Delete a user."""
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    user = conn.execute("SELECT email, tier FROM users WHERE id = ?", (user_id,)).fetchone()
    if not user:
        conn.close()
        return {"error": "User not found"}
    if user[1] == "admin":
        conn.close()
        return {"error": "Cannot delete admin"}

    conn.execute("DELETE FROM users WHERE id = ?", (user_id,))
    conn.commit()
    conn.close()
    logger.info(f"Admin deleted user {user_id} ({user[0]})")
    return {"status": "ok"}


def admin_add_user(email: str, name: str, password: str, tier: str = "explorer") -> dict:
    """Admin creates a user with specified tier."""
    result = register_user(email, name, password)
    if result.get("error"):
        return result
    if tier != "explorer" and tier in TIERS:
        admin_update_user(result["user"]["id"], tier=tier)
        result["user"]["tier"] = tier
    return result


def admin_stats() -> dict:
    """Dashboard stats for admin."""
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    total = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    active = conn.execute("SELECT COUNT(*) FROM users WHERE status = 'active'").fetchone()[0]
    held = conn.execute("SELECT COUNT(*) FROM users WHERE status = 'held'").fetchone()[0]
    explorers = conn.execute("SELECT COUNT(*) FROM users WHERE tier = 'explorer'").fetchone()[0]
    pros = conn.execute("SELECT COUNT(*) FROM users WHERE tier = 'pro'").fetchone()[0]
    conn.close()
    return {
        "total": total, "active": active, "held": held,
        "explorers": explorers, "pros": pros,
        "mrr_estimate": pros * 299,
    }


# ── Internal ──────────────────────────────────────────────────────────────────

def _generate_token(user_id: int, email: str, tier: str) -> str:
    payload = {
        "uid": user_id,
        "email": email,
        "tier": tier,
        "exp": datetime.now(timezone.utc) + timedelta(hours=JWT_EXPIRY_HOURS),
        "iat": datetime.now(timezone.utc),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGO)
