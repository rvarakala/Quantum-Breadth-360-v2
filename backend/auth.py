"""
Authentication & User Management
- BCrypt password hashing
- JWT session tokens
- 4 Subscription tiers: explorer (free), trader ($29), pro ($79), elite ($149)
- 14-day Pro trial on signup
- Admin functions: add, remove, hold, upgrade
"""

import sqlite3, logging, os
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

# ── Subscription Tiers ────────────────────────────────────────────────────────

TIERS = {
    "explorer": {
        "name": "Explorer", "price_monthly": 0, "price_annual": 0,
        "tabs": ["overview", "breadth", "compare", "sectors"],
    },
    "trader": {
        "name": "Trader", "price_monthly": 29, "price_annual": 290,
        "tabs": ["overview", "breadth", "compare", "sectors",
                 "smart-money", "leaders", "screeners", "charts", "scanner", "stockbee"],
    },
    "pro": {
        "name": "Pro", "price_monthly": 79, "price_annual": 790,
        "tabs": ["overview", "breadth", "compare", "sectors",
                 "smart-money", "leaders", "screeners", "charts", "scanner", "stockbee",
                 "fvalue", "smart-screener", "smart-metrics", "insider", "fiidii",
                 "journal", "watchlist"],
    },
    "elite": {
        "name": "Elite", "price_monthly": 149, "price_annual": 1490,
        "tabs": "__all__",
    },
    "admin": {
        "name": "Admin", "price_monthly": 0, "price_annual": 0,
        "tabs": "__all__",
    },
}

TRIAL_DAYS = 14
TRIAL_TIER = "pro"  # Trial gives Pro-level access


def ensure_auth_tables():
    """Create users table if not exists, seed admin account."""
    conn = sqlite3.connect(str(DB_PATH), timeout=10)

    # Create table with trial support
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
            subscription_end TEXT,
            trial_ends_at TEXT,
            email_verified INTEGER DEFAULT 0,
            reset_token TEXT,
            reset_token_expires TEXT
        )
    """)
    conn.commit()

    # Add columns if missing (for existing DBs)
    for col, default in [("trial_ends_at", "TEXT"), ("email_verified", "INTEGER DEFAULT 0"),
                         ("reset_token", "TEXT"), ("reset_token_expires", "TEXT")]:
        try:
            conn.execute(f"ALTER TABLE users ADD COLUMN {col} {default}")
            conn.commit()
        except:
            pass

    # Seed admin
    admin = conn.execute("SELECT id, tier FROM users WHERE email = ?",
                         ("admin@quantumtrade.pro",)).fetchone()
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
        pw_hash = bcrypt.hashpw("QTP@admin2026".encode(), bcrypt.gensalt()).decode()
        conn.execute("UPDATE users SET tier='admin', password_hash=?, status='active' WHERE email=?",
                     (pw_hash, "admin@quantumtrade.pro"))
        conn.commit()

    conn.close()


# ── Registration & Login ─────────────────────────────────────────────────────

def register_user(email: str, name: str, password: str) -> dict:
    """Register a new user with 14-day Pro trial."""
    email = email.strip().lower()
    name = name.strip()

    if not email or "@" not in email:
        return {"error": "Invalid email"}
    if len(password) < 6:
        return {"error": "Password must be at least 6 characters"}
    if not name:
        return {"error": "Name is required"}

    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    existing = conn.execute("SELECT id FROM users WHERE email=?", (email,)).fetchone()
    if existing:
        conn.close()
        return {"error": "Email already registered"}

    pw_hash = bcrypt.hashpw(password.encode(), bcrypt.gensalt()).decode()
    now = datetime.now(timezone.utc)
    trial_end = (now + timedelta(days=TRIAL_DAYS)).isoformat()

    conn.execute("""
        INSERT INTO users (email, name, password_hash, tier, status, created_at, trial_ends_at)
        VALUES (?, ?, ?, ?, ?, ?, ?)
    """, (email, name, pw_hash, "explorer", "active", now.isoformat(), trial_end))
    conn.commit()
    user_id = conn.execute("SELECT id FROM users WHERE email=?", (email,)).fetchone()[0]
    conn.close()

    token = _generate_token(user_id, email, "explorer")
    logger.info(f"New user registered: {email} (explorer + {TRIAL_DAYS}d trial)")
    return {
        "status": "ok", "token": token,
        "user": {"id": user_id, "email": email, "name": name,
                 "tier": "explorer", "trial_ends_at": trial_end,
                 "effective_tier": TRIAL_TIER}
    }


def login_user(email: str, password: str) -> dict:
    """Authenticate user, return JWT + effective tier."""
    email = email.strip().lower()
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM users WHERE email=?", (email,)).fetchone()

    if not row:
        conn.close()
        return {"error": "Invalid email or password"}

    user = dict(row)
    if user["status"] == "held":
        conn.close()
        return {"error": "Account suspended. Contact support."}
    if user["status"] == "banned":
        conn.close()
        return {"error": "Account banned."}
    if not bcrypt.checkpw(password.encode(), user["password_hash"].encode()):
        conn.close()
        return {"error": "Invalid email or password"}

    conn.execute("UPDATE users SET last_login=? WHERE id=?",
                 (datetime.now(timezone.utc).isoformat(), user["id"]))
    conn.commit()
    conn.close()

    eff = _effective_tier(user)
    token = _generate_token(user["id"], user["email"], user["tier"])
    return {
        "status": "ok", "token": token,
        "user": {
            "id": user["id"], "email": user["email"], "name": user["name"],
            "tier": user["tier"], "effective_tier": eff["effective_tier"],
            "trial_ends_at": user.get("trial_ends_at"),
            "trial_days_left": eff.get("trial_days_left"),
            "trial_active": eff.get("trial_active", False),
        }
    }


def get_me(user_token_data: dict) -> dict:
    """Get current user info with effective tier (called from /api/auth/me)."""
    db_user = get_user_by_id(user_token_data["id"])
    if not db_user:
        return {"error": "User not found"}
    if db_user["status"] != "active":
        return {"error": "Account suspended"}

    eff = _effective_tier(db_user)
    result = {
        "id": db_user["id"],
        "email": db_user["email"],
        "name": db_user.get("name", ""),
        "tier": db_user["tier"],
        "effective_tier": eff["effective_tier"],
        "trial_ends_at": db_user.get("trial_ends_at"),
        "trial_days_left": eff.get("trial_days_left"),
        "trial_active": eff.get("trial_active", False),
        "allowed_tabs": eff["allowed_tabs"],
    }
    # Refresh token if tier changed
    if db_user["tier"] != user_token_data.get("tier"):
        result["refreshed_token"] = _generate_token(db_user["id"], db_user["email"], db_user["tier"])
    return result


def _effective_tier(user: dict) -> dict:
    """Compute effective tier considering trial period."""
    base_tier = user.get("tier", "explorer")

    # Admin and paid tiers: use as-is
    if base_tier in ("admin", "elite", "pro", "trader"):
        tier_info = TIERS.get(base_tier, TIERS["explorer"])
        return {
            "effective_tier": base_tier,
            "trial_active": False,
            "trial_days_left": 0,
            "allowed_tabs": tier_info["tabs"],
        }

    # Explorer: check trial
    trial_end = user.get("trial_ends_at")
    if trial_end:
        try:
            trial_dt = datetime.fromisoformat(trial_end)
            if trial_dt.tzinfo is None:
                trial_dt = trial_dt.replace(tzinfo=timezone.utc)
            now = datetime.now(timezone.utc)
            if now < trial_dt:
                days_left = max(0, (trial_dt - now).days)
                trial_tier_info = TIERS.get(TRIAL_TIER, TIERS["pro"])
                return {
                    "effective_tier": TRIAL_TIER,
                    "trial_active": True,
                    "trial_days_left": days_left,
                    "allowed_tabs": trial_tier_info["tabs"],
                }
        except:
            pass

    # Expired trial or no trial
    tier_info = TIERS.get(base_tier, TIERS["explorer"])
    return {
        "effective_tier": base_tier,
        "trial_active": False,
        "trial_days_left": 0,
        "allowed_tabs": tier_info["tabs"],
    }


# ── Token & User Lookup ──────────────────────────────────────────────────────

def verify_token(token: str) -> Optional[dict]:
    try:
        payload = jwt.decode(token, JWT_SECRET, algorithms=[JWT_ALGO])
        return {"id": payload["uid"], "email": payload["email"], "tier": payload["tier"], "exp": payload["exp"]}
    except jwt.ExpiredSignatureError:
        return None
    except jwt.InvalidTokenError:
        return None


def get_user_by_id(user_id: int) -> Optional[dict]:
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.row_factory = sqlite3.Row
    row = conn.execute("SELECT * FROM users WHERE id=?", (user_id,)).fetchone()
    conn.close()
    return dict(row) if row else None


def check_tab_access(tier: str, tab: str) -> bool:
    t = TIERS.get(tier, TIERS["explorer"])
    if t["tabs"] == "__all__":
        return True
    return tab in t["tabs"]


# ── Admin Functions ──────────────────────────────────────────────────────────

def admin_list_users() -> list:
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT id,email,name,tier,status,created_at,last_login,subscription_end,trial_ends_at FROM users ORDER BY id"
    ).fetchall()
    conn.close()
    result = []
    for r in rows:
        u = dict(r)
        eff = _effective_tier(u)
        u["effective_tier"] = eff["effective_tier"]
        u["trial_active"] = eff.get("trial_active", False)
        u["trial_days_left"] = eff.get("trial_days_left", 0)
        result.append(u)
    return result


def admin_update_user(user_id: int, tier: str = None, status: str = None) -> dict:
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    user = conn.execute("SELECT id,email FROM users WHERE id=?", (user_id,)).fetchone()
    if not user:
        conn.close()
        return {"error": "User not found"}
    updates, params = [], []
    if tier and tier in TIERS:
        updates.append("tier=?"); params.append(tier)
    if status and status in ("active", "held", "banned"):
        updates.append("status=?"); params.append(status)
    if updates:
        params.append(user_id)
        conn.execute(f"UPDATE users SET {','.join(updates)} WHERE id=?", params)
        conn.commit()
    conn.close()
    return {"status": "ok", "user_id": user_id}


def admin_delete_user(user_id: int) -> dict:
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    user = conn.execute("SELECT email,tier FROM users WHERE id=?", (user_id,)).fetchone()
    if not user:
        conn.close()
        return {"error": "User not found"}
    if user[1] == "admin":
        conn.close()
        return {"error": "Cannot delete admin"}
    conn.execute("DELETE FROM users WHERE id=?", (user_id,))
    conn.commit()
    conn.close()
    return {"status": "ok"}


def admin_add_user(email: str, name: str, password: str, tier: str = "explorer") -> dict:
    result = register_user(email, name, password)
    if result.get("error"):
        return result
    if tier != "explorer" and tier in TIERS:
        admin_update_user(result["user"]["id"], tier=tier)
        result["user"]["tier"] = tier
    return result


def admin_stats() -> dict:
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    total = conn.execute("SELECT COUNT(*) FROM users").fetchone()[0]
    active = conn.execute("SELECT COUNT(*) FROM users WHERE status='active'").fetchone()[0]
    traders = conn.execute("SELECT COUNT(*) FROM users WHERE tier='trader'").fetchone()[0]
    pros = conn.execute("SELECT COUNT(*) FROM users WHERE tier='pro'").fetchone()[0]
    elites = conn.execute("SELECT COUNT(*) FROM users WHERE tier='elite'").fetchone()[0]
    conn.close()
    mrr = traders * 29 + pros * 79 + elites * 149
    return {"total": total, "active": active, "traders": traders, "pros": pros,
            "elites": elites, "mrr_estimate": mrr}


def _generate_token(user_id: int, email: str, tier: str) -> str:
    payload = {
        "uid": user_id, "email": email, "tier": tier,
        "exp": datetime.now(timezone.utc) + timedelta(hours=JWT_EXPIRY_HOURS),
        "iat": datetime.now(timezone.utc),
    }
    return jwt.encode(payload, JWT_SECRET, algorithm=JWT_ALGO)
