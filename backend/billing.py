"""
Billing — Razorpay Integration
- Create payment orders for tier upgrades
- Verify webhook signatures + update user tier
- Payments table for history
- Subscription cancellation (marks tier as explorer at period end)
"""

import sqlite3, hmac, hashlib, logging, os
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import Optional

logger = logging.getLogger(__name__)
DB_PATH = Path(__file__).parent / "breadth_data.db"

# ── Razorpay config (from env) ────────────────────────────────────────────────
RZP_KEY_ID     = os.environ.get("RAZORPAY_KEY_ID", "")
RZP_KEY_SECRET = os.environ.get("RAZORPAY_KEY_SECRET", "")
RZP_ENABLED    = bool(RZP_KEY_ID and RZP_KEY_SECRET)

# ── Pricing (USD → INR at ≈84; keep INR as base for Razorpay paise) ───────────
PLAN_PRICES = {
    # tier: { monthly_inr, annual_inr, monthly_usd, annual_usd }
    "trader": {"monthly_inr": 2499, "annual_inr": 23990, "monthly_usd": 29, "annual_usd": 290},
    "pro":    {"monthly_inr": 6799, "annual_inr": 65990, "monthly_usd": 79, "annual_usd": 790},
    "elite":  {"monthly_inr": 12990,"annual_inr":124990, "monthly_usd":149, "annual_usd":1490},
}


def ensure_billing_tables():
    """Create payments table if not exists. Add payment columns to users."""
    conn = sqlite3.connect(str(DB_PATH), timeout=10)

    conn.execute("""
        CREATE TABLE IF NOT EXISTS payments (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            razorpay_order_id TEXT,
            razorpay_payment_id TEXT,
            razorpay_signature TEXT,
            tier TEXT NOT NULL,
            billing_cycle TEXT NOT NULL DEFAULT 'monthly',
            amount_inr INTEGER,
            amount_usd REAL,
            currency TEXT DEFAULT 'INR',
            status TEXT NOT NULL DEFAULT 'pending',
            created_at TEXT NOT NULL,
            verified_at TEXT,
            notes TEXT,
            FOREIGN KEY (user_id) REFERENCES users(id)
        )
    """)
    conn.commit()

    # Add payment-related columns to users if missing
    for col, defn in [
        ("razorpay_customer_id", "TEXT"),
        ("subscription_cycle",   "TEXT DEFAULT 'monthly'"),
        ("cancel_at_period_end", "INTEGER DEFAULT 0"),
    ]:
        try:
            conn.execute(f"ALTER TABLE users ADD COLUMN {col} {defn}")
            conn.commit()
        except Exception:
            pass

    conn.close()


# ── Order creation ────────────────────────────────────────────────────────────

def create_order(user_id: int, tier: str, cycle: str = "monthly") -> dict:
    """Create a Razorpay order for a tier upgrade. Returns order details."""
    if tier not in PLAN_PRICES:
        return {"error": f"Unknown tier: {tier}"}
    if cycle not in ("monthly", "annual"):
        return {"error": "cycle must be 'monthly' or 'annual'"}

    price_key = f"{cycle}_inr"
    amount_inr = PLAN_PRICES[tier][price_key]
    amount_paise = amount_inr * 100  # Razorpay uses paise

    # Record pending payment in DB first
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.row_factory = sqlite3.Row
    user = conn.execute("SELECT email, name FROM users WHERE id=?", (user_id,)).fetchone()
    if not user:
        conn.close()
        return {"error": "User not found"}

    now = datetime.now(timezone.utc).isoformat()
    cur = conn.execute("""
        INSERT INTO payments (user_id, tier, billing_cycle, amount_inr, amount_usd,
                              currency, status, created_at)
        VALUES (?, ?, ?, ?, ?, 'INR', 'pending', ?)
    """, (user_id, tier, cycle, amount_inr, PLAN_PRICES[tier][f"{cycle}_usd"], now))
    payment_db_id = cur.lastrowid
    conn.commit()
    conn.close()

    # If Razorpay is configured, create real order
    if RZP_ENABLED:
        try:
            import razorpay
            client = razorpay.Client(auth=(RZP_KEY_ID, RZP_KEY_SECRET))
            order = client.order.create({
                "amount":   amount_paise,
                "currency": "INR",
                "receipt":  f"qb360_{payment_db_id}",
                "notes": {
                    "user_id":    str(user_id),
                    "tier":       tier,
                    "cycle":      cycle,
                    "db_payment": str(payment_db_id),
                }
            })
            # Store order ID
            conn2 = sqlite3.connect(str(DB_PATH), timeout=10)
            conn2.execute("UPDATE payments SET razorpay_order_id=? WHERE id=?",
                          (order["id"], payment_db_id))
            conn2.commit()
            conn2.close()

            return {
                "order_id":      order["id"],
                "amount":        amount_paise,
                "currency":      "INR",
                "key_id":        RZP_KEY_ID,
                "tier":          tier,
                "cycle":         cycle,
                "payment_db_id": payment_db_id,
                "prefill": {
                    "name":  user["name"],
                    "email": user["email"],
                },
            }
        except Exception as e:
            logger.error(f"Razorpay order creation failed: {e}")
            return {"error": f"Payment gateway error: {e}"}

    # Razorpay not configured — return mock for development
    return {
        "order_id":      f"order_dev_{payment_db_id}",
        "amount":        amount_paise,
        "currency":      "INR",
        "key_id":        "rzp_test_dev",
        "tier":          tier,
        "cycle":         cycle,
        "payment_db_id": payment_db_id,
        "dev_mode":      True,
        "prefill": {"name": user["name"], "email": user["email"]},
    }


# ── Payment verification ──────────────────────────────────────────────────────

def verify_payment(
    razorpay_order_id: str,
    razorpay_payment_id: str,
    razorpay_signature: str,
    user_id: int,
) -> dict:
    """Verify Razorpay signature, upgrade user tier, record payment."""

    # Dev mode bypass (no real keys)
    if not RZP_ENABLED or razorpay_order_id.startswith("order_dev_"):
        return _activate_subscription(user_id, razorpay_order_id,
                                      razorpay_payment_id, razorpay_signature)

    # Verify HMAC signature
    body = f"{razorpay_order_id}|{razorpay_payment_id}"
    expected = hmac.new(
        RZP_KEY_SECRET.encode(), body.encode(), hashlib.sha256
    ).hexdigest()
    if not hmac.compare_digest(expected, razorpay_signature):
        logger.warning(f"Razorpay signature mismatch for user {user_id}")
        return {"error": "Payment verification failed — invalid signature"}

    return _activate_subscription(user_id, razorpay_order_id,
                                  razorpay_payment_id, razorpay_signature)


def _activate_subscription(user_id, order_id, payment_id, signature) -> dict:
    """Upgrade user tier after successful payment."""
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.row_factory = sqlite3.Row

    # Look up the pending payment record
    payment = conn.execute("""
        SELECT id, tier, billing_cycle, amount_inr FROM payments
        WHERE user_id=? AND razorpay_order_id=? AND status='pending'
        ORDER BY id DESC LIMIT 1
    """, (user_id, order_id)).fetchone()

    if not payment:
        conn.close()
        return {"error": "Payment record not found or already processed"}

    now = datetime.now(timezone.utc)

    # Calculate subscription end date
    if payment["billing_cycle"] == "annual":
        sub_end = (now + timedelta(days=365)).isoformat()
    else:
        sub_end = (now + timedelta(days=31)).isoformat()

    # Update payment record
    conn.execute("""
        UPDATE payments SET
            razorpay_payment_id=?, razorpay_signature=?,
            status='paid', verified_at=?
        WHERE id=?
    """, (payment_id, signature, now.isoformat(), payment["id"]))

    # Upgrade user tier + clear trial (they're now paying)
    conn.execute("""
        UPDATE users SET
            tier=?, subscription_end=?, trial_ends_at=NULL,
            cancel_at_period_end=0
        WHERE id=?
    """, (payment["tier"], sub_end, user_id))

    conn.commit()

    # Fetch updated user for response
    user = conn.execute(
        "SELECT email, name, tier FROM users WHERE id=?", (user_id,)
    ).fetchone()
    conn.close()

    logger.info(f"✅ Subscription activated: user {user_id} → {payment['tier']} "
                f"({payment['billing_cycle']}) until {sub_end}")

    return {
        "status":           "ok",
        "tier":             payment["tier"],
        "billing_cycle":    payment["billing_cycle"],
        "subscription_end": sub_end,
        "email":            user["email"],
    }


# ── Subscription management ───────────────────────────────────────────────────

def get_subscription(user_id: int) -> dict:
    """Get full subscription + payment history for a user."""
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.row_factory = sqlite3.Row

    user = conn.execute("""
        SELECT id, email, name, tier, status, trial_ends_at,
               subscription_end, subscription_cycle, cancel_at_period_end,
               created_at, last_login
        FROM users WHERE id=?
    """, (user_id,)).fetchone()

    if not user:
        conn.close()
        return {"error": "User not found"}

    u = dict(user)

    # Payment history (last 10)
    payments = conn.execute("""
        SELECT id, tier, billing_cycle, amount_inr, amount_usd, currency,
               status, created_at, verified_at, razorpay_payment_id
        FROM payments
        WHERE user_id=? AND status='paid'
        ORDER BY created_at DESC LIMIT 10
    """, (user_id,)).fetchall()

    conn.close()

    # Compute trial info
    from auth import _effective_tier
    eff = _effective_tier(u)

    return {
        "user_id":            u["id"],
        "email":              u["email"],
        "name":               u["name"],
        "tier":               u["tier"],
        "effective_tier":     eff["effective_tier"],
        "trial_active":       eff.get("trial_active", False),
        "trial_days_left":    eff.get("trial_days_left", 0),
        "trial_ends_at":      u.get("trial_ends_at"),
        "subscription_end":   u.get("subscription_end"),
        "subscription_cycle": u.get("subscription_cycle", "monthly"),
        "cancel_at_period_end": bool(u.get("cancel_at_period_end", 0)),
        "plan_prices":        PLAN_PRICES,
        "rzp_enabled":        RZP_ENABLED,
        "payments":           [dict(p) for p in payments],
    }


def cancel_subscription(user_id: int) -> dict:
    """Mark subscription to cancel at period end (doesn't downgrade immediately)."""
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    user = conn.execute("SELECT tier, subscription_end FROM users WHERE id=?",
                        (user_id,)).fetchone()
    if not user:
        conn.close()
        return {"error": "User not found"}
    if user[0] in ("explorer", "admin"):
        conn.close()
        return {"error": "No active subscription to cancel"}

    conn.execute("UPDATE users SET cancel_at_period_end=1 WHERE id=?", (user_id,))
    conn.commit()
    conn.close()

    return {
        "status": "ok",
        "message": f"Subscription will cancel on {user[1] or 'end of period'}. "
                   f"You keep full access until then.",
        "subscription_end": user[1],
    }


def reactivate_subscription(user_id: int) -> dict:
    """Undo a cancellation — keep subscription active."""
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.execute("UPDATE users SET cancel_at_period_end=0 WHERE id=?", (user_id,))
    conn.commit()
    conn.close()
    return {"status": "ok", "message": "Subscription reactivated."}


def get_plan_prices() -> dict:
    """Public endpoint — return pricing without auth."""
    return {
        "plans":       PLAN_PRICES,
        "rzp_enabled": RZP_ENABLED,
        "currency":    "INR",
        "currency_usd": True,
    }


# ── Razorpay Webhook ──────────────────────────────────────────────────────────

def handle_webhook(payload: bytes, signature: str) -> dict:
    """
    Verify and process Razorpay webhook events.
    Called from POST /api/billing/webhook.
    Supported events:
      payment.captured      → activate subscription
      subscription.charged  → renew subscription period
      subscription.cancelled→ schedule downgrade at period end
    """
    if not RZP_ENABLED:
        return {"status": "skipped", "reason": "Razorpay not configured"}

    # ── 1. Verify signature ───────────────────────────────────────────────────
    expected = hmac.new(RZP_KEY_SECRET.encode(), payload, hashlib.sha256).hexdigest()
    if not hmac.compare_digest(expected, signature):
        logger.warning("Webhook signature mismatch — rejected")
        return {"error": "Invalid webhook signature", "_http_status": 400}

    import json
    try:
        event = json.loads(payload)
    except Exception:
        return {"error": "Invalid JSON payload", "_http_status": 400}

    event_type = event.get("event", "")
    entity     = event.get("payload", {}).get("payment", {}).get("entity", {}) \
                  or event.get("payload", {}).get("subscription", {}).get("entity", {})

    logger.info(f"Razorpay webhook received: {event_type}")

    # ── 2. Route event ────────────────────────────────────────────────────────
    if event_type == "payment.captured":
        return _webhook_payment_captured(entity)

    elif event_type == "subscription.charged":
        return _webhook_subscription_charged(entity)

    elif event_type in ("subscription.cancelled", "subscription.completed"):
        return _webhook_subscription_cancelled(entity)

    else:
        # Unhandled event — acknowledge silently (don't return 4xx)
        logger.info(f"Unhandled webhook event: {event_type} — acknowledged")
        return {"status": "acknowledged", "event": event_type}


def _webhook_payment_captured(entity: dict) -> dict:
    """Handle payment.captured — upgrade user tier from notes."""
    notes    = entity.get("notes", {})
    user_id  = _safe_int(notes.get("user_id"))
    tier     = notes.get("tier", "")
    cycle    = notes.get("cycle", "monthly")
    order_id = entity.get("order_id", "")
    pay_id   = entity.get("id", "")

    if not user_id or tier not in PLAN_PRICES:
        logger.warning(f"Webhook payment.captured — bad notes: {notes}")
        return {"status": "skipped", "reason": "missing user_id or tier in notes"}

    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.row_factory = sqlite3.Row

    # Idempotency — skip if already paid
    existing = conn.execute(
        "SELECT id FROM payments WHERE razorpay_payment_id=? AND status='paid'",
        (pay_id,)
    ).fetchone()
    if existing:
        conn.close()
        logger.info(f"Webhook payment.captured duplicate — skipping {pay_id}")
        return {"status": "duplicate", "payment_id": pay_id}

    now = datetime.now(timezone.utc)
    sub_end = (now + timedelta(days=365 if cycle == "annual" else 31)).isoformat()

    # Upsert payment record
    conn.execute("""
        INSERT INTO payments
            (user_id, razorpay_order_id, razorpay_payment_id, tier,
             billing_cycle, amount_inr, amount_usd, currency, status,
             created_at, verified_at, notes)
        VALUES (?, ?, ?, ?, ?, ?, ?, 'INR', 'paid', ?, ?, 'webhook:payment.captured')
    """, (
        user_id, order_id, pay_id, tier, cycle,
        PLAN_PRICES[tier][f"{cycle}_inr"],
        PLAN_PRICES[tier][f"{cycle}_usd"],
        now.isoformat(), now.isoformat(),
    ))

    # Upgrade user
    conn.execute("""
        UPDATE users SET
            tier=?, subscription_end=?, trial_ends_at=NULL, cancel_at_period_end=0
        WHERE id=?
    """, (tier, sub_end, user_id))
    conn.commit()
    conn.close()

    logger.info(f"✅ Webhook: user {user_id} upgraded to {tier} ({cycle}) via payment.captured")
    return {"status": "ok", "user_id": user_id, "tier": tier, "subscription_end": sub_end}


def _webhook_subscription_charged(entity: dict) -> dict:
    """Handle subscription.charged — renew period for recurring subscribers."""
    notes   = entity.get("notes", {})
    user_id = _safe_int(notes.get("user_id"))
    tier    = notes.get("tier", "")
    cycle   = notes.get("cycle", "monthly")
    pay_id  = entity.get("payment_id", "")

    if not user_id:
        logger.warning(f"Webhook subscription.charged — no user_id in notes: {notes}")
        return {"status": "skipped", "reason": "missing user_id"}

    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.row_factory = sqlite3.Row
    user = conn.execute("SELECT tier, subscription_end FROM users WHERE id=?", (user_id,)).fetchone()
    if not user:
        conn.close()
        return {"status": "skipped", "reason": "user not found"}

    active_tier = tier or user["tier"]
    if active_tier not in PLAN_PRICES:
        active_tier = user["tier"]

    now = datetime.now(timezone.utc)
    # Extend from current sub_end or now (whichever is later)
    try:
        base = datetime.fromisoformat(user["subscription_end"]) if user["subscription_end"] else now
        if base < now:
            base = now
    except Exception:
        base = now

    extension = timedelta(days=365 if cycle == "annual" else 31)
    new_end = (base + extension).isoformat()

    conn.execute("UPDATE users SET subscription_end=?, cancel_at_period_end=0 WHERE id=?",
                 (new_end, user_id))
    conn.commit()
    conn.close()

    logger.info(f"✅ Webhook: user {user_id} subscription renewed to {new_end}")
    return {"status": "ok", "user_id": user_id, "subscription_end": new_end}


def _webhook_subscription_cancelled(entity: dict) -> dict:
    """Handle subscription.cancelled — mark cancel_at_period_end=1."""
    notes   = entity.get("notes", {})
    user_id = _safe_int(notes.get("user_id"))

    if not user_id:
        logger.warning(f"Webhook subscription.cancelled — no user_id: {notes}")
        return {"status": "skipped", "reason": "missing user_id"}

    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.execute("UPDATE users SET cancel_at_period_end=1 WHERE id=?", (user_id,))
    conn.commit()
    conn.close()

    logger.info(f"⚠️ Webhook: user {user_id} subscription cancelled (at period end)")
    return {"status": "ok", "user_id": user_id, "cancel_at_period_end": True}


def _safe_int(val) -> Optional[int]:
    try:
        return int(val) if val is not None else None
    except (ValueError, TypeError):
        return None
