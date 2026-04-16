"""
Email Service — Trial expiry notifications + transactional emails
Uses smtplib (stdlib) — no extra deps.

Config via env vars:
  SMTP_HOST       default: smtp.gmail.com
  SMTP_PORT       default: 587
  SMTP_USER       your sending email
  SMTP_PASSWORD   app password / SMTP password
  EMAIL_FROM_NAME default: Quantum Breadth 360
"""

import smtplib, ssl, logging, os, sqlite3
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime, timezone, timedelta
from pathlib import Path

logger = logging.getLogger(__name__)
DB_PATH = Path(__file__).parent / "breadth_data.db"

SMTP_HOST     = os.environ.get("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT     = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USER     = os.environ.get("SMTP_USER", "")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "")
FROM_NAME     = os.environ.get("EMAIL_FROM_NAME", "Quantum Breadth 360")
FROM_ADDR     = SMTP_USER
APP_URL       = os.environ.get("APP_URL", "https://quantumtradeledger.com")

EMAIL_ENABLED = bool(SMTP_USER and SMTP_PASSWORD)


# ── Low-level sender ──────────────────────────────────────────────────────────

def send_email(to_email: str, subject: str, html_body: str, text_body: str = "") -> bool:
    """Send a single email. Returns True on success."""
    if not EMAIL_ENABLED:
        logger.info(f"[EMAIL DISABLED] Would send '{subject}' to {to_email}")
        return False

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = f"{FROM_NAME} <{FROM_ADDR}>"
    msg["To"]      = to_email

    if text_body:
        msg.attach(MIMEText(text_body, "plain"))
    msg.attach(MIMEText(html_body, "html"))

    try:
        ctx = ssl.create_default_context()
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            server.ehlo()
            server.starttls(context=ctx)
            server.login(SMTP_USER, SMTP_PASSWORD)
            server.sendmail(FROM_ADDR, to_email, msg.as_string())
        logger.info(f"✅ Email sent: '{subject}' → {to_email}")
        return True
    except Exception as e:
        logger.error(f"❌ Email failed to {to_email}: {e}")
        return False


# ── Email templates ───────────────────────────────────────────────────────────

def _base_template(content: str, preheader: str = "") -> str:
    """Wrap content in a clean dark-themed HTML email shell."""
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width,initial-scale=1">
<title>Quantum Breadth 360</title>
</head>
<body style="margin:0;padding:0;background:#060a14;font-family:'Helvetica Neue',Arial,sans-serif">
  {"<div style='display:none;max-height:0;overflow:hidden'>" + preheader + "</div>" if preheader else ""}
  <table width="100%" cellpadding="0" cellspacing="0" style="background:#060a14;padding:40px 0">
    <tr><td align="center">
      <table width="560" cellpadding="0" cellspacing="0" style="max-width:560px;width:100%">

        <!-- Header -->
        <tr><td style="padding:0 0 24px 0;text-align:center">
          <span style="font-size:20px;font-weight:800;color:#e2e8f0;
            font-family:'Helvetica Neue',Arial,sans-serif;letter-spacing:-.02em">
            ⚛ <span style="color:#06b6d4">Quantum</span> Breadth 360
          </span>
        </td></tr>

        <!-- Card -->
        <tr><td style="background:rgba(255,255,255,0.04);border:1px solid rgba(255,255,255,0.08);
          border-radius:16px;padding:32px">
          {content}
        </td></tr>

        <!-- Footer -->
        <tr><td style="padding:24px 0 0 0;text-align:center;
          font-size:11px;color:#475569;font-family:monospace;line-height:1.8">
          You're receiving this because you have an account at
          <a href="{APP_URL}" style="color:#06b6d4;text-decoration:none">quantumtradeledger.com</a><br>
          © {datetime.now().year} Quantum Breadth 360. All rights reserved.
        </td></tr>

      </table>
    </td></tr>
  </table>
</body>
</html>"""


def trial_expiry_email(name: str, days_left: int, email: str) -> tuple[str, str, str]:
    """Returns (subject, html, text) for trial expiry warning."""
    urgency_color = "#ef4444" if days_left <= 1 else "#f59e0b"
    days_label = "1 day" if days_left == 1 else f"{days_left} days"

    subject = (
        f"⚠️ Your Pro trial ends tomorrow — keep full access"
        if days_left == 1 else
        f"Your Pro trial ends in {days_label} — upgrade to keep access"
    )

    content = f"""
    <h1 style="margin:0 0 8px;font-size:22px;font-weight:800;color:#e2e8f0">
      Your trial ends in <span style="color:{urgency_color}">{days_label}</span>
    </h1>
    <p style="margin:0 0 20px;font-size:14px;color:#94a3b8;line-height:1.7">
      Hi {name},<br><br>
      Your <b style="color:#a855f7">14-day Pro trial</b> of Quantum Breadth 360 expires in
      <b style="color:{urgency_color}">{days_label}</b>. After that you'll be moved to the
      free Explorer plan and lose access to:
    </p>

    <table width="100%" cellpadding="0" cellspacing="0" style="margin-bottom:24px">
      {"".join(f'''
      <tr><td style="padding:6px 0;border-bottom:1px solid rgba(255,255,255,.05)">
        <span style="color:#ef4444;margin-right:8px">✗</span>
        <span style="font-size:13px;color:#94a3b8">{feat}</span>
      </td></tr>''' for feat in [
          "Smart Money signals &amp; Alpha Composite Score",
          "F-Value fundamental screener",
          "Insider Trading tracker",
          "FII/DII Institutional Money Matrix",
          "SMART Techno-Fundamental Screener",
          "Trading Journal &amp; Watchlists",
      ])}
    </table>

    <a href="{APP_URL}/app#billing"
      style="display:block;text-align:center;padding:14px 28px;border-radius:10px;
      background:#6366f1;color:#fff;font-size:15px;font-weight:700;
      text-decoration:none;margin-bottom:16px">
      Upgrade Now — Keep Full Access →
    </a>

    <p style="margin:0;font-size:11px;color:#475569;text-align:center;font-family:monospace">
      Trader $29/mo · Pro $79/mo · Elite $149/mo<br>
      Questions? Reply to this email or contact
      <a href="mailto:support@quantumtrade.pro" style="color:#06b6d4">support@quantumtrade.pro</a>
    </p>"""

    text = (f"Hi {name},\n\nYour Pro trial ends in {days_label}.\n\n"
            f"Upgrade at {APP_URL}/app#billing to keep full access.\n\n"
            f"— Quantum Breadth 360 Team")

    return subject, _base_template(content, preheader=f"Your trial ends in {days_label}"), text


def payment_confirmation_email(name: str, tier: str, cycle: str,
                                amount_inr: int, sub_end: str) -> tuple[str, str, str]:
    """Returns (subject, html, text) for payment confirmation."""
    subject = f"✅ Payment confirmed — Welcome to {tier.title()} plan"
    try:
        end_fmt = datetime.fromisoformat(sub_end).strftime("%B %d, %Y")
    except Exception:
        end_fmt = sub_end

    content = f"""
    <h1 style="margin:0 0 8px;font-size:22px;font-weight:800;color:#e2e8f0">
      Payment confirmed! 🎉
    </h1>
    <p style="margin:0 0 20px;font-size:14px;color:#94a3b8;line-height:1.7">
      Hi {name}, your <b style="color:#a855f7">{tier.title()} ({cycle})</b>
      subscription is now active. Your next renewal date is
      <b style="color:#06b6d4">{end_fmt}</b>.
    </p>
    <div style="background:rgba(99,102,241,.08);border:1px solid rgba(99,102,241,.2);
      border-radius:10px;padding:16px;margin-bottom:24px;font-family:monospace;font-size:13px">
      <div style="color:#94a3b8">Plan: <span style="color:#e2e8f0">{tier.title()} ({cycle})</span></div>
      <div style="color:#94a3b8;margin-top:6px">Amount: <span style="color:#e2e8f0">₹{amount_inr:,}</span></div>
      <div style="color:#94a3b8;margin-top:6px">Active until: <span style="color:#06b6d4">{end_fmt}</span></div>
    </div>
    <a href="{APP_URL}/app"
      style="display:block;text-align:center;padding:14px 28px;border-radius:10px;
      background:#06b6d4;color:#060a14;font-size:15px;font-weight:700;text-decoration:none">
      Open Dashboard →
    </a>"""

    text = (f"Hi {name},\n\nPayment confirmed! {tier.title()} ({cycle}) plan active until {end_fmt}.\n"
            f"Amount: ₹{amount_inr:,}\n\nOpen dashboard: {APP_URL}/app")

    return subject, _base_template(content), text


# ── Trial expiry scheduler ────────────────────────────────────────────────────

def _get_email_sent_key(user_id: int, days: int) -> str:
    return f"trial_warn_{days}d_{user_id}"


def run_trial_expiry_check():
    """
    Check all users whose trial expires in exactly 3 or 1 days.
    Send warning email if not already sent.
    Call this daily via scheduler (e.g. APScheduler or Windows Task Scheduler).
    """
    if not EMAIL_ENABLED:
        logger.info("Email not configured — skipping trial expiry check")
        return {"checked": 0, "sent": 0, "skipped_no_email": True}

    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.row_factory = sqlite3.Row

    now = datetime.now(timezone.utc)
    sent_count = 0
    checked = 0

    # Find explorer users with active trial
    rows = conn.execute("""
        SELECT id, email, name, trial_ends_at
        FROM users
        WHERE tier = 'explorer'
          AND trial_ends_at IS NOT NULL
          AND status = 'active'
    """).fetchall()

    for row in rows:
        checked += 1
        try:
            trial_dt = datetime.fromisoformat(row["trial_ends_at"])
            if trial_dt.tzinfo is None:
                trial_dt = trial_dt.replace(tzinfo=timezone.utc)
            days_left = (trial_dt - now).days

            for warn_days in (3, 1):
                if days_left == warn_days:
                    # Check if already sent using a sent_emails table
                    already = conn.execute("""
                        SELECT 1 FROM sent_emails
                        WHERE user_id=? AND email_key=?
                    """, (row["id"], _get_email_sent_key(row["id"], warn_days))).fetchone()

                    if not already:
                        subj, html, text = trial_expiry_email(
                            row["name"] or row["email"].split("@")[0],
                            warn_days,
                            row["email"],
                        )
                        ok = send_email(row["email"], subj, html, text)
                        if ok:
                            conn.execute("""
                                INSERT OR IGNORE INTO sent_emails (user_id, email_key, sent_at)
                                VALUES (?, ?, ?)
                            """, (row["id"], _get_email_sent_key(row["id"], warn_days),
                                  now.isoformat()))
                            conn.commit()
                            sent_count += 1
        except Exception as e:
            logger.warning(f"Trial check error for user {row['id']}: {e}")

    conn.close()
    logger.info(f"Trial expiry check: {checked} users checked, {sent_count} emails sent")
    return {"checked": checked, "sent": sent_count}


def ensure_email_tables():
    """Create sent_emails tracking table."""
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.execute("""
        CREATE TABLE IF NOT EXISTS sent_emails (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            user_id INTEGER NOT NULL,
            email_key TEXT NOT NULL,
            sent_at TEXT NOT NULL,
            UNIQUE(user_id, email_key)
        )
    """)
    conn.commit()
    conn.close()
