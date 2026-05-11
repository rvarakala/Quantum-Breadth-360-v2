"""
Growth Leaders — Overview Row 3 Card 4

Surfaces NIFTY 500 stocks where BOTH revenue (sales) and net profit grew
≥50% YoY in the most recently reported quarter. Strict AND filter, no
toggles — the card is a glanceable "high-quality fundamentals leaders"
list, not a full screener (use Smart Screener for that).

Reads from tv_fundamentals_detail (JSON blobs populated by the TV/yfinance
batch sync). For each ticker we look at the quarterly array oldest→newest
and pair index [-1] (latest quarter) vs [-5] (same quarter prior year).

Why YoY not QoQ:
  YoY eliminates seasonality (TCS Q3 vs Q4 always varies on billing cycles).
  Standard for IBD/Minervini/O'Neil growth-stock screening. Not a toggle —
  this card is opinionated.

Why AND not OR:
  A stock with 80% revenue growth and -20% profit growth is a margin-
  compression story (bad signal). 60% + 60% is operating leverage (good).
  Requiring both filters out the bad pattern.

Why exclude negative-base cases:
  Companies emerging from losses can show absurd growth percentages
  (-10 cr → +50 cr is "+600%"). Those are turnaround stories, valuable in
  their own context, but they don't belong on a "compounders" list.

Cache: 12-hour SQLite cache (growth_leaders_cache table). Quarterly data
only changes when companies report — no need for short TTL.
"""
import logging
import json
import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional

logger = logging.getLogger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))
DB_PATH = Path(__file__).parent / "breadth_data.db"

CACHE_TABLE = "growth_leaders_cache"
CACHE_TTL_HOURS = 12

# Growth thresholds (your spec: ≥50% on both metrics)
MIN_SALES_GROWTH_PCT = 50.0
MIN_PROFIT_GROWTH_PCT = 50.0

# Freshness guard: reject tickers whose latest reported quarter is older
# than this. Stale quarterlies usually mean the company has issues
# (delisted, suspended, late filer).
MAX_QUARTER_STALENESS_DAYS = 180

# Lazy-loaded NIFTY 500 universe
_universe_cache: Optional[set] = None


# ─────────────────────────────────────────────────────────────────────────────
# Cache infrastructure
# ─────────────────────────────────────────────────────────────────────────────

def _ensure_cache_table():
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {CACHE_TABLE} (
            cache_key  TEXT PRIMARY KEY,
            payload    TEXT,
            fetched_at TEXT
        )
    """)
    conn.commit()
    conn.close()


def _get_cached(key: str) -> Optional[Dict[str, Any]]:
    _ensure_cache_table()
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    row = conn.execute(
        f"SELECT payload, fetched_at FROM {CACHE_TABLE} WHERE cache_key=?",
        (key,)
    ).fetchone()
    conn.close()
    if not row:
        return None
    try:
        fetched = datetime.fromisoformat(row[1])
        age_hr = (datetime.now(timezone.utc) - fetched).total_seconds() / 3600
        if age_hr >= CACHE_TTL_HOURS:
            return None
        payload = json.loads(row[0])
        payload["cache_age_hr"] = round(age_hr, 2)
        return payload
    except Exception as e:
        logger.warning(f"[growth] cache parse failed: {e}")
        return None


def _set_cached(key: str, payload: Dict[str, Any]):
    _ensure_cache_table()
    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=10)
        conn.execute(
            f"INSERT OR REPLACE INTO {CACHE_TABLE} (cache_key, payload, fetched_at) VALUES (?, ?, ?)",
            (key, json.dumps(payload), datetime.now(timezone.utc).isoformat())
        )
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"[growth] cache write failed: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Universe + per-ticker fundamentals access
# ─────────────────────────────────────────────────────────────────────────────

def _load_universe() -> set:
    global _universe_cache
    if _universe_cache is not None:
        return _universe_cache
    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=10)
        rows = conn.execute(
            "SELECT ticker FROM ticker_universe WHERE market='India'"
        ).fetchall()
        conn.close()
        _universe_cache = {r[0].upper().replace(".NS", "") for r in rows if r[0]}
        logger.info(f"[growth] universe loaded: {len(_universe_cache)} NSE tickers")
    except Exception as e:
        logger.warning(f"[growth] universe load failed: {e}")
        _universe_cache = set()
    return _universe_cache


def _load_company_meta(tickers: List[str]) -> Dict[str, Dict[str, str]]:
    """Fetch company_name + sector for a batch of tickers from tv_fundamentals."""
    if not tickers:
        return {}
    out = {}
    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=10)
        placeholders = ",".join(["?"] * len(tickers))
        rows = conn.execute(
            f"SELECT ticker, company_name, sector FROM tv_fundamentals "
            f"WHERE ticker IN ({placeholders})",
            tickers
        ).fetchall()
        conn.close()
        for ticker, name, sector in rows:
            out[ticker] = {"company_name": name or ticker, "sector": sector or ""}
    except Exception as e:
        logger.warning(f"[growth] company meta load failed: {e}")
    return out


def _safe_growth_pct(latest: Any, prior: Any) -> Optional[float]:
    """
    YoY growth % with strict guards:
      - Both values must be present and numeric
      - Prior (base) must be POSITIVE — exclude turnaround/loss-emergence cases
        that produce misleading triple-digit growth on tiny/negative bases
      - Latest must be numeric (can be negative — that signals deterioration
        from a profitable base, which we should not screen IN, but the
        threshold check handles that naturally since it requires ≥50%)
    Returns None if computable conditions not met.
    """
    try:
        if latest is None or prior is None:
            return None
        l = float(latest)
        p = float(prior)
        if p <= 0:
            return None     # negative or zero base → not a meaningful growth %
        return ((l - p) / p) * 100.0
    except (TypeError, ValueError):
        return None


def _quarter_is_fresh(period_str: str) -> bool:
    """True if the quarter-end date is within MAX_QUARTER_STALENESS_DAYS of today."""
    if not period_str:
        return False
    # period_str is typically 'YYYY-MM-DD' (yfinance format) — sometimes
    # shorter. Try to parse the first 10 chars as ISO date.
    try:
        dt = datetime.strptime(period_str[:10], "%Y-%m-%d").date()
        age_days = (datetime.now(IST).date() - dt).days
        return 0 <= age_days <= MAX_QUARTER_STALENESS_DAYS
    except ValueError:
        return False


# ─────────────────────────────────────────────────────────────────────────────
# Core screen
# ─────────────────────────────────────────────────────────────────────────────

def _screen_ticker(ticker: str, blob_json: str) -> Optional[Dict[str, Any]]:
    """
    Apply the growth screen to a single ticker's quarterly data.
    Returns row dict if it qualifies, None otherwise. Stays silent on
    skip — there will be ~500 tickers and most will fail.
    """
    try:
        blob = json.loads(blob_json) if isinstance(blob_json, str) else blob_json
    except (ValueError, TypeError):
        return None

    quarterly = blob.get("quarterly", [])
    # Need at least 5 quarters (latest + 4 quarters back for YoY)
    if len(quarterly) < 5:
        return None

    latest = quarterly[-1]
    yoy    = quarterly[-5]

    period = latest.get("period", "")
    if not _quarter_is_fresh(period):
        return None

    sales_growth  = _safe_growth_pct(latest.get("sales"),      yoy.get("sales"))
    profit_growth = _safe_growth_pct(latest.get("net_profit"), yoy.get("net_profit"))

    if sales_growth is None or profit_growth is None:
        return None

    # Strict AND filter on both metrics
    if sales_growth < MIN_SALES_GROWTH_PCT or profit_growth < MIN_PROFIT_GROWTH_PCT:
        return None

    composite = (sales_growth + profit_growth) / 2.0

    return {
        "ticker":        ticker,
        "company":       blob.get("company_name", ticker),
        "sales_growth":  round(sales_growth, 1),
        "profit_growth": round(profit_growth, 1),
        "composite":     round(composite, 1),
        "quarter":       period,
    }


def compute_growth_leaders() -> Dict[str, Any]:
    """
    Main entry. Scans tv_fundamentals_detail for all NIFTY 500 tickers,
    applies the growth screen, returns sorted leader list.

    Honest behavior under bad inputs:
      - If tv_fundamentals_detail is empty → returns count=0 with a
        diagnostic message naming what's wrong, NOT a confusing empty card.
      - If no ticker qualifies → returns count=0 with a neutral message
        (genuinely possible in a low-growth period).
    """
    today_ist = datetime.now(IST).date()
    cache_key = f"growth_leaders_{today_ist.isoformat()}"

    cached = _get_cached(cache_key)
    if cached:
        return cached

    universe = _load_universe()
    if not universe:
        msg = "ticker_universe table is empty — run startup pipeline first"
        logger.warning(f"[growth] {msg}")
        payload = {"as_of": today_ist.isoformat(), "leaders": [],
                   "count": 0, "diagnostic": msg, "fetched_at": datetime.now(IST).isoformat()}
        _set_cached(cache_key, payload)
        return payload

    # Pull all detail blobs in one query — far faster than per-ticker reads
    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=10)
        rows = conn.execute(
            "SELECT ticker, data FROM tv_fundamentals_detail"
        ).fetchall()
        conn.close()
    except Exception as e:
        msg = f"tv_fundamentals_detail read failed: {e}"
        logger.error(f"[growth] {msg}")
        return {"as_of": today_ist.isoformat(), "leaders": [],
                "count": 0, "diagnostic": msg, "fetched_at": datetime.now(IST).isoformat()}

    if not rows:
        msg = "tv_fundamentals_detail is empty — run TV fundamentals sync"
        logger.warning(f"[growth] {msg}")
        payload = {"as_of": today_ist.isoformat(), "leaders": [],
                   "count": 0, "diagnostic": msg, "fetched_at": datetime.now(IST).isoformat()}
        _set_cached(cache_key, payload)
        return payload

    # Filter to universe + apply screen
    leaders = []
    skipped_stale = 0
    skipped_insufficient = 0
    skipped_neg_base = 0
    skipped_threshold = 0
    examined = 0
    for ticker, blob_json in rows:
        ticker = (ticker or "").upper().replace(".NS", "")
        if ticker not in universe:
            continue
        examined += 1
        try:
            blob = json.loads(blob_json) if isinstance(blob_json, str) else blob_json
        except Exception:
            continue
        quarterly = blob.get("quarterly", [])
        if len(quarterly) < 5:
            skipped_insufficient += 1
            continue
        latest, yoy = quarterly[-1], quarterly[-5]
        period = latest.get("period", "")
        if not _quarter_is_fresh(period):
            skipped_stale += 1
            continue
        sg = _safe_growth_pct(latest.get("sales"),      yoy.get("sales"))
        pg = _safe_growth_pct(latest.get("net_profit"), yoy.get("net_profit"))
        if sg is None or pg is None:
            skipped_neg_base += 1
            continue
        if sg < MIN_SALES_GROWTH_PCT or pg < MIN_PROFIT_GROWTH_PCT:
            skipped_threshold += 1
            continue
        composite = (sg + pg) / 2.0
        leaders.append({
            "ticker":        ticker,
            "company":       blob.get("company_name", ticker),
            "sales_growth":  round(sg, 1),
            "profit_growth": round(pg, 1),
            "composite":     round(composite, 1),
            "quarter":       period,
        })

    # Enrich with sector from tv_fundamentals (single batch query)
    if leaders:
        meta = _load_company_meta([l["ticker"] for l in leaders])
        for l in leaders:
            m = meta.get(l["ticker"], {})
            # Prefer detail-blob name (longer/cleaner) but fall back to batch
            if not l.get("company") or l["company"] == l["ticker"]:
                l["company"] = m.get("company_name", l["ticker"])
            l["sector"] = m.get("sector", "")

    # Sort by composite growth descending
    leaders.sort(key=lambda x: -x["composite"])

    logger.info(
        f"[growth] examined={examined} qualified={len(leaders)} "
        f"(skipped: stale={skipped_stale}, "
        f"insufficient_quarters={skipped_insufficient}, "
        f"neg_or_zero_base={skipped_neg_base}, "
        f"below_threshold={skipped_threshold})"
    )

    payload = {
        "as_of":         today_ist.isoformat(),
        "leaders":       leaders,
        "count":         len(leaders),
        "examined":      examined,
        "thresholds":    {
            "sales":  MIN_SALES_GROWTH_PCT,
            "profit": MIN_PROFIT_GROWTH_PCT,
            "basis":  "YoY",
        },
        "fetched_at":    datetime.now(IST).isoformat(),
        "cache_age_hr":  0,
    }
    _set_cached(cache_key, payload)
    return payload
