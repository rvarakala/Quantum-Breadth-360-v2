"""
Breadth Chart Detail — Drill-in for 8 Breadth Charts cards
==========================================================
Powers the modal that opens when a user clicks any of the 8 cards
on the Breadth Charts tab. Returns:
  - 90-day time series (where applicable)
  - Deterministic stats computed from the data
  - AI-generated analysis text (Groq, per-day cached)

Cards covered:
  Time-series (6):
    1. ad_line            — A-D Line Cumulative
    2. pct_above_50       — % Above 50 DMA Trend
    3. nh_nl              — New High vs New Low
    4. qbram_score        — Q-BRAM Score History
    5. iv_footprint       — IV / PPV / Bull Snort signals per day
    6. liquidity_stress   — LSS over time
  Status (2):
    7. regime_timeline    — 90-day regime sequence + transition analysis
    8. score_gauge        — Today's 7-component breakdown of Q-BRAM score

AI cache:
  Per-day cache in breadth_chart_ai_cache table, keyed by
  (card_id, date_ist, market). Max 8 AI calls per market per day.
  Stats are recomputed every call (fast — no AI needed).
"""

import logging
import json
import sqlite3
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple

logger = logging.getLogger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))
DB_PATH = Path(__file__).parent / "breadth_data.db"

CACHE_TABLE = "breadth_chart_ai_cache"

DEFAULT_DAYS = 90

VALID_CARDS = {
    "ad_line", "pct_above_50", "nh_nl",
    "qbram_score", "iv_footprint", "liquidity_stress",
    "regime_timeline", "score_gauge",
}


# ─────────────────────────────────────────────────────────────────────────────
# Cache infrastructure (AI analysis only — stats are cheap, computed each call)
# ─────────────────────────────────────────────────────────────────────────────

def _ensure_cache_table():
    conn = sqlite3.connect(str(DB_PATH), timeout=10)
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {CACHE_TABLE} (
            card_id    TEXT NOT NULL,
            date_ist   TEXT NOT NULL,
            market     TEXT NOT NULL DEFAULT 'INDIA',
            analysis   TEXT,
            created_at TEXT,
            PRIMARY KEY (card_id, date_ist, market)
        )
    """)
    conn.commit()
    conn.close()


def _get_cached_analysis(card_id: str, market: str) -> Optional[str]:
    """Return cached AI analysis for today's (card, market), or None."""
    _ensure_cache_table()
    today = datetime.now(IST).date().isoformat()
    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=10)
        row = conn.execute(
            f"SELECT analysis FROM {CACHE_TABLE} WHERE card_id=? AND date_ist=? AND market=?",
            (card_id, today, market)
        ).fetchone()
        conn.close()
        return row[0] if row else None
    except Exception as e:
        logger.warning(f"[chart-detail] cache read failed: {e}")
        return None


def _store_analysis(card_id: str, market: str, analysis: str):
    _ensure_cache_table()
    today = datetime.now(IST).date().isoformat()
    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=10)
        conn.execute(
            f"INSERT OR REPLACE INTO {CACHE_TABLE} "
            f"(card_id, date_ist, market, analysis, created_at) VALUES (?, ?, ?, ?, ?)",
            (card_id, today, market, analysis, datetime.now(timezone.utc).isoformat())
        )
        # Opportunistic purge: drop entries older than 30 days
        cutoff = (datetime.now(IST).date() - timedelta(days=30)).isoformat()
        conn.execute(f"DELETE FROM {CACHE_TABLE} WHERE date_ist < ?", (cutoff,))
        conn.commit()
        conn.close()
    except Exception as e:
        logger.warning(f"[chart-detail] cache write failed: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Stats helpers — deterministic, no AI
# ─────────────────────────────────────────────────────────────────────────────

def _series_stats(values: List[float]) -> Dict[str, Any]:
    """Generic time-series stats: current, high/low, 20D slope, position-in-range.
    Always returns the same key set so callers can format-string safely."""
    EMPTY = {"current": None, "high": None, "low": None,
             "slope_20d": 0.0, "trend": "n/a", "position_pct": None}
    if not values:
        return dict(EMPTY)
    vals = [v for v in values if v is not None]
    if not vals:
        return dict(EMPTY)
    current = vals[-1]
    hi, lo  = max(vals), min(vals)
    # 20D slope: simple last-20 endpoint slope
    if len(vals) >= 20:
        slope_20 = (vals[-1] - vals[-20]) / 19.0
    elif len(vals) >= 5:
        slope_20 = (vals[-1] - vals[0]) / max(len(vals) - 1, 1)
    else:
        slope_20 = 0.0
    trend = "up" if slope_20 > 0.001 else ("down" if slope_20 < -0.001 else "flat")
    # Position in 90D range: 0% = at low, 100% = at high
    if hi > lo:
        position_pct = round((current - lo) / (hi - lo) * 100, 1)
    else:
        position_pct = None
    return {
        "current":      round(current, 2) if isinstance(current, (int, float)) else current,
        "high":         round(hi, 2),
        "low":          round(lo, 2),
        "slope_20d":    round(slope_20, 3),
        "trend":        trend,
        "position_pct": position_pct,
    }


def _zone_counts(values: List[float], zones: List[Tuple[str, float, float]]
                  ) -> Dict[str, int]:
    """For each (label, lo, hi), count values in [lo, hi]."""
    out = {label: 0 for label, _, _ in zones}
    for v in values:
        if v is None:
            continue
        for label, lo, hi in zones:
            if lo <= v <= hi:
                out[label] += 1
                break
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Data fetchers — pull 90D series for each card_id
# ─────────────────────────────────────────────────────────────────────────────

def _fetch_breadth_history(market: str) -> Dict[str, List]:
    """Read the in-memory breadth cache (already contains 252D history)."""
    try:
        from cache import get_cache
        payload = get_cache(f"breadth_{market.upper()}")
        if not payload:
            return {}
        return {
            "ad_history":    payload.get("ad_history", []),
            "dma_history":   payload.get("dma_history", []),
            "nh_nl_history": payload.get("nh_nl_history", []),
        }
    except Exception as e:
        logger.warning(f"[chart-detail] breadth cache read failed: {e}")
        return {}


def _fetch_score_history(market: str, days: int) -> List[Dict[str, Any]]:
    """Read qbram_score_history from SQLite."""
    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=10)
        rows = conn.execute("""
            SELECT date, score, regime, pct_above_50, nh_nl, breadth_thrust, csd
            FROM qbram_score_history
            WHERE market = ? ORDER BY date DESC LIMIT ?
        """, (market.upper(), days)).fetchall()
        conn.close()
        return [
            {"date": r[0], "score": r[1], "regime": r[2], "pct_above_50": r[3],
             "nh_nl": r[4], "breadth_thrust": r[5], "csd": r[6]}
            for r in reversed(rows)
        ]
    except Exception as e:
        logger.warning(f"[chart-detail] score history read failed: {e}")
        return []


def _fetch_iv_footprint(market: str, days: int) -> List[Dict[str, Any]]:
    """Reuse liquidity_regime.compute_iv_footprint for 90D series."""
    try:
        from liquidity_regime import compute_iv_footprint
        return compute_iv_footprint(market, days)
    except Exception as e:
        logger.warning(f"[chart-detail] iv-footprint compute failed: {e}")
        return []


def _compute_lss_series(dma_hist: List, ad_hist: List, nh_hist: List
                         ) -> List[Dict[str, Any]]:
    """Mirror the frontend LSS formula so backend can analyze it."""
    def _stress(v, lo, hi, inverse):
        """0=healthy, 100=stressed. inverse=True flips direction."""
        if v is None: return 50
        if inverse:
            # low value = stressed; high = healthy
            if v <= lo: return 100
            if v >= hi: return 0
            return round((hi - v) / (hi - lo) * 100)
        else:
            # high value = stressed; low = healthy
            if v >= hi: return 100
            if v <= lo: return 0
            return round((v - lo) / (hi - lo) * 100)

    out = []
    n = min(len(dma_hist), len(ad_hist), len(nh_hist))
    for i in range(n):
        dma = dma_hist[i]
        ad  = ad_hist[i]
        nh  = nh_hist[i]
        p50 = dma.get("pct_above_50", 50)
        adr = (ad.get("advancers", 0) / max(ad.get("decliners", 1), 1)) if ad else 1
        nl_ratio = ((nh.get("new_lows", 0) or 0) / max(nh.get("new_highs", 1) or 1, 1)) if nh else 0
        bs = _stress(p50, 15, 70, inverse=True)
        as_ = _stress(adr, 0.5, 1.5, inverse=True)
        ns = _stress(nl_ratio, 0.5, 4.0, inverse=False)
        lss = round(bs * 0.40 + as_ * 0.30 + ns * 0.30)
        out.append({"date": dma.get("date"), "lss": lss})
    return out


# ─────────────────────────────────────────────────────────────────────────────
# Per-card handlers — each returns {series, stats, prompt_inputs}
# ─────────────────────────────────────────────────────────────────────────────

def _handle_ad_line(market: str) -> Dict[str, Any]:
    hist = _fetch_breadth_history(market)
    rows = hist.get("ad_history", [])[-DEFAULT_DAYS:]
    series = [{"date": r.get("date"), "value": r.get("cumulative")} for r in rows]
    vals = [r.get("cumulative") for r in rows if r.get("cumulative") is not None]
    stats = _series_stats(vals)
    return {
        "series": series, "stats": stats,
        "prompt_inputs": (
            f"A-D LINE measures cumulative breadth (advancers - decliners summed daily). "
            f"Current: {stats['current']}. 90D range: {stats['low']} to {stats['high']}. "
            f"20-day slope: {stats['slope_20d']:.1f} per day. Trend: {stats['trend']}. "
            f"Position in 90D range: {stats['position_pct']}%."
        ),
        "question": (
            "Is cumulative breadth confirming or diverging from likely index direction? "
            "Where is the inflection point in the recent trajectory?"
        ),
    }


def _handle_pct_above_50(market: str) -> Dict[str, Any]:
    hist = _fetch_breadth_history(market)
    rows = hist.get("dma_history", [])[-DEFAULT_DAYS:]
    series = [{"date": r.get("date"), "value": r.get("pct_above_50")} for r in rows]
    vals = [r.get("pct_above_50") for r in rows if r.get("pct_above_50") is not None]
    stats = _series_stats(vals)
    zones = _zone_counts(vals, [("oversold (<40)", 0, 40),
                                  ("neutral (40-60)", 40, 60),
                                  ("strong (>60)", 60, 100)])
    stats["zones"] = zones
    return {
        "series": series, "stats": stats,
        "prompt_inputs": (
            f"% ABOVE 50 DMA is medium-term participation breadth. "
            f"Current: {stats['current']}%. 90D range: {stats['low']}% to {stats['high']}%. "
            f"Days in each zone: oversold={zones['oversold (<40)']}, "
            f"neutral={zones['neutral (40-60)']}, strong={zones['strong (>60)']}. "
            f"Trend: {stats['trend']}."
        ),
        "question": (
            "What does participation tell us — broadening, narrowing, or topping? "
            "Is the current reading consistent with a sustainable up-move?"
        ),
    }


def _handle_nh_nl(market: str) -> Dict[str, Any]:
    hist = _fetch_breadth_history(market)
    rows = hist.get("nh_nl_history", [])[-DEFAULT_DAYS:]
    series = [{"date": r.get("date"),
                "nh": r.get("new_highs"), "nl": r.get("new_lows"),
                "net": r.get("net")} for r in rows]
    nets = [r.get("net", 0) for r in rows if r.get("net") is not None]
    stats = _series_stats(nets)
    pos_days = sum(1 for n in nets if n > 0)
    neg_days = sum(1 for n in nets if n < 0)
    nh_max = max((r.get("new_highs", 0) or 0) for r in rows) if rows else 0
    nl_max = max((r.get("new_lows", 0)  or 0) for r in rows) if rows else 0
    stats["positive_days"] = pos_days
    stats["negative_days"] = neg_days
    stats["nh_max_90d"] = nh_max
    stats["nl_max_90d"] = nl_max
    return {
        "series": series, "stats": stats,
        "prompt_inputs": (
            f"NEW HIGH vs NEW LOW (52-week within 2%). Net = NH - NL. "
            f"Current net: {stats['current']}. 90D max NH: {nh_max}, max NL: {nl_max}. "
            f"Days net-positive: {pos_days}, net-negative: {neg_days}. "
            f"Trend: {stats['trend']}."
        ),
        "question": (
            "Is the new-high/new-low ratio improving or deteriorating? "
            "Is leadership confirming the broader market?"
        ),
    }


def _handle_qbram_score(market: str) -> Dict[str, Any]:
    rows = _fetch_score_history(market, DEFAULT_DAYS)
    series = [{"date": r["date"], "score": r["score"], "regime": r["regime"]} for r in rows]
    scores = [r["score"] for r in rows if r.get("score") is not None]
    stats = _series_stats(scores)
    # Regime distribution
    regime_counts: Dict[str, int] = {}
    for r in rows:
        rg = r.get("regime") or "UNK"
        regime_counts[rg] = regime_counts.get(rg, 0) + 1
    current_regime = rows[-1]["regime"] if rows else "n/a"
    # How long has current regime held?
    tenure = 0
    for r in reversed(rows):
        if r.get("regime") == current_regime:
            tenure += 1
        else:
            break
    stats["regime_counts"] = regime_counts
    stats["current_regime"] = current_regime
    stats["current_regime_tenure_days"] = tenure
    return {
        "series": series, "stats": stats,
        "prompt_inputs": (
            f"Q-BRAM SCORE is a 0-100 composite of 7 breadth signals. "
            f"Current: {stats['current']} ({current_regime}). "
            f"90D range: {stats['low']} to {stats['high']}. "
            f"Days in each regime (last 90D): {regime_counts}. "
            f"Current regime tenure: {tenure} sessions."
        ),
        "question": (
            "What does the regime trajectory tell us about market state? "
            "How fresh or mature is the current regime?"
        ),
    }


def _handle_iv_footprint(market: str) -> Dict[str, Any]:
    rows = _fetch_iv_footprint(market, DEFAULT_DAYS)
    series = rows  # already shaped as list of {date, iv_buy, ppv, bull_snort}
    # Aggregate
    iv_total  = sum((r.get("iv_buy", 0)    or 0) for r in rows)
    ppv_total = sum((r.get("ppv", 0)       or 0) for r in rows)
    bs_total  = sum((r.get("bull_snort", 0) or 0) for r in rows)
    last7 = rows[-7:] if len(rows) >= 7 else rows
    last7_total = sum((r.get("iv_buy", 0) or 0) + (r.get("ppv", 0) or 0) +
                       (r.get("bull_snort", 0) or 0) for r in last7)
    last7_avg = last7_total / max(len(last7), 1)
    avg_90 = (iv_total + ppv_total + bs_total) / max(len(rows), 1)
    direction = "rising" if last7_avg > avg_90 * 1.1 else (
                 "falling" if last7_avg < avg_90 * 0.9 else "flat")
    stats = {
        "iv_total":   iv_total, "ppv_total": ppv_total, "bs_total": bs_total,
        "total":      iv_total + ppv_total + bs_total,
        "last7_avg":  round(last7_avg, 1), "avg_90d": round(avg_90, 1),
        "direction":  direction,
    }
    return {
        "series": series, "stats": stats,
        "prompt_inputs": (
            f"IV FOOTPRINT counts smart-money signals daily: IV Buy (institutional volume), "
            f"PPV (pocket pivot volume), Bull Snort (climactic vol). "
            f"90D totals: IV={iv_total}, PPV={ppv_total}, BS={bs_total}. "
            f"Last 7-day daily average: {stats['last7_avg']}, vs 90D average: {stats['avg_90d']}. "
            f"Recent direction: {direction}."
        ),
        "question": (
            "Are smart-money signals clustering (accumulation), dispersing (distribution), "
            "or quiet (transition)? Is current activity meaningful or noise?"
        ),
    }


def _handle_liquidity_stress(market: str) -> Dict[str, Any]:
    hist = _fetch_breadth_history(market)
    dma = hist.get("dma_history", [])[-DEFAULT_DAYS:]
    ad  = hist.get("ad_history", [])[-DEFAULT_DAYS:]
    nh  = hist.get("nh_nl_history", [])[-DEFAULT_DAYS:]
    lss_series = _compute_lss_series(dma, ad, nh)
    series = lss_series
    vals = [r["lss"] for r in lss_series if r.get("lss") is not None]
    stats = _series_stats(vals)
    # Zone classification
    if stats["current"] is not None:
        cur = stats["current"]
        if   cur < 25: zone = "healthy"
        elif cur < 50: zone = "normal"
        elif cur < 75: zone = "warning"
        else:          zone = "stressed"
    else:
        zone = "n/a"
    last14 = vals[-14:] if len(vals) >= 14 else vals
    if len(last14) >= 2:
        direction = "improving" if last14[-1] < last14[0] - 5 else (
                     "deteriorating" if last14[-1] > last14[0] + 5 else "stable")
    else:
        direction = "stable"
    stats["zone"] = zone
    stats["direction_14d"] = direction
    return {
        "series": series, "stats": stats,
        "prompt_inputs": (
            f"LIQUIDITY STRESS SCORE (LSS) measures market liquidity stress: "
            f"0=healthy, 100=crisis. Current: {stats['current']} ({zone}). "
            f"90D range: {stats['low']} to {stats['high']}. "
            f"14-day direction: {direction}."
        ),
        "question": (
            "Is liquidity supportive or stressed? Is the trend improving from a "
            "recent peak or deteriorating from a recent low?"
        ),
    }


def _handle_regime_timeline(market: str) -> Dict[str, Any]:
    """90-day regime sequence + transition analysis."""
    rows = _fetch_score_history(market, DEFAULT_DAYS)
    series = [{"date": r["date"], "regime": r["regime"], "score": r["score"]} for r in rows]
    if not rows:
        return {"series": [], "stats": {}, "prompt_inputs": "No regime history available.",
                "question": "Regime data is missing — explain."}

    # Transition count
    transitions = 0
    prev = None
    for r in rows:
        rg = r.get("regime")
        if prev is not None and rg != prev:
            transitions += 1
        prev = rg
    # Longest streak per regime
    longest: Dict[str, int] = {}
    cur_streak = 0
    cur_regime = None
    for r in rows:
        rg = r.get("regime") or "UNK"
        if rg == cur_regime:
            cur_streak += 1
        else:
            if cur_regime:
                longest[cur_regime] = max(longest.get(cur_regime, 0), cur_streak)
            cur_regime = rg
            cur_streak = 1
    if cur_regime:
        longest[cur_regime] = max(longest.get(cur_regime, 0), cur_streak)
    # Current regime + tenure
    current_regime = rows[-1].get("regime", "n/a")
    tenure = 0
    for r in reversed(rows):
        if r.get("regime") == current_regime: tenure += 1
        else: break
    # Regime distribution
    counts: Dict[str, int] = {}
    for r in rows:
        rg = r.get("regime") or "UNK"
        counts[rg] = counts.get(rg, 0) + 1

    stats = {
        "days": len(rows),
        "current_regime": current_regime,
        "current_regime_tenure_days": tenure,
        "transitions": transitions,
        "longest_streaks": longest,
        "regime_counts": counts,
    }
    return {
        "series": series, "stats": stats,
        "prompt_inputs": (
            f"REGIME TIMELINE — last {len(rows)} sessions of Q-BRAM regimes. "
            f"Current regime: {current_regime} (held {tenure} sessions). "
            f"Transitions in window: {transitions}. "
            f"Days per regime: {counts}. "
            f"Longest streak per regime: {longest}."
        ),
        "question": (
            "What does the regime progression pattern over the last 90 days reveal? "
            "How durable is the current regime — fresh thrust, maturing, or fading?"
        ),
    }


def _handle_score_gauge(market: str) -> Dict[str, Any]:
    """Today's 7-component decomposition + 90D score trajectory."""
    try:
        from cache import get_cache
        live = get_cache(f"breadth_{market.upper()}") or {}
    except Exception:
        live = {}

    components = live.get("score_components", {}) or {}
    score      = live.get("score")
    regime     = live.get("regime", "n/a")

    history = _fetch_score_history(market, DEFAULT_DAYS)
    score_trend = [{"date": r["date"], "score": r["score"]} for r in history]

    # Build component summary
    comp_lines = []
    for key in ["B50", "NH_NL", "BT", "B200", "B20_ACCEL", "VOLUME", "CSD"]:
        c = components.get(key, {})
        comp_lines.append(
            f"{key}: {c.get('points', 0)}/{c.get('max', 0)} pts "
            f"(raw value: {c.get('value', 'n/a')})"
        )

    # Identify weakest + strongest components
    comp_list = []
    for key, c in components.items():
        if c.get("max"):
            ratio = c.get("points", 0) / c.get("max", 1)
            comp_list.append((key, ratio, c.get("points"), c.get("max")))
    comp_list.sort(key=lambda x: x[1])
    weakest = comp_list[:2] if comp_list else []
    strongest = comp_list[-2:] if comp_list else []

    stats = {
        "score":     score,
        "regime":    regime,
        "components": components,
        "weakest":   [{"key": w[0], "points": w[2], "max": w[3]} for w in weakest],
        "strongest": [{"key": s[0], "points": s[2], "max": s[3]} for s in strongest],
    }
    return {
        "series": score_trend, "stats": stats,
        "prompt_inputs": (
            f"Q-BRAM SCORE GAUGE — today's score: {score} ({regime}). "
            f"7-component breakdown: " + "; ".join(comp_lines) + ". "
            f"Weakest components: {[w[0] for w in weakest]}. "
            f"Strongest: {[s[0] for s in strongest]}."
        ),
        "question": (
            "Which components are driving today's Q-BRAM regime? "
            "Are any showing weakness or divergence that warrants attention?"
        ),
    }


# Dispatcher
_HANDLERS = {
    "ad_line":          _handle_ad_line,
    "pct_above_50":     _handle_pct_above_50,
    "nh_nl":            _handle_nh_nl,
    "qbram_score":      _handle_qbram_score,
    "iv_footprint":     _handle_iv_footprint,
    "liquidity_stress": _handle_liquidity_stress,
    "regime_timeline":  _handle_regime_timeline,
    "score_gauge":      _handle_score_gauge,
}


# ─────────────────────────────────────────────────────────────────────────────
# AI analysis call
# ─────────────────────────────────────────────────────────────────────────────

_SYSTEM_PROMPT = (
    "You are Q-BRAM Intelligence, an expert market breadth analyst specializing in "
    "Indian equities (NIFTY 500). You analyze quantitative breadth data and provide "
    "concise, actionable insights for a systematic swing trader. "
    "STRICT FORMAT: 3-4 sentences maximum. Plain text only — no markdown, no bullet "
    "points, no preamble. Direct, analytical, no fluff. Reference the actual numbers. "
    "If the data is incomplete or ambiguous, say so directly rather than fabricate "
    "interpretation."
)


def _generate_ai_analysis(card_id: str, prompt_inputs: str, question: str,
                          market: str) -> str:
    """Call Groq for the AI analysis. Returns text or a fallback message."""
    try:
        from ai_insights import _call_groq_with_fallback, _get_api_key
        api_key = _get_api_key()
        if not api_key:
            return "AI analysis unavailable — Groq API key not configured."

        market_context = "NIFTY 500 (Indian equities)" if market.upper() == "INDIA" else "Russell 3000 (US equities)"
        prompt = (
            f"Market context: {market_context}.\n\n"
            f"DATA:\n{prompt_inputs}\n\n"
            f"QUESTION: {question}\n\n"
            f"Provide your analysis in 3-4 sentences. Be specific and reference the numbers."
        )

        analysis = _call_groq_with_fallback(prompt, _SYSTEM_PROMPT, api_key, max_tokens=400)
        return analysis or "AI returned empty response — see chart and stats."
    except Exception as e:
        logger.warning(f"[chart-detail] AI call failed for {card_id}: {e}")
        return f"AI analysis unavailable: {str(e)[:100]} — see chart and stats."


# ─────────────────────────────────────────────────────────────────────────────
# Public entry
# ─────────────────────────────────────────────────────────────────────────────

def get_chart_detail(card_id: str, market: str = "INDIA",
                      bypass_cache: bool = False) -> Dict[str, Any]:
    """
    Main entry. Returns the full payload for one drill-in modal:
      {card_id, market, days, series, stats, ai_analysis, ai_cached, error?}
    """
    if card_id not in VALID_CARDS:
        return {"error": f"Unknown card_id: {card_id}",
                 "valid_cards": sorted(VALID_CARDS)}

    market = market.upper()
    if market not in ("INDIA", "US"):
        market = "INDIA"

    try:
        handler = _HANDLERS[card_id]
        data = handler(market)
    except Exception as e:
        logger.error(f"[chart-detail] handler {card_id} failed: {e}", exc_info=True)
        return {"error": f"Data fetch failed: {e}", "card_id": card_id, "market": market}

    # AI analysis with per-day cache
    cached = None if bypass_cache else _get_cached_analysis(card_id, market)
    if cached:
        ai_analysis = cached
        ai_cached = True
    else:
        ai_analysis = _generate_ai_analysis(
            card_id, data.get("prompt_inputs", ""), data.get("question", ""), market
        )
        # Only cache successful results (don't cache error messages — retry tomorrow)
        if ai_analysis and not ai_analysis.startswith("AI analysis unavailable"):
            _store_analysis(card_id, market, ai_analysis)
        ai_cached = False

    return {
        "card_id":      card_id,
        "market":       market,
        "days":         DEFAULT_DAYS,
        "series":       data.get("series", []),
        "stats":        data.get("stats", {}),
        "ai_analysis":  ai_analysis,
        "ai_cached":    ai_cached,
        "generated_at": datetime.now(IST).isoformat(),
    }
