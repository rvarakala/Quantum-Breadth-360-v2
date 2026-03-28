"""
F-Value: Fundamental Value Screener
Quality grades (A through E) + Fair Value estimation using TV fundamentals data.

Grade is a composite of:
  - Profitability (ROE, margins)
  - Growth (EPS growth, revenue growth)
  - Balance sheet health (D/E, current ratio)
  - Valuation efficiency (PE, PB)

Fair Value uses an earnings-power model:
  FV = EPS_TTM * justified_PE
  justified_PE = f(ROE, growth, debt_level)
"""

import sqlite3, logging, math
from pathlib import Path
from datetime import datetime

logger = logging.getLogger(__name__)
DB_PATH = Path(__file__).parent / "breadth_data.db"


# ═══════════════════════════════════════════════════════════════════════════════
# QUALITY GRADE (A → E)
# ═══════════════════════════════════════════════════════════════════════════════

def _grade_letter(score: float) -> str:
    """Convert numeric score (0-100) to letter grade."""
    if score >= 85:  return "A"
    if score >= 70:  return "B+"
    if score >= 55:  return "B"
    if score >= 40:  return "C"
    if score >= 25:  return "D"
    return "E"


def _grade_color(grade: str) -> str:
    return {
        "A":  "#22c55e",
        "B+": "#4ade80",
        "B":  "#60a5fa",
        "C":  "#f59e0b",
        "D":  "#ef4444",
        "E":  "#7f1d1d",
    }.get(grade, "#64748b")


def compute_quality_grade(row: dict) -> dict:
    """
    Compute quality grade from fundamental metrics.
    row = dict with keys: pe_ratio, pb_ratio, roe, roa, gross_margin,
          operating_margin, net_margin, debt_to_equity, current_ratio,
          eps_ttm, eps_growth_ttm, revenue_growth, market_cap
    Returns: { score: 0-100, grade: 'A'-'E', components: {...} }
    """
    components = {}
    total = 0
    max_total = 0

    def _s(val):
        """Safe float."""
        if val is None: return None
        try:
            f = float(val)
            return None if (math.isnan(f) or math.isinf(f)) else f
        except: return None

    # ── 1. ROE (max 20 pts) ────────────────────────────────────────────────
    roe = _s(row.get("roe"))
    if roe is not None:
        max_total += 20
        if   roe >= 25: pts = 20
        elif roe >= 18: pts = 16
        elif roe >= 12: pts = 12
        elif roe >= 8:  pts = 8
        elif roe >= 3:  pts = 4
        else:           pts = 0
        total += pts
        components["ROE"] = {"value": round(roe, 1), "points": pts, "max": 20}

    # ── 2. Operating Margin (max 15 pts) ───────────────────────────────────
    opm = _s(row.get("operating_margin"))
    if opm is not None:
        max_total += 15
        if   opm >= 25: pts = 15
        elif opm >= 18: pts = 12
        elif opm >= 12: pts = 9
        elif opm >= 6:  pts = 6
        elif opm >= 0:  pts = 3
        else:           pts = 0
        total += pts
        components["OPM"] = {"value": round(opm, 1), "points": pts, "max": 15}

    # ── 3. Net Margin (max 10 pts) ─────────────────────────────────────────
    npm = _s(row.get("net_margin"))
    if npm is not None:
        max_total += 10
        if   npm >= 20: pts = 10
        elif npm >= 12: pts = 8
        elif npm >= 7:  pts = 6
        elif npm >= 3:  pts = 4
        elif npm >= 0:  pts = 2
        else:           pts = 0
        total += pts
        components["NPM"] = {"value": round(npm, 1), "points": pts, "max": 10}

    # ── 4. EPS Growth (max 20 pts) ─────────────────────────────────────────
    eg = _s(row.get("eps_growth_ttm"))
    if eg is not None:
        max_total += 20
        if   eg >= 40: pts = 20
        elif eg >= 25: pts = 16
        elif eg >= 15: pts = 12
        elif eg >= 8:  pts = 8
        elif eg >= 0:  pts = 4
        else:          pts = 0
        total += pts
        components["EPS_G"] = {"value": round(eg, 1), "points": pts, "max": 20}

    # ── 5. Revenue Growth (max 10 pts) ─────────────────────────────────────
    rg = _s(row.get("revenue_growth"))
    if rg is not None:
        max_total += 10
        if   rg >= 25: pts = 10
        elif rg >= 15: pts = 8
        elif rg >= 8:  pts = 6
        elif rg >= 3:  pts = 4
        elif rg >= 0:  pts = 2
        else:          pts = 0
        total += pts
        components["REV_G"] = {"value": round(rg, 1), "points": pts, "max": 10}

    # ── 6. Debt / Equity — INVERSE (max 15 pts) ───────────────────────────
    de = _s(row.get("debt_to_equity"))
    if de is not None:
        max_total += 15
        if   de <= 0.1: pts = 15
        elif de <= 0.3: pts = 12
        elif de <= 0.5: pts = 9
        elif de <= 1.0: pts = 6
        elif de <= 1.5: pts = 3
        else:           pts = 0
        total += pts
        components["D/E"] = {"value": round(de, 2), "points": pts, "max": 15}

    # ── 7. Current Ratio (max 10 pts) ──────────────────────────────────────
    cr = _s(row.get("current_ratio"))
    if cr is not None:
        max_total += 10
        if   cr >= 2.5: pts = 10
        elif cr >= 1.8: pts = 8
        elif cr >= 1.3: pts = 6
        elif cr >= 1.0: pts = 4
        elif cr >= 0.8: pts = 2
        else:           pts = 0
        total += pts
        components["CR"] = {"value": round(cr, 2), "points": pts, "max": 10}

    # Normalize to 0-100 scale
    score = round((total / max_total) * 100) if max_total > 0 else 0
    grade = _grade_letter(score)

    return {
        "score": score,
        "grade": grade,
        "grade_color": _grade_color(grade),
        "components": components,
        "max_possible": max_total,
        "points_earned": total,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# FAIR VALUE ESTIMATION
# ═══════════════════════════════════════════════════════════════════════════════

def compute_fair_value(row: dict) -> dict:
    """
    Estimate fair value using earnings-power model.

    Method: Justified PE approach
    - Base PE: Nifty 500 long-run avg PE ~22
    - Adjust up for: high ROE, high growth, low debt
    - Adjust down for: low ROE, negative growth, high debt
    - FV = EPS_TTM * justified_PE

    Also computes PEG ratio and Earnings Yield for cross-checks.
    """
    def _s(val):
        if val is None: return None
        try:
            f = float(val)
            return None if (math.isnan(f) or math.isinf(f)) else f
        except: return None

    eps = _s(row.get("eps_ttm"))
    pe = _s(row.get("pe_ratio"))
    roe = _s(row.get("roe"))
    eg = _s(row.get("eps_growth_ttm"))
    de = _s(row.get("debt_to_equity"))
    price = _s(row.get("close")) or _s(row.get("price"))

    # Need at least EPS and price
    if not eps or eps <= 0 or not price or price <= 0:
        return {
            "fair_value": None,
            "upside_pct": None,
            "fv_status": "N/A",
            "fv_status_color": "#64748b",
            "justified_pe": None,
            "peg_ratio": None,
            "earnings_yield": None,
        }

    # ── Justified PE Calculation ──────────────────────────────────────────
    base_pe = 22.0  # Nifty 500 long-run average

    # ROE adjustment: +/-5 PE
    roe_adj = 0
    if roe is not None:
        if   roe >= 25: roe_adj = 5
        elif roe >= 18: roe_adj = 3
        elif roe >= 12: roe_adj = 1
        elif roe >= 5:  roe_adj = 0
        elif roe >= 0:  roe_adj = -3
        else:           roe_adj = -6

    # Growth adjustment: +/-6 PE
    growth_adj = 0
    if eg is not None:
        if   eg >= 30: growth_adj = 6
        elif eg >= 20: growth_adj = 4
        elif eg >= 12: growth_adj = 2
        elif eg >= 5:  growth_adj = 0
        elif eg >= 0:  growth_adj = -2
        elif eg >= -10: growth_adj = -4
        else:           growth_adj = -6

    # Debt adjustment: +/-3 PE
    debt_adj = 0
    if de is not None:
        if   de <= 0.2: debt_adj = 3
        elif de <= 0.5: debt_adj = 1
        elif de <= 1.0: debt_adj = 0
        elif de <= 2.0: debt_adj = -2
        else:           debt_adj = -4

    justified_pe = max(5, min(45, base_pe + roe_adj + growth_adj + debt_adj))
    fair_value = round(eps * justified_pe, 2)

    # Upside
    upside_pct = round(((fair_value - price) / price) * 100, 1)

    # FV Status
    if   upside_pct >= 50:  fv_status = "DEEP VALUE";    fv_color = "#22c55e"
    elif upside_pct >= 20:  fv_status = "UNDERVALUED";   fv_color = "#4ade80"
    elif upside_pct >= -5:  fv_status = "FAIR";          fv_color = "#f59e0b"
    elif upside_pct >= -20: fv_status = "FULLY PRICED";  fv_color = "#fb923c"
    else:                   fv_status = "OVERVALUED";     fv_color = "#ef4444"

    # PEG ratio
    peg = None
    if pe and pe > 0 and eg and eg > 0:
        peg = round(pe / eg, 2)

    # Earnings yield
    ey = round((eps / price) * 100, 2) if price > 0 else None

    return {
        "fair_value": fair_value,
        "upside_pct": upside_pct,
        "fv_status": fv_status,
        "fv_status_color": fv_color,
        "justified_pe": round(justified_pe, 1),
        "peg_ratio": peg,
        "earnings_yield": ey,
    }


# ═══════════════════════════════════════════════════════════════════════════════
# MAIN SCREENER — Combines Grade + Fair Value for all stocks
# ═══════════════════════════════════════════════════════════════════════════════

def run_fvalue_screener(
    min_grade: str = None,
    fv_filter: str = None,
    sector: str = None,
    limit: int = 500,
) -> dict:
    """
    Run F-Value screener across all stocks with TV fundamentals.

    Returns ranked list with grade, fair value, and upside%.
    Filters:
      min_grade: "A", "B+", "B", "C", "D" — show only this grade or better
      fv_filter: "deep_value", "undervalued", "fair" — fair value status filter
      sector: filter by sector
    """
    conn = sqlite3.connect(str(DB_PATH), timeout=15)
    conn.row_factory = sqlite3.Row

    rows = conn.execute("""
        SELECT f.ticker, f.pe_ratio, f.pb_ratio, f.roe, f.roa,
               f.gross_margin, f.operating_margin, f.net_margin,
               f.debt_to_equity, f.current_ratio,
               f.eps_ttm, f.eps_growth_ttm, f.revenue_growth,
               f.market_cap, f.company_name, f.sector, f.industry,
               o.close as price
        FROM tv_fundamentals f
        LEFT JOIN (
            SELECT ticker, close FROM ohlcv
            WHERE (ticker, date) IN (
                SELECT ticker, MAX(date) FROM ohlcv WHERE market='India' GROUP BY ticker
            )
        ) o ON f.ticker = REPLACE(o.ticker, '.NS', '')
        WHERE f.eps_ttm IS NOT NULL AND f.eps_ttm > 0
        ORDER BY f.roe DESC
    """).fetchall()
    conn.close()

    # Grade ordering for filter comparison
    grade_order = {"A": 5, "B+": 4, "B": 3, "C": 2, "D": 1, "E": 0}
    min_grade_num = grade_order.get(min_grade, 0) if min_grade else 0

    fv_status_map = {
        "deep_value": "DEEP VALUE",
        "undervalued": "UNDERVALUED",
        "fair": "FAIR",
        "fully_priced": "FULLY PRICED",
        "overvalued": "OVERVALUED",
    }

    stocks = []
    grade_counts = {"A": 0, "B+": 0, "B": 0, "C": 0, "D": 0, "E": 0}
    fv_counts = {"DEEP VALUE": 0, "UNDERVALUED": 0, "FAIR": 0, "FULLY PRICED": 0, "OVERVALUED": 0, "N/A": 0}

    for r in rows:
        row = dict(r)
        # Use OHLCV price, fallback to nothing
        if not row.get("price"):
            continue

        grade_info = compute_quality_grade(row)
        fv_info = compute_fair_value({**row, "close": row["price"]})

        g = grade_info["grade"]
        fvs = fv_info["fv_status"]

        grade_counts[g] = grade_counts.get(g, 0) + 1
        fv_counts[fvs] = fv_counts.get(fvs, 0) + 1

        # Apply filters
        if min_grade and grade_order.get(g, 0) < min_grade_num:
            continue
        if fv_filter and fvs != fv_status_map.get(fv_filter, ""):
            continue
        if sector and (row.get("sector") or "").lower() != sector.lower():
            continue

        stocks.append({
            "ticker": row["ticker"],
            "company": row.get("company_name", ""),
            "sector": row.get("sector", ""),
            "industry": row.get("industry", ""),
            "price": round(float(row["price"]), 2),
            "market_cap": row.get("market_cap"),
            # Grade
            "grade": g,
            "grade_score": grade_info["score"],
            "grade_color": grade_info["grade_color"],
            "grade_components": grade_info["components"],
            # Fair Value
            "fair_value": fv_info["fair_value"],
            "upside_pct": fv_info["upside_pct"],
            "fv_status": fvs,
            "fv_status_color": fv_info["fv_status_color"],
            "justified_pe": fv_info["justified_pe"],
            "peg_ratio": fv_info["peg_ratio"],
            "earnings_yield": fv_info["earnings_yield"],
            # Raw fundamentals for display
            "pe": round(float(row.get("pe_ratio") or 0), 1),
            "roe": round(float(row.get("roe") or 0), 1),
            "eps_growth": round(float(row.get("eps_growth_ttm") or 0), 1),
            "debt_equity": round(float(row.get("debt_to_equity") or 0), 2),
            "opm": round(float(row.get("operating_margin") or 0), 1),
        })

    # Sort by grade score desc, then upside desc
    stocks.sort(key=lambda x: (-x["grade_score"], -(x["upside_pct"] or -999)))

    # Rank
    for i, s in enumerate(stocks):
        s["rank"] = i + 1

    return {
        "stocks": stocks[:limit],
        "total": len(stocks),
        "grade_distribution": grade_counts,
        "fv_distribution": fv_counts,
        "timestamp": datetime.utcnow().isoformat(),
    }
