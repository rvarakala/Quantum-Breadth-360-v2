"""
Sector Leadership — Row 3 Card 2

Computes sector Relative Strength (RS) vs NIFTY 500 benchmark.

RS formula:
  sector_return(N days) = mean of member ticker returns over N days
  benchmark_return(N days) = mean of all universe ticker returns over N days
  rs_diff(N) = sector_return - benchmark_return
  composite_rs = 0.1 * rs_5d + 0.6 * rs_20d + 0.3 * rs_60d

Sectors are then ranked 1-99 (IBD-style percentile) on composite RS.

Returns top 5 leaders + bottom 2 laggards with 1-day change + trend arrow.

Tables used:
- sector_map (ticker, sector)
- ohlcv (ticker, date, close)

Cache: 15-min in-memory (RS doesn't move fast).
"""
import logging
import time
import sqlite3
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Any
from pathlib import Path

logger = logging.getLogger(__name__)

IST = timezone(timedelta(hours=5, minutes=30))
DB_PATH = Path(__file__).parent / "breadth_data.db"

_cache: Dict[str, Any] = {"data": None, "fetched_at": 0}
CACHE_TTL = 900  # 15 min

# How many leaders/laggards to show
N_LEADERS = 5
N_LAGGARDS = 2

# Minimum sector members to be considered (filters out noise)
MIN_MEMBERS = 3


def _get_latest_date(conn: sqlite3.Connection) -> str:
    row = conn.execute(
        "SELECT MAX(date) FROM ohlcv WHERE market='India'"
    ).fetchone()
    return row[0] if row and row[0] else None


def _date_offset(latest: str, days_back: int) -> str:
    """Find the closest ohlcv date that is ~days_back calendar days before latest."""
    latest_dt = datetime.strptime(latest, "%Y-%m-%d")
    target = (latest_dt - timedelta(days=days_back)).strftime("%Y-%m-%d")
    return target


def _trend_arrow(rs_short: float, rs_long: float) -> str:
    """
    Arrow from comparing short-term RS (5d) vs long-term RS (60d).
    Short stronger than long → improving. Otherwise weakening.
    """
    diff = rs_short - rs_long
    if diff > 1.0:
        return "↑↑"      # accelerating
    if diff > 0:
        return "↑"       # mild improvement
    if diff > -1.0:
        return "→"       # stable
    if diff > -2.5:
        return "↓"       # mild decay
    return "↓↓"         # decelerating


def _compute_returns_single_query(conn: sqlite3.Connection, latest: str,
                                   days_back: int) -> Dict[str, float]:
    """
    Returns dict of {ticker: pct_return} for the trailing window.
    Uses a single SQL to pull both endpoints in one pass.
    """
    target = _date_offset(latest, days_back)
    # For each ticker: (latest close, earliest close >= target).
    # We approximate using MIN(date) where date >= target.
    sql = """
    WITH endpoints AS (
      SELECT
        ticker,
        (SELECT close FROM ohlcv o2 WHERE o2.ticker=o1.ticker AND o2.date <= ?
           ORDER BY date DESC LIMIT 1) AS last_close,
        (SELECT close FROM ohlcv o3 WHERE o3.ticker=o1.ticker AND o3.date >= ?
           ORDER BY date ASC  LIMIT 1) AS start_close
      FROM ohlcv o1
      WHERE o1.market='India'
      GROUP BY ticker
    )
    SELECT ticker, last_close, start_close
    FROM endpoints
    WHERE last_close IS NOT NULL AND start_close IS NOT NULL
      AND start_close > 0
    """
    rows = conn.execute(sql, (latest, target)).fetchall()
    returns = {}
    for ticker, last_close, start_close in rows:
        if last_close and start_close and start_close > 0:
            returns[ticker] = ((last_close - start_close) / start_close) * 100.0
    return returns


def _percentile_rank(value: float, sorted_values: List[float]) -> int:
    """IBD-style 1-99 percentile. sorted_values is ascending."""
    if not sorted_values:
        return 50
    n = len(sorted_values)
    below = sum(1 for v in sorted_values if v < value)
    pct = int(round((below / n) * 99)) + 1
    return max(1, min(99, pct))


def get_sector_rs() -> Dict[str, Any]:
    """
    Main entry.
    Returns:
    {
      leaders: [{sector, rs, change_1d, arrow, members}, ...],
      laggards: [...],
      benchmark: "NIFTY 500",
      as_of: "YYYY-MM-DD",
      updated_at: ISO timestamp
    }
    """
    cached = _cache.get("data")
    if cached and (time.time() - _cache.get("fetched_at", 0)) < CACHE_TTL:
        logger.info("[sector-rs] serving cached result")
        return cached

    _start = time.time()
    logger.info("[sector-rs] computing fresh — 4 window queries on full universe")

    try:
        conn = sqlite3.connect(str(DB_PATH), timeout=10)
    except Exception as e:
        logger.error(f"[sector-rs] db open failed: {e}")
        return {"error": "database unavailable", "leaders": [], "laggards": []}

    try:
        try:
            latest = _get_latest_date(conn)
        except sqlite3.OperationalError as e:
            logger.warning(f"[sector-rs] ohlcv table missing or empty: {e}")
            return {"error": "ohlcv table not populated yet", "leaders": [], "laggards": [],
                    "as_of": None, "sector_count": 0}
        if not latest:
            return {"error": "no OHLCV data", "leaders": [], "laggards": [],
                    "as_of": None}

        # 1. Load sector map
        try:
            sector_rows = conn.execute(
                "SELECT ticker, sector FROM sector_map "
                "WHERE sector IS NOT NULL AND sector != ''"
            ).fetchall()
        except sqlite3.OperationalError as e:
            logger.warning(f"[sector-rs] sector_map missing: {e}")
            return {"error": "sector_map table not populated", "leaders": [], "laggards": [],
                    "as_of": latest, "sector_count": 0}
        if not sector_rows:
            return {"error": "no sector_map", "leaders": [], "laggards": [],
                    "as_of": latest}

        ticker_to_sector: Dict[str, str] = {t: s for t, s in sector_rows}
        sector_members: Dict[str, List[str]] = {}
        for t, s in sector_rows:
            sector_members.setdefault(s, []).append(t)

        # 2. Compute returns at 3 windows in parallel SQL
        returns_5d = _compute_returns_single_query(conn, latest, 5)
        returns_20d = _compute_returns_single_query(conn, latest, 20)
        returns_60d = _compute_returns_single_query(conn, latest, 60)
        returns_1d = _compute_returns_single_query(conn, latest, 1)

        if not returns_20d:
            return {"error": "no return data", "leaders": [], "laggards": [],
                    "as_of": latest}

        # 3. Universe benchmark = mean return across all tickers in sector_map
        universe = [t for t in ticker_to_sector if t in returns_20d]
        if not universe:
            return {"error": "no overlapping universe", "leaders": [], "laggards": [],
                    "as_of": latest}

        def _mean(d, tickers):
            vals = [d.get(t) for t in tickers if d.get(t) is not None]
            return sum(vals) / len(vals) if vals else None

        bench_5d = _mean(returns_5d, universe)
        bench_20d = _mean(returns_20d, universe)
        bench_60d = _mean(returns_60d, universe)

        # 4. Per-sector aggregates
        sector_rs_list = []
        for sector, members in sector_members.items():
            if len(members) < MIN_MEMBERS:
                continue
            s_5d = _mean(returns_5d, members)
            s_20d = _mean(returns_20d, members)
            s_60d = _mean(returns_60d, members)
            s_1d = _mean(returns_1d, members)
            if s_20d is None:
                continue
            rs_5d = (s_5d or 0) - (bench_5d or 0)
            rs_20d = (s_20d or 0) - (bench_20d or 0)
            rs_60d = (s_60d or 0) - (bench_60d or 0)
            composite = 0.1 * rs_5d + 0.6 * rs_20d + 0.3 * rs_60d
            sector_rs_list.append({
                "sector": sector,
                "composite": composite,
                "rs_5d": rs_5d,
                "rs_60d": rs_60d,
                "change_1d": round(s_1d, 2) if s_1d is not None else 0,
                "members": len(members),
            })

        if not sector_rs_list:
            return {"error": "no sectors with enough members", "leaders": [],
                    "laggards": [], "as_of": latest}

        # 5. Assign 1-99 percentile ranks
        composites_sorted = sorted(s["composite"] for s in sector_rs_list)
        for s in sector_rs_list:
            s["rs"] = _percentile_rank(s["composite"], composites_sorted)
            s["arrow"] = _trend_arrow(s["rs_5d"], s["rs_60d"])

        # 6. Sort by composite and slice
        sector_rs_list.sort(key=lambda x: x["composite"], reverse=True)
        leaders = sector_rs_list[:N_LEADERS]
        laggards = sector_rs_list[-N_LAGGARDS:][::-1]  # worst first

        def _strip(s):
            return {
                "sector": s["sector"],
                "rs": s["rs"],
                "change_1d": round(s["change_1d"], 2),
                "arrow": s["arrow"],
                "members": s["members"],
            }

        payload = {
            "leaders": [_strip(s) for s in leaders],
            "laggards": [_strip(s) for s in laggards],
            "benchmark": "NIFTY 500",
            "as_of": latest,
            "sector_count": len(sector_rs_list),
            "updated_at": datetime.now(IST).isoformat(),
        }
        _cache["data"] = payload
        _cache["fetched_at"] = time.time()
        logger.info(
            f"[sector-rs] done in {time.time()-_start:.2f}s — "
            f"{len(sector_rs_list)} sectors, universe={len(universe)}"
        )
        return payload

    finally:
        conn.close()
