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
CACHE_TTL = 3600  # 60 min (EOD data — sectors don't shift intraday)

# Cap the ohlcv scan to the last N calendar days. 60d lookback + buffer
# for weekends/holidays. Prunes 20-year history → ~70 days of rows upfront.
SCAN_WINDOW_DAYS = 70

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


def _compute_all_returns(conn: sqlite3.Connection, latest: str
                         ) -> Dict[str, Dict[str, float]]:
    """
    Single-pass endpoint fetch for all four windows (1d, 5d, 20d, 60d).

    Why this shape:
      An earlier attempt used a CTE + 4 correlated subqueries. SQLite's
      planner does NOT materialize the CTE — each subquery re-scanned
      the base ohlcv table via idx_market_date, which was ~17s on a
      500-ticker × 2000-day DB.

      This version is ~25× faster:
      1. ONE flat index scan pulls the last ~70 days of India rows
         (~35k rows, dominates wall time at ~650ms).
      2. Python groups by ticker and walks each ticker's sorted series
         in-memory to locate the start-of-window close for each horizon
         (~10ms total — negligible).

    Returns: {ticker: {'ret_1d': pct, 'ret_5d': pct,
                       'ret_20d': pct, 'ret_60d': pct}}
    """
    target_1d  = _date_offset(latest, 1)
    target_5d  = _date_offset(latest, 5)
    target_20d = _date_offset(latest, 20)
    target_60d = _date_offset(latest, 60)
    scan_floor = _date_offset(latest, SCAN_WINDOW_DAYS)

    # Flat ordered scan — uses idx_market_date for the range, ORDER BY
    # matches (ticker, date) which is the PK so sort is cheap.
    rows = conn.execute(
        "SELECT ticker, date, close FROM ohlcv "
        "WHERE market=? AND date >= ? "
        "ORDER BY ticker, date",
        ("India", scan_floor)
    ).fetchall()

    # Group by ticker. Each ticker's series is already date-ascending.
    from collections import defaultdict
    by_ticker: Dict[str, list] = defaultdict(list)
    for ticker, date, close in rows:
        by_ticker[ticker].append((date, close))

    def _start_close(series, target_date):
        # Series is ascending by date; return first close at or after target.
        for dt, c in series:
            if dt >= target_date and c and c > 0:
                return c
        return None

    out: Dict[str, Dict[str, float]] = {}
    windows = (("ret_1d",  target_1d),  ("ret_5d",  target_5d),
               ("ret_20d", target_20d), ("ret_60d", target_60d))
    for ticker, series in by_ticker.items():
        if not series:
            continue
        _, last_close = series[-1]
        if not last_close or last_close <= 0:
            continue
        entry: Dict[str, float] = {}
        for key, tgt in windows:
            c_start = _start_close(series, tgt)
            if c_start:
                entry[key] = ((last_close - c_start) / c_start) * 100.0
        if entry:
            out[ticker] = entry
    return out


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
    logger.info(
        f"[sector-rs] computing fresh — flat scan + python compute, "
        f"scan window={SCAN_WINDOW_DAYS}d"
    )

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

        # 2. Single-pass compute of all 4 windows (prunes 20yr → 70d first)
        all_returns = _compute_all_returns(conn, latest)

        # Project into per-window dicts so the rest of the function is unchanged.
        # Only include a ticker in a window dict if that specific window succeeded
        # (preserves the old per-window gating behavior).
        returns_1d  = {t: r["ret_1d"]  for t, r in all_returns.items() if "ret_1d"  in r}
        returns_5d  = {t: r["ret_5d"]  for t, r in all_returns.items() if "ret_5d"  in r}
        returns_20d = {t: r["ret_20d"] for t, r in all_returns.items() if "ret_20d" in r}
        returns_60d = {t: r["ret_60d"] for t, r in all_returns.items() if "ret_60d" in r}

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
