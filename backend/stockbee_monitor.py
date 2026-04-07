"""
stockbee_monitor.py — Full Stockbee Market Monitor
Computes daily breadth metrics across full NSE universe:
  - 4% Bull/Bear (stocks moving ±4%)
  - Q25 Bull/Bear (25-day high/low breakouts)
  - M25 Bull/Bear (above/below 25-day MA)
  - M50 Bull/Bear (above/below 50-day MA)
  - 34/13 Bull/Bear (13 EMA > 34 EMA crossover)
  - 5-day and 10-day cumulative ratios
  - 10-DCR (10-Day Cumulative Ratio) for 4% BO, 20-day MA, 50-day MA
"""

import logging
import sqlite3
import numpy as np
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict

logger = logging.getLogger(__name__)
DB_PATH = str(Path(__file__).parent / "breadth_data.db")


def compute_stockbee_monitor(universe: str = "nifty500", days: int = 500) -> dict:
    """
    Compute full Stockbee Market Monitor metrics.
    universe: 'nifty500' or 'full' (all NSE tickers)
    days: number of trading days of history to return
    """
    import time
    t0 = time.time()

    conn = sqlite3.connect(DB_PATH, timeout=30)

    # Determine ticker universe
    if universe == "nifty500":
        try:
            idx_rows = conn.execute("""
                SELECT DISTINCT ticker FROM nse_index_constituents
                WHERE index_name = 'NIFTY 500'
            """).fetchall()
            tickers_filter = set(r[0] for r in idx_rows)
            logger.info(f"Stockbee Monitor: NIFTY 500 universe = {len(tickers_filter)} tickers")
        except:
            tickers_filter = None  # fall back to all
    else:
        tickers_filter = None

    # Load OHLCV data
    lookback_days = days + 60  # extra for MA warmup
    cutoff = (datetime.now() - timedelta(days=int(lookback_days * 1.5))).strftime("%Y-%m-%d")
    rows = conn.execute("""
        SELECT ticker, date, open, high, low, close, volume
        FROM ohlcv WHERE market='India' AND date >= ?
        ORDER BY ticker, date
    """, (cutoff,)).fetchall()
    conn.close()

    if not rows:
        return {"error": "No OHLCV data found"}

    # Group by ticker
    ticker_data = defaultdict(list)
    for ticker, dt, o, h, l, c, v in rows:
        if tickers_filter and ticker not in tickers_filter:
            continue
        if c and c > 0:
            ticker_data[ticker].append((dt, float(o or c), float(h or c), float(l or c), float(c), float(v or 0)))

    # Get sorted unique trading dates
    all_dates = sorted(set(dt for entries in ticker_data.values() for dt, *_ in entries))
    if len(all_dates) < 60:
        return {"error": f"Insufficient data: {len(all_dates)} days (need 60+)"}

    target_dates = all_dates[-days:] if len(all_dates) >= days else all_dates
    logger.info(f"Stockbee Monitor: computing {len(target_dates)} days for {len(ticker_data)} tickers")

    # Pre-build per-ticker arrays for fast computation
    ticker_arrays = {}
    for ticker, entries in ticker_data.items():
        if len(entries) < 55:
            continue
        dates = [e[0] for e in entries]
        closes = np.array([e[4] for e in entries], dtype=np.float64)
        highs = np.array([e[2] for e in entries], dtype=np.float64)
        lows = np.array([e[3] for e in entries], dtype=np.float64)
        ticker_arrays[ticker] = {"dates": dates, "close": closes, "high": highs, "low": lows}

    # Compute daily metrics for each target date
    daily_metrics = []
    date_to_idx = {}  # for each ticker, map date → index

    for ticker, arr in ticker_arrays.items():
        date_to_idx[ticker] = {d: i for i, d in enumerate(arr["dates"])}

    for dt in target_dates:
        bull_4pct = 0   # stocks up ≥4%
        bear_4pct = 0   # stocks down ≥4%
        q25_bull = 0    # stocks at 25-day high
        q25_bear = 0    # stocks at 25-day low
        m25_bull = 0    # stocks above 25-day MA
        m25_bear = 0    # stocks below 25-day MA
        m50_bull = 0    # stocks above 50-day MA
        m50_bear = 0    # stocks below 50-day MA
        ema34_13_bull = 0  # 13 EMA > 34 EMA
        ema34_13_bear = 0  # 13 EMA < 34 EMA
        total = 0

        for ticker, arr in ticker_arrays.items():
            idx = date_to_idx[ticker].get(dt)
            if idx is None or idx < 50:
                continue

            c = arr["close"]
            h = arr["high"]
            l = arr["low"]
            price = c[idx]
            prev_price = c[idx - 1]
            total += 1

            if prev_price <= 0:
                continue

            # 4% movers
            chg_pct = (price - prev_price) / prev_price * 100
            if chg_pct >= 4:
                bull_4pct += 1
            elif chg_pct <= -4:
                bear_4pct += 1

            # Q25: 25-day high/low breakout
            hi25 = np.max(h[max(0, idx - 25):idx])  # 25-day high BEFORE today
            lo25 = np.min(l[max(0, idx - 25):idx])
            if price > hi25:
                q25_bull += 1
            if price < lo25:
                q25_bear += 1

            # M25: above/below 25-day MA
            ma25 = np.mean(c[max(0, idx - 24):idx + 1])
            if price > ma25:
                m25_bull += 1
            else:
                m25_bear += 1

            # M50: above/below 50-day MA
            if idx >= 50:
                ma50 = np.mean(c[idx - 49:idx + 1])
                if price > ma50:
                    m50_bull += 1
                else:
                    m50_bear += 1

            # 34/13 EMA crossover
            if idx >= 34:
                # Simple EMA approximation using span
                ema13 = _ema_at(c, idx, 13)
                ema34 = _ema_at(c, idx, 34)
                if ema13 > ema34:
                    ema34_13_bull += 1
                else:
                    ema34_13_bear += 1

        daily_metrics.append({
            "date": dt,
            "total": total,
            "bull_4pct": bull_4pct,
            "bear_4pct": bear_4pct,
            "q25_bull": q25_bull,
            "q25_bear": q25_bear,
            "m25_bull": m25_bull,
            "m25_bear": m25_bear,
            "m50_bull": m50_bull,
            "m50_bear": m50_bear,
            "ema34_13_bull": ema34_13_bull,
            "ema34_13_bear": ema34_13_bear,
        })

    # Compute rolling ratios
    for i, m in enumerate(daily_metrics):
        # 5-day ratio (sum of bull_4pct / sum of bear_4pct over 5 days)
        if i >= 4:
            b5 = sum(daily_metrics[j]["bull_4pct"] for j in range(i - 4, i + 1))
            r5 = sum(daily_metrics[j]["bear_4pct"] for j in range(i - 4, i + 1))
            m["ratio_5d"] = round(b5 / max(r5, 1), 2)
        else:
            m["ratio_5d"] = 0

        # 10-day ratio
        if i >= 9:
            b10 = sum(daily_metrics[j]["bull_4pct"] for j in range(i - 9, i + 1))
            r10 = sum(daily_metrics[j]["bear_4pct"] for j in range(i - 9, i + 1))
            m["ratio_10d"] = round(b10 / max(r10, 1), 2)
        else:
            m["ratio_10d"] = 0

        # 10-DCR of 4% BO (10-day cumulative ratio of 4% breakouts)
        if i >= 9:
            dcr_vals = []
            for j in range(i - 9, i + 1):
                d = daily_metrics[j]
                bear = max(d["bear_4pct"], 1)
                dcr_vals.append(d["bull_4pct"] / bear)
            m["dcr_4pct_10d"] = round(sum(dcr_vals), 2)
        else:
            m["dcr_4pct_10d"] = 0

        # 10-DCR of 20-day MA (M25 bull/bear ratio over 10 days)
        if i >= 9:
            dcr_m25 = []
            for j in range(i - 9, i + 1):
                d = daily_metrics[j]
                bear = max(d["m25_bear"], 1)
                dcr_m25.append(d["m25_bull"] / bear)
            m["dcr_m25_10d"] = round(sum(dcr_m25), 2)
        else:
            m["dcr_m25_10d"] = 0

        # 10-DCR of 50-day MA
        if i >= 9:
            dcr_m50 = []
            for j in range(i - 9, i + 1):
                d = daily_metrics[j]
                bear = max(d["m50_bear"], 1)
                dcr_m50.append(d["m50_bull"] / bear)
            m["dcr_m50_10d"] = round(sum(dcr_m50), 2)
        else:
            m["dcr_m50_10d"] = 0

    elapsed = round(time.time() - t0, 2)
    logger.info(f"Stockbee Monitor: computed {len(daily_metrics)} days in {elapsed}s")

    # Q25 primary breadth ratio history (for top chart)
    q25_ratio_history = []
    for m in daily_metrics:
        bear = max(m["q25_bear"], 1)
        q25_ratio_history.append({
            "date": m["date"],
            "ratio": round(m["q25_bull"] / bear, 2),
            "bull": m["q25_bull"],
            "bear": m["q25_bear"],
        })

    return {
        "daily": daily_metrics,
        "q25_ratio_history": q25_ratio_history,
        "universe": universe,
        "ticker_count": len(ticker_arrays),
        "date_range": {"from": target_dates[0], "to": target_dates[-1]},
        "elapsed": elapsed,
    }


def _ema_at(arr, idx, span):
    """Compute EMA value at a specific index using exponential weighting."""
    k = 2.0 / (span + 1)
    start = max(0, idx - span * 3)  # use 3x span for warmup
    ema = float(arr[start])
    for i in range(start + 1, idx + 1):
        ema = arr[i] * k + ema * (1 - k)
    return ema
