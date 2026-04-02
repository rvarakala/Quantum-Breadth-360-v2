"""
smart_money.py — Smart Money Intelligence Tracker

Identifies stocks with institutional accumulation signals:
  - IV (Institutional Volume): vol > 2x avg, close in upper 25%, up day
  - PPV (Pocket Pivot Volume): vol > max of last 10 down-day volumes, up day
  - BS (Bull Snort): gap up > 3% on vol > 1.5x avg, close > open

Returns per-ticker signals enriched with RS, Stage, Sector, IV Range, FVG, Insider data.
"""

import sqlite3, logging, os, numpy as np, pandas as pd
from pathlib import Path
from datetime import datetime, timedelta
from collections import defaultdict

logger = logging.getLogger(__name__)
DB_PATH = str(Path(__file__).parent / "breadth_data.db")


def compute_smart_money_signals(market: str = "India", days: int = 10) -> dict:
    """
    Compute per-ticker IV/PPV/BS signals for the last N trading days.
    Returns {tickers: [...], summary: {...}, dates_covered: [...]}
    """
    if not os.path.exists(DB_PATH):
        return {"error": "No database", "tickers": []}

    conn = sqlite3.connect(DB_PATH, timeout=30)
    try:
        lookback = days + 60  # need 50-day avg warmup
        cutoff = (datetime.now() - timedelta(days=int(lookback * 1.6))).strftime("%Y-%m-%d")

        rows = conn.execute("""
            SELECT ticker, date, open, high, low, close, volume
            FROM ohlcv WHERE date >= ? ORDER BY ticker, date
        """, (cutoff,)).fetchall()
    finally:
        conn.close()

    if not rows:
        return {"error": "No OHLCV data", "tickers": []}

    # Organize by ticker
    ticker_data = defaultdict(list)
    for ticker, dt, o, h, l, c, v in rows:
        ticker_data[ticker].append((dt, o, h, l, c, v))

    # Target dates (last N trading days)
    all_dates = sorted(set(r[1] for r in rows))
    target_dates = all_dates[-days:] if len(all_dates) >= days else all_dates
    target_set = set(target_dates)

    # Collect per-ticker signals
    # ticker_signals[ticker] = [{date, signal_type, vol_ratio, iv_high, iv_low, ...}, ...]
    ticker_signals = defaultdict(list)

    for ticker, records in ticker_data.items():
        if len(records) < 55:
            continue

        for i in range(50, len(records)):
            dt, o, h, l, c, v = records[i]
            if dt not in target_set:
                continue
            if not c or not o or c <= 0:
                continue

            _, _, _, _, prev_c, _ = records[i - 1]
            if not prev_c or prev_c <= 0:
                continue

            # 50-day volume average
            vols_50 = [r[5] for r in records[max(0, i - 50):i] if r[5] and r[5] > 0]
            avg_vol = np.mean(vols_50) if vols_50 else 0
            if not v or v <= 0 or avg_vol <= 0:
                continue

            vol_ratio = round(v / avg_vol, 2)
            rng = h - l if h and l and h > l else 1
            close_position = (c - l) / rng if rng > 0 else 0

            signals_today = []

            # IV: volume > 2x avg, close in top 25% of range, close > prev close
            if vol_ratio > 2.0 and close_position > 0.75 and c > prev_c:
                signals_today.append("IV")

            # PPV: volume > max of last 10 down-day volumes, close > prev close
            if c > prev_c:
                down_vols = []
                for j in range(max(0, i - 10), i):
                    _, _, _, _, cj, vj = records[j]
                    if j > 0:
                        _, _, _, _, cprev, _ = records[j - 1]
                        if cj and cprev and cj < cprev and vj and vj > 0:
                            down_vols.append(vj)
                if down_vols and v > max(down_vols):
                    signals_today.append("PPV")

            # Bull Snort: gap up > 3% on volume > 1.5x avg, close > open
            gap_pct = (o - prev_c) / prev_c * 100 if o and prev_c else 0
            if gap_pct > 3.0 and vol_ratio > 1.5 and c > o:
                signals_today.append("BS")

            if signals_today:
                ticker_signals[ticker].append({
                    "date": dt,
                    "signals": signals_today,
                    "price": round(c, 2),
                    "change_pct": round((c - prev_c) / prev_c * 100, 2),
                    "vol_ratio": vol_ratio,
                    "iv_high": round(h, 2),
                    "iv_low": round(l, 2),
                })

    # Build ticker list with signal counts and latest data
    result_tickers = []
    for ticker, sigs in ticker_signals.items():
        all_types = []
        for s in sigs:
            all_types.extend(s["signals"])

        # Latest OHLCV for this ticker
        records = ticker_data[ticker]
        last = records[-1]
        dt, o, h, l, c, v = last
        prev_c = records[-2][4] if len(records) > 1 else c

        # Count per signal type
        iv_count = all_types.count("IV")
        ppv_count = all_types.count("PPV")
        bs_count = all_types.count("BS")
        total = iv_count + ppv_count + bs_count

        # Signal label: "IV & BS" or "IV" etc.
        types_set = sorted(set(all_types))
        signal_label = " & ".join(types_set)

        # Latest IV range (from the most recent IV signal)
        iv_sigs = [s for s in sigs if "IV" in s["signals"]]
        latest_iv = iv_sigs[-1] if iv_sigs else None

        # Recent FVG (compute simple FVG from last 20 bars)
        fvg = _compute_recent_fvg(records)

        result_tickers.append({
            "ticker": ticker,
            "signal_label": signal_label,
            "total_signals": total,
            "iv_count": iv_count,
            "ppv_count": ppv_count,
            "bs_count": bs_count,
            "dates": [s["date"] for s in sigs],
            "latest_date": sigs[-1]["date"],
            "price": round(c, 2),
            "change_pct": round((c - prev_c) / prev_c * 100, 2) if prev_c > 0 else 0,
            "vol_ratio": round(v / avg_vol, 2) if avg_vol > 0 else 0,
            "iv_high": latest_iv["iv_high"] if latest_iv else None,
            "iv_low": latest_iv["iv_low"] if latest_iv else None,
            "fvg": fvg,
        })

    # Sort by total signal count (cluster = most signals first)
    result_tickers.sort(key=lambda x: x["total_signals"], reverse=True)

    return {
        "tickers": result_tickers,
        "dates_covered": target_dates,
        "total_signals": sum(t["total_signals"] for t in result_tickers),
        "unique_tickers": len(result_tickers),
        "computed_at": datetime.now().isoformat(),
    }


def _compute_recent_fvg(records) -> dict:
    """Compute most recent bullish FVG from OHLCV records."""
    if len(records) < 20:
        return None
    recent = records[-20:]
    for i in range(len(recent) - 1, 1, -1):
        _, o2, h2, l2, c2, _ = recent[i]      # current bar
        _, o1, h1, l1, c1, _ = recent[i - 1]  # middle bar
        _, o0, h0, l0, c0, _ = recent[i - 2]  # first bar
        if not all([c2, c1, c0, o2, o1, l2, h0]):
            continue
        # Bullish FVG: middle green, current green, gap between first high and current low
        if c1 > o1 and c2 > o2 and l2 > h0:
            return {"upper": round(l2, 2), "lower": round(h0, 2), "type": "Bullish"}
    return None


def enrich_smart_money(sm_data: dict, rs_cache: dict = None, insider_days: int = 30) -> dict:
    """
    Enrich Smart Money tickers with:
    - RS Rating, A/D, Stage, Sector, Sector RS (from RS rankings cache)
    - Insider buys (from insider_trades table)
    """
    tickers = sm_data.get("tickers", [])
    if not tickers:
        return sm_data

    # 1. RS data enrichment
    rs_lookup = {}
    if rs_cache:
        stocks = rs_cache.get("data", {}).get("stocks", [])
        for s in stocks:
            rs_lookup[s.get("ticker", "")] = s

    # 2. Insider data
    insider_lookup = defaultdict(list)
    try:
        conn = sqlite3.connect(DB_PATH, timeout=10)
        cutoff = (datetime.now() - timedelta(days=insider_days)).strftime("%Y-%m-%d")
        rows = conn.execute("""
            SELECT symbol, insider_name, transaction_type, shares, value_cr, transaction_date
            FROM insider_trades
            WHERE transaction_type IN ('Buy', 'Purchase')
              AND transaction_date >= ?
            ORDER BY transaction_date DESC
        """, (cutoff,)).fetchall()
        conn.close()
        for symbol, name, txn, shares, val, dt in rows:
            insider_lookup[symbol].append({
                "name": name, "type": txn,
                "shares": shares, "value_cr": val, "date": dt
            })
    except Exception as e:
        logger.debug(f"Insider lookup failed: {e}")

    # 3. Sector RS lookup
    sector_rs_lookup = {}
    if rs_cache:
        stocks = rs_cache.get("data", {}).get("stocks", [])
        sector_scores = defaultdict(list)
        for s in stocks:
            sec = s.get("sector", "")
            if sec:
                sector_scores[sec].append(s.get("rs_rating", 0))
        for sec, scores in sector_scores.items():
            sector_rs_lookup[sec] = round(np.mean(scores), 1) if scores else 0

    # Enrich each ticker
    for t in tickers:
        ticker = t["ticker"]
        rs = rs_lookup.get(ticker, {})

        t["rs_rating"] = rs.get("rs_rating")
        t["ad_rating"] = rs.get("ad_rating")
        t["sector"] = rs.get("sector", "")
        t["trend_template"] = rs.get("trend_template")
        t["trend_score"] = rs.get("trend_score_tt")
        t["pct_from_high"] = rs.get("pct_from_high")
        t["mcap_cr"] = rs.get("mcap_cr")

        # Stage classification
        tt = rs.get("trend_template")
        tt_score = rs.get("trend_score_tt", 0)
        if tt:
            t["stage"] = "Stage 2"
        elif tt_score >= 5:
            t["stage"] = "Stage 1→2"
        elif (rs.get("chg_3m") or 0) < -10:
            t["stage"] = "Stage 4"
        else:
            t["stage"] = "Stage 3"

        # Sector RS
        t["sector_rs"] = sector_rs_lookup.get(t["sector"], None)

        # Insider buys
        ins = insider_lookup.get(ticker, [])
        t["insider_buys"] = len(ins)
        t["insider_details"] = ins[:3]  # top 3 recent insider buys

    return sm_data
