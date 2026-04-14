"""
Q-BRAM v2: Quantitative Breadth Regime Assessment Model
7-component scoring (0-100) with smoothing and regime confirmation.

Components:
  B50              20pts — % stocks above 50 DMA
  NH-NL Ratio      15pts — Net New Highs minus New Lows / Total
  Breadth Thrust   15pts — Daily advance ratio (3-day EMA smoothed)
  B200             15pts — % stocks above 200 DMA
  B20 Acceleration 10pts — Change in % above 20-DMA (3-day EMA smoothed)
  Volume Thrust    10pts — Volume on up days vs down days
  CSD              15pts — Cross-Sectional Dispersion (3-day EMA smoothed, inverse)
"""
import logging
import numpy as np
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor as _TPE

from cache import get_cache, set_cache
from utils import (
    safe_float, safe_download, get_close, get_change_pct,
    get_stock_data, fetch_batch,
    DB_AVAILABLE, INDIA_SECTORS, US_SECTORS,
    load_sector_map, load_ticker_universe,
)

logger = logging.getLogger(__name__)

# ── Prevent concurrent breadth computations (same market) ────────────────────
import threading
_compute_locks: dict = {}
_compute_locks_lock = threading.Lock()

# ── EMA smoothing cache for noisy v2 components (per market) ─────────────────
# Stores { market: { "bt_ema": float, "csd_ema": float, "b20a_ema": float,
#                     "prev_regime": str, "regime_confirm_count": int } }
_smoothing_state: dict = {}
_smoothing_lock = threading.Lock()

def _ema(prev, cur, alpha=0.5):
    """Exponential moving average with alpha=0.5 ≈ 3-period EMA."""
    if prev is None:
        return cur
    return alpha * cur + (1.0 - alpha) * prev

def _get_smooth(market: str) -> dict:
    with _smoothing_lock:
        if market not in _smoothing_state:
            _smoothing_state[market] = {
                "bt_ema": None, "csd_ema": None, "b20a_ema": None,
                "prev_regime": None, "regime_confirm_count": 0,
            }
        return _smoothing_state[market]

def _update_smooth(market: str, bt_raw, csd_raw, b20a_raw):
    """Update EMA state and return smoothed values."""
    st = _get_smooth(market)
    st["bt_ema"] = _ema(st["bt_ema"], bt_raw)
    st["csd_ema"] = _ema(st["csd_ema"], csd_raw)
    st["b20a_ema"] = _ema(st["b20a_ema"], b20a_raw)
    return st["bt_ema"], st["csd_ema"], st["b20a_ema"]

def _confirm_regime(market: str, new_regime: str) -> str:
    """
    2-day regime confirmation: regime only changes after 2 consecutive
    closes in the new regime. Prevents single-day whipsaws.
    Exception: PANIC transitions are immediate (safety first).
    """
    st = _get_smooth(market)
    prev = st["prev_regime"]

    # First ever computation or PANIC is always immediate
    if prev is None or new_regime == "PANIC":
        st["prev_regime"] = new_regime
        st["regime_confirm_count"] = 1
        return new_regime

    if new_regime == prev:
        # Same regime — reset counter
        st["regime_confirm_count"] = 1
        return new_regime
    else:
        # Different regime — need 2 consecutive days
        st["regime_confirm_count"] += 1
        if st["regime_confirm_count"] >= 2:
            st["prev_regime"] = new_regime
            st["regime_confirm_count"] = 1
            return new_regime
        else:
            # Hold previous regime (not confirmed yet)
            return prev

def _get_market_lock(market: str) -> threading.Lock:
    with _compute_locks_lock:
        if market not in _compute_locks:
            _compute_locks[market] = threading.Lock()
        return _compute_locks[market]



def compute_breadth(stock_data, index_ticker, market="INDIA"):
    """
    Q-BRAM v2: Quantitative Breadth Regime Assessment Model
    7-Component Scoring (0-100) with 3-day EMA smoothing on fast components.

    Components:
      B50              20pts — % stocks above 50 DMA
      NH-NL Ratio      15pts — Net New Highs minus New Lows / Total
      Breadth Thrust   15pts — Advancers / Total (3-day EMA smoothed)
      B200             15pts — % stocks above 200 DMA
      B20 Acceleration 10pts — Change in % above 20-DMA (3-day EMA smoothed)
      Volume Thrust    10pts — Up volume vs Down volume ratio
      CSD              15pts — Cross-Sectional Dispersion (3-day EMA smoothed, inverse)
    """
    adv=dec=unc=a20=a50=a200=nh=nl=valid=w200=0
    up_vol=dn_vol=0.0
    daily_returns = []   # for CSD calculation

    # Historical data for B20 acceleration (last 6 days)
    daily_a20 = [0]*6
    daily_valid_a20 = [0]*6  # track valid counts per day for %

    for ticker, df in stock_data.items():
        if df is None or len(df)<21: continue
        valid+=1
        c  = df["Close"]
        v  = df["Volume"] if "Volume" in df.columns else None
        cur = safe_float(c.iloc[-1])
        prev= safe_float(c.iloc[-2])

        # Today's A/D
        if   cur > prev*1.001: adv+=1
        elif cur < prev*0.999: dec+=1
        else: unc+=1

        # Daily return for CSD (Cross-Sectional Dispersion)
        if prev > 0:
            daily_returns.append((cur - prev) / prev * 100)

        # Volume attribution
        if v is not None and len(v)>=1:
            vol_today = safe_float(v.iloc[-1])
            if cur > prev: up_vol += vol_today
            else:          dn_vol += vol_today

        # DMA checks
        if len(df)>=20:
            m20 = safe_float(c.rolling(20).mean().iloc[-1])
            if cur > m20: a20+=1
        if len(df)>=50:
            m50 = safe_float(c.rolling(50).mean().iloc[-1])
            if cur > m50: a50+=1
        if len(df)>=200:
            w200+=1
            m200 = safe_float(c.rolling(200).mean().iloc[-1])
            if cur > m200: a200+=1

        # 52-week NH/NL
        lb = min(len(df),252)
        if cur >= safe_float(df["High"].tail(lb).max())*0.98: nh+=1
        if cur <= safe_float(df["Low"].tail(lb).min())*1.02:  nl+=1

        # Historical B20 for acceleration (last 6 days)
        n = len(df)
        for d_back in range(min(6, n-2)):
            try:
                idx_cur  = n - 1 - d_back
                if n >= 21:
                    start = max(0, idx_cur - 19)
                    roll_mean = float(c.iloc[start:idx_cur+1].mean())
                    daily_valid_a20[d_back] += 1
                    if safe_float(c.iloc[idx_cur]) > roll_mean:
                        daily_a20[d_back] += 1
            except: continue

    if valid==0:
        return {"error":"No valid stock data. Run /api/sync/start first, or wait 1 min."}

    # ── Core metrics ─────────────────────────────────────────────────────────
    adr   = round(adv/dec, 2) if dec>0 else float(adv)
    p20   = round(a20/valid*100, 1)
    p50   = round(a50/valid*100, 1)
    p200  = round(a200/w200*100, 1) if w200>0 else 0
    nh_nl = nh - nl

    # ── NEW v2: Breadth Thrust (raw) ─────────────────────────────────────────
    bt_raw = round(adv / valid, 4) if valid > 0 else 0.0

    # ── NEW v2: Cross-Sectional Dispersion (raw) ─────────────────────────────
    csd_raw = round(float(np.std(daily_returns)), 4) if len(daily_returns) > 10 else 2.0

    # ── B20 Acceleration (raw) ───────────────────────────────────────────────
    p20_today = round(daily_a20[0]/daily_valid_a20[0]*100, 1) if daily_valid_a20[0]>0 else 0
    p20_5d    = round(daily_a20[5]/daily_valid_a20[5]*100, 1) if daily_valid_a20[5]>0 else 0
    b20_accel_raw = round(p20_today - p20_5d, 1)

    # ── Volume ratio ─────────────────────────────────────────────────────────
    vol_ratio = round(up_vol/(dn_vol+1), 2)

    # ── Apply 3-day EMA smoothing to fast components ─────────────────────────
    bt_smooth, csd_smooth, b20a_smooth = _update_smooth(market, bt_raw, csd_raw, b20_accel_raw)
    bt_smooth = round(bt_smooth, 4)
    csd_smooth = round(csd_smooth, 4)
    b20a_smooth = round(b20a_smooth, 1)

    # ── NHNL ratio for scoring ───────────────────────────────────────────────
    nhnl_ratio = round(nh_nl / valid, 4) if valid > 0 else 0.0

    # ── Q-BRAM v2 Score (7 components) ───────────────────────────────────────
    score, score_components = _qbram_score_v2(
        p50, nhnl_ratio, bt_smooth, p200, b20a_smooth, vol_ratio, csd_smooth
    )

    # ── Regime with 2-day confirmation ───────────────────────────────────────
    raw_regime = _regime(score)
    regime = _confirm_regime(market, raw_regime)

    # ── Divergence detection ─────────────────────────────────────────────────
    div = None
    try:
        idx_df = safe_download(index_ticker)
        if not idx_df.empty and len(idx_df)>=2:
            c1 = get_close(idx_df)
            c2 = float(idx_df["Close"].dropna().iloc[-2])
            ic = (c1-c2)/c2*100 if c2 else 0
            if   ic > 0.5  and adr < 1.0: div={"type":"Narrow Rally","severity":"warning","message":"Index rising on narrow breadth — unsustainable."}
            elif ic < -0.5 and adr > 1.2: div={"type":"Stealth Strength","severity":"positive","message":"Breadth holding despite index dip — accumulation signal."}
    except: pass

    return dict(
        valid=valid, with_200dma=w200,
        advancers=adv, decliners=dec, unchanged=unc,
        ad_ratio=adr,
        breadth_thrust=bt_smooth, breadth_thrust_raw=bt_raw,
        csd=csd_smooth, csd_raw=csd_raw,
        pct_above_20=p20, pct_above_50=p50, pct_above_200=p200,
        b20_accel=b20a_smooth, b20_accel_raw=b20_accel_raw,
        vol_ratio=vol_ratio,
        nhnl_ratio=round(nhnl_ratio * 100, 2),  # as percentage for display
        new_highs=nh, new_lows=nl, nh_nl=nh_nl,
        score=score, score_components=score_components,
        regime=regime, raw_regime=raw_regime,
        regime_color=_rcolor(regime),
        divergence=div,
        qbram_version="v2",
        timestamp=datetime.now(timezone.utc).isoformat()
    )

def _qbram_score_v2(p50, nhnl_ratio, bt, p200, b20_accel, vol_ratio, csd):
    """
    Q-BRAM v2: 7-Component Scoring (0-100)
    B50              20pts — % above 50 DMA
    NH-NL Ratio      15pts — Net New Highs / Total (as fraction)
    Breadth Thrust   15pts — Advancers / Total (smoothed)
    B200             15pts — % above 200 DMA
    B20 Acceleration 10pts — Short-term breadth acceleration (smoothed)
    Volume Thrust    10pts — Up/Down volume ratio
    CSD              15pts — Cross-Sectional Dispersion (smoothed, inverse)
    """
    components = {}

    # ── B50: 20 points ────────────────────────────────────────────────────────
    if   p50 >= 80: b50_pts = 20
    elif p50 >= 65: b50_pts = 18
    elif p50 >= 50: b50_pts = 15
    elif p50 >= 40: b50_pts = 12
    elif p50 >= 30: b50_pts = 8
    elif p50 >= 20: b50_pts = 4
    else:           b50_pts = 0
    components["B50"] = {"value": p50, "points": b50_pts, "max": 20, "weight": "20%"}

    # ── NH-NL Ratio: 15 points ────────────────────────────────────────────────
    # nhnl_ratio is already (nh-nl)/total as a fraction
    if   nhnl_ratio >= 0.20: nhnl_pts = 15
    elif nhnl_ratio >= 0.10: nhnl_pts = 12
    elif nhnl_ratio >= 0.05: nhnl_pts = 9
    elif nhnl_ratio >= 0.00: nhnl_pts = 6
    elif nhnl_ratio >= -0.10: nhnl_pts = 3
    else:                     nhnl_pts = 0
    components["NH_NL"] = {"value": round(nhnl_ratio*100, 1), "points": nhnl_pts, "max": 15, "weight": "15%"}

    # ── Breadth Thrust: 15 points (NEW — replaces A/D ROC) ────────────────────
    if   bt >= 0.55: bt_pts = 15
    elif bt >= 0.50: bt_pts = 12
    elif bt >= 0.45: bt_pts = 9
    elif bt >= 0.40: bt_pts = 6
    elif bt >= 0.35: bt_pts = 3
    else:            bt_pts = 0
    components["BT"] = {"value": round(bt * 100, 1), "points": bt_pts, "max": 15, "weight": "15%"}

    # ── B200: 15 points ───────────────────────────────────────────────────────
    if   p200 >= 70: b200_pts = 15
    elif p200 >= 60: b200_pts = 12
    elif p200 >= 50: b200_pts = 9
    elif p200 >= 40: b200_pts = 6
    elif p200 >= 30: b200_pts = 3
    else:            b200_pts = 0
    components["B200"] = {"value": p200, "points": b200_pts, "max": 15, "weight": "15%"}

    # ── B20 Acceleration: 10 points ───────────────────────────────────────────
    if   b20_accel >= 10: b20_pts = 10
    elif b20_accel >= 5:  b20_pts = 8
    elif b20_accel >= 0:  b20_pts = 5
    elif b20_accel >= -5: b20_pts = 2
    else:                 b20_pts = 0
    components["B20_ACCEL"] = {"value": b20_accel, "points": b20_pts, "max": 10, "weight": "10%"}

    # ── Volume Thrust: 10 points ──────────────────────────────────────────────
    if   vol_ratio >= 2.0: vol_pts = 10
    elif vol_ratio >= 1.5: vol_pts = 8
    elif vol_ratio >= 1.0: vol_pts = 5
    elif vol_ratio >= 0.7: vol_pts = 2
    else:                  vol_pts = 0
    components["VOLUME"] = {"value": vol_ratio, "points": vol_pts, "max": 10, "weight": "10%"}

    # ── CSD (Cross-Sectional Dispersion): 15 points (NEW — inverse scoring) ──
    if   csd <= 2.0: csd_pts = 15
    elif csd <= 2.2: csd_pts = 12
    elif csd <= 2.4: csd_pts = 9
    elif csd <= 2.6: csd_pts = 6
    elif csd <= 2.8: csd_pts = 3
    else:            csd_pts = 0
    components["CSD"] = {"value": csd, "points": csd_pts, "max": 15, "weight": "15%"}

    total_score = min(100, b50_pts + nhnl_pts + bt_pts + b200_pts + b20_pts + vol_pts + csd_pts)
    return total_score, components

def _regime(s):
    """Q-BRAM Regime Classification"""
    if   s >= 80: return "EXPANSION"
    elif s >= 60: return "ACCUMULATION"
    elif s >= 40: return "TRANSITION"
    elif s >= 20: return "DISTRIBUTION"
    else:         return "PANIC"

def _rcolor(r):
    return {
        "EXPANSION":    "#22c55e",   # Green
        "ACCUMULATION": "#86efac",   # Light green
        "TRANSITION":   "#f59e0b",   # Amber
        "DISTRIBUTION": "#ef4444",   # Red
        "PANIC":        "#7f1d1d",   # Dark red
    }.get(r, "#64748b")

def _regime_interp(r):
    """Q-BRAM v2 regime descriptions with Circuit Breaker trading actions"""
    return {
        "EXPANSION":    "Broad-based rally, most stocks participating. Full position sizing (100%), trade aggressively.",
        "ACCUMULATION": "Early bull or recovery, institutions building positions. Full sizing (100-120%), buy optimal setups.",
        "TRANSITION":   "Mixed signals, regime uncertainty. Reduced sizing (60%), trade defensively, wait for clarity.",
        "DISTRIBUTION": "Smart money selling, narrow leadership. No new entries, hold existing with tight stops.",
        "PANIC":        "Crisis mode — extreme selling, high CSD. EXIT ALL POSITIONS. Watch for reversal signals.",
    }.get(r, "Regime analysis unavailable.")

def _sector_breadth(sector_map, stock_data):
    out=[]
    for name,tickers in sector_map.items():
        a50=tot=0; rets=[]; valid_tickers=[]
        for t in tickers:
            df=stock_data.get(t)
            if df is None or len(df)<51: continue
            tot+=1; c=df["Close"]; cur=safe_float(c.iloc[-1])
            valid_tickers.append(t)
            if cur>safe_float(c.rolling(50).mean().iloc[-1]): a50+=1
            if len(df)>=5:
                p5=safe_float(c.iloc[-5])
                if p5>0: rets.append((cur-p5)/p5*100)
        pct=round(a50/tot*100,1) if tot>0 else 0
        out.append({"sector":name,"pct_above_50":pct,
                    "week_return":round(float(np.mean(rets)),2) if rets else 0,
                    "stocks_counted":tot,"tickers":valid_tickers})
    return sorted(out,key=lambda x:x["pct_above_50"],reverse=True)

def _ad_history(stock_data,days=252):
    dates=sorted(set(d for df in stock_data.values() if df is not None and len(df)>=2
                     for d in df.index[-days:]))[-days:]
    cum=0; out=[]
    for date_val in dates:
        adv=dec=0
        for df in stock_data.values():
            if df is None or len(df)<2 or date_val not in df.index: continue
            loc=df.index.get_loc(date_val)
            if loc==0: continue
            try:
                cur=safe_float(df["Close"].iloc[loc]); prv=safe_float(df["Close"].iloc[loc-1])
                if cur>prv*1.001: adv+=1
                elif cur<prv*0.999: dec+=1
            except: pass
        cum+=adv-dec
        out.append({"date":str(date_val)[:10],"advancers":adv,"decliners":dec,
                    "net":adv-dec,"cumulative":cum})
    return out

def _dma_history(stock_data,days=252):
    dates=sorted(set(d for df in stock_data.values() if df is not None and len(df)>=51
                     for d in df.index[-days:]))[-days:]
    out=[]
    for date_val in dates:
        a=tot=0
        for df in stock_data.values():
            if df is None or len(df)<51 or date_val not in df.index: continue
            loc=df.index.get_loc(date_val)
            if loc<50: continue
            try:
                cur=safe_float(df["Close"].iloc[loc])
                ma=float(df["Close"].iloc[loc-50:loc+1].mean())
                tot+=1
                if cur>ma: a+=1
            except: pass
        if tot>0: out.append({"date":str(date_val)[:10],"pct_above_50":round(a/tot*100,1)})
    return out

def _nh_nl_history(stock_data,days=252):
    dates=sorted(set(d for df in stock_data.values() if df is not None and len(df)>=2
                     for d in df.index[-days:]))[-days:]
    out=[]
    for date_val in dates:
        h=l=0
        for df in stock_data.values():
            if df is None or len(df)<20 or date_val not in df.index: continue
            loc=df.index.get_loc(date_val)
            try:
                cur=safe_float(df["Close"].iloc[loc])
                h52=safe_float(df["High"].iloc[max(0,loc-251):loc+1].max())
                l52=safe_float(df["Low"].iloc[max(0,loc-251):loc+1].min())
                if cur>=h52*0.98: h+=1
                if cur<=l52*1.02: l+=1
            except: pass
        out.append({"date":str(date_val)[:10],"new_highs":h,"new_lows":l,"net":h-l})
    return out

def _compute_market(market: str, custom_tickers: dict = None) -> dict:
    """Compute breadth for a market. Uses per-market lock to prevent concurrent runs."""
    lock = _get_market_lock(market)
    if not lock.acquire(blocking=False):
        # Already computing this market — return cache immediately, don't wait
        logger.info(f"=== {market} breadth already computing — returning cache ===")
        from cache import get_cache
        cached = get_cache(f"breadth_{market}")
        if cached:
            return {**cached, "computing": True}
        # No cache at all — must wait
        logger.info(f"=== {market} no cache, waiting for computation ===")
        lock.acquire(blocking=True, timeout=120)
        try:
            cached = get_cache(f"breadth_{market}")
            if cached:
                return cached
        finally:
            try:
                lock.release()
            except RuntimeError:
                pass  # Lock wasn't held
        return {"error": "Breadth computation in progress, no cache available"}
    try:
        result = _compute_market_impl(market, custom_tickers)
        return result
    finally:
        try:
            lock.release()
        except RuntimeError:
            pass  # Already released


def _compute_market_impl(market: str, custom_tickers: dict = None) -> dict:
    if custom_tickers is None:
        custom_tickers = {}

    # Load sector map from SQLite if available
    db_sector_map = {}
    if DB_AVAILABLE:
        try:
            raw_map = load_sector_map()
            if raw_map:
                for ticker, info in raw_map.items():
                    s = info['sector']
                    if s not in db_sector_map:
                        db_sector_map[s] = []
                    db_sector_map[s].append(ticker)
        except Exception as e:
            logger.warning(f"Could not load sector map: {e}")

    cfg={
        "INDIA":dict(index="^CRSLDX",index_name="NIFTY 500",vix="^INDIAVIX",nifty50="^NSEI",
                     sectors=db_sector_map if db_sector_map else INDIA_SECTORS,
                     db_market="India"),
        "US":   dict(index="^RUA",index_name="Russell 3000", vix="^VIX",
                     sectors=US_SECTORS, db_market="US"),
    }[market]
    logger.info(f"=== Computing {market} ===")
    stock_data = get_stock_data(cfg["db_market"], custom_tickers=custom_tickers)
    if not stock_data:
        return {"error":"No data available. Run /api/sync/start or check connection.",
                "market":market,"timestamp":datetime.now(timezone.utc).isoformat()}
    metrics=compute_breadth(stock_data,cfg["index"],market=market)
    if "error" in metrics:
        return {**metrics,"market":market}
    ip=ic=vv=n50=n50c=0.0
    # Fetch index, VIX, NIFTY50 SEQUENTIALLY with retry
    ip = ic = vv = n50 = n50c = 0.0

    def _dl(ticker, retries=2):
        """Download index/VIX ticker prices.
        Uses Yahoo Finance v8 API directly — yfinance v1 is broken for Indian indices."""
        import time
        # For index tickers (^CRSLDX, ^INDIAVIX, ^NSEI etc.)
        # skip yfinance v1 entirely — go straight to v8 which works reliably
        is_index = ticker.startswith("^")
        if not is_index:
            # Stock tickers: try yfinance first
            for attempt in range(retries + 1):
                try:
                    df = safe_download(ticker, period="10d")
                    if df is not None and not df.empty and "Close" in df.columns:
                        return df
                    logger.warning(f"Empty result for {ticker} (attempt {attempt+1})")
                except Exception as e:
                    logger.warning(f"Download {ticker} attempt {attempt+1} failed: {e}")
                if attempt < retries:
                    time.sleep(1)

        # Index tickers or stock fallback: direct Yahoo Finance v8 API via httpx
        try:
            import httpx
            url = f"https://query1.finance.yahoo.com/v8/finance/chart/{ticker}"
            params = {"range": "10d", "interval": "1d"}
            headers = {"User-Agent": "Mozilla/5.0"}
            resp = httpx.get(url, params=params, headers=headers, timeout=15, follow_redirects=True)
            if resp.status_code == 200:
                data = resp.json()
                result = data.get("chart", {}).get("result", [])
                if result:
                    meta = result[0].get("meta", {})
                    price = meta.get("regularMarketPrice", 0)
                    prev = meta.get("chartPreviousClose", 0)
                    if price > 0:
                        chg_pct = ((price - prev) / prev * 100) if prev > 0 else 0
                        # Build a minimal DataFrame
                        import pandas as _pd
                        df = _pd.DataFrame({
                            "Close": [prev, price],
                            "Open": [prev, price],
                            "High": [prev, price],
                            "Low": [prev, price],
                            "Volume": [0, 0]
                        })
                        logger.info(f"Fallback API got {ticker}: {price} ({chg_pct:+.2f}%)")
                        return df
            logger.warning(f"Fallback API also failed for {ticker}: HTTP {resp.status_code}")
        except Exception as e:
            logger.warning(f"Fallback API error for {ticker}: {e}")

        import pandas as _pd
        return _pd.DataFrame()

    try:
        df = _dl(cfg["index"])
        ip, ic = get_close(df), get_change_pct(df)
        logger.info(f"Index {cfg['index']}: {ip} ({ic}%)")
    except Exception as e:
        logger.error(f"Index fetch failed: {e}")

    try:
        df = _dl(cfg["vix"])
        vv = get_close(df)
        logger.info(f"VIX {cfg['vix']}: {vv}")
    except Exception as e:
        logger.error(f"VIX fetch failed: {e}")

    if market == "INDIA" and cfg.get("nifty50"):
        try:
            df = _dl(cfg["nifty50"])
            n50, n50c = get_close(df), get_change_pct(df)
            logger.info(f"NIFTY50 {cfg['nifty50']}: {n50} ({n50c}%)")
        except Exception as e:
            logger.error(f"NIFTY50 fetch failed: {e}")

    logger.info(f"Live prices: index={ip} vix={vv} nifty50={n50}")

    # Cache last good values — fallback when yfinance fails
    _idx_cache_key = f"_index_prices_{market}"
    if ip > 0 or vv > 0 or n50 > 0:
        # Got at least some data — save it
        from cache import set_cache, get_cache
        set_cache(_idx_cache_key, {"ip": ip, "ic": ic, "vv": vv, "n50": n50, "n50c": n50c})
    else:
        # All zeros — try to use cached values
        from cache import get_cache
        cached_idx = get_cache(_idx_cache_key)
        if cached_idx:
            ip = cached_idx.get("ip", 0)
            ic = cached_idx.get("ic", 0)
            vv = cached_idx.get("vv", 0)
            n50 = cached_idx.get("n50", 0)
            n50c = cached_idx.get("n50c", 0)
            logger.info(f"Using cached index prices: index={ip} vix={vv} nifty50={n50}")

    # Use up to 252 days for charts (1 year), or all available if more
    chart_days = 252
    # ── Determine last OHLCV date across universe ──────────────────────────
    last_ohlcv_date = "unknown"
    try:
        import sqlite3 as _sql, pathlib as _pl
        _db = _pl.Path(__file__).parent / "breadth_data.db"
        if _db.exists():
            _conn = _sql.connect(str(_db), timeout=10)
            _row  = _conn.execute(
                "SELECT MAX(date) FROM ohlcv WHERE market='India'"
            ).fetchone()
            _conn.close()
            if _row and _row[0]:
                last_ohlcv_date = _row[0]   # e.g. "2026-03-21"
    except Exception:
        pass

    # Is data from today or yesterday?
    from datetime import date as _date
    _today = _date.today().isoformat()
    # Check freshness — EOD = data from last 5 trading days (covers weekends)
    try:
        from datetime import timedelta as _td
        _last_dt = _date.fromisoformat(last_ohlcv_date) if last_ohlcv_date != "unknown" else None
        if _last_dt:
            _days_old = (_date.today() - _last_dt).days
            _data_freshness = "today" if _days_old == 0 else (
                "EOD" if _days_old <= 5 else "stale"
            )
        else:
            _data_freshness = "unknown"
    except Exception:
        _data_freshness = "unknown"

    # ── Store Q-BRAM score to history table ─────────────────────────────────
    try:
        if DB_AVAILABLE and last_ohlcv_date != "unknown" and metrics.get("valid"):
            import sqlite3
            _hconn = sqlite3.connect(str(DB_PATH), timeout=10)
            _hconn.execute("""
                CREATE TABLE IF NOT EXISTS qbram_score_history (
                    date TEXT NOT NULL, market TEXT NOT NULL,
                    score INTEGER, regime TEXT,
                    pct_above_50 REAL, nh_nl INTEGER,
                    breadth_thrust REAL, csd REAL,
                    qbram_version TEXT DEFAULT 'v2',
                    PRIMARY KEY (date, market)
                )
            """)
            _hconn.execute("""
                INSERT OR REPLACE INTO qbram_score_history
                (date, market, score, regime, pct_above_50, nh_nl, breadth_thrust, csd, qbram_version)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, (
                last_ohlcv_date, market.upper(),
                metrics.get("score", 0), metrics.get("regime", ""),
                metrics.get("pct_above_50", 0), metrics.get("nh_nl", 0),
                metrics.get("breadth_thrust", 0), metrics.get("csd", 0),
                "v2",
            ))
            _hconn.commit()
            _hconn.close()
            logger.info(f"Q-BRAM score stored: {last_ohlcv_date} {market} score={metrics.get('score')} regime={metrics.get('regime')}")
    except Exception as _e:
        logger.debug(f"Score history store failed: {_e}")

    return {**metrics,"market":market,"index_name":cfg["index_name"],"nifty50_price":round(n50,2),"nifty50_change_pct":round(n50c,2),
            "index_price":round(ip,2),"index_change_pct":round(ic,2),"vix":round(vv,2),
            "ad_history":_ad_history(stock_data,chart_days),
            "dma_history":_dma_history(stock_data,chart_days),
            "nh_nl_history":_nh_nl_history(stock_data,chart_days),
            "sector_breadth":_sector_breadth(cfg["sectors"],stock_data),
            "universe_size":len(stock_data),
            "data_source":"local_db" if DB_AVAILABLE else "live_yfinance",
            "last_ohlcv_date": last_ohlcv_date,
            "data_freshness":  _data_freshness,
            "computed_at":     datetime.now(timezone.utc).isoformat()}


