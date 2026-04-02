"""
Market Breadth Engine — Backend
- Reads OHLCV from local SQLite DB (10 years)
- Falls back to live yfinance if DB is empty
- /api/sync/start   → kick off 10-year backfill
- /api/sync/update  → daily incremental update
- /api/sync/status  → see what's stored locally
"""
from fastapi import FastAPI, BackgroundTasks, UploadFile, Request, Depends
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import pandas as pd
from datetime import datetime, timezone, date, timedelta
from typing import Dict
import logging
import asyncio
from concurrent.futures import ThreadPoolExecutor
import os, pathlib

# Load .env file if present (GROQ_API_KEY etc.)
try:
    from dotenv import load_dotenv
    load_dotenv(pathlib.Path(__file__).parent / ".env")
    load_dotenv(pathlib.Path(__file__).parent.parent / ".env")  # project root too
except ImportError:
    pass  # python-dotenv not installed — env vars still work

from cache import _cache, get_cache, set_cache, _load_disk_cache
from utils import (
    safe_float, flatten_df, safe_download, get_close, get_change_pct,
    get_stock_data, get_screener_data, fetch_batch,
    DB_AVAILABLE, INDIA_TICKERS, SP500_TICKERS,
    US_SECTORS, INDIA_SECTORS,
    db_stats, load_sector_map, load_sector_counts, import_sectors_csv,
    save_ticker_universe, load_ticker_universe, import_ticker_universe_csv,
    save_sector_map, import_nifty500_csv,
    run_full_backfill, run_daily_update,
)
from breadth import (
    compute_breadth, _compute_market, _regime_interp,
)
from screeners import (
    CUSTOM_SCREENER_MAP, apply_custom_screener,
    _rs_cache, RS_CACHE_TTL, _rs_cache_key,
    _compute_rs_rankings, _build_sector_map,
    LEADERS_CACHE_TTL, _leaders_cache, _compute_leaders,
)
from stockbee import _compute_stockbee
from nse_sync import sync_nifty500, sync_full_history, _get_stale_tickers
from us_sync import import_iwv_holdings_csv, sync_us_ohlcv, sync_us_daily, full_us_setup
from fundamentals_sync import sync_fundamentals, get_eps_for_ticker
from charts import get_chart_data
from stock_metrics import compute_stock_metrics, compute_eps_async
from smart_metrics_service import get_smart_metrics, run_smart_screener
from liquidity_regime import compute_iv_footprint
from peep_into_past import compute_historical_breadth
from sectors_heatmap import compute_sector_heatmap
from insider import (
    get_insider_trades, get_insider_summary, sync_insider_data,
    import_insider_csv, compute_buy_score, _ensure_tables as _ensure_insider_tables,
)
from fvalue import run_fvalue_screener
from fiidii import fetch_fiidii_from_nse, get_fiidii_summary, import_fiidii_csv
from auth import (
    ensure_auth_tables, register_user, login_user, verify_token,
    check_tab_access, admin_list_users, admin_update_user, admin_delete_user,
    admin_add_user, admin_stats, TIERS,
)
from nse_data_adapter import (
    sync_insider_with_fallback,
    sync_fundamentals_indian_api,
    fetch_fundamentals_batch,
    fetch_ohlcv_bhavcopy,
    store_ohlcv_bhavcopy,
    health_check as adapter_health_check,
)
from watchlist import (
    list_watchlists, create_watchlist, delete_watchlist,
    add_ticker as wl_add_ticker, remove_ticker as wl_remove_ticker,
    get_watchlist_data, create_alert, list_alerts, delete_alert, check_alerts,
)
from email_digest import generate_market_summary, generate_summary_html
from tv_fundamentals import (
    fetch_batch_fundamentals, get_batch_fundamental,
    is_batch_fresh, _ensure_tables as ensure_tv_tables,
)
from ai_insights import (
    get_market_intelligence, get_stock_analysis,
    save_api_key, validate_api_key, _get_api_key,
)
from nse_indices import (
    sync_nse_indices, get_index_constituents, get_index_registry_status,
    get_ticker_indices, NSE_INDEX_REGISTRY, _ensure_tables as ensure_nse_tables,
    get_universe_stats, get_stale_constituent_tickers, get_all_constituent_tickers,
)
from market_cap import (
    import_market_cap_csv, get_mcap_for_ticker, get_all_mcaps,
    filter_by_mcap, format_mcap, get_mcap_tier, _ensure_table as ensure_mcap_table,
)

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s %(message)s')
logger = logging.getLogger(__name__)

app = FastAPI(title="Market Breadth Engine", version="2.0.0")
app.add_middleware(CORSMiddleware,
                   allow_origins=["*"],
                   allow_credentials=False,
                   allow_methods=["*"],
                   allow_headers=["*"])
executor = ThreadPoolExecutor(max_workers=4)

# ── Serve frontend from backend (no separate server needed) ──────────────────
FRONTEND_DIR = pathlib.Path(__file__).parent.parent / "frontend"

# Auto cache-bust: version = server start timestamp
_ASSET_VERSION = str(int(datetime.now(timezone.utc).timestamp()))
_NO_CACHE = {"Cache-Control": "no-cache, no-store, must-revalidate", "Pragma": "no-cache", "Expires": "0"}

def _serve_index_html():
    """Read index.html and inject current asset version."""
    from fastapi.responses import HTMLResponse
    html = (FRONTEND_DIR / "index.html").read_text(encoding="utf-8")
    html = html.replace("__ASSET_VERSION__", _ASSET_VERSION)
    return HTMLResponse(content=html, headers=_NO_CACHE)

def _serve_html(filename):
    from fastapi.responses import HTMLResponse
    html = (FRONTEND_DIR / filename).read_text(encoding="utf-8")
    return HTMLResponse(content=html, headers=_NO_CACHE)

@app.get("/")
def serve_root():
    """Landing page — login/register."""
    return _serve_html("landing.html")

@app.get("/app")
def serve_app():
    """Main dashboard — requires auth (checked client-side)."""
    return _serve_index_html()

@app.get("/admin")
def serve_admin():
    """Admin panel."""
    return _serve_html("admin.html")

@app.get("/index.html")
def serve_index():
    return _serve_index_html()


# ── Auth API ──────────────────────────────────────────────────────────────────

class AuthLogin(BaseModel):
    email: str
    password: str

class AuthRegister(BaseModel):
    name: str
    email: str
    password: str

class AdminUpdateUser(BaseModel):
    user_id: int
    tier: str = None
    status: str = None

class AdminAddUser(BaseModel):
    name: str
    email: str
    password: str
    tier: str = "explorer"

class AdminDeleteUser(BaseModel):
    user_id: int

def _get_current_user(request: Request) -> dict:
    """Extract and verify JWT from Authorization header."""
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer "):
        return None
    token = auth[7:]
    return verify_token(token)

@app.post("/api/auth/register")
def api_auth_register(body: AuthRegister):
    return register_user(body.email, body.name, body.password)

@app.post("/api/auth/login")
def api_auth_login(body: AuthLogin):
    return login_user(body.email, body.password)

@app.get("/api/auth/me")
def api_auth_me(request: Request):
    user = _get_current_user(request)
    if not user:
        return {"error": "Not authenticated"}
    # Always fetch REAL tier from DB (token may be stale)
    from auth import get_user_by_id, _generate_token
    db_user = get_user_by_id(user["id"])
    if not db_user:
        return {"error": "User not found"}
    if db_user["status"] != "active":
        return {"error": "Account suspended"}
    # Return real tier from DB + fresh token if tier changed
    result = {
        "id": db_user["id"],
        "email": db_user["email"],
        "name": db_user.get("name", ""),
        "tier": db_user["tier"],
    }
    if db_user["tier"] != user.get("tier"):
        result["refreshed_token"] = _generate_token(db_user["id"], db_user["email"], db_user["tier"])
    return result

@app.get("/api/auth/tiers")
def api_auth_tiers():
    return {k: {"name": v["name"], "tabs": v["tabs"], "price_monthly": v["price_monthly"],
                "price_annual": v["price_annual"]} for k, v in TIERS.items() if k != "admin"}


# ── Admin API ─────────────────────────────────────────────────────────────────

def _require_admin(request: Request):
    user = _get_current_user(request)
    if not user or user.get("tier") != "admin":
        return None
    return user

@app.get("/api/admin/stats")
def api_admin_stats(request: Request):
    if not _require_admin(request): return {"error": "Admin required"}
    return admin_stats()

@app.get("/api/admin/users")
def api_admin_users(request: Request):
    if not _require_admin(request): return {"error": "Admin required"}
    return admin_list_users()

@app.post("/api/admin/update-user")
def api_admin_update(request: Request, body: AdminUpdateUser):
    if not _require_admin(request): return {"error": "Admin required"}
    return admin_update_user(body.user_id, tier=body.tier, status=body.status)

@app.post("/api/admin/delete-user")
def api_admin_delete(request: Request, body: AdminDeleteUser):
    if not _require_admin(request): return {"error": "Admin required"}
    return admin_delete_user(body.user_id)

@app.post("/api/admin/add-user")
def api_admin_add(request: Request, body: AdminAddUser):
    if not _require_admin(request): return {"error": "Admin required"}
    return admin_add_user(body.email, body.name, body.password, body.tier)

# Serve static assets if any
if FRONTEND_DIR.exists():
    try:
        app.mount("/static", StaticFiles(directory=str(FRONTEND_DIR)), name="static")
    except Exception:
        pass

# NSE Indices sync state
_nse_indices_state = {"running": False, "progress": 0, "total": 0, "message": "", "result": None}

# Sync state (in-memory progress tracker)
_sync_state = {"running": False, "progress": 0, "total": 0,
               "message": "Idle", "last_run": None, "result": None}

# ── Dynamic ticker universe (set by frontend upload) ─────────────────────────
_custom_tickers: Dict[str, list] = {}   # { 'India': [...], 'US': [...] }


def _enrich_stocks_mcap(stocks: list):
    """Add mcap_cr and mcap_tier to each stock dict in-place."""
    all_mcaps = get_all_mcaps()
    for s in stocks:
        mcap_data = all_mcaps.get(s.get("ticker", ""), {})
        s["mcap_cr"] = mcap_data.get("mcap_cr", 0)
        s["mcap_tier"] = mcap_data.get("mcap_tier", "")

# ── API Routes ────────────────────────────────────────────────────────────────

@app.get("/api/health")
def health():
    return {"status":"ok","db":DB_AVAILABLE,
            "cached":list(_cache.keys()),
            "time":datetime.now(timezone.utc).isoformat()}

@app.get("/api/debug")
def debug():
    tests={"RELIANCE.NS":"India stock","AAPL":"US stock","^CRSLDX":"NIFTY 500 index","^VIX":"VIX"}
    results={}
    for ticker,desc in tests.items():
        try:
            df=safe_download(ticker)
            results[ticker]={"desc":desc,"ok":not df.empty,"rows":len(df),
                             "close":get_close(df) or None}
        except Exception as e:
            results[ticker]={"desc":desc,"ok":False,"error":str(e)[:120]}
    return {"all_ok":all(v["ok"] for v in results.values()),
            "tests":results,"db_available":DB_AVAILABLE,
            "time":datetime.now(timezone.utc).isoformat()}

@app.get("/api/sync/status")
def sync_status():
    """Check what's stored in the local DB"""
    if not DB_AVAILABLE:
        return {"error":"data_store.py not loaded"}
    stats = db_stats()
    return {**stats, "sync_running": _sync_state["running"],
            "sync_message": _sync_state["message"],
            "sync_progress": _sync_state["progress"],
            "sync_total": _sync_state["total"],
            "last_sync": _sync_state["last_run"]}

@app.post("/api/sync/start")
async def sync_start(background_tasks: BackgroundTasks,
                     market: str = "all", years: int = 10):
    """
    Kick off 10-year historical backfill in the background.
    market = 'India' | 'US' | 'Index' | 'all'
    This runs in background — poll /api/sync/status to track progress.
    """
    if not DB_AVAILABLE:
        return {"error":"data_store.py not found"}
    if _sync_state["running"]:
        return {"message":"Sync already running","progress":_sync_state["progress"],
                "total":_sync_state["total"]}

    def _run():
        _sync_state["running"] = True
        _sync_state["message"] = f"Downloading {years}y history for {market}..."
        _sync_state["last_run"] = datetime.now(timezone.utc).isoformat()
        try:
            result = run_full_backfill(market=market)
            _sync_state["result"]  = result
            _sync_state["message"] = f"Done! {result['done']} tickers, {result['total_rows']:,} rows stored"
        except Exception as e:
            _sync_state["message"] = f"Error: {e}"
            _sync_state["result"]  = {"error": str(e)}
        finally:
            _sync_state["running"] = False

    background_tasks.add_task(_run)
    return {"message":f"Backfill started for {market} ({years} years). Poll /api/sync/status for progress.",
            "tip":"This takes 5–15 minutes. Dashboard will auto-use DB data when ready."}

@app.post("/api/sync/update")
async def sync_update(background_tasks: BackgroundTasks):
    """Daily incremental update — only fetches new candles"""
    if not DB_AVAILABLE:
        return {"error":"data_store.py not found"}
    if _sync_state["running"]:
        return {"message":"Sync already running"}

    def _run():
        _sync_state["running"] = True
        _sync_state["message"] = "Running daily update..."
        try:
            result = run_daily_update()
            _sync_state["message"] = f"Update done: {result['updated']} tickers, {result['total_new_rows']} new rows"
            _sync_state["result"]  = result
        except Exception as e:
            _sync_state["message"] = f"Error: {e}"
        finally:
            _sync_state["running"] = False

    background_tasks.add_task(_run)
    return {"message":"Daily update started. Poll /api/sync/status to track."}



# ── NSE Direct Sync (Yahoo v8 API — no yfinance dependency) ──────────────────
_nse_sync_state = {"running": False, "progress": 0, "total": 0, "message": "", "result": None}

@app.get("/api/nse-sync/status")
def nse_sync_status():
    return _nse_sync_state

@app.post("/api/nse-sync/start")
async def nse_sync_start(background_tasks: BackgroundTasks, range: str = "3mo"):
    """Sync stale NIFTY 500 tickers via Yahoo v8 direct API."""
    if _nse_sync_state["running"]:
        return {"message": "Sync already running", **_nse_sync_state}

    def _run():
        _nse_sync_state["running"] = True
        _nse_sync_state["message"] = "Starting NSE sync..."
        try:
            result = sync_nifty500(range_str=range, max_workers=5, progress_state=_nse_sync_state)
            _nse_sync_state["result"] = result
            _nse_sync_state["message"] = result["message"]
        except Exception as e:
            _nse_sync_state["message"] = f"Error: {e}"
            _nse_sync_state["result"] = {"error": str(e)}
        finally:
            _nse_sync_state["running"] = False

    background_tasks.add_task(_run)
    return {"message": "NSE sync started. Poll /api/nse-sync/status for progress."}

@app.post("/api/nse-sync/full")
async def nse_sync_full(background_tasks: BackgroundTasks):
    """Full 2-year history sync for ALL NIFTY 500 tickers."""
    if _nse_sync_state["running"]:
        return {"message": "Sync already running", **_nse_sync_state}

    def _run():
        _nse_sync_state["running"] = True
        _nse_sync_state["message"] = "Full 2-year sync starting..."
        try:
            result = sync_full_history(range_str="2y", max_workers=3)
            _nse_sync_state["result"] = result
            _nse_sync_state["message"] = result["message"]
        except Exception as e:
            _nse_sync_state["message"] = f"Error: {e}"
        finally:
            _nse_sync_state["running"] = False

    background_tasks.add_task(_run)
    return {"message": "Full 2-year sync started. This takes 10-15 minutes."}

@app.post("/api/nse-sync/force-today")
async def nse_sync_force_today(background_tasks: BackgroundTasks):
    """Force sync ALL NIFTY 500 tickers with latest 1 month data.
    Use after market close to get today's EOD data."""
    if _nse_sync_state["running"]:
        return {"message": "Sync already running", **_nse_sync_state}

    def _run():
        _nse_sync_state["running"] = True
        _nse_sync_state["message"] = "Force EOD sync starting..."
        try:
            result = sync_full_history(range_str="1mo", max_workers=5)
            _nse_sync_state["result"] = result
            _nse_sync_state["message"] = result["message"]
            # Clear breadth cache so next load computes fresh
            from cache import _cache as CACHE
            keys_to_clear = [k for k in CACHE if k.startswith("breadth_") or k.startswith("stockbee_") or k.startswith("rs_")]
            for k in keys_to_clear:
                CACHE.pop(k, None)
            if keys_to_clear:
                _nse_sync_state["message"] += f" | Cache cleared ({len(keys_to_clear)} keys)"
        except Exception as e:
            _nse_sync_state["message"] = f"Error: {e}"
        finally:
            _nse_sync_state["running"] = False

    background_tasks.add_task(_run)
    return {"message": "Force EOD sync started for all 500 tickers (1 month). Takes 3-5 minutes."}


@app.post("/api/fundamentals/sync")
async def fundamentals_sync_endpoint(background_tasks: BackgroundTasks):
    """Sync EPS, PE, market cap from Yahoo Finance for all tickers missing data."""
    if _nse_sync_state["running"]:
        return {"message": "Another sync is already running"}
    
    def _run():
        _nse_sync_state["running"] = True
        _nse_sync_state["message"] = "Fetching fundamentals (EPS, PE, Market Cap)..."
        try:
            result = sync_fundamentals(max_workers=3, progress_state=_nse_sync_state)
            _nse_sync_state["result"] = result
            _nse_sync_state["message"] = result["message"]
        except Exception as e:
            _nse_sync_state["message"] = f"Error: {e}"
        finally:
            _nse_sync_state["running"] = False
    
    background_tasks.add_task(_run)
    return {"message": "Fundamentals sync started. This fetches EPS/PE/MCap for all tickers."}

@app.get("/api/eps/{ticker}")
async def get_eps(ticker: str):
    """Get EPS for a ticker. Fetches from Yahoo if not cached."""
    ticker = ticker.upper().strip()
    return get_eps_for_ticker(ticker)

@app.get("/api/nse-sync/stale")
def nse_sync_stale():
    """Check which NIFTY 500 tickers have stale data."""
    stale = _get_stale_tickers(days_threshold=3)
    return {"stale_count": len(stale), "total": 500, "fresh": 500 - len(stale),
            "stale_tickers": [(t, d) for t, d in stale[:30]]}

@app.get("/api/breadth/iv-footprint")
async def iv_footprint(market: str = "India", days: int = 30):
    """Count IV/PPV/Bull Snort signals per day across the universe."""
    loop = asyncio.get_event_loop()
    data = await loop.run_in_executor(executor, compute_iv_footprint, market, days)
    return {"market": market, "days": days, "data": data}


# ── Smart Money Tracker ──────────────────────────────────────────────────────

@app.get("/api/smart-money")
async def get_smart_money(days: int = 10):
    """
    Smart Money Intelligence: per-ticker IV/PPV/BS signals enriched with
    RS, Stage, Sector, IV Range, FVG, Insider Buys.
    """
    from smart_money import compute_smart_money_signals, enrich_smart_money
    loop = asyncio.get_event_loop()
    try:
        sm_data = await loop.run_in_executor(executor, compute_smart_money_signals, "India", days)
        if sm_data.get("error"):
            return sm_data

        # Enrich with RS rankings + insider data
        rs_cache_data = _rs_cache.get(_rs_cache_key("India"), {})
        sm_data = await loop.run_in_executor(executor, enrich_smart_money, sm_data, rs_cache_data)

        return sm_data
    except Exception as e:
        logger.error(f"Smart Money error: {e}")
        import traceback; traceback.print_exc()
        return {"error": str(e), "tickers": []}


@app.get("/api/smart-money/notes")
async def get_smart_money_notes():
    """Get all Smart Money notes."""
    try:
        conn = sqlite3.connect(str(pathlib.Path(__file__).parent / "breadth_data.db"), timeout=10)
        conn.execute("CREATE TABLE IF NOT EXISTS smart_money_notes (ticker TEXT PRIMARY KEY, note TEXT, updated_at TEXT)")
        rows = conn.execute("SELECT ticker, note, updated_at FROM smart_money_notes").fetchall()
        conn.close()
        return {r[0]: {"note": r[1], "updated_at": r[2]} for r in rows}
    except Exception as e:
        return {"error": str(e)}


@app.post("/api/smart-money/notes")
async def save_smart_money_note(request: Request):
    """Save a note for a ticker."""
    body = await request.json()
    ticker = body.get("ticker", "").upper().strip()
    note = body.get("note", "").strip()
    if not ticker:
        return {"error": "No ticker"}
    try:
        conn = sqlite3.connect(str(pathlib.Path(__file__).parent / "breadth_data.db"), timeout=10)
        conn.execute("CREATE TABLE IF NOT EXISTS smart_money_notes (ticker TEXT PRIMARY KEY, note TEXT, updated_at TEXT)")
        conn.execute("INSERT OR REPLACE INTO smart_money_notes (ticker, note, updated_at) VALUES (?,?,?)",
                     (ticker, note, datetime.now(timezone.utc).isoformat()))
        conn.commit()
        conn.close()
        return {"status": "ok", "ticker": ticker}
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/breadth/{market}")
async def get_breadth(market: str, refresh: bool = False):
    market=market.upper()
    if market not in ("INDIA","US"):
        return {"error":"Market must be INDIA or US"}
    key=f"breadth_{market}"
    if not refresh:
        cached=get_cache(key)
        if cached:
            return {**cached,"cached":True}
    loop=asyncio.get_event_loop()
    result=await loop.run_in_executor(executor, lambda: _compute_market(market, custom_tickers=_custom_tickers))
    if "error" not in result:
        set_cache(key,result)
    return {**result,"cached":False}

@app.get("/api/compare")
async def compare():
    def s(d):
        if not d or "error" in d: return None
        return {k:d.get(k) for k in ("score","regime","regime_color","pct_above_50",
                "pct_above_200","ad_ratio","nh_nl","vix","index_price",
                "index_change_pct","index_name")}
    return {"India":s(get_cache("breadth_INDIA")),"US":s(get_cache("breadth_US"))}


# ── US Market Sync (Russell 3000) ─────────────────────────────────────────────

_us_sync_state: dict = {"running": False, "message": "Idle", "synced": 0, "total": 0}

@app.get("/api/us-sync/status")
async def us_sync_status():
    return _us_sync_state

@app.post("/api/us-sync/import-csv")
async def us_sync_import_csv(file: UploadFile):
    """Import iShares IWV Russell 3000 holdings CSV."""
    import tempfile, shutil
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    try:
        shutil.copyfileobj(file.file, tmp)
        tmp.close()
        result = import_iwv_holdings_csv(tmp.name)
        return {"status": "ok", **result}
    except Exception as e:
        return {"status": "error", "message": str(e)}
    finally:
        try: os.unlink(tmp.name)
        except: pass

@app.post("/api/us-sync/start")
async def us_sync_start(background_tasks: BackgroundTasks, period: str = "2y"):
    """Start full OHLCV sync for Russell 3000 universe."""
    if _us_sync_state["running"]:
        return {"message": "US sync already running", **_us_sync_state}

    def _run():
        _us_sync_state["running"] = True
        _us_sync_state["message"] = "Starting US OHLCV sync..."
        try:
            def _progress(msg):
                _us_sync_state["message"] = msg
            result = sync_us_ohlcv(period=period, progress_callback=_progress)
            _us_sync_state["synced"] = result.get("synced", 0)
            _us_sync_state["total"] = result.get("total_tickers", 0)
            _us_sync_state["message"] = result.get("message", "Done")
            _us_sync_state["result"] = result
            # Clear US breadth cache so it recomputes
            from cache import _cache as CACHE
            keys_to_clear = [k for k in CACHE if "US" in k or "us" in k.lower()]
            for k in keys_to_clear:
                CACHE.pop(k, None)
        except Exception as e:
            _us_sync_state["message"] = f"Error: {e}"
        finally:
            _us_sync_state["running"] = False

    background_tasks.add_task(_run)
    return {"message": "US sync started. Poll /api/us-sync/status for progress."}

@app.post("/api/us-sync/daily")
async def us_sync_daily_endpoint(background_tasks: BackgroundTasks):
    """Quick daily EOD sync for US market (last 5 days)."""
    if _us_sync_state["running"]:
        return {"message": "US sync already running"}
    def _run():
        _us_sync_state["running"] = True
        _us_sync_state["message"] = "Running daily US EOD sync..."
        try:
            result = sync_us_daily()
            _us_sync_state["message"] = result.get("message", "Done")
            _us_sync_state["result"] = result
        except Exception as e:
            _us_sync_state["message"] = f"Error: {e}"
        finally:
            _us_sync_state["running"] = False
    background_tasks.add_task(_run)
    return {"message": "Daily US sync started."}


@app.get("/api/ohlcv/fetch")
async def fetch_ohlcv_gap(ticker: str, from_date: str = None, to_date: str = None):
    """
    Fetch OHLCV for a single ticker between two dates.
    Used by the browser sync engine to fill gaps.
    ticker: yfinance format e.g. RELIANCE.NS or AAPL
    """
    import yfinance as yf

    if not from_date:
        from_date = (date.today() - timedelta(days=7)).isoformat()
    if not to_date:
        to_date = (date.today() + timedelta(days=1)).isoformat()

    try:
        df = yf.download(ticker, start=from_date, end=to_date,
                         interval="1d", auto_adjust=True,
                         progress=False, timeout=20)
        if df is None or df.empty:
            return {"rows": [], "ticker": ticker, "from": from_date, "to": to_date}

        if isinstance(df.columns, pd.MultiIndex):
            df.columns = df.columns.get_level_values(0)

        rows = []
        for idx, row in df.iterrows():
            d = str(idx.date()) if hasattr(idx, 'date') else str(idx)[:10]
            close_val = safe_float(row.get("Close"))
            if not close_val:
                continue
            rows.append({
                "date":   d,
                "open":   safe_float(row.get("Open")),
                "high":   safe_float(row.get("High")),
                "low":    safe_float(row.get("Low")),
                "close":  close_val,
                "volume": int(row.get("Volume", 0) or 0),
            })

        logger.info(f"Gap fetch {ticker} {from_date}→{to_date}: {len(rows)} rows")
        return {"rows": rows, "ticker": ticker, "from": from_date, "to": to_date}

    except Exception as e:
        logger.error(f"Gap fetch failed {ticker}: {e}")
        return {"rows": [], "error": str(e)}


@app.get("/api/ohlcv/lastdate")
async def get_last_dates(market: str = "India"):
    """Return the last stored date per ticker from local DB (if available)"""
    if not DB_AVAILABLE:
        return {"error": "DB not available"}
    try:
        return db_stats()
    except Exception as e:
        return {"error": str(e)}

@app.post("/api/universe/set")
async def set_universe(payload: dict):
    """Set custom ticker universe from frontend upload"""
    tickers = payload.get("tickers", [])
    market  = payload.get("market", "India")
    if not tickers:
        return {"error": "Invalid ticker list"}
    clean = list(set([t.upper().replace(".NS","").replace(".BO","").strip()
                      for t in tickers if t and len(str(t)) <= 20]))
    _custom_tickers[market] = clean
    if DB_AVAILABLE:
        save_ticker_universe(market, clean)
    if f"breadth_{market.upper()}" in _cache:
        del _cache[f"breadth_{market.upper()}"]
    logger.info(f"Universe set: {len(clean)} {market} tickers")
    return {"ok": True, "market": market, "tickers_set": len(clean), "sample": clean[:5]}

@app.get("/api/universe/status")
def get_universe_status():
    """Check active ticker universes"""
    india = _custom_tickers.get("India") or (load_ticker_universe("India") if DB_AVAILABLE else []) or INDIA_TICKERS
    us    = _custom_tickers.get("US")    or SP500_TICKERS
    return {
        "India": {"source":"custom" if "India" in _custom_tickers else ("sqlite" if DB_AVAILABLE and load_ticker_universe("India") else "default"),
                  "count": len(india), "sample": india[:5]},
        "US":    {"source":"default", "count": len(us), "sample": us[:5]}
    }

@app.post("/api/sectors/upload")
async def upload_sectors(payload: dict):
    """Save sector mapping from frontend JSON upload"""
    records = payload.get("records", [])
    if not records:
        return {"error": "No records provided"}
    if DB_AVAILABLE:
        saved = save_sector_map(records)
        return {"ok": True, "saved": saved}
    return {"error": "DB not available"}

@app.get("/api/sectors/status")
def get_sectors_status():
    """Get sector map summary from SQLite"""
    if not DB_AVAILABLE:
        return {"available": False}
    counts = load_sector_counts()
    return {"available": True, "sectors": len(counts),
            "total_tickers": sum(s['tickers'] for s in counts),
            "breakdown": counts[:10]}

@app.get("/api/db/stats")
def get_db_stats():
    """Full database statistics"""
    if not DB_AVAILABLE:
        return {"available": False, "message": "Run import first"}
    stats = db_stats()
    if DB_AVAILABLE:
        try:
            stats["sector_count"]  = len(load_sector_counts())
            stats["india_universe"] = len(load_ticker_universe("India"))
        except: pass
    return {"available": True, **stats}

@app.get("/api/debug/india")
def debug_india():
    """Debug India index and VIX symbols specifically"""
    results = {}
    for ticker in ["^CRSLDX", "^INDIAVIX", "^NSEI", "^NIFTY50"]:
        try:
            df = safe_download(ticker, period="5d")
            if not df.empty:
                close = get_close(df)
                high  = round(float(df["High"].dropna().iloc[-1]), 2) if "High" in df.columns else None
                low   = round(float(df["Low"].dropna().iloc[-1]),  2) if "Low"  in df.columns else None
                results[ticker] = {"ok": True, "rows": len(df), "close": close,
                                   "high": high, "low": low,
                                   "columns": df.columns.tolist()}
            else:
                results[ticker] = {"ok": False, "rows": 0}
        except Exception as e:
            results[ticker] = {"ok": False, "error": str(e)[:80]}
    return results

@app.post("/api/universe/import-nifty500")
async def import_nifty500(payload: dict = {}):
    """
    Import NIFTY 500 CSV from backend/data/nifty500_clean.csv
    Stores 500 tickers + industry mapping in SQLite permanently.
    """
    if not DB_AVAILABLE:
        return {"error": "DB not available"}
    filepath = payload.get("filepath", "data/nifty500_clean.csv")
    # Try relative path from backend folder
    if not os.path.isabs(filepath):
        backend_dir = os.path.dirname(os.path.abspath(__file__))
        filepath = os.path.join(backend_dir, filepath)
    if not os.path.exists(filepath):
        return {"error": f"File not found: {filepath}"}
    n = import_nifty500_csv(filepath)
    # Invalidate cache
    if "breadth_INDIA" in _cache:
        del _cache["breadth_INDIA"]
    return {"ok": True, "tickers_imported": n, "filepath": filepath}


@app.get("/api/scanner/movers")
async def get_top_movers(market: str = "India", limit: int = 10):
    """
    Fast endpoint for Top Movers (Gainers/Losers/Vol Spikes).
    Reads directly from OHLCV DB — NO RS computation needed.
    Returns in ~200ms vs 15-30s for /api/screener/rs.
    """
    import sqlite3
    db_path = pathlib.Path(__file__).parent / "breadth_data.db"
    db_market = "India" if market.upper() == "INDIA" else market

    try:
        conn = sqlite3.connect(str(db_path), timeout=10)
        # Get the last 2 trading dates
        dates = conn.execute(
            "SELECT DISTINCT date FROM ohlcv WHERE market=? ORDER BY date DESC LIMIT 6",
            (db_market,)
        ).fetchall()
        if len(dates) < 2:
            conn.close()
            return {"gainers": [], "losers": [], "vol_spikes": [], "error": "Need at least 2 days of data"}

        latest = dates[0][0]
        prev = dates[1][0]

        # Get today's and yesterday's close + volume for all tickers in one query
        rows = conn.execute("""
            SELECT t.ticker,
                   t.close AS close_today, t.volume AS vol_today, t.high, t.low, t.open,
                   p.close AS close_prev, p.volume AS vol_prev
            FROM ohlcv t
            JOIN ohlcv p ON t.ticker = p.ticker AND p.date = ? AND p.market = ?
            WHERE t.date = ? AND t.market = ?
              AND t.close > 0 AND p.close > 0
        """, (prev, db_market, latest, db_market)).fetchall()

        # Also get 50-day avg volume for vol spike detection
        vol_avg_rows = conn.execute("""
            SELECT ticker, AVG(volume) as avg_vol
            FROM ohlcv
            WHERE market = ? AND date >= date(?, '-60 days')
            GROUP BY ticker
            HAVING COUNT(*) >= 20
        """, (db_market, latest)).fetchall()
        conn.close()

        vol_avg = {r[0]: r[1] for r in vol_avg_rows}

        stocks = []
        for r in rows:
            ticker, close, vol, high, low, opn, prev_close, prev_vol = r
            chg_pct = round((close - prev_close) / prev_close * 100, 2)
            avg_v = vol_avg.get(ticker, vol)
            vol_ratio = round(vol / avg_v, 2) if avg_v > 0 else 1.0
            stocks.append({
                "ticker": ticker,
                "price": round(close, 2),
                "chg_pct": chg_pct,
                "volume": int(vol),
                "vol_ratio": vol_ratio,
            })

        gainers = sorted([s for s in stocks if s["chg_pct"] > 0], key=lambda x: -x["chg_pct"])[:limit]
        losers = sorted([s for s in stocks if s["chg_pct"] < 0], key=lambda x: x["chg_pct"])[:limit]
        vol_spikes = sorted([s for s in stocks if s["vol_ratio"] > 1.5], key=lambda x: -x["vol_ratio"])[:limit]

        return {
            "gainers": gainers,
            "losers": losers,
            "vol_spikes": vol_spikes,
            "date": latest,
            "total_stocks": len(stocks),
        }

    except Exception as e:
        return {"gainers": [], "losers": [], "vol_spikes": [], "error": str(e)}


@app.get("/api/screener/rs")
async def get_rs_rankings(
    market: str = "India",
    min_rs: int = 0,
    min_mcap: float = 0,
    refresh: bool = False
):
    """IBD-Style RS Rankings for NIFTY 500 universe."""
    try:
        cache_key = _rs_cache_key(market)

        if not refresh and cache_key in _rs_cache:
            age = (datetime.now(timezone.utc) - _rs_cache[cache_key]["ts"]).total_seconds()
            if age < RS_CACHE_TTL:
                data = _rs_cache[cache_key]["data"]
                stocks = data.get("stocks", [])
                if min_rs > 0:
                    stocks = [s for s in stocks if (s.get("rs_rating") or 0) >= min_rs]
                if min_mcap > 0:
                    stocks = [s for s in stocks if (s.get("mcap_cr") or 0) >= min_mcap]
                return {**data, "stocks": stocks, "filtered": len(stocks), "cached": True}

        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(executor, _compute_rs_rankings, market)

        if "error" not in result:
            _enrich_stocks_mcap(result.get("stocks", []))
            result.pop("_stock_data", None)
            _rs_cache[cache_key] = {"data": result, "ts": datetime.now(timezone.utc)}

        stocks = result.get("stocks", [])
        if min_rs > 0:
            stocks = [s for s in stocks if (s.get("rs_rating") or 0) >= min_rs]
        if min_mcap > 0:
            stocks = [s for s in stocks if (s.get("mcap_cr") or 0) >= min_mcap]

        return {**result, "stocks": stocks, "filtered": len(stocks)}
    except Exception as e:
        logger.error(f"RS screener error: {e}")
        import traceback; traceback.print_exc()
        return {"error": str(e), "stocks": [], "filtered": 0}


@app.get("/api/leaders")
async def get_leaders(
    market: str = "India",
    min_mcap: float = 0,
    refresh: bool = False
):
    """
    Leaders tab endpoint.
    Returns: regime, sector health, tiered stocks, leader scores.
    Uses cached RS rankings + breadth data — no extra DB queries.
    """
    cache_key = f"leaders_{market}"

    if not refresh and cache_key in _leaders_cache:
        age = (datetime.now(timezone.utc) - _leaders_cache[cache_key]["ts"]).total_seconds()
        if age < LEADERS_CACHE_TTL:
            return {**_leaders_cache[cache_key]["data"], "cached": True}

    loop = asyncio.get_event_loop()

    # Get RS rankings (uses cache if available)
    rs_result = await loop.run_in_executor(executor, _compute_rs_rankings, market)
    if "error" in rs_result:
        return rs_result

    stocks = rs_result.get("stocks", [])
    if not stocks:
        return {"error": "No stock data available"}

    # Get breadth data for Q-BRAM regime
    # Uses same cache as /api/market — key is "breadth_INDIA" or "breadth_US"
    db_market_upper = market.upper() if market.upper() in ("INDIA","US") else "INDIA"
    breadth_cache_key = f"breadth_{db_market_upper}"
    breadth_data = {}
    cached_breadth = get_cache(breadth_cache_key)
    if cached_breadth:
        breadth_data = cached_breadth
    else:
        try:
            # _compute_market returns the full breadth dict
            bd = await loop.run_in_executor(executor, lambda: _compute_market(db_market_upper, custom_tickers=_custom_tickers))
            breadth_data = bd if isinstance(bd, dict) and "error" not in bd else {}
        except Exception as e:
            logger.warning(f"Breadth data unavailable for leaders: {e}")

    # Compute leaders
    result = _compute_leaders(market, stocks, breadth_data)

    # Enrich all tier stocks with mcap data
    for tier_key in (result.get("tiers") or {}):
        _enrich_stocks_mcap(result["tiers"][tier_key])

    # Filter by min_mcap if specified
    if min_mcap > 0:
        for tier_key in (result.get("tiers") or {}):
            result["tiers"][tier_key] = [
                s for s in result["tiers"][tier_key] if s.get("mcap_cr", 0) >= min_mcap
            ]

    result["market"] = market
    result["cached"] = False
    result["total_stocks"] = len(stocks)

    _leaders_cache[cache_key] = {
        "data": result,
        "ts": datetime.now(timezone.utc)
    }
    return result


@app.post("/api/leaders/clear-cache")
async def clear_leaders_cache(market: str = "India"):
    """Clear leaders cache to force refresh."""
    key = f"leaders_{market}"
    if key in _leaders_cache:
        del _leaders_cache[key]
    return {"cleared": True, "market": market}

@app.get("/api/db/check")
async def db_check():
    """Check what's actually in the SQLite DB"""
    if not DB_AVAILABLE:
        return {"error": "DB not available"}
    try:
        conn = __import__('sqlite3').connect(
            str(__import__('pathlib').Path(__file__).parent / 'breadth_data.db'),
            timeout=10
        )
        # Check total rows
        total = conn.execute("SELECT COUNT(*) FROM ohlcv").fetchone()[0]
        # Check market labels
        markets = conn.execute(
            "SELECT market, COUNT(DISTINCT ticker), COUNT(*), MIN(date), MAX(date) FROM ohlcv GROUP BY market"
        ).fetchall()
        # Check ticker_universe
        universe = conn.execute(
            "SELECT market, COUNT(*) FROM ticker_universe GROUP BY market"
        ).fetchall()
        # Check sector_map
        sectors = conn.execute("SELECT COUNT(*) FROM sector_map").fetchone()[0]
        conn.close()

        return {
            "total_ohlcv_rows": total,
            "markets": [{"market":r[0],"tickers":r[1],"rows":r[2],"from":r[3],"to":r[4]} for r in markets],
            "ticker_universe": [{"market":r[0],"count":r[1]} for r in universe],
            "sector_map_rows": sectors,
            "db_available": DB_AVAILABLE,
        }
    except Exception as e:
        return {"error": str(e)}


@app.get("/api/screener/multi")
async def get_multi_screener(
    market: str = "India",
    screeners: str = "rs80",
    min_mcap: float = 0,
    refresh: bool = False
):
    """
    Multi-screener intersection.
    screeners = comma-separated list of screener IDs.
    Returns stocks that pass ALL selected screeners.
    """
    scr_ids = [s.strip() for s in screeners.split(',') if s.strip()]
    cache_key = f"multi_scr_{market}_{'_'.join(sorted(scr_ids))}"

    if not refresh and cache_key in _rs_cache:
        age = (datetime.now(timezone.utc) - _rs_cache[cache_key]["ts"]).total_seconds()
        if age < RS_CACHE_TTL:
            return {**_rs_cache[cache_key]["data"], "cached": True}

    import time
    t0 = time.time()
    loop = asyncio.get_event_loop()

    # Always compute RS rankings as base
    try:
        rs_result = await loop.run_in_executor(executor, _compute_rs_rankings, market)
    except Exception as e:
        logger.error(f"RS rankings failed: {e}")
        return {"error": f"RS rankings error: {str(e)}", "stocks": []}

    if "error" in rs_result:
        return rs_result

    stocks = rs_result.get("stocks", [])
    if not stocks:
        return {"error": "No stocks returned from RS rankings", "stocks": []}

    # Apply each screener filter
    def apply_screener(stocks_list, scr_id):
        """Filter stocks by screener criteria"""
        filtered = []
        for s in stocks_list:
            passes = True
            if scr_id == 'rs90'   and s.get('rs_rating',0) < 90: passes = False
            if scr_id == 'rs80'   and s.get('rs_rating',0) < 80: passes = False
            if scr_id == 'rs_up'  and s.get('rs_trend') != '↑': passes = False
            if scr_id == 'near_high' and (s.get('pct_from_high') or -999) < -5: passes = False
            if scr_id == 'vol_dry' and (s.get('vol_ratio') or 999) >= 0.5: passes = False  # vol < 50% of avg
            if scr_id == 'stage2':
                # Use full trend_template if available, else proxy
                if s.get('trend_template') is not None:
                    passes = s.get('trend_template', False)
                else:
                    passes = (s.get('chg_3m',0) > 0 and s.get('rs_rating',0) >= 60)
            if scr_id == 'mtt':
                # Minervini Trend Template: uses full 8-condition check
                if s.get('trend_template') is not None:
                    passes = (s.get('trend_template', False) and
                              s.get('rs_rating', 0) >= 70)
                else:
                    passes = (s.get('rs_rating',0) >= 70 and
                              (s.get('pct_from_high') or -999) >= -15 and
                              s.get('rs_trend') == '↑')
            if scr_id == 'vcp':
                # VCP: high RS, volume drying up, near 52W high
                passes = (s.get('rs_rating',0) >= 75 and
                          (s.get('vol_ratio') or 999) < 1.2 and
                          (s.get('pct_from_high') or -999) >= -12)
            if scr_id == 'pocket':
                # Pocket pivot: RS>70, today vol > any down day in last 10
                passes = (s.get('rs_rating',0) >= 70 and
                          (s.get('vol_ratio') or 0) >= 1.5)
            if scr_id == 'mean_rev':
                # Mean reversion: oversold, below 20D MA
                passes = (s.get('chg_1m',0) < -5 and
                          (s.get('pct_from_high') or 0) < -15)
            if passes:
                filtered.append(s)
        return filtered

    # Intersect: stock must pass ALL screeners
    # Route: built-in screeners use apply_screener()
    #        custom AFL screeners use apply_custom_screener()
    CUSTOM_IDS = set(CUSTOM_SCREENER_MAP.keys())
    builtin_ids = [sid for sid in scr_ids if sid not in CUSTOM_IDS]
    custom_ids  = [sid for sid in scr_ids if sid in CUSTOM_IDS]

    result_stocks = stocks

    # Apply built-in filters first
    for scr_id in builtin_ids:
        result_stocks = apply_screener(result_stocks, scr_id)

    # Apply custom AFL screeners — need raw OHLCV DataFrames
    if custom_ids:
        # Normalize market for DB query
        db_market = "India" if market.upper() == "INDIA" else market

        # Fetch OHLCV data for remaining stocks only (already filtered by built-ins)
        remaining_tickers = [s['ticker'] for s in result_stocks]
        rs_lookup = {s['ticker']: s.get('rs_rating', 0) for s in result_stocks}

        # Load from screener data (full NSE universe already in memory from RS calc)
        # Re-use get_screener_data which queries SQLite directly
        try:
            raw_data = await loop.run_in_executor(executor, get_screener_data, db_market)
        except Exception as e:
            logger.error(f"Could not load OHLCV for custom screeners: {e}")
            raw_data = {}

        logger.info(f"Custom screeners: {len(custom_ids)} filters on {len(remaining_tickers)} stocks, {len(raw_data)} OHLCV frames available")

        for scr_id in custom_ids:
            filtered = []
            for s in result_stocks:
                ticker = s['ticker']
                df = raw_data.get(ticker)
                if df is None:
                    continue
                try:
                    rs_val = rs_lookup.get(ticker, 0)
                    passes, _ = apply_custom_screener(scr_id, df, rs_rating=rs_val)
                    if passes:
                        filtered.append(s)
                except Exception as e:
                    logger.debug(f"Custom screener {scr_id} error on {ticker}: {e}")
                    continue
            result_stocks = filtered
            logger.info(f"Custom screener '{scr_id}': {len(result_stocks)} stocks pass")

    # Enrich with mcap data
    _enrich_stocks_mcap(result_stocks)

    # Filter by min_mcap if specified
    if min_mcap > 0:
        result_stocks = [s for s in result_stocks if s.get("mcap_cr", 0) >= min_mcap]

    # Tag which screeners each stock matched
    for s in result_stocks:
        s['screeners_matched'] = scr_ids

    # Re-rank
    for i, s in enumerate(result_stocks):
        s['rank'] = i + 1

    elapsed = round(time.time() - t0, 2)
    result = {
        "market": market,
        "screeners": scr_ids,
        "stocks": result_stocks,
        "total": len(result_stocks),
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "elapsed": elapsed,
        "cached": False,
    }
    # Strip internal DataFrame dict before caching (not JSON-serializable)
    result.pop("_stock_data", None)
    _rs_cache[cache_key] = {"data": result, "ts": datetime.now(timezone.utc)}
    return result


# ── NSE Index Universe Endpoints ─────────────────────────────────────────────

@app.get("/api/nse-indices/status")
def nse_indices_status():
    """Get sync status for all NSE indices grouped by category."""
    try:
        return get_index_registry_status()
    except Exception as e:
        return {"error": str(e), "broad": [], "sectoral": [], "thematic": []}

@app.post("/api/nse-indices/sync")
async def nse_indices_sync(background_tasks: BackgroundTasks):
    """Sync all NSE index constituent CSVs from niftyindices.com."""
    if _nse_indices_state["running"]:
        return {"message": "Sync already running", **_nse_indices_state}

    def _run():
        _nse_indices_state["running"]  = True
        _nse_indices_state["progress"] = 0
        _nse_indices_state["message"]  = "Starting NSE index sync..."
        try:
            result = sync_nse_indices(progress_state=_nse_indices_state)
            _nse_indices_state["result"]  = result
            _nse_indices_state["message"] = result["message"]
        except Exception as e:
            _nse_indices_state["message"] = f"Error: {e}"
            _nse_indices_state["result"]  = {"error": str(e)}
        finally:
            _nse_indices_state["running"] = False

    background_tasks.add_task(_run)
    return {"message": f"Syncing {len(NSE_INDEX_REGISTRY)} NSE indices in background. Poll /api/nse-indices/sync/status"}

@app.get("/api/nse-indices/sync/status")
def nse_indices_sync_status():
    return _nse_indices_state

@app.get("/api/nse-indices/constituents")
def nse_index_constituents(index_name: str = "NIFTY 500"):
    """Get all constituent tickers for a given index."""
    try:
        stocks = get_index_constituents(index_name)
        return {"index_name": index_name, "count": len(stocks), "stocks": stocks}
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/nse-indices/ticker")
def nse_ticker_indices(ticker: str):
    """Get all indices a ticker belongs to."""
    try:
        return {"ticker": ticker, "indices": get_ticker_indices(ticker.upper())}
    except Exception as e:
        return {"error": str(e)}

@app.get("/api/nse-indices/list")
def nse_indices_list():
    """Return the full index registry grouped by category."""
    result = {"broad": [], "sectoral": [], "thematic": []}
    for name, (cat, csv_file) in NSE_INDEX_REGISTRY.items():
        result[cat].append({"index_name": name, "csv_file": csv_file})
    return result

@app.get("/api/nse-indices/universe-stats")
def nse_universe_stats():
    """Summary: unique tickers, OHLCV coverage, last sync."""
    try:
        return get_universe_stats()
    except Exception as e:
        return {"error": str(e), "unique_tickers": 0, "tickers_with_ohlcv": 0}

@app.post("/api/nse-indices/backfill-missing")
async def nse_backfill_missing(background_tasks: BackgroundTasks):
    """Backfill 2-year OHLCV for any constituent tickers missing history."""
    if _nse_indices_state["running"]:
        return {"message": "Sync already running"}

    def _run():
        from nse_indices import get_tickers_missing_ohlcv
        from nse_sync import sync_ticker
        _nse_indices_state["running"] = True
        try:
            missing = get_tickers_missing_ohlcv(years=2)
            total = len(missing)
            _nse_indices_state["total"]   = total
            _nse_indices_state["message"] = f"Backfilling {total} tickers with <2y history..."
            done = 0
            for ticker, count, last_date in missing:
                done += 1
                _nse_indices_state["progress"] = done
                _nse_indices_state["message"]  = f"Backfilling {ticker} ({done}/{total}) — had {count} days"
                sync_ticker(ticker, range_str="2y")
                import time; time.sleep(0.2)
            _nse_indices_state["message"] = f"✅ Backfill complete — {total} tickers updated"
        except Exception as e:
            _nse_indices_state["message"] = f"Error: {e}"
        finally:
            _nse_indices_state["running"] = False

    background_tasks.add_task(_run)
    return {"message": "Backfill started — poll /api/nse-indices/sync/status"}


# ── AI Insights Endpoints ─────────────────────────────────────────────────────

@app.get("/api/ai/market-intelligence")
async def ai_market_intelligence(market: str = "INDIA", refresh: bool = False):
    """
    Q-BRAM AI Market Intelligence — Overview tab.
    Generates regime narrative + sector analysis + trading bias.
    """
    # Get breadth data (from cache ideally)
    breadth_key = f"breadth_{market.upper()}"
    breadth_data = get_cache(breadth_key) or {}
    if not breadth_data:
        # Try to compute fresh
        loop = asyncio.get_event_loop()
        try:
            breadth_data = await loop.run_in_executor(
                executor, lambda: _compute_market(market.upper(),
                custom_tickers=_custom_tickers)
            )
        except Exception as e:
            return {"error": f"Could not load breadth data: {e}"}

    if not breadth_data or "error" in breadth_data:
        return {"error": "No breadth data available — run a sync first"}

    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(
        executor, get_market_intelligence, breadth_data
    )
    return result


@app.get("/api/ai/stock-analysis/{ticker}")
async def ai_stock_analysis(ticker: str):
    """
    AI stock setup analysis — Smart Metrics tab.
    Analyses RS, Stage, A/D, Trend Template and provides setup quality.
    """
    ticker = ticker.upper().strip()
    # Get smart metrics first (reuse existing endpoint logic)
    loop = asyncio.get_event_loop()
    metrics = await loop.run_in_executor(executor, get_smart_metrics, ticker)
    if "error" in metrics:
        return {"error": f"Could not load metrics for {ticker}: {metrics['error']}"}

    result = await loop.run_in_executor(
        executor, get_stock_analysis, ticker, metrics
    )
    return result


@app.post("/api/ai/settings")
async def ai_save_settings(payload: dict):
    """Save AI API key to DB."""
    api_key = (payload.get("groq_api_key") or "").strip()
    if not api_key:
        return {"error": "No API key provided"}
    # Validate first
    validation = validate_api_key(api_key)
    if not validation["valid"]:
        return {"error": f"Invalid key: {validation.get('error', 'unknown')}"}
    ok = save_api_key(api_key)
    return {"ok": ok, "message": "Groq API key saved successfully ✅"}


@app.get("/api/ai/settings")
def ai_get_settings():
    """Check if API key is configured (returns masked key)."""
    key = _get_api_key()
    if not key:
        return {"configured": False, "model": "qwen-qwq-32b"}
    masked = key[:8] + "..." + key[-4:]
    return {"configured": True, "masked_key": masked, "model": "qwen-qwq-32b"}


@app.post("/api/ai/validate-key")
async def ai_validate_key(payload: dict):
    """Test if provided API key works."""
    api_key = (payload.get("api_key") or "").strip()
    if not api_key:
        return {"valid": False, "error": "No key provided"}
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(executor, validate_api_key, api_key)
    return result


# ── SMART Screener Endpoints ──────────────────────────────────────────────────

_smart_scr_state: dict = {
    "running": False, "progress": 0, "total": 0,
    "message": "Idle", "result": None
}

@app.post("/api/screener/smart/run")
async def run_smart_screener_endpoint(
    background_tasks: BackgroundTasks,
    min_smart:      int   = 70,
    min_rs:         int   = 60,
    require_stage2: bool  = True,
    min_mcap_cr:    float = 500,
    market:         str   = "India",
    refresh:        bool  = False,
):
    """
    Run SMART Techno-Fundamental screener across full NIFTY universe.
    Two-pass: RS+Stage pre-filter → SMART score candidates.
    Results cached 4 hours — use refresh=true to force recompute.
    """
    if _smart_scr_state["running"]:
        return {"message": "Screener already running", **_smart_scr_state}

    def _run():
        _smart_scr_state["running"]  = True
        _smart_scr_state["progress"] = 0
        _smart_scr_state["message"]  = "Starting SMART screener..."
        try:
            result = run_smart_screener(
                min_smart=min_smart,
                min_rs=min_rs,
                require_stage2=require_stage2,
                min_mcap_cr=min_mcap_cr,
                market=market,
                progress_state=_smart_scr_state,
            )
            _smart_scr_state["result"]  = result
            _smart_scr_state["message"] = result.get("message", "Done")
        except Exception as e:
            _smart_scr_state["message"] = f"Error: {e}"
            _smart_scr_state["result"]  = {"error": str(e), "stocks": []}
        finally:
            _smart_scr_state["running"] = False

    background_tasks.add_task(_run)
    return {
        "message": "SMART screener started — poll /api/screener/smart/status",
        "params": {
            "min_smart": min_smart, "min_rs": min_rs,
            "require_stage2": require_stage2, "min_mcap_cr": min_mcap_cr,
        }
    }


@app.get("/api/screener/smart/status")
def smart_screener_status():
    """Poll SMART screener progress."""
    return _smart_scr_state


@app.get("/api/screener/smart/results")
def smart_screener_results():
    """Get latest SMART screener results (if available)."""
    if _smart_scr_state.get("result"):
        return _smart_scr_state["result"]
    return {"error": "No results yet — run /api/screener/smart/run first",
            "stocks": [], "total": 0}


@app.get("/api/breadth/score-history")
async def get_score_history(market: str = "INDIA", days: int = 30):
    """
    Return actual stored Q-BRAM v2 scores from qbram_score_history table.
    Used by Overview Breadth Charts tab for accurate score history.
    """
    import sqlite3
    db_path = pathlib.Path(__file__).parent / "breadth_data.db"
    try:
        conn = sqlite3.connect(str(db_path), timeout=10)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS qbram_score_history (
                date TEXT NOT NULL, market TEXT NOT NULL,
                score INTEGER, regime TEXT,
                pct_above_50 REAL, nh_nl INTEGER,
                breadth_thrust REAL, csd REAL,
                qbram_version TEXT DEFAULT 'v2',
                PRIMARY KEY (date, market)
            )
        """)
        # Add new columns if upgrading from v1 schema
        for col, ctype in [("breadth_thrust", "REAL"), ("csd", "REAL"), ("qbram_version", "TEXT")]:
            try:
                conn.execute(f"ALTER TABLE qbram_score_history ADD COLUMN {col} {ctype}")
            except:
                pass  # Column already exists
        rows = conn.execute("""
            SELECT date, score, regime, pct_above_50, nh_nl, breadth_thrust, csd, qbram_version
            FROM qbram_score_history
            WHERE market = ?
            ORDER BY date DESC LIMIT ?
        """, (market.upper(), days)).fetchall()
        conn.close()
        history = [
            {"date": r[0], "score": r[1], "regime": r[2],
             "pct_above_50": r[3], "nh_nl": r[4],
             "breadth_thrust": r[5], "csd": r[6],
             "qbram_version": r[7] or "v1"}
            for r in reversed(rows)
        ]
        return {"market": market, "days": len(history), "history": history}
    except Exception as e:
        return {"error": str(e), "history": []}


# ── Insider Trading Endpoints ────────────────────────────────────────────────

@app.get("/api/insider/trades")
async def api_insider_trades(
    days: int = 90,
    type: str = None,
    category: str = None,
    symbol: str = None,
    min_value: float = 0,
    limit: int = 200,
):
    """Get insider trades with optional filters."""
    trades = get_insider_trades(
        days=days, tx_type=type, category=category,
        symbol=symbol, min_value=min_value, limit=limit,
    )
    # Add buy score to each trade
    for t in trades:
        t["score"] = compute_buy_score(t)
    # Sort by score desc for buys, date desc for others
    trades.sort(key=lambda x: (-x["score"], x.get("transaction_date", "")), reverse=False)
    trades.sort(key=lambda x: -x["score"])
    return {"trades": trades, "count": len(trades)}


@app.get("/api/insider/summary")
async def api_insider_summary(days: int = 90):
    """Get insider activity summary stats."""
    return get_insider_summary(days=days)


@app.post("/api/insider/sync")
async def api_insider_sync(background_tasks: BackgroundTasks, days: int = 30):
    """
    Sync insider trades — tries jugaad NSE archives first (Cloudflare-safe),
    falls back to direct NSE API, then suggests CSV import.
    """
    result = await asyncio.get_event_loop().run_in_executor(
        executor, lambda: sync_insider_with_fallback(days_back=days)
    )
    return result


@app.post("/api/insider/repair")
async def api_insider_repair():
    """
    Repair: delete all records with transaction_type='Unknown' (bad parse),
    then re-sync last 90 days with the fixed parser that uses tdpTransactionType.
    """
    import sqlite3 as _sq
    db = os.path.join(os.path.dirname(__file__), "breadth_data.db")
    conn = _sq.connect(db, timeout=10)

    unknown_count = conn.execute(
        "SELECT COUNT(*) FROM insider_trades WHERE transaction_type = 'Unknown'"
    ).fetchone()[0]
    total_before = conn.execute("SELECT COUNT(*) FROM insider_trades").fetchone()[0]

    # Delete ALL records — re-fetch cleanly with fixed parser
    conn.execute("DELETE FROM insider_trades")
    conn.commit()
    conn.close()

    logger.info(f"Repair: deleted {total_before} records ({unknown_count} were Unknown). Re-syncing 90 days...")

    # Re-sync with fixed parser
    result = await asyncio.get_event_loop().run_in_executor(
        executor, lambda: sync_insider_data(days_back=90)
    )
    return {
        "status": "ok",
        "deleted_total": total_before,
        "deleted_unknown": unknown_count,
        "resync": result
    }


@app.post("/api/insider/import-csv")
async def api_insider_import_csv(file: UploadFile):
    """Import insider trades from uploaded NSE CSV file."""
    import tempfile, shutil
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    try:
        shutil.copyfileobj(file.file, tmp)
        tmp.close()
        count = import_insider_csv(tmp.name)
        return {"status": "ok", "imported": count, "filename": file.filename}
    except Exception as e:
        return {"status": "error", "message": str(e)}
    finally:
        try:
            os.unlink(tmp.name)
        except:
            pass


# ── FII/DII Institutional Flow ────────────────────────────────────────────────

@app.get("/api/fiidii/summary")
async def api_fiidii_summary(days: int = 30):
    """Get FII/DII summary with streaks, cumulative flows, sentiment."""
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(executor, lambda: get_fiidii_summary(days))

@app.post("/api/fiidii/sync")
async def api_fiidii_sync(background_tasks: BackgroundTasks, days: int = 30):
    """
    Sync FII/DII data — tries jugaad NSE archives first (Cloudflare-safe),
    falls back to direct NSE API.
    """
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(
        executor, lambda: fetch_fiidii_from_nse(days_back=days)
    )
    return result


@app.post("/api/fiidii/import-csv")
async def api_fiidii_import_csv(file: UploadFile):
    """Import FII/DII historical data from CSV file."""
    content = (await file.read()).decode("utf-8-sig")
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(executor, lambda: import_fiidii_csv(content))
    return result


# ── NSE Data Adapter — new Cloudflare-safe endpoints ─────────────────────────

@app.get("/api/data-sources/health")
async def api_data_sources_health():
    """
    Test connectivity to all data sources:
    - Indian Stock Market API (fundamentals)
    - NSE Archives via jugaad-data (OHLCV bhavcopy, FII/DII, Insider PIT)
    - Direct NSE API (likely Cloudflare-blocked)
    """
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(executor, adapter_health_check)
    return result


@app.post("/api/fundamentals/sync-indian-api")
async def api_fundamentals_sync_indian_api(market: str = "india"):
    """
    Sync fundamentals for all symbols in OHLCV table using Indian Stock Market API.
    Fills in PE, EPS, Market Cap, Sector where TradingView batch left gaps.
    Safe to run alongside existing TV batch — only fills NULL fields.
    """
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(
        executor, lambda: sync_fundamentals_indian_api(market=market)
    )
    return result


@app.post("/api/ohlcv/sync-bhavcopy")
async def api_ohlcv_sync_bhavcopy(
    market: str = "india",
    days_back: int = 365,
):
    """
    Sync OHLCV for India market using NSE bhavcopy archives (jugaad-data).
    Hits nsearchives.nseindia.com — NOT blocked by Cloudflare.
    Useful as fallback when yfinance is rate-limited.

    WARNING: Slow for many symbols — one HTTP request per trading day.
    Use for targeted symbol lists or short date ranges only.
    """
    try:
        conn = sqlite3.connect("breadth_data.db", timeout=10)
        rows = conn.execute(
            "SELECT DISTINCT symbol FROM ohlcv WHERE market = ? LIMIT 500",
            (market,)
        ).fetchall()
        conn.close()
        symbols = [r[0] for r in rows]
    except Exception as e:
        return {"status": "error", "message": str(e)}

    if not symbols:
        return {"status": "no_data", "message": "No symbols found — run primary OHLCV sync first"}

    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(
        executor, lambda: store_ohlcv_bhavcopy(symbols, days_back=days_back, market=market)
    )
    return result


# ── F-Value Fundamental Screener ──────────────────────────────────────────────

@app.get("/api/fvalue")
async def api_fvalue_screener(
    min_grade: str = None,
    fv_filter: str = None,
    sector: str = None,
    limit: int = 500,
    market: str = "India",
):
    """
    F-Value Screener — Quality grades (A-E) + Fair Value estimation.
    Supports both India and US markets.
    """
    db_market = "India" if market.upper() in ("INDIA",) else "US"
    cache_key = f"fvalue_{db_market}"
    if not min_grade and not fv_filter and not sector:
        cached = get_cache(cache_key)
        if cached:
            stocks = cached.get("stocks", [])[:limit]
            return {**cached, "stocks": stocks, "cached": True}

    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(
        executor, lambda: run_fvalue_screener(
            min_grade=min_grade, fv_filter=fv_filter,
            sector=sector, limit=limit, market=db_market,
        )
    )
    if not min_grade and not fv_filter and not sector and result.get("stocks"):
        set_cache(cache_key, result)
    return result


_tv_fund_state: dict = {"running": False, "message": "Idle", "count": 0}

@app.post("/api/fundamentals/tv-sync")
async def tv_fundamentals_sync(background_tasks: BackgroundTasks):
    """
    Fetch fundamental summary for ALL NSE stocks from TradingView in one call.
    Stores PE, ROE, EPS, Margins, D/E for every stock in tv_fundamentals table.
    Takes ~5-10 seconds. Run once daily (auto-stale after 24h).
    """
    if _tv_fund_state["running"]:
        return {"message": "Sync already running", **_tv_fund_state}

    def _run():
        _tv_fund_state["running"] = True
        _tv_fund_state["message"] = "Fetching from TradingView..."
        try:
            result = fetch_batch_fundamentals(market="india")
            count = len(result)
            _tv_fund_state["count"]   = count
            _tv_fund_state["message"] = f"✅ {count} stocks synced from TradingView"
        except Exception as e:
            _tv_fund_state["message"] = f"Error: {e}"
        finally:
            _tv_fund_state["running"] = False

    background_tasks.add_task(_run)
    return {"message": "TradingView fundamentals sync started (~5-10 seconds)"}


@app.get("/api/fundamentals/tv-sync/status")
def tv_fundamentals_status():
    fresh = is_batch_fresh(max_age_hours=24)
    return {**_tv_fund_state, "fresh": fresh}


@app.get("/api/fundamentals/tv/{ticker}")
async def tv_fundamental_detail(ticker: str):
    """Get fundamental detail for one ticker from TradingView (cached 24h)."""
    from tv_fundamentals import fetch_ticker_detail
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(
        executor, fetch_ticker_detail, ticker.upper()
    )
    return result


@app.post("/api/fundamentals/prefetch-quarterly")
async def prefetch_quarterly_fundamentals(background_tasks: BackgroundTasks,
                                           max_tickers: int = 200):
    """
    Pre-fetch and cache quarterly financial statements for top RS tickers.
    Run this before SMART Screener for faster results.
    Caches 24h in tv_fundamentals_detail table.
    """
    if _tv_fund_state["running"]:
        return {"message": "A sync is already running"}

    def _run():
        _tv_fund_state["running"] = True
        _tv_fund_state["message"] = "Prefetching quarterly data..."
        try:
            import sqlite3 as _sql
            from tv_fundamentals import fetch_ticker_detail, _get_cached_detail

            # Get top candidates by RS proxy (from OHLCV)
            db = pathlib.Path(__file__).parent / "breadth_data.db"
            conn = _sql.connect(str(db), timeout=15)
            rows = conn.execute("""
                SELECT ticker FROM (
                    SELECT ticker,
                           (MAX(close) - MIN(close)) / MIN(close) * 100 as momentum
                    FROM ohlcv
                    WHERE market='India'
                    GROUP BY ticker
                    HAVING COUNT(*) >= 60
                    ORDER BY momentum DESC
                    LIMIT ?
                )
            """, (max_tickers,)).fetchall()
            conn.close()
            tickers = [r[0] for r in rows]

            total = len(tickers)
            _tv_fund_state["total"] = total
            fetched = 0
            skipped = 0

            for i, ticker in enumerate(tickers):
                _tv_fund_state["progress"] = i + 1
                _tv_fund_state["message"] = (
                    f"Prefetch: {ticker} ({i+1}/{total})"
                    f" — {fetched} fetched, {skipped} cached"
                )
                # Skip if already cached
                if _get_cached_detail(ticker):
                    skipped += 1
                    continue
                fetch_ticker_detail(ticker)  # fetches + caches
                fetched += 1
                import time as _t
                _t.sleep(0.3)

            _tv_fund_state["message"] = (
                f"✅ Prefetch done: {fetched} fetched, {skipped} already cached"
            )
        except Exception as e:
            _tv_fund_state["message"] = f"Prefetch error: {e}"
        finally:
            _tv_fund_state["running"] = False

    background_tasks.add_task(_run)
    return {"message": f"Prefetching quarterly data for top {max_tickers} tickers"}

# ── Startup: load disk cache + pre-warm breadth in background ───────────────
@app.on_event("startup")
async def startup_event():
    """On startup: load disk cache first (instant), then pre-warm if stale."""
    # 1. Load disk cache immediately — dashboard shows data right away
    _load_disk_cache()
    _build_sector_map()  # load ticker→sector map

    # Load Groq API key from environment variable if set and not already in DB
    try:
        import os as _os, sqlite3 as _sq
        _env_key = _os.environ.get("GROQ_API_KEY", "").strip()
        if _env_key:
            _conn = _sq.connect(str(pathlib.Path(__file__).parent / "breadth_data.db"), timeout=10)
            _conn.execute("""CREATE TABLE IF NOT EXISTS app_settings
                (key TEXT PRIMARY KEY, value TEXT, updated TEXT)""")
            _existing = _conn.execute(
                "SELECT value FROM app_settings WHERE key='groq_api_key'"
            ).fetchone()
            if not _existing or not _existing[0]:
                _conn.execute("""INSERT OR REPLACE INTO app_settings (key,value,updated)
                    VALUES ('groq_api_key',?,?)""",
                    (_env_key, datetime.now(timezone.utc).isoformat()))
                _conn.commit()
                logger.info("✅ Groq API key loaded from GROQ_API_KEY env var")
            _conn.close()
    except Exception as _e:
        logger.warning(f"API key env load failed: {_e}")

    # Ensure TradingView fundamentals tables exist + auto-sync if stale
    try:
        ensure_tv_tables()
        logger.info("✅ TV fundamentals tables ready")
        # Auto-refresh TV batch fundamentals if >24h stale or empty
        if not is_batch_fresh(max_age_hours=24):
            logger.info("⏳ TV fundamentals stale — auto-syncing in background...")
            import asyncio as _aio
            async def _auto_tv_sync():
                await _aio.sleep(10)   # wait for server to finish starting
                try:
                    result = fetch_batch_fundamentals(market="india")
                    logger.info(f"✅ Auto TV sync: {len(result)} tickers refreshed")
                except Exception as _e:
                    logger.warning(f"Auto TV sync failed: {_e}")
            _aio.ensure_future(_auto_tv_sync())
    except Exception as e:
        logger.warning(f"TV fundamentals table init failed: {e}")

    # Ensure NSE index tables exist
    try:
        ensure_nse_tables()
        logger.info("✅ NSE index tables ready")
    except Exception as e:
        logger.warning(f"NSE index table init failed: {e}")

    # 1b. Auto-import market cap data if table is empty
    try:
        ensure_mcap_table()
        import sqlite3 as _sql
        _mc_conn = _sql.connect(str(pathlib.Path(__file__).parent / "breadth_data.db"), timeout=10)
        _mc_count = _mc_conn.execute("SELECT COUNT(*) FROM market_cap").fetchone()[0]
        _mc_conn.close()
        if _mc_count == 0:
            csv_path = pathlib.Path(__file__).parent / "data" / "market_cap.csv"
            if csv_path.exists():
                result = import_market_cap_csv(str(csv_path))
                logger.info(f"✅ Auto-imported market cap: {result['matched']} matched, {result['total_in_db']} in DB")
            else:
                logger.info("⏭ No market_cap.csv found — skipping auto-import")
        else:
            logger.info(f"✅ Market cap table already has {_mc_count} entries")
    except Exception as e:
        logger.warning(f"Market cap auto-import failed: {e}")

    # 2. Check if breadth cache is fresh enough
    india_cache = get_cache("breadth_INDIA")
    if india_cache:
        logger.info("✅ Startup: India breadth loaded from disk cache instantly")
    else:
        logger.info("⏳ Startup: No fresh cache — pre-warming India breadth in background")
        # Pre-warm in background so first dashboard load is fast
        asyncio.create_task(_prewarm_breadth())

    # 3. Insider trading — ensure tables + auto-sync last 7 days
    try:
        _ensure_insider_tables()
        logger.info("✅ Insider trading tables ready")
        asyncio.create_task(_auto_sync_insider())
    except Exception as e:
        logger.warning(f"Insider table init failed: {e}")

    # 3b. Auth tables
    try:
        ensure_auth_tables()
        logger.info("✅ Auth tables ready")
    except Exception as e:
        logger.warning(f"Auth table init failed: {e}")

    # 4. Start daily insider sync scheduler (runs at ~6:30 PM IST / 1 PM UTC)
    asyncio.create_task(_insider_daily_scheduler())

    # 4b. Auto-sync FII/DII data from NSE
    asyncio.create_task(_auto_sync_fiidii())

    # 4c. Auto-sync OHLCV data — ensures all prices are fresh
    asyncio.create_task(_auto_sync_ohlcv())

    # 5. Pre-warm RS rankings + Leaders + F-Value in background
    #    These are the slowest computations — do them once on startup so tabs load instantly
    #    NOTE: Waits for OHLCV sync to finish first (via delay in _prewarm_heavy_caches)
    asyncio.create_task(_prewarm_heavy_caches())


async def _auto_sync_ohlcv():
    """Auto-sync stale OHLCV data on startup. Quick 5-day update for all stale tickers."""
    await asyncio.sleep(5)  # let server finish starting
    loop = asyncio.get_event_loop()
    try:
        from nse_sync import _get_stale_tickers, sync_ticker
        stale = await loop.run_in_executor(executor, lambda: _get_stale_tickers(days_threshold=1))
        if not stale:
            logger.info("✅ OHLCV data is fresh — no sync needed")
            return

        total = len(stale)
        logger.info(f"⏳ Auto-syncing OHLCV: {total} stale tickers (5-day update)...")

        updated = 0
        failed = 0
        batch_size = 8

        tickers_to_sync = [t for t, _ in stale]
        for i in range(0, total, batch_size):
            batch = tickers_to_sync[i:i + batch_size]
            # Run batch in thread pool
            results = await asyncio.gather(*[
                loop.run_in_executor(executor, sync_ticker, t, "5d")
                for t in batch
            ])
            for ticker, n_rows, error in results:
                if error:
                    failed += 1
                else:
                    updated += 1

            # Progress log every 100 tickers
            done = min(i + batch_size, total)
            if done % 100 == 0 or done == total:
                logger.info(f"  OHLCV sync progress: {done}/{total} ({updated} updated, {failed} failed)")

            # Brief pause to avoid rate limiting
            await asyncio.sleep(0.3)

        logger.info(f"✅ OHLCV auto-sync complete: {updated} updated, {failed} failed out of {total}")

        # Invalidate breadth cache so next request recomputes with fresh data
        from cache import delete_cache
        delete_cache("breadth_INDIA")
        logger.info("🔄 Breadth cache invalidated — will recompute with fresh OHLCV")

    except Exception as e:
        logger.warning(f"OHLCV auto-sync failed: {e}")
        import traceback; traceback.print_exc()

async def _prewarm_heavy_caches():
    """Pre-compute RS rankings, Leaders, and F-Value in background after OHLCV sync."""
    # Wait for OHLCV auto-sync to finish (typically 2-5 minutes for 2500 tickers)
    # Check every 10s if data is fresh
    from nse_sync import _get_stale_tickers
    for attempt in range(30):  # max 5 minutes wait
        await asyncio.sleep(10)
        try:
            stale = _get_stale_tickers(days_threshold=1)
            stale_count = len(stale)
            if stale_count < 50:  # less than 50 stale = good enough
                logger.info(f"✅ OHLCV data fresh enough ({stale_count} stale) — starting pre-warm")
                break
            if attempt % 3 == 0:
                logger.info(f"⏳ Waiting for OHLCV sync... {stale_count} still stale")
        except Exception:
            break
    else:
        logger.warning("⏭ OHLCV sync taking too long — pre-warming with current data")
    loop = asyncio.get_event_loop()

    # 1. RS Rankings (used by Scanner + Screeners)
    try:
        rs_cache_key = _rs_cache_key("India")
        if rs_cache_key not in _rs_cache:
            logger.info("⏳ Pre-warming RS rankings (this takes 15-30s)...")
            result = await loop.run_in_executor(executor, _compute_rs_rankings, "India")
            if "error" not in result:
                _enrich_stocks_mcap(result.get("stocks", []))
                result.pop("_stock_data", None)
                _rs_cache[rs_cache_key] = {"data": result, "ts": datetime.now(timezone.utc)}
                logger.info(f"✅ RS rankings pre-warmed: {len(result.get('stocks', []))} stocks")
            else:
                logger.warning(f"RS pre-warm returned error: {result.get('error')}")
        else:
            logger.info("✅ RS rankings already cached")
    except Exception as e:
        logger.warning(f"RS pre-warm failed: {e}")

    # 2. Leaders (uses RS + breadth data)
    try:
        leaders_key = "leaders_India"
        if leaders_key not in _leaders_cache:
            logger.info("⏳ Pre-warming Leaders...")
            # Get cached breadth and RS data
            breadth_data = get_cache("breadth_INDIA") or {}
            rs_data = _rs_cache.get(_rs_cache_key("India"), {}).get("data", {})
            stocks = rs_data.get("stocks", [])
            if stocks:
                result = await loop.run_in_executor(
                    executor, _compute_leaders, "India", stocks, breadth_data
                )
                if result:
                    _leaders_cache[leaders_key] = {"data": result, "ts": datetime.now(timezone.utc)}
                    logger.info(f"✅ Leaders pre-warmed")
        else:
            logger.info("✅ Leaders already cached")
    except Exception as e:
        logger.warning(f"Leaders pre-warm failed: {e}")

    # 3. F-Value (uses tv_fundamentals — fast SQL, ~1s)
    try:
        logger.info("⏳ Pre-warming F-Value screener...")
        result = await loop.run_in_executor(executor, run_fvalue_screener)
        if result.get("stocks"):
            logger.info(f"✅ F-Value pre-warmed: {result.get('total', 0)} stocks graded")
    except Exception as e:
        logger.warning(f"F-Value pre-warm failed: {e}")

async def _prewarm_breadth():
    """Pre-compute breadth in background so first load is instant."""
    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(executor, lambda: _compute_market("INDIA", custom_tickers=_custom_tickers))
        if "error" not in result:
            set_cache("breadth_INDIA", result)
            logger.info(f"✅ Pre-warm done: India breadth cached ({result.get('universe_size',0)} stocks)")
    except Exception as e:
        logger.warning(f"Pre-warm failed: {e}")


async def _auto_sync_insider():
    """Auto-sync last 7 days of insider trades on startup."""
    await asyncio.sleep(15)  # wait for server to finish starting
    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(
            executor, lambda: sync_insider_data(days_back=7)
        )
        if result.get("status") == "ok":
            logger.info(f"✅ Insider auto-sync: {result.get('stored', 0)} trades from last 7 days")
        else:
            logger.info(f"⏭ Insider auto-sync: {result.get('message', 'No data')} — use CSV import")
    except Exception as e:
        logger.warning(f"Insider auto-sync failed: {e}")


async def _auto_sync_fiidii():
    """Auto-fetch FII/DII data from NSE on startup."""
    await asyncio.sleep(15)  # Wait for server to stabilize
    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(executor, lambda: fetch_fiidii_from_nse(days_back=60))
        if result.get("entries", 0) > 0:
            logger.info(f"✅ FII/DII auto-sync: {result['entries']} entries (latest: {result.get('latest_date')})")
        else:
            logger.info(f"⏭ FII/DII auto-sync: {result.get('error', 'No data')}")
    except Exception as e:
        logger.warning(f"FII/DII auto-sync failed: {e}")


async def _insider_daily_scheduler():
    """
    Background scheduler: auto-fetch insider data daily at ~6:30 PM IST.
    NSE publishes PIT disclosures throughout the day, most arrive by 6 PM.
    Also runs every 6 hours as a fallback.
    """
    import pytz
    INTERVAL_HOURS = 6  # fallback: sync every 6 hours

    await asyncio.sleep(60)  # initial delay

    while True:
        try:
            # Check if it's a good time to sync (after market hours IST)
            try:
                ist = pytz.timezone("Asia/Kolkata")
                now_ist = datetime.now(ist)
                hour = now_ist.hour
                is_weekday = now_ist.weekday() < 5
                # Best window: 6-8 PM IST on weekdays
                if is_weekday and 18 <= hour <= 20:
                    logger.info("🔄 Insider daily scheduler: syncing (6-8 PM IST window)...")
                    loop = asyncio.get_event_loop()
                    result = await loop.run_in_executor(
                        executor, lambda: sync_insider_data(days_back=3)
                    )
                    if result.get("status") == "ok":
                        logger.info(f"✅ Insider daily sync: {result.get('stored', 0)} new trades")
                    else:
                        logger.debug(f"Insider daily sync: {result.get('message', 'no data')}")
            except ImportError:
                # pytz not installed — just sync every INTERVAL_HOURS
                loop = asyncio.get_event_loop()
                result = await loop.run_in_executor(
                    executor, lambda: sync_insider_data(days_back=3)
                )
                if result.get("status") == "ok":
                    logger.info(f"✅ Insider periodic sync: {result.get('stored', 0)} new trades")
        except Exception as e:
            logger.warning(f"Insider scheduler error: {e}")

        # Sleep for INTERVAL_HOURS
        await asyncio.sleep(INTERVAL_HOURS * 3600)


@app.post("/api/cache/clear-breadth")
async def clear_breadth_cache():
    """Nuclear option: clear ALL breadth caches (memory + disk + peep_cache).
    Use when cached data is corrupt/stale (e.g. showing wrong score)."""
    cleared = []
    # 1. Clear in-memory breadth entries
    keys_to_remove = [k for k in _cache if k.startswith("breadth_")]
    for k in keys_to_remove:
        del _cache[k]
        cleared.append(k)
    # 2. Delete disk cache file
    disk_file = pathlib.Path(__file__).parent / "breadth_cache.json"
    if disk_file.exists():
        disk_file.unlink()
        cleared.append("breadth_cache.json")
    # 3. Clear peep_cache table
    try:
        import sqlite3
        db_path = str(pathlib.Path(__file__).parent / "breadth_data.db")
        conn = sqlite3.connect(db_path, timeout=10)
        conn.execute("DELETE FROM peep_cache")
        conn.commit()
        conn.close()
        cleared.append("peep_cache_table")
    except Exception as e:
        logger.warning(f"Could not clear peep_cache: {e}")
    # 4. Trigger fresh recompute
    asyncio.create_task(_prewarm_breadth())
    logger.info(f"🗑️ Breadth cache cleared: {cleared}. Recomputing...")
    return {"ok": True, "cleared": cleared, "message": "All breadth caches cleared. Recomputing fresh data..."}


@app.post("/api/screener/clear-cache")
async def clear_screener_cache(payload: dict = {}):
    """
    Clear screener cache for a specific market+screener combo.
    Called by Force Refresh button — forces fresh recompute on next run.
    """
    market   = payload.get("market", "India")
    screeners = payload.get("screeners", [])

    cleared = []

    # Clear specific screener combo cache
    if screeners:
        key = f"multi_scr_{market}_{'_'.join(sorted(screeners))}"
        if key in _rs_cache:
            del _rs_cache[key]
            cleared.append(key)

    # Also clear the base RS rankings cache for this market
    rs_key = f"rs_rankings_{market}"
    if rs_key in _rs_cache:
        del _rs_cache[rs_key]
        cleared.append(rs_key)

    # Clear ALL screener caches for this market (catches all combos)
    keys_to_del = [k for k in list(_rs_cache.keys()) if market.upper() in k.upper() or market.lower() in k.lower()]
    for k in keys_to_del:
        if k not in cleared:
            del _rs_cache[k]
            cleared.append(k)

    logger.info(f"Force refresh: cleared {len(cleared)} cache entries: {cleared}")
    return {
        "ok": True,
        "cleared": len(cleared),
        "keys": cleared,
        "message": f"Cache cleared — next screener run will fetch fresh data"
    }


# ── Stockbee Market Breadth ──────────────────────────────────────────────────

@app.get("/api/stockbee/{market}")
async def get_stockbee(market: str, refresh: bool = False):
    market = market.upper()
    if market not in ("INDIA", "US"):
        return {"error": "Market must be INDIA or US"}
    key = f"stockbee_{market}"
    if not refresh:
        cached = get_cache(key)
        if cached:
            return {**cached, "cached": True}
    loop = asyncio.get_event_loop()
    result = await loop.run_in_executor(executor, _compute_stockbee, market)
    if "error" not in result:
        set_cache(key, result)
    return {**result, "cached": False}


@app.get("/api/chart/{ticker}")
async def chart_data(ticker: str, tf: str = "daily", days: int = None):
    from fastapi.responses import JSONResponse
    loop = asyncio.get_event_loop()
    data = await loop.run_in_executor(executor, get_chart_data, ticker, tf, days)
    if "error" in data:
        return JSONResponse(status_code=404, content=data)
    return data


# ── Stock Metrics ─────────────────────────────────────────────────────────────

@app.get("/api/stock-metrics/{ticker}")
async def stock_metrics(ticker: str):
    """Compute 9 key metrics for a ticker (fast, from DB data). EPS excluded."""
    loop = asyncio.get_event_loop()
    data = await loop.run_in_executor(executor, compute_stock_metrics, ticker)
    if "error" in data:
        from fastapi.responses import JSONResponse
        return JSONResponse(status_code=404, content=data)
    return data

@app.get("/api/stock-metrics/{ticker}/eps")
async def stock_metrics_eps(ticker: str):
    """Fetch EPS from Yahoo Finance (async, don't block chart loading)."""
    loop = asyncio.get_event_loop()
    data = await loop.run_in_executor(executor, compute_eps_async, ticker)
    return data


# ── Smart Metrics (Techno-Fundamental Analysis) ──────────────────────────────

@app.get("/api/smart-metrics/{ticker}")
async def smart_metrics(ticker: str):
    """Full techno-fundamental analysis: screener.in scrape + OM score + technicals + Smart Score."""
    loop = asyncio.get_event_loop()
    data = await loop.run_in_executor(executor, get_smart_metrics, ticker)
    return data


# ── Smart Chart API ──────────────────────────────────────────────────────────

@app.get("/api/smart-chart/{ticker}")
async def smart_chart_data(ticker: str, days: int = 500):
    """
    All-in-one data for Smart Chart: OHLCV + RS + A/D + fundamentals + screener.in data.
    Returns everything the Smart Chart tab needs in a single call.
    """
    loop = asyncio.get_event_loop()
    try:
        # 1. OHLCV + technicals from our DB
        chart = await loop.run_in_executor(executor, get_chart_data, ticker, "daily", days)
        if "error" in chart:
            return {"error": chart["error"], "ticker": ticker}

        # 2. RS + A/D from screener cache (if available)
        rs_data = {}
        cache_key = _rs_cache_key("India")
        if cache_key in _rs_cache:
            stocks = _rs_cache[cache_key]["data"].get("stocks", [])
            for s in stocks:
                if s.get("ticker") == ticker.upper():
                    rs_data = {
                        "rs_rating": s.get("rs_rating"),
                        "rs_trend": s.get("rs_trend"),
                        "ad_rating": s.get("ad_rating"),
                        "ad_line": s.get("ad_line_val"),
                        "pct_from_high": s.get("pct_from_high"),
                        "vol_ratio": s.get("vol_ratio"),
                        "chg_1w": s.get("chg_1w"),
                        "chg_1m": s.get("chg_1m"),
                        "chg_3m": s.get("chg_3m"),
                        "sector": s.get("sector"),
                        "mcap_cr": s.get("mcap_cr"),
                        "trend_template": s.get("trend_template"),
                    }
                    break

        # 3. TV Fundamentals (PE, ROE, etc.)
        tv_data = {}
        try:
            from tv_fundamentals import get_screener_data_fast
            tv_data = get_screener_data_fast(ticker) or {}
        except Exception:
            pass

        # 4. Screener.in data (quarterly, shareholding, ratios)
        scr_data = {}
        try:
            from screener_in import get_screener_in_data
            scr_data = await loop.run_in_executor(executor, get_screener_in_data, ticker)
        except Exception as e:
            logger.warning(f"Smart Chart screener.in error for {ticker}: {e}")
            scr_data = {"quarterly": [], "annual": [], "shareholding": [], "ratios": {}}

        # 5. Sector peers (top RS in same sector)
        peers = []
        if rs_data.get("sector") and cache_key in _rs_cache:
            sector = rs_data["sector"]
            all_stocks = _rs_cache[cache_key]["data"].get("stocks", [])
            sector_stocks = [s for s in all_stocks if s.get("sector") == sector and s.get("ticker") != ticker.upper()]
            sector_stocks.sort(key=lambda x: x.get("rs_rating", 0), reverse=True)
            peers = [{"ticker": s["ticker"], "rs": s.get("rs_rating", 0), "ad": s.get("ad_rating", "?")}
                     for s in sector_stocks[:5]]

        return {
            "ticker": ticker.upper(),
            "chart": chart,
            "rs": rs_data,
            "fundamentals": tv_data,
            "screener_in": scr_data,
            "peers": peers,
        }
    except Exception as e:
        logger.error(f"Smart Chart error for {ticker}: {e}")
        import traceback; traceback.print_exc()
        return {"error": str(e), "ticker": ticker}


# ── Ticker Search (Autocomplete) ──────────────────────────────────────────────

@app.get("/api/tickers/search")
def ticker_search(q: str = "", limit: int = 10):
    """Search tickers by ticker prefix or company name substring. Sorted by mcap."""
    q = q.strip().upper()
    if len(q) < 2:
        return []
    import sqlite3
    conn = sqlite3.connect(str(pathlib.Path(__file__).parent / "breadth_data.db"), timeout=10)
    try:
        rows = conn.execute("""
            SELECT ticker, company_name, mcap_cr, mcap_tier
            FROM market_cap
            WHERE UPPER(ticker) LIKE ? OR UPPER(company_name) LIKE ?
            ORDER BY mcap_cr DESC
            LIMIT ?
        """, (f"{q}%", f"%{q}%", limit)).fetchall()
    except Exception:
        rows = []
    conn.close()

    # Also search ohlcv tickers not in market_cap (fallback)
    if len(rows) < limit:
        existing = {r[0] for r in rows}
        try:
            conn2 = sqlite3.connect(str(pathlib.Path(__file__).parent / "breadth_data.db"), timeout=10)
            ohlcv_rows = conn2.execute(
                "SELECT DISTINCT ticker FROM ohlcv WHERE UPPER(ticker) LIKE ? LIMIT ?",
                (f"{q}%", limit)
            ).fetchall()
            conn2.close()
            for r in ohlcv_rows:
                if r[0] not in existing and len(rows) < limit:
                    rows.append((r[0], "", 0, ""))
        except Exception:
            pass

    # Also add sector from sector_map
    try:
        smap = load_sector_map()
    except Exception:
        smap = {}
    return [
        {
            "ticker": r[0],
            "company_name": r[1] or "",
            "mcap_tier": r[3] or "",
            "sector": (smap.get(r[0], {}) or {}).get("sector", "") if isinstance(smap.get(r[0]), dict) else "",
        }
        for r in rows
    ]


# ── Fetch Live (Force Refresh) ───────────────────────────────────────────────

@app.post("/api/fetch-live")
async def fetch_live(background_tasks: BackgroundTasks):
    """Force live data refresh — clears all caches and syncs stale tickers."""
    # 1. Clear breadth cache
    _cache.clear()
    try:
        disk_cache = pathlib.Path(__file__).parent / "breadth_cache.json"
        if disk_cache.exists():
            disk_cache.unlink()
    except Exception:
        pass

    # 2. Clear screener/leaders/RS caches
    _rs_cache.clear()
    _leaders_cache.clear()

    # 3. Sync ALL tickers that don't have today's data
    try:
        from nse_sync import _get_stale_tickers, sync_nifty500
        stale = _get_stale_tickers(days_threshold=0)  # anything not updated today
        logger.info(f"Fetch Live: {len(stale)} tickers need today's data (full universe)")
        if stale:
            result = sync_nifty500(range_str="5d", max_workers=10)
        else:
            result = {"message": "All tickers already have today's data", "updated": 0}
    except Exception as e:
        result = {"error": str(e)}

    return {"message": "Live data refreshed", "sync": result}


# ── Sector Heatmap ─────────────────────────────────────────────────────────────

@app.get("/api/sectors/heatmap")
async def sectors_heatmap(market: str = "India", period: str = "1m"):
    loop = asyncio.get_event_loop()
    data = await loop.run_in_executor(executor, compute_sector_heatmap, market, period)
    return data


@app.get("/api/sectors/rotation")
async def sectors_rotation(market: str = "India"):
    """
    Sector Rotation Map (RRG-style scatter plot).
    X-axis: RS (% above 50 DMA) — relative strength
    Y-axis: Performance (15-day avg return) — momentum
    Bubble size: stock count in sector
    """
    loop = asyncio.get_event_loop()
    data = await loop.run_in_executor(executor, _compute_rotation, market)
    return data


def _compute_rotation(market: str = "India") -> dict:
    """Compute sector rotation data — bulk SQL, no per-ticker queries."""
    import sqlite3
    db_path = pathlib.Path(__file__).parent / "breadth_data.db"
    db_market = "India" if market.upper() == "INDIA" else market

    try:
        conn = sqlite3.connect(str(db_path), timeout=30)
        conn.row_factory = sqlite3.Row

        # Get sector map
        sec_rows = conn.execute("SELECT ticker, sector FROM sector_map WHERE sector IS NOT NULL AND sector != ''").fetchall()
        if not sec_rows:
            conn.close()
            return {"sectors": [], "error": "No sector map"}

        ticker_to_sector = {}
        sector_tickers = {}
        for r in sec_rows:
            ticker_to_sector[r["ticker"]] = r["sector"]
            sector_tickers.setdefault(r["sector"], []).append(r["ticker"])

        # Get latest date
        latest = conn.execute("SELECT MAX(date) as d FROM ohlcv WHERE market=?", (db_market,)).fetchone()
        if not latest or not latest["d"]:
            conn.close()
            return {"sectors": [], "error": "No OHLCV data"}
        latest_date = latest["d"]

        lookback_15d = (datetime.strptime(latest_date, "%Y-%m-%d") - timedelta(days=25)).strftime("%Y-%m-%d")
        dma50_date = (datetime.strptime(latest_date, "%Y-%m-%d") - timedelta(days=80)).strftime("%Y-%m-%d")

        # BULK: Get latest close for all tickers
        latest_rows = conn.execute(
            "SELECT ticker, close, volume FROM ohlcv WHERE date=? AND market=?",
            (latest_date, db_market)
        ).fetchall()
        latest_prices = {r["ticker"]: (float(r["close"] or 0), float(r["volume"] or 0)) for r in latest_rows}

        # BULK: Get ~15d ago close for all tickers
        past_rows = conn.execute("""
            SELECT o.ticker, o.close FROM ohlcv o
            INNER JOIN (
                SELECT ticker, MAX(date) as max_date FROM ohlcv
                WHERE market=? AND date <= ? AND date >= ?
                GROUP BY ticker
            ) sub ON o.ticker = sub.ticker AND o.date = sub.max_date
            WHERE o.market=?
        """, (db_market, lookback_15d, (datetime.strptime(lookback_15d, "%Y-%m-%d") - timedelta(days=10)).strftime("%Y-%m-%d"), db_market)).fetchall()
        past_prices = {r["ticker"]: float(r["close"] or 0) for r in past_rows}

        # BULK: Get 50-day moving average for all tickers
        dma_rows = conn.execute("""
            SELECT ticker, AVG(close) as dma50, COUNT(*) as cnt
            FROM (
                SELECT ticker, close FROM ohlcv
                WHERE market=? AND date >= ? AND close > 0
                ORDER BY date DESC
            )
            GROUP BY ticker
            HAVING cnt >= 30
        """, (db_market, dma50_date)).fetchall()
        dma50_map = {r["ticker"]: float(r["dma50"]) for r in dma_rows}

        # BULK: Get 50-day avg volume
        vol_rows = conn.execute("""
            SELECT ticker, AVG(volume) as avg_vol
            FROM ohlcv
            WHERE market=? AND date >= ?
            GROUP BY ticker
            HAVING COUNT(*) >= 20
        """, (db_market, dma50_date)).fetchall()
        avg_vol_map = {r["ticker"]: float(r["avg_vol"] or 1) for r in vol_rows}

        conn.close()

        # Aggregate per sector
        sectors_out = []
        all_rs = []
        all_perf = []

        for sector, tickers in sector_tickers.items():
            perfs = []
            above_50 = 0
            valid_50 = 0
            vol_ratios = []

            for t in tickers:
                c_now = latest_prices.get(t, (0, 0))[0]
                c_past = past_prices.get(t, 0)
                if c_now <= 0 or c_past <= 0:
                    continue

                perf = ((c_now - c_past) / c_past) * 100
                perfs.append(perf)

                # 50 DMA check
                dma50 = dma50_map.get(t)
                if dma50 and dma50 > 0:
                    valid_50 += 1
                    if c_now > dma50:
                        above_50 += 1

                # Vol ratio
                today_vol = latest_prices.get(t, (0, 0))[1]
                avg_v = avg_vol_map.get(t, 1)
                if avg_v > 0 and today_vol > 0:
                    vol_ratios.append(today_vol / avg_v)

            if not perfs or valid_50 == 0:
                continue

            rs_score = round((above_50 / valid_50) * 100, 1)
            avg_perf = round(sum(perfs) / len(perfs), 2)
            avg_vol_ratio = round(sum(vol_ratios) / len(vol_ratios), 2) if vol_ratios else 1.0

            if rs_score >= 50 and avg_perf >= 0:
                quadrant = "Leading"
            elif rs_score >= 50 and avg_perf < 0:
                quadrant = "Weakening"
            elif rs_score < 50 and avg_perf >= 0:
                quadrant = "Improving"
            else:
                quadrant = "Lagging"

            sectors_out.append({
                "sector": sector,
                "rs": rs_score,
                "perf": avg_perf,
                "stock_count": len(perfs),
                "vol_ratio": avg_vol_ratio,
                "quadrant": quadrant,
            })
            all_rs.append(rs_score)
            all_perf.append(avg_perf)

        median_rs = sorted(all_rs)[len(all_rs)//2] if all_rs else 50
        median_perf = sorted(all_perf)[len(all_perf)//2] if all_perf else 0

        return {
            "sectors": sectors_out,
            "median_rs": round(median_rs, 1),
            "median_perf": round(median_perf, 2),
            "latest_date": latest_date,
            "market": market,
        }

    except Exception as e:
        import traceback
        traceback.print_exc()
        return {"sectors": [], "error": str(e)}

# ── Watchlist CRUD ─────────────────────────────────────────────────────────────

@app.get("/api/watchlist")
def api_list_watchlists():
    return {"watchlists": list_watchlists()}

@app.post("/api/watchlist")
async def api_create_watchlist(req: dict = None):
    from fastapi import Request
    if not req or "name" not in req:
        return {"error": "name required"}
    return create_watchlist(req["name"])

@app.delete("/api/watchlist/{wid}")
def api_delete_watchlist(wid: int):
    return delete_watchlist(wid)

@app.post("/api/watchlist/{wid}/add")
async def api_add_ticker(wid: int, req: dict = None):
    if not req or "ticker" not in req:
        return {"error": "ticker required"}
    return wl_add_ticker(wid, req["ticker"], req.get("notes", ""))

@app.delete("/api/watchlist/{wid}/remove/{ticker}")
def api_remove_ticker(wid: int, ticker: str):
    return wl_remove_ticker(wid, ticker)

@app.get("/api/watchlist/{wid}/data")
async def api_watchlist_data(wid: int):
    loop = asyncio.get_event_loop()
    data = await loop.run_in_executor(executor, get_watchlist_data, wid)
    return data

# ── Alerts CRUD ────────────────────────────────────────────────────────────────

@app.post("/api/alerts")
async def api_create_alert(req: dict = None):
    if not req or "ticker" not in req or "condition_type" not in req:
        return {"error": "ticker and condition_type required"}
    return create_alert(req["ticker"], req["condition_type"], req.get("condition_value"))

@app.get("/api/alerts")
def api_list_alerts():
    return {"alerts": list_alerts()}

@app.delete("/api/alerts/{aid}")
def api_delete_alert(aid: int):
    return delete_alert(aid)

@app.get("/api/alerts/check")
async def api_check_alerts():
    loop = asyncio.get_event_loop()
    triggered = await loop.run_in_executor(executor, check_alerts)
    return {"triggered": triggered, "count": len(triggered)}

# ── Market Summary ─────────────────────────────────────────────────────────────

# ── Market Cap Endpoints ───────────────────────────────────────────────────────

@app.get("/api/market-cap/import")
async def mcap_import():
    """Trigger market cap import from CSV."""
    csv_path = pathlib.Path(__file__).parent / "data" / "market_cap.csv"
    if not csv_path.exists():
        return {"error": "market_cap.csv not found"}
    result = import_market_cap_csv(str(csv_path))
    return result

@app.get("/api/market-cap/stats")
async def mcap_stats():
    """Get market cap tier breakdown stats."""
    import sqlite3 as _sql
    conn = _sql.connect(str(pathlib.Path(__file__).parent / "breadth_data.db"), timeout=10)
    total = conn.execute("SELECT COUNT(*) FROM market_cap").fetchone()[0]
    tiers = conn.execute(
        "SELECT mcap_tier, COUNT(*), ROUND(AVG(mcap_cr),0) FROM market_cap GROUP BY mcap_tier ORDER BY AVG(mcap_cr) DESC"
    ).fetchall()
    conn.close()
    return {
        "total": total,
        "tiers": {t: {"count": c, "avg_mcap_cr": a} for t, c, a in tiers},
    }

@app.get("/api/market-cap/{ticker}")
async def mcap_ticker(ticker: str):
    """Get market cap for a single ticker."""
    data = get_mcap_for_ticker(ticker.upper())
    if not data:
        return {"error": f"No market cap data for {ticker}"}
    data["mcap_formatted"] = format_mcap(data["mcap_cr"])
    return data


@app.get("/api/summary")
async def api_summary(market: str = "India"):
    loop = asyncio.get_event_loop()
    data = await loop.run_in_executor(executor, generate_market_summary, market)
    return data

@app.get("/api/summary/html")
async def api_summary_html(market: str = "India"):
    from fastapi.responses import HTMLResponse
    loop = asyncio.get_event_loop()
    html = await loop.run_in_executor(executor, generate_summary_html, market)
    return HTMLResponse(content=html)


@app.get("/api/peep-into-past")
async def api_peep_into_past(date: str = "2020-03-23", market: str = "India"):
    try:
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(executor, compute_historical_breadth, date, market)
        return result
    except Exception as e:
        logger.error(f"Peep Into Past error for {date}: {e}", exc_info=True)
        return {"error": str(e), "target_date": date}


if __name__=="__main__":
    import uvicorn
    print("\n🚀  Market Breadth Engine v2.0")
    print("    API    →  http://localhost:8001")
    print("    Debug  →  http://localhost:8001/api/debug")
    print("    Sync   →  POST http://localhost:8001/api/sync/start")
    print("    Status →  http://localhost:8001/api/sync/status\n")
    uvicorn.run("main:app",host="0.0.0.0",port=8001,reload=False)
