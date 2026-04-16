
// ─── AUTH — validate token, apply tier gating ──────────────────────────────
let _currentUser = null;
let _allowedTabs = [];

// Tier → allowed tabs (synced with backend auth.py TIERS)
const TIER_TABS = {
  explorer: ['overview', 'breadth', 'compare', 'sectors'],
  trader:   ['overview', 'breadth', 'compare', 'sectors', 'smart-money', 'leaders', 'screeners', 'charts', 'scanner', 'stockbee'],
  pro:      ['overview', 'breadth', 'compare', 'sectors', 'smart-money', 'leaders', 'screeners', 'charts', 'scanner', 'stockbee',
             'fvalue', 'smart-screener', 'smart-metrics', 'insider', 'fiidii', 'journal', 'watchlist'],
  elite:    '__all__',
  admin:    '__all__',
};

(async function initAuth() {
  const token = localStorage.getItem('qb360_token');
  if (!token) { window.location.href = '/auth'; return; }

  try {
    const res = await fetch(`${API}/api/auth/me`, { headers: { 'Authorization': `Bearer ${token}` } });
    const data = await res.json();

    if (data.error || !data.email) {
      localStorage.removeItem('qb360_token');
      localStorage.removeItem('qb360_user');
      window.location.href = '/auth';
      return;
    }

    _currentUser = data;
    if (data.refreshed_token) localStorage.setItem('qb360_token', data.refreshed_token);

    // Use effective_tier (accounts for trial)
    const effectiveTier = data.effective_tier || data.tier;
    const tierTabs = TIER_TABS[effectiveTier];
    _allowedTabs = tierTabs === '__all__' ? '__all__' : (tierTabs || TIER_TABS.explorer);

    // Apply sidebar restrictions
    _applyTierRestrictions(effectiveTier);

    // Show user info in sidebar
    _renderSidebarUser(data);

    // Show trial banner if active
    if (data.trial_active && data.trial_days_left != null) {
      _showTrialBanner(data.trial_days_left);
    }

    // Reveal the app (remove loading screen)
    _revealApp();

  } catch (e) {
    console.warn('Auth check failed:', e);
    // Network error — still reveal app with cached data
    _revealApp();
  }
})();

function _revealApp() {
  const loading = document.getElementById('auth-loading');
  const layout = document.getElementById('app-layout');
  if (loading) loading.remove();
  if (layout) layout.style.opacity = '1';
}

function _applyTierRestrictions(effectiveTier) {
  if (effectiveTier === 'admin' || effectiveTier === 'elite') return; // Full access

  const allowed = TIER_TABS[effectiveTier] || TIER_TABS.explorer;
  if (allowed === '__all__') return;

  document.querySelectorAll('.sidebar .nav-item[data-tab]').forEach(btn => {
    const tab = btn.dataset.tab;
    if (!tab) return;
    if (!allowed.includes(tab)) {
      btn.classList.add('locked-tab');
      const label = btn.querySelector('.nav-label');
      if (label && !label.textContent.includes('🔒')) label.textContent += ' 🔒';

      // Determine which tier unlocks this tab
      let requiredTier = 'elite';
      for (const [t, tabs] of Object.entries(TIER_TABS)) {
        if (tabs === '__all__' || tabs.includes(tab)) { requiredTier = t; break; }
      }
      btn.setAttribute('title', `Requires ${requiredTier.charAt(0).toUpperCase() + requiredTier.slice(1)} plan`);
    }
  });
}

function _renderSidebarUser(user) {
  const el = document.getElementById('sidebar-user-info');
  if (!el) return;
  const eff = user.effective_tier || user.tier;
  const tierColors = { explorer: '#94a3b8', trader: '#06b6d4', pro: '#a855f7', elite: '#f59e0b', admin: '#ef4444' };
  const color = tierColors[eff] || '#94a3b8';
  const tierLabel = user.trial_active ? `PRO TRIAL (${user.trial_days_left}d)` : eff.toUpperCase();

  el.innerHTML = `
    <div style="padding:8px 12px;font-family:var(--font-mono);font-size:10px;line-height:1.6">
      <div style="color:var(--text2)">${user.name || user.email}</div>
      <div><span style="color:${color};font-weight:700">${tierLabel}</span></div>
    </div>`;

  // Show admin link if admin
  const adminLink = document.getElementById('sidebar-admin-link');
  if (adminLink) adminLink.style.display = eff === 'admin' ? '' : 'none';

  // Show logout button
  const logoutEl = document.getElementById('sidebar-logout');
  if (logoutEl) logoutEl.style.display = '';
}

function _showTrialBanner(daysLeft) {
  const existing = document.getElementById('trial-banner');
  if (existing) existing.remove();

  const banner = document.createElement('div');
  banner.id = 'trial-banner';
  const urgency = daysLeft <= 3 ? 'var(--red)' : daysLeft <= 7 ? 'var(--amber)' : 'var(--green)';
  banner.innerHTML = `
    <div style="background:rgba(99,102,241,.08);border-bottom:1px solid rgba(99,102,241,.15);
      padding:8px 16px;display:flex;align-items:center;justify-content:space-between;font-family:var(--font-mono);font-size:11px">
      <span>
        ✨ <b style="color:var(--cyan)">Pro Trial</b> —
        <b style="color:${urgency}">${daysLeft} day${daysLeft !== 1 ? 's' : ''} remaining</b>
        · Full access to all features
      </span>
      <a href="#" onclick="showUpgradeModal();return false"
        style="color:var(--cyan);font-weight:700;text-decoration:none;padding:4px 12px;
        border:1px solid var(--cyan);border-radius:6px;font-size:10px">Upgrade Now</a>
    </div>`;
  const main = document.querySelector('.main-content') || document.body;
  main.prepend(banner);
}

function logout() {
  localStorage.removeItem('qb360_token');
  localStorage.removeItem('qb360_user');
  localStorage.removeItem('qb360_mode');
  window.location.href = '/auth';
}

// ─── UPGRADE MODAL ───────────────────────────────────────────────────────
function showUpgradeModal(requiredTier) {
  let m = document.getElementById('upgrade-modal');
  if (!m) {
    m = document.createElement('div');
    m.id = 'upgrade-modal';
    m.style.cssText = 'position:fixed;inset:0;z-index:9999;background:rgba(0,0,0,.6);backdrop-filter:blur(4px);display:flex;align-items:center;justify-content:center';
    m.onclick = e => { if (e.target === m) m.style.display = 'none'; };
    document.body.appendChild(m);
  }

  const isLoggedIn = !!_currentUser;
  const currentTier = _currentUser?.effective_tier || _currentUser?.tier || 'explorer';
  const ctaBtn = isLoggedIn
    ? `<a href="mailto:support@quantumtrade.pro?subject=Upgrade to ${requiredTier || 'Pro'}&body=Account: ${_currentUser?.email}%0ATier: ${currentTier}%0ARequest: Upgrade"
        style="display:inline-block;padding:10px 28px;border-radius:8px;background:#6366f1;color:#fff;
        font-family:var(--font-mono);font-size:12px;font-weight:700;text-decoration:none;cursor:pointer;margin-right:12px">
        Contact to Upgrade</a>`
    : `<a href="/auth" style="display:inline-block;padding:10px 28px;border-radius:8px;background:#6366f1;color:#fff;
        font-family:var(--font-mono);font-size:12px;font-weight:700;text-decoration:none;cursor:pointer;margin-right:12px">
        Create Free Account</a>`;

  m.innerHTML = `
    <div style="background:var(--card-bg,#0f1628);border:1px solid var(--border,#1e293b);border-radius:16px;
      padding:32px;max-width:520px;width:92%;text-align:center;font-family:var(--font-mono);position:relative">
      <button onclick="document.getElementById('upgrade-modal').style.display='none'"
        style="position:absolute;top:12px;right:16px;background:none;border:none;color:var(--text3);
        cursor:pointer;font-size:18px">✕</button>
      <div style="font-size:32px;margin-bottom:12px">🚀</div>
      <h2 style="color:var(--text,#e2e8f0);font-size:18px;margin-bottom:6px">Upgrade Your Plan</h2>
      <p style="color:var(--text3,#64748b);font-size:12px;line-height:1.6;margin-bottom:20px">
        Unlock powerful tools for smarter trading decisions.
      </p>
      <div style="display:grid;grid-template-columns:1fr 1fr 1fr;gap:10px;margin-bottom:20px;text-align:center">
        <div style="border:1px solid ${currentTier==='trader'?'#06b6d4':'#06b6d433'};border-radius:10px;padding:14px;
          ${currentTier==='trader'?'background:rgba(6,182,212,.08)':''}">
          <div style="color:#06b6d4;font-weight:700;font-size:13px">Trader</div>
          <div style="color:var(--text,#e2e8f0);font-size:24px;font-weight:800;margin:4px 0">$29</div>
          <div style="color:var(--text3);font-size:10px">/month</div>
          <div style="color:var(--text3);font-size:9px;margin-top:8px;line-height:1.5">
            Smart Money · RS Rankings<br>Charts · Scanner · Leaders<br>Stockbee Monitor
          </div>
          ${currentTier==='trader'?'<div style="margin-top:8px;color:#06b6d4;font-size:9px;font-weight:700">CURRENT PLAN</div>':''}
        </div>
        <div style="border:2px solid #a855f7;border-radius:10px;padding:14px;background:rgba(168,85,247,.05);position:relative">
          <div style="position:absolute;top:-8px;left:50%;transform:translateX(-50%);background:#a855f7;color:#fff;
            font-size:8px;font-weight:700;padding:2px 10px;border-radius:10px;letter-spacing:.05em">POPULAR</div>
          <div style="color:#a855f7;font-weight:700;font-size:13px">Pro</div>
          <div style="color:var(--text,#e2e8f0);font-size:24px;font-weight:800;margin:4px 0">$79</div>
          <div style="color:var(--text3);font-size:10px">/month</div>
          <div style="color:var(--text3);font-size:9px;margin-top:8px;line-height:1.5">
            Everything in Trader +<br>F-Value · AI Screener<br>Insider · FII/DII · Export
          </div>
          ${currentTier==='pro'?'<div style="margin-top:8px;color:#a855f7;font-size:9px;font-weight:700">CURRENT PLAN</div>':''}
        </div>
        <div style="border:1px solid ${currentTier==='elite'?'#f59e0b':'#f59e0b33'};border-radius:10px;padding:14px;
          ${currentTier==='elite'?'background:rgba(245,158,11,.08)':''}">
          <div style="color:#f59e0b;font-weight:700;font-size:13px">Elite</div>
          <div style="color:var(--text,#e2e8f0);font-size:24px;font-weight:800;margin:4px 0">$149</div>
          <div style="color:var(--text3);font-size:10px">/month</div>
          <div style="color:var(--text3);font-size:9px;margin-top:8px;line-height:1.5">
            Everything + API Access<br>Admin Panel · Alerts<br>Peep Into Past
          </div>
          ${currentTier==='elite'?'<div style="margin-top:8px;color:#f59e0b;font-size:9px;font-weight:700">CURRENT PLAN</div>':''}
        </div>
      </div>
      <div style="display:flex;align-items:center;justify-content:center;gap:8px">
        ${ctaBtn}
        <button onclick="document.getElementById('upgrade-modal').style.display='none'"
          style="padding:10px 20px;border:1px solid var(--border,#1e293b);border-radius:8px;
          background:transparent;color:var(--text3);font-family:var(--font-mono);font-size:11px;cursor:pointer">
          Maybe Later
        </button>
      </div>
    </div>`;
  m.style.display = 'flex';
}
function _showUpgradeModal() { showUpgradeModal(); }

// ─── SESSION AUTO-REFRESH ────────────────────────────────────────────────
// Refresh token every 60 minutes to prevent expiry during long sessions
setInterval(async () => {
  const token = localStorage.getItem('qb360_token');
  if (!token) return;
  try {
    const res = await fetch(`${API}/api/auth/me`, { headers: { 'Authorization': `Bearer ${token}` } });
    const data = await res.json();
    if (data.error) { logout(); return; }
    if (data.refreshed_token) localStorage.setItem('qb360_token', data.refreshed_token);
    // Update trial banner if days changed
    if (_currentUser && data.trial_days_left !== _currentUser.trial_days_left) {
      _currentUser = data;
      if (data.trial_active) _showTrialBanner(data.trial_days_left);
    }
  } catch {}
}, 60 * 60 * 1000); // every 60 minutes

// Refresh on window focus (user returns to tab after being away)
document.addEventListener('visibilitychange', async () => {
  if (document.hidden) return;
  const token = localStorage.getItem('qb360_token');
  if (!token) return;
  try {
    const res = await fetch(`${API}/api/auth/me`, { headers: { 'Authorization': `Bearer ${token}` } });
    const data = await res.json();
    if (data.error) { logout(); return; }
    if (data.refreshed_token) localStorage.setItem('qb360_token', data.refreshed_token);
  } catch {}
});

// ─── AUTH HEADER HELPER — inject JWT into fetch calls ────────────────────
// Override global fetch to auto-inject Authorization header for API calls
const _originalFetch = window.fetch;
window.fetch = function(url, options = {}) {
  // Only inject for our API calls
  if (typeof url === 'string' && url.includes('/api/')) {
    const token = localStorage.getItem('qb360_token');
    if (token) {
      options.headers = options.headers || {};
      if (options.headers instanceof Headers) {
        if (!options.headers.has('Authorization')) options.headers.set('Authorization', `Bearer ${token}`);
      } else {
        if (!options.headers['Authorization']) options.headers['Authorization'] = `Bearer ${token}`;
      }
    }
  }
  return _originalFetch.call(this, url, options);
};

// ─── CLEAR CACHE ───────────────────────────────────────────────────────────
async function clearBreadthCache() {
  const btn = document.getElementById('sidebar-clear-cache-btn');
  if (btn) btn.style.opacity = '0.5';
  try {
    const resp = await fetch(`${API}/api/cache/clear-breadth`, { method: 'POST' });
    const data = await resp.json();
    if (data.ok) {
      // Show brief feedback
      const label = btn ? btn.querySelector('.nav-label') : null;
      if (label) { label.textContent = 'Recomputing...'; }
      // Reload after 2s to pick up fresh data
      setTimeout(() => { window.location.reload(); }, 2000);
    } else {
      alert('Failed to clear cache: ' + (data.error || 'Unknown error'));
    }
  } catch (e) {
    alert('Failed to clear cache: ' + e.message);
  } finally {
    if (btn) btn.style.opacity = '1';
  }
}

// ─── THEME TOGGLE ──────────────────────────────────────────────────────────
function toggleTheme() {
  const html = document.documentElement;
  const isLight = html.getAttribute('data-theme') === 'light';
  const newTheme = isLight ? 'dark' : 'light';
  if (newTheme === 'light') {
    html.setAttribute('data-theme', 'light');
  } else {
    html.removeAttribute('data-theme');
  }
  localStorage.setItem('breadth-theme', newTheme);
  // Re-render charts with new theme colors if visible
  if (typeof updateChartsTheme === 'function') updateChartsTheme();
}

// Apply saved theme on load
(function initTheme() {
  const saved = localStorage.getItem('breadth-theme');
  if (saved === 'light') {
    document.documentElement.setAttribute('data-theme', 'light');
  }
})();

// ─── SIDEBAR ────────────────────────────────────────────────────────────────
function toggleSidebar() {
  const sidebar = document.getElementById('sidebar');
  const overlay = document.getElementById('sidebar-overlay');
  if (!sidebar) return;
  // Mobile: toggle open class
  if (window.innerWidth <= 768) {
    sidebar.classList.toggle('open');
    if (overlay) overlay.classList.toggle('active', sidebar.classList.contains('open'));
  } else {
    // Desktop: toggle collapsed
    toggleSidebarCollapse();
  }
}

function closeSidebar() {
  const sidebar = document.getElementById('sidebar');
  const overlay = document.getElementById('sidebar-overlay');
  if (sidebar) sidebar.classList.remove('open');
  if (overlay) overlay.classList.remove('active');
}

function toggleSidebarCollapse() {
  const sidebar = document.getElementById('sidebar');
  if (!sidebar) return;
  sidebar.classList.toggle('collapsed');
  localStorage.setItem('sidebar-collapsed', sidebar.classList.contains('collapsed') ? '1' : '');
}

// Restore sidebar collapsed state
(function initSidebar() {
  const collapsed = localStorage.getItem('sidebar-collapsed');
  if (collapsed === '1') {
    const sidebar = document.getElementById('sidebar');
    if (sidebar) sidebar.classList.add('collapsed');
  }
})();

function switchTab(tab) {
  // Tier gate: check if tab is allowed for current user
  if (_currentUser && _allowedTabs !== '__all__' && !_allowedTabs.includes(tab)) {
    showUpgradeModal();
    return;
  }

  // Update sidebar nav items
  document.querySelectorAll('.sidebar .nav-item[data-tab]').forEach(b => b.classList.toggle('active', b.dataset.tab === tab));
  // Keep old tab-btn compat
  document.querySelectorAll('.tab-btn').forEach(b => b.classList.toggle('active', b.dataset.tab === tab));
  document.querySelectorAll('.tab-content').forEach(c => c.classList.toggle('active', c.id === `tab-${tab}`));

  // Close sidebar on mobile after selection
  if (window.innerWidth <= 768) closeSidebar();

  // Render charts when tab becomes visible
  if (tab === 'breadth' && currentData[currentMarket]) {
    setTimeout(() => renderCharts(currentData[currentMarket]), 50);
  }
  if (tab === 'sectors' && currentData[currentMarket]) {
    setTimeout(() => renderSectors(currentData[currentMarket]), 50);
    setTimeout(() => renderSectors(currentData[currentMarket]), 100);
  }
  if (tab === 'compare') {
    setTimeout(() => loadCompare(), 50);
  }
  if (tab === 'screeners') {
    const list = document.getElementById('scr-list');
    if (list && !list.children.length) initScrList();
  }
  if (tab === 'importer') {
    impCheckStored();
    if (typeof loadNseIndicesStatus === 'function') loadNseIndicesStatus();
  }
  if (tab === 'leaders') { if(!_leadersData) initLeadersTab(); }
  if (tab === 'smart-screener') { /* ready on demand — user clicks Run */ }
  if (tab === 'stockbee') { if(!_sbmData) loadStockbeeMonitor(); }
  if (tab === 'smart-money') { if(!_smMoneyData) loadSmartMoney(); }
  if (tab === 'journal') { jnlLoadTrades(); }
  if (tab === 'smart-metrics') { /* ready on demand */ }
  if (tab === 'peep-into-past') { _initPeepChips(); }
  if (tab === 'charts') { setTimeout(() => initChartsTab(), 50); }
  if (tab === 'sectors') { setTimeout(() => loadHeatmap(), 200); }
  if (tab === 'watchlist') { initWatchlistTab(); }
  if (tab === 'scanner') {
    const g=document.getElementById('scn-gainers');
    if(g&&g.innerHTML.includes('Loading')) initScannerTab();
    else updateScannerMarketBar();
  }
  if (tab === 'insider') { if(typeof onInsiderTabLoad==='function') onInsiderTabLoad(); }
  if (tab === 'fvalue') { if(typeof onFValueTabLoad==='function') onFValueTabLoad(); }
  if (tab === 'fiidii') { if(typeof onFiiDiiTabLoad==='function') onFiiDiiTabLoad(); }
}

// ── Sector Drill-Down → Smart Money Tab ─────────────────────────────────────

function drillIntoSector(sectorName, pctAbove50, weekReturn) {
  window._sectorDrillContext = {
    sector: sectorName,
    pctAbove50: pctAbove50 ?? 0,
    weekReturn: weekReturn ?? 0,
  };

  switchTab('smart-money');

  const _applyFilter = (retries = 0) => {
    if (retries > 20) return; // give up after 10 seconds

    const sel = document.getElementById('sm-filter-sector');
    if (!sel || !_smMoneyData?.tickers?.length) {
      setTimeout(() => _applyFilter(retries + 1), 500);
      return;
    }

    // Fuzzy match: find the closest sector name in Smart Money data
    const allSectors = [...new Set(_smMoneyData.tickers.map(t => t.sector).filter(Boolean))];
    const exactMatch = allSectors.find(s => s === sectorName);
    const looseMatch = allSectors.find(s =>
      s.toLowerCase() === sectorName.toLowerCase() ||
      s.toLowerCase().includes(sectorName.toLowerCase()) ||
      sectorName.toLowerCase().includes(s.toLowerCase())
    );
    const matchedSector = exactMatch || looseMatch;

    if (matchedSector) {
      window._sectorDrillContext.matchedSector = matchedSector;
      sel.value = matchedSector;
    } else {
      // No match in dropdown — inject it temporarily
      window._sectorDrillContext.matchedSector = sectorName;
      console.warn(`Sector '${sectorName}' not found in Smart Money data. Available:`, allSectors);
    }

    _showSectorDrillBanner();
    if (typeof filterSmartMoney === 'function') filterSmartMoney();
  };

  setTimeout(() => _applyFilter(0), 300);
}

function _showSectorDrillBanner() {
  const ctx = window._sectorDrillContext;
  if (!ctx) return;

  const old = document.getElementById('sm-sector-drill-banner');
  if (old) old.remove();

  // Use matched sector name for filtering
  const sectorKey = ctx.matchedSector || ctx.sector;
  let sectorTickers = (_smMoneyData?.tickers || []).filter(t =>
    t.sector === sectorKey ||
    (t.sector || '').toLowerCase() === (sectorKey || '').toLowerCase()
  );
  
  // If sector field is empty in SM data, use breadth tickers list
  if (sectorTickers.length === 0) {
    const breadthData = typeof currentData !== 'undefined' ? currentData?.[typeof currentMarket !== 'undefined' ? currentMarket : 'INDIA'] : null;
    if (breadthData?.sector_breadth) {
      const secData = breadthData.sector_breadth.find(s => 
        s.sector === ctx.sector || s.sector.toLowerCase() === ctx.sector.toLowerCase()
      );
      if (secData?.tickers) {
        const secTickerSet = new Set(secData.tickers.map(t => t.toUpperCase()));
        sectorTickers = (_smMoneyData?.tickers || []).filter(t => secTickerSet.has(t.ticker.toUpperCase()));
      }
    }
  }
  const totalStocks = sectorTickers.length;
  const stage2Count = sectorTickers.filter(t => t.stage === 'Stage 2').length;
  const avgRS = totalStocks ? Math.round(sectorTickers.reduce((s, t) => s + (t.rs_rating || 0), 0) / totalStocks) : 0;
  const totalSignals = sectorTickers.reduce((s, t) => s + (t.total_signals || 0), 0);
  const topGainer = sectorTickers.reduce((best, t) => (!best || (t.change_pct || 0) > (best.change_pct || 0)) ? t : best, null);
  const topLoser = sectorTickers.reduce((worst, t) => (!worst || (t.change_pct || 0) < (worst.change_pct || 0)) ? t : worst, null);
  const insiderBuys = sectorTickers.reduce((s, t) => s + (t.insider_buys || 0), 0);

  const pct = ctx.pctAbove50;
  const ret = ctx.weekReturn;
  const pctColor = pct >= 60 ? '#22c55e' : pct >= 40 ? '#f59e0b' : '#ef4444';
  const retColor = ret >= 0 ? '#22c55e' : '#ef4444';

  const banner = document.createElement('div');
  banner.id = 'sm-sector-drill-banner';
  banner.innerHTML = `
    <div style="background:linear-gradient(135deg, rgba(6,182,212,.06), rgba(168,85,247,.04));border:1px solid var(--border);border-radius:10px;padding:14px 18px;margin-bottom:14px;position:relative">
      <button onclick="clearSectorDrill()" style="position:absolute;top:8px;right:12px;background:none;border:none;color:var(--text3);cursor:pointer;font-size:16px" title="Clear sector filter">✕</button>
      <div style="display:flex;align-items:center;gap:8px;margin-bottom:10px">
        <span style="font-size:16px">📊</span>
        <span style="font-family:var(--font-mono);font-size:15px;font-weight:800;color:var(--cyan);letter-spacing:.04em">${ctx.sector.toUpperCase()}</span>
        <span style="font-size:10px;color:var(--text3);font-family:var(--font-mono)">SECTOR DRILL-DOWN</span>
        <span style="font-size:10px;color:var(--text3);font-family:var(--font-mono)">(${totalStocks} stocks)</span>
      </div>
      <div style="display:grid;grid-template-columns:repeat(7,1fr);gap:10px">
        <div><div style="font-size:8px;color:var(--text3);font-family:var(--font-mono);letter-spacing:.08em">% ABOVE 50 DMA</div><div style="font-size:18px;font-weight:800;color:${pctColor};font-family:var(--font-mono)">${pct.toFixed(1)}%</div></div>
        <div><div style="font-size:8px;color:var(--text3);font-family:var(--font-mono);letter-spacing:.08em">5D RETURN</div><div style="font-size:18px;font-weight:800;color:${retColor};font-family:var(--font-mono)">${ret>=0?'+':''}${ret.toFixed(1)}%</div></div>
        <div><div style="font-size:8px;color:var(--text3);font-family:var(--font-mono);letter-spacing:.08em">STOCKS</div><div style="font-size:18px;font-weight:800;color:var(--text);font-family:var(--font-mono)">${totalStocks}</div></div>
        <div><div style="font-size:8px;color:var(--text3);font-family:var(--font-mono);letter-spacing:.08em">STAGE 2</div><div style="font-size:18px;font-weight:800;color:var(--green);font-family:var(--font-mono)">${stage2Count}</div></div>
        <div><div style="font-size:8px;color:var(--text3);font-family:var(--font-mono);letter-spacing:.08em">AVG RS</div><div style="font-size:18px;font-weight:800;color:${avgRS>=70?'var(--green)':avgRS>=50?'var(--amber)':'var(--red)'};font-family:var(--font-mono)">${avgRS}</div></div>
        <div><div style="font-size:8px;color:var(--text3);font-family:var(--font-mono);letter-spacing:.08em">SIGNALS</div><div style="font-size:18px;font-weight:800;color:var(--cyan);font-family:var(--font-mono)">${totalSignals}</div></div>
        <div><div style="font-size:8px;color:var(--text3);font-family:var(--font-mono);letter-spacing:.08em">INSIDER BUYS</div><div style="font-size:18px;font-weight:800;color:${insiderBuys>0?'var(--green)':'var(--text3)'};font-family:var(--font-mono)">${insiderBuys}</div></div>
      </div>
      ${topGainer && totalStocks > 0 ? `<div style="margin-top:8px;display:flex;gap:16px;font-family:var(--font-mono);font-size:10px">
        <span style="color:var(--text3)">Top Gainer:</span>
        <span style="color:var(--green);font-weight:700;cursor:pointer" onclick="loadChart('${topGainer.ticker}')">${topGainer.ticker} +${(topGainer.change_pct||0).toFixed(1)}%</span>
        ${topLoser && topLoser.ticker !== topGainer.ticker ? `<span style="color:var(--text3)">Top Loser:</span><span style="color:var(--red);font-weight:700;cursor:pointer" onclick="loadChart('${topLoser.ticker}')">${topLoser.ticker} ${(topLoser.change_pct||0).toFixed(1)}%</span>` : ''}
      </div>` : ''}
    </div>`;

  const tableWrap = document.getElementById('sm-table-wrap');
  if (tableWrap) tableWrap.parentElement.insertBefore(banner, tableWrap);
}

function clearSectorDrill() {
  window._sectorDrillContext = null;
  const banner = document.getElementById('sm-sector-drill-banner');
  if (banner) banner.remove();
  const sel = document.getElementById('sm-filter-sector');
  if (sel) sel.value = 'all';
  if (typeof filterSmartMoney === 'function') filterSmartMoney();
}

// ─── SUMMARY MODAL ────────────────────────────────────────────────────────────

let _summaryHtml = '';

async function openSummaryModal() {
  const modal = document.getElementById('summary-modal');
  const body = document.getElementById('summary-modal-body');
  if (!modal || !body) return;
  modal.style.display = 'flex';
  body.innerHTML = '<div style="text-align:center;padding:40px;color:var(--text3)">Loading summary...</div>';

  try {
    const market = typeof currentMarket !== 'undefined' ? currentMarket : 'India';
    const res = await fetch(`${API}/api/summary/html?market=${market}`);
    _summaryHtml = await res.text();
    // Display in iframe to isolate email styles
    body.innerHTML = `<iframe id="summary-iframe" style="width:100%;height:100%;border:none;background:#fff;border-radius:6px" srcdoc="${_summaryHtml.replace(/"/g, '&quot;')}"></iframe>`;
  } catch (e) {
    body.innerHTML = `<div style="text-align:center;padding:40px;color:var(--red)">Error: ${e.message}</div>`;
  }
}

function closeSummaryModal() {
  const modal = document.getElementById('summary-modal');
  if (modal) modal.style.display = 'none';
}

function copySummaryHTML() {
  if (!_summaryHtml) return;
  navigator.clipboard.writeText(_summaryHtml).then(() => {
    const btn = document.querySelector('.summary-action-btn');
    if (btn) { const t = btn.textContent; btn.textContent = 'Copied!'; setTimeout(() => btn.textContent = t, 1500); }
  });
}

function downloadSummary() {
  if (!_summaryHtml) return;
  const blob = new Blob([_summaryHtml], { type: 'text/html' });
  const url = URL.createObjectURL(blob);
  const a = document.createElement('a');
  a.href = url;
  const market = typeof currentMarket !== 'undefined' ? currentMarket : 'India';
  a.download = `market_summary_${market}_${new Date().toISOString().slice(0, 10)}.html`;
  a.click();
  URL.revokeObjectURL(url);
}

// ─── SCREENER EXPORT ──────────────────────────────────────────────────────────

function _scrExportFilename(prefix) {
  const d = new Date().toISOString().slice(0, 10);
  return `${prefix}_${d}`;
}

function exportScreenerPNG() {
  const el = document.querySelector('.scr-tbl') || document.querySelector('.scn-results-tbl');
  if (!el) { alert('No results to export'); return; }
  html2canvas(el, { backgroundColor: '#0f0c29', scale: 2 }).then(canvas => {
    const a = document.createElement('a');
    a.href = canvas.toDataURL('image/png');
    a.download = _scrExportFilename('screener') + '.png';
    a.click();
  });
}

function exportScreenerExcel() {
  const tbl = document.querySelector('.scr-tbl') || document.querySelector('.scn-results-tbl');
  if (!tbl) { alert('No results to export'); return; }
  const wb = XLSX.utils.table_to_book(tbl, { sheet: 'Screener' });
  XLSX.writeFile(wb, _scrExportFilename('screener') + '.xlsx');
}

function exportScreenerPDF() {
  const tbl = document.querySelector('.scr-tbl') || document.querySelector('.scn-results-tbl');
  if (!tbl) { alert('No results to export'); return; }
  const { jsPDF } = window.jspdf;
  const doc = new jsPDF('l', 'pt', 'a4');
  doc.autoTable({ html: tbl, styles: { fontSize: 8 }, headStyles: { fillColor: [15, 12, 41] } });
  doc.save(_scrExportFilename('screener') + '.pdf');
}

function exportScannerPNG() {
  const el = document.getElementById('scn-qr-body');
  if (!el || !el.querySelector('table')) { alert('No results to export'); return; }
  html2canvas(el, { backgroundColor: '#0f0c29', scale: 2 }).then(canvas => {
    const a = document.createElement('a');
    a.href = canvas.toDataURL('image/png');
    a.download = _scrExportFilename('scanner') + '.png';
    a.click();
  });
}

function exportScannerExcel() {
  const tbl = document.querySelector('#scn-qr-body .scn-results-tbl');
  if (!tbl) { alert('No results to export'); return; }
  const wb = XLSX.utils.table_to_book(tbl, { sheet: 'Scanner' });
  XLSX.writeFile(wb, _scrExportFilename('scanner') + '.xlsx');
}

function exportScannerPDF() {
  const tbl = document.querySelector('#scn-qr-body .scn-results-tbl');
  if (!tbl) { alert('No results to export'); return; }
  const { jsPDF } = window.jspdf;
  const doc = new jsPDF('l', 'pt', 'a4');
  doc.autoTable({ html: tbl, styles: { fontSize: 8 }, headStyles: { fillColor: [15, 12, 41] } });
  doc.save(_scrExportFilename('scanner') + '.pdf');
}

// ─── FETCH LIVE ─────────────────────────────────────────────────────────────

async function fetchLive() {
  const btn = document.getElementById('fetch-live-btn');
  if (!btn) return;
  const origText = btn.textContent;
  btn.textContent = '⏳ Fetching...';
  btn.disabled = true;

  try {
    const res = await fetch(`${API}/api/fetch-live`, { method: 'POST' });
    const data = await res.json();
    console.log('Live fetch result:', data);
    // Re-load current tab data
    loadBreadth(true);
  } catch (e) {
    console.error('Live fetch failed:', e);
  } finally {
    btn.textContent = origText;
    btn.disabled = false;
  }
}

// ─── INIT ─────────────────────────────────────────────────────────────────────
setInterval(updateFreshness, 10000);

// Auto-load both markets in parallel
window.addEventListener('load', async () => {
  // Start India immediately (shown first)
  loadBreadth(false);

  // Load US in background after 2s delay to avoid rate limiting
  setTimeout(async () => {
    if (!currentData['US']) {
      try {
        const res = await fetch(`${API}/api/breadth/US`);
        if (res.ok) {
          const data = await res.json();
          if (!data.error) currentData['US'] = data;
          loadCompare();
        }
      } catch {}
    }
  }, 3000);

  // Wire autocomplete to Charts and Smart Metrics inputs
  if (typeof setupTickerAutocomplete === 'function') {
    setupTickerAutocomplete('chart-ticker-input', (ticker) => loadChart(ticker));
    setupTickerAutocomplete('sm-ticker-input', (ticker) => loadSmartMetrics(ticker));
  }
});


// ── NSE Data Sync (Yahoo v8 Direct API) ──────────────────────────────────────
async function startNseSync(range) {
  const statusEl = document.getElementById('nse-sync-status');
  const msgEl = document.getElementById('nse-sync-msg');
  const progEl = document.getElementById('nse-sync-progress');
  const barEl = document.getElementById('nse-sync-bar');
  if (!statusEl) return;

  statusEl.style.display = 'block';
  msgEl.textContent = 'Starting sync...';
  barEl.style.width = '0%';

  const endpoint = range === '2y' ? '/api/nse-sync/full' : `/api/nse-sync/start?range=${range}`;
  try {
    await fetch(`${API}${endpoint}`, { method: 'POST' });
  } catch(e) {
    msgEl.textContent = 'Failed to start sync: ' + e.message;
    return;
  }

  // Poll status
  const poll = setInterval(async () => {
    try {
      const res = await fetch(`${API}/api/nse-sync/status`);
      const s = await res.json();
      msgEl.textContent = s.message || 'Syncing...';
      if (s.total > 0) {
        const pct = Math.round((s.progress / s.total) * 100);
        progEl.textContent = `${s.progress}/${s.total}`;
        barEl.style.width = pct + '%';
      }
      if (!s.running) {
        clearInterval(poll);
        msgEl.textContent = s.message || 'Sync complete!';
        barEl.style.width = '100%';
        barEl.style.background = 'var(--green)';
        // Auto-refresh breadth data after sync
        setTimeout(() => loadBreadth(true), 2000);
      }
    } catch(e) {
      // ignore polling errors
    }
  }, 2000);
}

// ── Force EOD Sync — pulls today's data for ALL 500 tickers ──────────────────
async function startNseForceSync() {
  const statusEl = document.getElementById('nse-sync-status');
  const msgEl = document.getElementById('nse-sync-msg');
  const progEl = document.getElementById('nse-sync-progress');
  const barEl = document.getElementById('nse-sync-bar');
  if (!statusEl) return;

  statusEl.style.display = 'block';
  msgEl.textContent = 'Force EOD sync starting — all 500 tickers...';
  barEl.style.width = '0%';
  barEl.style.background = 'var(--accent1)';

  try {
    await fetch(`${API}/api/nse-sync/force-today`, { method: 'POST' });
  } catch(e) {
    msgEl.textContent = 'Failed: ' + e.message;
    return;
  }

  const poll = setInterval(async () => {
    try {
      const res = await fetch(`${API}/api/nse-sync/status`);
      const s = await res.json();
      msgEl.textContent = s.message || 'Syncing...';
      if (s.total > 0) {
        const pct = Math.round((s.progress / s.total) * 100);
        progEl.textContent = `${s.progress}/${s.total} (${pct}%)`;
        barEl.style.width = pct + '%';
      }
      if (!s.running) {
        clearInterval(poll);
        msgEl.textContent = '✅ ' + (s.message || 'EOD sync complete!');
        barEl.style.width = '100%';
        barEl.style.background = 'var(--green)';
        // Auto-refresh breadth after 2 sec
        setTimeout(() => { loadBreadth(true); }, 2000);
      }
    } catch(e) {}
  }, 2000);
}

// ── Sync Fundamentals (EPS, PE, Market Cap from Yahoo Finance) ───────────────
async function startFundamentalsSync() {
  const statusEl = document.getElementById('nse-sync-status');
  const msgEl = document.getElementById('nse-sync-msg');
  const progEl = document.getElementById('nse-sync-progress');
  const barEl = document.getElementById('nse-sync-bar');
  if (!statusEl) return;

  statusEl.style.display = 'block';
  msgEl.textContent = 'Fetching EPS, PE, Market Cap for all tickers...';
  barEl.style.width = '0%';
  barEl.style.background = '#f59e0b';

  try {
    await fetch(`${API}/api/fundamentals/sync`, { method: 'POST' });
  } catch(e) {
    msgEl.textContent = 'Failed: ' + e.message;
    return;
  }

  const poll = setInterval(async () => {
    try {
      const res = await fetch(`${API}/api/nse-sync/status`);
      const s = await res.json();
      msgEl.textContent = s.message || 'Syncing fundamentals...';
      if (s.total > 0) {
        const pct = Math.round((s.progress / s.total) * 100);
        progEl.textContent = `${s.progress}/${s.total} (${pct}%)`;
        barEl.style.width = pct + '%';
      }
      if (!s.running) {
        clearInterval(poll);
        msgEl.textContent = '✅ ' + (s.message || 'Fundamentals sync complete!');
        barEl.style.width = '100%';
        barEl.style.background = 'var(--green)';
      }
    } catch(e) {}
  }, 3000);
}
