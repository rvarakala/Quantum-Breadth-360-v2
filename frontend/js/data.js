// ─── DATA FETCHING ────────────────────────────────────────────────────────────

const AUTO_REFRESH_INTERVAL = 15 * 60 * 1000; // 15 minutes
let _autoRefreshTimer = null;

async function loadBreadth(force = false) {
  const btn = $('refresh-btn');
  btn.classList.add('loading');

  try {
    const url = `${API}/api/breadth/${currentMarket}${force ? '?refresh=true' : ''}`;
    const res = await fetch(url);
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const data = await res.json();

    if (data.error) {
      showError(data.error);
      return;
    }

    currentData[currentMarket] = data;
    lastUpdated[currentMarket] = new Date();

    renderOverview(data);
    renderCharts(data);
    renderSectors(data);
    updateFreshness();

    // Also refresh compare data
    loadCompare();

  } catch (e) {
    console.error('Fetch error:', e);
    showError('Backend not reachable. Start the backend server first.');
  } finally {
    btn.classList.remove('loading');
  }
}

async function autoRefreshAll() {
  console.log('[Auto-Refresh] Refreshing all data...');

  // 1. Breadth (Overview + Charts)
  try { await loadBreadth(true); } catch(e) { console.warn('Auto-refresh breadth failed:', e); }

  // 2. Active tab data — refresh whatever tab is currently open
  const activeTab = document.querySelector('.nav-item.active')?.dataset?.tab;
  try {
    if (activeTab === 'smart-money' && typeof loadSmartMoney === 'function') {
      _smMoneyData = null; loadSmartMoney();
    }
    if (activeTab === 'insider' && typeof loadInsiderData === 'function') {
      _insiderData = []; loadInsiderData();
    }
    if (activeTab === 'leaders' && typeof initLeadersTab === 'function') {
      _leadersData = null; initLeadersTab();
    }
    if (activeTab === 'fvalue' && typeof onFValueTabLoad === 'function') {
      _fvLoaded = false; onFValueTabLoad();
    }
    if (activeTab === 'scanner') {
      _scannerData = null;
    }
    if (activeTab === 'stockbee' && typeof loadStockbee === 'function') {
      _stockbeeData = null; loadStockbee();
    }
    if (activeTab === 'fiidii' && typeof onFiiDiiTabLoad === 'function') {
      onFiiDiiTabLoad();
    }
  } catch(e) { console.warn('Auto-refresh tab data failed:', e); }

  console.log('[Auto-Refresh] Complete. Next in 15 mins.');
}

function startAutoRefresh() {
  if (_autoRefreshTimer) clearInterval(_autoRefreshTimer);
  _autoRefreshTimer = setInterval(autoRefreshAll, AUTO_REFRESH_INTERVAL);
  console.log('[Auto-Refresh] Started — every 15 minutes');
}

// Start auto-refresh when page loads
document.addEventListener('DOMContentLoaded', () => {
  setTimeout(startAutoRefresh, 5000); // start 5s after page load
});

async function loadCompare() {
  try {
    const res = await fetch(`${API}/api/compare`);
    if (!res.ok) return;
    const data = await res.json();
    renderCompare(data.India, data.US);
  } catch {}
}

function showError(msg) {
  const grid = $('sector-grid');
  // Show error in score area
  $('score-interp').textContent = `⚠ ${msg}`;
  $('score-interp').style.color = '#ef4444';
}

function updateFreshness() {
  const lu = lastUpdated[currentMarket];
  const dot = document.querySelector('.freshness-dot');
  const txt = $('freshness-text');
  if (!lu) {
    dot.classList.add('stale');
    txt.textContent = 'Not loaded';
    return;
  }

  // Show OHLCV data date if available — more meaningful than API call time
  const data = currentData[currentMarket];
  const ohlcvDate = data?.last_ohlcv_date;
  const freshness = data?.data_freshness;

  if (ohlcvDate && ohlcvDate !== 'unknown') {
    dot.classList.remove('stale');
    const d = new Date(ohlcvDate);
    const formatted = d.toLocaleDateString('en-IN', {day:'2-digit', month:'short'});
    if (freshness === 'today') {
      txt.textContent = `Data: Today (${formatted})`;
      dot.style.background = 'var(--green)';
    } else if (freshness === 'EOD') {
      txt.textContent = `Data: EOD ${formatted}`;
      dot.style.background = '#f59e0b';
      dot.classList.add('stale');
    } else {
      txt.textContent = `Data: ${formatted} ⚠ stale`;
      dot.style.background = 'var(--red)';
      dot.classList.add('stale');
    }
    return;
  }

  // Fallback: show API call age
  const age = Math.round((Date.now() - lu) / 1000);
  dot.classList.remove('stale');
  if (age < 30) txt.textContent = 'Just updated';
  else if (age < 60) txt.textContent = `${age}s ago`;
  else {
    const m = Math.round(age / 60);
    txt.textContent = `${m}m ago`;
    if (m > 20) dot.classList.add('stale');
  }
}

// Update freshness text every 30 seconds to show live age
setInterval(() => {
  if (lastUpdated[currentMarket]) updateFreshness();
}, 30000);

// ─── NAVIGATION ───────────────────────────────────────────────────────────────

function switchMarket(market) {
  currentMarket = market;
  // Update sidebar toggle
  document.querySelectorAll('.market-tab').forEach(t => {
    t.classList.toggle('active', t.dataset.market === market);
  });
  // Update top nav toggle
  document.querySelectorAll('.nav-mkt-btn').forEach(t => {
    t.classList.toggle('active', t.dataset.market === market);
  });

  if (market === 'COMPARE') {
    switchTab('compare');
    loadCompare();
    return;
  }
  switchTab('overview');

  // Reset tab caches so they reload for new market
  _scannerData = null;
  _fvLoaded = false;
  if (typeof _leadersData !== 'undefined') _leadersData = null;
  if (typeof _stockbeeData !== 'undefined') _stockbeeData = null;

  if (currentData[market]) {
    renderOverview(currentData[market]);
    renderCharts(currentData[market]);
    renderSectors(currentData[market]);
    updateFreshness();
  } else {
    loadBreadth(false);
  }
}

// ════════════════════════════════════════════════════════════════════════════
// MARKET-AWARE UTILITIES — returns correct labels/symbols for current market
// ════════════════════════════════════════════════════════════════════════════

function mktCurrency()    { return currentMarket === 'US' ? '$' : '₹'; }
function mktLocale()      { return currentMarket === 'US' ? 'en-US' : 'en-IN'; }
function mktIndexName()   { return currentMarket === 'US' ? 'Russell 3000' : 'NIFTY 500'; }
function mktIndexLabel()  { return currentMarket === 'US' ? 'R3000' : 'NIFTY'; }
function mktExchange()    { return currentMarket === 'US' ? 'NYSE / NASDAQ' : 'NSE / BSE'; }
function mktVixLabel()    { return currentMarket === 'US' ? 'VIX' : 'VIX'; }
function mktBenchmark()   { return currentMarket === 'US' ? '^RUA' : '^CRSLDX'; }
function mktStrongerLabel() { return currentMarket === 'US' ? 'Stronger Than R3000' : 'Stronger Than Nifty'; }
function mktDbMarket()    { return currentMarket === 'US' ? 'US' : 'India'; }

function mktFormatPrice(val) {
  if (!val && val !== 0) return '—';
  const cur = mktCurrency();
  const loc = mktLocale();
  return cur + Number(val).toLocaleString(loc, {maximumFractionDigits: 1});
}

function mktFormatValue(val) {
  if (!val) return '—';
  const cur = mktCurrency();
  if (currentMarket === 'US') {
    if (val >= 1e9)  return cur + (val/1e9).toFixed(1) + 'B';
    if (val >= 1e6)  return cur + (val/1e6).toFixed(1) + 'M';
    if (val >= 1e3)  return cur + (val/1e3).toFixed(0) + 'K';
    return cur + val.toFixed(0);
  } else {
    if (val >= 1_00_00_000) return cur + (val/1_00_00_000).toFixed(1) + 'Cr';
    if (val >= 1_00_000)    return cur + (val/1_00_000).toFixed(1) + 'L';
    if (val >= 1000)        return cur + (val/1000).toFixed(0) + 'K';
    return cur + val.toFixed(0);
  }
}
