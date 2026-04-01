// ════════════════════════════════════════════════════════════════════════════
// SMART CHART — TradingView Lightweight Charts + Toggle Overlays
// Overlays: OHLCV, MA 50/150/200, RS Line, Sector RS, Quarterly, Ownership, Funds, Fundamentals
// ════════════════════════════════════════════════════════════════════════════

let _sc = {
  ticker: '',
  data: null,        // raw API response
  priceChart: null,
  volChart: null,
  candleSeries: null,
  volSeries: null,
  overlays: {},      // overlay name → series/markers
  activeOverlays: new Set(['ohlcv']),
};

const SC_COLORS = {
  ma50:  '#06b6d4',
  ma150: '#eab308',
  ma200: '#ef4444',
  rs:    '#3b82f6',
  secRs: '#f97316',
  qBuy:  'rgba(34,197,94,0.9)',
  qSell: 'rgba(239,68,68,0.9)',
  volUp: 'rgba(34,197,94,0.5)',
  volDn: 'rgba(239,68,68,0.5)',
};

// ── Load Smart Chart ─────────────────────────────────────────────────────────

async function loadSmartChart(ticker) {
  ticker = ticker || document.getElementById('sc-ticker-input')?.value?.trim().toUpperCase();
  if (!ticker) return;
  document.getElementById('sc-ticker-input').value = ticker;

  // Show loading
  const wrap = document.getElementById('sc-chart-wrap');
  const placeholder = document.getElementById('sc-placeholder');
  const container = document.getElementById('sc-chart-container');
  placeholder.innerHTML = '<div style="text-align:center;padding:40px"><div class="ai-spinner"></div><br><span style="color:var(--text3);font-size:12px;font-family:var(--font-mono)">Loading ' + ticker + '...</span></div>';
  placeholder.style.display = 'block';
  container.style.display = 'none';

  try {
    const res = await fetch(`${API}/api/smart-chart/${ticker}?days=500`);
    const data = await res.json();
    if (data.error) throw new Error(data.error);
    _sc.data = data;
    _sc.ticker = ticker;

    // Update header info
    _updateTickerHeader(data);

    // Show toggle bar
    document.getElementById('sc-toggle-bar').style.display = '';

    // Destroy old charts
    _destroyCharts();

    // Create charts
    placeholder.style.display = 'none';
    container.style.display = 'block';
    _createCharts();

    // Apply default overlays
    _sc.activeOverlays = new Set(['ohlcv']);
    document.querySelectorAll('.sc-toggle').forEach(b => {
      b.classList.toggle('active', b.dataset.overlay === 'ohlcv');
    });

    // Render peers
    _renderPeers(data.peers);

  } catch (e) {
    placeholder.innerHTML = `<div style="text-align:center;padding:40px;color:var(--red);font-family:var(--font-mono)">⚠ ${e.message}<br><button onclick="loadSmartChart('${ticker}')" style="margin-top:8px;padding:6px 16px;border-radius:6px;border:1px solid var(--border);background:var(--bg3);color:var(--cyan);cursor:pointer;font-family:var(--font-mono);font-size:11px">🔄 Retry</button></div>`;
    console.error('Smart Chart error:', e);
  }
}

// ── Update Header ────────────────────────────────────────────────────────────

function _updateTickerHeader(data) {
  const info = document.getElementById('sc-ticker-info');
  info.style.display = 'flex';

  const chart = data.chart || {};
  const candles = chart.candles || [];
  const last = candles.length ? candles[candles.length - 1] : {};
  const prev = candles.length > 1 ? candles[candles.length - 2] : {};
  const price = last.close || 0;
  const chg = prev.close ? ((price - prev.close) / prev.close * 100) : 0;

  document.getElementById('sc-name').textContent = data.ticker;
  document.getElementById('sc-price').textContent = '₹' + price.toLocaleString('en-IN', {maximumFractionDigits: 2});
  const chgEl = document.getElementById('sc-change');
  chgEl.textContent = (chg >= 0 ? '+' : '') + chg.toFixed(2) + '%';
  chgEl.style.color = chg >= 0 ? 'var(--green)' : 'var(--red)';

  const rs = data.rs || {};
  const rsEl = document.getElementById('sc-rs-badge');
  rsEl.textContent = 'RS ' + (rs.rs_rating || '—');
  rsEl.style.background = (rs.rs_rating || 0) >= 80 ? 'rgba(34,197,94,.15)' : 'rgba(100,116,139,.12)';
  rsEl.style.color = (rs.rs_rating || 0) >= 80 ? 'var(--green)' : 'var(--text2)';

  const adEl = document.getElementById('sc-ad-badge');
  adEl.textContent = 'A/D ' + (rs.ad_rating || '—');

  const secEl = document.getElementById('sc-sector-badge');
  secEl.textContent = rs.sector || '';
  secEl.style.display = rs.sector ? '' : 'none';
}

// ── Create Charts ────────────────────────────────────────────────────────────

function _destroyCharts() {
  if (_sc.priceChart) { try { _sc.priceChart.remove(); } catch(e){} _sc.priceChart = null; }
  if (_sc.volChart) { try { _sc.volChart.remove(); } catch(e){} _sc.volChart = null; }
  _sc.overlays = {};
  _sc.candleSeries = null;
  _sc.volSeries = null;
}

function _createCharts() {
  const LWC = window.LightweightCharts;
  if (!LWC) { console.error('LightweightCharts not loaded'); return; }

  const isDark = !document.documentElement.getAttribute('data-theme');
  const bg = isDark ? '#0a0e17' : '#ffffff';
  const txt = isDark ? '#94a3b8' : '#64748b';
  const grid = isDark ? 'rgba(30,41,59,0.5)' : 'rgba(226,232,240,0.5)';
  const border = isDark ? '#1e293b' : '#e2e8f0';

  const chartOpts = {
    layout: { background: { type: 'solid', color: bg }, textColor: txt, fontFamily: "'JetBrains Mono', monospace", fontSize: 10 },
    grid: { vertLines: { color: grid }, horzLines: { color: grid } },
    crosshair: { mode: 0 },
    rightPriceScale: { borderColor: border },
    timeScale: { borderColor: border, timeVisible: false, fixLeftEdge: true, fixRightEdge: true },
    handleScroll: { vertTouchDrag: false },
  };

  // Price chart
  const priceEl = document.getElementById('sc-price-chart');
  priceEl.innerHTML = '';
  _sc.priceChart = LWC.createChart(priceEl, { ...chartOpts, height: 400 });

  // Candlestick series
  const candles = (_sc.data.chart?.candles || []).map(c => ({
    time: c.time, open: c.open, high: c.high, low: c.low, close: c.close,
  }));
  _sc.candleSeries = _sc.priceChart.addCandlestickSeries({
    upColor: '#22c55e', downColor: '#ef4444', borderUpColor: '#22c55e', borderDownColor: '#ef4444',
    wickUpColor: '#22c55e', wickDownColor: '#ef4444',
  });
  _sc.candleSeries.setData(candles);

  // Volume chart
  const volEl = document.getElementById('sc-vol-chart');
  volEl.innerHTML = '';
  _sc.volChart = LWC.createChart(volEl, { ...chartOpts, height: 80 });
  _sc.volChart.timeScale().applyOptions({ visible: true });

  const volBars = _sc.data.chart?.volume?.bars || [];
  const volData = volBars.map(v => ({
    time: v.time, value: v.value || 0,
    color: v.color || SC_COLORS.volUp,
  }));
  _sc.volSeries = _sc.volChart.addHistogramSeries({ priceFormat: { type: 'volume' }, priceScaleId: '' });
  _sc.volSeries.setData(volData);

  // Sync time scales
  _sc.priceChart.timeScale().subscribeVisibleLogicalRangeChange(range => {
    if (range) _sc.volChart.timeScale().setVisibleLogicalRange(range);
  });
  _sc.volChart.timeScale().subscribeVisibleLogicalRangeChange(range => {
    if (range) _sc.priceChart.timeScale().setVisibleLogicalRange(range);
  });

  _sc.priceChart.timeScale().fitContent();
  _sc.volChart.timeScale().fitContent();
}

// ── Toggle Overlay ───────────────────────────────────────────────────────────

function toggleOverlay(name, btn) {
  if (name === 'ohlcv') return; // Always on
  if (_sc.activeOverlays.has(name)) {
    _sc.activeOverlays.delete(name);
    btn.classList.remove('active');
    _removeOverlay(name);
  } else {
    _sc.activeOverlays.add(name);
    btn.classList.add('active');
    _addOverlay(name);
  }
}

function _addOverlay(name) {
  if (!_sc.priceChart || !_sc.data) return;
  const candles = _sc.data.chart?.candles || [];
  const LWC = window.LightweightCharts;

  switch (name) {
    case 'ma': _addMAs(candles); break;
    case 'rs': _addRSLine(candles); break;
    case 'sector_rs': _addSectorRS(); break;
    case 'quarterly': _addQuarterlyMarkers(); break;
    case 'ownership': _showOwnershipPanel(); break;
    case 'funds': _showFundsOverlay(); break;
    case 'fundamentals': _showFundamentalsPanel(); break;
  }
}

function _removeOverlay(name) {
  // Remove series from chart
  if (_sc.overlays[name]) {
    if (Array.isArray(_sc.overlays[name])) {
      _sc.overlays[name].forEach(s => { try { _sc.priceChart.removeSeries(s); } catch(e){} });
    } else {
      try { _sc.priceChart.removeSeries(_sc.overlays[name]); } catch(e){}
    }
    delete _sc.overlays[name];
  }
  // Remove markers
  if (name === 'quarterly') {
    _sc.candleSeries?.setMarkers([]);
    document.getElementById('sc-quarterly-panel').style.display = 'none';
  }
  // Hide panels
  if (name === 'ownership') document.getElementById('sc-ownership-panel').style.display = 'none';
  if (name === 'fundamentals') document.getElementById('sc-fund-panel').style.display = 'none';
  if (name === 'funds') document.getElementById('sc-ownership-panel').style.display = 'none';
}

// ── MA Overlay ───────────────────────────────────────────────────────────────

function _addMAs(candles) {
  const closes = candles.map(c => c.close);
  const series = [];

  [{ period: 50, color: SC_COLORS.ma50 }, { period: 150, color: SC_COLORS.ma150 }, { period: 200, color: SC_COLORS.ma200 }].forEach(({ period, color }) => {
    const maData = [];
    for (let i = period - 1; i < closes.length; i++) {
      const slice = closes.slice(i - period + 1, i + 1);
      const avg = slice.reduce((a, b) => a + b, 0) / period;
      maData.push({ time: candles[i].time, value: avg });
    }
    const s = _sc.priceChart.addLineSeries({ color, lineWidth: 1, priceLineVisible: false, lastValueVisible: false, crosshairMarkerVisible: false });
    s.setData(maData);
    series.push(s);
  });

  _sc.overlays.ma = series;
}

// ── RS Line Overlay ──────────────────────────────────────────────────────────

function _addRSLine(candles) {
  // RS is a running relative strength vs benchmark
  // We compute a simple RS proxy: (stock close / benchmark close) normalized
  const closes = candles.map(c => c.close);
  if (closes.length < 20) return;

  // Compute RS as percentrank proxy over 63-day windows
  const rsData = [];
  for (let i = 63; i < closes.length; i++) {
    const window = closes.slice(i - 63, i + 1);
    const change = (window[window.length - 1] - window[0]) / window[0] * 100;
    // Normalize to 1-99 range approximation
    const rs = Math.min(99, Math.max(1, 50 + change * 2));
    rsData.push({ time: candles[i].time, value: rs });
  }

  const s = _sc.priceChart.addLineSeries({
    color: SC_COLORS.rs, lineWidth: 2, priceLineVisible: false, lastValueVisible: true,
    priceScaleId: 'rs', lineStyle: 0,
  });
  s.setData(rsData);
  _sc.priceChart.priceScale('rs').applyOptions({
    scaleMargins: { top: 0.7, bottom: 0.0 },
    borderVisible: false,
  });

  _sc.overlays.rs = s;
}

// ── Sector RS Overlay ────────────────────────────────────────────────────────

function _addSectorRS() {
  // Placeholder: show RS rating as horizontal line
  const rs = _sc.data.rs?.rs_rating;
  if (!rs || !_sc.priceChart) return;

  // Add as a dotted line at the RS level on the RS scale
  const candles = _sc.data.chart?.candles || [];
  if (candles.length < 2) return;

  const sectorLine = candles.map(c => ({ time: c.time, value: rs }));
  const s = _sc.priceChart.addLineSeries({
    color: SC_COLORS.secRs, lineWidth: 1, lineStyle: 2, // dashed
    priceLineVisible: false, lastValueVisible: false,
    priceScaleId: 'rs',
  });
  s.setData(sectorLine);
  _sc.overlays.sector_rs = s;
}

// ── Quarterly Results Markers ────────────────────────────────────────────────

function _addQuarterlyMarkers() {
  const quarterly = _sc.data.screener_in?.quarterly || [];
  const candles = _sc.data.chart?.candles || [];
  if (!quarterly.length || !candles.length) {
    _showQuarterlyPanel(quarterly);
    return;
  }

  // Map quarter names to approximate dates
  const markers = [];
  const quarterDateMap = {};
  quarterly.forEach(q => {
    const qd = _quarterToDate(q.quarter);
    if (qd) quarterDateMap[qd] = q;
  });

  // Find matching candle dates (closest)
  candles.forEach(c => {
    Object.keys(quarterDateMap).forEach(qd => {
      // Match if within 15 days
      const cd = new Date(c.time);
      const qDate = new Date(qd);
      const diff = Math.abs(cd - qDate) / (1000 * 60 * 60 * 24);
      if (diff < 15) {
        const q = quarterDateMap[qd];
        const epsChg = q.eps !== undefined ? (q.eps >= 0 ? '+' : '') + q.eps.toFixed(2) : '';
        markers.push({
          time: c.time,
          position: 'aboveBar',
          color: (q.profit || 0) >= 0 ? SC_COLORS.qBuy : SC_COLORS.qSell,
          shape: 'circle',
          text: q.quarter + (epsChg ? ' EPS:' + epsChg : ''),
        });
        delete quarterDateMap[qd]; // Avoid duplicates
      }
    });
  });

  if (markers.length) {
    markers.sort((a, b) => (a.time > b.time ? 1 : -1));
    _sc.candleSeries.setMarkers(markers);
  }

  _showQuarterlyPanel(quarterly);
}

function _quarterToDate(qStr) {
  // Convert "Mar 2025" → "2025-03-31"
  const months = { jan:'01', feb:'02', mar:'03', apr:'04', may:'05', jun:'06', jul:'07', aug:'08', sep:'09', oct:'10', nov:'11', dec:'12' };
  const parts = qStr.trim().split(/\s+/);
  if (parts.length < 2) return null;
  const mon = months[parts[0].toLowerCase().slice(0,3)];
  const year = parts[1] || parts[0];
  if (!mon) return null;
  // Use last day of month
  const lastDay = new Date(parseInt(year), parseInt(mon), 0).getDate();
  return `${year}-${mon}-${String(lastDay).padStart(2,'0')}`;
}

function _showQuarterlyPanel(quarterly) {
  const panel = document.getElementById('sc-quarterly-panel');
  if (!quarterly.length) {
    panel.innerHTML = '<div style="padding:8px;color:var(--text3);font-size:10px">No quarterly data from screener.in</div>';
    panel.style.display = 'block';
    return;
  }
  const rows = quarterly.slice(0, 8).map(q => {
    const epsColor = (q.eps || 0) >= 0 ? 'var(--green)' : 'var(--red)';
    return `<tr>
      <td style="font-weight:600">${q.quarter}</td>
      <td style="color:${epsColor}">${q.eps?.toFixed(2) ?? '—'}</td>
      <td>${q.sales?.toLocaleString('en-IN') ?? '—'}</td>
      <td style="color:${(q.profit||0)>=0?'var(--green)':'var(--red)'}">${q.profit?.toLocaleString('en-IN') ?? '—'}</td>
    </tr>`;
  }).join('');
  panel.innerHTML = `<div class="sc-panel-title">💰 Quarterly Results</div>
    <table class="sc-mini-table"><thead><tr><th>Qtr</th><th>EPS</th><th>Sales Cr</th><th>Profit Cr</th></tr></thead>
    <tbody>${rows}</tbody></table>`;
  panel.style.display = 'block';
}

// ── Ownership Panel ──────────────────────────────────────────────────────────

function _showOwnershipPanel() {
  const sh = _sc.data.screener_in?.shareholding || [];
  const panel = document.getElementById('sc-ownership-panel');
  if (!sh.length) {
    panel.innerHTML = '<div style="padding:8px;color:var(--text3);font-size:10px">No shareholding data</div>';
    panel.style.display = 'block';
    return;
  }
  const rows = sh.slice(0, 6).map(s => `<tr>
    <td style="font-weight:600">${s.quarter}</td>
    <td>${s.promoters?.toFixed(1) ?? '—'}%</td>
    <td>${s.fii?.toFixed(1) ?? '—'}%</td>
    <td>${s.dii?.toFixed(1) ?? '—'}%</td>
    <td>${s.public?.toFixed(1) ?? '—'}%</td>
  </tr>`).join('');
  panel.innerHTML = `<div class="sc-panel-title">👥 Shareholding Pattern</div>
    <table class="sc-mini-table"><thead><tr><th>Qtr</th><th>Promoter</th><th>FII</th><th>DII</th><th>Public</th></tr></thead>
    <tbody>${rows}</tbody></table>`;
  panel.style.display = 'block';
}

// ── Funds Overlay ────────────────────────────────────────────────────────────

function _showFundsOverlay() {
  const sh = _sc.data.screener_in?.shareholding || [];
  const panel = document.getElementById('sc-ownership-panel');
  // Show fund/shareholder counts if available
  const rows = sh.filter(s => s.num_shareholders).slice(0, 6).map(s => `<tr>
    <td style="font-weight:600">${s.quarter}</td>
    <td>${s.num_shareholders?.toLocaleString() ?? '—'}</td>
  </tr>`).join('');

  if (rows) {
    panel.innerHTML = `<div class="sc-panel-title">🏦 Shareholders</div>
      <table class="sc-mini-table"><thead><tr><th>Qtr</th><th>No. of Shareholders</th></tr></thead>
      <tbody>${rows}</tbody></table>`;
    panel.style.display = 'block';
  } else {
    // Fallback to ownership view
    _showOwnershipPanel();
  }
}

// ── Fundamentals Panel ───────────────────────────────────────────────────────

function _showFundamentalsPanel() {
  const tv = _sc.data.fundamentals || {};
  const rs = _sc.data.rs || {};
  const ratios = _sc.data.screener_in?.ratios || {};
  const panel = document.getElementById('sc-fund-panel');

  const items = [
    ['RS Rating', rs.rs_rating, rs.rs_rating >= 80 ? 'var(--green)' : rs.rs_rating >= 60 ? 'var(--amber)' : 'var(--red)'],
    ['A/D Rating', rs.ad_rating, 'var(--text)'],
    ['PE Ratio', tv.pe_ratio?.toFixed(1) || ratios['Stock P/E'] || '—', 'var(--text)'],
    ['ROE %', tv.roe?.toFixed(1) || ratios['ROE'] || '—', 'var(--text)'],
    ['Debt/Equity', tv.debt_equity?.toFixed(2) || ratios['Debt to equity'] || '—', 'var(--text)'],
    ['Mcap Cr', rs.mcap_cr ? '₹' + Math.round(rs.mcap_cr).toLocaleString('en-IN') : '—', 'var(--text)'],
    ['From High', rs.pct_from_high ? rs.pct_from_high.toFixed(1) + '%' : '—', (rs.pct_from_high||-99) > -10 ? 'var(--green)' : 'var(--red)'],
    ['Vol Ratio', rs.vol_ratio?.toFixed(2) + 'x' || '—', (rs.vol_ratio||0) > 1.5 ? 'var(--cyan)' : 'var(--text)'],
    ['1W Chg', rs.chg_1w ? (rs.chg_1w >= 0 ? '+' : '') + rs.chg_1w.toFixed(1) + '%' : '—', (rs.chg_1w||0) >= 0 ? 'var(--green)' : 'var(--red)'],
    ['3M Chg', rs.chg_3m ? (rs.chg_3m >= 0 ? '+' : '') + rs.chg_3m.toFixed(1) + '%' : '—', (rs.chg_3m||0) >= 0 ? 'var(--green)' : 'var(--red)'],
    ['Trend Template', rs.trend_template ? '✅ Pass' : '❌ Fail', rs.trend_template ? 'var(--green)' : 'var(--red)'],
  ];

  panel.innerHTML = `<div class="sc-panel-title">📋 Fundamentals</div>
    <div class="sc-fund-grid">${items.map(([k, v, c]) =>
      `<div class="sc-fund-row"><span class="sc-fund-label">${k}</span><span class="sc-fund-value" style="color:${c}">${v ?? '—'}</span></div>`
    ).join('')}</div>`;
  panel.style.display = 'block';
}

// ── Peers ────────────────────────────────────────────────────────────────────

function _renderPeers(peers) {
  const wrap = document.getElementById('sc-peers');
  const list = document.getElementById('sc-peers-list');
  if (!peers || !peers.length) { wrap.style.display = 'none'; return; }
  wrap.style.display = '';
  list.innerHTML = peers.map(p => `
    <div class="sc-peer-chip" onclick="loadSmartChart('${p.ticker}')">
      <span style="font-weight:700">${p.ticker}</span>
      <span class="sc-peer-rs">RS ${p.rs}</span>
      <span class="sc-peer-ad">${p.ad}</span>
    </div>
  `).join('');
}

// ── Init: Enter key support ──────────────────────────────────────────────────

document.addEventListener('DOMContentLoaded', () => {
  const inp = document.getElementById('sc-ticker-input');
  if (inp) inp.addEventListener('keydown', e => { if (e.key === 'Enter') loadSmartChart(); });
});
