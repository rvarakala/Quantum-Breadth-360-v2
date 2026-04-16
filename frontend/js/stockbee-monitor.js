// ════════════════════════════════════════════════════════════════════════════
// STOCKBEE MARKET MONITOR — Full breadth dashboard
// Q25 ratio, daily table, 6 monitor cards (bar + line charts)
// ════════════════════════════════════════════════════════════════════════════

let _sbmData = null;
let _sbmCharts = {};

function sbmSwitchView(view) {
  document.getElementById('sbm-monitor-view').style.display = view === 'monitor' ? '' : 'none';
  document.getElementById('sbm-classic-view').style.display = view === 'classic' ? '' : 'none';
  document.getElementById('sbm-v-monitor').classList.toggle('active', view === 'monitor');
  document.getElementById('sbm-v-classic').classList.toggle('active', view === 'classic');
  if (view === 'monitor' && !_sbmData) loadStockbeeMonitor();
  if (view === 'classic' && !_stockbeeData) loadStockbee();
}

async function loadStockbeeMonitor() {
  const universe = document.getElementById('sbm-universe')?.value || 'nifty500';
  const wrap = document.getElementById('sbm-table-wrap');
  if (wrap) wrap.innerHTML = _skeletonTable(6, 8, 'Loading market monitor…');

  try {
    const res = await fetch(`${API}/api/stockbee-monitor?universe=${universe}&days=400`);
    _sbmData = await res.json();
    if (_sbmData.error) throw new Error(_sbmData.error);
    console.log('Stockbee Monitor:', _sbmData.ticker_count, 'tickers,', _sbmData.daily?.length, 'days');
    _sbmRender();
  } catch (e) {
    if (wrap) wrap.innerHTML = `<div style="text-align:center;padding:30px;color:var(--red);font-family:var(--font-mono)">⚠ ${e.message}<br><button onclick="loadStockbeeMonitor()" style="margin-top:8px" class="sm-export-btn">🔄 Retry</button></div>`;
  }
}

function _sbmRender() {
  const d = _sbmData;
  if (!d || !d.daily?.length) return;

  _sbmRenderQ25Chart();
  _sbmRenderTable();
  _sbmRenderCards();
}

// ── Q25 Primary Breadth Ratio (top line chart) ───────────────────────────────

function _sbmRenderQ25Chart() {
  const hist = _sbmData.q25_ratio_history || [];
  if (!hist.length) return;
  const canvas = document.getElementById('sbm-chart-q25');
  if (!canvas) return;

  const info = document.getElementById('sbm-q25-info');
  if (info) info.textContent = `${_sbmData.ticker_count} tickers · ${_sbmData.date_range?.from} to ${_sbmData.date_range?.to}`;

  const labels = hist.map(h => h.date.slice(2)); // YY-MM-DD
  const vals = hist.map(h => h.ratio);

  if (_sbmCharts['q25']) _sbmCharts['q25'].destroy();
  _sbmCharts['q25'] = new Chart(canvas.getContext('2d'), {
    type: 'line',
    data: { labels, datasets: [{ data: vals, borderColor: '#3b82f6', backgroundColor: 'rgba(59,130,246,0.08)', fill: true, borderWidth: 1.5, pointRadius: 0, tension: 0.3 }] },
    options: { responsive: true, maintainAspectRatio: false, animation: { duration: 400 },
      plugins: { legend: { display: false }, tooltip: { mode: 'index', intersect: false, backgroundColor: '#111827', borderColor: '#253552', borderWidth: 1, titleFont: { family: "'Space Mono'", size: 9 }, bodyFont: { family: "'Space Mono'", size: 10 } } },
      scales: { x: { grid: { display: false }, ticks: { maxTicksLimit: 12, color: '#4b5e7a', font: { size: 8 } } }, y: { grid: { color: '#1e2d4a33' }, ticks: { color: '#4b5e7a' } } }
    }
  });
}

// ── Daily Breadth Table (last 10 days) ───────────────────────────────────────

function _sbmRenderTable() {
  const daily = _sbmData.daily || [];
  const last10 = daily.slice(-10).reverse(); // newest first
  if (!last10.length) return;

  const wrap = document.getElementById('sbm-table-wrap');
  const gc = v => v > 0 ? 'color:var(--green)' : 'color:var(--red)';

  const rows = last10.map(m => {
    const dt = new Date(m.date);
    const dateStr = dt.toLocaleDateString('en-IN', { day: '2-digit', month: 'short' });
    return `<tr class="sbm-tr">
      <td class="sbm-td">${dateStr}</td>
      <td class="sbm-td" style="color:var(--green);font-weight:700">${m.bull_4pct}</td>
      <td class="sbm-td" style="color:var(--red);font-weight:700">${m.bear_4pct}</td>
      <td class="sbm-td">${m.ratio_5d}</td>
      <td class="sbm-td">${m.ratio_10d}</td>
      <td class="sbm-td" style="color:var(--green)">${m.q25_bull}</td>
      <td class="sbm-td" style="color:var(--red)">${m.q25_bear}</td>
      <td class="sbm-td" style="color:var(--green)">${m.m25_bull}</td>
      <td class="sbm-td" style="color:var(--red)">${m.m25_bear}</td>
      <td class="sbm-td" style="color:var(--green)">${m.m50_bull}</td>
      <td class="sbm-td" style="color:var(--red)">${m.m50_bear}</td>
      <td class="sbm-td" style="color:var(--green)">${m.ema34_13_bull}</td>
      <td class="sbm-td" style="color:var(--red)">${m.ema34_13_bear}</td>
    </tr>`;
  }).join('');

  wrap.innerHTML = `<table class="sbm-table">
    <thead><tr>
      <th class="sbm-th">DATE</th>
      <th class="sbm-th" style="color:var(--green)">4% ↑</th><th class="sbm-th" style="color:var(--red)">4% ↓</th>
      <th class="sbm-th">5D Ratio</th><th class="sbm-th">10D Ratio</th>
      <th class="sbm-th" style="color:var(--green)">Q25 ↑</th><th class="sbm-th" style="color:var(--red)">Q25 ↓</th>
      <th class="sbm-th" style="color:var(--green)">M25 ↑</th><th class="sbm-th" style="color:var(--red)">M25 ↓</th>
      <th class="sbm-th" style="color:var(--green)">M50 ↑</th><th class="sbm-th" style="color:var(--red)">M50 ↓</th>
      <th class="sbm-th" style="color:var(--green)">34/13 ↑</th><th class="sbm-th" style="color:var(--red)">34/13 ↓</th>
    </tr></thead>
    <tbody>${rows}</tbody>
  </table>`;
}

// ── 6 Monitor Cards ──────────────────────────────────────────────────────────

function _sbmRenderCards() {
  const daily = _sbmData.daily || [];
  if (!daily.length) return;
  const last = daily[daily.length - 1];
  const dateStr = last.date.slice(5);

  // Row 1: 4% | 34/13 | 10-DCR 4% | Q25
  _sbmBarChart('sbm-chart-4pct', `${last.bull_4pct} bull`, `${last.bear_4pct} bear`, last.bull_4pct, last.bear_4pct);
  _setDate('sbm-card1-date', dateStr);

  _sbmBarChart('sbm-chart-3413', `${last.ema34_13_bull} bull`, `${last.ema34_13_bear} bear`, last.ema34_13_bull, last.ema34_13_bear);
  _setDate('sbm-card2-date', dateStr);

  _sbmLineChart('sbm-chart-dcr4', daily, 'dcr_4pct_10d', '10-dcr');
  _setDate('sbm-card3-date', `${daily.length} days`);

  _sbmBarChart('sbm-chart-q25bar', `${last.q25_bull} bull`, `${last.q25_bear} bear`, last.q25_bull, last.q25_bear);
  _setDate('sbm-card5-date', dateStr);

  // Row 2: M25 | M50 | 10-DCR 20DMA | 10-DCR 50DMA
  _sbmBarChart('sbm-chart-m25', `${last.m25_bull} bull`, `${last.m25_bear} bear`, last.m25_bull, last.m25_bear);
  _setDate('sbm-card4-date', dateStr);

  _sbmBarChart('sbm-chart-m50', `${last.m50_bull} bull`, `${last.m50_bear} bear`, last.m50_bull, last.m50_bear);
  _setDate('sbm-card7-date', dateStr);

  _sbmLineChart('sbm-chart-dcrm25', daily, 'dcr_m25_10d', '10-dcr');
  _setDate('sbm-card6-date', `${daily.length} days`);

  _sbmLineChart('sbm-chart-dcrm50', daily, 'dcr_m50_10d', '10-dcr');
  _setDate('sbm-card8-date', `${daily.length} days`);
}

function _setDate(id, val) { const el = document.getElementById(id); if (el) el.textContent = val; }

function _sbmBarChart(canvasId, label1, label2, val1, val2) {
  const canvas = document.getElementById(canvasId);
  if (!canvas) return;
  if (_sbmCharts[canvasId]) _sbmCharts[canvasId].destroy();
  _sbmCharts[canvasId] = new Chart(canvas.getContext('2d'), {
    type: 'bar',
    data: {
      labels: [label1, label2],
      datasets: [{ data: [val1, val2], backgroundColor: ['rgba(100,200,220,0.7)', 'rgba(220,100,130,0.7)'], borderWidth: 0, barPercentage: 0.7 }]
    },
    options: {
      indexAxis: 'y',
      responsive: true, maintainAspectRatio: false,
      animation: { duration: 400 },
      plugins: { legend: { display: false },
        tooltip: { backgroundColor: '#111827', borderColor: '#253552', borderWidth: 1, bodyFont: { family: "'Space Mono'", size: 10 } } },
      scales: {
        x: { grid: { color: '#1e2d4a33' }, ticks: { color: '#4b5e7a', font: { size: 9 } }, beginAtZero: true },
        y: { grid: { display: false }, ticks: { color: '#94a3b8', font: { family: "'Space Mono'", size: 10 } } }
      }
    }
  });
}

function _sbmLineChart(canvasId, data, field, label) {
  const canvas = document.getElementById(canvasId);
  if (!canvas) return;
  const labels = data.map(d => d.date.slice(2));
  const vals = data.map(d => d[field] || 0);
  if (_sbmCharts[canvasId]) _sbmCharts[canvasId].destroy();
  _sbmCharts[canvasId] = new Chart(canvas.getContext('2d'), {
    type: 'line',
    data: { labels, datasets: [
      { label, data: vals, borderColor: '#67d4e5', backgroundColor: 'rgba(100,210,230,0.06)', fill: true, borderWidth: 1.5, pointRadius: 0, tension: 0.3 },
    ] },
    options: {
      responsive: true, maintainAspectRatio: false,
      animation: { duration: 400 },
      plugins: {
        legend: { display: false },
        tooltip: { mode: 'index', intersect: false, backgroundColor: '#111827', borderColor: '#253552', borderWidth: 1, titleFont: { family: "'Space Mono'", size: 9 }, bodyFont: { family: "'Space Mono'", size: 10 } },
        annotation: undefined,
      },
      scales: {
        x: { grid: { display: false }, ticks: { maxTicksLimit: 8, color: '#4b5e7a', font: { size: 7 } } },
        y: { grid: { color: '#1e2d4a33' }, ticks: { color: '#4b5e7a', font: { size: 9 } }, beginAtZero: true }
      }
    }
  });
}
