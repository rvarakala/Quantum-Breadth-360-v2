/* ═══════════════════════════════════════════════════════════════════════════
   FII/DII TAB — Institutional Money Matrix
   Full dashboard: session data, streaks, velocity, matrices, chart
   ═══════════════════════════════════════════════════════════════════════════ */

let _fdLoaded = false;
let _fdData = null;

async function onFiiDiiTabLoad() {
  if (!_fdLoaded) loadFiiDiiTab();
}

async function loadFiiDiiTab() {
  try {
    const res = await fetch(`${API}/api/fiidii/summary?days=45`);
    const data = await res.json();
    if (data.error) {
      document.getElementById('fd-session-date').textContent = data.error;
      return;
    }
    _fdData = data;
    _fdLoaded = true;
    _renderFiiDiiTab(data);
  } catch (e) {
    document.getElementById('fd-session-date').textContent = '⚠ ' + e.message;
  }
}

function _fmtCr(v) {
  if (v == null) return '—';
  const sign = v >= 0 ? '+' : '';
  const abs = Math.abs(v);
  if (abs >= 10000) return `${sign}₹${(v/100).toFixed(0)}L Cr`;
  return `${sign}₹${v.toLocaleString('en-IN', {maximumFractionDigits:2})} Cr`;
}

function _fmtCrBig(v) {
  if (v == null) return '—';
  const sign = v >= 0 ? '+' : '';
  return `${sign}₹${Math.abs(v).toLocaleString('en-IN', {maximumFractionDigits:2})} Cr`;
}

function _renderFiiDiiTab(data) {
  const s = data.latest;
  const st = data.streaks;
  const c = data.cumulative;
  const h = data.history || [];

  // Sentiment badge
  const sentEl = document.getElementById('fd-sentiment');
  if (sentEl) {
    sentEl.textContent = data.sentiment;
    sentEl.style.color = data.sentiment_color;
    sentEl.style.background = data.sentiment_color + '18';
    sentEl.style.border = `1px solid ${data.sentiment_color}44`;
  }

  // Session date
  const dateEl = document.getElementById('fd-session-date');
  if (dateEl) {
    const dt = new Date(s.date);
    dateEl.textContent = dt.toLocaleDateString('en-IN', {weekday:'long', day:'numeric', month:'long', year:'numeric'});
  }

  // FII/DII Net values
  const fiiNetEl = document.getElementById('fd-fii-net');
  if (fiiNetEl) { fiiNetEl.textContent = _fmtCrBig(s.fii_net); fiiNetEl.style.color = s.fii_net >= 0 ? '#22c55e' : '#ef4444'; }
  const diiNetEl = document.getElementById('fd-dii-net');
  if (diiNetEl) { diiNetEl.textContent = _fmtCrBig(s.dii_net); diiNetEl.style.color = s.dii_net >= 0 ? '#22c55e' : '#ef4444'; }

  // Balance bar
  const fiiFill = document.getElementById('fd-bal-fii');
  const diiFill = document.getElementById('fd-bal-dii');
  if (fiiFill) fiiFill.style.width = s.fii_pct + '%';
  if (diiFill) { diiFill.style.width = s.dii_pct + '%'; diiFill.style.left = s.fii_pct + '%'; }
  const fiiPctEl = document.getElementById('fd-fii-pct');
  const diiPctEl = document.getElementById('fd-dii-pct');
  if (fiiPctEl) fiiPctEl.textContent = `FII ${st.fii.direction === 'Selling' ? 'SELLING' : 'BUYING'}: ${s.fii_pct}%`;
  if (diiPctEl) diiPctEl.textContent = `DII ${st.dii.direction === 'Buying' ? 'SUPPORT' : 'SELLING'}: ${s.dii_pct}%`;

  // Net liquidity
  const nlEl = document.getElementById('fd-net-liq');
  if (nlEl) { nlEl.textContent = _fmtCrBig(s.net_liquidity); nlEl.style.color = s.net_liquidity >= 0 ? '#22c55e' : '#ef4444'; }

  // FII Streak
  const fiiStrEl = document.getElementById('fd-fii-streak');
  if (fiiStrEl) {
    const col = st.fii.direction === 'Selling' ? '#ef4444' : '#22c55e';
    fiiStrEl.innerHTML = `<span style="color:${col}">★ ${st.fii.days} Days ${st.fii.direction}</span> <span style="color:${col}">${_fmtCrBig(st.fii.total)}</span>`;
  }
  const fiiStrCard = document.getElementById('fd-fii-streak-card');
  if (fiiStrCard) fiiStrCard.style.borderLeftColor = st.fii.direction === 'Selling' ? '#ef4444' : '#22c55e';

  // 5-Day FII Velocity
  const fii5dEl = document.getElementById('fd-fii-5d');
  if (fii5dEl) { fii5dEl.textContent = _fmtCrBig(c.fii_5d); fii5dEl.style.color = c.fii_5d >= 0 ? '#22c55e' : '#ef4444'; }
  const fii5dAvg = document.getElementById('fd-fii-5d-avg');
  if (fii5dAvg) { const avg = c.fii_5d / 5; fii5dAvg.textContent = `Avg daily selling: ${_fmtCrBig(avg)} Cr/day over the last 5 sessions.`; }
  const fiiVelCard = document.getElementById('fd-fii-velocity-card');
  if (fiiVelCard) fiiVelCard.style.borderLeftColor = c.fii_5d >= 0 ? '#22c55e' : '#f59e0b';

  // DII Streak
  const diiStrEl = document.getElementById('fd-dii-streak');
  if (diiStrEl) {
    const col = st.dii.direction === 'Buying' ? '#22c55e' : '#ef4444';
    diiStrEl.innerHTML = `<span style="color:${col}">★ ${st.dii.days} Days ${st.dii.direction}</span> <span style="color:${col}">${_fmtCrBig(st.dii.total)}</span>`;
  }
  const diiStrCard = document.getElementById('fd-dii-streak-card');
  if (diiStrCard) diiStrCard.style.borderLeftColor = st.dii.direction === 'Buying' ? '#22c55e' : '#ef4444';

  // Metric cards
  const fii20 = document.getElementById('fd-fii-cum');
  if (fii20) fii20.textContent = _fmtCrBig(c.fii_20d);
  const dii20 = document.getElementById('fd-dii-cum');
  if (dii20) dii20.textContent = _fmtCrBig(c.dii_20d);

  // Monthly net = approximate from 20d
  const monthNet = document.getElementById('fd-monthly-net');
  if (monthNet) {
    const mn = c.fii_20d + c.dii_20d;
    monthNet.textContent = _fmtCrBig(mn);
    monthNet.style.color = mn >= 0 ? '#22c55e' : '#ef4444';
  }

  // FII Intensity (today's selling vs 20d avg)
  const intEl = document.getElementById('fd-fii-intensity');
  const intSub = document.getElementById('fd-fii-intensity-sub');
  if (intEl && c.fii_20d !== 0) {
    const avgDaily = Math.abs(c.fii_20d / 20);
    const todayAbs = Math.abs(s.fii_net);
    const ratio = avgDaily > 0 ? (todayAbs / avgDaily * 100).toFixed(0) : 0;
    intEl.textContent = ratio + '%';
    intEl.style.color = ratio > 150 ? '#ef4444' : ratio > 100 ? '#f59e0b' : '#22c55e';
    intSub.textContent = ratio > 150 ? 'Heavy selling vs average' : ratio > 100 ? 'Above average activity' : 'Below average activity';
  }

  // 45-Day Matrices
  _renderDotMatrix('fd-fii-matrix', h.slice(-45), 'fii_net', true);
  _renderDotMatrix('fd-dii-matrix', h.slice(-45), 'dii_net', false);

  // Bar chart
  _renderFiiDiiChart(h);
}

function _renderDotMatrix(containerId, days, key, invertColor) {
  const el = document.getElementById(containerId);
  if (!el || !days.length) return;

  const maxAbs = Math.max(...days.map(d => Math.abs(d[key] || 0)), 1);
  el.innerHTML = days.map(d => {
    const val = d[key] || 0;
    const intensity = Math.min(Math.abs(val) / maxAbs, 1);
    let color;
    if (invertColor) {
      // FII: red = selling (negative), green = buying (positive)
      color = val < 0
        ? `rgba(239,68,68,${0.2 + intensity * 0.8})`
        : `rgba(34,197,94,${0.2 + intensity * 0.8})`;
    } else {
      // DII: green = buying (positive), red = selling (negative)
      color = val >= 0
        ? `rgba(34,197,94,${0.2 + intensity * 0.8})`
        : `rgba(239,68,68,${0.2 + intensity * 0.8})`;
    }
    const size = 8 + intensity * 6;
    return `<div class="fd-dot" style="background:${color};width:${size}px;height:${size}px" title="${d.date}: ${_fmtCrBig(val)}"></div>`;
  }).join('');
}

function _renderFiiDiiChart(history) {
  const canvas = document.getElementById('fd-chart-canvas');
  if (!canvas || !history.length) return;

  const ctx = canvas.getContext('2d');
  if (window._fdChart) window._fdChart.destroy();

  const labels = history.map(d => {
    const dt = new Date(d.date);
    return dt.toLocaleDateString('en-IN', {day:'2-digit', month:'short'});
  });

  window._fdChart = new Chart(ctx, {
    type: 'bar',
    data: {
      labels,
      datasets: [
        {
          label: 'FII Net',
          data: history.map(d => d.fii_net),
          backgroundColor: history.map(d => d.fii_net >= 0 ? 'rgba(34,197,94,0.7)' : 'rgba(239,68,68,0.7)'),
          borderWidth: 0,
          borderRadius: 2,
        },
        {
          label: 'DII Net',
          data: history.map(d => d.dii_net),
          backgroundColor: history.map(d => d.dii_net >= 0 ? 'rgba(96,165,250,0.7)' : 'rgba(251,146,60,0.7)'),
          borderWidth: 0,
          borderRadius: 2,
        }
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { labels: { color: '#94a3b8', font: { size: 10, family: 'var(--font-mono)' } } },
        tooltip: {
          callbacks: {
            label: ctx => `${ctx.dataset.label}: ₹${ctx.raw?.toLocaleString('en-IN')} Cr`
          }
        }
      },
      scales: {
        x: { ticks: { color: '#64748b', font: { size: 9 }, maxRotation: 45 }, grid: { color: 'rgba(255,255,255,0.04)' } },
        y: { ticks: { color: '#64748b', font: { size: 10 }, callback: v => `₹${v}Cr` }, grid: { color: 'rgba(255,255,255,0.06)' } }
      }
    }
  });
}

async function syncFiiDiiTab() {
  try {
    await fetch(`${API}/api/fiidii/sync`, { method: 'POST' });
    _fdLoaded = false;
    loadFiiDiiTab();
  } catch (e) { console.warn('FII/DII sync failed:', e); }
}
