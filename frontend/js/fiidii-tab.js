/* ═══════════════════════════════════════════════════════════════════════════
   FII/DII TAB — Institutional Money Matrix
   3 × 60-day dot matrices: FII, DII, Net (FII+DII)
   ═══════════════════════════════════════════════════════════════════════════ */

let _fdLoaded = false;
let _fdData = null;

async function onFiiDiiTabLoad() {
  if (!_fdLoaded) loadFiiDiiTab();
}

async function loadFiiDiiTab() {
  try {
    const res = await fetch(`${API}/api/fiidii/summary?days=60`);
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
  _setVal('fd-fii-net', s.fii_net);
  _setVal('fd-dii-net', s.dii_net);

  // Balance bar
  const fiiFill = document.getElementById('fd-bal-fii');
  const diiFill = document.getElementById('fd-bal-dii');
  if (fiiFill) fiiFill.style.width = s.fii_pct + '%';
  if (diiFill) { diiFill.style.width = s.dii_pct + '%'; diiFill.style.left = s.fii_pct + '%'; }
  _setText('fd-fii-pct', `FII ${st.fii.direction === 'Selling' ? 'SELLING' : 'BUYING'}: ${s.fii_pct}%`);
  _setText('fd-dii-pct', `DII ${st.dii.direction === 'Buying' ? 'SUPPORT' : 'SELLING'}: ${s.dii_pct}%`);

  // Net liquidity
  _setVal('fd-net-liq', s.net_liquidity);

  // FII Streak
  _setStreak('fd-fii-streak', st.fii);
  _setBorderColor('fd-fii-streak-card', st.fii.direction === 'Selling' ? '#ef4444' : '#22c55e');

  // 5-Day FII Velocity
  _setVal('fd-fii-5d', c.fii_5d);
  _setText('fd-fii-5d-avg', `Avg daily: ${_fmtCr(c.fii_5d_avg)} /day over last 5 sessions.`);
  _setBorderColor('fd-fii-velocity-card', c.fii_5d >= 0 ? '#22c55e' : '#f59e0b');

  // DII Streak
  _setStreak('fd-dii-streak', st.dii);
  _setBorderColor('fd-dii-streak-card', st.dii.direction === 'Buying' ? '#22c55e' : '#ef4444');

  // Metric cards
  _setVal('fd-fii-cum', c.fii_20d, '#ef4444');
  _setVal('fd-dii-cum', c.dii_20d, '#22c55e');
  const mn = c.fii_20d + c.dii_20d;
  const mnEl = document.getElementById('fd-monthly-net');
  if (mnEl) { mnEl.textContent = _fmtCr(mn); mnEl.style.color = mn >= 0 ? '#22c55e' : '#ef4444'; }

  // FII Intensity
  const intEl = document.getElementById('fd-fii-intensity');
  const intSub = document.getElementById('fd-fii-intensity-sub');
  if (intEl && c.fii_20d !== 0) {
    const avgDaily = Math.abs(c.fii_20d / 20);
    const todayAbs = Math.abs(s.fii_net);
    const ratio = avgDaily > 0 ? (todayAbs / avgDaily * 100).toFixed(0) : 0;
    intEl.textContent = ratio + '%';
    intEl.style.color = ratio > 150 ? '#ef4444' : ratio > 100 ? '#f59e0b' : '#22c55e';
    if (intSub) intSub.textContent = ratio > 150 ? 'Heavy activity vs average' : ratio > 100 ? 'Above average' : 'Below average';
  }

  // ── 3 × 60-Day Matrices ──
  _renderMatrix60('fd-fii-matrix', 'fd-fii-matrix-legend', h, 'fii_net', 'FII');
  _renderMatrix60('fd-dii-matrix', 'fd-dii-matrix-legend', h, 'dii_net', 'DII');
  _renderMatrix60('fd-net-matrix', 'fd-net-matrix-legend', h, 'net', 'NET');
}

function _renderMatrix60(containerId, legendId, days, key, label) {
  const el = document.getElementById(containerId);
  if (!el || !days.length) return;

  const vals = days.map(d => d[key] || 0);
  const maxAbs = Math.max(...vals.map(Math.abs), 1);

  el.innerHTML = days.map(d => {
    const val = d[key] || 0;
    const intensity = Math.min(Math.abs(val) / maxAbs, 1);
    const color = val >= 0
      ? `rgba(34,197,94,${0.15 + intensity * 0.85})`
      : `rgba(239,68,68,${0.15 + intensity * 0.85})`;
    const size = 10 + intensity * 8;
    const dt = new Date(d.date);
    const tip = `${dt.toLocaleDateString('en-IN',{day:'2-digit',month:'short'})}: ${_fmtCr(val)}`;
    return `<div class="fd-dot" style="background:${color};width:${size}px;height:${size}px" title="${tip}"></div>`;
  }).join('');

  // Legend: count buying vs selling days
  const buyDays = vals.filter(v => v > 0).length;
  const sellDays = vals.filter(v => v < 0).length;
  const total = vals.reduce((a, b) => a + b, 0);
  const legEl = document.getElementById(legendId);
  if (legEl) {
    legEl.innerHTML = `
      <span style="color:#22c55e">● ${buyDays} buy days</span>
      <span style="color:#ef4444">● ${sellDays} sell days</span>
      <span style="color:var(--text3)">Net: ${_fmtCr(total)}</span>
    `;
  }
}

// ── Helpers ──
function _setVal(id, val, forceColor) {
  const el = document.getElementById(id);
  if (!el) return;
  el.textContent = _fmtCr(val);
  el.style.color = forceColor || (val >= 0 ? '#22c55e' : '#ef4444');
}
function _setText(id, text) {
  const el = document.getElementById(id);
  if (el) el.textContent = text;
}
function _setBorderColor(id, color) {
  const el = document.getElementById(id);
  if (el) el.style.borderLeftColor = color;
}
function _setStreak(id, streak) {
  const el = document.getElementById(id);
  if (!el) return;
  const col = streak.direction === 'Selling' ? '#ef4444' : '#22c55e';
  el.innerHTML = `<span style="color:${col}">★ ${streak.days} Days ${streak.direction}</span> <span style="color:${col}">${_fmtCr(streak.total)}</span>`;
}

async function syncFiiDiiTab() {
  const btn = document.querySelector('.fd-sync-btn');
  if (btn) { btn.disabled = true; btn.textContent = '⏳ Syncing...'; }
  try {
    const res = await fetch(`${API}/api/fiidii/sync`, { method: 'POST' });
    const data = await res.json();
    if (data.entries > 0) {
      _fdLoaded = false;
      loadFiiDiiTab();
    } else {
      alert('Sync returned 0 entries: ' + (data.message || data.error || 'Check server logs'));
    }
  } catch (e) {
    alert('Sync failed: ' + e.message);
  }
  if (btn) { btn.disabled = false; btn.textContent = '⟳ Force Sync'; }
}
