/* ═══════════════════════════════════════════════════════════════════════════
   F-VALUE — Fundamental Value Screener JS Module
   ═══════════════════════════════════════════════════════════════════════════ */

let _fvData = [];
let _fvAllData = [];
let _fvGradeFilter = null;
let _fvFVFilter = null;
let _fvLoaded = false;

async function loadFValueData() {
  const tbody = document.getElementById('fv-tbody');
  if (tbody) tbody.innerHTML = _skeletonRows(10, 13);

  try {
    const res = await fetch(`${API}/api/fvalue?limit=1000&market=${currentMarket}`);
    if (!res.ok) throw new Error('HTTP ' + res.status);
    const data = await res.json();
    _fvAllData = data.stocks || [];
    _fvData = _fvAllData;
    _fvLoaded = true;

    _renderFVDistribution(data.grade_distribution, data.fv_distribution);

    const infoEl = document.getElementById('fv-info');
    if (infoEl) infoEl.textContent = `${data.total} stocks · TV Fundamentals`;

    filterFValueTable();
  } catch (e) {
    console.error('F-Value load error:', e);
    if (tbody) tbody.innerHTML = `<tr><td colspan="13" class="fv-empty" style="color:var(--red)">⚠ ${e.message}<br><span style="color:var(--text3);font-size:10px">Ensure TV Fundamentals are synced (Importer tab → TV Sync)</span></td></tr>`;
  }
}

function _renderFVDistribution(grades, fvDist) {
  // Grade bars
  const gEl = document.getElementById('fv-grade-dist');
  if (gEl && grades) {
    const gradeColors = { A: '#22c55e', 'B+': '#4ade80', B: '#60a5fa', C: '#f59e0b', D: '#ef4444', E: '#7f1d1d' };
    gEl.innerHTML = Object.entries(grades)
      .filter(([, v]) => v > 0)
      .map(([g, v]) => `<div class="fv-dist-chip" style="border-color:${gradeColors[g]}40;color:${gradeColors[g]}" onclick="fvFilterGrade('${g}')">${g} <span>${v}</span></div>`)
      .join('');
  }
  // FV bars
  const fEl = document.getElementById('fv-fv-dist');
  if (fEl && fvDist) {
    const fvColors = { 'DEEP VALUE': '#22c55e', 'UNDERVALUED': '#4ade80', 'FAIR': '#f59e0b', 'FULLY PRICED': '#fb923c', 'OVERVALUED': '#ef4444' };
    fEl.innerHTML = Object.entries(fvDist)
      .filter(([k, v]) => v > 0 && k !== 'N/A')
      .map(([k, v]) => `<div class="fv-dist-chip" style="border-color:${fvColors[k] || '#64748b'}40;color:${fvColors[k] || '#64748b'}">${k} <span>${v}</span></div>`)
      .join('');
  }
}

function fvFilterGrade(grade) {
  _fvGradeFilter = grade;
  // Update buttons
  document.querySelectorAll('[id^="fv-g-"]').forEach(b => b.classList.remove('active'));
  if (!grade) document.getElementById('fv-g-all')?.classList.add('active');
  else {
    const id = 'fv-g-' + (grade === 'B+' ? 'Bp' : grade);
    document.getElementById(id)?.classList.add('active');
  }
  filterFValueTable();
}

function fvFilterFV(fv) {
  _fvFVFilter = fv;
  document.querySelectorAll('[id^="fv-v-"]').forEach(b => b.classList.remove('active'));
  if (!fv) document.getElementById('fv-v-all')?.classList.add('active');
  else {
    const map = { deep_value: 'dv', undervalued: 'uv', fair: 'fair', overvalued: 'ov' };
    document.getElementById('fv-v-' + (map[fv] || 'all'))?.classList.add('active');
  }
  filterFValueTable();
}

function filterFValueTable() {
  const search = (document.getElementById('fv-search')?.value || '').toLowerCase();
  const gradeOrder = { A: 5, 'B+': 4, B: 3, C: 2, D: 1, E: 0 };
  const fvMap = { deep_value: 'DEEP VALUE', undervalued: 'UNDERVALUED', fair: 'FAIR', fully_priced: 'FULLY PRICED', overvalued: 'OVERVALUED' };

  let filtered = _fvAllData.filter(s => {
    if (_fvGradeFilter && (gradeOrder[s.grade] || 0) < (gradeOrder[_fvGradeFilter] || 0)) return false;
    if (_fvFVFilter && s.fv_status !== fvMap[_fvFVFilter]) return false;
    if (search) {
      const hay = `${s.ticker} ${s.company} ${s.sector} ${s.industry}`.toLowerCase();
      if (!hay.includes(search)) return false;
    }
    return true;
  });

  _renderFVTable(filtered);
}

function _renderFVTable(stocks) {
  const tbody = document.getElementById('fv-tbody');
  if (!tbody) return;

  if (!stocks.length) {
    tbody.innerHTML = '<tr><td colspan="13" class="fv-empty">No stocks match filters. Ensure TV Fundamentals are synced.</td></tr>';
    document.getElementById('fv-count').textContent = '0 stocks';
    return;
  }

  tbody.innerHTML = stocks.map((s, i) => {
    const gc = s.grade_color || '#64748b';
    const fvc = s.fv_status_color || '#64748b';
    const upc = (s.upside_pct || 0) >= 0 ? 'var(--green)' : 'var(--red)';

    return `<tr>
      <td class="fv-rank">${i + 1}</td>
      <td class="fv-ticker">${s.ticker}</td>
      <td class="fv-company">${_fvTrunc(s.company, 28)}</td>
      <td class="fv-sector">${_fvTrunc(s.sector, 18)}</td>
      <td class="fv-td-center"><span class="fv-grade-badge" style="background:${gc}18;color:${gc};border:1px solid ${gc}44">${s.grade}</span></td>
      <td><span class="fv-fvs-badge" style="background:${fvc}18;color:${fvc};border:1px solid ${fvc}44">${s.fv_status}</span></td>
      <td class="fv-right">${mktCurrency()}${s.price?.toLocaleString(mktLocale()) || '—'}</td>
      <td class="fv-right" style="font-weight:600">${mktCurrency()}${s.fair_value?.toLocaleString(mktLocale()) || '—'}</td>
      <td class="fv-right" style="color:${upc};font-weight:700">${s.upside_pct != null ? (s.upside_pct >= 0 ? '+' : '') + s.upside_pct + '%' : '—'}</td>
      <td class="fv-right">${s.pe || '—'}</td>
      <td class="fv-right" style="color:${s.roe >= 15 ? 'var(--green)' : s.roe >= 8 ? 'var(--text)' : 'var(--red)'}">${s.roe || '—'}</td>
      <td class="fv-right" style="color:${s.eps_growth >= 15 ? 'var(--green)' : s.eps_growth >= 0 ? 'var(--text)' : 'var(--red)'}">${s.eps_growth || '—'}</td>
      <td class="fv-right" style="color:${s.debt_equity <= 0.5 ? 'var(--green)' : s.debt_equity <= 1.0 ? 'var(--text)' : 'var(--red)'}">${s.debt_equity || '—'}</td>
    </tr>`;
  }).join('');

  document.getElementById('fv-count').textContent = `${stocks.length} stocks`;
}

function _fvTrunc(s, n) {
  if (!s) return '—';
  return s.length > n ? s.slice(0, n) + '…' : s;
}

function onFValueTabLoad() {
  if (!_fvLoaded) loadFValueData();
}
