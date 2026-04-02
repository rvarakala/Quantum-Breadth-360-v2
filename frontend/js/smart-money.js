// ════════════════════════════════════════════════════════════════════════════
// SMART MONEY TRACKER — IV / PPV / Bull Snort Signal Intelligence
// ════════════════════════════════════════════════════════════════════════════

let _smData = null;
let _smNotes = {};
let _smClusterMode = false;

async function loadSmartMoney() {
  const days = document.getElementById('sm-days')?.value || 10;
  const wrap = document.getElementById('sm-table-wrap');
  wrap.innerHTML = '<div style="text-align:center;padding:40px"><div class="ai-spinner"></div><br><span style="color:var(--text3);font-size:11px;font-family:var(--font-mono)">Scanning ' + days + ' days for Smart Money signals...<br><span style="font-size:10px">First load analyzes 2500+ stocks — may take 30-60 seconds</span></span></div>';

  try {
    console.log('Smart Money: fetching signals for', days, 'days...');
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 90000); // 90s timeout
    const [smRes, notesRes] = await Promise.all([
      fetch(`${API}/api/smart-money?days=${days}`, {signal: controller.signal}),
      fetch(`${API}/api/smart-money/notes`)
    ]);
    clearTimeout(timeout);
    _smData = await smRes.json();
    _smNotes = await notesRes.json();
    console.log('Smart Money: got', _smData.tickers?.length || 0, 'tickers,', _smData.total_signals || 0, 'signals', _smData.error || '');

    if (_smData.error) throw new Error(_smData.error);

    _renderSmartMoneyStats();
    _renderSmartMoneyTable();
  } catch (e) {
    wrap.innerHTML = `<div style="text-align:center;padding:40px;color:var(--red);font-family:var(--font-mono)">⚠ ${e.message}<br><button onclick="loadSmartMoney()" style="margin-top:8px;padding:6px 16px;border-radius:6px;border:1px solid var(--border);background:var(--bg3);color:var(--cyan);cursor:pointer;font-family:var(--font-mono);font-size:11px">🔄 Retry</button></div>`;
  }
}

function _renderSmartMoneyStats() {
  const d = _smData;
  if (!d) return;
  const el = document.getElementById('sm-stats');
  const ivTotal = d.tickers.reduce((a, t) => a + t.iv_count, 0);
  const ppvTotal = d.tickers.reduce((a, t) => a + t.ppv_count, 0);
  const bsTotal = d.tickers.reduce((a, t) => a + t.bs_count, 0);
  const insiderTotal = d.tickers.reduce((a, t) => a + (t.insider_buys || 0), 0);

  el.innerHTML = `
    <div class="sm-stat-card"><div class="sm-stat-num" style="color:var(--cyan)">${d.unique_tickers}</div><div class="sm-stat-label">TICKERS</div></div>
    <div class="sm-stat-card"><div class="sm-stat-num" style="color:var(--green)">${ivTotal}</div><div class="sm-stat-label">IV SIGNALS</div></div>
    <div class="sm-stat-card"><div class="sm-stat-num" style="color:var(--amber)">${ppvTotal}</div><div class="sm-stat-label">POCKET PIVOTS</div></div>
    <div class="sm-stat-card"><div class="sm-stat-num" style="color:var(--purple)">${bsTotal}</div><div class="sm-stat-label">BULL SNORTS</div></div>
    <div class="sm-stat-card"><div class="sm-stat-num" style="color:var(--pink,#f472b6)">${insiderTotal}</div><div class="sm-stat-label">INSIDER BUYS</div></div>
    <div class="sm-stat-card"><div class="sm-stat-num" style="color:var(--text)">${d.dates_covered?.length || 0}</div><div class="sm-stat-label">DAYS</div></div>
  `;
}

function _renderSmartMoneyTable() {
  let tickers = _getFilteredTickers();
  if (!tickers.length) {
    document.getElementById('sm-table-wrap').innerHTML = '<div style="text-align:center;padding:40px;color:var(--text3);font-family:var(--font-mono)">🔍 No signals match the current filters</div>';
    return;
  }

  const gc = v => (v || 0) >= 0 ? 'var(--green)' : 'var(--red)';
  const f = (v, d = 1) => v != null ? Number(v).toFixed(d) : '—';

  const rows = tickers.map((t, i) => {
    const sigBadges = [];
    if (t.iv_count) sigBadges.push(`<span class="sm-sig iv">IV ×${t.iv_count}</span>`);
    if (t.ppv_count) sigBadges.push(`<span class="sm-sig ppv">PPV ×${t.ppv_count}</span>`);
    if (t.bs_count) sigBadges.push(`<span class="sm-sig bs">BS ×${t.bs_count}</span>`);

    const stageColor = t.stage === 'Stage 2' ? 'var(--green)' : t.stage === 'Stage 1→2' ? 'var(--amber)' : 'var(--text3)';
    const rsColor = (t.rs_rating || 0) >= 80 ? 'var(--green)' : (t.rs_rating || 0) >= 60 ? 'var(--amber)' : 'var(--red)';

    const insiderBadge = t.insider_buys > 0
      ? `<span class="sm-sig insider" title="${t.insider_details?.map(d => d.name + ' ' + d.date).join(', ') || ''}">🏦 ${t.insider_buys}</span>`
      : '';

    const fvgStr = t.fvg ? `${t.fvg.lower}–${t.fvg.upper} (${t.fvg.type})` : '—';
    const ivRangeStr = t.iv_low && t.iv_high ? `${t.iv_low}–${t.iv_high}` : '—';

    const note = _smNotes[t.ticker]?.note || '';
    const noteId = `sm-note-${t.ticker}`;

    return `<tr class="sm-row">
      <td class="sm-td sm-rank">${i + 1}</td>
      <td class="sm-td sm-ticker"><span class="ticker-link" onclick="openTickerChart('${t.ticker}')">${t.ticker}</span></td>
      <td class="sm-td">${sigBadges.join(' ')} ${insiderBadge}</td>
      <td class="sm-td" style="font-family:var(--font-mono)">₹${t.price?.toLocaleString('en-IN') || '—'}</td>
      <td class="sm-td" style="color:${gc(t.change_pct)};font-family:var(--font-mono)">${t.change_pct >= 0 ? '+' : ''}${f(t.change_pct)}%</td>
      <td class="sm-td" style="color:${stageColor}">${t.stage || '—'}</td>
      <td class="sm-td"><span style="color:${rsColor};font-weight:700">${t.rs_rating ?? '—'}</span></td>
      <td class="sm-td" style="color:${(t.vol_ratio || 0) >= 2 ? 'var(--cyan)' : 'var(--text2)'};font-family:var(--font-mono)">${f(t.vol_ratio, 2)}x</td>
      <td class="sm-td"><span class="sm-sector">${t.sector || '—'}</span></td>
      <td class="sm-td" style="font-family:var(--font-mono)">${t.sector_rs != null ? f(t.sector_rs, 0) : '—'}</td>
      <td class="sm-td" style="font-family:var(--font-mono);font-size:10px">${ivRangeStr}</td>
      <td class="sm-td" style="font-family:var(--font-mono);font-size:10px">${fvgStr}</td>
      <td class="sm-td"><input class="sm-note-input" id="${noteId}" value="${_escHtml(note)}" placeholder="Add note..." onblur="saveSmNote('${t.ticker}',this.value)"></td>
    </tr>`;
  }).join('');

  document.getElementById('sm-table-wrap').innerHTML = `
    <div style="overflow-x:auto">
    <table class="sm-table">
      <thead><tr>
        <th class="sm-th">#</th>
        <th class="sm-th">TICKER</th>
        <th class="sm-th">SIGNALS</th>
        <th class="sm-th">PRICE</th>
        <th class="sm-th">CHG%</th>
        <th class="sm-th">STAGE</th>
        <th class="sm-th">RS</th>
        <th class="sm-th">VOL</th>
        <th class="sm-th">SECTOR</th>
        <th class="sm-th">SEC RS</th>
        <th class="sm-th">IV RANGE</th>
        <th class="sm-th">FVG</th>
        <th class="sm-th" style="min-width:140px">NOTES</th>
      </tr></thead>
      <tbody>${rows}</tbody>
    </table>
    </div>
    <div style="padding:8px 12px;font-size:10px;color:var(--text3);font-family:var(--font-mono)">
      ${tickers.length} tickers · ${_smData.total_signals} total signals · ${_smData.dates_covered?.length || 0} trading days
    </div>`;
}

function _getFilteredTickers() {
  if (!_smData?.tickers) return [];
  let tickers = [..._smData.tickers];

  const sigFilter = document.getElementById('sm-filter-signal')?.value || 'all';
  const stageFilter = document.getElementById('sm-filter-stage')?.value || 'all';

  if (sigFilter !== 'all') {
    tickers = tickers.filter(t => {
      if (sigFilter === 'IV') return t.iv_count > 0;
      if (sigFilter === 'PPV') return t.ppv_count > 0;
      if (sigFilter === 'BS') return t.bs_count > 0;
      return true;
    });
  }

  if (stageFilter === 'stage2') {
    tickers = tickers.filter(t => t.stage === 'Stage 2');
  }

  if (_smClusterMode) {
    tickers.sort((a, b) => b.total_signals - a.total_signals);
  }

  return tickers;
}

function filterSmartMoney() {
  _renderSmartMoneyTable();
}

function toggleClusterMode() {
  _smClusterMode = !_smClusterMode;
  const btn = document.getElementById('sm-cluster-btn');
  if (btn) {
    btn.classList.toggle('active', _smClusterMode);
    btn.textContent = _smClusterMode ? '📊 Cluster ON' : '📊 Cluster Mode';
  }
  _renderSmartMoneyTable();
}

async function saveSmNote(ticker, note) {
  try {
    await fetch(`${API}/api/smart-money/notes`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ticker, note })
    });
    _smNotes[ticker] = { note, updated_at: new Date().toISOString() };
  } catch (e) {
    console.warn('Failed to save note:', e);
  }
}

function _escHtml(s) {
  return (s || '').replace(/&/g, '&amp;').replace(/"/g, '&quot;').replace(/</g, '&lt;').replace(/>/g, '&gt;');
}

// Auto-load when tab is opened
document.addEventListener('DOMContentLoaded', () => {
  // Will be triggered by switchTab in app.js
});
