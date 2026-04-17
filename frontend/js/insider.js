/* ═══════════════════════════════════════════════════════════════════════════
   INSIDER TRADING TAB — JS Module
   Loads insider trades from API, renders table, handles filters/import/export.
   ═══════════════════════════════════════════════════════════════════════════ */

let _insiderData = [];       // current visible page
let _insiderAll  = [];       // full unpaginated result (for client-side filtering)
let _insiderFilter = 'all';  // Buy | Sell | cluster | all
let _insiderClusters = [];
let _insSortCol = 'score';
let _insSortDir = -1;        // -1 = desc, 1 = asc
let _insOffset  = 0;
let _insLimit   = 200;       // per page
let _insTotalAvailable = 0;

async function loadInsiderData() {
  const days = document.getElementById('ins-period')?.value || 90;
  const tbody = document.getElementById('ins-tbody');
  if (tbody) tbody.innerHTML = _skeletonRows(8, 11);

  // Reset pagination on new load
  _insOffset = 0;

  // Show inline loading state
  _setInsLoading(true);

  try {
    // Load trades — fetch up to 2000 for client-side filter + sort
    const res = await fetch(`${API}/api/insider/trades?days=${days}&limit=2000`);
    const data = await res.json();
    _insiderAll = data.trades || [];
    _insTotalAvailable = data.total || _insiderAll.length;

    // Load summary
    const sRes = await fetch(`${API}/api/insider/summary?days=${days}`);
    const summary = await sRes.json();
    _renderInsiderSummary(summary);
    _insiderClusters = summary.clusters || [];

    filterInsiderTable();
  } catch (e) {
    console.error('Insider load error:', e);
    if (tbody) tbody.innerHTML =
      '<tr><td colspan="11" class="ins-empty">Failed to load insider data. Try importing a CSV.</td></tr>';
  } finally {
    _setInsLoading(false);
  }
}

function _setInsLoading(on) {
  const chips = document.getElementById('ins-stats-bar');
  if (chips) chips.style.opacity = on ? '0.4' : '1';
  const live = document.getElementById('ins-live-dot');
  if (live) live.textContent = on ? '● SYNCING' : '● LIVE';
}

function _renderInsiderSummary(s) {
  const set = (id, val) => { const e = document.getElementById(id); if (e) e.textContent = val; };
  set('ins-total',    (s.total_transactions || 0).toLocaleString());
  set('ins-buys',     (s.buys_count  || 0).toLocaleString());
  set('ins-sells',    (s.sells_count || 0).toLocaleString());
  set('ins-top-buy',  s.top_buy_symbol  || '—');
  set('ins-top-sell', s.top_sell_symbol || '—');
  set('ins-last-date', s.last_data_date || '—');

  // Make TOP BUY / TOP SELL stat cards clickable — filter table by that symbol
  const bindClick = (id, sym) => {
    const card = document.getElementById(id)?.closest('.ins-stat-card');
    if (!card || !sym || sym === '—') return;
    card.style.cursor = 'pointer';
    card.title = `Click to filter by ${sym}`;
    card.onclick = () => {
      const sEl = document.getElementById('ins-search');
      if (sEl) { sEl.value = sym; filterInsiderTable(); }
    };
  };
  bindClick('ins-top-buy',  s.top_buy_symbol);
  bindClick('ins-top-sell', s.top_sell_symbol);

  // Clusters
  const cEl = document.getElementById('ins-clusters');
  if (cEl && s.clusters && s.clusters.length > 0) {
    cEl.style.display = 'block';
    cEl.innerHTML = `
      <div class="ins-cluster-title">🔥 CLUSTERED BUYING (multiple insiders)</div>
      <div class="ins-cluster-grid">
        ${s.clusters.map(c => `
          <div class="ins-cluster-chip" onclick="document.getElementById('ins-search').value='${c.symbol}';filterInsiderTable()">
            <span class="ins-cluster-sym">${c.symbol}</span>
            <span class="ins-cluster-count">${c.insiders} insiders</span>
            <span class="ins-cluster-val">${_fmtVal(c.total_value)}</span>
          </div>
        `).join('')}
      </div>`;
  } else if (cEl) {
    cEl.style.display = 'none';
  }
}

function insiderFilter(type) {
  _insiderFilter = type;
  _insOffset = 0;
  document.querySelectorAll('.ins-filter-btn').forEach(b => b.classList.remove('active'));
  const btn = document.getElementById(
    `ins-f-${type === 'all' ? 'all' : type === 'Buy' ? 'buy' : type === 'Sell' ? 'sell' : 'cluster'}`
  );
  if (btn) btn.classList.add('active');
  filterInsiderTable();
}

function filterInsiderTable() {
  const search     = (document.getElementById('ins-search')?.value || '').toLowerCase();
  const category   = document.getElementById('ins-category')?.value || '';
  const modeFilter = document.getElementById('ins-mode')?.value || '';
  const minVal     = _parseMinValue(document.getElementById('ins-minval')?.value);

  let filtered = _insiderAll.filter(t => {
    // Transaction type filter (buttons)
    if (_insiderFilter === 'Buy'  && t.transaction_type !== 'Buy')  return false;
    if (_insiderFilter === 'Sell' && t.transaction_type !== 'Sell') return false;
    if (_insiderFilter === 'cluster') {
      const clusterSyms = _insiderClusters.map(c => c.symbol);
      if (!clusterSyms.includes(t.symbol)) return false;
    }

    // Category filter
    if (category && !(t.category || '').includes(category)) return false;

    // Mode filter — now clearly scoped to the `mode` field only, not conflicting with type
    if (modeFilter) {
      const mode = (t.mode || '').toLowerCase();
      if (modeFilter === 'market_buy'    && !(mode.includes('market') && mode.includes('purchase'))) return false;
      if (modeFilter === 'market_sell'   && !(mode.includes('market') && mode.includes('sale')))     return false;
      if (modeFilter === 'pledge'        && !(mode.includes('pledge') && !mode.includes('revok'))) return false;
      if (modeFilter === 'revoke'        && !mode.includes('revok')) return false;
      if (modeFilter === 'off_market'    && !(mode.includes('off') && mode.includes('market'))) return false;
      if (modeFilter === 'esop'          && !(mode.includes('esop') || mode.includes('esosp'))) return false;
      if (modeFilter === 'preferential'  && !mode.includes('preferential')) return false;
    }

    // Minimum value filter
    if (minVal > 0 && (t.securities_value || 0) < minVal) return false;

    // Free-text search
    if (search) {
      const haystack = `${t.symbol || ''} ${t.company || ''} ${t.insider_name || ''}`.toLowerCase();
      if (!haystack.includes(search)) return false;
    }
    return true;
  });

  // Sort — tolerant of null/undefined
  filtered.sort((a, b) => {
    let va = a[_insSortCol], vb = b[_insSortCol];
    if (va == null) va = typeof vb === 'number' ? 0 : '';
    if (vb == null) vb = typeof va === 'number' ? 0 : '';
    if (typeof va === 'string') {
      va = va.toLowerCase(); vb = (vb + '').toLowerCase();
      if (va === vb) return 0;
      return (va < vb ? 1 : -1) * -_insSortDir;
    }
    return (((va ?? 0) - (vb ?? 0))) * -_insSortDir;
  });

  _updateSortIndicators();

  // Pagination
  const total = filtered.length;
  const page = filtered.slice(_insOffset, _insOffset + _insLimit);
  _insiderData = page;
  _renderInsiderTable(page, total);
  _renderInsiderPager(total);
}

function _updateSortIndicators() {
  document.querySelectorAll('[id^="ins-sort-"]').forEach(el => el.textContent = '');
  const sortEl = document.getElementById(`ins-sort-${_insSortCol}`);
  if (sortEl) sortEl.textContent = _insSortDir === -1 ? ' ▼' : ' ▲';
}

function insiderSort(col) {
  if (_insSortCol === col) _insSortDir *= -1;
  else { _insSortCol = col; _insSortDir = -1; }
  _insOffset = 0;
  filterInsiderTable();
}

function _renderInsiderTable(trades, totalFiltered) {
  const tbody = document.getElementById('ins-tbody');
  if (!tbody) return;

  if (trades.length === 0) {
    tbody.innerHTML = '<tr><td colspan="11" class="ins-empty">No insider trades match the current filters.</td></tr>';
    _updateCount(0, totalFiltered);
    return;
  }

  tbody.innerHTML = trades.map(t => {
    const isBuy  = t.transaction_type === 'Buy';
    const isSell = t.transaction_type === 'Sell';
    const typeClass = isBuy ? 'ins-buy' : isSell ? 'ins-sell' : '';
    // Score colour — baseline 50 is neutral, not a warning
    // 80+ = strong green, 65-79 = mid green, 51-64 = cool neutral,
    // 50 = grey baseline, <50 = amber (conviction below baseline)
    const score = t.score ?? 50;
    const scoreColor = score >= 80 ? '#22c55e'
                     : score >= 65 ? '#86efac'
                     : score >= 51 ? '#64748b'
                     : score === 50 ? '#94a3b8'
                     : '#f59e0b';

    const cluster = _insiderClusters.find(c => c.symbol === t.symbol);
    const clusterBadge = cluster ? `<span class="ins-cluster-badge">${cluster.insiders}</span>` : '';

    return `<tr class="${typeClass}">
      <td><span class="ins-score" style="background:${scoreColor}18;color:${scoreColor};border:1px solid ${scoreColor}44">${score}</span></td>
      <td><span class="ins-ticker">${t.symbol || '—'}</span> ${clusterBadge}</td>
      <td class="ins-company">${_truncate(t.company, 25)}</td>
      <td class="ins-insider">${_truncate(t.insider_name, 22)}</td>
      <td><span class="ins-cat-badge">${_shortCat(t.category)}</span></td>
      <td class="ins-date">${t.transaction_date || '—'}</td>
      <td><span class="ins-type-badge ${isBuy ? 'buy' : isSell ? 'sell' : ''}">${t.transaction_type || '—'}</span></td>
      <td class="ins-right">${(t.securities_count || 0).toLocaleString()}</td>
      <td class="ins-right ins-val">${t.value_fmt || '—'}</td>
      <td class="ins-right">${(typeof mktCurrency === 'function' ? mktCurrency() : '₹')}${(t.price_approx || 0).toLocaleString()}</td>
      <td class="ins-mode" title="${(t.mode || '').replace(/"/g,'&quot;')}">${_shortMode(t.mode)}</td>
    </tr>`;
  }).join('');

  _updateCount(trades.length, totalFiltered);
}

function _updateCount(shown, total) {
  const el = document.getElementById('ins-count');
  if (!el) return;
  if (total && total !== shown) {
    el.textContent = `Showing ${shown.toLocaleString()} of ${total.toLocaleString()} trades`;
  } else {
    el.textContent = `${shown.toLocaleString()} trades`;
  }
}

function _renderInsiderPager(total) {
  const el = document.getElementById('ins-pager');
  if (!el) return;
  if (total <= _insLimit) { el.innerHTML = ''; return; }

  const page = Math.floor(_insOffset / _insLimit) + 1;
  const pages = Math.ceil(total / _insLimit);
  const canPrev = _insOffset > 0;
  const canNext = _insOffset + _insLimit < total;

  el.innerHTML = `
    <button class="ins-page-btn" ${canPrev ? '' : 'disabled'} onclick="insiderPage(-1)">← Prev</button>
    <span class="ins-page-info">Page ${page} of ${pages}</span>
    <button class="ins-page-btn" ${canNext ? '' : 'disabled'} onclick="insiderPage(1)">Next →</button>
  `;
}

function insiderPage(delta) {
  _insOffset = Math.max(0, _insOffset + delta * _insLimit);
  filterInsiderTable();
  const wrap = document.querySelector('.ins-table-wrap');
  if (wrap) wrap.scrollIntoView({behavior: 'smooth', block: 'start'});
}

function _parseMinValue(raw) {
  if (!raw) return 0;
  const s = String(raw).trim().replace(/[,₹\s]/g, '').toLowerCase();
  if (!s) return 0;
  const m = s.match(/^([\d.]+)\s*(cr|l|k)?$/);
  if (!m) return parseFloat(s) || 0;
  const n = parseFloat(m[1]) || 0;
  const suffix = m[2] || '';
  if (suffix === 'cr') return n * 10000000;
  if (suffix === 'l')  return n * 100000;
  if (suffix === 'k')  return n * 1000;
  return n;
}

// ── CSV Import ────────────────────────────────────────────────────────────────

async function importInsiderCSV(input) {
  const file = input.files?.[0];
  if (!file) return;

  const formData = new FormData();
  formData.append('file', file);

  try {
    const res = await fetch(`${API}/api/insider/import-csv`, { method: 'POST', body: formData });
    const data = await res.json();
    if (data.status === 'ok') {
      const msg = [
        `✅ Imported from ${data.filename}`,
        `New rows added: ${(data.imported || 0).toLocaleString()}`,
        `Duplicates skipped: ${(data.skipped_duplicates || 0).toLocaleString()}`,
        `Total in DB now: ${(data.total_rows_now || 0).toLocaleString()}`,
      ].join('\n');
      alert(msg);
      loadInsiderData();
    } else {
      alert(`Import error: ${data.message || 'Unknown error'}`);
    }
  } catch (e) {
    alert(`Import failed: ${e.message}`);
  }
  input.value = '';
}

async function refreshInsiderData() {
  const btn = document.querySelector('.ins-refresh-btn');
  if (btn) { btn.disabled = true; btn.textContent = '↻ Syncing...'; }
  _setInsLoading(true);

  try {
    const res = await fetch(`${API}/api/insider/sync?days=30`, { method: 'POST' });
    const data = await res.json();
    if (data.status === 'ok') {
      await loadInsiderData();
    } else {
      alert(data.message || 'Sync returned no data. Try CSV import instead.');
    }
  } catch (e) {
    alert('Sync failed — NSE may be blocking. Use CSV import instead.');
  }

  if (btn) { btn.disabled = false; btn.textContent = '↻ Refresh'; }
  _setInsLoading(false);
}

async function repairInsiderData() {
  const msg =
    'This will delete ONLY records that are:\n' +
    '  • transaction_type = Unknown (parser failed)\n' +
    '  • missing transaction_date\n' +
    '  • both securities_count AND securities_value = 0\n\n' +
    'Your valid data (including CSV imports) will be preserved.\n' +
    'After cleanup, the last 90 days will be re-synced via the\n' +
    'Cloudflare-safe adapter.\n\n' +
    'Continue?';
  if (!confirm(msg)) return;

  const btn = document.querySelector('.ins-repair-btn');
  if (btn) { btn.disabled = true; btn.textContent = '🔧 Repairing...'; }

  try {
    const res = await fetch(`${API}/api/insider/repair`, { method: 'POST' });
    const data = await res.json();
    if (data.status === 'ok') {
      const r = data.resync || {};
      const lines = [
        `🔧 Repair complete`,
        ``,
        `Before: ${(data.before || 0).toLocaleString()} records`,
        `Deleted: ${(data.deleted_total || 0).toLocaleString()} bad records`,
        `  • Unknown type: ${(data.deleted_unknown || 0).toLocaleString()}`,
        `  • Missing date: ${(data.deleted_empty_dates || 0).toLocaleString()}`,
        `  • Both values zero: ${(data.deleted_zero_values || 0).toLocaleString()}`,
        `Kept: ${(data.kept || 0).toLocaleString()} valid records`,
        ``,
        `Re-sync: ${r.status === 'ok'
          ? `fetched ${r.fetched || 0}, stored ${r.stored || 0}`
          : (r.message || 'no data from NSE — use CSV import')}`,
      ];
      alert(lines.join('\n'));
      loadInsiderData();
    } else {
      alert('Repair failed: ' + (data.message || 'unknown error'));
    }
  } catch (e) {
    alert('Repair request failed: ' + e);
  }

  if (btn) { btn.disabled = false; btn.textContent = '🔧 Repair DB'; }
}

// ── Export CSV ────────────────────────────────────────────────────────────────

function exportInsiderCSV() {
  // Re-run current filter to get ALL rows, not just the visible page
  const search     = (document.getElementById('ins-search')?.value || '').toLowerCase();
  const category   = document.getElementById('ins-category')?.value || '';
  const modeFilter = document.getElementById('ins-mode')?.value || '';
  const minVal     = _parseMinValue(document.getElementById('ins-minval')?.value);

  const filtered = _insiderAll.filter(t => {
    if (_insiderFilter === 'Buy'  && t.transaction_type !== 'Buy')  return false;
    if (_insiderFilter === 'Sell' && t.transaction_type !== 'Sell') return false;
    if (_insiderFilter === 'cluster') {
      const clusterSyms = _insiderClusters.map(c => c.symbol);
      if (!clusterSyms.includes(t.symbol)) return false;
    }
    if (category && !(t.category || '').includes(category)) return false;
    if (minVal > 0 && (t.securities_value || 0) < minVal) return false;
    if (search) {
      const haystack = `${t.symbol || ''} ${t.company || ''} ${t.insider_name || ''}`.toLowerCase();
      if (!haystack.includes(search)) return false;
    }
    return true;
  });

  if (filtered.length === 0) { alert('Nothing to export — filter matches no rows.'); return; }

  const cols = [
    {key:'score',             label:'Score'},
    {key:'symbol',            label:'Symbol'},
    {key:'company',           label:'Company'},
    {key:'insider_name',      label:'Insider'},
    {key:'category',          label:'Category'},
    {key:'transaction_date',  label:'Date'},
    {key:'transaction_type',  label:'Type'},
    {key:'securities_count',  label:'Quantity'},
    {key:'securities_value',  label:'Value (INR)'},
    {key:'price_approx',      label:'Approx Price'},
    {key:'mode',              label:'Mode'},
    {key:'exchange',          label:'Exchange'},
  ];

  const esc = v => {
    const s = (v == null ? '' : String(v));
    return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };

  const header = cols.map(c => c.label).join(',');
  const rows   = filtered.map(r => cols.map(c => esc(r[c.key])).join(',')).join('\n');
  const csv    = header + '\n' + rows;

  const blob = new Blob([csv], {type: 'text/csv;charset=utf-8;'});
  const url  = URL.createObjectURL(blob);
  const a    = document.createElement('a');
  const stamp = new Date().toISOString().slice(0, 10);
  a.href = url;
  a.download = `insider_trades_${stamp}_${filtered.length}rows.csv`;
  document.body.appendChild(a);
  a.click();
  document.body.removeChild(a);
  URL.revokeObjectURL(url);
}

// ── Utilities ─────────────────────────────────────────────────────────────────

function _fmtVal(val) {
  if (!val) return '—';
  if (typeof mktFormatValue === 'function') return mktFormatValue(val);
  if (val >= 1e7)  return `₹${(val/1e7).toFixed(1)}Cr`;
  if (val >= 1e5)  return `₹${(val/1e5).toFixed(1)}L`;
  if (val >= 1000) return `₹${(val/1000).toFixed(0)}K`;
  return `₹${Math.round(val)}`;
}

function _truncate(s, n) {
  if (!s) return '—';
  return s.length > n ? s.slice(0, n) + '…' : s;
}

function _shortCat(cat) {
  if (!cat) return '—';
  if (cat.includes('Promoter') && !cat.includes('Group')) return 'Promoter';
  if (cat.includes('Promoter Group')) return 'Prom.Grp';
  if (cat.includes('Director')) return 'Director';
  if (cat.includes('KMP') || cat.includes('Key Managerial')) return 'KMP';
  if (cat.includes('Immediate')) return 'Relative';
  if (cat.includes('Designated')) return 'Desig.';
  return cat.slice(0, 10);
}

function _shortMode(mode) {
  if (!mode) return '—';
  const m = mode.toLowerCase();
  if (m.includes('market') && m.includes('purchase'))  return 'Mkt Buy';
  if (m.includes('market') && m.includes('sale'))      return 'Mkt Sell';
  if (m.includes('off')    && m.includes('market'))    return 'Off-Mkt';
  if (m.includes('preferential'))                      return 'Pref.';
  if (m.includes('esop') || m.includes('esosp'))       return 'ESOP';
  if (m.includes('pledge') && !m.includes('revok'))    return 'Pledge';
  if (m.includes('revok'))                             return 'Revoke';
  return mode.length > 12 ? mode.slice(0, 12) + '…' : mode;
}

// ── Auto-load when tab is shown ──────────────────────────────────────────────
function onInsiderTabLoad() {
  const sub = document.getElementById('ins-subtitle');
  const pwr = document.getElementById('ins-powered');
  if (typeof currentMarket !== 'undefined' && currentMarket === 'US') {
    if (sub) sub.textContent = 'SEC Form 4 Filings';
    if (pwr) pwr.textContent = 'Data: SEC EDGAR Form 4';
  } else {
    if (sub) sub.textContent = 'SEBI PIT Reg 7(2) Disclosures';
    if (pwr) pwr.textContent = 'Data: NSE SEBI PIT Disclosures';
  }
  if (_insiderAll.length === 0) loadInsiderData();
}
