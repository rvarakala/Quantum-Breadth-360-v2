/* ═══════════════════════════════════════════════════════════════════════════
   INSIDER TRADING TAB — JS Module
   Loads insider trades from API, renders table, handles filters/import.
   ═══════════════════════════════════════════════════════════════════════════ */

let _insiderData = [];
let _insiderFilter = 'all';
let _insiderClusters = [];

async function loadInsiderData() {
  const days = document.getElementById('ins-period')?.value || 90;
  const tbody = document.getElementById('ins-tbody');
  if (tbody) tbody.innerHTML = '<tr><td colspan="11" class="ins-empty">Loading insider data...</td></tr>';

  try {
    // Load trades
    const res = await fetch(`${API}/api/insider/trades?days=${days}&limit=500`);
    const data = await res.json();
    _insiderData = data.trades || [];

    // Load summary
    const sRes = await fetch(`${API}/api/insider/summary?days=${days}`);
    const summary = await sRes.json();
    _renderInsiderSummary(summary);
    _insiderClusters = summary.clusters || [];

    // Render
    filterInsiderTable();

  } catch (e) {
    console.error('Insider load error:', e);
    if (tbody) tbody.innerHTML = '<tr><td colspan="11" class="ins-empty">Failed to load insider data. Try importing a CSV.</td></tr>';
  }
}

function _renderInsiderSummary(s) {
  const el = (id, val) => { const e = document.getElementById(id); if (e) e.textContent = val; };
  el('ins-total', (s.total_transactions || 0).toLocaleString());
  el('ins-buys', (s.buys_count || 0).toLocaleString());
  el('ins-sells', (s.sells_count || 0).toLocaleString());
  el('ins-top-buy', s.top_buy_symbol || '—');
  el('ins-top-sell', s.top_sell_symbol || '—');
  el('ins-last-date', s.last_data_date || '—');

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
  // Update active button
  document.querySelectorAll('.ins-filter-btn').forEach(b => b.classList.remove('active'));
  const btn = document.getElementById(`ins-f-${type === 'all' ? 'all' : type === 'Buy' ? 'buy' : type === 'Sell' ? 'sell' : 'cluster'}`);
  if (btn) btn.classList.add('active');
  filterInsiderTable();
}

function filterInsiderTable() {
  const search = (document.getElementById('ins-search')?.value || '').toLowerCase();
  const category = document.getElementById('ins-category')?.value || '';

  let filtered = _insiderData.filter(t => {
    // Type filter
    if (_insiderFilter === 'Buy' && t.transaction_type !== 'Buy') return false;
    if (_insiderFilter === 'Sell' && t.transaction_type !== 'Sell') return false;
    if (_insiderFilter === 'cluster') {
      const clusterSyms = _insiderClusters.map(c => c.symbol);
      if (!clusterSyms.includes(t.symbol)) return false;
    }

    // Category filter
    if (category && !(t.category || '').includes(category)) return false;

    // Search filter
    if (search) {
      const haystack = `${t.symbol} ${t.company} ${t.insider_name}`.toLowerCase();
      if (!haystack.includes(search)) return false;
    }
    return true;
  });

  _renderInsiderTable(filtered);
}

function _renderInsiderTable(trades) {
  const tbody = document.getElementById('ins-tbody');
  if (!tbody) return;

  if (trades.length === 0) {
    tbody.innerHTML = '<tr><td colspan="11" class="ins-empty">No insider trades found. Import NSE PIT CSV or sync data.</td></tr>';
    _updateCount(0);
    return;
  }

  tbody.innerHTML = trades.map(t => {
    const isBuy = t.transaction_type === 'Buy';
    const isSell = t.transaction_type === 'Sell';
    const typeClass = isBuy ? 'ins-buy' : isSell ? 'ins-sell' : '';
    const scoreColor = t.score >= 80 ? '#22c55e' : t.score >= 65 ? '#86efac' : t.score >= 50 ? '#f59e0b' : '#64748b';

    // Find cluster count for this symbol
    const cluster = _insiderClusters.find(c => c.symbol === t.symbol);
    const clusterBadge = cluster ? `<span class="ins-cluster-badge">${cluster.insiders}</span>` : '';

    return `<tr class="${typeClass}">
      <td><span class="ins-score" style="background:${scoreColor}18;color:${scoreColor};border:1px solid ${scoreColor}44">${t.score}</span></td>
      <td><span class="ins-ticker">${t.symbol}</span> ${clusterBadge}</td>
      <td class="ins-company">${_truncate(t.company, 25)}</td>
      <td class="ins-insider">${_truncate(t.insider_name, 22)}</td>
      <td><span class="ins-cat-badge">${_shortCat(t.category)}</span></td>
      <td class="ins-date">${t.transaction_date || '—'}</td>
      <td><span class="ins-type-badge ${isBuy ? 'buy' : isSell ? 'sell' : ''}">${t.transaction_type}</span></td>
      <td class="ins-right">${(t.securities_count || 0).toLocaleString()}</td>
      <td class="ins-right ins-val">${t.value_fmt || '—'}</td>
      <td class="ins-right">${mktCurrency()}${(t.price_approx || 0).toLocaleString()}</td>
      <td class="ins-mode">${_shortMode(t.mode)}</td>
    </tr>`;
  }).join('');

  _updateCount(trades.length);
}

function _updateCount(n) {
  const el = document.getElementById('ins-count');
  if (el) el.textContent = `${n.toLocaleString()} trades`;
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
      alert(`Imported ${data.imported} insider trades from ${data.filename}`);
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

  try {
    const res = await fetch(`${API}/api/insider/sync?days=30`, { method: 'POST' });
    const data = await res.json();
    if (data.status === 'ok') {
      loadInsiderData();
    } else {
      alert(data.message || 'Sync returned no data. Try CSV import instead.');
    }
  } catch (e) {
    alert('Sync failed — NSE may be blocking. Use CSV import instead.');
  }

  if (btn) { btn.disabled = false; btn.textContent = '↻ Refresh'; }
}

async function repairInsiderData() {
  if (!confirm('This will delete records with missing dates or ₹0 values, then re-sync 90 days. Continue?')) return;
  const btn = document.querySelector('.ins-repair-btn');
  if (btn) { btn.disabled = true; btn.textContent = '🔧 Repairing...'; }

  try {
    const res = await fetch(`${API}/api/insider/repair`, { method: 'POST' });
    const data = await res.json();
    if (data.status === 'ok') {
      const r = data.resync;
      alert(`Repair done!\nDeleted: ${data.deleted_empty_dates} empty-date + ${data.deleted_zero_values} ₹0 records\nRe-synced: ${r.fetched || 0} fetched, ${r.stored || 0} stored`);
      loadInsiderData();
    } else {
      alert('Repair failed: ' + (data.message || 'unknown error'));
    }
  } catch (e) {
    alert('Repair request failed: ' + e);
  }

  if (btn) { btn.disabled = false; btn.textContent = '🔧 Repair DB'; }
}

// ── Utilities ─────────────────────────────────────────────────────────────────

function _fmtVal(val) {
  if (!val) return '—';
  return mktFormatValue(val);
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
  if (m.includes('market') && m.includes('purchase')) return 'Mkt Buy';
  if (m.includes('market') && m.includes('sale')) return 'Mkt Sell';
  if (m.includes('off') && m.includes('market')) return 'Off-Mkt';
  if (m.includes('preferential')) return 'Pref.';
  if (m.includes('esop') || m.includes('esosp')) return 'ESOP';
  return mode.slice(0, 12);
}

// ── Auto-load when tab is shown ──────────────────────────────────────────────
// Called from app.js switchTab()
function onInsiderTabLoad() {
  // Update labels based on current market
  const sub = document.getElementById('ins-subtitle');
  const pwr = document.getElementById('ins-powered');
  if (currentMarket === 'US') {
    if (sub) sub.textContent = 'SEC Form 4 Filings';
    if (pwr) pwr.textContent = 'Data: SEC EDGAR Form 4';
  } else {
    if (sub) sub.textContent = 'SEBI PIT Reg 7(2) Disclosures';
    if (pwr) pwr.textContent = 'Data: NSE SEBI PIT Disclosures';
  }
  if (_insiderData.length === 0) loadInsiderData();
}
