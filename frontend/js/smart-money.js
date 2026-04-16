// ════════════════════════════════════════════════════════════════════════════
// SMART MONEY TRACKER — IV / PPV / Bull Snort Signal Intelligence
// Sorting + Export (PDF, Excel, CSV, PNG)
// ════════════════════════════════════════════════════════════════════════════

let _smMoneyData = null;
let _smNotes = {};
let _smClusterMode = false;
let _smSortCol = 'total_signals';
let _smSortDir = 'desc';

const SM_COLUMNS = [
  {key:'_rank',         label:'#',         sortable:false},
  {key:'sm_score',      label:'ALPHA',   sortable:true, type:'number'},
  {key:'ticker',        label:'TICKER',    sortable:true, type:'string'},
  {key:'total_signals', label:'SIGNALS',   sortable:true, type:'number'},
  {key:'price',         label:'PRICE',     sortable:true, type:'number'},
  {key:'change_pct',    label:'CHG%',      sortable:true, type:'number'},
  {key:'fvalue_grade',  label:'F-VALUE',   sortable:true, type:'grade'},
  {key:'insider_value_cr', label:'INSIDER ₹', sortable:true, type:'number'},
  {key:'stage',         label:'STAGE',     sortable:true, type:'stage'},
  {key:'rs_rating',     label:'RS',        sortable:true, type:'number'},
  {key:'vol_ratio',     label:'VOL',       sortable:true, type:'number'},
  {key:'sector',        label:'SECTOR',    sortable:true, type:'string'},
  {key:'_notes',        label:'NOTES',     sortable:false},
];

async function loadSmartMoney() {
  const days = document.getElementById('sm-days')?.value || 10;
  const wrap = document.getElementById('sm-table-wrap');
  wrap.innerHTML = _skeletonTable(8, 12, 'Scanning Smart Money signals across 2500+ stocks…');

  try {
    console.log('Smart Money: fetching signals for', days, 'days...');
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), 90000);
    const [smRes, notesRes] = await Promise.all([
      fetch(`${API}/api/smart-money?days=${days}`, {signal: controller.signal}),
      fetch(`${API}/api/smart-money/notes`)
    ]);
    clearTimeout(timeout);
    _smMoneyData = await smRes.json();
    _smNotes = await notesRes.json();
    console.log('Smart Money: got', _smMoneyData.tickers?.length || 0, 'tickers,', _smMoneyData.total_signals || 0, 'signals', _smMoneyData.error || '');

    if (_smMoneyData.error) throw new Error(_smMoneyData.error);

    _renderFreshnessBar();
    _populateSectorFilter();
    _renderSmartMoneyStats();
    _renderSmartMoneyTable();
    
    // Re-show sector drill banner if context exists
    if (window._sectorDrillContext) {
      setTimeout(() => _showSectorDrillBanner(), 100);
    }
  } catch (e) {
    wrap.innerHTML = `<div style="text-align:center;padding:40px;color:var(--red);font-family:var(--font-mono)">\u26a0 ${e.message}<br><button onclick="loadSmartMoney()" style="margin-top:8px;padding:6px 16px;border-radius:6px;border:1px solid var(--border);background:var(--bg3);color:var(--cyan);cursor:pointer;font-family:var(--font-mono);font-size:11px">\ud83d\udd04 Retry</button></div>`;
  }
}

function _renderSmartMoneyStats() {
  const d = _smMoneyData;
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

function _freshBadge(label, timestamp, source) {
  if (!timestamp || timestamp === 'Never' || timestamp === 'Not computed' || timestamp === 'Unknown') {
    return `<span class="sm-fresh-item stale" title="${source || ''}"><span class="sm-fresh-dot stale"></span>${label}: <b>Not synced</b></span>`;
  }
  // Check if within last 24 hours
  let isFresh = false;
  try {
    const ts = new Date(timestamp);
    const ageHrs = (Date.now() - ts.getTime()) / (1000 * 60 * 60);
    isFresh = ageHrs < 24;
  } catch(e) {
    // For date-only strings like "2026-04-02"
    const today = new Date().toISOString().slice(0, 10);
    isFresh = timestamp >= today || timestamp >= new Date(Date.now() - 86400000).toISOString().slice(0, 10);
  }
  const shortTs = timestamp.length > 10 ? timestamp.slice(5, 16).replace('T', ' ') : timestamp.slice(5);
  return `<span class="sm-fresh-item ${isFresh ? 'fresh' : 'stale'}" title="${source || ''}: ${timestamp}"><span class="sm-fresh-dot ${isFresh ? 'fresh' : 'stale'}"></span>${label}: <b>${shortTs}</b></span>`;
}

// ── SORTING ──────────────────────────────────────────────────────────────────

function smSortBy(colKey) {
  if (_smSortCol === colKey) {
    _smSortDir = _smSortDir === 'desc' ? 'asc' : 'desc';
  } else {
    _smSortCol = colKey;
    _smSortDir = 'desc';
  }
  _renderSmartMoneyTable();
}

function _sortTickers(tickers) {
  const col = SM_COLUMNS.find(c => c.key === _smSortCol);
  if (!col || !col.sortable) return tickers;
  const dir = _smSortDir === 'desc' ? -1 : 1;
  return tickers.sort((a, b) => {
    let va = a[_smSortCol], vb = b[_smSortCol];
    if (col.type === 'stage') {
      const so = {'Stage 2': 4, 'Stage 1\u21922': 3, 'Stage 3': 2, 'Stage 4': 1};
      va = so[va] || 0; vb = so[vb] || 0;
    } else if (col.type === 'grade') {
      const go = {'A': 5, 'B': 4, 'C': 3, 'D': 2, 'E': 1, '': 0};
      va = go[va] || 0; vb = go[vb] || 0;
    } else if (col.type === 'string') {
      va = (va || '').toLowerCase(); vb = (vb || '').toLowerCase();
      return va < vb ? -dir : va > vb ? dir : 0;
    } else { va = va ?? -Infinity; vb = vb ?? -Infinity; }
    return (va - vb) * dir;
  });
}

// ── TABLE RENDER ─────────────────────────────────────────────────────────────

function _renderSmartMoneyTable() {
  let tickers = _getFilteredTickers();
  if (!tickers.length) {
    document.getElementById('sm-table-wrap').innerHTML = '<div style="text-align:center;padding:40px;color:var(--text3);font-family:var(--font-mono)">\ud83d\udd0d No signals match the current filters</div>';
    return;
  }
  tickers = _sortTickers(tickers);

  const gc = v => (v || 0) >= 0 ? 'var(--green)' : 'var(--red)';
  const f = (v, d = 1) => v != null ? Number(v).toFixed(d) : '\u2014';

  const rows = tickers.map((t, i) => {
    const sigBadges = [];
    if (t.iv_count) sigBadges.push(`<span class="sm-sig iv">IV \xd7${t.iv_count}</span>`);
    if (t.ppv_count) sigBadges.push(`<span class="sm-sig ppv">PPV \xd7${t.ppv_count}</span>`);
    if (t.bs_count) sigBadges.push(`<span class="sm-sig bs">BS \xd7${t.bs_count}</span>`);
    const insiderBadge = t.insider_buys > 0 ? `<span class="sm-sig insider" title="${t.insider_details?.map(d=>d.name+' ('+d.category+') '+d.date).join(', ')||''}">\ud83c\udfe6 ${t.insider_buys}</span>` : '';
    const stageColor = t.stage === 'Stage 2' ? 'var(--green)' : t.stage === 'Stage 1\u21922' ? 'var(--amber)' : 'var(--text3)';
    const rsColor = (t.rs_rating||0) >= 80 ? 'var(--green)' : (t.rs_rating||0) >= 60 ? 'var(--amber)' : 'var(--red)';

    // SM Score color + cluster tooltip
    const smScore = t.sm_score || 0;
    const smColor = smScore >= 70 ? 'var(--green)' : smScore >= 40 ? 'var(--amber)' : 'var(--text3)';
    const cl = t.alpha_clusters || {};
    const clTip = `Alpha Score: ${smScore}/100\nMomentum: ${cl.momentum||0}/20\nFlow (IV/PPV/BS): ${cl.flow||0}/20\nInstitutional: ${cl.institutional||0}/20\nFundamental: ${cl.fundamental||0}/20\nRegime: ${cl.regime||0}/20`;

    // F-Value badge
    const fvGrade = t.fvalue_grade || '';
    const fvColor = {'A':'var(--green)','B':'#4ade80','C':'var(--amber)','D':'#fb923c','E':'var(--red)'}[fvGrade] || 'var(--text3)';
    const fvStatus = t.fv_status || '';
    const fvBadge = fvGrade ? `<span style="color:${fvColor};font-weight:700">${fvGrade}</span> <span style="font-size:9px;color:${t.fv_status_color||'var(--text3)'}">${fvStatus}</span>` : '\u2014';

    // Insider value
    const insVal = t.insider_value_cr > 0 ? `\u20b9${t.insider_value_cr.toFixed(1)}Cr` : '\u2014';
    const insCat = t.insider_top_category || '';
    const insColor = insCat.includes('Promoter') ? 'var(--green)' : insCat.includes('Director') ? 'var(--cyan)' : 'var(--text2)';

    const note = _smNotes[t.ticker]?.note || '';

    return `<tr class="sm-row">
      <td class="sm-td sm-rank">${i+1}</td>
      <td class="sm-td"><span class="sm-score-pill" style="background:${smColor};color:#0a0e17" title="${clTip}">${smScore}</span></td>
      <td class="sm-td sm-ticker"><span class="ticker-link" onclick="openTickerChart('${t.ticker}')">${t.ticker}</span></td>
      <td class="sm-td">${sigBadges.join(' ')} ${insiderBadge}</td>
      <td class="sm-td" style="font-family:var(--font-mono)">\u20b9${t.price?.toLocaleString('en-IN')||'\u2014'}</td>
      <td class="sm-td" style="color:${gc(t.change_pct)};font-family:var(--font-mono)">${t.change_pct>=0?'+':''}${f(t.change_pct)}%</td>
      <td class="sm-td">${fvBadge}</td>
      <td class="sm-td" style="font-family:var(--font-mono);color:${insColor}">${insVal}${insCat ? `<br><span style="font-size:8px">${insCat}</span>` : ''}</td>
      <td class="sm-td" style="color:${stageColor}">${t.stage||'\u2014'}</td>
      <td class="sm-td"><span style="color:${rsColor};font-weight:700">${t.rs_rating??'\u2014'}</span></td>
      <td class="sm-td" style="color:${(t.vol_ratio||0)>=2?'var(--cyan)':'var(--text2)'};font-family:var(--font-mono)">${f(t.vol_ratio,2)}x</td>
      <td class="sm-td"><span class="sm-sector">${t.sector||'\u2014'}</span></td>
      <td class="sm-td"><input class="sm-note-input" id="sm-note-${t.ticker}" value="${_escHtml(note)}" placeholder="Add note..." onblur="saveSmNote('${t.ticker}',this.value)"></td>
    </tr>`;
  }).join('');

  const sortIcon = k => _smSortCol===k ? (_smSortDir==='desc'?' \u25bc':' \u25b2') : '';
  const thClick = col => col.sortable ? `onclick="smSortBy('${col.key}')" style="cursor:pointer;user-select:none" title="Click to sort"` : '';
  const headerCells = SM_COLUMNS.map(col =>
    `<th class="sm-th${col.sortable?' sm-th-sort':''}" ${thClick(col)} ${col.key==='_notes'?'style="min-width:140px"':''}>${col.label}${sortIcon(col.key)}</th>`
  ).join('');

  document.getElementById('sm-table-wrap').innerHTML = `
    <div style="overflow-x:auto" id="sm-table-container">
    <table class="sm-table" id="sm-export-table">
      <thead><tr>${headerCells}</tr></thead>
      <tbody>${rows}</tbody>
    </table>
    </div>
    <div style="padding:8px 12px;font-size:10px;color:var(--text3);font-family:var(--font-mono);display:flex;justify-content:space-between;align-items:center;flex-wrap:wrap;gap:8px">
      <span>${tickers.length} tickers \xb7 ${_smMoneyData.total_signals} total signals \xb7 ${_smMoneyData.dates_covered?.length||0} trading days</span>
      <span style="display:flex;gap:4px">
        <button class="sm-export-btn" onclick="smExportCSV()" title="Export CSV">\ud83d\udcc4 CSV</button>
        <button class="sm-export-btn" onclick="smExportExcel()" title="Export Excel">\ud83d\udcca Excel</button>
        <button class="sm-export-btn" onclick="smExportPDF()" title="Export PDF">\ud83d\udcd5 PDF</button>
        <button class="sm-export-btn" onclick="smExportPNG()" title="Export PNG">\ud83d\udcf7 PNG</button>
      </span>
    </div>`;
}

function _renderFreshnessBar() {
  const fr = _smMoneyData?.data_freshness || {};
  const el = document.getElementById('sm-freshness');
  if (!el || !Object.keys(fr).length) return;
  const items = [];
  if (fr.ohlcv) items.push(_freshBadge('OHLCV', fr.ohlcv.last_date, fr.ohlcv.source));
  if (fr.tv_fundamentals) items.push(_freshBadge('F-Value', fr.tv_fundamentals.last_sync, fr.tv_fundamentals.source));
  if (fr.insider) items.push(_freshBadge('Insider', fr.insider.last_sync, fr.insider.source + ' (' + (fr.insider.total_records||0) + ' trades)'));
  if (fr.rs_rankings) items.push(_freshBadge('RS Ranks', fr.rs_rankings.last_computed, fr.rs_rankings.source));
  if (fr.fiidii) items.push(_freshBadge('FII/DII', fr.fiidii.last_date, fr.fiidii.source));
  el.innerHTML = items.join('');
  el.style.display = items.length ? 'flex' : 'none';
}

function _populateSectorFilter() {
  const sel = document.getElementById('sm-filter-sector');
  if (!sel || !_smMoneyData?.tickers) return;
  const sectors = [...new Set(_smMoneyData.tickers.map(t => t.sector).filter(Boolean))].sort();
  
  // Preserve drill-down context if active
  const drillCtx = window._sectorDrillContext;
  const current = drillCtx?.matchedSector || drillCtx?.sector || sel.value;
  
  sel.innerHTML = '<option value="all">All Sectors</option>' +
    sectors.map(s => `<option value="${s}" ${s === current ? 'selected' : ''}>${s}</option>`).join('');
}

function _getFilteredTickers() {
  if (!_smMoneyData?.tickers) return [];
  let tickers = [..._smMoneyData.tickers];

  const sigFilter = document.getElementById('sm-filter-signal')?.value || 'all';
  const stageFilter = document.getElementById('sm-filter-stage')?.value || 'all';
  const fvFilter = document.getElementById('sm-filter-fvalue')?.value || 'all';
  const rsMin = parseInt(document.getElementById('sm-filter-rs')?.value || '0');
  const smMin = parseInt(document.getElementById('sm-filter-score')?.value || '0');
  const sectorFilter = document.getElementById('sm-filter-sector')?.value || 'all';

  // Signal filter
  if (sigFilter !== 'all') {
    tickers = tickers.filter(t => {
      if (sigFilter === 'IV') return t.iv_count > 0;
      if (sigFilter === 'PPV') return t.ppv_count > 0;
      if (sigFilter === 'BS') return t.bs_count > 0;
      return true;
    });
  }

  // Stage filter
  if (stageFilter === 'stage2') tickers = tickers.filter(t => t.stage === 'Stage 2');

  // F-Value filter
  if (fvFilter !== 'all') {
    if (fvFilter === 'A') tickers = tickers.filter(t => t.fvalue_grade === 'A');
    else if (fvFilter === 'B') tickers = tickers.filter(t => ['A','B'].includes(t.fvalue_grade));
    else if (fvFilter === 'C') tickers = tickers.filter(t => ['A','B','C'].includes(t.fvalue_grade));
    else if (fvFilter === 'deep') tickers = tickers.filter(t => t.fv_status === 'DEEP VALUE');
    else if (fvFilter === 'under') tickers = tickers.filter(t => ['DEEP VALUE','UNDERVALUED'].includes(t.fv_status));
  }

  // RS filter
  if (rsMin > 0) tickers = tickers.filter(t => (t.rs_rating || 0) >= rsMin);

  // SM Score filter
  if (smMin > 0) tickers = tickers.filter(t => (t.sm_score || 0) >= smMin);

  // Sector filter — with drill-down fallback using breadth data
  if (sectorFilter !== 'all') {
    const drillCtx = window._sectorDrillContext;
    const filtered = tickers.filter(t => t.sector === sectorFilter);
    
    if (filtered.length > 0) {
      tickers = filtered;
    } else if (drillCtx) {
      // Sectors empty in SM data — try matching from breadth sector data
      const breadthData = currentData?.[currentMarket];
      if (breadthData?.sector_breadth) {
        const secData = breadthData.sector_breadth.find(s => 
          s.sector === drillCtx.sector || 
          s.sector.toLowerCase() === (sectorFilter || '').toLowerCase()
        );
        if (secData?.tickers) {
          const secTickers = new Set(secData.tickers.map(t => t.toUpperCase()));
          tickers = tickers.filter(t => secTickers.has(t.ticker.toUpperCase()));
        }
      }
    }
  }

  // Cluster mode sort
  if (_smClusterMode) tickers.sort((a, b) => b.total_signals - a.total_signals);

  return tickers;
}

function filterSmartMoney() { _renderSmartMoneyTable(); }

function toggleClusterMode() {
  _smClusterMode = !_smClusterMode;
  const btn = document.getElementById('sm-cluster-btn');
  if (btn) { btn.classList.toggle('active', _smClusterMode); btn.textContent = _smClusterMode ? '\ud83d\udcca Cluster ON' : '\ud83d\udcca Cluster Mode'; }
  _renderSmartMoneyTable();
}

// ── EXPORT ───────────────────────────────────────────────────────────────────

function _getExportData() {
  return _getFilteredTickers().map(t => {
    const cl = t.alpha_clusters || {};
    return {
      'Alpha Score': t.sm_score || 0,
      'Momentum': cl.momentum || 0,
      'Flow': cl.flow || 0,
      'Institutional': cl.institutional || 0,
      'Fundamental': cl.fundamental || 0,
      'Regime': cl.regime || 0,
      'Ticker': t.ticker,
      'Signals': [t.iv_count?`IV\xd7${t.iv_count}`:'', t.ppv_count?`PPV\xd7${t.ppv_count}`:'', t.bs_count?`BS\xd7${t.bs_count}`:''
      ].filter(Boolean).join(' '),
      'Price': t.price || 0,
      'Chg%': t.change_pct != null ? t.change_pct.toFixed(2) : '',
      'F-Value Grade': t.fvalue_grade || '',
      'FV Status': t.fv_status || '',
      'Upside%': t.upside_pct != null ? t.upside_pct.toFixed(1) : '',
      'Insider Buys': t.insider_buys || 0,
      'Insider Value Cr': t.insider_value_cr || 0,
      'Insider Category': t.insider_top_category || '',
      'Stage': t.stage || '',
      'RS': t.rs_rating ?? '',
      'A/D': t.ad_rating || '',
      'Vol Surge': t.vol_ratio != null ? t.vol_ratio.toFixed(2) + 'x' : '',
      'Sector': t.sector || '',
      'Notes': _smNotes[t.ticker]?.note || '',
    };
  });
}

function smExportCSV() {
  const data = _getExportData();
  if (!data.length) return;
  const headers = Object.keys(data[0]);
  const csvRows = [headers.join(',')];
  data.forEach(row => {
    csvRows.push(headers.map(h => {
      let v = String(row[h] ?? '');
      if (v.includes(',') || v.includes('"') || v.includes('\n')) v = '"' + v.replace(/"/g, '""') + '"';
      return v;
    }).join(','));
  });
  const blob = new Blob([csvRows.join('\n')], {type:'text/csv'});
  const a = document.createElement('a'); a.href = URL.createObjectURL(blob);
  a.download = `smart_money_${new Date().toISOString().slice(0,10)}.csv`; a.click();
}

function smExportExcel() {
  const data = _getExportData();
  if (!data.length) return;
  const headers = Object.keys(data[0]);
  let html = '<table border="1" style="border-collapse:collapse;font-family:Calibri;font-size:11px"><thead><tr>';
  headers.forEach(h => html += `<th style="background:#1e3a5f;color:white;padding:6px 10px;font-weight:bold">${h}</th>`);
  html += '</tr></thead><tbody>';
  data.forEach(row => { html += '<tr>'; headers.forEach(h => html += `<td style="padding:4px 8px">${row[h]??''}</td>`); html += '</tr>'; });
  html += '</tbody></table>';
  const blob = new Blob([`<html><head><meta charset="utf-8"></head><body>${html}</body></html>`], {type:'application/vnd.ms-excel'});
  const a = document.createElement('a'); a.href = URL.createObjectURL(blob);
  a.download = `smart_money_${new Date().toISOString().slice(0,10)}.xls`; a.click();
}

function smExportPDF() {
  const tableEl = document.getElementById('sm-table-container');
  if (!tableEl) return;
  const win = window.open('', '_blank');
  win.document.write(`<html><head><title>Smart Money Tracker</title>
    <style>
      body{font-family:'Segoe UI',sans-serif;padding:20px;color:#1a1a1a}
      h1{font-size:18px;margin-bottom:4px} h2{font-size:12px;color:#666;margin-bottom:16px}
      table{width:100%;border-collapse:collapse;font-size:10px}
      th{background:#1e3a5f;color:white;padding:6px 8px;text-align:left;font-size:9px}
      td{padding:5px 8px;border-bottom:1px solid #e0e0e0}
      tr:nth-child(even) td{background:#f8f9fa}
      input{border:none;background:transparent;font-size:10px}
    </style></head><body>
    <h1>\u26a1 Smart Money Tracker</h1>
    <h2>${_smMoneyData?.unique_tickers||0} tickers \xb7 ${_smMoneyData?.total_signals||0} signals \xb7 ${new Date().toLocaleString()}</h2>
    ${tableEl.innerHTML}
    <script>setTimeout(()=>window.print(),500)<\/script></body></html>`);
  win.document.close();
}

function smExportPNG() {
  const el = document.getElementById('sm-table-container');
  if (!el) return;
  if (typeof html2canvas === 'function') {
    html2canvas(el, {backgroundColor:'#0a0e17', scale:2}).then(canvas => {
      const a = document.createElement('a'); a.href = canvas.toDataURL('image/png');
      a.download = `smart_money_${new Date().toISOString().slice(0,10)}.png`; a.click();
    });
  } else {
    // Fallback: open print dialog
    smExportPDF();
  }
}

// ── NOTES ────────────────────────────────────────────────────────────────────

async function saveSmNote(ticker, note) {
  try {
    await fetch(`${API}/api/smart-money/notes`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ ticker, note })
    });
    _smNotes[ticker] = { note, updated_at: new Date().toISOString() };
  } catch (e) { console.warn('Failed to save note:', e); }
}

function _escHtml(s) {
  return (s||'').replace(/&/g,'&amp;').replace(/"/g,'&quot;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}
