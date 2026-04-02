// ════════════════════════════════════════════════════════════════════════════
// TRADING JOURNAL — Edgewonk-style Trade Tracker
// R-Multiples, Psychology, Setup Analysis, Equity Curve
// ════════════════════════════════════════════════════════════════════════════

let _jnlTrades = [];
let _jnlAnalytics = null;

async function jnlLoadTrades() {
  const status = document.getElementById('jnl-filter-status')?.value || 'all';
  try {
    const res = await fetch(`${API}/api/journal/trades?status=${status}`);
    _jnlTrades = await res.json();
    _jnlRenderStats();
    _jnlRenderTable();
  } catch (e) {
    console.error('Journal load error:', e);
  }
}

function _jnlRenderStats() {
  const el = document.getElementById('jnl-stats');
  const trades = _jnlTrades;
  const closed = trades.filter(t => t.status === 'Closed' || t.status === 'StoppedOut');
  const opens = trades.filter(t => t.status === 'Open');
  const winners = closed.filter(t => t.pnl_pct > 0);
  const winRate = closed.length ? (winners.length / closed.length * 100).toFixed(1) : '0';
  const totalPnl = closed.reduce((a, t) => a + (t.pnl_amount || 0), 0);
  const avgR = closed.length ? (closed.reduce((a, t) => a + (t.r_multiple || 0), 0) / closed.length).toFixed(2) : '0';
  const livePnl = opens.reduce((a, t) => a + (t.live_pnl_amount || 0), 0);

  el.innerHTML = `
    <div class="sm-stat-card"><div class="sm-stat-num" style="color:var(--text)">${trades.length}</div><div class="sm-stat-label">TOTAL</div></div>
    <div class="sm-stat-card"><div class="sm-stat-num" style="color:var(--cyan)">${opens.length}</div><div class="sm-stat-label">OPEN</div></div>
    <div class="sm-stat-card"><div class="sm-stat-num" style="color:var(--green)">${winRate}%</div><div class="sm-stat-label">WIN RATE</div></div>
    <div class="sm-stat-card"><div class="sm-stat-num" style="color:${avgR >= 0 ? 'var(--green)' : 'var(--red)'}">${avgR}R</div><div class="sm-stat-label">AVG R</div></div>
    <div class="sm-stat-card"><div class="sm-stat-num" style="color:${totalPnl >= 0 ? 'var(--green)' : 'var(--red)'}">₹${Math.round(totalPnl).toLocaleString('en-IN')}</div><div class="sm-stat-label">REALIZED P&L</div></div>
    <div class="sm-stat-card"><div class="sm-stat-num" style="color:${livePnl >= 0 ? 'var(--green)' : 'var(--red)'}">₹${Math.round(livePnl).toLocaleString('en-IN')}</div><div class="sm-stat-label">OPEN P&L</div></div>
  `;
}

function _jnlRenderTable() {
  const trades = _jnlTrades;
  if (!trades.length) {
    document.getElementById('jnl-table-wrap').innerHTML = '<div style="text-align:center;padding:40px;color:var(--text3);font-family:var(--font-mono)">📖 No trades. Click <b>+ New Trade</b> to start journaling.</div>';
    return;
  }

  const gc = v => (v || 0) >= 0 ? 'var(--green)' : 'var(--red)';
  const f = (v, d = 1) => v != null ? Number(v).toFixed(d) : '—';

  const rows = trades.map(t => {
    const isOpen = t.status === 'Open';
    const pnl = isOpen ? (t.live_pnl_pct || 0) : (t.pnl_pct || 0);
    const pnlAmt = isOpen ? (t.live_pnl_amount || 0) : (t.pnl_amount || 0);
    const rMult = isOpen ? (t.live_r_multiple || 0) : (t.r_multiple || 0);
    const price = isOpen ? (t.live_price || t.entry_price) : (t.exit_price || t.entry_price);

    const statusBadge = t.status === 'Open' ? '<span class="jnl-badge open">OPEN</span>'
      : t.status === 'StoppedOut' ? '<span class="jnl-badge stopped">STOPPED</span>'
      : pnl >= 0 ? '<span class="jnl-badge win">WIN</span>' : '<span class="jnl-badge loss">LOSS</span>';

    const emotionIcon = {'Confident':'😎','Patient':'🧘','FOMO':'😰','Revenge':'😡','Bored':'😴','Fearful':'😨'}[t.pre_emotion] || '';
    const setupBadge = t.setup_type ? `<span class="jnl-setup-tag">${t.setup_type}</span>` : '';
    const regimeBadge = t.regime ? `<span class="jnl-regime-tag ${t.regime.toLowerCase()}">${t.regime}</span>` : '';

    return `<tr class="sm-row" ondblclick="jnlEditTrade(${t.id})">
      <td class="sm-td" style="font-family:var(--font-mono);font-weight:700">${t.ticker}</td>
      <td class="sm-td">${t.direction === 'Short' ? '🔴' : '🟢'} ${t.direction}</td>
      <td class="sm-td">${setupBadge} ${regimeBadge}</td>
      <td class="sm-td" style="font-family:var(--font-mono)">${t.entry_date?.slice(0,10) || '—'}</td>
      <td class="sm-td" style="font-family:var(--font-mono)">₹${t.entry_price?.toLocaleString('en-IN') || '—'}</td>
      <td class="sm-td" style="font-family:var(--font-mono);color:var(--red)">${t.stop_loss ? '₹' + t.stop_loss.toLocaleString('en-IN') : '—'}</td>
      <td class="sm-td" style="font-family:var(--font-mono)">₹${price?.toLocaleString('en-IN') || '—'}</td>
      <td class="sm-td" style="color:${gc(pnl)};font-family:var(--font-mono);font-weight:700">${pnl >= 0 ? '+' : ''}${f(pnl)}%</td>
      <td class="sm-td" style="color:${gc(rMult)};font-family:var(--font-mono);font-weight:700">${rMult >= 0 ? '+' : ''}${f(rMult, 2)}R</td>
      <td class="sm-td">${statusBadge}</td>
      <td class="sm-td">${emotionIcon} ${t.discipline_score ? '⭐' + t.discipline_score : ''}</td>
      <td class="sm-td" style="font-size:10px;color:var(--text3);max-width:150px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${(t.notes||'').replace(/"/g,'&quot;')}">${t.notes || ''}</td>
      <td class="sm-td">
        <button class="sm-export-btn" onclick="jnlEditTrade(${t.id})" title="Edit">✏</button>
        ${isOpen ? `<button class="sm-export-btn" onclick="jnlCloseTrade(${t.id})" title="Close trade">✅</button>` : ''}
        <button class="sm-export-btn" onclick="jnlDeleteTrade(${t.id})" title="Delete" style="color:var(--red)">✗</button>
      </td>
    </tr>`;
  }).join('');

  document.getElementById('jnl-table-wrap').innerHTML = `
    <div style="overflow-x:auto">
    <table class="sm-table">
      <thead><tr>
        <th class="sm-th">TICKER</th><th class="sm-th">DIR</th><th class="sm-th">SETUP / REGIME</th>
        <th class="sm-th">ENTRY DATE</th><th class="sm-th">ENTRY ₹</th><th class="sm-th">STOP</th>
        <th class="sm-th">CMP / EXIT</th><th class="sm-th">P&L%</th><th class="sm-th">R-MULT</th>
        <th class="sm-th">STATUS</th><th class="sm-th">PSYCH</th><th class="sm-th">NOTES</th>
        <th class="sm-th">ACTIONS</th>
      </tr></thead>
      <tbody>${rows}</tbody>
    </table>
    </div>`;
}

// ── MODAL ────────────────────────────────────────────────────────────────────

function jnlShowAddModal() {
  document.getElementById('jnl-edit-id').value = '';
  document.getElementById('jnl-modal-title').textContent = 'New Trade';
  ['jnl-ticker','jnl-entry-price','jnl-stop','jnl-target','jnl-qty','jnl-exit-price','jnl-notes'].forEach(id => {
    const el = document.getElementById(id); if (el) el.value = '';
  });
  document.getElementById('jnl-entry-date').value = new Date().toISOString().slice(0,10);
  document.getElementById('jnl-exit-date').value = '';
  document.getElementById('jnl-direction').value = 'Long';
  document.getElementById('jnl-setup').value = '';
  document.getElementById('jnl-timeframe').value = 'Swing';
  document.getElementById('jnl-status').value = 'Open';
  document.getElementById('jnl-regime').value = '';
  document.getElementById('jnl-emotion').value = '';
  document.getElementById('jnl-discipline').value = '';
  document.getElementById('jnl-review').value = '';
  document.getElementById('jnl-modal').style.display = 'flex';
}

function jnlCloseModal() {
  document.getElementById('jnl-modal').style.display = 'none';
}

async function jnlEditTrade(id) {
  const trade = _jnlTrades.find(t => t.id === id);
  if (!trade) return;
  document.getElementById('jnl-edit-id').value = id;
  document.getElementById('jnl-modal-title').textContent = 'Edit Trade #' + id;
  document.getElementById('jnl-ticker').value = trade.ticker || '';
  document.getElementById('jnl-direction').value = trade.direction || 'Long';
  document.getElementById('jnl-setup').value = trade.setup_type || '';
  document.getElementById('jnl-timeframe').value = trade.timeframe || 'Swing';
  document.getElementById('jnl-entry-date').value = (trade.entry_date || '').slice(0,10);
  document.getElementById('jnl-entry-price').value = trade.entry_price || '';
  document.getElementById('jnl-stop').value = trade.stop_loss || '';
  document.getElementById('jnl-target').value = trade.target || '';
  document.getElementById('jnl-qty').value = trade.quantity || '';
  document.getElementById('jnl-exit-date').value = (trade.exit_date || '').slice(0,10);
  document.getElementById('jnl-exit-price').value = trade.exit_price || '';
  document.getElementById('jnl-status').value = trade.status || 'Open';
  document.getElementById('jnl-regime').value = trade.regime || '';
  document.getElementById('jnl-emotion').value = trade.pre_emotion || '';
  document.getElementById('jnl-discipline').value = trade.discipline_score || '';
  document.getElementById('jnl-review').value = trade.post_review || '';
  document.getElementById('jnl-notes').value = trade.notes || '';
  document.getElementById('jnl-modal').style.display = 'flex';
}

async function jnlSaveTrade() {
  const editId = document.getElementById('jnl-edit-id').value;
  const trade = {
    ticker: document.getElementById('jnl-ticker').value,
    direction: document.getElementById('jnl-direction').value,
    setup_type: document.getElementById('jnl-setup').value,
    timeframe: document.getElementById('jnl-timeframe').value,
    entry_date: document.getElementById('jnl-entry-date').value,
    entry_price: parseFloat(document.getElementById('jnl-entry-price').value) || 0,
    stop_loss: parseFloat(document.getElementById('jnl-stop').value) || null,
    target: parseFloat(document.getElementById('jnl-target').value) || null,
    quantity: parseFloat(document.getElementById('jnl-qty').value) || 0,
    exit_date: document.getElementById('jnl-exit-date').value || null,
    exit_price: parseFloat(document.getElementById('jnl-exit-price').value) || null,
    status: document.getElementById('jnl-status').value,
    regime: document.getElementById('jnl-regime').value,
    pre_emotion: document.getElementById('jnl-emotion').value,
    discipline_score: parseInt(document.getElementById('jnl-discipline').value) || 0,
    post_review: document.getElementById('jnl-review').value,
    notes: document.getElementById('jnl-notes').value,
  };

  if (!trade.ticker || !trade.entry_price) { alert('Ticker and Entry Price are required'); return; }

  try {
    if (editId) {
      await fetch(`${API}/api/journal/trades/${editId}`, { method:'PUT', headers:{'Content-Type':'application/json'}, body:JSON.stringify(trade) });
    } else {
      await fetch(`${API}/api/journal/trades`, { method:'POST', headers:{'Content-Type':'application/json'}, body:JSON.stringify(trade) });
    }
    jnlCloseModal();
    jnlLoadTrades();
  } catch (e) { alert('Save failed: ' + e.message); }
}

async function jnlCloseTrade(id) {
  const price = prompt('Enter exit price:');
  if (!price) return;
  await fetch(`${API}/api/journal/trades/${id}`, {
    method:'PUT', headers:{'Content-Type':'application/json'},
    body: JSON.stringify({ exit_price: parseFloat(price), exit_date: new Date().toISOString().slice(0,10), status: 'Closed' })
  });
  jnlLoadTrades();
}

async function jnlDeleteTrade(id) {
  if (!confirm('Delete this trade?')) return;
  await fetch(`${API}/api/journal/trades/${id}`, { method:'DELETE' });
  jnlLoadTrades();
}

// ── ANALYTICS ────────────────────────────────────────────────────────────────

async function jnlShowAnalytics() {
  const panel = document.getElementById('jnl-analytics-panel');
  if (panel.style.display !== 'none') { panel.style.display = 'none'; return; }
  panel.innerHTML = '<div style="padding:20px;text-align:center"><div class="ai-spinner"></div></div>';
  panel.style.display = 'block';

  try {
    const res = await fetch(`${API}/api/journal/analytics`);
    _jnlAnalytics = await res.json();
    _jnlRenderAnalytics();
  } catch (e) {
    panel.innerHTML = '<div style="padding:20px;color:var(--red)">Failed to load analytics</div>';
  }
}

function _jnlRenderAnalytics() {
  const a = _jnlAnalytics;
  if (!a || a.total === 0) {
    document.getElementById('jnl-analytics-panel').innerHTML = '<div style="padding:20px;color:var(--text3)">No closed trades yet for analytics.</div>';
    return;
  }

  const gc = v => v >= 0 ? 'var(--green)' : 'var(--red)';

  // By setup table
  const setupRows = Object.entries(a.by_setup || {}).map(([s, d]) =>
    `<tr><td class="sm-td">${s}</td><td class="sm-td">${d.trades}</td><td class="sm-td" style="color:${gc(d.win_rate-50)}">${d.win_rate}%</td><td class="sm-td" style="color:${gc(d.avg_r)}">${d.avg_r}R</td></tr>`
  ).join('');

  // By regime table
  const regimeRows = Object.entries(a.by_regime || {}).map(([r, d]) =>
    `<tr><td class="sm-td">${r}</td><td class="sm-td">${d.trades}</td><td class="sm-td" style="color:${gc(d.win_rate-50)}">${d.win_rate}%</td><td class="sm-td" style="color:${gc(d.avg_r)}">${d.avg_r}R</td></tr>`
  ).join('');

  // By emotion table
  const emotionRows = Object.entries(a.by_emotion || {}).map(([e, d]) =>
    `<tr><td class="sm-td">${e}</td><td class="sm-td">${d.trades}</td><td class="sm-td" style="color:${gc(d.win_rate-50)}">${d.win_rate}%</td><td class="sm-td" style="color:${gc(d.avg_r)}">${d.avg_r}R</td></tr>`
  ).join('');

  // Mistake frequency
  const mistakeRows = Object.entries(a.mistakes || {}).sort((a,b) => b[1]-a[1]).map(([m, c]) =>
    `<span class="jnl-mistake-tag">${m} (${c})</span>`
  ).join(' ');

  // R distribution (simple text histogram)
  const rVals = a.r_values || [];
  const rBig = rVals.filter(r => r >= 3).length;
  const rGood = rVals.filter(r => r >= 1 && r < 3).length;
  const rSmall = rVals.filter(r => r > 0 && r < 1).length;
  const rLoss = rVals.filter(r => r <= 0).length;

  document.getElementById('jnl-analytics-panel').innerHTML = `
    <div class="jnl-analytics-grid">
      <div class="jnl-analytics-card">
        <div class="sc-panel-title">📊 Performance Summary</div>
        <div class="sc-fund-grid">
          <div class="sc-fund-row"><span class="sc-fund-label">Win Rate</span><span class="sc-fund-value" style="color:${gc(a.win_rate-50)}">${a.win_rate}%</span></div>
          <div class="sc-fund-row"><span class="sc-fund-label">Avg Winner</span><span class="sc-fund-value" style="color:var(--green)">+${a.avg_winner}%</span></div>
          <div class="sc-fund-row"><span class="sc-fund-label">Avg Loser</span><span class="sc-fund-value" style="color:var(--red)">${a.avg_loser}%</span></div>
          <div class="sc-fund-row"><span class="sc-fund-label">Avg R-Multiple</span><span class="sc-fund-value" style="color:${gc(a.avg_r)}">${a.avg_r}R</span></div>
          <div class="sc-fund-row"><span class="sc-fund-label">Expectancy</span><span class="sc-fund-value" style="color:${gc(a.expectancy)}">${a.expectancy}%</span></div>
          <div class="sc-fund-row"><span class="sc-fund-label">Profit Factor</span><span class="sc-fund-value">${a.profit_factor}</span></div>
          <div class="sc-fund-row"><span class="sc-fund-label">Avg Hold</span><span class="sc-fund-value">${a.avg_holding_days}d</span></div>
          <div class="sc-fund-row"><span class="sc-fund-label">Win Streak</span><span class="sc-fund-value" style="color:var(--green)">${a.max_win_streak}</span></div>
          <div class="sc-fund-row"><span class="sc-fund-label">Loss Streak</span><span class="sc-fund-value" style="color:var(--red)">${a.max_loss_streak}</span></div>
        </div>
      </div>

      <div class="jnl-analytics-card">
        <div class="sc-panel-title">🎯 R-Multiple Distribution</div>
        <div style="font-family:var(--font-mono);font-size:11px;line-height:2">
          <div>🟢 Big Winners (≥3R): <b style="color:var(--green)">${rBig}</b></div>
          <div>🟢 Good (1-3R): <b style="color:var(--green)">${rGood}</b></div>
          <div>🟡 Small (0-1R): <b style="color:var(--amber)">${rSmall}</b></div>
          <div>🔴 Losers (≤0R): <b style="color:var(--red)">${rLoss}</b></div>
        </div>
      </div>

      <div class="jnl-analytics-card">
        <div class="sc-panel-title">⚡ By Setup Type</div>
        <table class="sm-table"><thead><tr><th class="sm-th">Setup</th><th class="sm-th">Trades</th><th class="sm-th">Win%</th><th class="sm-th">Avg R</th></tr></thead>
        <tbody>${setupRows || '<tr><td class="sm-td" colspan="4">No data</td></tr>'}</tbody></table>
      </div>

      <div class="jnl-analytics-card">
        <div class="sc-panel-title">📈 By Q-BRAM Regime</div>
        <table class="sm-table"><thead><tr><th class="sm-th">Regime</th><th class="sm-th">Trades</th><th class="sm-th">Win%</th><th class="sm-th">Avg R</th></tr></thead>
        <tbody>${regimeRows || '<tr><td class="sm-td" colspan="4">No data</td></tr>'}</tbody></table>
      </div>

      <div class="jnl-analytics-card">
        <div class="sc-panel-title">🧠 By Pre-Trade Emotion</div>
        <table class="sm-table"><thead><tr><th class="sm-th">Emotion</th><th class="sm-th">Trades</th><th class="sm-th">Win%</th><th class="sm-th">Avg R</th></tr></thead>
        <tbody>${emotionRows || '<tr><td class="sm-td" colspan="4">No data</td></tr>'}</tbody></table>
      </div>

      <div class="jnl-analytics-card">
        <div class="sc-panel-title">⚠ Mistake Frequency</div>
        <div style="padding:8px 0">${mistakeRows || '<span style="color:var(--text3);font-size:11px">No mistakes logged yet</span>'}</div>
      </div>
    </div>`;
}

// ── EXPORT ───────────────────────────────────────────────────────────────────

function jnlExportCSV() {
  const headers = ['Ticker','Direction','Setup','Timeframe','Regime','Entry Date','Entry Price','Stop Loss','Target','Qty','Exit Date','Exit Price','Status','P&L%','R-Multiple','Holding Days','Emotion','Review','Discipline','Notes'];
  const rows = [headers.join(',')];
  _jnlTrades.forEach(t => {
    rows.push([t.ticker,t.direction,t.setup_type,t.timeframe,t.regime,t.entry_date,t.entry_price,t.stop_loss,t.target,t.quantity,t.exit_date,t.exit_price,t.status,t.pnl_pct,t.r_multiple,t.holding_days,t.pre_emotion,t.post_review,t.discipline_score,`"${(t.notes||'').replace(/"/g,'""')}"`].join(','));
  });
  const blob = new Blob([rows.join('\n')], {type:'text/csv'});
  const a = document.createElement('a'); a.href = URL.createObjectURL(blob);
  a.download = `trade_journal_${new Date().toISOString().slice(0,10)}.csv`; a.click();
}
