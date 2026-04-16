// ════════════════════════════════════════════════════════════════════════════
// TRADING JOURNAL — Enhanced (Sprint 5)
// Tilt Meter™, Psychology sliders, Equity/Drawdown/Monthly charts, AI Coach
// ════════════════════════════════════════════════════════════════════════════

let _jnlTrades       = [];
let _jnlAnalytics    = null;
let _jnlCharts       = {};
let _jnlView         = 'table';
let _jnlAccounts     = [];            // all accounts from API
let _jnlActiveAcct   = null;         // null = All Accounts, int = specific account id

async function jnlLoadTrades() {
  // Init accounts on first load
  if (_jnlAccounts.length === 0) await jnlInitAccounts();

  // All Accounts view: show summary cards instead of trade table
  if (_jnlActiveAcct === null) {
    _jnlRenderStats();
    await _jnlRenderAllAccountsSummary();
    _jnlLoadTilt();
    _jnlLoadRiskCheck();
    return;
  }

  const status = document.getElementById('jnl-filter-status')?.value || 'all';
  const acctParam = _jnlActiveAcct ? `&account_id=${_jnlActiveAcct}` : '';
  try {
    const res = await fetch(`${API}/api/journal/trades?status=${status}${acctParam}`);
    _jnlTrades = await res.json();
    _jnlRenderStats();
    _jnlRenderTable();
    _jnlLoadTilt();
    _jnlLoadRiskCheck();
    if (_jnlView === 'analytics') _jnlLoadAnalytics();
    if (_jnlView === 'ai')        _jnlLoadAICoach();
    if (_jnlView === 'settings')  { jnlLoadSettings(); }
  } catch (e) { console.error('Journal load error:', e); }
}

function jnlToggleView(view) {
  _jnlView = view;
  ['table','analytics','ai','settings'].forEach(v => {
    const p = document.getElementById(`jnl-view-${v}-panel`);
    if (p) p.style.display = v === view ? '' : 'none';
    const b = document.getElementById(`jnl-view-${v}`);
    if (b) b.classList.toggle('active', v === view);
  });
  if (view === 'analytics') _jnlLoadAnalytics();
  if (view === 'ai')        _jnlLoadAICoach();
  if (view === 'settings')  jnlLoadSettings();
}

async function _jnlLoadTilt() {
  try {
    const res  = await fetch(`${API}/api/journal/tilt${_jnlAcctParam()}`);
    const data = await res.json();
    _jnlRenderTilt(data);
  } catch {}
}

function _jnlRenderTilt(d) {
  const score  = d.score || 0;
  const level  = d.level || 'Calm';
  const color  = d.color || '#22c55e';
  const scoreEl  = document.getElementById('jnl-tilt-score');
  const levelEl  = document.getElementById('jnl-tilt-level');
  const barEl    = document.getElementById('jnl-tilt-bar');
  const factorEl = document.getElementById('jnl-tilt-factors');
  if (scoreEl)  { scoreEl.textContent = score; scoreEl.style.color = color; }
  if (levelEl)  { levelEl.textContent = level; levelEl.style.color = color; }
  if (barEl)    { barEl.style.width = score + '%'; barEl.style.background = color; }
  if (factorEl) factorEl.innerHTML = (d.factors||[]).slice(0,3).map(f => `<div style="color:var(--text3)">• ${f}</div>`).join('');
  const widget = document.getElementById('jnl-tilt-widget');
  if (widget) {
    widget.style.borderColor = score > 60 ? color : 'var(--card-border)';
    widget.style.boxShadow   = score > 60 ? `0 0 12px ${color}33` : 'none';
  }
}

function _jnlRenderStats() {
  const el     = document.getElementById('jnl-stats');
  const trades = _jnlTrades;
  const closed  = trades.filter(t => t.status === 'Closed' || t.status === 'StoppedOut');
  const opens   = trades.filter(t => t.status === 'Open');
  const winners = closed.filter(t => t.pnl_pct > 0);
  const losers  = closed.filter(t => t.pnl_pct < 0);
  const winRate  = closed.length ? (winners.length / closed.length * 100).toFixed(1) : '0';
  const totalPnl = closed.reduce((a,t) => a + (t.pnl_amount||0), 0);
  const avgR     = closed.length ? (closed.reduce((a,t) => a + (t.r_multiple||0), 0) / closed.length).toFixed(2) : '0';
  const livePnl  = opens.reduce((a,t) => a + (t.live_pnl_amount||0), 0);
  const avgWin   = winners.length ? winners.reduce((a,t) => a + t.pnl_pct, 0) / winners.length : 0;
  const avgLoss  = losers.length  ? losers.reduce((a,t)  => a + t.pnl_pct, 0) / losers.length  : 0;
  const expect   = closed.length
    ? ((winners.length/closed.length*avgWin) + (losers.length/closed.length*avgLoss)).toFixed(2) : '0';
  el.innerHTML = `
    <div class="sm-stat-card"><div class="sm-stat-num" style="color:var(--text)">${trades.length}</div><div class="sm-stat-label">TOTAL</div></div>
    <div class="sm-stat-card"><div class="sm-stat-num" style="color:var(--cyan)">${opens.length}</div><div class="sm-stat-label">OPEN</div></div>
    <div class="sm-stat-card"><div class="sm-stat-num" style="color:var(--green)">${winRate}%</div><div class="sm-stat-label">WIN RATE</div></div>
    <div class="sm-stat-card"><div class="sm-stat-num" style="color:${avgR>=0?'var(--green)':'var(--red)'}">Ø${avgR}R</div><div class="sm-stat-label">AVG R</div></div>
    <div class="sm-stat-card"><div class="sm-stat-num" style="color:${expect>=0?'var(--green)':'var(--red)'}">${expect}%</div><div class="sm-stat-label">EXPECTANCY</div></div>
    <div class="sm-stat-card"><div class="sm-stat-num" style="color:${totalPnl>=0?'var(--green)':'var(--red)'}">₹${Math.round(totalPnl).toLocaleString('en-IN')}</div><div class="sm-stat-label">REALIZED P&L</div></div>
    <div class="sm-stat-card"><div class="sm-stat-num" style="color:${livePnl>=0?'var(--green)':'var(--red)'}">₹${Math.round(livePnl).toLocaleString('en-IN')}</div><div class="sm-stat-label">OPEN P&L</div></div>`;
}

function _jnlRenderTable() {
  const trades = _jnlTrades;
  const wrap   = document.getElementById('jnl-table-wrap');
  if (!trades.length) {
    wrap.innerHTML = '<div style="text-align:center;padding:40px;color:var(--text3);font-family:var(--font-mono)">📖 No trades. Click <b>+ New Trade</b> to start journaling.</div>';
    return;
  }
  const gc = v => (v||0) >= 0 ? 'var(--green)' : 'var(--red)';
  const f  = (v,d=1) => v != null ? Number(v).toFixed(d) : '—';
  const rows = trades.map(t => {
    const isOpen  = t.status === 'Open';
    const pnl     = isOpen ? (t.live_pnl_pct||0) : (t.pnl_pct||0);
    const rMult   = isOpen ? (t.live_r_multiple||0) : (t.r_multiple||0);
    const price   = isOpen ? (t.live_price||t.entry_price) : (t.exit_price||t.entry_price);
    const statusBadge = t.status==='Open' ? '<span class="jnl-badge open">OPEN</span>'
      : t.status==='StoppedOut' ? '<span class="jnl-badge stopped">STOPPED</span>'
      : pnl>=0 ? '<span class="jnl-badge win">WIN</span>' : '<span class="jnl-badge loss">LOSS</span>';
    const gradeBadge = t.trade_grade ? `<span class="jnl-grade-tag ${t.trade_grade}">${t.trade_grade}</span>` : '';
    const planIcon   = t.followed_plan===0 ? '<span style="color:var(--red);font-size:10px">✗ broke</span>' : t.followed_plan===1 ? '<span style="color:var(--green);font-size:10px">✓ plan</span>' : '';
    const setupBadge = t.setup_type ? `<span class="jnl-setup-tag">${t.setup_type}</span>` : '';
    const pos = ((t.psych_confidence||0)+(t.psych_focus||0)+(t.psych_patience||0))/3;
    const psychColor = pos>=7?'var(--green)':pos>=4?'var(--amber)':'var(--red)';
    const psychBar = pos>0
      ? `<div style="display:flex;align-items:center;gap:4px"><div style="width:40px;height:3px;background:var(--card-border);border-radius:2px;overflow:hidden"><div style="height:100%;width:${Math.round(pos*10)}%;background:${psychColor}"></div></div><span style="font-size:9px;color:${psychColor}">${Math.round(pos)}</span></div>`
      : (t.discipline_score ? `⭐${t.discipline_score}` : '—');
    return `<tr class="sm-row" ondblclick="jnlEditTrade(${t.id})">
      <td class="sm-td" style="font-family:var(--font-mono);font-weight:700">${t.ticker}</td>
      <td class="sm-td">${t.direction==='Short'?'🔴':'🟢'} ${t.direction}</td>
      <td class="sm-td">${setupBadge}</td>
      <td class="sm-td" style="font-family:var(--font-mono)">${t.entry_date?.slice(0,10)||'—'}</td>
      <td class="sm-td" style="font-family:var(--font-mono)">₹${t.entry_price?.toLocaleString('en-IN')||'—'}</td>
      <td class="sm-td" style="font-family:var(--font-mono);color:var(--red)">${t.stop_loss?'₹'+t.stop_loss.toLocaleString('en-IN'):'—'}</td>
      <td class="sm-td" style="font-family:var(--font-mono)">₹${price?.toLocaleString('en-IN')||'—'}</td>
      <td class="sm-td" style="color:${gc(pnl)};font-family:var(--font-mono);font-weight:700">${pnl>=0?'+':''}${f(pnl)}%</td>
      <td class="sm-td" style="color:${gc(rMult)};font-family:var(--font-mono);font-weight:700">${rMult>=0?'+':''}${f(rMult,2)}R</td>
      <td class="sm-td">${statusBadge} ${gradeBadge}</td>
      <td class="sm-td">${psychBar}</td>
      <td class="sm-td">${planIcon}</td>
      <td class="sm-td" style="font-size:10px;color:var(--text3);max-width:140px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap">${t.notes||''}</td>
      <td class="sm-td">
        <button class="sm-export-btn" onclick="jnlEditTrade(${t.id})">✏</button>
        ${isOpen?`<button class="sm-export-btn" onclick="jnlCloseTrade(${t.id})" title="Close this trade" style="color:var(--cyan);font-weight:700">Close</button>`:''}
        <button class="sm-export-btn" onclick="jnlDeleteTrade(${t.id})" style="color:var(--red)">✗</button>
      </td>
    </tr>`;
  }).join('');
  wrap.innerHTML = `<div style="overflow-x:auto"><table class="sm-table" style="min-width:900px">
    <thead><tr>
      <th class="sm-th">TICKER</th><th class="sm-th">DIR</th><th class="sm-th">SETUP</th>
      <th class="sm-th">DATE</th><th class="sm-th">ENTRY ₹</th><th class="sm-th">STOP</th>
      <th class="sm-th">CMP/EXIT</th><th class="sm-th">P&L%</th><th class="sm-th">R</th>
      <th class="sm-th">STATUS</th><th class="sm-th">PSYCH</th><th class="sm-th">PLAN</th>
      <th class="sm-th">NOTES</th><th class="sm-th">ACTIONS</th>
    </tr></thead><tbody>${rows}</tbody></table></div>`;
}

async function _jnlLoadAnalytics() {
  try {
    const [analRes,ddRes,monthRes] = await Promise.all([
      fetch(`${API}/api/journal/analytics${_jnlAcctParam()}`),
      fetch(`${API}/api/journal/drawdown${_jnlAcctParam()}`),
      fetch(`${API}/api/journal/monthly${_jnlAcctParam()}`),
    ]);
    _jnlAnalytics    = await analRes.json();
    const dd         = await ddRes.json();
    const monthly    = (await monthRes.json()).monthly || [];
    if (!_jnlAnalytics || _jnlAnalytics.total === 0) {
      document.getElementById('jnl-perf-content').innerHTML = '<div style="color:var(--text3);font-size:11px;padding:8px">No closed trades yet.</div>';
      return;
    }
    _jnlRenderPerfCard(_jnlAnalytics, dd);
    _jnlRenderEquityChart(_jnlAnalytics.equity_curve || []);
    _jnlRenderDDChart(dd.curve || []);
    _jnlRenderRDist(_jnlAnalytics.r_values || []);
    _jnlRenderMonthly(monthly);
    _jnlRenderBreakdownTable('jnl-setup-content',   _jnlAnalytics.by_setup   || {});
    _jnlRenderBreakdownTable('jnl-regime-content',  _jnlAnalytics.by_regime  || {});
    _jnlRenderBreakdownTable('jnl-emotion-content', _jnlAnalytics.by_emotion || {});
    _jnlRenderMistakes(_jnlAnalytics.mistakes || {});
    _jnlRenderPsychTrend();
    // TOD + DOW charts
    try {
      const [todRes, dowRes] = await Promise.all([
        fetch(`${API}/api/journal/time-of-day${_jnlAcctParam()}`),
        fetch(`${API}/api/journal/day-of-week${_jnlAcctParam()}`),
      ]);
      _jnlRenderTOD((await todRes.json()).hours || []);
      _jnlRenderDOW((await dowRes.json()).days  || []);
    } catch {}
    // Gamification badges
    _jnlLoadGamification();
  } catch (e) { console.error('Analytics error:', e); }
}

function _jnlRenderPerfCard(a, dd) {
  const gc = v => v >= 0 ? 'var(--green)' : 'var(--red)';
  document.getElementById('jnl-perf-content').innerHTML = `<div class="sc-fund-grid">${[
    ['Win Rate',a.win_rate+'%',gc(a.win_rate-50)],
    ['Avg Winner','+'+a.avg_winner+'%','var(--green)'],
    ['Avg Loser',a.avg_loser+'%','var(--red)'],
    ['Avg R',a.avg_r+'R',gc(a.avg_r)],
    ['Expectancy',a.expectancy+'%',gc(a.expectancy)],
    ['Profit Factor',a.profit_factor,gc(a.profit_factor-1)],
    ['Avg Hold',a.avg_holding_days+'d','var(--text)'],
    ['Win Streak',a.max_win_streak,'var(--green)'],
    ['Loss Streak',a.max_loss_streak,'var(--red)'],
    ['Max Drawdown',dd.max_drawdown_pct+'%','var(--red)'],
    ['Cur Drawdown',dd.current_drawdown_pct+'%',dd.current_drawdown_pct<-5?'var(--red)':'var(--amber)'],
  ].map(([k,v,c])=>`<div class="sc-fund-row"><span class="sc-fund-label">${k}</span><span class="sc-fund-value" style="color:${c}">${v}</span></div>`).join('')}</div>`;
}

function _jnlDestroyChart(id) {
  if (_jnlCharts[id]) { try { _jnlCharts[id].destroy(); } catch(e){} _jnlCharts[id]=null; }
}

function _jnlChartOpts(isDark) {
  return {
    responsive: true,
    maintainAspectRatio: false,
    animation: { duration: 400 },
    plugins: { legend: { display: false } },
    scales: {
      x: { ticks: { color: isDark ? '#64748b' : '#94a3b8', maxTicksLimit: 8, font: { size: 9 } }, grid: { color: isDark ? 'rgba(255,255,255,0.04)' : 'rgba(0,0,0,0.04)' } },
      y: { ticks: { color: isDark ? '#64748b' : '#94a3b8', font: { size: 9 } },        grid: { color: isDark ? 'rgba(255,255,255,0.04)' : 'rgba(0,0,0,0.04)' } },
    },
  };
}

function _jnlRenderEquityChart(curve) {
  const canvas = document.getElementById('jnl-equity-canvas');
  if (!canvas||!curve.length) return;
  _jnlDestroyChart('jnl-equity-canvas');
  const isDark = !document.documentElement.getAttribute('data-theme');
  const data   = curve.map(p => p.pnl);
  const color  = data[data.length-1]>=0?'#22c55e':'#ef4444';
  _jnlCharts['jnl-equity-canvas'] = new Chart(canvas, {type:'line',
    data:{labels:curve.map(p=>p.date?.slice(0,10)),datasets:[{data,borderColor:color,backgroundColor:color+'18',fill:true,tension:0.3,pointRadius:2,borderWidth:2}]},
    options:_jnlChartOpts(isDark)});
}

function _jnlRenderDDChart(curve) {
  const canvas = document.getElementById('jnl-dd-canvas');
  if (!canvas||!curve.length) return;
  _jnlDestroyChart('jnl-dd-canvas');
  const isDark = !document.documentElement.getAttribute('data-theme');
  const opts = _jnlChartOpts(isDark);
  opts.scales.y.max = 0;
  _jnlCharts['jnl-dd-canvas'] = new Chart(canvas, {type:'line',
    data:{labels:curve.map(p=>p.date?.slice(0,10)),datasets:[{data:curve.map(p=>p.drawdown),borderColor:'#ef4444',backgroundColor:'rgba(239,68,68,0.12)',fill:true,tension:0.3,pointRadius:0,borderWidth:2}]},
    options:opts});
}

function _jnlRenderRDist(rVals) {
  const canvas = document.getElementById('jnl-rdist-canvas');
  if (!canvas||!rVals.length) return;
  _jnlDestroyChart('jnl-rdist-canvas');
  const isDark = !document.documentElement.getAttribute('data-theme');
  const bins=['<-2','-2→-1','-1→0','0→1','1→2','2→3','>3'];
  const counts=[0,0,0,0,0,0,0];
  rVals.forEach(r=>{if(r<-2)counts[0]++;else if(r<-1)counts[1]++;else if(r<0)counts[2]++;else if(r<1)counts[3]++;else if(r<2)counts[4]++;else if(r<3)counts[5]++;else counts[6]++;});
  _jnlCharts['jnl-rdist-canvas'] = new Chart(canvas,{type:'bar',
    data:{labels:bins,datasets:[{data:counts,backgroundColor:['#dc2626','#ef4444','#f87171','#fbbf24','#34d399','#22c55e','#16a34a']}]},
    options:_jnlChartOpts(isDark)});
}

function _jnlRenderMonthly(monthly) {
  const canvas = document.getElementById('jnl-monthly-canvas');
  if (!canvas||!monthly.length) return;
  _jnlDestroyChart('jnl-monthly-canvas');
  const isDark = !document.documentElement.getAttribute('data-theme');
  _jnlCharts['jnl-monthly-canvas'] = new Chart(canvas,{type:'bar',
    data:{labels:monthly.map(m=>m.month),datasets:[{data:monthly.map(m=>m.pnl),backgroundColor:monthly.map(m=>m.pnl>=0?'rgba(34,197,94,0.8)':'rgba(239,68,68,0.8)')}]},
    options:_jnlChartOpts(isDark)});
}

function _jnlRenderPsychTrend() {
  const canvas = document.getElementById('jnl-psych-canvas');
  if (!canvas) return;
  _jnlDestroyChart('jnl-psych-canvas');
  const isDark = !document.documentElement.getAttribute('data-theme');
  const withPsych = _jnlTrades.filter(t=>t.psych_confidence>0||t.psych_focus>0).slice(-20);
  if (!withPsych.length) return;
  const opts = _jnlChartOpts(isDark);
  opts.plugins = {legend:{labels:{color:isDark?'#94a3b8':'#64748b',font:{size:9}}}};
  opts.scales.y = {min:0,max:10,ticks:{color:isDark?'#64748b':'#94a3b8',font:{size:9}}};
  _jnlCharts['jnl-psych-canvas'] = new Chart(canvas,{type:'line',
    data:{labels:withPsych.map(t=>t.entry_date?.slice(0,10)),datasets:[
      {label:'Confidence',data:withPsych.map(t=>t.psych_confidence||0),borderColor:'#22c55e',tension:0.3,pointRadius:2,borderWidth:2},
      {label:'Stress',data:withPsych.map(t=>t.psych_stress||0),borderColor:'#ef4444',tension:0.3,pointRadius:2,borderWidth:2},
    ]},options:opts});
}

function _jnlRenderBreakdownTable(elId, data) {
  const el = document.getElementById(elId);
  if (!el) return;
  const entries = Object.entries(data);
  if (!entries.length) { el.innerHTML='<div style="color:var(--text3);font-size:11px;padding:4px">No data</div>'; return; }
  const gc = v => v>=0?'var(--green)':'var(--red)';
  el.innerHTML=`<table class="sm-table"><thead><tr><th class="sm-th">Name</th><th class="sm-th">Trades</th><th class="sm-th">Win%</th><th class="sm-th">Avg R</th></tr></thead><tbody>${
    entries.map(([n,d])=>`<tr><td class="sm-td">${n}</td><td class="sm-td">${d.trades}</td><td class="sm-td" style="color:${gc(d.win_rate-50)}">${d.win_rate}%</td><td class="sm-td" style="color:${gc(d.avg_r)}">${d.avg_r}R</td></tr>`).join('')
  }</tbody></table>`;
}

function _jnlRenderMistakes(mistakes) {
  const el = document.getElementById('jnl-mistake-content');
  if (!el) return;
  const entries = Object.entries(mistakes).sort((a,b)=>b[1]-a[1]);
  el.innerHTML = entries.length
    ? entries.map(([m,c])=>`<span class="jnl-mistake-tag">${m} (${c})</span>`).join(' ')
    : '<span style="color:var(--green);font-size:11px">No mistakes logged 🎉</span>';
}

async function _jnlLoadAICoach() {
  const el = document.getElementById('jnl-ai-content');
  if (!el) return;
  el.innerHTML = _skeletonCards(4, 'Analysing your journal…');
  try {
    const res  = await fetch(`${API}/api/journal/ai-insights${_jnlAcctParam()}`);
    const data = await res.json();
    const insights = data.insights || [];
    const header = `<div style="display:flex;align-items:center;gap:12px;margin-bottom:16px">
      <div style="font-size:28px">🤖</div>
      <div><div style="font-family:var(--font-mono);font-size:14px;font-weight:800;color:var(--text)">AI Coach</div>
      <div style="font-size:11px;color:var(--text3)">Pattern analysis from your last ${_jnlTrades.length} trades</div></div>
    </div>`;
    el.innerHTML = header + insights.map(i=>`<div class="jnl-ai-card ${i.type}">
      <div class="jnl-ai-icon">${i.icon}</div><div class="jnl-ai-text">${i.text}</div></div>`).join('');
  } catch (e) {
    el.innerHTML=`<div style="color:var(--red);padding:20px">Failed: ${e.message}</div>`;
  }
}

// ── Modal ─────────────────────────────────────────────────────────────────────
function jnlShowAddModal() {
  document.getElementById('jnl-edit-id').value='';
  const acctName = _jnlActiveAcct
    ? (_jnlAccounts.find(a=>a.id===_jnlActiveAcct)?.name || 'Account')
    : 'Account 1';
  document.getElementById('jnl-modal-title').textContent = `New Trade — ${acctName}`;
  ['jnl-ticker','jnl-entry-price','jnl-stop','jnl-target','jnl-qty','jnl-exit-price',
   'jnl-notes','jnl-fees','jnl-risk-amount','jnl-mae','jnl-mfe','jnl-broker','jnl-post-notes','jnl-strategy'
  ].forEach(id=>{ const el=document.getElementById(id); if(el) el.value=''; });
  document.getElementById('jnl-entry-date').value=new Date().toISOString().slice(0,10);
  ['jnl-exit-date','jnl-entry-time','jnl-exit-time'].forEach(id=>{ const el=document.getElementById(id); if(el) el.value=''; });
  ['jnl-direction','jnl-setup','jnl-timeframe','jnl-status','jnl-regime','jnl-emotion','jnl-discipline','jnl-review'].forEach(id=>{
    const el=document.getElementById(id); if(el) el.value=id==='jnl-direction'?'Long':id==='jnl-timeframe'?'Swing':id==='jnl-status'?'Open':'';
  });
  document.getElementById('jnl-market').value='Stocks';
  document.getElementById('jnl-grade').value='';
  document.querySelectorAll('.jnl-grade-btn').forEach(b=>b.classList.remove('active'));
  document.getElementById('jnl-plan-yes').checked=true;
  document.getElementById('jnl-repeat-yes').checked=true;
  ['confidence','focus','patience'].forEach(k=>{ const el=document.getElementById(`jnl-psych-${k}`); if(el){el.value=5;jnlUpdateSliderVal(el);} });
  ['energy','sleep'].forEach(k=>{ const el=document.getElementById(`jnl-psych-${k}`); if(el){el.value=7;jnlUpdateSliderVal(el);} });
  ['stress','fomo','revenge'].forEach(k=>{ const el=document.getElementById(`jnl-psych-${k}`); if(el){el.value=1;jnlUpdateSliderVal(el);} });
  jnlSwitchModalTab('trade',document.querySelector('.jnl-modal-tab'));
  document.getElementById('jnl-r-preview').textContent='';
  document.getElementById('jnl-modal').style.display='flex';
}

function jnlCloseModal() { document.getElementById('jnl-modal').style.display='none'; }

function jnlSwitchModalTab(tab,btn) {
  ['trade','psychology','review'].forEach(t=>{
    const el=document.getElementById(`jnl-tab-${t}`); if(el) el.style.display=t===tab?'':'none';
  });
  document.querySelectorAll('.jnl-modal-tab').forEach(b=>b.classList.remove('active'));
  if(btn) btn.classList.add('active');
}

function jnlSetGrade(grade,btn) {
  document.getElementById('jnl-grade').value=grade;
  document.querySelectorAll('.jnl-grade-btn').forEach(b=>b.classList.remove('active'));
  btn.classList.add('active');
}

function jnlUpdateSliderVal(el) {
  const v=document.getElementById(el.id+'-val'); if(v) v.textContent=el.value;
}

function jnlUpdateRPreview() {
  const entry=parseFloat(document.getElementById('jnl-entry-price')?.value)||0;
  const stop =parseFloat(document.getElementById('jnl-stop')?.value)||0;
  const target=parseFloat(document.getElementById('jnl-target')?.value)||0;
  const qty  =parseFloat(document.getElementById('jnl-qty')?.value)||0;
  const dir  =document.getElementById('jnl-direction')?.value||'Long';
  const prev =document.getElementById('jnl-r-preview');
  const riskEl=document.getElementById('jnl-risk-amount');
  if(!entry||!stop||entry===stop){if(prev)prev.textContent='';return;}
  const riskPt=Math.abs(entry-stop);
  const riskAmt=riskPt*qty;
  if(riskEl) riskEl.value=riskAmt.toFixed(2);
  let parts=[];
  if(target){const rp=dir==='Long'?(target-entry):(entry-target);parts.push(`R:R ${(rp/riskPt).toFixed(2)}`);}
  if(qty) parts.push(`Risk: ₹${riskAmt.toLocaleString('en-IN',{maximumFractionDigits:0})}`);
  if(prev){prev.textContent=parts.join(' · ');prev.style.color='var(--cyan)';}
}

async function jnlEditTrade(id) {
  const t=_jnlTrades.find(t=>t.id===id); if(!t) return;
  document.getElementById('jnl-edit-id').value=id;
  document.getElementById('jnl-modal-title').textContent='Edit Trade #'+id;
  const set=(id,v)=>{ const el=document.getElementById(id); if(el) el.value=v||''; };
  set('jnl-ticker',t.ticker);set('jnl-direction',t.direction||'Long');set('jnl-setup',t.setup_type);
  set('jnl-timeframe',t.timeframe||'Swing');set('jnl-market',t.market_type||'Stocks');set('jnl-broker',t.broker);
  set('jnl-entry-date',(t.entry_date||'').slice(0,10));set('jnl-entry-time',t.entry_time);
  set('jnl-entry-price',t.entry_price);set('jnl-stop',t.stop_loss);set('jnl-target',t.target);
  set('jnl-qty',t.quantity);set('jnl-fees',t.fees);
  set('jnl-exit-date',(t.exit_date||'').slice(0,10));set('jnl-exit-time',t.exit_time);
  set('jnl-exit-price',t.exit_price);set('jnl-status',t.status||'Open');set('jnl-regime',t.regime);
  set('jnl-emotion',t.pre_emotion);set('jnl-discipline',t.discipline_score);
  set('jnl-review',t.post_review);set('jnl-notes',t.notes);set('jnl-post-notes',t.post_notes);
  set('jnl-mae',t.mae);set('jnl-mfe',t.mfe);set('jnl-grade',t.trade_grade);
  document.querySelectorAll('.jnl-grade-btn').forEach(b=>b.classList.toggle('active',b.dataset.grade===t.trade_grade));
  document.getElementById(t.followed_plan===0?'jnl-plan-no':'jnl-plan-yes').checked=true;
  document.getElementById(t.would_repeat===0?'jnl-repeat-no':'jnl-repeat-yes').checked=true;
  ['confidence','focus','stress','patience','fomo','revenge','sleep','energy'].forEach(k=>{
    const el=document.getElementById(`jnl-psych-${k}`);
    if(el){el.value=t[`psych_${k}`]||(['stress','fomo','revenge'].includes(k)?1:5);jnlUpdateSliderVal(el);}
  });
  jnlSwitchModalTab('trade',document.querySelector('.jnl-modal-tab'));
  jnlUpdateRPreview();
  document.getElementById('jnl-modal').style.display='flex';
}

async function jnlSaveTrade() {
  const editId=document.getElementById('jnl-edit-id').value;
  const g=(id)=>document.getElementById(id);
  const trade={
    account_id:  _jnlActiveAcct || 1,
    ticker:      g('jnl-ticker').value.trim().toUpperCase(),
    direction:   g('jnl-direction').value, setup_type: g('jnl-setup').value,
    timeframe:   g('jnl-timeframe').value, market_type:g('jnl-market').value,
    broker:      g('jnl-broker').value,    entry_date: g('jnl-entry-date').value,
    entry_time:  g('jnl-entry-time').value,entry_price:parseFloat(g('jnl-entry-price').value)||0,
    stop_loss:   parseFloat(g('jnl-stop').value)||null, target:parseFloat(g('jnl-target').value)||null,
    quantity:    parseFloat(g('jnl-qty').value)||0,     fees:parseFloat(g('jnl-fees').value)||0,
    risk_amount: parseFloat(g('jnl-risk-amount').value)||0,
    exit_date:   g('jnl-exit-date').value||null, exit_time:g('jnl-exit-time').value,
    exit_price:  parseFloat(g('jnl-exit-price').value)||null, status:g('jnl-status').value,
    regime:      g('jnl-regime').value,    pre_emotion:g('jnl-emotion').value,
    discipline_score:parseInt(g('jnl-discipline').value)||0,
    post_review: g('jnl-review').value,    notes:g('jnl-notes').value,
    post_notes:  g('jnl-post-notes').value,trade_grade:g('jnl-grade').value,
    followed_plan:document.querySelector('input[name="jnl-plan"]:checked')?.value==='1'?1:0,
    would_repeat: document.querySelector('input[name="jnl-repeat"]:checked')?.value==='1'?1:0,
    mae:parseFloat(g('jnl-mae').value)||0, mfe:parseFloat(g('jnl-mfe').value)||0,
    psych_confidence:parseInt(g('jnl-psych-confidence').value)||0,
    psych_focus:     parseInt(g('jnl-psych-focus').value)||0,
    psych_stress:    parseInt(g('jnl-psych-stress').value)||0,
    psych_patience:  parseInt(g('jnl-psych-patience').value)||0,
    psych_fomo:      parseInt(g('jnl-psych-fomo').value)||0,
    psych_revenge:   parseInt(g('jnl-psych-revenge').value)||0,
    psych_sleep:     parseInt(g('jnl-psych-sleep').value)||0,
    psych_energy:    parseInt(g('jnl-psych-energy').value)||0,
  };
  if(!trade.ticker||!trade.entry_price){alert('Ticker and Entry Price are required');return;}
  try {
    if(editId){
      await fetch(`${API}/api/journal/trades/${editId}`,{method:'PUT',headers:{'Content-Type':'application/json'},body:JSON.stringify(trade)});
    } else {
      await fetch(`${API}/api/journal/trades`,{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify(trade)});
    }
    jnlCloseModal(); jnlLoadTrades();
  } catch(e){alert('Save failed: '+e.message);}
}

function jnlCloseTrade(id) {
  // Find the trade to pre-fill stop as default exit
  const trade = _jnlTrades.find(t => t.id === id);
  const stopSuggestion = trade?.stop_loss ? ` (stop: ${trade.stop_loss})` : '';
  const entrySuggestion = trade ? ` Entry: ₹${trade.entry_price}` : '';

  // Remove any existing close dialog
  const existing = document.getElementById('jnl-close-dialog');
  if (existing) existing.remove();

  const dialog = document.createElement('div');
  dialog.id = 'jnl-close-dialog';
  dialog.style.cssText = 'position:fixed;inset:0;z-index:10000;background:rgba(0,0,0,.7);backdrop-filter:blur(4px);display:flex;align-items:center;justify-content:center';
  dialog.innerHTML = `
    <div style="background:var(--card-bg,#0f1628);border:1px solid var(--card-border,#1e293b);
      border-radius:14px;padding:24px 28px;min-width:320px;max-width:400px;width:90%;
      font-family:var(--font-mono,'JetBrains Mono',monospace);box-shadow:0 8px 40px rgba(0,0,0,.4)">

      <div style="font-size:13px;font-weight:800;color:var(--text,#e2e8f0);margin-bottom:4px">
        Close Trade — <span style="color:var(--cyan)">${trade?.ticker || '#'+id}</span>
      </div>
      <div style="font-size:10px;color:var(--text3);margin-bottom:16px">${entrySuggestion}${stopSuggestion}</div>

      <div style="margin-bottom:12px">
        <label style="font-size:9px;font-weight:700;letter-spacing:.08em;color:var(--text3);display:block;margin-bottom:4px">EXIT PRICE ₹</label>
        <input id="jnl-close-price" type="number" step="0.01"
          placeholder="Enter exit price"
          style="width:100%;padding:10px 12px;border:1.5px solid var(--card-border,#1e293b);border-radius:8px;
          background:var(--input-bg,rgba(255,255,255,.06));color:var(--text,#e2e8f0);
          font-family:var(--font-mono);font-size:13px;outline:none;box-sizing:border-box"
          onkeydown="if(event.key==='Enter')jnlConfirmClose(${id})"
          oninput="jnlPreviewClose(${id},this.value)">
        <div id="jnl-close-preview" style="margin-top:6px;font-size:10px;color:var(--text3);min-height:16px"></div>
      </div>

      <div style="margin-bottom:16px">
        <label style="font-size:9px;font-weight:700;letter-spacing:.08em;color:var(--text3);display:block;margin-bottom:6px">OUTCOME</label>
        <div style="display:flex;gap:8px">
          <label style="display:flex;align-items:center;gap:5px;cursor:pointer;font-size:11px;color:var(--text2)">
            <input type="radio" name="jnl-close-status" value="Closed" checked> Closed (target/manual)
          </label>
          <label style="display:flex;align-items:center;gap:5px;cursor:pointer;font-size:11px;color:var(--text2)">
            <input type="radio" name="jnl-close-status" value="StoppedOut"> Stopped Out
          </label>
        </div>
      </div>

      <div style="display:flex;gap:8px;justify-content:flex-end">
        <button onclick="document.getElementById('jnl-close-dialog').remove()"
          style="padding:8px 18px;border-radius:8px;border:1px solid var(--card-border,#1e293b);
          background:transparent;color:var(--text3);font-family:var(--font-mono);font-size:11px;cursor:pointer">
          Cancel
        </button>
        <button id="jnl-confirm-close-btn" onclick="jnlConfirmClose(${id})"
          style="padding:8px 20px;border-radius:8px;border:none;background:var(--cyan,#06b6d4);
          color:#0a0e17;font-family:var(--font-mono);font-size:11px;font-weight:700;cursor:pointer">
          Close Trade ✓
        </button>
      </div>
    </div>`;

  document.body.appendChild(dialog);
  // Auto-focus price input
  setTimeout(() => {
    const inp = document.getElementById('jnl-close-price');
    if (inp) inp.focus();
    // Auto-fill stop loss as default if stopped out
    if (trade?.stop_loss) inp.value = trade.stop_loss;
    jnlPreviewClose(id, inp?.value || '');
  }, 50);
}

function jnlPreviewClose(id, priceVal) {
  const trade   = _jnlTrades.find(t => t.id === id);
  const preview = document.getElementById('jnl-close-preview');
  if (!trade || !preview) return;

  const price = parseFloat(priceVal);
  if (!price || isNaN(price)) { preview.textContent = ''; return; }

  const entry = trade.entry_price || 0;
  const qty   = trade.quantity   || 0;
  const stop  = trade.stop_loss  || 0;
  const dir   = trade.direction  || 'Long';

  const pnlPct = entry > 0
    ? (dir === 'Long' ? (price - entry) / entry * 100 : (entry - price) / entry * 100)
    : 0;
  const pnlAmt = pnlPct / 100 * entry * qty;
  let rMult = '';
  if (stop && entry && Math.abs(entry - stop) > 0) {
    const risk   = Math.abs(entry - stop);
    const reward = dir === 'Long' ? (price - entry) : (entry - price);
    rMult = ` · ${(reward / risk).toFixed(2)}R`;
  }

  const color = pnlPct >= 0 ? '#22c55e' : '#ef4444';
  preview.innerHTML = `<span style="color:${color};font-weight:700">${pnlPct >= 0 ? '+' : ''}${pnlPct.toFixed(2)}%</span>`
    + (qty ? ` <span style="color:${color}">₹${Math.round(pnlAmt).toLocaleString('en-IN')}</span>` : '')
    + `<span style="color:var(--text3)">${rMult}</span>`;
}

async function jnlConfirmClose(id) {
  const priceEl = document.getElementById('jnl-close-price');
  const price   = parseFloat(priceEl?.value);
  const status  = document.querySelector('input[name="jnl-close-status"]:checked')?.value || 'Closed';

  if (!price || isNaN(price) || price <= 0) {
    priceEl?.style && (priceEl.style.borderColor = 'var(--red)');
    priceEl?.focus();
    return;
  }

  const btn = document.getElementById('jnl-confirm-close-btn');
  if (btn) { btn.textContent = 'Saving…'; btn.disabled = true; }

  try {
    const res = await fetch(`${API}/api/journal/trades/${id}`, {
      method: 'PUT',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        exit_price: price,
        exit_date:  new Date().toISOString().slice(0, 10),
        status:     status,
      }),
    });
    const data = await res.json();
    if (data.error) throw new Error(data.error);
    document.getElementById('jnl-close-dialog')?.remove();
    await jnlLoadTrades();
  } catch (e) {
    alert('Failed to close trade: ' + e.message);
    if (btn) { btn.textContent = 'Close Trade ✓'; btn.disabled = false; }
  }
}

async function jnlDeleteTrade(id) {
  if(!confirm('Delete this trade?')) return;
  await fetch(`${API}/api/journal/trades/${id}`,{method:'DELETE'});
  jnlLoadTrades();
}

function jnlExportCSV() {
  const headers=['Ticker','Direction','Setup','Timeframe','Market','Broker','Regime',
    'EntryDate','EntryTime','EntryPrice','Stop','Target','Qty','Fees','RiskAmt',
    'ExitDate','ExitTime','ExitPrice','Status','PnL%','PnLAmt','R','HoldDays',
    'Conf','Focus','Stress','Patience','FOMO','Revenge','Sleep','Energy',
    'Emotion','Discipline','FollowedPlan','Grade','WouldRepeat','Mistake','MAE','MFE','Notes'];
  const rows=[headers.join(',')];
  _jnlTrades.forEach(t=>{rows.push([
    t.ticker,t.direction,t.setup_type,t.timeframe,t.market_type,t.broker,t.regime,
    t.entry_date,t.entry_time,t.entry_price,t.stop_loss,t.target,t.quantity,t.fees,t.risk_amount,
    t.exit_date,t.exit_time,t.exit_price,t.status,t.pnl_pct,t.pnl_amount,t.r_multiple,t.holding_days,
    t.psych_confidence,t.psych_focus,t.psych_stress,t.psych_patience,t.psych_fomo,t.psych_revenge,t.psych_sleep,t.psych_energy,
    t.pre_emotion,t.discipline_score,t.followed_plan,t.trade_grade,t.would_repeat,
    t.post_review,t.mae,t.mfe,`"${(t.notes||'').replace(/"/g,'""')}"`
  ].join(','));});
  const blob=new Blob([rows.join('\n')],{type:'text/csv'});
  const a=document.createElement('a');a.href=URL.createObjectURL(blob);
  a.download=`trade_journal_${new Date().toISOString().slice(0,10)}.csv`;a.click();
}

// ── Settings view ─────────────────────────────────────────────────────────────
async function jnlLoadSettings() {
  try {
    const res  = await fetch(`${API}/api/journal/settings`);
    const data = await res.json();
    const s = (id, v) => { const el = document.getElementById(id); if (el && v != null) el.value = v; };
    s('jnl-setting-capital',    data.starting_capital);
    s('jnl-setting-risk-pct',   data.max_risk_per_trade);
    s('jnl-setting-daily-loss', data.max_daily_loss_pct);
    s('jnl-setting-weekly-dd',  data.max_weekly_drawdown_pct);
    s('jnl-setting-max-trades', data.max_trades_per_day);
    s('jnl-setting-max-losses', data.max_consecutive_losses);
    // Prefill calculator with capital
    const calcCap = document.getElementById('jnl-calc-capital');
    if (calcCap && !calcCap.value) calcCap.value = data.starting_capital;
  } catch {}
}

async function jnlSaveSettings() {
  const g = id => parseFloat(document.getElementById(id)?.value) || 0;
  const settings = {
    starting_capital:          g('jnl-setting-capital')    || 1000000,
    max_risk_per_trade:        g('jnl-setting-risk-pct')   || 1.0,
    max_daily_loss_pct:        g('jnl-setting-daily-loss') || 2.0,
    max_weekly_drawdown_pct:   g('jnl-setting-weekly-dd')  || 5.0,
    max_trades_per_day:        parseInt(document.getElementById('jnl-setting-max-trades')?.value) || 5,
    max_consecutive_losses:    parseInt(document.getElementById('jnl-setting-max-losses')?.value) || 3,
  };
  try {
    await fetch(`${API}/api/journal/settings`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify(settings)
    });
    // Flash confirmation
    const btn = document.querySelector('[onclick="jnlSaveSettings()"]');
    if (btn) { const orig = btn.textContent; btn.textContent = '✅ Saved!'; btn.style.background = 'var(--green)';
      setTimeout(() => { btn.textContent = orig; btn.style.background = ''; }, 1500); }
    // Re-run risk check with new settings
    _jnlLoadRiskCheck();
  } catch (e) { alert('Save failed: ' + e.message); }
}

// ── Risk Rules Engine ─────────────────────────────────────────────────────────
async function _jnlLoadRiskCheck() {
  try {
    const res  = await fetch(`${API}/api/journal/risk-check${_jnlAcctParam()}`);
    const data = await res.json();
    _jnlRenderRiskAlert(data);
  } catch {}
}

function _jnlRenderRiskAlert(d) {
  const banner  = document.getElementById('jnl-risk-alert');
  const content = document.getElementById('jnl-risk-alert-content');
  const lock    = document.getElementById('jnl-lock-banner');
  if (!banner || !content) return;

  if (!d.alerts || d.alerts.length === 0) {
    banner.style.display = 'none';
    return;
  }

  banner.style.display = '';
  content.innerHTML = d.alerts.map(a => `
    <div style="display:flex;gap:8px;align-items:start;margin-bottom:6px;font-family:var(--font-mono);font-size:10px">
      <span>${a.icon}</span>
      <span><b style="color:var(--text)">${a.rule}:</b> <span style="color:var(--text2)">${a.msg}</span></span>
    </div>`).join('');

  if (lock) lock.style.display = d.lock_trading ? '' : 'none';
}

// ── Position Sizing Calculator ────────────────────────────────────────────────
function jnlCalcPosition() {
  const capital  = parseFloat(document.getElementById('jnl-calc-capital')?.value)   || 0;
  const riskPct  = parseFloat(document.getElementById('jnl-calc-risk-pct')?.value)  || 0;
  const entry    = parseFloat(document.getElementById('jnl-calc-entry')?.value)     || 0;
  const stop     = parseFloat(document.getElementById('jnl-calc-stop')?.value)      || 0;
  const result   = document.getElementById('jnl-calc-result');
  if (!result) return;

  if (!capital || !riskPct || !entry || !stop || entry === stop) {
    result.innerHTML = '<span style="color:var(--text3)">Enter all values above to calculate</span>';
    return;
  }

  const riskAmt   = capital * riskPct / 100;
  const riskPtRs  = Math.abs(entry - stop);
  const qty       = Math.floor(riskAmt / riskPtRs);
  const posSize   = qty * entry;
  const posPct    = (posSize / capital * 100).toFixed(1);
  const color     = qty > 0 ? 'var(--cyan)' : 'var(--red)';

  result.innerHTML = `
    <div style="color:${color}"><b style="font-size:16px">${qty.toLocaleString('en-IN')}</b> shares</div>
    <div style="color:var(--text3)">Position Size: <b style="color:var(--text)">₹${posSize.toLocaleString('en-IN', {maximumFractionDigits:0})}</b> (${posPct}% of capital)</div>
    <div style="color:var(--text3)">Risk Amount: <b style="color:var(--red)">₹${riskAmt.toLocaleString('en-IN', {maximumFractionDigits:0})}</b> (${riskPct}% of ₹${capital.toLocaleString('en-IN')})</div>
    <div style="color:var(--text3)">Risk per Share: <b style="color:var(--text)">₹${riskPtRs.toFixed(2)}</b></div>`;
}

// ── Gamification Badges ───────────────────────────────────────────────────────
async function _jnlLoadGamification() {
  try {
    const res  = await fetch(`${API}/api/journal/gamification${_jnlAcctParam()}`);
    const data = await res.json();
    _jnlRenderBadges(data);
  } catch {}
}

function _jnlRenderBadges(data) {
  const el = document.getElementById('jnl-badges-content');
  if (!el) return;
  const badges = data.badges || [];
  if (!badges.length) {
    el.innerHTML = '<span style="color:var(--text3);font-size:11px;font-family:var(--font-mono)">Keep trading consistently to earn badges!</span>';
    return;
  }
  el.innerHTML = badges.map(b => `
    <div style="display:inline-flex;align-items:center;gap:6px;padding:6px 14px;border-radius:20px;
      border:1px solid ${b.color}44;background:${b.color}11;font-family:var(--font-mono);font-size:10px;font-weight:700;color:${b.color}">
      ${b.icon} ${b.label}
    </div>`).join('');
}

// ── Time-of-day + Day-of-week charts ──────────────────────────────────────────
function _jnlRenderTOD(hours) {
  const canvas = document.getElementById('jnl-tod-canvas');
  if (!canvas || !hours.length) return;
  _jnlDestroyChart('jnl-tod-canvas');
  const isDark = !document.documentElement.getAttribute('data-theme');
  const colors = hours.map(h => h.win_rate >= 60 ? 'rgba(34,197,94,0.8)' : h.win_rate >= 40 ? 'rgba(245,158,11,0.8)' : 'rgba(239,68,68,0.8)');
  _jnlCharts['jnl-tod-canvas'] = new Chart(canvas, {
    type: 'bar',
    data: { labels: hours.map(h => h.label), datasets: [{
      data: hours.map(h => h.win_rate), backgroundColor: colors,
      label: 'Win Rate %',
    }]},
    options: { ..._jnlChartOpts(isDark),
      plugins: { legend: { display: false },
        tooltip: { callbacks: { label: ctx => `Win: ${ctx.raw}% (${hours[ctx.dataIndex].trades} trades)` } } }
    }
  });
}

function _jnlRenderDOW(days) {
  const canvas = document.getElementById('jnl-dow-canvas');
  if (!canvas || !days.length) return;
  _jnlDestroyChart('jnl-dow-canvas');
  const isDark = !document.documentElement.getAttribute('data-theme');
  const colors = days.map(d => d.total_pnl >= 0 ? 'rgba(34,197,94,0.8)' : 'rgba(239,68,68,0.8)');
  _jnlCharts['jnl-dow-canvas'] = new Chart(canvas, {
    type: 'bar',
    data: { labels: days.map(d => d.day), datasets: [{
      data: days.map(d => d.win_rate), backgroundColor: colors, label: 'Win Rate %',
    }]},
    options: { ..._jnlChartOpts(isDark),
      plugins: { legend: { display: false },
        tooltip: { callbacks: { label: ctx => `Win: ${ctx.raw}% (${days[ctx.dataIndex].trades} trades)` } } }
    }
  });
}

// ── CSV Import ────────────────────────────────────────────────────────────────
async function jnlImportCSV() {
  const csv    = document.getElementById('jnl-import-csv')?.value.trim();
  const broker = document.getElementById('jnl-import-broker')?.value || 'generic';
  const result = document.getElementById('jnl-import-result');
  if (!csv) { if(result) result.innerHTML='<span style="color:var(--red)">Paste CSV content first</span>'; return; }
  if (result) result.innerHTML = '⏳ Importing…';
  try {
    const res  = await fetch(`${API}/api/journal/import-csv`, {
      method: 'POST', headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ content: csv, broker }),
    });
    const data = await res.json();
    if (result) {
      result.innerHTML = data.error
        ? `<span style="color:var(--red)">Error: ${data.error}</span>`
        : `<span style="color:var(--green)">✅ Imported ${data.imported} trades${data.errors ? ` (${data.errors} skipped)` : ''}</span>`;
    }
    if (data.imported > 0) {
      document.getElementById('jnl-import-csv').value = '';
      await jnlLoadTrades();
    }
  } catch(e) { if(result) result.innerHTML=`<span style="color:var(--red)">Failed: ${e.message}</span>`; }
}

// ════════════════════════════════════════════════════════════════════════════
// ACCOUNT MANAGEMENT
// ════════════════════════════════════════════════════════════════════════════

/** Query-string helper — appends ?account_id=N or '' for All Accounts */
function _jnlAcctParam() {
  return _jnlActiveAcct ? `?account_id=${_jnlActiveAcct}` : '';
}

/** Load accounts from API and render the strip. Called once on tab init. */
async function jnlInitAccounts() {
  try {
    const res  = await fetch(`${API}/api/journal/accounts`);
    const data = await res.json();
    _jnlAccounts = data.accounts || [];
    _jnlRenderAccountStrip();
    // Default to Account 1 on first load
    if (_jnlActiveAcct === null && _jnlAccounts.length > 0) {
      jnlSelectAccount(_jnlAccounts[0].id, false);
    }
  } catch (e) { console.warn('Account load failed:', e); }
}

/** Render the account tab strip */
function _jnlRenderAccountStrip() {
  const container = document.getElementById('jnl-account-tabs');
  if (!container) return;

  const allTab = `
    <button class="jnl-acct-tab all-tab ${_jnlActiveAcct === null ? 'active' : ''}"
      onclick="jnlSelectAccount(null)"
      style="${_jnlActiveAcct === null ? '--acct-color:#94a3b8;background:#94a3b820;border-color:#94a3b8;color:#0a0e17' : ''}">
      <span class="jnl-acct-dot" style="background:#94a3b8"></span>
      All Accounts
    </button>`;

  const accountTabs = _jnlAccounts.map(a => {
    const isActive = _jnlActiveAcct === a.id;
    return `
      <button class="jnl-acct-tab ${isActive ? 'active' : ''}"
        onclick="jnlSelectAccount(${a.id})"
        style="--acct-color:${a.color}">
        <span class="jnl-acct-dot" style="background:${a.color}"></span>
        ${a.name}
        <span class="jnl-acct-edit" onclick="event.stopPropagation();jnlShowEditAccountModal(${a.id})">✏</span>
      </button>`;
  }).join('');

  container.innerHTML = allTab + accountTabs;
}

/** Switch active account and reload all data */
function jnlSelectAccount(accountId, reload = true) {
  _jnlActiveAcct = accountId;
  _jnlRenderAccountStrip();

  // Update badge in header
  const badge = document.getElementById('jnl-active-account-badge');
  if (badge) {
    if (accountId === null) {
      badge.textContent = 'All Accounts';
      badge.style.background = 'rgba(148,163,184,.12)';
      badge.style.color = '#94a3b8';
    } else {
      const acct = _jnlAccounts.find(a => a.id === accountId);
      if (acct) {
        badge.textContent = acct.name;
        badge.style.background = acct.color + '22';
        badge.style.color = acct.color;
      }
    }
  }

  if (reload) jnlLoadTrades();
}

// ── All Accounts summary view ─────────────────────────────────────────────────
async function _jnlRenderAllAccountsSummary() {
  const wrap = document.getElementById('jnl-table-wrap');
  if (!wrap) return;
  try {
    const res  = await fetch(`${API}/api/journal/accounts/summary`);
    const data = await res.json();
    const accounts = data.accounts || [];

    if (!accounts.length) {
      wrap.innerHTML = '<div style="text-align:center;padding:40px;color:var(--text3);font-family:var(--font-mono)">No accounts yet. Click <b>＋ New Account</b> to create one.</div>';
      return;
    }

    const cards = accounts.map(a => {
      const pnlColor = a.total_pnl >= 0 ? 'var(--green)' : 'var(--red)';
      const cur = a.currency === 'USD' ? '$' : '₹';
      return `
        <div class="jnl-acct-summary-card" style="--acct-color:${a.color}"
          onclick="jnlSelectAccount(${a.id})">
          <div style="display:flex;align-items:center;gap:8px;margin-bottom:10px">
            <div style="width:10px;height:10px;border-radius:50%;background:${a.color};flex-shrink:0"></div>
            <div style="font-family:var(--font-mono);font-size:13px;font-weight:800;color:var(--text)">${a.name}</div>
          </div>
          ${a.broker ? `<div style="font-size:10px;color:var(--text3);font-family:var(--font-mono);margin-bottom:8px">${a.broker}</div>` : ''}
          <div class="sc-fund-grid">
            <div class="sc-fund-row">
              <span class="sc-fund-label">Trades</span>
              <span class="sc-fund-value">${a.total_trades}</span>
            </div>
            <div class="sc-fund-row">
              <span class="sc-fund-label">Open</span>
              <span class="sc-fund-value" style="color:var(--cyan)">${a.open_trades}</span>
            </div>
            <div class="sc-fund-row">
              <span class="sc-fund-label">Win Rate</span>
              <span class="sc-fund-value" style="color:${a.win_rate>=50?'var(--green)':'var(--red)'}">${a.win_rate}%</span>
            </div>
            <div class="sc-fund-row">
              <span class="sc-fund-label">Realized P&L</span>
              <span class="sc-fund-value" style="color:${pnlColor}">${cur}${Math.round(a.total_pnl).toLocaleString('en-IN')}</span>
            </div>
          </div>
          <div style="margin-top:10px;font-size:9px;color:var(--cyan);font-family:var(--font-mono);font-weight:700">
            Click to open →
          </div>
        </div>`;
    }).join('');

    // Total combined P&L
    const totalPnl = accounts.reduce((s, a) => s + a.total_pnl, 0);
    const totalTrades = accounts.reduce((s, a) => s + a.total_trades, 0);
    const totalOpen   = accounts.reduce((s, a) => s + a.open_trades, 0);

    wrap.innerHTML = `
      <div style="padding:12px 0 6px">
        <div style="font-family:var(--font-mono);font-size:11px;color:var(--text3);margin-bottom:12px">
          Combined — ${totalTrades} trades &nbsp;·&nbsp; ${totalOpen} open
          &nbsp;·&nbsp; <b style="color:${totalPnl>=0?'var(--green)':'var(--red)'}">
          ₹${Math.round(totalPnl).toLocaleString('en-IN')} total P&L</b>
        </div>
        <div class="jnl-acct-summary-grid">${cards}</div>
      </div>`;
  } catch (e) {
    wrap.innerHTML = `<div style="color:var(--red);padding:20px">Failed to load summary: ${e.message}</div>`;
  }
}

// ── Account Modal ─────────────────────────────────────────────────────────────
function jnlShowNewAccountModal() {
  document.getElementById('jnl-acct-edit-id').value = '';
  document.getElementById('jnl-account-modal-title').textContent = 'New Account';
  document.getElementById('jnl-acct-name').value    = '';
  document.getElementById('jnl-acct-broker').value  = '';
  document.getElementById('jnl-acct-capital').value = '1000000';
  document.getElementById('jnl-acct-currency').value= 'INR';
  document.getElementById('jnl-acct-notes').value   = '';
  document.getElementById('jnl-acct-color').value   = '#06b6d4';
  document.getElementById('jnl-acct-delete-btn').style.display = 'none';
  // Reset swatches
  document.querySelectorAll('.jnl-color-swatch').forEach(s => {
    s.classList.toggle('active', s.dataset.color === '#06b6d4');
  });
  document.getElementById('jnl-account-modal').style.display = 'flex';
  setTimeout(() => document.getElementById('jnl-acct-name').focus(), 60);
}

function jnlShowEditAccountModal(id) {
  const acct = _jnlAccounts.find(a => a.id === id);
  if (!acct) return;
  document.getElementById('jnl-acct-edit-id').value   = id;
  document.getElementById('jnl-account-modal-title').textContent = `Edit — ${acct.name}`;
  document.getElementById('jnl-acct-name').value    = acct.name    || '';
  document.getElementById('jnl-acct-broker').value  = acct.broker  || '';
  document.getElementById('jnl-acct-capital').value = acct.starting_capital || 1000000;
  document.getElementById('jnl-acct-currency').value= acct.currency || 'INR';
  document.getElementById('jnl-acct-notes').value   = acct.notes   || '';
  document.getElementById('jnl-acct-color').value   = acct.color   || '#06b6d4';
  // Show delete btn (but not for Account 1)
  document.getElementById('jnl-acct-delete-btn').style.display = id === 1 ? 'none' : '';
  // Set swatch
  document.querySelectorAll('.jnl-color-swatch').forEach(s => {
    s.classList.toggle('active', s.dataset.color === acct.color);
  });
  document.getElementById('jnl-account-modal').style.display = 'flex';
}

function jnlCloseAccountModal() {
  document.getElementById('jnl-account-modal').style.display = 'none';
}

function jnlPickColor(el) {
  document.querySelectorAll('.jnl-color-swatch').forEach(s => s.classList.remove('active'));
  el.classList.add('active');
  document.getElementById('jnl-acct-color').value = el.dataset.color;
}

async function jnlSaveAccount() {
  const editId  = document.getElementById('jnl-acct-edit-id').value;
  const payload = {
    name:             document.getElementById('jnl-acct-name').value.trim(),
    broker:           document.getElementById('jnl-acct-broker').value.trim(),
    starting_capital: parseFloat(document.getElementById('jnl-acct-capital').value) || 1000000,
    currency:         document.getElementById('jnl-acct-currency').value,
    notes:            document.getElementById('jnl-acct-notes').value.trim(),
    color:            document.getElementById('jnl-acct-color').value,
  };
  if (!payload.name) {
    document.getElementById('jnl-acct-name').style.borderColor = 'var(--red)';
    document.getElementById('jnl-acct-name').focus();
    return;
  }
  try {
    let res;
    if (editId) {
      res = await fetch(`${API}/api/journal/accounts/${editId}`, {
        method: 'PUT', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
    } else {
      res = await fetch(`${API}/api/journal/accounts`, {
        method: 'POST', headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload),
      });
    }
    const data = await res.json();
    if (data.error) throw new Error(data.error);
    jnlCloseAccountModal();
    await jnlInitAccounts();             // refresh strip
    if (!editId && data.id) {
      jnlSelectAccount(data.id);         // auto-switch to new account
    } else {
      jnlLoadTrades();
    }
  } catch (e) { alert('Save failed: ' + e.message); }
}

async function jnlDeleteAccount() {
  const editId = document.getElementById('jnl-acct-edit-id').value;
  const acct   = _jnlAccounts.find(a => a.id === parseInt(editId));
  if (!acct) return;
  if (!confirm(`Delete account "${acct.name}"?\n\nOnly possible if it has 0 trades.`)) return;
  try {
    const res  = await fetch(`${API}/api/journal/accounts/${editId}`, { method: 'DELETE' });
    const data = await res.json();
    if (data.error) { alert(data.error); return; }
    jnlCloseAccountModal();
    if (_jnlActiveAcct === parseInt(editId)) _jnlActiveAcct = null;
    await jnlInitAccounts();
    jnlLoadTrades();
  } catch (e) { alert('Delete failed: ' + e.message); }
}
