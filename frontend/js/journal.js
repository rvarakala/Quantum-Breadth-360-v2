// ════════════════════════════════════════════════════════════════════════════
// TRADING JOURNAL — Enhanced (Sprint 5)
// Tilt Meter™, Psychology sliders, Equity/Drawdown/Monthly charts, AI Coach
// ════════════════════════════════════════════════════════════════════════════

let _jnlTrades    = [];
let _jnlAnalytics = null;
let _jnlCharts    = {};
let _jnlView      = 'table';

async function jnlLoadTrades() {
  const status = document.getElementById('jnl-filter-status')?.value || 'all';
  try {
    const res = await fetch(`${API}/api/journal/trades?status=${status}`);
    _jnlTrades = await res.json();
    _jnlRenderStats();
    _jnlRenderTable();
    _jnlLoadTilt();
    if (_jnlView === 'analytics') _jnlLoadAnalytics();
    if (_jnlView === 'ai')        _jnlLoadAICoach();
  } catch (e) { console.error('Journal load error:', e); }
}

function jnlToggleView(view) {
  _jnlView = view;
  ['table','analytics','ai'].forEach(v => {
    const p = document.getElementById(`jnl-view-${v}-panel`);
    if (p) p.style.display = v === view ? '' : 'none';
    const b = document.getElementById(`jnl-view-${v}`);
    if (b) b.classList.toggle('active', v === view);
  });
  if (view === 'analytics') _jnlLoadAnalytics();
  if (view === 'ai')        _jnlLoadAICoach();
}

async function _jnlLoadTilt() {
  try {
    const res  = await fetch(`${API}/api/journal/tilt`);
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
    const planIcon   = t.followed_plan===0 ? '❌' : t.followed_plan===1 ? '✅' : '';
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
        ${isOpen?`<button class="sm-export-btn" onclick="jnlCloseTrade(${t.id})">✅</button>`:''}
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
      fetch(`${API}/api/journal/analytics`),
      fetch(`${API}/api/journal/drawdown`),
      fetch(`${API}/api/journal/monthly`),
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
  return { responsive:true, maintainAspectRatio:false, plugins:{legend:{display:false}},
    scales:{ x:{ticks:{color:isDark?'#64748b':'#94a3b8',maxTicksLimit:8,font:{size:9}}},
             y:{ticks:{color:isDark?'#64748b':'#94a3b8',font:{size:9}}} } };
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
    const res  = await fetch(`${API}/api/journal/ai-insights`);
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
  document.getElementById('jnl-modal-title').textContent='New Trade';
  ['jnl-ticker','jnl-entry-price','jnl-stop','jnl-target','jnl-qty','jnl-exit-price',
   'jnl-notes','jnl-fees','jnl-risk-amount','jnl-mae','jnl-mfe','jnl-broker','jnl-post-notes'
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

async function jnlCloseTrade(id) {
  const price=prompt('Enter exit price:'); if(!price) return;
  await fetch(`${API}/api/journal/trades/${id}`,{method:'PUT',headers:{'Content-Type':'application/json'},
    body:JSON.stringify({exit_price:parseFloat(price),exit_date:new Date().toISOString().slice(0,10),status:'Closed'})});
  jnlLoadTrades();
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
