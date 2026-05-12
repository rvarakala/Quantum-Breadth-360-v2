/**
 * Breadth Charts Drill-In Modal
 * ────────────────────────────────────────────────────────────────────────────
 * Click any .breadth-detail-card → opens a modal showing:
 *   - 90-day chart (or status-specific layout)
 *   - Deterministic stats panel
 *   - Groq AI analysis (per-day cached on backend)
 *
 * Single endpoint: /api/breadth/chart-detail?card_id=<id>&market=INDIA
 * Two layout variants:
 *   • time-series cards (6) — chart at top, stats below
 *   • status cards (2)      — variant: regime_timeline strip, score_gauge bars
 */
(function () {
  'use strict';

  const ENDPOINT = '/api/breadth/chart-detail';

  // Card metadata: title + chart variant
  const CARD_META = {
    ad_line:          { title: 'A-D LINE (Cumulative)',        variant: 'line',   color: '#3b82f6' },
    pct_above_50:     { title: '% Above 50 DMA Trend',         variant: 'line',   color: '#a855f7' },
    nh_nl:            { title: 'New High vs New Low',          variant: 'bar',    color: '#22c55e' },
    qbram_score:      { title: 'Q-BRAM Score History',         variant: 'line',   color: '#a855f7' },
    iv_footprint:     { title: 'IV Footprint — Smart Money',   variant: 'stack',  color: '#22c55e' },
    liquidity_stress: { title: 'Liquidity Stress Score',       variant: 'line',   color: '#ef4444' },
    regime_timeline:  { title: 'Regime Timeline (90 days)',    variant: 'regime', color: '#a855f7' },
    score_gauge:      { title: 'Breadth Score — Components',   variant: 'gauge',  color: '#22c55e' },
  };

  // Regime → color mapping (mirrors backend palette)
  const REGIME_COLOR = {
    PANIC:        '#ef4444',
    DISTRIBUTION: '#f97316',
    TRANSITION:   '#eab308',
    ACCUMULATION: '#22c55e',
    EXPANSION:    '#16a34a',
  };

  let _activeChart = null;
  let _isOpen = false;

  function _esc(s) {
    return (s == null ? '' : String(s)).replace(/[&<>"']/g, c => ({
      '&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'
    })[c]);
  }

  function _fmt(v, digits) {
    if (v == null || v === '') return '—';
    if (typeof v === 'number') return v.toFixed(digits == null ? 1 : digits);
    return String(v);
  }

  function _$(id) { return document.getElementById(id); }

  // ─── Open / close ─────────────────────────────────────────────────────────
  function open(cardId) {
    if (_isOpen) return;
    if (!CARD_META[cardId]) { console.warn('Unknown card_id', cardId); return; }
    _isOpen = true;
    const overlay = _$('breadth-detail-overlay');
    overlay.style.display = 'block';

    const meta = CARD_META[cardId];
    _$('bd-title').textContent = meta.title;
    _$('bd-subtitle').textContent = '90 days · loading…';
    _$('bd-body').innerHTML = `
      <div style="text-align:center;padding:48px;color:var(--text3,#64748b);font-size:11px">
        Loading 90-day analysis…
      </div>`;
    _$('bd-footer-left').textContent = '—';
    _$('bd-footer-right').textContent = '—';

    _fetchAndRender(cardId);
  }

  function close() {
    if (!_isOpen) return;
    _isOpen = false;
    _$('breadth-detail-overlay').style.display = 'none';
    if (_activeChart) {
      try { _activeChart.destroy(); } catch (e) {}
      _activeChart = null;
    }
  }
  window.closeBreadthDetail = close;

  // Esc to close + click-outside to close
  document.addEventListener('keydown', e => {
    if (e.key === 'Escape' && _isOpen) close();
  });
  document.addEventListener('click', e => {
    if (!_isOpen) return;
    if (e.target && e.target.id === 'breadth-detail-overlay') close();
  });

  // Attach click handlers to all cards (once DOM is ready)
  function _wireCards() {
    document.querySelectorAll('.breadth-detail-card').forEach(card => {
      if (card.__bd_wired) return;
      card.__bd_wired = true;
      card.addEventListener('click', () => {
        const id = card.getAttribute('data-card-id');
        if (id) open(id);
      });
    });
  }
  if (document.readyState !== 'loading') _wireCards();
  else document.addEventListener('DOMContentLoaded', _wireCards);

  // ─── Fetch + dispatch ─────────────────────────────────────────────────────
  async function _fetchAndRender(cardId) {
    try {
      const market = (window._currentMarket || 'INDIA').toUpperCase();
      const r = await fetch(`${ENDPOINT}?card_id=${encodeURIComponent(cardId)}&market=${market}`);
      if (!r.ok) {
        _renderError(`HTTP ${r.status}`);
        return;
      }
      const data = await r.json();
      if (data.error) { _renderError(data.error); return; }
      _render(cardId, data);
    } catch (e) {
      _renderError(e.message || 'Network error');
    }
  }

  function _renderError(msg) {
    _$('bd-body').innerHTML = `
      <div style="text-align:center;padding:48px;color:var(--red,#ef4444);font-size:11px">
        ${_esc(msg)}
      </div>`;
  }

  // ─── Renderer dispatcher ──────────────────────────────────────────────────
  function _render(cardId, data) {
    const meta = CARD_META[cardId];
    _$('bd-subtitle').textContent = `${data.days || 90} days · ${data.market || 'INDIA'}`;

    let chartHtml = '';
    if (meta.variant === 'regime') {
      chartHtml = _renderRegimeStrip(data);
    } else if (meta.variant === 'gauge') {
      chartHtml = _renderScoreGauge(data);
    } else {
      chartHtml = `<div style="position:relative;height:300px;margin-bottom:16px">
        <canvas id="bd-chart-canvas"></canvas>
      </div>`;
    }

    const statsHtml = _renderStats(cardId, data);
    const aiHtml = _renderAI(data);

    _$('bd-body').innerHTML = chartHtml + statsHtml + aiHtml;

    // Now render the actual chart for variants that use canvas
    if (meta.variant === 'line') _renderLineChart(data, meta.color);
    else if (meta.variant === 'bar') _renderBarChart(data, meta.color);
    else if (meta.variant === 'stack') _renderStackChart(data);

    // Footer
    const aiStatus = data.ai_cached ? 'AI cached (today)' : 'AI fresh';
    _$('bd-footer-left').textContent = aiStatus;
    _$('bd-footer-right').textContent = `Generated ${(data.generated_at || '').slice(0, 19).replace('T', ' ')}`;
  }

  // ─── Variant: line chart (5 cards) ────────────────────────────────────────
  function _renderLineChart(data, color) {
    const canvas = _$('bd-chart-canvas');
    if (!canvas || typeof Chart === 'undefined') return;
    const series = data.series || [];
    let labels, values;

    // Each card stores its value differently — adapt
    if (data.card_id === 'qbram_score') {
      labels = series.map(s => (s.date || '').slice(5));
      values = series.map(s => s.score);
    } else if (data.card_id === 'liquidity_stress') {
      labels = series.map(s => (s.date || '').slice(5));
      values = series.map(s => s.lss);
    } else {
      labels = series.map(s => (s.date || '').slice(5));
      values = series.map(s => s.value);
    }

    if (_activeChart) { try { _activeChart.destroy(); } catch (e) {} }
    _activeChart = new Chart(canvas.getContext('2d'), {
      type: 'line',
      data: {
        labels,
        datasets: [{
          data: values, borderColor: color,
          backgroundColor: color + '22', fill: true, borderWidth: 1.5,
          pointRadius: 0, tension: 0.2,
        }]
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        animation: { duration: 400 },
        plugins: { legend: { display: false } },
        scales: {
          x: { ticks: { color: '#94a3b8', font: { family: "'Space Mono'", size: 8 }, maxRotation: 0, autoSkip: true, maxTicksLimit: 12 }, grid: { color: 'rgba(100,116,139,.08)' } },
          y: { ticks: { color: '#94a3b8', font: { family: "'Space Mono'", size: 9 } }, grid: { color: 'rgba(100,116,139,.08)' } },
        },
      },
    });
  }

  // ─── Variant: bar chart (NH-NL) ───────────────────────────────────────────
  function _renderBarChart(data, color) {
    const canvas = _$('bd-chart-canvas');
    if (!canvas || typeof Chart === 'undefined') return;
    const series = data.series || [];
    const labels = series.map(s => (s.date || '').slice(5));
    const nets = series.map(s => s.net || 0);
    const colors = nets.map(v => v >= 0 ? 'rgba(34,197,94,0.75)' : 'rgba(239,68,68,0.75)');

    if (_activeChart) { try { _activeChart.destroy(); } catch (e) {} }
    _activeChart = new Chart(canvas.getContext('2d'), {
      type: 'bar',
      data: { labels, datasets: [{ data: nets, backgroundColor: colors, borderWidth: 0 }] },
      options: {
        responsive: true, maintainAspectRatio: false,
        animation: { duration: 400 },
        plugins: { legend: { display: false } },
        scales: {
          x: { ticks: { color: '#94a3b8', font: { family: "'Space Mono'", size: 8 }, maxRotation: 0, autoSkip: true, maxTicksLimit: 12 }, grid: { color: 'rgba(100,116,139,.08)' } },
          y: { ticks: { color: '#94a3b8', font: { family: "'Space Mono'", size: 9 } }, grid: { color: 'rgba(100,116,139,.08)' } },
        },
      },
    });
  }

  // ─── Variant: stacked bar (IV Footprint) ──────────────────────────────────
  function _renderStackChart(data) {
    const canvas = _$('bd-chart-canvas');
    if (!canvas || typeof Chart === 'undefined') return;
    const series = data.series || [];
    const labels = series.map(s => (s.date || '').slice(5));
    if (_activeChart) { try { _activeChart.destroy(); } catch (e) {} }
    _activeChart = new Chart(canvas.getContext('2d'), {
      type: 'bar',
      data: {
        labels,
        datasets: [
          { label: 'IV Buy',     data: series.map(s => s.iv_buy     || 0), backgroundColor: '#22c55e' },
          { label: 'PPV',        data: series.map(s => s.ppv        || 0), backgroundColor: '#3b82f6' },
          { label: 'Bull Snort', data: series.map(s => s.bull_snort || 0), backgroundColor: '#f59e0b' },
        ]
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        animation: { duration: 400 },
        plugins: { legend: { display: true, position: 'top', labels: { color: '#94a3b8', font: { family: "'Space Mono'", size: 9 }, boxWidth: 10, padding: 6 } } },
        scales: {
          x: { stacked: true, ticks: { color: '#94a3b8', font: { family: "'Space Mono'", size: 8 }, maxRotation: 0, autoSkip: true, maxTicksLimit: 12 } },
          y: { stacked: true, ticks: { color: '#94a3b8', font: { family: "'Space Mono'", size: 9 } } },
        },
      },
    });
  }

  // ─── Variant: regime strip (Regime Timeline) ─────────────────────────────
  function _renderRegimeStrip(data) {
    const series = data.series || [];
    if (!series.length) return '<div style="padding:24px;color:var(--text3);text-align:center">No regime history</div>';
    const cells = series.map(r => {
      const code = (r.regime || '').slice(0, 3);
      const full = r.regime || 'UNK';
      const color = REGIME_COLOR[full] || '#64748b';
      return `<div title="${_esc(r.date)} · ${_esc(full)} · ${_esc(r.score)}" style="
        flex:1;min-width:14px;height:32px;background:${color};
        display:flex;align-items:center;justify-content:center;
        font-size:7px;font-weight:700;color:rgba(0,0,0,.8);
        border-right:1px solid rgba(0,0,0,.18)">${_esc(code)}</div>`;
    }).join('');
    return `
      <div style="margin-bottom:16px">
        <div style="font-size:9px;color:var(--text3);margin-bottom:6px">
          90-day regime sequence (oldest → newest) — hover any cell for date/score
        </div>
        <div style="display:flex;border-radius:4px;overflow:hidden;border:1px solid var(--border2,#334155)">${cells}</div>
        <div style="display:flex;justify-content:space-between;margin-top:6px;font-size:8px;color:var(--text3)">
          <span>${_esc((series[0]||{}).date)}</span>
          <span>${_esc((series[series.length-1]||{}).date)}</span>
        </div>
      </div>`;
  }

  // ─── Variant: score gauge (7-component bars) ─────────────────────────────
  function _renderScoreGauge(data) {
    const stats = data.stats || {};
    const components = stats.components || {};
    const score = stats.score;
    const regime = stats.regime || '—';
    const regimeColor = REGIME_COLOR[regime] || '#94a3b8';

    const order = ['B50','BT','NH_NL','B200','B20_ACCEL','VOLUME','CSD'];
    const labels = {
      B50: 'B50 (% above 50 DMA)',
      BT: 'BT (Breadth Thrust)',
      NH_NL: 'NH-NL Ratio',
      B200: 'B200 (% above 200 DMA)',
      B20_ACCEL: 'B20 Acceleration',
      VOLUME: 'Volume Thrust',
      CSD: 'CSD (Dispersion, inverse)',
    };

    const bars = order.map(key => {
      const c = components[key] || {};
      const pts = c.points || 0;
      const max = c.max || 1;
      const pct = Math.round(pts / max * 100);
      // Color graduates with contribution
      const fillColor = pct >= 80 ? '#22c55e' : pct >= 50 ? '#a3e635' : pct >= 25 ? '#eab308' : '#ef4444';
      return `
        <div style="margin-bottom:10px">
          <div style="display:flex;justify-content:space-between;align-items:baseline;font-size:10px;margin-bottom:3px">
            <span style="color:var(--text2,#cbd5e1);font-weight:600">${labels[key] || key}</span>
            <span style="font-family:var(--font-mono)">
              <span style="color:${fillColor};font-weight:700">${pts}</span>
              <span style="color:var(--text3,#64748b)">/${max}</span>
              <span style="color:var(--text3,#64748b);margin-left:6px;font-size:9px">raw: ${_fmt(c.value, 1)}</span>
            </span>
          </div>
          <div style="height:8px;background:rgba(100,116,139,.18);border-radius:4px;overflow:hidden">
            <div style="height:100%;width:${pct}%;background:${fillColor};transition:width .4s"></div>
          </div>
        </div>`;
    }).join('');

    return `
      <div style="display:flex;align-items:baseline;gap:14px;margin-bottom:16px;padding-bottom:12px;border-bottom:1px solid var(--border2,#334155)">
        <span style="font-size:30px;font-weight:700;color:var(--text1,#e2e8f0);font-family:var(--font-mono)">${_fmt(score, 0)}</span>
        <span style="font-size:13px;font-weight:700;letter-spacing:.06em;
          padding:3px 10px;border-radius:4px;
          background:${regimeColor}33;color:${regimeColor};
          border:1px solid ${regimeColor}55">${_esc(regime)}</span>
        <span style="font-size:9px;color:var(--text3,#64748b);margin-left:auto">
          7-component decomposition · today's reading
        </span>
      </div>
      <div>${bars}</div>`;
  }

  // ─── Stats panel (card-specific) ──────────────────────────────────────────
  function _renderStats(cardId, data) {
    const s = data.stats || {};
    let cells = [];

    if (cardId === 'score_gauge') {
      // Already shown above; just trend over 90D
      const series = data.series || [];
      const scores = series.map(x => x.score).filter(v => v != null);
      const high = scores.length ? Math.max(...scores) : '—';
      const low  = scores.length ? Math.min(...scores) : '—';
      cells.push(['90D High', high]);
      cells.push(['90D Low',  low]);
      cells.push(['Sessions stored', series.length]);
    } else if (cardId === 'regime_timeline') {
      cells.push(['Current regime',     s.current_regime || '—']);
      cells.push(['Tenure (days)',      s.current_regime_tenure_days || 0]);
      cells.push(['Transitions in 90D', s.transitions || 0]);
      const counts = s.regime_counts || {};
      const summary = Object.entries(counts).map(([k,v]) => `${k.slice(0,3)}=${v}`).join(' · ');
      cells.push(['Distribution', summary || '—']);
    } else if (cardId === 'iv_footprint') {
      cells.push(['IV Buy (90D)',    s.iv_total]);
      cells.push(['PPV (90D)',       s.ppv_total]);
      cells.push(['Bull Snort (90D)', s.bs_total]);
      cells.push(['7D vs 90D avg',   `${s.last7_avg} vs ${s.avg_90d}`]);
      cells.push(['Direction',       s.direction || '—']);
    } else if (cardId === 'liquidity_stress') {
      cells.push(['Current',  s.current]);
      cells.push(['Zone',     s.zone || '—']);
      cells.push(['90D High', s.high]);
      cells.push(['90D Low',  s.low]);
      cells.push(['14D Direction', s.direction_14d || '—']);
    } else if (cardId === 'qbram_score') {
      cells.push(['Current',         s.current]);
      cells.push(['Current Regime',  s.current_regime || '—']);
      cells.push(['Regime Tenure',   `${s.current_regime_tenure_days || 0} days`]);
      cells.push(['90D High',        s.high]);
      cells.push(['90D Low',         s.low]);
    } else if (cardId === 'nh_nl') {
      cells.push(['Current Net',     s.current]);
      cells.push(['90D Max NH',      s.nh_max_90d]);
      cells.push(['90D Max NL',      s.nl_max_90d]);
      cells.push(['Days Net-Positive', s.positive_days]);
      cells.push(['Days Net-Negative', s.negative_days]);
    } else if (cardId === 'pct_above_50') {
      cells.push(['Current',     `${s.current}%`]);
      cells.push(['90D High',    `${s.high}%`]);
      cells.push(['90D Low',     `${s.low}%`]);
      const z = s.zones || {};
      cells.push(['Oversold days',   z['oversold (<40)']  || 0]);
      cells.push(['Strong days',     z['strong (>60)']    || 0]);
      cells.push(['Trend',           s.trend || '—']);
    } else {
      // Generic time-series stats
      cells.push(['Current',    s.current]);
      cells.push(['90D High',   s.high]);
      cells.push(['90D Low',    s.low]);
      cells.push(['Position',   s.position_pct != null ? `${s.position_pct}% of range` : '—']);
      cells.push(['Trend',      s.trend || '—']);
    }

    const grid = cells.map(([label, val]) => `
      <div style="flex:1;min-width:130px">
        <div style="font-size:9px;color:var(--text3,#64748b);text-transform:uppercase;letter-spacing:.06em;margin-bottom:2px">${_esc(label)}</div>
        <div style="font-size:13px;color:var(--text1,#e2e8f0);font-weight:700;font-family:var(--font-mono)">${_esc(val == null ? '—' : val)}</div>
      </div>`).join('');

    return `
      <div style="display:flex;flex-wrap:wrap;gap:14px;padding:12px;
        background:rgba(99,102,241,.05);border:1px solid rgba(99,102,241,.18);
        border-radius:6px;margin-bottom:14px">${grid}</div>`;
  }

  // ─── AI analysis section ──────────────────────────────────────────────────
  function _renderAI(data) {
    const txt = data.ai_analysis || '';
    const unavailable = !txt || /unavailable|not configured|empty response/i.test(txt);
    const bg = unavailable ? 'rgba(245,158,11,.08)' : 'rgba(34,197,94,.06)';
    const border = unavailable ? 'rgba(245,158,11,.3)' : 'rgba(34,197,94,.3)';
    const icon = unavailable ? '⚠' : '🤖';
    return `
      <div style="background:${bg};border:1px solid ${border};border-radius:6px;
        padding:14px 16px;margin-bottom:8px">
        <div style="display:flex;align-items:center;gap:8px;font-size:10px;
          color:var(--text3,#64748b);margin-bottom:8px;font-weight:600;
          text-transform:uppercase;letter-spacing:.06em">
          <span>${icon}</span><span>AI Analysis (Groq · Llama 3.3)</span>
        </div>
        <div style="color:var(--text1,#e2e8f0);font-size:12px;line-height:1.6;
          font-family:'Inter',sans-serif">${_esc(txt) || '(no analysis returned)'}</div>
        ${unavailable ? '' : `
        <div style="font-size:8px;color:var(--text3,#64748b);margin-top:8px;
          padding-top:8px;border-top:1px dashed var(--border2,#334155)">
          AI-generated. Verify against chart and stats. Updated once per trading day.
        </div>`}
      </div>`;
  }
})();
