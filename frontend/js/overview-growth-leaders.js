/**
 * Growth Leaders card — Overview Row 3 Card 4
 * Renders /api/overview/growth-leaders
 *
 * Shows NIFTY 500 stocks where BOTH sales & net profit grew ≥50% YoY in
 * the latest reported quarter. Sorted by composite growth desc.
 *
 * Polls every 60 min (backend caches 12hr; quarterly data only changes
 * when companies report). Initial fetch on Overview tab activation.
 */
(function () {
  'use strict';

  const ENDPOINT = '/api/overview/growth-leaders';
  const POLL_INTERVAL_MS = 60 * 60 * 1000;
  let _pollTimer = null;
  let _inFlight = false;

  function _escapeHtml(s) {
    return (s || '').replace(/[&<>"']/g, c => ({
      '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
    })[c]);
  }

  function _renderError(msg) {
    const body = document.getElementById('gl-body');
    if (!body) return;
    body.innerHTML = `<div style="text-align:center;padding:12px;color:var(--red);font-size:10px">${_escapeHtml(msg)}</div>`;
  }

  function _renderEmpty(diagnostic) {
    const body = document.getElementById('gl-body');
    if (!body) return;
    // Diagnostic is set when something is upstream-broken (empty tables, etc.).
    // Otherwise it's a genuine "no qualifying stocks this quarter" result.
    if (diagnostic) {
      body.innerHTML = `
        <div style="text-align:center;padding:12px;color:var(--amber);font-size:10px;line-height:1.6">
          <div style="margin-bottom:4px">⚠ Data issue</div>
          <div style="font-size:9px;color:var(--text3)">${_escapeHtml(diagnostic)}</div>
        </div>`;
    } else {
      body.innerHTML = `
        <div style="text-align:center;padding:12px;color:var(--text3);font-size:10px;line-height:1.6">
          No stocks meet the 50%+ growth bar this quarter
          <div style="font-size:9px;margin-top:4px;color:var(--text3)">
            Try checking back after the next results window
          </div>
        </div>`;
    }
  }

  // Color the growth number — gentle gradient based on magnitude
  function _growthColor(pct) {
    if (pct >= 100) return '#22c55e';   // bright green (100%+)
    if (pct >= 75)  return '#4ade80';
    if (pct >= 50)  return '#86efac';   // pale green (passes filter)
    return '#94a3b8';                   // never shown — but defensive
  }

  function _renderPayload(payload) {
    const body  = document.getElementById('gl-body');
    const badge = document.getElementById('gl-count-badge');
    const foot  = document.getElementById('gl-foot');
    if (!body) return;

    if (payload.error) { _renderError(payload.error); return; }

    const leaders = payload.leaders || [];
    if (!leaders.length) {
      _renderEmpty(payload.diagnostic);
      if (badge) badge.style.display = 'none';
      return;
    }

    // Count badge
    if (badge) {
      badge.textContent = `${payload.count} qualified`;
      badge.style.display = 'inline-block';
    }

    const rows = leaders.map(it => {
      const sgCol = _growthColor(it.sales_growth);
      const pgCol = _growthColor(it.profit_growth);

      const tickerCell = `<span style="color:var(--text1);font-weight:700;
        font-family:var(--font-mono);font-size:11px">${_escapeHtml(it.ticker)}</span>`;

      const companyCell = `<span style="color:var(--text3);font-size:9px;
        overflow:hidden;text-overflow:ellipsis;white-space:nowrap;display:block"
        title="${_escapeHtml(it.company || '')}${it.sector ? ' · ' + _escapeHtml(it.sector) : ''}">
        ${_escapeHtml(it.company || '')}
      </span>`;

      const growthBlock = `
        <div style="display:flex;flex-direction:column;align-items:flex-end;gap:1px;
          font-family:var(--font-mono);min-width:88px">
          <div style="display:flex;align-items:center;gap:6px;justify-content:flex-end">
            <span style="color:var(--text3);font-size:8px">SALES</span>
            <span style="color:${sgCol};font-weight:700;font-size:11px">+${it.sales_growth}%</span>
          </div>
          <div style="display:flex;align-items:center;gap:6px;justify-content:flex-end">
            <span style="color:var(--text3);font-size:8px">PROFIT</span>
            <span style="color:${pgCol};font-weight:700;font-size:11px">+${it.profit_growth}%</span>
          </div>
        </div>`;

      return `
        <div style="display:flex;align-items:center;justify-content:space-between;
          padding:6px 4px;border-bottom:1px dashed rgba(100,116,139,.15);gap:8px">
          <div style="flex:1;min-width:0;display:flex;flex-direction:column;gap:1px">
            ${tickerCell}
            ${companyCell}
          </div>
          ${growthBlock}
        </div>`;
    }).join('');

    body.innerHTML = rows;

    if (foot) {
      const cacheAge = payload.cache_age_hr || 0;
      const examined = payload.examined || 0;
      foot.textContent = `${payload.count}/${examined} · YoY ≥50% · cache ${cacheAge}h`;
    }
  }

  async function load() {
    if (_inFlight) return;
    _inFlight = true;
    try {
      const r = await fetch(ENDPOINT);
      if (!r.ok) { _renderError(`HTTP ${r.status}`); return; }
      const data = await r.json();
      _renderPayload(data);
    } catch (e) {
      _renderError(e.message || 'Network error');
    } finally {
      _inFlight = false;
    }
  }

  function _schedulePoll() {
    if (_pollTimer) clearInterval(_pollTimer);
    _pollTimer = setInterval(load, POLL_INTERVAL_MS);
  }

  function _onTabActivate() {
    const tab = document.getElementById('tab-overview');
    if (!tab) return;
    if (tab.classList.contains('active')) {
      load();
      _schedulePoll();
    }
  }

  document.addEventListener('DOMContentLoaded', () => {
    _onTabActivate();
    document.body.addEventListener('click', e => {
      const target = e.target;
      if (target && target.getAttribute && target.getAttribute('data-tab') === 'overview') {
        setTimeout(_onTabActivate, 100);
      }
    });
  });

  window.refreshGrowthLeaders = load;
})();
