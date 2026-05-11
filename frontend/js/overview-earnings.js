/**
 * Earnings & Events card (Overview Row 3 Card 4)
 * Renders /api/overview/earnings — companies reporting in the next 7 days.
 * Pattern mirrors overview-news.js: fetch on tab activate + every 60 min.
 */
(function () {
  'use strict';

  const ENDPOINT = '/api/overview/earnings';
  const POLL_INTERVAL_MS = 60 * 60 * 1000;  // 60 min — 4hr backend cache, so this is generous
  let _pollTimer = null;
  let _inFlight = false;

  function _escapeHtml(s) {
    return (s || '').replace(/[&<>"']/g, c => ({
      '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
    })[c]);
  }

  function _renderError(msg) {
    const body = document.getElementById('ev-body');
    if (!body) return;
    body.innerHTML = `<div style="text-align:center;padding:12px;color:var(--red);font-size:10px">${_escapeHtml(msg)}</div>`;
  }

  function _renderEmpty() {
    const body = document.getElementById('ev-body');
    if (!body) return;
    body.innerHTML = `<div style="text-align:center;padding:12px;color:var(--text3);font-size:10px">No earnings scheduled this week</div>`;
  }

  function _dayLabel(isoDate, today) {
    // Returns short day-of-week label: 'Today', 'Tomorrow', 'Mon', 'Tue' …
    if (!isoDate) return '—';
    const d = new Date(isoDate + 'T00:00:00');
    const t = new Date(today + 'T00:00:00');
    const diffMs = d - t;
    const diffDays = Math.round(diffMs / 86400000);
    if (diffDays === 0) return 'Today';
    if (diffDays === 1) return 'Tomorrow';
    const dayNames = ['Sun','Mon','Tue','Wed','Thu','Fri','Sat'];
    return dayNames[d.getDay()];
  }

  function _dayColor(label) {
    if (label === 'Today')    return '#22c55e';  // green
    if (label === 'Tomorrow') return '#3b82f6';  // blue
    return '#94a3b8';                            // slate
  }

  function _typeColor(type) {
    const t = (type || '').toLowerCase();
    if (t.includes('q1') || t.includes('q2') || t.includes('q3') || t.includes('q4')) return '#fbbf24';
    if (t.includes('annual'))  return '#22c55e';
    if (t.includes('audited')) return '#a78bfa';
    return '#64748b';
  }

  function _renderPayload(payload) {
    const body  = document.getElementById('ev-body');
    const badge = document.getElementById('ev-next-badge');
    const foot  = document.getElementById('ev-foot');
    if (!body) return;

    if (payload.error) { _renderError(payload.error); return; }
    const items = payload.items || [];
    if (!items.length) { _renderEmpty(); return; }

    // Next-session badge
    if (badge) {
      if (payload.next_session_count > 0) {
        badge.textContent = `▶ ${payload.next_session_count} tomorrow`;
        badge.style.display = 'inline-block';
      } else {
        badge.style.display = 'none';
      }
    }

    const today = payload.as_of || new Date().toISOString().slice(0,10);

    // Group by date — first heading row per date, then ticker rows under it
    const groups = {};
    for (const it of items) {
      const k = it.date || 'TBA';
      (groups[k] = groups[k] || []).push(it);
    }
    const sortedDates = Object.keys(groups).sort();

    const rows = sortedDates.map(d => {
      const lbl = _dayLabel(d, today);
      const lblColor = _dayColor(lbl);
      const groupRows = groups[d].map(it => {
        const tickerStr = it.ticker
          ? `<span style="color:var(--text1);font-weight:700;font-family:var(--font-mono);
              font-size:11px">${_escapeHtml(it.ticker)}</span>`
          : `<span style="color:var(--text3);font-style:italic">—</span>`;
        const typeStr = `<span style="color:${_typeColor(it.type)};font-size:9px;
          font-family:var(--font-mono);text-transform:uppercase">${_escapeHtml(it.type || 'Results')}</span>`;
        return `
          <div style="display:flex;align-items:center;justify-content:space-between;
            padding:5px 4px;border-bottom:1px dashed rgba(100,116,139,.15)">
            <span style="display:flex;flex-direction:column;gap:1px;min-width:0;flex:1">
              ${tickerStr}
              <span style="color:var(--text3);font-size:9px;overflow:hidden;
                text-overflow:ellipsis;white-space:nowrap" title="${_escapeHtml(it.company || '')}">
                ${_escapeHtml(it.company || '')}
              </span>
            </span>
            ${typeStr}
          </div>`;
      }).join('');
      return `
        <div style="margin-bottom:6px">
          <div style="font-size:9px;font-weight:700;color:${lblColor};
            font-family:var(--font-mono);padding:4px 4px 2px;
            background:linear-gradient(90deg, ${lblColor}10, transparent);
            border-left:2px solid ${lblColor};text-transform:uppercase;letter-spacing:.04em">
            ${lbl} · ${d}
          </div>
          ${groupRows}
        </div>`;
    }).join('');

    body.innerHTML = rows;

    // Footer: source + count + cache age
    if (foot) {
      const cacheAge = payload.cache_age_hr || 0;
      const sources = Array.from(new Set((items || []).map(i => i.source))).join('+');
      foot.textContent = `${payload.count} events · ${sources || 'nse'} · cache ${cacheAge}h`;
    }
  }

  async function load() {
    if (_inFlight) return;
    _inFlight = true;
    try {
      const r = await fetch(ENDPOINT);
      if (!r.ok) {
        _renderError(`HTTP ${r.status}`);
        return;
      }
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

  // Initial load when Overview tab is shown
  function _onTabActivate() {
    const tab = document.getElementById('tab-overview');
    if (!tab) return;
    if (tab.classList.contains('active')) {
      load();
      _schedulePoll();
    }
  }

  // Listen for tab activation (works with the existing tab system)
  document.addEventListener('DOMContentLoaded', () => {
    _onTabActivate();
    // Also re-load when user clicks back into the overview tab
    document.body.addEventListener('click', e => {
      const target = e.target;
      if (target && target.getAttribute && target.getAttribute('data-tab') === 'overview') {
        setTimeout(_onTabActivate, 100);
      }
    });
  });

  // Expose for manual refresh from console / refresh button
  window.refreshEarningsCard = load;
})();
