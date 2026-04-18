/**
 * Sector Leadership RS card — renders /api/overview/sector-rs
 * Called on overview tab load + every 15 min.
 */
(function () {
  'use strict';

  const ENDPOINT = '/api/overview/sector-rs';
  let _pollTimer = null;
  let _inFlight = false;

  function _esc(s) {
    return (s || '').replace(/[&<>"']/g, c => ({
      '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
    })[c]);
  }

  function _fmtPct(v) {
    if (v == null) return '—';
    const sign = v >= 0 ? '+' : '';
    return `${sign}${v.toFixed(2)}%`;
  }

  function _rsColor(rs) {
    if (rs >= 80) return 'var(--green)';
    if (rs >= 60) return '#10b981';
    if (rs >= 40) return 'var(--text2)';
    if (rs >= 20) return '#f59e0b';
    return 'var(--red)';
  }

  function _arrowColor(arrow) {
    if (arrow === '↑↑' || arrow === '↑') return 'var(--green)';
    if (arrow === '↓↓' || arrow === '↓') return 'var(--red)';
    return 'var(--text3)';
  }

  function _renderError(msg) {
    const body = document.getElementById('sr-body');
    if (!body) return;
    body.innerHTML = `<div style="text-align:center;padding:12px;color:var(--red);font-size:10px">${_esc(msg)}</div>`;
  }

  function _renderEmpty(hint) {
    const body = document.getElementById('sr-body');
    if (!body) return;
    body.innerHTML = `<div style="text-align:center;padding:12px;color:var(--text3);font-size:10px">${_esc(hint || 'No data')}</div>`;
  }

  function _row(entry, isLaggard) {
    const arrow = entry.arrow || '—';
    const rsColor = _rsColor(entry.rs);
    const arrColor = _arrowColor(arrow);
    const chgColor = entry.change_1d >= 0 ? 'var(--green)' : 'var(--red)';
    const marker = isLaggard ? '▼' : '▲';
    const markerColor = isLaggard ? 'var(--red)' : 'var(--green)';
    return `
      <div class="sr-row">
        <span class="sr-marker" style="color:${markerColor}">${marker}</span>
        <span class="sr-sector" title="${_esc(entry.sector)} — ${entry.members} stocks">${_esc(entry.sector)}</span>
        <span class="sr-rs" style="color:${rsColor}">RS ${entry.rs}</span>
        <span class="sr-chg" style="color:${chgColor}">${_fmtPct(entry.change_1d)}</span>
        <span class="sr-arrow" style="color:${arrColor}">${arrow}</span>
      </div>`;
  }

  function _renderPayload(payload) {
    const body = document.getElementById('sr-body');
    if (!body) return;

    if (payload.error) {
      _renderError(payload.error);
      return;
    }
    const leaders = payload.leaders || [];
    const laggards = payload.laggards || [];
    if (!leaders.length && !laggards.length) {
      _renderEmpty('No sector data yet');
      return;
    }

    const leadersHtml = leaders.map(e => _row(e, false)).join('');
    const laggardsHtml = laggards.map(e => _row(e, true)).join('');

    body.innerHTML = `
      <div class="sr-rows">
        ${leadersHtml}
        ${laggardsHtml ? '<div class="sr-divider"></div>' : ''}
        ${laggardsHtml}
      </div>
      <div class="sr-footer">As of ${_esc(payload.as_of || '')} · ${payload.sector_count || 0} sectors</div>
    `;
  }

  async function loadSectorRS(force) {
    if (_inFlight) return;
    _inFlight = true;
    try {
      const url = ENDPOINT + (force ? `?t=${Date.now()}` : '');
      const res = await fetch(url);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const payload = await res.json();
      _renderPayload(payload);
    } catch (e) {
      console.warn('[sector-rs] fetch failed:', e);
      _renderError('Unable to load');
    } finally {
      _inFlight = false;
    }
  }

  function _schedulePolling() {
    if (_pollTimer) clearInterval(_pollTimer);
    _pollTimer = setInterval(() => loadSectorRS(false), 15 * 60 * 1000);
  }

  window.loadSectorRS = loadSectorRS;

  function _init() {
    const tab = document.getElementById('tab-overview');
    if (tab && tab.classList.contains('active')) {
      loadSectorRS(false);
      _schedulePolling();
    }
  }
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', _init);
  } else {
    _init();
  }

  const _origSwitchTab = window.switchTab;
  if (typeof _origSwitchTab === 'function') {
    window.switchTab = function (tabName) {
      const r = _origSwitchTab.apply(this, arguments);
      if (tabName === 'overview') {
        loadSectorRS(false);
        _schedulePolling();
      }
      return r;
    };
  }
})();
