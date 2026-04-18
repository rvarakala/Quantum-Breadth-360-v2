/**
 * Global Markets card — renders /api/overview/global-markets
 * Called on overview tab load + every 5 min.
 */
(function () {
  'use strict';

  const ENDPOINT = '/api/overview/global-markets';
  let _pollTimer = null;
  let _inFlight = false;

  function _fmtPrice(n) {
    if (n == null) return '—';
    if (n >= 10000) return n.toLocaleString('en-US', { maximumFractionDigits: 0 });
    return n.toLocaleString('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  }

  function _fmtPct(p) {
    if (p == null) return '—';
    const sign = p >= 0 ? '+' : '';
    return `${sign}${p.toFixed(2)}%`;
  }

  function _arrow(p) {
    if (p == null) return '—';
    if (p > 0.15) return '▲';
    if (p < -0.15) return '▼';
    return '—';
  }

  function _tonePillClass(tone) {
    if (tone === 'RISK_ON') return 'gm-tone-on';
    if (tone === 'RISK_OFF') return 'gm-tone-off';
    return 'gm-tone-mixed';
  }

  function _toneLabel(tone) {
    if (tone === 'RISK_ON') return '● RISK-ON';
    if (tone === 'RISK_OFF') return '● RISK-OFF';
    return '● MIXED';
  }

  function _renderError(msg) {
    const body = document.getElementById('gm-body');
    if (!body) return;
    body.innerHTML = `<div style="text-align:center;padding:12px;color:var(--red);font-size:10px">${msg}</div>`;
    const pill = document.getElementById('gm-tone-pill');
    if (pill) { pill.textContent = '—'; pill.className = 'gm-tone-pill'; }
  }

  function _renderPayload(payload) {
    const body = document.getElementById('gm-body');
    const pill = document.getElementById('gm-tone-pill');
    if (!body) return;

    if (pill) {
      pill.textContent = _toneLabel(payload.tone);
      pill.className = 'gm-tone-pill ' + _tonePillClass(payload.tone);
    }

    const rows = (payload.markets || []).map(m => {
      if (m.status !== 'OK' && m.status !== 'FALLBACK_SPOT') {
        return `
          <div class="gm-row">
            <span class="gm-label">${m.label}</span>
            <span class="gm-price" style="color:var(--text3)">—</span>
            <span class="gm-pct" style="color:var(--text3)">n/a</span>
          </div>`;
      }
      const isPos = (m.change_pct || 0) >= 0;
      const color = Math.abs(m.change_pct || 0) < 0.15 ? 'var(--text2)' : (isPos ? 'var(--green)' : 'var(--red)');
      const fallbackNote = m.status === 'FALLBACK_SPOT'
        ? ` <span title="Spot NIFTY — GIFT unavailable" style="font-size:8px;color:var(--amber);font-family:var(--font-mono)">(spot)</span>`
        : '';
      return `
        <div class="gm-row">
          <span class="gm-label">${m.label}${fallbackNote}</span>
          <span class="gm-price">${_fmtPrice(m.price)}</span>
          <span class="gm-pct" style="color:${color}">${_arrow(m.change_pct)} ${_fmtPct(m.change_pct)}</span>
        </div>`;
    }).join('');

    const updated = payload.updated_at
      ? new Date(payload.updated_at).toLocaleTimeString('en-IN', { hour: '2-digit', minute: '2-digit' })
      : '';
    body.innerHTML = `
      <div class="gm-rows">${rows}</div>
      <div class="gm-footer">Updated ${updated}</div>
    `;
  }

  async function loadGlobalMarkets(force) {
    if (_inFlight) return;
    _inFlight = true;
    try {
      const url = ENDPOINT + (force ? `?t=${Date.now()}` : '');
      const res = await fetch(url);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const payload = await res.json();
      if (payload.error) {
        _renderError(payload.error);
      } else {
        _renderPayload(payload);
      }
    } catch (e) {
      console.warn('[global-markets] fetch failed:', e);
      _renderError('Unable to load');
    } finally {
      _inFlight = false;
    }
  }

  function _schedulePolling() {
    if (_pollTimer) clearInterval(_pollTimer);
    _pollTimer = setInterval(() => loadGlobalMarkets(false), 5 * 60 * 1000);
  }

  // Expose globally for onclick + manual refresh
  window.loadGlobalMarkets = loadGlobalMarkets;

  // Auto-load on DOM ready if overview tab is active
  function _init() {
    const tab = document.getElementById('tab-overview');
    if (tab && tab.classList.contains('active')) {
      loadGlobalMarkets(false);
      _schedulePolling();
    }
  }
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', _init);
  } else {
    _init();
  }

  // Also load whenever overview tab is switched to
  const _origSwitchTab = window.switchTab;
  if (typeof _origSwitchTab === 'function') {
    window.switchTab = function (tabName) {
      const r = _origSwitchTab.apply(this, arguments);
      if (tabName === 'overview') {
        loadGlobalMarkets(false);
        _schedulePolling();
      }
      return r;
    };
  }
})();
