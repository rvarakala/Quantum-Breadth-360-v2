/**
 * Market News card — renders /api/overview/news
 * Called on overview tab load + every 10 min + on category change.
 */
(function () {
  'use strict';

  const ENDPOINT = '/api/overview/news';
  let _currentCategory = 'all';
  let _pollTimer = null;
  let _inFlight = false;

  function _escapeHtml(s) {
    return (s || '').replace(/[&<>"']/g, c => ({
      '&': '&amp;', '<': '&lt;', '>': '&gt;', '"': '&quot;', "'": '&#39;'
    })[c]);
  }

  function _sentimentClass(s) {
    if (s === 'BULL') return 'nw-senti-bull';
    if (s === 'BEAR') return 'nw-senti-bear';
    return 'nw-senti-neutral';
  }

  function _sentimentLabel(s) {
    if (s === 'BULL')    return '▲';
    if (s === 'BEAR')    return '▼';
    return '—';
  }

  function _renderError(msg) {
    const body = document.getElementById('nw-body');
    if (!body) return;
    body.innerHTML = `<div style="text-align:center;padding:12px;color:var(--red);font-size:10px">${_escapeHtml(msg)}</div>`;
  }

  function _renderEmpty() {
    const body = document.getElementById('nw-body');
    if (!body) return;
    body.innerHTML = `<div style="text-align:center;padding:12px;color:var(--text3);font-size:10px">No recent news</div>`;
  }

  function _renderPayload(payload) {
    const body = document.getElementById('nw-body');
    const badge = document.getElementById('nw-senti-badge');
    if (!body) return;

    if (badge) {
      if (payload.sentiment_enabled) {
        badge.textContent = '✨ AI';
        badge.style.display = 'inline-block';
      } else {
        badge.style.display = 'none';
      }
    }

    const items = payload.items || [];
    if (!items.length) { _renderEmpty(); return; }

    // Compute aggregate tone from sentiment
    let bull = 0, bear = 0;
    items.forEach(i => {
      if (i.sentiment === 'BULL') bull++;
      else if (i.sentiment === 'BEAR') bear++;
    });

    const rows = items.map(item => {
      const sClass = _sentimentClass(item.sentiment);
      const sIcon = _sentimentLabel(item.sentiment);
      const showSenti = payload.sentiment_enabled;
      const senti = showSenti
        ? `<span class="nw-senti ${sClass}" title="${item.sentiment}">${sIcon}</span>`
        : '';
      return `
        <a class="nw-item" href="${_escapeHtml(item.link)}" target="_blank" rel="noopener">
          <div class="nw-line1">
            <span class="nw-source">${_escapeHtml(item.source)}</span>
            ${senti}
            <span class="nw-time">${_escapeHtml(item.time_ago)}</span>
          </div>
          <div class="nw-title">${_escapeHtml(item.title)}</div>
        </a>`;
    }).join('');

    body.innerHTML = rows;
  }

  async function loadMarketNews(force) {
    if (_inFlight) return;
    _inFlight = true;
    try {
      const url = `${ENDPOINT}?category=${_currentCategory}${force ? `&t=${Date.now()}` : ''}`;
      const res = await fetch(url);
      if (!res.ok) throw new Error(`HTTP ${res.status}`);
      const payload = await res.json();
      if (payload.error) { _renderError(payload.error); }
      else { _renderPayload(payload); }
    } catch (e) {
      console.warn('[market-news] fetch failed:', e);
      _renderError('Unable to load');
    } finally {
      _inFlight = false;
    }
  }

  function setNewsCategory(cat) {
    _currentCategory = cat;
    // Toggle chip active state
    document.querySelectorAll('#nw-filters .nw-chip').forEach(b => {
      b.classList.toggle('active', b.dataset.cat === cat);
    });
    loadMarketNews(true);
  }

  function _schedulePolling() {
    if (_pollTimer) clearInterval(_pollTimer);
    _pollTimer = setInterval(() => loadMarketNews(false), 10 * 60 * 1000);
  }

  window.loadMarketNews = loadMarketNews;
  window.setNewsCategory = setNewsCategory;

  function _init() {
    const tab = document.getElementById('tab-overview');
    if (tab && tab.classList.contains('active')) {
      loadMarketNews(false);
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
        loadMarketNews(false);
        _schedulePolling();
      }
      return r;
    };
  }
})();
