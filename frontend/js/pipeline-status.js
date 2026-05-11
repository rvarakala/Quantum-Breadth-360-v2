// ── Startup Pipeline status pill + detail panel ───────────────────────────────
// Polls /api/startup-pipeline/status every 5s while pipeline is running,
// every 60s while idle/done/skipped. Updates the nav pill and detail panel.

(function () {
  let _panelOpen = false;
  let _pollTimer = null;
  let _lastStatus = null;

  // ── DOM helpers ─────────────────────────────────────────────────────────────
  function $(id) { return document.getElementById(id); }

  function _pillStyle(status) {
    switch (status) {
      case 'running':
        return { bg: 'rgba(59,130,246,.15)',  border: 'rgba(59,130,246,.5)',  dot: '#3b82f6', text: '#93c5fd' };
      case 'done':
        return { bg: 'rgba(34,197,94,.15)',   border: 'rgba(34,197,94,.5)',   dot: '#22c55e', text: '#86efac' };
      case 'partial':
        return { bg: 'rgba(245,158,11,.15)',  border: 'rgba(245,158,11,.5)',  dot: '#f59e0b', text: '#fcd34d' };
      case 'error':
        return { bg: 'rgba(239,68,68,.15)',   border: 'rgba(239,68,68,.5)',   dot: '#ef4444', text: '#fca5a5' };
      case 'skipped':
        return { bg: 'rgba(148,163,184,.15)', border: 'rgba(148,163,184,.4)', dot: '#94a3b8', text: '#cbd5e1' };
      case 'idle':
      default:
        return { bg: 'rgba(71,85,105,.15)',   border: 'rgba(71,85,105,.4)',   dot: '#64748b', text: '#94a3b8' };
    }
  }

  function _pillLabel(status, data) {
    if (status === 'running') {
      const cs = data.current_step;
      const stepObj = (data.steps || []).find(s => s.key === cs);
      if (stepObj) return `Step ${stepObj.order || '?'}/7 · ${stepObj.label}`;
      return 'Running…';
    }
    if (status === 'done')    return `Ready · ${(data.steps || []).length} steps`;
    if (status === 'partial') return `Partial · ${(data.steps || []).filter(s => s.status === 'error').length} err`;
    if (status === 'skipped') return 'Up-to-date';
    if (status === 'error')   return 'Pipeline error';
    return 'Idle';
  }

  // ── Status fetch + render ───────────────────────────────────────────────────
  async function fetchStatus() {
    try {
      const r = await fetch('/api/startup-pipeline/status');
      if (!r.ok) return;
      const data = await r.json();
      renderPill(data);
      if (_panelOpen) renderPanel(data);
      _lastStatus = data.status;
      // Adjust poll cadence based on state
      _schedulePoll(data.status === 'running' ? 5000 : 60000);
    } catch (e) {
      // Silent — endpoint may not be ready yet during initial boot
    }
  }

  function renderPill(data) {
    const pill    = $('pipeline-pill');
    const dot     = $('pipeline-pill-dot');
    const textEl  = $('pipeline-pill-text');
    if (!pill || !dot || !textEl) return;
    const style = _pillStyle(data.status);
    pill.style.background = style.bg;
    pill.style.borderColor = style.border;
    pill.style.color = style.text;
    dot.style.background = style.dot;
    // Pulse the dot if running
    if (data.status === 'running') {
      dot.style.animation = 'pipeline-pulse 1.2s infinite';
    } else {
      dot.style.animation = '';
    }
    textEl.textContent = _pillLabel(data.status, data);
  }

  function renderPanel(data) {
    const panel = $('pipeline-panel');
    if (!panel) return;
    const stepsBox  = $('pipeline-panel-steps');
    const metaEl    = $('pipeline-panel-meta');
    const footerEl  = $('pipeline-panel-footer');
    if (!stepsBox) return;

    // 7 expected steps from the spec
    const expected = [
      { key: 'nifty500',       order: 1, label: 'NIFTY 500 OHLCV' },
      { key: 'sectors',        order: 2, label: 'Sectors' },
      { key: 'smart_screener', order: 3, label: 'Smart Screener' },
      { key: 'journal',        order: 4, label: 'Trading Journal' },
      { key: 'fiidii',         order: 5, label: 'FII / DII' },
      { key: 'leaders',        order: 6, label: 'Leaders (RS+F-Value)' },
      { key: 'scanners',       order: 7, label: 'Swing Cockpit (9)' },
    ];
    const byKey = {};
    (data.steps || []).forEach(s => { byKey[s.key] = s; });

    stepsBox.innerHTML = expected.map(e => {
      const got = byKey[e.key];
      const status = got ? got.status : 'pending';
      const icon = status === 'done' ? '✅'
                 : status === 'error' ? '❌'
                 : status === 'running' ? '⏳'
                 : '◯';
      const color = status === 'done' ? '#22c55e'
                  : status === 'error' ? '#ef4444'
                  : status === 'running' ? '#3b82f6'
                  : '#64748b';
      const detail = got
        ? (got.error
            ? `<span style="color:#fca5a5;font-size:9px">${got.error}</span>`
            : got.detail
              ? `<span style="color:var(--text3);font-size:9px">${got.detail}</span>`
              : got.duration_s
                ? `<span style="color:var(--text3);font-size:9px">${got.duration_s}s</span>`
                : '')
        : '<span style="color:var(--text3);font-size:9px">pending</span>';
      return `
        <div style="display:flex;align-items:center;justify-content:space-between;
          padding:4px 0;border-bottom:1px dashed rgba(100,116,139,.2)">
          <span style="display:flex;align-items:center;gap:8px">
            <span style="font-size:10px">${icon}</span>
            <span style="color:${color};font-weight:${status==='running'?'700':'500'}">
              ${e.order}. ${e.label}
            </span>
          </span>
          ${detail}
        </div>`;
    }).join('');

    if (metaEl) {
      metaEl.textContent = data.status ? data.status.toUpperCase() : '—';
    }
    if (footerEl) {
      const parts = [];
      if (data.last_run_date) parts.push(`Last run: ${data.last_run_date}`);
      if (data.started_at && data.status === 'running') {
        const elapsed = Math.round((Date.now() - new Date(data.started_at).getTime()) / 1000);
        parts.push(`Elapsed: ${elapsed}s`);
      }
      footerEl.textContent = parts.join(' · ') || '—';
    }
  }

  // ── Toggle handler (exposed globally for onclick) ────────────────────────────
  window.togglePipelinePanel = function () {
    const panel = $('pipeline-panel');
    if (!panel) return;
    _panelOpen = !_panelOpen;
    panel.style.display = _panelOpen ? 'block' : 'none';
    if (_panelOpen) fetchStatus();   // immediate refresh on open
  };

  // ── Force re-run (admin only) ────────────────────────────────────────────────
  window.forcePipelineRun = async function () {
    const btn = $('pipeline-force-btn');
    if (btn) { btn.disabled = true; btn.textContent = '⟳ Triggering…'; }
    try {
      const token = localStorage.getItem('jwt') || '';
      const r = await fetch('/api/startup-pipeline/run', {
        method: 'POST',
        headers: token ? { 'Authorization': 'Bearer ' + token } : {},
      });
      const data = await r.json();
      if (data.ok) {
        if (btn) btn.textContent = '✓ Started';
        setTimeout(fetchStatus, 500);
      } else {
        alert(data.error || 'Pipeline trigger failed');
        if (btn) btn.textContent = '⟳ Force Re-Run';
      }
    } catch (e) {
      alert('Network error: ' + e.message);
      if (btn) btn.textContent = '⟳ Force Re-Run';
    } finally {
      setTimeout(() => { if (btn) { btn.disabled = false; btn.textContent = '⟳ Force Re-Run'; } }, 2500);
    }
  };

  // ── Close panel when clicking outside ────────────────────────────────────────
  document.addEventListener('click', function (e) {
    if (!_panelOpen) return;
    const panel = $('pipeline-panel');
    const pill  = $('pipeline-pill');
    if (panel && !panel.contains(e.target) && pill && !pill.contains(e.target)) {
      _panelOpen = false;
      panel.style.display = 'none';
    }
  });

  // ── Inject keyframes for the running-dot pulse (one-time) ────────────────────
  (function injectKeyframes() {
    if (document.getElementById('pipeline-keyframes')) return;
    const s = document.createElement('style');
    s.id = 'pipeline-keyframes';
    s.textContent = '@keyframes pipeline-pulse { 0%,100% { opacity: 1; } 50% { opacity: .35; } }';
    document.head.appendChild(s);
  })();

  // ── Polling lifecycle ────────────────────────────────────────────────────────
  function _schedulePoll(delay) {
    if (_pollTimer) clearTimeout(_pollTimer);
    _pollTimer = setTimeout(fetchStatus, delay);
  }

  // Boot: first fetch in 2s to let the backend's own startup settle
  setTimeout(fetchStatus, 2000);
})();
