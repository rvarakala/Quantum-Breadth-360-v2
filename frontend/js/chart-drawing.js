// ════════════════════════════════════════════════════════════════════════════
// CHART DRAWING TOOLS — Overlay canvas for TradingView Lightweight Charts
// Tools: Trend Line, Horizontal Line, Rectangle, Text, Eraser
// Drawings persist per ticker in localStorage
// ════════════════════════════════════════════════════════════════════════════

const _draw = {
  canvas: null,
  ctx: null,
  tool: null,        // 'trend' | 'horizontal' | 'rectangle' | 'text' | 'eraser' | 'move' | null
  drawings: [],      // [{type, points, color, ...}]
  tempPoints: [],    // in-progress drawing
  dragging: false,
  movingIdx: -1,     // index of drawing being moved
  moveStart: null,   // {x, y} start of move drag
  ticker: '',
  color: '#06b6d4',
  lineWidth: 2,
};

// ── INIT: Create overlay canvas on top of chart ──────────────────────────────

function initDrawingOverlay() {
  const container = document.getElementById('chart-container');
  if (!container) return;

  // Remove old overlay if exists
  const old = document.getElementById('draw-overlay');
  if (old) old.remove();

  const canvas = document.createElement('canvas');
  canvas.id = 'draw-overlay';
  canvas.style.cssText = 'position:absolute;top:0;left:0;width:100%;height:100%;z-index:50;pointer-events:none;cursor:crosshair';
  container.style.position = 'relative';
  container.appendChild(canvas);

  // Size to container
  const rect = container.getBoundingClientRect();
  canvas.width = rect.width;
  canvas.height = rect.height;

  _draw.canvas = canvas;
  _draw.ctx = canvas.getContext('2d');

  // Mouse events on container (not canvas, since canvas has pointer-events:none when no tool)
  container.addEventListener('mousedown', _onDrawMouseDown);
  container.addEventListener('mousemove', _onDrawMouseMove);
  container.addEventListener('mouseup', _onDrawMouseUp);
  container.addEventListener('dblclick', _onDrawDblClick);

  // Resize observer
  const ro = new ResizeObserver(() => {
    const r = container.getBoundingClientRect();
    canvas.width = r.width;
    canvas.height = r.height;
    _redrawAll();
  });
  ro.observe(container);
}

// ── TOOL SELECTION ───────────────────────────────────────────────────────────

function setDrawTool(tool) {
  _draw.tool = _draw.tool === tool ? null : tool; // toggle off if same
  _draw.tempPoints = [];
  _draw.movingIdx = -1;

  const overlay = document.getElementById('draw-overlay');
  if (overlay) {
    overlay.style.pointerEvents = _draw.tool ? 'auto' : 'none';
    overlay.style.cursor = _draw.tool === 'eraser' ? 'not-allowed' : _draw.tool === 'move' ? 'grab' : _draw.tool ? 'crosshair' : 'default';
  }

  // Only disable pressedMouseMove (pan), keep mouseWheel (zoom) enabled
  if (_chartState.priceChart) {
    _chartState.priceChart.applyOptions({
      handleScroll: { mouseWheel: true, pressedMouseMove: !_draw.tool },
      handleScale: { mouseWheel: true, axisPressedMouseMove: !_draw.tool },
    });
  }

  // Update toolbar button states
  document.querySelectorAll('.draw-tool-btn').forEach(b => {
    b.classList.toggle('active', b.dataset.tool === _draw.tool);
  });
}

function setDrawColor(color) {
  _draw.color = color;
  document.querySelectorAll('.draw-color-btn').forEach(b => {
    b.classList.toggle('active', b.dataset.color === color);
  });
}

function clearAllDrawings() {
  if (!confirm('Clear all drawings for ' + (_draw.ticker || 'this chart') + '?')) return;
  _draw.drawings = [];
  _saveDrawings();
  _redrawAll();
}

function undoLastDrawing() {
  if (_draw.drawings.length === 0) return;
  _draw.drawings.pop();
  _saveDrawings();
  _redrawAll();
}

// ── MOUSE HANDLERS ───────────────────────────────────────────────────────────

function _onDrawMouseDown(e) {
  if (!_draw.tool) return;
  const pt = _getCanvasPoint(e);

  if (_draw.tool === 'eraser') {
    _eraseAt(pt);
    return;
  }

  // Move mode: check if clicking on existing drawing
  if (_draw.tool === 'move') {
    const idx = _findNearestDrawing(pt, 20);
    if (idx >= 0) {
      _draw.movingIdx = idx;
      _draw.moveStart = pt;
      _draw.dragging = true;
      const overlay = document.getElementById('draw-overlay');
      if (overlay) overlay.style.cursor = 'grabbing';
    }
    return;
  }

  if (_draw.tool === 'text') {
    const text = prompt('Enter text:');
    if (!text) { setDrawTool(null); return; }
    _draw.drawings.push({
      type: 'text', x: pt.x, y: pt.y, text, color: _draw.color,
      fontSize: 13, canvasW: _draw.canvas.width, canvasH: _draw.canvas.height,
    });
    _saveDrawings();
    _redrawAll();
    // Auto-deselect text tool after placing
    setDrawTool(null);
    return;
  }

  if (_draw.tool === 'horizontal') {
    _draw.drawings.push({
      type: 'horizontal', y: pt.y, color: _draw.color, lineWidth: _draw.lineWidth,
      canvasW: _draw.canvas.width, canvasH: _draw.canvas.height,
    });
    _saveDrawings();
    _redrawAll();
    return;
  }

  // Trend line or Rectangle — click-drag
  if (_draw.tempPoints.length === 0) {
    _draw.tempPoints = [pt];
    _draw.dragging = true;
  }
}

function _onDrawMouseMove(e) {
  if (!_draw.tool) return;
  const pt = _getCanvasPoint(e);

  // Move mode: drag existing drawing
  if (_draw.tool === 'move' && _draw.dragging && _draw.movingIdx >= 0) {
    const d = _draw.drawings[_draw.movingIdx];
    const dx = pt.x - _draw.moveStart.x;
    const dy = pt.y - _draw.moveStart.y;
    if (d.type === 'trend') { d.x1 += dx; d.y1 += dy; d.x2 += dx; d.y2 += dy; }
    else if (d.type === 'horizontal') { d.y += dy; }
    else if (d.type === 'rectangle') { d.x1 += dx; d.y1 += dy; d.x2 += dx; d.y2 += dy; }
    else if (d.type === 'text') { d.x += dx; d.y += dy; }
    _draw.moveStart = pt;
    _redrawAll();
    return;
  }

  // Temp drawing preview
  if (_draw.dragging && _draw.tempPoints.length > 0) {
    _redrawAll();
    _drawTemp(_draw.tempPoints[0], pt);
  }
}

function _onDrawMouseUp(e) {
  // Move mode: finish moving
  if (_draw.tool === 'move' && _draw.movingIdx >= 0) {
    _draw.movingIdx = -1;
    _draw.dragging = false;
    _saveDrawings();
    const overlay = document.getElementById('draw-overlay');
    if (overlay) overlay.style.cursor = 'grab';
    return;
  }

  if (!_draw.tool || !_draw.dragging || _draw.tempPoints.length === 0) return;
  const pt = _getCanvasPoint(e);
  const p1 = _draw.tempPoints[0];

  // Minimum distance check
  const dist = Math.sqrt((pt.x - p1.x) ** 2 + (pt.y - p1.y) ** 2);
  if (dist < 5) { _draw.dragging = false; _draw.tempPoints = []; return; }

  if (_draw.tool === 'trend') {
    _draw.drawings.push({
      type: 'trend', x1: p1.x, y1: p1.y, x2: pt.x, y2: pt.y,
      color: _draw.color, lineWidth: _draw.lineWidth,
      canvasW: _draw.canvas.width, canvasH: _draw.canvas.height,
    });
  } else if (_draw.tool === 'rectangle') {
    _draw.drawings.push({
      type: 'rectangle', x1: p1.x, y1: p1.y, x2: pt.x, y2: pt.y,
      color: _draw.color, lineWidth: _draw.lineWidth,
      canvasW: _draw.canvas.width, canvasH: _draw.canvas.height,
    });
  }

  _draw.tempPoints = [];
  _draw.dragging = false;
  _saveDrawings();
  _redrawAll();
}

function _onDrawDblClick(e) {
  if (!_draw.tool) {
    // Double-click with no tool = erase nearest drawing
    const pt = _getCanvasPoint(e);
    _eraseAt(pt);
  }
}

// ── DRAWING FUNCTIONS ────────────────────────────────────────────────────────

function _drawTemp(p1, p2) {
  const ctx = _draw.ctx;
  ctx.save();
  ctx.setLineDash([4, 4]);
  ctx.strokeStyle = _draw.color;
  ctx.lineWidth = _draw.lineWidth;

  if (_draw.tool === 'trend') {
    ctx.beginPath();
    ctx.moveTo(p1.x, p1.y);
    ctx.lineTo(p2.x, p2.y);
    ctx.stroke();
  } else if (_draw.tool === 'rectangle') {
    ctx.strokeRect(p1.x, p1.y, p2.x - p1.x, p2.y - p1.y);
    ctx.fillStyle = _draw.color + '15';
    ctx.fillRect(p1.x, p1.y, p2.x - p1.x, p2.y - p1.y);
  }
  ctx.restore();
}

function _redrawAll() {
  const ctx = _draw.ctx;
  const cw = _draw.canvas.width;
  const ch = _draw.canvas.height;
  ctx.clearRect(0, 0, cw, ch);

  for (const d of _draw.drawings) {
    // Scale coordinates if canvas was resized since drawing was made
    const sx = cw / (d.canvasW || cw);
    const sy = ch / (d.canvasH || ch);

    ctx.save();
    ctx.strokeStyle = d.color || '#06b6d4';
    ctx.fillStyle = d.color || '#06b6d4';
    ctx.lineWidth = d.lineWidth || 2;
    ctx.setLineDash([]);

    if (d.type === 'trend') {
      ctx.beginPath();
      ctx.moveTo(d.x1 * sx, d.y1 * sy);
      ctx.lineTo(d.x2 * sx, d.y2 * sy);
      ctx.stroke();
      // Small circles at endpoints
      ctx.beginPath(); ctx.arc(d.x1 * sx, d.y1 * sy, 3, 0, Math.PI * 2); ctx.fill();
      ctx.beginPath(); ctx.arc(d.x2 * sx, d.y2 * sy, 3, 0, Math.PI * 2); ctx.fill();
    }

    else if (d.type === 'horizontal') {
      const y = d.y * sy;
      ctx.setLineDash([6, 3]);
      ctx.beginPath();
      ctx.moveTo(0, y);
      ctx.lineTo(cw, y);
      ctx.stroke();
      // Price label
      ctx.setLineDash([]);
      ctx.fillStyle = d.color + 'cc';
      ctx.fillRect(cw - 55, y - 8, 55, 16);
      ctx.fillStyle = '#fff';
      ctx.font = '10px "Space Mono", monospace';
      ctx.textAlign = 'center';
      ctx.fillText('━━', cw - 28, y + 4);
    }

    else if (d.type === 'rectangle') {
      const x = d.x1 * sx, y = d.y1 * sy;
      const w = (d.x2 - d.x1) * sx, h = (d.y2 - d.y1) * sy;
      ctx.strokeRect(x, y, w, h);
      ctx.fillStyle = (d.color || '#06b6d4') + '12';
      ctx.fillRect(x, y, w, h);
    }

    else if (d.type === 'text') {
      const x = d.x * sx, y = d.y * sy;
      ctx.font = `${d.fontSize || 13}px "Space Mono", monospace`;
      ctx.textAlign = 'left';
      // Background
      const metrics = ctx.measureText(d.text);
      ctx.fillStyle = '#0a0f1ecc';
      ctx.fillRect(x - 2, y - (d.fontSize || 13), metrics.width + 6, (d.fontSize || 13) + 4);
      // Text
      ctx.fillStyle = d.color || '#06b6d4';
      ctx.fillText(d.text, x, y);
    }

    ctx.restore();
  }
}

// ── FIND / ERASE ─────────────────────────────────────────────────────────────

function _findNearestDrawing(pt, threshold) {
  const cw = _draw.canvas.width;
  const ch = _draw.canvas.height;
  let nearestIdx = -1;
  let nearestDist = Infinity;

  for (let i = 0; i < _draw.drawings.length; i++) {
    const d = _draw.drawings[i];
    const sx = cw / (d.canvasW || cw);
    const sy = ch / (d.canvasH || ch);
    let dist = Infinity;

    if (d.type === 'trend') {
      dist = _pointToLineDist(pt, {x: d.x1*sx, y: d.y1*sy}, {x: d.x2*sx, y: d.y2*sy});
    } else if (d.type === 'horizontal') {
      dist = Math.abs(pt.y - d.y * sy);
    } else if (d.type === 'rectangle') {
      const cx = (d.x1 + d.x2) / 2 * sx;
      const cy = (d.y1 + d.y2) / 2 * sy;
      dist = Math.sqrt((pt.x - cx) ** 2 + (pt.y - cy) ** 2);
    } else if (d.type === 'text') {
      dist = Math.sqrt((pt.x - d.x * sx) ** 2 + (pt.y - d.y * sy) ** 2);
    }

    if (dist < nearestDist) { nearestDist = dist; nearestIdx = i; }
  }

  return (nearestIdx >= 0 && nearestDist < threshold) ? nearestIdx : -1;
}

function _eraseAt(pt) {
  const idx = _findNearestDrawing(pt, 15);
  if (idx >= 0) {
    _draw.drawings.splice(idx, 1);
    _saveDrawings();
    _redrawAll();
  }
}

function _pointToLineDist(pt, a, b) {
  const dx = b.x - a.x, dy = b.y - a.y;
  const lenSq = dx * dx + dy * dy;
  if (lenSq === 0) return Math.sqrt((pt.x - a.x) ** 2 + (pt.y - a.y) ** 2);
  let t = ((pt.x - a.x) * dx + (pt.y - a.y) * dy) / lenSq;
  t = Math.max(0, Math.min(1, t));
  const px = a.x + t * dx, py = a.y + t * dy;
  return Math.sqrt((pt.x - px) ** 2 + (pt.y - py) ** 2);
}

// ── PERSISTENCE (localStorage per ticker) ────────────────────────────────────

function _saveDrawings() {
  if (!_draw.ticker) return;
  const key = `chart_draw_${_draw.ticker}`;
  localStorage.setItem(key, JSON.stringify(_draw.drawings));
}

function loadDrawingsForTicker(ticker) {
  _draw.ticker = ticker;
  const key = `chart_draw_${ticker}`;
  try {
    _draw.drawings = JSON.parse(localStorage.getItem(key) || '[]');
  } catch { _draw.drawings = []; }
  _redrawAll();
}

// ── HELPERS ──────────────────────────────────────────────────────────────────

function _getCanvasPoint(e) {
  const rect = _draw.canvas.getBoundingClientRect();
  return {
    x: (e.clientX - rect.left) * (_draw.canvas.width / rect.width),
    y: (e.clientY - rect.top) * (_draw.canvas.height / rect.height),
  };
}
