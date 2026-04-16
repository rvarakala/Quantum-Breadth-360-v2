// ════════════════════════════════════════════════════════════════════════════
// SECTOR ROTATION HEATMAP — D3 treemap
// ════════════════════════════════════════════════════════════════════════════

let _heatmapData = null;
let _heatmapPeriod = '1m';

function _hmColor(change) {
  // green for positive, red for negative, grey for 0
  if (change >= 10) return '#15803d';
  if (change >= 5)  return '#22c55e';
  if (change >= 2)  return '#4ade80';
  if (change >= 0.5) return '#86efac';
  if (change >= -0.5) return '#9ca3af';
  if (change >= -2)  return '#fca5a5';
  if (change >= -5)  return '#ef4444';
  if (change >= -10) return '#dc2626';
  return '#991b1b';
}

function _hmTextColor(change) {
  if (Math.abs(change) < 0.5) return '#374151';
  return '#fff';
}

async function loadHeatmap(period) {
  if (period) _heatmapPeriod = period;

  // Update active button
  document.querySelectorAll('.hm-period-btn').forEach(b => {
    b.classList.toggle('active', b.dataset.period === _heatmapPeriod);
  });

  const container = document.getElementById('heatmap-container');
  if (!container) return;
  container.innerHTML = _skeletonCards(12, 'Loading sector heatmap…');

  try {
    const market = typeof currentMarket !== 'undefined' ? currentMarket : 'India';
    const res = await fetch(`${API}/api/sectors/heatmap?market=${market}&period=${_heatmapPeriod}`);
    if (!res.ok) throw new Error('HTTP ' + res.status);
    const data = await res.json();
    _heatmapData = data;
    _renderTreemap(data);
  } catch (e) {
    container.innerHTML = `<div style="text-align:center;padding:40px;color:var(--red)">Error: ${e.message}</div>`;
  }
}

function _renderTreemap(data) {
  const container = document.getElementById('heatmap-container');
  if (!container || !data.sectors || !data.sectors.length) {
    if (container) container.innerHTML = '<div style="text-align:center;padding:40px;color:var(--text3)">No sector data available</div>';
    return;
  }

  const width = container.clientWidth || 800;
  const height = Math.max(400, Math.min(width * 0.55, 550));
  container.innerHTML = '';

  // Build hierarchy
  const root = d3.hierarchy({
    name: 'root',
    children: data.sectors.map(s => ({
      name: s.sector,
      value: Math.max(s.stock_count, 1),
      change: s.avg_change,
      stock_count: s.stock_count,
      pct_above_50: s.pct_above_50dma,
      pct_above_200: s.pct_above_200dma,
      top_stocks: s.top_stocks || [],
      health: s.health,
    }))
  }).sum(d => d.value);

  d3.treemap()
    .size([width, height])
    .padding(2)
    .round(true)(root);

  const svg = d3.select(container)
    .append('svg')
    .attr('width', width)
    .attr('height', height)
    .style('font-family', 'var(--font-mono)')
    .style('cursor', 'default');

  // Tooltip
  let tooltip = document.getElementById('hm-tooltip');
  if (!tooltip) {
    tooltip = document.createElement('div');
    tooltip.id = 'hm-tooltip';
    tooltip.className = 'hm-tooltip';
    document.body.appendChild(tooltip);
  }

  const leaves = svg.selectAll('g')
    .data(root.leaves())
    .join('g')
    .attr('transform', d => `translate(${d.x0},${d.y0})`);

  // Rectangles
  leaves.append('rect')
    .attr('width', d => Math.max(0, d.x1 - d.x0))
    .attr('height', d => Math.max(0, d.y1 - d.y0))
    .attr('fill', d => _hmColor(d.data.change))
    .attr('rx', 3)
    .attr('stroke', 'var(--bg)')
    .attr('stroke-width', 1.5)
    .style('transition', 'opacity 0.15s')
    .on('mouseover', function(event, d) {
      d3.select(this).style('opacity', 0.85);
      const tops = d.data.top_stocks.map(s => `${s.ticker}: ${s.change >= 0 ? '+' : ''}${s.change}%`).join('<br>');
      tooltip.innerHTML = `
        <div style="font-weight:700;font-size:13px;margin-bottom:4px">${d.data.name}</div>
        <div>Change: <span style="font-weight:700;color:${d.data.change >= 0 ? '#22c55e' : '#ef4444'}">${d.data.change >= 0 ? '+' : ''}${d.data.change.toFixed(2)}%</span></div>
        <div>Stocks: ${d.data.stock_count}</div>
        <div>&gt;50 DMA: ${d.data.pct_above_50}%</div>
        <div>&gt;200 DMA: ${d.data.pct_above_200}%</div>
        <div>Health: <span style="color:${d.data.health === 'hot' ? '#22c55e' : d.data.health === 'warm' ? '#f59e0b' : '#ef4444'}">${d.data.health.toUpperCase()}</span></div>
        ${tops ? `<div style="margin-top:4px;border-top:1px solid rgba(255,255,255,.15);padding-top:4px;font-size:10px">Top:<br>${tops}</div>` : ''}
      `;
      tooltip.style.display = 'block';
      tooltip.style.left = (event.pageX + 12) + 'px';
      tooltip.style.top = (event.pageY - 10) + 'px';
    })
    .on('mousemove', function(event) {
      tooltip.style.left = (event.pageX + 12) + 'px';
      tooltip.style.top = (event.pageY - 10) + 'px';
    })
    .on('mouseout', function() {
      d3.select(this).style('opacity', 1);
      tooltip.style.display = 'none';
    });

  // Labels
  leaves.each(function(d) {
    const w = d.x1 - d.x0;
    const h = d.y1 - d.y0;
    const g = d3.select(this);
    const textCol = _hmTextColor(d.data.change);

    if (w > 60 && h > 35) {
      // Full label
      g.append('text')
        .attr('x', (w) / 2)
        .attr('y', (h) / 2 - 6)
        .attr('text-anchor', 'middle')
        .attr('fill', textCol)
        .attr('font-size', Math.min(12, w / 8) + 'px')
        .attr('font-weight', '700')
        .text(d.data.name.length > w / 8 ? d.data.name.substring(0, Math.floor(w / 8)) : d.data.name);

      g.append('text')
        .attr('x', (w) / 2)
        .attr('y', (h) / 2 + 10)
        .attr('text-anchor', 'middle')
        .attr('fill', textCol)
        .attr('font-size', Math.min(11, w / 9) + 'px')
        .attr('opacity', 0.9)
        .text(`${d.data.change >= 0 ? '+' : ''}${d.data.change.toFixed(1)}%`);
    } else if (w > 35 && h > 20) {
      // Abbreviated
      g.append('text')
        .attr('x', (w) / 2)
        .attr('y', (h) / 2 + 4)
        .attr('text-anchor', 'middle')
        .attr('fill', textCol)
        .attr('font-size', '9px')
        .attr('font-weight', '600')
        .text(`${d.data.change >= 0 ? '+' : ''}${d.data.change.toFixed(1)}%`);
    }
  });

  // Date info
  if (data.latest_date) {
    const infoEl = document.getElementById('hm-info');
    if (infoEl) infoEl.textContent = `Data as of ${data.latest_date} · ${data.sectors.length} sectors · ${_heatmapPeriod.toUpperCase()} change`;
  }
}

// Re-render on window resize
window.addEventListener('resize', () => {
  if (_heatmapData && document.getElementById('heatmap-container')?.offsetParent) {
    _renderTreemap(_heatmapData);
  }
});

// ════════════════════════════════════════════════════════════════════════════
// SECTOR ROTATION MAP — RRG-style scatter plot
// ════════════════════════════════════════════════════════════════════════════

let _rotationData = null;

function switchSectorView(view) {
  const hm = document.getElementById('heatmap-container');
  const rm = document.getElementById('rotation-container');
  const pp = document.getElementById('hm-period-group');
  document.getElementById('hm-view-heatmap')?.classList.toggle('active', view === 'heatmap');
  document.getElementById('hm-view-rotation')?.classList.toggle('active', view === 'rotation');

  if (view === 'heatmap') {
    if (hm) hm.style.display = '';
    if (rm) rm.style.display = 'none';
    if (pp) pp.style.display = '';
  } else {
    if (hm) hm.style.display = 'none';
    if (rm) rm.style.display = '';
    if (pp) pp.style.display = 'none';  // period buttons not relevant for rotation
    loadRotationMap();
  }
}

async function loadRotationMap() {
  const container = document.getElementById('rotation-container');
  if (!container) return;
  container.innerHTML = '<div style="text-align:center;padding:40px;color:var(--text3)">Loading rotation map...</div>';

  try {
    const market = typeof currentMarket !== 'undefined' ? currentMarket : 'India';
    const res = await fetch(`${API}/api/sectors/rotation?market=${market}`);
    if (!res.ok) throw new Error('HTTP ' + res.status);
    const data = await res.json();
    if (data.error) throw new Error(data.error);
    _rotationData = data;
    _renderRotationMap(data);
  } catch (e) {
    container.innerHTML = `<div style="text-align:center;padding:40px;color:var(--red)">Error: ${e.message}</div>`;
  }
}

function _renderRotationMap(data) {
  const container = document.getElementById('rotation-container');
  if (!container || !data.sectors || !data.sectors.length) return;

  const W = container.clientWidth || 800;
  const H = Math.max(420, Math.min(W * 0.55, 520));
  const margin = { top: 40, right: 30, bottom: 50, left: 60 };
  const w = W - margin.left - margin.right;
  const h = H - margin.top - margin.bottom;
  container.innerHTML = '';

  const sectors = data.sectors;
  const medRS = data.median_rs || 50;
  const medPerf = data.median_perf || 0;

  // Scales
  const rsMin = Math.max(0, Math.min(...sectors.map(s => s.rs)) - 5);
  const rsMax = Math.min(100, Math.max(...sectors.map(s => s.rs)) + 5);
  const perfMin = Math.min(...sectors.map(s => s.perf)) - 0.5;
  const perfMax = Math.max(...sectors.map(s => s.perf)) + 0.5;
  const sizeMax = Math.max(...sectors.map(s => s.stock_count));

  const xScale = d3.scaleLinear().domain([rsMin, rsMax]).range([0, w]);
  const yScale = d3.scaleLinear().domain([perfMin, perfMax]).range([h, 0]);
  const rScale = d3.scaleSqrt().domain([1, sizeMax]).range([6, 28]);

  const quadColor = q => q === 'Leading' ? '#22c55e' : q === 'Weakening' ? '#f59e0b' : q === 'Improving' ? '#a855f7' : '#ef4444';

  const svg = d3.select(container)
    .append('svg')
    .attr('width', W)
    .attr('height', H)
    .style('font-family', 'var(--font-mono)');

  const g = svg.append('g').attr('transform', `translate(${margin.left},${margin.top})`);

  // Quadrant backgrounds (subtle)
  const cx = xScale(medRS), cy = yScale(medPerf);
  g.append('rect').attr('x', cx).attr('y', 0).attr('width', w - cx).attr('height', cy)
    .attr('fill', 'rgba(34,197,94,0.04)');  // Leading (top-right)
  g.append('rect').attr('x', cx).attr('y', cy).attr('width', w - cx).attr('height', h - cy)
    .attr('fill', 'rgba(245,158,11,0.04)');  // Weakening (bottom-right)
  g.append('rect').attr('x', 0).attr('y', 0).attr('width', cx).attr('height', cy)
    .attr('fill', 'rgba(168,85,247,0.04)');  // Improving (top-left)
  g.append('rect').attr('x', 0).attr('y', cy).attr('width', cx).attr('height', h - cy)
    .attr('fill', 'rgba(239,68,68,0.04)');  // Lagging (bottom-left)

  // Crosshair lines
  g.append('line').attr('x1', cx).attr('x2', cx).attr('y1', 0).attr('y2', h)
    .attr('stroke', 'rgba(255,255,255,0.15)').attr('stroke-dasharray', '4,4');
  g.append('line').attr('x1', 0).attr('x2', w).attr('y1', cy).attr('y2', cy)
    .attr('stroke', 'rgba(255,255,255,0.15)').attr('stroke-dasharray', '4,4');

  // Quadrant labels
  const qlStyle = 'font-size:10px;font-weight:700;letter-spacing:0.08em';
  g.append('text').attr('x', w - 5).attr('y', 14).attr('text-anchor', 'end')
    .attr('fill', '#22c55e').attr('style', qlStyle).text('● Leading');
  g.append('text').attr('x', w - 5).attr('y', h - 6).attr('text-anchor', 'end')
    .attr('fill', '#f59e0b').attr('style', qlStyle).text('● Weakening');
  g.append('text').attr('x', 5).attr('y', 14)
    .attr('fill', '#a855f7').attr('style', qlStyle).text('● Improving');
  g.append('text').attr('x', 5).attr('y', h - 6)
    .attr('fill', '#ef4444').attr('style', qlStyle).text('● Lagging');

  // Axes
  g.append('g').attr('transform', `translate(0,${h})`).call(d3.axisBottom(xScale).ticks(6))
    .selectAll('text').attr('fill', 'var(--text3)').attr('font-size', '10px');
  g.append('g').call(d3.axisLeft(yScale).ticks(6).tickFormat(d => d.toFixed(1) + '%'))
    .selectAll('text').attr('fill', 'var(--text3)').attr('font-size', '10px');
  g.selectAll('.domain, .tick line').attr('stroke', 'rgba(255,255,255,0.1)');

  // Axis labels
  svg.append('text').attr('x', W / 2).attr('y', H - 8)
    .attr('text-anchor', 'middle').attr('fill', 'var(--text3)')
    .attr('font-size', '10px').attr('font-family', 'var(--font-mono)')
    .text('RS (% Above 50 DMA) →');
  svg.append('text').attr('x', 14).attr('y', H / 2)
    .attr('text-anchor', 'middle').attr('fill', 'var(--text3)')
    .attr('font-size', '10px').attr('font-family', 'var(--font-mono)')
    .attr('transform', `rotate(-90, 14, ${H/2})`)
    .text('15D Performance % →');

  // Title
  svg.append('text').attr('x', margin.left + 4).attr('y', 18)
    .attr('fill', 'var(--text)').attr('font-size', '14px').attr('font-weight', '800')
    .attr('font-family', 'var(--font-mono)').attr('letter-spacing', '0.06em')
    .text('◎ Sector Rotation Map');
  svg.append('text').attr('x', margin.left + 210).attr('y', 18)
    .attr('fill', 'var(--text3)').attr('font-size', '10px')
    .text(`Upper Right: Leading | Ext. Left: Lagging`);

  // Tooltip
  let tooltip = document.getElementById('hm-tooltip');
  if (!tooltip) {
    tooltip = document.createElement('div');
    tooltip.id = 'hm-tooltip';
    tooltip.className = 'hm-tooltip';
    document.body.appendChild(tooltip);
  }

  // Bubbles
  const bubbles = g.selectAll('circle.rrg-bubble')
    .data(sectors)
    .join('circle')
    .attr('class', 'rrg-bubble')
    .attr('cx', d => xScale(d.rs))
    .attr('cy', d => yScale(d.perf))
    .attr('r', d => rScale(d.stock_count))
    .attr('fill', d => quadColor(d.quadrant))
    .attr('fill-opacity', 0.65)
    .attr('stroke', d => quadColor(d.quadrant))
    .attr('stroke-width', 1.5)
    .style('cursor', 'pointer')
    .style('transition', 'r 0.2s, fill-opacity 0.2s')
    .on('mouseover', function(event, d) {
      d3.select(this).attr('fill-opacity', 0.9).attr('r', rScale(d.stock_count) + 3);
      tooltip.innerHTML = `
        <div style="font-weight:700;font-size:14px;margin-bottom:4px">${d.sector}</div>
        <div>Perf: <span style="font-weight:700;color:${d.perf>=0?'#22c55e':'#ef4444'}">${d.perf>=0?'+':''}${d.perf.toFixed(2)}%</span></div>
        <div>RSI: <span style="font-weight:700">${d.rs}</span></div>
        <div>Vol Ratio: <span style="font-weight:700;color:var(--cyan)">${d.vol_ratio}x</span></div>
        <div style="margin-top:4px;font-size:10px;color:${quadColor(d.quadrant)}">${d.quadrant.toUpperCase()} · ${d.stock_count} stocks</div>
      `;
      tooltip.style.display = 'block';
      tooltip.style.left = (event.pageX + 14) + 'px';
      tooltip.style.top = (event.pageY - 10) + 'px';
    })
    .on('mousemove', function(event) {
      tooltip.style.left = (event.pageX + 14) + 'px';
      tooltip.style.top = (event.pageY - 10) + 'px';
    })
    .on('mouseout', function(event, d) {
      d3.select(this).attr('fill-opacity', 0.65).attr('r', rScale(d.stock_count));
      tooltip.style.display = 'none';
    });

  // Labels on larger bubbles
  g.selectAll('text.rrg-label')
    .data(sectors.filter(d => rScale(d.stock_count) >= 14))
    .join('text')
    .attr('class', 'rrg-label')
    .attr('x', d => xScale(d.rs))
    .attr('y', d => yScale(d.perf) + 3)
    .attr('text-anchor', 'middle')
    .attr('fill', '#fff')
    .attr('font-size', '8px')
    .attr('font-weight', '700')
    .attr('pointer-events', 'none')
    .text(d => d.sector.length > 12 ? d.sector.slice(0, 10) + '..' : d.sector);

  // Info
  const infoEl = document.getElementById('hm-info');
  if (infoEl) infoEl.textContent = `${data.latest_date} · ${sectors.length} sectors · 15D return vs RS`;
}

// Re-render rotation on resize
window.addEventListener('resize', () => {
  if (_rotationData && document.getElementById('rotation-container')?.offsetParent) {
    _renderRotationMap(_rotationData);
  }
});
