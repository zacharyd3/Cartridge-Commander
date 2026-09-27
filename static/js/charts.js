'use strict';
// ══════════════════════════════════════════════════════════════════════════════
// CHARTS — donut, legend, column, horizontal bar and line charts as HTML/SVG strings.
// Marks follow one spec: thin bars (≤24px), 4px rounded data ends, 2px surface gaps,
// hairline solid gridlines, hover tooltips; values are always also in a legend/label.
// ══════════════════════════════════════════════════════════════════════════════

// segs: [{v, color, label}] — zero segments are skipped.
function donutChart(segs, {size=148, stroke=18, center='', sub='', aria=''}={}){
  const r = (size - stroke) / 2 - 2;
  const c = size / 2;
  const C = 2 * Math.PI * r;
  const live = segs.filter(s => s.v > 0);
  const total = live.reduce((a,s)=>a+s.v, 0);
  let circles = '';
  if(!total){
    circles = `<circle cx="${c}" cy="${c}" r="${r}" fill="none" stroke="var(--grid)" stroke-width="${stroke}"></circle>`;
  } else {
    let acc = 0;
    const gap = live.length > 1 ? 2 : 0;
    for(const s of live){
      const len = s.v / total * C;
      const d = Math.max(len - gap, 0.8);
      circles += `<circle class="seg" cx="${c}" cy="${c}" r="${r}" fill="none" stroke="${s.color}" stroke-width="${stroke}" stroke-dasharray="${d.toFixed(2)} ${(C-d).toFixed(2)}" stroke-dashoffset="${(-acc).toFixed(2)}"><title>${esc(s.label||'')}: ${esc(s.text ?? s.v)}</title></circle>`;
      acc += len;
    }
  }
  return `<div class="donut" style="width:${size}px;height:${size}px">
    <svg width="${size}" height="${size}" viewBox="0 0 ${size} ${size}" role="img" aria-label="${esc(aria)}"><g transform="rotate(-90 ${c} ${c})">${circles}</g></svg>
    <div class="donut-center"><span class="v">${center}</span>${sub?`<span class="s">${sub}</span>`:''}</div>
  </div>`;
}

// rows: [{color|icon, label, value, pct}]
function legendHTML(rows, {pct=false}={}){
  return `<div class="legend${pct?' pct':''}">` + rows.map(r =>
    `${r.icon ? r.icon : `<span class="sw${r.outline?' outline':''}" style="background:${r.color}"></span>`}<span class="lbl">${r.label}</span><span class="val">${r.value}</span>${pct?`<span class="p">${r.pct??''}</span>`:''}`
  ).join('') + `</div>`;
}

// cols: [{segs:[{v,color}], x, tip:{title, lines:[]}, cap}]; max: y max; ticks: [values]
function columnChart(cols, {max, ticks, fmtTick=v=>v, height=200, barWidth=24, aria=''}={}){
  if(!max || max <= 0) max = 1;
  const y = v => height - (v / max) * height;
  const yaxis = ticks.map(t => `<span style="top:${y(t).toFixed(1)}px">${fmtTick(t)}</span>`).join('');
  const grid = ticks.map((t,i) => `<span class="gl${t===0?' base':''}" style="top:${Math.min(y(t),height-1).toFixed(1)}px"></span>`).join('');
  const body = cols.map(c => {
    const tot = c.segs.reduce((a,s)=>a+s.v,0);
    const h = Math.max(0, Math.min(100, tot / max * 100));
    const segs = c.segs.filter(s=>s.v>0).map(s => `<span style="flex-grow:${s.v};background:${s.color}"></span>`).join('');
    const tip = c.tip ? `<div class="tip"><b>${c.tip.title}</b>${(c.tip.lines||[]).map(l=>`<span>${l}</span>`).join('')}</div>` : '';
    return `<div class="col" tabindex="${c.tip?0:-1}" aria-label="${esc(c.aria||'')}">${tip}${c.cap?`<span class="cap">${c.cap}</span>`:''}<span class="bar" style="height:${h.toFixed(2)}%;width:${barWidth}px">${segs}</span></div>`;
  }).join('');
  const xaxis = cols.map(c => `<span>${c.x ?? ''}</span>`).join('');
  return `<div class="colchart" role="img" aria-label="${esc(aria)}">
    <div class="yaxis" style="height:${height}px">${yaxis}</div>
    <div class="plot" style="height:${height}px">${grid}<div class="cols">${body}</div></div>
    <span></span><div class="xaxis">${xaxis}</div>
  </div>`;
}

// Nice tick values for a column chart axis.
function niceTicks(maxVal, count=3){
  if(!(maxVal > 0)) return {max:1, ticks:[0,1]};
  const raw = maxVal / count;
  const mag = Math.pow(10, Math.floor(Math.log10(raw)));
  const step = [1,2,2.5,5,10].map(m=>m*mag).find(s => s >= raw) || raw;
  const max = Math.ceil(maxVal / step) * step;
  const ticks = [];
  for(let v=0; v<=max+1e-9; v+=step) ticks.push(+v.toFixed(6));
  return {max, ticks};
}

// rows: [{label, v, text, mono}]
function hbarChart(rows, {max, labelWidth=120, maxWidth=78}={}){
  max = max || Math.max(1, ...rows.map(r=>r.v));
  return `<div class="hbars" style="--hb-label:${labelWidth}px">` + rows.map(r =>
    `<div class="hbar" title="${esc(r.label)}: ${esc(r.text)}"><span class="l${r.mono?' mono':''}">${esc(r.label)}</span><span class="t"><span class="b" style="width:${(r.v/max*maxWidth).toFixed(1)}%"></span><span class="v">${esc(r.text)}</span></span></div>`
  ).join('') + `</div>`;
}

// samples: [{t (ms), v}] — single series line with 10% area wash, hover crosshair per sample.
function lineChart(samples, {width=1000, height=170, max, fmt=v=>v, ticks=[], color='var(--s1)', aria=''}={}){
  if(samples.length < 2) return `<div class="chart-empty" style="min-height:${height}px">Collecting samples…</div>`;
  const t0 = samples[0].t, t1 = samples[samples.length-1].t;
  const span = Math.max(t1 - t0, 1);
  const X = t => (t - t0) / span * width;
  const Y = v => height - Math.min(v, max) / max * height;
  let d = '';
  samples.forEach((s,i) => { d += (i?' L ':'M ') + X(s.t).toFixed(1) + ' ' + Y(s.v).toFixed(1); });
  const area = d + ` L ${width} ${height} L 0 ${height} Z`;
  const grid = ticks.map(t => `<line x1="0" y1="${Y(t).toFixed(1)}" x2="${width}" y2="${Y(t).toFixed(1)}" stroke="${t===0?'var(--axis)':'var(--grid)'}" stroke-width="1"></line>`).join('');
  const last = samples[samples.length-1];
  const step = Math.max(1, Math.floor(samples.length / 120));
  let hits = '';
  const bw = width / samples.length;
  for(let i=0;i<samples.length;i+=step){
    const s = samples[i], x = X(s.t);
    hits += `<g><rect x="${(x-bw*step/2).toFixed(1)}" y="0" width="${(bw*step).toFixed(1)}" height="${height}"></rect><line x1="${x.toFixed(1)}" y1="0" x2="${x.toFixed(1)}" y2="${height}" stroke-width="1" vector-effect="non-scaling-stroke"></line><title>${new Date(s.t).toLocaleTimeString([], {hour:'2-digit',minute:'2-digit',second:'2-digit'})} · ${fmt(s.v)}</title></g>`;
  }
  const yl = ticks.map(t => `<span style="position:absolute;right:10px;top:${(Y(t)-7).toFixed(1)}px">${Math.round(t)}</span>`).join('');
  const xl = [0,.2,.4,.6,.8,1].map(f => `<span>${new Date(t0 + span*f).toLocaleTimeString([], {hour:'2-digit',minute:'2-digit'})}</span>`).join('');
  return `<div style="display:grid;grid-template-columns:52px minmax(0,1fr)">
    <div style="position:relative;height:${height}px;font-size:11px;color:var(--muted)" class="tnum">${yl}</div>
    <div class="linechart" role="img" aria-label="${esc(aria)}" style="position:relative">
      <span class="tnum" style="position:absolute;right:0;top:${Math.max(0,Y(last.v)-22).toFixed(1)}px;font-size:11.5px;font-weight:600;background:var(--panel);padding:0 4px">${fmt(last.v)}</span>
      <svg width="100%" height="${height}" viewBox="0 0 ${width} ${height}" preserveAspectRatio="none">
        ${grid}
        <path d="${area}" fill="${color}" fill-opacity="0.1"></path>
        <path d="${d}" fill="none" stroke="${color}" stroke-width="2" stroke-linejoin="round" stroke-linecap="round" vector-effect="non-scaling-stroke"></path>
        <g class="hit">${hits}</g>
      </svg>
      <div class="xaxis">${xl}</div>
    </div>
  </div>`;
}
