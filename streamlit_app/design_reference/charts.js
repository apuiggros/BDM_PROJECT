/* Lightweight chart rendering for the Exploitation mockup.
   Muted Dabang-style bars + a streaming area/line. No deps. */

(function () {
  const C = {
    blue: '#5A9BF6', green: '#3FCF8E', yellow: '#F4C44C',
    purple: '#8C7CF8', coral: '#F2728C', teal: '#3FC2C2'
  };

  /* ---- vertical bar chart ---- */
  function vbars(el) {
    const data = JSON.parse(el.dataset.bars);   // [{x,v}]
    const color = el.dataset.color || C.blue;
    const max = Math.max(...data.map(d => d.v));
    el.innerHTML = data.map(d => `
      <div class="bar-col">
        <div class="bar-track">
          <div class="bar" style="height:${Math.max(6,(d.v/max)*100)}%;background:${color}" title="${d.x}: ${d.v}"></div>
        </div>
        <div class="bar-x">${d.x}</div>
      </div>`).join('');
  }

  /* ---- grouped (2-series) vertical bars ---- */
  function gbars(el) {
    const data = JSON.parse(el.dataset.gbars); // [{x, a, b}]
    const ca = el.dataset.ca || C.blue, cb = el.dataset.cb || C.green;
    const max = Math.max(...data.flatMap(d => [d.a, d.b]));
    el.innerHTML = data.map(d => `
      <div class="bar-col">
        <div class="bar-track">
          <div class="bar" style="height:${Math.max(6,(d.a/max)*100)}%;background:${ca};max-width:13px"></div>
          <div class="bar" style="height:${Math.max(6,(d.b/max)*100)}%;background:${cb};max-width:13px"></div>
        </div>
        <div class="bar-x">${d.x}</div>
      </div>`).join('');
  }

  /* ---- horizontal ranked bars ---- */
  function hbars(el) {
    const data = JSON.parse(el.dataset.hbars); // [{l,v,c?}]
    const max = Math.max(...data.map(d => d.v));
    el.innerHTML = data.map(d => `
      <div class="hbar">
        <div class="hl">${d.l}</div>
        <div class="ht"><div class="hf" style="width:${(d.v/max)*100}%;background:${d.c||C.purple}"></div></div>
        <div class="hv">${d.v}</div>
      </div>`).join('');
  }

  /* ---- streaming area + line ---- */
  function area(el) {
    const data = JSON.parse(el.dataset.area); // numbers
    const W = 640, H = 150, pad = 6;
    const max = Math.max(...data) * 1.15, min = 0;
    const step = (W - pad * 2) / (data.length - 1);
    const pts = data.map((v, i) => [pad + i * step, H - pad - ((v - min) / (max - min)) * (H - pad * 2)]);
    const line = pts.map((p, i) => (i ? 'L' : 'M') + p[0].toFixed(1) + ' ' + p[1].toFixed(1)).join(' ');
    const fill = line + ` L${pts[pts.length-1][0].toFixed(1)} ${H-pad} L${pts[0][0].toFixed(1)} ${H-pad} Z`;
    el.innerHTML = `
      <svg viewBox="0 0 ${W} ${H}" preserveAspectRatio="none">
        <defs>
          <linearGradient id="ag" x1="0" y1="0" x2="0" y2="1">
            <stop offset="0" stop-color="${C.purple}" stop-opacity=".28"/>
            <stop offset="1" stop-color="${C.purple}" stop-opacity="0"/>
          </linearGradient>
        </defs>
        <path d="${fill}" fill="url(#ag)"/>
        <path d="${line}" fill="none" stroke="${C.purple}" stroke-width="2.5" stroke-linecap="round" stroke-linejoin="round"/>
        ${pts.filter((_,i)=>i===pts.length-1).map(p=>`<circle cx="${p[0].toFixed(1)}" cy="${p[1].toFixed(1)}" r="4.5" fill="#fff" stroke="${C.purple}" stroke-width="2.5"/>`).join('')}
      </svg>`;
  }

  function render() {
    document.querySelectorAll('[data-bars]').forEach(vbars);
    document.querySelectorAll('[data-gbars]').forEach(gbars);
    document.querySelectorAll('[data-hbars]').forEach(hbars);
    document.querySelectorAll('[data-area]').forEach(area);
  }
  document.addEventListener('DOMContentLoaded', render);
})();
