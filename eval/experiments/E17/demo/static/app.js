"use strict";
// Repair Lab front end: pick a DAG, run it undisrupted, draw disruptions
// (link cuts, jammers) on the map and a time window on the baseline chart,
// then re-run it under one repair strategy. Hovering either chart shows that
// moment on the map.

const SVGNS = "http://www.w3.org/2000/svg";
const $ = (s) => document.querySelector(s);
const STRATS = [
  { id: "wait", name: "Wait", desc: "No repair. Transfers on a downed link stall until it recovers." },
  { id: "reroute", name: "Reroute", desc: "No repair. Traffic detours through a relay node." },
  { id: "local", name: "Local repair", desc: "Re-plan the tasks the broken link reaches." },
  { id: "global", name: "Global repair", desc: "Re-plan every task that has not started." },
];
const HW_SHORT = { "UP board": "UP board", "Raspberry Pi 4": "Pi 4", "Raspberry Pi 3": "Pi 3" };
const NODE_FILL = { "UP board": "var(--node-up)", "Raspberry Pi 4": "var(--node-pi4)", "Raspberry Pi 3": "var(--node-pi3)" };
const NODE_TEXT = { "UP board": "var(--node-text-dark)", "Raspberry Pi 4": "var(--node-text-dark)", "Raspberry Pi 3": "var(--node-text-light)" };

const S = {
  lab: null, scenarios: [], scen: null, structure: null, ccr: null,
  runs: [], shownId: null,
  drafts: [], pending: null, tool: null, strategy: "global",
  cursor: null,          // {t, chart: "baseline" | "disrupted"}
  jamDraft: null,        // jammer being drawn on the map
  hoverTask: null,       // task under the pointer, in the graph or a chart
  drag: null,            // window being dragged on the baseline chart
};

// ------------------------------------------------------------------ utils
async function api(path, opts) {
  const r = await fetch("api/" + path, opts);
  const body = await r.json().catch(() => ({}));
  if (!r.ok) throw new Error(body.error || r.statusText);
  return body;
}
function el(tag, attrs, parent) {
  const e = document.createElementNS(SVGNS, tag);
  for (const [k, v] of Object.entries(attrs || {})) if (v !== undefined && v !== null) e.setAttribute(k, v);
  if (parent) parent.appendChild(e);
  return e;
}
function html(tag, cls, text) {
  const e = document.createElement(tag);
  if (cls) e.className = cls;
  if (text !== undefined) e.textContent = text;
  return e;
}
const fmtS = (t) => (t == null ? "–" : `${t.toFixed(1)} s`);
const fmtMB = (b) => `${(b / 1e6).toFixed(b < 1e7 ? 2 : 1)} MB`;
let toastTimer = null;
function toast(msg) {
  const t = $("#toast");
  t.textContent = msg;
  t.classList.add("show");
  clearTimeout(toastTimer);
  toastTimer = setTimeout(() => t.classList.remove("show"), 3800);
}
function tip(evt, htmlText) {
  const t = $("#tip");
  if (!htmlText) { t.style.display = "none"; return; }
  t.innerHTML = htmlText;
  t.style.display = "block";
  const pad = 14, w = t.offsetWidth, h = t.offsetHeight;
  let x = evt.clientX + pad, y = evt.clientY + pad;
  if (x + w > window.innerWidth - 8) x = evt.clientX - w - pad;
  if (y + h > window.innerHeight - 8) y = evt.clientY - h - pad;
  t.style.left = `${x}px`;
  t.style.top = `${y}px`;
}
const esc = (s) => String(s).replace(/[&<>"]/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));

// ------------------------------------------------------- disruption logic
// Same rule as the server (and the paper): a radio pair is down while a cut
// on it, or a jammer with one of its endpoints within range, is active.
function nodePos(id) { const n = S.scen.nodes.find((m) => m.id === id); return [n.x, n.y]; }
function radioPairs() { return S.scen.pairs.filter((p) => !p.wired); }
function pairsHit(d) {
  if (d.type === "link") return [[d.a, d.b].sort().join("-")];
  return radioPairs().filter((p) => {
    const [ax, ay] = nodePos(p.a), [bx, by] = nodePos(p.b);
    return Math.hypot(ax - d.x, ay - d.y) <= d.r || Math.hypot(bx - d.x, by - d.y) <= d.r;
  }).map((p) => `${p.a}-${p.b}`);
}
function labels(list) {
  let l = 0, j = 0;
  return list.map((d) => (d.type === "link" ? `L${++l}` : `J${++j}`));
}
function describe(d) {
  if (d.type === "link") return `Link ${d.a}–${d.b} cut`;
  const k = pairsHit(d).length;
  return `Jammer, radius ${d.r.toFixed(0)} (${k} radio link${k === 1 ? "" : "s"})`;
}

// Measured rate (Mbit/s) of one series at time t: the sample nearest t, if
// it is within a second.
function rateAt(series, t) {
  let best = null;
  for (const [ts, v] of series || []) if (Math.abs(ts - t) <= 1 && (!best || Math.abs(ts - t) < Math.abs(best[0] - t))) best = [ts, v];
  return best ? best[1] : 0;
}
// Spans where a measured data series moved: consecutive half-second samples
// with data join into one span [from, to, MB].
const SAMPLE = 0.5;
function activeSpans(series) {
  const out = [];
  for (const [t, v] of series || []) {
    if (v <= 0) continue;
    const mb = (v * SAMPLE) / 8, last = out[out.length - 1];
    if (last && t - SAMPLE / 2 <= last[1] + SAMPLE / 2) { last[1] = t + SAMPLE / 2; last[2] += mb; }
    else out.push([t - SAMPLE / 2, t + SAMPLE / 2, mb]);
  }
  return out;
}

// ------------------------------------------------------------- run views
function runsOf(kind) { return S.runs.filter((r) => r.kind === kind); }
function baselineRun() {
  const b = runsOf("baseline");
  const live = b.find((r) => r.status === "running" || r.status === "queued");
  return live || b.find((r) => r.status === "done") || b[0] || null;
}
function doneBaseline() { return runsOf("baseline").find((r) => r.status === "done") || null; }
function shownRun() {
  const d = runsOf("disrupted");
  return d.find((r) => r.id === S.shownId) || d[0] || null;
}
function busy() { return S.runs.some((r) => r.status === "running" || r.status === "queued"); }
function data(run) {
  if (!run) return null;
  if (run.status === "running" && run.live) {
    // Moved: started, or planned to start, somewhere other than the initial plan.
    const moved = {};
    for (const p of run.live.plans || []) for (const [t, mv] of Object.entries(p.moves || {})) moved[t] = mv[1];
    for (const t of run.live.tasks || []) {
      if (t.node && t.start != null) { if (t.node !== S.scen.plan[t.id]) moved[t.id] = t.node; else delete moved[t.id]; }
    }
    for (const [t, n] of Object.entries(moved)) if (n === S.scen.plan[t]) delete moved[t];
    return { tasks: run.live.tasks || [], flows: run.live.flows || [], plans: run.live.plans || [], moved, now: run.live.now, live: true,
      links: run.live.links || {}, relays: run.live.relays || {}, routes: run.live.routes || [] };
  }
  if (run.result) return { ...run.result, moved: run.result.moved || {}, now: null, live: false };
  return null;
}
function endOf(d) {
  if (!d) return 0;
  let m = d.now || 0;
  for (const t of d.tasks) m = Math.max(m, t.end || t.start || 0);
  for (const f of d.flows) m = Math.max(m, f.end || 0);
  return m;
}
function bytesOf(d) { return (d ? d.flows : []).reduce((s, f) => s + (f.bytes || 0), 0); }

// ------------------------------------------------------------------ setup
async function init() {
  STRATS.forEach((s) => {
    const b = html("button", "strategy");
    b.setAttribute("role", "radio");
    b.dataset.id = s.id;
    b.append(html("b", null, s.name), html("span", null, s.desc));
    b.onclick = () => { S.strategy = s.id; renderControls(); };
    $("#strategies").append(b);
  });
  document.querySelectorAll(".tool").forEach((b) => {
    b.onclick = () => {
      const t = b.dataset.tool;
      S.tool = S.tool === t ? null : t;
      S.pending = null;
      renderAll();
    };
  });
  $("#run-baseline").onclick = runBaseline;
  $("#all-radio").onclick = () => setWired([], "Every link is radio now.");
  $("#run-disrupted").onclick = runDisrupted;
  document.addEventListener("keydown", (e) => {
    if (e.key === "Escape") { S.tool = null; S.pending = null; S.jamDraft = null; renderAll(); }
  });
  window.addEventListener("resize", () => { renderCharts(); renderDag(); });

  S.lab = await api("state");
  $("#lane").replaceChildren(...S.lab.lane.map((n) => {
    const s = html("span");
    s.append(`${n.id} `, html("code", null, n.name), ` ${HW_SHORT[n.hw]}`);
    return s;
  }));
  S.scenarios = await api("scenarios");
  const byFamily = {};
  for (const s of S.scenarios) (byFamily[s.family] ||= new Map()).set(s.structure, s);
  const sel = $("#dag");
  for (const [fam, m] of Object.entries(byFamily)) {
    const g = document.createElement("optgroup");
    g.label = fam;
    for (const st of m.keys()) {
      const radio = S.scenarios.filter((x) => x.structure === st).map((x) => x.radio);
      const none = radio.every((r) => !r);
      g.append(new Option(`${st.replace(/_/g, " ")}${none ? " (no radio transfers)" : ""}`, st));
    }
    sel.append(g);
  }
  sel.onchange = () => pickScenario(sel.value, S.ccr);
  // Default: a small DAG whose plan uses the radio links (in-tree at CCR 5).
  const first = S.scenarios.find((s) => s.structure === "in_tree" && s.ccr === 5) || S.scenarios[0];
  sel.value = first.structure;
  await pickScenario(first.structure, first.ccr);
  setInterval(poll, 1000);
}

async function pickScenario(structure, ccr, quiet) {
  const choices = S.scenarios.filter((s) => s.structure === structure);
  const pick = choices.find((s) => s.ccr === ccr) || choices[0];
  if (!quiet && S.scen && S.scen.id !== pick.id && S.drafts.length) toast("Disruptions cleared: their windows belonged to the other DAG's baseline.");
  if (!S.scen || S.scen.id !== pick.id) { S.drafts = []; S.pending = null; S.shownId = null; }
  S.structure = structure;
  S.ccr = pick.ccr;
  const seg = $("#ccr");
  seg.replaceChildren(...choices.map((c) => {
    const b = html("button", null, String(c.ccr));
    b.title = `CCR ${c.ccr}: ${c.radio} radio transfer${c.radio === 1 ? "" : "s"} in its plan on these links`;
    if (!c.radio) b.style.color = "var(--faint)";
    b.setAttribute("role", "radio");
    b.setAttribute("aria-checked", String(c.ccr === pick.ccr));
    b.onclick = () => pickScenario(structure, c.ccr);
    return b;
  }));
  S.scen = await api(`scenarios/${pick.id}`);
  renderFacts();
  S.runs = await api(`runs?scenario=${encodeURIComponent(pick.id)}`);
  renderAll();
}

function reconfiguring() { return !!(S.lab && S.lab.reconfiguring); }

function renderFacts() {
  const f = $("#dag-facts");
  f.replaceChildren(`${S.scen.tasks.length} tasks · ${S.scen.edges.length} edges · ${S.scen.family}. ` +
    `On these links its plan sends ${S.scen.cross} of its edges across the network, ${S.scen.radio} over radio.`);
  if (!S.scen.radio) {
    f.append(html("span", "warn", S.scen.cross
      ? "None of its transfers use a radio link, so no disruption can affect it. Pick another DAG or CCR, or make fewer links wired."
      : "Its plan keeps everything on one node, so no disruption can affect it. Pick another DAG or CCR."));
  }
}

async function setWired(pairs, what) {
  try {
    S.lab = { ...(S.lab || {}), reconfiguring: true };
    renderControls();
    await api("topology", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify({ wired: pairs }) });
    S.drafts = []; S.pending = null; S.shownId = null;
    S.lab = await api("state");
    S.scenarios = await api("scenarios");
    await pickScenario(S.structure, S.ccr, true);
    toast(`${what} Each set of links has its own plan and baselines.`);
  } catch (e) {
    toast(e.message);
    S.lab = await api("state").catch(() => S.lab);
    renderAll();
  }
}

async function poll() {
  try {
    S.lab = await api("state");
    if (S.scen) {
      const before = S.runs.map((r) => r.status).join();
      S.runs = await api(`runs?scenario=${encodeURIComponent(S.scen.id)}`);
      const after = S.runs.map((r) => r.status).join();
      if (before !== after) {
        const fin = S.runs.find((r) => r.status === "failed" && r.finished && Date.now() / 1000 - r.finished < 3);
        if (fin) toast(`Run ${fin.id} failed: ${fin.error}`);
      }
    }
    if (!S.drag && !S.jamDraft) renderAll();
    else renderStatus();
  } catch (e) {
    renderStatus(e.message);
  }
}

async function runBaseline() {
  try {
    const r = await api("runs", { method: "POST", headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ scenario: S.scen.id, kind: "baseline" }) });
    S.runs.unshift(r);
    toast("Baseline queued. It runs the DAG undisrupted on the lane.");
    renderAll();
  } catch (e) { toast(e.message); }
}

async function runDisrupted() {
  try {
    const body = { scenario: S.scen.id, kind: "disrupted", strategy: S.strategy,
      disruptions: S.drafts.map(({ type, a, b, x, y, r, start, end }) => ({ type, a, b, x, y, r, start, end })) };
    const r = await api("runs", { method: "POST", headers: { "Content-Type": "application/json" }, body: JSON.stringify(body) });
    S.runs.unshift(r);
    S.shownId = r.id;
    toast(`${STRATS.find((s) => s.id === S.strategy).name} run queued.`);
    renderAll();
  } catch (e) { toast(e.message); }
}

// ---------------------------------------------------------------- render
function renderAll() {
  renderStatus();
  renderControls();
  renderList();
  renderMap();
  renderDag();
  renderCharts();
  renderHistory();
}

function renderStatus(err) {
  const box = $("#lab-status");
  box.replaceChildren();
  const dot = html("span", "dot");
  let text;
  if (err) text = `Lab unreachable: ${err}`;
  else if (S.lab && S.lab.active) { dot.classList.add("busy"); text = `Running ${S.lab.active}${S.lab.queued ? `, ${S.lab.queued} queued` : ""}`; }
  else { dot.classList.add("idle"); text = "Lane idle"; }
  box.append(dot, text);
}

function renderControls() {
  const base = doneBaseline();
  $("#run-baseline").disabled = !S.scen || busy();
  $("#run-baseline").textContent = base ? "Run baseline again" : "Run baseline";
  $("#run-disrupted").disabled = !base || !S.drafts.length || busy();
  document.querySelectorAll(".tool").forEach((b) => {
    b.disabled = b.dataset.tool === "wire" ? busy() || reconfiguring() : !base;
    b.setAttribute("aria-pressed", String(S.tool === b.dataset.tool));
  });
  const wiredNow = S.scen ? S.scen.pairs.filter((p) => p.wired) : [];
  $("#topo-label").innerHTML = wiredNow.length
    ? `Wired: <b>${wiredNow.map((p) => `${p.a}–${p.b}`).join(", ")}</b>; every other link is radio.`
    : "Every link is <b>radio</b> (emulated).";
  $("#all-radio").hidden = !wiredNow.length;
  $("#all-radio").disabled = busy() || reconfiguring();
  document.querySelectorAll(".strategy").forEach((b) => b.setAttribute("aria-checked", String(b.dataset.id === S.strategy)));
  const hint = $("#hint");
  const lab = S.pending ? labels([...S.drafts, S.pending]).pop() : null;
  if (reconfiguring()) hint.innerHTML = "Rebuilding the lane's links…";
  else if (S.tool === "wire") hint.innerHTML = busy() ? "Wait for the run in progress to finish before changing the links."
    : "Click a link to switch it between <b>radio</b> and <b>wired</b>. Each set of links gets its own plan and baseline.";
  else if (!base) hint.innerHTML = "Run the baseline first: its chart is where disruption windows are drawn.";
  else if (S.pending && S.pending.type === "link") hint.innerHTML = `Now drag across the <b>baseline chart</b> to choose when <b>${S.pending.a}–${S.pending.b}</b> is cut.`;
  else if (S.pending && !pairsHit(S.pending).length) hint.innerHTML = `<b>${lab}</b> reaches no node. A jammer silences the radios of the nodes inside its circle; place it over a node or drag a larger radius.`;
  else if (S.pending) hint.innerHTML = `Now drag across the <b>baseline chart</b> to choose when <b>${lab}</b> jams ${pairsHit(S.pending).length} radio link(s).`;
  else if (S.tool === "link") hint.innerHTML = "Click a <b>radio</b> link on the map. Wired links are never disrupted.";
  else if (S.tool === "jammer") hint.innerHTML = "Press where the jammer sits and drag outward to set its radius. It silences the radios of the nodes inside the circle.";
  else hint.innerHTML = "Hover a chart to see that moment on the map.";
}

function renderList() {
  const ul = $("#dlist");
  ul.replaceChildren();
  if (!S.drafts.length) { ul.append(html("li", "empty", "None yet. Use Cut link or Jammer.")); return; }
  const labs = labels(S.drafts);
  S.drafts.forEach((d, i) => {
    const li = html("li");
    const what = html("div", "what", describe(d));
    what.append(html("small", null, `${fmtS(d.start)} → ${fmtS(d.end)}`));
    const x = html("button", null, "×");
    x.title = "Remove";
    x.setAttribute("aria-label", `Remove ${labs[i]}`);
    x.onclick = () => { S.drafts.splice(i, 1); renderAll(); };
    li.append(html("span", "chip", labs[i]), what, x);
    ul.append(li);
  });
}

// -------------------------------------------------------------------- map
function mapContext() {
  // Which run and time the map shows, and which disruptions it draws.
  const base = baselineRun(), shown = shownRun();
  if (S.cursor) {
    const run = S.cursor.chart === "baseline" ? base : shown;
    const ds = S.cursor.chart === "baseline" ? S.drafts : (shown ? shown.disruptions : []);
    return { run, t: S.cursor.t, ds, real: S.cursor.chart !== "baseline" };
  }
  if (shown && shown.status === "running" && shown.live) return { run: shown, t: shown.live.now, ds: shown.disruptions, real: true };
  if (base && base.status === "running" && base.live) return { run: base, t: base.live.now, ds: S.drafts, real: false };
  return { run: null, t: null, ds: S.drafts, real: false };
}

function renderMap() {
  const svg = $("#map");
  svg.replaceChildren();
  svg.setAttribute("class", S.tool ? `tool-${S.tool}` : "");
  if (!S.scen) return;
  const xs = S.scen.nodes.map((n) => n.x), ys = S.scen.nodes.map((n) => n.y);
  const pad = 15;
  const cx = (Math.min(...xs) + Math.max(...xs)) / 2, cy = (Math.min(...ys) + Math.max(...ys)) / 2;
  const span = Math.max(Math.max(...xs) - Math.min(...xs), Math.max(...ys) - Math.min(...ys)) + 2 * pad;
  svg.setAttribute("viewBox", `${cx - span / 2} ${cy - span / 2} ${span} ${span}`);
  const R = span * 0.052, fs = span * 0.032;
  const defs = el("defs", {}, svg);
  const mk = el("marker", { id: "arr", viewBox: "0 0 10 10", refX: 9, refY: 5, markerWidth: 4.5, markerHeight: 4.5, orient: "auto-start-reverse" }, defs);
  el("path", { d: "M0,0 L10,5 L0,10 z", style: "fill:var(--task)" }, mk);
  const mk2 = el("marker", { id: "arr-relay", viewBox: "0 0 10 10", refX: 9, refY: 5, markerWidth: 4.5, markerHeight: 4.5, orient: "auto-start-reverse" }, defs);
  el("path", { d: "M0,0 L10,5 L0,10 z", style: "fill:var(--relay)" }, mk2);

  const ctx = mapContext();
  const at = ctx.t;
  const inWin = (d) => at != null && d.start <= at && at < d.end;
  // Pairs down now (real run) or planned (drafts), and the disruptions active.
  const down = new Set(), planned = new Set();
  ctx.ds.forEach((d) => {
    const hit = pairsHit(d);
    if (at == null) hit.forEach((p) => planned.add(p));
    else if (inWin(d)) hit.forEach((p) => (ctx.real ? down : planned).add(p));
  });
  if (S.pending) pairsHit(S.pending).forEach((p) => planned.add(p));
  if (S.jamDraft && S.jamDraft.r > 0) pairsHit({ type: "jammer", ...S.jamDraft }).forEach((p) => planned.add(p));

  const gLinks = el("g", {}, svg), gJam = el("g", {}, svg), gFlow = el("g", {}, svg), gNodes = el("g", {}, svg);
  for (const p of S.scen.pairs) {
    const [ax, ay] = nodePos(p.a), [bx, by] = nodePos(p.b);
    const key = `${p.a}-${p.b}`;
    let style = p.wired ? "stroke:var(--wired);stroke-width:3.5" : "stroke:var(--radio);stroke-width:1.8;stroke-dasharray:6 5";
    if (planned.has(key)) style = "stroke:var(--jam);stroke-width:3;stroke-dasharray:6 4";
    if (down.has(key)) style = "stroke:var(--down);stroke-width:3";
    el("line", { x1: ax, y1: ay, x2: bx, y2: by, style: `${style};vector-effect:non-scaling-stroke` }, gLinks);
    if (down.has(key)) {
      const t = el("text", { x: (ax + bx) / 2, y: (ay + by) / 2 + fs * 0.35, "text-anchor": "middle", "font-size": fs * 1.25,
        style: "fill:var(--down);font-weight:700" }, gLinks);
      t.textContent = "✕";
    }
    const hit = el("line", { x1: ax, y1: ay, x2: bx, y2: by, class: `link-hit ${p.wired ? "wired" : "radio"}`,
      style: "stroke:transparent;stroke-width:16;vector-effect:non-scaling-stroke" }, gLinks);
    hit.addEventListener("mouseenter", (e) => tip(e, p.wired
      ? `<b>${p.a}–${p.b}</b> wired (switch, unshaped)<br>never disrupted`
      : `<b>${p.a}–${p.b}</b> radio (emulated: 2 Mbit/s, 10 ms ±2 ms, 0.2% loss)`));
    hit.addEventListener("mousemove", (e) => tip(e, $("#tip").innerHTML));
    hit.addEventListener("mouseleave", (e) => tip(e, null));
    hit.addEventListener("click", () => {
      if (S.tool === "wire") {
        if (busy() || reconfiguring()) { toast("Wait for the run in progress to finish before changing the links."); return; }
        const wired = S.scen.pairs.filter((q) => q.wired && !(q.a === p.a && q.b === p.b)).map((q) => [q.a, q.b]);
        if (!p.wired) wired.push([p.a, p.b]);
        setWired(wired, `${p.a}–${p.b} is now ${p.wired ? "radio" : "wired"}.`);
        return;
      }
      if (S.tool !== "link") return;
      if (p.wired) { toast("Wired links are never disrupted, as in the paper: pick a dashed radio link."); return; }
      S.pending = { type: "link", a: p.a, b: p.b };
      renderAll();
    });
  }
  // Jammers: drafts or the shown run's, plus the one pending or being drawn.
  const jams = ctx.ds.map((d, i) => [d, labels(ctx.ds)[i]]).filter(([d]) => d.type === "jammer");
  if (S.pending && S.pending.type === "jammer") jams.push([S.pending, labels([...S.drafts, S.pending]).pop()]);
  if (S.jamDraft && S.jamDraft.r > 0) jams.push([{ type: "jammer", ...S.jamDraft }, `r ${S.jamDraft.r.toFixed(0)}`]);
  for (const [d, lab] of jams) {
    const active = at == null || inWin(d) || d === S.pending;
    el("circle", { cx: d.x, cy: d.y, r: d.r, style: `fill:var(--jam-fill);stroke:var(--jam);stroke-width:1.5;stroke-dasharray:5 4;` +
      `vector-effect:non-scaling-stroke;opacity:${active ? 1 : 0.35}` }, gJam);
    el("circle", { cx: d.x, cy: d.y, r: R * 0.22, style: "fill:var(--jam)" }, gJam);
    const t = el("text", { x: d.x, y: d.y - R * 0.45, "text-anchor": "middle", "font-size": fs, class: "mono",
      style: "fill:var(--jam);font-weight:700" }, gJam);
    t.textContent = lab;
  }
  // Transfers in flight and nodes computing at the map's time.
  const d = data(ctx.run);
  const busyNodes = new Set(), relaying = new Set();
  if (d && at != null) {
    const lanes = {};          // directed link -> legs drawn on it
    const leg = (a, b, arrow, color) => {
      const [ax, ay] = nodePos(a), [bx, by] = nodePos(b);
      const L = Math.hypot(bx - ax, by - ay), ux = (bx - ax) / L, uy = (by - ay) / L;
      const n = lanes[`${a}>${b}`] = (lanes[`${a}>${b}`] || 0) + 1;
      const off = R * 0.35 * (1 + (n - 1) * 1.6);   // both directions apart; a second kind of traffic beside the first
      el("line", { x1: ax + ux * R * 1.1 - uy * off, y1: ay + uy * R * 1.1 + ux * off,
        x2: bx - ux * (arrow ? R * 1.25 : R * 1.1) - uy * off, y2: by - uy * (arrow ? R * 1.25 : R * 1.1) + ux * off,
        "marker-end": arrow ? (color === "var(--relay)" ? "url(#arr-relay)" : "url(#arr)") : null,
        style: `stroke:${color || "var(--task)"};stroke-width:3.5;stroke-linecap:round;vector-effect:non-scaling-stroke;opacity:0.85` }, gFlow);
    };
    // Each transfer in flight follows the path the emulator's routes gave it
    // at this moment; every leg is labeled with the rate measured on that
    // emulated link (wired links have no per-link counter).
    const drawn = new Set(), drawnKind = new Set();
    for (const f of d.flows) {
      if (!f.src || !f.dst || f.src === f.dst || f.start == null || !(f.start <= at && at <= f.end)) continue;
      const seg = (f.relayed || []).find((r) => r.from <= at && at <= r.to);
      const path = seg ? seg.path : [f.src, f.dst];
      const color = seg ? "var(--relay)" : "var(--task)";
      for (let k = 0; k + 1 < path.length; k++) {
        const key = `${path[k]}>${path[k + 1]}`;
        if (drawnKind.has(`${key}|${color}`)) continue;     // one leg per link and kind of traffic
        drawnKind.add(`${key}|${color}`);
        drawn.add(key);
        leg(path[k], path[k + 1], true, color);
      }
    }
    // Every drawn leg is labeled with the data rate measured on its link at
    // this moment (packets carrying payload; acknowledgments are counted
    // apart). Arrows come only from transfers, never from the counters.
    for (const key of drawn) {
      const v = (d.links || {})[key];
      if (!v) continue;
      const r = rateAt(v.series, at);
      if (r <= 0) continue;
      const [a, b] = key.split(">");
      const [ax, ay] = nodePos(a), [bx, by] = nodePos(b);
      const L = Math.hypot(bx - ax, by - ay), ux = (bx - ax) / L, uy = (by - ay) / L, off = R * 1.0;
      const t = el("text", { x: ax + (bx - ax) * 0.42 - uy * off, y: ay + (by - ay) * 0.42 + ux * off + fs * 0.3, "text-anchor": "middle",
        "font-size": fs * 0.82, class: "mono", style: "fill:var(--ink);font-weight:650;paint-order:stroke;stroke:var(--soft);stroke-width:3px;vector-effect:non-scaling-stroke" }, gFlow);
      t.textContent = r.toFixed(1);
    }
    for (const f of d.flows) {
      if (f.start == null || !(f.start <= at && at <= f.end)) continue;
      const seg = (f.relayed || []).find((r) => r.from <= at && at <= r.to);
      if (seg) seg.path.slice(1, -1).forEach((n) => relaying.add(n));
    }
    for (const t of d.tasks) if (t.node && t.start != null && t.start <= at && (t.end == null || at < t.end)) busyNodes.add(t.node);
  }
  for (const n of S.scen.nodes) {
    if (relaying.has(n.id)) {
      el("circle", { cx: n.x, cy: n.y, r: R * 1.62, style: "fill:none;stroke:var(--relay);stroke-width:2.5;stroke-dasharray:4 3;vector-effect:non-scaling-stroke" }, gNodes);
    }
    if (busyNodes.has(n.id)) {
      el("circle", { cx: n.x, cy: n.y, r: R * 1.32, style: "fill:none;stroke:var(--task);stroke-width:3;vector-effect:non-scaling-stroke" }, gNodes);
    }
    const c = el("circle", { cx: n.x, cy: n.y, r: R, style: `fill:${NODE_FILL[n.hw]};stroke:var(--panel);stroke-width:2;vector-effect:non-scaling-stroke` }, gNodes);
    c.addEventListener("mouseenter", (e) => tip(e, `<b>${n.id}</b> ${esc(n.name)}<br>${n.hw}, relative speed ${n.speed.toFixed(2)}`));
    c.addEventListener("mousemove", (e) => tip(e, $("#tip").innerHTML));
    c.addEventListener("mouseleave", (e) => tip(e, null));
    const t = el("text", { x: n.x, y: n.y + fs * 0.36, "text-anchor": "middle", "font-size": fs * 1.05, class: "mono",
      style: `fill:${NODE_TEXT[n.hw]};font-weight:700;pointer-events:none` }, gNodes);
    t.textContent = n.id;
    const l1 = el("text", { x: n.x, y: n.y + R + fs * 1.15, "text-anchor": "middle", "font-size": fs * 0.92, style: "pointer-events:none" }, gNodes);
    l1.textContent = HW_SHORT[n.hw];
    const l2 = el("text", { x: n.x, y: n.y + R + fs * 2.15, "text-anchor": "middle", "font-size": fs * 0.78, class: "faint mono", style: "pointer-events:none" }, gNodes);
    l2.textContent = n.name;
  }
  if (at != null) {
    const t = el("text", { x: cx - span / 2 + span * 0.03, y: cy - span / 2 + span * 0.06, "font-size": fs * 0.95, class: "muted" }, svg);
    t.textContent = `${ctx.run && ctx.run.kind === "disrupted" ? stratName(ctx.run.strategy) : "baseline"} at ${fmtS(at)}`;
  }
  // Jammer placement: press, drag to the radius, release.
  svg.onpointerdown = (e) => {
    if (S.tool !== "jammer" || !doneBaseline()) return;
    const p = toMap(svg, e);
    S.jamDraft = { x: p.x, y: p.y, r: 0 };
    svg.setPointerCapture(e.pointerId);
  };
  svg.onpointermove = (e) => {
    if (!S.jamDraft) return;
    const p = toMap(svg, e);
    S.jamDraft.r = Math.hypot(p.x - S.jamDraft.x, p.y - S.jamDraft.y);
    renderMap();
  };
  svg.onpointerup = () => {
    if (!S.jamDraft) return;
    const j = S.jamDraft;
    S.jamDraft = null;
    if (j.r < 3) j.r = 20;
    S.pending = { type: "jammer", x: j.x, y: j.y, r: j.r };
    renderAll();
  };
}
function toMap(svg, e) {
  const p = svg.createSVGPoint();
  p.x = e.clientX; p.y = e.clientY;
  return p.matrixTransform(svg.getScreenCTM().inverse());
}
const stratName = (id) => (STRATS.find((s) => s.id === id) || { name: id }).name;

// -------------------------------------------------------------- task graph
// Layered layout: a column per level (longest path from a source), tasks in
// a column ordered by the mean position of their parents, then of their
// children (a few sweeps), to keep edges from crossing.
let dagLayoutCache = null;
function dagLayout() {
  if (dagLayoutCache && dagLayoutCache.id === S.scen.id) return dagLayoutCache;
  const ids = S.scen.tasks.map((t) => t.id);
  const parents = Object.fromEntries(ids.map((i) => [i, []])), children = Object.fromEntries(ids.map((i) => [i, []]));
  for (const e of S.scen.edges) { parents[e.to].push(e.from); children[e.from].push(e.to); }
  const level = {};
  const lv = (t) => (t in level ? level[t] : (level[t] = parents[t].length ? 1 + Math.max(...parents[t].map(lv)) : 0));
  ids.forEach(lv);
  const cols = [];
  ids.forEach((t) => (cols[level[t]] ||= []).push(t));
  const pos = {};
  const place = () => cols.forEach((c) => c.forEach((t, k) => (pos[t] = (k + 0.5) / c.length)));
  place();
  const mean = (xs) => xs.reduce((a, b) => a + b, 0) / xs.length;
  for (let sweep = 0; sweep < 6; sweep++) {
    const down = sweep % 2 === 0;
    const order = down ? cols.slice(1) : cols.slice(0, -1).reverse();
    for (const c of order) {
      c.sort((a, b) => {
        const na = down ? parents[a] : children[a], nb = down ? parents[b] : children[b];
        return (na.length ? mean(na.map((x) => pos[x])) : pos[a]) - (nb.length ? mean(nb.map((x) => pos[x])) : pos[b]);
      });
      c.forEach((t, k) => (pos[t] = (k + 0.5) / c.length));
    }
  }
  const sizes = S.scen.edges.map((e) => e.size).filter((x) => x > 0);
  dagLayoutCache = { id: S.scen.id, cols, level, pos, lo: Math.log(Math.min(...sizes)), hi: Math.log(Math.max(...sizes)) };
  return dagLayoutCache;
}

function dagContext() {
  // Which run (and moment) the graph shows: the chart under the pointer, a
  // run in progress, else the shown disrupted run, else the baseline.
  const base = baselineRun(), shown = shownRun();
  if (S.cursor) return { run: S.cursor.chart === "baseline" ? base : shown, t: S.cursor.t };
  if (shown && shown.status === "running" && shown.live) return { run: shown, t: shown.live.now };
  if (base && base.status === "running" && base.live) return { run: base, t: base.live.now };
  return { run: (shown && shown.result) ? shown : (base && base.result ? base : null), t: null };
}

function renderDag() {
  const box = $("#dagview");
  if (!S.scen) { box.replaceChildren(); return; }
  const L = dagLayout();
  const ctx = dagContext();
  const d = data(ctx.run);
  const at = ctx.t;
  const nodeOf = {}, task = {};
  for (const t of S.scen.tasks) nodeOf[t.id] = S.scen.plan[t.id];
  if (d) for (const t of d.tasks) { task[t.id] = t; if (t.node) nodeOf[t.id] = t.node; }
  const moved = d ? d.moved : {};
  $("#dag-sub").textContent = !ctx.run ? "initial plan"
    : `${ctx.run.kind === "baseline" ? "baseline" : stratName(ctx.run.strategy)}${at != null ? ` at ${fmtS(at)}` : ", where each task ran"}`;
  const hw = Object.fromEntries(S.scen.nodes.map((n) => [n.id, n.hw]));
  const wired = new Set(S.scen.pairs.filter((p) => p.wired).map((p) => `${p.a}-${p.b}`));

  const W = Math.max(640, box.clientWidth || 640);
  const nc = L.cols.length, maxc = Math.max(...L.cols.map((c) => c.length));
  const padX = 16, padY = 12;
  const H = Math.min(460, Math.max(120, maxc * 34 + 2 * padY));
  const colW = nc > 1 ? (W - 2 * padX) / nc : W - 2 * padX;
  const bw = Math.min(118, colW * 0.72), bh = Math.min(24, ((H - 2 * padY) / maxc) * 0.72);
  const compact = bh < 14;
  const X = (t) => padX + (L.level[t] + 0.5) * colW, Y = (t) => padY + L.pos[t] * (H - 2 * padY);
  const svg = el("svg", { width: W, height: H, viewBox: `0 0 ${W} ${H}` });
  const defs = el("defs", {}, svg);
  const mk = el("marker", { id: "dag-arr", viewBox: "0 0 10 10", refX: 9, refY: 5, markerWidth: 5, markerHeight: 5, orient: "auto" }, defs);
  el("path", { d: "M0,0 L10,5 L0,10 z", style: "fill:var(--faint)" }, mk);

  // Edges: what the transfer between the two tasks went over, in this run.
  const flowOf = {};
  if (d) for (const f of d.flows) flowOf[`${f.from}>${f.to}`] = f;
  const gE = el("g", {}, svg);
  for (const e of S.scen.edges) {
    const a = nodeOf[e.from], b = nodeOf[e.to];
    const f = flowOf[`${e.from}>${e.to}`];
    const local = a === b;
    const relayed = f && (f.relayed || []).length;
    const inflight = f && at != null && f.start != null && f.start <= at && at <= f.end;
    // Width by data size (log scale); dense graphs get thinner, fainter edges
    // and no arrowheads (the layout already reads left to right).
    const w = (compact ? 0.4 : 1) * (0.8 + 3.2 * (L.hi > L.lo ? (Math.log(Math.max(e.size, 1e-12)) - L.lo) / (L.hi - L.lo) : 0.5));
    let style = local ? "stroke:var(--line)" : wired.has([a, b].sort().join("-")) ? "stroke:var(--wired)" : "stroke:var(--radio);stroke-dasharray:5 4";
    if (relayed) style = "stroke:var(--relay)";
    if (inflight) style = `stroke:${relayed && (f.relayed || []).some((r) => r.from <= at && at <= r.to) ? "var(--relay)" : "var(--task)"}`;
    const hot = S.hoverTask && (S.hoverTask === e.from || S.hoverTask === e.to);
    const x1 = X(e.from) + (compact ? bh / 2 : bw / 2), y1 = Y(e.from), x2 = X(e.to) - (compact ? bh / 2 : bw / 2), y2 = Y(e.to);
    const mx = (x1 + x2) / 2;
    const opacity = S.hoverTask && !hot ? 0.2 : inflight || hot ? 1 : compact ? 0.4 : local ? 0.7 : 0.95;
    const path = el("path", { d: `M${x1},${y1} C${mx},${y1} ${mx},${y2} ${x2},${y2}`, "marker-end": compact ? null : "url(#dag-arr)",
      style: `${style};fill:none;stroke-width:${(inflight || hot ? w + 1.5 : w).toFixed(2)};opacity:${opacity}` }, gE);
    const hit = el("path", { d: path.getAttribute("d"), style: "fill:none;stroke:transparent;stroke-width:9" }, gE);
    const how = local ? `same node (${a}), no network` : `${a} → ${b} over ${wired.has([a, b].sort().join("-")) ? "a wire" : "radio"}`;
    hit.dataset.tip = `<b>${esc(e.from)} → ${esc(e.to)}</b><br>${how}` +
      (f ? `<br>${fmtMB(f.bytes || 0)}, ${fmtS(f.start)} → ${fmtS(f.end)}` + (f.relayed || []).map((r) => `<br>relayed ${r.path.join(" → ")} ${fmtS(r.from)} → ${fmtS(r.to)}`).join("") : "");
  }
  // Tasks: hardware color of the node they ran on; state at the moment shown.
  const gT = el("g", {}, svg);
  for (const t of S.scen.tasks) {
    const n = nodeOf[t.id], info = task[t.id] || {};
    const state = at == null ? null : info.start == null || info.start > at ? "waiting" : info.end != null && info.end <= at ? "done" : "running";
    const fill = NODE_FILL[hw[n]] || "var(--soft)";
    const isMoved = t.id in moved, hot = S.hoverTask === t.id;
    const op = state === "waiting" ? 0.28 : 1;
    const g = el("g", { style: `opacity:${S.hoverTask && !hot ? Math.min(op, 0.45) : op}` }, gT);
    const x = X(t.id), y = Y(t.id);
    const stroke = isMoved ? "stroke:var(--moved);stroke-width:2.5" : hot ? "stroke:var(--ink);stroke-width:2" : "stroke:var(--panel);stroke-width:1";
    const shape = compact
      ? el("circle", { cx: x, cy: y, r: Math.max(3, bh / 2), style: `fill:${fill};${stroke}` }, g)
      : el("rect", { x: x - bw / 2, y: y - bh / 2, width: bw, height: bh, rx: 4, style: `fill:${fill};${stroke}` }, g);
    if (state === "running") {
      if (compact) el("circle", { cx: x, cy: y, r: Math.max(3, bh / 2) + 3, style: "fill:none;stroke:var(--task);stroke-width:2" }, g);
      else el("rect", { x: x - bw / 2 - 3, y: y - bh / 2 - 3, width: bw + 6, height: bh + 6, rx: 6, style: "fill:none;stroke:var(--task);stroke-width:2" }, g);
    }
    if (!compact) {
      const lab = el("text", { x: x - bw / 2 + 6, y: y + 4, "font-size": 11, style: `fill:${NODE_TEXT[hw[n]]};pointer-events:none` }, g);
      const name = t.id.split("_")[0];
      lab.textContent = name.length > Math.floor((bw - 34) / 6.2) ? name.slice(0, Math.max(3, Math.floor((bw - 34) / 6.2))) + "…" : name;
      const nb = el("text", { x: x + bw / 2 - 6, y: y + 4, "text-anchor": "end", "font-size": 10.5, class: "mono",
        style: `fill:${NODE_TEXT[hw[n]]};font-weight:700;pointer-events:none` }, g);
      nb.textContent = n;
    }
    shape.dataset.task = t.id;
    const plan = S.scen.plan[t.id];
    shape.dataset.tip = `<b>${esc(t.id)}</b> on ${n} (${HW_SHORT[hw[n]]})${isMoved ? `, moved by repair from ${plan}` : n !== plan ? "" : ""}` +
      `<br>work ${t.cost.toPrecision(3)} (simulator units)` +
      (info.start != null ? `<br>${fmtS(info.start)} → ${info.end != null ? fmtS(info.end) : "running"}` : "") +
      (state ? `<br>${state} at ${fmtS(at)}` : "");
  }
  svg.addEventListener("pointermove", (e) => {
    const tsk = (e.target.dataset && e.target.dataset.task) || null;
    tip(e, e.target.dataset && e.target.dataset.tip ? e.target.dataset.tip : null);
    if (tsk !== S.hoverTask) { S.hoverTask = tsk; renderDag(); renderCharts(); }
  });
  svg.addEventListener("pointerleave", (e) => {
    tip(e, null);
    if (S.hoverTask) { S.hoverTask = null; renderDag(); renderCharts(); }
  });
  box.replaceChildren(svg);
  // Summary: how the plan in view uses the network.
  const kd = $("#dag-kpis");
  kd.replaceChildren();
  const cross = S.scen.edges.filter((e) => nodeOf[e.from] !== nodeOf[e.to]);
  const radio = cross.filter((e) => !wired.has([nodeOf[e.from], nodeOf[e.to]].sort().join("-")));
  kd.append(kpi(String(S.scen.tasks.length), "tasks"), kpi(String(cross.length), "edges cross nodes"), kpi(String(radio.length), "over radio"));
  if (d && Object.keys(moved).length) kd.append(kpi(String(Object.keys(moved).length), "moved by repair"));
}

// ------------------------------------------------------------------ charts
function tmax() {
  let m = 10;
  m = Math.max(m, endOf(data(baselineRun())), endOf(data(shownRun())));
  for (const d of S.drafts) m = Math.max(m, d.end);
  const sh = shownRun();
  if (sh) for (const d of sh.disruptions) m = Math.max(m, d.end);
  return m * 1.04;
}

function renderCharts() {
  if (!S.scen) return;
  const T = tmax();
  const base = baselineRun();
  drawChart($("#chart-baseline"), base, "baseline", T, S.drafts);
  const shown = shownRun();
  drawChart($("#chart-disrupted"), shown, "disrupted", T, shown ? shown.disruptions : []);
  // KPIs
  const bd = data(base), sd = data(shown);
  const kb = $("#kpi-baseline");
  kb.replaceChildren();
  if (bd && !bd.live && bd.makespan) kb.append(kpi(fmtS(bd.makespan), "makespan"), kpi(fmtMB(bytesOf(bd)), "moved over the network"));
  else if (base) kb.append(kpi(base.status, ""));
  const kd = $("#kpi-disrupted");
  kd.replaceChildren();
  $("#disrupted-title").innerHTML = shown ? `${stratName(shown.strategy)} <span class="sub">${esc(labels(shown.disruptions).join(", "))}</span>` : "Disrupted run";
  if (sd && !sd.live && sd.makespan && bd && bd.makespan) {
    const ratio = sd.makespan / bd.makespan;
    kd.append(kpi(fmtS(sd.makespan), "makespan"),
      kpi(`${ratio.toFixed(2)}×`, "baseline", ratio > 1.02 ? "bad" : "good"),
      kpi(String(Object.keys(sd.moved).length), "tasks moved"),
      kpi(fmtMB(bytesOf(sd)), "moved over the network"));
  } else if (shown) kd.append(kpi(shown.status, ""));
  renderTraffic(bd, sd);
}

function renderTraffic(bd, sd) {
  const box = $("#traffic");
  box.replaceChildren();
  if (!sd || sd.live || !sd.links) return;
  const keys = new Set([...Object.keys(sd.links || {}), ...Object.keys((bd && bd.links) || {})]);
  const get = (d, k) => ((d && d.links && d.links[k]) || {});
  const rows = [...keys].map((k) => [k, get(sd, k).mb || 0, bd && bd.links ? (get(bd, k).mb || 0) : null, get(sd, k).ack_mb])
    .filter(([, a, b, acks]) => a > 0 || (b || 0) > 0 || (acks || 0) > 0)
    .sort((x, y) => Math.abs(y[1] - (y[2] || 0)) - Math.abs(x[1] - (x[2] || 0)));
  const relays = Object.entries(sd.relays || {}).filter(([, v]) => v.mb > 0 || (v.ack_mb || 0) > 0);
  if (!rows.length && !relays.length) return;
  box.append(html("h3", null, "Measured on the links"));
  const tbl = html("table");
  const head = html("tr");
  ["link", "data, this run", "data, baseline", "change", "acks, this run"].forEach((h) => head.append(html("th", null, h)));
  tbl.append(head);
  const mb = (x) => (x == null ? "–" : `${x.toFixed(2)} MB`);
  for (const [k, a, b, acks] of rows) {
    const tr = html("tr");
    const delta = b == null ? null : a - b;
    const dc = html("td", delta == null ? "" : delta > 0 ? "up" : delta < 0 ? "downc" : "", delta == null ? "–" : `${delta >= 0 ? "+" : ""}${delta.toFixed(2)} MB`);
    tr.append(html("td", null, k.replace(">", " → ")), html("td", null, mb(a)), html("td", null, mb(b)), dc, html("td", null, mb(acks)));
    tbl.append(tr);
  }
  for (const [k, v] of relays) {
    const [via, dst] = k.split(">");
    const tr = html("tr", "relayrow");
    tr.append(html("td", null, `${via} relayed → ${dst}`), html("td", null, mb(v.mb)), html("td", null, ""), html("td", null, ""), html("td", null, mb(v.ack_mb)));
    tbl.append(tr);
  }
  box.append(tbl);
  box.append(html("p", null, "Counted on the nodes every half second: what each node sent to each next hop (its own and relayed traffic), and what it relayed for others. " +
    "Data is IP packets longer than 80 bytes; a TCP packet without payload is never longer (20-byte IP header, at most 60 bytes of TCP header), so acks are the bare acknowledgments."));
}
function kpi(v, label, cls) {
  const s = html("span", cls || "");
  s.append(html("b", null, v), label);
  return s;
}

function drawChart(box, run, which, T, disruptions) {
  const W = Math.max(640, box.clientWidth || 640);
  const d = data(run);
  box.classList.toggle("armed", which === "baseline" && !!S.pending);
  if (!d) {
    box.replaceChildren();
    const msg = which === "baseline"
      ? (run ? `Baseline ${run.status}${run.error ? `: ${run.error}` : "…"}` : "Run the baseline to see how the DAG executes undisrupted.")
      : (run ? `Run ${run.status}${run.error ? `: ${run.error}` : "…"}` : "Draw a disruption, pick a repair strategy, and run it.");
    box.append(html("div", "empty", msg));
    return;
  }
  const nodes = S.scen.nodes;
  const L = 128, Rp = 18, Tp = 46, rowH = 50, B = 22;
  const H = Tp + nodes.length * rowH + B;
  const X = (t) => L + (t / T) * (W - L - Rp);
  const svg = el("svg", { width: W, height: H, viewBox: `0 0 ${W} ${H}` });
  el("rect", { x: L, y: Tp, width: W - L - Rp, height: nodes.length * rowH, rx: 4, class: "plot-bg",
    style: "fill:var(--soft);stroke:var(--line)" }, svg);
  // Time axis.
  const steps = [1, 2, 5, 10, 15, 20, 30, 60, 120, 300];
  const step = steps.find((s) => (W - L - Rp) / (T / s) >= 70) || 600;
  for (let t = 0; t <= T; t += step) {
    el("line", { x1: X(t), y1: Tp, x2: X(t), y2: Tp + nodes.length * rowH, style: "stroke:var(--line);stroke-width:1" }, svg);
    const lab = el("text", { x: X(t), y: H - 6, "text-anchor": "middle", "font-size": 11, class: "muted" }, svg);
    lab.textContent = `${t}s`;
  }
  // Rows.
  const rowY = {};
  nodes.forEach((n, i) => {
    rowY[n.id] = Tp + i * rowH;
    if (i) el("line", { x1: L, y1: rowY[n.id], x2: W - Rp, y2: rowY[n.id], style: "stroke:var(--line)" }, svg);
    const a = el("text", { x: 10, y: rowY[n.id] + 22, "font-size": 13, class: "mono", style: "font-weight:700" }, svg);
    a.textContent = n.id;
    const b = el("text", { x: 38, y: rowY[n.id] + 22, "font-size": 12 }, svg);
    b.textContent = HW_SHORT[n.hw];
    const c = el("text", { x: 10, y: rowY[n.id] + 38, "font-size": 10.5, class: "faint mono" }, svg);
    c.textContent = n.name;
  });
  // Labels above the plot: each takes the first of two rows where it fits.
  const rowsEnd = [-Infinity, -Infinity];
  const place = (x, text, style) => {
    const w = text.length * 6.6 + 8;
    let r = rowsEnd.findIndex((end) => x >= end);
    if (r < 0) r = rowsEnd[0] <= rowsEnd[1] ? 0 : 1;
    rowsEnd[r] = x + w;
    const t = el("text", { x: x + 3, y: Tp - 8 - r * 15, "font-size": 11, style }, svg);
    t.textContent = text;
    return t;
  };
  // Disruption windows.
  const labs = labels(disruptions);
  disruptions.forEach((dd, i) => {
    const x0 = X(dd.start), x1 = X(Math.min(dd.end, T));
    el("rect", { x: x0, y: Tp, width: Math.max(1, x1 - x0), height: nodes.length * rowH, style: "fill:var(--jam-fill)" }, svg);
    el("line", { x1: x0, y1: Tp, x2: x0, y2: Tp + nodes.length * rowH, style: "stroke:var(--jam);stroke-width:1.2" }, svg);
    el("line", { x1: x1, y1: Tp, x2: x1, y2: Tp + nodes.length * rowH, style: "stroke:var(--jam);stroke-width:1.2" }, svg);
    place(x0, `${labs[i]} ${dd.type === "link" ? `${dd.a}–${dd.b}` : `jammer`}`,
      "fill:var(--jam);font-weight:700;font-family:ui-monospace,SFMono-Regular,Menlo,monospace");
  });
  // Incoming transfers, under each destination's compute track.
  const wired = new Set(S.scen.pairs.filter((p) => p.wired).map((p) => `${p.a}-${p.b}`));
  for (const f of d.flows) {
    if (!f.dst || f.start == null || f.end == null || !(f.dst in rowY)) continue;
    const pk = [f.src, f.dst].sort().join("-");
    const r = el("rect", { x: X(f.start), y: rowY[f.dst] + 34, width: Math.max(1.5, X(f.end) - X(f.start)), height: 7, rx: 1.5,
      style: `fill:${wired.has(pk) ? "var(--line)" : "var(--xfer)"};${f.ok ? "" : "stroke:var(--down);stroke-width:1.2;"}opacity:0.9` }, svg);
    const relayText = (f.relayed || []).map((x) => `<br>relayed ${x.path.join(" → ")} from ${fmtS(x.from)} to ${fmtS(x.to)}`).join("");
    r.dataset.tip = `<b>transfer</b> ${esc(f.from)} → ${esc(f.to)}<br>${f.src} → ${f.dst} (${wired.has(pk) ? "wired" : "radio"}), ` +
      `${fmtMB(f.bytes || 0)}<br>${fmtS(f.start)} → ${fmtS(f.end)} (${(f.end - f.start).toFixed(1)} s)${relayText}${f.ok ? "" : "<br>failed, retried"}`;
    for (const x of f.relayed || []) {
      const rr = el("rect", { x: X(x.from), y: rowY[f.dst] + 34, width: Math.max(1.5, X(x.to) - X(x.from)), height: 7, rx: 1.5,
        style: "fill:var(--relay)" }, svg);
      rr.dataset.tip = r.dataset.tip;
    }
  }
  // Forwarding measured on each relay node: what it passed on for others.
  for (const [key, v] of Object.entries(d.relays || {})) {
    const [via, dst] = key.split(">");
    if (!(via in rowY)) continue;
    for (const [a, b, mb] of activeSpans(v.series)) {
      const fr = el("rect", { x: X(Math.max(0, a)), y: rowY[via] + 44, width: Math.max(1.5, X(b) - X(Math.max(0, a))), height: 3.5,
        style: "fill:var(--relay)" }, svg);
      fr.dataset.tip = `<b>${via} forwarding</b> traffic bound for ${dst}<br>${fmtS(Math.max(0, a))} → ${fmtS(b)}, about ${mb.toFixed(2)} MB` +
        `<br>measured on ${via} (all it relayed for ${dst} this run: ${v.mb.toFixed(2)} MB)`;
    }
  }
  // Tasks.
  for (const t of d.tasks) {
    if (!t.node || t.start == null || !(t.node in rowY)) continue;
    const end = t.end != null ? t.end : (d.now != null ? d.now : t.start);
    const moved = t.id in d.moved;
    const running = t.end == null;
    const r = el("rect", { x: X(t.start), y: rowY[t.node] + 7, width: Math.max(2, X(end) - X(t.start)), height: 22, rx: 3,
      style: `fill:${moved ? "var(--moved)" : running ? "var(--task-live)" : "var(--task)"};stroke:var(--panel);stroke-width:1` }, svg);
    const plan = S.scen.plan[t.id];
    r.dataset.task = t.id;
    if (S.hoverTask === t.id) { r.style.stroke = "var(--ink)"; r.style.strokeWidth = "2.5"; }
    r.dataset.tip = `<b>${esc(t.id)}</b> on ${t.node}${moved ? ` (repair moved it from ${plan})` : ""}` +
      `<br>${fmtS(t.start)} → ${t.end != null ? fmtS(t.end) : "running"}${t.end != null ? ` (${(t.end - t.start).toFixed(1)} s)` : ""}`;
    if (X(end) - X(t.start) > 54) {
      const tx = el("text", { x: X(t.start) + 5, y: rowY[t.node] + 22, "font-size": 10.5,
        style: "fill:var(--panel);pointer-events:none" }, svg);
      tx.textContent = t.id.split("_")[0].slice(0, Math.floor((X(end) - X(t.start) - 10) / 6));
    }
  }
  // Repairs.
  for (const p of d.plans || []) {
    const n = Object.keys(p.moves || {}).length;
    const ln = el("line", { x1: X(p.t), y1: Tp - 4, x2: X(p.t), y2: Tp + nodes.length * rowH,
      style: `stroke:var(--moved);stroke-width:${n ? 1.6 : 1};stroke-dasharray:5 3;opacity:${n ? 1 : 0.45}` }, svg);
    const list = Object.entries(p.moves || {}).map(([t, [a, b]]) => `${esc(t)} ${a}→${b}`).join(", ");
    ln.dataset.tip = `<b>repair at ${fmtS(p.t)}</b><br>${n ? `moved ${list}` : "re-planned, nothing moved"}`;
    el("line", { x1: X(p.t), y1: Tp - 4, x2: X(p.t), y2: Tp + nodes.length * rowH, "data-tip": ln.dataset.tip,
      style: "stroke:transparent;stroke-width:9" }, svg);
    if (n) place(X(p.t), `repair · ${n} moved`, "fill:var(--moved);font-weight:650");
  }
  // Now line for a live run; the shared cursor.
  if (d.live && d.now != null) {
    el("line", { x1: X(d.now), y1: Tp - 4, x2: X(d.now), y2: Tp + nodes.length * rowH, style: "stroke:var(--ink);stroke-width:1.4" }, svg);
  }
  if (S.cursor) {
    el("line", { class: "cursor", x1: X(S.cursor.t), y1: Tp, x2: X(S.cursor.t), y2: Tp + nodes.length * rowH,
      style: "stroke:var(--ink);stroke-width:1;stroke-dasharray:2 3;pointer-events:none" }, svg);
  }
  // Interaction: hover tips, cursor, and (baseline, armed) window dragging.
  const dragRect = el("rect", { y: Tp, height: nodes.length * rowH,
    style: "fill:var(--jam-fill);stroke:var(--jam);stroke-dasharray:4 3;pointer-events:none;display:none" }, svg);
  const showDrag = () => {
    const a = Math.min(S.drag.t0, S.drag.t1), b = Math.max(S.drag.t0, S.drag.t1);
    dragRect.setAttribute("x", X(a));
    dragRect.setAttribute("width", Math.max(1, X(b) - X(a)));
    dragRect.style.display = "";
  };
  const toT = (e) => Math.max(0, Math.min(T, ((e.clientX - svg.getBoundingClientRect().left - L) / (W - L - Rp)) * T));
  const inPlot = (e) => { const x = e.clientX - svg.getBoundingClientRect().left; return x >= L && x <= W - Rp; };
  svg.addEventListener("pointermove", (e) => {
    tip(e, e.target.dataset && e.target.dataset.tip ? e.target.dataset.tip : null);
    if (S.drag) { S.drag.t1 = toT(e); showDrag(); return; }
    if (!inPlot(e)) return;
    S.cursor = { t: toT(e), chart: which };
    S.hoverTask = (e.target.dataset && e.target.dataset.task) || null;
    renderMap();
    renderDag();
    moveCursors();
  });
  svg.addEventListener("pointerleave", (e) => {
    tip(e, null);
    if (S.drag) return;
    S.cursor = null;
    S.hoverTask = null;
    renderMap();
    renderDag();
    moveCursors();
  });
  if (which === "baseline") {
    svg.addEventListener("pointerdown", (e) => {
      if (!S.pending || !inPlot(e)) return;
      S.drag = { t0: toT(e), t1: toT(e) };
      showDrag();
      svg.setPointerCapture(e.pointerId);
    });
    svg.addEventListener("pointerup", () => {
      if (!S.drag) return;
      const a = Math.min(S.drag.t0, S.drag.t1), b = Math.max(S.drag.t0, S.drag.t1);
      S.drag = null;
      if (b - a < 0.3) { toast("Drag across the chart to give the disruption a duration."); renderCharts(); return; }
      if (!pairsHit(S.pending).length) { toast("That jammer reaches no node: place it over a node or make it larger."); renderCharts(); return; }
      S.drafts.push({ ...S.pending, start: +a.toFixed(2), end: +b.toFixed(2) });
      S.pending = null;
      renderAll();
    });
  }
  svg.dataset.which = which;
  svg.dataset.l = L; svg.dataset.w = W; svg.dataset.r = Rp; svg.dataset.T = T; svg.dataset.top = Tp; svg.dataset.h = nodes.length * rowH;
  box.replaceChildren(svg);
}

function moveCursors() {
  // Redraw only the dashed cursor line in both charts (cheaper than a re-render).
  document.querySelectorAll(".chart svg").forEach((svg) => {
    svg.querySelectorAll(".cursor").forEach((c) => c.remove());
    if (!S.cursor) return;
    const L = +svg.dataset.l, W = +svg.dataset.w, Rp = +svg.dataset.r, T = +svg.dataset.T, top = +svg.dataset.top, h = +svg.dataset.h;
    const x = L + (S.cursor.t / T) * (W - L - Rp);
    el("line", { class: "cursor", x1: x, y1: top, x2: x, y2: top + h,
      style: "stroke:var(--ink);stroke-width:1;stroke-dasharray:2 3;pointer-events:none" }, svg);
  });
}

// ----------------------------------------------------------------- history
function renderHistory() {
  const tb = $("#history");
  tb.replaceChildren();
  const bd = data(doneBaseline());
  const shown = shownRun();
  for (const r of S.runs) {
    const tr = html("tr");
    if (shown && r.id === shown.id) tr.className = "sel";
    const rd = data(r);
    const ms = rd && !rd.live ? rd.makespan : null;
    const ratio = ms && bd && bd.makespan && r.kind === "disrupted" ? `${(ms / bd.makespan).toFixed(2)}×` : "";
    const pill = html("span", `pill ${r.status}`, r.status);
    tr.append(html("td", "mono", r.id), html("td", null, r.kind === "baseline" ? "Baseline" : stratName(r.strategy)),
      html("td", null, r.kind === "baseline" ? "–" : labels(r.disruptions).join(", ")),
      html("td", "num", ms ? fmtS(ms) : "–"), html("td", "num", ratio));
    const st = html("td");
    st.append(pill);
    if (r.kind === "disrupted") {
      const reuse = html("button", "pill", "reuse");
      reuse.title = "Copy this run's disruptions and strategy into the editor";
      reuse.style.marginLeft = "6px";
      reuse.style.cursor = "pointer";
      reuse.onclick = (e) => {
        e.stopPropagation();
        S.drafts = r.disruptions.map((d) => ({ ...d }));
        S.strategy = r.strategy;
        renderAll();
      };
      st.append(reuse);
    }
    tr.append(st);
    if (r.kind === "disrupted") tr.onclick = () => { S.shownId = r.id; renderAll(); };
    tb.append(tr);
  }
  if (!S.runs.length) {
    const tr = html("tr");
    const td = html("td", null, "No runs yet for this DAG.");
    td.colSpan = 6;
    td.style.color = "var(--faint)";
    tr.append(td);
    tb.append(tr);
  }
}

init().catch((e) => toast(`Could not load the lab: ${e.message}`));
