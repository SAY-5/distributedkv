// distributedkv — operator console.
// No build step, no framework. Pure ES modules, fetch + DOM.

const POLL_MS = 1500;

const screen = document.getElementById("screen");

function tag(name, attrs = {}, ...children) {
  const el = document.createElement(name);
  for (const [k, v] of Object.entries(attrs)) {
    if (k === "class") el.className = v;
    else if (k === "html") el.innerHTML = v;
    else el.setAttribute(k, v);
  }
  for (const c of children.flat()) {
    if (c == null) continue;
    el.append(typeof c === "string" ? document.createTextNode(c) : c);
  }
  return el;
}

function frame() {
  screen.innerHTML = "";
  const titlebar = tag(
    "div",
    { class: "titlebar" },
    tag("span", {}, "distributedkv • OPERATOR CONSOLE"),
    tag("span", { class: "blink" }, "_"),
    tag(
      "div",
      { class: "right" },
      tag("span", { id: "ts" }, "—"),
      tag("span", {}, "v0.1.0"),
    ),
  );
  const menubar = tag(
    "div",
    { class: "menubar" },
    tag("span", {}, tag("b", {}, "F1"), "topology"),
    tag("span", {}, tag("b", {}, "F2"), "metrics"),
    tag("span", {}, tag("b", {}, "F3"), "log"),
    tag("span", { id: "cluster-state", class: "pill" }, "CLUSTER UP"),
    tag("div", { class: "right", id: "leader-pill" },
      tag("span", { class: "pill" }, "LEADER —"),
    ),
  );
  const grid = tag(
    "div",
    { class: "grid" },
    panel("01 · TOPOLOGY [F1]", "topo", tag("table", { class: "topo", id: "topo-table" })),
    panel("02 · CLUSTER GRAPH", "graph", tag("pre", { class: "ascii-topo", id: "ascii" })),
    panel("03 · METRICS [F2]", "metrics", tag("div", { class: "gauges", id: "gauges" })),
    panel("04 · EVENT STREAM [F3]", "log", tag("div", { class: "log", id: "log" })),
  );
  const status = tag(
    "div",
    { class: "statusbar" },
    tag("span", { id: "status-summary" }, "INIT"),
    tag(
      "div",
      { class: "right" },
      tag("span", { id: "uptime" }, "uptime —"),
      tag("span", {}, "POLL " + POLL_MS + "MS"),
      tag("span", {}, "PRESS Q TO EXIT"),
    ),
  );
  screen.append(titlebar, menubar, grid, status);
}

function panel(title, slug, body) {
  const idMatch = title.match(/^(\d+)/);
  return tag(
    "div",
    { class: "panel", "data-slug": slug },
    tag(
      "header",
      {},
      tag("span", { class: "id" }, "[" + (idMatch ? idMatch[1] : "00") + "]"),
      tag("span", {}, title.replace(/^\d+\s*·\s*/, "")),
    ),
    tag("div", { class: "body" }, body),
  );
}

const events = [];
function pushEvent(level, msg) {
  const t = new Date();
  const line = {
    t: t.toTimeString().slice(0, 8),
    lvl: level,
    msg,
  };
  events.unshift(line);
  if (events.length > 60) events.pop();
}

function renderTopology(info) {
  const t = document.getElementById("topo-table");
  t.innerHTML = "";
  const head = tag(
    "tr",
    {},
    ["NODE", "ROLE", "RAFT", "HTTP", "STATUS"].map((h) => tag("th", {}, h)),
  );
  t.append(head);
  for (const s of info.servers || []) {
    const isLeader = s.is_leader === true;
    const row = tag(
      "tr",
      { class: isLeader ? "leader" : "" },
      tag("td", {}, s.id),
      tag("td", { class: "role" }, isLeader ? "LEADER" : "FOLLOWER"),
      tag("td", {}, s.raft_addr || "—"),
      tag("td", {}, s.http_addr || "—"),
      tag("td", {}, "OK"),
    );
    t.append(row);
  }
}

function renderAsciiTopo(info) {
  const a = document.getElementById("ascii");
  const servers = info.servers || [];
  if (servers.length === 0) {
    a.textContent = "(no nodes)";
    return;
  }
  // Render a circular ASCII diagram with the leader highlighted.
  const lines = [];
  lines.push("                ┌──────────────────┐");
  lines.push("                │   CLIENT (kvctl) │");
  lines.push("                └────────┬─────────┘");
  lines.push("                         │");
  lines.push("                ┌────────▼─────────┐");
  for (const s of servers) {
    const tag_ = s.is_leader ? "★ LEADER " : "  follower";
    lines.push(`                │  ${pad(s.id, 4)}   ${tag_}│`);
  }
  lines.push("                └──────────────────┘");
  lines.push("                  raft replication ");
  lines.push("                ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼ ▼");
  lines.push(`              applied=${info.applied}  keys=${info.keys}`);
  a.innerHTML = "";
  for (const ln of lines) {
    if (ln.includes("LEADER")) {
      a.append(tag("span", { class: "leader" }, ln + "\n"));
    } else {
      a.append(document.createTextNode(ln + "\n"));
    }
  }
}

function pad(s, n) {
  s = String(s);
  return s.length >= n ? s : s + " ".repeat(n - s.length);
}

function renderGauges(stats, info) {
  const g = document.getElementById("gauges");
  g.innerHTML = "";
  const items = [
    {
      label: "applied index",
      value: info.applied ?? 0,
      sub: "raft fsm-apply counter",
    },
    {
      label: "live keys",
      value: (info.keys ?? 0).toLocaleString(),
      sub: "active set size",
    },
    {
      label: "writes window",
      value: stats.writes ?? 0,
      sub: "rolling 1024",
    },
    {
      label: "p50 write",
      value: fmtMs(stats.p50_ms),
      sub: "median apply latency",
    },
    {
      label: "p95 write",
      value: fmtMs(stats.p95_ms),
      sub: "95th percentile",
      warn: stats.p95_ms > 30,
    },
    {
      label: "p99 write",
      value: fmtMs(stats.p99_ms),
      sub: "99th percentile",
      alert: stats.p99_ms > 100,
    },
  ];
  for (const it of items) {
    g.append(
      tag(
        "div",
        { class: "gauge" },
        tag("div", { class: "label" }, it.label),
        tag(
          "div",
          { class: "value" + (it.warn ? " warn" : it.alert ? " alert" : "") },
          String(it.value ?? "—"),
        ),
        tag("div", { class: "bar" }, tag("i", { style: barWidth(it) })),
        tag("div", { class: "sub" }, it.sub),
      ),
    );
  }
}

function barWidth(it) {
  if (it.label === "p50 write") return "width: " + Math.min(100, (it.value || 0) * 5) + "%";
  if (it.label === "p95 write") return "width: " + Math.min(100, (it.value || 0) * 2) + "%";
  if (it.label === "p99 write") return "width: " + Math.min(100, (it.value || 0)) + "%";
  return "width: " + Math.min(100, Math.log10(Math.max(1, Number(String(it.value).replace(/,/g, ""))) + 1) * 14) + "%";
}

function fmtMs(v) {
  if (v == null) return "—";
  return Number(v).toFixed(2) + " ms";
}

function renderLog() {
  const c = document.getElementById("log");
  c.innerHTML = "";
  for (const e of events) {
    c.append(
      tag(
        "div",
        { class: "line" },
        tag("span", { class: "t" }, e.t),
        tag("span", { class: "lvl " + e.lvl }, e.lvl.toUpperCase()),
        tag("span", { class: "msg", html: e.msg }),
      ),
    );
  }
}

const startedAt = Date.now();
function renderClock() {
  const ts = document.getElementById("ts");
  if (ts) ts.textContent = new Date().toISOString().replace("T", " ").slice(0, 19) + "Z";
  const up = document.getElementById("uptime");
  if (up) {
    const sec = Math.floor((Date.now() - startedAt) / 1000);
    up.textContent = `uptime ${String(Math.floor(sec / 60)).padStart(2, "0")}:${String(sec % 60).padStart(2, "0")}`;
  }
}

let lastApplied = 0;
async function poll() {
  try {
    const [info, stats] = await Promise.all([
      fetch("/v1/cluster").then((r) => r.json()),
      fetch("/v1/stats").then((r) => r.json()),
    ]);
    renderTopology(info);
    renderAsciiTopo(info);
    renderGauges(stats, info);
    document.getElementById("leader-pill").innerHTML = "";
    document
      .getElementById("leader-pill")
      .append(tag("span", { class: "pill" }, "LEADER " + (leaderIDOf(info) || "—")));
    document.getElementById("cluster-state").textContent =
      info.leader ? "CLUSTER UP" : "NO LEADER";
    document.getElementById("cluster-state").className =
      "pill" + (info.leader ? "" : " alert");
    document.getElementById("status-summary").textContent =
      `OK · NODES ${info.servers.length} · APPLIED ${info.applied}`;
    if (info.applied > lastApplied && lastApplied !== 0) {
      pushEvent(
        "info",
        `applied <b>${info.applied - lastApplied}</b> entries (head=${info.applied})`,
      );
    }
    lastApplied = info.applied;
  } catch (err) {
    pushEvent("err", "poll failed: " + err.message);
    document.getElementById("status-summary").textContent = "POLL ERROR";
  }
  renderLog();
}

function leaderIDOf(info) {
  for (const s of info.servers || []) if (s.is_leader) return s.id;
  return "";
}

frame();
pushEvent("info", "console booted");
poll();
setInterval(poll, POLL_MS);
setInterval(renderClock, 1000);
renderClock();

document.addEventListener("keydown", (e) => {
  if (e.key === "q" || e.key === "Q") {
    document.body.style.opacity = "0";
    setTimeout(() => location.reload(), 250);
  }
});
