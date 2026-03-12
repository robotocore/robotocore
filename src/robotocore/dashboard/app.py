"""Dashboard web UI served at /_robotocore/dashboard."""

import os

from starlette.requests import Request
from starlette.responses import HTMLResponse, JSONResponse

DASHBOARD_HTML = """\
<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Robotocore Dashboard</title>
<style>
:root {
  --bg-primary: #0d1117;
  --bg-secondary: #161b22;
  --bg-tertiary: #21262d;
  --border: #30363d;
  --text-primary: #e6edf3;
  --text-secondary: #8b949e;
  --accent: #58a6ff;
  --accent-hover: #79c0ff;
  --success: #3fb950;
  --warning: #d29922;
  --danger: #f85149;
  --font: -apple-system, BlinkMacSystemFont, "Segoe UI", Helvetica, Arial, sans-serif;
}
* { margin: 0; padding: 0; box-sizing: border-box; }
body {
  font-family: var(--font);
  background: var(--bg-primary);
  color: var(--text-primary);
  display: flex;
  min-height: 100vh;
}
nav {
  width: 220px;
  background: var(--bg-secondary);
  border-right: 1px solid var(--border);
  padding: 16px 0;
  position: fixed;
  top: 0;
  left: 0;
  height: 100vh;
  overflow-y: auto;
}
nav .logo {
  padding: 0 16px 16px;
  border-bottom: 1px solid var(--border);
  margin-bottom: 8px;
  font-size: 18px;
  font-weight: 600;
  color: var(--accent);
}
nav a {
  display: block;
  padding: 8px 16px;
  color: var(--text-secondary);
  text-decoration: none;
  font-size: 14px;
  border-left: 3px solid transparent;
}
nav a:hover, nav a.active {
  color: var(--text-primary);
  background: var(--bg-tertiary);
  border-left-color: var(--accent);
}
main {
  margin-left: 220px;
  flex: 1;
  padding: 24px;
  max-width: 1200px;
}
header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 24px;
  padding-bottom: 16px;
  border-bottom: 1px solid var(--border);
}
header h1 { font-size: 24px; }
.controls {
  display: flex;
  align-items: center;
  gap: 12px;
}
.toggle-label {
  display: flex;
  align-items: center;
  gap: 6px;
  font-size: 13px;
  color: var(--text-secondary);
  cursor: pointer;
}
.toggle-label input { cursor: pointer; }
section {
  margin-bottom: 32px;
}
section h2 {
  font-size: 18px;
  margin-bottom: 12px;
  color: var(--text-primary);
  border-bottom: 1px solid var(--border);
  padding-bottom: 8px;
}
.cards {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
  gap: 12px;
  margin-bottom: 16px;
}
.card {
  background: var(--bg-secondary);
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 16px;
}
.card .label { font-size: 12px; color: var(--text-secondary); text-transform: uppercase; }
.card .value { font-size: 28px; font-weight: 600; margin-top: 4px; }
.card .value.success { color: var(--success); }
.card .value.accent { color: var(--accent); }
table {
  width: 100%;
  border-collapse: collapse;
  background: var(--bg-secondary);
  border: 1px solid var(--border);
  border-radius: 6px;
  overflow: hidden;
  font-size: 13px;
}
th, td { padding: 8px 12px; text-align: left; border-bottom: 1px solid var(--border); }
th {
  background: var(--bg-tertiary); color: var(--text-secondary);
  font-weight: 600; cursor: pointer;
}
th:hover { color: var(--text-primary); }
tr:last-child td { border-bottom: none; }
tr:hover td { background: var(--bg-tertiary); }
.badge {
  display: inline-block;
  padding: 2px 8px;
  border-radius: 12px;
  font-size: 11px;
  font-weight: 600;
}
.badge-native { background: rgba(88,166,255,0.15); color: var(--accent); }
.badge-moto { background: rgba(63,185,80,0.15); color: var(--success); }
input[type="text"], select {
  background: var(--bg-tertiary);
  border: 1px solid var(--border);
  color: var(--text-primary);
  padding: 6px 10px;
  border-radius: 4px;
  font-size: 13px;
}
input[type="text"]:focus, select:focus { outline: none; border-color: var(--accent); }
button {
  background: var(--accent);
  color: #fff;
  border: none;
  padding: 6px 14px;
  border-radius: 4px;
  cursor: pointer;
  font-size: 13px;
  font-weight: 600;
}
button:hover { background: var(--accent-hover); }
button.danger { background: var(--danger); }
button.danger:hover { background: #ff6e6a; }
.form-row {
  display: flex;
  gap: 8px;
  align-items: center;
  margin-bottom: 12px;
  flex-wrap: wrap;
}
.form-row label {
  font-size: 13px;
  color: var(--text-secondary);
  min-width: 80px;
}
.search-bar {
  margin-bottom: 12px;
}
.search-bar input {
  width: 100%;
  max-width: 400px;
}
.tree-item {
  margin-left: 16px;
}
.tree-toggle {
  cursor: pointer;
  user-select: none;
  padding: 4px 0;
  font-size: 14px;
}
.tree-toggle:hover { color: var(--accent); }
.tree-children { display: none; margin-left: 16px; }
.tree-children.open { display: block; }
.hidden { display: none; }
.loading { color: var(--text-secondary); font-style: italic; }
td, .tree-toggle, .tree-children div { font-family: "SF Mono", Menlo, Consolas, monospace; }
.health-item {
  background: var(--bg-secondary);
  border: 1px solid var(--border);
  border-radius: 6px;
  padding: 12px 16px;
  display: flex;
  align-items: center;
  gap: 10px;
  font-size: 13px;
}
.health-dot {
  width: 10px; height: 10px; border-radius: 50%; flex-shrink: 0;
}
.health-dot.running { background: var(--success); }
.health-dot.degraded { background: var(--warning); }
.health-dot.down { background: var(--danger); }
.health-item .svc-name { font-weight: 600; }
.health-item .svc-type { color: var(--text-secondary); font-size: 11px; }
.health-item .svc-reqs { margin-left: auto; color: var(--text-secondary); font-size: 12px; }
@media (max-width: 768px) {
  nav { width: 180px; }
  main { margin-left: 180px; padding: 16px; }
  .cards { grid-template-columns: 1fr 1fr; }
}
</style>
</head>
<body>
<nav>
  <div class="logo">Robotocore</div>
  <a href="#overview" class="active" data-section="overview">Overview</a>
  <a href="#resources" data-section="resources">Resources</a>
  <a href="#audit" data-section="audit">Audit Log</a>
  <a href="#state" data-section="state">State / Snapshots</a>
  <a href="#health" data-section="health">Health</a>
  <a href="#chaos" data-section="chaos">Chaos Rules</a>
  <a href="#config" data-section="config">Configuration</a>
  <a href="#services" data-section="services">Services</a>
</nav>
<main>
  <header>
    <h1 id="page-title">Overview</h1>
    <div class="controls">
      <label class="toggle-label">
        <input type="checkbox" id="autoRefresh"> Auto-refresh (5s)
      </label>
    </div>
  </header>

  <section id="overview">
    <div class="cards">
      <div class="card">
        <div class="label">Status</div>
        <div class="value success" id="status-value">--</div>
      </div>
      <div class="card">
        <div class="label">Version</div>
        <div class="value" id="version-value">--</div>
      </div>
      <div class="card">
        <div class="label">Uptime</div>
        <div class="value" id="uptime-value">--</div>
      </div>
      <div class="card">
        <div class="label">Services</div>
        <div class="value accent" id="services-count-value">--</div>
      </div>
      <div class="card">
        <div class="label">Total Requests</div>
        <div class="value" id="requests-value">--</div>
      </div>
      <div class="card">
        <div class="label">Error Rate</div>
        <div class="value" id="error-rate-value">--</div>
      </div>
    </div>
  </section>

  <section id="resources" class="hidden">
    <h2>Resource Browser</h2>
    <div id="resources-tree" class="loading">Loading resources...</div>
  </section>

  <section id="chaos" class="hidden">
    <h2>Chaos Engineering</h2>
    <div style="margin-bottom: 16px;">
      <h3 style="font-size: 14px; margin-bottom: 8px; color: var(--text-secondary);">Add Rule</h3>
      <div class="form-row">
        <label>Service</label>
        <input type="text" id="chaos-service" placeholder="e.g. s3 or *">
      </div>
      <div class="form-row">
        <label>Operation</label>
        <input type="text" id="chaos-operation" placeholder="e.g. PutObject or *">
      </div>
      <div class="form-row">
        <label>Error Code</label>
        <input type="text" id="chaos-error"
          placeholder="e.g. ThrottlingException"
          value="ThrottlingException">
      </div>
      <div class="form-row">
        <label>Rate (%)</label>
        <input type="text" id="chaos-rate" placeholder="0-100" value="100">
      </div>
      <div class="form-row">
        <button onclick="addChaosRule()">Add Rule</button>
      </div>
    </div>
    <table id="chaos-table">
      <thead>
        <tr>
          <th>ID</th>
          <th>Service</th>
          <th>Operation</th>
          <th>Error</th>
          <th>Rate</th>
          <th>Actions</th>
        </tr>
      </thead>
      <tbody id="chaos-body">
        <tr><td colspan="6" class="loading">Loading...</td></tr>
      </tbody>
    </table>
  </section>

  <section id="audit" class="hidden">
    <h2>Audit Log</h2>
    <div class="search-bar">
      <input type="text" id="audit-search"
        placeholder="Filter by service, operation, or status..."
        oninput="filterAudit()">
    </div>
    <table id="audit-table">
      <thead>
        <tr>
          <th>Time</th>
          <th>Service</th>
          <th>Operation</th>
          <th>Status</th>
          <th>Duration</th>
        </tr>
      </thead>
      <tbody id="audit-body">
        <tr><td colspan="5" class="loading">Loading...</td></tr>
      </tbody>
    </table>
  </section>

  <section id="state" class="hidden">
    <h2>State / Snapshots</h2>
    <div style="margin-bottom: 16px;">
      <div class="form-row">
        <label>Name</label>
        <input type="text" id="state-name" placeholder="Snapshot name">
        <button onclick="saveSnapshot()">Save Snapshot</button>
        <button class="danger" onclick="resetState()">Reset All State</button>
      </div>
    </div>
    <table id="state-table">
      <thead>
        <tr>
          <th>Name</th>
          <th>Created</th>
          <th>Services</th>
          <th>Size</th>
          <th>Actions</th>
        </tr>
      </thead>
      <tbody id="state-body">
        <tr><td colspan="5" class="loading">Loading...</td></tr>
      </tbody>
    </table>
  </section>

  <section id="health" class="hidden">
    <h2>Service Health</h2>
    <div id="health-grid" class="cards loading">Loading health data...</div>
  </section>

  <section id="config" class="hidden">
    <h2>Configuration</h2>
    <table id="config-table">
      <thead>
        <tr><th>Setting</th><th>Value</th></tr>
      </thead>
      <tbody id="config-body">
        <tr><td colspan="2" class="loading">Loading...</td></tr>
      </tbody>
    </table>
  </section>

  <section id="services" class="hidden">
    <h2>Service Registry</h2>
    <div class="search-bar">
      <input type="text" id="services-search"
        placeholder="Filter services..."
        oninput="filterServices()">
    </div>
    <table id="services-table">
      <thead>
        <tr>
          <th onclick="sortServices('name')">Name</th>
          <th onclick="sortServices('status')">Provider</th>
          <th onclick="sortServices('protocol')">Protocol</th>
          <th>Description</th>
        </tr>
      </thead>
      <tbody id="services-body">
        <tr><td colspan="4" class="loading">Loading...</td></tr>
      </tbody>
    </table>
  </section>
</main>

<script>
(function() {
  "use strict";

  var refreshTimer = null;
  var allAuditEntries = [];
  var allServices = [];
  var servicesSortKey = "name";
  var servicesSortAsc = true;

  // Navigation
  document.querySelectorAll("nav a").forEach(function(a) {
    a.addEventListener("click", function(e) {
      e.preventDefault();
      var section = this.getAttribute("data-section");
      showSection(section);
    });
  });

  function showSection(name) {
    document.querySelectorAll("main > section").forEach(function(s) {
      s.classList.add("hidden");
    });
    var el = document.getElementById(name);
    if (el) el.classList.remove("hidden");
    document.querySelectorAll("nav a").forEach(function(a) {
      a.classList.toggle("active", a.getAttribute("data-section") === name);
    });
    document.getElementById("page-title").textContent =
      name.charAt(0).toUpperCase() + name.slice(1);
    refreshSection(name);
  }

  // Auto-refresh toggle
  var autoRefreshCheckbox = document.getElementById("autoRefresh");
  autoRefreshCheckbox.addEventListener("change", function() {
    if (this.checked) {
      refreshTimer = setInterval(refreshCurrent, 5000);
    } else {
      clearInterval(refreshTimer);
      refreshTimer = null;
    }
  });

  function refreshCurrent() {
    var active = document.querySelector("nav a.active");
    if (active) refreshSection(active.getAttribute("data-section"));
  }

  function refreshSection(name) {
    if (name === "overview") loadOverview();
    else if (name === "resources") loadResources();
    else if (name === "chaos") loadChaosRules();
    else if (name === "audit") loadAudit();
    else if (name === "state") loadSnapshots();
    else if (name === "health") loadHealth();
    else if (name === "config") loadConfig();
    else if (name === "services") loadServices();
  }

  // ----- Overview -----
  function loadOverview() {
    fetch("/_robotocore/health")
      .then(function(r) { return r.json(); })
      .then(function(data) {
        document.getElementById("status-value").textContent = data.status || "--";
        document.getElementById("version-value").textContent = data.version || "--";
        var uptime = data.uptime_seconds || 0;
        document.getElementById("uptime-value").textContent = formatUptime(uptime);
        var svcObj = data.services || {};
        var svcCount = Object.keys(svcObj).length;
        document.getElementById("services-count-value").textContent = svcCount;
        var totalReqs = 0;
        for (var k in svcObj) { totalReqs += (svcObj[k].requests || 0); }
        document.getElementById("requests-value").textContent = totalReqs;
      })
      .catch(function() {
        document.getElementById("status-value").textContent = "error";
      });
    // Error rate placeholder
    document.getElementById("error-rate-value").textContent = "0%";
  }

  function formatUptime(seconds) {
    if (seconds < 60) return Math.round(seconds) + "s";
    if (seconds < 3600) return Math.round(seconds / 60) + "m";
    return Math.round(seconds / 3600) + "h " + Math.round((seconds % 3600) / 60) + "m";
  }

  // ----- Resources -----
  function loadResources() {
    var container = document.getElementById("resources-tree");
    container.innerHTML = '<span class="loading">Loading resources...</span>';
    fetch("/_robotocore/resources")
      .then(function(r) { return r.json(); })
      .then(function(data) {
        var resources = data.resources || {};
        container.innerHTML = "";
        var services = Object.keys(resources).sort();
        if (services.length === 0) {
          container.textContent = "No resources found.";
          return;
        }
        services.forEach(function(svc) {
          var item = document.createElement("div");
          item.className = "tree-item";
          var toggle = document.createElement("div");
          toggle.className = "tree-toggle";
          var count = 0;
          var svcData = resources[svc];
          if (typeof svcData === "number") { count = svcData; }
          else if (typeof svcData === "object") {
            for (var k in svcData) { count += (typeof svcData[k] === "number" ? svcData[k] : 0); }
          }
          toggle.textContent = "\u25b6 " + svc + " (" + count + ")";
          var children = document.createElement("div");
          children.className = "tree-children";
          toggle.addEventListener("click", function() {
            var open = children.classList.toggle("open");
            toggle.textContent = (open ? "\u25bc " : "\u25b6 ") + svc + " (" + count + ")";
            if (open && children.innerHTML === "") {
              fetch("/_robotocore/resources/" + svc)
                .then(function(r) { return r.json(); })
                .then(function(d) {
                  var res = d.resources || {};
                  if (Array.isArray(res)) {
                    res.forEach(function(r) {
                      var p = document.createElement("div");
                      p.style.padding = "2px 0";
                      p.style.fontSize = "13px";
                      p.style.color = "var(--text-secondary)";
                      p.textContent = typeof r === "string" ? r : JSON.stringify(r);
                      children.appendChild(p);
                    });
                  } else {
                    for (var rtype in res) {
                      var p = document.createElement("div");
                      p.style.padding = "2px 0";
                      p.style.fontSize = "13px";
                      p.textContent = rtype + ": " + JSON.stringify(res[rtype]);
                      children.appendChild(p);
                    }
                  }
                  if (children.children.length === 0) {
                    children.textContent = "(empty)";
                  }
                });
            }
          });
          item.appendChild(toggle);
          item.appendChild(children);
          container.appendChild(item);
        });
      })
      .catch(function() { container.textContent = "Failed to load resources."; });
  }

  // ----- Chaos Rules -----
  function loadChaosRules() {
    fetch("/_robotocore/chaos/rules")
      .then(function(r) { return r.json(); })
      .then(function(data) {
        var body = document.getElementById("chaos-body");
        var rules = data.rules || [];
        if (rules.length === 0) {
          body.innerHTML = '<tr><td colspan="6" ' +
            'style="color:var(--text-secondary)">' +
            'No active rules</td></tr>';
          return;
        }
        body.innerHTML = "";
        rules.forEach(function(rule) {
          var tr = document.createElement("tr");
          tr.innerHTML =
            "<td>" + esc(rule.id || rule.rule_id || "--") + "</td>" +
            "<td>" + esc(rule.service || "*") + "</td>" +
            "<td>" + esc(rule.operation || "*") + "</td>" +
            "<td>" + esc(rule.error_code || rule.error || "--") + "</td>" +
            "<td>" + (rule.rate != null ? rule.rate + "%" : "100%") + "</td>" +
            '<td><button class="danger" onclick="deleteChaosRule(\'' +
            esc(rule.id || rule.rule_id) + '\')">Delete</button></td>';
          body.appendChild(tr);
        });
      })
      .catch(function() {
        document.getElementById("chaos-body").innerHTML =
          '<tr><td colspan="6">Failed to load</td></tr>';
      });
  }

  window.addChaosRule = function() {
    var rule = {
      service: document.getElementById("chaos-service").value || "*",
      operation: document.getElementById("chaos-operation").value || "*",
      error_code: document.getElementById("chaos-error").value || "ThrottlingException",
      rate: parseInt(document.getElementById("chaos-rate").value || "100", 10)
    };
    fetch("/_robotocore/chaos/rules", {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify(rule)
    }).then(function() { loadChaosRules(); });
  };

  window.deleteChaosRule = function(ruleId) {
    fetch("/_robotocore/chaos/rules/" + ruleId, { method: "DELETE" })
      .then(function() { loadChaosRules(); });
  };

  // ----- Audit Log -----
  function loadAudit() {
    fetch("/_robotocore/audit?limit=200")
      .then(function(r) { return r.json(); })
      .then(function(data) {
        allAuditEntries = data.entries || [];
        renderAudit(allAuditEntries);
      })
      .catch(function() {
        document.getElementById("audit-body").innerHTML =
          '<tr><td colspan="5">Failed to load</td></tr>';
      });
  }

  function renderAudit(entries) {
    var body = document.getElementById("audit-body");
    if (entries.length === 0) {
      body.innerHTML = '<tr><td colspan="5" ' +
        'style="color:var(--text-secondary)">' +
        'No entries</td></tr>';
      return;
    }
    body.innerHTML = "";
    entries.forEach(function(e) {
      var tr = document.createElement("tr");
      var ts = e.timestamp || e.time || "--";
      if (typeof ts === "number") ts = new Date(ts * 1000).toLocaleTimeString();
      var status = e.status_code || e.status || "--";
      var statusColor = status >= 400 ? "var(--danger)" : "var(--success)";
      tr.innerHTML =
        "<td>" + esc(ts) + "</td>" +
        "<td>" + esc(e.service || "--") + "</td>" +
        "<td>" + esc(e.operation || e.action || "--") + "</td>" +
        '<td style="color:' + statusColor + '">' + status + "</td>" +
        "<td>" + (e.duration_ms != null ? e.duration_ms + "ms" : "--") + "</td>";
      body.appendChild(tr);
    });
  }

  window.filterAudit = function() {
    var q = document.getElementById("audit-search").value.toLowerCase();
    if (!q) { renderAudit(allAuditEntries); return; }
    var filtered = allAuditEntries.filter(function(e) {
      return (e.service || "").toLowerCase().indexOf(q) !== -1 ||
        (e.operation || e.action || "").toLowerCase().indexOf(q) !== -1 ||
        String(e.status_code || e.status || "").indexOf(q) !== -1;
    });
    renderAudit(filtered);
  };

  // ----- State / Snapshots -----
  function loadSnapshots() {
    fetch("/_robotocore/state/snapshots")
      .then(function(r) { return r.json(); })
      .then(function(data) {
        var body = document.getElementById("state-body");
        var snaps = data.snapshots || [];
        if (snaps.length === 0) {
          body.innerHTML = '<tr><td colspan="5" ' +
            'style="color:var(--text-secondary)">' +
            'No snapshots saved</td></tr>';
          return;
        }
        body.innerHTML = "";
        snaps.forEach(function(snap) {
          var tr = document.createElement("tr");
          var name = snap.name || snap.path || "--";
          var created = snap.created || snap.timestamp || "--";
          if (typeof created === "number") created = new Date(created * 1000).toLocaleString();
          var services = snap.services ? snap.services.join(", ") : "all";
          var size = snap.size || snap.size_bytes || "--";
          if (typeof size === "number") size = (size / 1024).toFixed(1) + " KB";
          tr.innerHTML =
            "<td>" + esc(name) + "</td>" +
            "<td>" + esc(created) + "</td>" +
            "<td>" + esc(services) + "</td>" +
            "<td>" + esc(size) + "</td>" +
            '<td><button onclick="loadSnapshot(\'' + esc(name) +
            '\')">Load</button> ' +
            '<button class="danger" onclick="deleteSnapshot(\'' + esc(name) +
            '\')">Delete</button></td>';
          body.appendChild(tr);
        });
      })
      .catch(function() {
        document.getElementById("state-body").innerHTML =
          '<tr><td colspan="5">Failed to load</td></tr>';
      });
  }

  window.saveSnapshot = function() {
    var name = document.getElementById("state-name").value;
    if (!name) { alert("Enter a snapshot name"); return; }
    fetch("/_robotocore/state/save", {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify({name: name})
    }).then(function() {
      document.getElementById("state-name").value = "";
      loadSnapshots();
    });
  };

  window.loadSnapshot = function(name) {
    fetch("/_robotocore/state/load", {
      method: "POST",
      headers: {"Content-Type": "application/json"},
      body: JSON.stringify({name: name})
    }).then(function() { loadSnapshots(); });
  };

  window.deleteSnapshot = function(name) {
    if (!confirm("Delete snapshot '" + name + "'?")) return;
    fetch("/_robotocore/state/reset", { method: "POST" })
      .then(function() { loadSnapshots(); });
  };

  window.resetState = function() {
    if (!confirm("Reset ALL emulator state? This cannot be undone.")) return;
    fetch("/_robotocore/state/reset", { method: "POST" })
      .then(function() { loadSnapshots(); });
  };

  // ----- Health -----
  function loadHealth() {
    var container = document.getElementById("health-grid");
    container.innerHTML = '<span class="loading">Loading health data...</span>';
    fetch("/_robotocore/health")
      .then(function(r) { return r.json(); })
      .then(function(data) {
        var services = data.services || {};
        var names = Object.keys(services).sort();
        container.innerHTML = "";
        container.className = "cards";
        if (names.length === 0) {
          container.textContent = "No services found.";
          return;
        }
        names.forEach(function(name) {
          var svc = services[name];
          var status = svc.status || "unknown";
          var dotClass = status === "running" ? "running"
            : status === "degraded" ? "degraded" : "down";
          var item = document.createElement("div");
          item.className = "health-item";
          item.innerHTML =
            '<span class="health-dot ' + dotClass + '"></span>' +
            '<span class="svc-name">' + esc(name) + '</span>' +
            '<span class="svc-type">' + esc(svc.type || "") + '</span>' +
            '<span class="svc-reqs">' + (svc.requests || 0) + ' reqs</span>';
          container.appendChild(item);
        });
      })
      .catch(function() {
        container.innerHTML = "Failed to load health data.";
      });
  }

  // ----- Config -----
  function loadConfig() {
    fetch("/_robotocore/config")
      .then(function(r) { return r.json(); })
      .then(function(data) {
        var body = document.getElementById("config-body");
        body.innerHTML = "";
        for (var key in data) {
          var tr = document.createElement("tr");
          tr.innerHTML = "<td>" + esc(key) + "</td><td>" + esc(String(data[key])) + "</td>";
          body.appendChild(tr);
        }
      })
      .catch(function() {
        document.getElementById("config-body").innerHTML =
          '<tr><td colspan="2">Failed to load</td></tr>';
      });
  }

  // ----- Services -----
  function loadServices() {
    fetch("/_robotocore/services")
      .then(function(r) { return r.json(); })
      .then(function(data) {
        allServices = data.services || [];
        renderServices();
      })
      .catch(function() {
        document.getElementById("services-body").innerHTML =
          '<tr><td colspan="4">Failed to load</td></tr>';
      });
  }

  function renderServices() {
    var q = (document.getElementById("services-search").value || "").toLowerCase();
    var list = allServices;
    if (q) {
      list = list.filter(function(s) {
        return s.name.toLowerCase().indexOf(q) !== -1 ||
          (s.protocol || "").toLowerCase().indexOf(q) !== -1 ||
          (s.status || "").toLowerCase().indexOf(q) !== -1;
      });
    }
    list.sort(function(a, b) {
      var va = (a[servicesSortKey] || "").toLowerCase();
      var vb = (b[servicesSortKey] || "").toLowerCase();
      if (va < vb) return servicesSortAsc ? -1 : 1;
      if (va > vb) return servicesSortAsc ? 1 : -1;
      return 0;
    });
    var body = document.getElementById("services-body");
    body.innerHTML = "";
    list.forEach(function(s) {
      var tr = document.createElement("tr");
      var badge = s.status === "native"
        ? '<span class="badge badge-native">native</span>'
        : '<span class="badge badge-moto">moto</span>';
      tr.innerHTML =
        "<td>" + esc(s.name) + "</td>" +
        "<td>" + badge + "</td>" +
        "<td>" + esc(s.protocol || "--") + "</td>" +
        "<td>" + esc(s.description || "") + "</td>";
      body.appendChild(tr);
    });
    if (list.length === 0) {
      body.innerHTML = '<tr><td colspan="4" ' +
        'style="color:var(--text-secondary)">' +
        'No matches</td></tr>';
    }
  }

  window.sortServices = function(key) {
    if (servicesSortKey === key) servicesSortAsc = !servicesSortAsc;
    else { servicesSortKey = key; servicesSortAsc = true; }
    renderServices();
  };

  window.filterServices = function() { renderServices(); };

  function esc(s) {
    if (s == null) return "";
    return String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  // Initial load
  loadOverview();
})();
</script>
</body>
</html>
"""


async def dashboard_endpoint(request: Request) -> HTMLResponse | JSONResponse:
    """Serve the dashboard HTML or return 404 if disabled."""
    if os.environ.get("DASHBOARD_DISABLED", "0") == "1":
        return JSONResponse({"error": "Dashboard is disabled"}, status_code=404)
    return HTMLResponse(DASHBOARD_HTML)
