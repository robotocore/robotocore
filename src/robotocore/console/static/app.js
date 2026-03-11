/* ============================================================
   Robotocore Console -- Main Application
   ============================================================ */

(function () {
  "use strict";

  // ---------------------------------------------------------------------------
  // Global state
  // ---------------------------------------------------------------------------
  var state = {
    currentPage: "dashboard",
    region: "us-east-1",
    accountId: "123456789012",
    connected: false,
  };

  // ---------------------------------------------------------------------------
  // Utility: HTML escaping
  // ---------------------------------------------------------------------------
  function esc(s) {
    if (s == null) return "";
    return String(s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  // ---------------------------------------------------------------------------
  // Utility: format bytes
  // ---------------------------------------------------------------------------
  function formatBytes(bytes) {
    if (bytes === 0 || bytes == null) return "0 B";
    var units = ["B", "KB", "MB", "GB", "TB"];
    var i = Math.floor(Math.log(bytes) / Math.log(1024));
    return (bytes / Math.pow(1024, i)).toFixed(1) + " " + units[i];
  }

  // ---------------------------------------------------------------------------
  // Utility: format date
  // ---------------------------------------------------------------------------
  function formatDate(d) {
    if (!d) return "--";
    var date = new Date(d);
    if (isNaN(date.getTime())) return String(d);
    return date.toLocaleString();
  }

  function formatTime(d) {
    if (!d) return "--";
    if (typeof d === "number") d = new Date(d * 1000);
    else d = new Date(d);
    if (isNaN(d.getTime())) return "--";
    return d.toLocaleTimeString();
  }

  // ---------------------------------------------------------------------------
  // API helper
  // ---------------------------------------------------------------------------
  function apiCall(service, action, params) {
    var body = Object.assign({}, params || {}, {
      _region: state.region,
      _account_id: state.accountId,
    });
    return fetch("/_robotocore/console/api/" + service + "/" + action, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    }).then(function (r) {
      if (!r.ok) {
        return r.text().then(function (t) {
          throw new Error("API error " + r.status + ": " + t);
        });
      }
      var ct = r.headers.get("content-type") || "";
      if (ct.indexOf("json") !== -1) return r.json();
      if (ct.indexOf("xml") !== -1) return r.text();
      return r.text();
    });
  }

  // Fetch from management endpoints
  function mgmtFetch(path) {
    return fetch(path).then(function (r) {
      return r.json();
    });
  }

  // ---------------------------------------------------------------------------
  // Toast notifications
  // ---------------------------------------------------------------------------
  function toast(message, type) {
    type = type || "info";
    var container = document.getElementById("toast-container");
    var el = document.createElement("div");
    el.className = "toast " + type;
    el.textContent = message;
    container.appendChild(el);
    setTimeout(function () {
      el.classList.add("toast-exit");
      setTimeout(function () {
        el.remove();
      }, 300);
    }, 4000);
  }

  // ---------------------------------------------------------------------------
  // Modal
  // ---------------------------------------------------------------------------
  function showModal(title, bodyHtml, footerHtml) {
    document.getElementById("modal-title").textContent = title;
    document.getElementById("modal-body").innerHTML = bodyHtml;
    document.getElementById("modal-footer").innerHTML = footerHtml || "";
    document.getElementById("modal-overlay").classList.remove("hidden");
  }

  function closeModal() {
    document.getElementById("modal-overlay").classList.add("hidden");
  }

  // ---------------------------------------------------------------------------
  // Navigation
  // ---------------------------------------------------------------------------
  function navigate(page, params) {
    state.currentPage = page;
    // Update nav
    document.querySelectorAll(".nav-item").forEach(function (a) {
      a.classList.toggle("active", a.getAttribute("data-page") === page);
    });
    // Render page
    renderPage(page, params);
  }

  document.querySelectorAll(".nav-item[data-page]").forEach(function (a) {
    a.addEventListener("click", function (e) {
      e.preventDefault();
      navigate(this.getAttribute("data-page"));
    });
  });

  // Region selector
  document
    .getElementById("region-select")
    .addEventListener("change", function () {
      state.region = this.value;
      renderPage(state.currentPage);
    });

  // Handle hash navigation
  function handleHash() {
    var hash = location.hash.replace("#", "") || "dashboard";
    var parts = hash.split("/");
    navigate(parts[0], parts.slice(1));
  }
  window.addEventListener("hashchange", handleHash);

  // Close modal on overlay click
  document
    .getElementById("modal-overlay")
    .addEventListener("click", function (e) {
      if (e.target === this) closeModal();
    });

  // ---------------------------------------------------------------------------
  // Page Router
  // ---------------------------------------------------------------------------
  function renderPage(page, params) {
    var content = document.getElementById("page-content");
    var title = document.getElementById("page-title");
    var breadcrumb = document.getElementById("breadcrumb");

    if (page === "dashboard") {
      title.textContent = "Dashboard";
      breadcrumb.innerHTML = "";
      renderDashboard(content);
    } else if (page === "s3" && window.S3Console) {
      window.S3Console.render(content, title, breadcrumb, params);
    } else if (page === "dynamodb" && window.DynamoDBConsole) {
      window.DynamoDBConsole.render(content, title, breadcrumb, params);
    } else if (page === "sqs" && window.SQSConsole) {
      window.SQSConsole.render(content, title, breadcrumb, params);
    } else if (page === "lambda" && window.LambdaConsole) {
      window.LambdaConsole.render(content, title, breadcrumb, params);
    } else if (page === "cloudwatch" && window.CloudWatchConsole) {
      window.CloudWatchConsole.render(content, title, breadcrumb, params);
    } else {
      title.textContent = page;
      breadcrumb.innerHTML = "";
      content.innerHTML =
        '<div class="loading-state"><div class="loading-spinner"></div>Loading...</div>';
    }
  }

  // ---------------------------------------------------------------------------
  // Dashboard Page
  // ---------------------------------------------------------------------------
  function renderDashboard(container) {
    container.innerHTML =
      '<div class="loading-state"><div class="loading-spinner"></div>Loading dashboard...</div>';

    Promise.all([
      mgmtFetch("/_robotocore/health"),
      mgmtFetch("/_robotocore/resources"),
      mgmtFetch("/_robotocore/audit?limit=15"),
    ])
      .then(function (results) {
        var health = results[0];
        var resources = results[1];
        var audit = results[2];

        state.connected = true;
        document.getElementById("status-dot").className = "status-dot connected";
        document.getElementById("status-text").textContent = "Connected";

        var resourceCounts = resources.resources || {};
        var totalResources = 0;
        for (var k in resourceCounts) {
          var v = resourceCounts[k];
          if (typeof v === "number") totalResources += v;
          else if (typeof v === "object") {
            for (var j in v)
              if (typeof v[j] === "number") totalResources += v[j];
          }
        }

        var svcObj = health.services || {};
        var totalReqs = 0;
        for (var s in svcObj) totalReqs += svcObj[s].requests || 0;

        var uptime = health.uptime_seconds || 0;
        var uptimeStr;
        if (uptime < 60) uptimeStr = Math.round(uptime) + "s";
        else if (uptime < 3600)
          uptimeStr = Math.round(uptime / 60) + "m";
        else
          uptimeStr =
            Math.floor(uptime / 3600) +
            "h " +
            Math.round((uptime % 3600) / 60) +
            "m";

        var html = "";

        // Stats cards
        html += '<div class="cards-grid">';
        html +=
          '<div class="stat-card"><div class="label">Status</div><div class="value success">' +
          esc(health.status || "unknown") +
          "</div></div>";
        html +=
          '<div class="stat-card"><div class="label">Services</div><div class="value gold">' +
          Object.keys(svcObj).length +
          "</div></div>";
        html +=
          '<div class="stat-card"><div class="label">Resources</div><div class="value info">' +
          totalResources +
          "</div></div>";
        html +=
          '<div class="stat-card"><div class="label">API Calls</div><div class="value">' +
          totalReqs +
          "</div></div>";
        html +=
          '<div class="stat-card"><div class="label">Uptime</div><div class="value">' +
          uptimeStr +
          "</div></div>";
        html +=
          '<div class="stat-card"><div class="label">Version</div><div class="value" style="font-size:16px">' +
          esc(health.version || "--") +
          "</div></div>";
        html += "</div>";

        // Service cards
        html +=
          '<div class="section-header mt-24"><h2>Services</h2></div>';
        html += '<div class="cards-grid">';

        var serviceCards = [
          {
            id: "s3",
            name: "S3",
            icon: "&#9707;",
            desc: "Object Storage",
            countKey: "s3",
          },
          {
            id: "dynamodb",
            name: "DynamoDB",
            icon: "&#9783;",
            desc: "NoSQL Database",
            countKey: "dynamodb",
          },
          {
            id: "sqs",
            name: "SQS",
            icon: "&#9993;",
            desc: "Message Queue",
            countKey: "sqs",
          },
          {
            id: "lambda",
            name: "Lambda",
            icon: "&#955;",
            desc: "Serverless Compute",
            countKey: "lambda",
          },
          {
            id: "cloudwatch",
            name: "CloudWatch",
            icon: "&#9729;",
            desc: "Logs & Metrics",
            countKey: "logs",
          },
        ];

        serviceCards.forEach(function (sc) {
          var count = 0;
          var r = resourceCounts[sc.countKey];
          if (typeof r === "number") count = r;
          else if (typeof r === "object") {
            for (var x in r)
              if (typeof r[x] === "number") count += r[x];
          }
          html +=
            '<div class="service-card" onclick="location.hash=\'' +
            sc.id +
            "'\">" +
            '<div class="icon">' +
            sc.icon +
            "</div>" +
            '<div class="info"><h3>' +
            sc.name +
            "</h3>" +
            '<div class="count">' +
            sc.desc +
            " &middot; " +
            count +
            " resources</div></div></div>";
        });
        html += "</div>";

        // Quick actions
        html +=
          '<div class="section-header mt-24"><h2>Quick Actions</h2></div>';
        html += '<div class="quick-actions">';
        html +=
          '<button class="quick-action" id="qa-create-bucket"><span class="qa-icon">&#9707;</span>Create S3 Bucket</button>';
        html +=
          '<button class="quick-action" id="qa-create-queue"><span class="qa-icon">&#9993;</span>Create SQS Queue</button>';
        html +=
          '<button class="quick-action" id="qa-create-table"><span class="qa-icon">&#9783;</span>Create DynamoDB Table</button>';
        html += "</div>";

        // Recent API calls
        html +=
          '<div class="section-header mt-24"><h2>Recent API Calls</h2></div>';
        var entries = audit.entries || [];
        if (entries.length === 0) {
          html +=
            '<div class="text-muted text-center" style="padding:20px">No recent API calls</div>';
        } else {
          html += '<div class="panel"><div class="panel-body" style="padding:12px 16px">';
          entries.slice(0, 10).forEach(function (e) {
            var status = e.status_code || e.status || "--";
            var statusClass =
              status >= 400 ? "text-danger" : "text-success";
            var ts = e.timestamp || e.time || "";
            html +=
              '<div class="audit-entry">' +
              '<span class="audit-time">' +
              formatTime(ts) +
              "</span>" +
              '<span class="audit-service">' +
              esc(e.service || "--") +
              "</span>" +
              '<span class="audit-action">' +
              esc(e.operation || e.action || "--") +
              "</span>" +
              '<span class="audit-status ' +
              statusClass +
              '">' +
              status +
              "</span>" +
              "</div>";
          });
          html += "</div></div>";
        }

        container.innerHTML = html;

        // Quick action handlers
        var qaBucket = document.getElementById("qa-create-bucket");
        if (qaBucket)
          qaBucket.addEventListener("click", function () {
            navigate("s3", ["create"]);
          });
        var qaQueue = document.getElementById("qa-create-queue");
        if (qaQueue)
          qaQueue.addEventListener("click", function () {
            navigate("sqs", ["create"]);
          });
        var qaTable = document.getElementById("qa-create-table");
        if (qaTable)
          qaTable.addEventListener("click", function () {
            navigate("dynamodb", ["create"]);
          });
      })
      .catch(function (err) {
        state.connected = false;
        document.getElementById("status-dot").className = "status-dot error";
        document.getElementById("status-text").textContent = "Disconnected";
        container.innerHTML =
          '<div class="loading-state text-danger">Failed to connect to server: ' +
          esc(err.message) +
          "</div>";
      });
  }

  // ---------------------------------------------------------------------------
  // Expose globals for service modules
  // ---------------------------------------------------------------------------
  window.AppUI = {
    state: state,
    esc: esc,
    formatBytes: formatBytes,
    formatDate: formatDate,
    formatTime: formatTime,
    apiCall: apiCall,
    mgmtFetch: mgmtFetch,
    toast: toast,
    showModal: showModal,
    closeModal: closeModal,
    navigate: navigate,
  };

  // ---------------------------------------------------------------------------
  // Initial load
  // ---------------------------------------------------------------------------
  handleHash();
})();
