/* ============================================================
   CloudWatch Logs Console Module
   ============================================================ */

(function () {
  "use strict";

  var UI = window.AppUI;

  window.CloudWatchConsole = {
    render: function (container, titleEl, breadcrumb, params) {
      if (params && params.length >= 2) {
        var group = decodeURIComponent(params[0]);
        var stream = decodeURIComponent(params[1]);
        showLogEvents(container, titleEl, breadcrumb, group, stream);
      } else if (params && params[0]) {
        var groupName = decodeURIComponent(params[0]);
        showLogStreams(container, titleEl, breadcrumb, groupName);
      } else {
        showLogGroups(container, titleEl, breadcrumb);
      }
    },
  };

  function showLogGroups(container, titleEl, breadcrumb) {
    titleEl.textContent = "CloudWatch Log Groups";
    breadcrumb.innerHTML = "CloudWatch Logs";
    container.innerHTML =
      '<div class="loading-state"><div class="loading-spinner"></div>Loading log groups...</div>';

    UI.apiCall("logs", "DescribeLogGroups", {})
      .then(function (data) {
        var groups = data.logGroups || [];

        var html = "";
        html +=
          '<div class="section-header"><h2>Log Groups (' +
          groups.length +
          ")</h2></div>";

        if (groups.length === 0) {
          html +=
            '<div class="data-table-wrapper"><table class="data-table"><tbody>' +
            '<tr><td class="empty-state">No log groups found. Log groups are created automatically when services emit logs.</td></tr>' +
            "</tbody></table></div>";
        } else {
          html +=
            '<div class="data-table-wrapper"><table class="data-table">' +
            "<thead><tr><th>Log Group Name</th><th>Stored Bytes</th><th>Retention</th><th>Created</th></tr></thead><tbody>";

          groups.forEach(function (g) {
            var retention = g.retentionInDays
              ? g.retentionInDays + " days"
              : "Never expire";
            html +=
              '<tr class="clickable" data-group="' +
              UI.esc(g.logGroupName) +
              '">' +
              '<td><strong class="mono">' +
              UI.esc(g.logGroupName) +
              "</strong></td>" +
              "<td>" +
              UI.formatBytes(g.storedBytes || 0) +
              "</td>" +
              "<td>" +
              retention +
              "</td>" +
              "<td>" +
              UI.formatDate(g.creationTime) +
              "</td>" +
              "</tr>";
          });
          html += "</tbody></table></div>";
        }

        container.innerHTML = html;

        container
          .querySelectorAll("tr.clickable")
          .forEach(function (tr) {
            tr.addEventListener("click", function () {
              location.hash =
                "cloudwatch/" +
                encodeURIComponent(this.getAttribute("data-group"));
            });
          });
      })
      .catch(function (err) {
        container.innerHTML =
          '<div class="loading-state text-danger">Error: ' +
          UI.esc(err.message) +
          "</div>";
      });
  }

  function showLogStreams(container, titleEl, breadcrumb, groupName) {
    titleEl.textContent = groupName.split("/").pop();
    breadcrumb.innerHTML =
      '<a href="#cloudwatch">CloudWatch Logs</a> &rsaquo; ' +
      UI.esc(groupName);
    container.innerHTML =
      '<div class="loading-state"><div class="loading-spinner"></div>Loading log streams...</div>';

    UI.apiCall("logs", "DescribeLogStreams", {
      logGroupName: groupName,
    })
      .then(function (data) {
        var streams = data.logStreams || [];

        var html = "";
        html +=
          '<div class="toolbar"><button class="btn btn-secondary" onclick="location.hash=\'cloudwatch\'">Back to Log Groups</button></div>';
        html +=
          '<div class="section-header"><h2>Log Streams (' +
          streams.length +
          ")</h2></div>";

        // Search bar
        html +=
          '<div class="search-bar"><input type="text" id="log-search" placeholder="Search log streams..."></div>';

        if (streams.length === 0) {
          html +=
            '<div class="data-table-wrapper"><table class="data-table"><tbody>' +
            '<tr><td class="empty-state">No log streams in this group.</td></tr>' +
            "</tbody></table></div>";
        } else {
          html +=
            '<div class="data-table-wrapper"><table class="data-table">' +
            "<thead><tr><th>Stream Name</th><th>Last Event</th><th>Created</th></tr></thead><tbody id=\"streams-body\">";

          streams.forEach(function (s) {
            html +=
              '<tr class="clickable stream-row" data-stream="' +
              UI.esc(s.logStreamName) +
              '" data-name-lower="' +
              UI.esc(s.logStreamName.toLowerCase()) +
              '">' +
              '<td class="mono">' +
              UI.esc(s.logStreamName) +
              "</td>" +
              "<td>" +
              UI.formatDate(s.lastEventTimestamp || s.lastIngestionTime) +
              "</td>" +
              "<td>" +
              UI.formatDate(s.creationTime) +
              "</td>" +
              "</tr>";
          });
          html += "</tbody></table></div>";
        }

        container.innerHTML = html;

        // Search filter
        var searchInput = document.getElementById("log-search");
        if (searchInput) {
          searchInput.addEventListener("input", function () {
            var q = this.value.toLowerCase();
            container
              .querySelectorAll(".stream-row")
              .forEach(function (row) {
                var name = row.getAttribute("data-name-lower");
                row.style.display =
                  !q || name.indexOf(q) !== -1 ? "" : "none";
              });
          });
        }

        container
          .querySelectorAll("tr.clickable")
          .forEach(function (tr) {
            tr.addEventListener("click", function () {
              location.hash =
                "cloudwatch/" +
                encodeURIComponent(groupName) +
                "/" +
                encodeURIComponent(this.getAttribute("data-stream"));
            });
          });
      })
      .catch(function (err) {
        container.innerHTML =
          '<div class="loading-state text-danger">Error: ' +
          UI.esc(err.message) +
          "</div>";
      });
  }

  function showLogEvents(
    container,
    titleEl,
    breadcrumb,
    groupName,
    streamName
  ) {
    titleEl.textContent = streamName.split("/").pop();
    breadcrumb.innerHTML =
      '<a href="#cloudwatch">CloudWatch Logs</a> &rsaquo; ' +
      '<a href="#cloudwatch/' +
      encodeURIComponent(groupName) +
      '">' +
      UI.esc(groupName.split("/").pop()) +
      "</a> &rsaquo; " +
      UI.esc(streamName);
    container.innerHTML =
      '<div class="loading-state"><div class="loading-spinner"></div>Loading log events...</div>';

    UI.apiCall("logs", "GetLogEvents", {
      logGroupName: groupName,
      logStreamName: streamName,
      limit: 100,
    })
      .then(function (data) {
        var events = data.events || [];

        var html = "";
        html +=
          '<div class="toolbar"><button class="btn btn-secondary" onclick="location.hash=\'cloudwatch/' +
          encodeURIComponent(groupName) +
          "'\">" +
          "Back to Streams</button>" +
          '<div class="spacer"></div>' +
          '<span class="text-muted">' +
          events.length +
          " events</span></div>";

        if (events.length === 0) {
          html +=
            '<div class="data-table-wrapper"><table class="data-table"><tbody>' +
            '<tr><td class="empty-state">No log events found.</td></tr>' +
            "</tbody></table></div>";
        } else {
          html += '<div class="panel"><div class="panel-body" style="padding: 0">';
          events.forEach(function (e) {
            html +=
              '<div style="display:flex; gap:12px; padding:8px 16px; border-bottom:1px solid var(--border); font-size:12px;">' +
              '<span style="color:var(--text-tertiary); font-family:var(--font-mono); white-space:nowrap; min-width:80px">' +
              UI.formatTime(e.timestamp) +
              "</span>" +
              '<span style="font-family:var(--font-mono); white-space:pre-wrap; word-break:break-all; color:var(--text-secondary)">' +
              UI.esc(e.message || "") +
              "</span></div>";
          });
          html += "</div></div>";
        }

        container.innerHTML = html;
      })
      .catch(function (err) {
        container.innerHTML =
          '<div class="loading-state text-danger">Error: ' +
          UI.esc(err.message) +
          "</div>";
      });
  }
})();
