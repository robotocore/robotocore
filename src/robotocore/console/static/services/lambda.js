/* ============================================================
   Lambda Console Module
   ============================================================ */

(function () {
  "use strict";

  var UI = window.AppUI;

  window.LambdaConsole = {
    render: function (container, titleEl, breadcrumb, params) {
      if (params && params[0]) {
        var funcName = decodeURIComponent(params[0]);
        showFunctionDetail(container, titleEl, breadcrumb, funcName);
      } else {
        showFunctionList(container, titleEl, breadcrumb);
      }
    },
  };

  function showFunctionList(container, titleEl, breadcrumb) {
    titleEl.textContent = "Lambda Functions";
    breadcrumb.innerHTML = "Lambda";
    container.innerHTML =
      '<div class="loading-state"><div class="loading-spinner"></div>Loading functions...</div>';

    UI.apiCall("lambda", "ListFunctions", {})
      .then(function (data) {
        var functions = data.Functions || [];

        var html = "";
        html +=
          '<div class="section-header"><h2>Functions (' +
          functions.length +
          ")</h2></div>";

        if (functions.length === 0) {
          html +=
            '<div class="data-table-wrapper"><table class="data-table"><tbody>' +
            '<tr><td class="empty-state">No Lambda functions found. Create one using the AWS CLI or SDK.</td></tr>' +
            "</tbody></table></div>";
        } else {
          html +=
            '<div class="data-table-wrapper"><table class="data-table">' +
            "<thead><tr><th>Function Name</th><th>Runtime</th><th>Memory</th><th>Timeout</th><th>Last Modified</th></tr></thead><tbody>";

          functions.forEach(function (fn) {
            html +=
              '<tr class="clickable" data-func="' +
              UI.esc(fn.FunctionName) +
              '">' +
              "<td><strong>" +
              UI.esc(fn.FunctionName) +
              "</strong></td>" +
              '<td><span class="badge badge-info">' +
              UI.esc(fn.Runtime || "--") +
              "</span></td>" +
              "<td>" +
              (fn.MemorySize || 128) +
              " MB</td>" +
              "<td>" +
              (fn.Timeout || 3) +
              "s</td>" +
              "<td>" +
              UI.formatDate(fn.LastModified) +
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
                "lambda/" +
                encodeURIComponent(this.getAttribute("data-func"));
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

  function showFunctionDetail(container, titleEl, breadcrumb, funcName) {
    titleEl.textContent = funcName;
    breadcrumb.innerHTML =
      '<a href="#lambda">Lambda</a> &rsaquo; ' + UI.esc(funcName);
    container.innerHTML =
      '<div class="loading-state"><div class="loading-spinner"></div>Loading function...</div>';

    UI.apiCall("lambda", "GetFunction", { FunctionName: funcName })
      .then(function (data) {
        var config = data.Configuration || {};

        var html = "";
        html +=
          '<div class="toolbar"><button class="btn btn-secondary" onclick="location.hash=\'lambda\'">Back to Functions</button>' +
          '<div class="spacer"></div>' +
          '<button class="btn btn-primary" id="lambda-invoke">Test Invoke</button></div>';

        // Config panel
        html +=
          '<div class="panel"><div class="panel-header"><h3>Configuration</h3></div>';
        html += '<div class="panel-body"><dl class="key-value-list">';
        html +=
          "<dt>Function ARN</dt><dd>" +
          UI.esc(config.FunctionArn || "--") +
          "</dd>";
        html +=
          "<dt>Runtime</dt><dd>" +
          UI.esc(config.Runtime || "--") +
          "</dd>";
        html +=
          "<dt>Handler</dt><dd>" +
          UI.esc(config.Handler || "--") +
          "</dd>";
        html +=
          "<dt>Memory</dt><dd>" +
          (config.MemorySize || 128) +
          " MB</dd>";
        html +=
          "<dt>Timeout</dt><dd>" + (config.Timeout || 3) + "s</dd>";
        html +=
          "<dt>Code Size</dt><dd>" +
          UI.formatBytes(config.CodeSize || 0) +
          "</dd>";
        html +=
          "<dt>Last Modified</dt><dd>" +
          UI.formatDate(config.LastModified) +
          "</dd>";
        html +=
          "<dt>State</dt><dd>" +
          UI.esc(config.State || "Active") +
          "</dd>";
        html += "</dl></div></div>";

        // Invocation result area
        html +=
          '<div class="section-header"><h2>Test Result</h2></div>';
        html += '<div id="lambda-result">';
        html +=
          '<div class="text-muted text-center" style="padding:20px">Click "Test Invoke" to execute the function</div>';
        html += "</div>";

        container.innerHTML = html;

        document
          .getElementById("lambda-invoke")
          .addEventListener("click", function () {
            showInvokeModal(funcName);
          });
      })
      .catch(function (err) {
        container.innerHTML =
          '<div class="loading-state text-danger">Error: ' +
          UI.esc(err.message) +
          "</div>";
      });
  }

  function showInvokeModal(funcName) {
    var body =
      '<div class="form-group">' +
      "<label>Event JSON</label>" +
      '<textarea class="form-input" id="invoke-payload" rows="8" placeholder=\'{"key": "value"}\'>{}</textarea>' +
      "</div>";

    var footer =
      '<button class="btn btn-secondary" onclick="window.AppUI.closeModal()">Cancel</button>' +
      '<button class="btn btn-primary" id="invoke-submit">Invoke</button>';

    UI.showModal("Invoke " + funcName, body, footer);

    document
      .getElementById("invoke-submit")
      .addEventListener("click", function () {
        var payload = document
          .getElementById("invoke-payload")
          .value.trim();
        try {
          JSON.parse(payload);
        } catch (e) {
          UI.toast("Invalid JSON: " + e.message, "error");
          return;
        }

        UI.closeModal();

        var resultArea = document.getElementById("lambda-result");
        if (resultArea) {
          resultArea.innerHTML =
            '<div class="loading-state"><div class="loading-spinner"></div>Invoking function...</div>';
        }

        UI.apiCall("lambda", "Invoke", {
          FunctionName: funcName,
          Payload: payload,
        })
          .then(function (data) {
            if (resultArea) {
              var html =
                '<div class="panel"><div class="panel-header"><h3>Response</h3></div>';
              html += '<div class="panel-body">';
              html += "<pre>" + UI.esc(JSON.stringify(data, null, 2)) + "</pre>";
              html += "</div></div>";
              resultArea.innerHTML = html;
            }
            UI.toast("Function invoked successfully", "success");
          })
          .catch(function (err) {
            if (resultArea) {
              resultArea.innerHTML =
                '<div class="panel"><div class="panel-header"><h3>Error</h3></div>' +
                '<div class="panel-body"><pre class="text-danger">' +
                UI.esc(err.message) +
                "</pre></div></div>";
            }
            UI.toast("Invocation failed", "error");
          });
      });
  }
})();
