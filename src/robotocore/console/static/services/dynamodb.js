/* ============================================================
   DynamoDB Console Module
   ============================================================ */

(function () {
  "use strict";

  var UI = window.AppUI;

  window.DynamoDBConsole = {
    render: function (container, titleEl, breadcrumb, params) {
      if (params && params[0] === "create") {
        showCreateTable(container, titleEl, breadcrumb);
      } else if (params && params[0]) {
        var tableName = decodeURIComponent(params[0]);
        showTableDetail(container, titleEl, breadcrumb, tableName);
      } else {
        showTableList(container, titleEl, breadcrumb);
      }
    },
  };

  function showTableList(container, titleEl, breadcrumb) {
    titleEl.textContent = "DynamoDB Tables";
    breadcrumb.innerHTML = "DynamoDB";
    container.innerHTML =
      '<div class="loading-state"><div class="loading-spinner"></div>Loading tables...</div>';

    UI.apiCall("dynamodb", "ListTables", {})
      .then(function (data) {
        var tableNames = data.TableNames || [];

        var html = "";
        html +=
          '<div class="section-header"><h2>Tables (' +
          tableNames.length +
          ')</h2><div class="actions">';
        html +=
          '<button class="btn btn-primary" id="ddb-create-btn">Create Table</button>';
        html += "</div></div>";

        if (tableNames.length === 0) {
          html +=
            '<div class="data-table-wrapper"><table class="data-table"><tbody>' +
            '<tr><td class="empty-state">No tables found. Create one to get started.</td></tr>' +
            "</tbody></table></div>";
          container.innerHTML = html;
          document
            .getElementById("ddb-create-btn")
            .addEventListener("click", function () {
              location.hash = "dynamodb/create";
            });
          return;
        }

        // Fetch details for each table
        var promises = tableNames.map(function (name) {
          return UI.apiCall("dynamodb", "DescribeTable", {
            TableName: name,
          }).catch(function () {
            return { Table: { TableName: name } };
          });
        });

        Promise.all(promises).then(function (tables) {
          html +=
            '<div class="data-table-wrapper"><table class="data-table">' +
            "<thead><tr><th>Table Name</th><th>Status</th><th>Items</th><th>Key Schema</th><th>Actions</th></tr></thead><tbody>";

          tables.forEach(function (data) {
            var t = data.Table || {};
            var keys = (t.KeySchema || [])
              .map(function (k) {
                return k.AttributeName + " (" + k.KeyType + ")";
              })
              .join(", ");
            var status = t.TableStatus || "ACTIVE";
            var statusClass =
              status === "ACTIVE" ? "badge-success" : "badge-warning";

            html +=
              '<tr class="clickable" data-table="' +
              UI.esc(t.TableName) +
              '">' +
              "<td><strong>" +
              UI.esc(t.TableName) +
              "</strong></td>" +
              '<td><span class="badge ' +
              statusClass +
              '">' +
              UI.esc(status) +
              "</span></td>" +
              "<td>" +
              (t.ItemCount || 0) +
              "</td>" +
              '<td class="mono">' +
              UI.esc(keys) +
              "</td>" +
              '<td><button class="btn btn-danger btn-sm ddb-delete-btn" data-table="' +
              UI.esc(t.TableName) +
              '">Delete</button></td>' +
              "</tr>";
          });

          html += "</tbody></table></div>";
          container.innerHTML = html;

          document
            .getElementById("ddb-create-btn")
            .addEventListener("click", function () {
              location.hash = "dynamodb/create";
            });

          container
            .querySelectorAll("tr.clickable")
            .forEach(function (tr) {
              tr.addEventListener("click", function (e) {
                if (e.target.tagName === "BUTTON") return;
                location.hash =
                  "dynamodb/" +
                  encodeURIComponent(this.getAttribute("data-table"));
              });
            });

          container
            .querySelectorAll(".ddb-delete-btn")
            .forEach(function (btn) {
              btn.addEventListener("click", function (e) {
                e.stopPropagation();
                var name = this.getAttribute("data-table");
                if (confirm("Delete table '" + name + "'?")) {
                  UI.apiCall("dynamodb", "DeleteTable", {
                    TableName: name,
                  })
                    .then(function () {
                      UI.toast("Table deleted: " + name, "success");
                      showTableList(container, titleEl, breadcrumb);
                    })
                    .catch(function (err) {
                      UI.toast("Error: " + err.message, "error");
                    });
                }
              });
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

  function showCreateTable(container, titleEl, breadcrumb) {
    titleEl.textContent = "Create Table";
    breadcrumb.innerHTML =
      '<a href="#dynamodb">DynamoDB</a> &rsaquo; Create Table';

    var html = '<div class="panel"><div class="panel-body">';
    html += '<div class="form-group">';
    html += "<label>Table Name</label>";
    html +=
      '<input type="text" class="form-input" id="ct-name" placeholder="my-table">';
    html += "</div>";
    html += '<div class="form-row">';
    html += '<div class="form-group">';
    html += "<label>Partition Key Name</label>";
    html +=
      '<input type="text" class="form-input" id="ct-pk" placeholder="id">';
    html += "</div>";
    html += '<div class="form-group">';
    html += "<label>Partition Key Type</label>";
    html +=
      '<select class="form-input" id="ct-pk-type"><option value="S">String</option><option value="N">Number</option></select>';
    html += "</div>";
    html += "</div>";
    html += '<div class="form-row">';
    html += '<div class="form-group">';
    html += "<label>Sort Key Name (optional)</label>";
    html +=
      '<input type="text" class="form-input" id="ct-sk" placeholder="">';
    html += "</div>";
    html += '<div class="form-group">';
    html += "<label>Sort Key Type</label>";
    html +=
      '<select class="form-input" id="ct-sk-type"><option value="S">String</option><option value="N">Number</option></select>';
    html += "</div>";
    html += "</div>";
    html += '<div class="form-row mt-16">';
    html +=
      '<button class="btn btn-primary" id="ct-submit">Create Table</button>';
    html +=
      '<button class="btn btn-secondary" onclick="location.hash=\'dynamodb\'">Cancel</button>';
    html += "</div>";
    html += "</div></div>";

    container.innerHTML = html;

    document
      .getElementById("ct-submit")
      .addEventListener("click", function () {
        var name = document.getElementById("ct-name").value.trim();
        var pk = document.getElementById("ct-pk").value.trim();
        var pkType = document.getElementById("ct-pk-type").value;
        var sk = document.getElementById("ct-sk").value.trim();
        var skType = document.getElementById("ct-sk-type").value;

        if (!name || !pk) {
          UI.toast("Table name and partition key are required", "warning");
          return;
        }

        var keySchema = [{ AttributeName: pk, KeyType: "HASH" }];
        var attrDefs = [{ AttributeName: pk, AttributeType: pkType }];

        if (sk) {
          keySchema.push({ AttributeName: sk, KeyType: "RANGE" });
          attrDefs.push({ AttributeName: sk, AttributeType: skType });
        }

        UI.apiCall("dynamodb", "CreateTable", {
          TableName: name,
          KeySchema: keySchema,
          AttributeDefinitions: attrDefs,
          BillingMode: "PAY_PER_REQUEST",
        })
          .then(function () {
            UI.toast("Table created: " + name, "success");
            location.hash = "dynamodb";
          })
          .catch(function (err) {
            UI.toast("Error: " + err.message, "error");
          });
      });
  }

  function showTableDetail(container, titleEl, breadcrumb, tableName) {
    titleEl.textContent = tableName;
    breadcrumb.innerHTML =
      '<a href="#dynamodb">DynamoDB</a> &rsaquo; ' + UI.esc(tableName);
    container.innerHTML =
      '<div class="loading-state"><div class="loading-spinner"></div>Loading table...</div>';

    Promise.all([
      UI.apiCall("dynamodb", "DescribeTable", { TableName: tableName }),
      UI.apiCall("dynamodb", "Scan", { TableName: tableName, Limit: 50 }),
    ])
      .then(function (results) {
        var tableInfo = results[0].Table || {};
        var scanResult = results[1];
        var items = scanResult.Items || [];

        var html = "";

        // Table info panel
        html +=
          '<div class="toolbar"><button class="btn btn-secondary" onclick="location.hash=\'dynamodb\'">Back to Tables</button>' +
          '<div class="spacer"></div>' +
          '<button class="btn btn-primary" id="ddb-put-item">Put Item</button></div>';

        html += '<div class="panel"><div class="panel-header"><h3>Table Details</h3></div>';
        html += '<div class="panel-body"><dl class="key-value-list">';
        html +=
          "<dt>Status</dt><dd>" +
          UI.esc(tableInfo.TableStatus || "--") +
          "</dd>";
        html +=
          "<dt>Item Count</dt><dd>" + (tableInfo.ItemCount || 0) + "</dd>";
        html +=
          "<dt>Table Size</dt><dd>" +
          UI.formatBytes(tableInfo.TableSizeBytes || 0) +
          "</dd>";
        var keys = (tableInfo.KeySchema || [])
          .map(function (k) {
            return k.AttributeName + " (" + k.KeyType + ")";
          })
          .join(", ");
        html += "<dt>Key Schema</dt><dd>" + UI.esc(keys) + "</dd>";
        html += "</dl></div></div>";

        // Items table
        html +=
          '<div class="section-header"><h2>Items (' +
          items.length +
          ")</h2></div>";

        if (items.length === 0) {
          html +=
            '<div class="data-table-wrapper"><table class="data-table"><tbody>' +
            '<tr><td class="empty-state">No items in this table.</td></tr>' +
            "</tbody></table></div>";
        } else {
          // Get all attribute names
          var allKeys = {};
          items.forEach(function (item) {
            for (var k in item) allKeys[k] = true;
          });
          var columns = Object.keys(allKeys);

          html +=
            '<div class="data-table-wrapper"><table class="data-table"><thead><tr>';
          columns.forEach(function (col) {
            html += "<th>" + UI.esc(col) + "</th>";
          });
          html += "<th>Actions</th></tr></thead><tbody>";

          items.forEach(function (item, idx) {
            html += "<tr>";
            columns.forEach(function (col) {
              var val = item[col];
              var display = val ? formatDDBValue(val) : "";
              html += '<td class="mono">' + UI.esc(display) + "</td>";
            });
            html +=
              '<td><button class="btn btn-danger btn-sm ddb-delitem" data-idx="' +
              idx +
              '">Delete</button></td>';
            html += "</tr>";
          });
          html += "</tbody></table></div>";
        }

        container.innerHTML = html;

        document
          .getElementById("ddb-put-item")
          .addEventListener("click", function () {
            showPutItemModal(tableName, container, titleEl, breadcrumb);
          });

        container
          .querySelectorAll(".ddb-delitem")
          .forEach(function (btn) {
            btn.addEventListener("click", function () {
              var idx = parseInt(this.getAttribute("data-idx"), 10);
              var item = items[idx];
              if (!item) return;
              // Build key from first key schema attribute
              var keySchema = tableInfo.KeySchema || [];
              var key = {};
              keySchema.forEach(function (ks) {
                if (item[ks.AttributeName]) {
                  key[ks.AttributeName] = item[ks.AttributeName];
                }
              });
              UI.apiCall("dynamodb", "DeleteItem", {
                TableName: tableName,
                Key: key,
              })
                .then(function () {
                  UI.toast("Item deleted", "success");
                  showTableDetail(container, titleEl, breadcrumb, tableName);
                })
                .catch(function (err) {
                  UI.toast("Error: " + err.message, "error");
                });
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

  function showPutItemModal(tableName, container, titleEl, breadcrumb) {
    var body =
      '<div class="form-group">' +
      "<label>Item JSON</label>" +
      '<textarea class="form-input" id="put-item-json" rows="10" placeholder=\'{"id": {"S": "123"}, "name": {"S": "test"}}\'></textarea>' +
      "</div>";

    var footer =
      '<button class="btn btn-secondary" onclick="window.AppUI.closeModal()">Cancel</button>' +
      '<button class="btn btn-primary" id="put-item-submit">Put Item</button>';

    UI.showModal("Put Item into " + tableName, body, footer);

    document
      .getElementById("put-item-submit")
      .addEventListener("click", function () {
        var jsonStr = document.getElementById("put-item-json").value.trim();
        if (!jsonStr) {
          UI.toast("Please enter item JSON", "warning");
          return;
        }
        try {
          var item = JSON.parse(jsonStr);
          UI.apiCall("dynamodb", "PutItem", {
            TableName: tableName,
            Item: item,
          })
            .then(function () {
              UI.closeModal();
              UI.toast("Item added", "success");
              showTableDetail(container, titleEl, breadcrumb, tableName);
            })
            .catch(function (err) {
              UI.toast("Error: " + err.message, "error");
            });
        } catch (e) {
          UI.toast("Invalid JSON: " + e.message, "error");
        }
      });
  }

  function formatDDBValue(val) {
    if (!val || typeof val !== "object") return String(val);
    if (val.S !== undefined) return val.S;
    if (val.N !== undefined) return val.N;
    if (val.BOOL !== undefined) return String(val.BOOL);
    if (val.NULL !== undefined) return "null";
    if (val.L !== undefined) return JSON.stringify(val.L);
    if (val.M !== undefined) return JSON.stringify(val.M);
    return JSON.stringify(val);
  }
})();
