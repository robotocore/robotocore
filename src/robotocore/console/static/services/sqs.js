/* ============================================================
   SQS Console Module
   ============================================================ */

(function () {
  "use strict";

  var UI = window.AppUI;

  window.SQSConsole = {
    render: function (container, titleEl, breadcrumb, params) {
      if (params && params[0] === "create") {
        showCreateQueue(container, titleEl, breadcrumb);
      } else if (params && params[0]) {
        var queueUrl = decodeURIComponent(params[0]);
        showQueueDetail(container, titleEl, breadcrumb, queueUrl);
      } else {
        showQueueList(container, titleEl, breadcrumb);
      }
    },
  };

  function showQueueList(container, titleEl, breadcrumb) {
    titleEl.textContent = "SQS Queues";
    breadcrumb.innerHTML = "SQS";
    container.innerHTML =
      '<div class="loading-state"><div class="loading-spinner"></div>Loading queues...</div>';

    UI.apiCall("sqs", "ListQueues", {})
      .then(function (data) {
        var queueUrls = parseQueueUrlsXml(data);

        var html = "";
        html +=
          '<div class="section-header"><h2>Queues (' +
          queueUrls.length +
          ')</h2><div class="actions">';
        html +=
          '<button class="btn btn-primary" id="sqs-create-btn">Create Queue</button>';
        html += "</div></div>";

        if (queueUrls.length === 0) {
          html +=
            '<div class="data-table-wrapper"><table class="data-table"><tbody>' +
            '<tr><td class="empty-state">No queues found. Create one to get started.</td></tr>' +
            "</tbody></table></div>";
          container.innerHTML = html;
          document
            .getElementById("sqs-create-btn")
            .addEventListener("click", function () {
              location.hash = "sqs/create";
            });
          return;
        }

        // Fetch attributes for each queue
        var promises = queueUrls.map(function (url) {
          return UI.apiCall("sqs", "GetQueueAttributes", {
            QueueUrl: url,
            "AttributeName.1": "All",
          })
            .then(function (attrData) {
              return { url: url, attrs: parseAttributesXml(attrData) };
            })
            .catch(function () {
              return { url: url, attrs: {} };
            });
        });

        Promise.all(promises).then(function (queues) {
          html +=
            '<div class="data-table-wrapper"><table class="data-table">' +
            "<thead><tr><th>Queue Name</th><th>Messages</th><th>In Flight</th><th>Delayed</th><th>Actions</th></tr></thead><tbody>";

          queues.forEach(function (q) {
            var name = q.url.split("/").pop();
            var msgs =
              q.attrs.ApproximateNumberOfMessages || "0";
            var inflight =
              q.attrs.ApproximateNumberOfMessagesNotVisible || "0";
            var delayed =
              q.attrs.ApproximateNumberOfMessagesDelayed || "0";

            html +=
              '<tr class="clickable" data-url="' +
              UI.esc(q.url) +
              '">' +
              "<td><strong>" +
              UI.esc(name) +
              "</strong></td>" +
              "<td>" +
              msgs +
              "</td>" +
              "<td>" +
              inflight +
              "</td>" +
              "<td>" +
              delayed +
              "</td>" +
              '<td><button class="btn btn-danger btn-sm sqs-delete-btn" data-url="' +
              UI.esc(q.url) +
              '">Delete</button></td>' +
              "</tr>";
          });
          html += "</tbody></table></div>";
          container.innerHTML = html;

          document
            .getElementById("sqs-create-btn")
            .addEventListener("click", function () {
              location.hash = "sqs/create";
            });

          container
            .querySelectorAll("tr.clickable")
            .forEach(function (tr) {
              tr.addEventListener("click", function (e) {
                if (e.target.tagName === "BUTTON") return;
                location.hash =
                  "sqs/" +
                  encodeURIComponent(this.getAttribute("data-url"));
              });
            });

          container
            .querySelectorAll(".sqs-delete-btn")
            .forEach(function (btn) {
              btn.addEventListener("click", function (e) {
                e.stopPropagation();
                var url = this.getAttribute("data-url");
                var name = url.split("/").pop();
                if (confirm("Delete queue '" + name + "'?")) {
                  UI.apiCall("sqs", "DeleteQueue", { QueueUrl: url })
                    .then(function () {
                      UI.toast("Queue deleted: " + name, "success");
                      showQueueList(container, titleEl, breadcrumb);
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

  function showCreateQueue(container, titleEl, breadcrumb) {
    titleEl.textContent = "Create Queue";
    breadcrumb.innerHTML =
      '<a href="#sqs">SQS</a> &rsaquo; Create Queue';

    var html = '<div class="panel"><div class="panel-body">';
    html += '<div class="form-group">';
    html += "<label>Queue Name</label>";
    html +=
      '<input type="text" class="form-input" id="cq-name" placeholder="my-queue">';
    html += "</div>";
    html += '<div class="form-row mt-16">';
    html +=
      '<button class="btn btn-primary" id="cq-submit">Create Queue</button>';
    html +=
      '<button class="btn btn-secondary" onclick="location.hash=\'sqs\'">Cancel</button>';
    html += "</div>";
    html += "</div></div>";

    container.innerHTML = html;

    document
      .getElementById("cq-submit")
      .addEventListener("click", function () {
        var name = document.getElementById("cq-name").value.trim();
        if (!name) {
          UI.toast("Please enter a queue name", "warning");
          return;
        }
        UI.apiCall("sqs", "CreateQueue", { QueueName: name })
          .then(function () {
            UI.toast("Queue created: " + name, "success");
            location.hash = "sqs";
          })
          .catch(function (err) {
            UI.toast("Error: " + err.message, "error");
          });
      });
  }

  function showQueueDetail(container, titleEl, breadcrumb, queueUrl) {
    var queueName = queueUrl.split("/").pop();
    titleEl.textContent = queueName;
    breadcrumb.innerHTML =
      '<a href="#sqs">SQS</a> &rsaquo; ' + UI.esc(queueName);
    container.innerHTML =
      '<div class="loading-state"><div class="loading-spinner"></div>Loading queue...</div>';

    UI.apiCall("sqs", "GetQueueAttributes", {
      QueueUrl: queueUrl,
      "AttributeName.1": "All",
    })
      .then(function (data) {
        var attrs = parseAttributesXml(data);

        var html = "";
        html +=
          '<div class="toolbar"><button class="btn btn-secondary" onclick="location.hash=\'sqs\'">Back to Queues</button>' +
          '<div class="spacer"></div>' +
          '<button class="btn btn-primary" id="sqs-send-msg">Send Message</button>' +
          '<button class="btn btn-secondary" id="sqs-receive-msg">Receive Messages</button></div>';

        // Queue attributes
        html +=
          '<div class="panel"><div class="panel-header"><h3>Queue Attributes</h3></div>';
        html += '<div class="panel-body"><dl class="key-value-list">';
        html +=
          "<dt>URL</dt><dd>" + UI.esc(queueUrl) + "</dd>";
        html +=
          "<dt>Messages Available</dt><dd>" +
          (attrs.ApproximateNumberOfMessages || "0") +
          "</dd>";
        html +=
          "<dt>Messages In Flight</dt><dd>" +
          (attrs.ApproximateNumberOfMessagesNotVisible || "0") +
          "</dd>";
        html +=
          "<dt>Visibility Timeout</dt><dd>" +
          (attrs.VisibilityTimeout || "30") +
          "s</dd>";
        html +=
          "<dt>Created</dt><dd>" +
          UI.formatDate(
            attrs.CreatedTimestamp
              ? parseInt(attrs.CreatedTimestamp, 10) * 1000
              : null
          ) +
          "</dd>";
        html += "</dl></div></div>";

        // Messages area
        html +=
          '<div class="section-header"><h2>Messages</h2></div>';
        html += '<div id="sqs-messages-area">';
        html +=
          '<div class="text-muted text-center" style="padding:20px">Click "Receive Messages" to poll the queue</div>';
        html += "</div>";

        container.innerHTML = html;

        document
          .getElementById("sqs-send-msg")
          .addEventListener("click", function () {
            showSendMessageModal(
              queueUrl,
              container,
              titleEl,
              breadcrumb
            );
          });

        document
          .getElementById("sqs-receive-msg")
          .addEventListener("click", function () {
            receiveMessages(queueUrl);
          });
      })
      .catch(function (err) {
        container.innerHTML =
          '<div class="loading-state text-danger">Error: ' +
          UI.esc(err.message) +
          "</div>";
      });
  }

  function showSendMessageModal(queueUrl, container, titleEl, breadcrumb) {
    var body =
      '<div class="form-group">' +
      "<label>Message Body</label>" +
      '<textarea class="form-input" id="sqs-msg-body" rows="6" placeholder="Enter message body..."></textarea>' +
      "</div>";

    var footer =
      '<button class="btn btn-secondary" onclick="window.AppUI.closeModal()">Cancel</button>' +
      '<button class="btn btn-primary" id="sqs-send-submit">Send</button>';

    UI.showModal("Send Message", body, footer);

    document
      .getElementById("sqs-send-submit")
      .addEventListener("click", function () {
        var msgBody = document
          .getElementById("sqs-msg-body")
          .value.trim();
        if (!msgBody) {
          UI.toast("Please enter a message body", "warning");
          return;
        }
        UI.apiCall("sqs", "SendMessage", {
          QueueUrl: queueUrl,
          MessageBody: msgBody,
        })
          .then(function () {
            UI.closeModal();
            UI.toast("Message sent", "success");
          })
          .catch(function (err) {
            UI.toast("Error: " + err.message, "error");
          });
      });
  }

  function receiveMessages(queueUrl) {
    var area = document.getElementById("sqs-messages-area");
    if (!area) return;
    area.innerHTML =
      '<div class="loading-state"><div class="loading-spinner"></div>Receiving messages...</div>';

    UI.apiCall("sqs", "ReceiveMessage", {
      QueueUrl: queueUrl,
      MaxNumberOfMessages: "10",
      WaitTimeSeconds: "1",
    })
      .then(function (data) {
        var messages = parseReceiveMessageXml(data);

        if (messages.length === 0) {
          area.innerHTML =
            '<div class="text-muted text-center" style="padding:20px">No messages available</div>';
          return;
        }

        var html =
          '<div class="data-table-wrapper"><table class="data-table">' +
          "<thead><tr><th>Message ID</th><th>Body</th><th>Actions</th></tr></thead><tbody>";
        messages.forEach(function (msg) {
          html +=
            "<tr>" +
            '<td class="mono">' +
            UI.esc(msg.messageId).substring(0, 16) +
            "...</td>" +
            "<td>" +
            UI.esc(
              msg.body.length > 100
                ? msg.body.substring(0, 100) + "..."
                : msg.body
            ) +
            "</td>" +
            '<td><button class="btn btn-danger btn-sm sqs-del-msg" data-receipt="' +
            UI.esc(msg.receiptHandle) +
            '">Delete</button></td>' +
            "</tr>";
        });
        html += "</tbody></table></div>";
        area.innerHTML = html;

        area.querySelectorAll(".sqs-del-msg").forEach(function (btn) {
          btn.addEventListener("click", function () {
            var receipt = this.getAttribute("data-receipt");
            UI.apiCall("sqs", "DeleteMessage", {
              QueueUrl: queueUrl,
              ReceiptHandle: receipt,
            })
              .then(function () {
                UI.toast("Message deleted", "success");
                receiveMessages(queueUrl);
              })
              .catch(function (err) {
                UI.toast("Error: " + err.message, "error");
              });
          });
        });
      })
      .catch(function (err) {
        area.innerHTML =
          '<div class="text-danger" style="padding:20px">Error: ' +
          UI.esc(err.message) +
          "</div>";
      });
  }

  // ---------------------------------------------------------------------------
  // XML Parsers
  // ---------------------------------------------------------------------------
  function parseQueueUrlsXml(data) {
    if (typeof data !== "string") return [];
    var urls = [];
    var parser = new DOMParser();
    var doc = parser.parseFromString(data, "text/xml");
    doc.querySelectorAll("QueueUrl").forEach(function (el) {
      urls.push(el.textContent);
    });
    return urls;
  }

  function parseAttributesXml(data) {
    if (typeof data !== "string") return {};
    var attrs = {};
    var parser = new DOMParser();
    var doc = parser.parseFromString(data, "text/xml");
    doc.querySelectorAll("Attribute").forEach(function (el) {
      var name = el.querySelector("Name");
      var value = el.querySelector("Value");
      if (name && value) {
        attrs[name.textContent] = value.textContent;
      }
    });
    return attrs;
  }

  function parseReceiveMessageXml(data) {
    if (typeof data !== "string") return [];
    var messages = [];
    var parser = new DOMParser();
    var doc = parser.parseFromString(data, "text/xml");
    doc.querySelectorAll("Message").forEach(function (el) {
      var msgId = el.querySelector("MessageId");
      var body = el.querySelector("Body");
      var receipt = el.querySelector("ReceiptHandle");
      messages.push({
        messageId: msgId ? msgId.textContent : "",
        body: body ? body.textContent : "",
        receiptHandle: receipt ? receipt.textContent : "",
      });
    });
    return messages;
  }
})();
