/* ============================================================
   S3 Console Module
   ============================================================ */

(function () {
  "use strict";

  var UI = window.AppUI;
  var currentBucket = null;

  window.S3Console = {
    render: function (container, titleEl, breadcrumb, params) {
      if (params && params[0] === "create") {
        showCreateBucket(container, titleEl, breadcrumb);
      } else if (params && params[0]) {
        currentBucket = decodeURIComponent(params[0]);
        showBucketContents(container, titleEl, breadcrumb, currentBucket);
      } else {
        showBucketList(container, titleEl, breadcrumb);
      }
    },
  };

  function showBucketList(container, titleEl, breadcrumb) {
    titleEl.textContent = "S3 Buckets";
    breadcrumb.innerHTML = "S3";
    container.innerHTML =
      '<div class="loading-state"><div class="loading-spinner"></div>Loading buckets...</div>';

    UI.apiCall("s3", "ListBuckets", {})
      .then(function (data) {
        // Parse XML response
        var buckets = parseListBucketsXml(data);

        var html = "";
        html +=
          '<div class="section-header"><h2>Buckets (' +
          buckets.length +
          ')</h2><div class="actions">';
        html +=
          '<button class="btn btn-primary" id="s3-create-btn">Create Bucket</button>';
        html += "</div></div>";

        if (buckets.length === 0) {
          html +=
            '<div class="data-table-wrapper"><table class="data-table"><tbody>' +
            '<tr><td class="empty-state">No buckets found. Create one to get started.</td></tr>' +
            "</tbody></table></div>";
        } else {
          html +=
            '<div class="data-table-wrapper"><table class="data-table">' +
            "<thead><tr><th>Bucket Name</th><th>Creation Date</th><th>Actions</th></tr></thead><tbody>";
          buckets.forEach(function (b) {
            html +=
              '<tr class="clickable" data-bucket="' +
              UI.esc(b.name) +
              '">' +
              "<td><strong>" +
              UI.esc(b.name) +
              "</strong></td>" +
              "<td>" +
              UI.formatDate(b.creationDate) +
              "</td>" +
              '<td><button class="btn btn-danger btn-sm s3-delete-btn" data-bucket="' +
              UI.esc(b.name) +
              '">Delete</button></td>' +
              "</tr>";
          });
          html += "</tbody></table></div>";
        }

        container.innerHTML = html;

        // Event handlers
        document
          .getElementById("s3-create-btn")
          .addEventListener("click", function () {
            location.hash = "s3/create";
          });

        container.querySelectorAll("tr.clickable").forEach(function (tr) {
          tr.addEventListener("click", function (e) {
            if (e.target.tagName === "BUTTON") return;
            var bucket = this.getAttribute("data-bucket");
            location.hash = "s3/" + encodeURIComponent(bucket);
          });
        });

        container.querySelectorAll(".s3-delete-btn").forEach(function (btn) {
          btn.addEventListener("click", function (e) {
            e.stopPropagation();
            var bucket = this.getAttribute("data-bucket");
            if (confirm("Delete bucket '" + bucket + "'?")) {
              UI.apiCall("s3", "DeleteBucket", { Bucket: bucket })
                .then(function () {
                  UI.toast("Bucket deleted: " + bucket, "success");
                  showBucketList(container, titleEl, breadcrumb);
                })
                .catch(function (err) {
                  UI.toast("Error: " + err.message, "error");
                });
            }
          });
        });
      })
      .catch(function (err) {
        container.innerHTML =
          '<div class="loading-state text-danger">Error loading buckets: ' +
          UI.esc(err.message) +
          "</div>";
      });
  }

  function showCreateBucket(container, titleEl, breadcrumb) {
    titleEl.textContent = "Create Bucket";
    breadcrumb.innerHTML =
      '<a href="#s3">S3</a> &rsaquo; Create Bucket';

    var html = "";
    html += '<div class="panel"><div class="panel-body">';
    html += '<div class="form-group">';
    html += "<label>Bucket Name</label>";
    html +=
      '<input type="text" class="form-input" id="create-bucket-name" placeholder="my-bucket-name">';
    html += "</div>";
    html += '<div class="form-row">';
    html +=
      '<button class="btn btn-primary" id="create-bucket-submit">Create Bucket</button>';
    html +=
      '<button class="btn btn-secondary" id="create-bucket-cancel">Cancel</button>';
    html += "</div>";
    html += "</div></div>";

    container.innerHTML = html;

    document
      .getElementById("create-bucket-submit")
      .addEventListener("click", function () {
        var name = document.getElementById("create-bucket-name").value.trim();
        if (!name) {
          UI.toast("Please enter a bucket name", "warning");
          return;
        }
        UI.apiCall("s3", "CreateBucket", { Bucket: name })
          .then(function () {
            UI.toast("Bucket created: " + name, "success");
            location.hash = "s3";
          })
          .catch(function (err) {
            UI.toast("Error: " + err.message, "error");
          });
      });

    document
      .getElementById("create-bucket-cancel")
      .addEventListener("click", function () {
        location.hash = "s3";
      });
  }

  function showBucketContents(container, titleEl, breadcrumb, bucket) {
    titleEl.textContent = bucket;
    breadcrumb.innerHTML =
      '<a href="#s3">S3</a> &rsaquo; ' + UI.esc(bucket);
    container.innerHTML =
      '<div class="loading-state"><div class="loading-spinner"></div>Loading objects...</div>';

    UI.apiCall("s3", "ListObjects", { Bucket: bucket })
      .then(function (data) {
        var objects = parseListObjectsXml(data);

        var html = "";
        html +=
          '<div class="section-header"><h2>Objects (' +
          objects.length +
          ')</h2><div class="actions">';
        html +=
          '<button class="btn btn-primary" id="s3-upload-btn">Upload Object</button>';
        html +=
          '<button class="btn btn-secondary" onclick="location.hash=\'s3\'">Back to Buckets</button>';
        html += "</div></div>";

        if (objects.length === 0) {
          html +=
            '<div class="data-table-wrapper"><table class="data-table"><tbody>' +
            '<tr><td class="empty-state">No objects in this bucket.</td></tr>' +
            "</tbody></table></div>";
        } else {
          html +=
            '<div class="data-table-wrapper"><table class="data-table">' +
            "<thead><tr><th>Key</th><th>Size</th><th>Last Modified</th><th>Actions</th></tr></thead><tbody>";
          objects.forEach(function (obj) {
            html +=
              "<tr>" +
              '<td class="mono">' +
              UI.esc(obj.key) +
              "</td>" +
              "<td>" +
              UI.formatBytes(obj.size) +
              "</td>" +
              "<td>" +
              UI.formatDate(obj.lastModified) +
              "</td>" +
              '<td><button class="btn btn-ghost btn-sm s3-download-btn" data-key="' +
              UI.esc(obj.key) +
              '">Download</button>' +
              '<button class="btn btn-danger btn-sm s3-delobj-btn" data-key="' +
              UI.esc(obj.key) +
              '">Delete</button></td>' +
              "</tr>";
          });
          html += "</tbody></table></div>";
        }

        container.innerHTML = html;

        document
          .getElementById("s3-upload-btn")
          .addEventListener("click", function () {
            showUploadModal(bucket, container, titleEl, breadcrumb);
          });

        container.querySelectorAll(".s3-download-btn").forEach(function (btn) {
          btn.addEventListener("click", function () {
            var key = this.getAttribute("data-key");
            window.open(
              "http://localhost:4566/" + bucket + "/" + key,
              "_blank"
            );
          });
        });

        container.querySelectorAll(".s3-delobj-btn").forEach(function (btn) {
          btn.addEventListener("click", function () {
            var key = this.getAttribute("data-key");
            if (confirm("Delete object '" + key + "'?")) {
              UI.apiCall("s3", "DeleteObject", { Bucket: bucket, Key: key })
                .then(function () {
                  UI.toast("Deleted: " + key, "success");
                  showBucketContents(container, titleEl, breadcrumb, bucket);
                })
                .catch(function (err) {
                  UI.toast("Error: " + err.message, "error");
                });
            }
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

  function showUploadModal(bucket, container, titleEl, breadcrumb) {
    var body =
      '<div class="form-group">' +
      "<label>Object Key</label>" +
      '<input type="text" class="form-input" id="upload-key" placeholder="path/to/file.txt">' +
      "</div>" +
      '<div class="form-group">' +
      "<label>Content</label>" +
      '<textarea class="form-input" id="upload-content" rows="6" placeholder="Enter file content..."></textarea>' +
      "</div>";

    var footer =
      '<button class="btn btn-secondary" onclick="window.AppUI.closeModal()">Cancel</button>' +
      '<button class="btn btn-primary" id="upload-submit">Upload</button>';

    UI.showModal("Upload Object to " + bucket, body, footer);

    document
      .getElementById("upload-submit")
      .addEventListener("click", function () {
        var key = document.getElementById("upload-key").value.trim();
        var content = document.getElementById("upload-content").value;
        if (!key) {
          UI.toast("Please enter an object key", "warning");
          return;
        }
        UI.apiCall("s3", "PutObject", {
          Bucket: bucket,
          Key: key,
          Body: content,
        })
          .then(function () {
            UI.closeModal();
            UI.toast("Uploaded: " + key, "success");
            showBucketContents(container, titleEl, breadcrumb, bucket);
          })
          .catch(function (err) {
            UI.toast("Error: " + err.message, "error");
          });
      });
  }

  // ---------------------------------------------------------------------------
  // XML Parsers
  // ---------------------------------------------------------------------------
  function parseListBucketsXml(data) {
    if (typeof data !== "string") return [];
    var buckets = [];
    var parser = new DOMParser();
    var doc = parser.parseFromString(data, "text/xml");
    var bucketEls = doc.querySelectorAll("Bucket");
    bucketEls.forEach(function (el) {
      var name = el.querySelector("Name");
      var date = el.querySelector("CreationDate");
      buckets.push({
        name: name ? name.textContent : "",
        creationDate: date ? date.textContent : "",
      });
    });
    return buckets;
  }

  function parseListObjectsXml(data) {
    if (typeof data !== "string") return [];
    var objects = [];
    var parser = new DOMParser();
    var doc = parser.parseFromString(data, "text/xml");
    var contentEls = doc.querySelectorAll("Contents");
    contentEls.forEach(function (el) {
      var key = el.querySelector("Key");
      var size = el.querySelector("Size");
      var modified = el.querySelector("LastModified");
      objects.push({
        key: key ? key.textContent : "",
        size: size ? parseInt(size.textContent, 10) : 0,
        lastModified: modified ? modified.textContent : "",
      });
    });
    return objects;
  }
})();
