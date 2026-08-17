(function () {
  "use strict";

  var previewRows = [];
  var loadedFixture = null;
  var summaryEl = document.getElementById("summary");
  var tableWrap = document.getElementById("previewTableWrap");
  var statusEl = document.getElementById("status");
  var verificationPanelEl = document.getElementById("verificationSummaryPanel");
  var verificationContentEl = document.getElementById("verificationSummaryContent");

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;");
  }

  function setStatus(msg, isError) {
    if (!statusEl) return;
    statusEl.textContent = msg;
    statusEl.className = "status" + (isError ? " error" : " ok");
  }

  function getEnvelope() {
    return {
      market: document.getElementById("market").value.trim(),
      country: document.getElementById("country").value.trim(),
      region: document.getElementById("region").value.trim(),
    };
  }

  function parseCsv(text) {
    var lines = text.trim().split(/\r?\n/).filter(Boolean);
    if (lines.length < 2) return [];
    var headers = lines[0].split(",").map(function (h) {
      return h.trim().replace(/^"|"$/g, "");
    });
    return lines.slice(1).map(function (line) {
      var cols = [];
      var cur = "";
      var inQ = false;
      for (var i = 0; i < line.length; i += 1) {
        var ch = line[i];
        if (ch === '"') inQ = !inQ;
        else if (ch === "," && !inQ) {
          cols.push(cur.trim());
          cur = "";
        } else cur += ch;
      }
      cols.push(cur.trim());
      var obj = {};
      headers.forEach(function (h, idx) {
        if (cols[idx] != null && cols[idx] !== "") obj[h] = cols[idx];
      });
      return obj;
    });
  }

  function renderVerificationSummary(verification) {
    if (!verificationPanelEl || !verificationContentEl) return;
    if (!verification || typeof verification !== "object") {
      verificationPanelEl.style.display = "none";
      verificationContentEl.innerHTML = "";
      return;
    }
    verificationPanelEl.style.display = "block";
    verificationContentEl.innerHTML =
      "<div><strong>Method:</strong> " + esc(verification.method || "—") + "</div>" +
      "<div><strong>Verified At:</strong> " + esc(verification.verifiedAt || "—") + "</div>" +
      "<div><strong>Verified Records:</strong> " + esc(verification.verifiedRecords != null ? verification.verifiedRecords : "—") + "</div>" +
      "<div><strong>Excluded Records:</strong> " + esc(verification.excludedRecords != null ? verification.excludedRecords : "—") + "</div>" +
      "<div><strong>Requirement:</strong> " + esc(verification.requirement || "—") + "</div>";
  }

  function parseInputEnvelope() {
    var jsonText = document.getElementById("jsonInput").value.trim();
    var csvFile = document.getElementById("csvFile").files[0];
    if (csvFile) {
      return csvFile.text().then(function (text) {
        return { points: parseCsv(text) };
      });
    }
    if (!jsonText) throw new Error("Paste JSON or choose a CSV file.");
    var parsed = JSON.parse(jsonText);
    if (Array.isArray(parsed)) return Promise.resolve({ points: parsed });
    if (parsed.points && Array.isArray(parsed.points)) return Promise.resolve(parsed);
    throw new Error("JSON must be an array or { points: [] }");
  }

  function renderPreview(data) {
    previewRows = data.preview || [];
    if (summaryEl) {
      summaryEl.innerHTML =
        "<strong>Summary:</strong> " +
        esc(JSON.stringify(data.summary || {}, null, 0));
    }
    if (!tableWrap) return;
    if (!previewRows.length) {
      tableWrap.innerHTML = "<p class=\"muted\">No preview rows.</p>";
      return;
    }
    var html =
      '<table class="preview-table"><thead><tr>' +
      "<th>Include</th><th>Name</th><th>Point Type</th><th>Segment</th>" +
      "<th>City</th><th>Country</th><th>Lat</th><th>Lng</th>" +
      "<th>Relevance</th><th>Confidence</th><th>Warnings</th><th>Duplicate</th>" +
      "</tr></thead><tbody>";
    previewRows.forEach(function (row, i) {
      var dup =
        row.duplicateStatus === "possible_duplicate"
          ? "Possible duplicate"
          : "—";
      html +=
        "<tr data-index=\"" +
        i +
        "\">" +
        '<td><input type="checkbox" class="row-include" ' +
        (row.include !== false && row.valid !== false ? "checked" : "") +
        (row.valid === false ? " disabled" : "") +
        "></td>" +
        "<td>" +
        esc(row.name) +
        "</td>" +
        "<td>" +
        esc(row.pointType) +
        "</td>" +
        "<td>" +
        esc(row.demandSegment) +
        "</td>" +
        "<td>" +
        esc(row.city) +
        "</td>" +
        "<td>" +
        esc(row.country) +
        "</td>" +
        "<td>" +
        esc(row.latitude) +
        "</td>" +
        "<td>" +
        esc(row.longitude) +
        "</td>" +
        "<td>" +
        esc(row.demandRelevance) +
        "</td>" +
        "<td>" +
        esc(row.dataConfidence) +
        "</td>" +
        "<td>" +
        esc((row.warnings || []).join("; ")) +
        (row.errors && row.errors.length ? " ERR: " + row.errors.join("; ") : "") +
        "</td>" +
        "<td>" +
        esc(dup) +
        "</td></tr>";
    });
    html += "</tbody></table>";
    tableWrap.innerHTML = html;
  }

  function selectedRecords() {
    var checks = document.querySelectorAll(".row-include");
    var out = [];
    checks.forEach(function (cb, i) {
      if (!cb.checked || !previewRows[i]) return;
      var row = previewRows[i];
      out.push({
        include: true,
        normalized: row.normalized,
        name: row.name,
        pointType: row.pointType,
        duplicateStatus: row.duplicateStatus,
        valid: row.valid,
        index: row.index,
      });
    });
    return out;
  }

  document.getElementById("btnPreview").addEventListener("click", function () {
    setStatus("Running preview…");
    parseInputEnvelope()
      .then(function (envelope) {
        var env = getEnvelope();
        return fetch("/api/radar-map-points/demand-anchors/import-preview", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            market: env.market,
            country: env.country,
            region: env.region,
            points: envelope.points || [],
            verification: envelope.verification || loadedFixture?.verification || null,
            requireVerifiedFile: document.getElementById("requireVerifiedFixture").checked,
          }),
        }).then(function (r) {
          return r.json().then(function (data) {
            if (!r.ok || !data.ok) throw new Error(data.message || "Preview failed");
            return data;
          });
        });
      })
      .then(function (data) {
        renderPreview(data);
        setStatus("Preview ready — review rows and save selected.");
      })
      .catch(function (err) {
        setStatus(err.message || String(err), true);
      });
  });

  document.getElementById("btnSave").addEventListener("click", function () {
    var records = selectedRecords();
    if (!records.length) {
      setStatus("Select at least one valid row to save.", true);
      return;
    }
    var env = getEnvelope();
    var skipDup = document.getElementById("skipDuplicates").checked;
    setStatus("Saving " + records.length + " record(s)…");
    fetch("/api/radar-map-points/demand-anchors/import-commit", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        records: records,
        skipDuplicates: skipDup,
        market: env.market,
        country: env.country,
        region: env.region,
        verification: loadedFixture?.verification || null,
        requireVerifiedFile: document.getElementById("requireVerifiedFixture").checked,
      }),
    })
      .then(function (r) {
        return r.json().then(function (data) {
          return { status: r.status, data: data };
        });
      })
      .then(function (res) {
        var d = res.data;
        if (!d.ok && !d.created?.length) {
          throw new Error(d.message || "Save failed");
        }
        setStatus(
          "Created " +
            (d.created?.length || 0) +
            ", skipped " +
            (d.skipped?.length || 0) +
            ", errors " +
            (d.errors?.length || 0)
        );
      })
      .catch(function (err) {
        setStatus(err.message || String(err), true);
      });
  });

  document.getElementById("btnLoadFixture").addEventListener("click", function () {
    var sel = document.getElementById("fixtureSelect").value;
    if (!sel) return;
    fetch("/fixtures/" + sel)
      .then(function (r) {
        return r.json();
      })
      .then(function (data) {
        loadedFixture = data && typeof data === "object" ? data : null;
        document.getElementById("market").value = data.market || "";
        document.getElementById("country").value = data.country || "";
        document.getElementById("region").value = data.region || "";
        document.getElementById("jsonInput").value = JSON.stringify(data, null, 2);
        renderVerificationSummary(data.verification || null);
        setStatus("Loaded fixture " + sel);
      })
      .catch(function (e) {
        setStatus("Fixture load failed: " + e.message, true);
      });
  });
})();
