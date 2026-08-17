/**
 * Market Demand Intelligence — frontend renderer and data loader.
 */
(function (global) {
  "use strict";

  var CATEGORY_MIX = [
    { key: "leisure", label: "Leisure" },
    { key: "corporate", label: "Corporate" },
    { key: "group", label: "Group / Event" },
    { key: "medical", label: "Medical" },
    { key: "education", label: "Education" },
    { key: "transportation", label: "Transportation" },
    { key: "industrial", label: "Industrial" },
    { key: "retailMixedUse", label: "Retail / Mixed-Use" },
    { key: "government", label: "Government" },
  ];

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function fmtDistance(mi) {
    if (mi == null || !Number.isFinite(Number(mi))) return "—";
    return Number(mi).toFixed(1) + " mi";
  }

  function fmtDrive(min) {
    if (min == null || !Number.isFinite(Number(min))) return "—";
    return Math.round(Number(min)) + " min";
  }

  function countByCategory(demandCenters) {
    var out = {};
    (demandCenters || []).forEach(function (dc) {
      var label = dc.category || "Uncategorized";
      out[label] = (out[label] || 0) + 1;
    });
    return out;
  }

  function renderSummaryCards(state) {
    var snap = state.snapshot;
    var summary = state.demandSummary || {};
    var total = summary.totalDemandCenters != null ? summary.totalDemandCenters : (state.demandCenters || []).length;
    var profile = snap && snap.primaryDemandProfile ? snap.primaryDemandProfile : (summary.topCategories || []).map(function (c) { return c.label; }).slice(0, 3).join(", ") || "—";
    var strength = snap && snap.overallDemandStrength ? snap.overallDemandStrength : "—";
    var confidence = snap && snap.dataConfidence ? snap.dataConfidence : summary.dataConfidence || "—";

    return (
      '<div class="md-cards">' +
      '<div class="md-card"><div class="md-card-label">Total Demand Centers</div><div class="md-card-value">' + esc(total) + "</div></div>" +
      '<div class="md-card"><div class="md-card-label">Primary Demand Profile</div><div class="md-card-value">' + esc(profile || "—") + "</div></div>" +
      '<div class="md-card"><div class="md-card-label">Overall Demand Strength</div><div class="md-card-value">' + esc(strength) + "</div></div>" +
      '<div class="md-card"><div class="md-card-label">Data Confidence</div><div class="md-card-value">' + esc(confidence) + "</div></div>" +
      "</div>"
    );
  }

  function countForMixCategory(cat, apiCategories, demandCenters) {
    var count = 0;
    if (apiCategories && typeof apiCategories === "object") {
      if (apiCategories[cat.label]) count += apiCategories[cat.label];
      Object.keys(apiCategories).forEach(function (label) {
        if (label === cat.label) return;
        var lc = label.toLowerCase();
        if (cat.key === "group" && (lc.indexOf("group") >= 0 || lc.indexOf("event") >= 0)) count += apiCategories[label];
        else if (cat.key === "retailMixedUse" && (lc.indexOf("retail") >= 0 || lc.indexOf("mixed") >= 0)) count += apiCategories[label];
        else if (lc.indexOf(cat.key.toLowerCase()) >= 0) count += apiCategories[label];
      });
      return count;
    }
    var counts = countByCategory(demandCenters);
    Object.keys(counts).forEach(function (label) {
      if (label === cat.label) count += counts[label];
      else if (cat.key === "group" && /group|event/i.test(label)) count += counts[label];
      else if (cat.key === "retailMixedUse" && /retail|mixed/i.test(label)) count += counts[label];
      else if (label.toLowerCase().indexOf(cat.key.toLowerCase()) >= 0) count += counts[label];
    });
    return count;
  }

  function renderDemandMix(state) {
    var apiCategories = (state.demandSummary && state.demandSummary.categories) || null;
    var scores = (state.snapshot && state.snapshot.scores) || {};
    var html = '<div class="md-mix-grid">';
    CATEGORY_MIX.forEach(function (cat) {
      var count = countForMixCategory(cat, apiCategories, state.demandCenters);
      var score = scores[cat.key];
      html +=
        '<div class="md-mix-item">' +
        '<div class="label">' + esc(cat.label) + "</div>" +
        '<div class="count">' + esc(count) + "</div>" +
        (score != null ? '<div class="score">Score: ' + esc(score) + "</div>" : '<div class="score">Score: —</div>') +
        "</div>";
    });
    html += "</div>";
    return html;
  }

  function renderDemandCentersTable(centers) {
    if (!centers || !centers.length) {
      return (
        '<div class="md-empty">No demand centers have been added for this opportunity yet. Add demand centers manually or import them later from a location data source.</div>'
      );
    }
    var rows = centers
      .map(function (dc) {
        return (
          "<tr>" +
          "<td>" + esc(dc.name || "—") + "</td>" +
          "<td>" + esc(dc.category || "—") + "</td>" +
          "<td>" + esc(dc.subcategory || "—") + "</td>" +
          "<td>" + esc(fmtDistance(dc.distanceFromDeal)) + "</td>" +
          "<td>" + esc(fmtDrive(dc.estimatedDriveTime)) + "</td>" +
          "<td>" + esc(dc.demandStrength || "—") + "</td>" +
          "<td>" + esc(dc.relevanceToHotelDemand || (dc.relevanceScore != null ? dc.relevanceScore : "—")) + "</td>" +
          "<td>" + esc(dc.dataConfidence || "—") + "</td>" +
          "<td>" + esc((dc.source || []).join(", ") || "—") + "</td>" +
          "</tr>"
        );
      })
      .join("");
    return (
      '<div class="md-table-wrap"><table class="md-table"><thead><tr>' +
      "<th>Demand Center</th><th>Category</th><th>Subcategory</th><th>Distance</th><th>Drive Time</th>" +
      "<th>Demand Strength</th><th>Relevance</th><th>Confidence</th><th>Source</th>" +
      "</tr></thead><tbody>" +
      rows +
      "</tbody></table></div>"
    );
  }

  function renderNearbyTable(hotels) {
    if (!hotels || !hotels.length) {
      return '<div class="md-empty">No nearby hotel supply records linked to this opportunity yet.</div>';
    }
    var rows = hotels
      .map(function (h) {
        return (
          "<tr>" +
          "<td>" + esc(h.hotelName || "—") + "</td>" +
          "<td>" + esc(h.brand || "—") + "</td>" +
          "<td>" + esc(h.parentCompany || "—") + "</td>" +
          "<td>" + esc(h.chainScale || "—") + "</td>" +
          "<td>" + esc(fmtDistance(h.distanceFromDeal)) + "</td>" +
          "<td>" + esc(h.competitiveRelevance || "—") + "</td>" +
          "<td>" + esc(h.dataConfidence || "—") + "</td>" +
          "</tr>"
        );
      })
      .join("");
    return (
      '<div class="md-table-wrap"><table class="md-table"><thead><tr>' +
      "<th>Hotel</th><th>Brand</th><th>Parent Company</th><th>Chain Scale</th><th>Distance</th><th>Competitive Relevance</th><th>Confidence</th>" +
      "</tr></thead><tbody>" + rows + "</tbody></table></div>"
    );
  }

  function renderSnapshotBlock(snap) {
    if (!snap) {
      return '<div class="md-empty">No market demand snapshot has been generated yet. Use Generate Demand Snapshot after adding demand centers.</div>';
    }
    function block(title, text) {
      return (
        '<div class="md-snapshot-block"><h3>' + esc(title) + "</h3><p>" + esc(text || "—") + "</p></div>"
      );
    }
    return (
      block("Demand Summary", snap.demandSummary) +
      block("Demand Gaps", snap.demandGaps) +
      block("Brand Considerations", snap.brandImplications) +
      block("Operator Considerations", snap.operatorImplications) +
      block("Recommended Follow-Up", snap.recommendedFollowUp)
    );
  }

  function renderPage(root, state, opts) {
    var dealId = opts.dealId || "";
    var backHref = opts.backHref || "/my-deals.html";
    root.innerHTML =
      (opts.fullPage ? '<a class="md-back" href="' + esc(backHref) + '">← Back to My Deals</a>' : "") +
      '<header class="md-head"><h1>Market Demand</h1>' +
      '<p class="md-helper">Review location-based demand signals connected to this opportunity. This view is intended to provide early location context, not a formal market study or appraisal.</p></header>' +
      (state.banner ? '<div class="md-banner ' + esc(state.bannerType || "info") + '">' + esc(state.banner) + "</div>" : "") +
      '<div class="md-toolbar">' +
      '<button type="button" data-md-action="generate">Generate Demand Snapshot</button>' +
      '<button type="button" class="secondary" data-md-action="import">Import Demand Centers</button>' +
      '<button type="button" class="secondary" data-md-action="add-hotel" disabled title="Coming soon">Add Nearby Hotel</button>' +
      '<button type="button" class="secondary" data-md-action="refresh">Refresh Data</button>' +
      "</div>" +
      renderSummaryCards(state) +
      '<section class="md-section"><h2>Demand Mix</h2>' + renderDemandMix(state) + "</section>" +
      '<div class="md-map-placeholder">Map view coming soon. Demand centers are currently shown in the table below.</div>' +
      '<section class="md-section"><h2>Demand Centers</h2>' + renderDemandCentersTable(state.demandCenters) + "</section>" +
      '<section class="md-section"><h2>Nearby Hotel Supply</h2>' + renderNearbyTable(state.nearbyHotelSupply) + "</section>" +
      '<section class="md-section"><h2>Market Demand Snapshot</h2>' + renderSnapshotBlock(state.snapshot) + "</section>";

    root.querySelectorAll("[data-md-action]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        var action = btn.getAttribute("data-md-action");
        if (action === "refresh" && opts.onRefresh) opts.onRefresh();
        if (action === "generate" && opts.onGenerate) opts.onGenerate();
        if (action === "import" && opts.onImport) opts.onImport();
        if (action === "add-hotel" && opts.onAddHotel) opts.onAddHotel();
      });
    });
  }

  function loadMarketDemandData(dealId, fetchFn) {
    var base = "/api/deals/" + encodeURIComponent(dealId);
    return Promise.all([
      fetchFn(base + "/demand-centers", { method: "GET" }).then(function (r) { return r.json(); }),
      fetchFn(base + "/nearby-hotel-supply", { method: "GET" }).then(function (r) { return r.json(); }),
      fetchFn(base + "/market-demand-snapshot", { method: "GET" }).then(function (r) { return r.json(); }),
    ]).then(function (pack) {
      var dc = pack[0];
      var supply = pack[1];
      var snap = pack[2];
      var err = null;
      if (!dc.ok) err = dc.message || dc.error;
      else if (!supply.ok) err = supply.message || supply.error;
      else if (!snap.ok) err = snap.message || snap.error;
      return {
        error: err,
        errorCode: dc.error || supply.error || snap.error,
        demandCenters: dc.ok ? dc.demandCenters : [],
        demandSummary: dc.ok ? dc.summary : {},
        nearbyHotelSupply: supply.ok ? supply.nearbyHotelSupply : [],
        supplySummary: supply.ok ? supply.summary : {},
        snapshot: snap.ok && snap.hasSnapshot ? snap.snapshot : null,
      };
    });
  }

  function parseImportJson(text) {
    var parsed = JSON.parse(text);
    if (Array.isArray(parsed)) return parsed;
    if (parsed && Array.isArray(parsed.demandCenters)) return parsed.demandCenters;
    throw new Error("JSON must be an array or { demandCenters: [...] }");
  }

  function renderImportPreviewTable(rows) {
    if (!rows || !rows.length) {
      return '<div class="md-empty">No rows to preview.</div>';
    }
    var body = rows
      .map(function (row) {
        var item = row.item || {};
        var src = Array.isArray(item.source) ? item.source.join(", ") : item.source || "—";
        var statusClass =
          row.importStatus === "accepted"
            ? "md-status-accepted"
            : row.importStatus === "warning"
              ? "md-status-warning"
              : "md-status-rejected";
        return (
          "<tr>" +
          '<td><input type="checkbox" class="md-import-select" data-index="' +
          row.index +
          '" ' +
          (row.selected && row.importStatus !== "rejected" ? "checked" : "") +
          (row.importStatus === "rejected" ? " disabled" : "") +
          " /></td>" +
          "<td>" + esc(item.name || "—") + "</td>" +
          "<td>" + esc(item.category || "—") + "</td>" +
          "<td>" + esc(item.subcategory || "—") + "</td>" +
          "<td>" + esc(fmtDistance(item.distanceFromDeal)) + "</td>" +
          "<td>" + esc(fmtDrive(item.estimatedDriveTime)) + "</td>" +
          "<td>" + esc(item.demandStrength || "—") + "</td>" +
          "<td>" + esc(item.relevanceToHotelDemand || item.relevanceScore || "—") + "</td>" +
          "<td>" + esc(item.dataConfidence || "—") + "</td>" +
          "<td>" + esc(src) + "</td>" +
          '<td class="' + statusClass + '">' + esc(row.importStatus) + "</td>" +
          "</tr>"
        );
      })
      .join("");
    return (
      '<div class="md-table-wrap"><table class="md-table md-import-table"><thead><tr>' +
      "<th>Select</th><th>Demand Center</th><th>Category</th><th>Subcategory</th><th>Distance</th>" +
      "<th>Drive Time</th><th>Demand Strength</th><th>Relevance</th><th>Confidence</th><th>Source</th><th>Import Status</th>" +
      "</tr></thead><tbody>" + body + "</tbody></table></div>"
    );
  }

  function openImportModal(dealId, fetchFn, onComplete) {
    var overlay = document.createElement("div");
    overlay.className = "md-import-overlay";
    overlay.innerHTML =
      '<div class="md-import-modal" role="dialog" aria-labelledby="mdImportTitle">' +
      '<div class="md-import-header"><h2 id="mdImportTitle">Import Demand Centers</h2>' +
      '<button type="button" class="md-import-close" aria-label="Close">×</button></div>' +
      '<p class="md-import-help">Paste or upload JSON in the same shape as <code>fixtures/market-demand-sample-import.json</code>. Preview before saving. External location APIs are not connected yet.</p>' +
      '<div class="md-import-input-row">' +
      '<textarea id="mdImportJson" class="md-import-textarea" placeholder=\'{"demandCenters":[...]}\'></textarea>' +
      '<label class="md-import-file-label">Upload JSON file<input type="file" id="mdImportFile" accept=".json,application/json" hidden /></label>' +
      "</div>" +
      '<div class="md-import-actions">' +
      '<button type="button" class="secondary" id="mdImportPreviewBtn">Preview Records</button>' +
      '<button type="button" id="mdImportSaveBtn" disabled>Save Selected Records</button>' +
      '<button type="button" class="secondary" id="mdImportCancelBtn">Cancel Import</button>' +
      "</div>" +
      '<div id="mdImportStatus" class="md-import-status" aria-live="polite"></div>' +
      '<div id="mdImportPreviewWrap"></div>' +
      "</div>";

    document.body.appendChild(overlay);

    var previewState = null;
    var rawItems = [];

    function close() {
      if (overlay.parentNode) overlay.parentNode.removeChild(overlay);
    }

    function setStatus(msg, type) {
      var el = overlay.querySelector("#mdImportStatus");
      if (!el) return;
      el.textContent = msg || "";
      el.className = "md-import-status" + (type ? " md-import-status--" + type : "");
    }

    overlay.querySelector(".md-import-close").addEventListener("click", close);
    overlay.querySelector("#mdImportCancelBtn").addEventListener("click", close);

    overlay.querySelector("#mdImportFile").addEventListener("change", function (e) {
      var file = e.target.files && e.target.files[0];
      if (!file) return;
      var reader = new FileReader();
      reader.onload = function () {
        overlay.querySelector("#mdImportJson").value = reader.result;
      };
      reader.readAsText(file);
    });

    overlay.querySelector("#mdImportPreviewBtn").addEventListener("click", function () {
      var text = overlay.querySelector("#mdImportJson").value.trim();
      if (!text) {
        setStatus("Paste or upload JSON first.", "error");
        return;
      }
      try {
        rawItems = parseImportJson(text);
      } catch (err) {
        setStatus(err.message || "Invalid JSON", "error");
        return;
      }
      setStatus("Validating preview…", "info");
      fetchFn("/api/deals/" + encodeURIComponent(dealId) + "/preview-demand-center-import", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ demandCenters: rawItems }),
      })
        .then(function (r) { return r.json(); })
        .then(function (data) {
          if (!data.ok) throw new Error(data.message || data.error || "Preview failed");
          previewState = data;
          var wrap = overlay.querySelector("#mdImportPreviewWrap");
          wrap.innerHTML = renderImportPreviewTable(data.previewRows);
          var saveBtn = overlay.querySelector("#mdImportSaveBtn");
          saveBtn.disabled = !data.summary || data.summary.acceptedCount === 0;
          setStatus(
            "Preview ready: " +
              data.summary.acceptedCount +
              " accepted, " +
              data.summary.rejectedCount +
              " rejected, " +
              data.summary.warningCount +
              " with warnings.",
            "ok"
          );
        })
        .catch(function (err) {
          setStatus(err.message || "Preview failed", "error");
        });
    });

    overlay.querySelector("#mdImportSaveBtn").addEventListener("click", function () {
      if (!rawItems.length) return;
      var selected = [];
      overlay.querySelectorAll(".md-import-select:checked").forEach(function (cb) {
        selected.push(Number(cb.getAttribute("data-index")));
      });
      if (!selected.length) {
        setStatus("Select at least one row to import.", "error");
        return;
      }
      setStatus("Saving selected records…", "info");
      fetchFn("/api/deals/" + encodeURIComponent(dealId) + "/import-demand-centers", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ demandCenters: rawItems, selectedIndices: selected }),
      })
        .then(function (r) { return r.json(); })
        .then(function (data) {
          if (!data.ok) throw new Error(data.message || data.error || "Import failed");
          close();
          if (onComplete) onComplete(data);
        })
        .catch(function (err) {
          setStatus(err.message || "Import failed", "error");
        });
    });
  }

  global.MarketDemand = {
    render: renderPage,
    load: loadMarketDemandData,
    openImportModal: openImportModal,
    esc: esc,
    parseImportJson: parseImportJson,
    buildImportPreviewTable: renderImportPreviewTable,
  };
})(typeof window !== "undefined" ? window : globalThis);
