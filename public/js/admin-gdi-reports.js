/**
 * AI Demand Admin — GDI Reports tab.
 * Shared AI Admin table pattern (sort / filter / select / compact actions).
 * Client Opportunities = external client-list count (before sanitization).
 * Ready / Action Set = strict report metrics. Watch may be a capped report sample.
 * Open/Copy Client Link follow a valid share and do not require Ready > 0 or a PDF.
 */
(function () {
  "use strict";

  var T = window.DealalityAiAdminTable;
  if (!T) {
    console.error("[GDI Reports] DealalityAiAdminTable missing — load admin-ai-demand-table.js first");
    return;
  }

  var catalog = [];
  var counts = {};
  /** @type {Record<string, 'GENERATING'|'FAILED'>} */
  var rowBusy = {};
  /** @type {Record<string, string>} */
  var rowErrors = {};
  var bulkRunning = false;

  var ctrl = T.createTableController({
    defaultSortKey: "displayName",
    idKey: "hotelId",
    tieBreak: "displayName",
    sortTypes: {
      displayName: "string",
      market: "string",
      clientOpportunityCount: "number",
      readyCount: "number",
      actionSetCount: "number",
      watchCount: "number",
      reportStatus: "string",
      pdfStatus: "string",
      lastGeneratedAt: "date",
    },
    onResetFilters: function () {
      if ($("gdiRptSearch")) $("gdiRptSearch").value = "";
      if ($("gdiRptFilterReport")) $("gdiRptFilterReport").value = "";
      if ($("gdiRptFilterPdf")) $("gdiRptFilterPdf").value = "";
      if ($("gdiRptDateFrom")) $("gdiRptDateFrom").value = "";
      if ($("gdiRptDateTo")) $("gdiRptDateTo").value = "";
    },
  });

  function authFetch(url, opts) {
    opts = opts || {};
    if (window.DealalityMemberstackAuth && window.DealalityMemberstackAuth.authFetch) {
      return window.DealalityMemberstackAuth.authFetch(url, opts);
    }
    return fetch(url, opts);
  }

  function $(id) {
    return document.getElementById(id);
  }

  function setStatus(msg) {
    var el = $("gdiRptStatusLine");
    if (el) el.textContent = msg || "";
  }

  function reportStatusKind(s) {
    if (s === "READY") return "ok";
    if (s === "BLOCKED" || s === "NEEDS_BUILD") return "danger";
    return "warn";
  }

  function reportStatusLabel(s) {
    if (s === "WATCH_ONLY") return "WATCH ONLY";
    if (s === "NO_READY_OPPORTUNITIES") return "NO READY OPPORTUNITIES";
    if (s === "NEEDS_BUILD") return "NEEDS BUILD";
    return s || "—";
  }

  function copyText(text) {
    function legacyCopy() {
      return new Promise(function (resolve, reject) {
        try {
          var ta = document.createElement("textarea");
          ta.value = text;
          ta.setAttribute("readonly", "readonly");
          ta.style.position = "fixed";
          ta.style.left = "-9999px";
          document.body.appendChild(ta);
          ta.select();
          var ok = document.execCommand("copy");
          ta.remove();
          if (!ok) reject(new Error("copy_failed"));
          else resolve();
        } catch (e) {
          reject(e);
        }
      });
    }
    if (navigator.clipboard && navigator.clipboard.writeText) {
      return navigator.clipboard.writeText(text).catch(legacyCopy);
    }
    return legacyCopy();
  }

  function effectivePdfStatus(row) {
    if (rowBusy[row.hotelId] === "GENERATING") return "GENERATING";
    if (rowBusy[row.hotelId] === "FAILED") return "FAILED";
    return row.pdfStatus || (row.pdfReady ? "READY" : "MISSING");
  }

  function normalizeCatalogRow(h) {
    if (!h || typeof h !== "object") return h;
    var readFailed = !!h.readFailed;
    var ready = readFailed
      ? null
      : Number(h.readyCount != null ? h.readyCount : h.ready) || 0;
    var actionSet = readFailed
      ? null
      : Number(h.actionSetCount != null ? h.actionSetCount : h.actionSet) || 0;
    var watch = readFailed
      ? null
      : Number(h.watchCount != null ? h.watchCount : h.watch) || 0;
    var watchTotal = readFailed
      ? null
      : h.watchTotal == null
        ? watch
        : Number(h.watchTotal) || 0;
    var clientOpportunityCount = readFailed
      ? null
      : h.clientOpportunityCount == null
        ? null
        : Number(h.clientOpportunityCount) || 0;
    var pdfReady = !!(
      (h.pdf && h.pdf.available) ||
      h.pdfReady ||
      h.pdfStatus === "READY"
    );
    var reportStatus = h.reportStatus;
    if (!reportStatus) {
      if (!h.available) reportStatus = "BLOCKED";
      else if (ready > 0) reportStatus = "READY";
      else if (watch > 0) reportStatus = "WATCH_ONLY";
      else reportStatus = "NO_READY_OPPORTUNITIES";
    }
    var gdiAvail = !!(
      (h.gdiClient && h.gdiClient.available) ||
      h.gdiShareAvailable
    );
    var adpAvail = !!(
      (h.adpClient && h.adpClient.available) ||
      h.adpShareAvailable
    );
    var lastGeneratedAt =
      h.lastGeneratedAt || (h.pdf && h.pdf.generatedAt) || null;
    return Object.assign({}, h, {
      readFailed: readFailed,
      ready: ready,
      actionSet: actionSet,
      watch: watch,
      readyCount: ready,
      actionSetCount: actionSet,
      watchCount: watch,
      watchTotal: watchTotal,
      watchCapped: !readFailed && watchTotal != null && watch != null && watchTotal > watch,
      clientOpportunityCount: clientOpportunityCount,
      gdiShareReason: h.gdiShareReason || (h.gdiClient && h.gdiClient.reason) || null,
      reportStatus: reportStatus,
      pdfReady: pdfReady,
      pdfStatus: h.pdfStatus || (h.pdf && h.pdf.status) || (pdfReady ? "READY" : "MISSING"),
      lastGeneratedAt: lastGeneratedAt,
      gdiShareAvailable: gdiAvail,
      adpShareAvailable: adpAvail,
      gdiClient: { available: gdiAvail },
      adpClient: { available: adpAvail },
      displayName: h.displayName || h.hotelName || h.hotelId,
      market: h.market || null,
    });
  }

  function countsFromRows(rows) {
    var list = rows || [];
    return {
      hotels: list.length,
      reportReady: list.filter(function (r) {
        return r.reportStatus === "READY";
      }).length,
      pdfReady: list.filter(function (r) {
        return r.pdfReady || r.pdfStatus === "READY";
      }).length,
      needsPdf: list.filter(function (r) {
        return r.available && !(r.pdfReady || r.pdfStatus === "READY");
      }).length,
      blocked: list.filter(function (r) {
        return r.reportStatus === "BLOCKED" || r.reportStatus === "NEEDS_BUILD";
      }).length,
    };
  }

  function filterFn(r) {
    var q = ($("gdiRptSearch") && $("gdiRptSearch").value.trim().toLowerCase()) || "";
    var reportF = ($("gdiRptFilterReport") && $("gdiRptFilterReport").value) || "";
    var pdfF = ($("gdiRptFilterPdf") && $("gdiRptFilterPdf").value) || "";
    var from = ($("gdiRptDateFrom") && $("gdiRptDateFrom").value) || "";
    var to = ($("gdiRptDateTo") && $("gdiRptDateTo").value) || "";
    if (q) {
      var hay = (
        (r.displayName || "") +
        " " +
        (r.hotelName || "") +
        " " +
        (r.market || "") +
        " " +
        (r.hotelId || "")
      ).toLowerCase();
      if (hay.indexOf(q) === -1) return false;
    }
    if (reportF && r.reportStatus !== reportF) return false;
    if (pdfF && effectivePdfStatus(r) !== pdfF) return false;
    if (from || to) {
      var ts = r.lastGeneratedAt ? new Date(r.lastGeneratedAt).getTime() : null;
      if (ts == null || Number.isNaN(ts)) return false;
      if (from) {
        var fromTs = new Date(from + "T00:00:00.000Z").getTime();
        if (ts < fromTs) return false;
      }
      if (to) {
        var toTs = new Date(to + "T23:59:59.999Z").getTime();
        if (ts > toTs) return false;
      }
    }
    return true;
  }

  function activeFilterCount() {
    var n = 0;
    if ($("gdiRptSearch") && $("gdiRptSearch").value.trim()) n++;
    if ($("gdiRptFilterReport") && $("gdiRptFilterReport").value) n++;
    if ($("gdiRptFilterPdf") && $("gdiRptFilterPdf").value) n++;
    if ($("gdiRptDateFrom") && $("gdiRptDateFrom").value) n++;
    if ($("gdiRptDateTo") && $("gdiRptDateTo").value) n++;
    return n;
  }

  function renderCounts() {
    var el = $("gdiRptCounts");
    if (!el) return;
    var c = counts || {};
    var items = [
      ["GDI Hotels", c.hotels || catalog.length || 0],
      ["Report Ready", c.reportReady || 0],
      ["PDF Ready", c.pdfReady || 0],
      ["Needs PDF", c.needsPdf || 0],
      ["Blocked", c.blocked || 0],
    ];
    el.innerHTML = items
      .map(function (it) {
        return (
          '<div class="adr-count"><span class="adr-count__n">' +
          it[1] +
          '</span><span class="adr-count__l">' +
          T.esc(it[0]) +
          "</span></div>"
        );
      })
      .join("");
  }

  function buildActions(row) {
    var st = effectivePdfStatus(row);
    var gdiAvail =
      !!(row.gdiClient && row.gdiClient.available) || !!row.gdiShareAvailable;
    var adpAvail =
      !!(row.adpClient && row.adpClient.available) || !!row.adpShareAvailable;
    var primary = [];
    var more = [];

    if (st === "READY" || row.pdfReady) {
      primary.push({ label: "View PDF", act: "view-pdf", title: "View GDI report PDF" });
      more.push({ label: "Download PDF", act: "download-pdf" });
    } else if (st === "GENERATING") {
      primary.push({
        label: "Generating…",
        act: "noop",
        disabled: true,
        title: "PDF generation in progress",
      });
    } else if (st === "FAILED") {
      primary.push({
        label: "Retry PDF",
        act: "generate",
        title: rowErrors[row.hotelId] || "Retry PDF generation",
      });
    } else if (row.available) {
      primary.push({
        label: "Generate PDF",
        act: "generate",
        title: "Generate GDI report PDF",
      });
    } else {
      primary.push({
        label: "PDF unavailable",
        act: "noop",
        disabled: true,
        title: row.reason || "Report data unavailable",
      });
    }

    var emptyClient =
      !row.readFailed && Number(row.clientOpportunityCount) === 0;
    if (gdiAvail) {
      primary.push({
        label: "Open Client Link",
        act: "open-gdi",
        icon: "external",
        title: emptyClient
          ? "Open client link. No published opportunities."
          : "Open client link",
      });
      primary.push({
        label: "Copy Client Link",
        act: "copy-gdi",
        icon: "copy",
        title: emptyClient
          ? "Copy client link. No published opportunities."
          : "Copy client link",
      });
    } else {
      more.push({
        label: "Open Client Link",
        act: "open-gdi",
        disabled: true,
        title: row.gdiShareReason || "No valid GDI client link for this hotel",
      });
    }
    if (adpAvail) {
      more.push({ label: "Open ADP", act: "open-adp" });
      more.push({ label: "Copy ADP URL", act: "copy-adp" });
    } else {
      more.push({
        label: "Open ADP",
        act: "open-adp",
        disabled: true,
        title: "No authorized ADP share link for this hotel",
      });
    }
    if (row.available) {
      more.push({
        label: "Regenerate PDF",
        act: "regenerate",
        disabled: rowBusy[row.hotelId] === "GENERATING",
        title:
          rowBusy[row.hotelId] === "GENERATING"
            ? "Generation already running"
            : "Regenerate GDI PDF",
      });
    }
    more.push({ label: "Report Archive", act: "archive" });
    return { primary: primary, more: more };
  }

  function renderResultsBar(view) {
    var host = $("gdiRptResultsRow");
    if (!host) return;
    var badge = activeFilterCount();
    var resetBadge =
      badge > 0
        ? '<span class="ada-filter-badge">' + badge + "</span>"
        : "";
    host.innerHTML =
      T.resultsCountHtml(view.showing, view.total, "hotels") +
      '<div class="ada-bulk-wrap">' +
      T.selectionBarHtml(ctrl.selection.count(), true) +
      '<button type="button" class="ada-bulk-btn" id="gdiRptBulkBtn" ' +
      (ctrl.selection.count() ? "" : "disabled") +
      ">Bulk Actions ▾</button>" +
      '<button type="button" class="ada-btn-reset" id="gdiRptResetView">Reset View' +
      resetBadge +
      "</button>" +
      "</div>";
    var bulkBtn = $("gdiRptBulkBtn");
    if (bulkBtn) {
      bulkBtn.addEventListener("click", function (ev) {
        ev.stopPropagation();
        toggleBulkMenu(bulkBtn);
      });
    }
    var resetBtn = $("gdiRptResetView");
    if (resetBtn) {
      resetBtn.addEventListener("click", function () {
        ctrl.resetView();
        renderTable();
      });
    }
  }

  function toggleBulkMenu(anchor) {
    var existing = document.getElementById("gdiRptBulkMenu");
    if (existing) {
      existing.remove();
      return;
    }
    var menu = document.createElement("div");
    menu.id = "gdiRptBulkMenu";
    menu.className = "ada-bulk-menu";
    menu.innerHTML =
      '<button type="button" data-bulk="generate-missing-pdf">Generate missing PDFs (selected)</button>' +
      '<button type="button" data-bulk="export-csv">Export selected rows (CSV)</button>' +
      '<button type="button" data-bulk="clear">Clear selection</button>';
    anchor.parentNode.style.position = "relative";
    anchor.parentNode.appendChild(menu);
    menu.addEventListener("click", function (ev) {
      var b = ev.target.closest("button[data-bulk]");
      if (!b) return;
      var act = b.getAttribute("data-bulk");
      menu.remove();
      if (act === "clear") {
        ctrl.selection.clear();
        renderTable();
        return;
      }
      if (act === "export-csv") exportSelectedCsv();
      if (act === "generate-missing-pdf") bulkGenerateMissingPdfs();
    });
    setTimeout(function () {
      document.addEventListener(
        "click",
        function once() {
          var m = document.getElementById("gdiRptBulkMenu");
          if (m) m.remove();
        },
        { once: true }
      );
    }, 0);
  }

  function exportSelectedCsv() {
    var ids = ctrl.selection.ids();
    var rows = catalog.filter(function (r) {
      return ids.indexOf(String(r.hotelId)) >= 0;
    });
    if (!rows.length) {
      setStatus("No rows selected for export.");
      return;
    }
    var header = [
      "hotelId",
      "displayName",
      "location",
      "readyCount",
      "actionSetCount",
      "watchCount",
      "reportStatus",
      "pdfStatus",
      "lastGeneratedAt",
      "metricSource",
    ];
    var lines = [header.join(",")];
    rows.forEach(function (r) {
      lines.push(
        [
          r.hotelId,
          csvEsc(r.displayName),
          csvEsc(r.market),
          r.readyCount,
          r.actionSetCount,
          r.watchCount,
          r.reportStatus,
          effectivePdfStatus(r),
          r.lastGeneratedAt || "",
          "gdi_pdf_report_executive_summary",
        ].join(",")
      );
    });
    var blob = new Blob([lines.join("\n")], { type: "text/csv;charset=utf-8" });
    var url = URL.createObjectURL(blob);
    var a = document.createElement("a");
    a.href = url;
    a.download = "gdi-admin-selected-hotels.csv";
    a.click();
    setStatus("Exported " + rows.length + " selected hotel row(s).");
  }

  function csvEsc(v) {
    var s = String(v == null ? "" : v);
    if (/[",\n]/.test(s)) return '"' + s.replace(/"/g, '""') + '"';
    return s;
  }

  async function bulkGenerateMissingPdfs() {
    if (bulkRunning) return;
    var ids = ctrl.selection.ids();
    var targets = catalog.filter(function (r) {
      return (
        ids.indexOf(String(r.hotelId)) >= 0 &&
        r.available &&
        effectivePdfStatus(r) === "MISSING"
      );
    });
    if (!targets.length) {
      setStatus(
        "No selected hotels need PDF generation (only MISSING + available rows are eligible)."
      );
      return;
    }
    var ok = window.confirm(
      "Generate GDI PDFs for " +
        targets.length +
        " selected hotel(s) with missing PDFs?\n\n" +
        "This uses the existing admin PDF generate endpoint. It does not run paid research or change publication.\n\n" +
        "Continue?"
    );
    if (!ok) return;
    bulkRunning = true;
    var okN = 0;
    var failN = 0;
    for (var i = 0; i < targets.length; i++) {
      setStatus(
        "Generating PDF " + (i + 1) + "/" + targets.length + ": " + targets[i].displayName
      );
      try {
        await generatePdf(targets[i].hotelId, { silentReload: true });
        if (rowBusy[targets[i].hotelId] === "FAILED") failN++;
        else okN++;
      } catch (_e) {
        failN++;
      }
    }
    bulkRunning = false;
    await loadCatalog();
    setStatus(
      "Bulk PDF complete: " + okN + " succeeded, " + failN + " failed. Select failed rows to retry."
    );
  }

  function unavailableCell(reason) {
    return (
      '<td class="ada-cell-num" title="' +
      T.esc(reason || "Canonical opportunity read failed. Retry after the source is available.") +
      '">Unavailable</td>'
    );
  }

  function clientInventoryCell(h) {
    if (h.readFailed || h.clientOpportunityCount == null) return unavailableCell(h.reason);
    var n = Number(h.clientOpportunityCount);
    var notes = "";
    if (n === 0) {
      notes += '<span class="ada-cell-note">No Published Opportunities</span>';
      notes +=
        '<span class="ada-cell-note">' +
        (h.gdiShareAvailable ? "Working empty share" : "Client link missing") +
        "</span>";
    }
    return '<td class="ada-cell-num">' + T.esc(n) + notes + "</td>";
  }

  function metricCell(h, value) {
    if (h.readFailed || value == null) return unavailableCell(h.reason);
    return T.numCell(value);
  }

  function watchCell(h) {
    if (h.readFailed || h.watchCount == null) return unavailableCell(h.reason);
    if (h.watchCapped && h.watchTotal != null) {
      return (
        '<td class="ada-cell-num" title="' +
        T.esc(
          "Report sample " +
            h.watchCount +
            " of " +
            h.watchTotal +
            ". Not the full Watch inventory."
        ) +
        '">' +
        T.esc(h.watchCount) +
        '<span class="ada-cell-note">sample of ' +
        T.esc(h.watchTotal) +
        "</span></td>"
      );
    }
    return T.numCell(h.watchCount);
  }

  function renderTable() {
    var body = $("gdiRptTableBody");
    var thead = $("gdiRptTableHead");
    if (!body) return;
    var view = ctrl.apply(catalog, filterFn);
    renderResultsBar(view);
    ctrl.paintSortHeaders(thead);

    var pageIds = view.rows.map(function (r) {
      return r.hotelId;
    });
    var allPageSelected =
      pageIds.length > 0 &&
      pageIds.every(function (id) {
        return ctrl.selection.has(id);
      });
    var someSelected = pageIds.some(function (id) {
      return ctrl.selection.has(id);
    });

    if (thead) {
      var checkTh = thead.querySelector(".ada-select-page");
      if (checkTh) {
        checkTh.checked = allPageSelected;
        checkTh.indeterminate = !allPageSelected && someSelected;
      }
    }

    if (!view.rows.length) {
      body.innerHTML =
        '<tr><td colspan="11">No GDI hotels match the current filters.</td></tr>';
      return;
    }

    body.innerHTML = view.rows
      .map(function (h) {
        var name = h.displayName || h.hotelName || h.hotelId;
        var st = effectivePdfStatus(h);
        return (
          '<tr data-hotel-id="' +
          T.esc(h.hotelId) +
          '"' +
          (ctrl.selection.has(h.hotelId) ? ' class="is-selected"' : "") +
          ">" +
          T.checkboxCell(h.hotelId, ctrl.selection.has(h.hotelId)) +
          T.hotelLinkCell(name, null, h.hotelId) +
          T.textCell(h.market || "—", "ada-cell-loc") +
          clientInventoryCell(h) +
          metricCell(h, h.readyCount) +
          metricCell(h, h.actionSetCount) +
          watchCell(h) +
          "<td>" +
          T.statusBadge(reportStatusKind(h.reportStatus), reportStatusLabel(h.reportStatus)) +
          "</td>" +
          "<td>" +
          T.statusBadge(
            st === "READY" ? "ok" : st === "FAILED" || st === "MISSING" ? "warn" : "warn",
            st
          ) +
          "</td>" +
          "<td>" +
          T.esc(T.fmtDateTime(h.lastGeneratedAt)) +
          "</td>" +
          '<td class="ada-col-actions">' +
          T.actionsHtml(buildActions(h), {
            hotel: h.hotelId,
            name: name,
          }) +
          "</td>" +
          "</tr>"
        );
      })
      .join("");
  }

  async function loadCatalog() {
    var res = await authFetch("/api/admin/group-demand-intelligence/reports");
    var json = await res.json();
    if (!res.ok || !json.ok) throw new Error((json && json.error) || "catalog_failed");
    catalog = (json.hotels || []).map(normalizeCatalogRow);
    counts = countsFromRows(catalog);
    renderCounts();
    renderTable();
  }

  async function fetchExternalLinks(hotelId) {
    var res = await authFetch(
      "/api/admin/ai-demand/hotels/" +
        encodeURIComponent(hotelId) +
        "/external-client-links"
    );
    var json = await res.json();
    if (!res.ok || !json.ok) {
      throw new Error((json && json.error) || "external_links_failed");
    }
    return json;
  }

  async function openOrCopyClient(hotelId, kind, mode) {
    setStatus("Loading " + kind.toUpperCase() + " client link…");
    try {
      var links = await fetchExternalLinks(hotelId);
      var pack = kind === "gdi" ? links.gdi : links.adp;
      if (!pack || !pack.available || !pack.url) {
        setStatus(
          (kind === "gdi" ? "GDI" : "ADP") +
            " client link unavailable" +
            (pack && pack.reason ? ": " + pack.reason : "")
        );
        return;
      }
      if (/localhost|127\.0\.0\.1/i.test(pack.url)) {
        setStatus(
          (kind === "gdi" ? "GDI" : "ADP") +
            " client URL is not an external production client link."
        );
        return;
      }
      if (mode === "open") {
        window.open(pack.url, "_blank", "noopener");
        setStatus("Opened " + (kind === "gdi" ? "GDI" : "ADP") + " client view.");
      } else {
        await copyText(pack.url);
        setStatus("Copied " + (kind === "gdi" ? "GDI" : "ADP") + " External Client URL.");
      }
    } catch (err) {
      setStatus("Client link failed: " + String(err.message || err));
    }
  }

  async function openPdf(hotelId, download) {
    var url =
      "/api/admin/group-demand-intelligence/hotels/" +
      encodeURIComponent(hotelId) +
      "/report-pdf" +
      (download ? "?download=1" : "?generate=1");
    setStatus(download ? "Downloading PDF…" : "Opening PDF…");
    try {
      var res = await authFetch(url);
      if (!res.ok) {
        var err = {};
        try {
          err = await res.json();
        } catch (_e) {}
        setStatus(
          "Could not open PDF: " + (err.reason || err.error || res.status)
        );
        return;
      }
      var blob = await res.blob();
      var filename =
        res.headers.get("X-GDI-PDF-Filename") || "Dealality_GDI_Report.pdf";
      var objectUrl = URL.createObjectURL(blob);
      if (download) {
        var a = document.createElement("a");
        a.href = objectUrl;
        a.download = filename;
        a.click();
        setStatus("Downloaded " + filename);
      } else {
        window.open(objectUrl, "_blank", "noopener");
        setStatus("Opened PDF.");
      }
      await loadCatalog();
    } catch (err) {
      setStatus("PDF failed: " + String(err.message || err));
    }
  }

  async function generatePdf(hotelId, opts) {
    opts = opts || {};
    if (rowBusy[hotelId] === "GENERATING") return;
    rowBusy[hotelId] = "GENERATING";
    delete rowErrors[hotelId];
    if (!opts.silentReload) renderTable();
    if (!opts.silentReload) setStatus("Generating GDI PDF… this may take a minute.");
    try {
      var res = await authFetch(
        "/api/admin/group-demand-intelligence/hotels/" +
          encodeURIComponent(hotelId) +
          "/report-pdf/generate",
        {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: "{}",
        }
      );
      var json = await res.json();
      if (!res.ok || !json.ok) {
        rowBusy[hotelId] = "FAILED";
        rowErrors[hotelId] = String(
          json.reason || json.message || json.error || res.status
        );
        if (!opts.silentReload) {
          setStatus("Generate failed: " + rowErrors[hotelId]);
          renderTable();
        }
        return;
      }
      delete rowBusy[hotelId];
      delete rowErrors[hotelId];
      if (!opts.silentReload) {
        setStatus(
          "Generated " +
            (json.filename || "PDF") +
            " (" +
            (json.byteLength || 0) +
            " bytes)."
        );
        await loadCatalog();
      }
    } catch (err) {
      rowBusy[hotelId] = "FAILED";
      rowErrors[hotelId] = String(err.message || err);
      if (!opts.silentReload) {
        setStatus("Generate failed: " + rowErrors[hotelId]);
        renderTable();
      }
    }
  }

  function openArchive(hotelId, hotelName) {
    try {
      sessionStorage.setItem(
        "dealality_report_archive_focus",
        JSON.stringify({
          hotelId: hotelId,
          reportType: "GDI",
          hotelName: hotelName || "",
          ts: Date.now(),
        })
      );
    } catch (_e) {}
    if (typeof window.__ADP_ADMIN_SET_TAB__ === "function") {
      window.__ADP_ADMIN_SET_TAB__("report-archive");
    }
    setStatus("Opening Report Archive for " + (hotelName || hotelId) + " (GDI).");
  }

  function onTableClick(ev) {
    var check = ev.target.closest("input.ada-row-check");
    if (check) {
      var id = check.getAttribute("data-id");
      if (check.checked) ctrl.selection.add(id);
      else ctrl.selection.remove(id);
      renderTable();
      return;
    }
    var btn = ev.target.closest("button[data-act]");
    if (!btn || btn.disabled) return;
    var act = btn.getAttribute("data-act");
    var wrap = btn.closest("[data-hotel]");
    var hotelId = wrap && wrap.getAttribute("data-hotel");
    var hotelName = wrap && wrap.getAttribute("data-name");
    if (!act || !hotelId || act === "noop") return;
    if (T && typeof T.closeMoreMenu === "function") T.closeMoreMenu();
    if (act === "view-pdf") openPdf(hotelId, false);
    else if (act === "download-pdf") openPdf(hotelId, true);
    else if (act === "generate" || act === "regenerate") generatePdf(hotelId);
    else if (act === "open-gdi") openOrCopyClient(hotelId, "gdi", "open");
    else if (act === "copy-gdi") openOrCopyClient(hotelId, "gdi", "copy");
    else if (act === "open-adp") openOrCopyClient(hotelId, "adp", "open");
    else if (act === "copy-adp") openOrCopyClient(hotelId, "adp", "copy");
    else if (act === "archive") openArchive(hotelId, hotelName || "");
  }

  function wire() {
    var thead = $("gdiRptTableHead");
    ctrl.wireSortHeaders(thead, renderTable);
    ctrl.paintSortHeaders(thead);

    if (thead) {
      thead.addEventListener("change", function (ev) {
        var sel = ev.target.closest(".ada-select-page");
        if (!sel) return;
        var view = ctrl.apply(catalog, filterFn);
        ctrl.selection.setPage(
          view.rows.map(function (r) {
            return r.hotelId;
          }),
          sel.checked
        );
        renderTable();
      });
    }

    ["gdiRptApplyFilters", "gdiRptSearch", "gdiRptFilterReport", "gdiRptFilterPdf", "gdiRptDateFrom", "gdiRptDateTo"].forEach(
      function (id) {
        var el = $(id);
        if (!el) return;
        if (id === "gdiRptApplyFilters") {
          el.addEventListener("click", renderTable);
        } else if (el.tagName === "INPUT" && el.type === "search") {
          el.addEventListener("keydown", function (ev) {
            if (ev.key === "Enter") {
              ev.preventDefault();
              renderTable();
            }
          });
          el.addEventListener("input", function () {
            /* live search optional — apply on idle */
          });
        } else {
          el.addEventListener("change", renderTable);
        }
      }
    );
    if ($("gdiRptTableBody")) {
      $("gdiRptTableBody").addEventListener("click", onTableClick);
    }
  }

  async function bootLoad() {
    try {
      await loadCatalog();
      setStatus("");
    } catch (err) {
      setStatus("Failed to load GDI report catalog: " + String(err.message || err));
      if ($("gdiRptTableBody")) {
        $("gdiRptTableBody").innerHTML =
          '<tr><td colspan="11">Failed to load catalog.</td></tr>';
      }
    }
  }

  function boot() {
    wire();
    window.addEventListener("dealality-adp-admin-ready", bootLoad);
    window.addEventListener("dealality-adp-admin-tab", function (ev) {
      if (ev.detail && ev.detail.tab === "gdi-reports") bootLoad();
    });
    if (window.__ADP_ADMIN_WORKSPACE_AUTH__) bootLoad();
  window.DealalityGdiAdminReports = {
    buildActions: buildActions,
    clientInventoryCell: clientInventoryCell,
    watchCell: watchCell,
  };
  }

  boot();
})();
