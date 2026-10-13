/**
 * AI Demand Admin — GDI Reports tab (row-based UX, parity with AI Demand Reviews).
 * One hotel = one operating row for PDF + client links + archive.
 */
(function () {
  "use strict";

  var catalog = [];
  var counts = {};
  /** @type {Record<string, 'GENERATING'|'FAILED'>} */
  var rowBusy = {};
  /** @type {Record<string, string>} */
  var rowErrors = {};

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

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function setStatus(msg) {
    var el = $("gdiRptStatusLine");
    if (el) el.textContent = msg || "";
  }

  function fmtDateTime(iso) {
    if (!iso) return "—";
    var d = new Date(iso);
    if (Number.isNaN(d.getTime())) return String(iso);
    return d.toISOString().replace("T", " ").slice(0, 16) + " UTC";
  }

  function statusBadge(kind, label) {
    var cls = "adr-badge";
    if (kind === "ok") cls += " adr-badge--ok";
    if (kind === "warn") cls += " adr-badge--warn";
    if (kind === "danger") cls += " adr-badge--danger";
    return '<span class="' + cls + '">' + esc(label) + "</span>";
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
    if (navigator.clipboard && navigator.clipboard.writeText) {
      return navigator.clipboard.writeText(text);
    }
    return new Promise(function (resolve, reject) {
      try {
        var ta = document.createElement("textarea");
        ta.value = text;
        document.body.appendChild(ta);
        ta.select();
        document.execCommand("copy");
        ta.remove();
        resolve();
      } catch (e) {
        reject(e);
      }
    });
  }

  function effectivePdfStatus(row) {
    if (rowBusy[row.hotelId] === "GENERATING") return "GENERATING";
    if (rowBusy[row.hotelId] === "FAILED") return "FAILED";
    return row.pdfStatus || (row.pdfReady ? "READY" : "MISSING");
  }

  function sortRows(rows) {
    var rank = {
      READY: 0,
      NO_READY_OPPORTUNITIES: 2,
      WATCH_ONLY: 3,
      NEEDS_BUILD: 4,
      BLOCKED: 5,
    };
    return rows.slice().sort(function (a, b) {
      var ar = rank[a.reportStatus] != null ? rank[a.reportStatus] : 9;
      var br = rank[b.reportStatus] != null ? rank[b.reportStatus] : 9;
      // Needs PDF among READY hotels first
      if (ar === 0 && br === 0) {
        var an = a.pdfReady ? 1 : 0;
        var bn = b.pdfReady ? 1 : 0;
        if (an !== bn) return an - bn;
      }
      if (ar !== br) return ar - br;
      return String(a.displayName || "").localeCompare(String(b.displayName || ""));
    });
  }

  function filteredRows() {
    var q = ($("gdiRptSearch") && $("gdiRptSearch").value.trim().toLowerCase()) || "";
    var reportF = ($("gdiRptFilterReport") && $("gdiRptFilterReport").value) || "";
    var pdfF = ($("gdiRptFilterPdf") && $("gdiRptFilterPdf").value) || "";
    var rows = sortRows(catalog);
    if (q) {
      rows = rows.filter(function (r) {
        var hay = (
          (r.displayName || "") +
          " " +
          (r.hotelName || "") +
          " " +
          (r.market || "")
        ).toLowerCase();
        return hay.indexOf(q) !== -1;
      });
    }
    if (reportF) {
      rows = rows.filter(function (r) {
        return r.reportStatus === reportF;
      });
    }
    if (pdfF) {
      rows = rows.filter(function (r) {
        return effectivePdfStatus(r) === pdfF;
      });
    }
    return rows;
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
          esc(it[0]) +
          "</span></div>"
        );
      })
      .join("");
  }

  function pdfCell(row) {
    var st = effectivePdfStatus(row);
    var id = esc(row.hotelId);
    if (st === "GENERATING") {
      return statusBadge("warn", "GENERATING");
    }
    if (st === "FAILED") {
      return (
        statusBadge("danger", "FAILED") +
        ' <button type="button" class="adr-btn adr-btn--tiny" data-act="generate" data-hotel="' +
        id +
        '">Retry</button>' +
        (rowErrors[row.hotelId]
          ? '<div class="adr-subtitle" title="' +
            esc(rowErrors[row.hotelId]) +
            '">' +
            esc(rowErrors[row.hotelId].slice(0, 80)) +
            "</div>"
          : "")
      );
    }
    if (st === "READY" || row.pdfReady) {
      return (
        '<button type="button" class="adr-btn adr-btn--tiny" data-act="view-pdf" data-hotel="' +
        id +
        '">View PDF</button>' +
        '<button type="button" class="adr-btn adr-btn--tiny" data-act="download-pdf" data-hotel="' +
        id +
        '">Download</button>'
      );
    }
    if (!row.available) {
      return statusBadge("danger", "—");
    }
    return (
      statusBadge("warn", "MISSING") +
      ' <button type="button" class="adr-btn adr-btn--tiny" data-act="generate" data-hotel="' +
      id +
      '">Generate PDF</button>'
    );
  }

  function clientCell(kind, row) {
    var available =
      kind === "gdi"
        ? !!(row.gdiClient && row.gdiClient.available) || !!row.gdiShareAvailable
        : !!(row.adpClient && row.adpClient.available) || !!row.adpShareAvailable;
    var id = esc(row.hotelId);
    if (!available) {
      return '<span class="adr-subtitle">—</span>';
    }
    return (
      '<button type="button" class="adr-btn adr-btn--tiny" data-act="open-' +
      kind +
      '" data-hotel="' +
      id +
      '">Open</button>' +
      '<button type="button" class="adr-btn adr-btn--tiny" data-act="copy-' +
      kind +
      '" data-hotel="' +
      id +
      '">Copy URL</button>' +
      '<span class="adr-subtitle" data-copy-toast hidden style="margin-left:6px;color:#276749">Copied</span>'
    );
  }

  function showCopiedToast(btn) {
    var cell = btn && btn.closest ? btn.closest("td") : null;
    var toast = cell ? cell.querySelector("[data-copy-toast]") : null;
    if (!toast) return;
    toast.hidden = false;
    toast.textContent = "Copied";
    clearTimeout(toast.__hideTimer);
    toast.__hideTimer = setTimeout(function () {
      toast.hidden = true;
    }, 1600);
  }

  function normalizeCatalogRow(h) {
    if (!h || typeof h !== "object") return h;
    var ready = Number(h.readyCount != null ? h.readyCount : h.ready) || 0;
    var actionSet = Number(h.actionSetCount != null ? h.actionSetCount : h.actionSet) || 0;
    var watch = Number(h.watchCount != null ? h.watchCount : h.watch) || 0;
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
      ready: ready,
      actionSet: actionSet,
      watch: watch,
      readyCount: ready,
      actionSetCount: actionSet,
      watchCount: watch,
      reportStatus: reportStatus,
      pdfReady: pdfReady,
      pdfStatus: h.pdfStatus || (h.pdf && h.pdf.status) || (pdfReady ? "READY" : "MISSING"),
      lastGeneratedAt: lastGeneratedAt,
      gdiShareAvailable: gdiAvail,
      adpShareAvailable: adpAvail,
      gdiClient: { available: gdiAvail },
      adpClient: { available: adpAvail },
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

  function moreCell(row) {
    var id = esc(row.hotelId);
    var html = "";
    var gdiAvail =
      !!(row.gdiClient && row.gdiClient.available) || !!row.gdiShareAvailable;
    if (gdiAvail) {
      html +=
        '<button type="button" class="adr-btn adr-btn--tiny" data-act="open-gdi" data-hotel="' +
        id +
        '">Open GDI</button>';
    }
    if (row.available) {
      html +=
        '<button type="button" class="adr-btn adr-btn--tiny" data-act="regenerate" data-hotel="' +
        id +
        '"' +
        (rowBusy[row.hotelId] === "GENERATING" ? " disabled" : "") +
        ">Regenerate</button>";
    }
    html +=
      '<button type="button" class="adr-btn adr-btn--tiny" data-act="archive" data-hotel="' +
      id +
      '" data-name="' +
      esc(row.displayName || row.hotelName || "") +
      '">Archive</button>';
    return html || "—";
  }

  function renderTable() {
    var body = $("gdiRptTableBody");
    if (!body) return;
    var rows = filteredRows();
    if (!rows.length) {
      body.innerHTML =
        '<tr><td colspan="10">No GDI hotels match the current filters.</td></tr>';
      return;
    }
    body.innerHTML = rows
      .map(function (h) {
        var name = h.displayName || h.hotelName || h.hotelId;
        var market = h.market ? '<div class="adr-subtitle">' + esc(h.market) + "</div>" : "";
        return (
          '<tr data-hotel-id="' +
          esc(h.hotelId) +
          '">' +
          "<td><strong>" +
          esc(name) +
          "</strong>" +
          market +
          "</td>" +
          "<td>" +
          (h.available ? h.ready || 0 : "—") +
          "</td>" +
          "<td>" +
          (h.available ? h.actionSet || 0 : "—") +
          "</td>" +
          "<td>" +
          (h.available ? h.watch || 0 : "—") +
          "</td>" +
          "<td>" +
          statusBadge(reportStatusKind(h.reportStatus), reportStatusLabel(h.reportStatus)) +
          "</td>" +
          '<td class="adr-col-pdf" style="white-space:nowrap">' +
          pdfCell(h) +
          "</td>" +
          '<td style="white-space:nowrap">' +
          clientCell("gdi", h) +
          "</td>" +
          '<td style="white-space:nowrap">' +
          clientCell("adp", h) +
          "</td>" +
          "<td>" +
          esc(fmtDateTime(h.lastGeneratedAt)) +
          "</td>" +
          '<td class="adr-actions" style="white-space:nowrap">' +
          moreCell(h) +
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
    // Law: cards reconcile from the same normalized rows as the table.
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

  async function openOrCopyClient(hotelId, kind, mode, sourceBtn) {
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
        setStatus(
          "Opened " +
            (kind === "gdi" ? "GDI" : "ADP") +
            " client view." +
            (pack.servedFromProductionHost
              ? " (production host — stable Bethesda contract token)"
              : "")
        );
      } else {
        await copyText(pack.url);
        showCopiedToast(sourceBtn);
        setStatus(
          "Copied " +
            (kind === "gdi" ? "GDI" : "ADP") +
            " External Client URL." +
            (pack.servedFromProductionHost
              ? " (production host — stable Bethesda contract token)"
              : "")
        );
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

  async function generatePdf(hotelId) {
    if (rowBusy[hotelId] === "GENERATING") return;
    rowBusy[hotelId] = "GENERATING";
    delete rowErrors[hotelId];
    renderTable();
    setStatus("Generating GDI PDF… this may take a minute.");
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
        setStatus("Generate failed: " + rowErrors[hotelId]);
        renderTable();
        return;
      }
      delete rowBusy[hotelId];
      delete rowErrors[hotelId];
      setStatus(
        "Generated " +
          (json.filename || "PDF") +
          " (" +
          (json.byteLength || 0) +
          " bytes" +
          (json.pageCountHint != null ? ", ~" + json.pageCountHint + " pages" : "") +
          ")."
      );
      await loadCatalog();
    } catch (err) {
      rowBusy[hotelId] = "FAILED";
      rowErrors[hotelId] = String(err.message || err);
      setStatus("Generate failed: " + rowErrors[hotelId]);
      renderTable();
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
    } else {
      var btn = document.getElementById("adaTabReportArchive");
      if (btn) btn.click();
    }
    setStatus("Opening Report Archive for " + (hotelName || hotelId) + " (GDI).");
  }

  function onTableClick(ev) {
    var btn = ev.target.closest("button[data-act]");
    if (!btn) return;
    var act = btn.getAttribute("data-act");
    var hotelId = btn.getAttribute("data-hotel");
    if (!act || !hotelId) return;
    if (act === "view-pdf") openPdf(hotelId, false);
    else if (act === "download-pdf") openPdf(hotelId, true);
    else if (act === "generate" || act === "regenerate") generatePdf(hotelId);
    else if (act === "open-gdi") openOrCopyClient(hotelId, "gdi", "open", btn);
    else if (act === "copy-gdi") openOrCopyClient(hotelId, "gdi", "copy", btn);
    else if (act === "open-adp") openOrCopyClient(hotelId, "adp", "open", btn);
    else if (act === "copy-adp") openOrCopyClient(hotelId, "adp", "copy", btn);
    else if (act === "archive") {
      openArchive(hotelId, btn.getAttribute("data-name") || "");
    }
  }

  function wire() {
    if ($("gdiRptApplyFilters")) {
      $("gdiRptApplyFilters").addEventListener("click", function () {
        renderTable();
      });
    }
    if ($("gdiRptSearch")) {
      $("gdiRptSearch").addEventListener("keydown", function (ev) {
        if (ev.key === "Enter") {
          ev.preventDefault();
          renderTable();
        }
      });
    }
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
          '<tr><td colspan="10">Failed to load catalog.</td></tr>';
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
  }

  boot();
})();
