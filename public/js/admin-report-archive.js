/**
 * AI Demand Admin — Report Archive tab
 */
(function () {
  "use strict";

  var entries = [];

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

  function emptyMessage() {
    var hotel = ($("rarHotel") && $("rarHotel").value.trim()) || "";
    if (hotel) return "No archived reports yet.";
    return "No archived reports match these filters.";
  }

  function renderRows(rows) {
    var body = $("rarTableBody");
    var counts = $("rarCounts");
    if (!body) return;
    if (!rows.length) {
      body.innerHTML =
        '<tr><td colspan="8" class="rar-empty">' + esc(emptyMessage()) + "</td></tr>";
      if (counts) counts.textContent = "0 archived reports";
      return;
    }
    if (counts) {
      counts.textContent =
        rows.length + " archived report" + (rows.length === 1 ? "" : "s");
    }
    body.innerHTML = rows
      .map(function (e) {
        var pdfCell = e.hasPdf
          ? "YES"
          : e.pdfMissingReason
            ? '<span title="' +
              esc(e.pdfMissingReason) +
              '">NO <small>(not previously archived)</small></span>'
            : "NO";
        var actions =
          (e.hasPdf
            ? '<button type="button" class="adr-btn adr-btn--secondary rar-view" data-id="' +
              esc(e.archiveId) +
              '">View PDF</button> ' +
              '<button type="button" class="adr-btn adr-btn--secondary rar-dl" data-id="' +
              esc(e.archiveId) +
              '">Download</button> '
            : "") +
          '<button type="button" class="adr-btn adr-btn--secondary rar-data" data-id="' +
          esc(e.archiveId) +
          '">View Data</button>';
        return (
          "<tr>" +
          "<td>" +
          esc(e.reportDate || "—") +
          "</td>" +
          "<td>" +
          esc(e.hotelName || e.hotelId || "—") +
          "</td>" +
          "<td>" +
          esc(e.reportType) +
          "</td>" +
          "<td>" +
          esc(e.reportClass) +
          "</td>" +
          "<td>" +
          esc(e.version) +
          "</td>" +
          "<td>" +
          pdfCell +
          "</td>" +
          "<td>" +
          (e.hasSnapshot ? "YES" : "NO") +
          "</td>" +
          '<td style="white-space:nowrap">' +
          actions +
          "</td>" +
          "</tr>"
        );
      })
      .join("");
  }

  async function loadCatalog() {
    var hotel = ($("rarHotel") && $("rarHotel").value.trim()) || "";
    var type = ($("rarType") && $("rarType").value) || "ALL";
    var cls = ($("rarClass") && $("rarClass").value) || "ALL";
    var q = new URLSearchParams();
    if (hotel) q.set("hotelId", hotel);
    if (type) q.set("reportType", type);
    if (cls) q.set("reportClass", cls);
    if ($("rarTableBody")) {
      $("rarTableBody").innerHTML =
        '<tr><td colspan="8">Loading…</td></tr>';
    }
    var res = await authFetch("/api/admin/report-archive?" + q.toString());
    var json = await res.json();
    if (!res.ok || !json.ok) {
      if ($("rarTableBody")) {
        $("rarTableBody").innerHTML =
          '<tr><td colspan="8">Failed to load archive' +
          (json.error ? ": " + esc(json.error) : "") +
          ". Retry or check Admin auth.</td></tr>";
      }
      if ($("rarCounts")) $("rarCounts").textContent = "Error loading archive";
      return;
    }
    entries = json.entries || [];
    renderRows(entries);
  }

  function detailRow(label, value) {
    if (value == null || value === "") return "";
    return (
      "<div class=\"rar-detail-row\"><dt>" +
      esc(label) +
      "</dt><dd>" +
      esc(String(value)) +
      "</dd></div>"
    );
  }

  async function openData(archiveId) {
    var res = await authFetch(
      "/api/admin/report-archive/" + encodeURIComponent(archiveId)
    );
    var json = await res.json();
    var drawer = $("rarDrawer");
    var body = $("rarDrawerBody");
    var title = $("rarDrawerTitle");
    if (!drawer || !body) return;
    if (!res.ok || !json.ok) {
      body.textContent = "Failed to load archive entry.";
      drawer.hidden = false;
      return;
    }
    var m = json.meta || {};
    if (title) {
      title.textContent =
        (m.hotelName || m.hotelId || "Hotel") +
        " · " +
        (m.reportType || "") +
        " · " +
        (m.reportDate || "");
    }
    var summary =
      '<dl class="rar-detail">' +
      detailRow("Hotel", m.hotelName || m.hotelId) +
      detailRow("Report Type", m.reportType) +
      detailRow("Report Date", m.reportDate) +
      detailRow("Version", m.version) +
      detailRow("Report Class", m.reportClass) +
      detailRow("Snapshot Date", m.snapshotDate) +
      detailRow("Generated At", m.generatedAt) +
      detailRow("Archived At", m.archivedAt) +
      detailRow("Status", m.immutable ? "IMMUTABLE" : "—") +
      detailRow("Methodology Version", m.methodologyVersion) +
      detailRow("Query Set", m.querySetId) +
      detailRow("Baseline ID", m.baselineId) +
      detailRow("PDF", json.hasPdf ? "Archived" : m.pdfMissingReason || "Not archived") +
      detailRow("Data Snapshot", json.snapshot ? "Archived" : "Missing") +
      detailRow(
        "PDF Checksum OK",
        json.pdfChecksumOk == null ? "n/a" : json.pdfChecksumOk ? "YES" : "NO"
      ) +
      detailRow(
        "Snapshot Checksum OK",
        json.snapshotChecksumOk == null
          ? "n/a"
          : json.snapshotChecksumOk
            ? "YES"
            : "NO"
      ) +
      detailRow("PDF Checksum", m.pdfChecksumSha256) +
      detailRow("Snapshot Checksum", m.snapshotChecksumSha256) +
      detailRow("Code SHA", m.codeSha) +
      detailRow("External Share Token", m.externalShareTokenId ? "(present)" : null) +
      detailRow("Notes", m.notes) +
      "</dl>";
    var snapPreview =
      '<details open><summary>Archived data snapshot</summary>' +
      '<pre style="white-space:pre-wrap;font-size:12px;max-height:50vh;overflow:auto">' +
      esc(JSON.stringify(json.snapshot, null, 2)) +
      "</pre></details>";
    body.innerHTML = summary + snapPreview;
    drawer.hidden = false;
  }

  function openPdf(archiveId, download) {
    var url =
      "/api/admin/report-archive/" +
      encodeURIComponent(archiveId) +
      "/pdf" +
      (download ? "?download=1" : "");
    window.open(url, "_blank", "noopener");
  }

  /**
   * Apply focus handed off from GDI Reports (or other tabs):
   * sessionStorage dealality_report_archive_focus = { hotelId, reportType, hotelName }
   */
  function applyArchiveFocus() {
    try {
      var raw = sessionStorage.getItem("dealality_report_archive_focus");
      if (!raw) return false;
      sessionStorage.removeItem("dealality_report_archive_focus");
      var focus = JSON.parse(raw);
      if (!focus || !focus.hotelId) return false;
      if ($("rarHotel")) $("rarHotel").value = String(focus.hotelId);
      if ($("rarType") && focus.reportType) {
        $("rarType").value = String(focus.reportType);
      }
      return true;
    } catch (_e) {
      return false;
    }
  }

  function setTypeChip(type) {
    if ($("rarType")) $("rarType").value = type || "ALL";
    var chips = document.querySelectorAll("[data-rar-type-chip]");
    chips.forEach(function (btn) {
      var t = btn.getAttribute("data-rar-type-chip");
      btn.classList.toggle("active", t === (type || "ALL"));
      btn.setAttribute("aria-pressed", t === (type || "ALL") ? "true" : "false");
    });
    loadCatalog();
  }

  function wire() {
    if ($("rarApply")) $("rarApply").addEventListener("click", loadCatalog);
    if ($("rarDrawerClose")) {
      $("rarDrawerClose").addEventListener("click", function () {
        if ($("rarDrawer")) $("rarDrawer").hidden = true;
      });
    }
    if ($("rarTableBody")) {
      $("rarTableBody").addEventListener("click", function (ev) {
        var btn = ev.target.closest("button[data-id]");
        if (!btn) return;
        var id = btn.getAttribute("data-id");
        if (btn.classList.contains("rar-view")) openPdf(id, false);
        else if (btn.classList.contains("rar-dl")) openPdf(id, true);
        else if (btn.classList.contains("rar-data")) openData(id);
      });
    }
    document.querySelectorAll("[data-rar-type-chip]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        setTypeChip(btn.getAttribute("data-rar-type-chip"));
      });
    });
    // Default Bethesda pilot hotel for convenience (unless a focus handoff is pending)
    if (
      $("rarHotel") &&
      !$("rarHotel").value &&
      !sessionStorage.getItem("dealality_report_archive_focus")
    ) {
      $("rarHotel").value = "recLuxvwwxID7U2B8";
    }
  }

  function onReady() {
    if (!window.__ADP_ADMIN_WORKSPACE_AUTH__) return;
    wire();
    applyArchiveFocus();
    loadCatalog();
  }

  window.addEventListener("dealality-adp-admin-ready", onReady);
  window.addEventListener("dealality-adp-admin-tab", function (ev) {
    if (ev.detail && ev.detail.tab === "report-archive") {
      applyArchiveFocus();
      loadCatalog();
    }
  });
  if (window.__ADP_ADMIN_WORKSPACE_AUTH__) onReady();
})();
