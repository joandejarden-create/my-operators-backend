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

  function renderRows(rows) {
    var body = $("rarTableBody");
    var counts = $("rarCounts");
    if (!body) return;
    if (!rows.length) {
      body.innerHTML = '<tr><td colspan="8">No archived reports match these filters.</td></tr>';
      if (counts) counts.textContent = "0 archived reports";
      return;
    }
    if (counts) counts.textContent = rows.length + " archived report" + (rows.length === 1 ? "" : "s");
    body.innerHTML = rows
      .map(function (e) {
        return (
          "<tr>" +
          "<td>" +
          esc(e.reportDate || "—") +
          "</td>" +
          "<td>" +
          esc(e.hotelName || e.hotelKey) +
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
          (e.hasPdf ? "YES" : "NO") +
          "</td>" +
          "<td>" +
          (e.hasSnapshot ? "YES" : "NO") +
          "</td>" +
          '<td style="white-space:nowrap">' +
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
          '">View Data</button>' +
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
    var res = await authFetch("/api/admin/report-archive?" + q.toString());
    var json = await res.json();
    if (!res.ok || !json.ok) {
      if ($("rarTableBody")) {
        $("rarTableBody").innerHTML =
          '<tr><td colspan="8">Failed to load archive' +
          (json.error ? ": " + esc(json.error) : "") +
          "</td></tr>";
      }
      return;
    }
    entries = json.entries || [];
    renderRows(entries);
  }

  async function openData(archiveId) {
    var res = await authFetch("/api/admin/report-archive/" + encodeURIComponent(archiveId));
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
    if (title) title.textContent = (json.meta && json.meta.archiveId) || archiveId;
    body.innerHTML =
      "<pre style=\"white-space:pre-wrap;font-size:12px;max-height:70vh;overflow:auto\">" +
      esc(JSON.stringify({ meta: json.meta, snapshot: json.snapshot }, null, 2)) +
      "</pre>";
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
    // Default Bethesda pilot hotel for convenience (unless a focus handoff is pending)
    if ($("rarHotel") && !$("rarHotel").value && !sessionStorage.getItem("dealality_report_archive_focus")) {
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
