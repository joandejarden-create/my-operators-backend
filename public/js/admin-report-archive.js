/**
 * AI Demand Admin — Report Archive tab (shared table pattern).
 * Rows are report versions, not unique hotels.
 */
(function () {
  "use strict";

  var T = window.DealalityAiAdminTable;
  var entries = [];
  var ctrl = T
    ? T.createTableController({
        defaultSortKey: "archivedAt",
        defaultSortDir: -1,
        idKey: "archiveId",
        tieBreak: "archiveId",
        sortTypes: {
          archivedAt: "date",
          hotelName: "string",
          location: "string",
          reportType: "string",
          reportClass: "string",
          versionLabel: "string",
          pdfStatus: "string",
        },
        onResetFilters: function () {
          if ($("rarHotel")) $("rarHotel").value = "";
          if ($("rarType")) $("rarType").value = "ALL";
          if ($("rarClass")) $("rarClass").value = "ALL";
          if ($("rarDateFrom")) $("rarDateFrom").value = "";
          if ($("rarDateTo")) $("rarDateTo").value = "";
          document.querySelectorAll("[data-rar-type-chip]").forEach(function (b) {
            var on = b.getAttribute("data-rar-type-chip") === "ALL";
            b.classList.toggle("active", on);
            b.setAttribute("aria-pressed", on ? "true" : "false");
          });
        },
      })
    : null;

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
    return T ? T.esc(s) : String(s == null ? "" : s);
  }

  function emptyMessage() {
    var hotel = ($("rarHotel") && $("rarHotel").value.trim()) || "";
    if (hotel) return "No archived reports yet.";
    return "No archived reports match these filters.";
  }

  function normalize(e) {
    return Object.assign({}, e, {
      archivedAt: e.archivedAt || e.reportDate || e.createdAt || null,
      hotelName: e.hotelName || e.displayName || e.hotelId || "",
      location: e.location || e.market || e.city || "",
      versionLabel: e.version || e.versionLabel || "",
      pdfStatus: e.hasPdf ? "READY" : "MISSING",
    });
  }

  function filterFn(e) {
    var q = ($("rarHotel") && $("rarHotel").value.trim().toLowerCase()) || "";
    var type = ($("rarType") && $("rarType").value) || "ALL";
    var cls = ($("rarClass") && $("rarClass").value) || "ALL";
    var from = ($("rarDateFrom") && $("rarDateFrom").value) || "";
    var to = ($("rarDateTo") && $("rarDateTo").value) || "";
    if (q) {
      var hay = (
        (e.hotelName || "") +
        " " +
        (e.hotelId || "") +
        " " +
        (e.propertyId || "") +
        " " +
        (e.location || "")
      ).toLowerCase();
      if (hay.indexOf(q) === -1) return false;
    }
    if (type && type !== "ALL" && e.reportType !== type) return false;
    if (cls && cls !== "ALL" && e.reportClass !== cls) return false;
    if (from || to) {
      var ts = e.archivedAt ? new Date(e.archivedAt).getTime() : null;
      if (ts == null || Number.isNaN(ts)) return false;
      if (from && ts < new Date(from + "T00:00:00.000Z").getTime()) return false;
      if (to && ts > new Date(to + "T23:59:59.999Z").getTime()) return false;
    }
    return true;
  }

  function renderRows() {
    var body = $("rarTableBody");
    var counts = $("rarCounts");
    var results = $("rarResultsRow");
    var thead = $("rarTableHead");
    if (!body) return;

    var view = ctrl
      ? ctrl.apply(entries, filterFn)
      : { rows: entries.filter(filterFn), total: entries.length, showing: 0 };
    if (!ctrl) view.showing = view.rows.length;

    if (ctrl) ctrl.paintSortHeaders(thead);
    if (results && T) {
      results.innerHTML =
        T.resultsCountHtml(view.showing, view.total, "report versions") +
        '<div class="ada-bulk-wrap"></div>';
    }
    if (counts) {
      counts.innerHTML =
        '<div class="adr-count"><span class="adr-count__n">' +
        view.total +
        '</span><span class="adr-count__l">Archived versions</span></div>';
    }

    if (!view.rows.length) {
      body.innerHTML =
        '<tr><td colspan="9" class="rar-empty">' + esc(emptyMessage()) + "</td></tr>";
      return;
    }

    body.innerHTML = view.rows
      .map(function (e) {
        var actions = T
          ? T.actionsHtml(
              {
                primary: e.hasPdf
                  ? [{ label: "View PDF", act: "view-pdf", title: "View archived PDF" }]
                  : [
                      {
                        label: "PDF missing",
                        act: "noop",
                        disabled: true,
                        title: e.pdfMissingReason || "PDF was not archived",
                      },
                    ],
                more: [
                  e.hasPdf
                    ? { label: "Download PDF", act: "download-pdf" }
                    : null,
                  { label: "View data", act: "view-data" },
                ].filter(Boolean),
              },
              { id: e.archiveId }
            )
          : "";
        return (
          "<tr>" +
          "<td>" +
          esc(T ? T.fmtDate(e.archivedAt) : e.reportDate || "—") +
          "</td>" +
          '<td class="ada-cell-hotel"><strong>' +
          esc(e.hotelName || "—") +
          "</strong>" +
          (e.hotelId
            ? '<div class="ada-subtitle">' + esc(e.hotelId) + "</div>"
            : "") +
          "</td>" +
          '<td class="ada-cell-loc">' +
          esc(e.location || "—") +
          "</td>" +
          "<td>" +
          esc(e.reportType) +
          "</td>" +
          "<td>" +
          esc(e.reportClass) +
          "</td>" +
          "<td>" +
          esc(e.versionLabel) +
          "</td>" +
          "<td>" +
          (T
            ? T.statusBadge(e.hasPdf ? "ok" : "warn", e.pdfStatus)
            : e.hasPdf
              ? "YES"
              : "NO") +
          "</td>" +
          "<td>" +
          (e.hasSnapshot ? "YES" : "NO") +
          "</td>" +
          '<td class="ada-col-actions">' +
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
        '<tr><td colspan="9">Loading…</td></tr>';
    }
    var res = await authFetch("/api/admin/report-archive?" + q.toString());
    var json = await res.json();
    if (!res.ok || !json.ok) {
      if ($("rarTableBody")) {
        $("rarTableBody").innerHTML =
          '<tr><td colspan="9">Failed to load archive' +
          (json.error ? ": " + esc(json.error) : "") +
          "</td></tr>";
      }
      return;
    }
    entries = (json.entries || json.items || []).map(normalize);
    renderRows();
  }

  async function openPdf(archiveId, download) {
    var res = await authFetch(
      "/api/admin/report-archive/" +
        encodeURIComponent(archiveId) +
        "/pdf" +
        (download ? "?download=1" : "")
    );
    if (!res.ok) {
      alert("PDF unavailable for this archive entry.");
      return;
    }
    var blob = await res.blob();
    var url = URL.createObjectURL(blob);
    if (download) {
      var a = document.createElement("a");
      a.href = url;
      a.download = "Dealality_Archive.pdf";
      a.click();
    } else {
      window.open(url, "_blank", "noopener");
    }
  }

  async function openData(archiveId) {
    var res = await authFetch(
      "/api/admin/report-archive/" + encodeURIComponent(archiveId)
    );
    var json = await res.json();
    if (!res.ok || !json.ok) {
      alert("Could not load archive data.");
      return;
    }
    var drawer = $("rarDrawer");
    var body = $("rarDrawerBody");
    var title = $("rarDrawerTitle");
    if (title) title.textContent = "Archive " + archiveId;
    if (body) {
      body.innerHTML =
        "<pre style=\"white-space:pre-wrap;font-size:12px\">" +
        esc(JSON.stringify(json.entry || json, null, 2)) +
        "</pre>";
    }
    if (drawer) drawer.hidden = false;
  }

  function applyFocusFromHandoff() {
    try {
      var raw = sessionStorage.getItem("dealality_report_archive_focus");
      if (!raw) return;
      sessionStorage.removeItem("dealality_report_archive_focus");
      var focus = JSON.parse(raw);
      if (focus.hotelId && $("rarHotel")) $("rarHotel").value = focus.hotelId;
      if (focus.reportType && $("rarType")) $("rarType").value = focus.reportType;
      document.querySelectorAll("[data-rar-type-chip]").forEach(function (b) {
        var t = b.getAttribute("data-rar-type-chip");
        var on = t === (focus.reportType || "ALL") || (t === "ALL" && !focus.reportType);
        b.classList.toggle("active", on);
        b.setAttribute("aria-pressed", on ? "true" : "false");
      });
    } catch (_e) {}
  }

  function wire() {
    if ($("rarApply")) $("rarApply").addEventListener("click", function () {
      loadCatalog();
    });
    if ($("rarResetView") && ctrl) {
      $("rarResetView").addEventListener("click", function () {
        ctrl.resetView();
        loadCatalog();
      });
    }
    if ($("rarDrawerClose")) {
      $("rarDrawerClose").addEventListener("click", function () {
        if ($("rarDrawer")) $("rarDrawer").hidden = true;
      });
    }
    document.querySelectorAll("[data-rar-type-chip]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        var t = btn.getAttribute("data-rar-type-chip");
        document.querySelectorAll("[data-rar-type-chip]").forEach(function (b) {
          var on = b === btn;
          b.classList.toggle("active", on);
          b.setAttribute("aria-pressed", on ? "true" : "false");
        });
        if ($("rarType")) $("rarType").value = t || "ALL";
        loadCatalog();
      });
    });
    if (ctrl && $("rarTableHead")) {
      ctrl.wireSortHeaders($("rarTableHead"), renderRows);
      ctrl.paintSortHeaders($("rarTableHead"));
    }
    if ($("rarTableBody")) {
      $("rarTableBody").addEventListener("click", function (ev) {
        var btn = ev.target.closest("button[data-act]");
        if (!btn || btn.disabled) return;
        var wrap = btn.closest("[data-id]");
        var id = (wrap && wrap.getAttribute("data-id")) || btn.getAttribute("data-id");
        var act = btn.getAttribute("data-act");
        var more = btn.closest("details.ada-more");
        if (more) more.open = false;
        if (act === "view-pdf") openPdf(id, false);
        else if (act === "download-pdf") openPdf(id, true);
        else if (act === "view-data") openData(id);
      });
    }
  }

  function onReady() {
    applyFocusFromHandoff();
    wire();
    loadCatalog();
  }

  window.addEventListener("dealality-adp-admin-ready", onReady);
  window.addEventListener("dealality-adp-admin-tab", function (ev) {
    if (ev.detail && ev.detail.tab === "report-archive") {
      applyFocusFromHandoff();
      loadCatalog();
    }
  });
  if (window.__ADP_ADMIN_WORKSPACE_AUTH__) onReady();
})();
