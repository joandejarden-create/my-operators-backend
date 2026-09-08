/**
 * Admin — AI Demand Leak Audit report picker + shared client preview.
 */
(function () {
  "use strict";

  var API = "/api/admin/adp-leak-audits";
  var catalog = [];
  var selected = null;
  var promoteConfirmationText = "";
  var loadedCatalogId = null;

  function authFetch(url, opts) {
    opts = opts || {};
    var auth = window.DealalityMemberstackAuth;
    if (auth && typeof auth.authFetch === "function") {
      return auth.authFetch(url, opts);
    }
    return fetch(url, Object.assign({ credentials: "same-origin" }, opts));
  }

  function showDenied(message) {
    var loadingEl = document.getElementById("supportGateLoading");
    var deniedEl = document.getElementById("supportGateDenied");
    var contentEl = document.getElementById("supportGateContent");
    if (window.SupportAdminGate) {
      window.SupportAdminGate.hidePageLoading(loadingEl);
    } else if (loadingEl) {
      loadingEl.hidden = true;
    }
    if (contentEl) contentEl.hidden = true;
    if (deniedEl) {
      deniedEl.hidden = false;
      var msg = deniedEl.querySelector("[data-gate-message]");
      if (msg && message) msg.textContent = message;
    }
  }

  function setStatus(text) {
    var el = document.getElementById("alaPickerStatus");
    if (el) el.textContent = text || "";
  }

  function absoluteUrl(path) {
    try {
      return new URL(path, window.location.origin).href;
    } catch (_err) {
      return path;
    }
  }

  function findSelected() {
    var id = document.getElementById("alaReportSelect").value;
    if (!id) return null;
    for (var i = 0; i < catalog.length; i++) {
      if (catalog[i].catalogId === id) return catalog[i];
    }
    return null;
  }

  function syncActionButtons() {
    selected = findSelected();
    var has = Boolean(selected && selected.previewUrl);
    document.getElementById("alaOpenWeb").disabled = !has;
    document.getElementById("alaDownloadPdf").disabled = !has;
    document.getElementById("alaCopyShare").disabled = !has;
    document.getElementById("alaMarkSent").disabled = !(selected && selected.canMarkSent && selected.requestId);
    document.getElementById("alaPromote").disabled = !(selected && selected.canPromote && selected.requestId);
  }

  function fillSelect(rows) {
    var sel = document.getElementById("alaReportSelect");
    var prev = sel.value;
    sel.innerHTML = '<option value="">Select a report…</option>';
    rows.forEach(function (row) {
      var opt = document.createElement("option");
      opt.value = row.catalogId;
      opt.textContent = row.label;
      sel.appendChild(opt);
    });
    if (prev && rows.some(function (r) { return r.catalogId === prev; })) {
      sel.value = prev;
    }
    syncActionButtons();
  }

  async function loadCatalog() {
    var qs = new URLSearchParams();
    var reportType = document.getElementById("alaFilterReportType").value;
    var status = document.getElementById("alaFilterStatus").value;
    var source = document.getElementById("alaFilterSource").value;
    if (reportType) qs.set("reportType", reportType);
    if (status) qs.set("status", status);
    if (source) qs.set("source", source);
    var res = await authFetch(API + "/report-catalog?" + qs.toString());
    var data = await res.json();
    if (!res.ok || !data.ok) {
      throw new Error((data && data.message) || "Failed to load report catalog");
    }
    if (data.promoteConfirmationText) {
      promoteConfirmationText = data.promoteConfirmationText;
    }
    catalog = data.reports || [];
    fillSelect(catalog);
    setStatus(catalog.length ? catalog.length + " report(s) available" : "No reports match these filters");
  }

  function loadPreview() {
    selected = findSelected();
    if (!selected || !selected.previewUrl) {
      setStatus("Select a report first.");
      return;
    }
    var empty = document.getElementById("alaPreviewEmpty");
    var frame = document.getElementById("alaPreviewFrame");
    empty.hidden = true;
    frame.hidden = false;
    frame.src = selected.previewUrl;
    loadedCatalogId = selected.catalogId;
    setStatus("Loaded: " + selected.label);
    syncActionButtons();
  }

  function openWebReport() {
    selected = findSelected();
    if (!selected || !selected.previewUrl) return;
    window.open(selected.previewUrl, "_blank", "noopener");
  }

  function downloadPdf() {
    selected = findSelected();
    if (!selected || !selected.previewUrl) return;
    var frame = document.getElementById("alaPreviewFrame");
    if (loadedCatalogId === selected.catalogId && frame && frame.contentWindow) {
      try {
        frame.contentWindow.focus();
        frame.contentWindow.print();
        setStatus("Print dialog opened for PDF (same 3-page client renderer).");
        return;
      } catch (_err) {
        /* fall through */
      }
    }
    var win = window.open(selected.previewUrl, "_blank", "noopener");
    if (!win) {
      setStatus("Popup blocked — open the web report, then use Print / Save as PDF.");
      return;
    }
    var tries = 0;
    var timer = setInterval(function () {
      tries += 1;
      try {
        if (win.document && win.document.readyState === "complete") {
          clearInterval(timer);
          win.focus();
          win.print();
        }
      } catch (_e) {
        /* cross-origin or not ready */
      }
      if (tries > 40) clearInterval(timer);
    }, 250);
    setStatus("Opened report for PDF print.");
  }

  async function copyShareLink() {
    selected = findSelected();
    if (!selected || !selected.shareUrl) return;
    var url = absoluteUrl(selected.shareUrl);
    try {
      if (navigator.clipboard && navigator.clipboard.writeText) {
        await navigator.clipboard.writeText(url);
      } else {
        window.prompt("Copy share link:", url);
      }
      setStatus("Share link copied: " + url);
    } catch (err) {
      window.prompt("Copy share link:", url);
      setStatus(err && err.message ? err.message : "Copy manually from the prompt.");
    }
  }

  async function markSent() {
    selected = findSelected();
    if (!selected || !selected.requestId || !selected.canMarkSent) return;
    var res = await authFetch(API + "/" + selected.requestId + "/mark-sent", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: "{}",
    });
    var data = await res.json();
    if (!res.ok || !data.ok) {
      throw new Error((data && (data.message || data.error)) || "Mark sent failed");
    }
    setStatus("Marked sent.");
    await loadCatalog();
  }

  function openPromoteModal() {
    selected = findSelected();
    if (!selected || !selected.requestId || !selected.canPromote) return;
    var modal = document.getElementById("alaPromoteModal");
    var body = document.getElementById("alaPromoteBody");
    body.textContent =
      promoteConfirmationText ||
      "This will create or connect this hotel to a paid ADP monitoring workflow. Free audit records will remain separate. No production records will be overwritten.";
    modal.hidden = false;
  }

  function closePromoteModal() {
    document.getElementById("alaPromoteModal").hidden = true;
  }

  async function confirmPromote() {
    selected = findSelected();
    if (!selected || !selected.requestId) return;
    var res = await authFetch(API + "/" + selected.requestId + "/promote", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ confirmed: true }),
    });
    var data = await res.json();
    if (!res.ok || !data.ok) {
      throw new Error((data && (data.message || data.error)) || "Promote failed");
    }
    closePromoteModal();
    setStatus("Promotion stub created. Leak audit records remain separate; no production ADP writes.");
    await loadCatalog();
  }

  function wantsNavDebug() {
    try {
      return new URLSearchParams(window.location.search).get("debug") === "nav";
    } catch (_err) {
      return false;
    }
  }

  async function probeRoute(path) {
    try {
      var res = await fetch(path, { method: "GET", credentials: "same-origin", redirect: "manual" });
      return {
        path: path,
        status: res.status,
        redirected: res.type === "opaqueredirect" || (res.status >= 300 && res.status < 400),
        location: res.headers.get("Location") || null,
      };
    } catch (err) {
      return { path: path, status: "error", message: err && err.message ? err.message : String(err) };
    }
  }

  async function renderNavDebug() {
    if (!wantsNavDebug()) return;
    var panel = document.getElementById("alaNavDebug");
    var body = document.getElementById("alaNavDebugBody");
    if (!panel || !body) return;
    panel.hidden = false;

    var mePayload = null;
    try {
      var meRes = await authFetch("/api/me", { maxWaitMs: 20000 });
      mePayload = await meRes.json();
    } catch (err) {
      mePayload = { error: err && err.message ? err.message : String(err) };
    }

    var d = (mePayload && mePayload.dealality) || {};
    var user = (mePayload && mePayload.user) || {};
    var routes = [
      "/admin/adp-leak-audits",
      "/admin/adp-leak-audits/reports",
      "/admin/adp-leak-audits/new",
      "/admin/adp-leak-audits/portfolios",
      "/admin/adp-leak-audits/samples",
      "/admin/adp-leak-audits/converted",
    ];
    var routeStatus = [];
    for (var i = 0; i < routes.length; i++) {
      routeStatus.push(await probeRoute(routes[i]));
    }

    var lines = [
      "currentUser.email: " + (user.email || d.email || "(unknown)"),
      "dealality.role: " + (d.role || "(none)"),
      "dealality.isAdmin: " + String(d.isAdmin === true),
      "dealality.flags.isAdmin: " + String(!!(d.flags && d.flags.isAdmin)),
      "dealality.governedPlatformAdminElevated: " + String(!!d.governedPlatformAdminElevated),
      "workspaceAccess: " + JSON.stringify(d.workspaceAccess || []),
      "featureFlags.leakAudits: none (visibility is isAdmin / governed adminEmails — not a product FF)",
      "elevationSources: ADP_OWNER_APP_ADMIN_EMAILS + data/ai-demand-positioning/owner-property-access/assignments.v1.json#adminEmails",
      "navConfigSource: public/app.js#NAV_SECTIONS (adminResources → AI Demand Leak Audits)",
      "leakAuditNavGroupRegistered: true (see public/app.js NAV_SECTIONS; requires hasAdminNavAccess via /api/me isAdmin)",
      "paidAdpNavUnchanged: Market Intelligence → AI Demand Positioning remains separate",
      "",
      "routeStatus:",
    ];
    routeStatus.forEach(function (r) {
      lines.push(
        "  " +
          r.path +
          " → status=" +
          r.status +
          (r.location ? " location=" + r.location : "") +
          (r.message ? " err=" + r.message : "")
      );
    });
    body.textContent = lines.join("\n");
  }

  async function init() {
    if (!window.SupportAdminGate) {
      showDenied("Unable to load admin gate.");
      return;
    }
    var allowed = await window.SupportAdminGate.requireAdpMonthlyReviewAdmin({
      contentId: "supportGateContent",
    });
    if (!allowed) return;

    var loadingEl = document.getElementById("supportGateLoading");
    window.SupportAdminGate.setPageLoadingMessage(
      loadingEl,
      "Loading AI Demand Leak Audits…"
    );

    try {
      await loadCatalog();
      await renderNavDebug();
      window.SupportAdminGate.hidePageLoading(loadingEl);
      document.getElementById("supportGateContent").hidden = false;
    } catch (err) {
      window.SupportAdminGate.hidePageLoading(loadingEl);
      document.getElementById("supportGateContent").hidden = false;
      setStatus(
        (err && err.message ? err.message : "Unable to load report catalog") +
          " — filters and preview stay available after access is restored."
      );
      await renderNavDebug().catch(function () {});
    }

    ["alaFilterReportType", "alaFilterStatus", "alaFilterSource"].forEach(function (id) {
      document.getElementById(id).addEventListener("change", function () {
        loadCatalog().catch(function (err) {
          setStatus(err.message || String(err));
        });
      });
    });
    document.getElementById("alaReportSelect").addEventListener("change", syncActionButtons);
    document.getElementById("alaLoadReport").addEventListener("click", loadPreview);
    document.getElementById("alaOpenWeb").addEventListener("click", openWebReport);
    document.getElementById("alaDownloadPdf").addEventListener("click", function () {
      downloadPdf();
    });
    document.getElementById("alaCopyShare").addEventListener("click", function () {
      copyShareLink().catch(function (err) {
        setStatus(err.message || String(err));
      });
    });
    document.getElementById("alaMarkSent").addEventListener("click", function () {
      markSent().catch(function (err) {
        alert(err.message || String(err));
      });
    });
    document.getElementById("alaPromote").addEventListener("click", openPromoteModal);
    document.getElementById("alaPromoteCancel").addEventListener("click", closePromoteModal);
    document.getElementById("alaPromoteConfirm").addEventListener("click", function () {
      confirmPromote().catch(function (err) {
        alert(err.message || String(err));
      });
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
