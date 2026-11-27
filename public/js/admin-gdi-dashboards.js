/**
 * Admin Resources — GDI Sales Performance + GDI Research Intelligence.
 * Reuses Executive Summary aggregation + panel renderers.
 * Access: SupportAdminGate.requireAdmin (founder/admin only).
 */
(function () {
  "use strict";

  var API_BASE = "/api/group-demand-intelligence";
  var DEFAULT_HOTEL = "recnPB3Q02NO8Ma1o";

  function setStatus(msg) {
    var el = document.getElementById("agdiStatus");
    if (el) el.textContent = msg || "";
  }

  function qsHotel() {
    try {
      var q = new URLSearchParams(window.location.search || "");
      return String(q.get("hotelId") || "").trim();
    } catch (_) {
      return "";
    }
  }

  async function fetchExecutiveSummary(hotelId, preview) {
    var path =
      API_BASE +
      "/hotels/" +
      encodeURIComponent(hotelId) +
      "/executive-summary" +
      (preview ? "?preview=" + encodeURIComponent(preview) : "");
    var auth = window.DealalityMemberstackAuth;
    if (!auth || typeof auth.fetchMyDealsApi !== "function") {
      throw new Error("Authentication unavailable");
    }
    var res = await auth.fetchMyDealsApi(path);
    var data = res && typeof res.json === "function" ? await res.json() : res;
    if (!data || data.ok === false) {
      var err = new Error(
        (data && (data.message || data.error)) || "Unable to load dashboard",
      );
      err.status = data && data.status;
      throw err;
    }
    return data;
  }

  function renderSurface(mode, payload) {
    var root = document.getElementById("agdiDashboardRoot");
    var Tab = window.DealalityGdiExecutiveSummaryTab;
    if (!root || !Tab) {
      setStatus("Dashboard modules failed to load.");
      return;
    }
    if (mode === "research") {
      if (!payload.sections || !payload.sections.researchIntelligence) {
        root.innerHTML =
          '<div class="gdi-es-banner gdi-es-banner--warn">Research Intelligence was not authorized for this session.</div>';
        return;
      }
      root.innerHTML = Tab.renderResearchIntelligence(payload);
      return;
    }
    if (!payload.sections || !payload.sections.salesPerformance) {
      root.innerHTML =
        '<div class="gdi-es-banner gdi-es-banner--warn">Sales Performance was not authorized for this session.</div>';
      return;
    }
    root.innerHTML = Tab.renderSalesPerformance(payload);
  }

  async function load(mode) {
    var hotelEl = document.getElementById("agdiHotelId");
    var previewEl = document.getElementById("agdiPreview");
    var hotelId = String((hotelEl && hotelEl.value) || "").trim() || DEFAULT_HOTEL;
    var preview = previewEl ? String(previewEl.value || "") : "staged";
    if (hotelEl && !hotelEl.value) hotelEl.value = hotelId;
    setStatus("Loading…");
    try {
      var payload = await fetchExecutiveSummary(hotelId, preview);
      if (mode === "research" && payload.researchIntelligenceAuthorized !== true) {
        setStatus("Denied — Research Intelligence requires founder/admin authorization.");
        renderSurface(mode, { sections: {} });
        return;
      }
      if (
        mode === "sales" &&
        !(payload.sections && payload.sections.salesPerformance)
      ) {
        setStatus("Denied — Sales Performance requires founder/admin authorization.");
        renderSurface(mode, { sections: {} });
        return;
      }
      renderSurface(mode, payload);
      var periodLabel =
        payload.sections &&
        payload.sections.executiveCommercialSnapshot &&
        payload.sections.executiveCommercialSnapshot.reportingPeriod &&
        payload.sections.executiveCommercialSnapshot.reportingPeriod.label;
      setStatus(
        (payload.hotelName || hotelId) +
          " · " +
          (payload.dataState || "") +
          (periodLabel ? " · " + periodLabel : ""),
      );
    } catch (err) {
      setStatus(err && err.message ? err.message : "Load failed");
      var root = document.getElementById("agdiDashboardRoot");
      if (root) {
        root.innerHTML =
          '<div class="gdi-es-banner gdi-es-banner--warn">' +
          String((err && err.message) || "Load failed") +
          "</div>";
      }
    }
  }

  async function boot(mode) {
    var gate = window.SupportAdminGate;
    var deniedEl = document.getElementById("supportGateDenied");
    var loadingEl = document.getElementById("supportGateLoading");
    if (!gate || typeof gate.requireAdmin !== "function") {
      if (loadingEl) loadingEl.hidden = true;
      if (deniedEl) {
        deniedEl.hidden = false;
        var msg = deniedEl.querySelector("[data-gate-message]");
        if (msg) {
          msg.textContent =
            "Authentication unavailable. Sign in through the Dealality app to view this page.";
        }
      }
      return;
    }
    if (
      !window.DealalityMemberstackAuth ||
      typeof window.DealalityMemberstackAuth.authFetch !== "function"
    ) {
      if (loadingEl) loadingEl.hidden = true;
      if (deniedEl) {
        deniedEl.hidden = false;
        var authMsg = deniedEl.querySelector("[data-gate-message]");
        if (authMsg) {
          authMsg.textContent =
            "Authentication unavailable. Sign in through the Dealality app to view this page.";
        }
      }
      return;
    }
    var ok = await gate.requireAdmin({
      loadingId: "supportGateLoading",
      deniedId: "supportGateDenied",
      contentId: "supportGateContent",
      loadingMessage: "Verifying admin access…",
    });
    if (!ok) return;

    var hotelEl = document.getElementById("agdiHotelId");
    if (hotelEl) hotelEl.value = qsHotel() || DEFAULT_HOTEL;
    var btn = document.getElementById("agdiLoadBtn");
    if (btn) {
      btn.addEventListener("click", function () {
        load(mode);
      });
    }
    await load(mode);
  }

  window.DealalityGdiAdminDashboards = { boot: boot, load: load };
})();
