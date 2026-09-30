/**
 * AI Demand Admin workspace — tab shell (Reviews | Action Plan).
 * Doctrine: ADP_ADMIN_WORKSPACE_AUTH_PARITY
 */
(function () {
  "use strict";

  var STORAGE_KEY = "dealality_adp_admin_tab";
  var ALLOWED = { reviews: true, "action-plan": true, "gdi-reports": true };

  function readTabFromUrl() {
    try {
      var params = new URLSearchParams(window.location.search || "");
      var t = params.get("tab");
      if (t && ALLOWED[t]) return t;
    } catch (_e) {}
    try {
      if (window.parent && window.parent !== window) {
        var hash = String(window.parent.location.hash || "");
        var qIdx = hash.indexOf("?");
        if (qIdx >= 0) {
          var hp = new URLSearchParams(hash.slice(qIdx + 1));
          var ht = hp.get("tab");
          if (ht && ALLOWED[ht]) return ht;
        }
        var path = hash.replace(/^#/, "").split("?")[0];
        if (path === "/admin/ai-demand-reviews") return "reviews";
        if (path === "/admin/adp-action-plan") return "action-plan";
      }
    } catch (_e2) {}
    try {
      var stored = sessionStorage.getItem(STORAGE_KEY);
      if (stored && ALLOWED[stored]) return stored;
    } catch (_e3) {}
    return "reviews";
  }

  function syncParentHash(tab) {
    try {
      if (!window.parent || window.parent === window) return;
      var hash = String(window.parent.location.hash || "");
      var path = hash.replace(/^#/, "").split("?")[0] || "/admin/ai-demand";
      if (
        path === "/admin/ai-demand-reviews" ||
        path === "/admin/adp-action-plan" ||
        path === "/admin/ai-demand"
      ) {
        path = "/admin/ai-demand";
      }
      var next = "#/" + path.replace(/^\//, "") + "?tab=" + encodeURIComponent(tab);
      if (window.parent.location.hash !== next) {
        window.parent.history.replaceState(null, "", next);
      }
    } catch (_e) {}
  }

  function setActiveTab(tab) {
    if (!ALLOWED[tab]) tab = "reviews";
    try {
      sessionStorage.setItem(STORAGE_KEY, tab);
    } catch (_e) {}

    var reviewsBtn = document.getElementById("adaTabReviews");
    var planBtn = document.getElementById("adaTabActionPlan");
    var gdiBtn = document.getElementById("adaTabGdiReports");
    var reviewsPanel = document.getElementById("adaPanelReviews");
    var planPanel = document.getElementById("adaPanelActionPlan");
    var gdiPanel = document.getElementById("adaPanelGdiReports");

    if (reviewsBtn) {
      reviewsBtn.classList.toggle("active", tab === "reviews");
      reviewsBtn.setAttribute("aria-selected", tab === "reviews" ? "true" : "false");
    }
    if (planBtn) {
      planBtn.classList.toggle("active", tab === "action-plan");
      planBtn.setAttribute("aria-selected", tab === "action-plan" ? "true" : "false");
    }
    if (gdiBtn) {
      gdiBtn.classList.toggle("active", tab === "gdi-reports");
      gdiBtn.setAttribute("aria-selected", tab === "gdi-reports" ? "true" : "false");
    }
    if (reviewsPanel) reviewsPanel.hidden = tab !== "reviews";
    if (planPanel) planPanel.hidden = tab !== "action-plan";
    if (gdiPanel) gdiPanel.hidden = tab !== "gdi-reports";

    syncParentHash(tab);
    window.dispatchEvent(
      new CustomEvent("dealality-adp-admin-tab", { detail: { tab: tab } })
    );
  }

  function wireTabs() {
    document.querySelectorAll(".ada-tabs [data-tab]").forEach(function (btn) {
      btn.addEventListener("click", function () {
        setActiveTab(btn.getAttribute("data-tab"));
      });
    });
  }

  async function boot() {
    if (!window.SupportAdminGate) return;
    wireTabs();
    setActiveTab(readTabFromUrl());

    var allowed = await window.SupportAdminGate.requireAdpMonthlyReviewAdmin({
      contentId: "supportGateContent",
      loadingMessage: "Verifying AI Demand Admin access…",
    });
    if (!allowed) return;

    var loadingEl = document.getElementById("supportGateLoading");
    if (window.SupportAdminGate.hidePageLoading) {
      window.SupportAdminGate.hidePageLoading(loadingEl);
    }
    var content = document.getElementById("supportGateContent");
    if (content) content.hidden = false;

    // Child modules self-init; signal workspace ready under shared auth.
    window.__ADP_ADMIN_WORKSPACE_AUTH__ = true;
    window.dispatchEvent(new CustomEvent("dealality-adp-admin-ready"));
  }

  window.__ADP_ADMIN_SET_TAB__ = setActiveTab;
  boot();
})();
