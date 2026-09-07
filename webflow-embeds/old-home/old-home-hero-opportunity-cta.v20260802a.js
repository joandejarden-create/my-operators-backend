/**
 * Old Home hero Opportunity CTA (v20260802a)
 * Locale-aware Opportunity Review iframe URL + Spanish label on /es.
 */
(function () {
  "use strict";
  function isEsPath() {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    return path === "/es" || path.indexOf("/es/") === 0;
  }
  function orUrl() {
    return isEsPath() ? "/es/opportunity-review" : "/opportunity-review";
  }
  function orLabel() {
    return isEsPath() ? "Explora Tu Oportunidad Hotelera" : "Explore Your Hotel Opportunity";
  }
  function emailVisible(em) {
    if (!em) return false;
    var wrap = document.getElementById("fsw-field-wrap");
    if (wrap) {
      try {
        if (window.getComputedStyle(wrap).display === "none") return false;
      } catch (err) {}
    }
    return em.offsetParent !== null;
  }
  function openOpportunity(url, label) {
    if (typeof window.ohOpenOpportunityReview === "function") {
      window.ohOpenOpportunityReview(url, label || orLabel());
      return;
    }
    window.location.href = url;
  }
  function go(e) {
    if (e) {
      e.preventDefault();
      e.stopPropagation();
    }
    var em = document.getElementById("fsw-email");
    if (emailVisible(em) && em && typeof em.reportValidity === "function" && !em.reportValidity()) {
      return;
    }
    var v = ((em && em.value) || "").trim();
    var url = orUrl();
    if (v) url += (url.indexOf("?") === -1 ? "?" : "&") + "email=" + encodeURIComponent(v);
    openOpportunity(url, orLabel());
  }
  function bind() {
    var btn = document.getElementById("fsw-btn");
    if (!btn || btn.getAttribute("data-oh-or-bound") === "1") return;
    btn.setAttribute("data-oh-or-bound", "1");
    btn.addEventListener("click", go, true);
    var hit = document.getElementById("fsw-submit-hit");
    if (hit) hit.addEventListener("click", go, true);
    var form = document.getElementById("fsw-inner");
    if (form) form.addEventListener("submit", go, true);
  }
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bind);
  } else {
    bind();
  }
  setTimeout(bind, 0);
  setTimeout(bind, 500);
})();
