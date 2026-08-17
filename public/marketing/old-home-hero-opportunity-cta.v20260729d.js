(function () {
  "use strict";
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
      window.ohOpenOpportunityReview(url, label || "Explore Your Hotel Opportunity");
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
    var url = "https://www.dealality.com/opportunity-review";
    if (v) url += "?email=" + encodeURIComponent(v);
    openOpportunity(url, "Explore Your Hotel Opportunity");
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
  // Late bind if footer OH script mounts later
  setTimeout(bind, 0);
  setTimeout(bind, 500);
})();
