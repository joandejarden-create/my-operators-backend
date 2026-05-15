/**
 * Synchronous KPI paint before ES modules run — avoids an empty KPI strip on full page loads
 * when switching FDD workspace tabs. Keep card markup in sync with public/js/fdd-workspace-kpi.js.
 * PREFIX must match `FDD_KPI_SESSION_PREFIX` in fdd-workspace-kpi.js.
 */
(function () {
  var PREFIX = "fdd-intelligence-kpi-v1:";

  function esc(s) {
    return String(s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function paint(el, s) {
    var items = [
      ["Approved rows", s.approvedCount],
      ["Needs Review rows", s.needsReviewCount],
      ["Possible duplicate rows", s.possibleDuplicateCount],
      ["Legal review (Yes)", s.legalReviewCount],
      ["Commercial review (Yes)", s.commercialReviewCount],
    ];
    var cards = items
      .map(function (pair) {
        var label = pair[0];
        var n = pair[1];
        return (
          '<div class="fdd-kpi-metric-card">' +
          '<div class="fdd-kpi-metric-card__label-wrap">' +
          '<span class="fdd-kpi-metric-card__label">' +
          esc(label) +
          "</span></div>" +
          '<div class="fdd-kpi-metric-card__value">' +
          esc(String(n != null ? n : 0)) +
          "</div>" +
          '<div class="fdd-kpi-metric-card__footer"></div></div>'
        );
      })
      .join("");
    el.innerHTML = '<div class="fdd-kpi-strip-inner">' + cards + "</div>";
  }

  function zeros() {
    return {
      approvedCount: 0,
      needsReviewCount: 0,
      possibleDuplicateCount: 0,
      legalReviewCount: 0,
      commercialReviewCount: 0,
    };
  }

  function run() {
    var el = document.getElementById("fdd-summary-cards");
    if (!el) return;
    var ctx = el.getAttribute("data-kpi-context");
    if (!ctx) return;
    var s = zeros();
    try {
      var raw = sessionStorage.getItem(PREFIX + ctx);
      if (raw) {
        var parsed = JSON.parse(raw);
        if (parsed && typeof parsed.approvedCount === "number") s = parsed;
      }
    } catch (e) {
      /* ignore */
    }
    paint(el, s);
  }

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", run);
  else run();
})();
