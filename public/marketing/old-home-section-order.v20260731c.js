/**
 * Old Home section order (v20260731c)
 * Path-gated to /old-home.
 * Hero → Problem → How Dealality Works → Owner Benefits → Features →
 * Full Ecosystem → About → FAQ → Insights → Pricing → CTA → Footer
 */
(function () {
  try {
    var path = (window.location && window.location.pathname) || "";
    if (path !== "/old-home") return;
    if (window.__ohSectionOrder >= 202607313) return;
    window.__ohSectionOrder = 202607313;

    var ORDER = [
      "hero",
      "about",
      "oh-how-we-do-it",
      "modules",
      "many-futures",
      "ecosystem",
      "trust",
      "faq",
      "insights",
      "pricing",
      "cta-band",
      "footer-new",
      "footer",
    ];

    function byId(id) {
      return document.getElementById(id);
    }

    function applyOrder() {
      var els = [];
      var parent = null;
      for (var i = 0; i < ORDER.length; i++) {
        var el = byId(ORDER[i]);
        if (!el) continue;
        if (!parent) parent = el.parentNode;
        if (!parent || el.parentNode !== parent) continue;
        els.push(el);
      }
      if (!parent || els.length < 2) return false;

      var earliestIdx = Infinity;
      var earliest = els[0];
      for (var j = 0; j < els.length; j++) {
        var idx = Array.prototype.indexOf.call(parent.children, els[j]);
        if (idx > -1 && idx < earliestIdx) {
          earliestIdx = idx;
          earliest = els[j];
        }
      }

      var marker = document.createComment("oh-section-order");
      parent.insertBefore(marker, earliest);
      for (var k = 0; k < els.length; k++) {
        parent.insertBefore(els[k], marker);
      }
      parent.removeChild(marker);

      if (typeof console !== "undefined" && console.info) {
        console.info(
          "[oh-section-order]",
          els
            .map(function (node) {
              return node.id;
            })
            .join(" → ")
        );
      }
      return true;
    }

    function runWhenReady(attempt) {
      var n = attempt || 0;
      var hasProcess = !!byId("oh-how-we-do-it");
      var hasModules = !!byId("modules");
      var hasCta = !!byId("cta-band");
      if (hasModules && hasCta && (hasProcess || n > 12)) {
        if (applyOrder()) return;
      }
      if (n < 40) {
        window.setTimeout(function () {
          runWhenReady(n + 1);
        }, 150);
      } else {
        applyOrder();
      }
    }

    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", function () {
        runWhenReady(0);
      });
    } else {
      runWhenReady(0);
    }
  } catch (err) {
    if (typeof console !== "undefined" && console.error) {
      console.error("[oh-section-order]", err);
    }
  }
})();
