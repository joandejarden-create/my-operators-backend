/**
 * Dealality Old Home — light motion (local preview)
 * Marks section headers + card grids, reveals once via IntersectionObserver.
 * Does not animate #hero rotator / globe or #oh-how-we-do-it step panels.
 */
(function () {
  "use strict";

  var ROOT_ATTR = "data-oh-motion";
  var LEVEL = "light";

  function qs(sel, root) {
    return Array.prototype.slice.call((root || document).querySelectorAll(sel));
  }

  function reducedMotion() {
    try {
      return window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    } catch (err) {
      return false;
    }
  }

  function motionEnabled() {
    var v = document.documentElement.getAttribute(ROOT_ATTR);
    if (v === "0" || v === "off") return false;
    if (reducedMotion()) return false;
    return v === LEVEL || v === "1" || !v;
  }

  function markReveal(el, index) {
    if (!el || el.classList.contains("oh-m-reveal")) return;
    el.classList.add("oh-m-reveal");
    if (typeof index === "number") el.style.setProperty("--oh-m-i", String(index));
  }

  function markGroup(nodes) {
    nodes.forEach(function (el, i) {
      markReveal(el, i);
    });
  }

  function markLift(nodes) {
    nodes.forEach(function (el) {
      if (el) el.classList.add("oh-m-lift");
    });
  }

  function decorate(root) {
    var scope = root || document;

    // Section headers / leads (skip hero — already has rotator + globe)
    markGroup(
      qs(
        [
          "#about-badge",
          "#about-h2",
          "#about-lead",
          "#persp-badges",
          "#persp-h2",
          "#persp-lead",
          "#persp-tabs",
          "#oh-how-we-do-it .oh-how-head",
          "#platform-features-badges",
          "#platform-features-h2",
          "#platform-features-lead",
          "#modules-badges",
          "#modules-h2",
          "#modules-lead",
          "#testimonials-badges",
          "#testimonials-h2",
          "#testimonials-lead",
          "#pricing-badges",
          "#pricing-h2",
          "#pricing-lead",
          "#faq-badges",
          "#faq-h2",
          "#faq-lead",
          "#insights-badges",
          "#insights-h2",
          "#insights-lead",
          "#cta-band-card",
        ].join(","),
        scope
      )
    );

    // Dense box / card areas — stagger children
    markGroup(qs("#about-points > li", scope));
    markLift(qs("#about-points > li", scope));

    markGroup(qs("#persp-owners-grid > article, #persp-brands-grid > article, #persp-advisors-grid > article", scope));
    markLift(qs("#persp-owners-grid > article, #persp-brands-grid > article, #persp-advisors-grid > article", scope));

    markGroup(qs("#platform-features-grid > article", scope));
    markLift(qs("#platform-features-grid > article", scope));

    markGroup(qs("#modules-grid > article, #modules-grid-platform > article", scope));
    markLift(qs("#modules-grid > article, #modules-grid-platform > article", scope));

    markGroup(qs("#testimonials .oh-testimonial-card, #testimonials-track article", scope));
    markLift(qs("#testimonials .oh-testimonial-card, #testimonials-track article", scope));

    markGroup(qs("#pricing-grid > article, #pricing-cards > article, #pricing-inner article", scope));
    markLift(qs("#pricing-grid > article, #pricing-cards > article, #pricing-inner article", scope));

    markGroup(qs("#faq-list > details, #faq-list .oh-faq-item", scope));

    markGroup(qs("#insights-grid > article", scope));
    markLift(qs("#insights-grid > article", scope));
  }

  function revealAll() {
    qs(".oh-m-reveal").forEach(function (el) {
      el.classList.add("is-in");
    });
  }

  function observe() {
    var targets = qs(".oh-m-reveal");
    if (!targets.length) return;

    if (!motionEnabled()) {
      revealAll();
      return;
    }

    if (typeof IntersectionObserver !== "function") {
      revealAll();
      return;
    }

    var io = new IntersectionObserver(
      function (entries) {
        entries.forEach(function (entry) {
          if (!entry.isIntersecting) return;
          entry.target.classList.add("is-in");
          io.unobserve(entry.target);
        });
      },
      { root: null, rootMargin: "0px 0px -8% 0px", threshold: 0.12 }
    );

    targets.forEach(function (el) {
      io.observe(el);
    });
  }

  function setLevel(level) {
    document.documentElement.setAttribute(ROOT_ATTR, level);
    if (level === "0" || level === "off" || reducedMotion()) {
      revealAll();
    }
  }

  function init(root) {
    if (!document.documentElement.getAttribute(ROOT_ATTR)) {
      document.documentElement.setAttribute(ROOT_ATTR, LEVEL);
    }
    decorate(root);
    observe();
  }

  window.DealalityOldHomeMotionLight = {
    init: init,
    setLevel: setLevel,
    decorate: decorate,
    observe: observe,
  };

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", function () {
      // Preview page may inject markup later; only auto-init if content exists.
      if (document.getElementById("dc-premium") || document.getElementById("modules")) {
        init(document);
      }
    });
  } else if (document.getElementById("dc-premium") || document.getElementById("modules")) {
    init(document);
  }
})();
