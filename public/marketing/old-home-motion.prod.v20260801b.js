/**
 * Dealality Old Home — production motion
 * Locked preset: medium · rise · cue · early · edge · all sections
 * Version: v20260801b
 * 01b: testimonials section root is #trust (live) with #testimonials fallback;
 *      quote cards marked via #testimonials-viewport article.
 */
(function () {
  "use strict";

  var PRESET = {
    level: "medium",
    style: "rise",
    cue: "1",
    early: "1",
    depth: "0",
    edge: "1",
  };

  var SECTION_ROOTS = [
    { id: "about", sel: "#about" },
    { id: "perspectives", sel: "#perspectives" },
    { id: "how", sel: "#oh-how-we-do-it" },
    { id: "features", sel: "#platform-features" },
    { id: "modules", sel: "#modules" },
    { id: "testimonials", sel: "#trust, #testimonials" },
    { id: "pricing", sel: "#pricing" },
    { id: "faq", sel: "#faq" },
    { id: "insights", sel: "#insights" },
    { id: "cta", sel: "#cta-band" },
  ];

  var io = null;
  var started = false;

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

  function isOldHome() {
    var path = (location.pathname || "").replace(/\/+$/, "") || "/";
    return path === "/old-home" || path.indexOf("/old-home") === 0;
  }

  function markReveal(el, index, sectionId) {
    if (!el) return;
    el.classList.add("oh-m-reveal");
    el.setAttribute("data-oh-sec", sectionId);
    el.style.setProperty("--oh-m-i", String(index));
  }

  function markSequence(sectionId, lists) {
    var i = 0;
    lists.forEach(function (nodes) {
      (nodes || []).forEach(function (el) {
        if (!el) return;
        markReveal(el, i++, sectionId);
      });
    });
  }

  function markLift(nodes) {
    nodes.forEach(function (el) {
      if (el) el.classList.add("oh-m-lift");
    });
  }

  function ensureScrollCue() {
    if (document.getElementById("oh-scroll-cue")) return;
    var hero = document.getElementById("hero");
    if (!hero || !hero.parentNode) return;
    var cue = document.createElement("div");
    cue.id = "oh-scroll-cue";
    cue.setAttribute("aria-hidden", "true");
    cue.innerHTML =
      '<div class="oh-scroll-cue-inner"><span>Scroll to explore</span><span class="oh-scroll-cue-chev"></span></div>';
    if (hero.nextSibling) hero.parentNode.insertBefore(cue, hero.nextSibling);
    else hero.parentNode.appendChild(cue);
  }

  function decorate() {
    // Problem / Manual Process — header + problem cards (diagram keeps its own draw)
    var dmp = document.getElementById("dealality-manual-process");
    if (dmp) {
      markSequence("about", [
        qs(".dmp-badges, .dmp-badge", dmp).slice(0, 1),
        qs("#dmp-h2, .dmp-header h2", dmp).slice(0, 1),
        qs(".dmp-lead, .dmp-header p", dmp).slice(0, 1),
        qs(
          ".dmp-problem-card, .dmp-problems .dmp-card, .dmp-problem, [data-dmp-problem]",
          dmp
        ),
      ]);
      markLift(
        qs(
          ".dmp-problem-card, .dmp-problems .dmp-card, .dmp-problem, [data-dmp-problem]",
          dmp
        )
      );
    } else {
      markSequence("about", [
        qs("#about-badge"),
        qs("#about-h2"),
        qs("#about-lead"),
        qs("#about-points > li"),
      ]);
      markLift(qs("#about-points > li"));
    }

    markSequence("perspectives", [
      qs("#persp-badges"),
      qs("#persp-h2"),
      qs("#persp-lead"),
      qs("#persp-tabs"),
      qs("#persp-owners-grid > article"),
    ]);
    markLift(
      qs(
        "#persp-owners-grid > article, #persp-brands-grid > article, #persp-advisors-grid > article"
      )
    );
    // Mark other perspective panels for tab restagger
    qs("#persp-brands-grid > article, #persp-advisors-grid > article").forEach(function (el, i) {
      markReveal(el, 4 + i, "perspectives");
      el.classList.add("oh-m-lift");
    });

    markSequence("how", [qs("#oh-how-we-do-it .oh-how-head, #oh-how-we-do-it .dealality-process_header").slice(0, 1)]);

    markSequence("features", [
      qs("#platform-features-badges"),
      qs("#platform-features-h2"),
      qs("#platform-features-lead"),
      qs("#platform-features-grid > article"),
    ]);
    markLift(qs("#platform-features-grid > article"));

    markSequence("modules", [
      qs("#modules-badges"),
      qs("#modules-h2"),
      qs("#modules-lead"),
      qs("#modules-grid > article"),
    ]);
    qs("#modules-grid-platform > article").forEach(function (el, i) {
      markReveal(el, 3 + i, "modules");
      el.classList.add("oh-m-lift");
    });
    markLift(qs("#modules-grid > article"));

    markSequence("testimonials", [
      qs("#testimonials-badges"),
      qs("#testimonials-h2"),
      qs("#testimonials-lead"),
      (function () {
        var activeCards = qs(
          "#testimonials .oh-testimonial-slide.is-active .oh-testimonial-card, #trust .oh-testimonial-slide.is-active .oh-testimonial-card"
        );
        if (activeCards.length) return activeCards;
        var cards = qs(
          "#testimonials .oh-testimonial-card, #trust .oh-testimonial-card"
        );
        if (cards.length) return cards;
        return qs(
          "#testimonials-viewport > div[data-slide].is-active article, #testimonials-viewport > div[data-slide]:first-child article"
        );
      })(),
    ]);
    markLift(
      qs(
        "#testimonials .oh-testimonial-card, #trust .oh-testimonial-card, #testimonials-viewport article"
      )
    );

    markSequence("pricing", [
      qs("#pricing-badges"),
      qs("#pricing-h2"),
      qs("#pricing-card-owners"),
      qs("#pricing-card-brands"),
      qs("#pricing-card-operators"),
    ]);
    markLift(qs("#pricing-grid > article"));

    markSequence("faq", [
      qs("#faq-badges"),
      qs("#faq-h2"),
      qs("#faq-lead"),
      qs("#faq-list"),
    ]);

    markSequence("insights", [
      qs("#insights-badges"),
      qs("#insights-h2"),
      qs("#insights-lead"),
      qs("#insights-grid > article"),
    ]);
    markLift(qs("#insights-grid > article"));

    markSequence("cta", [qs("#cta-band-card")]);

    SECTION_ROOTS.forEach(function (r) {
      var el = document.querySelector(r.sel);
      if (el) el.setAttribute("data-oh-sec-root", r.id);
    });

    ensureScrollCue();
  }

  function playSection(sectionId) {
    var root = document.querySelector('[data-oh-sec-root="' + sectionId + '"]');
    if (root) root.classList.add("oh-m-section-in");

    var items = qs('.oh-m-reveal[data-oh-sec="' + sectionId + '"]').filter(function (el) {
      if (el.closest && el.closest("[hidden]")) return false;
      var slide = el.closest && el.closest(".oh-testimonial-slide");
      if (slide && slide.getAttribute("aria-hidden") === "true") return false;
      return true;
    });

    items.sort(function (a, b) {
      var ia = parseInt(a.style.getPropertyValue("--oh-m-i") || "0", 10);
      var ib = parseInt(b.style.getPropertyValue("--oh-m-i") || "0", 10);
      return ia - ib;
    });

    items.forEach(function (el, i) {
      el.style.setProperty("--oh-m-i", String(i));
      el.classList.add("is-in");
    });
  }

  function observe() {
    if (io) {
      io.disconnect();
      io = null;
    }
    if (reducedMotion()) {
      qs(".oh-m-reveal").forEach(function (el) {
        el.classList.add("is-in");
      });
      return;
    }

    var roots = qs("[data-oh-sec-root]").filter(function (el) {
      return !el.classList.contains("oh-m-section-in");
    });
    if (!roots.length) return;

    if (typeof IntersectionObserver !== "function") {
      roots.forEach(function (el) {
        playSection(el.getAttribute("data-oh-sec-root"));
      });
      return;
    }

    io = new IntersectionObserver(
      function (entries) {
        entries.forEach(function (entry) {
          if (!entry.isIntersecting) return;
          var id = entry.target.getAttribute("data-oh-sec-root");
          if (!id) return;
          playSection(id);
          io.unobserve(entry.target);
        });
      },
      { root: null, rootMargin: "0px 0px -12% 0px", threshold: 0.08 }
    );

    roots.forEach(function (el) {
      var rect = el.getBoundingClientRect();
      var vh = window.innerHeight || 800;
      if (rect.top < vh * 0.75 && rect.bottom > vh * 0.1) {
        playSection(el.getAttribute("data-oh-sec-root"));
      } else {
        io.observe(el);
      }
    });
  }

  function restaggerIn(container) {
    if (!container || reducedMotion()) return;
    var cards = qs(".oh-m-reveal", container).filter(function (el) {
      return !el.closest("[hidden]");
    });
    if (!cards.length) return;
    cards.forEach(function (el, i) {
      el.classList.remove("is-in");
      el.style.setProperty("--oh-m-i", String(3 + i));
      void el.offsetWidth;
    });
    cards.forEach(function (el, i) {
      window.setTimeout(function () {
        el.classList.add("is-in");
      }, 20 + i * 70);
    });
  }

  function wireTabs() {
    document.addEventListener(
      "click",
      function (ev) {
        var tab = ev.target.closest && ev.target.closest("#persp-tabs [role='tab']");
        if (tab) {
          window.setTimeout(function () {
            var panelId = tab.getAttribute("data-panel") || tab.getAttribute("aria-controls");
            var panel = panelId && document.getElementById(panelId);
            if (panel) restaggerIn(panel);
          }, 50);
          return;
        }
        var modTab =
          ev.target.closest &&
          ev.target.closest("#modules-tab-outcomes, #modules-tab-platform");
        if (modTab) {
          window.setTimeout(function () {
            var outcomes = document.getElementById("modules-tab-outcomes");
            var isOut =
              outcomes && outcomes.getAttribute("aria-selected") === "true";
            var panel = document.getElementById(
              isOut ? "modules-panel-outcomes" : "modules-panel-platform"
            );
            if (panel) restaggerIn(panel);
          }, 50);
        }
      },
      true
    );
  }

  function bindScrollHints() {
    var onScroll = function () {
      var hero = document.getElementById("hero");
      var past = false;
      if (hero) past = hero.getBoundingClientRect().bottom < 80;
      document.documentElement.classList.toggle("oh-scrolled-past-hero", past);
      var doc = document.documentElement;
      var remaining = doc.scrollHeight - (window.scrollY + window.innerHeight);
      document.documentElement.classList.toggle("oh-near-page-end", remaining < 160);
    };
    window.addEventListener("scroll", onScroll, { passive: true });
    onScroll();
  }

  function applyPreset() {
    var html = document.documentElement;
    html.setAttribute("data-oh-motion", PRESET.level);
    html.setAttribute("data-oh-motion-style", PRESET.style);
    html.setAttribute("data-oh-scroll-cue", PRESET.cue);
    html.setAttribute("data-oh-early-reveal", PRESET.early);
    html.setAttribute("data-oh-depth", PRESET.depth);
    html.setAttribute("data-oh-edge", PRESET.edge);
  }

  function boot() {
    if (started || !isOldHome()) return;
    started = true;
    applyPreset();
    decorate();
    observe();
    wireTabs();
    bindScrollHints();

    // Manual Process HTML may load async via boot fetch — re-decorate once
    var tries = 0;
    var t = window.setInterval(function () {
      tries += 1;
      var dmp = document.getElementById("dealality-manual-process");
      if (dmp && !dmp.querySelector(".oh-m-reveal")) {
        decorate();
        observe();
      }
      if (dmp || tries > 40) window.clearInterval(t);
    }, 250);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})();
