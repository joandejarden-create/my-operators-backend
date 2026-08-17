/**
 * Dealality Old Home — production motion
 * Locked preset: medium · rise · cue · early · edge · all sections
 * Version: v20260801g
 * 01g: unlock path gate for live Home (`/` + `/old-home`) after homepage cutover.
 * 01f: unify enter contract — header + one body group per section;
 *      Platform: whole #dealality-many-futures after mf-js-ready (not hotel-only);
 *      sticky gated until section-in; decorate retry waits for embed body;
 *      Ecosystem stage as one body; FAQ per-item stagger; CTA note; Manual Process header-only.
 * 01e: How lead + principle callout in stagger; wait for lead tag on late mount;
 *      double-rAF before sync play so late inject paints opacity:0 first.
 * 01d: re-decorate when late-injected How / Platform / pricing lead / CTA appear.
 * 01c: retarget live SoT — How We Do It (dealality-process_*), Platform
 *      (#many-futures), Ecosystem (#ecosystem); pricing lead + advisors;
 *      CTA header stack (badges/h2/lead) matching Benefits/FAQs pattern.
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
    { id: "features", sel: "#many-futures, #platform-features" },
    { id: "modules", sel: "#modules" },
    { id: "ecosystem", sel: "#ecosystem" },
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
    return path === "/" || path === "/old-home" || path.indexOf("/old-home") === 0;
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

  function clearReveal(el) {
    if (!el) return;
    el.classList.remove("oh-m-reveal", "is-in");
    el.removeAttribute("data-oh-sec");
    el.style.removeProperty("--oh-m-i");
  }

  function ensureUnifyCss() {
    if (document.getElementById("oh-m-unify-css")) return;
    var css = document.createElement("style");
    css.id = "oh-m-unify-css";
    css.textContent = [
      "/* Sticky only after Platform section enter — avoid sticky fighting rise */",
      "@media screen and (min-width:992px){",
      "#many-futures:not(.oh-m-section-in) .mf-rail-sticky,",
      "#platform-features:not(.oh-m-section-in) .mf-rail-sticky{",
      "position:relative!important;top:auto!important;max-height:none!important",
      "}",
      "}",
      "@media (prefers-reduced-motion:reduce){",
      "#many-futures .mf-rail-sticky,#platform-features .mf-rail-sticky{",
      "position:relative!important;top:auto!important",
      "}",
      "}",
    ].join("");
    document.head.appendChild(css);
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

  function platformBodyNodes() {
    var embed = document.getElementById("dealality-many-futures");
    if (embed && embed.classList.contains("mf-js-ready")) {
      // Clear hotel-only marks from older motion builds so opacity does not stick at 0
      qs(".mf-hotel.oh-m-reveal, article.mf-hotel.oh-m-reveal", embed).forEach(clearReveal);
      return [embed];
    }
    // Legacy fallback grid (no async embed)
    var legacy = qs("#platform-features-grid > article");
    if (legacy.length) return legacy;
    return [];
  }

  function featuresDecorateComplete() {
    var mf = document.getElementById("many-futures");
    if (!mf && !document.getElementById("platform-features")) return true;
    if (!mf) return true;
    var hasHeader = !!mf.querySelector(
      '.oh-insights-badges.oh-m-reveal[data-oh-sec="features"], #many-futures-h2.oh-m-reveal'
    );
    var embed = document.getElementById("dealality-many-futures");
    if (!embed) {
      // Embed not in DOM yet — keep retrying
      return false;
    }
    if (!embed.classList.contains("mf-js-ready")) return false;
    var hasBody = embed.classList.contains("oh-m-reveal") && embed.getAttribute("data-oh-sec") === "features";
    return hasHeader && hasBody;
  }

  function decorate() {
    ensureUnifyCss();

    // Problem / Manual Process — header only; diagram keeps its own path-draw IO
    var dmp = document.getElementById("dealality-manual-process");
    if (dmp) {
      qs(
        ".dmp-problem-card.oh-m-reveal, .dmp-problems .dmp-card.oh-m-reveal, .dmp-problem.oh-m-reveal, [data-dmp-problem].oh-m-reveal",
        dmp
      ).forEach(clearReveal);
      markSequence("about", [
        qs(".dmp-badges, .dmp-badge", dmp).slice(0, 1),
        qs("#dmp-h2, .dmp-header h2", dmp).slice(0, 1),
        qs(".dmp-lead, .dmp-header p", dmp).slice(0, 1),
      ]);
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
    qs("#persp-brands-grid > article, #persp-advisors-grid > article").forEach(function (el, i) {
      markReveal(el, 4 + i, "perspectives");
      el.classList.add("oh-m-lift");
    });

    // How: header stack + one content shell (steps live inside shell)
    markSequence("how", [
      qs("#oh-how-we-do-it .dealality-process_eyebrow").slice(0, 1),
      qs("#dealality-process-h2, #oh-how-we-do-it .dealality-process_h2").slice(0, 1),
      qs("#oh-how-we-do-it .dealality-process_lead").slice(0, 1),
      qs("#oh-how-we-do-it .dealality-process_principle").slice(0, 1),
      qs("#dealality-process-content, #oh-how-we-do-it .dealality-process_content").slice(0, 1),
    ]);

    // Platform: header + whole embed body (never hotel-only beside static workspace)
    markSequence("features", [
      qs("#many-futures .oh-insights-badges, #platform-features-badges").slice(0, 1),
      qs("#many-futures-h2, #platform-features-h2").slice(0, 1),
      qs("#many-futures .oh-section-lead, #platform-features-lead").slice(0, 1),
      platformBodyNodes(),
    ]);
    markLift(qs("#dealality-many-futures .mf-hotel, #platform-features-grid > article"));

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

    // Ecosystem: header + stage as one body (cards no longer rise alone over a static diagram)
    qs("#ecosystem article.oh-eco-card-1.oh-m-reveal, #ecosystem .oh-eco-card-1.oh-m-reveal").forEach(
      clearReveal
    );
    markSequence("ecosystem", [
      qs("#eco-badges").slice(0, 1),
      qs("#eco-h2").slice(0, 1),
      qs("#eco-lead").slice(0, 1),
      qs("#ecosystem .oh-eco-stage").slice(0, 1),
    ]);
    markLift(qs("#ecosystem article.oh-eco-card-1, #ecosystem .oh-eco-card-1"));

    markSequence("testimonials", [
      qs("#testimonials-badges"),
      qs("#testimonials-h2"),
      qs("#testimonials-lead"),
      (function () {
        var activeCards = qs(
          "#testimonials .oh-testimonial-slide.is-active .oh-testimonial-card, #trust .oh-testimonial-slide.is-active .oh-testimonial-card"
        );
        if (activeCards.length) return activeCards;
        var cards = qs("#testimonials .oh-testimonial-card, #trust .oh-testimonial-card");
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
      qs("#pricing-lead"),
      qs("#pricing-card-owners"),
      qs("#pricing-card-brands"),
      qs("#pricing-card-operators"),
      qs("#pricing-card-advisors"),
    ]);
    markLift(qs("#pricing-grid > article, #pricing-card-advisors"));

    // FAQ: per-item stagger (parity with Insights cards)
    var faqList = document.getElementById("faq-list");
    if (faqList && faqList.classList.contains("oh-m-reveal")) clearReveal(faqList);
    markSequence("faq", [
      qs("#faq-badges"),
      qs("#faq-h2"),
      qs("#faq-lead"),
      qs("#faq-list > details, #faq-list > .oh-faq-item"),
    ]);

    markSequence("insights", [
      qs("#insights-badges"),
      qs("#insights-h2"),
      qs("#insights-lead"),
      qs("#insights-grid > article"),
    ]);
    markLift(qs("#insights-grid > article"));

    markSequence("cta", [
      qs("#cta-band-badges").slice(0, 1),
      qs("#cta-band-h2").slice(0, 1),
      qs("#cta-band-lead").slice(0, 1),
      qs("#cta-band-note").slice(0, 1),
      qs("#cta-band-demo-wrap, #cta-band-btn").slice(0, 1),
    ]);

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

  function forceInIfPlayed(sectionId, rootSel) {
    var root = document.querySelector(rootSel);
    if (!root || !root.classList.contains("oh-m-section-in")) return;
    qs('.oh-m-reveal[data-oh-sec="' + sectionId + '"]').forEach(function (el) {
      if (!el.classList.contains("is-in")) el.classList.add("is-in");
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
      qs("[data-oh-sec-root]").forEach(function (el) {
        el.classList.add("oh-m-section-in");
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
      var inView = rect.top < vh * 0.75 && rect.bottom > vh * 0.1;
      if (!inView) {
        io.observe(el);
        return;
      }
      // Late-injected How/etc. must paint opacity:0 one frame before is-in,
      // or the lead/subheader rise is skipped (same-tick decorate → play).
      var id = el.getAttribute("data-oh-sec-root");
      window.requestAnimationFrame(function () {
        window.requestAnimationFrame(function () {
          if (el.classList.contains("oh-m-section-in")) return;
          playSection(id);
        });
      });
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

    // Late-injected sections — re-decorate until stacks (incl. Platform body) are tagged
    var tries = 0;
    var t = window.setInterval(function () {
      tries += 1;
      var need = false;
      var how = document.getElementById("oh-how-we-do-it");
      if (
        how &&
        (!how.querySelector('.oh-m-reveal[data-oh-sec="how"]') ||
          !how.querySelector(".dealality-process_lead.oh-m-reveal") ||
          !how.querySelector(".dealality-process_principle.oh-m-reveal") ||
          !how.querySelector(
            "#dealality-process-content.oh-m-reveal, .dealality-process_content.oh-m-reveal"
          ))
      ) {
        need = true;
      }
      if (!featuresDecorateComplete()) need = true;
      var eco = document.getElementById("ecosystem");
      if (
        eco &&
        (!eco.querySelector('.oh-m-reveal[data-oh-sec="ecosystem"]') ||
          !eco.querySelector(".oh-eco-stage.oh-m-reveal"))
      ) {
        need = true;
      }
      var pLead = document.getElementById("pricing-lead");
      if (pLead && !pLead.classList.contains("oh-m-reveal")) need = true;
      var ctaBadge = document.getElementById("cta-band-badges");
      var ctaNote = document.getElementById("cta-band-note");
      if (document.getElementById("cta-band") && ctaBadge && !ctaBadge.classList.contains("oh-m-reveal")) {
        need = true;
      }
      if (ctaNote && !ctaNote.classList.contains("oh-m-reveal")) need = true;
      var dmp = document.getElementById("dealality-manual-process");
      if (dmp && !dmp.querySelector(".oh-m-reveal")) need = true;
      var faqItem = document.querySelector("#faq-list > details, #faq-list > .oh-faq-item");
      if (faqItem && !faqItem.classList.contains("oh-m-reveal")) need = true;
      if (need) {
        decorate();
        observe();
      }
      // Late-tagged nodes after section already played must not stay opacity:0
      forceInIfPlayed("how", "#oh-how-we-do-it");
      forceInIfPlayed("features", "#many-futures");
      forceInIfPlayed("ecosystem", "#ecosystem");
      forceInIfPlayed("about", "#about");
      forceInIfPlayed("faq", "#faq");
      forceInIfPlayed("cta", "#cta-band");
      if ((!need && tries > 4) || tries > 48) window.clearInterval(t);
    }, 250);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", boot);
  } else {
    boot();
  }
})();
