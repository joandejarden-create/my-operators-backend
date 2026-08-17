/**
 * Dealality Old Home — Motion Lab (local preview only)
 * Global: data-oh-motion, data-oh-motion-style, scroll aids
 * Sections: data-oh-sec-{id} = 1|0  (per-section motion on/off)
 */
(function () {
  "use strict";

  var ATTR = {
    level: "data-oh-motion",
    style: "data-oh-motion-style",
    cue: "data-oh-scroll-cue",
    early: "data-oh-early-reveal",
    depth: "data-oh-depth",
    edge: "data-oh-edge",
  };

  /** Section playbook — recommended defaults on */
  var SECTIONS = [
    {
      id: "hero",
      label: "Hero cue",
      hint: "Scroll cue under hero only (no content reveal)",
      defaultOn: true,
    },
    {
      id: "about",
      label: "Problem",
      hint: "Header + 3 pain points stagger top→bottom",
      defaultOn: true,
    },
    {
      id: "perspectives",
      label: "Perspectives",
      hint: "Header/tabs once; cards restagger on tab change",
      defaultOn: true,
    },
    {
      id: "how",
      label: "How We Do It",
      hint: "Header only (keep existing step UI)",
      defaultOn: true,
    },
    {
      id: "features",
      label: "Features",
      hint: "Header + product cards stagger",
      defaultOn: true,
    },
    {
      id: "modules",
      label: "Benefits (6)",
      hint: "Row cascade 1→2→3 then 4→5→6; replay on tab",
      defaultOn: true,
    },
    {
      id: "testimonials",
      label: "About / Trust",
      hint: "Header + active cards; calm crossfade feel",
      defaultOn: true,
    },
    {
      id: "pricing",
      label: "Pricing",
      hint: "3 cards L→R; Owners slightly earlier",
      defaultOn: true,
    },
    {
      id: "faq",
      label: "FAQ",
      hint: "Header + list shell only (no item stagger)",
      defaultOn: true,
    },
    {
      id: "insights",
      label: "Insights",
      hint: "Quiet card stagger",
      defaultOn: true,
    },
    {
      id: "cta",
      label: "CTA band",
      hint: "Single decisive rise of the CTA card",
      defaultOn: true,
    },
  ];

  var DEFAULTS = {
    level: "medium",
    style: "rise",
    cue: "1",
    early: "1",
    depth: "0",
    edge: "1",
  };

  var io = null;
  var scrollBound = false;
  var tabsWired = false;

  function qs(sel, root) {
    return Array.prototype.slice.call((root || document).querySelectorAll(sel));
  }

  function htmlEl() {
    return document.documentElement;
  }

  function secAttr(id) {
    return "data-oh-sec-" + id;
  }

  function reducedMotion() {
    try {
      return window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    } catch (err) {
      return false;
    }
  }

  function getLevel() {
    return htmlEl().getAttribute(ATTR.level) || DEFAULTS.level;
  }

  function motionOn() {
    var v = getLevel();
    if (v === "0" || v === "off") return false;
    if (reducedMotion()) return false;
    return true;
  }

  function sectionOn(id) {
    var v = htmlEl().getAttribute(secAttr(id));
    if (v == null) {
      var def = SECTIONS.find(function (s) {
        return s.id === id;
      });
      return !def || def.defaultOn !== false;
    }
    return v === "1";
  }

  /** Section root → id. One IO trigger per section keeps reading order. */
  var SECTION_ROOTS = [
    { id: "about", sel: "#about" },
    { id: "perspectives", sel: "#perspectives" },
    { id: "how", sel: "#oh-how-we-do-it" },
    { id: "features", sel: "#platform-features" },
    { id: "modules", sel: "#modules" },
    { id: "testimonials", sel: "#testimonials" },
    { id: "pricing", sel: "#pricing" },
    { id: "faq", sel: "#faq" },
    { id: "insights", sel: "#insights" },
    { id: "cta", sel: "#cta-band" },
  ];

  function markReveal(el, index, sectionId) {
    if (!el) return;
    el.classList.add("oh-m-reveal");
    el.setAttribute("data-oh-sec", sectionId);
    if (typeof index === "number") el.style.setProperty("--oh-m-i", String(index));
  }

  /**
   * Mark nodes in strict reading order: badge → title → lead → body/cards.
   * Continuous --oh-m-i so CSS delay never lets cards beat the header.
   */
  function markSequence(sectionId, nodeLists) {
    var i = 0;
    nodeLists.forEach(function (nodes) {
      (nodes || []).forEach(function (el) {
        if (!el) return;
        markReveal(el, i, sectionId);
        i += 1;
      });
    });
    return i;
  }

  function markLift(nodes) {
    nodes.forEach(function (el) {
      if (el) el.classList.add("oh-m-lift");
    });
  }

  function first(sel, root) {
    return qs(sel, root);
  }

  function ensureScrollCue() {
    if (document.getElementById("oh-scroll-cue")) return;
    var hero = document.getElementById("hero");
    if (!hero || !hero.parentNode) return;
    var cue = document.createElement("div");
    cue.id = "oh-scroll-cue";
    cue.setAttribute("aria-hidden", "true");
    cue.setAttribute("data-oh-sec", "hero");
    cue.innerHTML =
      '<div class="oh-scroll-cue-inner"><span>Scroll to explore</span><span class="oh-scroll-cue-chev"></span></div>';
    if (hero.nextSibling) hero.parentNode.insertBefore(cue, hero.nextSibling);
    else hero.parentNode.appendChild(cue);
  }

  function clearMarks(root) {
    qs(".oh-m-reveal, .oh-m-lift", root).forEach(function (el) {
      el.classList.remove("oh-m-reveal", "oh-m-lift", "is-in", "oh-m-forced");
      el.removeAttribute("data-oh-sec");
      el.style.removeProperty("--oh-m-i");
    });
    qs("[data-oh-sec-root]", root).forEach(function (el) {
      el.removeAttribute("data-oh-sec-root");
      el.classList.remove("oh-m-section-in");
    });
  }

  function decorate(root) {
    var scope = root || document;
    clearMarks(scope);

    // Problem — badge → h2 → lead → 3 points
    markSequence("about", [
      first("#about-badge", scope),
      first("#about-h2", scope),
      first("#about-lead", scope),
      first("#about-points > li", scope),
    ]);
    markLift(first("#about-points > li", scope));

    // Perspectives — badge → h2 → lead → tabs, then cards (cards restagger on tab)
    markSequence("perspectives", [
      first("#persp-badges", scope),
      first("#persp-h2", scope),
      first("#persp-lead", scope),
      first("#persp-tabs", scope),
      first("#persp-owners-grid > article", scope),
      first("#persp-brands-grid > article", scope),
      first("#persp-advisors-grid > article", scope),
    ]);
    markLift(
      first(
        "#persp-owners-grid > article, #persp-brands-grid > article, #persp-advisors-grid > article",
        scope
      )
    );

    // How We Do It — header block only (single unit)
    markSequence("how", [first("#oh-how-we-do-it .oh-how-head", scope)]);

    // Features — badge → h2 → lead → cards
    markSequence("features", [
      first("#platform-features-badges", scope),
      first("#platform-features-h2", scope),
      first("#platform-features-lead", scope),
      first("#platform-features-grid > article", scope),
    ]);
    markLift(first("#platform-features-grid > article", scope));

    // Benefits — badge → h2 → lead → journey cards (DOM order)
    markSequence("modules", [
      first("#modules-badges", scope),
      first("#modules-h2", scope),
      first("#modules-lead", scope),
      first("#modules-grid > article", scope),
    ]);
    // Platform tab cards: same body slot indices (after header 0–2 → cards 3+)
    first("#modules-grid-platform > article", scope).forEach(function (el, i) {
      markReveal(el, 3 + i, "modules");
      el.setAttribute("data-oh-m-body", "1");
    });
    first("#modules-grid > article", scope).forEach(function (el) {
      el.setAttribute("data-oh-m-body", "1");
    });
    markLift(first("#modules-grid > article, #modules-grid-platform > article", scope));

    // Testimonials — badge → h2 → lead → cards
    markSequence("testimonials", [
      first("#testimonials-badges", scope),
      first("#testimonials-h2", scope),
      first("#testimonials-lead", scope),
      first("#testimonials .oh-testimonial-slide.is-active .oh-testimonial-card", scope).length
        ? first("#testimonials .oh-testimonial-slide.is-active .oh-testimonial-card", scope)
        : first("#testimonials .oh-testimonial-card", scope),
    ]);
    markLift(first("#testimonials .oh-testimonial-card", scope));

    // Pricing — badge → h2 → Owners → Brands → Operators
    markSequence("pricing", [
      first("#pricing-badges", scope),
      first("#pricing-h2", scope),
      first("#pricing-card-owners", scope),
      first("#pricing-card-brands", scope),
      first("#pricing-card-operators", scope),
    ]);
    markLift(first("#pricing-grid > article", scope));

    // FAQ — badge → h2 → lead → list shell
    markSequence("faq", [
      first("#faq-badges", scope),
      first("#faq-h2", scope),
      first("#faq-lead", scope),
      first("#faq-list", scope),
    ]);

    // Insights — badge → h2 → lead → cards
    markSequence("insights", [
      first("#insights-badges", scope),
      first("#insights-h2", scope),
      first("#insights-lead", scope),
      first("#insights-grid > article", scope),
    ]);
    markLift(first("#insights-grid > article", scope));

    // CTA — single card
    markSequence("cta", [first("#cta-band-card", scope)]);

    // Tag section roots for sequenced reveal
    SECTION_ROOTS.forEach(function (r) {
      var el = scope.querySelector ? scope.querySelector(r.sel) : document.querySelector(r.sel);
      if (!el && scope !== document) el = document.querySelector(r.sel);
      if (el) {
        el.setAttribute("data-oh-sec-root", r.id);
      }
    });

    ensureScrollCue();
    applySectionVisibility();
    wireSectionInteractions();
  }

  function applySectionVisibility() {
    // Force visible when section off or global off
    qs(".oh-m-reveal").forEach(function (el) {
      var sid = el.getAttribute("data-oh-sec");
      if (!motionOn() || (sid && !sectionOn(sid))) {
        el.classList.add("is-in");
        el.classList.add("oh-m-forced");
      } else {
        el.classList.remove("oh-m-forced");
      }
    });

    var cue = document.getElementById("oh-scroll-cue");
    if (cue) {
      cue.hidden = !(motionOn() && sectionOn("hero") && htmlEl().getAttribute(ATTR.cue) === "1");
    }
  }

  function revealAll() {
    qs(".oh-m-reveal").forEach(function (el) {
      el.classList.add("is-in");
    });
  }

  function resetReveals() {
    qs("[data-oh-sec-root]").forEach(function (el) {
      el.classList.remove("oh-m-section-in");
    });
    qs(".oh-m-reveal").forEach(function (el) {
      var sid = el.getAttribute("data-oh-sec");
      if (!motionOn() || (sid && !sectionOn(sid))) {
        el.classList.add("is-in");
        el.classList.add("oh-m-forced");
        return;
      }
      el.classList.remove("is-in", "oh-m-forced");
      void el.offsetWidth;
    });
  }

  function disconnectIo() {
    if (io) {
      io.disconnect();
      io = null;
    }
  }

  function staggerMs() {
    var level = getLevel();
    if (level === "bold") return 90;
    if (level === "light") return 50;
    return 70;
  }

  /** Play a section's reveals in --oh-m-i order (same moment start + CSS delay). */
  function playSection(sectionId) {
    if (!sectionOn(sectionId) || !motionOn()) {
      qs('.oh-m-reveal[data-oh-sec="' + sectionId + '"]').forEach(function (el) {
        el.classList.add("is-in");
      });
      return;
    }
    var root = document.querySelector('[data-oh-sec-root="' + sectionId + '"]');
    if (root) root.classList.add("oh-m-section-in");

    var items = qs('.oh-m-reveal[data-oh-sec="' + sectionId + '"]').filter(function (el) {
      // Skip hidden tab panels / aria-hidden slides so they don't burn stagger
      if (el.closest && el.closest("[hidden]")) return false;
      var slide = el.closest && el.closest(".oh-testimonial-slide");
      if (slide && slide.getAttribute("aria-hidden") === "true") return false;
      if (el.classList.contains("oh-m-forced")) return false;
      return true;
    });

    items.sort(function (a, b) {
      var ia = parseInt(a.style.getPropertyValue("--oh-m-i") || "0", 10);
      var ib = parseInt(b.style.getPropertyValue("--oh-m-i") || "0", 10);
      return ia - ib;
    });

    // Re-normalize visible sequence to 0..n so gaps from hidden panels don't delay cards oddly
    items.forEach(function (el, i) {
      el.style.setProperty("--oh-m-i", String(i));
      el.classList.add("is-in");
    });
  }

  function observerOptions() {
    var early = htmlEl().getAttribute(ATTR.early) === "1";
    // Trigger when section top enters — not when a lower card peeks first
    return early
      ? { root: null, rootMargin: "0px 0px -12% 0px", threshold: 0.08 }
      : { root: null, rootMargin: "0px 0px -22% 0px", threshold: 0.12 };
  }

  function observe() {
    disconnectIo();
    applySectionVisibility();

    if (!motionOn()) {
      revealAll();
      return;
    }

    var roots = qs("[data-oh-sec-root]").filter(function (el) {
      var id = el.getAttribute("data-oh-sec-root");
      return sectionOn(id) && !el.classList.contains("oh-m-section-in");
    });

    if (!roots.length) {
      // Still arm any orphan reveals
      return;
    }

    if (typeof IntersectionObserver !== "function") {
      roots.forEach(function (el) {
        playSection(el.getAttribute("data-oh-sec-root"));
      });
      return;
    }

    io = new IntersectionObserver(function (entries) {
      entries.forEach(function (entry) {
        if (!entry.isIntersecting) return;
        var id = entry.target.getAttribute("data-oh-sec-root");
        if (!id) return;
        playSection(id);
        io.unobserve(entry.target);
      });
    }, observerOptions());

    roots.forEach(function (el) {
      // If already largely in view (e.g. after toggle), play immediately
      var rect = el.getBoundingClientRect();
      var vh = window.innerHeight || 800;
      if (rect.top < vh * 0.75 && rect.bottom > vh * 0.1) {
        playSection(el.getAttribute("data-oh-sec-root"));
      } else {
        io.observe(el);
      }
    });
  }

  /** Restagger body cards only (after header already shown) */
  function restaggerIn(container) {
    if (!container || !motionOn()) return;
    var cards = qs(".oh-m-reveal", container).filter(function (el) {
      return !el.closest("[hidden]");
    });
    if (!cards.length) return;
    var sectionId = cards[0].getAttribute("data-oh-sec");
    if (sectionId && !sectionOn(sectionId)) return;

    // Body cards start after header indices (0 badge, 1 title, 2 lead → body from 3)
    var bodyStart = 3;
    cards.forEach(function (el, i) {
      el.classList.remove("is-in");
      el.style.setProperty("--oh-m-i", String(bodyStart + i));
      void el.offsetWidth;
    });
    var ms = staggerMs();
    cards.forEach(function (el, i) {
      window.setTimeout(function () {
        if (sectionOn(el.getAttribute("data-oh-sec"))) el.classList.add("is-in");
      }, 20 + i * ms);
    });
  }

  function wireSectionInteractions() {
    if (tabsWired) return;
    tabsWired = true;

    document.addEventListener(
      "click",
      function (ev) {
        var tab = ev.target.closest && ev.target.closest("#persp-tabs [role='tab']");
        if (tab) {
          window.setTimeout(function () {
            var panelId = tab.getAttribute("data-panel") || tab.getAttribute("aria-controls");
            var panel = panelId && document.getElementById(panelId);
            if (panel && sectionOn("perspectives")) restaggerIn(panel);
          }, 40);
          return;
        }

        var modTab = ev.target.closest && ev.target.closest("#modules-tab-outcomes, #modules-tab-platform");
        if (modTab && sectionOn("modules")) {
          window.setTimeout(function () {
            var outcomes = document.getElementById("modules-tab-outcomes");
            var isOutcomes = outcomes && outcomes.getAttribute("aria-selected") === "true";
            var panel = document.getElementById(
              isOutcomes ? "modules-panel-outcomes" : "modules-panel-platform"
            );
            if (panel) restaggerIn(panel);
          }, 40);
        }
      },
      true
    );
  }

  function bindScrollHints() {
    if (scrollBound) return;
    scrollBound = true;
    var onScroll = function () {
      var hero = document.getElementById("hero");
      var past = false;
      if (hero) {
        var rect = hero.getBoundingClientRect();
        past = rect.bottom < 80;
      }
      htmlEl().classList.toggle("oh-scrolled-past-hero", past);

      var doc = document.documentElement;
      var remaining = doc.scrollHeight - (window.scrollY + window.innerHeight);
      htmlEl().classList.toggle("oh-near-page-end", remaining < 160);
    };
    window.addEventListener("scroll", onScroll, { passive: true });
    onScroll();
  }

  function defaultSectionMap() {
    var map = {};
    SECTIONS.forEach(function (s) {
      map[s.id] = s.defaultOn ? "1" : "0";
    });
    return map;
  }

  function applyConfig(cfg) {
    cfg = cfg || {};
    var level = cfg.level != null ? cfg.level : getLevel();
    var style = cfg.style != null ? cfg.style : htmlEl().getAttribute(ATTR.style) || DEFAULTS.style;
    var cue = cfg.cue != null ? cfg.cue : htmlEl().getAttribute(ATTR.cue) || DEFAULTS.cue;
    var early = cfg.early != null ? cfg.early : htmlEl().getAttribute(ATTR.early) || DEFAULTS.early;
    var depth = cfg.depth != null ? cfg.depth : htmlEl().getAttribute(ATTR.depth) || DEFAULTS.depth;
    var edge = cfg.edge != null ? cfg.edge : htmlEl().getAttribute(ATTR.edge) || DEFAULTS.edge;

    htmlEl().setAttribute(ATTR.level, level);
    htmlEl().setAttribute(ATTR.style, style);
    htmlEl().setAttribute(ATTR.cue, cue);
    htmlEl().setAttribute(ATTR.early, early);
    htmlEl().setAttribute(ATTR.depth, depth);
    htmlEl().setAttribute(ATTR.edge, edge);

    var sections = cfg.sections || {};
    SECTIONS.forEach(function (s) {
      var val = sections[s.id] != null ? sections[s.id] : htmlEl().getAttribute(secAttr(s.id));
      if (val == null) val = s.defaultOn ? "1" : "0";
      htmlEl().setAttribute(secAttr(s.id), val === "1" || val === true ? "1" : "0");
    });

    applySectionVisibility();

    return getConfig();
  }

  function getConfig() {
    var sections = {};
    SECTIONS.forEach(function (s) {
      sections[s.id] = htmlEl().getAttribute(secAttr(s.id)) || (s.defaultOn ? "1" : "0");
    });
    return {
      level: htmlEl().getAttribute(ATTR.level) || DEFAULTS.level,
      style: htmlEl().getAttribute(ATTR.style) || DEFAULTS.style,
      cue: htmlEl().getAttribute(ATTR.cue) || DEFAULTS.cue,
      early: htmlEl().getAttribute(ATTR.early) || DEFAULTS.early,
      depth: htmlEl().getAttribute(ATTR.depth) || DEFAULTS.depth,
      edge: htmlEl().getAttribute(ATTR.edge) || DEFAULTS.edge,
      sections: sections,
    };
  }

  function replay() {
    try {
      window.scrollTo({ top: 0, left: 0, behavior: "auto" });
    } catch (err) {
      window.scrollTo(0, 0);
    }
    if (!motionOn()) {
      revealAll();
      return;
    }
    resetReveals();
    observe();
    bindScrollHints();
  }

  function init(root, cfg) {
    var merged = Object.assign({}, DEFAULTS, cfg || {});
    if (!merged.sections) merged.sections = defaultSectionMap();
    applyConfig(merged);
    decorate(root);
    if (!motionOn()) revealAll();
    else {
      resetReveals();
      observe();
    }
    bindScrollHints();
  }

  function setSection(id, on) {
    htmlEl().setAttribute(secAttr(id), on ? "1" : "0");
    var root = document.querySelector('[data-oh-sec-root="' + id + '"]');
    if (root) root.classList.remove("oh-m-section-in");
    applySectionVisibility();
    if (on && motionOn()) {
      qs('.oh-m-reveal[data-oh-sec="' + id + '"]').forEach(function (el) {
        el.classList.remove("is-in", "oh-m-forced");
        void el.offsetWidth;
      });
      observe();
    } else {
      qs('.oh-m-reveal[data-oh-sec="' + id + '"]').forEach(function (el) {
        el.classList.add("is-in", "oh-m-forced");
      });
      if (root) root.classList.add("oh-m-section-in");
    }
  }

  window.DealalityOldHomeMotionLab = {
    SECTIONS: SECTIONS,
    init: init,
    applyConfig: applyConfig,
    observe: observe,
    replay: replay,
    resetReveals: resetReveals,
    revealAll: revealAll,
    getConfig: getConfig,
    decorate: decorate,
    setSection: setSection,
    restaggerIn: restaggerIn,
  };

  window.DealalityOldHomeMotionLight = window.DealalityOldHomeMotionLab;
})();
