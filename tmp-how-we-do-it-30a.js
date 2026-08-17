/**
 * Old Home — How We Do It (v20260730a)
 * Path-gated to /old-home. Injects scroll-synced timeline under #features (Process).
 * Gobiz pattern: sticky numbered trail + progress fill + active step panel while section scrolls.
 * Mobile: stacked cards (no horizontal scroll). Process section left in place.
 */
(function () {
  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    if (path !== "/old-home") return;
    if (window.__ohHowWeDoIt >= 202607301) return;
    window.__ohHowWeDoIt = 202607301;

    var CSS_HREF =
      "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a6a7d1c4dc6e94f40f5e4c6_dealality-old-home-how-we-do-it.v20260730a.css";
    var IMG_BRAND =
      "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a679518a66ce83bcb18be55_brand-explorer.png";
    var IMG_COMPARE =
      "https://cdn.prod.website-files.com/68108c29063eeb5d1bd7ae4a/6a679519982910d5c314bf1f_deal-compare.png";

    var STEPS = [
      {
        n: "01",
        title: "Explore credible paths",
        body: "Identify brand, operator, conversion, and partner alternatives worth evaluating.",
        media: "img",
        src: IMG_BRAND,
        alt: "Exploring credible strategic paths",
      },
      {
        n: "02",
        title: "Prepare the opportunity",
        body: "Organize the property story, constraints, and decision criteria.",
        media: "img",
        src: IMG_COMPARE,
        alt: "Preparing and comparing opportunity responses",
      },
      {
        n: "03",
        title: "Engage counterparties",
        body: "Bring the right parties into a controlled process.",
        media: "img",
        src: IMG_BRAND,
        alt: "Engaging the right counterparties",
      },
      {
        n: "04",
        title: "Compare the trade-offs",
        body: "Evaluate commercial and strategic differences side by side.",
        media: "img",
        src: IMG_COMPARE,
        alt: "Comparing commercial and strategic trade-offs",
      },
      {
        n: "05",
        title: "Make the decision",
        body: "Advance with documented rationale and clear priorities.",
        media: "decision",
        decisionTitle: "Selected path",
        decisionBody:
          "Documented rationale, unresolved terms, and negotiation priorities stay with the opportunity.",
      },
    ];

    function ensureCss() {
      if (document.querySelector('link[data-oh-how="1"]')) return;
      var link = document.createElement("link");
      link.rel = "stylesheet";
      link.href = CSS_HREF;
      link.setAttribute("data-oh-how", "1");
      (document.head || document.documentElement).appendChild(link);
    }

    function esc(s) {
      return String(s || "")
        .replace(/&/g, "&amp;")
        .replace(/</g, "&lt;")
        .replace(/>/g, "&gt;")
        .replace(/"/g, "&quot;");
    }

    function mediaHtml(step) {
      if (step.media === "decision") {
        return (
          '<div class="oh-how-decision">' +
          "<strong>" +
          esc(step.decisionTitle) +
          "</strong>" +
          "<p>" +
          esc(step.decisionBody) +
          "</p>" +
          "</div>"
        );
      }
      return (
        '<div class="oh-how-media"><img src="' +
        esc(step.src) +
        '" alt="' +
        esc(step.alt) +
        '" loading="lazy" width="640" height="400"></div>'
      );
    }

    function buildSection() {
      var dots = STEPS.map(function (step, i) {
        return (
          '<button type="button" class="oh-how-dot' +
          (i === 0 ? " is-active" : "") +
          '" data-oh-how-step="' +
          i +
          '" aria-controls="oh-how-panel-' +
          (i + 1) +
          '" aria-pressed="' +
          (i === 0 ? "true" : "false") +
          '">' +
          '<span class="oh-how-num" aria-hidden="true">' +
          esc(step.n) +
          "</span>" +
          '<span class="oh-how-dot-label">' +
          esc(step.title) +
          "</span>" +
          "</button>"
        );
      }).join("");

      var panels = STEPS.map(function (step, i) {
        return (
          '<article class="oh-how-panel' +
          (i === 0 ? " is-active" : "") +
          '" id="oh-how-panel-' +
          (i + 1) +
          '" data-oh-how-panel="' +
          i +
          '"' +
          (i === 0 ? "" : " hidden") +
          ">" +
          mediaHtml(step) +
          '<div class="oh-how-copy">' +
          '<p class="oh-how-step-kicker">Step ' +
          esc(step.n) +
          "</p>" +
          '<h3 class="oh-how-step-title">' +
          esc(step.title) +
          "</h3>" +
          '<p class="oh-how-step-body">' +
          esc(step.body) +
          "</p>" +
          "</div>" +
          "</article>"
        );
      }).join("");

      return (
        '<section id="oh-how-we-do-it" class="oh-how" aria-labelledby="oh-how-h2" style="--oh-how-steps:' +
        STEPS.length +
        '">' +
        '<div class="oh-how-glow" aria-hidden="true"></div>' +
        '<div class="oh-how-inner">' +
        '<div class="oh-how-head">' +
        "<div>" +
        '<p class="oh-how-badge">How Dealality Works</p>' +
        '<h2 class="oh-how-h2" id="oh-how-h2">How We Do It</h2>' +
        "</div>" +
        '<p class="oh-how-lead">Dealality brings the strategic paths, market responses, commercial terms, and decision criteria around one hotel opportunity into a single structured process.</p>' +
        "</div>" +
        '<div class="oh-how-runway" id="oh-how-runway">' +
        '<div class="oh-how-stage">' +
        '<div class="oh-how-trail" role="tablist" aria-label="Dealality process steps">' +
        '<div class="oh-how-trail-line" aria-hidden="true"><div class="oh-how-trail-progress" id="oh-how-progress"></div></div>' +
        dots +
        "</div>" +
        '<div class="oh-how-panels" id="oh-how-panels">' +
        panels +
        "</div>" +
        "</div>" +
        "</div>" +
        '<p class="oh-how-sr" id="oh-how-live" aria-live="polite"></p>' +
        "</div>" +
        "</section>"
      );
    }

    function mount() {
      if (document.getElementById("oh-how-we-do-it")) return true;
      var features = document.getElementById("features");
      if (!features || !features.parentNode) return false;
      var wrap = document.createElement("div");
      wrap.innerHTML = buildSection();
      features.parentNode.insertBefore(wrap.firstChild, features.nextSibling);
      return true;
    }

    function bind(section) {
      if (!section || section.getAttribute("data-oh-how-bound") === "1") return;
      section.setAttribute("data-oh-how-bound", "1");

      var runway = section.querySelector("#oh-how-runway");
      var dots = Array.prototype.slice.call(
        section.querySelectorAll("[data-oh-how-step]")
      );
      var panels = Array.prototype.slice.call(
        section.querySelectorAll("[data-oh-how-panel]")
      );
      var progress = section.querySelector("#oh-how-progress");
      var live = section.querySelector("#oh-how-live");
      var active = 0;
      var mq = window.matchMedia("(max-width: 960px)");
      var reduceMotion = window.matchMedia("(prefers-reduced-motion: reduce)");

      function setActive(index, fromUser) {
        if (index < 0 || index >= STEPS.length) return;
        active = index;
        dots.forEach(function (dot, i) {
          var on = i === active;
          var done = i < active;
          dot.classList.toggle("is-active", on);
          dot.classList.toggle("is-done", done);
          dot.setAttribute("aria-pressed", on ? "true" : "false");
        });
        if (mq.matches || reduceMotion.matches) {
          panels.forEach(function (panel) {
            panel.classList.add("is-active");
            panel.removeAttribute("hidden");
          });
        } else {
          panels.forEach(function (panel, i) {
            var on = i === active;
            panel.classList.toggle("is-active", on);
            if (on) panel.removeAttribute("hidden");
            else panel.setAttribute("hidden", "");
          });
        }
        if (progress) {
          var pct =
            STEPS.length <= 1
              ? 100
              : Math.round((active / (STEPS.length - 1)) * 100);
          progress.style.height = pct + "%";
        }
        if (live) {
          live.textContent =
            "Step " + STEPS[active].n + ": " + STEPS[active].title;
        }
        if (fromUser && typeof window.gtag === "function") {
          try {
            window.gtag("event", "oh_how_step", {
              step: STEPS[active].n,
              title: STEPS[active].title,
            });
          } catch (_e) {}
        }
      }

      function scrollToStep(index) {
        if (!runway || mq.matches || reduceMotion.matches) {
          setActive(index, true);
          return;
        }
        var rect = runway.getBoundingClientRect();
        var top = window.pageYOffset + rect.top;
        var usable = Math.max(1, runway.offsetHeight - window.innerHeight * 0.35);
        var target =
          top + (index / Math.max(1, STEPS.length - 1)) * usable;
        window.scrollTo({ top: target, behavior: "smooth" });
        setActive(index, true);
      }

      dots.forEach(function (dot) {
        dot.addEventListener("click", function () {
          var idx = parseInt(dot.getAttribute("data-oh-how-step"), 10);
          if (!isNaN(idx)) scrollToStep(idx);
        });
      });

      function syncFromScroll() {
        if (mq.matches || reduceMotion.matches) {
          setActive(0, false);
          return;
        }
        if (!runway) return;
        var rect = runway.getBoundingClientRect();
        var vh = window.innerHeight || 800;
        var total = Math.max(1, runway.offsetHeight - vh * 0.35);
        var scrolled = Math.min(total, Math.max(0, -rect.top + vh * 0.2));
        var ratio = scrolled / total;
        var idx = Math.min(
          STEPS.length - 1,
          Math.max(0, Math.round(ratio * (STEPS.length - 1)))
        );
        if (progress) progress.style.height = Math.round(ratio * 100) + "%";
        if (idx !== active) setActive(idx, false);
      }

      var raf = 0;
      function onScroll() {
        if (raf) return;
        raf = window.requestAnimationFrame(function () {
          raf = 0;
          syncFromScroll();
        });
      }

      window.addEventListener("scroll", onScroll, { passive: true });
      window.addEventListener("resize", onScroll);
      if (typeof mq.addEventListener === "function") {
        mq.addEventListener("change", function () {
          setActive(active, false);
          syncFromScroll();
        });
      }

      setActive(0, false);
      syncFromScroll();
    }

    function run() {
      ensureCss();
      if (!mount()) return;
      bind(document.getElementById("oh-how-we-do-it"));
    }

    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", run);
    } else {
      run();
    }
    window.addEventListener("load", run);
    setTimeout(run, 900);
    setTimeout(run, 2200);
  } catch (err) {
    if (typeof console !== "undefined" && console.warn) {
      console.warn("[oh-how-we-do-it]", err);
    }
  }
})();
