/**
 * Old Home — hero tertiary scroll cue (v20260731d)
 * Path-gated to /old-home.
 * Pins "See How Dealality Works" to the bottom of the user's viewport
 * while remaining inside #hero (absolute, not position:fixed).
 * Uses a V-shaped chevron (no stem), not ↓.
 * Does not alter Explore / Request a Demo CTAs.
 */
(function () {
  "use strict";

  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    if (path !== "/old-home") return;
    if (window.__ohHeroScrollCue && window.__ohHeroScrollCue >= 202607314) return;
    window.__ohHeroScrollCue = 202607314;

    var DEST_ID = "oh-how-we-do-it";
    var DEST_HASH = "#" + DEST_ID;
    var API_BASE =
      (window.DEALALITY_API_BASE ||
        "https://my-operators-backend-production.up.railway.app") +
      "/api/marketing/landing-events";
    var SESSION_KEY = "dl_landing_sid_v1";
    var PAD = 18; // gap from visible viewport bottom

    function sessionId() {
      try {
        var existing = sessionStorage.getItem(SESSION_KEY);
        if (existing) return existing;
        var id =
          "dl_" +
          Date.now().toString(36) +
          "_" +
          Math.random().toString(36).slice(2, 11);
        sessionStorage.setItem(SESSION_KEY, id);
        return id;
      } catch (_e) {
        return "dl_" + Date.now();
      }
    }

    function track(event, extra) {
      var payload = {
        event: event,
        sessionId: sessionId(),
        path: (window.location.pathname || "") + (window.location.search || ""),
        landingVersion: "old-home",
        section: "hero",
        element: "hero_scroll_cue",
      };
      if (extra && typeof extra === "object") {
        Object.keys(extra).forEach(function (k) {
          payload[k] = extra[k];
        });
      }
      try {
        if (navigator.sendBeacon) {
          navigator.sendBeacon(API_BASE, JSON.stringify(payload));
          return;
        }
      } catch (_e) {}
      try {
        fetch(API_BASE, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify(payload),
          keepalive: true,
        });
      } catch (_e2) {}
    }

    function prefersReducedMotion() {
      return (
        window.matchMedia &&
        window.matchMedia("(prefers-reduced-motion: reduce)").matches
      );
    }

    function viewportBottom() {
      try {
        if (window.visualViewport && window.visualViewport.height) {
          return window.visualViewport.offsetTop + window.visualViewport.height;
        }
      } catch (_e) {}
      return window.innerHeight || document.documentElement.clientHeight || 800;
    }

    function injectCss() {
      if (document.getElementById("oh-hero-scroll-cue-css")) return;
      var css = document.createElement("style");
      css.id = "oh-hero-scroll-cue-css";
      css.textContent = [
        "#oh-how-we-do-it{scroll-margin-top:calc(72px + env(safe-area-inset-top,0px))!important}",
        "html{scroll-padding-top:calc(72px + env(safe-area-inset-top,0px))}",
        "#hero.oh-hero.has-oh-scroll-cue,#hero.has-oh-scroll-cue{",
        "position:relative!important;",
        "overflow:visible!important",
        "}",
        "#hero #oh-hero-scroll-cue.oh-hero-scroll-cue,",
        "#oh-hero-scroll-cue.oh-hero-scroll-cue{",
        "position:absolute!important;",
        "left:50%!important;",
        "transform:translateX(-50%)!important;",
        "z-index:6!important;",
        "display:inline-flex!important;",
        "flex-direction:column!important;",
        "align-items:center!important;",
        "justify-content:center!important;",
        "gap:.22rem!important;",
        "width:max-content!important;",
        "max-width:calc(100% - 1.5rem)!important;",
        "box-sizing:border-box!important;",
        "margin:0!important;",
        "padding:.4rem .85rem!important;",
        "text-decoration:none!important;",
        "color:rgba(232,238,248,.82)!important;",
        'font-family:"Inter Tight","Plus Jakarta Sans",system-ui,sans-serif!important;',
        "font-size:.8rem!important;",
        "font-weight:500!important;",
        "letter-spacing:.02em!important;",
        "line-height:1.25!important;",
        "text-align:center!important;",
        "background:transparent!important;",
        "border:0!important;",
        "box-shadow:none!important;",
        "cursor:pointer!important;",
        "pointer-events:auto!important;",
        /* CSS fallback before JS measures: near viewport bottom accounting for sticky nav */
        "bottom:calc(0.85rem + env(safe-area-inset-bottom,0px))!important;",
        "top:auto!important;",
        "transition:color .2s ease,opacity .2s ease!important",
        "}",
        "#oh-hero-scroll-cue.oh-hero-scroll-cue.is-positioned{",
        "bottom:auto!important",
        "}",
        "#oh-hero-scroll-cue .oh-hero-scroll-cue__label{",
        "display:block!important;",
        "white-space:nowrap!important",
        "}",
        "#oh-hero-scroll-cue .oh-hero-scroll-cue__arrow{",
        "display:block!important;",
        "width:.72rem!important;",
        "height:.72rem!important;",
        "margin:.12rem auto 0!important;",
        "box-sizing:border-box!important;",
        "border-right:2px solid currentColor!important;",
        "border-bottom:2px solid currentColor!important;",
        "border-left:0!important;",
        "border-top:0!important;",
        "background:transparent!important;",
        "font-size:0!important;",
        "line-height:0!important;",
        "opacity:.9!important;",
        "transform:rotate(45deg) translateY(0)!important;",
        "transition:transform .2s ease,opacity .2s ease!important",
        "}",
        "#oh-hero-scroll-cue.oh-hero-scroll-cue:hover,",
        "#oh-hero-scroll-cue.oh-hero-scroll-cue:focus-visible{",
        "color:rgba(255,255,255,.96)!important",
        "}",
        "#oh-hero-scroll-cue.oh-hero-scroll-cue:hover .oh-hero-scroll-cue__arrow,",
        "#oh-hero-scroll-cue.oh-hero-scroll-cue:focus-visible .oh-hero-scroll-cue__arrow{",
        "opacity:1!important;",
        "transform:rotate(45deg) translateY(2px)!important",
        "}",
        "#oh-hero-scroll-cue.oh-hero-scroll-cue:focus-visible{",
        "outline:2px solid rgba(140,146,255,.9)!important;",
        "outline-offset:3px!important;",
        "border-radius:6px!important",
        "}",
        "@media (prefers-reduced-motion:reduce){",
        "#oh-hero-scroll-cue.oh-hero-scroll-cue,",
        "#oh-hero-scroll-cue .oh-hero-scroll-cue__arrow{",
        "transition:none!important",
        "}",
        "#oh-hero-scroll-cue.oh-hero-scroll-cue:hover .oh-hero-scroll-cue__arrow,",
        "#oh-hero-scroll-cue.oh-hero-scroll-cue:focus-visible .oh-hero-scroll-cue__arrow{",
        "transform:rotate(45deg)!important",
        "}",
        "html{scroll-behavior:auto!important}",
        "}",
        "@media (max-width:767px){",
        "#oh-hero-scroll-cue.oh-hero-scroll-cue{",
        "font-size:.76rem!important;",
        "padding:.3rem .55rem!important;",
        "max-width:calc(100% - 1rem)!important",
        "}",
        "#oh-hero-scroll-cue .oh-hero-scroll-cue__label{",
        "white-space:normal!important",
        "}",
        "}",
        "@media (prefers-reduced-motion:no-preference){",
        "#oh-hero-scroll-cue.oh-hero-scroll-cue.is-enter{",
        "animation:oh-hero-scroll-cue-in .55s ease both",
        "}",
        "@keyframes oh-hero-scroll-cue-in{",
        "from{opacity:0}",
        "to{opacity:1}",
        "}",
        "}",
      ].join("");
      (document.head || document.documentElement).appendChild(css);
    }

    function layoutCue(cue, hero) {
      if (!cue || !hero) return;
      var heroRect = hero.getBoundingClientRect();
      var cueH = cue.offsetHeight || 44;
      var safeBottom = 0;
      try {
        // env() isn’t readable from JS reliably; keep a small constant pad.
        safeBottom = 0;
      } catch (_e) {}
      var viewBottom = viewportBottom();
      // Place so the cue sits just above the visible viewport bottom.
      var top = viewBottom - PAD - safeBottom - cueH - heroRect.top;

      // Keep below CTAs when they would overlap.
      var cta =
        document.getElementById("form-subscribe-wrap") ||
        document.getElementById("fsw-cta");
      if (cta) {
        var ctaBottom = cta.getBoundingClientRect().bottom - heroRect.top + 10;
        if (ctaBottom > top) top = ctaBottom;
      }

      // Keep inside the hero box.
      var maxTop = Math.max(0, hero.clientHeight - cueH - 8);
      if (top > maxTop) top = maxTop;
      if (top < 8) top = 8;

      cue.style.top = Math.round(top) + "px";
      cue.style.bottom = "auto";
      cue.classList.add("is-positioned");
    }

    function ensureCue() {
      var hero = document.getElementById("hero");
      if (!hero) return null;

      var existing = document.getElementById("oh-hero-scroll-cue");
      if (existing) {
        var arrow = existing.querySelector(".oh-hero-scroll-cue__arrow");
        if (arrow) arrow.textContent = "";
        hero.classList.add("has-oh-scroll-cue");
        layoutCue(existing, hero);
        return existing;
      }

      var a = document.createElement("a");
      a.id = "oh-hero-scroll-cue";
      a.className = "oh-hero-scroll-cue";
      a.href = DEST_HASH;
      a.setAttribute(
        "aria-label",
        "See How Dealality Works — scroll to the How We Do It section"
      );
      a.innerHTML =
        '<span class="oh-hero-scroll-cue__label">See How Dealality Works</span>' +
        '<span class="oh-hero-scroll-cue__arrow" aria-hidden="true"></span>';

      hero.appendChild(a);
      hero.classList.add("has-oh-scroll-cue");

      if (!prefersReducedMotion()) {
        a.classList.add("is-enter");
        a.addEventListener(
          "animationend",
          function () {
            a.classList.remove("is-enter");
          },
          { once: true }
        );
      }

      a.addEventListener("click", function (e) {
        var target = document.getElementById(DEST_ID);
        track("hero_how_it_works_click", {
          destination: DEST_HASH,
          found: !!target,
        });
        if (!target) return;
        e.preventDefault();
        if (history && history.pushState) {
          try {
            history.pushState(null, "", DEST_HASH);
          } catch (_e) {
            location.hash = DEST_ID;
          }
        } else {
          location.hash = DEST_ID;
        }
        target.scrollIntoView({
          behavior: prefersReducedMotion() ? "auto" : "smooth",
          block: "start",
        });
      });

      layoutCue(a, hero);
      return a;
    }

    var layoutRaf = 0;
    function scheduleLayout() {
      if (layoutRaf) return;
      layoutRaf = window.requestAnimationFrame
        ? requestAnimationFrame(function () {
            layoutRaf = 0;
            var hero = document.getElementById("hero");
            var cue = document.getElementById("oh-hero-scroll-cue");
            layoutCue(cue, hero);
          })
        : (layoutRaf = setTimeout(function () {
            layoutRaf = 0;
            var hero = document.getElementById("hero");
            var cue = document.getElementById("oh-hero-scroll-cue");
            layoutCue(cue, hero);
          }, 16));
    }

    function boot() {
      injectCss();
      ensureCue();
      window.addEventListener("resize", scheduleLayout, { passive: true });
      window.addEventListener("orientationchange", scheduleLayout, {
        passive: true,
      });
      window.addEventListener("scroll", scheduleLayout, { passive: true });
      try {
        if (window.visualViewport) {
          window.visualViewport.addEventListener("resize", scheduleLayout, {
            passive: true,
          });
          window.visualViewport.addEventListener("scroll", scheduleLayout, {
            passive: true,
          });
        }
      } catch (_e) {}
      // Fonts / late hero-fit can shift layout.
      setTimeout(scheduleLayout, 120);
      setTimeout(scheduleLayout, 600);
      setTimeout(scheduleLayout, 1400);
    }

    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", boot);
    } else {
      boot();
    }
  } catch (err) {
    if (typeof console !== "undefined" && console.warn) {
      console.warn("[oh-hero-scroll-cue]", err);
    }
  }
})();
