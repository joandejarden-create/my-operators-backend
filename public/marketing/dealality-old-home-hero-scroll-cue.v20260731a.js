/**
 * Old Home — hero tertiary scroll cue (v20260731a)
 * Path-gated to /old-home.
 * Quiet bottom-center link: "See How Dealality Works" → #oh-how-we-do-it
 * Does not alter Explore / Request a Demo CTAs.
 */
(function () {
  "use strict";

  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    if (path !== "/old-home") return;
    if (window.__ohHeroScrollCue && window.__ohHeroScrollCue >= 202607311) return;
    window.__ohHeroScrollCue = 202607311;

    var DEST_ID = "oh-how-we-do-it";
    var DEST_HASH = "#" + DEST_ID;
    var API_BASE =
      (window.DEALALITY_API_BASE ||
        "https://my-operators-backend-production.up.railway.app") +
      "/api/marketing/landing-events";
    var SESSION_KEY = "dl_landing_sid_v1";

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

    function injectCss() {
      if (document.getElementById("oh-hero-scroll-cue-css")) return;
      var css = document.createElement("style");
      css.id = "oh-hero-scroll-cue-css";
      css.textContent = [
        "#oh-how-we-do-it{scroll-margin-top:calc(72px + env(safe-area-inset-top,0px))!important}",
        "html{scroll-padding-top:calc(72px + env(safe-area-inset-top,0px))}",
        "#hero.oh-hero,#hero{position:relative!important}",
        "#hero #oh-hero-scroll-cue.oh-hero-scroll-cue,",
        "#oh-hero-scroll-cue.oh-hero-scroll-cue{",
        "position:absolute!important;",
        "left:50%!important;",
        "bottom:clamp(.55rem,1.8vh,1.05rem)!important;",
        "transform:translateX(-50%)!important;",
        "z-index:4!important;",
        "display:inline-flex!important;",
        "flex-direction:column!important;",
        "align-items:center!important;",
        "justify-content:center!important;",
        "gap:.2rem!important;",
        "margin:0!important;",
        "padding:.35rem .75rem!important;",
        "width:max-content!important;",
        "max-width:calc(100% - 1.5rem)!important;",
        "box-sizing:border-box!important;",
        "text-decoration:none!important;",
        "color:rgba(232,238,248,.58)!important;",
        'font-family:"Inter Tight","Plus Jakarta Sans",system-ui,sans-serif!important;',
        "font-size:.78rem!important;",
        "font-weight:500!important;",
        "letter-spacing:.02em!important;",
        "line-height:1.25!important;",
        "text-align:center!important;",
        "background:transparent!important;",
        "border:0!important;",
        "box-shadow:none!important;",
        "cursor:pointer!important;",
        "pointer-events:auto!important;",
        "transition:color .2s ease,opacity .2s ease,transform .2s ease!important",
        "}",
        "#hero.oh-hero.has-oh-scroll-cue{",
        "padding-bottom:clamp(3.25rem,7vh,4.25rem)!important",
        "}",
        /* Keep Watch Platform Overview above this cue when both are visible */
        "#hero.has-oh-scroll-cue > #fsw-secondary-wrap[data-oh-visible='1'],",
        "#hero.has-oh-scroll-cue #fsw-secondary-wrap[data-oh-visible='1']{",
        "bottom:clamp(2.55rem,5.4vh,3.35rem)!important",
        "}",
        "#oh-hero-scroll-cue .oh-hero-scroll-cue__label{",
        "display:block!important;",
        "white-space:nowrap!important",
        "}",
        "#oh-hero-scroll-cue .oh-hero-scroll-cue__arrow{",
        "display:block!important;",
        "font-size:.85rem!important;",
        "line-height:1!important;",
        "opacity:.78!important;",
        "transform:translateY(0)!important;",
        "transition:transform .2s ease,opacity .2s ease!important",
        "}",
        "#oh-hero-scroll-cue.oh-hero-scroll-cue:hover,",
        "#oh-hero-scroll-cue.oh-hero-scroll-cue:focus-visible{",
        "color:rgba(232,238,248,.88)!important",
        "}",
        "#oh-hero-scroll-cue.oh-hero-scroll-cue:hover .oh-hero-scroll-cue__arrow,",
        "#oh-hero-scroll-cue.oh-hero-scroll-cue:focus-visible .oh-hero-scroll-cue__arrow{",
        "opacity:1!important;",
        "transform:translateY(2px)!important",
        "}",
        "#oh-hero-scroll-cue.oh-hero-scroll-cue:focus-visible{",
        "outline:2px solid rgba(140,146,255,.85)!important;",
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
        "transform:none!important",
        "}",
        "html{scroll-behavior:auto!important}",
        "}",
        "@media (max-width:767px){",
        "#hero #oh-hero-scroll-cue.oh-hero-scroll-cue,",
        "#oh-hero-scroll-cue.oh-hero-scroll-cue{",
        "position:static!important;",
        "left:auto!important;",
        "bottom:auto!important;",
        "transform:none!important;",
        "display:flex!important;",
        "width:100%!important;",
        "max-width:100%!important;",
        "margin:1.15rem auto 0!important;",
        "padding:.25rem .5rem .1rem!important;",
        "font-size:.76rem!important",
        "}",
        "#hero.oh-hero.has-oh-scroll-cue{",
        "padding-bottom:clamp(1.4rem,4vh,2rem)!important",
        "}",
        "#hero.has-oh-scroll-cue > #fsw-secondary-wrap[data-oh-visible='1'],",
        "#hero.has-oh-scroll-cue #fsw-secondary-wrap[data-oh-visible='1']{",
        "bottom:auto!important;",
        "margin:1rem 0 0!important",
        "}",
        "#oh-hero-scroll-cue .oh-hero-scroll-cue__label{",
        "white-space:normal!important",
        "}",
        "}",
        /* One-time quiet entrance (no loop) */
        "@media (prefers-reduced-motion:no-preference){",
        "#oh-hero-scroll-cue.oh-hero-scroll-cue.is-enter{",
        "animation:oh-hero-scroll-cue-in .55s ease both",
        "}",
        "@keyframes oh-hero-scroll-cue-in{",
        "from{opacity:0;transform:translateX(-50%) translateY(6px)}",
        "to{opacity:1;transform:translateX(-50%) translateY(0)}",
        "}",
        "@media (max-width:767px){",
        "@keyframes oh-hero-scroll-cue-in{",
        "from{opacity:0;transform:translateY(6px)}",
        "to{opacity:1;transform:none}",
        "}",
        "}",
        "}",
      ].join("");
      (document.head || document.documentElement).appendChild(css);
    }

    function ensureCue() {
      var hero = document.getElementById("hero");
      if (!hero) return null;
      var existing = document.getElementById("oh-hero-scroll-cue");
      if (existing) {
        hero.classList.add("has-oh-scroll-cue");
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
        '<span class="oh-hero-scroll-cue__arrow" aria-hidden="true">↓</span>';

      // Keep after CTAs / secondary wrap so absolute bottom layering stays predictable.
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
        if (!target) return; // native hash still updates; section may mount async
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

      return a;
    }

    function boot() {
      injectCss();
      ensureCue();
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
