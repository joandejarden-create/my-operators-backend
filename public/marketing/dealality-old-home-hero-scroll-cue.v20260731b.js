/**
 * Old Home — hero tertiary scroll cue (v20260731b)
 * Path-gated to /old-home.
 * Quiet link under hero CTAs: "See How Dealality Works" → #oh-how-we-do-it
 * Placed in-flow after CTAs so it stays in the first viewport (hero is 100vh
 * below sticky nav, so absolute hero-bottom sits under the fold + overflow:hidden).
 * Does not alter Explore / Request a Demo CTAs.
 */
(function () {
  "use strict";

  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    if (path !== "/old-home") return;
    if (window.__ohHeroScrollCue && window.__ohHeroScrollCue >= 202607312) return;
    window.__ohHeroScrollCue = 202607312;

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
        "#hero.oh-hero.has-oh-scroll-cue,#hero.has-oh-scroll-cue{",
        "overflow:visible!important;",
        "padding-bottom:clamp(2.25rem,5.5vh,3.25rem)!important",
        "}",
        "#hero #oh-hero-scroll-cue.oh-hero-scroll-cue,",
        "#oh-hero-scroll-cue.oh-hero-scroll-cue{",
        "position:relative!important;",
        "display:flex!important;",
        "flex-direction:column!important;",
        "align-items:center!important;",
        "justify-content:center!important;",
        "gap:.22rem!important;",
        "width:max-content!important;",
        "max-width:calc(100% - 1.5rem)!important;",
        "box-sizing:border-box!important;",
        "margin:1.15rem auto 0!important;",
        "padding:.4rem .85rem!important;",
        "z-index:5!important;",
        "text-decoration:none!important;",
        "color:rgba(232,238,248,.78)!important;",
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
        "transition:color .2s ease,opacity .2s ease,transform .2s ease!important",
        "}",
        /* Keep Watch Platform Overview below cue when both are visible */
        "#hero.has-oh-scroll-cue > #fsw-secondary-wrap[data-oh-visible='1'],",
        "#hero.has-oh-scroll-cue #fsw-secondary-wrap[data-oh-visible='1']{",
        "position:relative!important;",
        "left:auto!important;",
        "bottom:auto!important;",
        "transform:none!important;",
        "display:block!important;",
        "width:100%!important;",
        "margin:0.85rem auto 0!important;",
        "text-align:center!important",
        "}",
        "#oh-hero-scroll-cue .oh-hero-scroll-cue__label{",
        "display:block!important;",
        "white-space:nowrap!important",
        "}",
        "#oh-hero-scroll-cue .oh-hero-scroll-cue__arrow{",
        "display:block!important;",
        "font-size:.9rem!important;",
        "line-height:1!important;",
        "opacity:.85!important;",
        "transform:translateY(0)!important;",
        "transition:transform .2s ease,opacity .2s ease!important",
        "}",
        "#oh-hero-scroll-cue.oh-hero-scroll-cue:hover,",
        "#oh-hero-scroll-cue.oh-hero-scroll-cue:focus-visible{",
        "color:rgba(232,238,248,.96)!important",
        "}",
        "#oh-hero-scroll-cue.oh-hero-scroll-cue:hover .oh-hero-scroll-cue__arrow,",
        "#oh-hero-scroll-cue.oh-hero-scroll-cue:focus-visible .oh-hero-scroll-cue__arrow{",
        "opacity:1!important;",
        "transform:translateY(2px)!important",
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
        "transform:none!important",
        "}",
        "html{scroll-behavior:auto!important}",
        "}",
        "@media (max-width:767px){",
        "#oh-hero-scroll-cue.oh-hero-scroll-cue{",
        "margin:1rem auto 0!important;",
        "padding:.3rem .5rem!important;",
        "font-size:.76rem!important;",
        "max-width:100%!important",
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
        "from{opacity:0;transform:translateY(6px)}",
        "to{opacity:1;transform:none}",
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
        placeCue(existing, hero);
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

      placeCue(a, hero);
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

      return a;
    }

    function placeCue(a, hero) {
      var cta =
        document.getElementById("form-subscribe-wrap") ||
        document.getElementById("fsw-cta") ||
        hero.querySelector(".oh-fsw-wrap");
      var secondary = document.getElementById("fsw-secondary-wrap");
      if (cta && cta.parentNode) {
        // Sit directly under the CTA group, before the secondary reopen link.
        if (secondary && secondary.parentNode === cta.parentNode) {
          cta.parentNode.insertBefore(a, secondary);
        } else if (cta.nextSibling) {
          cta.parentNode.insertBefore(a, cta.nextSibling);
        } else {
          cta.parentNode.appendChild(a);
        }
      } else {
        hero.appendChild(a);
      }
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
