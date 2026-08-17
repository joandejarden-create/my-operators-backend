/**
 * Old Home — hero tertiary scroll cue (v20260801a)
 * Path-gated to /old-home.
 * Pins "See How Dealality Works" to the bottom of the user's viewport
 * while remaining inside #hero (absolute, not position:fixed).
 * Uses a V-shaped chevron (no stem), not ↓.
 * Does not alter Explore / Request a Demo CTAs.
 *
 * 01a: one-shot smooth scroll — lock layout during programmatic scroll,
 * stop scroll-listener thrash (hero shake), compute target once,
 * set hash after scroll settles (no hash/scroll fight).
 */
(function () {
  "use strict";

  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    if (path !== "/old-home") return;
    if (window.__ohHeroScrollCue && window.__ohHeroScrollCue >= 202608011) return;
    window.__ohHeroScrollCue = 202608011;

    var DEST_ID = "oh-how-we-do-it";
    var DEST_HASH = "#" + DEST_ID;
    var API_BASE =
      (window.DEALALITY_API_BASE ||
        "https://my-operators-backend-production.up.railway.app") +
      "/api/marketing/landing-events";
    var SESSION_KEY = "dl_landing_sid_v1";
    var PAD = 18;
    var NAV_OFFSET = 72;
    var scrollingProgrammatic = false;
    var scrollUnlockTimer = 0;

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

    function navOffsetPx() {
      try {
        var nav =
          document.querySelector(".navbar") ||
          document.querySelector("[data-nav]") ||
          document.querySelector("nav");
        if (nav) {
          var h = Math.round(nav.getBoundingClientRect().height);
          if (h > 40 && h < 120) return h;
        }
      } catch (_e) {}
      return NAV_OFFSET;
    }

    function injectCss() {
      if (document.getElementById("oh-hero-scroll-cue-css")) return;
      var css = document.createElement("style");
      css.id = "oh-hero-scroll-cue-css";
      css.textContent = [
        "#oh-how-we-do-it{scroll-margin-top:calc(72px + env(safe-area-inset-top,0px))!important}",
        "#hero.oh-hero.has-oh-scroll-cue,#hero.has-oh-scroll-cue{",
        "position:relative!important;",
        "overflow:hidden!important",
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
        "bottom:auto!important;",
        "top:var(--oh-cue-top,0)!important;",
        "opacity:0!important;",
        "visibility:hidden!important;",
        "pointer-events:none!important;",
        "transition:color .2s ease,opacity .2s ease!important",
        "}",
        "#hero #oh-hero-scroll-cue.oh-hero-scroll-cue.is-positioned,",
        "#oh-hero-scroll-cue.oh-hero-scroll-cue.is-positioned{",
        "visibility:visible!important;",
        "pointer-events:auto!important;",
        "opacity:0!important",
        "}",
        "#hero #oh-hero-scroll-cue.oh-hero-scroll-cue.is-positioned.is-shown,",
        "#oh-hero-scroll-cue.oh-hero-scroll-cue.is-positioned.is-shown{",
        "opacity:1!important",
        "}",
        "#hero #oh-hero-scroll-cue.oh-hero-scroll-cue.is-positioned.is-enter,",
        "#oh-hero-scroll-cue.oh-hero-scroll-cue.is-positioned.is-enter{",
        "opacity:0",
        "}",
        "#hero #oh-hero-scroll-cue.oh-hero-scroll-cue.is-scrolling,",
        "#oh-hero-scroll-cue.oh-hero-scroll-cue.is-scrolling{",
        "opacity:0!important;",
        "pointer-events:none!important;",
        "transition:opacity .15s ease!important",
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
      if (scrollingProgrammatic) return;

      var heroRect = hero.getBoundingClientRect();
      // Hero fully above viewport — stop pinning (avoids mid-scroll thrash).
      if (heroRect.bottom <= 0) return;

      var cueH = cue.offsetHeight || 48;
      var viewBottom = viewportBottom();
      var top = Math.round(viewBottom - PAD - cueH - heroRect.top);
      var maxInHero = Math.max(0, Math.round(hero.clientHeight - cueH - 8));

      // Hard clamp inside hero — never expand past hero height.
      if (top > maxInHero) top = maxInHero;
      if (top < 8) top = 8;

      cue.style.setProperty("--oh-cue-top", top + "px");
      cue.classList.add("is-positioned");
    }

    function setHashQuiet(hash) {
      try {
        if (history && history.replaceState) {
          history.replaceState(null, "", hash);
          return;
        }
      } catch (_e) {}
      // Avoid location.hash — it forces an instant jump that fights smooth scroll.
    }

    function unlockScrollLayout() {
      scrollingProgrammatic = false;
      var cue = document.getElementById("oh-hero-scroll-cue");
      if (cue) cue.classList.remove("is-scrolling");
      scheduleLayout();
    }

    function smoothScrollToTarget(target) {
      var y =
        Math.round(
          target.getBoundingClientRect().top +
            (window.pageYOffset || window.scrollY || 0)
        ) - navOffsetPx();
      if (y < 0) y = 0;

      scrollingProgrammatic = true;
      var cue = document.getElementById("oh-hero-scroll-cue");
      if (cue) cue.classList.add("is-scrolling");
      if (scrollUnlockTimer) {
        clearTimeout(scrollUnlockTimer);
        scrollUnlockTimer = 0;
      }

      var reduced = prefersReducedMotion();
      try {
        window.scrollTo({
          top: y,
          left: 0,
          behavior: reduced ? "auto" : "smooth",
        });
      } catch (_e) {
        window.scrollTo(0, y);
      }

      // Unlock after the smooth scroll should have finished.
      var unlockMs = reduced ? 50 : 900;
      scrollUnlockTimer = setTimeout(function () {
        scrollUnlockTimer = 0;
        // If still settling, wait one more beat near target.
        var cur = window.pageYOffset || window.scrollY || 0;
        if (!reduced && Math.abs(cur - y) > 8) {
          scrollUnlockTimer = setTimeout(unlockScrollLayout, 400);
          return;
        }
        unlockScrollLayout();
      }, unlockMs);
    }

    function onCueClick(e) {
      var target = document.getElementById(DEST_ID);
      track("hero_how_it_works_click", {
        destination: DEST_HASH,
        found: !!target,
      });
      if (!target) return;

      e.preventDefault();
      e.stopPropagation();
      if (typeof e.stopImmediatePropagation === "function") {
        e.stopImmediatePropagation();
      }

      // Hash after starting scroll — never before (avoids jump+smooth fight).
      smoothScrollToTarget(target);
      setHashQuiet(DEST_HASH);
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
        if (existing.getAttribute("data-oh-cue-click") !== "1") {
          existing.setAttribute("data-oh-cue-click", "1");
          existing.addEventListener("click", onCueClick, true);
        }
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
      a.setAttribute("data-oh-cue-click", "1");
      a.addEventListener("click", onCueClick, true);

      function startEnter() {
        if (a.dataset.ohEnterStarted === "1") return;
        if (!a.classList.contains("is-positioned")) return;
        a.dataset.ohEnterStarted = "1";
        if (prefersReducedMotion()) {
          a.classList.add("is-shown");
          return;
        }
        a.classList.add("is-enter");
        a.addEventListener(
          "animationend",
          function () {
            a.classList.remove("is-enter");
            a.classList.add("is-shown");
          },
          { once: true }
        );
        setTimeout(function () {
          a.classList.remove("is-enter");
          a.classList.add("is-shown");
        }, 700);
      }

      function revealCue() {
        layoutCue(a, hero);
        requestAnimationFrame(function () {
          layoutCue(a, hero);
          requestAnimationFrame(startEnter);
        });
      }

      if (document.documentElement.classList.contains("oh-ready")) {
        revealCue();
      } else {
        var enterObs = new MutationObserver(function () {
          if (document.documentElement.classList.contains("oh-ready")) {
            enterObs.disconnect();
            revealCue();
          }
        });
        enterObs.observe(document.documentElement, {
          attributes: true,
          attributeFilter: ["class"],
        });
        setTimeout(function () {
          try {
            enterObs.disconnect();
          } catch (_e) {}
          revealCue();
        }, 1800);
      }

      return a;
    }

    var layoutRaf = 0;
    function scheduleLayout() {
      if (scrollingProgrammatic) return;
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

    function wireLayout() {
      window.addEventListener("resize", scheduleLayout, { passive: true });
      window.addEventListener("orientationchange", scheduleLayout, {
        passive: true,
      });
      // Only re-pin while the user is idle in the hero — not during cue scroll.
      window.addEventListener(
        "scroll",
        function () {
          if (scrollingProgrammatic) return;
          scheduleLayout();
        },
        { passive: true }
      );
      try {
        if (window.visualViewport) {
          window.visualViewport.addEventListener("resize", scheduleLayout, {
            passive: true,
          });
        }
      } catch (_e) {}
      setTimeout(scheduleLayout, 320);
    }

    function mountCue() {
      ensureCue();
      wireLayout();
    }

    function boot() {
      injectCss();
      if (
        document.documentElement.classList.contains("oh-boot") &&
        !document.documentElement.classList.contains("oh-ready")
      ) {
        var bootObs = new MutationObserver(function () {
          if (document.documentElement.classList.contains("oh-ready")) {
            bootObs.disconnect();
            mountCue();
          }
        });
        bootObs.observe(document.documentElement, {
          attributes: true,
          attributeFilter: ["class"],
        });
        setTimeout(function () {
          try {
            bootObs.disconnect();
          } catch (_e) {}
          mountCue();
        }, 1800);
        return;
      }
      mountCue();
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
