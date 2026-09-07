/**
 * Old Home Many Futures — mobile open scroll (v20260801a)
 * Path-gated to / and /old-home.
 *
 * On mobile, selecting a question relocates the answer workspace after that
 * button. Collapsing a previous panel above the click point jumps the
 * document so the question ends above the viewport. After selection, scroll
 * so the question top sits under the sticky nav — read by scrolling down.
 */
(function () {
  "use strict";

  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    if (path !== "/" && path !== "/old-home") return;
    if (window.__ohMfMobileScroll >= 202608011) return;
  } catch (ePath) {
    return;
  }

  var MOBILE_MQ = "(max-width: 767px)";
  var BOUND = "data-oh-mf-scroll";

  function isMobile() {
    try {
      return window.matchMedia(MOBILE_MQ).matches;
    } catch (eM) {
      return window.innerWidth <= 767;
    }
  }

  function stickyNavOffset() {
    var nav = document.getElementById("nav");
    if (!nav) return 72;
    var h = Math.round(nav.getBoundingClientRect().height || 0);
    return Math.max(56, h + 8);
  }

  function preferReducedMotion() {
    try {
      return window.matchMedia("(prefers-reduced-motion: reduce)").matches;
    } catch (eR) {
      return false;
    }
  }

  function injectCss() {
    if (document.getElementById("oh-mf-mobile-scroll-css")) return;
    var css = document.createElement("style");
    css.id = "oh-mf-mobile-scroll-css";
    css.textContent = [
      "@media (max-width:767px){",
      "#dealality-many-futures .mf-q{",
      "scroll-margin-top:72px",
      "}}",
    ].join("");
    (document.head || document.documentElement).appendChild(css);
  }

  function scrollQuestionIntoView(btn) {
    if (!btn || !isMobile()) return;
    var behavior = preferReducedMotion() ? "auto" : "smooth";
    var run = function () {
      if (!btn.isConnected || !isMobile()) return;
      if (!btn.classList.contains("is-active")) return;
      var y =
        btn.getBoundingClientRect().top +
        (window.pageYOffset || document.documentElement.scrollTop || 0) -
        stickyNavOffset();
      if (typeof window.scrollTo === "function") {
        try {
          window.scrollTo({ top: Math.max(0, y), behavior: behavior });
          return;
        } catch (eS) {}
      }
      window.scrollTo(0, Math.max(0, y));
    };

    /* Wait for Many Futures placeMobilePanel DOM move + layout reflow. */
    window.requestAnimationFrame(function () {
      window.requestAnimationFrame(function () {
        run();
        /* Second pass after panel height settles under sticky chrome. */
        window.setTimeout(run, 120);
      });
    });
  }

  function questionFromEvent(e) {
    if (!e || !e.target || !e.target.closest) return null;
    var btn = e.target.closest(".mf-q[data-q]");
    if (!btn) return null;
    var root = document.getElementById("dealality-many-futures");
    if (!root || !root.contains(btn)) return null;
    return btn;
  }

  function onActivate(e) {
    var btn = questionFromEvent(e);
    if (!btn) return;
    if (e.type === "keydown" && e.key !== "Enter" && e.key !== " ") return;
    scrollQuestionIntoView(btn);
  }

  function bind() {
    if (document.documentElement.getAttribute(BOUND) === "1") return;
    document.documentElement.setAttribute(BOUND, "1");
    injectCss();
    document.addEventListener("click", onActivate, false);
    document.addEventListener("keydown", onActivate, false);
    window.__ohMfMobileScroll = 202608011;
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bind);
  } else {
    bind();
  }
})();
