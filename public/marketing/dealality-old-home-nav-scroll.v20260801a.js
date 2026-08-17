/**
 * Old Home — nav section scroll (v20260801a)
 * Path-gated to / and /old-home.
 *
 * Fixes hash-nav twitch: html { scroll-behavior:smooth } from
 * dealality-old-home-dark overshoots on native fragment navigation, then
 * corrects. Disable CSS smooth scroll and drive one programmatic scrollTo.
 */
(function () {
  try {
    var path =
      ((window.location && window.location.pathname) || "")
        .replace(/\/+$/, "")
        .toLowerCase() || "/";
    if (path !== "/" && path !== "/old-home") return;
    if (window.__ohNavScroll >= 202608011) return;
    window.__ohNavScroll = 202608011;

    var STYLE_ID = "oh-nav-scroll-01a";
    var SCROLL_IDS =
      "#oh-how-we-do-it,#modules,#many-futures,#ecosystem,#trust,#faq,#insights,#pricing,#cta-band,#about";

    function injectCss() {
      if (document.getElementById(STYLE_ID)) return;
      var st = document.createElement("style");
      st.id = STYLE_ID;
      st.textContent = [
        "html{scroll-behavior:auto!important;}",
        SCROLL_IDS + "{scroll-margin-top:var(--oh-nav-scroll-margin, 76px);}",
      ].join("");
      (document.head || document.documentElement).appendChild(st);
    }

    function navOffset() {
      var nav =
        document.getElementById("nav") ||
        document.querySelector(".oh-nav, .navbar-2, .w-nav");
      if (!nav) return 76;
      var h = nav.getBoundingClientRect().height;
      return Math.max(56, Math.ceil(h) + 8);
    }

    function syncMargin() {
      document.documentElement.style.setProperty(
        "--oh-nav-scroll-margin",
        navOffset() + "px"
      );
    }

    function targetY(el) {
      var y = Math.round(
        el.getBoundingClientRect().top + (window.pageYOffset || window.scrollY || 0) - navOffset()
      );
      return y < 0 ? 0 : y;
    }

    var token = 0;
    function scrollToEl(el) {
      if (!el) return false;
      syncMargin();
      var y = targetY(el);
      var my = ++token;
      try {
        window.scrollTo({ top: y, behavior: "smooth" });
      } catch (err) {
        window.scrollTo(0, y);
      }
      // Re-assert once after layout settles (images / late embeds) without a second smooth pass.
      window.setTimeout(function () {
        if (my !== token) return;
        var y2 = targetY(el);
        if (Math.abs((window.pageYOffset || window.scrollY || 0) - y2) > 24) {
          try {
            window.scrollTo({ top: y2, behavior: "auto" });
          } catch (err2) {
            window.scrollTo(0, y2);
          }
        }
      }, 500);
      return true;
    }

    function idFromHash(hash) {
      if (!hash || hash.charAt(0) !== "#") return "";
      try {
        return decodeURIComponent(hash.slice(1));
      } catch (err) {
        return hash.slice(1);
      }
    }

    function onClick(e) {
      if (e.defaultPrevented) return;
      if (e.button != null && e.button !== 0) return;
      if (e.metaKey || e.ctrlKey || e.shiftKey || e.altKey) return;
      var a = e.target && e.target.closest ? e.target.closest('a[href^="#"]') : null;
      if (!a) return;
      var href = a.getAttribute("href") || "";
      if (!href || href === "#" || href.indexOf("#") !== 0) return;
      var id = idFromHash(href);
      if (!id) return;
      var el = document.getElementById(id);
      if (!el) return;

      e.preventDefault();
      e.stopPropagation();
      try {
        if (history.pushState) history.pushState(null, "", href);
        else location.hash = href;
      } catch (err) {
        location.hash = href;
      }
      scrollToEl(el);
    }

    function onHashChange() {
      var id = idFromHash(location.hash || "");
      if (!id) return;
      var el = document.getElementById(id);
      if (el) scrollToEl(el);
    }

    function boot() {
      injectCss();
      syncMargin();
      document.addEventListener("click", onClick, true);
      window.addEventListener("hashchange", onHashChange);
      window.addEventListener("resize", syncMargin, { passive: true });
      if (location.hash && location.hash.length > 1) {
        window.setTimeout(function () {
          var el = document.getElementById(idFromHash(location.hash));
          if (el) scrollToEl(el);
        }, 50);
      }
    }

    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", boot);
    } else {
      boot();
    }
  } catch (err) {
    if (typeof console !== "undefined" && console.error) {
      console.error("[oh-nav-scroll]", err);
    }
  }
})();
