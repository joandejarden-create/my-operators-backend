/**
 * Old Home — nav section scroll (v20260801b)
 * Path-gated to / and /old-home.
 *
 * 01b: Match hero "See How Dealality Works" landing —
 *      aim at section eyebrow/badges (not padded section top),
 *      offset = sticky/fixed chrome only + 8px gap (not full #nav when sticky is broken).
 * 01a: kill CSS smooth-hash overshoot; one programmatic scrollTo.
 */
(function () {
  try {
    var path =
      ((window.location && window.location.pathname) || "")
        .replace(/\/+$/, "")
        .toLowerCase() || "/";
    if (path !== "/" && path !== "/old-home") return;
    if (window.__ohNavScroll >= 202608012) return;
    window.__ohNavScroll = 202608012;

    var STYLE_ID = "oh-nav-scroll-01b";
    var EYEBROW_GAP_PX = 8;
    var SCROLL_IDS =
      "#oh-how-we-do-it,#modules,#many-futures,#ecosystem,#trust,#faq,#insights,#pricing,#cta-band,#about";

    function safeAreaTopPx() {
      try {
        var probe = document.createElement("div");
        probe.style.cssText =
          "position:absolute;visibility:hidden;top:0;left:0;" +
          "padding-top:env(safe-area-inset-top,0px)";
        (document.body || document.documentElement).appendChild(probe);
        var px = parseFloat(getComputedStyle(probe).paddingTop) || 0;
        probe.parentNode.removeChild(probe);
        return Math.round(px);
      } catch (_e) {
        return 0;
      }
    }

    /**
     * Only count chrome that actually sticks/fixes at the top of the viewport.
     * Measuring #nav height when position is relative (sticky broken) over-offsets.
     * Same contract as dealality-old-home-hero-scroll-cue.
     */
    function stickyChromeOffsetPx() {
      var best = 0;
      try {
        var nodes = document.querySelectorAll(
          "#nav, .oh-nav, .navbar, .navbar-2, [data-nav], nav, header, [class*='nav']"
        );
        for (var i = 0; i < nodes.length; i++) {
          var el = nodes[i];
          var cs = getComputedStyle(el);
          var pos = cs.position;
          if (pos !== "fixed" && pos !== "sticky") continue;
          var r = el.getBoundingClientRect();
          if (r.height < 32 || r.height > 140) continue;
          if (r.width < 120) continue;
          if (r.top > 8 || r.bottom < 24) continue;
          var h = Math.round(Math.min(r.bottom, r.height));
          if (h > best) best = h;
        }
      } catch (_e) {}
      return best;
    }

    function scrollOffsetPx() {
      // Prefer shared cue offset when hero script already computed it.
      try {
        var shared = getComputedStyle(document.documentElement).getPropertyValue(
          "--oh-scroll-cue-offset"
        );
        var n = parseFloat(shared);
        if (!isNaN(n) && n >= 0 && n < 160) return Math.round(n);
      } catch (_e) {}
      return stickyChromeOffsetPx() + EYEBROW_GAP_PX + safeAreaTopPx();
    }

    function injectCss() {
      var existing = document.getElementById(STYLE_ID);
      if (existing) existing.parentNode.removeChild(existing);
      var st = document.createElement("style");
      st.id = STYLE_ID;
      var off = scrollOffsetPx();
      st.textContent = [
        "html{scroll-behavior:auto!important;}",
        ":root{--oh-nav-scroll-margin:" + off + "px;}",
        SCROLL_IDS + "{scroll-margin-top:var(--oh-nav-scroll-margin)!important;}",
        SCROLL_IDS + " [id$='-badges']," +
          SCROLL_IDS + " .oh-insights-badges," +
          SCROLL_IDS + " .dealality-process_eyebrow," +
          SCROLL_IDS + " [class*='eyebrow']{" +
          "scroll-margin-top:var(--oh-nav-scroll-margin)!important;}",
      ].join("");
      (document.head || document.documentElement).appendChild(st);
      try {
        document.documentElement.style.setProperty(
          "--oh-nav-scroll-margin",
          off + "px"
        );
      } catch (_e2) {}
    }

    function syncMargin() {
      var off = scrollOffsetPx();
      try {
        document.documentElement.style.setProperty(
          "--oh-nav-scroll-margin",
          off + "px"
        );
      } catch (_e) {}
      var el = document.getElementById(STYLE_ID);
      if (el) {
        el.textContent = el.textContent.replace(
          /(--oh-nav-scroll-margin:\s*)\d+px/g,
          "$1" + off + "px"
        );
      }
    }

    /** Prefer eyebrow / badges so section padding is not the first paint under nav. */
    function resolveScrollTarget(section) {
      if (!section) return null;
      return (
        section.querySelector(".dealality-process_eyebrow") ||
        section.querySelector("[class*='eyebrow']") ||
        section.querySelector("[id$='-badges']") ||
        section.querySelector(".oh-insights-badges") ||
        section.querySelector("#dealality-process-h2") ||
        section.querySelector("h2") ||
        section
      );
    }

    function targetY(el) {
      var y = Math.round(
        el.getBoundingClientRect().top +
          (window.pageYOffset || window.scrollY || 0) -
          scrollOffsetPx()
      );
      return y < 0 ? 0 : y;
    }

    var token = 0;
    var lastTarget = null;

    function correctResidual() {
      if (!lastTarget || !lastTarget.getBoundingClientRect) return;
      try {
        var gap = scrollOffsetPx();
        var top = lastTarget.getBoundingClientRect().top;
        var delta = Math.round(top - gap);
        if (Math.abs(delta) > 2 && Math.abs(delta) < 140) {
          window.scrollBy(0, delta);
        }
      } catch (_e) {}
    }

    function scrollToEl(sectionOrEl) {
      if (!sectionOrEl) return false;
      syncMargin();
      var target = resolveScrollTarget(sectionOrEl) || sectionOrEl;
      lastTarget = target;
      var y = targetY(target);
      var my = ++token;
      var reduced = false;
      try {
        reduced =
          window.matchMedia &&
          window.matchMedia("(prefers-reduced-motion: reduce)").matches;
      } catch (_e) {}

      try {
        window.scrollTo({
          top: y,
          left: 0,
          behavior: reduced ? "auto" : "smooth",
        });
      } catch (err) {
        window.scrollTo(0, y);
      }

      var unlockMs = reduced ? 50 : 900;
      window.setTimeout(function () {
        if (my !== token) return;
        var cur = window.pageYOffset || window.scrollY || 0;
        if (!reduced && Math.abs(cur - y) > 8) {
          window.setTimeout(function () {
            if (my !== token) return;
            correctResidual();
          }, 400);
          return;
        }
        correctResidual();
      }, unlockMs);
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
