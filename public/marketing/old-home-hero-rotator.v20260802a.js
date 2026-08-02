/**
 * Old Home — hero headline rotator (v20260801c)
 * Path-gated to /, /es, and /old-home (homepage + Spanish locale).
 * 01c: CSS owns --hr-lh (never write probe floor 40px — that crushed tall heroes);
 *      JS only reads computed --hr-lh for translateY; clears legacy inline crush.
 * 01b: measure width without toggling all words `.on` (no orange stack flash);
 *      wait for document.fonts before markReady; remeasure after fonts.
 * 01a: max-content width default; mark html.oh-rotator-ready after first paint.
 */
(function () {
  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    if (path !== "/" && path !== "/old-home" && path !== "/es") return;
    if (window.__ohHeroRotator >= 202608021) return;
    window.__ohHeroRotator = 202608021;

    var stale = document.getElementById("oh-hero-rotator-30f");
    if (stale && stale.parentNode) stale.parentNode.removeChild(stale);
    var staleG = document.getElementById("oh-hero-rotator-30g");
    if (staleG && staleG.parentNode) staleG.parentNode.removeChild(staleG);
    var staleA = document.getElementById("oh-hero-rotator-01a");
    if (staleA && staleA.parentNode) staleA.parentNode.removeChild(staleA);
    var staleB = document.getElementById("oh-hero-rotator-01b");
    if (staleB && staleB.parentNode) staleB.parentNode.removeChild(staleB);

    var STYLE_ID = "oh-hero-rotator-01c";
    if (!document.getElementById(STYLE_ID)) {
      var st = document.createElement("style");
      st.id = STYLE_ID;
      st.textContent = [
        "#h1wrap.oh-h1wrap,#h1wrap{",
        "display:flex!important;flex-direction:row!important;flex-wrap:nowrap!important;",
        "align-items:center!important;justify-content:flex-start!important;",
        "gap:.55em!important;overflow:visible!important;",
        "--hr-slots:5!important;--hr-center:2!important;",
        "--hr-lh:clamp(40px,5.2vw,70px)!important;",
        "}",
        "#hrwrap,.oh-hrwrap{",
        "display:block!important;position:relative!important;z-index:1!important;",
        "height:calc(var(--hr-lh)*var(--hr-slots))!important;",
        "min-height:calc(var(--hr-lh)*var(--hr-slots))!important;",
        "max-height:none!important;",
        "width:var(--hr-w,max-content)!important;",
        "min-width:0!important;",
        "overflow:hidden!important;flex-shrink:0!important;",
        "-webkit-mask-image:linear-gradient(180deg,transparent 0%,#000 12%,#000 88%,transparent 100%)!important;",
        "mask-image:linear-gradient(180deg,transparent 0%,#000 12%,#000 88%,transparent 100%)!important;",
        "}",
        "#rotator,.oh-rotator{",
        "display:flex!important;flex-direction:column!important;align-items:flex-start!important;",
        "width:max-content!important;min-width:100%!important;will-change:transform;",
        "}",
        "#rotator > *,.oh-hrword,#rotator > .oh-hrword-on{",
        "display:flex!important;align-items:center!important;box-sizing:border-box!important;",
        "font-family:Lora,Georgia,serif!important;font-style:italic!important;font-weight:400!important;",
        "font-size:clamp(32px,4.5vw,58px)!important;",
        "line-height:var(--hr-lh)!important;height:var(--hr-lh)!important;",
        "min-height:var(--hr-lh)!important;max-height:var(--hr-lh)!important;",
        "color:#8B90FF!important;white-space:nowrap!important;opacity:.12!important;",
        "padding:0!important;margin:0!important;text-align:left!important;",
        "pointer-events:none!important;",
        "}",
        "#rotator > *.near,.oh-hrword.near{opacity:.42!important}",
        "#rotator > *.far,.oh-hrword.far{opacity:.22!important}",
        "#rotator > *.on,#rotator > *.oh-hrword-on,.oh-hrword-on{",
        "font-family:\"Inter Tight\",\"Plus Jakarta Sans\",system-ui,sans-serif!important;",
        "font-style:normal!important;font-weight:800!important;letter-spacing:-.035em!important;",
        "color:#D78E2C!important;opacity:1!important;pointer-events:auto!important;",
        "}",
        "@media (max-width:960px){",
        "#h1wrap.oh-h1wrap,#h1wrap{",
        "flex-direction:column!important;align-items:flex-start!important;",
        "--hr-slots:3!important;--hr-center:1!important;",
        "--hr-lh:clamp(34px,8vw,52px)!important;",
        "}",
        "#hrwrap,.oh-hrwrap{min-width:0!important;width:var(--hr-w,max-content)!important}",
        "#hstatic,.oh-hstatic,#rotator > *,.oh-hrword,.oh-hrword-on{font-size:clamp(26px,7.2vw,40px)!important}",
        "}",
      ].join("");
      (document.head || document.documentElement).appendChild(st);
    }

    function markReady() {
      try {
        document.documentElement.classList.add("oh-rotator-ready");
      } catch (_e) {}
    }

    var remasure = null;

    function start() {
      if (window.__ohHeroRotatorStarted) {
        if (typeof remasure === "function") remasure();
        markReady();
        return true;
      }
      var rot = document.getElementById("rotator");
      if (!rot) return false;
      var words = [].slice.call(rot.children);
      if (!words.length) return false;
      window.__ohHeroRotatorStarted = true;

      var wrap = rot.parentElement || document.getElementById("hrwrap");
      var h1 = document.getElementById("h1wrap");
      var startIdx = 2;
      var suffixBuffer = 2;
      var loopEnd = words.length - suffixBuffer - 1;
      if (loopEnd <= startIdx) {
        startIdx = 0;
        loopEnd = Math.max(0, words.length - 1);
      }
      var ri = startIdx;
      var reduce = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

      function centerSlot() {
        return window.matchMedia("(max-width:960px)").matches ? 1 : 2;
      }

      function setActiveClass(w, on) {
        w.classList.toggle("on", on);
        w.classList.toggle("oh-hrword-on", on);
        w.classList.toggle("oh-hrword", !on);
      }

      // Off-DOM probe: width only (never drives --hr-lh — CSS media queries own that).
      function makeProbe() {
        var probe = document.createElement("span");
        probe.setAttribute("aria-hidden", "true");
        var cssLh = h1
          ? getComputedStyle(h1).getPropertyValue("--hr-lh").trim()
          : "";
        var liveFs =
          words[0] && getComputedStyle(words[0]).fontSize
            ? getComputedStyle(words[0]).fontSize
            : "clamp(32px,4.5vw,58px)";
        probe.style.cssText =
          "position:absolute!important;left:-9999px!important;top:0!important;" +
          "visibility:hidden!important;pointer-events:none!important;" +
          "display:inline-flex!important;align-items:center!important;" +
          "white-space:nowrap!important;box-sizing:border-box!important;" +
          'font-family:"Inter Tight","Plus Jakarta Sans",system-ui,sans-serif!important;' +
          "font-style:normal!important;font-weight:800!important;" +
          "letter-spacing:-.035em!important;" +
          "font-size:" +
          liveFs +
          "!important;" +
          "line-height:var(--hr-lh)!important;height:auto!important;" +
          "padding:0!important;margin:0!important;";
        if (cssLh) probe.style.setProperty("--hr-lh", cssLh);
        if (window.matchMedia("(max-width:960px)").matches && !words[0]) {
          probe.style.fontSize = "clamp(26px,7.2vw,40px)";
        }
        (document.body || document.documentElement).appendChild(probe);
        return probe;
      }

      var probeEl = null;
      function getProbe() {
        if (!probeEl || !probeEl.parentNode) probeEl = makeProbe();
        return probeEl;
      }

      /** Resolved CSS --hr-lh (px). Never invent a floor that crushes tall viewports. */
      function lineH() {
        var h = 0;
        if (h1) {
          var raw = getComputedStyle(h1).getPropertyValue("--hr-lh").trim();
          h = parseFloat(raw) || 0;
        }
        if (!h && words.length) {
          var sample = words[Math.min(ri, words.length - 1)] || words[0];
          h = Math.round(sample.getBoundingClientRect().height) || 0;
        }
        return h > 0 ? h : 70;
      }

      function clearLegacyLhInline() {
        if (h1) h1.style.removeProperty("--hr-lh");
        if (wrap) {
          wrap.style.removeProperty("--hr-lh");
          wrap.style.removeProperty("height");
          wrap.style.removeProperty("min-height");
        }
      }

      function setWidth() {
        if (!wrap) return;
        var mx = 0;
        var p = getProbe();
        words.forEach(function (w) {
          p.textContent = (w.textContent || "").trim();
          mx = Math.max(mx, p.scrollWidth || p.offsetWidth || 0);
        });
        mx = Math.ceil(mx + 28);
        var mobile = window.matchMedia("(max-width:960px)").matches;
        var cap = h1 ? h1.clientWidth : 0;
        if (mobile && cap > 0) mx = Math.min(mx, cap);
        wrap.style.setProperty("--hr-w", mx + "px", "important");
        wrap.style.setProperty("width", mx + "px", "important");
      }

      function paint(animate) {
        clearLegacyLhInline();
        var h = lineH();
        var c = centerSlot();
        var useAnim = animate && !reduce;
        rot.style.transition = useAnim
          ? "transform .65s cubic-bezier(.77,0,.18,1)"
          : "none";
        rot.style.transform = "translateY(" + (c - ri) * h + "px)";
        words.forEach(function (w, idx) {
          w.style.transition = useAnim ? "opacity .45s ease" : "none";
          var dist = Math.abs(idx - ri);
          setActiveClass(w, dist === 0);
          w.classList.toggle("near", dist === 1);
          w.classList.toggle("far", dist === 2);
          w.setAttribute("aria-hidden", dist === 0 ? "false" : "true");
        });
      }

      remasure = function () {
        setWidth();
        paint(false);
      };

      remasure();
      markReady();

      var rotMs = 3400;
      window.setInterval(function () {
        ri += 1;
        paint(true);
        if (ri >= loopEnd) {
          window.setTimeout(function () {
            ri = startIdx;
            paint(false);
          }, reduce ? 0 : 700);
        }
      }, rotMs);

      window.addEventListener(
        "resize",
        function () {
          remasure();
        },
        { passive: true }
      );
      return true;
    }

    function fontsReady(cb) {
      if (document.fonts && document.fonts.ready) {
        document.fonts.ready.then(cb).catch(cb);
        return;
      }
      cb();
    }

    function boot() {
      if (start()) return;
      window.setTimeout(start, 50);
      window.setTimeout(start, 250);
      window.setTimeout(start, 800);
    }

    function bootAfterFonts() {
      fontsReady(function () {
        boot();
        // Remeasure once fonts settle even if start already ran early.
        window.setTimeout(function () {
          if (typeof remasure === "function") remasure();
          markReady();
        }, 0);
      });
    }

    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", bootAfterFonts);
    } else {
      bootAfterFonts();
    }
  } catch (err) {
    try {
      document.documentElement.classList.add("oh-rotator-ready");
    } catch (_e) {}
    if (typeof console !== "undefined" && console.warn) {
      console.warn("[oh-hero-rotator]", err);
    }
  }
})();
