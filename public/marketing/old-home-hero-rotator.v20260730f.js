/**
 * Old Home — hero headline rotator (v20260730f)
 * Path-gated to /old-home. Clean single-word cycle (no tall ghost stack).
 * Cycles even when prefers-reduced-motion is on (instant swap, no transition).
 */
(function () {
  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    if (path !== "/old-home") return;
    if (window.__ohHeroRotator >= 202607306) return;
    window.__ohHeroRotator = 202607306;

    var STYLE_ID = "oh-hero-rotator-30f";
    if (!document.getElementById(STYLE_ID)) {
      var st = document.createElement("style");
      st.id = STYLE_ID;
      st.textContent = [
        "#h1wrap.oh-h1wrap,#h1wrap{",
        "display:flex!important;flex-direction:row!important;flex-wrap:nowrap!important;",
        "align-items:center!important;justify-content:flex-start!important;",
        "gap:.55em!important;--hr-slots:1!important;--hr-center:0!important;",
        "}",
        "#hrwrap,.oh-hrwrap{",
        "display:block!important;position:relative!important;",
        "height:var(--hr-lh,48px)!important;min-height:var(--hr-lh,48px)!important;",
        "max-height:var(--hr-lh,48px)!important;overflow:hidden!important;",
        "min-width:0!important;flex:0 1 auto!important;",
        "-webkit-mask-image:none!important;mask-image:none!important;",
        "}",
        "#rotator,.oh-rotator{",
        "display:flex!important;flex-direction:column!important;align-items:flex-start!important;",
        "will-change:transform;",
        "}",
        "#rotator > *,.oh-hrword,.oh-hrword-on{",
        "display:flex!important;align-items:center!important;",
        "height:var(--hr-lh,48px)!important;min-height:var(--hr-lh,48px)!important;",
        "max-height:var(--hr-lh,48px)!important;line-height:var(--hr-lh,48px)!important;",
        "opacity:0!important;pointer-events:none!important;",
        "}",
        "#rotator > *.on,#rotator > *.oh-hrword-on,.oh-hrword-on{",
        "opacity:1!important;pointer-events:auto!important;",
        "font-family:\"Inter Tight\",\"Plus Jakarta Sans\",system-ui,sans-serif!important;",
        "font-style:normal!important;font-weight:800!important;color:#D78E2C!important;",
        "}",
      ].join("");
      (document.head || document.documentElement).appendChild(st);
    }

    function start() {
      var rot = document.getElementById("rotator");
      if (!rot) return false;
      var words = [].slice.call(rot.children);
      if (!words.length) return false;

      var wrap = rot.parentElement || document.getElementById("hrwrap");
      var h1 = document.getElementById("h1wrap");
      var startIdx = 0;
      for (var i = 0; i < words.length; i++) {
        if (
          words[i].classList.contains("oh-hrword-on") ||
          words[i].classList.contains("on")
        ) {
          startIdx = i;
          break;
        }
      }
      // Prefer first unique cycle (skip trailing duplicate buffer if present)
      var uniqueCount = Math.max(1, Math.ceil(words.length * 0.7));
      var loopEnd = Math.min(words.length - 1, Math.max(startIdx + 1, uniqueCount - 1));
      var ri = startIdx;
      var reduce = window.matchMedia("(prefers-reduced-motion: reduce)").matches;

      function setActiveClass(w, on) {
        w.classList.toggle("on", on);
        w.classList.toggle("oh-hrword-on", on);
        w.classList.toggle("oh-hrword", !on);
        w.classList.remove("near", "far");
        w.setAttribute("aria-hidden", on ? "false" : "true");
      }

      function measureLine() {
        var probe = words[ri] || words[0];
        if (!probe) return 48;
        setActiveClass(probe, true);
        var h = Math.max(probe.offsetHeight || 0, 40);
        return h;
      }

      function setWidth() {
        if (!wrap) return;
        var mx = 0;
        words.forEach(function (w) {
          setActiveClass(w, true);
          mx = Math.max(mx, w.scrollWidth || 0);
          setActiveClass(w, false);
        });
        mx = Math.ceil(mx + 8);
        var cap = h1 ? h1.clientWidth : 0;
        var staticEl = document.getElementById("hstatic");
        if (cap > 0 && staticEl) {
          mx = Math.min(mx, Math.max(120, cap - staticEl.offsetWidth - 24));
        }
        wrap.style.setProperty("--hr-w", mx + "px", "important");
        wrap.style.setProperty("width", mx + "px", "important");
        wrap.style.setProperty("min-width", "0", "important");
      }

      function paint(animate) {
        var h = measureLine();
        if (h1) h1.style.setProperty("--hr-lh", h + "px", "important");
        if (wrap) {
          wrap.style.setProperty("--hr-lh", h + "px", "important");
          wrap.style.setProperty("height", h + "px", "important");
        }
        var useAnim = animate && !reduce;
        rot.style.transition = useAnim
          ? "transform .55s cubic-bezier(.77,0,.18,1)"
          : "none";
        // Single visible slot: active word at top of clipped wrap
        rot.style.transform = "translateY(" + -ri * h + "px)";
        words.forEach(function (w, idx) {
          w.style.transition = useAnim ? "opacity .35s ease" : "none";
          setActiveClass(w, idx === ri);
        });
      }

      setWidth();
      paint(false);

      var rotMs = 3200;
      window.setInterval(function () {
        ri += 1;
        if (ri > loopEnd) ri = 0;
        paint(true);
      }, rotMs);

      window.addEventListener(
        "resize",
        function () {
          setWidth();
          paint(false);
        },
        { passive: true }
      );
      return true;
    }

    function boot() {
      if (start()) return;
      window.setTimeout(start, 50);
      window.setTimeout(start, 250);
      window.setTimeout(start, 800);
    }

    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", boot);
    } else {
      boot();
    }
    if (document.fonts && document.fonts.ready) {
      document.fonts.ready.then(function () {
        boot();
      }).catch(function () {});
    }
  } catch (err) {
    if (typeof console !== "undefined" && console.warn) {
      console.warn("[oh-hero-rotator]", err);
    }
  }
})();
