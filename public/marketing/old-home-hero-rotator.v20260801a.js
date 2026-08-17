/**
 * Old Home — hero headline rotator (v20260801a)
 * Path-gated to /old-home.
 * 01a: max-content width default (no 640→measure jump); mark html.oh-rotator-ready
 * after first paint so FOUC gate can reveal without layout flash.
 */
(function () {
  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    if (path !== "/old-home") return;
    if (window.__ohHeroRotator >= 202608011) return;
    window.__ohHeroRotator = 202608011;

    var stale = document.getElementById("oh-hero-rotator-30f");
    if (stale && stale.parentNode) stale.parentNode.removeChild(stale);
    var staleG = document.getElementById("oh-hero-rotator-30g");
    if (staleG && staleG.parentNode) staleG.parentNode.removeChild(staleG);

    var STYLE_ID = "oh-hero-rotator-01a";
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

    function start() {
      if (window.__ohHeroRotatorStarted) {
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

      function measureLine() {
        var probe = words[ri] || words[startIdx] || words[0];
        if (!probe) return 48;
        var hadOn =
          probe.classList.contains("on") ||
          probe.classList.contains("oh-hrword-on");
        setActiveClass(probe, true);
        var h = Math.max(probe.offsetHeight || 0, 40);
        if (!hadOn) setActiveClass(probe, false);
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
        mx = Math.ceil(mx + 28);
        var mobile = window.matchMedia("(max-width:960px)").matches;
        var cap = h1 ? h1.clientWidth : 0;
        if (mobile && cap > 0) mx = Math.min(mx, cap);
        wrap.style.setProperty("--hr-w", mx + "px", "important");
        wrap.style.setProperty("width", mx + "px", "important");
      }

      function paint(animate) {
        var h = measureLine();
        var c = centerSlot();
        if (h1) h1.style.setProperty("--hr-lh", h + "px", "important");
        if (wrap) {
          wrap.style.setProperty("--hr-lh", h + "px", "important");
          wrap.style.setProperty(
            "height",
            "calc(" + h + "px * var(--hr-slots))",
            "important"
          );
        }
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

      setWidth();
      paint(false);
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
      document.fonts.ready
        .then(function () {
          boot();
        })
        .catch(function () {});
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
