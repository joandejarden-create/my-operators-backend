/**
 * Old Home — hero headline rotator (v20260730e)
 * Path-gated to /old-home. Restores #rotator cycling after footer-oh was dropped.
 */
(function () {
  try {
    var path = (location.pathname || "").replace(/\/+$/, "").toLowerCase() || "/";
    if (path !== "/old-home") return;
    if (window.__ohHeroRotator >= 202607305) return;
    window.__ohHeroRotator = 202607305;

    var rot = document.getElementById("rotator");
    if (!rot) return;

    var words = [].slice.call(rot.children);
    if (!words.length) return;

    var wrap = rot.parentElement;
    var start = 2;
    var suffixBuffer = 2;
    var loopEnd = words.length - suffixBuffer - 1;
    if (loopEnd <= start) {
      start = 0;
      loopEnd = Math.max(0, words.length - 1);
    }
    var ri = start;
    var h1 = document.getElementById("h1wrap");

    function centerSlot() {
      return window.matchMedia("(max-width:960px)").matches ? 1 : 2;
    }

    function setActiveClass(w, on) {
      w.classList.toggle("on", on);
      w.classList.toggle("oh-hrword-on", on);
      w.classList.toggle("oh-hrword", !on);
    }

    function gh() {
      var probe = words[start] || words[0];
      if (!probe) return 48;
      var hadOn =
        probe.classList.contains("on") ||
        probe.classList.contains("oh-hrword-on");
      setActiveClass(probe, true);
      var h = probe.offsetHeight || 48;
      if (!hadOn) setActiveClass(probe, false);
      return h;
    }

    function setWidth() {
      if (!wrap) return;
      var mx = 0;
      words.forEach(function (w) {
        setActiveClass(w, true);
        mx = Math.max(mx, w.scrollWidth);
        setActiveClass(w, false);
      });
      mx = Math.ceil(mx + 24);
      var mobile = window.matchMedia("(max-width:960px)").matches;
      var cap = h1
        ? h1.clientWidth
        : wrap.parentElement
          ? wrap.parentElement.clientWidth
          : 0;
      if (mobile && cap > 0) mx = Math.min(mx, cap);
      wrap.style.setProperty("--hr-w", mx + "px");
      wrap.style.width = mx + "px";
    }

    function paint(animate) {
      var h = gh();
      var c = centerSlot();
      if (h1) h1.style.setProperty("--hr-lh", h + "px");
      rot.style.transition = animate
        ? "transform .65s cubic-bezier(.77,0,.18,1)"
        : "none";
      rot.style.transform = "translateY(" + (c - ri) * h + "px)";
      words.forEach(function (w, i) {
        w.style.transition = animate ? "opacity .45s ease" : "none";
        var dist = Math.abs(i - ri);
        setActiveClass(w, dist === 0);
        w.classList.toggle("near", dist === 1);
        w.classList.toggle("far", dist === 2);
        w.setAttribute("aria-hidden", dist === 0 ? "false" : "true");
      });
    }

    function boot() {
      setWidth();
      paint(false);
    }

    if (document.fonts && document.fonts.ready) {
      document.fonts.ready.then(boot).catch(boot);
    } else {
      boot();
    }

    var rotMs = window.matchMedia("(prefers-reduced-motion: reduce)").matches
      ? 0
      : 3400;
    if (rotMs > 0) {
      setInterval(function () {
        ri++;
        paint(true);
        if (ri >= loopEnd) {
          setTimeout(function () {
            ri = start;
            paint(false);
          }, 700);
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
    }
  } catch (err) {
    if (typeof console !== "undefined" && console.warn) {
      console.warn("[oh-hero-rotator]", err);
    }
  }
})();
