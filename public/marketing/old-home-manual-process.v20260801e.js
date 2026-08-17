/**
 * Dealality — Manual Process problem section animation (v1.1.20)
 * Scoped to #dealality-manual-process. Vanilla JS only.
 * Path dots underlap Manual Process left edge (Features mf-node pattern).
 * Incoming lines: start near path labels, tangle mid-gap, end at underlapped dots.
 */
(function () {
  try {
    var ROOT_ID = "dealality-manual-process";
    var root = document.getElementById(ROOT_ID);
    if (!root || root.getAttribute("data-dmp-bound") === "1") return;
    root.setAttribute("data-dmp-bound", "1");

    function prefersReducedMotion() {
      return (
        typeof window.matchMedia === "function" &&
        window.matchMedia("(prefers-reduced-motion: reduce)").matches
      );
    }

    function isStacked() {
      return (
        typeof window.matchMedia === "function" &&
        window.matchMedia("(max-width: 900px)").matches
      );
    }

    /** Features-style underlap: half the glowing dots sit behind Manual's left edge. */
    function underlapPathDots() {
      var manual = root.querySelector(".dmp-card--manual");
      var dots = root.querySelectorAll(".dmp-card--opp .dmp-path-dot");
      if (!manual || !dots.length) return;
      if (isStacked()) {
        for (var r = 0; r < dots.length; r++) {
          dots[r].style.transform = "";
        }
        return;
      }
      /* Features mf-node: center on card left edge so ~half the core is hidden. */
      var targetCenterX = manual.getBoundingClientRect().left;
      for (var i = 0; i < dots.length; i++) {
        var d = dots[i];
        d.style.transform = "";
        var dr = d.getBoundingClientRect();
        if (!dr.width) continue;
        var dx = targetCenterX - (dr.left + dr.width * 0.5);
        d.style.transform = "translateX(" + dx.toFixed(1) + "px)";
      }
    }

    /** Messy mid-gap curve; ends at underlapped dots on Manual Process. */
    function tanglePath(x0, y0, x1, y1, variant) {
      var mx = (x0 + x1) * 0.48;
      var amp = 36 + (variant % 4) * 14;
      var dir = variant % 2 === 0 ? 1 : -1;
      var yMid = (y0 + y1) * 0.5 + dir * (10 + (variant % 3) * 8);
      var c1x = x0 + (x1 - x0) * 0.22;
      var c2x = x0 + (x1 - x0) * 0.62;
      return (
        "M" +
        x0.toFixed(1) +
        " " +
        y0.toFixed(1) +
        " C" +
        c1x.toFixed(1) +
        " " +
        (y0 - amp * dir * 0.35).toFixed(1) +
        ", " +
        (mx - 18).toFixed(1) +
        " " +
        (yMid + amp * dir).toFixed(1) +
        ", " +
        mx.toFixed(1) +
        " " +
        yMid.toFixed(1) +
        " S" +
        c2x.toFixed(1) +
        " " +
        (y1 + amp * dir * 0.25).toFixed(1) +
        ", " +
        x1.toFixed(1) +
        " " +
        y1.toFixed(1)
      );
    }

    function remapIncomingFromDots() {
      underlapPathDots();
      var svg = root.querySelector(
        ".dmp-connectors--desktop.dmp-connectors--in svg"
      );
      if (!svg || isStacked()) return;
      var dots = root.querySelectorAll(".dmp-card--opp .dmp-path-dot");
      var paths = svg.querySelectorAll("path.dmp-line-in");
      if (!dots.length || !paths.length) return;

      var svgRect = svg.getBoundingClientRect();
      if (!svgRect.width || !svgRect.height) return;
      var vb = svg.viewBox && svg.viewBox.baseVal;
      var vbW = vb && vb.width ? vb.width : 160;
      var vbH = vb && vb.height ? vb.height : 320;

      for (var i = 0; i < dots.length; i++) {
        var pathEl = dots[i].closest(".dmp-path");
        var label = pathEl
          ? pathEl.querySelector(".dmp-path-label")
          : null;
        var startRect = label
          ? label.getBoundingClientRect()
          : pathEl
            ? pathEl.getBoundingClientRect()
            : null;
        var dr = dots[i].getBoundingClientRect();
        if (!dr.width && !dr.height) continue;

        var startCx = startRect
          ? Math.min(startRect.right + 6, svgRect.left + 10)
          : svgRect.left + 4;
        var startCy = startRect
          ? startRect.top + startRect.height * 0.5
          : dr.top + dr.height * 0.5;
        var endCx = dr.left + dr.width * 0.5;
        var endCy = dr.top + dr.height * 0.5;

        var x0 = ((startCx - svgRect.left) / svgRect.width) * vbW;
        var y0 = ((startCy - svgRect.top) / svgRect.height) * vbH;
        var x1 = ((endCx - svgRect.left) / svgRect.width) * vbW;
        var y1 = ((endCy - svgRect.top) / svgRect.height) * vbH;

        x0 = Math.max(-6, Math.min(vbW * 0.22, x0));
        y0 = Math.max(4, Math.min(vbH - 4, y0));
        x1 = Math.max(vbW * 0.72, Math.min(vbW + 10, x1));
        y1 = Math.max(4, Math.min(vbH - 4, y1));

        var solid = paths[i * 2];
        var dash = paths[i * 2 + 1];
        if (solid) solid.setAttribute("d", tanglePath(x0, y0, x1, y1, i * 2));
        if (dash) {
          var y0b = Math.max(4, Math.min(vbH - 4, y0 + (i % 2 === 0 ? 5 : -5)));
          var y1b = Math.max(
            8,
            Math.min(vbH - 8, y1 + (i % 2 === 0 ? 8 : -8))
          );
          dash.setAttribute("d", tanglePath(x0, y0b, x1, y1b, i * 2 + 1));
        }
      }
    }

    function preparePaths() {
      var paths = root.querySelectorAll("[data-dmp-draw]");
      for (var i = 0; i < paths.length; i++) {
        var p = paths[i];
        var len = 0;
        try {
          len = p.getTotalLength();
        } catch (err) {
          len = 420;
        }
        p.setAttribute("data-dmp-len", String(Math.ceil(len)));
        p.style.strokeDasharray = String(Math.ceil(len));
        p.style.strokeDashoffset = String(Math.ceil(len));
      }
    }

    function finalizeStrokeStyles() {
      var paths = root.querySelectorAll("[data-dmp-draw]");
      for (var i = 0; i < paths.length; i++) {
        var p = paths[i];
        var kind = p.getAttribute("data-dmp-draw");
        p.style.strokeDashoffset = "0";
        if (kind === "out-dot") {
          p.style.strokeDasharray = "2.4 5.2";
        } else if (p.classList.contains("dmp-line-in--dash")) {
          p.style.strokeDasharray = "3.5 5.5";
        } else {
          p.style.strokeDasharray = "none";
        }
      }
    }

    function showStatic() {
      root.classList.remove("is-animating");
      root.classList.add("is-drawn");
      finalizeStrokeStyles();
    }

    function play() {
      if (root.getAttribute("data-dmp-played") === "1") return;
      root.setAttribute("data-dmp-played", "1");
      remapIncomingFromDots();
      if (prefersReducedMotion()) {
        showStatic();
        return;
      }
      preparePaths();
      root.classList.add("is-animating");
      // Force reflow then draw
      void root.offsetWidth;
      window.requestAnimationFrame(function () {
        var paths = root.querySelectorAll("[data-dmp-draw]");
        for (var i = 0; i < paths.length; i++) {
          paths[i].style.strokeDashoffset = "0";
        }
      });
      window.setTimeout(function () {
        root.classList.add("is-drawn");
        finalizeStrokeStyles();
      }, 2100);
    }

    function onVisible(entries, obs) {
      for (var i = 0; i < entries.length; i++) {
        if (entries[i].isIntersecting) {
          play();
          if (obs) obs.disconnect();
          break;
        }
      }
    }

    // Position dots under Manual edge immediately (before / without animation).
    underlapPathDots();
    if (typeof IntersectionObserver === "function") {
      var io = new IntersectionObserver(onVisible, {
        root: null,
        threshold: 0.28,
      });
      io.observe(root);
    } else {
      play();
    }

    window.addEventListener(
      "resize",
      function () {
        if (root.getAttribute("data-dmp-played") !== "1") return;
        remapIncomingFromDots();
        finalizeStrokeStyles();
      },
      { passive: true }
    );
  } catch (err) {
    if (typeof console !== "undefined" && console.warn) {
      console.warn("[dealality-manual-process]", err);
    }
  }
})();
