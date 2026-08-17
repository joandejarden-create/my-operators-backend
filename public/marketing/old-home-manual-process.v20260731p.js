/**
 * Dealality — Manual Process problem section animation (v1.1.14)
 * Scoped to #dealality-manual-process. Vanilla JS only.
 * Remaps incoming “messy” lines so each pair starts at a path-dot.
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

    function windingPath(x0, y0, x1, y1, variant) {
      var mx = (x0 + x1) * 0.5;
      var my = (y0 + y1) * 0.5;
      var amp = 28 + (variant % 3) * 16;
      var dir = variant % 2 === 0 ? 1 : -1;
      var c1x = x0 + (x1 - x0) * 0.28;
      var c2x = x0 + (x1 - x0) * 0.72;
      return (
        "M" +
        x0.toFixed(1) +
        " " +
        y0.toFixed(1) +
        " C" +
        c1x.toFixed(1) +
        " " +
        (y0 - amp * dir).toFixed(1) +
        ", " +
        (mx - 12).toFixed(1) +
        " " +
        (my + amp * dir * 0.85).toFixed(1) +
        ", " +
        mx.toFixed(1) +
        " " +
        my.toFixed(1) +
        " S" +
        c2x.toFixed(1) +
        " " +
        (y1 + amp * dir * 0.35).toFixed(1) +
        ", " +
        x1.toFixed(1) +
        " " +
        y1.toFixed(1)
      );
    }

    function endYsForDots(count, vbH) {
      // Fan landings across Manual Process left edge (full height)
      var out = [];
      var pad = vbH * 0.08;
      var span = vbH - pad * 2;
      for (var i = 0; i < count; i++) {
        out.push(pad + (span * i) / Math.max(1, count - 1));
      }
      return out;
    }

    function remapIncomingFromDots() {
      var svg = root.querySelector(
        ".dmp-connectors--desktop.dmp-connectors--in svg"
      );
      if (!svg) return;
      var dots = root.querySelectorAll(".dmp-card--opp .dmp-path-dot");
      var paths = svg.querySelectorAll("path.dmp-line-in");
      if (!dots.length || !paths.length) return;

      var svgRect = svg.getBoundingClientRect();
      if (!svgRect.width || !svgRect.height) return;
      var vb = svg.viewBox && svg.viewBox.baseVal;
      var vbW = vb && vb.width ? vb.width : 160;
      var vbH = vb && vb.height ? vb.height : 420;
      var ends = endYsForDots(dots.length, vbH);

      for (var i = 0; i < dots.length; i++) {
        var dr = dots[i].getBoundingClientRect();
        if (!dr.width && !dr.height) continue;
        var cx = dr.left + dr.width * 0.5;
        var cy = dr.top + dr.height * 0.5;
        var x0 = ((cx - svgRect.left) / svgRect.width) * vbW;
        var y0 = ((cy - svgRect.top) / svgRect.height) * vbH;
        // Keep starts at/near the connector’s left edge (dot underlap)
        x0 = Math.max(-2, Math.min(10, x0));
        y0 = Math.max(4, Math.min(vbH - 4, y0));
        var x1 = vbW;
        var y1 = ends[i];

        var solid = paths[i * 2];
        var dash = paths[i * 2 + 1];
        if (solid) solid.setAttribute("d", windingPath(x0, y0, x1, y1, i * 2));
        if (dash) {
          // Slightly offset dashed twin so pairs read as tangled
          var y0b = Math.max(4, Math.min(vbH - 4, y0 + (i % 2 === 0 ? 6 : -6)));
          var y1b = Math.max(8, Math.min(vbH - 8, y1 + (i % 2 === 0 ? 14 : -14)));
          dash.setAttribute("d", windingPath(x0, y0b, x1, y1b, i * 2 + 1));
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
          // Keep dotted pattern after the draw reveal.
          p.style.strokeDasharray = "3 5";
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
      // Force layout, then draw
      void root.offsetWidth;
      requestAnimationFrame(function () {
        root.classList.add("is-drawn");
        window.setTimeout(finalizeStrokeStyles, 2100);
      });
    }

    // Layout may settle after fonts/images; remap once more before play if needed
    function boot() {
      remapIncomingFromDots();
      if (prefersReducedMotion()) {
        showStatic();
        return;
      }

      if (typeof IntersectionObserver === "function") {
        var io = new IntersectionObserver(
          function (entries) {
            for (var i = 0; i < entries.length; i++) {
              if (
                entries[i].isIntersecting &&
                entries[i].intersectionRatio > 0.28
              ) {
                play();
                io.disconnect();
                break;
              }
            }
          },
          { threshold: [0.28, 0.4] }
        );
        io.observe(root);
      } else {
        play();
      }
    }

    if (document.readyState === "complete") {
      window.setTimeout(boot, 40);
    } else {
      window.addEventListener("load", function () {
        window.setTimeout(boot, 40);
      });
      // Also allow early play if section is already in view before full load
      window.setTimeout(boot, 120);
    }

    window.addEventListener(
      "resize",
      function () {
        remapIncomingFromDots();
      },
      { passive: true }
    );
  } catch (err) {
    if (typeof console !== "undefined" && console.warn) {
      console.warn("[dealality-manual-process]", err);
    }
  }
})();
