/**
 * Dealality — Manual Process problem section animation (v1.1.16)
 * Scoped to #dealality-manual-process. Vanilla JS only.
 * Remaps incoming lines: clean starts at five path-dots, messy mid-gap tangle,
 * converge into Manual Process left edge so destinations are not readable.
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

    /** Messy mid-gap curve; ends cluster near Manual Process mid-left. */
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

    function endYsClustered(count, vbH) {
      // Converge into the Manual Process mid-edge (not readable destinations)
      var mid = vbH * 0.5;
      var spread = vbH * 0.12;
      var out = [];
      for (var i = 0; i < count; i++) {
        var t = count === 1 ? 0.5 : i / (count - 1);
        out.push(mid - spread + spread * 2 * t);
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
      var vbH = vb && vb.height ? vb.height : 320;
      var ends = endYsClustered(dots.length, vbH);

      for (var i = 0; i < dots.length; i++) {
        var dr = dots[i].getBoundingClientRect();
        if (!dr.width && !dr.height) continue;
        var cx = dr.left + dr.width * 0.5;
        var cy = dr.top + dr.height * 0.5;
        var x0 = ((cx - svgRect.left) / svgRect.width) * vbW;
        var y0 = ((cy - svgRect.top) / svgRect.height) * vbH;
        // Start cleanly at/near each glowing path dot
        x0 = Math.max(-4, Math.min(14, x0));
        y0 = Math.max(4, Math.min(vbH - 4, y0));
        // Overshoot slightly into Manual Process so stroke disappears under the card
        var x1 = vbW + 8;
        var y1 = ends[i];

        var solid = paths[i * 2];
        var dash = paths[i * 2 + 1];
        if (solid) solid.setAttribute("d", tanglePath(x0, y0, x1, y1, i * 2));
        if (dash) {
          var y0b = Math.max(4, Math.min(vbH - 4, y0 + (i % 2 === 0 ? 5 : -5)));
          var y1b = Math.max(
            8,
            Math.min(vbH - 8, y1 + (i % 2 === 0 ? 10 : -10))
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
