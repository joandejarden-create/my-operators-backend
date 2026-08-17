/**
 * Dealality — Manual Process problem section animation (v1.1.39)
 * Scoped to #dealality-manual-process. Vanilla JS only.
 * - Path dots underlap Opportunity right edge
 * - Incoming lines: start at Opp-edge dots, tangle mid-gap, end at Manual left
 * - Bottom paths clamped so curves stay inside the journey row (no spill into problem cards)
 * - Connectors/dots hidden until first remap (no load flash)
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

    function clamp(n, lo, hi) {
      return Math.max(lo, Math.min(hi, n));
    }

    /** Most of each glowing dot sits behind Opportunity's right edge (outer crescent shows). */
    function underlapPathDots() {
      var opp = root.querySelector(".dmp-card--opp");
      var dots = root.querySelectorAll(".dmp-card--opp .dmp-path-dot");
      if (!opp || !dots.length) return;
      if (isStacked()) {
        for (var r = 0; r < dots.length; r++) {
          dots[r].style.transform = "";
        }
        return;
      }
      var oppRight = opp.getBoundingClientRect().right;
      for (var i = 0; i < dots.length; i++) {
        var d = dots[i];
        d.style.transform = "";
        var dr = d.getBoundingClientRect();
        if (!dr.width) continue;
        var targetCenterX = oppRight - dr.width * 0.15;
        var dx = targetCenterX - (dr.left + dr.width * 0.5);
        d.style.transform = "translateX(" + dx.toFixed(1) + "px)";
      }
    }

    /**
     * Messy mid-gap curve. Control points stay inside [yMin, yMax] so bottom
     * trails never dive into the problem cards below the journey row.
     */
    function tanglePath(x0, y0, x1, y1, variant, vbH) {
      var yMin = 10;
      var yMax = (vbH || 320) - 10;
      y0 = clamp(y0, yMin, yMax);
      y1 = clamp(y1, yMin, yMax);

      var span = x1 - x0;
      var midBand = (yMin + yMax) * 0.5;
      /* Prefer bending toward vertical center — bottom dots bend up, top bend down */
      var towardCenter = y0 >= midBand ? -1 : 1;
      var dir = towardCenter;
      var flip = variant % 3 === 0 ? -1 : 1;

      /* Softer amps than v1.1.27; further reduce when near top/bottom edges */
      var edgeProx = Math.min(y0 - yMin, yMax - y0) / ((yMax - yMin) * 0.5);
      var ampScale = 0.45 + 0.55 * clamp(edgeProx, 0.15, 1);
      var amp1 = (14 + (variant % 5) * 7) * ampScale;
      var amp2 = (18 + ((variant * 3) % 5) * 8) * ampScale;
      var amp3 = (12 + ((variant * 2) % 4) * 6) * ampScale;

      var t1 = 0.16 + (variant % 4) * 0.05;
      var t2 = 0.4 + (variant % 3) * 0.07;
      var t3 = 0.68 + (variant % 4) * 0.04;
      var xA = x0 + span * t1;
      var xB = x0 + span * t2;
      var xC = x0 + span * t3;

      var yA = clamp(y0 + dir * amp1 * flip * 0.45, yMin, yMax);
      var yB = clamp((y0 + y1) * 0.5 + dir * amp2 * 0.55, yMin, yMax);
      var yC = clamp((y0 + y1) * 0.55 + dir * amp3 * flip * 0.5, yMin, yMax);
      var yCtrl0 = clamp(y0 - dir * amp1 * 0.4, yMin, yMax);
      var yCtrlEnd = clamp(y1 + dir * amp2 * 0.12 * flip, yMin, yMax);

      return (
        "M" +
        x0.toFixed(1) +
        " " +
        y0.toFixed(1) +
        " C" +
        (x0 + span * (0.06 + (variant % 3) * 0.03)).toFixed(1) +
        " " +
        yCtrl0.toFixed(1) +
        ", " +
        (xA - 14 * flip).toFixed(1) +
        " " +
        yA.toFixed(1) +
        ", " +
        xA.toFixed(1) +
        " " +
        ((y0 + yA) * 0.5).toFixed(1) +
        " S" +
        (xB + 14 * dir).toFixed(1) +
        " " +
        yB.toFixed(1) +
        ", " +
        xB.toFixed(1) +
        " " +
        ((yB + yC) * 0.5).toFixed(1) +
        " S" +
        (xC - 12 * flip).toFixed(1) +
        " " +
        yC.toFixed(1) +
        ", " +
        xC.toFixed(1) +
        " " +
        ((yC + y1) * 0.55).toFixed(1) +
        " S" +
        (x1 - span * (0.1 + (variant % 3) * 0.03)).toFixed(1) +
        " " +
        yCtrlEnd.toFixed(1) +
        ", " +
        x1.toFixed(1) +
        " " +
        y1.toFixed(1)
      );
    }

    function loopPath(xL, y0, y1, xTip, yTip, variant) {
      var dir = variant % 2 === 0 ? -1 : 1;
      var bulge = 14 + (variant % 3) * 9;
      var wiggle = 8 + (variant % 4) * 5;
      var midX = (xL + xTip) * 0.5;
      return (
        "M" +
        xL.toFixed(1) +
        " " +
        y0.toFixed(1) +
        " C" +
        (xL - 10 - (variant % 3) * 4).toFixed(1) +
        " " +
        (y0 + dir * (6 + variant * 2)).toFixed(1) +
        ", " +
        (midX + wiggle * dir * 0.4).toFixed(1) +
        " " +
        (y0 + dir * bulge).toFixed(1) +
        ", " +
        xTip.toFixed(1) +
        " " +
        yTip.toFixed(1) +
        " S" +
        (midX - wiggle * 0.5).toFixed(1) +
        " " +
        (y1 - dir * (bulge * 0.55)).toFixed(1) +
        ", " +
        xL.toFixed(1) +
        " " +
        y1.toFixed(1)
      );
    }

    function drawFaintLoops() {
      var wrap = root.querySelector(".dmp-connectors--loops");
      var svg = wrap && wrap.querySelector("svg");
      var opp = root.querySelector(".dmp-card--opp");
      var manual = root.querySelector(".dmp-card--manual");
      var row = root.querySelector(".dmp-journey-row");
      if (!svg || !opp || !manual || !row) return;
      var paths = svg.querySelectorAll("path.dmp-line-loop");
      if (!paths.length) return;

      if (isStacked()) {
        wrap.style.display = "none";
        return;
      }
      wrap.style.display = "";

      var rowRect = row.getBoundingClientRect();
      var oppRect = opp.getBoundingClientRect();
      var manRect = manual.getBoundingClientRect();
      if (!rowRect.width || !manRect.width) return;

      var vb = svg.viewBox && svg.viewBox.baseVal;
      var vbW = vb && vb.width ? vb.width : 1000;
      var vbH = vb && vb.height ? vb.height : 400;

      function toX(clientX) {
        return ((clientX - rowRect.left) / rowRect.width) * vbW;
      }
      function toY(clientY) {
        return ((clientY - rowRect.top) / rowRect.height) * vbH;
      }

      var xLeft = toX(manRect.left);
      var xOppRight = toX(oppRect.right);
      var gap = xLeft - xOppRight;
      if (gap < 8) return;

      var reaches = [0.52, 0.68, 0.78, 0.58, 0.72];
      var startFrac = [0.14, 0.28, 0.46, 0.64, 0.2];
      var endDelta = [0.22, -0.18, 0.2, -0.24, 0.28];
      var tipYBias = [-0.06, 0.08, -0.1, 0.05, 0.11];

      for (var i = 0; i < paths.length; i++) {
        var sf = startFrac[i % startFrac.length];
        var y0 = toY(manRect.top + manRect.height * sf);
        var y1 = toY(
          manRect.top +
            manRect.height *
              Math.max(0.08, Math.min(0.92, sf + endDelta[i % endDelta.length]))
        );
        var reach = reaches[i % reaches.length];
        var xTip = xLeft - gap * reach;
        var minTip = xOppRight + gap * 0.22;
        if (xTip < minTip) xTip = minTip;
        var yTip = toY(
          manRect.top +
            manRect.height *
              Math.max(
                0.1,
                Math.min(
                  0.9,
                  (sf + sf + endDelta[i % endDelta.length]) * 0.5 +
                    tipYBias[i % tipYBias.length]
                )
              )
        );
        y0 = clamp(y0, 6, vbH - 6);
        y1 = clamp(y1, 6, vbH - 6);
        yTip = clamp(yTip, 6, vbH - 6);
        paths[i].setAttribute("d", loopPath(xLeft, y0, y1, xTip, yTip, i));
      }
    }

    function remapIncomingFromDots() {
      underlapPathDots();
      drawFaintLoops();
      var svg = root.querySelector(
        ".dmp-connectors--desktop.dmp-connectors--in svg"
      );
      var manual = root.querySelector(".dmp-card--manual");
      if (!svg || !manual || isStacked()) return;
      var dots = root.querySelectorAll(".dmp-card--opp .dmp-path-dot");
      var paths = svg.querySelectorAll("path.dmp-line-in");
      if (!dots.length || !paths.length) return;

      var svgRect = svg.getBoundingClientRect();
      var manRect = manual.getBoundingClientRect();
      if (!svgRect.width || !svgRect.height) return;
      var vb = svg.viewBox && svg.viewBox.baseVal;
      var vbW = vb && vb.width ? vb.width : 160;
      var vbH = vb && vb.height ? vb.height : 320;

      for (var i = 0; i < dots.length; i++) {
        var dr = dots[i].getBoundingClientRect();
        if (!dr.width && !dr.height) continue;

        var startCx = dr.left + dr.width * 0.5;
        var startCy = dr.top + dr.height * 0.5;
        var endCx = manRect.left;
        /* Keep end Y near start; bottom dots stay high enough to avoid problem cards */
        var endCy = startCy + (i % 2 === 0 ? 1 : -1) * (3 + (i % 3) * 4);

        var x0 = ((startCx - svgRect.left) / svgRect.width) * vbW;
        var y0 = ((startCy - svgRect.top) / svgRect.height) * vbH;
        var x1 = ((endCx - svgRect.left) / svgRect.width) * vbW;
        var y1 = ((endCy - svgRect.top) / svgRect.height) * vbH;

        x0 = clamp(x0, -8, vbW * 0.18);
        y0 = clamp(y0, 8, vbH - 8);
        x1 = clamp(x1, vbW * 0.78, vbW + 12);
        y1 = clamp(y1, 8, vbH - 8);

        var solid = paths[i * 2];
        var dash = paths[i * 2 + 1];
        if (solid) solid.setAttribute("d", tanglePath(x0, y0, x1, y1, i * 2, vbH));
        if (dash) {
          var y0b = clamp(y0 + (i % 2 === 0 ? 5 : -5), 8, vbH - 8);
          var y1b = clamp(y1 + (i % 2 === 0 ? 7 : -7), 8, vbH - 8);
          dash.setAttribute("d", tanglePath(x0, y0b, x1, y1b, i * 2 + 1, vbH));
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
        if (kind === "out-dot" || kind === "loop") {
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

    /** Position geometry once, then reveal — prevents placeholder flash on refresh. */
    function revealConnectors() {
      remapIncomingFromDots();
      root.classList.add("is-connectors-ready");
    }

    function play() {
      if (root.getAttribute("data-dmp-played") === "1") return;
      root.setAttribute("data-dmp-played", "1");
      remapIncomingFromDots();
      root.classList.add("is-connectors-ready");
      if (prefersReducedMotion()) {
        showStatic();
        return;
      }
      preparePaths();
      root.classList.add("is-animating");
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

    /* Hide until remapped; double-rAF waits for layout after fonts/cards settle. */
    window.requestAnimationFrame(function () {
      window.requestAnimationFrame(function () {
        revealConnectors();
        if (typeof IntersectionObserver === "function") {
          var io = new IntersectionObserver(onVisible, {
            root: null,
            threshold: 0.28,
          });
          io.observe(root);
        } else {
          play();
        }
      });
    });

    window.addEventListener(
      "resize",
      function () {
        if (root.getAttribute("data-dmp-played") !== "1") {
          remapIncomingFromDots();
          root.classList.add("is-connectors-ready");
          return;
        }
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
