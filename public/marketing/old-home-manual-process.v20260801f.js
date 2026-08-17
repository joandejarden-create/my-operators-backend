/**
 * Dealality — Manual Process problem section animation (v1.1.27)
 * Scoped to #dealality-manual-process. Vanilla JS only.
 * Path dots underlap One Hotel Opportunity right edge.
 * Incoming lines: start at Opp-edge dots, tangle mid-gap, end at Manual left edge.
 * Faint loops in Opp↔Manual gap (out-dot trail style).
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
        /* Center ~35% of the way into the card from the right edge → majority behind the box */
        var targetCenterX = oppRight - dr.width * 0.15;
        var dx = targetCenterX - (dr.left + dr.width * 0.5);
        d.style.transform = "translateX(" + dx.toFixed(1) + "px)";
      }
    }

    /** Messy mid-gap curve with more random wiggles; ends at Manual left edge. */
    function tanglePath(x0, y0, x1, y1, variant) {
      var span = x1 - x0;
      var dir = variant % 2 === 0 ? 1 : -1;
      var flip = variant % 3 === 0 ? -1 : 1;
      var amp1 = 32 + (variant % 5) * 18;
      var amp2 = 48 + ((variant * 3) % 5) * 16;
      var amp3 = 26 + ((variant * 2) % 4) * 14;
      var t1 = 0.16 + (variant % 4) * 0.05;
      var t2 = 0.4 + (variant % 3) * 0.07;
      var t3 = 0.68 + (variant % 4) * 0.04;
      var xA = x0 + span * t1;
      var xB = x0 + span * t2;
      var xC = x0 + span * t3;
      var yA = y0 + dir * amp1 * flip * 0.55;
      var yB = (y0 + y1) * 0.48 - dir * amp2 * 0.85;
      var yC = (y0 + y1) * 0.58 + dir * amp3 * flip;
      return (
        "M" +
        x0.toFixed(1) +
        " " +
        y0.toFixed(1) +
        " C" +
        (x0 + span * (0.06 + (variant % 3) * 0.03)).toFixed(1) +
        " " +
        (y0 - dir * amp1 * 0.65).toFixed(1) +
        ", " +
        (xA - 14 * flip).toFixed(1) +
        " " +
        yA.toFixed(1) +
        ", " +
        xA.toFixed(1) +
        " " +
        ((y0 + yA) * 0.5).toFixed(1) +
        " S" +
        (xB + 18 * dir).toFixed(1) +
        " " +
        yB.toFixed(1) +
        ", " +
        xB.toFixed(1) +
        " " +
        ((yB + yC) * 0.5).toFixed(1) +
        " S" +
        (xC - 16 * flip).toFixed(1) +
        " " +
        yC.toFixed(1) +
        ", " +
        xC.toFixed(1) +
        " " +
        ((yC + y1) * 0.55).toFixed(1) +
        " S" +
        (x1 - span * (0.1 + (variant % 3) * 0.03)).toFixed(1) +
        " " +
        (y1 + dir * amp2 * 0.22 * flip).toFixed(1) +
        ", " +
        x1.toFixed(1) +
        " " +
        y1.toFixed(1)
      );
    }

    /**
     * Squiggly imperfect loop in the Opp↔Manual gap:
     * leave Manual left edge → reach toward Opp (never touch) → return to a
     * different Y on Manual left. Matches right-side faint out-dot trail style.
     */
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

    /** Faint dashed circular trails in the gap: Manual left → toward Opp → Manual left. */
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

      /* Varying reach into the gap — never touch Opportunity (leave ≥22% of gap). */
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
        y0 = Math.max(6, Math.min(vbH - 6, y0));
        y1 = Math.max(6, Math.min(vbH - 6, y1));
        yTip = Math.max(6, Math.min(vbH - 6, yTip));
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

        /* Start at Opp-edge dots; end on Manual left edge at a related Y. */
        var startCx = dr.left + dr.width * 0.5;
        var startCy = dr.top + dr.height * 0.5;
        var endCx = manRect.left;
        var endCy =
          startCy +
          (i % 2 === 0 ? 1 : -1) * (4 + (i % 3) * 6);

        var x0 = ((startCx - svgRect.left) / svgRect.width) * vbW;
        var y0 = ((startCy - svgRect.top) / svgRect.height) * vbH;
        var x1 = ((endCx - svgRect.left) / svgRect.width) * vbW;
        var y1 = ((endCy - svgRect.top) / svgRect.height) * vbH;

        x0 = Math.max(-8, Math.min(vbW * 0.18, x0));
        y0 = Math.max(4, Math.min(vbH - 4, y0));
        x1 = Math.max(vbW * 0.78, Math.min(vbW + 12, x1));
        y1 = Math.max(4, Math.min(vbH - 4, y1));

        var solid = paths[i * 2];
        var dash = paths[i * 2 + 1];
        if (solid) solid.setAttribute("d", tanglePath(x0, y0, x1, y1, i * 2));
        if (dash) {
          var y0b = Math.max(4, Math.min(vbH - 4, y0 + (i % 2 === 0 ? 7 : -7)));
          var y1b = Math.max(
            8,
            Math.min(vbH - 8, y1 + (i % 2 === 0 ? 11 : -11))
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
    drawFaintLoops();
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
        if (root.getAttribute("data-dmp-played") !== "1") {
          underlapPathDots();
          drawFaintLoops();
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
