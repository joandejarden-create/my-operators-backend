(function () {
  var root = document.getElementById("dealality-many-futures");
  if (!root || root.dataset.initialized === "true") return;
  root.dataset.initialized = "true";

  var DEFAULT_ID = "new-operator";
  var pinnedId = DEFAULT_ID;
  var activeId = DEFAULT_ID;
  var reduceMotion = false;

  try {
    reduceMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  } catch (e) {}

  var futures = root.querySelectorAll("[data-future-id]");
  var screens = root.querySelectorAll(".mf-preview--desktop [data-preview-id]");
  var svg = root.querySelector(".mf-connectors");
  var hotelNode = root.querySelector(".mf-hotel-node");
  var paths = {};
  var nodes = {};
  var resizeTimer = null;
  var mqMobile = null;

  try {
    mqMobile = window.matchMedia("(max-width: 767px)");
  } catch (e) {}

  function isMobile() {
    return mqMobile ? mqMobile.matches : window.innerWidth <= 767;
  }

  function isTabletOrBelow() {
    return window.innerWidth <= 991;
  }

  function setFutureState(id) {
    activeId = id;
    var i;
    for (i = 0; i < futures.length; i++) {
      var btn = futures[i];
      var on = btn.getAttribute("data-future-id") === id;
      btn.classList.toggle("is-active", on);
      btn.setAttribute("aria-pressed", on ? "true" : "false");
    }

    for (i = 0; i < screens.length; i++) {
      var screen = screens[i];
      screen.classList.toggle("is-active", screen.getAttribute("data-preview-id") === id);
    }

    var captions = root.querySelectorAll(".mf-preview--desktop [data-caption-for]");
    for (i = 0; i < captions.length; i++) {
      var cap = captions[i];
      var showCap = cap.getAttribute("data-caption-for") === id;
      cap.hidden = !showCap;
    }

    // Mobile: preview panels live inside each future group
    var mobilePanels = root.querySelectorAll(".mf-mobile-preview");
    for (i = 0; i < mobilePanels.length; i++) {
      var panel = mobilePanels[i];
      var group = panel.closest(".mf-future-group");
      var btn = group ? group.querySelector("[data-future-id]") : null;
      var match = btn && btn.getAttribute("data-future-id") === id;
      panel.hidden = !match;
      var mobileScreen = panel.querySelector("[data-preview-id]");
      if (mobileScreen) mobileScreen.classList.toggle("is-active", match);
    }

    updateConnectorActive(id);
  }

  function updateConnectorActive(id) {
    var key;
    for (key in paths) {
      if (!Object.prototype.hasOwnProperty.call(paths, key)) continue;
      var path = paths[key];
      var on = key === id;
      path.classList.toggle("is-active", on);
      if (nodes[key]) nodes[key].classList.toggle("is-active", on);
    }
  }

  function pointInRoot(el) {
    if (!el || !root) return null;
    var rr = root.getBoundingClientRect();
    var er = el.getBoundingClientRect();
    return {
      x: er.left + er.width / 2 - rr.left,
      y: er.top + er.height / 2 - rr.top,
    };
  }

  function curvePath(x1, y1, x2, y2) {
    var dx = Math.max(24, (x2 - x1) * 0.45);
    return (
      "M " +
      x1.toFixed(1) +
      " " +
      y1.toFixed(1) +
      " C " +
      (x1 + dx).toFixed(1) +
      " " +
      y1.toFixed(1) +
      ", " +
      (x2 - dx).toFixed(1) +
      " " +
      y2.toFixed(1) +
      ", " +
      x2.toFixed(1) +
      " " +
      y2.toFixed(1)
    );
  }

  function ensureConnectorElements() {
    if (!svg || !hotelNode) return;
    var i;
    for (i = 0; i < futures.length; i++) {
      var id = futures[i].getAttribute("data-future-id");
      if (!paths[id]) {
        var path = document.createElementNS("http://www.w3.org/2000/svg", "path");
        path.setAttribute("data-connector-for", id);
        path.setAttribute("pathLength", "1");
        svg.appendChild(path);
        paths[id] = path;

        var circle = document.createElementNS("http://www.w3.org/2000/svg", "circle");
        circle.setAttribute("r", "4");
        circle.setAttribute("class", "mf-node");
        circle.setAttribute("data-node-for", id);
        svg.appendChild(circle);
        nodes[id] = circle;
      }
    }
  }

  function updateConnectorPaths() {
    if (!svg || !hotelNode) return;
    if (isTabletOrBelow()) {
      svg.style.display = "none";
      return;
    }
    svg.style.display = "";

    ensureConnectorElements();
    var origin = pointInRoot(hotelNode);
    if (!origin) return;

    var rr = root.getBoundingClientRect();
    svg.setAttribute("viewBox", "0 0 " + rr.width + " " + rr.height);
    svg.setAttribute("width", String(rr.width));
    svg.setAttribute("height", String(rr.height));

    var i;
    for (i = 0; i < futures.length; i++) {
      var btn = futures[i];
      var id = btn.getAttribute("data-future-id");
      var target = btn.querySelector(".mf-future-node") || btn;
      var end = pointInRoot(target);
      if (!end || !paths[id]) continue;
      paths[id].setAttribute("d", curvePath(origin.x, origin.y, end.x, end.y));
      if (nodes[id]) {
        nodes[id].setAttribute("cx", end.x.toFixed(1));
        nodes[id].setAttribute("cy", end.y.toFixed(1));
      }
    }
    updateConnectorActive(activeId);
  }

  function drawEntranceConnectors() {
    if (reduceMotion || isTabletOrBelow()) return;
    var key;
    for (key in paths) {
      if (!Object.prototype.hasOwnProperty.call(paths, key)) continue;
      paths[key].classList.add("is-drawing");
    }
    window.setTimeout(function () {
      var k;
      for (k in paths) {
        if (!Object.prototype.hasOwnProperty.call(paths, k)) continue;
        paths[k].classList.remove("is-drawing");
      }
    }, 520);
  }

  function pin(id) {
    pinnedId = id;
    setFutureState(id);
  }

  function preview(id) {
    setFutureState(id);
  }

  function onEnterFuture(id) {
    if (isMobile()) return;
    preview(id);
  }

  function onLeaveFutures() {
    if (isMobile()) return;
    setFutureState(pinnedId);
  }

  for (var fi = 0; fi < futures.length; fi++) {
    (function (btn) {
      var id = btn.getAttribute("data-future-id");
      btn.addEventListener("mouseenter", function () {
        onEnterFuture(id);
      });
      btn.addEventListener("focus", function () {
        preview(id);
      });
      btn.addEventListener("click", function () {
        pin(id);
      });
      btn.addEventListener("keydown", function (ev) {
        if (ev.key === "Enter" || ev.key === " ") {
          ev.preventDefault();
          pin(id);
        }
      });
    })(futures[fi]);
  }

  var futuresWrap = root.querySelector(".mf-futures");
  if (futuresWrap) {
    futuresWrap.addEventListener("mouseleave", onLeaveFutures);
  }

  function onResize() {
    window.clearTimeout(resizeTimer);
    resizeTimer = window.setTimeout(updateConnectorPaths, 80);
  }

  if (typeof ResizeObserver !== "undefined") {
    var ro = new ResizeObserver(onResize);
    ro.observe(root);
  }
  window.addEventListener("resize", onResize);

  function afterImages(cb) {
    var imgs = root.querySelectorAll("img");
    var pending = 0;
    var i;
    for (i = 0; i < imgs.length; i++) {
      if (!imgs[i].complete) pending++;
    }
    if (!pending) {
      cb();
      return;
    }
    var done = 0;
    function tick() {
      done++;
      if (done >= pending) cb();
    }
    for (i = 0; i < imgs.length; i++) {
      if (!imgs[i].complete) {
        imgs[i].addEventListener("load", tick, { once: true });
        imgs[i].addEventListener("error", tick, { once: true });
      }
    }
  }

  // Initialize default state before marking ready so no-JS and first paint align
  setFutureState(DEFAULT_ID);
  root.classList.add("mf-js-ready");

  // Ensure entrance animation cannot leave critical content at opacity 0
  window.setTimeout(function () {
    root.classList.add("mf-entered");
  }, reduceMotion ? 0 : 1100);

  afterImages(function () {
    updateConnectorPaths();
    drawEntranceConnectors();
  });

  // Fallback if DOM already complete
  if (document.readyState === "complete") {
    updateConnectorPaths();
  } else {
    window.addEventListener("load", function () {
      updateConnectorPaths();
    });
  }
})();
