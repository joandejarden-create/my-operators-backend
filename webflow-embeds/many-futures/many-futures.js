(function () {
  var root = document.getElementById("dealality-many-futures");
  if (!root) return;

  var questions = root.querySelectorAll(".mf-q[data-q]");
  var panels = root.querySelectorAll(".mf-panel[data-panel]");
  var workspace = root.querySelector(".mf-workspace");
  var questionsCol = root.querySelector(".mf-questions");
  var svg = root.querySelector(".mf-connectors");
  var hotelNode = root.querySelector(".mf-hotel-node");
  var pinned = null;
  var reduceMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  var isMobile = function () {
    return window.matchMedia("(max-width: 767px)").matches;
  };

  function deferPanelImages(panel) {
    if (!panel) return;
    var imgs = panel.querySelectorAll("img.mf-feat-img[src]");
    for (var i = 0; i < imgs.length; i++) {
      if (!imgs[i].dataset.mfSrc) {
        imgs[i].dataset.mfSrc = imgs[i].getAttribute("src");
        imgs[i].removeAttribute("src");
      }
    }
  }

  function loadPanelImages(panel) {
    if (!panel) return;
    var imgs = panel.querySelectorAll("img.mf-feat-img[data-mf-src]");
    for (var i = 0; i < imgs.length; i++) {
      imgs[i].setAttribute("src", imgs[i].dataset.mfSrc);
      delete imgs[i].dataset.mfSrc;
    }
  }

  function setActive(id, pin) {
    if (!id) return;
    if (pin) pinned = id;

    var i;
    for (i = 0; i < questions.length; i++) {
      var on = questions[i].getAttribute("data-q") === id;
      questions[i].classList.toggle("is-active", on);
      questions[i].setAttribute("aria-pressed", on ? "true" : "false");
    }

    var activePanel = null;
    for (i = 0; i < panels.length; i++) {
      var match = panels[i].getAttribute("data-panel") === id;
      panels[i].classList.toggle("is-active", match);
      if (match) {
        activePanel = panels[i];
        panels[i].removeAttribute("hidden");
      } else {
        panels[i].setAttribute("hidden", "");
      }
    }

    loadPanelImages(activePanel);
    placeMobilePanel(id);
    drawConnectors(id);
  }

  function placeMobilePanel(id) {
    if (!workspace || !questionsCol) return;
    var activePanel = root.querySelector('.mf-panel[data-panel="' + id + '"]');
    var activeBtn = root.querySelector('.mf-q[data-q="' + id + '"]');
    if (!activePanel || !activeBtn) return;

    if (isMobile()) {
      // Insert active panel content region immediately after the active question
      if (activeBtn.nextElementSibling !== workspace) {
        activeBtn.insertAdjacentElement("afterend", workspace);
      }
    } else if (workspace.parentElement !== root.querySelector(".mf-layout")) {
      var layout = root.querySelector(".mf-layout");
      if (layout) layout.appendChild(workspace);
    }
  }

  function activeId() {
    return pinned || "rebrand";
  }

  function drawConnectors(id) {
    if (!svg || isMobile() || window.matchMedia("(max-width: 991px)").matches) {
      if (svg) svg.innerHTML = "";
      return;
    }
    var layout = root.querySelector(".mf-layout");
    if (!layout || !hotelNode) return;

    var layoutRect = layout.getBoundingClientRect();
    var hotelRect = hotelNode.getBoundingClientRect();
    var btn = root.querySelector('.mf-q[data-q="' + id + '"].is-active') || root.querySelector('.mf-q[data-q="' + id + '"]');
    var workspaceEl = root.querySelector(".mf-workspace");
    if (!btn || !workspaceEl) return;

    var btnRect = btn.getBoundingClientRect();
    var wsRect = workspaceEl.getBoundingClientRect();

    var x1 = hotelRect.left + hotelRect.width / 2 - layoutRect.left;
    var y1 = hotelRect.top + hotelRect.height / 2 - layoutRect.top;
    var x2 = btnRect.left - layoutRect.left;
    var y2 = btnRect.top + btnRect.height / 2 - layoutRect.top;
    var x3 = wsRect.left - layoutRect.left;
    var y3 = wsRect.top + Math.min(80, wsRect.height * 0.2) - layoutRect.top;

    var d1 =
      "M " +
      x1 +
      " " +
      y1 +
      " C " +
      (x1 + 40) +
      " " +
      y1 +
      ", " +
      (x2 - 36) +
      " " +
      y2 +
      ", " +
      x2 +
      " " +
      y2;
    var d2 =
      "M " +
      (btnRect.right - layoutRect.left) +
      " " +
      y2 +
      " C " +
      (btnRect.right - layoutRect.left + 36) +
      " " +
      y2 +
      ", " +
      (x3 - 28) +
      " " +
      y3 +
      ", " +
      x3 +
      " " +
      y3;

    while (svg.firstChild) svg.removeChild(svg.firstChild);

    var path1 = document.createElementNS("http://www.w3.org/2000/svg", "path");
    path1.setAttribute("d", d1);
    path1.classList.add("is-active");
    svg.appendChild(path1);

    var path2 = document.createElementNS("http://www.w3.org/2000/svg", "path");
    path2.setAttribute("d", d2);
    path2.classList.add("is-active");
    svg.appendChild(path2);

    var node = document.createElementNS("http://www.w3.org/2000/svg", "circle");
    node.setAttribute("cx", String(x2));
    node.setAttribute("cy", String(y2));
    node.setAttribute("r", "4");
    node.classList.add("mf-node", "is-active");
    svg.appendChild(node);
  }

  for (var i = 0; i < questions.length; i++) {
    (function (btn) {
      btn.addEventListener("mouseenter", function () {
        if (isMobile()) return;
        setActive(btn.getAttribute("data-q"), false);
      });
      btn.addEventListener("focus", function () {
        if (isMobile()) return;
        setActive(btn.getAttribute("data-q"), false);
      });
      btn.addEventListener("click", function () {
        setActive(btn.getAttribute("data-q"), true);
      });
      btn.addEventListener("keydown", function (e) {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          setActive(btn.getAttribute("data-q"), true);
        }
      });
    })(questions[i]);
  }

  if (questionsCol) {
    questionsCol.addEventListener("mouseleave", function () {
      if (isMobile()) return;
      setActive(activeId(), false);
    });
  }

  window.addEventListener("resize", function () {
    setActive(activeId(), false);
  });

  pinned = "rebrand";
  for (var p = 0; p < panels.length; p++) {
    if (!panels[p].classList.contains("is-active")) deferPanelImages(panels[p]);
  }
  setActive("rebrand", true);
  root.classList.add("mf-js-ready");

  if (reduceMotion) root.classList.add("mf-reduced-motion");
})();
