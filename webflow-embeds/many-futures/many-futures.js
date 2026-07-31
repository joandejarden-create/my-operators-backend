(function () {
  var root = document.getElementById("dealality-many-futures");
  if (!root) return;

  var questions = root.querySelectorAll(".mf-q[data-q]");
  var panels = root.querySelectorAll(".mf-panel[data-panel]");
  var workspace = root.querySelector(".mf-workspace");
  var questionsCol = root.querySelector(".mf-questions");
  var svg = root.querySelector(".mf-connectors");
  var hotelNode = root.querySelector(".mf-hotel-node");
  var current = "rebrand";
  var reduceMotion = window.matchMedia("(prefers-reduced-motion: reduce)").matches;
  var isMobile = function () {
    return window.matchMedia("(max-width: 767px)").matches;
  };

  function stashAttr(el, name) {
    if (!el || !el.hasAttribute(name)) return;
    var dataKey = name === "src" ? "mfSrc" : name === "srcset" ? "mfSrcset" : null;
    if (!dataKey) return;
    if (!el.dataset[dataKey]) el.dataset[dataKey] = el.getAttribute(name);
    el.removeAttribute(name);
  }

  function restoreAttr(el, name) {
    if (!el) return;
    var dataKey = name === "src" ? "mfSrc" : name === "srcset" ? "mfSrcset" : null;
    if (!dataKey || !el.dataset[dataKey]) return;
    el.setAttribute(name, el.dataset[dataKey]);
    delete el.dataset[dataKey];
  }

  function deferPicture(picture) {
    if (!picture) return;
    var sources = picture.querySelectorAll("source");
    for (var i = 0; i < sources.length; i++) stashAttr(sources[i], "srcset");
    var img = picture.querySelector("img");
    if (img) {
      stashAttr(img, "srcset");
      stashAttr(img, "src");
    }
  }

  function loadPicture(picture) {
    if (!picture) return;
    var sources = picture.querySelectorAll("source");
    for (var i = 0; i < sources.length; i++) restoreAttr(sources[i], "srcset");
    var img = picture.querySelector("img");
    if (img) {
      restoreAttr(img, "srcset");
      restoreAttr(img, "src");
    }
  }

  function deferPanelImages(panel) {
    if (!panel) return;
    var pictures = panel.querySelectorAll("picture");
    for (var i = 0; i < pictures.length; i++) {
      if (pictures[i].closest(".mf-hotel-media")) continue;
      deferPicture(pictures[i]);
    }
    var imgs = panel.querySelectorAll("img.mf-feat-img[src]");
    for (var j = 0; j < imgs.length; j++) {
      if (!imgs[j].closest("picture")) stashAttr(imgs[j], "src");
    }
  }

  function loadPanelImages(panel) {
    if (!panel) return;
    var pictures = panel.querySelectorAll("picture");
    for (var i = 0; i < pictures.length; i++) {
      if (pictures[i].closest(".mf-hotel-media")) continue;
      loadPicture(pictures[i]);
    }
    var imgs = panel.querySelectorAll("img.mf-feat-img");
    for (var j = 0; j < imgs.length; j++) {
      if (!imgs[j].closest("picture")) restoreAttr(imgs[j], "src");
    }
  }

  function setActive(id) {
    if (!id) return;
    if (id === current) {
      placeMobilePanel(id);
      syncWorkspaceLayout(id);
      drawConnectors(id);
      return;
    }
    current = id;

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
        deferPanelImages(panels[i]);
      }
    }

    loadPanelImages(activePanel);
    placeMobilePanel(id);
    syncWorkspaceLayout(id);
    drawConnectors(id);
  }

  function syncWorkspaceLayout(id) {
    if (!workspace) return;
    var panel = root.querySelector('.mf-panel[data-panel="' + id + '"]');
    var two =
      panel &&
      (panel.querySelector('.mf-features[data-mf-layout="two-panel"]') ||
        panel.querySelector(".mf-features--duo"));
    workspace.classList.toggle("mf-workspace--two-panel", !!two);
  }

  function placeMobilePanel(id) {
    if (!workspace || !questionsCol) return;
    var activePanel = root.querySelector('.mf-panel[data-panel="' + id + '"]');
    var activeBtn = root.querySelector('.mf-q[data-q="' + id + '"]');
    if (!activePanel || !activeBtn) return;

    if (isMobile()) {
      if (activeBtn.nextElementSibling !== workspace) {
        activeBtn.insertAdjacentElement("afterend", workspace);
      }
    } else {
      var layout = root.querySelector(".mf-layout");
      if (layout && workspace.parentElement !== layout) {
        layout.appendChild(workspace);
      }
    }
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
    var btn =
      root.querySelector('.mf-q[data-q="' + id + '"].is-active') ||
      root.querySelector('.mf-q[data-q="' + id + '"]');
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
      "M " + x1 + " " + y1 + " C " + (x1 + 40) + " " + y1 + ", " + (x2 - 36) + " " + y2 + ", " + x2 + " " + y2;
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
      /* Desktop: click/keyboard select. Hover preview removed — it reset when
         the pointer moved into the feature workspace to read the panels. */
      btn.addEventListener("click", function () {
        setActive(btn.getAttribute("data-q"));
      });
      btn.addEventListener("keydown", function (e) {
        if (e.key === "Enter" || e.key === " ") {
          e.preventDefault();
          setActive(btn.getAttribute("data-q"));
        }
      });
    })(questions[i]);
  }

  window.addEventListener("resize", function () {
    setActive(current);
  });

  for (var p = 0; p < panels.length; p++) {
    if (!panels[p].classList.contains("is-active")) deferPanelImages(panels[p]);
  }
  setActive("rebrand");
  root.classList.add("mf-js-ready");

  if (reduceMotion) root.classList.add("mf-reduced-motion");
})();
