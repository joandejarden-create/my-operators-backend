/**
 * Mount alignment tab on consolidated Operator DNA profile (Explorer shell).
 */
(function (global) {
  "use strict";

  var ALIGNMENT_TAB = "Alignment Context";

  var ALIGNMENT_ICON =
    '<svg viewBox="0 0 24 24"><path d="M12 1v22"></path><path d="M17 5H9.5a3.5 3.5 0 0 0 0 7h5a3.5 3.5 0 1 1 0 7H6"></path></svg>';

  var ALIGNMENT_LABEL = "Alignment<br>Context";

  function nz(v) {
    if (v == null) return "";
    return String(v).trim();
  }

  function escapeHtml(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function activateTab(tabName) {
    document.querySelectorAll(".section-nav-item").forEach(function (b) {
      b.classList.toggle("active", b.getAttribute("data-tab") === tabName);
    });
    document.querySelectorAll(".tab-panel").forEach(function (p) {
      p.classList.toggle("active", p.getAttribute("data-panel") === tabName);
    });
  }

  function findTabNavButton(tabsEl, tabName) {
    if (!tabsEl || !tabName) return null;
    var buttons = tabsEl.querySelectorAll(".section-nav-item");
    for (var i = 0; i < buttons.length; i++) {
      if (buttons[i].getAttribute("data-tab") === tabName) return buttons[i];
    }
    return null;
  }

  function findTabPanel(panelsRoot, tabName) {
    if (!panelsRoot || !tabName) return null;
    var panels = panelsRoot.querySelectorAll(".tab-panel");
    for (var i = 0; i < panels.length; i++) {
      if (panels[i].getAttribute("data-panel") === tabName) return panels[i];
    }
    return null;
  }

  function isOperatorDnaProfilePage() {
    return /operator-dna-profile/i.test(global.location && global.location.pathname ? global.location.pathname : "");
  }

  /**
   * DNA-only tabs: Operator Materials (before Dealality Insights), then Dealality Insights.
   * Safe to call more than once — appendCustomTab skips duplicates.
   * @param {object} vm
   */
  function mountDnaExtensionTabs(vm) {
    if (!vm) return;
    if (!global.OperatorDnaDealalityInsights && !global.OperatorDnaMaterials) return;
    if (global.OperatorDnaMaterials && typeof global.OperatorDnaMaterials.mountTab === "function") {
      global.OperatorDnaMaterials.mountTab(vm);
    }
    if (global.OperatorDnaDealalityInsights && typeof global.OperatorDnaDealalityInsights.mountTab === "function") {
      global.OperatorDnaDealalityInsights.mountTab(vm);
    }
  }

  /**
   * Append a tab after gold-mock mount (DNA-only extensions).
   * @param {{ tabName: string, iconHtml: string, labelHtml: string, panelHtml: string, activateOnAppend?: boolean, insertBeforeTab?: string }} opts
   */
  function appendCustomTab(opts) {
    var tabsEl = document.getElementById("tabs");
    var panelsRoot = document.getElementById("panels");
    if (!tabsEl || !panelsRoot || !opts || !opts.tabName || !opts.panelHtml) return false;

    var tabName = opts.tabName;
    if (findTabNavButton(tabsEl, tabName)) return false;

    var btn = document.createElement("button");
    btn.type = "button";
    btn.className = "section-nav-item";
    btn.setAttribute("data-tab", tabName);
    btn.innerHTML =
      '<div class="section-nav-icon">' +
      (opts.iconHtml || "") +
      '</div><div class="section-nav-label">' +
      (opts.labelHtml || escapeHtml(tabName)) +
      "</div>";

    var section = document.createElement("section");
    section.className = "tab-panel";
    section.setAttribute("data-panel", tabName);
    section.innerHTML = opts.panelHtml;

    var beforeName = nz(opts.insertBeforeTab);
    if (beforeName) {
      var refBtn = findTabNavButton(tabsEl, beforeName);
      if (refBtn) tabsEl.insertBefore(btn, refBtn);
      else tabsEl.appendChild(btn);
      var refPanel = findTabPanel(panelsRoot, beforeName);
      if (refPanel) panelsRoot.insertBefore(section, refPanel);
      else panelsRoot.appendChild(section);
    } else {
      tabsEl.appendChild(btn);
      panelsRoot.appendChild(section);
    }

    btn.addEventListener("click", function () {
      activateTab(tabName);
    });

    if (opts.activateOnAppend) activateTab(tabName);
    return true;
  }

  function appendAlignmentTab(panelHtml) {
    appendCustomTab({
      tabName: ALIGNMENT_TAB,
      iconHtml: ALIGNMENT_ICON,
      labelHtml: ALIGNMENT_LABEL,
      panelHtml: panelHtml,
      activateOnAppend: false,
    });
  }

  global.OperatorDnaProfileMount = {
    ALIGNMENT_TAB: ALIGNMENT_TAB,
    appendAlignmentTab: appendAlignmentTab,
    appendCustomTab: appendCustomTab,
    activateTab: activateTab,
    mountDnaExtensionTabs: mountDnaExtensionTabs,
    isOperatorDnaProfilePage: isOperatorDnaProfilePage,
    escapeHtml: escapeHtml,
  };
})(typeof window !== "undefined" ? window : global);
