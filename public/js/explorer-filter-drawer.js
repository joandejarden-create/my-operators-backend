/**
 * Shared slide-over filter drawer for Brand Explorer and Operator Explorer.
 * Reuses radar-filter-drawer.css class names.
 */
(function (global) {
  "use strict";

  function el(id) {
    return document.getElementById(id);
  }

  function init(config) {
    var overlay = el(config.overlayId);
    var drawer = el(config.drawerId);
    var openBtn = el(config.openBtnId);
    var closeBtn = el(config.closeBtnId);
    var doneBtn = el(config.doneBtnId);
    var resetBtn = config.resetBtnId ? el(config.resetBtnId) : null;
    var filterIds = config.filterIds || [];
    var onChange = typeof config.onFilterChange === "function" ? config.onFilterChange : function () {};
    var countFn = typeof config.countActiveFilters === "function" ? config.countActiveFilters : null;
    var drawerBadgeId = config.drawerBadgeId || null;

    function isOpen() {
      return drawer && drawer.classList.contains("is-open");
    }

    function updateBadge() {
      if (!countFn) return;
      var count = countFn();
      var mainBadge = el(config.mainBadgeId || "filterCountBadge");
      if (mainBadge) {
        if (count > 0) {
          mainBadge.textContent = String(count);
          mainBadge.style.display = "inline-flex";
        } else {
          mainBadge.style.display = "none";
        }
      }
      if (drawerBadgeId) {
        var drawerBadge = el(drawerBadgeId);
        if (drawerBadge) {
          if (count > 0) {
            drawerBadge.textContent = String(count);
            drawerBadge.style.display = "inline-flex";
          } else {
            drawerBadge.style.display = "none";
          }
        }
      }
    }

    function openDrawer() {
      if (!drawer || !overlay) return;
      drawer.classList.add("is-open");
      overlay.classList.add("is-open");
      drawer.setAttribute("aria-hidden", "false");
      overlay.setAttribute("aria-hidden", "false");
      document.body.classList.add("explorer-filter-drawer-open");
    }

    function closeDrawer() {
      if (!drawer || !overlay) return;
      drawer.classList.remove("is-open");
      overlay.classList.remove("is-open");
      drawer.setAttribute("aria-hidden", "true");
      overlay.setAttribute("aria-hidden", "true");
      document.body.classList.remove("explorer-filter-drawer-open");
    }

    if (openBtn) openBtn.addEventListener("click", openDrawer);
    else if (typeof console !== "undefined" && console.warn) {
      console.warn("[ExplorerFilterDrawer] open button not found:", config.openBtnId);
    }
    if (closeBtn) closeBtn.addEventListener("click", closeDrawer);
    if (doneBtn) doneBtn.addEventListener("click", closeDrawer);
    if (overlay) {
      overlay.addEventListener("click", function (e) {
        if (e.target === overlay) closeDrawer();
      });
    }
    if (resetBtn) {
      resetBtn.addEventListener("click", function () {
        if (typeof config.onReset === "function") config.onReset();
        updateBadge();
      });
    }

    filterIds.forEach(function (id) {
      var node = el(id);
      if (!node) return;
      var evt = node.tagName === "INPUT" ? "input" : "change";
      node.addEventListener(evt, function () {
        onChange();
        updateBadge();
      });
    });

    document.addEventListener("keydown", function (e) {
      if (e.key === "Escape" && isOpen()) closeDrawer();
    });

    return {
      open: openDrawer,
      close: closeDrawer,
      updateBadge: updateBadge,
      isOpen: isOpen,
    };
  }

  global.ExplorerFilterDrawer = { init: init };
})(typeof window !== "undefined" ? window : this);
