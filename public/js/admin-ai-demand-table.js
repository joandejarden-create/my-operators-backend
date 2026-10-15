/**
 * Shared AI Demand Admin table pattern (My Deals–inspired).
 * Sort / filter / select / compact actions — no My Deals business logic.
 *
 * Window export: DealalityAiAdminTable
 */
(function (global) {
  "use strict";

  var SORT_INDICATOR =
    '<span class="ada-sort-indicator" aria-hidden="true">' +
    '<span class="ada-sort-arrow ada-sort-arrow--up"></span>' +
    '<span class="ada-sort-arrow ada-sort-arrow--down"></span>' +
    "</span>";

  function esc(s) {
    return String(s == null ? "" : s)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function cmpNullLast(a, b, dir) {
    var aNull = a == null || a === "" || (typeof a === "number" && Number.isNaN(a));
    var bNull = b == null || b === "" || (typeof b === "number" && Number.isNaN(b));
    if (aNull && bNull) return 0;
    if (aNull) return 1; // missing always after defined
    if (bNull) return -1;
    if (a < b) return -1 * dir;
    if (a > b) return 1 * dir;
    return 0;
  }

  /**
   * @param {object[]} rows
   * @param {{ key: string, dir: 1|-1, type?: 'string'|'number'|'date', tieBreak?: string }} sort
   * @param {(row:object, key:string)=>any} [getter]
   */
  function sortRows(rows, sort, getter) {
    var key = sort && sort.key;
    var dir = sort && sort.dir === -1 ? -1 : 1;
    var type = (sort && sort.type) || "string";
    var tie = (sort && sort.tieBreak) || "displayName";
    var get =
      getter ||
      function (row, k) {
        return row[k];
      };
    return (rows || []).slice().sort(function (ra, rb) {
      var a = get(ra, key);
      var b = get(rb, key);
      if (type === "number") {
        a = a == null || a === "" ? null : Number(a);
        b = b == null || b === "" ? null : Number(b);
        if (a != null && Number.isNaN(a)) a = null;
        if (b != null && Number.isNaN(b)) b = null;
      } else if (type === "date") {
        a = a ? new Date(a).getTime() : null;
        b = b ? new Date(b).getTime() : null;
        if (a != null && Number.isNaN(a)) a = null;
        if (b != null && Number.isNaN(b)) b = null;
      } else {
        a = a == null || a === "" ? null : String(a).toLowerCase();
        b = b == null || b === "" ? null : String(b).toLowerCase();
      }
      var c = cmpNullLast(a, b, dir);
      if (c !== 0) return c;
      var ta = String(get(ra, tie) || get(ra, "hotelId") || get(ra, "id") || "");
      var tb = String(get(rb, tie) || get(rb, "hotelId") || get(rb, "id") || "");
      return ta.localeCompare(tb);
    });
  }

  function createSelectionStore() {
    var selected = Object.create(null);
    return {
      has: function (id) {
        return !!selected[String(id)];
      },
      add: function (id) {
        if (id == null || id === "") return;
        selected[String(id)] = true;
      },
      remove: function (id) {
        delete selected[String(id)];
      },
      toggle: function (id) {
        id = String(id);
        if (selected[id]) delete selected[id];
        else selected[id] = true;
      },
      clear: function () {
        selected = Object.create(null);
      },
      /** Keep only IDs still present in the accessible set (e.g. after filter). */
      retain: function (idList) {
        var allow = Object.create(null);
        (idList || []).forEach(function (id) {
          allow[String(id)] = true;
        });
        Object.keys(selected).forEach(function (id) {
          if (!allow[id]) delete selected[id];
        });
      },
      ids: function () {
        return Object.keys(selected);
      },
      count: function () {
        return Object.keys(selected).length;
      },
      setPage: function (idList, on) {
        (idList || []).forEach(function (id) {
          if (on) selected[String(id)] = true;
          else delete selected[String(id)];
        });
      },
    };
  }

  function createTableController(opts) {
    opts = opts || {};
    var sortKey = opts.defaultSortKey || "displayName";
    var sortDir = opts.defaultSortDir === -1 ? -1 : 1;
    var sortTypes = opts.sortTypes || {};
    var selection = createSelectionStore();
    var pageSize = opts.pageSize || 0; // 0 = no client pagination
    var page = 1;

    return {
      selection: selection,
      getSort: function () {
        return {
          key: sortKey,
          dir: sortDir,
          type: sortTypes[sortKey] || "string",
          tieBreak: opts.tieBreak || "displayName",
        };
      },
      setSort: function (key) {
        if (!key) return;
        if (sortKey === key) sortDir = sortDir === 1 ? -1 : 1;
        else {
          sortKey = key;
          sortDir = 1;
        }
        page = 1;
      },
      resetView: function () {
        sortKey = opts.defaultSortKey || "displayName";
        sortDir = opts.defaultSortDir === -1 ? -1 : 1;
        page = 1;
        selection.clear();
        if (typeof opts.onResetFilters === "function") opts.onResetFilters();
      },
      getPage: function () {
        return page;
      },
      setPage: function (n) {
        page = Math.max(1, Number(n) || 1);
      },
      pageSize: pageSize,
      /**
       * @returns {{ rows: object[], total: number, showing: number, page: number, pageCount: number }}
       */
      apply: function (allRows, filterFn) {
        var filtered = typeof filterFn === "function" ? allRows.filter(filterFn) : allRows.slice();
        var sorted = sortRows(filtered, this.getSort(), opts.getter);
        // Selection must not silently expand — retain only IDs still in filtered set
        var idKey = opts.idKey || "hotelId";
        selection.retain(
          sorted.map(function (r) {
            return r[idKey];
          })
        );
        var total = sorted.length;
        var pageCount = pageSize > 0 ? Math.max(1, Math.ceil(total / pageSize)) : 1;
        if (page > pageCount) page = pageCount;
        var slice =
          pageSize > 0
            ? sorted.slice((page - 1) * pageSize, page * pageSize)
            : sorted;
        return {
          rows: slice,
          allFiltered: sorted,
          total: total,
          showing: slice.length,
          page: page,
          pageCount: pageCount,
        };
      },
      paintSortHeaders: function (thead) {
        if (!thead) return;
        thead.querySelectorAll("th[data-sort]").forEach(function (th) {
          th.classList.add("ada-th--sortable");
          th.setAttribute("role", "columnheader");
          th.setAttribute("tabindex", "0");
          var k = th.getAttribute("data-sort");
          th.classList.toggle("sort-asc", k === sortKey && sortDir === 1);
          th.classList.toggle("sort-desc", k === sortKey && sortDir === -1);
          th.setAttribute(
            "aria-sort",
            k === sortKey ? (sortDir === 1 ? "ascending" : "descending") : "none"
          );
          if (!th.querySelector(".ada-sort-indicator")) {
            var label = th.querySelector(".ada-th-label") || th;
            if (th.querySelector(".ada-th-label")) {
              th.querySelector(".ada-th-label").insertAdjacentHTML(
                "beforeend",
                SORT_INDICATOR
              );
            } else {
              th.innerHTML =
                '<span class="ada-th-label">' +
                th.innerHTML +
                "</span>" +
                SORT_INDICATOR;
            }
          }
        });
      },
      wireSortHeaders: function (thead, onChange) {
        if (!thead || thead.__adaSortWired) return;
        thead.__adaSortWired = true;
        thead.addEventListener("click", function (ev) {
          var th = ev.target.closest("th[data-sort]");
          if (!th || !thead.contains(th)) return;
          this.setSort(th.getAttribute("data-sort"));
          this.paintSortHeaders(thead);
          if (typeof onChange === "function") onChange();
        }.bind(this));
        thead.addEventListener("keydown", function (ev) {
          if (ev.key !== "Enter" && ev.key !== " ") return;
          var th = ev.target.closest("th[data-sort]");
          if (!th) return;
          ev.preventDefault();
          this.setSort(th.getAttribute("data-sort"));
          this.paintSortHeaders(thead);
          if (typeof onChange === "function") onChange();
        }.bind(this));
      },
    };
  }

  function resultsCountHtml(showing, total, entityLabel) {
    entityLabel = entityLabel || "hotels";
    return (
      '<div class="ada-results-count" aria-live="polite">Showing <strong>' +
      esc(showing) +
      "</strong> of <strong>" +
      esc(total) +
      "</strong> " +
      esc(entityLabel) +
      "</div>"
    );
  }

  function selectionBarHtml(selectedCount, pageSelectHint) {
    return (
      '<div class="ada-selection-bar" role="status">' +
      '<span class="ada-selection-count">' +
      esc(selectedCount) +
      " selected</span>" +
      (pageSelectHint
        ? '<span class="ada-selection-hint" title="The header checkbox selects only the rows on this page, not every filtered row."> · Select this page</span>'
        : "") +
      "</div>"
    );
  }

  function iconSvg(name) {
    var common =
      ' viewBox="0 0 24 24" width="18" height="18" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true"';
    if (name === "file") {
      return (
        "<svg" +
        common +
        '><path d="M14 2H6a2 2 0 0 0-2 2v16a2 2 0 0 0 2 2h12a2 2 0 0 0 2-2V8z"/><polyline points="14 2 14 8 20 8"/></svg>'
      );
    }
    if (name === "download") {
      return (
        "<svg" +
        common +
        '><path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"/><polyline points="7 10 12 15 17 10"/><line x1="12" y1="15" x2="12" y2="3"/></svg>'
      );
    }
    if (name === "external") {
      return (
        "<svg" +
        common +
        '><path d="M18 13v6a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2V8a2 2 0 0 1 2-2h6"/><polyline points="15 3 21 3 21 9"/><line x1="10" y1="14" x2="21" y2="3"/></svg>'
      );
    }
    if (name === "refresh") {
      return (
        "<svg" +
        common +
        '><polyline points="23 4 23 10 17 10"/><polyline points="1 20 1 14 7 14"/><path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"/></svg>'
      );
    }
    if (name === "copy") {
      return (
        "<svg" +
        common +
        '><rect x="9" y="9" width="13" height="13" rx="2"/><path d="M5 15H4a2 2 0 0 1-2-2V4a2 2 0 0 1 2-2h9a2 2 0 0 1 2 2v1"/></svg>'
      );
    }
    if (name === "archive") {
      return (
        "<svg" +
        common +
        '><polyline points="21 8 21 21 3 21 3 8"/><rect x="1" y="3" width="22" height="5"/><line x1="10" y1="12" x2="14" y2="12"/></svg>'
      );
    }
    if (name === "users") {
      return (
        "<svg" +
        common +
        '><path d="M17 21v-2a4 4 0 0 0-4-4H5a4 4 0 0 0-4 4v2"/><circle cx="9" cy="7" r="4"/><path d="M23 21v-2a4 4 0 0 0-3-3.87"/><path d="M16 3.13a4 4 0 0 1 0 7.75"/></svg>'
      );
    }
    if (name === "list") {
      return (
        "<svg" +
        common +
        '><line x1="8" y1="6" x2="21" y2="6"/><line x1="8" y1="12" x2="21" y2="12"/><line x1="8" y1="18" x2="21" y2="18"/><line x1="3" y1="6" x2="3.01" y2="6"/><line x1="3" y1="12" x2="3.01" y2="12"/><line x1="3" y1="18" x2="3.01" y2="18"/></svg>'
      );
    }
    if (name === "more") {
      return (
        "<svg" +
        common +
        '><circle cx="12" cy="5" r="1"/><circle cx="12" cy="12" r="1"/><circle cx="12" cy="19" r="1"/></svg>'
      );
    }
    if (name === "ban") {
      return (
        "<svg" +
        common +
        '><circle cx="12" cy="12" r="10"/><line x1="4.93" y1="4.93" x2="19.07" y2="19.07"/></svg>'
      );
    }
    return (
      "<svg" +
      common +
      '><circle cx="12" cy="12" r="9"/></svg>'
    );
  }

  function iconForAct(act, disabled) {
    if (disabled && (act === "noop" || !act)) return "ban";
    var map = {
      "view-pdf": "file",
      "download-pdf": "download",
      open: "external",
      "open-gdi": "external",
      "open-adp": "external",
      "open-adp-client": "external",
      "open-action-plan": "list",
      "goto-action-plan": "list",
      generate: "refresh",
      regenerate: "refresh",
      draft: "refresh",
      "copy-gdi": "copy",
      "copy-adp": "copy",
      "copy-adp-client": "copy",
      archive: "archive",
      "open-archive": "archive",
      meeting: "users",
      "view-data": "list",
      noop: "ban",
    };
    return map[act] || "external";
  }

  /** Portal menu state (My Deals pattern: fixed to document.body — escapes sticky stacking). */
  var moreMenuState = {
    el: null,
    trigger: null,
    onDocClick: null,
    onKey: null,
    onReposition: null,
  };

  function closeMoreMenu() {
    if (moreMenuState.trigger) {
      moreMenuState.trigger.setAttribute("aria-expanded", "false");
      moreMenuState.trigger.classList.remove("is-open");
    }
    if (moreMenuState.el && moreMenuState.el.parentNode) {
      moreMenuState.el.parentNode.removeChild(moreMenuState.el);
    }
    if (moreMenuState.onDocClick) {
      document.removeEventListener("mousedown", moreMenuState.onDocClick, true);
      document.removeEventListener("click", moreMenuState.onDocClick, true);
    }
    if (moreMenuState.onKey) {
      document.removeEventListener("keydown", moreMenuState.onKey, true);
    }
    if (moreMenuState.onReposition) {
      window.removeEventListener("scroll", moreMenuState.onReposition, true);
      window.removeEventListener("resize", moreMenuState.onReposition);
    }
    var restore = moreMenuState.trigger;
    moreMenuState.el = null;
    moreMenuState.trigger = null;
    moreMenuState.onDocClick = null;
    moreMenuState.onKey = null;
    moreMenuState.onReposition = null;
    if (restore && typeof restore.focus === "function") {
      try {
        restore.focus();
      } catch (_e) {}
    }
  }

  function positionMoreMenu(menu, trigger) {
    if (!menu || !trigger) return;
    var rect = trigger.getBoundingClientRect();
    var pad = 8;
    var mw = Math.max(menu.offsetWidth || 220, 200);
    var mh = menu.offsetHeight || 160;
    var left = Math.min(
      Math.max(pad, rect.right - mw),
      Math.max(pad, window.innerWidth - mw - pad)
    );
    var below = rect.bottom + 6;
    var above = rect.top - mh - 6;
    var top;
    if (below + mh <= window.innerHeight - pad) top = below;
    else if (above >= pad) top = above;
    else {
      top = Math.min(
        Math.max(pad, below),
        Math.max(pad, window.innerHeight - mh - pad)
      );
    }
    menu.style.position = "fixed";
    menu.style.left = Math.round(left) + "px";
    menu.style.top = Math.round(top) + "px";
    menu.style.right = "auto";
    menu.style.bottom = "auto";
    menu.style.zIndex = "10050";
  }

  function fireRowAct(trigger, act) {
    var wrap = trigger && trigger.closest(".ada-row-actions");
    if (!wrap || !act) return;
    var proxy = document.createElement("button");
    proxy.type = "button";
    proxy.setAttribute("data-act", act);
    proxy.className = "ada-action-proxy";
    proxy.style.cssText =
      "position:absolute;width:1px;height:1px;opacity:0;pointer-events:none;left:0;top:0;";
    wrap.appendChild(proxy);
    try {
      proxy.click();
    } finally {
      if (proxy.parentNode) proxy.parentNode.removeChild(proxy);
    }
  }

  function openMoreMenu(trigger) {
    if (!trigger || !document.body) return;
    if (moreMenuState.trigger === trigger) {
      closeMoreMenu();
      return;
    }
    closeMoreMenu();

    var template = trigger.parentNode
      ? trigger.parentNode.querySelector("template.ada-more-template")
      : null;
    if (!template) return;

    var menu = document.createElement("div");
    menu.className = "ada-more-menu";
    menu.setAttribute("role", "menu");
    menu.setAttribute("data-ada-portal-menu", "1");
    menu.appendChild(template.content.cloneNode(true));

    document.body.appendChild(menu);
    moreMenuState.el = menu;
    moreMenuState.trigger = trigger;
    trigger.setAttribute("aria-expanded", "true");
    trigger.classList.add("is-open");

    positionMoreMenu(menu, trigger);

    var items = menu.querySelectorAll('[role="menuitem"]');
    if (items.length) {
      try {
        items[0].focus();
      } catch (_e) {}
    }

    moreMenuState.onDocClick = function (evt) {
      if (!moreMenuState.el) return;
      if (moreMenuState.el.contains(evt.target)) return;
      if (moreMenuState.trigger && moreMenuState.trigger.contains(evt.target)) {
        return;
      }
      closeMoreMenu();
    };
    moreMenuState.onKey = function (evt) {
      if (!moreMenuState.el) return;
      if (evt.key === "Escape") {
        evt.preventDefault();
        closeMoreMenu();
        return;
      }
      if (evt.key !== "ArrowDown" && evt.key !== "ArrowUp" && evt.key !== "Home" && evt.key !== "End") {
        return;
      }
      var list = Array.prototype.slice.call(
        moreMenuState.el.querySelectorAll('[role="menuitem"]:not([disabled])')
      );
      if (!list.length) return;
      var idx = list.indexOf(document.activeElement);
      evt.preventDefault();
      if (evt.key === "Home") list[0].focus();
      else if (evt.key === "End") list[list.length - 1].focus();
      else if (evt.key === "ArrowDown") {
        list[(idx + 1) % list.length].focus();
      } else if (evt.key === "ArrowUp") {
        list[(idx <= 0 ? list.length : idx) - 1].focus();
      }
    };
    moreMenuState.onReposition = function () {
      if (!moreMenuState.el || !moreMenuState.trigger) return;
      if (!document.body.contains(moreMenuState.trigger)) {
        closeMoreMenu();
        return;
      }
      positionMoreMenu(moreMenuState.el, moreMenuState.trigger);
    };

    document.addEventListener("mousedown", moreMenuState.onDocClick, true);
    document.addEventListener("keydown", moreMenuState.onKey, true);
    window.addEventListener("scroll", moreMenuState.onReposition, true);
    window.addEventListener("resize", moreMenuState.onReposition);

    menu.addEventListener("click", function (evt) {
      var item = evt.target.closest("[data-act]");
      if (!item || !menu.contains(item)) return;
      evt.preventDefault();
      evt.stopPropagation();
      if (item.disabled || item.classList.contains("is-disabled")) return;
      var act = item.getAttribute("data-act");
      var anchor = moreMenuState.trigger;
      closeMoreMenu();
      fireRowAct(anchor, act);
    });
  }

  function installMoreMenuOnce() {
    if (global.__ADA_MORE_MENU_INSTALLED__) return;
    global.__ADA_MORE_MENU_INSTALLED__ = true;
    document.addEventListener(
      "click",
      function (ev) {
        var trigger = ev.target.closest(".ada-more-trigger");
        if (!trigger) return;
        ev.preventDefault();
        ev.stopPropagation();
        openMoreMenu(trigger);
      },
      true
    );
  }

  if (typeof document !== "undefined") {
    if (document.readyState === "loading") {
      document.addEventListener("DOMContentLoaded", installMoreMenuOnce);
    } else {
      installMoreMenuOnce();
    }
  }

  /**
   * Compact icon actions + portal More menu (escapes sticky Actions stacking).
   * @param {{ primary?: {label, act, disabled?, title?, icon?}[], more?: {label, act, disabled?, title?, icon?}[] }} pack
   */
  function actionsHtml(pack, dataAttrs) {
    pack = pack || {};
    installMoreMenuOnce();
    var attrs = "";
    Object.keys(dataAttrs || {}).forEach(function (k) {
      attrs += " data-" + k + '="' + esc(dataAttrs[k]) + '"';
    });
    var html = '<div class="ada-row-actions"' + attrs + ">";
    (pack.primary || []).forEach(function (a) {
      if (!a) return;
      var label = a.label || a.act || "Action";
      var title = a.title || label;
      var icon = a.icon || iconForAct(a.act, a.disabled);
      html +=
        '<button type="button" class="ada-action-icon' +
        (a.disabled ? " is-disabled" : "") +
        '" data-act="' +
        esc(a.act) +
        '"' +
        (a.disabled ? " disabled" : "") +
        ' title="' +
        esc(title) +
        '" aria-label="' +
        esc(label) +
        '">' +
        iconSvg(icon) +
        "</button>";
    });
    if (pack.more && pack.more.length) {
      html +=
        '<button type="button" class="ada-action-icon ada-more-trigger" aria-label="More actions" title="More actions" aria-haspopup="menu" aria-expanded="false">' +
        iconSvg("more") +
        "</button>";
      html += '<template class="ada-more-template">';
      pack.more.forEach(function (a) {
        if (!a) return;
        var label = a.label || a.act || "Action";
        var title = a.title || label;
        var icon = a.icon || iconForAct(a.act, a.disabled);
        html +=
          '<button type="button" role="menuitem" class="ada-more-menu__item' +
          (a.disabled ? " is-disabled" : "") +
          '" data-act="' +
          esc(a.act) +
          '"' +
          (a.disabled ? " disabled" : "") +
          ' title="' +
          esc(title) +
          '" aria-label="' +
          esc(label) +
          '">' +
          '<span class="ada-more-menu__icon" aria-hidden="true">' +
          iconSvg(icon) +
          '</span><span class="ada-more-menu__label">' +
          esc(label) +
          "</span>" +
          (a.disabled && a.title
            ? '<span class="ada-more-menu__why">' + esc(a.title) + "</span>"
            : "") +
          "</button>";
      });
      html += "</template>";
    }
    html += "</div>";
    return html;
  }

  function checkboxCell(id, checked) {
    return (
      '<td class="ada-cell-check">' +
      '<input type="checkbox" class="ada-row-check" data-id="' +
      esc(id) +
      '"' +
      (checked ? " checked" : "") +
      ' aria-label="Select row" />' +
      "</td>"
    );
  }

  function selectAllHeader(checked, indeterminate) {
    return (
      '<th class="ada-cell-check ada-th--check" scope="col">' +
      '<input type="checkbox" class="ada-select-page" title="Select this page (does not select every filtered row)" aria-label="Select this page"' +
      (checked ? " checked" : "") +
      (indeterminate ? " data-indeterminate=1" : "") +
      " />" +
      "</th>"
    );
  }

  function hotelLinkCell(name, href, subtitle) {
    var inner = href
      ? '<a class="ada-hotel-link" href="' +
        esc(href) +
        '" target="_blank" rel="noopener">' +
        esc(name) +
        "</a>"
      : "<strong>" + esc(name || "—") + "</strong>";
    return (
      '<td class="ada-cell-hotel">' +
      inner +
      (subtitle
        ? '<div class="ada-subtitle ada-hotel-id">' + esc(subtitle) + "</div>"
        : "") +
      "</td>"
    );
  }

  function numCell(v) {
    if (v == null || v === "") return '<td class="ada-cell-num">—</td>';
    return '<td class="ada-cell-num">' + esc(v) + "</td>";
  }

  function textCell(v, cls) {
    if (v == null || v === "") return "<td" + (cls ? ' class="' + cls + '"' : "") + ">—</td>";
    return "<td" + (cls ? ' class="' + cls + '"' : "") + ">" + esc(v) + "</td>";
  }

  function fmtDate(iso) {
    if (!iso) return "—";
    var d = new Date(iso);
    if (Number.isNaN(d.getTime())) return String(iso);
    return d.toISOString().slice(0, 10);
  }

  function fmtDateTime(iso) {
    if (!iso) return "—";
    var d = new Date(iso);
    if (Number.isNaN(d.getTime())) return String(iso);
    return d.toISOString().replace("T", " ").slice(0, 16) + " UTC";
  }

  function statusBadge(kind, label) {
    var cls = "adr-badge";
    if (kind === "ok") cls += " adr-badge--ok";
    if (kind === "warn") cls += " adr-badge--warn";
    if (kind === "danger") cls += " adr-badge--danger";
    return '<span class="' + cls + '">' + esc(label) + "</span>";
  }

  global.DealalityAiAdminTable = {
    esc: esc,
    sortRows: sortRows,
    createSelectionStore: createSelectionStore,
    createTableController: createTableController,
    resultsCountHtml: resultsCountHtml,
    selectionBarHtml: selectionBarHtml,
    actionsHtml: actionsHtml,
    closeMoreMenu: closeMoreMenu,
    openMoreMenu: openMoreMenu,
    installMoreMenuOnce: installMoreMenuOnce,
    checkboxCell: checkboxCell,
    selectAllHeader: selectAllHeader,
    hotelLinkCell: hotelLinkCell,
    numCell: numCell,
    textCell: textCell,
    fmtDate: fmtDate,
    fmtDateTime: fmtDateTime,
    statusBadge: statusBadge,
    SORT_INDICATOR: SORT_INDICATOR,
  };
})(typeof window !== "undefined" ? window : globalThis);
