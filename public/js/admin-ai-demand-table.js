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

  function selectionBarHtml(selectedCount, pageCount) {
    return (
      '<div class="ada-selection-bar" role="status">' +
      '<span class="ada-selection-count">' +
      esc(selectedCount) +
      " selected</span>" +
      (pageCount
        ? '<span class="ada-selection-hint"> · page select ≠ all filtered</span>'
        : "") +
      "</div>"
    );
  }

  /**
   * Compact actions: primary buttons + More menu.
   * @param {{ primary?: {label, act, disabled?, title?}[], more?: {label, act, disabled?, title?}[] }} pack
   */
  function actionsHtml(pack, dataAttrs) {
    pack = pack || {};
    var attrs = "";
    Object.keys(dataAttrs || {}).forEach(function (k) {
      attrs += " data-" + k + '="' + esc(dataAttrs[k]) + '"';
    });
    var html = '<div class="ada-row-actions"' + attrs + ">";
    (pack.primary || []).forEach(function (a) {
      if (!a) return;
      html +=
        '<button type="button" class="ada-icon-btn' +
        (a.disabled ? " is-disabled" : "") +
        '" data-act="' +
        esc(a.act) +
        '"' +
        (a.disabled ? " disabled" : "") +
        ' title="' +
        esc(a.title || a.label) +
        '" aria-label="' +
        esc(a.label) +
        '">' +
        esc(a.label) +
        "</button>";
    });
    if (pack.more && pack.more.length) {
      html +=
        '<details class="ada-more"><summary class="ada-icon-btn ada-more__sum" aria-label="More actions">More</summary><div class="ada-more__menu" role="menu">';
      pack.more.forEach(function (a) {
        if (!a) return;
        html +=
          '<button type="button" role="menuitem" class="ada-more__item' +
          (a.disabled ? " is-disabled" : "") +
          '" data-act="' +
          esc(a.act) +
          '"' +
          (a.disabled ? " disabled" : "") +
          ' title="' +
          esc(a.title || a.label) +
          '">' +
          esc(a.label) +
          (a.disabled && a.title
            ? '<span class="ada-more__why">' + esc(a.title) + "</span>"
            : "") +
          "</button>";
      });
      html += "</div></details>";
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
      '<input type="checkbox" class="ada-select-page" title="Select all on this page" aria-label="Select all on this page"' +
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
