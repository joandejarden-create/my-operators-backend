/**
 * Dealality reusable sortable table (Hotel Explorer / dark surfaces).
 * High-contrast headers, asc/desc chevrons, numeric-aware sort, horizontal scroll host.
 * Hotel-agnostic — no property-specific logic.
 */
(function (global) {
  "use strict";

  var SORT_SVG =
    '<span class="dl-sort-indicator" aria-hidden="true">' +
    '<svg class="dl-sort-svg" width="10" height="14" viewBox="0 0 10 14" focusable="false">' +
    '<path class="dl-sort-chevron dl-sort-chevron--up" d="M5 2 L9 6 H1 Z"/>' +
    '<path class="dl-sort-chevron dl-sort-chevron--down" d="M5 12 L1 8 H9 Z"/>' +
    "</svg></span>";

  function esc(v) {
    return String(v == null ? "" : v)
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }

  function parseSortValue(raw, type) {
    if (raw == null || raw === "" || raw === "—" || raw === "-") return null;
    if (type === "number") {
      var n = Number(String(raw).replace(/[^0-9.\-]/g, ""));
      return Number.isFinite(n) ? n : null;
    }
    if (type === "text") return String(raw).toLowerCase();
    // auto
    var cleaned = String(raw).replace(/,/g, "").trim();
    var asNum = Number(cleaned.replace(/[^0-9.\-]/g, ""));
    if (Number.isFinite(asNum) && /[0-9]/.test(cleaned)) return asNum;
    return String(raw).toLowerCase();
  }

  function compare(a, b, dir) {
    if (a == null && b == null) return 0;
    if (a == null) return 1;
    if (b == null) return -1;
    if (typeof a === "number" && typeof b === "number") {
      return dir === "desc" ? b - a : a - b;
    }
    var sa = String(a);
    var sb = String(b);
    if (sa < sb) return dir === "desc" ? 1 : -1;
    if (sa > sb) return dir === "desc" ? -1 : 1;
    return 0;
  }

  function buildHeaderCell(col) {
    var sortable = col.sortable !== false;
    var type = col.type || "auto";
    var align = col.align || (type === "number" ? "right" : "left");
    var classes = ["dl-th"];
    if (sortable) classes.push("dl-th--sortable");
    if (align === "right") classes.push("dl-th--num");
    if (align === "center") classes.push("dl-th--center");
    var attrs =
      ' class="' +
      classes.join(" ") +
      '" scope="col"' +
      (sortable ? ' data-sort="' + esc(col.key) + '" data-sort-type="' + esc(type) + '" tabindex="0" role="columnheader" aria-sort="none"' : "");
    return (
      "<th" +
      attrs +
      ">" +
      '<span class="dl-th-label">' +
      esc(col.label) +
      (sortable ? SORT_SVG : "") +
      "</span></th>"
    );
  }

  /**
   * @param {object} opts
   * @param {Array<{key:string,label:string,type?:string,sortable?:boolean,align?:string,width?:string}>} opts.columns
   * @param {Array<object>} opts.rows — objects keyed by column.key; optional _rowClass, _cells (prebuilt HTML by key)
   * @param {string} [opts.tableClass]
   * @param {string} [opts.emptyMessage]
   * @param {string} [opts.defaultSortKey]
   * @param {"asc"|"desc"} [opts.defaultSortDir]
   */
  function renderMarkup(opts) {
    opts = opts || {};
    var columns = Array.isArray(opts.columns) ? opts.columns : [];
    var rows = Array.isArray(opts.rows) ? opts.rows : [];
    if (!columns.length) return "";
    var tableClass =
      "dl-table hiw-table " + (opts.tableClass ? String(opts.tableClass) : "");
    if (!rows.length) {
      return (
        '<div class="dl-table-wrap hiw-table-wrap">' +
        '<table class="' +
        esc(tableClass) +
        '"><thead><tr>' +
        columns.map(buildHeaderCell).join("") +
        '</tr></thead><tbody><tr><td colspan="' +
        columns.length +
        '" class="dl-td--empty">' +
        esc(opts.emptyMessage || "No rows available.") +
        "</td></tr></tbody></table></div>"
      );
    }

    var sortKey = opts.defaultSortKey || null;
    var sortDir = opts.defaultSortDir === "desc" ? "desc" : "asc";
    var ordered = rows.slice();
    if (sortKey) {
      var col = columns.find(function (c) {
        return c.key === sortKey;
      });
      var type = (col && col.type) || "auto";
      ordered.sort(function (ra, rb) {
        return compare(
          parseSortValue(ra[sortKey], type),
          parseSortValue(rb[sortKey], type),
          sortDir
        );
      });
    }

    var thead =
      "<thead><tr>" +
      columns
        .map(function (col) {
          var html = buildHeaderCell(col);
          if (sortKey && col.key === sortKey) {
            html = html
              .replace(' aria-sort="none"', ' aria-sort="' + (sortDir === "desc" ? "descending" : "ascending") + '"')
              .replace(
                ' class="dl-th',
                ' class="dl-th ' + (sortDir === "desc" ? "sort-desc" : "sort-asc")
              );
          }
          return html;
        })
        .join("") +
      "</tr></thead>";

    var tbody =
      "<tbody>" +
      ordered
        .map(function (row) {
          var trClass = row._rowClass ? ' class="' + esc(row._rowClass) + '"' : "";
          var cells = columns
            .map(function (col) {
              var align = col.align || (col.type === "number" ? "right" : "left");
              var tdClass = ["dl-td"];
              if (align === "right") tdClass.push("dl-td--num");
              if (align === "center") tdClass.push("dl-td--center");
              if (col.key === columns[0].key) tdClass.push("dl-td--primary");
              var content =
                row._cells && row._cells[col.key] != null
                  ? row._cells[col.key]
                  : esc(row[col.key] == null || row[col.key] === "" ? "—" : row[col.key]);
              return (
                '<td class="' +
                tdClass.join(" ") +
                '" data-sort-value="' +
                esc(row[col.key] == null ? "" : row[col.key]) +
                '">' +
                content +
                "</td>"
              );
            })
            .join("");
          return "<tr" + trClass + ">" + cells + "</tr>";
        })
        .join("") +
      "</tbody>";

    return (
      '<div class="dl-table-wrap hiw-table-wrap" data-dl-sortable="1">' +
      '<table class="' +
      esc(tableClass) +
      '" data-dl-default-sort="' +
      esc(sortKey || "") +
      '" data-dl-default-dir="' +
      esc(sortDir) +
      '">' +
      thead +
      tbody +
      "</table></div>"
    );
  }

  function sortTable(table, key, dir) {
    var ths = table.querySelectorAll("thead th[data-sort]");
    var type = "auto";
    ths.forEach(function (th) {
      th.classList.remove("sort-asc", "sort-desc");
      th.setAttribute("aria-sort", "none");
      if (th.getAttribute("data-sort") === key) {
        type = th.getAttribute("data-sort-type") || "auto";
        th.classList.add(dir === "desc" ? "sort-desc" : "sort-asc");
        th.setAttribute("aria-sort", dir === "desc" ? "descending" : "ascending");
      }
    });
    var tbody = table.tBodies[0];
    if (!tbody) return;
    var colIndex = -1;
    ths.forEach(function (th, i) {
      if (th.getAttribute("data-sort") === key) colIndex = i;
    });
    if (colIndex < 0) return;
    var rows = Array.prototype.slice.call(tbody.rows);
    rows.sort(function (ra, rb) {
      var ca = ra.cells[colIndex];
      var cb = rb.cells[colIndex];
      var va = parseSortValue(ca && ca.getAttribute("data-sort-value"), type);
      var vb = parseSortValue(cb && cb.getAttribute("data-sort-value"), type);
      return compare(va, vb, dir);
    });
    rows.forEach(function (r) {
      tbody.appendChild(r);
    });
  }

  function bind(root) {
    if (!root) return;
    var wraps = root.querySelectorAll
      ? root.querySelectorAll("[data-dl-sortable]")
      : [];
    if (root.getAttribute && root.getAttribute("data-dl-sortable")) {
      wraps = [root];
    }
    Array.prototype.forEach.call(wraps, function (wrap) {
      if (wrap.getAttribute("data-dl-bound") === "1") return;
      wrap.setAttribute("data-dl-bound", "1");
      var table = wrap.querySelector("table");
      if (!table) return;
      var state = {
        key: table.getAttribute("data-dl-default-sort") || null,
        dir: table.getAttribute("data-dl-default-dir") === "desc" ? "desc" : "asc",
      };
      function onActivate(th) {
        if (!th || !th.getAttribute("data-sort")) return;
        var key = th.getAttribute("data-sort");
        if (state.key === key) {
          state.dir = state.dir === "asc" ? "desc" : "asc";
        } else {
          state.key = key;
          state.dir = "asc";
        }
        sortTable(table, state.key, state.dir);
      }
      wrap.addEventListener("click", function (ev) {
        var th = ev.target.closest && ev.target.closest("th[data-sort]");
        onActivate(th);
      });
      wrap.addEventListener("keydown", function (ev) {
        if (ev.key !== "Enter" && ev.key !== " ") return;
        var th = ev.target.closest && ev.target.closest("th[data-sort]");
        if (!th) return;
        ev.preventDefault();
        onActivate(th);
      });
    });
  }

  global.DealalitySortableTable = {
    renderMarkup: renderMarkup,
    bind: bind,
    sortTable: sortTable,
    parseSortValue: parseSortValue,
  };
})(typeof window !== "undefined" ? window : globalThis);
