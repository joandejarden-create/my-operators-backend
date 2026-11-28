/**
 * Browser build — Hotel Explorer Sources & Evidence DATE column.
 * Mirrors lib/hotel-intelligence/sources/he-customer-source-date-v1.js
 *
 * Sort priority (documented): publication_date > verified_at > accessed_at
 */
(function (global) {
  "use strict";

  var HE_CUSTOMER_SOURCE_DATE_V1 = "he-customer-source-date-v1";

  var SOURCE_DATE_SORT_PRIORITY = Object.freeze([
    "publication_date",
    "verified_at",
    "accessed_at",
  ]);

  var MONTHS = [
    "Jan",
    "Feb",
    "Mar",
    "Apr",
    "May",
    "Jun",
    "Jul",
    "Aug",
    "Sep",
    "Oct",
    "Nov",
    "Dec",
  ];

  function formatReadableSourceDate(value) {
    if (value == null || value === "") return null;
    var s = String(value).trim();
    if (!s || s === "—") return null;

    var y;
    var m;
    var d;
    var mIso = /^(\d{4})-(\d{2})-(\d{2})/.exec(s);
    if (mIso) {
      y = Number(mIso[1]);
      m = Number(mIso[2]);
      d = Number(mIso[3]);
    } else if (/^\d{4}$/.test(s)) {
      return s;
    } else {
      var dt = new Date(s);
      if (isNaN(dt.getTime())) return null;
      y = dt.getUTCFullYear();
      m = dt.getUTCMonth() + 1;
      d = dt.getUTCDate();
    }
    if (!y || !m || !d || m < 1 || m > 12) return null;
    return MONTHS[m - 1] + " " + d + ", " + y;
  }

  function resolveAccessedDate(dates) {
    var d = dates || {};
    return d.verified_at || d.accessed_at || null;
  }

  function citationDateSortIso(dates) {
    var d = dates || {};
    for (var i = 0; i < SOURCE_DATE_SORT_PRIORITY.length; i++) {
      var key = SOURCE_DATE_SORT_PRIORITY[i];
      var raw = d[key];
      if (raw == null || raw === "") continue;
      var s = String(raw).trim();
      if (!s) continue;
      var mMatch = /^(\d{4}-\d{2}-\d{2})/.exec(s);
      if (mMatch) return mMatch[1];
      if (/^\d{4}$/.test(s)) return s + "-01-01";
      var dt = new Date(s);
      if (!isNaN(dt.getTime())) return dt.toISOString().slice(0, 10);
    }
    return null;
  }

  function formatCustomerSourceDateCell(dates) {
    var d = dates || {};
    var publication = d.publication_date || null;
    var accessed = resolveAccessedDate(d);
    var pubLabel = formatReadableSourceDate(publication);
    var accLabel = formatReadableSourceDate(accessed);
    var lines = [];
    if (pubLabel) lines.push("Published: " + pubLabel);
    if (accLabel) lines.push("Accessed: " + accLabel);
    if (!lines.length) {
      return {
        lines: ["—"],
        text: "—",
        sortValue: null,
        publication_date: publication,
        accessed_date: accessed,
      };
    }
    return {
      lines: lines,
      text: lines.join("\n"),
      sortValue: citationDateSortIso(d),
      publication_date: publication,
      accessed_date: accessed,
    };
  }

  function compareCitationDates(a, b, dir) {
    var direction = dir === "asc" ? "asc" : "desc";
    var sa = citationDateSortIso(a);
    var sb = citationDateSortIso(b);
    if (sa == null && sb == null) return 0;
    if (sa == null) return 1;
    if (sb == null) return -1;
    if (sa === sb) return 0;
    if (direction === "asc") return sa < sb ? -1 : 1;
    return sa < sb ? 1 : -1;
  }

  global.HeCustomerSourceDate = {
    HE_CUSTOMER_SOURCE_DATE_V1: HE_CUSTOMER_SOURCE_DATE_V1,
    SOURCE_DATE_SORT_PRIORITY: SOURCE_DATE_SORT_PRIORITY,
    formatReadableSourceDate: formatReadableSourceDate,
    resolveAccessedDate: resolveAccessedDate,
    formatCustomerSourceDateCell: formatCustomerSourceDateCell,
    citationDateSortIso: citationDateSortIso,
    compareCitationDates: compareCitationDates,
  };
})(typeof window !== "undefined" ? window : globalThis);
