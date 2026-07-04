/**
 * Title Case for Explorer subsection card headings (engagement, brand, operating, etc.).
 */
(function (global) {
  "use strict";

  var LABEL_ACRONYMS = {
    ihg: "IHG",
    usa: "USA",
    uk: "UK",
    cala: "CALA",
    qa: "QA",
    pip: "PIP",
    capex: "CapEx",
    gop: "GOP",
    mbr: "MBR",
    nda: "NDA",
    pms: "PMS",
    rms: "RMS",
    crs: "CRS",
    ota: "OTA",
    hr: "HR",
    kpi: "KPI",
  };

  function nz(v) {
    if (v == null) return "";
    return String(v).trim();
  }

  function titleCaseToken(token) {
    var t = nz(token);
    if (!t) return "";
    return t
      .split("-")
      .map(function (part) {
        var lower = part.toLowerCase();
        if (LABEL_ACRONYMS[lower]) return LABEL_ACRONYMS[lower];
        return lower.charAt(0).toUpperCase() + lower.slice(1);
      })
      .join("-");
  }

  function formatCardTitle(raw) {
    var s = nz(raw);
    if (!s) return "";
    if (/\s*&\s*/.test(s)) {
      return s
        .split(/\s*&\s*/)
        .map(function (part) {
          return formatCardTitle(part.trim());
        })
        .join(" & ");
    }
    if (s.indexOf("/") >= 0) {
      return s
        .split(/\s*\/\s*/)
        .map(function (segment) {
          return formatCardTitle(segment.trim());
        })
        .join(" / ");
    }
    return s
      .split(/\s+/)
      .map(function (word) {
        return titleCaseToken(word);
      })
      .join(" ");
  }

  global.OperatorExplorerCardTitle = {
    formatCardTitle: formatCardTitle,
  };
})(typeof globalThis !== "undefined" ? globalThis : global);
