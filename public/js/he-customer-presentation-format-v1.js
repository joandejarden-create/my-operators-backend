/**
 * Hotel Explorer — shared customer presentation formatters (HE presentation cleanup).
 * Formats display values only; does not mutate canonical HPC / Airtable records.
 */
(function (global) {
  "use strict";

  var INTERNAL_TERMS = [
    /\bHPC\b/gi,
    /\bHotel Property Census\b/gi,
    /\bLegacy Hotel Census\b/gi,
    /\bHotel Census\b/gi,
    /\binventory cohorts?\b/gi,
    /\bmatched HPC cohort\b/gi,
    /\bnever fabricated\b/gi,
    /\bnot a fabricated complete census\b/gi,
    /\bresearch[- ]authorization\b/gi,
    /\bimplementation notes?\b/gi,
    /\bpersisted\b/gi,
    /\bSTAGED\b/g,
    /\bPROPOSED_STAGED\b/g,
    /\bAirtable\b/gi,
    /\bWebhound\b/gi,
    /\bScout\/Radar\b/gi,
  ];

  function present(v) {
    return v != null && String(v).trim() !== "" && String(v).trim() !== "—";
  }

  function titleCaseLabel(label) {
    var raw = String(label || "").trim();
    if (!raw) return "";
    var small = { a: 1, an: 1, and: 1, at: 1, by: 1, for: 1, in: 1, of: 1, on: 1, or: 1, the: 1, to: 1, vs: 1 };
    var preserve = {
      wifi: "WiFi",
      "wi-fi": "Wi-Fi",
      "sq.": "Sq.",
      "ft.": "Ft.",
      fsb: "FSB",
      nih: "NIH",
    };
    return raw
      .replace(/_/g, " ")
      .replace(/\s+/g, " ")
      .split(" ")
      .map(function (word, i) {
        var lower = word.toLowerCase();
        if (preserve[lower]) return preserve[lower];
        if (i > 0 && small[lower]) return lower;
        if (/^[A-Z0-9]{2,}$/.test(word) && word.length <= 4) return word;
        return word.charAt(0).toUpperCase() + word.slice(1).toLowerCase();
      })
      .join(" ");
  }

  function preserveProperName(name) {
    var s = String(name == null ? "" : name).trim();
    if (!s) return "";
    // Keep brand/company casing when already mixed; only collapse ALL CAPS blobs.
    if (s === s.toUpperCase() && /[A-Z]/.test(s) && s.length > 3) {
      return titleCaseLabel(s);
    }
    return s;
  }

  function formatInteger(n) {
    var num = Number(n);
    if (!Number.isFinite(num)) return null;
    return Math.round(num).toLocaleString("en-US");
  }

  function formatYear(v) {
    if (v == null || v === "" || v === "—") return null;
    var n = Number(v);
    if (Number.isFinite(n) && n >= 1800 && n <= 2100) return String(Math.round(n));
    var m = String(v).trim().match(/(18|19|20)\d{2}/);
    return m ? m[0] : null;
  }

  function formatSqFt(n) {
    var formatted = formatInteger(n);
    return formatted ? formatted + " Sq. Ft." : null;
  }

  /**
   * Customer Last Renovated line. Secondary milestone years stay out of Property Facts
   * (Sources / About This Analysis only).
   */
  function formatLastRenovated(primaryYear, opts) {
    opts = opts || {};
    var year = formatYear(primaryYear);
    if (!year) return null;
    if (opts.major === false) return year;
    return year + " (Major Renovation)";
  }

  function scrubInternalTerminology(text) {
    var out = String(text == null ? "" : text);
    INTERNAL_TERMS.forEach(function (re) {
      out = out.replace(re, "");
    });
    return out.replace(/\s{2,}/g, " ").replace(/\s+([.,;:])/g, "$1").trim();
  }

  function customerPersistenceLabel(inventory) {
    inventory = inventory || {};
    if (inventory.persisted === true || inventory.sourceOfTruth === "HOTEL_PROPERTY_CENSUS") {
      return "Verified property record";
    }
    if (/PROPOSED/i.test(String(inventory.persistenceStatus || ""))) {
      return "Under review";
    }
    return "Brand reference";
  }

  var HeCustomerPresentationFormat = {
    present: present,
    titleCaseLabel: titleCaseLabel,
    preserveProperName: preserveProperName,
    formatInteger: formatInteger,
    formatYear: formatYear,
    formatSqFt: formatSqFt,
    formatLastRenovated: formatLastRenovated,
    scrubInternalTerminology: scrubInternalTerminology,
    customerPersistenceLabel: customerPersistenceLabel,
  };

  global.HeCustomerPresentationFormat = HeCustomerPresentationFormat;
})(typeof window !== "undefined" ? window : globalThis);
