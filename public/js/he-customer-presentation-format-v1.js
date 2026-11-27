/**
 * Hotel Explorer — shared customer presentation formatters.
 * Formats display values only; does not mutate canonical HPC / Airtable records.
 *
 * Permanent capitalization standard:
 *   - Field headers / labels → ALL CAPS
 *   - Descriptive values → Proper Case (preserve official brand/company casing,
 *     abbreviations, acronyms, numbers, URLs, legal entity names)
 */
(function (global) {
  "use strict";

  var INTERNAL_TERMS = [
    /\bHPC\b/gi,
    /\bHotel Property Census\b/gi,
    /\bLegacy Hotel Census\b/gi,
    /\bHotel Census\b/gi,
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

  var VALUE_ACRONYMS = {
    llc: "LLC",
    llp: "LLP",
    lp: "LP",
    inc: "Inc",
    ltd: "Ltd",
    plc: "PLC",
    usa: "USA",
    uk: "UK",
    us: "US",
    nih: "NIH",
    ihg: "IHG",
    fsb: "FSB",
    ubo: "UBO",
    adr: "ADR",
    revpar: "RevPAR",
    wifi: "WiFi",
    "wi-fi": "Wi-Fi",
  };

  function present(v) {
    return v != null && String(v).trim() !== "" && String(v).trim() !== "—";
  }

  /**
   * Customer-facing field headers / labels — always ALL CAPS.
   * Does not rewrite measurement unit tokens that belong in values (Sq. Ft. / Sq. M.).
   */
  function allCapsFieldLabel(label) {
    var raw = String(label == null ? "" : label)
      .replace(/_/g, " ")
      .replace(/\s+/g, " ")
      .trim();
    if (!raw) return "";
    return raw.toUpperCase();
  }

  /** @deprecated Use allCapsFieldLabel — Title Case headers are no longer customer standard. */
  function titleCaseLabel(label) {
    return allCapsFieldLabel(label);
  }

  function properCaseToken(word, index) {
    var bare = String(word || "");
    if (!bare) return "";
    var lower = bare.toLowerCase();
    if (VALUE_ACRONYMS[lower]) return VALUE_ACRONYMS[lower];
    // Keep short ALL-CAPS tokens (IHG, LP) and mixed legal particles.
    if (/^[A-Z0-9]{2,5}$/.test(bare) && bare === bare.toUpperCase()) return bare;
    if (/^(?:of|and|the|for|to|in|on|at|by|or|a|an|vs)$/i.test(bare) && index > 0) {
      return lower;
    }
    if (/^[A-Z][a-z]+(?:'[A-Za-z]+)?$/.test(bare)) return bare;
    if (/[a-z][A-Z]/.test(bare)) return bare; // camel / internal caps
    return bare.charAt(0).toUpperCase() + bare.slice(1).toLowerCase();
  }

  /**
   * Proper Case for descriptive values. Preserves URLs, emails, phones, years,
   * measurement strings, and already-correct brand/legal spellings.
   */
  function properCaseValue(name) {
    var s = String(name == null ? "" : name).trim();
    if (!s) return "";
    if (/^https?:\/\//i.test(s) || /^www\./i.test(s)) return s;
    if (/@/.test(s) && /\./.test(s)) return s;
    if (/^tel:/i.test(s) || /^[\d+().\-\s]{7,}$/.test(s)) return s;
    if (/^\d{4}$/.test(s)) return s;
    if (/\bSq\.\s*Ft\.|\bSq\.\s*M\./i.test(s) || /^[\d,]+\s*(Sq\.|sq)/i.test(s)) return s;
    if (/\d/.test(s) && /%|\/\s*5$/.test(s)) return s;
    // Already mixed-case company/brand spelling — keep.
    if (/[a-z]/.test(s) && /[A-Z]/.test(s) && s !== s.toUpperCase()) {
      return s;
    }
    // Collapse ALL CAPS / all-lower blobs into Proper Case.
    return s
      .split(/\s+/)
      .map(function (word, i) {
        // Preserve parenthetical qualifiers like (Major Renovation).
        var m = word.match(/^(\()?(.*?)(\))?$/);
        if (!m) return properCaseToken(word, i);
        var inner = properCaseToken(m[2], i);
        return (m[1] || "") + inner + (m[3] || "");
      })
      .join(" ");
  }

  function preserveProperName(name) {
    return properCaseValue(name);
  }

  /**
   * Drop redundant suffixes when the field header already states the relationship.
   * Presentation only — does not change stored Airtable values.
   */
  function stripRedundantRelationshipSuffix(value) {
    var s = String(value == null ? "" : value).trim();
    if (!s) return "";
    return s
      .replace(/\s*\(\s*brand\s+parent(?:\s+company)?\s*\)\s*$/i, "")
      .replace(/\s*\(\s*Managed\s*\)\s*$/i, "")
      .replace(/\s*\(\s*Franchised\s*\)\s*$/i, "")
      .trim();
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

  function formatSqM(n) {
    var formatted = formatInteger(n);
    return formatted ? formatted + " Sq. M." : null;
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
      return "Verified Property Record";
    }
    if (/PROPOSED/i.test(String(inventory.persistenceStatus || ""))) {
      return "Under Review";
    }
    return "Brand Reference";
  }

  function formatFieldRow(label, value) {
    return {
      label: allCapsFieldLabel(label),
      value: value == null || value === "" ? "—" : value,
    };
  }

  var HeCustomerPresentationFormat = {
    present: present,
    allCapsFieldLabel: allCapsFieldLabel,
    titleCaseLabel: titleCaseLabel,
    properCaseValue: properCaseValue,
    preserveProperName: preserveProperName,
    stripRedundantRelationshipSuffix: stripRedundantRelationshipSuffix,
    formatInteger: formatInteger,
    formatYear: formatYear,
    formatSqFt: formatSqFt,
    formatSqM: formatSqM,
    formatLastRenovated: formatLastRenovated,
    scrubInternalTerminology: scrubInternalTerminology,
    customerPersistenceLabel: customerPersistenceLabel,
    formatFieldRow: formatFieldRow,
  };

  global.HeCustomerPresentationFormat = HeCustomerPresentationFormat;
})(typeof window !== "undefined" ? window : globalThis);
