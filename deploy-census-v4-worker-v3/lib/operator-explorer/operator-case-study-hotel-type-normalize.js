/**
 * Canonical Operator Explorer Asset Type values (case study `hotel_type`).
 * Used for Airtable cleanup and read-path normalization.
 */
export const OPERATOR_EXPLORER_ASSET_TYPE_VALUES = [
  "Full-Service",
  "Select-Service",
  "Resort",
  "Boutique",
  "Lifestyle",
  "Branded Residences / Condo Hotel",
  "Mixed-Use Hospitality",
];

const EXACT_ALIASES = new Map([
  ["full service", "Full-Service"],
  ["full-service", "Full-Service"],
  ["fullservice", "Full-Service"],
  ["select service", "Select-Service"],
  ["select-service", "Select-Service"],
  ["selectservice", "Select-Service"],
  ["upscale select-service", "Select-Service"],
  ["upscale select service", "Select-Service"],
  ["resort", "Resort"],
  ["eco resort / pipeline pre-opening", "Resort"],
  ["luxury resort / branded", "Resort"],
  ["resort / branded lifestyle", "Resort"],
  ["boutique", "Boutique"],
  ["lifestyle", "Lifestyle"],
  ["branded residences / condo hotel", "Branded Residences / Condo Hotel"],
  ["condo hotel", "Branded Residences / Condo Hotel"],
  ["condo-hotel", "Branded Residences / Condo Hotel"],
  ["mixed-use hospitality", "Mixed-Use Hospitality"],
  ["mixed use hospitality", "Mixed-Use Hospitality"],
]);

function trimStr(v) {
  if (v == null || v === "") return "";
  return String(v).trim();
}

function normalizeDashes(s) {
  return trimStr(s).replace(/\u2013/g, "-").replace(/\u2014/g, "-");
}

function collapseWhitespace(s) {
  return trimStr(s).replace(/\s+/g, " ");
}

/**
 * Map free-text / legacy case study hotel_type → canonical Asset Type label.
 * @param {unknown} raw
 * @returns {{ normalized: string, changed: boolean, reason: string }}
 */
export function normalizeOperatorCaseStudyHotelType(raw) {
  const original = collapseWhitespace(normalizeDashes(raw));
  if (!original) {
    return { normalized: "", changed: false, reason: "empty" };
  }
  if (OPERATOR_EXPLORER_ASSET_TYPE_VALUES.includes(original)) {
    return { normalized: original, changed: false, reason: "already-canonical" };
  }

  const lower = original.toLowerCase();
  if (EXACT_ALIASES.has(lower)) {
    const normalized = EXACT_ALIASES.get(lower);
    return {
      normalized,
      changed: normalized !== original,
      reason: "exact-alias",
    };
  }

  // Compound strings — take primary hospitality asset type by keyword priority.
  if (/\bresort\b/i.test(original)) {
    return { normalized: "Resort", changed: original !== "Resort", reason: "keyword-resort" };
  }
  if (/\bboutique\b/i.test(original)) {
    return { normalized: "Boutique", changed: original !== "Boutique", reason: "keyword-boutique" };
  }
  if (/\blifestyle\b/i.test(original)) {
    return { normalized: "Lifestyle", changed: original !== "Lifestyle", reason: "keyword-lifestyle" };
  }
  if (/\bfull[\s-]?service\b/i.test(original)) {
    return { normalized: "Full-Service", changed: original !== "Full-Service", reason: "keyword-full-service" };
  }
  if (/\bselect[\s-]?service\b/i.test(original)) {
    return { normalized: "Select-Service", changed: original !== "Select-Service", reason: "keyword-select-service" };
  }
  if (/\bextended[\s-]?stay\b/i.test(original)) {
    return { normalized: "Select-Service", changed: original !== "Select-Service", reason: "keyword-extended-stay" };
  }
  if (/\bbranded[\s-]?residence|condo[\s-]?hotel\b/i.test(original)) {
    return {
      normalized: "Branded Residences / Condo Hotel",
      changed: original !== "Branded Residences / Condo Hotel",
      reason: "keyword-branded-residence",
    };
  }
  if (/\bmixed[\s-]?use\b/i.test(original)) {
    return {
      normalized: "Mixed-Use Hospitality",
      changed: original !== "Mixed-Use Hospitality",
      reason: "keyword-mixed-use",
    };
  }

  return { normalized: original, changed: false, reason: "unmapped" };
}

/**
 * @param {unknown} raw
 * @returns {string}
 */
export function normalizeOperatorCaseStudyHotelTypeValue(raw) {
  return normalizeOperatorCaseStudyHotelType(raw).normalized;
}
