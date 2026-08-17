/**
 * Convert Deliverables text to Proper / Title Case for Founder Project Plan.
 */

const SMALL_WORDS = new Set([
  "a",
  "an",
  "the",
  "and",
  "or",
  "but",
  "in",
  "on",
  "at",
  "to",
  "for",
  "of",
  "vs",
  "per",
  "as",
  "by",
  "with",
  "from",
]);

/** Brand / product tokens with non-standard casing. */
const BRAND_WORDS = new Map([
  ["linkedin", "LinkedIn"],
  ["dealality", "Dealality"],
  ["webflow", "Webflow"],
  ["memberstack", "Memberstack"],
  ["hubspot", "HubSpot"],
  ["airtable", "Airtable"],
]);

/** Known acronyms / brand tokens — uppercase when whole word matches. */
const ACRONYMS = new Set([
  "api",
  "apis",
  "bas",
  "ccpa",
  "crm",
  "gdpr",
  "loi",
  "drs",
  "gtm",
  "kpi",
  "kpis",
  "mic",
  "mvp",
  "nps",
  "oas",
  "pdf",
  "pmo",
  "qa",
  "sop",
  "swot",
  "ui",
  "ux",
]);

function caseToken(token, isFirst) {
  const lower = token.toLowerCase();
  if (BRAND_WORDS.has(lower)) return BRAND_WORDS.get(lower);
  if (ACRONYMS.has(lower)) {
    if (lower === "kpis") return "KPIs";
    if (lower === "apis") return "APIs";
    return lower.toUpperCase();
  }
  if (!isFirst && SMALL_WORDS.has(lower)) return lower;
  return lower.charAt(0).toUpperCase() + lower.slice(1);
}

function titleWord(word, isFirst) {
  if (!word) return word;
  const core = word.replace(/^[^A-Za-z0-9+-]+|[^A-Za-z0-9+-]+$/g, "");
  if (!core) return word;

  let cased;
  if (core.includes("-")) {
    cased = core
      .split("-")
      .map((part, index) => caseToken(part, isFirst && index === 0))
      .join("-");
  } else {
    cased = caseToken(core, isFirst);
  }

  return word.replace(core, cased);
}

/**
 * Title-case a segment (words separated by spaces).
 * @param {string} segment
 * @param {boolean} forceFirst - first word always capped (after punctuation split)
 */
function titleSegment(segment, forceFirst = true) {
  const parts = segment.split(/(\s+)/);
  let wordIndex = 0;
  return parts
    .map((part) => {
      if (/^\s+$/.test(part)) return part;
      const isFirst = wordIndex === 0 && forceFirst;
      wordIndex += 1;
      return titleWord(part, isFirst);
    })
    .join("");
}

/**
 * Proper Case for full deliverable string (handles /, &, parentheses, commas).
 * @param {string} value
 */
export function toDeliverablesProperCase(value) {
  if (value == null || value === "") return value;
  const str = String(value).trim();
  if (!str) return str;

  return str
    .split(/(\s*\/\s*|\s*,\s*|\s*&\s*|\s*\(\s*|\s*\)\s*)/)
    .map((chunk, i, arr) => {
      if (/^\s*\/\s*$/.test(chunk)) return chunk;
      if (/^\s*,\s*$/.test(chunk)) return chunk;
      if (/^\s*&\s*$/.test(chunk)) return chunk;
      if (/^\s*\(\s*$/.test(chunk)) return chunk;
      if (/^\s*\)\s*$/.test(chunk)) return chunk;
      const prev = arr[i - 1] || "";
      const afterOpenParen = /\(\s*$/.test(prev);
      const afterAmpersand = /&\s*$/.test(prev);
      const afterSlash = /\/\s*$/.test(prev);
      const capFirst = afterOpenParen || afterAmpersand || afterSlash || chunk === str;
      return titleSegment(chunk, capFirst);
    })
    .join("");
}

export function deliverablesNeedsUpdate(current) {
  if (!current) return false;
  const next = toDeliverablesProperCase(current);
  return next !== current;
}

/** Alias for Workstream and other FPP title fields. */
export const toProperCaseText = toDeliverablesProperCase;

export function properCaseNeedsUpdate(current) {
  return deliverablesNeedsUpdate(current);
}
