/**
 * Customer-facing prose completeness helpers for GDI Full E2E.
 * Rejects cut-off essential commercial sentences; prefers sentence-boundary
 * soft clamps over hard character slices.
 */

export const GDI_CUSTOMER_PROSE_COMPLETENESS_V1 =
  "gdi-customer-prose-completeness-v1";

export const ESSENTIAL_COMMERCIAL_PROSE_FIELDS = Object.freeze([
  "whatIsHappening",
  "potentialHotelBusiness",
  "whyThisHotel",
  "whyActNow",
  "recommendedNextStep",
]);

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

/**
 * True when prose looks cut mid-thought (hard slice artifact).
 */
export function appearsTruncatedCustomerProse(value) {
  const t = text(value);
  if (!t || t.length < 180) return false;
  if (/[.!?)"'\u201d\u2019]\s*$/.test(t)) return false;
  if (/[—–-]\s*$/.test(t)) return true;
  if (/\b(and|or|with|for|the|a|an|to|of|in|at|by|from|as|is|are|was|were|be|been|being)\s*$/i.test(t)) {
    return true;
  }
  return /[a-z0-9,;:]\s*$/i.test(t) && t.length >= 220;
}

/**
 * Soft clamp that never ends mid-sentence when a prior sentence boundary exists.
 * Falls back to an explicit ellipsis only when no sentence boundary is available.
 */
export function softClampCustomerProse(value, maxLen = 900) {
  const t = text(value);
  if (!t) return "";
  if (t.length <= maxLen) return t;
  const cut = t.slice(0, maxLen);
  const sp = cut.lastIndexOf(". ");
  if (sp > maxLen * 0.4) return cut.slice(0, sp + 1).trim();
  const q = cut.lastIndexOf("? ");
  if (q > maxLen * 0.4) return cut.slice(0, q + 1).trim();
  const e = cut.lastIndexOf("! ");
  if (e > maxLen * 0.4) return cut.slice(0, e + 1).trim();
  return `${cut.trim()}…`;
}

/**
 * Audit essential commercial fields on one opportunity.
 */
export function auditEssentialCommercialProseCompleteness(opportunity = {}) {
  const o = opportunity || {};
  const defects = [];
  const bodies = {
    whatIsHappening: o.whatIsHappening || o.summaryWhat,
    potentialHotelBusiness: o.potentialHotelBusiness || o.potentialBusiness,
    whyThisHotel: o.whyThisHotel,
    whyActNow: o.whyActNow || o.whyNow,
    recommendedNextStep: o.recommendedNextStep || o.recommendedAction,
  };
  for (const field of ESSENTIAL_COMMERCIAL_PROSE_FIELDS) {
    const v = text(bodies[field]);
    if (!v) {
      defects.push({
        code: "empty_essential_commercial_field",
        field,
        opportunityId: o.id || null,
      });
      continue;
    }
    if (appearsTruncatedCustomerProse(v)) {
      defects.push({
        code: "truncated_essential_commercial_prose",
        field,
        opportunityId: o.id || null,
        length: v.length,
      });
    }
  }
  return {
    ok: defects.length === 0,
    opportunityId: o.id || null,
    defects,
  };
}

export function assertPackageEssentialProseCompleteness(pkg = {}) {
  const rows = (pkg.opportunities || []).map((o) =>
    auditEssentialCommercialProseCompleteness(o),
  );
  const failing = rows.filter((r) => !r.ok);
  return {
    ok: failing.length === 0,
    audited: rows.length,
    failingCount: failing.length,
    failing,
    contract: GDI_CUSTOMER_PROSE_COMPLETENESS_V1,
  };
}
