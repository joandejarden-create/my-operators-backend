/**
 * Branded Residences — read-path API shape for Brand Explorer (no setup/pipeline deps).
 * @see docs/data-intelligence/brand-residences-status-field.md
 */

export const MAP_BRAND_RESIDENCES = Object.freeze({
  status: "Branded Residences Status",
  notes: "Branded Residences Notes",
  sourceUrl: "Branded Residences Source URL",
  reviewStatus: "Branded Residences Review Status",
});

export const RESIDENCES_STATUS_VALUES = Object.freeze([
  "Yes",
  "Case-by-Case",
  "No",
  "Not Confirmed",
]);

export const RESIDENCES_REVIEW_STATUS_VALUES = Object.freeze([
  "Source-Backed",
  "Founder-Reviewed",
  "Needs Review",
  "Not Confirmed",
]);

export const DEFAULT_RESIDENCES_STATUS = "Not Confirmed";
export const DEFAULT_RESIDENCES_REVIEW_STATUS = "Not Confirmed";

function nz(v) {
  return v == null ? "" : String(v).trim();
}

export function normalizeResidencesStatus(raw) {
  const v = nz(raw);
  if (!v) return DEFAULT_RESIDENCES_STATUS;
  const exact = RESIDENCES_STATUS_VALUES.find((x) => x.toLowerCase() === v.toLowerCase());
  if (exact) return exact;
  if (/^yes$/i.test(v)) return "Yes";
  if (/^no$/i.test(v)) return "No";
  if (/case/i.test(v)) return "Case-by-Case";
  return DEFAULT_RESIDENCES_STATUS;
}

export function normalizeResidencesReviewStatus(raw) {
  const v = nz(raw);
  if (!v) return DEFAULT_RESIDENCES_REVIEW_STATUS;
  const exact = RESIDENCES_REVIEW_STATUS_VALUES.find((x) => x.toLowerCase() === v.toLowerCase());
  if (exact) return exact;
  if (/source/i.test(v)) return "Source-Backed";
  if (/founder/i.test(v)) return "Founder-Reviewed";
  if (/needs review/i.test(v)) return "Needs Review";
  return DEFAULT_RESIDENCES_REVIEW_STATUS;
}

export function buildResidencesApiShape(brandFields = {}) {
  const status = normalizeResidencesStatus(brandFields[MAP_BRAND_RESIDENCES.status]);
  const reviewStatus = normalizeResidencesReviewStatus(brandFields[MAP_BRAND_RESIDENCES.reviewStatus]);
  const notes = nz(brandFields[MAP_BRAND_RESIDENCES.notes]) || null;
  const sourceUrl = nz(brandFields[MAP_BRAND_RESIDENCES.sourceUrl]) || null;
  return {
    status,
    notes,
    sourceUrl,
    reviewStatus,
  };
}
