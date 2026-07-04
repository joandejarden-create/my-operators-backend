/**
 * Brand Setup - Brand Footprint explicit verification fields (MVP base).
 */

export const FOOTPRINT_VERIFICATION_AIRTABLE = {
  status: "Footprint Data Status",
  source: "Footprint Data Source",
  figuresAsOf: "Footprint Figures As Of",
  notes: "Footprint Notes",
};

export const FOOTPRINT_DATA_STATUS = {
  VERIFIED: "Verified",
  ESTIMATED: "Estimated",
  PLACEHOLDER: "Placeholder",
  NEEDS_REVIEW: "Needs Review",
};

function trimOrNull(v) {
  if (v == null || v === "") return null;
  const s = String(v).trim();
  return s || null;
}

function normalizeFiguresAsOf(val) {
  const s = trimOrNull(val);
  if (!s) return null;
  if (s.length >= 10 && s.indexOf("T") > 0) return s.slice(0, s.indexOf("T"));
  if (s.length >= 10 && /^\d{4}-\d{2}-\d{2}/.test(s)) return s.slice(0, 10);
  return s;
}

/**
 * @param {Record<string, unknown>|null|undefined} footprintFields Raw Airtable Brand Footprint fields
 * @returns {null|{ status: string|null, source: string|null, figuresAsOf: string|null, notes: string|null }}
 */
export function readFootprintVerificationFromFields(footprintFields) {
  if (!footprintFields || typeof footprintFields !== "object") return null;

  const status = trimOrNull(footprintFields[FOOTPRINT_VERIFICATION_AIRTABLE.status]);
  const source = trimOrNull(footprintFields[FOOTPRINT_VERIFICATION_AIRTABLE.source]);
  const figuresAsOf = normalizeFiguresAsOf(
    footprintFields[FOOTPRINT_VERIFICATION_AIRTABLE.figuresAsOf]
  );
  const notes = trimOrNull(footprintFields[FOOTPRINT_VERIFICATION_AIRTABLE.notes]);

  if (!status && !source && !figuresAsOf && !notes) return null;

  return { status, source, figuresAsOf, notes };
}
