/**
 * Temporal Affiliation V1 — lightweight brand/parent history on a physical property.
 * Do not fabricate date precision.
 */

export const TEMPORAL_AFFILIATION_VERSION = "temporal-affiliation-v1";

export const AFFILIATION_DATE_PRECISION = Object.freeze({
  EXACT: "exact",
  AS_OF: "as_of",
  BEFORE: "before",
  UNKNOWN: "unknown",
});

/**
 * @param {object} input
 */
export function createAffiliationPeriod(input = {}) {
  const startPrecision = input.affiliation_start_precision || AFFILIATION_DATE_PRECISION.UNKNOWN;
  const endPrecision = input.affiliation_end
    ? input.affiliation_end_precision || AFFILIATION_DATE_PRECISION.UNKNOWN
    : null;

  return {
    temporal_affiliation_version: TEMPORAL_AFFILIATION_VERSION,
    brand: input.brand || null,
    parent: input.parent || null,
    affiliation_start: input.affiliation_start || null,
    affiliation_start_precision: startPrecision,
    affiliation_end: input.affiliation_end || null,
    affiliation_end_precision: endPrecision,
    current: input.current === true,
    evidence: Array.isArray(input.evidence) ? input.evidence : [],
    evidence_date: input.evidence_date || null,
    confidence: input.confidence || "Medium",
    notes: input.notes || null,
  };
}

/**
 * Format human-readable temporal bound without fabricating precision.
 * @param {string|null} date
 * @param {string} precision
 */
export function formatTemporalBound(date, precision) {
  if (!date && precision === AFFILIATION_DATE_PRECISION.UNKNOWN) return "Unknown";
  if (!date) return "Unknown";
  if (precision === AFFILIATION_DATE_PRECISION.BEFORE) return `Before ${date}`;
  if (precision === AFFILIATION_DATE_PRECISION.AS_OF) return `As of ${date}`;
  return date;
}

/**
 * Seed current affiliation from a verified record (as-of discovery date).
 * @param {object} record
 * @param {{ asOf?: string }} [opts]
 */
export function seedCurrentAffiliationFromRecord(record, opts = {}) {
  const asOf = opts.asOf || record.first_independently_discovered_at || new Date().toISOString().slice(0, 10);
  const period = createAffiliationPeriod({
    brand: record.brand || record.fields?.Affiliation || null,
    parent: record.parent || record.fields?.["Parent Company"] || null,
    affiliation_start: asOf,
    affiliation_start_precision: AFFILIATION_DATE_PRECISION.AS_OF,
    affiliation_end: null,
    current: true,
    evidence: [
      {
        type: "independent_discovery",
        source: record.discovery_source,
        independent_record_id: record.independent_record_id,
      },
    ],
    evidence_date: asOf,
    confidence: "High",
    notes: "Current affiliation as of independent discovery; start date may be earlier (Unknown exact start)",
  });

  record.affiliation_history = [period];
  record.current_affiliation_period = period;
  if (record.property_identity) {
    record.property_identity.affiliation_history = [period];
    record.property_identity.current_affiliation = {
      brand: period.brand,
      parent: period.parent,
      current: true,
      as_of: asOf,
    };
  }
  return period;
}

/**
 * Apply affiliation history to all records in a cohort.
 * @param {object[]} records
 */
export function applyTemporalAffiliationSeed(records) {
  for (const r of records || []) {
    seedCurrentAffiliationFromRecord(r);
  }
  return {
    temporal_affiliation_version: TEMPORAL_AFFILIATION_VERSION,
    records_seeded: (records || []).length,
    note: "Exact affiliation_start unknown unless official evidence provides it; stored as As of [discovery date]",
  };
}

/**
 * When cross-family / reflag link is confirmed or probable, append historical period.
 * Does not auto-close without steward review for Probable.
 * @param {object} propertyIdentity
 * @param {object} historical - { brand, parent, evidence, confidence }
 * @param {{ asOf?: string, classify?: string }} [opts]
 */
export function appendHistoricalAffiliation(propertyIdentity, historical, opts = {}) {
  if (!propertyIdentity) return null;
  const asOf = opts.asOf || new Date().toISOString().slice(0, 10);
  const period = createAffiliationPeriod({
    brand: historical.brand,
    parent: historical.parent,
    affiliation_start: null,
    affiliation_start_precision: AFFILIATION_DATE_PRECISION.UNKNOWN,
    affiliation_end: asOf,
    affiliation_end_precision: AFFILIATION_DATE_PRECISION.AS_OF,
    current: false,
    evidence: historical.evidence || [],
    evidence_date: asOf,
    confidence: historical.confidence || "Medium",
    notes: `Historical affiliation candidate (${opts.classify || "review"}); end bound As of ${asOf}`,
  });
  propertyIdentity.affiliation_history = [...(propertyIdentity.affiliation_history || []), period];
  return period;
}
