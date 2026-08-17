/**
 * Operator Explorer — protected quality baseline (Arbor + Hotel Equities).
 *
 * Parallel to Brand Explorer protected baseline, but quality-freeze focused:
 * these two operators define tab-by-tab / field-by-field product quality for
 * all future Operator Explorer profiles.
 *
 * Docs:
 * - docs/data-intelligence/operator-explorer-protected-baseline-rules.md
 * - docs/data-intelligence/operator-explorer-arbor-hotel-equities-quality-baseline.md
 */

export const OPERATOR_QUALITY_BASELINE_VERSION = "frozen_2_operator_quality_baseline";
export const OPERATOR_QUALITY_BASELINE_EXPECTED_COUNT = 2;

/** @typedef {'arbor-lodging-cala'|'hotel-equities-cala'} OperatorQualityBaselineSlug */

/**
 * @typedef {object} OperatorQualityBaselineEntry
 * @property {OperatorQualityBaselineSlug} slug
 * @property {string} recordId
 * @property {string} companyName
 * @property {string} domain
 * @property {string} region
 * @property {string} explorerUrl
 * @property {string} referenceFolder
 * @property {string[]} fixtureGlobs
 */

/** @type {ReadonlyArray<OperatorQualityBaselineEntry>} */
export const OPERATOR_QUALITY_BASELINE_OPERATORS = Object.freeze([
  Object.freeze({
    slug: "arbor-lodging-cala",
    recordId: "recF5Z87OAqFgndoq",
    companyName: "Arbor Lodging (CALA)",
    domain: "arborlodging.com",
    region: "CALA",
    explorerUrl: "/operator-explorer-gold-mock.html?id=recF5Z87OAqFgndoq",
    referenceFolder: "Arbor Lodging",
    fixtureGlobs: Object.freeze([
      "fixtures/operator-*-arbor-cala.json",
      "public/fixtures/operator-*-arbor-cala.json",
    ]),
  }),
  Object.freeze({
    slug: "hotel-equities-cala",
    recordId: "recWPKu5laVZxsvpn",
    companyName: "Hotel Equities (CALA)",
    domain: "hotelequities.com",
    region: "CALA",
    explorerUrl: "/operator-explorer-gold-mock.html?id=recWPKu5laVZxsvpn",
    referenceFolder: "Hotel Equities",
    fixtureGlobs: Object.freeze([
      "fixtures/operator-*-he-cala.json",
      "public/fixtures/operator-*-he-cala.json",
    ]),
  }),
]);

/** @type {ReadonlySet<string>} */
export const OPERATOR_QUALITY_BASELINE_SLUGS = Object.freeze(
  new Set(OPERATOR_QUALITY_BASELINE_OPERATORS.map((o) => o.slug))
);

/** @type {ReadonlySet<string>} */
export const OPERATOR_QUALITY_BASELINE_RECORD_IDS = Object.freeze(
  new Set(OPERATOR_QUALITY_BASELINE_OPERATORS.map((o) => o.recordId))
);

/**
 * @param {string} slugOrRecordId
 * @returns {OperatorQualityBaselineEntry | null}
 */
export function getOperatorQualityBaselineEntry(slugOrRecordId) {
  const key = String(slugOrRecordId || "").trim();
  if (!key) return null;
  return (
    OPERATOR_QUALITY_BASELINE_OPERATORS.find(
      (o) => o.slug === key || o.recordId === key
    ) || null
  );
}

/**
 * @param {string} slugOrRecordId
 * @returns {boolean}
 */
export function isProtectedOperatorQualityBaseline(slugOrRecordId) {
  return getOperatorQualityBaselineEntry(slugOrRecordId) != null;
}

/**
 * Snapshot used by freeze tests and audits.
 * @returns {{
 *   version: string,
 *   expectedCount: number,
 *   operators: Array<{ slug: string, recordId: string, companyName: string }>
 * }}
 */
export function getOperatorQualityBaselineFreezeSnapshot() {
  return {
    version: OPERATOR_QUALITY_BASELINE_VERSION,
    expectedCount: OPERATOR_QUALITY_BASELINE_EXPECTED_COUNT,
    operators: OPERATOR_QUALITY_BASELINE_OPERATORS.map((o) => ({
      slug: o.slug,
      recordId: o.recordId,
      companyName: o.companyName,
    })),
  };
}
