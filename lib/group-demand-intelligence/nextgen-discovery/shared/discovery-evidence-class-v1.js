/**
 * Discovery evidence classification — never conflate fixtures with live finds.
 *
 * SYNTHETIC_TEST_FIXTURE: illustrative / cache-backed demo rows (not commercial inventory).
 * HISTORICAL_RESEARCH_REPLAY: previously retrieved official evidence, independently verifiable.
 * LIVE_PUBLIC_DISCOVERY: freshly retrieved free/public source in this run.
 * LIVE_PAID_DISCOVERY: paid provider retrieval (Exa/Apify) under explicit authorization.
 */

export const DISCOVERY_EVIDENCE_CLASS = Object.freeze({
  SYNTHETIC_TEST_FIXTURE: "SYNTHETIC_TEST_FIXTURE",
  HISTORICAL_RESEARCH_REPLAY: "HISTORICAL_RESEARCH_REPLAY",
  LIVE_PUBLIC_DISCOVERY: "LIVE_PUBLIC_DISCOVERY",
  LIVE_PAID_DISCOVERY: "LIVE_PAID_DISCOVERY",
});

export const DISCOVERY_EVIDENCE_CLASS_LIST = Object.freeze(
  Object.values(DISCOVERY_EVIDENCE_CLASS)
);

export function isCustomerInventoryEligibleEvidenceClass(cls) {
  return (
    cls === DISCOVERY_EVIDENCE_CLASS.LIVE_PUBLIC_DISCOVERY ||
    cls === DISCOVERY_EVIDENCE_CLASS.LIVE_PAID_DISCOVERY ||
    cls === DISCOVERY_EVIDENCE_CLASS.HISTORICAL_RESEARCH_REPLAY
  );
}

export function isSyntheticOrFixtureEvidenceClass(cls) {
  return cls === DISCOVERY_EVIDENCE_CLASS.SYNTHETIC_TEST_FIXTURE;
}

/**
 * Normalize a raw discovery row's evidence class. Defaults to synthetic fixture
 * when unlabeled (safe: fixtures must not enter customer inventory).
 */
export function resolveDiscoveryEvidenceClass(row = {}, fallback = DISCOVERY_EVIDENCE_CLASS.SYNTHETIC_TEST_FIXTURE) {
  const raw = String(
    row.discoveryEvidenceClass ||
      row.evidenceClass ||
      row.discoveryClass ||
      ""
  )
    .trim()
    .toUpperCase();
  if (DISCOVERY_EVIDENCE_CLASS_LIST.includes(raw)) return raw;
  return fallback;
}

export function stampDiscoveryEvidenceClass(
  row = {},
  cls = DISCOVERY_EVIDENCE_CLASS.SYNTHETIC_TEST_FIXTURE
) {
  return {
    ...row,
    discoveryEvidenceClass: cls,
    // Activity dates on synthetic fixtures are illustrative only.
    activityDateVerified:
      cls === DISCOVERY_EVIDENCE_CLASS.SYNTHETIC_TEST_FIXTURE
        ? false
        : row.activityDateVerified !== false,
    customerInventoryEligible: isCustomerInventoryEligibleEvidenceClass(cls),
    publicationEligible: false,
  };
}
