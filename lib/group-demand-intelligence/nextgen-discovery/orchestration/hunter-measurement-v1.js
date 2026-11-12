/**
 * Per-hunter measurement ledger — incremental commercial value, not API volume.
 */

export const HUNTER_MEASUREMENT_V1 = "gdi-nextgen-hunter-measurement-v1";

export function buildHunterMeasurementReport({
  hunterId,
  hotelId,
  rawDiscoveries = 0,
  incrementalUnique = 0,
  evidenceGroundedTheses = 0,
  qualifiedForOrs = 0,
  usefulContacts = 0,
  rejected = 0,
  duplicates = 0,
  previouslyRejected = 0,
  researchCostUsd = 0,
  researchLevels = null,
  discoveryEvidenceClass = null,
  paidAuthorized = false,
} = {}) {
  const cost = Number(researchCostUsd) || 0;
  const qualified = Number(qualifiedForOrs) || 0;
  return {
    schema: HUNTER_MEASUREMENT_V1,
    hunterId,
    hotelId,
    discoveryEvidenceClass,
    paidAuthorized: paidAuthorized === true,
    counts: {
      sourceDiscoveries: Number(rawDiscoveries) || 0,
      incrementalUniqueCandidates: Number(incrementalUnique) || 0,
      evidenceGroundedHotelTheses: Number(evidenceGroundedTheses) || 0,
      qualifiedOpportunities: qualified,
      usefulContacts: Number(usefulContacts) || 0,
      rejectedLeads: Number(rejected) || 0,
      duplicates: Number(duplicates) || 0,
      previouslyRejected: Number(previouslyRejected) || 0,
    },
    researchLevels: researchLevels || null,
    cost: {
      researchCostUsd: cost,
      costPerQualifiedOpportunityUsd:
        qualified > 0 ? Number((cost / qualified).toFixed(4)) : null,
      note:
        cost === 0
          ? "Zero-paid path — cost per qualified opportunity is null or $0."
          : "Paid cost attributed only when founder-authorized spend occurs.",
    },
    successMetric:
      "Incremental commercially credible opportunities hotels can act on — not discovery volume or API calls.",
  };
}
