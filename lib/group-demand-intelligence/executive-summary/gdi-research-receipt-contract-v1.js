/**
 * Canonical GDI Research Receipt contract.
 *
 * Permanent Dealality brand/design principle (see
 * docs/ai-build-system/DEALALITY_RESEARCH_RECEIPT_PRINCIPLES.md).
 * This module is the **GDI reference implementation only** — do not treat it
 * as a cross-product reporting engine or shared ADP/HE/Deal Opportunities runtime.
 *
 * Phase 1: schema + availability rules; aggregation is read-only.
 *
 * Attribution layers:
 *   SHARED_MARKET — research that can benefit multiple hotels (do not
 *                   attribute full run counts to every hotel).
 *   HOTEL_SPECIFIC — fit, need-period, qualification, pursuit advice.
 *   REUSED — prior research legitimately reused without re-billing.
 *
 * Never emit synthetic counts. Prefer UNAVAILABLE / omit over zero-fill.
 */

export const GDI_RESEARCH_RECEIPT_CONTRACT_ID =
  "gdi-research-receipt-contract-v1";

export const RESEARCH_RECEIPT_AVAILABILITY = Object.freeze({
  AVAILABLE_EXISTING: "AVAILABLE_EXISTING",
  AVAILABLE_DERIVED: "AVAILABLE_DERIVED",
  PARTIAL_COVERAGE: "PARTIAL_COVERAGE",
  TELEMETRY_REQUIRED: "TELEMETRY_REQUIRED",
  HOTEL_INPUT_REQUIRED: "HOTEL_INPUT_REQUIRED",
  NOT_CURRENTLY_SUPPORTED: "NOT_CURRENTLY_SUPPORTED",
  UNAVAILABLE: "UNAVAILABLE",
});

export const RESEARCH_RECEIPT_ATTRIBUTION = Object.freeze({
  SHARED_MARKET: "SHARED_MARKET",
  HOTEL_SPECIFIC: "HOTEL_SPECIFIC",
  REUSED: "REUSED",
  MIXED: "MIXED",
  UNKNOWN: "UNKNOWN",
});

/**
 * Metric catalog — customer-facing Research Receipt keys.
 * Each entry documents counting unit, attribution, and Phase-1 availability.
 */
export const RESEARCH_RECEIPT_METRIC_CATALOG = Object.freeze([
  {
    key: "demandSignalsEvaluated",
    label: "Demand signals evaluated",
    countingUnit: "signal",
    attribution: RESEARCH_RECEIPT_ATTRIBUTION.SHARED_MARKET,
    phase1Availability: RESEARCH_RECEIPT_AVAILABILITY.TELEMETRY_REQUIRED,
    customerFacing: true,
    notes:
      "Requires hotel-attributable stage counts before/after qualification; do not assign full market-run totals to one hotel.",
  },
  {
    key: "uniqueOpportunitiesDiscovered",
    label: "Unique opportunities discovered",
    countingUnit: "opportunityIdentity",
    attribution: RESEARCH_RECEIPT_ATTRIBUTION.MIXED,
    phase1Availability: RESEARCH_RECEIPT_AVAILABILITY.PARTIAL_COVERAGE,
    customerFacing: true,
    notes:
      "Phase 1: customer-visible inventory size when CURRENT publication exists; not raw discovery volume.",
  },
  {
    key: "opportunitiesDeeplyResearched",
    label: "Opportunities deeply researched",
    countingUnit: "opportunity",
    attribution: RESEARCH_RECEIPT_ATTRIBUTION.HOTEL_SPECIFIC,
    phase1Availability: RESEARCH_RECEIPT_AVAILABILITY.PARTIAL_COVERAGE,
    customerFacing: true,
    notes:
      "Derived from dossier depth markers on customer-visible opportunities when present.",
  },
  {
    key: "uniqueEvidenceSourcesEvaluated",
    label: "Unique evidence sources evaluated",
    countingUnit: "sourceUrl",
    attribution: RESEARCH_RECEIPT_ATTRIBUTION.MIXED,
    phase1Availability: RESEARCH_RECEIPT_AVAILABILITY.PARTIAL_COVERAGE,
    customerFacing: true,
    notes: "Deduped HTTP evidence URLs on customer-visible opportunities.",
  },
  {
    key: "opportunitiesScreenedOut",
    label: "Opportunities screened out",
    countingUnit: "opportunity",
    attribution: RESEARCH_RECEIPT_ATTRIBUTION.HOTEL_SPECIFIC,
    phase1Availability: RESEARCH_RECEIPT_AVAILABILITY.TELEMETRY_REQUIRED,
    customerFacing: true,
    notes: "Needs durable exclusion ledger with hotel attribution.",
  },
  {
    key: "whoHowResearchPerformed",
    label: "WHO/HOW research performed",
    countingUnit: "opportunity",
    attribution: RESEARCH_RECEIPT_ATTRIBUTION.HOTEL_SPECIFIC,
    phase1Availability: RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_DERIVED,
    customerFacing: true,
    notes: "Count of customer-visible opportunities with WHO/HOW presentation present.",
  },
  {
    key: "verifiedDirectBuyers",
    label: "Verified direct buyers identified",
    countingUnit: "personSlot",
    attribution: RESEARCH_RECEIPT_ATTRIBUTION.HOTEL_SPECIFIC,
    phase1Availability: RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_DERIVED,
    customerFacing: true,
  },
  {
    key: "verifiedBuyerInfluencers",
    label: "Verified buyer influencers identified",
    countingUnit: "personSlot",
    attribution: RESEARCH_RECEIPT_ATTRIBUTION.HOTEL_SPECIFIC,
    phase1Availability: RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_DERIVED,
    customerFacing: true,
  },
  {
    key: "verifiedAlternativeEntryPoints",
    label: "Verified alternative entry points identified",
    countingUnit: "personSlot",
    attribution: RESEARCH_RECEIPT_ATTRIBUTION.HOTEL_SPECIFIC,
    phase1Availability: RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_DERIVED,
    customerFacing: true,
  },
  {
    key: "verifiedPersonSpecificOutreachRoutes",
    label: "Verified person-specific outreach routes",
    countingUnit: "channel",
    attribution: RESEARCH_RECEIPT_ATTRIBUTION.HOTEL_SPECIFIC,
    phase1Availability: RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_DERIVED,
    customerFacing: true,
  },
  {
    key: "opportunitiesMatchedToNeedPeriods",
    label: "Opportunities matched to need periods",
    countingUnit: "opportunity",
    attribution: RESEARCH_RECEIPT_ATTRIBUTION.HOTEL_SPECIFIC,
    phase1Availability: RESEARCH_RECEIPT_AVAILABILITY.PARTIAL_COVERAGE,
    customerFacing: true,
  },
  {
    key: "newlyDiscoveredIntelligence",
    label: "Newly discovered intelligence",
    countingUnit: "opportunity",
    attribution: RESEARCH_RECEIPT_ATTRIBUTION.HOTEL_SPECIFIC,
    phase1Availability: RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_DERIVED,
    customerFacing: true,
    notes: "Weekly-delta NEW on customer-visible inventory.",
  },
  {
    key: "materiallyChangedIntelligence",
    label: "Materially changed intelligence",
    countingUnit: "opportunity",
    attribution: RESEARCH_RECEIPT_ATTRIBUTION.HOTEL_SPECIFIC,
    phase1Availability: RESEARCH_RECEIPT_AVAILABILITY.AVAILABLE_DERIVED,
    customerFacing: true,
    notes: "Weekly-delta UPDATED (+ REACTIVATED).",
  },
  {
    key: "existingIntelligenceReused",
    label: "Existing intelligence reused",
    countingUnit: "opportunity",
    attribution: RESEARCH_RECEIPT_ATTRIBUTION.REUSED,
    phase1Availability: RESEARCH_RECEIPT_AVAILABILITY.TELEMETRY_REQUIRED,
    customerFacing: true,
    notes: "Needs reuse provenance stamps; do not invent.",
  },
]);

/**
 * @param {string} key
 * @param {{ value: number|null, availability: string, attribution?: string, reason?: string|null }} measured
 */
export function buildResearchReceiptMetric(key, measured = {}) {
  const def = RESEARCH_RECEIPT_METRIC_CATALOG.find((m) => m.key === key) || {
    key,
    label: key,
    countingUnit: "unknown",
    attribution: RESEARCH_RECEIPT_ATTRIBUTION.UNKNOWN,
    phase1Availability: RESEARCH_RECEIPT_AVAILABILITY.UNAVAILABLE,
    customerFacing: true,
  };
  const availability =
    measured.availability ||
    def.phase1Availability ||
    RESEARCH_RECEIPT_AVAILABILITY.UNAVAILABLE;
  const hasValue =
    measured.value != null &&
    Number.isFinite(Number(measured.value)) &&
    availability !== RESEARCH_RECEIPT_AVAILABILITY.UNAVAILABLE &&
    availability !== RESEARCH_RECEIPT_AVAILABILITY.TELEMETRY_REQUIRED &&
    availability !== RESEARCH_RECEIPT_AVAILABILITY.NOT_CURRENTLY_SUPPORTED &&
    availability !== RESEARCH_RECEIPT_AVAILABILITY.HOTEL_INPUT_REQUIRED;

  return {
    key: def.key,
    label: def.label,
    countingUnit: def.countingUnit,
    attribution:
      measured.attribution || def.attribution || RESEARCH_RECEIPT_ATTRIBUTION.UNKNOWN,
    availability,
    value: hasValue ? Number(measured.value) : null,
    reason: measured.reason || null,
    customerFacing: def.customerFacing !== false,
  };
}

/**
 * Compact four-metric Research Receipt for future PDF page 2.
 */
export function buildCompactResearchReceiptForPdf(metrics = []) {
  const byKey = Object.fromEntries(
    (metrics || []).map((m) => [m.key, m]),
  );
  const pick = (key) => {
    const m = byKey[key];
    if (!m || m.value == null) return { key, value: null, availability: m?.availability || RESEARCH_RECEIPT_AVAILABILITY.UNAVAILABLE };
    return { key, value: m.value, availability: m.availability };
  };
  return {
    demandSignalsReviewed: pick("demandSignalsEvaluated"),
    signalsFilteredOrConsolidated: pick("opportunitiesScreenedOut"),
    existingOpportunitiesRevalidated: pick("materiallyChangedIntelligence"),
    commercialContactPathsValidated: pick("verifiedPersonSpecificOutreachRoutes"),
  };
}

export function emptyResearchReceiptShell(opts = {}) {
  return {
    contractId: GDI_RESEARCH_RECEIPT_CONTRACT_ID,
    hotelId: opts.hotelId || null,
    reportingPeriod: opts.reportingPeriod || null,
    lastVerifiedAt: opts.lastVerifiedAt || null,
    publicationPointer: opts.publicationPointer || null,
    metrics: RESEARCH_RECEIPT_METRIC_CATALOG.map((m) =>
      buildResearchReceiptMetric(m.key, {
        value: null,
        availability: m.phase1Availability,
        reason:
          m.phase1Availability === RESEARCH_RECEIPT_AVAILABILITY.TELEMETRY_REQUIRED
            ? "Hotel-attributable telemetry not yet durable"
            : m.phase1Availability === RESEARCH_RECEIPT_AVAILABILITY.NOT_CURRENTLY_SUPPORTED
              ? "Not supported in Phase 1"
              : "Awaiting customer-authorized inventory",
      }),
    ),
    provenance: {
      measuredVsDerived: "MIXED",
      sharedMarketAttributionPolicy:
        "Do not attribute full shared-market research runs to every hotel",
      syntheticCountsProhibited: true,
    },
  };
}
