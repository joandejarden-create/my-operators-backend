/**
 * D4 Option B — operator-visible separation of synthetic / provisional / seed data.
 * Prefer existing provenance/status fields. No deletions. No schema migration here.
 */

export const GDI_SYNTHETIC_PROVISIONAL_ISOLATION_V1 =
  "gdi-synthetic-provisional-isolation-v1";

export const INTELLIGENCE_PROVENANCE_CLASS = Object.freeze({
  VERIFIED_HOTEL_PROVIDED: "VERIFIED_HOTEL_PROVIDED",
  PROVISIONAL_INTERNAL_PLANNING: "PROVISIONAL_INTERNAL_PLANNING",
  SYNTHETIC_TEST: "SYNTHETIC_TEST",
  GENERIC_ONBOARDING_SEED: "GENERIC_ONBOARDING_SEED",
  UNVERIFIED_RESEARCH: "UNVERIFIED_RESEARCH",
});

const SYNTHETIC_RE =
  /SYNTHETIC_TEST|founder_test_scenario|founder_lifecycle_isolate|Synthetic Test/i;
const PROVISIONAL_RE =
  /PROVISIONAL_INTERNAL_PLANNING|PROVISIONAL|Test Scenario|MARKET_ESTIMATED/i;
const SEED_RE = /generic_seed|generic_onboard_seed|gdi_hotel_onboard_seed/i;
const HOTEL_REPORTED_RE = /HOTEL_REPORTED|Hotel Confirmed|Company Validated/i;

/**
 * Classify a record using existing provenance/status/source fields.
 */
export function classifyIntelligenceProvenance(input = {}) {
  const blob = [
    input.provenance,
    input.source,
    input.status,
    input.changeReason,
    input.fitRationale,
    input.reasonMonitored,
    input.researchReason,
    input.notes,
    typeof input.payload === "string" ? input.payload : JSON.stringify(input.payload || {}),
  ]
    .filter(Boolean)
    .join("\n");

  if (SYNTHETIC_RE.test(blob) || input.status === "Test Scenario" && /SYNTHETIC|founder_test/i.test(blob)) {
    return {
      class: INTELLIGENCE_PROVENANCE_CLASS.SYNTHETIC_TEST,
      customerEligible: false,
      hotelConfirmed: false,
    };
  }
  if (PROVISIONAL_RE.test(blob) || input.status === "Test Scenario") {
    return {
      class: INTELLIGENCE_PROVENANCE_CLASS.PROVISIONAL_INTERNAL_PLANNING,
      customerEligible: false,
      hotelConfirmed: false,
    };
  }
  if (SEED_RE.test(blob)) {
    return {
      class: INTELLIGENCE_PROVENANCE_CLASS.GENERIC_ONBOARDING_SEED,
      customerEligible: false,
      hotelConfirmed: false,
    };
  }
  if (HOTEL_REPORTED_RE.test(blob) || input.status === "Active" && /HOTEL_REPORTED/i.test(blob)) {
    return {
      class: INTELLIGENCE_PROVENANCE_CLASS.VERIFIED_HOTEL_PROVIDED,
      customerEligible: true,
      hotelConfirmed: true,
    };
  }
  return {
    class: INTELLIGENCE_PROVENANCE_CLASS.UNVERIFIED_RESEARCH,
    customerEligible: false,
    hotelConfirmed: false,
  };
}

/**
 * Recommended Airtable filtered views (no schema change).
 */
export function recommendedOperatorIsolationViews() {
  return [
    {
      table: "GDI Hotel Need Periods",
      view: "Verified Hotel-Reported Only",
      filterHint: "Status = Active AND Source = HOTEL_REPORTED",
    },
    {
      table: "GDI Hotel Need Periods",
      view: "Provisional / Test Scenarios",
      filterHint: "Status = 'Test Scenario' OR Source contains PROVISIONAL OR SYNTHETIC",
    },
    {
      table: "GDI Hotel Commercial Profiles",
      view: "Exclude Synthetic Test",
      filterHint: "Provenance != 'Synthetic Test'",
    },
    {
      table: "Hotel Demand Generator Fit",
      view: "Non-seed Fits",
      filterHint: "NOT FIND('generic_seed', {Fit Rationale})",
    },
    {
      table: "GDI Research Targets",
      view: "Committed Monitoring (non-seed)",
      filterHint: "NOT FIND('generic_seed', {Reason Monitored})",
    },
  ];
}

export function assertNotPersistedAsHotelConfirmed(classification) {
  if (!classification) return { ok: false, reason: "MISSING_CLASSIFICATION" };
  if (
    classification.class === INTELLIGENCE_PROVENANCE_CLASS.SYNTHETIC_TEST ||
    classification.class === INTELLIGENCE_PROVENANCE_CLASS.PROVISIONAL_INTERNAL_PLANNING ||
    classification.class === INTELLIGENCE_PROVENANCE_CLASS.GENERIC_ONBOARDING_SEED
  ) {
    return {
      ok: true,
      mayPresentAsHotelConfirmed: false,
      mayWriteAsHotelConfirmed: false,
    };
  }
  return {
    ok: true,
    mayPresentAsHotelConfirmed: classification.hotelConfirmed === true,
    mayWriteAsHotelConfirmed: classification.hotelConfirmed === true,
  };
}

export default {
  GDI_SYNTHETIC_PROVISIONAL_ISOLATION_V1,
  INTELLIGENCE_PROVENANCE_CLASS,
  classifyIntelligenceProvenance,
  recommendedOperatorIsolationViews,
  assertNotPersistedAsHotelConfirmed,
};
