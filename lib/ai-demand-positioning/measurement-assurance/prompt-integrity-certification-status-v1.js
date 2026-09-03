/**
 * Certification / Period-2 readiness after founder-approved recovery planning.
 * Published reports NOT changed.
 */

export const ADP_PROMPT_INTEGRITY_CERTIFICATION_STATUS_V1 = Object.freeze({
  version: "ADP_PROMPT_INTEGRITY_CERTIFICATION_STATUS_V1",
  updatedAt: "2026-08-21",
  properties: Object.freeze({
    adp_now_now_noho: {
      status: "ASSURANCE_REVIEW_REQUIRED",
      flags: ["PROFILE_AFFILIATION_CORRECTED_IN_FIXTURE", "CORE_PROMPT_METHODOLOGY_REVIEW"],
      note: "Fixture affiliation corrected Independent/Dovetail; published reports still original until correction publish",
    },
    adp_waterstone_boca_raton: {
      status: "PRIOR_PLUS_CORE_PROMPT_METHODOLOGY_REVIEW",
      flags: ["CORE_PROMPT_METHODOLOGY_REVIEW"],
    },
    adp_renaissance_times_square: {
      status: "PRIOR_PLUS_CORE_PROMPT_METHODOLOGY_REVIEW",
      flags: ["CORE_PROMPT_METHODOLOGY_REVIEW"],
    },
    adp_cambridge_beaches_bermuda: {
      status: "PRIOR_PLUS_CORE_PROMPT_METHODOLOGY_REVIEW",
      flags: ["CORE_PROMPT_METHODOLOGY_REVIEW"],
    },
    adp_hotel_phillips_kansas_city: {
      status: "PRIOR_PLUS_CORE_PROMPT_METHODOLOGY_REVIEW",
      flags: ["CORE_PROMPT_METHODOLOGY_REVIEW"],
    },
  }),
  promptIntegrityReadyForNextPeriod: Object.freeze({
    status: "FAIL",
    gate: "PROMPT_INTEGRITY_READY_FOR_NEXT_PERIOD",
    blockers: Object.freeze([
      "Neutral replacement paid execution not founder-approved/executed",
      "Same-period corrected metrics not recalculated/recertified",
      "ADP_MEASUREMENT_CONTRACT_V1_1 not activated",
      "Historical periods lack exactRenderedPrompt on observations (runner persistence enabled for future only)",
    ]),
  }),
  adpRealPeriod2Ready: Object.freeze({
    status: "FAIL",
    gate: "ADP_REAL_PERIOD_2_READY",
    requiresBoth: Object.freeze([
      "PROMPT_INTEGRITY_READY_FOR_NEXT_PERIOD=PASS",
      "REAL_SECOND_PERIOD_HISTORY_PERSISTENCE_READY=PASS",
    ]),
  }),
  pausedUntilReady: Object.freeze([
    "production Airtable historical activation",
    "real Period 2 monitoring",
    "production comparison UI",
    "weekly automation",
    "paid neutral replacement execution (pending founder approval of manifest)",
  ]),
});
