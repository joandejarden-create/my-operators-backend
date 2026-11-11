/**
 * GDI Permanent Operating Contract V30 — first-time + weekly 14-workflow research.
 *
 * Binding rules for every future 14-workflow run. Zero-paid module.
 */
export const GDI_PERMANENT_OPERATING_CONTRACT_V30 =
  "gdi-permanent-operating-contract-v30";

export const GDI_PERMANENT_OPERATING_RULES_V30 = Object.freeze({
  version: GDI_PERMANENT_OPERATING_CONTRACT_V30,
  appliesTo: ["first_time_hotel_onboarding", "weekly_fourteen_workflow_refresh"],
  rules: Object.freeze([
    "Rank opportunities by commercial potential before Deep Research spend.",
    "Investigate the strongest candidates first (hotel strategy profile + shared scorer).",
    "Preserve unfinished eligible research queues for adaptive continuation.",
    "Distinguish technical execution (LIVE_COMPLETED) from research completeness (COMPLETE / PARTIAL_*).",
    "Record exact stop reasons on every workflow ledger row.",
    "Continue adaptively within authorized budgets only — never spend merely because funds remain.",
    "Apply independent commercial acceptance unchanged (Sales Ready gate is not weakened).",
    "Preserve previously validated research and customer-facing inventories.",
    "Avoid duplication across discovery, deep, and continuation passes.",
    "Report unfinished high-value candidates transparently in every Full E2E summary.",
    "A first-time hotel onboarding MUST NOT be marked fully researched if commercially promising candidates remain unfinished solely because of arbitrary candidate caps.",
    "Do not hardcode hotel-specific organization names into the shared allocator.",
  ]),
  researchCompletenessStatuses: Object.freeze([
    "COMPLETE",
    "PARTIAL_BUDGET_LIMIT",
    "PARTIAL_CANDIDATE_CAP",
    "PARTIAL_PROVIDER_FAILURE",
    "RESEARCH_EXHAUSTED",
    "NO_ELIGIBLE_DEMAND",
  ]),
  salesReadyGateUnchanged: true,
  paidRequiresFounderGo: true,
});

export function assertPermanentOperatingContractCompliance(summary = {}) {
  const unfinished = Number(summary.unfinishedHighPriorityEligible || 0);
  const markedComplete =
    summary.commercialResearchComplete === true ||
    String(summary.researchCompletenessStatus || "") === "COMPLETE";
  const capLimited =
    summary.candidateCapHit === true ||
    /PARTIAL_CANDIDATE_CAP|CANDIDATE_CAP/i.test(
      String(summary.researchCompletenessStatus || summary.stopReason || ""),
    );
  if (markedComplete && unfinished > 0 && capLimited) {
    const err = new Error(
      "PERMANENT_CONTRACT_VIOLATION: COMPLETE forbidden while high-priority eligible remain under candidate cap",
    );
    err.code = "gdi_permanent_operating_contract_v30";
    throw err;
  }
  return true;
}
