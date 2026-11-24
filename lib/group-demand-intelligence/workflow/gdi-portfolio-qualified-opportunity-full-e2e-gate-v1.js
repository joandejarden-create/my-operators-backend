/**
 * Permanent Full E2E gate — every hotel opportunity must be reviewed and accounted for.
 *
 * Founder directive 2026-11-24: No Full E2E run is COMPLETE until every opportunity
 * belonging to that hotel has a documented PASS / HOLD / REJECT / HISTORICAL disposition
 * against the six QUALIFIED OPPORTUNITY gates.
 *
 * Does not write STAGED/CURRENT pointers, Airtable, or paid research.
 */

export const GDI_PORTFOLIO_QUALIFIED_OPPORTUNITY_FULL_E2E_GATE_V1 =
  "gdi-portfolio-qualified-opportunity-full-e2e-gate-v1";

export const FQE2E_DISPOSITION = Object.freeze({
  PASS: "PASS",
  HOLD: "HOLD",
  REJECT: "REJECT",
  HISTORICAL: "HISTORICAL",
});

export const FQE2E_REQUIRED_STEPS = Object.freeze([
  "LOAD_HISTORICAL_AND_CURRENT_INVENTORY",
  "RECONCILE_ORIGINAL_EXISTING_AND_NEW",
  "REVALIDATE_EVERY_OPPORTUNITY",
  "REUSE_EVIDENCE_AND_COMPLETE_AUTHORIZED_RESEARCH",
  "APPLY_SIX_QUALIFICATION_GATES",
  "PREPARE_VERSIONED_STAGED_CANDIDATE",
  "COMPARE_AGAINST_EXISTING_STAGED_AND_CURRENT",
  "RECORD_PASS_HOLD_REJECT_HISTORICAL",
  "CERTIFY_HOTEL_INVENTORY",
  "PRODUCE_FOUNDER_COMPLETION_REPORT",
]);

/**
 * @param {{
 *   hotelId?: string,
 *   dispositions?: Array<{ opportunityId?: string, disposition?: string }>,
 *   opportunityIds?: string[],
 *   stepsCompleted?: string[],
 *   stagedCandidatePrepared?: boolean,
 *   stagedPointerActivated?: boolean,
 *   paidResearchExecutedWithoutAuth?: boolean,
 * }} input
 */
export function evaluatePortfolioQualifiedOpportunityFullE2eGate(input = {}) {
  const hotelId = String(input.hotelId || "").trim();
  const opportunityIds = Array.isArray(input.opportunityIds)
    ? [...new Set(input.opportunityIds.map((id) => String(id || "").trim()).filter(Boolean))]
    : [];
  const dispositions = Array.isArray(input.dispositions) ? input.dispositions : [];
  const byId = new Map();
  for (const row of dispositions) {
    const id = String(row?.opportunityId || "").trim();
    if (!id) continue;
    byId.set(id, String(row?.disposition || "").trim().toUpperCase());
  }

  const missingDisposition = opportunityIds.filter((id) => !byId.has(id));
  const invalidDisposition = [...byId.entries()]
    .filter(([, d]) => !Object.values(FQE2E_DISPOSITION).includes(d))
    .map(([id, d]) => ({ opportunityId: id, disposition: d }));

  const stepsCompleted = new Set(
    (Array.isArray(input.stepsCompleted) ? input.stepsCompleted : []).map((s) =>
      String(s || "").trim().toUpperCase(),
    ),
  );
  const missingSteps = FQE2E_REQUIRED_STEPS.filter(
    (s) => !stepsCompleted.has(s),
  );

  const stagedPointerActivated = input.stagedPointerActivated === true;
  const paidWithoutAuth = input.paidResearchExecutedWithoutAuth === true;
  const candidatePrepared = input.stagedCandidatePrepared === true;

  const complete =
    Boolean(hotelId) &&
    opportunityIds.length > 0 &&
    missingDisposition.length === 0 &&
    invalidDisposition.length === 0 &&
    missingSteps.length === 0 &&
    candidatePrepared === true &&
    stagedPointerActivated === false &&
    paidWithoutAuth === false;

  return {
    contractVersion: GDI_PORTFOLIO_QUALIFIED_OPPORTUNITY_FULL_E2E_GATE_V1,
    hotelId,
    complete,
    incompleteReason: complete
      ? null
      : missingDisposition.length
        ? "OPPORTUNITIES_MISSING_DISPOSITION"
        : missingSteps.length
          ? "REQUIRED_STEPS_INCOMPLETE"
          : stagedPointerActivated
            ? "STAGED_POINTER_ACTIVATED_WITHOUT_FOUNDER_APPROVAL"
            : paidWithoutAuth
              ? "PAID_RESEARCH_WITHOUT_AUTHORIZATION"
              : !candidatePrepared
                ? "STAGED_CANDIDATE_NOT_PREPARED"
                : opportunityIds.length === 0
                  ? "NO_OPPORTUNITIES_TO_REVIEW"
                  : "INCOMPLETE",
    counts: {
      opportunities: opportunityIds.length,
      dispositions: byId.size,
      missingDisposition: missingDisposition.length,
      pass: [...byId.values()].filter((d) => d === FQE2E_DISPOSITION.PASS).length,
      hold: [...byId.values()].filter((d) => d === FQE2E_DISPOSITION.HOLD).length,
      reject: [...byId.values()].filter((d) => d === FQE2E_DISPOSITION.REJECT)
        .length,
      historical: [...byId.values()].filter(
        (d) => d === FQE2E_DISPOSITION.HISTORICAL,
      ).length,
    },
    missingDispositionIds: missingDisposition.slice(0, 50),
    invalidDisposition,
    missingSteps,
    stagedCandidatePrepared: candidatePrepared,
    stagedPointerActivated,
    note:
      "Full E2E COMPLETE requires every opportunity dispositioned. STAGED activation remains a separate founder approval step.",
  };
}

export default {
  GDI_PORTFOLIO_QUALIFIED_OPPORTUNITY_FULL_E2E_GATE_V1,
  FQE2E_DISPOSITION,
  FQE2E_REQUIRED_STEPS,
  evaluatePortfolioQualifiedOpportunityFullE2eGate,
};
