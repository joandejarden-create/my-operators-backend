/**
 * Full GDI E2E publication-quality gates (Renaissance → standard process).
 *
 * Shared rules so the next hotel inherits fit/drawer/eligibility/dedupe
 * requirements through the coordinator evaluator — not a hotel-specific repair.
 */

/** Research / narrative fields required on hotel-facing canonical writes. */
export const CANONICAL_ELIGIBILITY_RESEARCH_FIELDS = Object.freeze([
  "hotelFitRationale",
  "localConnection",
]);

/**
 * Soft-required: at least one travel/occasion evidence signal when publishing
 * corporate / entertainment / exhibitor / scaling packages.
 */
export const CANONICAL_ELIGIBILITY_EVIDENCE_ANY_OF = Object.freeze([
  "travelEvidence",
  "travelEvidenceNote",
  "relevantTravelers",
  "occasionEvidence",
  "eventActivityEvidence",
  "whyThisMatters",
  "proposedHotelUse",
]);

/**
 * Assert an opportunity bag is eligible for canonical write without a later
 * "eligibility repair" pass.
 * @param {object} opp
 * @returns {{ ok: boolean, missing: string[], reasons: string[] }}
 */
export function assertCanonicalWriteEligibilityFields(opp = {}) {
  const missing = [];
  const reasons = [];
  for (const key of CANONICAL_ELIGIBILITY_RESEARCH_FIELDS) {
    const v = opp[key];
    const ok =
      (typeof v === "string" && v.trim().length > 0) ||
      (Array.isArray(v) && v.length > 0) ||
      (v && typeof v === "object" && Object.keys(v).length > 0);
    if (!ok) missing.push(key);
  }
  if (missing.length) {
    reasons.push(
      `Canonical write missing eligibility research fields: ${missing.join(", ")}`
    );
  }
  const anyEvidence = CANONICAL_ELIGIBILITY_EVIDENCE_ANY_OF.some((key) => {
    const v = opp[key];
    if (v === true) return true;
    if (typeof v === "string" && v.trim()) return true;
    if (Array.isArray(v) && v.length) return true;
    return false;
  });
  if (!anyEvidence) {
    missing.push("travel_or_occasion_evidence");
    reasons.push(
      "Canonical write missing travel/occasion evidence (any of: " +
        CANONICAL_ELIGIBILITY_EVIDENCE_ANY_OF.join(", ") +
        ")"
    );
  }
  return { ok: missing.length === 0, missing, reasons };
}

/**
 * Evaluate publication-quality + identity gates for Full E2E COMPLETE.
 * @param {object} publication
 * @param {object} ctx
 * @returns {{ ok: boolean, incomplete: boolean }}
 */
export function evaluatePublicationQuality(publication = {}, ctx = {}) {
  const { blockers, reasons, qualifiedPublishedHint } = ctx;
  const pub = publication || {};
  const published =
    pub.qualifiedPublished === true || qualifiedPublishedHint === true;

  let incomplete = false;

  if (published) {
    if (pub.structuredHotelFitOk === false || pub.hotelFitAssessmentsPresent === false) {
      incomplete = true;
      blockers.push({ code: "publication_without_structured_hotel_fit" });
      reasons.push(
        "Structured hotel-fit assessments must be generated before publication COMPLETE"
      );
    } else if (
      pub.structuredHotelFitOk !== true &&
      pub.hotelFitAssessmentsPresent !== true &&
      pub.zeroQualifiedEmptyStateOk !== true
    ) {
      // Required evidence missing (not asserted) when claiming publication
      incomplete = true;
      blockers.push({ code: "publication_hotel_fit_unverified" });
      reasons.push(
        "Publication claimed without verified structured hotel-fit evidence"
      );
    }

    if (pub.drawersComplete === false || pub.drawerQualityOk === false) {
      incomplete = true;
      blockers.push({ code: "publication_without_complete_drawers" });
      reasons.push("Complete drawers are required before publication COMPLETE");
    } else if (
      pub.drawersComplete !== true &&
      pub.drawerQualityOk !== true &&
      pub.zeroQualifiedEmptyStateOk !== true
    ) {
      incomplete = true;
      blockers.push({ code: "publication_drawers_unverified" });
      reasons.push("Publication claimed without verified drawer-completeness evidence");
    }

    if (pub.postImportEligibilityRepairRequired === true) {
      incomplete = true;
      blockers.push({ code: "post_import_eligibility_repair_required" });
      reasons.push(
        "Canonical writers must ship eligibility research fields — post-import eligibility repair is not part of the standard process"
      );
    }

    if (pub.eligibilityWriteComplete === false) {
      incomplete = true;
      blockers.push({ code: "eligibility_write_incomplete" });
      reasons.push("Canonical write failed eligibility-field completeness gate");
    }

    if (pub.identityDedupeReconciled === false) {
      incomplete = true;
      blockers.push({ code: "identity_dedupe_unreconciled" });
      reasons.push(
        "New records are not necessarily new opportunities — reconcile duplicates and preserve customer history before COMPLETE"
      );
    } else if (
      pub.identityDedupeReconciled !== true &&
      pub.zeroQualifiedEmptyStateOk !== true &&
      (Number(pub.newRecordCount || 0) > 0 || published)
    ) {
      incomplete = true;
      blockers.push({ code: "identity_dedupe_unverified" });
      reasons.push(
        "Publication claimed without verified identity/dedupe reconciliation"
      );
    }

    // newRecordCount > newOpportunityCount is OK when duplicates collapsed;
    // newOpportunityCount > newRecordCount without explanation is inconsistent.
    if (
      Number.isFinite(Number(pub.newRecordCount)) &&
      Number.isFinite(Number(pub.newOpportunityCount)) &&
      Number(pub.newOpportunityCount) > Number(pub.newRecordCount) &&
      pub.identityInflationExplained !== true
    ) {
      incomplete = true;
      blockers.push({ code: "identity_count_inconsistent" });
      reasons.push(
        "newOpportunityCount exceeds newRecordCount without identityInflationExplained"
      );
    }
  }

  return { ok: !incomplete, incomplete };
}

/**
 * Sports and educational axes must not be satisfied by proxy from another segment.
 */
export function evaluateIndependentSegmentAxes(applicable = [], { blockers, reasons }) {
  let incomplete = 0;
  for (const s of applicable) {
    const id = String(s.segmentId || "");
    if (id === "sports" || id.includes("sport")) {
      if (
        s.satisfiedByExhibitorProxy === true ||
        s.satisfiedByAssociationContacts === true ||
        s.executionMode === "exhibitor_proxy" ||
        s.independentAssessment === false
      ) {
        incomplete += 1;
        reasons.push(
          "Sports coverage requires an independent assessment — exhibitor/association proxy is insufficient"
        );
      }
    }
    if (
      id === "tours_educational_specialist" ||
      id.includes("educational") ||
      id.includes("tours_")
    ) {
      if (
        s.satisfiedBySportsProxy === true ||
        s.satisfiedByMedicalProxy === true ||
        s.satisfiedByAssociationContacts === true ||
        s.executionMode === "sports_proxy" ||
        s.independentAssessment === false
      ) {
        incomplete += 1;
        reasons.push(
          "Educational/tour coverage requires an independent assessment — sports/medical/association proxy is insufficient"
        );
      }
    }
  }
  if (incomplete > 0) {
    blockers.push({ code: "independent_segment_assessment_missing", count: incomplete });
  }
  return { incomplete };
}

/**
 * Contact depth cannot be skipped merely because an organization route exists.
 */
export function evaluateContactDepthOrgRoutePolicy(contactDepth = {}, { blockers, reasons }) {
  if (
    contactDepth.skippedBecauseOrgRouteExists === true ||
    contactDepth.skippedForOrganizationFallback === true ||
    contactDepth.status === "skipped_org_route_exists"
  ) {
    blockers.push({ code: "contact_depth_skipped_for_org_route" });
    reasons.push(
      "Contact depth is mandatory even when an organization fallback exists — document the attempt and disposition"
    );
    return { incomplete: true };
  }
  return { incomplete: false };
}

/**
 * Hotel/product budgets must remain isolated.
 */
export function evaluateBudgetIsolation(budget = {}, { blockers, reasons, hotelId }) {
  if (budget.crossHotelCombined === true || budget.crossProductCombined === true) {
    blockers.push({ code: "budget_cross_scope_combined" });
    reasons.push("Hotel/product budgets must remain separate — combined ledgers cannot COMPLETE");
    return { ok: false };
  }
  if (
    budget.hotelId &&
    hotelId &&
    String(budget.hotelId) !== String(hotelId)
  ) {
    blockers.push({ code: "budget_wrong_hotel" });
    reasons.push(
      `Budget hotelId=${budget.hotelId} does not match run hotelId=${hotelId}`
    );
    return { ok: false };
  }
  return { ok: true };
}
