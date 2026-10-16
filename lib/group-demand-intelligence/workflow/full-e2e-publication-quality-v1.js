/**
 * Full GDI E2E publication-quality gates (Mayflower / San Diego / Renaissance → standard).
 *
 * Shared rules so the next hotel inherits fit/drawer/eligibility/dedupe
 * requirements through the coordinator evaluator — not a hotel-specific repair.
 */

import { evaluateProspectSuitability } from "../prospect-publication-eligibility-v1.js";
import { evaluateDrawerContentCompleteness } from "./drawer-content-contract-v1.js";

/** Research / narrative fields required on hotel-facing canonical writes. */
export const CANONICAL_ELIGIBILITY_RESEARCH_FIELDS = Object.freeze([
  "hotelFitRationale",
  "localConnection",
]);

/**
 * Presentation / thesis prose that must NOT alone establish travel, recurring
 * activity, or a qualified demand mechanism (Renaissance eligibility shortcut).
 */
export const PROSE_PRESENCE_NOT_EVIDENCE = Object.freeze([
  "whyThisMatters",
  "proposedHotelUse",
  "summaryWhat",
  "summaryWhyMatters",
  "summaryWhyHotel",
]);

/**
 * Assert an opportunity bag is eligible for canonical write without a later
 * "eligibility repair" pass.
 *
 * Uses the shared use-case suitability contract — not mere presence of
 * whyThisMatters / proposedHotelUse prose.
 *
 * @param {object} opp
 * @returns {{ ok: boolean, missing: string[], reasons: string[], suitability?: object }}
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

  const suitability = evaluateProspectSuitability(opp);
  if (!suitability?.ok) {
    for (const f of suitability?.failed || ["suitability"]) {
      if (!missing.includes(f)) missing.push(f);
    }
    reasons.push(
      `Use-case suitability failed (${suitability?.useCase || "unknown"}): ${(suitability?.failed || []).join(", ")}. ` +
        "whyThisMatters/proposedHotelUse alone do not establish travel, recurring activity, or a qualified demand mechanism."
    );
  }

  return {
    ok: missing.length === 0,
    missing,
    reasons,
    suitability,
  };
}

/**
 * Structured hotel-fit + drawer readiness required before authorized publication writes.
 * Supports incomplete / unknown states without inventing PASS.
 *
 * @param {object} opp
 * @param {object} [opts]
 * @returns {{ ok: boolean, incomplete: boolean, unknown: boolean, blockers: object[], reasons: string[], drawer?: object, fit?: object }}
 */
export function assertPublicationWriteReadiness(opp = {}, opts = {}) {
  const blockers = [];
  const reasons = [];
  let incomplete = false;
  let unknown = false;

  const elig = assertCanonicalWriteEligibilityFields(opp);
  if (!elig.ok) {
    incomplete = true;
    blockers.push({
      code: "publication_eligibility_incomplete",
      missing: elig.missing,
    });
    reasons.push(...elig.reasons);
  }

  const fit =
    (opp.hotelFitAssessment && typeof opp.hotelFitAssessment === "object"
      ? opp.hotelFitAssessment
      : null) ||
    (opp.hotelFitAssessmentStructured &&
    typeof opp.hotelFitAssessmentStructured === "object"
      ? opp.hotelFitAssessmentStructured
      : null);

  if (!fit) {
    // Assessment stage never executed — unfinished work, not an "unknown fact".
    if (opts.requireStructuredHotelFit === false) {
      incomplete = true;
      blockers.push({ code: "structured_hotel_fit_missing_optional_path" });
      reasons.push(
        "Structured hotel-fit assessment not present (assessment never executed — not PASS)"
      );
    } else {
      incomplete = true;
      blockers.push({ code: "structured_hotel_fit_missing" });
      reasons.push(
        "Structured hotel-fit assessment required before authorized publication write"
      );
    }
  } else {
    const status = String(fit.overallStatus || fit.status || "").toLowerCase();
    const dimensions =
      fit.dimensions ||
      fit.dimensionAssessments ||
      opp.hotelFitDimensionAssessments ||
      null;
    const dimensionCount = Array.isArray(dimensions)
      ? dimensions.length
      : dimensions && typeof dimensions === "object"
        ? Object.keys(dimensions).length
        : 0;
    const assessmentExecuted = Boolean(
      fit.assessedAt ||
        fit.assessmentDate ||
        fit.scoringVersion ||
        fit.version ||
        fit.ruleVersion ||
        dimensionCount > 0
    );

    // Placeholder / never-run stamps conceal unfinished work.
    if (
      /never_run|not_started|unassessed|assessment_pending/i.test(status) ||
      (fit.incomplete === true && !assessmentExecuted)
    ) {
      incomplete = true;
      blockers.push({ code: "structured_hotel_fit_never_executed" });
      reasons.push(
        "Structured hotel-fit assessment stage was never executed (incomplete — not an unknown fact)"
      );
    }
    // Honest provisional / insufficient_evidence / qualify_further after assessment,
    // and overall UNKNOWN with assessed dimensions (unknown room volume, etc.),
    // must NOT fail an otherwise qualified prospect.
    // Do not treat overallStatus === "UNKNOWN" alone as unfinished work.
  }

  const drawer = evaluateDrawerContentCompleteness(opp, opts);
  if (!drawer.ok) {
    incomplete = true;
    blockers.push({
      code: "drawer_content_incomplete",
      failed: drawer.failed || [],
    });
    reasons.push(
      `Drawer content contract failed: ${(drawer.failed || []).join(", ") || "incomplete"}`
    );
  }

  return {
    ok: blockers.length === 0 && !incomplete && !unknown,
    incomplete,
    unknown,
    blockers,
    reasons,
    eligibility: elig,
    drawer,
    fit: fit
      ? {
          present: true,
          version: fit.version || null,
          overallStatus: fit.overallStatus || fit.status || null,
        }
      : { present: false },
  };
}

/**
 * Derive publication-quality evidence from canonical persist / import outputs
 * (not caller-asserted flags alone).
 *
 * @param {object} persistEvidence — corporate persist report, import result, PE persist, change ledger
 * @returns {object} publication quality fields for completion input
 */
export function derivePublicationQualityFromPersistOutputs(persistEvidence = {}) {
  const {
    corporatePersist = null,
    importResult = null,
    pePersist = null,
    changeLedger = null,
    fitDoc = null,
    drawerDoc = null,
    claimed = {},
  } = persistEvidence;

  const corporateRows = []
    .concat(corporatePersist?.results || [])
    .concat(corporatePersist?.published || [])
    .filter(Boolean);
  const eligibilityWriteComplete =
    claimed.eligibilityWriteComplete === true ||
    (corporateRows.length > 0 &&
      corporateRows.every((r) => r.eligibilityWriteComplete !== false && r.ok !== false)) ||
    importResult?.eligibilityWriteComplete === true ||
    null;

  const drawerFromImport =
    importResult?.drawerCompleteness ||
    importResult?.results?.[0]?.drawerCompleteness ||
    null;
  const drawersComplete =
    claimed.drawersComplete === true
      ? true
      : drawerDoc?.ok === true ||
          drawerDoc?.summary?.allPass === true ||
          drawerDoc?.pass === true ||
          (drawerFromImport &&
            Number(drawerFromImport.drawerCompletenessFail || 0) === 0 &&
            Number(drawerFromImport.drawerCompletenessPass || 0) > 0)
        ? true
        : drawerFromImport &&
            Number(drawerFromImport.drawerCompletenessFail || 0) > 0
          ? false
          : claimed.drawersComplete === false
            ? false
            : null;

  const structuredHotelFitOk =
    claimed.structuredHotelFitOk === true
      ? true
      : fitDoc?.ok === true ||
          fitDoc?.summary?.structuredAssessmentsOk === true ||
          (Array.isArray(fitDoc?.assessments) && fitDoc.assessments.length > 0)
        ? true
        : claimed.structuredHotelFitOk === false
          ? false
          : null;

  const identityDedupeReconciled =
    claimed.identityDedupeReconciled === true
      ? true
      : changeLedger?.dedupeReconciled === true ||
          changeLedger?.identityReconciled === true ||
          importResult?.identityDedupeReconciled === true ||
          pePersist?.identityDedupeReconciled === true
        ? true
        : claimed.identityDedupeReconciled === false
          ? false
          : null;

  const newRecordCount =
    claimed.newRecordCount ??
    changeLedger?.createdCount ??
    importResult?.createdCount ??
    null;
  const newOpportunityCount =
    claimed.newOpportunityCount ??
    changeLedger?.netNewOpportunityCount ??
    importResult?.netNewOpportunityCount ??
    null;

  return {
    eligibilityWriteComplete,
    drawersComplete,
    drawerQualityOk: drawersComplete,
    structuredHotelFitOk,
    hotelFitAssessmentsPresent: structuredHotelFitOk,
    identityDedupeReconciled,
    newRecordCount,
    newOpportunityCount,
    postImportEligibilityRepairRequired:
      claimed.postImportEligibilityRepairRequired === true,
    derivedFromPersistOutputs: true,
    sources: {
      corporatePersist: Boolean(corporatePersist),
      importResult: Boolean(importResult),
      pePersist: Boolean(pePersist),
      changeLedger: Boolean(changeLedger),
      fitDoc: Boolean(fitDoc),
      drawerDoc: Boolean(drawerDoc),
    },
  };
}

/**
 * Evaluate publication-quality + identity gates for Full E2E COMPLETE.
 * Incomplete / unknown evidence fails closed when publication is claimed.
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
 * True when a segment row has supported independent assessment evidence —
 * not merely the absence of a flag admitting borrowed/proxy research.
 */
export function hasSupportedIndependentSegmentAssessment(s = {}) {
  if (s.independentAssessment === true) return true;
  if (s.independentAssessment === false) return false;

  const proxied =
    s.satisfiedByExhibitorProxy === true ||
    s.satisfiedByAssociationContacts === true ||
    s.satisfiedBySportsProxy === true ||
    s.satisfiedByMedicalProxy === true ||
    s.executionMode === "exhibitor_proxy" ||
    s.executionMode === "sports_proxy" ||
    s.executionMode === "association_contacts_proxy" ||
    s.executionMode === "medical_proxy";
  if (proxied) return false;

  // Supported evidence of dedicated research (not an anti-proxy silence).
  const dedicatedMode =
    s.executionMode === "fresh_research" ||
    s.executionMode === "bounded_live" ||
    s.executionMode === "paid_multisegment";
  const hasProviderRuns =
    Array.isArray(s.providerRunIds) && s.providerRunIds.length > 0;
  const scopeComplete =
    s.coverageStatus === "completed_for_declared_scope" ||
    s.researchedZeroYield === true;
  const depthOk = s.depthOk !== false && !isPlanOnlyLike(s);

  return Boolean(dedicatedMode && hasProviderRuns && scopeComplete && depthOk);
}

function isPlanOnlyLike(s = {}) {
  const mode = String(s.executionMode || "").toLowerCase();
  const cov = String(s.coverageStatus || "").toLowerCase();
  return (
    mode === "plan_only" ||
    cov === "plan_only" ||
    cov === "not_started" ||
    cov === "label_only"
  );
}

/**
 * Sports and educational axes must have supported independent assessment —
 * absence of an explicit proxy-admission flag is insufficient.
 */
export function evaluateIndependentSegmentAxes(applicable = [], { blockers, reasons }) {
  let incomplete = 0;
  for (const s of applicable) {
    const id = String(s.segmentId || "");
    const needsIndependent =
      id === "sports" ||
      id.includes("sport") ||
      id === "tours_educational_specialist" ||
      id.includes("educational") ||
      id.includes("tours_");
    if (!needsIndependent) continue;

    if (!hasSupportedIndependentSegmentAssessment(s)) {
      incomplete += 1;
      if (id.includes("sport")) {
        reasons.push(
          "Sports coverage requires a supported independent assessment — exhibitor/association proxy or silent non-admission is insufficient"
        );
      } else {
        reasons.push(
          "Educational/tour coverage requires a supported independent assessment — sports/medical/association proxy or silent non-admission is insufficient"
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
