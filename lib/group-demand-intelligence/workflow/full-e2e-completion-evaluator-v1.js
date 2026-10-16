/**
 * Full GDI E2E completion evaluator V1.
 *
 * Derives overall status from evidence. Founder/Admin summaries must consume
 * this result — not independently written success claims.
 *
 * Association-only publication + plan-only applicable segments → PARTIAL (never COMPLETE).
 */

import {
  FULL_E2E_CONTRACT_VERSION,
  E2E_OVERALL_STATUS,
  E2E_STAGE,
  E2E_STAGE_DEFINITIONS,
  E2E_REQUIRED_DELIVERABLES,
  isForbiddenNotApplicableReason,
  isPlanOnlyExecution,
  SEGMENT_COVERAGE_STATUS,
  EXECUTION_MODE,
} from "./full-e2e-contract-v1.js";

/**
 * @typedef {object} SegmentCoverageInput
 * @property {string} segmentId
 * @property {string} applicability  applicable | not_applicable
 * @property {string} [applicabilityReason]
 * @property {string} [executionMode]  plan_only | reuse | fresh_research | ...
 * @property {string} [coverageStatus]  not_started | partial | completed_for_declared_scope | blocked | not_applicable
 * @property {string} [taskExecutionStatus]
 * @property {boolean} [researchedZeroYield]
 * @property {string[]} [depthFailures]  rule ids from DEPTH_INADEQUACY_RULES
 * @property {boolean} [reuseSatisfiesScope]
 * @property {object} [declaredScope]
 */

/**
 * Evaluate Full GDI E2E completion from run artifacts.
 *
 * @param {object} input
 * @returns {object} completion result
 */
export function evaluateFullE2eCompletion(input = {}) {
  const {
    runId = null,
    hotelId = null,
    runMode = "full_e2e",
    ledger = {},
    segmentCoverage = [],
    contactDepth = {},
    publication = {},
    localVerification = {},
    productionVerification = {},
    deliverables = [],
    budget = {},
    stageResults = null,
    weeklyRefreshCompleted = false,
    initialCoverageClaimedCompleteFromWeeklyOnly = false,
  } = input;

  const reasons = [];
  const blockers = [];
  const stageEval = [];

  // --- Segment coverage ---
  const applicable = (segmentCoverage || []).filter(
    (s) => String(s.applicability || "").toLowerCase() === "applicable"
  );
  const notApplicable = (segmentCoverage || []).filter(
    (s) => String(s.applicability || "").toLowerCase() === "not_applicable"
  );

  for (const s of notApplicable) {
    if (isForbiddenNotApplicableReason(s.applicabilityReason)) {
      blockers.push({
        code: "unsupported_not_applicable",
        segmentId: s.segmentId,
        reason: s.applicabilityReason,
      });
      reasons.push(
        `Segment ${s.segmentId}: NOT_APPLICABLE rationale forbidden (${s.applicabilityReason})`
      );
    }
    if (!s.applicabilityReason || !String(s.applicabilityReason).trim()) {
      blockers.push({
        code: "missing_not_applicable_rationale",
        segmentId: s.segmentId,
      });
      reasons.push(`Segment ${s.segmentId}: NOT_APPLICABLE missing rationale`);
    }
  }

  let applicablePlanOnly = 0;
  let applicableIncompleteCoverage = 0;
  let applicableComplete = 0;
  let applicableResearchedZero = 0;

  for (const s of applicable) {
    const mode = s.executionMode || s.mode || null;
    const cov = String(s.coverageStatus || "").toLowerCase();
    const depthFails = s.depthFailures || [];

    if (isPlanOnlyExecution(mode)) {
      applicablePlanOnly += 1;
      reasons.push(
        `Applicable segment ${s.segmentId} remains plan_only — not substantive research`
      );
      continue;
    }

    if (depthFails.length > 0 && !s.reuseSatisfiesScope) {
      applicableIncompleteCoverage += 1;
      reasons.push(
        `Segment ${s.segmentId}: depth inadequacy [${depthFails.join(", ")}]`
      );
      continue;
    }

    if (
      cov === SEGMENT_COVERAGE_STATUS.COMPLETED_FOR_DECLARED_SCOPE ||
      cov === "completed_for_declared_scope" ||
      cov === "researched"
    ) {
      if (s.researchedZeroYield) applicableResearchedZero += 1;
      applicableComplete += 1;
      continue;
    }

    if (s.reuseSatisfiesScope === true && s.declaredScope) {
      applicableComplete += 1;
      continue;
    }

    if (cov === "blocked" || cov === SEGMENT_COVERAGE_STATUS.NOT_STARTED || !cov) {
      applicableIncompleteCoverage += 1;
      reasons.push(
        `Applicable segment ${s.segmentId}: coverage=${cov || "missing"}`
      );
      continue;
    }

    // partial or other
    applicableIncompleteCoverage += 1;
    reasons.push(`Applicable segment ${s.segmentId}: coverage still ${cov}`);
  }

  // Association contacts ≠ planner coverage (explicit cross-check)
  const plannerAxis = applicable.find(
    (s) =>
      s.segmentId === "professional_planners_dmc" ||
      s.coverageAxis === "planner_intermediary_discovery"
  );
  if (plannerAxis) {
    const claimedViaAssoc =
      plannerAxis.satisfiedByAssociationContacts === true ||
      plannerAxis.executionMode === "association_contacts_proxy";
    if (claimedViaAssoc || isPlanOnlyExecution(plannerAxis.executionMode)) {
      applicablePlanOnly += isPlanOnlyExecution(plannerAxis.executionMode) ? 0 : 0;
      if (claimedViaAssoc) {
        applicableIncompleteCoverage += 1;
        reasons.push(
          "Named association contacts do not satisfy professional-planner discovery coverage"
        );
      }
    }
  }

  // --- Contact depth ---
  const contact = {
    required: contactDepth.required !== false,
    attempted: Number(contactDepth.attempted || 0),
    notAttempted: Number(contactDepth.notAttempted || 0),
    namedComplete: Number(contactDepth.namedComplete || 0),
    exhaustedOrgFallback: Number(contactDepth.exhaustedOrgFallback || 0),
    silentNotAttemptedToExhausted:
      contactDepth.silentNotAttemptedToExhausted === true,
    status: contactDepth.status || null,
  };
  if (contact.required) {
    if (contact.silentNotAttemptedToExhausted) {
      blockers.push({ code: "silent_not_attempted_to_exhausted" });
      reasons.push(
        "Contact depth: NOT_ATTEMPTED silently converted to exhausted fallback"
      );
    }
    if (contact.notAttempted > 0 && contact.status !== "skipped_no_candidates") {
      reasons.push(
        `Contact depth: ${contact.notAttempted} required queue items still NOT_ATTEMPTED`
      );
    }
  }

  // --- Publication ---
  const pub = {
    qualifiedPublished: publication.qualifiedPublished === true,
    associationOnly: publication.associationOnly === true,
    readbackOk: publication.readbackOk === true,
    backupsPresent: publication.backupsPresent === true,
    otherSegmentsPartial: publication.otherSegmentsPartial === true,
  };
  if (pub.associationOnly && applicablePlanOnly + applicableIncompleteCoverage > 0) {
    reasons.push(
      "Association-only publication while other applicable segments remain incomplete — overall cannot be COMPLETE"
    );
  }

  // --- Deliverables ---
  const deliverableMap = new Map(
    (deliverables || []).map((d) => [d.id, d])
  );
  for (const req of E2E_REQUIRED_DELIVERABLES) {
    const got = deliverableMap.get(req.id);
    if (!got) {
      if (req.allowGapIfUnsupported) {
        reasons.push(`Deliverable ${req.id}: missing — treat as implementation gap`);
        blockers.push({ code: "deliverable_gap_unrecorded", id: req.id });
      } else {
        blockers.push({ code: "deliverable_missing", id: req.id });
        reasons.push(`Required deliverable missing: ${req.id}`);
      }
      continue;
    }
    if (got.status === "gap" || got.status === "unsupported") {
      if (!got.gapRecorded) {
        blockers.push({ code: "deliverable_gap_unrecorded", id: req.id });
        reasons.push(`Deliverable ${req.id}: unsupported without concrete gap record`);
      }
      continue;
    }
    if (got.status !== "verified" && got.status !== "passed") {
      reasons.push(`Deliverable ${req.id}: status=${got.status || "unknown"}`);
    }
  }

  // Share link broken + Airtable write → not COMPLETE
  const share = deliverableMap.get("hotel_scoped_share_link");
  if (
    publication.airtableWriteOk === true &&
    share &&
    (share.status === "broken" || share.status === "failed")
  ) {
    blockers.push({ code: "airtable_ok_share_broken" });
    reasons.push(
      "Airtable write succeeded but external share link is broken — not COMPLETE"
    );
  }

  // --- Local / production verification ---
  const local = normalizeVerification(localVerification, "local");
  const prod = normalizeVerification(productionVerification, "production");

  if (local.status === "passed" && prod.status !== "passed") {
    reasons.push(
      "Local verification passed but production proof missing/failed — not COMPLETE"
    );
  }
  if (local.status === "asset_only_as_pass") {
    blockers.push({ code: "weak_local_substituted" });
    reasons.push(
      "Local browser check cannot be replaced by a weaker asset check and called passed"
    );
  }
  if (prod.status === "stale") {
    blockers.push({ code: "stale_production_verification" });
    reasons.push("Production verification is stale relative to current artifacts");
  }
  if (prod.status === "airtable_write_only") {
    blockers.push({ code: "airtable_write_only_as_production" });
    reasons.push("Airtable write alone does not satisfy production delivery");
  }
  if (local.status === "blocked" || prod.status === "blocked") {
    reasons.push("Required verification BLOCKED/NOT_RUN — cannot COMPLETE");
  }

  // --- Budget ---
  const budgetOk =
    budget.reconciled === true &&
    (budget.outstandingReservationsUsd == null ||
      Number(budget.outstandingReservationsUsd) === 0 ||
      budget.reservationsAccounted === true);
  if (budget.exhausted === true && (applicablePlanOnly > 0 || applicableIncompleteCoverage > 0)) {
    reasons.push(
      "Budget exhausted with incomplete applicable research — PARTIAL (preserve completed work)"
    );
  }
  if (budget.reconciled !== true) {
    reasons.push("Budget not reconciled");
  }

  // --- Stage evaluation (from ledger or explicit stageResults) ---
  const stages = stageResults || deriveStagesFromLedger(ledger);
  for (const def of E2E_STAGE_DEFINITIONS) {
    const hit = stages.find((s) => s.id === def.id || s.letter === def.letter);
    const status = hit?.status || "NOT_STARTED";
    stageEval.push({
      id: def.id,
      letter: def.letter,
      title: def.title,
      status,
      detail: hit?.detail || null,
    });
  }

  // --- Weekly refresh must not imply full initial COMPLETE ---
  if (weeklyRefreshCompleted && initialCoverageClaimedCompleteFromWeeklyOnly) {
    blockers.push({ code: "weekly_refresh_cannot_imply_full_e2e" });
    reasons.push(
      "Weekly refresh completion does not imply full initial market coverage COMPLETE"
    );
  }

  // --- Dry-run isolation ---
  if (ledger.dryRun === true && ledger.paidCallsAttempted === true) {
    blockers.push({ code: "dry_run_paid_violation" });
    reasons.push("Dry-run attempted paid calls");
  }
  if (ledger.dryRun === true && ledger.productionWritesAttempted === true) {
    blockers.push({ code: "dry_run_write_violation" });
    reasons.push("Dry-run attempted production writes");
  }

  // --- Overall status ---
  let overall = E2E_OVERALL_STATUS.NOT_STARTED;
  const hardFail = blockers.some((b) =>
    [
      "dry_run_paid_violation",
      "dry_run_write_violation",
      "unsupported_not_applicable",
    ].includes(b.code)
  );
  const hardBlock = blockers.some((b) =>
    [
      "airtable_ok_share_broken",
      "stale_production_verification",
      "airtable_write_only_as_production",
      "silent_not_attempted_to_exhausted",
      "deliverable_missing",
      "deliverable_gap_unrecorded",
      "weak_local_substituted",
      "weekly_refresh_cannot_imply_full_e2e",
    ].includes(b.code)
  );

  const anyRunning = stageEval.some((s) => s.status === "RUNNING");
  const anyFailed = stageEval.some((s) => s.status === "FAILED");
  const researchIncomplete =
    applicablePlanOnly > 0 || applicableIncompleteCoverage > 0;
  const contactIncomplete =
    contact.required &&
    contact.notAttempted > 0 &&
    contact.status !== "skipped_no_candidates";
  const deliveryIncomplete =
    local.status !== "passed" || prod.status !== "passed";
  const started =
    stageEval.some((s) => s.status !== "NOT_STARTED") ||
    (segmentCoverage || []).length > 0 ||
    Boolean(ledger.startedAt);

  if (hardFail || anyFailed) {
    overall = E2E_OVERALL_STATUS.FAILED;
  } else if (hardBlock || local.status === "blocked" || prod.status === "blocked") {
    overall = E2E_OVERALL_STATUS.BLOCKED;
  } else if (!started) {
    overall = E2E_OVERALL_STATUS.NOT_STARTED;
  } else if (anyRunning) {
    overall = E2E_OVERALL_STATUS.RUNNING;
  } else if (
    !researchIncomplete &&
    !contactIncomplete &&
    !deliveryIncomplete &&
    budgetOk &&
    blockers.length === 0 &&
    pub.readbackOk !== false &&
    !(pub.associationOnly && researchIncomplete)
  ) {
    // Final COMPLETE gate — all applicable segments complete (incl. researched-zero),
    // contact depth OK, local+prod passed, budget reconciled, no blockers.
    const allApplicableDone =
      applicable.length === 0 ||
      applicableComplete === applicable.length;
    if (allApplicableDone && applicablePlanOnly === 0) {
      overall = E2E_OVERALL_STATUS.COMPLETE;
    } else {
      overall = E2E_OVERALL_STATUS.PARTIAL;
      if (!allApplicableDone) {
        reasons.push("Not all applicable segments completed for declared scope");
      }
    }
  } else {
    overall = E2E_OVERALL_STATUS.PARTIAL;
  }

  // Explicit: association-only + incomplete others never COMPLETE
  if (
    overall === E2E_OVERALL_STATUS.COMPLETE &&
    (applicablePlanOnly > 0 ||
      applicableIncompleteCoverage > 0 ||
      (pub.associationOnly && researchIncomplete))
  ) {
    overall = E2E_OVERALL_STATUS.PARTIAL;
    reasons.push(
      "COMPLETE overridden to PARTIAL: applicable research still incomplete"
    );
  }

  return {
    version: FULL_E2E_CONTRACT_VERSION,
    runId,
    hotelId,
    runMode,
    overallStatus: overall,
    computedAt: new Date().toISOString(),
    distinctions: {
      taskExecution: "see stageEval / ledger.stages",
      researchCoverage: {
        applicableCount: applicable.length,
        applicableComplete,
        applicablePlanOnly,
        applicableIncompleteCoverage,
        applicableResearchedZeroYield: applicableResearchedZero,
      },
      publication: pub,
      customerDelivery: { local, production: prod },
    },
    segmentSummary: {
      applicable: applicable.length,
      notApplicable: notApplicable.length,
      planOnlyApplicable: applicablePlanOnly,
      incompleteCoverage: applicableIncompleteCoverage,
      completedForDeclaredScope: applicableComplete,
      researchedZeroYield: applicableResearchedZero,
    },
    contactDepth: contact,
    deliverables: E2E_REQUIRED_DELIVERABLES.map((r) => ({
      ...r,
      result: deliverableMap.get(r.id) || { status: "missing" },
    })),
    budget: {
      reconciled: budget.reconciled === true,
      exhausted: budget.exhausted === true,
      outstandingReservationsUsd: budget.outstandingReservationsUsd ?? null,
      estimatedIncurredUsd: budget.estimatedIncurredUsd ?? null,
      accountingHoldUsd: budget.accountingHoldUsd ?? null,
    },
    stageEval,
    blockers,
    reasons,
    canClaimComplete: overall === E2E_OVERALL_STATUS.COMPLETE,
    note:
      overall === E2E_OVERALL_STATUS.COMPLETE
        ? "All required stages passed or legitimately N/A; applicable research scopes met; contact depth, publication, reports/links, local and production verified; budget reconciled."
        : "Do not independently write COMPLETE in founder/Admin reports — consume overallStatus.",
  };
}

function normalizeVerification(v = {}, kind) {
  const status = String(v.status || "missing").toLowerCase();
  return {
    kind,
    status,
    verifiedAt: v.verifiedAt || null,
    buildSha: v.buildSha || null,
    dataRevision: v.dataRevision || null,
    runId: v.runId || null,
    notes: v.notes || null,
  };
}

function deriveStagesFromLedger(ledger = {}) {
  const stages = ledger.stages || {};
  const map = {
    validate_config: E2E_STAGE.A_RESOLVE_HOTEL,
    reuse_evidence: E2E_STAGE.C_FREEZE_INVENTORY,
    discover_gaps: E2E_STAGE.D_SEGMENT_SCOPE,
    paid_multisegment: E2E_STAGE.E_SUBSTANTIVE_RESEARCH,
    evaluate_eligibility: E2E_STAGE.F_QUALIFY_DEDUPE,
    buyer_contact_enrichment: E2E_STAGE.G_CONTACT_DEPTH,
    synthesize_hotel_relevance: E2E_STAGE.H_HOTEL_FIT,
    generate_cards: E2E_STAGE.I_CARDS_DRAWERS,
    prepare_manifest: E2E_STAGE.J_PUBLICATION_DIFF,
    apply_publish: E2E_STAGE.K_CANONICAL_PERSIST,
    local_delivery_verify: E2E_STAGE.M_LOCAL_VERIFY,
    production_delivery_verify: E2E_STAGE.N_PRODUCTION_VERIFY,
    reconcile_run: E2E_STAGE.O_RECONCILE,
    evaluate_completion: E2E_STAGE.O_RECONCILE,
  };
  const out = [];
  for (const [coord, e2eId] of Object.entries(map)) {
    const st = stages[coord];
    if (!st) continue;
    const status = String(st.status || "").toUpperCase();
    out.push({
      id: e2eId,
      status:
        status === "COMPLETED"
          ? "PASSED"
          : status === "FAILED"
            ? "FAILED"
            : status === "BLOCKED"
              ? "BLOCKED"
              : status === "RUNNING"
                ? "RUNNING"
                : status || "NOT_STARTED",
      detail: st,
    });
  }
  return out;
}

/**
 * Convenience: evaluate a fixture-shaped case used in regressions.
 */
export function evaluateFixtureCase(fixture = {}) {
  return evaluateFullE2eCompletion(fixture);
}
