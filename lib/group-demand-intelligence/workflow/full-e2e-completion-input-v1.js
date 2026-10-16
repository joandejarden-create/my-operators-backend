/**
 * Assemble Full GDI E2E completion-evaluator input from a run directory.
 * Prefers real execution artifacts over handwritten overlays.
 * Operator overlays cannot override failed machine verification.
 */

import fs from "node:fs";
import path from "node:path";
import {
  FULL_E2E_CONTRACT_VERSION,
  E2E_REQUIRED_DELIVERABLES,
  E2E_EXPECTED_SEGMENT_IDS,
  FULL_E2E_COORDINATOR_STAGES,
} from "./full-e2e-contract-v1.js";
import {
  reconcileAuthoritativeBudget,
  computeRunArtifactFingerprint,
} from "./full-e2e-verification-adapters-v1.js";
import {
  derivePublicationQualityFromPersistOutputs,
  hasSupportedIndependentSegmentAssessment,
} from "./full-e2e-publication-quality-v1.js";
import { buildLessonsCaptureArtifact } from "./full-e2e-lessons-capture-v1.js";

/**
 * Stamp independentAssessment from dedicated research evidence when absent.
 * Does not invent true for plan_only / proxy / silent rows.
 */
function withIndependentAssessmentStamp(row = {}) {
  if (row.independentAssessment === true || row.independentAssessment === false) {
    return row;
  }
  if (hasSupportedIndependentSegmentAssessment(row)) {
    return { ...row, independentAssessment: true };
  }
  return row;
}

function readJson(p) {
  if (!p || !fs.existsSync(p)) return null;
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return null;
  }
}

export function loadCompletionEvidenceOverlay(evidencePath) {
  return readJson(evidencePath) || {};
}

/**
 * Merge machine verification with operator overlay — machine failure wins.
 */
export function mergeVerification(machine, overlay) {
  if (!machine) return overlay || { status: "missing" };
  if (!overlay) return machine;
  const m = String(machine.machineStatus || machine.status || "").toLowerCase();
  const o = String(overlay.status || "").toLowerCase();
  if (
    ["failed", "blocked", "not_run", "stale"].includes(m) &&
    o === "passed"
  ) {
    return {
      ...overlay,
      ...machine,
      status: m,
      machineStatus: m,
      notes: `operator passed ignored; machine=${m}. ${overlay.notes || ""}`,
    };
  }
  return {
    ...machine,
    ...overlay,
    machineStatus: machine.machineStatus || machine.status,
    status: overlay.status || machine.status,
  };
}

/**
 * Build evaluator input from stage ledger + artifacts + optional evidence overlay.
 */
export function buildCompletionInputFromRun({
  ledger = {},
  runDir = null,
  overlay = {},
  runMode = "full_e2e",
  budgetLedgerPath = null,
  authorizationRef = null,
} = {}) {
  const evidencePath = runDir
    ? path.join(runDir, "full-e2e-evidence.json")
    : null;
  const diskOverlay = evidencePath
    ? loadCompletionEvidenceOverlay(evidencePath)
    : {};
  const ev = { ...diskOverlay, ...overlay };

  const segmentCoverage =
    deriveSegmentCoverageFromArtifacts(ledger, runDir, ev) ||
    ev.segmentCoverage ||
    ledger.fullE2e?.segmentCoverage ||
    [];

  const contactDepth = deriveContactDepthFromArtifacts(ledger, runDir, ev);

  const publication = {
    ...derivePublicationFromLedger(ledger, runDir, ev),
    ...(ledger.fullE2e?.publication || {}),
    ...(ev.publication || {}),
  };

  const machineLocal =
    ledger.fullE2e?.localVerificationMachine ||
    readJson(runDir && path.join(runDir, "full_e2e", "local-delivery-verify.json"))
      ?.verification ||
    null;
  const machineProd =
    ledger.fullE2e?.productionVerificationMachine ||
    readJson(
      runDir && path.join(runDir, "full_e2e", "production-delivery-verify.json")
    )?.verification ||
    null;

  const localVerification = mergeVerification(
    machineLocal || deriveVerification(ledger, "local_delivery_verify", "local"),
    ev.localVerification || ledger.fullE2e?.localVerification
  );
  const productionVerification = mergeVerification(
    machineProd ||
      deriveVerification(ledger, "production_delivery_verify", "production"),
    ev.productionVerification || ledger.fullE2e?.productionVerification
  );

  const deliverables =
    ev.deliverables ||
    ledger.fullE2e?.deliverables ||
    deriveDeliverables(ledger, runDir, ev);

  const budget = deriveBudget({
    ledger,
    runDir,
    ev,
    budgetLedgerPath:
      budgetLedgerPath ||
      ledger.budget?.ledgerPath ||
      process.env.GDI_PARALLEL_BUDGET_LEDGER_PATH ||
      null,
    authorizationRef:
      authorizationRef || ledger.fullE2e?.authorizationRef || null,
    hotelId: ledger.hotelId || ev.hotelId || null,
  });

  const artifactFingerprint = runDir
    ? computeRunArtifactFingerprint(runDir)
    : null;

  return {
    runId: ledger.runId || ev.runId || null,
    hotelId: ledger.hotelId || ev.hotelId || null,
    runMode: ev.runMode || ledger.fullE2e?.runMode || runMode,
    expectedSegmentIds: ev.expectedSegmentIds || [...E2E_EXPECTED_SEGMENT_IDS],
    ledger: {
      ...ledger,
      dryRun: ledger.dryRun === true,
      // Only explicit Full-E2E violation flags — do not infer from apply_publish
      // completed under a stale dryRun ledger bit (status-only must not invent
      // dry_run_write_violation from historical coordinator quirks).
      paidCallsAttempted: ledger.fullE2e?.paidCallsAttempted === true,
      productionWritesAttempted:
        ledger.fullE2e?.productionWritesAttempted === true,
      startedAt: ledger.startedAt || null,
    },
    segmentCoverage,
    contactDepth,
    publication,
    localVerification,
    productionVerification,
    deliverables,
    budget,
    currentFingerprints: {
      dataRevision:
        ev.currentDataRevision ||
        ledger.fullE2e?.dataRevision ||
        artifactFingerprint,
      buildSha: ev.currentBuildSha || ledger.fullE2e?.buildSha || null,
      artifactFingerprint,
    },
    weeklyRefreshCompleted:
      ev.weeklyRefreshCompleted === true ||
      ledger.stages?.weekly_refresh?.status === "completed",
    initialCoverageClaimedCompleteFromWeeklyOnly:
      ev.initialCoverageClaimedCompleteFromWeeklyOnly === true,
    operatorEvidence: ev.operatorEvidence || null,
    lessonsCapture: deriveLessonsCapture(ledger, runDir, ev),
    contractVersion: FULL_E2E_CONTRACT_VERSION,
  };
}

function deriveLessonsCapture(ledger, runDir, ev) {
  if (ev.lessonsCapture && typeof ev.lessonsCapture === "object") {
    return ev.lessonsCapture;
  }
  if (ledger.fullE2e?.lessonsCapture && typeof ledger.fullE2e.lessonsCapture === "object") {
    return ledger.fullE2e.lessonsCapture;
  }
  const fromDisk = readJson(
    runDir && path.join(runDir, "full_e2e", "lessons-capture-v1.json")
  );
  if (fromDisk) return fromDisk;
  // Auto-build from canonical register so status evaluation always has a
  // capture artifact; incorporatedThisRunIds come from overlay/ledger when present.
  return buildLessonsCaptureArtifact({
    runId: ledger.runId || ev.runId || null,
    hotelId: ledger.hotelId || ev.hotelId || null,
    releaseRef: ev.releaseRef || ledger.fullE2e?.releaseRef || null,
    incorporatedThisRunIds:
      ev.incorporatedLessonIds ||
      ledger.fullE2e?.incorporatedLessonIds ||
      [],
  });
}

function deriveSegmentCoverageFromArtifacts(ledger, runDir, ev) {
  if (Array.isArray(ev.segmentCoverage) && ev.segmentCoverage.length) {
    return ev.segmentCoverage;
  }
  const fromPaid = readJson(
    runDir && path.join(runDir, "paid_multisegment", "segment-coverage.json")
  );
  if (fromPaid?.segments) {
    return fromPaid.segments.map(withIndependentAssessmentStamp);
  }
  if (Array.isArray(fromPaid)) {
    return fromPaid.map(withIndependentAssessmentStamp);
  }

  const scorecard = readJson(
    runDir &&
      path.join(runDir, "paid_multisegment", "scorecard", "research-scorecard.json")
  );
  if (scorecard?.segments?.length) {
    return scorecard.segments.map((s) =>
      withIndependentAssessmentStamp({
        segmentId: s.segmentId || s.id,
        applicability: s.applicability || "applicable",
        applicabilityReason: s.applicabilityReason || s.rationale || null,
        executionMode: s.executionMode || s.freshVsReuse || "fresh_research",
        coverageStatus: s.coverageStatus || "partial",
        declaredScope: s.declaredScope || s.scope || null,
        scopeVersion: s.scopeVersion || null,
        providerRunIds: s.providerRunIds || [],
        sourceFreshness: s.sourceFreshness || s.sourceDate || null,
        candidateDispositions: s.candidateDispositions || null,
        depthAssessment: s.depthAssessment || null,
        depthFailures: s.depthFailures || [],
        stoppingReason: s.stoppingReason || null,
        researchedZeroYield: s.researchedZeroYield === true,
        executionArtifacts: s.briefRef || s.scorecardRef || true,
        independentAssessment: s.independentAssessment,
        satisfiedByExhibitorProxy: s.satisfiedByExhibitorProxy,
        satisfiedByAssociationContacts: s.satisfiedByAssociationContacts,
        satisfiedBySportsProxy: s.satisfiedBySportsProxy,
        satisfiedByMedicalProxy: s.satisfiedByMedicalProxy,
      })
    );
  }

  const plan = readJson(
    runDir &&
      path.join(
        runDir,
        "reuse_prior_intelligence",
        "segment-research-plan.json"
      )
  );
  if (plan?.segments?.length) {
    // Plan alone → plan_only / not_started (never invent completed)
    return plan.segments.map((s) => ({
      segmentId: s.segmentId || s.id,
      applicability: s.applicability || s.status || "unresolved",
      applicabilityReason: s.applicabilityReason || s.rationale || null,
      executionMode: "plan_only",
      coverageStatus: "not_started",
    }));
  }
  return null;
}

function deriveContactDepthFromArtifacts(ledger, runDir, ev) {
  if (ev.contactDepth && typeof ev.contactDepth === "object") {
    return ev.contactDepth;
  }
  const pass = readJson(
    runDir &&
      path.join(
        runDir,
        "buyer_contact_enrichment",
        "buyer-contact-enrichment-pass.json"
      )
  );
  const queueDoc = readJson(
    runDir &&
      path.join(
        runDir,
        "buyer_contact_enrichment",
        "buyer-contact-enrichment-queue.json"
      )
  );
  const stage = ledger.stages?.buyer_contact_enrichment;

  if (!pass && !queueDoc && !stage) {
    return {
      required: true,
      summaryMissing: true,
      notAttempted: null,
      attempted: null,
      status: null,
    };
  }

  const items = queueDoc?.items || pass?.queue?.items || [];
  const results = pass?.results || [];
  const dispositionsById = {};
  const attemptIds = [];
  const resultIds = [];

  for (const item of items) {
    const id = item.opportunityId || item.id;
    if (!id) continue;
    attemptIds.push(id);
    const r = results.find((x) => x.opportunityId === id);
    if (r) {
      resultIds.push(id);
      const axes = r.after?.axes || r.axes || {};
      const outcome = axes.contactDepthOutcome || r.contactDepthOutcome;
      if (outcome === "relevant_named_contact_with_channels_pending") {
        dispositionsById[id] = "further_enrichment_pending";
      } else if (axes.namedResearchStatus === "NAMED_RELEVANT_CONTACT_FOUND") {
        dispositionsById[id] = "named_relevant_contact_found";
      } else if (axes.publicationContactStatus === "USABLE_ROUTE") {
        dispositionsById[id] = "public_research_exhausted_org_fallback";
      } else {
        dispositionsById[id] = "further_enrichment_pending";
      }
    } else {
      dispositionsById[id] = "not_attempted";
    }
  }

  const notAttempted = Object.values(dispositionsById).filter(
    (d) => d === "not_attempted"
  ).length;

  return {
    required: true,
    attempted: resultIds.length,
    notAttempted,
    namedComplete: Object.values(dispositionsById).filter(
      (d) => d === "named_relevant_contact_found"
    ).length,
    exhaustedOrgFallback: Object.values(dispositionsById).filter(
      (d) => d === "public_research_exhausted_org_fallback"
    ).length,
    queue: items,
    requiredQueueIds: items.map((i) => i.opportunityId || i.id).filter(Boolean),
    dispositionsById,
    attemptIds,
    resultIds,
    queueVerifiedEmpty: items.length === 0 && Boolean(queueDoc || pass),
    silentNotAttemptedToExhausted: false,
    status:
      items.length === 0
        ? "empty_verified_queue"
        : notAttempted > 0
          ? "incomplete"
          : "completed",
  };
}

function derivePublicationFromLedger(ledger, runDir = null, ev = {}) {
  const pub = ledger.stages?.apply_publish;
  const fromEv = ev.publication && typeof ev.publication === "object" ? ev.publication : {};
  const fromLedger = ledger.fullE2e?.publication || {};
  const fitDoc = readJson(
    runDir && path.join(runDir, "independent_hotel_fit", "independent-hotel-fit.json")
  );
  const drawerDoc = readJson(
    runDir && path.join(runDir, "drawer_completeness", "drawer-completeness.json")
  );
  const changeLedger = readJson(
    runDir && path.join(runDir, "record-level-change-ledger.json")
  ) || readJson(
    runDir && path.join(runDir, "continuation", "record-level-change-ledger.json")
  );
  const corporatePersist =
    readJson(
      runDir && path.join(runDir, "corporate_persist", "corporate-persist-apply.json")
    ) ||
    readJson(
      runDir && path.join(runDir, "corporate_packages", "corporate-persist-apply.json")
    );
  const importResult =
    readJson(
      runDir && path.join(runDir, "production-apply", "import-result.json")
    ) ||
    readJson(
      runDir &&
        path.join(runDir, "production-apply", "import-result-apply.json")
    ) ||
    (() => {
      // Prefer newest import-result-apply-*.json under production-apply/
      if (!runDir) return null;
      const dir = path.join(runDir, "production-apply");
      try {
        if (!fs.existsSync(dir)) return null;
        const files = fs
          .readdirSync(dir)
          .filter((f) => /^import-result.*\.json$/i.test(f))
          .map((f) => ({
            f,
            m: fs.statSync(path.join(dir, f)).mtimeMs,
          }))
          .sort((a, b) => b.m - a.m);
        return files[0] ? readJson(path.join(dir, files[0].f)) : null;
      } catch {
        return null;
      }
    })();
  const pePersist = readJson(
    runDir && path.join(runDir, "pe_canonical_persist", "coordinator-pe-apply.json")
  );

  // Prefer evidence derived from canonical persist outputs over asserted flags.
  const derived = derivePublicationQualityFromPersistOutputs({
    corporatePersist,
    importResult,
    pePersist,
    changeLedger,
    fitDoc,
    drawerDoc,
    claimed: { ...fromLedger, ...fromEv },
  });

  const structuredHotelFitOk =
    derived.structuredHotelFitOk === true ||
    fromEv.structuredHotelFitOk === true ||
    fromLedger.structuredHotelFitOk === true;

  const drawersComplete =
    derived.drawersComplete === true ||
    fromEv.drawersComplete === true ||
    fromLedger.drawersComplete === true;

  const identityDedupeReconciled =
    derived.identityDedupeReconciled === true ||
    fromEv.identityDedupeReconciled === true ||
    fromLedger.identityDedupeReconciled === true;

  // eligibilityWriteComplete: false from persist wins; otherwise require positive evidence when published
  const eligibilityWriteComplete =
    derived.eligibilityWriteComplete === false ||
    fromEv.eligibilityWriteComplete === false ||
    fromLedger.eligibilityWriteComplete === false
      ? false
      : derived.eligibilityWriteComplete === true ||
          fromEv.eligibilityWriteComplete === true ||
          fromLedger.eligibilityWriteComplete === true
        ? true
        : fromEv.postImportEligibilityRepairRequired !== true &&
          fromLedger.postImportEligibilityRepairRequired !== true
          ? null
          : false;

  return {
    qualifiedPublished: pub?.status === "completed" || fromEv.qualifiedPublished === true,
    associationOnly:
      fromEv.associationOnly === true || fromLedger.associationOnly === true,
    readbackOk:
      fromEv.readbackOk === true || fromLedger.readbackOk === true,
    backupsPresent:
      ledger.stages?.prepare_manifest?.status === "completed" ||
      fromEv.backupsPresent === true ||
      fromLedger.backupsPresent === true,
    airtableWriteOk: pub?.status === "completed" && pub?.dryRun !== true,
    otherSegmentsPartial:
      fromEv.otherSegmentsPartial === true ||
      fromLedger.otherSegmentsPartial === true,
    zeroQualifiedEmptyStateOk:
      fromEv.zeroQualifiedEmptyStateOk === true ||
      fromLedger.zeroQualifiedEmptyStateOk === true,
    structuredHotelFitOk:
      structuredHotelFitOk === true
        ? true
        : derived.structuredHotelFitOk === false ||
            fromEv.structuredHotelFitOk === false
          ? false
          : structuredHotelFitOk,
    hotelFitAssessmentsPresent:
      structuredHotelFitOk === true
        ? true
        : derived.structuredHotelFitOk === false
          ? false
          : structuredHotelFitOk,
    drawersComplete:
      drawersComplete === true
        ? true
        : derived.drawersComplete === false || fromEv.drawersComplete === false
          ? false
          : drawersComplete,
    drawerQualityOk:
      drawersComplete === true
        ? true
        : derived.drawersComplete === false
          ? false
          : drawersComplete,
    eligibilityWriteComplete,
    postImportEligibilityRepairRequired:
      fromEv.postImportEligibilityRepairRequired === true ||
      fromLedger.postImportEligibilityRepairRequired === true,
    identityDedupeReconciled:
      identityDedupeReconciled === true
        ? true
        : derived.identityDedupeReconciled === false ||
            fromEv.identityDedupeReconciled === false
          ? false
          : identityDedupeReconciled,
    newRecordCount:
      derived.newRecordCount ??
      fromEv.newRecordCount ??
      fromLedger.newRecordCount ??
      null,
    newOpportunityCount:
      derived.newOpportunityCount ??
      fromEv.newOpportunityCount ??
      fromLedger.newOpportunityCount ??
      null,
    derivedFromPersistOutputs: derived.derivedFromPersistOutputs === true,
    persistEvidenceSources: derived.sources || null,
  };
}

function deriveVerification(ledger, stageId, kind) {
  const st = ledger.stages?.[stageId];
  if (!st) return { status: "missing", kind };
  const status = String(st.status || "").toLowerCase();
  const vs = String(st.verificationStatus || "").toLowerCase();
  if (status === "completed" || status === "passed") {
    // completed with not_run ≠ passed
    if (vs && vs !== "passed" && vs !== "verified") {
      return {
        status: vs || "not_run",
        machineStatus: vs || "not_run",
        verifiedAt: st.at || null,
        buildSha: st.buildSha || null,
        dataRevision: st.dataRevision || null,
        runId: ledger.runId || null,
        hotelId: ledger.hotelId || null,
        notes: st.note || null,
      };
    }
    return {
      status: vs || "passed",
      machineStatus: vs || "passed",
      verifiedAt: st.at || null,
      buildSha: st.buildSha || null,
      dataRevision: st.dataRevision || null,
      runId: ledger.runId || null,
      hotelId: ledger.hotelId || null,
      notes: st.note || null,
    };
  }
  if (status === "blocked") {
    return { status: "blocked", machineStatus: "blocked", notes: st.reason || st.note || null };
  }
  if (status === "failed") {
    return { status: "failed", machineStatus: "failed", notes: st.error || st.note || null };
  }
  return { status: status || "missing", notes: st.note || null };
}

function deriveDeliverables(ledger, runDir, ev) {
  if (Array.isArray(ev.deliverables) && ev.deliverables.length) {
    return ev.deliverables;
  }
  return E2E_REQUIRED_DELIVERABLES.map((d) => ({
    id: d.id,
    status: "missing",
    gapRecorded: false,
  }));
}

function deriveBudget({ ledger, runDir, ev, budgetLedgerPath, authorizationRef, hotelId }) {
  if (ev.budget && ev.budget.authoritative === true) {
    return ev.budget;
  }
  const reconcileFile = readJson(
    runDir && path.join(runDir, "full_e2e", "reconcile-run.json")
  );
  if (reconcileFile?.budget?.authoritative === true) {
    return reconcileFile.budget;
  }

  const pathGuess =
    budgetLedgerPath ||
    reconcileFile?.ledgerPath ||
    ledger.budget?.ledgerPath ||
    null;

  if (pathGuess || authorizationRef) {
    const auth = reconcileAuthoritativeBudget({
      ledgerPath: pathGuess,
      authorizationId: authorizationRef,
      hotelId,
    });
    if (auth.authoritative) return auth;
  }

  // Copied non-authoritative fields — evaluator will reject reconciled:true without authoritative
  return {
    reconciled: ledger.budget?.reconciled === true,
    authoritative: false,
    exhausted: ledger.budget?.exhausted === true,
    outstandingReservationsUsd:
      ledger.budget?.outstandingReservationsUsd ?? null,
    estimatedIncurredUsd: ledger.budget?.usedUsd ?? null,
    accountingHoldUsd: ledger.budget?.accountingHoldUsd ?? null,
    reservationsAccounted: ledger.budget?.reservationsAccounted === true,
    authorizationId: authorizationRef || null,
  };
}

export function writeFullE2eArtifacts({
  outDir,
  contractDoc,
  completion,
  inputSnapshot = null,
}) {
  fs.mkdirSync(outDir, { recursive: true });
  const contractPath = path.join(outDir, "full-e2e-contract.json");
  const completionPath = path.join(outDir, "full-e2e-completion.json");
  fs.writeFileSync(contractPath, JSON.stringify(contractDoc, null, 2) + "\n");
  fs.writeFileSync(completionPath, JSON.stringify(completion, null, 2) + "\n");
  if (inputSnapshot) {
    fs.writeFileSync(
      path.join(outDir, "full-e2e-completion-input.json"),
      JSON.stringify(inputSnapshot, null, 2) + "\n"
    );
  }
  const lessonsCapture =
    inputSnapshot?.lessonsCapture || completion?.lessonsCapture || null;
  let lessonsCapturePath = null;
  if (lessonsCapture) {
    lessonsCapturePath = path.join(outDir, "lessons-capture-v1.json");
    fs.writeFileSync(
      lessonsCapturePath,
      JSON.stringify(lessonsCapture, null, 2) + "\n"
    );
  }
  return { contractPath, completionPath, lessonsCapturePath };
}

export function listFullE2eCoordinatorStages() {
  return [...FULL_E2E_COORDINATOR_STAGES];
}
