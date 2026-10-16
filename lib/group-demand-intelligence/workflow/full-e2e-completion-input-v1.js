/**
 * Assemble Full GDI E2E completion-evaluator input from a run directory
 * and optional operator evidence file. Read-only — safe against active runs.
 */

import fs from "node:fs";
import path from "node:path";
import {
  FULL_E2E_CONTRACT_VERSION,
  E2E_REQUIRED_DELIVERABLES,
  FULL_E2E_COORDINATOR_STAGES,
} from "./full-e2e-contract-v1.js";

function readJson(p) {
  if (!p || !fs.existsSync(p)) return null;
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return null;
  }
}

/**
 * Load operator-supplied evidence overlay (segmentCoverage, verifications, etc.).
 * Path may be runDir/full-e2e-evidence.json or --completion-evidence.
 */
export function loadCompletionEvidenceOverlay(evidencePath) {
  return readJson(evidencePath) || {};
}

/**
 * Build evaluator input from stage ledger + optional evidence overlay.
 *
 * Does not mutate the ledger or resubmit research.
 *
 * @param {object} opts
 * @param {object} opts.ledger  stage-completion-ledger.json
 * @param {string} [opts.runDir]
 * @param {object} [opts.overlay]  full-e2e-evidence.json contents
 * @param {string} [opts.runMode]
 */
export function buildCompletionInputFromRun({
  ledger = {},
  runDir = null,
  overlay = {},
  runMode = "full_e2e",
} = {}) {
  const evidencePath = runDir
    ? path.join(runDir, "full-e2e-evidence.json")
    : null;
  const diskOverlay = evidencePath ? loadCompletionEvidenceOverlay(evidencePath) : {};
  const ev = { ...diskOverlay, ...overlay };

  const segmentCoverage =
    ev.segmentCoverage ||
    ledger.fullE2e?.segmentCoverage ||
    deriveSegmentCoverageFromLedger(ledger, runDir);

  const contactDepth =
    ev.contactDepth ||
    ledger.fullE2e?.contactDepth ||
    deriveContactDepthFromLedger(ledger, runDir);

  const publication =
    ev.publication ||
    ledger.fullE2e?.publication ||
    derivePublicationFromLedger(ledger);

  const localVerification =
    ev.localVerification ||
    ledger.fullE2e?.localVerification ||
    deriveVerification(ledger, "local_delivery_verify", "local");

  const productionVerification =
    ev.productionVerification ||
    ledger.fullE2e?.productionVerification ||
    deriveVerification(ledger, "production_delivery_verify", "production");

  const deliverables =
    ev.deliverables ||
    ledger.fullE2e?.deliverables ||
    deriveDeliverables(ledger, runDir, ev);

  const budget =
    ev.budget ||
    ledger.fullE2e?.budget ||
    {
      reconciled: ledger.budget?.reconciled === true,
      exhausted: ledger.budget?.exhausted === true,
      outstandingReservationsUsd:
        ledger.budget?.outstandingReservationsUsd ?? null,
      estimatedIncurredUsd: ledger.budget?.usedUsd ?? null,
      accountingHoldUsd: ledger.budget?.accountingHoldUsd ?? null,
      reservationsAccounted: ledger.budget?.reservationsAccounted === true,
    };

  return {
    runId: ledger.runId || ev.runId || null,
    hotelId: ledger.hotelId || ev.hotelId || null,
    runMode: ev.runMode || ledger.fullE2e?.runMode || runMode,
    ledger: {
      ...ledger,
      dryRun: ledger.dryRun === true,
      paidCallsAttempted: ledger.fullE2e?.paidCallsAttempted === true,
      productionWritesAttempted:
        ledger.fullE2e?.productionWritesAttempted === true ||
        ledger.stages?.apply_publish?.status === "completed",
      startedAt: ledger.startedAt || null,
    },
    segmentCoverage,
    contactDepth,
    publication,
    localVerification,
    productionVerification,
    deliverables,
    budget,
    weeklyRefreshCompleted:
      ev.weeklyRefreshCompleted === true ||
      ledger.stages?.weekly_refresh?.status === "completed",
    initialCoverageClaimedCompleteFromWeeklyOnly:
      ev.initialCoverageClaimedCompleteFromWeeklyOnly === true,
    contractVersion: FULL_E2E_CONTRACT_VERSION,
  };
}

function deriveSegmentCoverageFromLedger(ledger, runDir) {
  const fromPaid = readJson(
    runDir && path.join(runDir, "paid_multisegment", "segment-coverage.json")
  );
  if (fromPaid?.segments) return fromPaid.segments;
  if (Array.isArray(fromPaid)) return fromPaid;

  const plan = readJson(
    runDir &&
      path.join(
        runDir,
        "reuse_prior_intelligence",
        "segment-research-plan.json"
      )
  );
  if (plan?.segments?.length) {
    return plan.segments.map((s) => ({
      segmentId: s.segmentId || s.id,
      applicability: s.applicability || s.status || "applicable",
      applicabilityReason: s.applicabilityReason || s.rationale || null,
      executionMode:
        s.executionMode ||
        (ledger.stages?.paid_multisegment?.status === "completed"
          ? "fresh_research"
          : "plan_only"),
      coverageStatus:
        s.coverageStatus ||
        (ledger.stages?.paid_multisegment?.status === "completed"
          ? "partial"
          : "not_started"),
    }));
  }
  return [];
}

function deriveContactDepthFromLedger(ledger, runDir) {
  const pass = readJson(
    runDir &&
      path.join(
        runDir,
        "buyer_contact_enrichment",
        "buyer-contact-enrichment-pass.json"
      )
  );
  const stage = ledger.stages?.buyer_contact_enrichment;
  if (!pass && !stage) {
    return { required: true, notAttempted: 1, attempted: 0, status: null };
  }
  const summary = pass?.summary || pass?.totals || {};
  return {
    required: true,
    attempted: Number(summary.attempted || summary.processed || 0),
    notAttempted: Number(summary.notAttempted || summary.not_attempted || 0),
    namedComplete: Number(
      summary.namedComplete || summary.named_relevant_contact_found || 0
    ),
    exhaustedOrgFallback: Number(
      summary.exhaustedOrgFallback || summary.exhausted_org_fallback || 0
    ),
    silentNotAttemptedToExhausted:
      summary.silentNotAttemptedToExhausted === true,
    status: stage?.status === "completed" ? "completed" : stage?.status || null,
  };
}

function derivePublicationFromLedger(ledger) {
  const pub = ledger.stages?.apply_publish;
  return {
    qualifiedPublished: pub?.status === "completed",
    associationOnly: ledger.fullE2e?.publication?.associationOnly === true,
    readbackOk: ledger.fullE2e?.publication?.readbackOk === true,
    backupsPresent:
      ledger.stages?.prepare_manifest?.status === "completed" ||
      ledger.fullE2e?.publication?.backupsPresent === true,
    airtableWriteOk: pub?.status === "completed",
    otherSegmentsPartial:
      ledger.fullE2e?.publication?.otherSegmentsPartial === true,
  };
}

function deriveVerification(ledger, stageId, kind) {
  const st = ledger.stages?.[stageId];
  if (!st) return { status: "missing", kind };
  const status = String(st.status || "").toLowerCase();
  if (status === "completed" || status === "passed") {
    return {
      status: st.verificationStatus || "passed",
      verifiedAt: st.at || null,
      buildSha: st.buildSha || null,
      dataRevision: st.dataRevision || null,
      runId: ledger.runId || null,
      notes: st.note || null,
    };
  }
  if (status === "blocked") {
    return { status: "blocked", notes: st.reason || st.note || null };
  }
  if (status === "failed") {
    return { status: "failed", notes: st.error || st.note || null };
  }
  return { status: status || "missing", notes: st.note || null };
}

function deriveDeliverables(ledger, runDir, ev) {
  if (Array.isArray(ev.deliverables) && ev.deliverables.length) {
    return ev.deliverables;
  }
  // Default: mark as missing so evaluator fails closed until operator evidence recorded
  return E2E_REQUIRED_DELIVERABLES.map((d) => ({
    id: d.id,
    status: "missing",
    gapRecorded: false,
  }));
}

/**
 * Write contract document + completion result into a run or reports pack.
 */
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
  return { contractPath, completionPath };
}

export function listFullE2eCoordinatorStages() {
  return [...FULL_E2E_COORDINATOR_STAGES];
}
