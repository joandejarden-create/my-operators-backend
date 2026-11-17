/**
 * Reconstruct historical Full E2E execution observability from immutable reports.
 * Never invents call counts. Missing fields → UNKNOWN. No paid provider calls.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  createEmptyExecutionRun,
  recordProviderCall,
  recordApifyLifecycle,
  updateSegmentStatus,
  updateStageStatus,
  certifyExecutionRun,
  saveExecutionRun,
  SEGMENT_EXEC_STATUS,
  STAGE_STATUS,
  PROVIDER_CALL_STATUS,
  APIFY_LIFECYCLE,
  GDI_PROVIDER_IDS,
  E2E_STAGE,
} from "./gdi-execution-ledger-store-v1.js";
import { listFourteenWorkflowSegmentIds } from "../research/universal-contact-research-contract-v1.js";

export const GDI_HISTORICAL_EXECUTION_RECONCILIATION_V1 =
  "gdi-historical-execution-reconciliation-v1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

function readJsonSafe(p) {
  try {
    if (!fs.existsSync(p)) return null;
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return null;
  }
}

function exists(p) {
  return fs.existsSync(p);
}

const HOTEL_SPECS = Object.freeze([
  Object.freeze({
    hotelId: "recrPQcZg7SFARRb2",
    name: "YOTEL Geneva Lake",
    reportDirs: [
      "reports/gdi-yotel-geneva-lake-exa-30usd-scale-v1",
      "reports/gdi-yotel-geneva-lake-40plus-exa-35usd-pilot-v1",
      "reports/gdi-yotel-geneva-local-staged-preview-reconciliation-v1",
    ],
    runId: "hist_yotel_geneva_exa_30usd_scale_v1",
    label: "YOTEL Geneva Lake $30 Exa scale (historical reconciliation)",
  }),
  Object.freeze({
    hotelId: "recH9h3McgMNFQEla",
    name: "Radisson Blu Toronto Downtown",
    reportDirs: [
      "reports/gdi-radisson-toronto-six-missing-segments-exa-30usd-v1",
      "reports/gdi-radisson-toronto-14-segment-exa-expansion-20usd-v1",
      "reports/gdi-radisson-blu-toronto-15usd-full-e2e-v1",
    ],
    runId: "hist_radisson_toronto_exa_campaigns_v1",
    label: "Radisson Blu Toronto Exa campaigns (historical reconciliation)",
  }),
  Object.freeze({
    hotelId: "recLuxvwwxID7U2B8",
    name: "Bethesda Marriott",
    reportDirs: [
      "reports/gdi-full-e2e-hotel-template-v1",
      "reports/gdi-bethesda-v16-fourteen-workflow-incremental-pilot",
    ],
    runId: "hist_bethesda_full_e2e_template_v1",
    label: "Bethesda Marriott Full E2E template baseline (historical reconciliation)",
  }),
]);

function scanReportDir(dirAbs) {
  const out = {
    dir: dirAbs,
    present: exists(dirAbs),
    files: [],
    exaMentions: false,
    apifyMentions: false,
    spendUsd: null,
    segmentHints: [],
  };
  if (!out.present) return out;
  const walk = (d, depth = 0) => {
    if (depth > 2) return;
    for (const ent of fs.readdirSync(d, { withFileTypes: true })) {
      const p = path.join(d, ent.name);
      if (ent.isDirectory()) walk(p, depth + 1);
      else if (/\.(json|md|csv)$/i.test(ent.name)) {
        out.files.push(path.relative(dirAbs, p).replace(/\\/g, "/"));
        if (/exa/i.test(ent.name)) out.exaMentions = true;
        if (/apify/i.test(ent.name)) out.apifyMentions = true;
        if (/budget|spend|cost|closeout/i.test(ent.name) && ent.name.endsWith(".json")) {
          const j = readJsonSafe(p);
          const spend =
            j?.totalSpendUsd ??
            j?.spendUsd ??
            j?.cost?.totalUsd ??
            j?.budget?.spentUsd ??
            j?.exaSpendUsd ??
            null;
          if (spend != null && Number.isFinite(Number(spend))) {
            out.spendUsd = Number(spend);
          }
        }
      }
    }
  };
  walk(dirAbs);
  return out;
}

/**
 * Build a historical run ledger with UNKNOWN where evidence is missing.
 */
export function reconcileHistoricalHotel(spec, { root = ROOT, persist = false } = {}) {
  let run = createEmptyExecutionRun({
    hotelId: spec.hotelId,
    runId: spec.runId,
    label: spec.label,
    plannedSegmentIds: listFourteenWorkflowSegmentIds(),
  });
  run.historical = true;
  run.historicalConfidence = "PARTIAL_EVIDENCE";
  run.startedAt = null;
  run.completedAt = null;
  run.runStatus = "HISTORICAL";

  const scans = (spec.reportDirs || []).map((rel) =>
    scanReportDir(path.join(root, rel)),
  );
  const anyExa = scans.some((s) => s.exaMentions);
  const anyApify = scans.some((s) => s.apifyMentions);
  const spend = scans.map((s) => s.spendUsd).find((n) => n != null) ?? null;

  // Mark all segments PLANNED; evidence of per-segment Exa execution is UNKNOWN
  // unless report artifacts explicitly name them.
  for (const sid of listFourteenWorkflowSegmentIds()) {
    run = updateSegmentStatus(run, sid, {
      planned: true,
      status: SEGMENT_EXEC_STATUS.UNKNOWN,
      discoveryStatus: SEGMENT_EXEC_STATUS.UNKNOWN,
      deepResearchStatus: SEGMENT_EXEC_STATUS.UNKNOWN,
      errorOrSkipReason: "historical_segment_execution_not_fully_ledgered",
    });
  }

  if (anyExa) {
    // Cross-segment shared note — do not invent per-segment call counts.
    run = recordProviderCall(run, {
      providerId: GDI_PROVIDER_IDS.EXA_AGENT,
      segmentId: null,
      operation: "HISTORICAL_CAMPAIGN_AGGREGATE",
      status: PROVIDER_CALL_STATUS.COMPLETED,
      actualCostUsd: spend,
      providerRequestId: null,
      skipReason: null,
      providerDetails: {
        note: "Historical Exa usage evidenced by campaign reports; per-request segment attribution UNKNOWN prior to PR #333 observability.",
        reportDirs: scans.filter((s) => s.present).map((s) => s.dir),
      },
      correlationId: `${spec.runId}_exa_aggregate`,
    });
    // Fix CROSS_SEGMENT — recordProviderCall with null segment becomes CROSS_SEGMENT
  }

  if (anyApify) {
    run = recordApifyLifecycle(run, {
      state: APIFY_LIFECYCLE.APIFY_CONFIGURED,
      reason: "apify_mentioned_in_historical_reports",
    });
    run = recordApifyLifecycle(run, {
      state: APIFY_LIFECYCLE.APIFY_SKIPPED,
      reason:
        "no_durable_actor_job_ids_in_reconciled_artifacts — configured/mentioned ≠ executed",
    });
  } else {
    run = recordApifyLifecycle(run, {
      state: APIFY_LIFECYCLE.APIFY_CONFIGURED,
      reason: "actors_exist_in_registry",
    });
    run = recordApifyLifecycle(run, {
      state: APIFY_LIFECYCLE.APIFY_SKIPPED,
      reason: "no_apify_execution_evidence_in_historical_reports",
    });
  }

  // Stage timeline: only mark COMPLETED when report pack clearly exists
  const hasReports = scans.some((s) => s.present && s.files.length);
  for (const stageId of [
    E2E_STAGE.A_RESOLVE_HOTEL,
    E2E_STAGE.D_SEGMENT_SCOPE,
    E2E_STAGE.E_SUBSTANTIVE_RESEARCH,
    E2E_STAGE.L_REPORTS_LINKS,
  ]) {
    run = updateStageStatus(run, stageId, {
      status: hasReports ? STAGE_STATUS.COMPLETED : STAGE_STATUS.UNKNOWN,
      itemCounts: { reportPacksPresent: scans.filter((s) => s.present).length },
    });
  }
  for (const stage of run.stages) {
    if (stage.status === STAGE_STATUS.PENDING) {
      stage.status = STAGE_STATUS.UNKNOWN;
      stage.error = "historical_stage_status_unknown";
    }
  }

  run.certificate = certifyExecutionRun(run, {
    stagedPublicationGenerated: spec.hotelId === "recrPQcZg7SFARRb2",
    founderApprovalRequired: true,
    outputsIngested: hasReports,
    customerVisibilityGatesPassed: null,
  });
  // Historical runs are never FULL_E2E_EXECUTION_COMPLETE without full evidence
  if (run.certificate.verdict === "FULL_E2E_EXECUTION_COMPLETE") {
    run.certificate.verdict = "FULL_E2E_PARTIAL";
    run.certificate.note =
      "Historical reconciliation — incomplete per-segment/provider evidence forces PARTIAL.";
  }
  run.verdict = run.certificate.verdict;
  run.errors.push({
    code: "HISTORICAL_RECONCILIATION",
    message:
      "Per-request segment attribution and Apify job IDs may be UNKNOWN for pre-observability campaigns.",
    scans: scans.map((s) => ({
      dir: s.dir,
      present: s.present,
      fileCount: s.files.length,
      exaMentions: s.exaMentions,
      apifyMentions: s.apifyMentions,
      spendUsd: s.spendUsd,
    })),
  });

  if (persist) saveExecutionRun(run, root);
  return run;
}

export function reconcileAllHistoricalHotels(opts = {}) {
  return HOTEL_SPECS.map((spec) => ({
    hotelId: spec.hotelId,
    name: spec.name,
    run: reconcileHistoricalHotel(spec, opts),
  }));
}

export { HOTEL_SPECS };
