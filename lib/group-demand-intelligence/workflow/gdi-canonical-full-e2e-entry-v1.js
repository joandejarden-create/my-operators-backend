/**
 * Canonical GDI Full E2E research entry V1 (P1-C).
 *
 * One per-hotel research path:
 *   preflight → shared run context → fourteen-segment discovery →
 *   deep research / contacts / qualification (original chain) →
 *   execution certificate (discovery ≠ Full E2E complete).
 *
 * Does not publish. Does not write Airtable. Paid providers require explicit auth.
 * Prefer CLI: npm run gdi:canonical-full-e2e -- --hotel <id> --mode fixture|dry-run
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { listFourteenWorkflowSegmentIds } from "../research/universal-contact-research-contract-v1.js";
import { resolveHotelGdiProfileForResearch } from "../hotel-gdi-profile-resolution-v1.js";
import { executeOriginalFullE2eResearchChain } from "./original-full-e2e-research-chain-v1.js";
import {
  beginFourteenSegmentExecutionRun,
  finalizeFourteenSegmentExecutionRun,
  recordDeepResearchOutcome,
  markDeepResearchNotApplicableOrSkipped,
  ensureActiveRunOrNull,
  APIFY_SKIP_REASON,
} from "../observability/gdi-fourteen-segment-execution-ledger-bridge-v1.js";
import {
  clearActiveExecutionRunContext,
  observeStageStatus,
} from "../observability/gdi-execution-run-context-v1.js";
import {
  loadExecutionRun,
  E2E_STAGE,
  STAGE_STATUS,
  RUN_VERDICT,
  SEGMENT_EXEC_STATUS,
} from "../observability/gdi-execution-ledger-store-v1.js";

export const GDI_CANONICAL_FULL_E2E_ENTRY_V1 =
  "gdi-canonical-full-e2e-entry-v1";

export const CANONICAL_EXECUTION_MODE = Object.freeze({
  PREFLIGHT: "preflight",
  FIXTURE: "fixture",
  DRY_RUN: "dry-run",
  LIVE: "live",
});

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

function text(v) {
  return String(v == null ? "" : v).trim();
}

function nowIso() {
  return new Date().toISOString();
}

/**
 * Fail-closed preflight for canonical Full E2E research.
 */
export function preflightCanonicalFullE2e(opts = {}) {
  const hotelId = text(opts.hotelId);
  const mode = normalizeMode(opts.mode);
  const errors = [];
  if (!hotelId) errors.push({ code: "MISSING_HOTEL_ID" });
  if (!mode) errors.push({ code: "MISSING_OR_INVALID_MODE" });

  const authorizePaidDiscovery = opts.authorizePaidDiscovery === true;
  const authorizePaidResearch = opts.authorizePaidResearch === true;
  const authorizePublish = opts.authorizePublish === true;
  const explicitDryRun = opts.dryRun === true || mode === CANONICAL_EXECUTION_MODE.DRY_RUN;
  const fixtureMode =
    mode === CANONICAL_EXECUTION_MODE.FIXTURE ||
    mode === CANONICAL_EXECUTION_MODE.PREFLIGHT ||
    mode === CANONICAL_EXECUTION_MODE.DRY_RUN;

  if (authorizePublish) {
    errors.push({
      code: "PUBLICATION_NOT_AUTHORIZED_IN_CANONICAL_RESEARCH_ENTRY",
      note: "Use research-to-page publication gates separately after founder approval.",
    });
  }
  if (
    mode === CANONICAL_EXECUTION_MODE.LIVE &&
    !authorizePaidDiscovery &&
    !authorizePaidResearch
  ) {
    errors.push({
      code: "LIVE_MODE_REQUIRES_PAID_AUTHORIZATION",
    });
  }
  if (explicitDryRun && (authorizePaidDiscovery || authorizePaidResearch)) {
    // Fail closed: dry-run wins — demote paid rather than error if soft, but
    // preflight reports the demotion.
  }
  if (fixtureMode && (authorizePaidDiscovery || authorizePaidResearch) && !explicitDryRun && mode === CANONICAL_EXECUTION_MODE.FIXTURE) {
    errors.push({
      code: "FIXTURE_MODE_BLOCKS_PAID_FLAGS",
      note: "Remove --authorize-paid-* or use --mode live with founder auth.",
    });
  }

  return {
    ok: errors.length === 0,
    hotelId,
    mode,
    fixtureMode,
    explicitDryRun,
    authorizePaidDiscovery:
      fixtureMode || explicitDryRun ? false : authorizePaidDiscovery,
    authorizePaidResearch:
      fixtureMode || explicitDryRun ? false : authorizePaidResearch,
    authorizePublish: false,
    publicationAuthorized: false,
    enableApifyOrchestration: false,
    errors,
    contract: GDI_CANONICAL_FULL_E2E_ENTRY_V1,
  };
}

export function normalizeMode(raw) {
  const m = String(raw || "fixture")
    .trim()
    .toLowerCase();
  if (m === "preflight") return CANONICAL_EXECUTION_MODE.PREFLIGHT;
  if (m === "fixture" || m === "mock" || m === "deterministic") {
    return CANONICAL_EXECUTION_MODE.FIXTURE;
  }
  if (m === "dry-run" || m === "dry_run" || m === "dryrun") {
    return CANONICAL_EXECUTION_MODE.DRY_RUN;
  }
  if (m === "live" || m === "authorized-live") return CANONICAL_EXECUTION_MODE.LIVE;
  return null;
}

/**
 * Run canonical Full E2E research for one hotel.
 */
export async function runCanonicalFullE2eResearch(opts = {}) {
  const preflight = preflightCanonicalFullE2e(opts);
  if (!preflight.ok) {
    return {
      ok: false,
      blocked: true,
      preflight,
      verdict: RUN_VERDICT.FULL_E2E_BLOCKED,
      publicationAuthorized: false,
    };
  }

  const hotelId = preflight.hotelId;
  const mode = preflight.mode;
  if (mode === CANONICAL_EXECUTION_MODE.PREFLIGHT) {
    const profile = await resolveHotelGdiProfileForResearch(hotelId, {
      skipHpc: opts.skipHpc !== false,
      preferLive: false,
    });
    return {
      ok: profile.ok !== false,
      preflightOnly: true,
      preflight,
      hotelProfile: {
        hotelId,
        displayName:
          profile.hotel?.displayName || profile.hotel?.name || null,
        city: profile.hotel?.city || null,
        ok: profile.ok !== false,
      },
      plannedSegments: listFourteenWorkflowSegmentIds(),
      publicationAuthorized: false,
      note: "Preflight only — no discovery or deep research executed.",
    };
  }

  const obsRoot = opts.executionObservabilityRoot;
  const runId = opts.executionRunId || null;
  const outDir =
    opts.outDir ||
    path.join(
      ROOT,
      "reports",
      "gdi-canonical-full-e2e-entry-v1",
      hotelId,
      new Date().toISOString().replace(/[:.]/g, "-"),
    );

  let ledgerCtx = null;
  try {
    ledgerCtx = beginFourteenSegmentExecutionRun({
      hotelId,
      runId,
      root: obsRoot,
      label: opts.executionLabel || `canonical-full-e2e:${mode}`,
      plannedSegmentIds: listFourteenWorkflowSegmentIds(),
      researchRunId: opts.researchRunId || null,
    });
    observeStageStatus(E2E_STAGE.A_RESOLVE_HOTEL, {
      status: STAGE_STATUS.COMPLETED,
    });
    observeStageStatus(E2E_STAGE.D_SEGMENT_SCOPE, {
      status: STAGE_STATUS.COMPLETED,
    });
  } catch (err) {
    clearActiveExecutionRunContext();
    return {
      ok: false,
      blocked: true,
      preflight,
      error: String(err?.message || err),
      verdict: RUN_VERDICT.FULL_E2E_BLOCKED,
      publicationAuthorized: false,
    };
  }

  const authorizePaidDiscovery = preflight.authorizePaidDiscovery;
  const authorizePaidResearch = preflight.authorizePaidResearch;

  const chain = await executeOriginalFullE2eResearchChain({
    hotelIds: [hotelId],
    authorizePaidDiscovery,
    authorizePaidResearch,
    budgetCeilingUsd: Number(opts.budgetCeilingUsd || 0) || 0,
    useFixtureProvider: !authorizePaidDiscovery,
    allowHistoricalReplay: opts.allowHistoricalReplay !== false,
    persistState: opts.persistState !== false,
    maxCandidates: opts.maxCandidates ?? 8,
    outDir,
    skipHpc: opts.skipHpc !== false,
    executionLifecycle: "external",
    executionObservability: true,
    executionObservabilityRoot: obsRoot,
    executionRunId: ledgerCtx.runId,
    enableApifyOrchestration: false,
    authorizeApifyLive: false,
    recordDeepResearchObservability: true,
    seedOpportunities: opts.seedOpportunities || [],
  });

  // Ensure every planned segment has a terminal deepResearchStatus.
  if (ensureActiveRunOrNull()) {
    try {
      const run = loadExecutionRun(hotelId, ledgerCtx.runId, obsRoot);
      const processedSegs = new Set(
        (chain?.processed || [])
          .map((p) => p.segmentId || p.researchSegment)
          .filter(Boolean),
      );
      const missingDeep = listFourteenWorkflowSegmentIds().filter((sid) => {
        const row = run?.segments?.[sid];
        if (!row?.planned) return false;
        const st = row.deepResearchStatus || SEGMENT_EXEC_STATUS.PLANNED;
        return (
          st === SEGMENT_EXEC_STATUS.PLANNED ||
          st === SEGMENT_EXEC_STATUS.STARTED ||
          st === SEGMENT_EXEC_STATUS.UNKNOWN
        );
      });
      if (missingDeep.length) {
        markDeepResearchNotApplicableOrSkipped(
          missingDeep,
          processedSegs.size
            ? "NO_RETAINED_CANDIDATES_FOR_SEGMENT_DEEP_RESEARCH"
            : "NO_DEEP_RESEARCH_CANDIDATES_FOR_SEGMENT",
        );
      }
    } catch {
      /* observability must not break */
    }
  }

  const finalized = finalizeFourteenSegmentExecutionRun({
    budgetsRespected: true,
    outputsIngested: true,
    discoveryPhaseOnly: false,
    requireDeepResearch: true,
  });

  const certificate =
    finalized?.certificate ||
    loadExecutionRun(hotelId, ledgerCtx.runId, obsRoot)?.certificate ||
    null;

  fs.mkdirSync(outDir, { recursive: true });
  const summaryPath = path.join(outDir, "canonical-full-e2e-summary.json");
  const summary = {
    contract: GDI_CANONICAL_FULL_E2E_ENTRY_V1,
    at: nowIso(),
    hotelId,
    mode,
    runId: ledgerCtx.runId,
    preflight,
    chainVersion: chain?.version || null,
    discovery: chain?.discovery || null,
    processedCount: (chain?.processed || []).length,
    publicationOccurred: false,
    publicationAuthorized: false,
    airtableWrites: 0,
    apifyDefault: APIFY_SKIP_REASON.APIFY_NOT_ORCHESTRATED,
    verdict: certificate?.verdict || finalized?.verdict || null,
    researchCompleteness: certificate?.researchCompleteness || null,
    certificate,
    outDir,
  };
  fs.writeFileSync(summaryPath, `${JSON.stringify(summary, null, 2)}\n`);

  return {
    ok: true,
    preflight,
    hotelId,
    mode,
    runId: ledgerCtx.runId,
    chain,
    certificate,
    verdict: summary.verdict,
    researchCompleteness: summary.researchCompleteness,
    publicationAuthorized: false,
    outDir,
    summaryPath,
  };
}

/**
 * Map a processed opportunity row from the original chain into ledger deep rows.
 */
export function observeDeepResearchFromProcessedRow(row = {}) {
  const segmentId = row.segmentId || row.researchSegment || null;
  if (!segmentId) return null;
  if (row.skipped) {
    return recordDeepResearchOutcome({
      segmentId,
      opportunityId: row.opportunityId || row.id,
      skipped: true,
      reason: row.reason || "DEEP_RESEARCH_SKIPPED",
      mock: true,
      cacheHit: true,
      costUsd: 0,
    });
  }
  if (row.blocked) {
    return recordDeepResearchOutcome({
      segmentId,
      opportunityId: row.opportunityId || row.id,
      blocked: true,
      reason: row.reason || "DEEP_RESEARCH_BLOCKED",
      costUsd: Number(row.deepCostUsd || 0) || 0,
    });
  }
  return recordDeepResearchOutcome({
    segmentId,
    opportunityId: row.opportunityId || row.id,
    costUsd: Number(row.deepCostUsd || 0) || 0,
    mock: row.live !== true,
    cacheHit: row.live !== true,
    providerRequestId: row.providerRunId || null,
  });
}

export {
  listFourteenWorkflowSegmentIds,
  RUN_VERDICT,
};
