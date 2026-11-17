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
import {
  quoteExaAgentRun,
  EXA_AGENT_EFFORT,
  isExaAgentLiveAuthorized,
} from "../research/exa-agent/exa-agent-provider-adapter-v1.js";

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
/**
 * Live-readiness assessment (zero-cost). Does not spend.
 * Returns LIVE_READY or BLOCKED with precise reasons.
 */
export function assessCanonicalLiveReadiness(opts = {}) {
  const preflight = preflightCanonicalFullE2e({
    ...opts,
    mode: CANONICAL_EXECUTION_MODE.LIVE,
  });
  const blockers = [...(preflight.errors || [])];
  const hasKey = Boolean(process.env.EXA_API_KEY || process.env.EXA_KEY);
  const effort = opts.exaEffort || EXA_AGENT_EFFORT.MEDIUM;
  const quote = quoteExaAgentRun({ effort });
  const ceiling = Number(opts.budgetCeilingUsd || 0);
  const authorizeExa =
    opts.authorizeExaAgentLive === true ||
    (opts.authorizePaidDiscovery === true && opts.authorizePaidResearch === true);

  if (!hasKey) {
    blockers.push({ code: "EXA_API_KEY_ABSENT" });
  }
  if (quote.bounded !== true || !(Number(quote.maxUsd) > 0)) {
    blockers.push({
      code: "EXA_QUOTE_UNBOUNDED_OR_INVALID",
      quote,
    });
  }
  if (!(ceiling > 0)) {
    blockers.push({
      code: "BUDGET_CEILING_REQUIRED_FOR_LIVE",
      note: "Pass --budget-ceiling-usd with a positive hard ceiling.",
    });
  }
  if (ceiling > 0 && Number(quote.maxUsd) > ceiling + 1e-9) {
    blockers.push({
      code: "UNIT_QUOTE_EXCEEDS_HARD_CEILING",
      unitMaxUsd: quote.maxUsd,
      hardCeilingUsd: ceiling,
    });
  }
  if (!authorizeExa) {
    blockers.push({
      code: "AUTHORIZE_EXA_AGENT_LIVE_REQUIRED",
      note: "Pass --authorize-exa-agent-live (and paid discovery/research flags).",
    });
  }
  if (
    authorizeExa &&
    !isExaAgentLiveAuthorized({ authorizeExaAgentLive: true })
  ) {
    blockers.push({
      code: "GDI_EXA_AGENT_LIVE_ENV_REQUIRED",
      note: "Set process env GDI_EXA_AGENT_LIVE=1 for the live process only.",
    });
  }
  if (opts.enableApifyOrchestration === true || opts.authorizeApifyLive === true) {
    blockers.push({
      code: "APIFY_NOT_AUTHORIZED_IN_THIS_CAMPAIGN",
    });
  }

  const liveReady = blockers.length === 0 && preflight.ok;
  return {
    status: liveReady ? "LIVE_READY" : "BLOCKED",
    liveReady,
    preflight,
    hasExaCredential: hasKey,
    quote,
    effort,
    hardCeilingUsd: ceiling,
    authorizeExaAgentLive: authorizeExa === true,
    gdiExaAgentLiveEnv:
      String(process.env.GDI_EXA_AGENT_LIVE || "") === "1",
    blockers,
    contract: GDI_CANONICAL_FULL_E2E_ENTRY_V1,
  };
}

export function preflightCanonicalFullE2e(opts = {}) {
  const hotelId = text(opts.hotelId);
  const mode = normalizeMode(opts.mode);
  const errors = [];
  if (!hotelId) errors.push({ code: "MISSING_HOTEL_ID" });
  if (!mode) errors.push({ code: "MISSING_OR_INVALID_MODE" });

  const authorizePaidDiscovery = opts.authorizePaidDiscovery === true;
  const authorizePaidResearch = opts.authorizePaidResearch === true;
  const authorizeExaAgentLive = opts.authorizeExaAgentLive === true;
  const authorizePublish = opts.authorizePublish === true;
  const budgetCeilingUsd = Number(opts.budgetCeilingUsd || 0) || 0;
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
  if (mode === CANONICAL_EXECUTION_MODE.LIVE && !(budgetCeilingUsd > 0)) {
    errors.push({
      code: "LIVE_MODE_REQUIRES_BUDGET_CEILING_USD",
    });
  }
  if (
    mode === CANONICAL_EXECUTION_MODE.LIVE &&
    (authorizePaidDiscovery || authorizePaidResearch) &&
    !authorizeExaAgentLive
  ) {
    errors.push({
      code: "LIVE_MODE_REQUIRES_AUTHORIZE_EXA_AGENT_LIVE",
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
    authorizeExaAgentLive:
      fixtureMode || explicitDryRun ? false : authorizeExaAgentLive,
    budgetCeilingUsd,
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
    const liveReadiness = assessCanonicalLiveReadiness({
      ...opts,
      hotelId,
      mode: CANONICAL_EXECUTION_MODE.LIVE,
      // Preflight assessment is informational unless founder live flags present.
      authorizePaidDiscovery: opts.authorizePaidDiscovery === true,
      authorizePaidResearch: opts.authorizePaidResearch === true,
      authorizeExaAgentLive: opts.authorizeExaAgentLive === true,
      budgetCeilingUsd: opts.budgetCeilingUsd,
    });
    return {
      ok: profile.ok !== false,
      preflightOnly: true,
      preflight,
      liveReadinessStatus: liveReadiness.status,
      liveReadiness,
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

  const authorizePaidDiscovery = preflight.authorizePaidDiscovery;
  const authorizePaidResearch = preflight.authorizePaidResearch;
  const authorizeExaAgentLive = preflight.authorizeExaAgentLive === true;
  const budgetCeilingUsd = Number(
    preflight.budgetCeilingUsd || opts.budgetCeilingUsd || 0,
  );
  const allowLiveExaDeepSeed =
    authorizeExaAgentLive === true && authorizePaidResearch === true;

  let liveReadiness = null;
  if (mode === CANONICAL_EXECUTION_MODE.LIVE) {
    liveReadiness = assessCanonicalLiveReadiness({
      ...opts,
      hotelId,
      mode: CANONICAL_EXECUTION_MODE.LIVE,
      authorizePaidDiscovery,
      authorizePaidResearch,
      authorizeExaAgentLive,
      budgetCeilingUsd,
    });
    if (!liveReadiness.liveReady) {
      return {
        ok: false,
        blocked: true,
        preflight,
        liveReadiness,
        liveReadinessStatus: "BLOCKED",
        verdict: RUN_VERDICT.FULL_E2E_BLOCKED,
        publicationAuthorized: false,
        error: "LIVE_READINESS_BLOCKED",
        blockers: liveReadiness.blockers,
      };
    }
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
      liveReadiness,
      error: String(err?.message || err),
      verdict: RUN_VERDICT.FULL_E2E_BLOCKED,
      publicationAuthorized: false,
    };
  }

  const chain = await executeOriginalFullE2eResearchChain({
    hotelIds: [hotelId],
    authorizePaidDiscovery,
    authorizePaidResearch,
    authorizeExaAgentLive,
    allowLiveExaDeepSeed,
    budgetCeilingUsd,
    exaEffort: opts.exaEffort || EXA_AGENT_EFFORT.MEDIUM,
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

  const spentUsd = Number(chain?.paidSpendUsd ?? 0);
  const remainingUsd =
    chain?.discovery?.paidBudgetRemainingUsd != null
      ? Math.max(
          0,
          budgetCeilingUsd > 0
            ? budgetCeilingUsd - spentUsd
            : Number(chain.discovery.paidBudgetRemainingUsd),
        )
      : budgetCeilingUsd > 0
        ? Math.max(0, budgetCeilingUsd - spentUsd)
        : null;
  const budgetsRespected =
    budgetCeilingUsd > 0
      ? spentUsd <= budgetCeilingUsd + 1e-9
      : !(authorizePaidDiscovery || authorizePaidResearch);
  const discoveryCells =
    Number(chain?.discovery?.cellCount || 0) ||
    (Array.isArray(chain?.discovery?.cells)
      ? chain.discovery.cells.length
      : 0);
  const processedCount = Array.isArray(chain?.processed)
    ? chain.processed.length
    : 0;
  const outputsIngested =
    Boolean(outDir && fs.existsSync(outDir)) &&
    (discoveryCells > 0 ||
      processedCount > 0 ||
      Boolean(chain?.version) ||
      fs.existsSync(path.join(outDir, "chain-execution-trace.json")));

  const finalized = finalizeFourteenSegmentExecutionRun({
    budgetsRespected,
    outputsIngested,
    discoveryPhaseOnly: false,
    requireDeepResearch: true,
    blocked: budgetsRespected !== true,
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
    liveReadiness,
    chainVersion: chain?.version || null,
    discovery: chain?.discovery || null,
    processedCount,
    spentUsd,
    remainingUsd,
    hardCeilingUsd: budgetCeilingUsd,
    budgetsRespected,
    outputsIngested,
    authorizeExaAgentLive,
    allowLiveExaDeepSeed,
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
    liveReadiness,
    hotelId,
    mode,
    runId: ledgerCtx.runId,
    chain,
    certificate,
    verdict: summary.verdict,
    researchCompleteness: summary.researchCompleteness,
    spentUsd,
    remainingUsd,
    budgetsRespected,
    outputsIngested,
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
