/**
 * Canonical GDI Full E2E research entry V1 (P1-C).
 *
 * One per-hotel research path:
 *   preflight → shared run context → fourteen-segment discovery →
 *   deep research / contacts / qualification (original chain) →
 *   hotel-wide finalization (mandatory: every canonical opportunity reviewed) →
 *   execution certificate (discovery ≠ Full E2E complete).
 *
 * Does not publish. Airtable research-inventory persistence is opt-in via the
 * research persistence bridge (dry-run default; production fail-closed).
 * Paid providers require explicit auth.
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
import {
  resolveLocalDemandTrack,
  buildLocalDemandTrackContextBlock,
} from "../research/gdi-local-demand-research-tracks-v1.js";
import {
  buildReusePriorIntelligenceLedger,
} from "./reuse-prior-intelligence-v1.js";
import {
  persistFullE2eResearchInventory,
  writeResearchPersistenceLedger,
  RESEARCH_PERSISTENCE_WRITE_SET_POLICY,
} from "./gdi-full-e2e-research-persistence-bridge-v1.js";
import {
  runHotelWideFinalizationStage,
  GDI_HOTEL_WIDE_FINALIZATION_STAGE_V1,
} from "./gdi-hotel-wide-finalization-stage-v1.js";
import {
  resolveCanonicalLeisureOptIn,
  runCanonicalLeisureOptInStage,
  GDI_CANONICAL_LEISURE_OPT_IN_WIRE_V1,
} from "./gdi-canonical-leisure-opt-in-wire-v1.js";
import {
  resolveCanonicalCadOptIn,
  runCanonicalCadOptInStage,
  applyCadEnrichmentToResearchRows,
  GDI_CANONICAL_CAD_OPT_IN_STAGE_V1,
} from "./gdi-canonical-cad-opt-in-stage-v1.js";
import {
  buildCanonicalCommercialPrioritiesRunContext,
  applyCommercialPrioritiesToOpportunityRows,
  GDI_CANONICAL_COMMERCIAL_PRIORITIES_RUN_CONTEXT_V1,
} from "./gdi-canonical-commercial-priorities-run-context-v1.js";
import {
  resolveDeepResearchOperatingMode,
  DEEP_RESEARCH_OPERATING_MODE,
} from "../research/gdi-commercial-deep-research-operating-modes-v1.js";
import { applyCompetitiveBusinessClassification } from "../commercial-consistency/gdi-competitive-business-classification-v1.js";
import {
  buildResearchCompletenessReport,
} from "../research/gdi-commercial-research-completion-queue-v1.js";

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
  const authorizationId = text(opts.authorizationId);
  if (!authorizationId || authorizationId === "GDI_UNAUTHORIZED") {
    blockers.push({
      code: "AUTHORIZATION_ID_REQUIRED_FOR_LIVE",
      note: "Pass --authorization-id for durable paid budget identity (not timestamped outDir).",
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

  // Coerce null → undefined so ledger helpers keep their default ROOT.
  const obsRoot = opts.executionObservabilityRoot || undefined;
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

  const researchTrack = resolveLocalDemandTrack(opts.researchTrack);
  if (opts.researchTrack && !researchTrack) {
    return {
      ok: false,
      blocked: true,
      preflight,
      error: "UNKNOWN_RESEARCH_TRACK",
      verdict: RUN_VERDICT.FULL_E2E_BLOCKED,
      publicationAuthorized: false,
    };
  }
  const plannedSegmentIds = Array.isArray(opts.segmentIds) && opts.segmentIds.length
    ? opts.segmentIds
    : researchTrack
      ? researchTrack.segments
      : listFourteenWorkflowSegmentIds();
  const researchTrackContextBlock = researchTrack
    ? buildLocalDemandTrackContextBlock(researchTrack.trackId, {
        displayName: opts.hotelDisplayName || null,
        city: opts.hotelCity || null,
      })
    : null;

  let ledgerCtx = null;
  try {
    ledgerCtx = beginFourteenSegmentExecutionRun({
      hotelId,
      runId,
      root: obsRoot,
      label:
        opts.executionLabel ||
        (researchTrack
          ? `canonical-full-e2e:${mode}:track:${researchTrack.trackId}`
          : `canonical-full-e2e:${mode}`),
      plannedSegmentIds,
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

  // Reuse existing shared intelligence before discovery/deep spend.
  // Default: filesystem + hotel inventory only. Airtable DG/program/signal
  // reads require explicit loadSharedIntelligenceFromAirtable (still no writes).
  let sharedIntelligenceReuse = null;
  let additionalInventoryOrgKeysForChain = [];
  try {
    const reuseDir = path.join(outDir, "shared-intelligence-reuse");
    fs.mkdirSync(reuseDir, { recursive: true });
    const skipAirtableReuse = opts.loadSharedIntelligenceFromAirtable !== true;
    const reuseLedger = await buildReusePriorIntelligenceLedger(hotelId, {
      skipAirtable: skipAirtableReuse,
    });
    additionalInventoryOrgKeysForChain = [
      ...new Set(
        (reuseLedger.rows || [])
          .map((r) => String(r.organizationIdentityKey || "").trim())
          .filter(Boolean),
      ),
    ];
    fs.writeFileSync(
      path.join(reuseDir, "reuse-ledger.json"),
      `${JSON.stringify(reuseLedger, null, 2)}\n`,
    );
    sharedIntelligenceReuse = {
      version: reuseLedger.version || null,
      skipAirtable: skipAirtableReuse,
      rowCount: Array.isArray(reuseLedger.rows) ? reuseLedger.rows.length : 0,
      additionalInventoryOrgKeys: additionalInventoryOrgKeysForChain.length,
      path: path.join(reuseDir, "reuse-ledger.json"),
      note: "Existing reuse-prior-intelligence ledger; no new shared model.",
    };
  } catch (err) {
    sharedIntelligenceReuse = {
      ok: false,
      error: String(err?.message || err),
      note: "Reuse ledger failed closed to empty additional keys; chain continues.",
    };
    additionalInventoryOrgKeysForChain = [];
  }

  // Commercial Priorities + Need Dates — standard governed run-context.
  // Guides research emphasis/ranking; never silently removes demand segments.
  let commercialPrioritiesRunContext = null;
  try {
    commercialPrioritiesRunContext =
      await buildCanonicalCommercialPrioritiesRunContext(hotelId, {
        useAirtable: false,
        allowLocalFallback: true,
        commercialProfile: opts.commercialProfile || null,
        hotelConfig: opts.hotelConfig || null,
        needPeriodIsolation: opts.needPeriodIsolation || undefined,
      });
    fs.mkdirSync(outDir, { recursive: true });
    fs.writeFileSync(
      path.join(outDir, "commercial-priorities-run-context.json"),
      `${JSON.stringify(
        {
          contract: GDI_CANONICAL_COMMERCIAL_PRIORITIES_RUN_CONTEXT_V1,
          snapshot: commercialPrioritiesRunContext.snapshot || null,
          savedProfileApplied:
            commercialPrioritiesRunContext.savedProfileApplied === true,
          ok: commercialPrioritiesRunContext.ok !== false,
        },
        null,
        2,
      )}\n`,
    );
  } catch (err) {
    commercialPrioritiesRunContext = {
      contract: GDI_CANONICAL_COMMERCIAL_PRIORITIES_RUN_CONTEXT_V1,
      ok: false,
      hotelId,
      error: String(err?.message || err),
      note: "Commercial priorities run-context failed closed to neutral; chain continues.",
    };
  }

  const deepMode = resolveDeepResearchOperatingMode({
    operatingMode: opts.operatingMode || opts.deepResearchOperatingMode || null,
    maxCandidates: opts.maxCandidates,
    certificationSample: opts.certificationSample === true,
    commercialFullE2e: opts.commercialFullE2e === true,
  });
  // Isolated buyer-WHO ledger under an allowlisted reports/data prefix.
  const buyerWhoLedgerPath =
    opts.buyerWhoLedgerPath ||
    path.join(outDir, "buyer-who-live-ledgers", `${hotelId}.json`);
  try {
    fs.mkdirSync(path.dirname(buyerWhoLedgerPath), { recursive: true });
    if (!fs.existsSync(buyerWhoLedgerPath)) {
      fs.writeFileSync(
        buyerWhoLedgerPath,
        `${JSON.stringify(
          {
            version: "gdi-buyer-who-live-ledger-v1",
            hotelId,
            currency: "USD",
            budgetUsd: Number(budgetCeilingUsd || 0),
            spentUsd: 0,
            operations: [],
            createdAt: new Date().toISOString(),
          },
          null,
          2,
        )}\n`,
      );
    }
  } catch {
    // Non-fatal — deep seed still proceeds; WHO may still refuse if path invalid.
  }
  fs.writeFileSync(
    path.join(outDir, "deep-research-operating-mode.json"),
    `${JSON.stringify(deepMode, null, 2)}\n`,
  );

  const chain = await executeOriginalFullE2eResearchChain({
    hotelIds: [hotelId],
    authorizePaidDiscovery,
    authorizePaidResearch,
    authorizeExaAgentLive,
    allowLiveExaDeepSeed,
    budgetCeilingUsd,
    authorizationId: opts.authorizationId || undefined,
    exaEffort: opts.exaEffort || EXA_AGENT_EFFORT.MEDIUM,
    useFixtureProvider: !authorizePaidDiscovery,
    allowHistoricalReplay: opts.allowHistoricalReplay !== false,
    persistState: opts.persistState !== false,
    // CERTIFICATION_SAMPLE defaults to 8; COMMERCIAL_FULL_E2E uses null (all eligible, budget-gated).
    maxCandidates: deepMode.maxCandidates,
    allowPartial:
      opts.allowPartial === true ||
      mode !== CANONICAL_EXECUTION_MODE.LIVE ||
      !authorizePaidDiscovery ||
      deepMode.isCommercialFullE2e,
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
    segmentIds: plannedSegmentIds,
    researchTrack: researchTrack?.trackId || null,
    researchTrackContextBlock,
    forceRerun: opts.forceRerun === true,
    stateRoot: opts.stateRoot || undefined,
    additionalInventoryOrgKeys: additionalInventoryOrgKeysForChain,
    applySavedCommercialProfile: true,
    useAirtableCommercialProfile: false,
    commercialProfileByHotelId: commercialPrioritiesRunContext?.commercialProfile
      ? { [hotelId]: commercialPrioritiesRunContext.commercialProfile }
      : undefined,
    buyerWhoLedgerPath,
    ledgerPath: buyerWhoLedgerPath,
  });

  // Soft commercial-alignment explanations on processed rows (never reject).
  if (
    commercialPrioritiesRunContext?.ok &&
    Array.isArray(chain?.processed) &&
    chain.processed.length
  ) {
    chain.processed = applyCommercialPrioritiesToOpportunityRows(
      chain.processed,
      commercialPrioritiesRunContext,
    );
  }

  // Ensure every planned segment has a terminal deepResearchStatus.
  if (ensureActiveRunOrNull()) {
    try {
      const run = loadExecutionRun(hotelId, ledgerCtx.runId, obsRoot);
      const processedSegs = new Set(
        (chain?.processed || [])
          .map((p) => p.segmentId || p.researchSegment)
          .filter(Boolean),
      );
      const missingDeep = plannedSegmentIds.filter((sid) => {
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

  // Leisure Demand Intelligence — STANDARD stage (always assess; execute applicable).
  // Ordinary 14-segment discovery ≠ specialized leisure-module execution.
  // Explicit disableLeisureStandardStage is isolated-test only.
  let leisureOptIn = null;
  const leisureResolved = resolveCanonicalLeisureOptIn({
    ...opts,
    authorizePaidDiscovery,
    authorizePaidResearch,
  });
  if (leisureResolved.enabled) {
    try {
      leisureOptIn = await runCanonicalLeisureOptInStage({
        ...opts,
        hotelId,
        hotelProfile: {
          hotelId,
          displayName:
            opts.hotelDisplayName ||
            chain?.hotel?.displayName ||
            chain?.hotelName ||
            null,
          city: opts.hotelCity || chain?.hotel?.city || null,
          ...(opts.hotelProfile && typeof opts.hotelProfile === "object"
            ? opts.hotelProfile
            : {}),
        },
        authorizePaidDiscovery,
        authorizePaidResearch,
        existingInventory: [
          ...(opts.seedOpportunities || []),
          ...(Array.isArray(chain?.processed) ? chain.processed : []),
        ],
        outDir: path.join(outDir, "leisure-standard-stage"),
        writeLeisureArtifacts: opts.writeLeisureArtifacts !== false,
      });
    } catch (err) {
      leisureOptIn = {
        contract: GDI_CANONICAL_LEISURE_OPT_IN_WIRE_V1,
        status: "APPLICABLE_EXECUTION_FAILED",
        enabled: true,
        standardStage: true,
        ok: false,
        spentUsd: 0,
        paidCalls: 0,
        error: String(err?.message || err),
      };
    }
  } else {
    leisureOptIn = {
      contract: GDI_CANONICAL_LEISURE_OPT_IN_WIRE_V1,
      status: "SKIPPED_EXPLICITLY_DISABLED",
      enabled: false,
      standardStage: true,
      spentUsd: 0,
      paidCalls: 0,
      specializedLeisureExecution: false,
      note: leisureResolved.note,
    };
  }

  // Competitive Account Discovery — STANDARD Research SoT enrichment.
  // Never auto-promotes FQO / persistence / publication. Not a full predictive engine.
  let cadOptIn = null;
  const cadResolved = resolveCanonicalCadOptIn(opts);
  if (cadResolved.enabled) {
    try {
      cadOptIn = runCanonicalCadOptInStage({
        ...opts,
        hotelId,
        chain,
        seedOpportunities: opts.seedOpportunities || [],
        opportunities: opts.cadOpportunities || null,
        researchSotRows: opts.researchSotRows || null,
      });
      if (cadOptIn.enabled && Array.isArray(chain?.processed)) {
        chain.processed = applyCadEnrichmentToResearchRows(
          chain.processed,
          cadOptIn,
        );
      }
    } catch (err) {
      cadOptIn = {
        contract: GDI_CANONICAL_CAD_OPT_IN_STAGE_V1,
        status: "FAILED",
        enabled: true,
        standardStage: true,
        ok: false,
        classifiedCount: 0,
        fqoAutoPromoted: false,
        persistenceAuthorized: false,
        predictiveEngineClaim: false,
        error: String(err?.message || err),
      };
    }
  } else {
    cadOptIn = {
      contract: GDI_CANONICAL_CAD_OPT_IN_STAGE_V1,
      status: "SKIPPED_EXPLICITLY_DISABLED",
      enabled: false,
      standardStage: true,
      classifiedCount: 0,
      fqoAutoPromoted: false,
      persistenceAuthorized: false,
      predictiveEngineClaim: false,
      note: cadResolved.note,
    };
  }

  // Competitive business classification (analytical; never auto-FQO).
  if (Array.isArray(chain?.processed) && chain.processed.length) {
    chain.processed = chain.processed.map((row) => {
      if (row?.opportunity && typeof row.opportunity === "object") {
        return {
          ...row,
          opportunity: applyCompetitiveBusinessClassification(row.opportunity),
        };
      }
      return applyCompetitiveBusinessClassification(row);
    });
  }

  const spentUsd =
    Number(chain?.paidSpendUsd ?? 0) + Number(leisureOptIn?.spentUsd || 0);
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

  // Research inventory persistence — independent of research completion.
  // Default dry-run. Production requires bridge gates + founder activation.
  let researchPersistence = null;
  let researchPersistenceLedgerPath = null;
  const wantPersistencePlan =
    opts.planResearchPersistence === true ||
    opts.authorizeResearchPersistence === true ||
    opts.persistResearchInventory === true;
  if (wantPersistencePlan) {
    try {
      researchPersistence = await persistFullE2eResearchInventory({
        hotelId,
        hotelName:
          opts.hotelDisplayName ||
          chain?.hotel?.displayName ||
          chain?.hotelName ||
          null,
        runId: ledgerCtx.runId,
        chain,
        opportunities: opts.persistenceOpportunities || null,
        seedOpportunities: opts.seedOpportunities || [],
        repoRoot: ROOT,
        researchCompleted: true,
        authorizeResearchPersistence:
          opts.authorizeResearchPersistence === true,
        exactWriteSetAuthorization: opts.exactWriteSetAuthorization || null,
        apply: opts.applyResearchPersistence === true,
        authorizePublish: false,
        authorizeGdoPersistence: false,
        authorizePaidResearch,
        writeBudget: opts.researchPersistenceWriteBudget || 0,
        readBack: opts.readBackResearchPersistence === true,
        // Permanent standard lock: canonical entry never plans/applies a
        // research-universe write set. Explicit audit callers use the bridge
        // directly with RESEARCH_UNIVERSE_AUDIT.
        writeSetPolicy:
          RESEARCH_PERSISTENCE_WRITE_SET_POLICY.VALIDATED_FQO_ONLY,
        requireValidatedFqoWriteSet: true,
        allowHistoricalSchemaCompat:
          opts.allowHistoricalSchemaCompat !== false,
        allowedOpportunityIds: opts.allowedOpportunityIds || null,
      });
      researchPersistenceLedgerPath = writeResearchPersistenceLedger(
        outDir,
        researchPersistence,
      );
    } catch (err) {
      researchPersistence = {
        ok: false,
        code: "RESEARCH_PERSISTENCE_BRIDGE_THREW",
        error: String(err?.message || err),
        dryRun: true,
        gdoWrites: 0,
        publicationWrites: 0,
      };
    }
  }

  // Mandatory hotel-wide finalization — every canonical opportunity reviewed.
  // Review ≠ paid research. Agent OFF unless separately authorized.
  // skipHotelWideFinalization is ISOLATED-TEST ONLY and never yields production COMPLETE.
  let hotelWideFinalization = null;
  const isolatedTestSkip =
    opts.skipHotelWideFinalization === true &&
    opts.allowIsolatedTestSkipHotelWideFinalization === true &&
    (mode === CANONICAL_EXECUTION_MODE.FIXTURE ||
      opts.isolatedTestHarness === true);
  const skipHotelWide = isolatedTestSkip;
  if (opts.skipHotelWideFinalization === true && !isolatedTestSkip) {
    // Fail closed before running the stage — unauthorized skip must not execute.
    hotelWideFinalization = {
      ok: false,
      blocked: true,
      contract: GDI_HOTEL_WIDE_FINALIZATION_STAGE_V1,
      error:
        "skipHotelWideFinalization refused outside isolated test harness — hotel-wide stage is mandatory for production Full E2E",
      completionFailed: true,
      fullE2eHotelWideComplete: false,
      completionOutcome: "FULL_E2E_BLOCKED",
    };
  } else if (!skipHotelWide) {
    try {
      hotelWideFinalization = await runHotelWideFinalizationStage({
        hotelId,
        hotelName:
          opts.hotelDisplayName ||
          chain?.hotel?.displayName ||
          chain?.hotelName ||
          null,
        runId: ledgerCtx.runId,
        chain,
        seedOpportunities: opts.seedOpportunities || [],
        outDir: path.join(outDir, "hotel-wide-finalization"),
        // Production: always prepare/certify candidate automatically.
        // Explicit false only for isolated unit tests.
        writeCandidatePackage: opts.writeHotelWideCandidatePackage !== false,
        paidSpendUsd: spentUsd,
        authorizationId: text(opts.authorizationId) || null,
        agentAuthorized: authorizeExaAgentLive === true,
      });
    } catch (err) {
      hotelWideFinalization = {
        ok: false,
        blocked: true,
        contract: GDI_HOTEL_WIDE_FINALIZATION_STAGE_V1,
        error: String(err?.message || err),
        completionFailed: true,
        fullE2eHotelWideComplete: false,
        completionOutcome: "FULL_E2E_BLOCKED",
      };
    }
  }

  fs.mkdirSync(outDir, { recursive: true });
  const summaryPath = path.join(outDir, "canonical-full-e2e-summary.json");
  const persistenceWrites =
    researchPersistence && researchPersistence.dryRun === false
      ? Number(researchPersistence.writesUsed || 0)
      : 0;
  // Never treat skip as hotel-wide success for production COMPLETE claims.
  const hotelWideOk =
    !skipHotelWide &&
    (hotelWideFinalization?.hotelInventoryReconciled === true ||
      hotelWideFinalization?.fullE2eHotelWideComplete === true ||
      hotelWideFinalization?.ok === true);
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
    stagedPointerActivated: false,
    airtableWrites: persistenceWrites,
    researchPersistencePlanned: wantPersistencePlan,
    sharedIntelligenceReuse,
    commercialPrioritiesRunContext: commercialPrioritiesRunContext
      ? {
          contract:
            commercialPrioritiesRunContext.contract ||
            GDI_CANONICAL_COMMERCIAL_PRIORITIES_RUN_CONTEXT_V1,
          ok: commercialPrioritiesRunContext.ok !== false,
          savedProfileApplied:
            commercialPrioritiesRunContext.savedProfileApplied === true,
          confirmationStatus:
            commercialPrioritiesRunContext.snapshot?.confirmationStatus || null,
          profileId: commercialPrioritiesRunContext.snapshot?.profileId || null,
          currentVersion:
            commercialPrioritiesRunContext.snapshot?.currentVersion ?? null,
          activeNeedPeriodCount: Array.isArray(
            commercialPrioritiesRunContext.activeNeedPeriods,
          )
            ? commercialPrioritiesRunContext.activeNeedPeriods.length
            : 0,
          doesNotLimitDiscovery: true,
          softSignalOnly: true,
        }
      : null,
    leisureOptIn: leisureOptIn
      ? {
          contract: leisureOptIn.contract || GDI_CANONICAL_LEISURE_OPT_IN_WIRE_V1,
          status: leisureOptIn.status || null,
          enabled: leisureOptIn.enabled === true,
          standardStage: leisureOptIn.standardStage !== false,
          specializedLeisureExecution:
            leisureOptIn.specializedLeisureExecution === true,
          moduleIds: leisureOptIn.moduleIds || [],
          moduleOutcomes: leisureOptIn.moduleOutcomes || [],
          spentUsd: Number(leisureOptIn.spentUsd || 0),
          paidCalls: Number(leisureOptIn.paidCalls || 0),
          paidProductionOff: leisureOptIn.paidProductionOff !== false,
          opportunityCount: leisureOptIn.opportunityCount || 0,
        }
      : null,
    cadOptIn: cadOptIn
      ? {
          contract: cadOptIn.contract || GDI_CANONICAL_CAD_OPT_IN_STAGE_V1,
          status: cadOptIn.status || null,
          enabled: cadOptIn.enabled === true,
          standardStage: cadOptIn.standardStage !== false,
          classifiedCount: cadOptIn.classifiedCount || 0,
          classCounts: cadOptIn.classCounts || null,
          predictiveSourcing: cadOptIn.predictiveSourcing || null,
          fqoAutoPromoted: cadOptIn.fqoAutoPromoted === true,
          persistenceAuthorized: cadOptIn.persistenceAuthorized === true,
          predictiveEngineClaim: cadOptIn.predictiveEngineClaim === true,
        }
      : null,
    researchPersistence: researchPersistence
      ? {
          ok: researchPersistence.ok !== false,
          dryRun: researchPersistence.dryRun !== false,
          code: researchPersistence.code || null,
          counts: researchPersistence.counts || null,
          blockedApply: researchPersistence.blockedApply === true,
          stageStatus: researchPersistence.stageStatus || null,
          persistenceCertified:
            researchPersistence.persistenceCertified === true,
          ledgerPath: researchPersistenceLedgerPath,
        }
      : {
          ok: true,
          dryRun: true,
          code: null,
          counts: null,
          blockedApply: false,
          stageStatus: "NOT_REQUESTED",
          persistenceCertified: false,
          ledgerPath: null,
        },
    hotelWideFinalization: hotelWideFinalization
      ? {
          contract: GDI_HOTEL_WIDE_FINALIZATION_STAGE_V1,
          ok: hotelWideFinalization.ok === true,
          skipped: skipHotelWide,
          isolatedTestSkip,
          completionFailed: hotelWideFinalization.completionFailed === true,
          completionOutcome:
            hotelWideFinalization.completionOutcome ||
            hotelWideFinalization.manifest?.completionOutcome ||
            null,
          hotelInventoryReconciled:
            hotelWideFinalization.hotelInventoryReconciled === true,
          researchGapsRemain:
            hotelWideFinalization.researchGapsRemain === true,
          qualifiedCandidateCertified:
            hotelWideFinalization.qualifiedCandidateCertified === true,
          manifestPath: hotelWideFinalization.manifestPath || null,
          dispositions: hotelWideFinalization.manifest?.dispositions || null,
          stagedCandidate: hotelWideFinalization.manifest?.stagedCandidate || null,
          pointersUnchanged: hotelWideFinalization.pointersUnchanged !== false,
        }
      : { skipped: skipHotelWide, isolatedTestSkip },
    apifyDefault: APIFY_SKIP_REASON.APIFY_NOT_ORCHESTRATED,
    verdict: certificate?.verdict || finalized?.verdict || null,
    deepResearchOperatingMode: deepMode,
    researchCompletenessReport: buildResearchCompletenessReport({
      operatingMode: deepMode.operatingMode,
      standardStageAssessed: true,
      liveResearchExecuted:
        authorizePaidDiscovery === true || authorizePaidResearch === true,
      deepResearchCompletedCount: Number(processedCount || 0),
      deepResearchRequiredCount: Number(
        chain?.deepResearchSelection?.deferredCount != null
          ? Number(chain.deepResearchSelection.selectedCount || 0) +
              Number(chain.deepResearchSelection.deferredCount || 0)
          : processedCount || 0,
      ),
      competitiveClassificationCompleted: Array.isArray(chain?.processed)
        ? chain.processed.every((r) => r?.competitiveBusinessClassification || r?.opportunity?.competitiveBusinessClassification)
        : false,
      strictFqoCompleted: false,
      remainingResearchContinuation: Number(
        chain?.deepResearchSelection?.deferredCount || 0,
      ),
      providerAuthorizationPending: !(
        authorizePaidDiscovery === true && authorizePaidResearch === true
      ),
      leisureMode:
        leisureOptIn?.researchMode ||
        leisureOptIn?.mode ||
        (leisureOptIn?.paidProductionOff !== false ? "mock_or_gated" : "live"),
      predictiveSourcingStatus:
        cadOptIn?.predictiveSourcing?.capabilityStatus ||
        cadOptIn?.predictiveSourcing?.status ||
        null,
      comprehensiveResearchComplete:
        deepMode.isCommercialFullE2e === true &&
        Number(chain?.deepResearchSelection?.deferredCount || 0) === 0 &&
        authorizePaidResearch === true,
    }),
    researchCompleteness: certificate?.researchCompleteness || null,
    coverageStatus: chain?.coverageStatus || chain?.segmentCoverage?.coverageStatus || null,
    segmentCoverage: chain?.segmentCoverage || null,
    discoveryYieldCounts: chain?.discoveryYieldCounts || null,
    // Inventory reconcile alone is not Full E2E COMPLETE.
    fullE2eCompleteClaimAllowed:
      chain?.fullE2eCompleteClaimAllowed === true &&
      hotelWideOk &&
      hotelWideFinalization?.completionOutcome === "FULL_E2E_COMPLETE" &&
      !skipHotelWide,
    certificate,
    outDir,
  };
  fs.writeFileSync(summaryPath, `${JSON.stringify(summary, null, 2)}\n`);

  const coverageOk =
    chain?.ok !== false &&
    hotelWideOk &&
    !skipHotelWide &&
    (chain?.fullE2eCompleteClaimAllowed === true ||
      chain?.allowPartial === true ||
      mode !== CANONICAL_EXECUTION_MODE.LIVE);

  return {
    ok: coverageOk,
    coverageStatus: summary.coverageStatus,
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
    stagedPointerActivated: false,
    sharedIntelligenceReuse,
    commercialPrioritiesRunContext,
    leisureOptIn,
    cadOptIn,
    researchPersistence,
    researchPersistenceLedgerPath,
    hotelWideFinalization,
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
