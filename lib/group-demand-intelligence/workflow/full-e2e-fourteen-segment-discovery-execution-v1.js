/**
 * Full E2E fourteen-segment discovery execution V1.
 *
 * Invokes registered executors (or honest stubs), replays historical ledger
 * candidates as Stage A discoveries when zero-cost/live paid is unavailable,
 * and emits per-cell execution traces.
 *
 * Never calls paid Exa/Firecrawl/Surfe without authorizePaidDiscovery=true.
 * Never publishes. Never invents provider calls.
 */

import fs from "node:fs";
import path from "node:path";
import { pathToFileURL, fileURLToPath } from "node:url";
import { createHash } from "node:crypto";
import { listFourteenWorkflowSegmentIds } from "../research/universal-contact-research-contract-v1.js";
import {
  FULL_E2E_FOURTEEN_SEGMENT_DISCOVERY_EXECUTOR_REGISTRY_V1,
  getFourteenSegmentExecutorSpec,
} from "./full-e2e-fourteen-segment-discovery-executor-registry-v1.js";
import {
  runSegmentDiscoveryExecutor,
  createFixtureDiscoveryProvider,
  createHistoricalHitProvider,
  DISCOVERY_EXECUTION_STATUS,
} from "../research/segment-discovery-executor-v1.js";
import {
  loadDiscoveryState,
  persistDiscoveryTrace,
  shouldSkipLiveProviderInvocation,
  newResearchRunId,
} from "../research/full-e2e-fourteen-segment-discovery-state-v1.js";
import { attachQualificationHandoffToTrace } from "../research/discovery-to-qualification-handoff-v1.js";
import { createExaLiveDiscoveryProvider } from "../research/exa-agent/exa-live-discovery-provider-bridge-v1.js";
import { EXA_AGENT_EFFORT } from "../research/exa-agent/exa-agent-provider-adapter-v1.js";
import {
  canAffordQuotedCall,
  settleReservedCall,
  reserveQuotedCall,
  releaseReservation,
  remainingBudgetUsd,
} from "../research/gdi-authorized-paid-budget-ledger-v1.js";

export const FULL_E2E_FOURTEEN_SEGMENT_DISCOVERY_EXECUTION_V1 =
  "gdi-full-e2e-fourteen-segment-discovery-execution-v1";

const SEGMENT_DISCOVERY_NOTE = "runSegmentDiscoveryExecutor";

const DEFAULT_LEDGER_ROOTS = [
  "C:/Dev/dealality/_railway-lean-stage-gdi-spice/data/group-demand-intelligence/discovery-ledgers",
  "C:/Dev/dealality/_railway-lean-stage-gdi-spice-recovery-v1/data/group-demand-intelligence/discovery-ledgers",
];

/** Map 14 PowerShell workflow ids → historical ledger workflow field values. */
const SEGMENT_TO_LEDGER_WORKFLOW = Object.freeze({
  associations_conferences_events: [
    "associations_conferences_events",
    "association_events",
    "associations_government",
  ],
  corporate_accounts_scaling: [
    "corporate_accounts_scaling",
    "scaling_companies",
    "scaling",
  ],
  corporate_meetings_incentives: [
    "corporate_meetings_incentives",
    "corporate_meetings_training",
    "mice_incentive_travel",
  ],
  government_public_sector: ["government_public_sector", "government"],
  sports_entertainment: ["sports_entertainment", "sports", "entertainment_production"],
  education_scientific: ["education_scientific", "medical_academic"],
  projects_infrastructure: ["projects_infrastructure", "projects"],
  social_special_events: [
    "social_special_events",
    "weddings_social_private_events",
    "social",
  ],
  local_catering_day_meetings: [
    "local_catering_day_meetings",
    "local_meetings_catering",
  ],
  corporate_celebrations_private_events: [
    "corporate_celebrations_private_events",
  ],
  travel_trade_group_intermediaries: [
    "travel_trade_group_intermediaries",
    "professional_planners_dmc",
  ],
  relocation_extended_stay: ["relocation_extended_stay"],
  preferred_lodging_partnerships: ["preferred_lodging_partnerships", "preferred_lodging"],
  professional_event_planners: [
    "professional_event_planners",
    "professional_planners_dmc",
  ],
});

function sha12(s) {
  return createHash("sha1").update(String(s || "")).digest("hex").slice(0, 12);
}

function hasHttpUrl(row) {
  const urls = [
    ...(row.originalSourceUrls || []),
    ...(row.evidenceUrls || []),
    row.sourceUrl,
    row.url,
  ]
    .filter(Boolean)
    .map(String);
  return urls.some((u) => /^https?:\/\//i.test(u));
}

function orgKey(row) {
  return String(row.organizationName || row.orgName || row.title || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function loadHistoricalLedger(hotelId, ledgerRoots = DEFAULT_LEDGER_ROOTS) {
  for (const root of ledgerRoots) {
    const p = path.join(
      root,
      hotelId,
      "four-hotel-14-workflow-full-e2e-v1.json"
    );
    if (!fs.existsSync(p)) continue;
    const doc = JSON.parse(fs.readFileSync(p, "utf8"));
    const rows = Array.isArray(doc)
      ? doc
      : doc.candidates || doc.rows || doc.items || [];
    return { path: p, rows, campaignId: doc.campaignId || "four-hotel-14-workflow-full-e2e-v1" };
  }
  return { path: null, rows: [], campaignId: null };
}

function filterLedgerForSegment(rows, segmentId) {
  const aliases = new Set(
    (SEGMENT_TO_LEDGER_WORKFLOW[segmentId] || [segmentId]).map((s) =>
      String(s).toLowerCase()
    )
  );
  return (rows || []).filter((row) => {
    const wf = String(
      row.workflowId || row.workflow || row.segmentId || row.demandSegment || ""
    ).toLowerCase();
    return aliases.has(wf);
  });
}

function stageAFromLedgerRow(row, hotelId, segmentId) {
  const idSeed = `${hotelId}|${segmentId}|${orgKey(row)}|${row.programmeName || row.title || ""}`;
  return {
    candidateId: row.candidateId || `hist_${sha12(idSeed)}`,
    hotelId,
    segmentId,
    organizationName: row.organizationName || row.orgName || null,
    title: row.title || row.programmeName || null,
    stage: "A_DISCOVERY",
    sourceFamily: "historical_ledger_replay",
    evidenceUrls: [
      ...(row.originalSourceUrls || []),
      ...(row.evidenceUrls || []),
    ].filter(Boolean),
    hasSourceEvidence: hasHttpUrl(row),
    historicalStatus: row.status || row.disposition || null,
    duplicateFlag: row.duplicateFlag || row.isDuplicate || null,
    provenance: {
      campaignId: "four-hotel-14-workflow-full-e2e-v1",
      exaRunId: row.exaRunId || null,
      replayOnly: true,
      freshRetrieval: false,
    },
  };
}

async function loadRunner(spec) {
  const abs = path.resolve(
    path.dirname(fileURLToPath(import.meta.url)),
    "../../..",
    spec.hotelRunnerRel
  );
  if (!fs.existsSync(abs)) {
    return { ok: false, error: "RUNNER_FILE_MISSING", abs };
  }
  const mod = await import(pathToFileURL(abs).href);
  const fn = mod[spec.exportName];
  if (typeof fn !== "function") {
    return { ok: false, error: "EXPORT_MISSING", abs, exportName: spec.exportName };
  }
  return { ok: true, fn, abs };
}

/**
 * Execute one hotel×segment discovery cell.
 *
 * @param {object} opts
 * @param {string} opts.hotelId
 * @param {string} opts.segmentId
 * @param {boolean} [opts.authorizePaidDiscovery=false]
 * @param {boolean} [opts.allowHistoricalReplay=true]
 * @param {object} [opts.fixtureExecutor] - inject for CI: async (ctx)=>trace
 * @param {string[]} [opts.ledgerRoots]
 */
export async function executeFourteenSegmentDiscoveryCell(opts = {}) {
  const startedAt = new Date().toISOString();
  const hotelId = String(opts.hotelId || "").trim();
  const segmentId = String(opts.segmentId || "").trim();
  const authorizePaid = opts.authorizePaidDiscovery === true;
  const allowHistoricalReplay = opts.allowHistoricalReplay !== false;
  const spec = getFourteenSegmentExecutorSpec(segmentId);

  const baseTrace = {
    version: FULL_E2E_FOURTEEN_SEGMENT_DISCOVERY_EXECUTION_V1,
    registryVersion: FULL_E2E_FOURTEEN_SEGMENT_DISCOVERY_EXECUTOR_REGISTRY_V1,
    hotelId,
    segmentId,
    startedAt,
    finishedAt: null,
    executorInvoked: false,
    executorId: null,
    executorAbs: null,
    classification: spec?.classification || "BROKEN",
    strategy: spec?.strategy || null,
    sourceFamily: null,
    queryCount: 0,
    providerCallCount: 0,
    cacheHits: 0,
    freshRetrievals: 0,
    rawResults: 0,
    extractedCandidates: 0,
    validatedCandidates: 0,
    deduplicatedCandidates: 0,
    persistedResearchCandidates: 0,
    rejections: [],
    researchCostUsd: 0,
    stopReason: null,
    retrievalMode: null,
    candidates: [],
  };

  if (!hotelId || !segmentId) {
    return {
      ...baseTrace,
      finishedAt: new Date().toISOString(),
      stopReason: "INVALID_ARGS",
      ok: false,
    };
  }
  if (!spec) {
    return {
      ...baseTrace,
      finishedAt: new Date().toISOString(),
      stopReason: "MISSING_EXECUTOR_SPEC",
      ok: false,
    };
  }

  // CI / unit injection — proves orchestrator calls executors.
  if (typeof opts.fixtureExecutor === "function") {
    const injected = await opts.fixtureExecutor({ hotelId, segmentId, spec });
    return {
      ...baseTrace,
      ...injected,
      executorInvoked: true,
      executorId: injected.executorId || "fixtureExecutor",
      finishedAt: new Date().toISOString(),
      retrievalMode: injected.retrievalMode || "FIXTURE",
      ok: true,
    };
  }

  // Historical ledger Stage A recovery (zero-cost, evidenced replay).
  let ledgerCandidates = [];
  let ledgerMeta = { path: null };
  if (allowHistoricalReplay) {
    ledgerMeta = loadHistoricalLedger(hotelId, opts.ledgerRoots || DEFAULT_LEDGER_ROOTS);
    const segRows = filterLedgerForSegment(ledgerMeta.rows, segmentId);
    const named = segRows.filter((r) => orgKey(r));
    const withSource = named.filter((r) => hasHttpUrl(r));
    const missingSource = named.length - withSource.length;
    // Preserve named discoveries even without URL — Stage A incomplete, not silent drop.
    const byOrg = new Map();
    for (const row of named) {
      const k = orgKey(row);
      if (!byOrg.has(k)) byOrg.set(k, row);
      else if (hasHttpUrl(row) && !hasHttpUrl(byOrg.get(k))) byOrg.set(k, row);
    }
    ledgerCandidates = [...byOrg.values()].map((r) =>
      stageAFromLedgerRow(r, hotelId, segmentId)
    );
    if (missingSource > 0) {
      baseTrace.rejections.push({
        reason: "NO_HTTP_SOURCE_EVIDENCE_KEPT_AS_STAGE_A_INCOMPLETE",
        count: missingSource,
      });
    }
    if (segRows.length - named.length > 0) {
      baseTrace.rejections.push({
        reason: "MISSING_ORGANIZATION_NAME",
        count: segRows.length - named.length,
      });
    }
  }

  // Primary path: canonical segment discovery executor (GDI hotel resolution, query adapters).
  const segRows = filterLedgerForSegment(ledgerMeta.rows, segmentId);
  const useFixture = opts.useFixtureProvider === true;
  const persistState = opts.persistState !== false;
  const stateRoot = opts.stateRoot || undefined;
  const priorState = persistState
    ? loadDiscoveryState(hotelId, segmentId, { root: stateRoot })
    : null;
  const liveSkip = shouldSkipLiveProviderInvocation(priorState, {
    forceRerun: opts.forceRerun === true,
  });
  const researchRunId =
    opts.researchRunId ||
    priorState?.researchRunId ||
    newResearchRunId();

  let provider = opts.provider || null;
  let resumedFromCheckpoint = false;
  if (authorizePaid && liveSkip.skip && !opts.forceRerun) {
    // Do not repeat completed live provider calls; historical/fixture may still enrich.
    resumedFromCheckpoint = true;
    if (!provider && allowHistoricalReplay && segRows.length) {
      provider = createHistoricalHitProvider(segRows);
    } else if (!provider && useFixture) {
      provider = createFixtureDiscoveryProvider(opts.fixtureOpts || {});
    }
  } else if (!provider) {
    if (authorizePaid && !liveSkip.skip) {
      // Inject existing Exa live discovery bridge under global budget ledger.
      const ledger = opts.paidBudgetLedger || null;
      const effort = opts.exaEffort || EXA_AGENT_EFFORT.MEDIUM;
      const inner = createExaLiveDiscoveryProvider({
        effort,
        maxCostDollars: null,
        authorizePaidDiscovery: true,
        authorizeExaAgentLive: true,
        forceMockOnly: false,
        segmentId,
      });
      provider = {
        id: "budgeted_exa_live_discovery_provider_v1",
        isLive: true,
        async search(ctx) {
          if (!ledger) {
            return {
              providerCallCount: 0,
              freshRetrievals: 0,
              cacheHits: 0,
              costUsd: 0,
              hits: [],
              mode: "BLOCKED_PAID",
              blockedReason: "PAID_LEDGER_REQUIRED_FOR_LIVE_DISCOVERY",
            };
          }
          const gate = canAffordQuotedCall(ledger);
          if (!gate.ok) {
            ledger.blockedOperations.push({
              at: new Date().toISOString(),
              hotelId,
              segmentId,
              stage: "DISCOVERY",
              ...gate,
            });
            return {
              providerCallCount: 0,
              freshRetrievals: 0,
              cacheHits: 0,
              costUsd: 0,
              hits: [],
              mode: "BLOCKED_BUDGET",
              blockedReason: gate.reason,
              remainingBudgetUsd: remainingBudgetUsd(ledger),
            };
          }
          const reserved = reserveQuotedCall(ledger, {
            quotedMaxUsd: gate.proposedMax,
          });
          if (!reserved.ok) {
            return {
              providerCallCount: 0,
              freshRetrievals: 0,
              cacheHits: 0,
              costUsd: 0,
              hits: [],
              mode: "BLOCKED_BUDGET",
              blockedReason: reserved.reason,
            };
          }
          try {
            const result = await inner.search(ctx);
            settleReservedCall(ledger, {
              quotedMaxUsd: gate.proposedMax,
              actualCostUsd: Number(result.costUsd || 0),
              hotelId,
              segmentId,
              stage: "DISCOVERY",
              providerRunId: result.providerRunId || null,
              mode: result.mode,
              blockedReason: result.blockedReason || null,
            });
            return result;
          } catch (err) {
            releaseReservation(ledger, gate.proposedMax);
            throw err;
          }
        },
      };
    } else if (useFixture) {
      provider = createFixtureDiscoveryProvider(opts.fixtureOpts || {});
    } else if (allowHistoricalReplay && segRows.length) {
      provider = createHistoricalHitProvider(segRows);
    }
  }

  let runnerResult;
  try {
    runnerResult = await runSegmentDiscoveryExecutor({
      hotelId,
      segmentId,
      authorizePaidDiscovery: authorizePaid && !liveSkip.skip,
      skipHpc: opts.skipHpc !== false,
      provider,
      historicalRows: !provider && allowHistoricalReplay ? segRows : undefined,
      useFixtureProvider: useFixture && !provider,
    });
  } catch (err) {
    runnerResult = {
      ok: false,
      error: String(err?.message || err),
      classification: "BROKEN",
      executionStatus: DISCOVERY_EXECUTION_STATUS.EXECUTION_BLOCKED,
      providerCalls: 0,
      queryCount: 0,
      candidates: [],
      executorInvoked: true,
    };
  }

  // Also allow specialized runners to resolve hotel context (compat proof) without claiming discovery.
  let specializedNote = null;
  if (spec.specializedRunnerRel) {
    const loaded = await loadRunner({
      ...spec,
      hotelRunnerRel: spec.specializedRunnerRel,
      exportName:
        segmentId === "associations_conferences_events"
          ? "runAssociationEventsHotelRunner"
          : segmentId === "corporate_accounts_scaling"
            ? "runScalingCompaniesHotelRunner"
            : spec.exportName,
    });
    if (loaded.ok) {
      try {
        const specialized = await loaded.fn({
          hotelId,
          mode: "preview",
          authorizePaidResearch: false,
          skipHpc: true,
          maxCandidates: 2,
        });
        specializedNote = {
          ok: specialized?.ok !== false,
          executed: specialized?.executed === true,
          hotelResolved: Boolean(specialized?.hotel || specialized?.profile),
          skippedReason: specialized?.discovery?.skippedReason || specialized?.reason || null,
        };
      } catch (err) {
        specializedNote = { ok: false, error: String(err?.message || err) };
      }
    } else {
      specializedNote = {
        ok: false,
        available: false,
        hotelResolved: false,
        executed: false,
        error: loaded.error,
      };
    }
  }

  const providerCalls = Number(runnerResult?.providerCalls || 0);
  const fresh = Number(runnerResult?.freshRetrievals || 0);
  const queries = Number(runnerResult?.queryCount || 0);
  const runnerCandidates = Array.isArray(runnerResult?.candidates)
    ? runnerResult.candidates
    : [];

  // Merge runner + historical Stage A by candidateId/org without inventing commercial inventory.
  const merged = new Map();
  for (const c of [...runnerCandidates, ...ledgerCandidates]) {
    const key = String(c.candidateId || orgKey(c) || "").toLowerCase();
    if (!key) continue;
    if (!merged.has(key)) merged.set(key, c);
    else if (c.hasSourceEvidence && !merged.get(key).hasSourceEvidence) merged.set(key, c);
  }
  const candidates = [...merged.values()];

  const retrievalMode =
    providerCalls > 0 || fresh > 0
      ? "FRESH_RETRIEVAL"
      : runnerResult?.retrievalMode ||
        (ledgerCandidates.length ? "HISTORICAL_REPLAY" : "NO_RETRIEVAL");

  const finishedAt = new Date().toISOString();
  const costForTrace = resumedFromCheckpoint
    ? 0
    : Number(runnerResult?.researchCostUsd || 0);
  const providerCallsForTrace = resumedFromCheckpoint ? 0 : providerCalls;
  const freshForTrace = resumedFromCheckpoint ? 0 : fresh;

  const hotelName =
    runnerResult?.hotel?.displayName ||
    runnerResult?.hotel?.name ||
    opts.hotelName ||
    null;
  const hotelCity = runnerResult?.hotel?.city || opts.hotelCity || null;

  const trace = {
    ...baseTrace,
    finishedAt,
    researchRunId,
    hotelName,
    hotelCity,
    executorInvoked: runnerResult?.executorInvoked !== false,
    executorId: `${SEGMENT_DISCOVERY_NOTE}@${segmentId}`,
    executorAbs: null,
    classification: runnerResult?.executionStatus || runnerResult?.classification || spec.classification,
    executionStatus: runnerResult?.executionStatus || null,
    strategy: spec.strategy,
    sourceFamily: (spec.sourceFamilies || [])[0] || null,
    queryCount: queries,
    querySpecs: runnerResult?.queries || [],
    providerCallCount: providerCallsForTrace,
    cacheHits: Number(runnerResult?.cacheHits || ledgerCandidates.length || 0),
    freshRetrievals: freshForTrace,
    rawResults: Number(runnerResult?.rawResults || 0),
    extractedCandidates: candidates.length,
    validatedCandidates: candidates.filter((c) => c.hasSourceEvidence).length,
    deduplicatedCandidates: candidates.length,
    persistedResearchCandidates: 0,
    researchCostUsd: costForTrace,
    stopReason: resumedFromCheckpoint
      ? `RESUMED_SKIP_LIVE:${liveSkip.reason}`
      : runnerResult?.stopReason || null,
    retrievalMode: resumedFromCheckpoint
      ? `RESUMED_${retrievalMode}`
      : retrievalMode,
    historicalLedgerPath: ledgerMeta.path,
    historicalRawForSegment: segRows.length,
    candidates,
    specializedRunner: specializedNote,
    runnerOk: runnerResult?.ok !== false,
    runnerNote: runnerResult?.error || null,
    discoveryCompleted: runnerResult?.discoveryCompleted === true,
    resumedFromCheckpoint,
    liveSkipReason: liveSkip.skip ? liveSkip.reason : null,
    publicationOccurred: false,
    airtableWrites: 0,
    ok: true,
  };

  if (persistState) {
    try {
      const saved = persistDiscoveryTrace(trace, {
        root: stateRoot,
        researchRunId,
      });
      trace.persistedResearchCandidates = (
        saved.state?.deduplicatedCandidateIds || []
      ).length;
      trace.durableStatePath = saved.path;
      trace.durableExecutionStatus = saved.state?.executionStatus || null;
      trace.cumulativeCostUsd = saved.state?.providerUsage?.costUsd || 0;
      trace.lastSuccessfulCheckpoint = saved.state?.lastSuccessfulCheckpoint || null;
    } catch (err) {
      trace.persistError = String(err?.message || err);
    }
  }

  // Discovery finishing is not commercial research COMPLETE.
  // Zero-cost qualification handoff when candidates exist.
  if (opts.runQualificationHandoff !== false && (trace.candidates || []).length) {
    try {
      const withQual = attachQualificationHandoffToTrace(trace, {
        persist: persistState,
        persistDiscoveryState: persistState,
        outDir: opts.qualificationOutDir,
      });
      return withQual;
    } catch (err) {
      trace.qualificationHandoffError = String(err?.message || err);
      trace.commercialResearchComplete = false;
    }
  } else {
    trace.commercialResearchComplete = false;
  }

  return trace;
}

/**
 * Execute all hotel×segment cells (42 for Aimbridge trio).
 */
export async function executeFourteenSegmentDiscoveryMatrix(opts = {}) {
  const hotelIds = opts.hotelIds || [];
  const segmentIds = opts.segmentIds || listFourteenWorkflowSegmentIds();
  const researchRunId = opts.researchRunId || newResearchRunId();
  const cells = [];
  for (const hotelId of hotelIds) {
    for (const segmentId of segmentIds) {
      // Stop scheduling new live discovery when global ledger cannot afford next quoted call.
      if (
        opts.authorizePaidDiscovery === true &&
        opts.paidBudgetLedger &&
        !canAffordQuotedCall(opts.paidBudgetLedger).ok
      ) {
        cells.push({
          version: FULL_E2E_FOURTEEN_SEGMENT_DISCOVERY_EXECUTION_V1,
          hotelId,
          segmentId,
          startedAt: new Date().toISOString(),
          finishedAt: new Date().toISOString(),
          executorInvoked: false,
          executionStatus: DISCOVERY_EXECUTION_STATUS.EXECUTION_BLOCKED,
          stopReason: "GLOBAL_BUDGET_EXHAUSTED_OR_FROZEN",
          providerCallCount: 0,
          freshRetrievals: 0,
          researchCostUsd: 0,
          candidates: [],
          remainingBudgetUsd: remainingBudgetUsd(opts.paidBudgetLedger),
        });
        continue;
      }
      // eslint-disable-next-line no-await-in-loop
      const trace = await executeFourteenSegmentDiscoveryCell({
        ...opts,
        hotelId,
        segmentId,
        researchRunId,
      });
      cells.push(trace);
    }
  }
  const summary = {
    version: FULL_E2E_FOURTEEN_SEGMENT_DISCOVERY_EXECUTION_V1,
    at: new Date().toISOString(),
    researchRunId,
    hotelIds,
    segmentIds,
    cellCount: cells.length,
    executorInvokedCount: cells.filter((c) => c.executorInvoked).length,
    freshRetrievalCells: cells.filter((c) => c.freshRetrievals > 0 || c.providerCallCount > 0).length,
    historicalReplayCells: cells.filter((c) =>
      String(c.retrievalMode || "").includes("HISTORICAL")
    ).length,
    resumedCells: cells.filter((c) => c.resumedFromCheckpoint).length,
    durablePersistedCells: cells.filter((c) => c.durableStatePath).length,
    stubCells: cells.filter((c) => c.classification === "STUB_OR_NO_OP").length,
    blockedPaidCells: cells.filter((c) =>
      String(c.stopReason || "").includes("PAID_PROVIDER") ||
      String(c.stopReason || "").includes("BUDGET")
    ).length,
    totalExtractedCandidates: cells.reduce((n, c) => n + (c.extractedCandidates || 0), 0),
    totalResearchCostUsd: cells.reduce((n, c) => n + Number(c.researchCostUsd || 0), 0),
    paidBudgetRemainingUsd: opts.paidBudgetLedger
      ? remainingBudgetUsd(opts.paidBudgetLedger)
      : null,
    publicationOccurred: false,
    airtableWrites: 0,
    discoveryCompleteClaimAllowed: false,
    discoveryCompleteClaimBlockedReason:
      "Complete claim requires fresh retrieval evidence or authorized paid executors for applicable segments — historical replay alone is Stage A recovery, not FullE2E discovery completion.",
  };
  return { summary, cells };
}
