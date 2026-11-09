#!/usr/bin/env node
/**
 * GDI_LIVE_DISCOVERY_PILOT_V1 — founder-authorized live Exa discovery.
 *
 * Hard ceiling: USD $5.00 / max 16 fixed-effort medium agent runs.
 * Hotels: recCAGqkERSXgiku4, recTk5I8VOzsPhxDz
 * Segments: associations_conferences_events, corporate_accounts_scaling
 *
 * No Airtable writes. No publication. No Railway.
 * Uses fixed Exa Agent effort=medium only (bounded quote) so the ceiling
 * can be enforced before each billable create.
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createHash } from "node:crypto";
import { resolveHotelGdiProfileForResearch } from "../lib/group-demand-intelligence/hotel-gdi-profile-resolution-v1.js";
import { buildSegmentDiscoveryQueries } from "../lib/group-demand-intelligence/research/segment-discovery-adapters-v1.js";
import {
  runSegmentDiscoveryExecutor,
  DISCOVERY_EXECUTION_STATUS,
} from "../lib/group-demand-intelligence/research/segment-discovery-executor-v1.js";
import {
  loadDiscoveryState,
  persistDiscoveryTrace,
  shouldSkipLiveProviderInvocation,
  newResearchRunId,
  buildDiscoveryResumePlan,
} from "../lib/group-demand-intelligence/research/full-e2e-fourteen-segment-discovery-state-v1.js";
import {
  createExaLiveDiscoveryProvider,
  inspectExaLiveDiscoveryReadiness,
} from "../lib/group-demand-intelligence/research/exa-agent/exa-live-discovery-provider-bridge-v1.js";
import {
  quoteExaAgentRun,
  EXA_AGENT_EFFORT,
  isExaAgentLiveAuthorized,
} from "../lib/group-demand-intelligence/research/exa-agent/exa-agent-provider-adapter-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const CEILING_USD = 5.0;
const CEILING_RUNS = 16;
const EFFORT = EXA_AGENT_EFFORT.MEDIUM;
const PILOT_ID = "GDI_LIVE_DISCOVERY_PILOT_V1";

const HOTELS = [
  { hotelId: "recCAGqkERSXgiku4", name: "Sheraton Guadalajara Expo" },
  { hotelId: "recTk5I8VOzsPhxDz", name: "voco Cancún Zona Hotelera" },
];
const SEGMENTS = [
  "associations_conferences_events",
  "corporate_accounts_scaling",
];

const OUT_DIR = path.join(ROOT, "reports", "gdi-live-discovery-pilot-v1-executed");

function has(flag) {
  return process.argv.includes(flag);
}

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function sha12(s) {
  return createHash("sha1").update(String(s || "")).digest("hex").slice(0, 12);
}

function writeJson(file, doc) {
  fs.mkdirSync(path.dirname(file), { recursive: true });
  fs.writeFileSync(file, `${JSON.stringify(doc, null, 2)}\n`);
}

function loadInventoryRows(hotelId) {
  const map = {
    recCAGqkERSXgiku4:
      "reports/aimbridge-three-hotel-full-e2e-research-baseline-v1/inventories/sheraton-guadalajara-expo-inventory.json",
    recTk5I8VOzsPhxDz:
      "reports/aimbridge-three-hotel-full-e2e-research-baseline-v1/inventories/voco-cancun-zona-hotelera-inventory.json",
  };
  const rel = map[hotelId];
  if (!rel) return [];
  const p = path.join(ROOT, rel);
  if (!fs.existsSync(p)) return [];
  const doc = JSON.parse(fs.readFileSync(p, "utf8"));
  return doc.rows || [];
}

function norm(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function fuzzyHit(a, b) {
  if (!a || !b) return false;
  if (a === b) return true;
  if (a.includes(b) || b.includes(a)) return true;
  const at = new Set(a.split(" ").filter((t) => t.length > 3));
  const bt = b.split(" ").filter((t) => t.length > 3);
  if (!bt.length) return false;
  return bt.filter((t) => at.has(t)).length / bt.length >= 0.6;
}

function classifyCommercial(candidate, hotel) {
  const evidence = Boolean(candidate.hasSourceEvidence);
  const org = text(candidate.organizationName || candidate.title);
  const city = text(hotel.city);
  const relevant =
    !city ||
    fuzzyHit(norm(`${org} ${candidate.title || ""}`), norm(city)) ||
    /guadalajara|cancun|cancún|mexico|expo|zona hotelera/i.test(
      `${org} ${candidate.title || ""} ${candidate.provenance?.excerpt || ""}`
    );

  if (!org) {
    return {
      disposition: "Rejected",
      reason: "missing_organization_identity",
      commercialClass: "REJECTED",
    };
  }
  if (!evidence) {
    return {
      disposition: "Needs More Research",
      reason: "no_http_source_evidence",
      commercialClass: "NEEDS_MORE_RESEARCH",
    };
  }
  if (!relevant) {
    return {
      disposition: "Watch",
      reason: "weak_hotel_market_relevance",
      commercialClass: "WATCH",
    };
  }
  // Live Stage A with evidence + relevance — not Sales Ready without buyer/sourcing depth.
  return {
    disposition: "Account Development",
    reason: "evidence_backed_stage_a_needs_commercial_depth",
    commercialClass: "ACCOUNT_DEVELOPMENT",
    namedContactRequiredForSalesReady: true,
    openRfpNotRequired: true,
  };
}

function createBudgetLedger() {
  return {
    ceilingUsd: CEILING_USD,
    ceilingRuns: CEILING_RUNS,
    spentUsd: 0,
    runs: 0,
    operations: [],
    blockedOperations: [],
  };
}

function canAfford(ledger, quoteMaxUsd) {
  const maxCharge = Number(quoteMaxUsd);
  if (!Number.isFinite(maxCharge) || maxCharge <= 0) {
    return {
      ok: false,
      reason: "UNPREDICTABLE_OR_INVALID_QUOTE",
      remaining: Number((ledger.ceilingUsd - ledger.spentUsd).toFixed(6)),
    };
  }
  if (ledger.runs >= ledger.ceilingRuns) {
    return {
      ok: false,
      reason: "RUN_CEILING_REACHED",
      remaining: Number((ledger.ceilingUsd - ledger.spentUsd).toFixed(6)),
    };
  }
  const remaining = ledger.ceilingUsd - ledger.spentUsd;
  if (maxCharge > remaining + 1e-9) {
    return {
      ok: false,
      reason: "WOULD_EXCEED_SPEND_CEILING",
      remaining: Number(remaining.toFixed(6)),
      proposedMax: maxCharge,
    };
  }
  return { ok: true, remaining: Number(remaining.toFixed(6)), proposedMax: maxCharge };
}

function recordSpend(ledger, op) {
  ledger.spentUsd = Number((ledger.spentUsd + Number(op.actualCostUsd || 0)).toFixed(6));
  ledger.runs += Number(op.runs || 0);
  ledger.operations.push(op);
}

/**
 * Budget-aware live provider wrapper around Exa bridge.
 * One agent run per cell (not one per query) to keep ceiling enforceable.
 */
function createBudgetedExaProvider({ ledger, quote, hotel, segmentId }) {
  const inner = createExaLiveDiscoveryProvider({
    effort: EFFORT,
    maxCostDollars: null, // fixed medium — do not use auto/max
    authorizePaidDiscovery: true,
    authorizeExaAgentLive: true,
    forceMockOnly: false,
    segmentId,
  });

  return {
    id: "budgeted_exa_live_discovery_provider_v1",
    isLive: true,
    async search(ctx) {
      const gate = canAfford(ledger, quote.maxUsd);
      if (!gate.ok) {
        ledger.blockedOperations.push({
          at: new Date().toISOString(),
          hotelId: hotel.hotelId,
          segmentId,
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
        };
      }

      const result = await inner.search(ctx);
      const actual = Number(result.costUsd || 0);
      const runs = Number(result.providerCallCount || 0) > 0 ? 1 : 0;

      // If provider reported a charge above remaining after the fact, record and stop further ops.
      recordSpend(ledger, {
        at: new Date().toISOString(),
        hotelId: hotel.hotelId,
        segmentId,
        quotedMaxUsd: quote.maxUsd,
        actualCostUsd: actual,
        runs,
        providerRunId: result.providerRunId || null,
        mode: result.mode,
        blockedReason: result.blockedReason || null,
      });

      if (actual > quote.maxUsd * 1.01) {
        // Unexpected overage vs fixed quote — freeze further paid calls.
        ledger.blockedOperations.push({
          at: new Date().toISOString(),
          reason: "ACTUAL_EXCEEDED_FIXED_QUOTE_FREEZE",
          actual,
          quotedMaxUsd: quote.maxUsd,
        });
        ledger.runs = ledger.ceilingRuns; // soft-freeze by exhausting run budget
      }

      return result;
    },
  };
}

async function authPreflight() {
  const keyPresent = Boolean(
    String(process.env.EXA_API_KEY || process.env.EXA_KEY || "").trim()
  );
  const liveFlag = String(process.env.GDI_EXA_AGENT_LIVE || "") === "1";
  const authorizeFlag = has("--authorize-paid-discovery");
  const dualAuth = isExaAgentLiveAuthorized({ authorizeExaAgentLive: true });
  const quote = quoteExaAgentRun({ effort: EFFORT });

  return {
    keyPresent,
    liveFlag,
    authorizeFlag,
    dualAuthOk: dualAuth && keyPresent && authorizeFlag && liveFlag,
    quote,
    quoteBounded: quote.bounded === true,
    quotePredictableFixedEffort: quote.bounded === true && EFFORT === "medium",
    keyValueRedacted: true,
  };
}

async function runCell({ hotelMeta, segmentId, ledger, quote, researchRunId, forceRerun }) {
  const hotelId = hotelMeta.hotelId;
  const startedAt = new Date().toISOString();
  const prior = loadDiscoveryState(hotelId, segmentId);
  const skip = shouldSkipLiveProviderInvocation(prior, { forceRerun });

  const resolved = await resolveHotelGdiProfileForResearch(hotelId, {
    skipHpc: true,
    preferLive: false,
  });
  const hotel = {
    ...(resolved.hotel || resolved.profile || {}),
    hotelId,
  };

  const readiness = inspectExaLiveDiscoveryReadiness({
    hotel,
    segmentId,
    effort: EFFORT,
    authorizeExaAgentLive: true,
  });

  const built = buildSegmentDiscoveryQueries(segmentId, hotel);
  const plannedQueries = (built.queries || []).slice(0, 4);

  if (skip.skip) {
    return {
      hotelId,
      hotelName: hotelMeta.name,
      segmentId,
      startedAt,
      finishedAt: new Date().toISOString(),
      skippedLive: true,
      skipReason: skip.reason,
      executorId: "runSegmentDiscoveryExecutor",
      queryIds: plannedQueries.map((q) => q.id),
      providerCallCount: 0,
      researchCostUsd: 0,
      candidates: [],
      durableStatePath: null,
      executionStatus: prior?.executionStatus || null,
      note: "Completed live provider work skipped on resume",
    };
  }

  const afford = canAfford(ledger, quote.maxUsd);
  if (!afford.ok) {
    return {
      hotelId,
      hotelName: hotelMeta.name,
      segmentId,
      startedAt,
      finishedAt: new Date().toISOString(),
      skippedLive: true,
      skipReason: afford.reason,
      budget: afford,
      providerCallCount: 0,
      researchCostUsd: 0,
      candidates: [],
      executionStatus: DISCOVERY_EXECUTION_STATUS.EXECUTION_BLOCKED,
    };
  }

  const provider = createBudgetedExaProvider({
    ledger,
    quote,
    hotel,
    segmentId,
  });

  // Limit executor to the planned query subset by wrapping provider context only;
  // adapter still builds full set — pass planned via provider search using executor queries.
  const exec = await runSegmentDiscoveryExecutor({
    hotelId,
    segmentId,
    authorizePaidDiscovery: true,
    skipHpc: true,
    provider: {
      ...provider,
      async search(ctx) {
        const limited = {
          ...ctx,
          queries: (ctx.queries || []).filter((q) =>
            plannedQueries.some((p) => p.id === q.id || p.query === q.query)
          ),
        };
        if (!limited.queries.length) limited.queries = plannedQueries;
        return provider.search(limited);
      },
    },
  });

  const inventory = loadInventoryRows(hotelId);
  const retained = [];
  const duplicates = [];
  const rejected = [];
  const classifications = [];

  for (const c of exec.candidates || []) {
    const org = norm(c.organizationName || c.title);
    const invHit = inventory.find((row) =>
      fuzzyHit(org, norm(`${row.organizationName || ""} ${row.title || ""}`))
    );
    const histDup = (prior?.candidateIds || []).some((id) => id === c.candidateId);
    const commercial = classifyCommercial(c, hotel);

    if (!c.organizationName && !c.title) {
      rejected.push({ candidateId: c.candidateId, reason: "missing_identity" });
      continue;
    }
    if (invHit) {
      duplicates.push({
        candidateId: c.candidateId,
        matchedInventoryId: invHit.candidateId,
        kind: "commercial_inventory_match",
      });
      classifications.push({
        candidateId: c.candidateId,
        ...commercial,
        enrichmentOfExisting: invHit.candidateId,
      });
      retained.push({
        ...c,
        retainedKind: "ENRICHMENT_OF_EXISTING",
        matchedInventoryId: invHit.candidateId,
        commercial,
      });
      continue;
    }
    if (histDup && !c.hasSourceEvidence) {
      duplicates.push({
        candidateId: c.candidateId,
        kind: "historical_id_without_new_evidence",
      });
      continue;
    }
    retained.push({
      ...c,
      retainedKind: c.hasSourceEvidence ? "NEW_OR_EVIDENCED" : "NEW_UNSUPPORTED_NAME",
      commercial,
    });
    classifications.push({ candidateId: c.candidateId, ...commercial });
  }

  const trace = {
    hotelId,
    segmentId,
    researchRunId,
    executorId: "runSegmentDiscoveryExecutor",
    executionStatus: exec.executionStatus,
    classification: exec.executionStatus,
    retrievalMode: exec.retrievalMode,
    providerCallCount: exec.providerCalls || 0,
    freshRetrievals: exec.freshRetrievals || 0,
    cacheHits: exec.cacheHits || 0,
    researchCostUsd: exec.researchCostUsd || 0,
    stopReason: exec.stopReason,
    querySpecs: plannedQueries,
    candidates: retained,
    rejections: [
      ...(exec.exclusions || []),
      ...rejected.map((r) => ({ reason: r.reason, candidateId: r.candidateId })),
    ],
    providerRunIds: ledger.operations
      .filter((o) => o.hotelId === hotelId && o.segmentId === segmentId && o.providerRunId)
      .map((o) => o.providerRunId),
    researchHorizon: "pilot_v1_fixed_medium_effort",
    discoveryCompleted: false,
  };

  const saved = persistDiscoveryTrace(trace, { researchRunId });

  return {
    hotelId,
    hotelName: hotelMeta.name,
    segmentId,
    startedAt,
    finishedAt: new Date().toISOString(),
    skippedLive: false,
    executorId: "runSegmentDiscoveryExecutor",
    builder: built.builder,
    readinessContractId: readiness.contractId,
    queryIds: plannedQueries.map((q) => q.id),
    queryPreviews: plannedQueries.map((q) => ({
      id: q.id,
      intent: q.intent,
      preview: text(q.query).slice(0, 180),
    })),
    providerCallCount: exec.providerCalls || 0,
    freshRetrievals: exec.freshRetrievals || 0,
    cacheHits: exec.cacheHits || 0,
    researchCostUsd: exec.researchCostUsd || 0,
    providerRunIds: trace.providerRunIds,
    retrievalMode: exec.retrievalMode,
    executionStatus: exec.executionStatus,
    stopReason: exec.stopReason,
    rawCandidates: (exec.candidates || []).length,
    evidenceBacked: (exec.candidates || []).filter((c) => c.hasSourceEvidence).length,
    duplicateCount: duplicates.length,
    rejectedCount: rejected.length,
    newlyRetained: retained.filter((c) => c.retainedKind !== "ENRICHMENT_OF_EXISTING").length,
    enrichments: retained.filter((c) => c.retainedKind === "ENRICHMENT_OF_EXISTING").length,
    retained,
    duplicates,
    rejected,
    classifications,
    namedContactsFound: 0,
    durableStatePath: saved.path,
    cumulativeCostUsd: saved.state?.providerUsage?.costUsd || 0,
    lastCheckpoint: saved.state?.lastSuccessfulCheckpoint || null,
    publicationOccurred: false,
    airtableWrites: 0,
  };
}

async function main() {
  const replayOnly = has("--replay-only");
  const forceRerun = has("--force-rerun");
  const researchRunId = newResearchRunId("gdi_live_pilot");

  const auth = await authPreflight();
  const quote = auth.quote;

  fs.mkdirSync(OUT_DIR, { recursive: true });

  if (!auth.keyPresent) {
    const blocked = {
      report: "DEALALITY_GDI_LIVE_DISCOVERY_PILOT_V1_BLOCKED",
      blocker: "EXA_API_KEY_ABSENT",
      detail:
        "No EXA_API_KEY/EXA_KEY in Aimbridge worktree process env or .env. Founder must supply the authorized credential into this environment only (do not paste into chat).",
      howToUnblock: [
        "Create C:\\Dev\\dealality\\my-operators-backend-aimbridge-three-hotel-e2e\\.env",
        "Add line: EXA_API_KEY=<authorized_key>",
        "Optionally keep GDI_EXA_AGENT_LIVE unset until the runner sets it",
        "Re-run: node scripts/gdi-live-discovery-pilot-v1.mjs --authorize-paid-discovery",
      ],
      paidCalls: 0,
      spentUsd: 0,
      ceilingUsd: CEILING_USD,
      at: new Date().toISOString(),
    };
    writeJson(path.join(OUT_DIR, "BLOCKED.json"), blocked);
    console.log(JSON.stringify(blocked, null, 2));
    process.exit(10);
  }

  if (!auth.quoteBounded || !auth.quotePredictableFixedEffort) {
    const blocked = {
      report: "DEALALITY_GDI_LIVE_DISCOVERY_PILOT_V1_BLOCKED",
      blocker: "EXA_BILLING_NOT_PREDICTABLE_FOR_CEILING",
      detail: quote,
      paidCalls: 0,
      spentUsd: 0,
      at: new Date().toISOString(),
    };
    writeJson(path.join(OUT_DIR, "BLOCKED.json"), blocked);
    console.log(JSON.stringify(blocked, null, 2));
    process.exit(11);
  }

  if (!replayOnly) {
    if (!has("--authorize-paid-discovery")) {
      const blocked = {
        report: "DEALALITY_GDI_LIVE_DISCOVERY_PILOT_V1_BLOCKED",
        blocker: "MISSING_AUTHORIZE_PAID_DISCOVERY_FLAG",
        at: new Date().toISOString(),
      };
      writeJson(path.join(OUT_DIR, "BLOCKED.json"), blocked);
      console.log(JSON.stringify(blocked, null, 2));
      process.exit(12);
    }
    process.env.GDI_EXA_AGENT_LIVE = "1";
  }

  const ledger = createBudgetLedger();
  const cells = [];

  if (replayOnly) {
    // Idempotency proof: no provider, load state only.
    for (const h of HOTELS) {
      for (const segmentId of SEGMENTS) {
        const st = loadDiscoveryState(h.hotelId, segmentId);
        const skip = shouldSkipLiveProviderInvocation(st);
        cells.push({
          hotelId: h.hotelId,
          segmentId,
          mode: "REPLAY_ONLY",
          skipLive: skip.skip,
          skipReason: skip.reason,
          costUsd: st?.providerUsage?.costUsd || 0,
          liveCalls: st?.providerUsage?.liveCalls || 0,
          candidateIds: st?.candidateIds || [],
          checkpoint: st?.lastSuccessfulCheckpoint || null,
          providerCallCount: 0,
          researchCostUsd: 0,
        });
      }
    }
  } else {
    for (const h of HOTELS) {
      for (const segmentId of SEGMENTS) {
        // eslint-disable-next-line no-await-in-loop
        const cell = await runCell({
          hotelMeta: h,
          segmentId,
          ledger,
          quote,
          researchRunId,
          forceRerun,
        });
        cells.push(cell);
        if (ledger.spentUsd >= CEILING_USD - 1e-9 || ledger.runs >= CEILING_RUNS) {
          break;
        }
      }
      if (ledger.spentUsd >= CEILING_USD - 1e-9 || ledger.runs >= CEILING_RUNS) break;
    }
  }

  const liveCells = cells.filter((c) => (c.providerCallCount || 0) > 0);
  const reportName =
    liveCells.length > 0 || (replayOnly && cells.some((c) => c.liveCalls > 0))
      ? replayOnly
        ? "DEALALITY_GDI_LIVE_DISCOVERY_PILOT_V1_REPLAY_VERIFIED"
        : "DEALALITY_GDI_LIVE_DISCOVERY_PILOT_V1_EXECUTED"
      : "DEALALITY_GDI_LIVE_DISCOVERY_PILOT_V1_BLOCKED";

  const resumeAfter = buildDiscoveryResumePlan({
    hotelIds: HOTELS.map((h) => h.hotelId),
    segmentIds: SEGMENTS,
  });

  const report = {
    report: reportName,
    pilotId: PILOT_ID,
    at: new Date().toISOString(),
    researchRunId,
    replayOnly,
    authorization: {
      ceilingUsd: CEILING_USD,
      ceilingRuns: CEILING_RUNS,
      effort: EFFORT,
      quote,
      founderAuthorizedSpendUsd: 5,
      airtableWrites: 0,
      publicationOccurred: false,
      railwayDeploy: false,
    },
    budget: ledger,
    cells,
    totals: {
      liveProviderCells: liveCells.length,
      totalProviderCalls: cells.reduce((n, c) => n + (c.providerCallCount || 0), 0),
      totalSpentUsd: ledger.spentUsd,
      remainingUsd: Number((CEILING_USD - ledger.spentUsd).toFixed(6)),
      totalRawCandidates: cells.reduce((n, c) => n + (c.rawCandidates || 0), 0),
      totalEvidenceBacked: cells.reduce((n, c) => n + (c.evidenceBacked || 0), 0),
      totalNewlyRetained: cells.reduce((n, c) => n + (c.newlyRetained || 0), 0),
      totalEnrichments: cells.reduce((n, c) => n + (c.enrichments || 0), 0),
      totalDuplicates: cells.reduce((n, c) => n + (c.duplicateCount || 0), 0),
    },
    resumeAfter,
    safety: {
      publicationOccurred: false,
      airtableWrites: 0,
      surfeUsed: false,
      firecrawlUsed: false,
      hotelsOutsideScope: false,
      workflowsOutsideScope: false,
    },
  };

  if (!replayOnly && liveCells.length === 0) {
    const allSkippedCompleted = cells.length > 0 && cells.every((c) =>
      String(c.skipReason || "").includes("LIVE_PROVIDER_ALREADY_COMPLETED")
    );
    if (allSkippedCompleted) {
      report.report =
        "DEALALITY_GDI_LIVE_DISCOVERY_PILOT_V1_RESUME_NOOP_VERIFIED";
      report.blocker = null;
      report.resumeNoop = true;
      report.note =
        "All four cells skipped live provider calls because durable checkpoints already recorded completed Exa runs. Additional spend: $0.";
    } else {
      report.report = "DEALALITY_GDI_LIVE_DISCOVERY_PILOT_V1_BLOCKED";
      report.blocker =
        ledger.blockedOperations[0]?.reason ||
        cells.find((c) => c.stopReason)?.stopReason ||
        cells.find((c) => c.skipReason)?.skipReason ||
        "NO_LIVE_PROVIDER_CALLS";
    }
  }

  writeJson(path.join(OUT_DIR, "pilot-execution-report.json"), report);
  writeJson(path.join(OUT_DIR, "budget-ledger.json"), ledger);
  writeJson(path.join(OUT_DIR, "cells.json"), cells);

  console.log(JSON.stringify({
    report: report.report,
    blocker: report.blocker || null,
    spentUsd: ledger.spentUsd,
    remainingUsd: report.totals.remainingUsd,
    liveProviderCells: report.totals.liveProviderCells,
    totalProviderCalls: report.totals.totalProviderCalls,
    totalNewlyRetained: report.totals.totalNewlyRetained,
    outDir: OUT_DIR,
  }, null, 2));

  if (report.report.includes("BLOCKED") && !replayOnly) process.exit(13);
  process.exit(0);
}

main().catch((err) => {
  console.error(JSON.stringify({
    report: "DEALALITY_GDI_LIVE_DISCOVERY_PILOT_V1_BLOCKED",
    blocker: "UNCAUGHT_ERROR",
    error: String(err?.stack || err),
  }, null, 2));
  process.exit(1);
});
