/**
 * Product-wide authorized live Leisure Demand Intelligence execution.
 *
 * Reuses fourteen-segment discovery cells + durable budget ledger.
 * Not a parallel leisure engine. Not Wyndham-specific.
 *
 * DryRun / fixture remains default. Live spend requires:
 *   - authorizePaidResearch
 *   - allowLivePilot
 *   - authorizationId (FOUNDER_GO_…)
 *   - hardCeilingUsd > 0
 *   - GDI_LDI_LIVE=1 (env)
 *   - budgetUsd > 0 (per-run cap ≤ remaining ceiling)
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  assertLeisureRunAuthorization,
  runLeisureModules,
} from "./gdi-leisure-module-runner-v1.js";
import {
  getLeisureModule,
  listLeisureModuleIds,
} from "./gdi-leisure-module-registry-v1.js";
import { selectLeisureModulesForHotel } from "./gdi-leisure-hotel-module-selection-v1.js";
import { buildLeisureOpportunityContract } from "./gdi-leisure-opportunity-contract-v1.js";
import { dedupeLeisureAgainstInventory } from "./gdi-leisure-cross-segment-dedupe-v1.js";
import {
  evaluateLeisureFinalizationGate,
} from "./gdi-leisure-live-e2e-integration-v1.js";
import { executeFourteenSegmentDiscoveryCell } from "../workflow/full-e2e-fourteen-segment-discovery-execution-v1.js";
import {
  loadOrCreatePaidBudgetLedger,
  remainingBudgetUsd,
  canAffordQuotedCall,
  reserveQuotedCall,
  settleReservedCall,
  persistPaidBudgetLedger,
} from "../research/gdi-authorized-paid-budget-ledger-v1.js";

export const GDI_LEISURE_AUTHORIZED_LIVE_V1 = "gdi-leisure-authorized-live-v1";
export const GDI_LDI_LIVE_ENV = "GDI_LDI_LIVE";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

function truthyEnv(v) {
  const s = String(v == null ? "" : v).trim().toLowerCase();
  return s === "1" || s === "true" || s === "yes" || s === "on";
}

/**
 * Controlled authorization for paid LDI live — does not strip fail-closed
 * protections; replaces blanket refusal with founder-gated dual auth.
 */
export function assertLeisureAuthorizedLive(opts = {}) {
  const errors = [];
  const mode = String(opts.researchMode || opts.mode || "fixture").toLowerCase();

  if (mode === "fixture" || mode === "replay" || mode === "preview" || mode === "dryrun") {
    return {
      ok: true,
      live: false,
      errors: [],
      remainingUsd: null,
      mode: "ZERO_PAID_DEFAULT",
    };
  }

  if (mode !== "live" && mode !== "authorized_live") {
    errors.push("MODE_MUST_BE_LIVE_OR_FIXTURE");
  }

  const base = assertLeisureRunAuthorization({
    budgetUsd: opts.budgetUsd,
    authorizePaidResearch: opts.authorizePaidResearch,
    allowLivePilot: opts.allowLivePilot,
    authorizePublication: opts.authorizePublication,
    authorizeFindymail: opts.authorizeFindymail,
    authorizeSurfe: opts.authorizeSurfe,
  });
  if (!base.ok) errors.push(base.error);

  if (!text(opts.authorizationId)) {
    errors.push("MISSING_AUTHORIZATION_ID");
  }

  const ceiling = Number(opts.hardCeilingUsd);
  if (!Number.isFinite(ceiling) || ceiling <= 0) {
    errors.push("HARD_CEILING_USD_REQUIRED");
  }

  if (opts.skipEnvGate !== true && !truthyEnv(process.env[GDI_LDI_LIVE_ENV])) {
    errors.push("GDI_LDI_LIVE_ENV_NOT_SET");
  }

  if (opts.authorizePublication === true) {
    errors.push("PUBLICATION_NOT_AUTHORIZED_IN_LDI_V1");
  }

  let remainingUsd = null;
  let ledger = null;
  if (text(opts.authorizationId) && Number.isFinite(ceiling) && ceiling > 0) {
    const loaded = loadOrCreatePaidBudgetLedger({
      authorizationId: opts.authorizationId,
      hardCeilingUsd: ceiling,
      root: opts.root || ROOT,
      resume: true,
    });
    if (!loaded.ok || !loaded.ledger) {
      errors.push(loaded.reason || "LEDGER_UNAVAILABLE");
    } else {
      ledger = loaded.ledger;
      remainingUsd = remainingBudgetUsd(ledger);
      const runBudget = Number(opts.budgetUsd || 0);
      if (runBudget > remainingUsd + 1e-9) {
        errors.push("RUN_BUDGET_EXCEEDS_REMAINING_CEILING");
      }
      if (remainingUsd <= 1e-9) {
        errors.push("BUDGET_CEILING_EXHAUSTED");
      }
    }
  }

  return {
    ok: errors.length === 0,
    live: errors.length === 0,
    errors,
    remainingUsd,
    ledger,
    mode: errors.length === 0 ? "AUTHORIZED_LIVE" : "BLOCKED",
  };
}

/**
 * $0 operational readiness probe — never spends.
 * Does not require GDI_LDI_LIVE (that gate applies only to live spend).
 */
export function preflightLeisureAuthorizedLive(opts = {}) {
  const auth = assertLeisureAuthorizedLive({
    ...opts,
    researchMode: "live",
    // Preflight never spends — env gate is informational via liveSpendRequiresEnv
    skipEnvGate: opts.skipEnvGate !== false,
  });
  const envSet = truthyEnv(process.env[GDI_LDI_LIVE_ENV]);
  return {
    ok: true,
    preflight: true,
    paid: false,
    spentUsd: 0,
    remainingUsd: auth.remainingUsd,
    liveSpendReady: auth.ok === true && envSet,
    liveSpendRequiresEnv: GDI_LDI_LIVE_ENV,
    envGatePresent: envSet,
    authorization: auth,
    schema: GDI_LEISURE_AUTHORIZED_LIVE_V1,
    defaultMode: "fixture",
    durableJobs: "fourteen-segment discovery cell settled-job reuse",
    resume: true,
    parallelEngineProhibited: true,
  };
}

/**
 * Execute leisure modules. Fixture/replay → existing runner.
 * Live → authorized fourteen-segment paid discovery when gates pass.
 */
export async function runLeisureAuthorizedOrFixture(opts = {}) {
  const mode = String(opts.researchMode || opts.mode || "fixture").toLowerCase();

  if (mode === "fixture" || mode === "replay" || mode === "preview" || mode === "dryrun") {
    return runLeisureModules({ ...opts, researchMode: mode === "dryrun" ? "fixture" : mode });
  }

  if (opts.preflightOnly === true) {
    return preflightLeisureAuthorizedLive(opts);
  }

  const auth = assertLeisureAuthorizedLive(opts);
  if (!auth.ok) {
    return {
      ok: false,
      schema: GDI_LEISURE_AUTHORIZED_LIVE_V1,
      error: "LDI_AUTHORIZED_LIVE_BLOCKED",
      errors: auth.errors,
      spentUsd: 0,
      remainingUsd: auth.remainingUsd,
    };
  }

  const hotelId = text(opts.hotelId);
  if (!hotelId) {
    return {
      ok: false,
      schema: GDI_LEISURE_AUTHORIZED_LIVE_V1,
      error: "HOTEL_ID_REQUIRED",
      spentUsd: 0,
    };
  }

  const hotelProfile = {
    hotelId,
    ...(opts.hotelProfile && typeof opts.hotelProfile === "object"
      ? opts.hotelProfile
      : {}),
  };

  const selection = selectLeisureModulesForHotel(hotelProfile, {
    forceModules: opts.modules || opts.forceModules,
    disableModules: opts.disableModules,
  });

  let moduleIds =
    Array.isArray(opts.modules) && opts.modules.length
      ? opts.modules.map((m) => getLeisureModule(m)?.id).filter(Boolean)
      : selection.moduleIds;
  if (!moduleIds.length) moduleIds = listLeisureModuleIds().slice(0, 1);

  let existingInventory = Array.isArray(opts.existingInventory)
    ? opts.existingInventory
    : [];
  if (
    !existingInventory.length &&
    text(opts.existingInventoryPath) &&
    fs.existsSync(opts.existingInventoryPath)
  ) {
    try {
      const loaded = JSON.parse(
        fs.readFileSync(opts.existingInventoryPath, "utf8"),
      );
      existingInventory = Array.isArray(loaded)
        ? loaded
        : loaded.opportunities || loaded.inventory || [];
    } catch {
      existingInventory = [];
    }
  }

  const runBudget = Number(opts.budgetUsd || 0);
  let spentUsd = 0;
  const allOpps = [];
  const moduleResults = [];
  const fourteenSegmentCalls = [];
  const telemetry = {
    signalsEvaluated: 0,
    candidatesDiscovered: 0,
    duplicatesConsolidated: 0,
    opportunitiesScreenedOut: 0,
    deepResearchCompleted: 0,
    namedPeopleIdentified: 0,
    existingEvidenceReused: 0,
    contactRoutesValidated: 0,
    fullyQualifiedOpportunities: 0,
    attribution: "MIXED_SHARED_MARKET_AND_HOTEL_SPECIFIC",
  };

  for (const moduleId of moduleIds) {
    if (spentUsd + 0.05 > runBudget + 1e-9) {
      moduleResults.push({
        leisureModuleId: moduleId,
        status: "SKIPPED_BUDGET",
        spentUsd: 0,
      });
      continue;
    }

    const mod = getLeisureModule(moduleId);
    if (!mod) continue;
    const segments = [...(mod.primarySegments || [])];
    const segmentTraces = [];
    const rawCandidates = [];
    let moduleSpend = 0;

    for (const segmentId of segments) {
      const remainingRun = runBudget - spentUsd;
      if (remainingRun <= 1e-9) break;

      const afford = canAffordQuotedCall(auth.ledger, 0.1);
      if (!afford.ok) {
        segmentTraces.push({
          segmentId,
          ok: false,
          stopReason: afford.reason || "BUDGET_CEILING",
        });
        break;
      }
      reserveQuotedCall(auth.ledger, {
        stage: "LDI_DISCOVERY",
        kind: "FOURTEEN_SEGMENT_CELL",
        hotelId,
        segmentId,
        leisureModuleId: moduleId,
        quotedMaxUsd: 0.1,
      });

      const trace = await executeFourteenSegmentDiscoveryCell({
        hotelId,
        segmentId,
        authorizePaidDiscovery: true,
        allowHistoricalReplay: opts.resume !== false,
        forceRerun: opts.forceRerun === true,
        authorizationId: opts.authorizationId,
        hardCeilingUsd: opts.hardCeilingUsd,
      });

      const cellCost = Number(trace.researchCostUsd || 0);
      moduleSpend += cellCost;
      spentUsd += cellCost;
      telemetry.signalsEvaluated += Number(trace.queryCount || 0);
      telemetry.candidatesDiscovered += Number(
        (trace.candidates || []).length || trace.validatedCandidates || 0,
      );
      telemetry.duplicatesConsolidated += Math.max(
        0,
        Number(trace.extractedCandidates || 0) -
          Number(trace.deduplicatedCandidates || 0),
      );
      if (trace.retrievalMode === "CACHE" || trace.retrievalMode === "REPLAY") {
        telemetry.existingEvidenceReused += 1;
      }

      settleReservedCall(auth.ledger, {
        quotedMaxUsd: 0.1,
        actualCostUsd: cellCost,
        stage: "LDI_DISCOVERY",
        kind: "FOURTEEN_SEGMENT_CELL",
        hotelId,
        segmentId,
        leisureModuleId: moduleId,
        opportunityId: null,
      });

      segmentTraces.push({
        segmentId,
        ok: trace.ok !== false,
        retrievalMode: trace.retrievalMode,
        candidateCount: (trace.candidates || []).length,
        researchCostUsd: cellCost,
        stopReason: trace.stopReason,
        reused: trace.retrievalMode === "CACHE" || liveSkipReuse(trace),
      });
      fourteenSegmentCalls.push({
        leisureModuleId: moduleId,
        segmentId,
        retrievalMode: trace.retrievalMode,
        costUsd: cellCost,
      });

      for (const c of trace.candidates || []) {
        rawCandidates.push({
          ...(c.raw || c),
          hotelId,
          leisureModuleId: moduleId,
          segmentId,
          demandGenerator: c.title || c.demandGenerator,
          organization: c.organizationName || c.organization,
          sourceUrls: c.sourceUrls || [],
          startDate: c.startDate,
        });
      }
    }

    const seen = new Set();
    const uniqueRaw = [];
    for (const row of rawCandidates) {
      const key = [
        String(row.organization || "").toLowerCase(),
        String(row.demandGenerator || "").toLowerCase(),
        String(row.startDate || ""),
      ].join("|");
      if (seen.has(key)) {
        telemetry.duplicatesConsolidated += 1;
        continue;
      }
      seen.add(key);
      uniqueRaw.push(row);
    }

    const contracts = uniqueRaw.map((row) =>
      buildLeisureOpportunityContract(
        { ...row, hotelId, leisureModuleId: moduleId },
        { needPeriodContext: opts.needPeriodContext || null },
      ),
    );

    const deduped = dedupeLeisureAgainstInventory(contracts, [
      ...existingInventory,
      ...allOpps,
    ]);
    telemetry.opportunitiesScreenedOut += Number(deduped.suppressedCount || 0);

    const gated = deduped.accepted.map((c) => {
      const gate = evaluateLeisureFinalizationGate(c);
      return { ...c, finalizationGate: gate, salesReady: false };
    });

    moduleResults.push({
      leisureModuleId: moduleId,
      label: mod.label,
      researchMode: "live",
      primarySegments: segments,
      segmentTraces,
      discovered: uniqueRaw.length,
      acceptedCount: gated.length,
      suppressedCount: deduped.suppressedCount,
      spentUsd: moduleSpend,
      paidCalls: segmentTraces.filter((t) => Number(t.researchCostUsd) > 0).length,
      status: "AUTHORIZED_LIVE_COMPLETE",
    });
    allOpps.push(...gated);
  }

  const outDir = opts.outDir
    ? path.resolve(opts.outDir)
    : path.join(
        ROOT,
        "reports/gdi-leisure-authorized-live-v1",
        `run_${hotelId}_${Date.now()}`,
      );

  if (opts.writeArtifacts !== false) {
    fs.mkdirSync(outDir, { recursive: true });
    fs.writeFileSync(
      path.join(outDir, "leisure-authorized-live-run.json"),
      JSON.stringify(
        {
          schema: GDI_LEISURE_AUTHORIZED_LIVE_V1,
          hotelId,
          authorizationId: opts.authorizationId,
          hardCeilingUsd: opts.hardCeilingUsd,
          spentUsd,
          remainingUsd: Math.max(0, (auth.remainingUsd || 0) - spentUsd),
          findymail: false,
          surfe: false,
          publication: false,
          moduleResults,
          fourteenSegmentCalls,
          opportunities: allOpps,
          researchReceiptTelemetry: telemetry,
        },
        null,
        2,
      ),
      "utf8",
    );
  }

  return {
    ok: true,
    schema: GDI_LEISURE_AUTHORIZED_LIVE_V1,
    hotelId,
    spentUsd,
    remainingUsd: Math.max(0, (auth.remainingUsd || 0) - spentUsd),
    moduleResults,
    opportunities: allOpps,
    opportunityCount: allOpps.length,
    fourteenSegmentCalls,
    researchReceiptTelemetry: telemetry,
    outDir: opts.writeArtifacts === false ? null : outDir,
    parallelEngineProhibited: true,
  };
}

function liveSkipReuse(trace) {
  return Boolean(trace?.settledJobReused || trace?.jobReused);
}
