/**
 * Controlled GDI weekly research coverage cycle (no broad rediscovery).
 *
 * Usage:
 *   node scripts/gdi-research-coverage-run.mjs --dry-run --hotel recLuxvwwxID7U2B8
 *   node scripts/gdi-research-coverage-run.mjs --apply --hotel recLuxvwwxID7U2B8
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  getDgBase,
  isDgAirtableConfigured,
} from "../lib/group-demand-intelligence/demand-generators/airtable-client.js";
import {
  DEMAND_GENERATOR_SIGNALS_TABLE_NAME,
  MAP_DEMAND_GENERATOR_SIGNAL,
} from "../lib/group-demand-intelligence/demand-generators/airtable-field-map.js";
import {
  listTargetsForHotel,
  upsertResearchTarget,
  upsertResearchRun,
  upsertTargetRun,
  executeCoverageCycle,
  buildCustomerResearchSummary,
  formatCustomerResearchSummaryText,
  isResearchCoverageAirtableConfigured,
  RUN_TYPE,
  EXECUTION_STATUS,
} from "../lib/group-demand-intelligence/research-coverage/index.js";
import { loadOpportunities } from "../lib/group-demand-intelligence/repository.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = process.argv.includes("--apply");
const DRY = !APPLY;
const HOTEL_DEFAULT = "recLuxvwwxID7U2B8";
const HOTEL_NAMES = {
  recLuxvwwxID7U2B8: "Bethesda Marriott",
  recG66DQJKP2c0UNh: "Renaissance New York Times Square Hotel",
  recIwaP1etgx2g9nA: "Cambridge Beaches Resort & Spa",
};

function argVal(flag) {
  const i = process.argv.indexOf(flag);
  return i >= 0 ? process.argv[i + 1] : null;
}

async function loadSignals(hotelId) {
  if (!isDgAirtableConfigured()) return [];
  const base = getDgBase();
  const S = MAP_DEMAND_GENERATOR_SIGNAL;
  const out = [];
  const formula = `{${S.hotelId}} = "${String(hotelId).replace(/"/g, '\\"')}"`;
  await base(DEMAND_GENERATOR_SIGNALS_TABLE_NAME)
    .select({ filterByFormula: formula, pageSize: 100 })
    .eachPage((records, next) => {
      for (const r of records) {
        const f = r.fields || {};
        out.push({
          signalId: f[S.signalId],
          demandGeneratorId: f[S.demandGeneratorId],
          programId: f[S.programId],
          hotelId: f[S.hotelId],
          firstSeenAt: f[S.firstSeenAt],
          lastSeenAt: f[S.lastSeenAt],
          sourceUrl: f[S.sourceUrl],
        });
      }
      next();
    });
  return out;
}

async function main() {
  const hotelId = argVal("--hotel") || HOTEL_DEFAULT;
  const hotelName = HOTEL_NAMES[hotelId] || hotelId;
  const outDir = path.join(
    ROOT,
    "reports",
    "group-demand-intelligence",
    "research-coverage-v1"
  );
  fs.mkdirSync(outDir, { recursive: true });

  if (!isResearchCoverageAirtableConfigured() && APPLY) {
    throw new Error("Research coverage Airtable not configured");
  }

  let targets = [];
  if (isResearchCoverageAirtableConfigured()) {
    targets = await listTargetsForHotel(hotelId);
  }

  if (!targets.length) {
    throw new Error(
      `No research targets for ${hotelId}. Run backfill first (dry-run then --apply).`
    );
  }

  const FORCE_DUE = process.argv.includes("--force-due");
  if (FORCE_DUE) {
    const nowIso = new Date().toISOString();
    targets = targets.map((t) => ({
      ...t,
      nextResearchAt: nowIso,
    }));
  }

  const signals = await loadSignals(hotelId).catch(() => []);
  let opportunities = [];
  try {
    const loaded = loadOpportunities(hotelId);
    opportunities = Array.isArray(loaded)
      ? loaded
      : Array.isArray(loaded?.opportunities)
        ? loaded.opportunities
        : [];
  } catch {
    opportunities = [];
  }

  const cycle = executeCoverageCycle({
    hotelId,
    hotelName,
    targets,
    evidence: { signals, opportunities },
    runType: RUN_TYPE.WEEKLY,
    notes: "Controlled weekly coverage LEDGER ONLY — no web research; use gdi-weekly-discovery-run for RESEARCHED fetches",
  });

  // Ledger-only: do not claim RESEARCHED. Customer summary stays null unless
  // a separate weekly discovery orchestrator run produced RESEARCHED target runs.
  const persist = { run: null, targetRuns: [], targets: [] };
  if (APPLY) {
    const runUpsert = await upsertResearchRun(cycle.run, { dryRun: false });
    persist.run = runUpsert;
    const runRecordId = runUpsert.recordId;

    for (const t of cycle.updatedTargets) {
      const u = await upsertResearchTarget(t, { dryRun: false });
      persist.targets.push({ targetId: t.targetId, action: u.action, recordId: u.recordId });
    }

    const targetRecordById = new Map(
      persist.targets.map((x) => [x.targetId, x.recordId])
    );
    // Also map from original targets
    for (const t of targets) {
      if (t.airtableRecordId) targetRecordById.set(t.targetId, t.airtableRecordId);
    }

    for (const tr of cycle.targetRuns) {
      const withLinks = {
        ...tr,
        researchRunRecordId: runRecordId,
        researchTargetRecordId:
          tr.researchTargetRecordId || targetRecordById.get(tr.targetId) || null,
      };
      const u = await upsertTargetRun(withLinks, { dryRun: false });
      persist.targetRuns.push({
        targetRunId: tr.targetRunId,
        executionStatus: tr.executionStatus,
        resultType: tr.resultType,
        action: u.action,
        recordId: u.recordId,
      });
    }
  }

  const researched = cycle.researchedTargetRuns;
  const customerSummary = buildCustomerResearchSummary({
    run: cycle.run,
    researchedTargetRuns: researched,
    activeTargetCount: targets.filter((t) => t.status === "ACTIVE").length,
  });

  const newOpps = opportunities.filter(
    (o) =>
      o.firstDiscoveredRunId === cycle.run.runId ||
      o.firstSeenRunId === cycle.run.runId
  );
  const provenance = {
    newOpportunities: newOpps.length,
    withRunId: newOpps.filter((o) => o.firstDiscoveredRunId || o.firstSeenRunId).length,
    withTargetId: newOpps.filter((o) => o.discoveryTargetId).length,
    withTargetRunId: newOpps.filter((o) => o.discoveryTargetRunId).length,
    falseNew: 0,
  };

  const report = {
    generatedAt: new Date().toISOString(),
    apply: APPLY,
    hotelId,
    hotelName,
    runId: cycle.run.runId,
    runStatus: cycle.run.status,
    coverage: {
      targetsActive: targets.filter((t) => t.status === "ACTIVE").length,
      targetsDue: cycle.plan.due.length,
      targetsResearched: cycle.run.targetsCompleted,
      targetsMissed: cycle.run.targetsMissed,
      targetsFailed: cycle.run.targetsFailed,
      targetsNoChange: cycle.run.targetsNoChange,
      coveragePct: cycle.coveragePct,
    },
    signals: {
      newSignals: cycle.run.newSignals,
      updatedSignals: cycle.run.updatedSignals,
    },
    opportunities: {
      new: cycle.run.newOpportunities,
      updated: cycle.run.updatedOpportunities,
      note: "Counts from evidence classification only — no opportunities created by this cycle",
    },
    learning: cycle.learning,
    customerSummary,
    customerSummaryText: formatCustomerResearchSummaryText(customerSummary),
    provenance,
    persist: APPLY ? persist : { dryRun: true },
    researchedSample: researched.slice(0, 10).map((tr) => ({
      targetId: tr.targetId,
      resultType: tr.resultType,
      executionStatus: tr.executionStatus,
    })),
  };

  const outPath = path.join(
    outDir,
    `COVERAGE_RUN_${hotelId}_${cycle.run.runId}${DRY ? "_DRY" : ""}.json`
  );
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log(
    JSON.stringify(
      {
        ok: true,
        outPath,
        runId: cycle.run.runId,
        coverage: report.coverage,
        customerSummaryText: report.customerSummaryText,
        apply: APPLY,
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
