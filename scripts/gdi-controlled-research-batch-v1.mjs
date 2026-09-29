/**
 * GDI Controlled Research Batch V1
 *
 * Hotels (exact): AC A Coruña, Spice Island, Cambridge Beaches, NOW NOW NoHo, W Rome.
 * Bethesda excluded.
 *
 * Pass A — Target ledger: Weekly Discovery V1.1 force-due → Research Run + Target Runs
 * Pass B — Opportunity depth: Weekly Discovery V1.2 on priority subset (bounded)
 *
 *   node scripts/gdi-controlled-research-batch-v1.mjs --dry-run
 *   node scripts/gdi-controlled-research-batch-v1.mjs --apply
 *   node scripts/gdi-controlled-research-batch-v1.mjs --apply --hotel=rec2PVBDavppGpenm
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { execSync } from "node:child_process";
import {
  listTargetsForHotel,
  upsertResearchTarget,
  upsertResearchRun,
  upsertTargetRun,
  isResearchCoverageAirtableConfigured,
  EXECUTION_STATUS,
  planCoverage,
} from "../lib/group-demand-intelligence/research-coverage/index.js";
import { runWeeklyDiscoveryOrchestrator } from "../lib/group-demand-intelligence/weekly-discovery-orchestrator.js";
import { runWeeklyDiscoveryOrchestratorV12 } from "../lib/group-demand-intelligence/weekly-discovery-orchestrator-v1-2.js";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import {
  assessSubstantiveResearchEvidence,
  classifyGdiResearchMaturity,
} from "../lib/group-demand-intelligence/research-maturity-v1.js";
import Airtable from "airtable";
import {
  MAP_RESEARCH_RUN as RUN,
  MAP_RESEARCH_TARGET_RUN as TR,
} from "../lib/group-demand-intelligence/research-coverage/airtable-field-map.js";
import {
  CANONICAL_INTELLIGENCE_BASE_ID,
  assertNotLegacyMvpCanonicalBase,
  getGdiOpportunitiesAirtableBaseId,
} from "../lib/decision-outcomes/airtable-base.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(
  ROOT,
  "reports/group-demand-intelligence/controlled-research-batch-v1"
);
const APPLY = process.argv.includes("--apply");
const hotelFilter =
  process.argv.find((a) => a.startsWith("--hotel="))?.slice(8) || null;

const BETHESDA = "recLuxvwwxID7U2B8";

/** Exact batch — never include Bethesda. */
const BATCH = [
  {
    name: "AC Hotel A Coruña",
    hpc: "rec2PVBDavppGpenm",
    ledgerLimit: 14,
    depthLimit: 6,
    contactLimit: 4,
    laneKeywords: [
      "port",
      "maritime",
      "industrial",
      "corporate",
      "train",
      "health",
      "university",
      "science",
      "tech",
      "association",
      "sport",
      "govern",
    ],
  },
  {
    name: "Spice Island Beach Resort",
    hpc: "recKRJjcPnb4tVDDS",
    ledgerLimit: 14,
    depthLimit: 6,
    contactLimit: 4,
    laneKeywords: [
      "incentive",
      "retreat",
      "wedding",
      "yacht",
      "marine",
      "corporate",
      "association",
      "production",
      "luxury",
      "advisor",
    ],
  },
  {
    name: "Cambridge Beaches Resort & Spa",
    hpc: "recIwaP1etgx2g9nA",
    ledgerLimit: 22,
    depthLimit: 8,
    contactLimit: 5,
    laneKeywords: [
      "insurance",
      "reinsur",
      "financial",
      "retreat",
      "incentive",
      "wedding",
      "marine",
      "sail",
      "corporate",
      "association",
    ],
  },
  {
    name: "NOW NOW NOHO",
    hpc: "recGkME49yYuxQl0u",
    ledgerLimit: 22,
    depthLimit: 8,
    contactLimit: 5,
    laneKeywords: [
      "fashion",
      "production",
      "agency",
      "corporate",
      "train",
      "creative",
      "association",
      "university",
      "alumni",
      "medical",
      "pharma",
      "brand",
    ],
  },
  {
    name: "W Rome",
    hpc: "rece0or38cxo3Fymb",
    ledgerLimit: 19,
    depthLimit: 8,
    contactLimit: 4,
    preferUnresearched: true,
    laneKeywords: [
      "fashion",
      "luxury",
      "corporate",
      "production",
      "retreat",
      "association",
      "pharma",
      "medical",
      "diplomat",
      "incentive",
      "agency",
    ],
  },
];

process.env.WEBHOUND_UNAVAILABLE = "true";
process.env.WEBHOUND_DISABLED = "1";
process.env.CONTACT_INTELLIGENCE_PAID_ENRICHMENT_ENABLED = "0";

const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
const intel =
  getGdiOpportunitiesAirtableBaseId() ||
  process.env.ADP_AIRTABLE_BASE_ID ||
  CANONICAL_INTELLIGENCE_BASE_ID;
assertNotLegacyMvpCanonicalBase(intel, { surface: "controlled-research-batch" });
if (intel !== CANONICAL_INTELLIGENCE_BASE_ID) {
  throw new Error(`wrong_intel_base:${intel}`);
}
const intelBase = new Airtable({ apiKey: token }).base(intel);

function gitSha() {
  try {
    return execSync("git rev-parse --short HEAD", { encoding: "utf8" }).trim();
  } catch {
    return null;
  }
}

function esc(s) {
  return String(s || "").replace(/"/g, '\\"');
}

async function listBy(table, field, value, fields) {
  const rows = [];
  await intelBase(table)
    .select({
      pageSize: 100,
      filterByFormula: `{${field}} = "${esc(value)}"`,
      fields,
    })
    .eachPage((recs, next) => {
      for (const r of recs) rows.push({ id: r.id, fields: r.fields || {} });
      next();
    });
  return rows;
}

function orderTargets(targets, { preferUnresearched, laneKeywords }) {
  const scored = targets
    .filter((t) => t && t.status !== "PAUSED" && t.status !== "RETIRED")
    .map((t) => {
      const blob = `${t.canonicalName || ""} ${t.targetType || ""} ${t.entityType || ""} ${t.reasonMonitored || ""}`.toLowerCase();
      let score = 0;
      if (preferUnresearched && !(t.lastResearchedAt || Number(t.successfulRuns) > 0)) {
        score += 100;
      }
      for (const kw of laneKeywords || []) {
        if (blob.includes(kw.toLowerCase())) score += 10;
      }
      if (t.primarySourceUrl) score += 5;
      if (t.priority === "HIGH") score += 3;
      return { t, score };
    });
  scored.sort((a, b) => b.score - a.score || String(a.t.canonicalName || "").localeCompare(String(b.t.canonicalName || "")));
  return scored.map((x) => x.t);
}

async function snapshotHotel(hpc) {
  const targets = await listTargetsForHotel(hpc);
  const runs = await listBy("GDI Research Runs", RUN.hotelId, hpc, [
    RUN.runId,
    RUN.notes,
    RUN.queries,
    RUN.fetches,
    RUN.payloadJson,
  ]);
  const trs = await listBy("GDI Research Target Runs", TR.hotelId, hpc, [
    TR.targetRunId,
    TR.targetId,
    TR.executionStatus,
    TR.queriesUsed,
    TR.fetchesUsed,
    TR.sourceCount,
  ]);
  const doc = await loadOpportunitiesCanonical(hpc);
  const customer = filterCustomerFacingOpportunities(doc.opportunities || []);
  const evidence = assessSubstantiveResearchEvidence({
    runs: runs.map((r) => ({
      runId: r.fields[RUN.runId],
      notes: r.fields[RUN.notes],
      queries: r.fields[RUN.queries] || 0,
      fetches: r.fields[RUN.fetches] || 0,
      payloadJson: r.fields[RUN.payloadJson],
    })),
    targetRuns: trs.map((r) => ({
      targetId: r.fields[TR.targetId],
      executionStatus: r.fields[TR.executionStatus],
      queriesUsed: r.fields[TR.queriesUsed] || 0,
      fetchesUsed: r.fields[TR.fetchesUsed] || 0,
      sourceCount: r.fields[TR.sourceCount] || 0,
    })),
    customerReady: customer.length,
  });
  const researched = new Set(evidence.researchedTargetIds);
  for (const t of targets) {
    if (t.lastResearchedAt || Number(t.successfulRuns) > 0) researched.add(t.targetId);
  }
  const { maturity, systemState, rationale } = classifyGdiResearchMaturity({
    totalTargets: targets.length,
    targetsResearched: researched.size,
    customerReady: customer.length,
    evidence,
  });
  return {
    targets,
    targetCount: targets.length,
    researchedCount: researched.size,
    coveragePct: targets.length
      ? Math.round((researched.size / targets.length) * 1000) / 10
      : 0,
    runCount: runs.length,
    targetRunCount: trs.length,
    targetRunIds: trs.map((r) => r.id),
    oppCount: (doc.opportunities || []).length,
    customerReady: customer.length,
    customerOpps: customer,
    maturity,
    systemState,
    rationale,
    evidenceStrength: evidence.strength,
  };
}

async function persistOrchestratorResult(result, targets, label) {
  if (!APPLY) {
    return {
      dryRun: true,
      researched: result.targetRuns.filter(
        (tr) => tr.executionStatus === EXECUTION_STATUS.RESEARCHED
      ).length,
      targetRuns: result.targetRuns.map((tr) => ({
        targetRunId: tr.targetRunId,
        targetId: tr.targetId,
        executionStatus: tr.executionStatus,
        action: "PREVIEW",
      })),
    };
  }
  const runUpsert = await upsertResearchRun(
    {
      ...result.run,
      notes: `${result.run.notes || ""} | controlled_batch_v1:${label}`.trim(),
    },
    { dryRun: false }
  );
  const persistTargets = [];
  for (const t of result.updatedTargets || []) {
    const u = await upsertResearchTarget(t, { dryRun: false });
    persistTargets.push({ targetId: t.targetId, action: u.action, recordId: u.recordId });
  }
  const targetRecordById = new Map();
  for (const t of targets) {
    if (t.airtableRecordId) targetRecordById.set(t.targetId, t.airtableRecordId);
  }
  for (const x of persistTargets) {
    if (x.recordId) targetRecordById.set(x.targetId, x.recordId);
  }
  const persistTr = [];
  for (const tr of result.targetRuns) {
    const u = await upsertTargetRun(
      {
        ...tr,
        researchRunRecordId: runUpsert.recordId,
        researchTargetRecordId:
          tr.researchTargetRecordId || targetRecordById.get(tr.targetId) || null,
      },
      { dryRun: false }
    );
    persistTr.push({
      targetRunId: tr.targetRunId,
      targetId: tr.targetId,
      executionStatus: tr.executionStatus,
      resultType: tr.resultType,
      queriesUsed: tr.queriesUsed || 0,
      fetchesUsed: tr.fetchesUsed || 0,
      action: u.action,
      recordId: u.recordId,
    });
  }
  return {
    dryRun: false,
    runRecordId: runUpsert.recordId,
    runId: result.run.runId,
    researched: persistTr.filter((t) => t.executionStatus === EXECUTION_STATUS.RESEARCHED)
      .length,
    targetRuns: persistTr,
    targets: persistTargets,
  };
}

async function runHotel(spec) {
  if (spec.hpc === BETHESDA) throw new Error("BETHESDA_FORBIDDEN");
  const t0 = Date.now();
  console.error(`\n=== ${spec.name} (${spec.hpc}) apply=${APPLY} ===`);
  const before = await snapshotHotel(spec.hpc);
  console.error(
    `before maturity=${before.maturity} targets=${before.targetCount} researched=${before.researchedCount} tr=${before.targetRunCount} ready=${before.customerReady}`
  );

  let targets = await listTargetsForHotel(spec.hpc);
  if (!targets.length) throw new Error(`no_targets_${spec.hpc}`);

  // Force due for controlled batch
  const nowIso = new Date().toISOString();
  targets = targets.map((t) => ({ ...t, nextResearchAt: nowIso }));
  const ordered = orderTargets(targets, spec);

  // PASS A — ledger (V1.1)
  const ledgerSlice = ordered.slice(0, spec.ledgerLimit);
  console.error(`[ledger] researching ${ledgerSlice.length} targets via V1.1`);
  const ledgerResult = await runWeeklyDiscoveryOrchestrator({
    hotelId: spec.hpc,
    hotelName: spec.name,
    targets: ledgerSlice,
    limit: ledgerSlice.length,
    forceDue: true,
    gitSha: gitSha(),
  });
  const ledgerPersist = await persistOrchestratorResult(ledgerResult, targets, "ledger_v1_1");

  // PASS B — depth (V1.2) on priority subset
  const refreshed = await listTargetsForHotel(spec.hpc);
  const depthOrdered = orderTargets(
    refreshed.map((t) => ({ ...t, nextResearchAt: nowIso })),
    spec
  ).slice(0, spec.depthLimit);
  console.error(`[depth] researching ${depthOrdered.length} targets via V1.2`);
  const beforeDoc = await loadOpportunitiesCanonical(spec.hpc);
  const depthResult = await runWeeklyDiscoveryOrchestratorV12({
    hotelId: spec.hpc,
    hotelName: spec.name,
    targets: depthOrdered,
    existingOpps: beforeDoc.opportunities || [],
    limit: depthOrdered.length,
    forceDue: true,
    dryRun: !APPLY,
    peMaxQueries: 0,
    resolveContacts: true,
    contactBackfillLimit: spec.contactLimit,
    promoteTrue: true,
    gitSha: gitSha(),
  });
  const depthPersist = await persistOrchestratorResult(depthResult, refreshed, "depth_v1_2");

  const after = await snapshotHotel(spec.hpc);
  const missingLedger = ledgerPersist.targetRuns.filter(
    (tr) =>
      tr.executionStatus === EXECUTION_STATUS.RESEARCHED &&
      APPLY &&
      !tr.recordId
  );
  const hotelReport = {
    hotel: spec.name,
    hpc: spec.hpc,
    apply: APPLY,
    runtimeMs: Date.now() - t0,
    before: {
      maturity: before.maturity,
      targets: before.targetCount,
      researched: before.researchedCount,
      coveragePct: before.coveragePct,
      targetRuns: before.targetRunCount,
      customerReady: before.customerReady,
      opps: before.oppCount,
    },
    after: {
      maturity: after.maturity,
      targets: after.targetCount,
      researched: after.researchedCount,
      coveragePct: after.coveragePct,
      targetRuns: after.targetRunCount,
      customerReady: after.customerReady,
      opps: after.oppCount,
      rationale: after.rationale,
    },
    ledger: {
      metrics: ledgerResult.metrics || {
        queries: ledgerResult.run?.queries,
        fetches: ledgerResult.run?.fetches,
      },
      persist: ledgerPersist,
    },
    depth: {
      metrics: depthResult.metrics,
      lanesExecuted: depthResult.lanesExecuted,
      promotions: (depthResult.promotionResults || []).slice(0, 40),
      persist: depthPersist,
    },
    targetRunPersistence: {
      targetsResearchedThisBatch:
        (ledgerPersist.researched || 0) + (depthPersist.researched || 0),
      // depth may re-research overlap; unique by targetId
      uniqueTargetsResearched: [
        ...new Set([
          ...ledgerPersist.targetRuns.map((t) => t.targetId),
          ...depthPersist.targetRuns.map((t) => t.targetId),
        ]),
      ].length,
      created: [...ledgerPersist.targetRuns, ...depthPersist.targetRuns].filter(
        (t) => t.action === "CREATE"
      ).length,
      updated: [...ledgerPersist.targetRuns, ...depthPersist.targetRuns].filter(
        (t) => t.action === "UPDATE"
      ).length,
      missing: missingLedger.length,
      recordIds: [...ledgerPersist.targetRuns, ...depthPersist.targetRuns]
        .map((t) => t.recordId)
        .filter(Boolean),
    },
    customerReadyOpps: after.customerOpps.map((o) => ({
      id: o.id,
      title: o.title || o.opportunityName || o.name,
      who: o.primaryContactName || o.contactGrade || o.whoState || null,
      whyNow: o.whyNow || o.summaryWhyNow || null,
      action: o.recommendedAction || null,
    })),
  };

  console.error(
    `after maturity=${after.maturity} coverage=${after.coveragePct}% tr=${after.targetRunCount} ready=${after.customerReady} missingTR=${missingLedger.length}`
  );
  return hotelReport;
}

async function main() {
  if (!isResearchCoverageAirtableConfigured()) {
    throw new Error("Research coverage Airtable not configured");
  }
  fs.mkdirSync(OUT, { recursive: true });
  const hotels = BATCH.filter(
    (h) =>
      !hotelFilter ||
      h.hpc === hotelFilter ||
      h.name.toLowerCase().includes(hotelFilter.toLowerCase())
  );
  if (hotels.some((h) => h.hpc === BETHESDA)) {
    throw new Error("BETHESDA_MUST_NOT_BE_IN_BATCH");
  }

  const bethesdaBefore = await snapshotHotel(BETHESDA);
  const results = [];
  for (const h of hotels) {
    results.push(await runHotel(h));
  }
  const bethesdaAfter = await snapshotHotel(BETHESDA);
  const bethesdaChanged =
    bethesdaBefore.targetRunCount !== bethesdaAfter.targetRunCount ||
    bethesdaBefore.customerReady !== bethesdaAfter.customerReady ||
    bethesdaBefore.runCount !== bethesdaAfter.runCount;

  const summary = {
    generatedAt: new Date().toISOString(),
    apply: APPLY,
    gitSha: gitSha(),
    hotels: results,
    bethesdaProtection: {
      changed: bethesdaChanged,
      before: {
        ready: bethesdaBefore.customerReady,
        targetRuns: bethesdaBefore.targetRunCount,
        runs: bethesdaBefore.runCount,
      },
      after: {
        ready: bethesdaAfter.customerReady,
        targetRuns: bethesdaAfter.targetRunCount,
        runs: bethesdaAfter.runCount,
      },
    },
  };
  const outPath = path.join(OUT, APPLY ? "BATCH_APPLY.json" : "BATCH_DRY.json");
  fs.writeFileSync(outPath, JSON.stringify(summary, null, 2));
  console.log(
    JSON.stringify(
      {
        ok: true,
        outPath,
        apply: APPLY,
        hotels: results.map((r) => ({
          hotel: r.hotel,
          before: r.before.maturity,
          after: r.after.maturity,
          coverage: r.after.coveragePct,
          ready: r.after.customerReady,
          missingTR: r.targetRunPersistence.missing,
        })),
        bethesdaChanged,
      },
      null,
      2
    )
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
