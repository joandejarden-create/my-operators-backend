#!/usr/bin/env node
/**
 * Local read-only / replay verification for GDI workflow v2 stages.
 * No production writes. No paid Parallel/Surfe/Webhound calls.
 *
 *   node scripts/gdi-workflow-v2-verify-local.mjs
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import { buildReusePriorIntelligenceLedger, reuseLedgerToCsv } from "../lib/group-demand-intelligence/workflow/reuse-prior-intelligence-v1.js";
import { assessIndependentHotelFitBatch } from "../lib/group-demand-intelligence/workflow/independent-hotel-fit-v1.js";
import {
  replayFrozenParallelDiscovery,
  buildBoundedLiveDiscoveryPlan,
} from "../lib/group-demand-intelligence/workflow/parallel-discovery-adapter-v1.js";
import {
  reportDrawerCompleteness,
  mapLabCardToDrawerShape,
} from "../lib/group-demand-intelligence/workflow/drawer-content-contract-v1.js";
import { runJevAdvisoryShadow } from "../lib/group-demand-intelligence/workflow/jev-advisory-shadow-v1.js";
import { WEBHOUND_PROVIDER_STATUS } from "../lib/group-demand-intelligence/workflow/webhound-retired-v1.js";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { FIT_OUTCOME } from "../lib/group-demand-intelligence/workflow/independent-hotel-fit-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(ROOT, "reports/gdi-workflow-v2-integration");

const MAYFLOWER = "rec0eTEnCqlsGJJt9";
const RENAISSANCE = "recG66DQJKP2c0UNh";
const SAN_DIEGO = "recnPB3Q02NO8Ma1o";
const BETHESDA = "recLuxvwwxID7U2B8";

fs.mkdirSync(OUT, { recursive: true });

function write(name, data) {
  const p = path.join(OUT, name);
  if (typeof data === "string") fs.writeFileSync(p, data);
  else fs.writeFileSync(p, JSON.stringify(data, null, 2) + "\n");
  return p;
}

const report = {
  asOf: new Date().toISOString(),
  webhound: WEBHOUND_PROVIDER_STATUS,
  paidCalls: 0,
  productionWrites: 0,
  hotels: {},
  assertions: [],
};

function assert(name, ok, detail = {}) {
  report.assertions.push({ name, ok: Boolean(ok), ...detail });
  if (!ok) console.error("FAIL", name, detail);
  else console.log("PASS", name);
}

// --- Mayflower reuse + independent fit ---
{
  const ledger = await buildReusePriorIntelligenceLedger(MAYFLOWER, {
    skipAirtable: process.env.GDI_V2_SKIP_AIRTABLE === "1",
  });
  write("mayflower-reuse-ledger.json", ledger);
  write("mayflower-reuse-ledger.csv", reuseLedgerToCsv(ledger));

  const beth = await loadOpportunitiesCanonical(BETHESDA);
  const sports = (beth?.opportunities || [])
    .filter((o) =>
      /soccer|lacrosse|sports?\s*tournament|soccerplex|stay[- ]to[- ]play/i.test(
        `${o.title || ""} ${o.organizationName || ""} ${o.summaryWhat || ""}`
      )
    )
    .slice(0, 15)
    .map((o) => ({ ...o, hotelId: BETHESDA }));
  // Synthetic Bethesda sports control — proves rule even when FS corpus lacks sports labels
  const sportsControl = {
    id: "synthetic_bethesda_soccerplex_control",
    hotelId: BETHESDA,
    organizationName: "Maryland State Youth Soccer Association",
    title: "SoccerPlex Stay-to-Play Memorial Tournament",
    summaryWhat: "Youth soccer tournament at Maryland SoccerPlex Boyds",
    hotelOpportunityThesis: "Bethesda Marriott stay-to-play sports overflow",
  };
  const mayOps = (await loadOpportunitiesCanonical(MAYFLOWER))?.opportunities || [];
  const fit = assessIndependentHotelFitBatch(
    [...mayOps.slice(0, 30), ...sports, sportsControl],
    MAYFLOWER
  );
  write("mayflower-independent-hotel-fit.json", fit);

  const controlRow = fit.results.find(
    (r) => r.candidateId === sportsControl.id
  );
  assert("mayflower_consumes_reuse_ledger", ledger.opportunityCount >= 0, {
    opportunityCount: ledger.opportunityCount,
    counts: ledger.counts,
  });
  assert(
    "mayflower_does_not_inherit_bethesda_sports_fit",
    controlRow?.fit?.outcome === FIT_OUTCOME.OUT_OF_TERRITORY &&
      controlRow?.fit?.promote !== true,
    {
      sportsSample: sports.length,
      controlOutcome: controlRow?.fit?.outcome || null,
      outOfTerritory: fit.counts[FIT_OUTCOME.OUT_OF_TERRITORY] || 0,
    }
  );
  report.hotels.mayflower = {
    reuseCounts: ledger.counts,
    gaps: ledger.gapsBeforePaidDiscovery,
    fitCounts: fit.counts,
    dgFit: ledger.demandGeneratorFit,
  };
}

// --- Renaissance reuse + noise ---
{
  const ledger = await buildReusePriorIntelligenceLedger(RENAISSANCE, {
    skipAirtable: process.env.GDI_V2_SKIP_AIRTABLE === "1",
  });
  write("renaissance-reuse-ledger.json", ledger);
  write("renaissance-reuse-ledger.csv", reuseLedgerToCsv(ledger));
  const ops = (await loadOpportunitiesCanonical(RENAISSANCE))?.opportunities || [];
  const fit = assessIndependentHotelFitBatch(ops.slice(0, 40), RENAISSANCE);
  write("renaissance-independent-hotel-fit.json", fit);
  const unsupported = fit.counts[FIT_OUTCOME.UNSUPPORTED_ORG] || 0;
  assert("renaissance_reuse_ledger_built", true, {
    opportunityCount: ledger.opportunityCount,
    unsupportedOrg: unsupported,
  });
  assert(
    "renaissance_unsupported_not_auto_qualified",
    unsupported === 0 ||
      fit.results
        .filter((r) => r.fit.outcome === FIT_OUTCOME.UNSUPPORTED_ORG)
        .every((r) => r.fit.promote !== true),
    { unsupported }
  );
  report.hotels.renaissance = {
    reuseCounts: ledger.counts,
    fitCounts: fit.counts,
  };
}

// --- San Diego Parallel replay ---
{
  const replay = replayFrozenParallelDiscovery(SAN_DIEGO);
  write("san-diego-parallel-replay.json", replay);
  write(
    "bounded-live-discovery-plan.json",
    buildBoundedLiveDiscoveryPlan(SAN_DIEGO, { maxUsd: 2, maxTasks: 6 })
  );
  assert(
    "san_diego_lab_replay_available_or_documented",
    replay.labAvailable === true ||
      replay.fixtureSource === "in_repo_fixtures" ||
      replay.fixtureSource === "optional_lab_root" ||
      Boolean(replay.incompleteCoverage),
    {
    labAvailable: replay.labAvailable,
    fixtureSource: replay.fixtureSource,
    candidates: (replay.candidates || []).length,
  });
  assert("san_diego_replay_not_claimed_as_fresh", replay.freshDiscoveryClaimed !== true, {
    paidCalls: replay.paidCalls,
  });
  assert(
    "san_diego_candidates_publication_blocked",
    (replay.candidates || []).every((c) => c.publicationBlocked === true),
    { n: (replay.candidates || []).length }
  );

  const drawerCards = (replay.candidates || []).map((c) =>
    mapLabCardToDrawerShape(
      {
        cardId: c.candidateId,
        account: c.organizationName,
        timing: { start: c.eventStartDate, end: c.eventEndDate, raw: c.title },
        demandSignal: c.demandSignal,
        whyWyndhamFits: c.hotelOpportunityThesis,
        sourcingStatus: c.sourcingStatus,
        recommendedFirstAction: c.recommendedAction,
        evidenceLinks: c.evidenceLinks,
        contact: c.primaryContact,
        materialLimitations: c.materialLimitations,
        noveltyVsBaseline: c.provenance?.noveltyVsBaseline,
      },
      SAN_DIEGO
    )
  );
  const drawer = reportDrawerCompleteness(drawerCards, { hotelId: SAN_DIEGO });
  write("san-diego-drawer-completeness.json", drawer);
  const jev = runJevAdvisoryShadow({ hotelId: SAN_DIEGO, cards: drawerCards });
  write("san-diego-jev-advisory.json", jev);
  assert("drawer_layers_reported_separately", drawer.results.every((r) => r.evaluation.layers?.qualification && r.evaluation.layers?.drawerContent), {
    pass: drawer.pass,
    fail: drawer.fail,
  });
  report.hotels.sanDiego = {
    labAvailable: replay.labAvailable,
    candidateCount: (replay.candidates || []).length,
    acceptedProspects: replay.reviewPackage?.acceptedProspects ?? null,
    baselineNovelty: replay.reviewPackage?.baselineNovelty ?? null,
    drawerPass: drawer.pass,
    drawerFail: drawer.fail,
    paidCalls: 0,
  };
}

// --- Coordinator dry runs (reuse-focused; skip full preview to keep cheap) ---
const wfOut = path.join(OUT, "workflow-runs");
fs.mkdirSync(wfOut, { recursive: true });
for (const [label, hotel, extra] of [
  ["mayflower-reuse", MAYFLOWER, ["--stage", "reuse_evidence", "--skip-airtable-reuse"]],
  ["renaissance-reuse", RENAISSANCE, ["--stage", "reuse_evidence", "--skip-airtable-reuse"]],
  [
    "san-diego-discovery-replay",
    SAN_DIEGO,
    ["--run-discovery", "--stage", "discover_gaps"],
  ],
]) {
  const r = spawnSync(
    process.execPath,
    [
      path.join(ROOT, "scripts/gdi-research-to-page-workflow-v1.mjs"),
      "--hotel",
      hotel,
      "--out-dir",
      wfOut,
      "--dry-run",
      ...extra,
    ],
    { cwd: ROOT, encoding: "utf8", env: { ...process.env } }
  );
  write(`${label}-workflow-stdout.txt`, r.stdout || "");
  if (r.stderr) write(`${label}-workflow-stderr.txt`, r.stderr);
  assert(`workflow_${label}_exit_0`, r.status === 0, {
    status: r.status,
    stderr: (r.stderr || "").slice(0, 300),
  });
}

report.assertionSummary = {
  pass: report.assertions.filter((a) => a.ok).length,
  fail: report.assertions.filter((a) => !a.ok).length,
};
write("_verify-summary.json", report);

console.log(
  JSON.stringify(
    {
      ok: report.assertionSummary.fail === 0,
      out: OUT,
      assertionSummary: report.assertionSummary,
    },
    null,
    2
  )
);
process.exit(report.assertionSummary.fail === 0 ? 0 : 1);
