/**
 * GDI Weekly Discovery Orchestrator run — real bounded research (not ledger-only).
 *
 * Usage:
 *   node scripts/gdi-weekly-discovery-run.mjs --dry-run --hotel recLuxvwwxID7U2B8
 *   node scripts/gdi-weekly-discovery-run.mjs --apply --hotel recLuxvwwxID7U2B8 --limit 15
 *
 * Coverage remains the audit ledger. This script executes playbook fetches for due targets
 * and persists RESEARCHED Target Runs. Does not broad-expand the market. Does not run Surfe/PDL.
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { execSync } from "child_process";
import {
  listTargetsForHotel,
  upsertResearchTarget,
  upsertResearchRun,
  upsertTargetRun,
  buildCustomerResearchSummary,
  formatCustomerResearchSummaryText,
  isResearchCoverageAirtableConfigured,
  EXECUTION_STATUS,
  planCoverage,
} from "../lib/group-demand-intelligence/research-coverage/index.js";
import {
  runWeeklyDiscoveryOrchestrator,
  routeTargetToPlaybook,
} from "../lib/group-demand-intelligence/weekly-discovery-orchestrator.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = process.argv.includes("--apply");
const DRY = !APPLY;
const FORCE_DUE = process.argv.includes("--force-due");
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

function gitSha() {
  try {
    return execSync("git rev-parse --short HEAD", { encoding: "utf8" }).trim();
  } catch {
    return null;
  }
}

function laneFromPlaybook(playbook) {
  const p = String(playbook || "");
  if (p === "DEMAND_GENERATOR") return "Demand Generators";
  if (p === "VENUE_PARTNERSHIP" || p === "PRIVATE_EVENT_SIGNAL") return "Private Events / Venue";
  if (p === "SPORTS_HOUSING") return "Sports";
  if (p === "TRAINING_PROGRAM") return "Training";
  if (p === "GOVERNMENT_PROJECT") return "Government / Projects";
  if (p === "EVENT_FUTURE_CYCLE" || p === "LODGING_HOUSING") return "New Opportunities / Programs";
  return "Other";
}

async function main() {
  const hotelId = argVal("--hotel") || HOTEL_DEFAULT;
  const hotelName = HOTEL_NAMES[hotelId] || hotelId;
  const limit = Math.max(1, Number(argVal("--limit") || 20) || 20);
  const outDir = path.join(
    ROOT,
    "reports",
    "group-demand-intelligence",
    "weekly-discovery-v1-1"
  );
  fs.mkdirSync(outDir, { recursive: true });

  if (!isResearchCoverageAirtableConfigured()) {
    throw new Error("Research coverage Airtable not configured");
  }

  let targets = await listTargetsForHotel(hotelId);
  if (!targets.length) {
    throw new Error(
      `No research targets for ${hotelId}. Run coverage backfill first.`
    );
  }

  if (FORCE_DUE) {
    const nowIso = new Date().toISOString();
    targets = targets.map((t) => ({ ...t, nextResearchAt: nowIso }));
  }

  // Prefer targets with a primary source URL for real fetch research
  const duePreview = FORCE_DUE
    ? targets.filter((t) => t.status !== "PAUSED" && t.status !== "RETIRED")
    : planCoverage({ targets }).due;
  const withUrl = duePreview.filter((t) => t.primarySourceUrl);
  const withoutUrl = duePreview.filter((t) => !t.primarySourceUrl);
  const ordered = [...withUrl, ...withoutUrl];

  const result = await runWeeklyDiscoveryOrchestrator({
    hotelId,
    hotelName,
    targets: ordered,
    limit,
    forceDue: true, // already filtered to due (or force-due) above
    gitSha: gitSha(),
  });

  const researched = result.targetRuns.filter(
    (tr) => tr.executionStatus === EXECUTION_STATUS.RESEARCHED
  );
  const customerSummary = buildCustomerResearchSummary({
    run: result.run,
    researchedTargetRuns: researched,
    activeTargetCount: targets.filter((t) => t.status === "ACTIVE").length,
  });

  const byLane = {};
  for (const tr of result.targetRuns) {
    const t = targets.find((x) => x.targetId === tr.targetId) || {};
    const playbook = tr.playbook || routeTargetToPlaybook(t);
    const lane = laneFromPlaybook(playbook);
    if (!byLane[lane]) {
      byLane[lane] = {
        due: 0,
        researched: 0,
        candidates: 0,
        watch: 0,
        true: 0,
        promoted: 0,
        neu: 0,
        fetches: 0,
        queries: 0,
      };
    }
    byLane[lane].due += 1;
    if (tr.executionStatus === EXECUTION_STATUS.RESEARCHED) byLane[lane].researched += 1;
    byLane[lane].fetches += Number(tr.fetchesUsed) || 0;
    byLane[lane].queries += Number(tr.queriesUsed) || 0;
    byLane[lane].candidates += Number(tr.updatedSignalCount) || 0;
  }

  const persist = { run: null, targetRuns: [], targets: [] };
  if (APPLY) {
    const runUpsert = await upsertResearchRun(
      {
        ...result.run,
        notes: `${result.run.notes || ""}; Weekly Discovery Orchestrator V1.1 real fetches`,
      },
      { dryRun: false }
    );
    persist.run = runUpsert;
    const runRecordId = runUpsert.recordId;

    for (const t of result.updatedTargets) {
      const u = await upsertResearchTarget(t, { dryRun: false });
      persist.targets.push({
        targetId: t.targetId,
        action: u.action,
        recordId: u.recordId,
      });
    }

    const targetRecordById = new Map();
    for (const t of targets) {
      if (t.airtableRecordId) targetRecordById.set(t.targetId, t.airtableRecordId);
    }
    for (const x of persist.targets) {
      if (x.recordId) targetRecordById.set(x.targetId, x.recordId);
    }

    for (const tr of result.targetRuns) {
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
        playbook: tr.playbook,
        queriesUsed: tr.queriesUsed,
        fetchesUsed: tr.fetchesUsed,
        action: u.action,
        recordId: u.recordId,
      });
    }
  }

  const report = {
    generatedAt: new Date().toISOString(),
    apply: APPLY,
    hotelId,
    hotelName,
    architecture: {
      orchestrator: "weekly-discovery-orchestrator.js",
      coverageRole: "ledger / audit / cadence",
      discoveryRole: "bounded playbook URL research for due targets",
      promotionService: "promote-qualified-opportunity.js (not invoked in monitor-only cycle)",
    },
    runId: result.run.runId,
    runStatus: result.run.status,
    funnel: {
      targetsDue: result.metrics.targetsDue,
      targetsResearched: result.metrics.targetsResearched,
      queries: result.metrics.queries,
      fetches: result.metrics.fetches,
      coveragePct: result.coveragePct,
      newSignals: result.metrics.newSignals,
      updatedSignals: result.metrics.updatedSignals,
      true: result.metrics.true,
      promoted: result.metrics.promoted,
      neu: result.metrics.neu,
    },
    byLane,
    customerSummary,
    customerSummaryText: formatCustomerResearchSummaryText(customerSummary),
    sample: result.targetRuns.slice(0, 12).map((tr) => ({
      targetId: tr.targetId,
      playbook: tr.playbook,
      executionStatus: tr.executionStatus,
      resultType: tr.resultType,
      fetchesUsed: tr.fetchesUsed,
      queriesUsed: tr.queriesUsed,
      lastResultSummary: tr.lastResultSummary,
    })),
    persist: APPLY ? persist : { dryRun: true },
  };

  const outPath = path.join(
    outDir,
    `WEEKLY_DISCOVERY_${hotelId}_${result.run.runId}${DRY ? "_DRY" : ""}.json`
  );
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log(
    JSON.stringify(
      {
        ok: true,
        outPath,
        runId: result.run.runId,
        funnel: report.funnel,
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
