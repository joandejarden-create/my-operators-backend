/**
 * GDI Weekly Discovery V1.2 — full lane harvest + contact resolution.
 *
 *   node scripts/gdi-weekly-discovery-v1-2-run.mjs --dry-run --hotel recLuxvwwxID7U2B8
 *   node scripts/gdi-weekly-discovery-v1-2-run.mjs --apply --hotel recLuxvwwxID7U2B8 --limit 8 --force-due
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
import { runWeeklyDiscoveryOrchestratorV12 } from "../lib/group-demand-intelligence/weekly-discovery-orchestrator-v1-2.js";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import {
  summarizeContactCoverage,
  classifyContactTier,
} from "../lib/group-demand-intelligence/contact-tiers-v1-2.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const APPLY = process.argv.includes("--apply");
const FORCE_DUE = process.argv.includes("--force-due");
const HOTEL_DEFAULT = "recLuxvwwxID7U2B8";

function argVal(flag) {
  const i = process.argv.indexOf(flag);
  return i >= 0 ? process.argv[i + 1] : null;
}

/** Resolve display name from hotel config when present — no hotel hardcodes. */
function resolveHotelDisplayName(hotelId) {
  const cfgPath = path.join(
    ROOT,
    "config/group-demand-intelligence/hotels",
    `${hotelId}.json`
  );
  try {
    if (!fs.existsSync(cfgPath)) return null;
    const cfg = JSON.parse(fs.readFileSync(cfgPath, "utf8"));
    return cfg.displayName || cfg.hotelName || null;
  } catch {
    return null;
  }
}

function gitSha() {
  try {
    return execSync("git rev-parse --short HEAD", { encoding: "utf8" }).trim();
  } catch {
    return null;
  }
}

async function main() {
  const hotelId = argVal("--hotel") || HOTEL_DEFAULT;
  const hotelName = resolveHotelDisplayName(hotelId) || hotelId;
  const limit = Math.max(1, Number(argVal("--limit") || 8) || 8);
  const contactLimit = Math.max(0, Number(argVal("--contact-limit") || 8) || 8);
  const outDir = path.join(
    ROOT,
    "reports/group-demand-intelligence/weekly-discovery-v1-2"
  );
  fs.mkdirSync(outDir, { recursive: true });

  if (!isResearchCoverageAirtableConfigured()) {
    throw new Error("Research coverage Airtable not configured");
  }

  let targets = await listTargetsForHotel(hotelId);
  if (!targets.length) throw new Error("No research targets — run coverage backfill first");

  if (FORCE_DUE) {
    const nowIso = new Date().toISOString();
    targets = targets.map((t) => ({ ...t, nextResearchAt: nowIso }));
  }

  const duePreview = FORCE_DUE
    ? targets.filter((t) => t.status !== "PAUSED" && t.status !== "RETIRED")
    : planCoverage({ targets }).due;
  const withUrl = duePreview.filter((t) => t.primarySourceUrl);
  const ordered = [...withUrl, ...duePreview.filter((t) => !t.primarySourceUrl)];

  const beforeDoc = await loadOpportunitiesCanonical(hotelId);
  const beforeCust = filterCustomerFacingOpportunities(beforeDoc.opportunities || []);
  const beforeCoverage = summarizeContactCoverage(beforeCust);

  const result = await runWeeklyDiscoveryOrchestratorV12({
    hotelId,
    hotelName,
    targets: ordered,
    existingOpps: beforeDoc.opportunities || [],
    limit,
    forceDue: true,
    dryRun: !APPLY,
    peMaxQueries: 0,
    resolveContacts: true,
    contactBackfillLimit: contactLimit,
    promoteTrue: true,
    gitSha: gitSha(),
  });

  const persist = { run: null, targetRuns: [], targets: [] };
  if (APPLY) {
    const runUpsert = await upsertResearchRun(result.run, { dryRun: false });
    persist.run = runUpsert;
    for (const t of result.updatedTargets) {
      const u = await upsertResearchTarget(t, { dryRun: false });
      persist.targets.push({ targetId: t.targetId, action: u.action, recordId: u.recordId });
    }
    const targetRecordById = new Map();
    for (const t of targets) {
      if (t.airtableRecordId) targetRecordById.set(t.targetId, t.airtableRecordId);
    }
    for (const x of persist.targets) {
      if (x.recordId) targetRecordById.set(x.targetId, x.recordId);
    }
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
      persist.targetRuns.push({
        targetRunId: tr.targetRunId,
        executionStatus: tr.executionStatus,
        resultType: tr.resultType,
        playbook: tr.playbook,
        action: u.action,
      });
    }
  }

  const afterDoc = APPLY
    ? await loadOpportunitiesCanonical(hotelId)
    : { opportunities: result.workingOpps || beforeDoc.opportunities };
  const afterCust = filterCustomerFacingOpportunities(afterDoc.opportunities || []);
  const afterCoverage = summarizeContactCoverage(afterCust);

  const researched = result.targetRuns.filter(
    (tr) => tr.executionStatus === EXECUTION_STATUS.RESEARCHED
  );
  const customerSummary = buildCustomerResearchSummary({
    run: result.run,
    researchedTargetRuns: researched,
    activeTargetCount: targets.filter((t) => t.status === "ACTIVE").length,
  });

  const blanks = afterCoverage.rows.filter(
    (r) => r.contactTier === "NO_CONTACT" || r.contactTier === "GENERIC_ONLY"
  );

  const report = {
    generatedAt: new Date().toISOString(),
    apply: APPLY,
    hotelId,
    hotelName,
    runId: result.run.runId,
    lanesExecuted: result.lanesExecuted,
    funnel: result.metrics,
    contactBefore: beforeCoverage.counts,
    contactAfter: afterCoverage.counts,
    gradesAfter: afterCoverage.grades,
    blanksRemaining: blanks.slice(0, 30),
    contactReport: {
      researched: result.contactReport.researched,
      namedAdded: result.contactReport.namedAdded,
      functionalAdded: result.contactReport.functionalAdded,
      improved: result.contactReport.improved,
      stillBlank: result.contactReport.stillBlank,
      surfeCalls: result.contactReport.surfeCalls,
    },
    promotions: result.promotionResults.map((p) => ({
      action: p.action,
      id: p.opportunity?.id,
      title: p.opportunity?.title,
      dryRun: p.dryRun,
    })),
    customerSummary,
    customerSummaryText: formatCustomerResearchSummaryText(customerSummary),
    persist: APPLY ? persist : { dryRun: true },
  };

  const outPath = path.join(
    outDir,
    `WEEKLY_V12_${hotelId}_${result.run.runId}${APPLY ? "" : "_DRY"}.json`
  );
  fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
  console.log(
    JSON.stringify(
      {
        ok: true,
        outPath,
        apply: APPLY,
        funnel: report.funnel,
        lanesExecuted: report.lanesExecuted,
        contactBefore: report.contactBefore,
        contactAfter: report.contactAfter,
        customerSummaryText: report.customerSummaryText,
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
