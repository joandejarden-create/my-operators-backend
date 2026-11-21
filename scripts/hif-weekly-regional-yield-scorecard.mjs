#!/usr/bin/env node
/**
 * Generate / refresh HIF V6 weekly regional coverage & research yield scorecard.
 *
 * Default: seed immutable V5 baseline week snapshot + founder export (no live research).
 * Flags:
 *   --week YYYY-Www
 *   --baseline-v5 (default when no prior inputs)
 *   --allow-overwrite (only for non-immutable regen in local WIP — prefer new week ids)
 */
import fs from "node:fs";
import path from "node:path";
import {
  buildEmptyRegionalRegister,
  recordYieldObservation,
  saveRegionalRegister,
} from "../lib/hotel-intelligence/sec-edgar/hif-regional-coverage-yield-register-v6.js";
import {
  buildFounderExecutiveOverview,
  buildResearchInvestmentRecommendations,
  buildV5BaselineWeeklySnapshot,
  computeWeeklyDelta,
  exportFounderOverviewMarkdown,
  identifyRegionalResearchGaps,
  isoWeekId,
  listWeeklySnapshots,
  loadWeeklySnapshot,
  saveWeeklyDelta,
  saveWeeklySnapshot,
  ensureWeeklyScorecardLayout,
  defaultWeeklyScorecardRoot,
} from "../lib/hotel-intelligence/sec-edgar/hif-weekly-regional-yield-scorecard-v6.js";

const args = process.argv.slice(2);
const get = (flag) => {
  const i = args.indexOf(flag);
  return i >= 0 ? args[i + 1] : null;
};
const has = (flag) => args.includes(flag);

const cwd = process.cwd();
const weekId = get("--week") || "2026-W47";
const allowOverwrite = has("--allow-overwrite");

ensureWeeklyScorecardLayout(cwd);

const snapshot = buildV5BaselineWeeklySnapshot({
  weekId,
  asOf: "2026-11-21",
});
if (allowOverwrite) snapshot.allowOverwrite = true;

const saved = saveWeeklySnapshot(snapshot, cwd);
if (!saved.ok) {
  console.error(JSON.stringify(saved, null, 2));
  process.exit(1);
}

// Align regional register with baseline (US / NORTH_AMERICA covered)
const register = buildEmptyRegionalRegister(snapshot.asOf);
recordYieldObservation(register, {
  country: "US",
  region: "NORTH_AMERICA",
  hotelsDiscovered: 446,
  hotelsIdentityVerified: 150,
  ownersVerified: 0,
  agreementsIdentified: 81,
  contractDatesVerified: 79,
  noticePeriodsVerified: 2,
  qualifiedCommercialOpportunities: 139,
  documentsExamined: 0,
  costUsd: 0,
  lastResearchedDate: snapshot.asOf,
  sourceType: "SEC_CMBS",
  agreementFamily: "FRANCHISE",
  sourceAvailability: "FREE_OFFICIAL",
});
const registerPath = path.join(
  defaultWeeklyScorecardRoot(cwd),
  "ledgers",
  "REGIONAL_COVERAGE_YIELD_REGISTER.json",
);
saveRegionalRegister(register, registerPath);

const gaps = identifyRegionalResearchGaps(snapshot, register);
const recommendations = buildResearchInvestmentRecommendations(snapshot, gaps);

const weeks = listWeeklySnapshots(cwd);
const prevId = weeks.filter((w) => w < weekId).pop() || null;
const previous = prevId ? loadWeeklySnapshot(prevId, cwd) : null;
const delta = computeWeeklyDelta(previous, snapshot, {
  // Baseline seed: cumulative inventory; do not claim newly discovered
  distinctHotelsDiscovered: { newlyDiscoveredRecords: 0, reclassifiedNotNew: 0 },
});
const deltaPath = saveWeeklyDelta(delta, cwd);

const overview = buildFounderExecutiveOverview(snapshot, delta, gaps, recommendations);
const exportDir = path.join(defaultWeeklyScorecardRoot(cwd), "exports");
fs.writeFileSync(path.join(exportDir, `${weekId}-FOUNDER_OVERVIEW.json`), JSON.stringify(overview, null, 2));
fs.writeFileSync(
  path.join(exportDir, `${weekId}-FOUNDER_OVERVIEW.md`),
  exportFounderOverviewMarkdown(overview),
);
fs.writeFileSync(path.join(exportDir, `${weekId}-GAPS.json`), JSON.stringify(gaps, null, 2));
fs.writeFileSync(
  path.join(exportDir, `${weekId}-RECOMMENDATIONS.json`),
  JSON.stringify(recommendations, null, 2),
);

console.log(
  JSON.stringify(
    {
      ok: true,
      weekId,
      isoWeekNow: isoWeekId(),
      snapshot: saved.path,
      register: registerPath,
      delta: deltaPath,
      previousWeekId: prevId,
      intensiveRegionsNeedingAdapters:
        gaps.regionsRequiringNewSourceAdaptersOrStrategies?.length ?? 0,
      customerBlocked: true,
    },
    null,
    2,
  ),
);
