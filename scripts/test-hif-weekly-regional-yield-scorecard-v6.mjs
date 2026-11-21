#!/usr/bin/env node
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  attributeHotelGeography,
  buildFounderExecutiveOverview,
  buildResearchInvestmentRecommendations,
  buildV5BaselineWeeklySnapshot,
  buildWeeklySnapshot,
  computeWeeklyDelta,
  emptyWeeklyMetrics,
  exportFounderOverviewMarkdown,
  identifyRegionalResearchGaps,
  isoWeekId,
  saveWeeklySnapshot,
  WEEKLY_METRIC_KEYS,
  COVERAGE_QUALIFICATION_LADDER,
  SOURCE_FAMILY_YIELD,
} from "../lib/hotel-intelligence/sec-edgar/hif-weekly-regional-yield-scorecard-v6.js";
import { buildEmptyRegionalRegister, recordYieldObservation } from "../lib/hotel-intelligence/sec-edgar/hif-regional-coverage-yield-register-v6.js";

const errors = [];

if (!String(isoWeekId("2026-11-21")).match(/^2026-W\d{2}$/)) errors.push("ISO_WEEK_BAD");

const geo = attributeHotelGeography({
  hotelCountry: "ES",
  city: "Barcelona",
  sourceJurisdiction: "SG",
});
if (geo.hotelCountry !== "ES") errors.push("GEO_HOTEL_COUNTRY");
if (geo.intensiveRegion !== "SOUTHERN_EUROPE") errors.push("GEO_REGION");
if (geo.sourceJurisdiction !== "SG") errors.push("GEO_SOURCE_JURISDICTION");

if (WEEKLY_METRIC_KEYS.length < 18) errors.push("METRICS_TOO_FEW");
if (Object.keys(COVERAGE_QUALIFICATION_LADDER).length !== 9) errors.push("LADDER_COUNT");
if (SOURCE_FAMILY_YIELD.length < 10) errors.push("SOURCE_FAMILIES");

const tmp = fs.mkdtempSync(path.join(os.tmpdir(), "hif-weekly-"));
const baseline = buildV5BaselineWeeklySnapshot({ weekId: "2026-W47" });
if (baseline.global.newlyDiscoveredHotelsThisWeek !== 0) {
  errors.push("BASELINE_MUST_NOT_CLAIM_NEW_DISCOVERIES");
}
if (baseline.global.distinctHotelsDiscovered !== 446) errors.push("BASELINE_DISCOVERED");
if (baseline.customerBlocked !== true) errors.push("CUSTOMER_BLOCKED");

const saved = saveWeeklySnapshot(baseline, tmp);
if (!saved.ok) errors.push("SAVE_FAILED");
const blocked = saveWeeklySnapshot(baseline, tmp);
if (blocked.ok || blocked.error !== "IMMUTABLE_SNAPSHOT_EXISTS") {
  errors.push("IMMUTABLE_ENFORCEMENT");
}

const week2 = buildWeeklySnapshot({
  weekId: "2026-W48",
  asOf: "2026-11-28",
  global: {
    ...emptyWeeklyMetrics(),
    distinctHotelsDiscovered: 450,
    verifiedFranchiseExpirations: 80,
    newBrandConversionTargets: 137,
    researchCostUsd: 12.5,
  },
});
const delta = computeWeeklyDelta(baseline, week2, {
  distinctHotelsDiscovered: {
    newlyDiscoveredRecords: 4,
    reclassifiedNotNew: 0,
  },
  verifiedFranchiseExpirations: {
    newlyDiscoveredRecords: 1,
    improvedExistingRecords: 0,
  },
});
if (delta.metrics.distinctHotelsDiscovered.netChange !== 4) errors.push("DELTA_NET");
if (delta.metrics.distinctHotelsDiscovered.newlyDiscoveredRecords !== 4) errors.push("DELTA_NEW");
if (delta.rule.includes("reclassification") === false) errors.push("DELTA_RULE");

const register = buildEmptyRegionalRegister("2026-11-21");
recordYieldObservation(register, {
  country: "US",
  hotelsDiscovered: 446,
  contractDatesVerified: 79,
  documentsExamined: 1,
});
const gaps = identifyRegionalResearchGaps(baseline, register);
if (!gaps.regionsRequiringNewSourceAdaptersOrStrategies.some((r) => r.region === "WESTERN_EUROPE")) {
  errors.push("GAP_EUROPE");
}
if (!gaps.rule.includes("zero findings")) errors.push("GAP_RULE");

const rec = buildResearchInvestmentRecommendations(baseline, gaps);
if (!rec.recommendedNextDiscoveryAllocations?.length) errors.push("RECS_EMPTY");

const overview = buildFounderExecutiveOverview(baseline, delta, gaps, rec);
const md = exportFounderOverviewMarkdown(overview);
if (!md.includes("CUSTOMER_BLOCKED")) errors.push("MD_BLOCKED");
if (!md.includes("2026-W47")) errors.push("MD_WEEK");

try {
  fs.rmSync(tmp, { recursive: true, force: true });
} catch {
  /* ignore */
}

if (errors.length) {
  console.error("FAIL", errors);
  process.exit(1);
}
console.log(
  JSON.stringify(
    {
      ok: true,
      week: baseline.weekId,
      metrics: WEEKLY_METRIC_KEYS.length,
      ladder: Object.keys(COVERAGE_QUALIFICATION_LADDER).length,
      sourceFamilies: SOURCE_FAMILY_YIELD.length,
      geoSpainFromSgFiling: geo,
    },
    null,
    2,
  ),
);
