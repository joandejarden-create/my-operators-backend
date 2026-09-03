#!/usr/bin/env node
/**
 * Rebake Core published Trends + Prior Run from canonical comparable periods.
 * No provider reruns. Dry-run by default; pass --apply to write.
 */

import { readFileSync, writeFileSync, existsSync } from "fs";
import { join } from "path";
import { ADP_CERTIFIED_PROPERTY_IDS } from "../lib/ai-demand-positioning/contracts/adp-certified-property-cohort-v1.js";
import { loadAllPeriods, loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import {
  buildTrendsFromCanonicalResolution,
  resolveCanonicalComparablePeriods,
} from "../lib/ai-demand-positioning/metrics/canonical-comparable-period-resolver-v1.js";
import { buildOptionalExecutiveMetrics } from "../lib/ai-demand-positioning/metrics/optional-executive-metrics.js";
import { MEASUREMENT_CONTRACT_V1_1 } from "../lib/ai-demand-positioning/measurement-assurance/adp-measurement-contract-v1-1-candidate.js";

const apply = process.argv.includes("--apply");
const PUB = join(process.cwd(), "data/ai-demand-positioning/published");

function loadJson(path) {
  return JSON.parse(readFileSync(path, "utf8"));
}

const summary = [];
for (const propertyId of ADP_CERTIFIED_PROPERTY_IDS) {
  const manifestPath = join(PUB, propertyId, "manifest.json");
  if (!existsSync(manifestPath)) {
    summary.push({ propertyId, ok: false, error: "missing_manifest" });
    continue;
  }
  const manifest = loadJson(manifestPath);
  const reportPath = join(PUB, propertyId, manifest.reportFile || `report-${manifest.latestPeriodId}.json`);
  const report = loadJson(reportPath);
  const profile = loadPropertyProfile(propertyId);
  const scenarios = buildScenarioUniverse(profile);
  const allPeriods = loadAllPeriods(propertyId);
  const currentPeriodId = report.periodId || report.payload?.period?.periodId || manifest.latestPeriodId;
  const resolution = resolveCanonicalComparablePeriods({
    allPeriods,
    scenarios,
    currentPeriodId,
  });
  const useV11 =
    report.measurementContractVersion === MEASUREMENT_CONTRACT_V1_1 ||
    report.payload?.measurementContractVersion === MEASUREMENT_CONTRACT_V1_1 ||
    (resolution.comparablePeriods || []).some(
      (p) =>
        p?.measurementContractVersionActiveForCorrection === MEASUREMENT_CONTRACT_V1_1 ||
        p?.measurementContractVersion === MEASUREMENT_CONTRACT_V1_1
    );
  const trends = buildTrendsFromCanonicalResolution({
    resolution,
    scenarios,
    propertyProfile: profile,
    measurementContractVersion: useV11 ? MEASUREMENT_CONTRACT_V1_1 : null,
  });
  const currentPeriod = resolution.currentPeriod;
  const em = buildOptionalExecutiveMetrics(currentPeriod, scenarios, profile, { allPeriods });
  const before = {
    trendsLen: report.payload?.trends?.length || 0,
    hasCurrentVsPrior: Boolean(report.payload?.executiveMetrics?.currentVsPrior),
  };
  report.payload.trends = trends;
  report.payload.executiveMetrics = {
    ...(report.payload.executiveMetrics || {}),
    ...(em || {}),
    currentVsPrior: em?.currentVsPrior || report.payload.executiveMetrics?.currentVsPrior || null,
  };
  if (apply) {
    writeFileSync(reportPath, JSON.stringify(report, null, 2));
  }
  summary.push({
    propertyId,
    ok: true,
    apply,
    before,
    after: {
      trendsLen: trends?.length || 0,
      trendDates: (trends || []).map((t) => String(t.date || "").slice(0, 10)),
      priorPeriodId: em?.currentVsPrior?.priorComparablePeriodId || null,
      deltas: em?.currentVsPrior?.deltas || null,
    },
  });
}

console.log(JSON.stringify({ apply, summary }, null, 2));
if (!apply) {
  console.log("Dry-run only. Re-run with --apply to write published reports.");
}
