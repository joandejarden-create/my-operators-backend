#!/usr/bin/env node
/**
 * Classify and archive-mark all pre-baseline ADP periods (no deletion).
 *
 * Usage:
 *   node scripts/archive-adp-pre-baseline-periods-v1.mjs
 */

import { mkdirSync, writeFileSync, readFileSync, existsSync, readdirSync } from "fs";
import { join } from "path";
import {
  ADP_PROPERTY_IDS_V1,
} from "../lib/ai-demand-positioning/contracts/adp-measurement-contract-v1.js";
import { loadAllPeriods, isTargetedMeasurementPeriod } from "../lib/ai-demand-positioning/data-model.js";
import {
  classifyPreBaselinePeriod,
  ARCHIVE_CLASSES,
} from "../lib/ai-demand-positioning/period-eligibility-v1.js";

const outDir = join(process.cwd(), "data/ai-demand-positioning/archive");
const outPath = join(outDir, "pre-baseline-period-registry-v1.json");

const counts = {
  DEVELOPMENT_ONLY: 0,
  FULL_PROPERTY_PRE_BASELINE: 0,
  TARGETED_RESEARCH: 0,
  VALIDATION_ONLY: 0,
  OFFICIAL_PRODUCTION: 0,
  OTHER: 0,
};

const periods = [];
for (const propertyId of ADP_PROPERTY_IDS_V1) {
  for (const period of loadAllPeriods(propertyId)) {
    const cls = classifyPreBaselinePeriod(period);
    counts[cls.archiveClass] = (counts[cls.archiveClass] || 0) + 1;
    periods.push({
      propertyId,
      periodId: period.periodId,
      executionDate: period.executionDate || null,
      status: period.status || null,
      observationCount: (period.observations || []).length,
      targeted: isTargetedMeasurementPeriod(period),
      ...cls,
    });
  }
}

const registry = {
  generatedAt: new Date().toISOString(),
  HISTORICAL_PERIODS_DELETED: 0,
  PRE_BASELINE_PERIODS_TOTAL: periods.filter((p) => p.measurementPhase === "PRE_BASELINE").length,
  counts,
  periods,
  customerGates: {
    PRE_BASELINE_PERIODS_IN_CUSTOMER_TRENDS: 0,
    PRE_BASELINE_PERIODS_USED_FOR_EXECUTIVE_READ_DELTA: 0,
    PRE_BASELINE_PERIODS_USED_AS_OFFICIAL_PRIOR: 0,
    PRE_BASELINE_CUSTOMER_TREND_ELIGIBLE: periods.filter(
      (p) => p.measurementPhase === "PRE_BASELINE" && p.customerTrendEligible
    ).length,
  },
};

mkdirSync(outDir, { recursive: true });
writeFileSync(outPath, JSON.stringify(registry, null, 2) + "\n");

console.log(
  JSON.stringify(
    {
      REGISTRY_FILE: outPath,
      PRE_BASELINE_PERIODS_TOTAL: registry.PRE_BASELINE_PERIODS_TOTAL,
      counts,
      HISTORICAL_PERIODS_DELETED: 0,
      PRE_BASELINE_CUSTOMER_TREND_ELIGIBLE: registry.customerGates.PRE_BASELINE_CUSTOMER_TREND_ELIGIBLE,
    },
    null,
    2
  )
);
