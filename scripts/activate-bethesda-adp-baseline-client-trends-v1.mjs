#!/usr/bin/env node
/**
 * Bethesda Pilot 001 — activate official Oct 1 ADP baseline for client Trends (State 2).
 *
 * DATA LAW: does NOT re-run, recalculate, or overwrite observation corpus / frozen KPI rates.
 * Only flips customer-visibility metadata and bakes Trends from the already-published
 * executiveMetrics + realityGap snapshot (immutable baseline values).
 *
 * Usage:
 *   node scripts/activate-bethesda-adp-baseline-client-trends-v1.mjs --dry-run
 *   node scripts/activate-bethesda-adp-baseline-client-trends-v1.mjs --apply
 */

import { createHash } from "crypto";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "fs";
import { join } from "path";
import { loadPeriod, savePeriod } from "../lib/ai-demand-positioning/data-model.js";

const APPLY = process.argv.includes("--apply");
const DRY_RUN = !APPLY;

const PROPERTY_ID = "adp_bethesda_marriott";
const PERIOD_ID = "adp_period_adp_bethesda_marriott_20260909091016_9f3a60";
const BASELINE_ID = "BETHESDA_ADP_BASELINE_V1";
const BASELINE_DATE = "2026-10-01";
const BASELINE_MARKER = "ADP_BETHESDA_MARRIOTT_BASELINE_PERIOD_001";

const ROOT = process.cwd();
const reportPath = join(
  ROOT,
  "data/ai-demand-positioning/published/adp_bethesda_marriott",
  `report-${PERIOD_ID}.json`
);
const baselinePath = join(ROOT, "data/pilots/bethesda-marriott-001/adp-october-baseline-v1.json");
const outDir = join(ROOT, "reports/bethesda-pilot/day2/2026-10-02");

function sha256(buf) {
  return createHash("sha256").update(buf).digest("hex");
}

function round1(n) {
  return Math.round(Number(n) * 10) / 10;
}

function buildFrozenTrendPoint(report) {
  const payload = report.payload || {};
  const em = payload.executiveMetrics || {};
  const rg = payload.realityGap || {};
  let propertyRealityCoverage = null;
  if (rg.totalAttributes > 0 && rg.recognizedCount != null) {
    propertyRealityCoverage = round1((Number(rg.recognizedCount) / Number(rg.totalAttributes)) * 100);
  }
  return {
    periodId: PERIOD_ID,
    date: BASELINE_DATE,
    baselineDate: BASELINE_DATE,
    baselineId: BASELINE_ID,
    baselineMarker: BASELINE_MARKER,
    propertyRealityCoverage,
    scenarioPresenceRate: em.scenarioPresence?.rate ?? null,
    considerationRate: em.considerationRate?.rate ?? null,
    demandCaptureRate: payload.demandCapture?.overallRate ?? null,
    providerCount: 4,
    observationCount: em.considerationRate?.comparableObservations ?? null,
    measurementContractVersion:
      report.measurementContractVersion || payload.period?.measurementContractVersion || null,
    role: "current",
    isOfficialBaseline: true,
    monitoringStatus: "ACTIVE",
    nextFormalRemeasurement: "2026-11",
    nextFormalRemeasurementLabel: "November 2026",
  };
}

if (!existsSync(reportPath)) {
  console.error("MISSING_PUBLISHED_REPORT", reportPath);
  process.exit(1);
}

const reportRaw = readFileSync(reportPath);
const reportHashBefore = sha256(reportRaw);
const report = JSON.parse(reportRaw.toString("utf8"));
const period = loadPeriod(PERIOD_ID);
if (!period) {
  console.error("MISSING_RUNTIME_PERIOD", PERIOD_ID);
  process.exit(1);
}

const observationHashBefore = sha256(
  JSON.stringify((period.observations || []).map((o) => o.observationId || o.id || o))
);

const emBefore = report.payload?.executiveMetrics
  ? JSON.parse(JSON.stringify(report.payload.executiveMetrics))
  : null;
const frozenTrend = buildFrozenTrendPoint(report);

const periodMetaBefore = {
  certified: period.certified,
  customerVisible: period.customerVisible,
  customerTrendEligible: period.customerTrendEligible,
  publicationBlocked: period.publicationBlocked,
  publicationBlockReason: period.publicationBlockReason || null,
  externalShareDistributed: period.externalShareDistributed,
};

const periodPatch = {
  certified: true,
  certifiedAt: period.certifiedAt || "2026-09-10T11:19:14.589Z",
  customerVisible: true,
  customerTrendEligible: true,
  publicationBlocked: false,
  publicationBlockReason: null,
  externalShareDistributed: true,
  baselinePeriod: true,
  baselineMarker: BASELINE_MARKER,
  baselineId: BASELINE_ID,
  baselineDate: BASELINE_DATE,
  officialPeriod: true,
  measurementPhase: "OFFICIAL_PRODUCTION",
  firstOfficialPropertyPeriod: true,
};

const payloadPeriod = {
  ...(report.payload?.period || {}),
  periodId: PERIOD_ID,
  officialPeriod: true,
  certified: true,
  baselinePeriod: true,
  baselineMarker: BASELINE_MARKER,
  baselineId: BASELINE_ID,
  baselineDate: BASELINE_DATE,
  firstOfficialPropertyPeriod: true,
  customerVisible: true,
  customerTrendEligible: true,
  measurementPhase: "OFFICIAL_PRODUCTION",
};

const nextReport = {
  ...report,
  payload: {
    ...report.payload,
    period: payloadPeriod,
    trends: [frozenTrend],
    baselineMonitoring: {
      state: "BASELINE_ONLY",
      baselineId: BASELINE_ID,
      baselineDate: BASELINE_DATE,
      monitoringStatus: "ACTIVE",
      nextFormalRemeasurement: "2026-11",
      nextFormalRemeasurementLabel: "November 2026",
      officialLaterMeasurementExists: false,
    },
  },
};

const emAfter = nextReport.payload.executiveMetrics;
const emUnchanged = JSON.stringify(emBefore) === JSON.stringify(emAfter);

const result = {
  ok: true,
  mode: DRY_RUN ? "DRY_RUN" : "APPLY",
  propertyId: PROPERTY_ID,
  periodId: PERIOD_ID,
  baselineId: BASELINE_ID,
  baselineDate: BASELINE_DATE,
  periodMetaBefore,
  periodMetaAfter: periodPatch,
  frozenTrend,
  executiveMetricsUnchanged: emUnchanged,
  observationHashBefore,
  reportHashBefore,
  DATA_LAW: {
    observationsRewritten: false,
    executiveMetricsRewritten: !emUnchanged,
    trendsSource: "PUBLISHED_SNAPSHOT_EXECUTIVE_METRICS_AND_REALITY_GAP",
    liveRebuildForbidden: true,
  },
};

if (!emUnchanged) {
  console.error("ABORT: executiveMetrics would change — refusing apply");
  process.exit(1);
}

if (APPLY) {
  Object.assign(period, periodPatch);
  savePeriod(period);
  writeFileSync(reportPath, JSON.stringify(nextReport, null, 2) + "\n");

  if (existsSync(baselinePath)) {
    const baseline = JSON.parse(readFileSync(baselinePath, "utf8"));
    baseline.clientTrendsActivatedAt = new Date().toISOString();
    baseline.clientTrendsActivation =
      "BAKED_FROZEN_TRENDS_FROM_PUBLISHED_SNAPSHOT_NO_REMEASURE";
    writeFileSync(baselinePath, JSON.stringify(baseline, null, 2) + "\n");
  }

  const periodAfter = loadPeriod(PERIOD_ID);
  const observationHashAfter = sha256(
    JSON.stringify((periodAfter.observations || []).map((o) => o.observationId || o.id || o))
  );
  result.observationHashAfter = observationHashAfter;
  result.observationsUnchanged = observationHashBefore === observationHashAfter;
  result.reportHashAfter = sha256(readFileSync(reportPath));
  if (!result.observationsUnchanged) {
    console.error("ABORT POST-CHECK: observations changed");
    process.exit(1);
  }
}

mkdirSync(outDir, { recursive: true });
const outPath = join(outDir, "ADP_BASELINE_CLIENT_TRENDS_ACTIVATION.json");
writeFileSync(outPath, JSON.stringify(result, null, 2) + "\n");
console.log(JSON.stringify(result, null, 2));
console.log(DRY_RUN ? "\nDRY_RUN only — re-run with --apply to write." : `\nAPPLIED → ${outPath}`);
