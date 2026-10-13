#!/usr/bin/env node
/**
 * Promote Bethesda Oct 2 ADP period to official pilot baseline V2,
 * publish snapshot, supersede Sept as PRE_PILOT_REFERENCE, bake client Trends.
 *
 * Usage (after live remasurement completes):
 *   node scripts/promote-bethesda-adp-oct2-baseline-v2.mjs --period <periodId> --dry-run
 *   node scripts/promote-bethesda-adp-oct2-baseline-v2.mjs --period <periodId> --apply
 */

import "../load-env.js";
import { createHash } from "crypto";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "fs";
import { join } from "path";
import { execSync } from "child_process";
import {
  loadPeriod,
  savePeriod,
  loadPropertyProfile,
} from "../lib/ai-demand-positioning/data-model.js";
import {
  buildPublishedSnapshotBundle,
  savePublishedSnapshotBundle,
} from "../lib/ai-demand-positioning/published-snapshot.js";
import { resolveCensusRecordIdForPublish } from "../lib/ai-demand-positioning/census-link-registry.js";
import {
  BETHESDA_OCT2_BASELINE_ID,
  BETHESDA_OCT2_BASELINE_MARKER,
  BETHESDA_CONTROL_QUERY_SET_ID,
  BETHESDA_SEP_PERIOD_ID,
} from "../lib/ai-demand-positioning/execution/bethesda-marriott-oct2-baseline-remeasure-v1.js";

const APPLY = process.argv.includes("--apply");
const args = process.argv.slice(2);
const periodIdx = args.indexOf("--period");
const PERIOD_ID =
  periodIdx >= 0
    ? args[periodIdx + 1]
    : args.find((a) => a.startsWith("--period="))?.slice("--period=".length);

const PROPERTY_ID = "adp_bethesda_marriott";
const HPC = "recLuxvwwxID7U2B8";
const BASELINE_DATE = "2026-10-02";
const PILOT_START = "2026-10-01";
const OUT = join(process.cwd(), "reports/bethesda-pilot/adp-baseline/2026-10-02");

function sha256File(path) {
  return createHash("sha256").update(readFileSync(path)).digest("hex");
}

function round1(n) {
  return Math.round(Number(n) * 10) / 10;
}

function gitSha() {
  try {
    return execSync("git rev-parse HEAD", { encoding: "utf8" }).trim();
  } catch {
    return null;
  }
}

function buildFrozenTrendPoint(report, periodId) {
  const payload = report.payload || report;
  const em = payload.executiveMetrics || {};
  const rg = payload.realityGap || {};
  let propertyRealityCoverage = null;
  if (rg.totalAttributes > 0 && rg.recognizedCount != null) {
    propertyRealityCoverage = round1(
      (Number(rg.recognizedCount) / Number(rg.totalAttributes)) * 100
    );
  }
  return {
    periodId,
    date: BASELINE_DATE,
    baselineDate: BASELINE_DATE,
    baselineId: BETHESDA_OCT2_BASELINE_ID,
    baselineMarker: BETHESDA_OCT2_BASELINE_MARKER,
    pilotStartDate: PILOT_START,
    lastMeasurementDate: BASELINE_DATE,
    propertyRealityCoverage,
    scenarioPresenceRate: em.scenarioPresence?.rate ?? null,
    considerationRate: em.considerationRate?.rate ?? null,
    demandCaptureRate: payload.demandCapture?.overallRate ?? null,
    providerCount: 4,
    observationCount: em.considerationRate?.comparableObservations ?? null,
    measurementContractVersion:
      report.measurementContractVersion ||
      payload.period?.measurementContractVersion ||
      null,
    role: "current",
    isOfficialBaseline: true,
    monitoringStatus: "ACTIVE",
    nextFormalRemeasurement: "2026-11",
    nextFormalRemeasurementLabel: "November 2026",
    comparisonAnchor: "OCT2_OFFICIAL_BASELINE",
  };
}

if (!PERIOD_ID) {
  console.error("Usage: --period <adp_period_...> [--dry-run|--apply]");
  process.exit(1);
}

async function main() {
  mkdirSync(OUT, { recursive: true });
  const period = loadPeriod(PERIOD_ID);
  if (!period || period.propertyId !== PROPERTY_ID) {
    console.error("PERIOD_NOT_FOUND_OR_WRONG_PROPERTY", PERIOD_ID);
    process.exit(1);
  }

  const obs = period.observations || [];
  const expected = 63 * 4;
  const success = obs.filter(
    (o) => !o.error && (o.rawResponse || o.parsed)
  ).length;
  const completenessPct = Math.round((success / expected) * 1000) / 10;
  if (completenessPct < 80) {
    console.error("COMPLETENESS_BELOW_THRESHOLD", completenessPct);
    process.exit(2);
  }

  const profile = loadPropertyProfile(PROPERTY_ID);
  const censusRecordId = resolveCensusRecordIdForPublish(PROPERTY_ID, HPC);
  const { buildBethesdaControlScenarioUniverse } = await import(
    "../lib/ai-demand-positioning/execution/bethesda-marriott-oct2-baseline-remeasure-v1.js"
  );
  const controlScenarios = buildBethesdaControlScenarioUniverse(profile);

  // Mark period certified + customer-visible for publish
  const promoted = {
    ...period,
    certified: true,
    measurementCertified: true,
    measurementCertifiedAt: new Date().toISOString(),
    certifiedAt: new Date().toISOString(),
    baselineId: BETHESDA_OCT2_BASELINE_ID,
    baselineDate: BASELINE_DATE,
    baselineMeasurementDate: BASELINE_DATE,
    pilotStartDate: PILOT_START,
    baselineEstablishedDate: BASELINE_DATE,
    status: "OFFICIAL_BASELINE",
    immutable: true,
    customerVisible: true,
    customerTrendEligible: true,
    publicationBlocked: false,
    publicationBlockReason: null,
    externalShareDistributed: true,
    controlQuerySetId: BETHESDA_CONTROL_QUERY_SET_ID,
    priorComparablePeriod: BETHESDA_SEP_PERIOD_ID,
    novemberComparisonAnchor: true,
  };

  // Supersede Sept runtime metadata (do not alter observation timestamps)
  const sep = loadPeriod(BETHESDA_SEP_PERIOD_ID);
  let sepUpdated = null;
  if (sep) {
    sepUpdated = {
      ...sep,
      status: "PRE_PILOT_REFERENCE",
      officialBaselineRole: "SUPERSEDED_AS_OFFICIAL_BASELINE",
      supersededByBaselineId: BETHESDA_OCT2_BASELINE_ID,
      supersededByPeriodId: PERIOD_ID,
      supersededAt: new Date().toISOString(),
      immutable: true,
      customerVisible: false,
      customerTrendEligible: false,
      // Keep corpus untouched
    };
  }

  const bundle = buildPublishedSnapshotBundle({
    period: promoted,
    profile,
    censusRecordId,
    scenarios: controlScenarios,
  });
  if (!bundle.ok) {
    console.error("PUBLISH_BUNDLE_FAILED", bundle.error || bundle.message);
    process.exit(3);
  }

  // Bake Trends onto payload as a canonical ARRAY (never object-spread an array).
  const trend = buildFrozenTrendPoint(bundle.report || bundle, PERIOD_ID);
  if (bundle.report?.payload) {
    const existing = Array.isArray(bundle.report.payload.trends)
      ? bundle.report.payload.trends.filter(Boolean)
      : [];
    const withoutCurrent = existing.filter((t) => t && t.role !== "current");
    bundle.report.payload.trends = [
      ...withoutCurrent,
      {
        ...trend,
        role: "current",
        isOfficialBaseline: true,
        monitoringStatus: "ACTIVE",
        nextFormalRemeasurementLabel: "November 2026",
        priorOfficialComparable: null,
        note: "Official pilot baseline — no later official measurement yet; no period-over-period delta.",
      },
    ];
    bundle.report.payload.pilot = {
      pilotId: "Bethesda Marriott Founding Pilot 001",
      pilotStartDate: PILOT_START,
      baselineMeasurementDate: BASELINE_DATE,
      lastMeasurementDate: BASELINE_DATE,
      nextFormalRemeasurement: "November 2026",
      monitoringStatus: "ACTIVE",
      state: "BASELINE",
    };
    bundle.report.payload.labelSemantics = {
      adpLastMeasurementLabel: "Last Measurement",
      gdiLastResearchLabel: "Last Research",
    };
  }

  const pilotPointer = {
    baselineId: BETHESDA_OCT2_BASELINE_ID,
    baselineVersion: "V2",
    status: "OFFICIAL_BASELINE",
    immutable: true,
    pilotStartDate: PILOT_START,
    baselineMeasurementDate: BASELINE_DATE,
    baselineEstablishedDate: BASELINE_DATE,
    baselineDate: BASELINE_DATE,
    hotelId: HPC,
    propertyId: PROPERTY_ID,
    periodId: PERIOD_ID,
    baselineMarker: BETHESDA_OCT2_BASELINE_MARKER,
    controlQuerySetId: BETHESDA_CONTROL_QUERY_SET_ID,
    priorPeriodId: BETHESDA_SEP_PERIOD_ID,
    priorPeriodRole: "PRE_PILOT_REFERENCE",
    novemberComparisonAnchorPeriodId: PERIOD_ID,
    codeSha: gitSha(),
    completenessPct,
    observationCount: success,
    queryCount: 63,
    promotedAt: new Date().toISOString(),
  };

  const preview = {
    mode: APPLY ? "APPLY" : "DRY_RUN",
    PERIOD_ID,
    completenessPct,
    pilotPointer,
    sepSuperseded: !!sepUpdated,
    publishPeriodId: bundle.summary?.periodId || PERIOD_ID,
  };
  writeFileSync(
    join(OUT, "PROMOTE_PREVIEW.json"),
    JSON.stringify(preview, null, 2) + "\n"
  );
  console.log(JSON.stringify(preview, null, 2));

  if (!APPLY) {
    console.log("\n[DRY RUN] No writes.");
    return;
  }

  savePeriod(promoted);
  if (sepUpdated) savePeriod(sepUpdated);

  const saved = savePublishedSnapshotBundle(bundle, { seed: false });
  writeFileSync(
    join(process.cwd(), "data/pilots/bethesda-marriott-001/adp-october-baseline-v2.json"),
    JSON.stringify(pilotPointer, null, 2) + "\n"
  );

  // Keep V1 file but mark superseded
  const v1Path = join(
    process.cwd(),
    "data/pilots/bethesda-marriott-001/adp-october-baseline-v1.json"
  );
  if (existsSync(v1Path)) {
    const v1 = JSON.parse(readFileSync(v1Path, "utf8"));
    v1.status = "PRE_PILOT_REFERENCE";
    v1.officialBaselineRole = "SUPERSEDED_AS_OFFICIAL_BASELINE";
    v1.supersededByBaselineId = BETHESDA_OCT2_BASELINE_ID;
    v1.supersededByPeriodId = PERIOD_ID;
    v1.supersededAt = new Date().toISOString();
    writeFileSync(v1Path, JSON.stringify(v1, null, 2) + "\n");
  }

  const result = {
    ok: true,
    PERIOD_ID,
    OFFICIAL_BASELINE_ID: BETHESDA_OCT2_BASELINE_ID,
    saved,
    pilotPointer,
    runtimeSha: sha256File(
      join(
        process.cwd(),
        `data/ai-demand-positioning/runtime/${PERIOD_ID}.json`
      )
    ),
  };
  writeFileSync(
    join(OUT, "PROMOTE_RESULT.json"),
    JSON.stringify(result, null, 2) + "\n"
  );
  console.log("\nPROMOTED", JSON.stringify(result, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
