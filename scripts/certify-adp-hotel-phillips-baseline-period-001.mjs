#!/usr/bin/env node
/**
 * Certify Hotel Phillips first official baseline period + enable dropdown.
 *
 * Usage:
 *   node scripts/certify-adp-hotel-phillips-baseline-period-001.mjs --from-run-report
 *   node scripts/certify-adp-hotel-phillips-baseline-period-001.mjs --period <periodId>
 */

import "../load-env.js";
import { readFileSync, writeFileSync, existsSync, mkdirSync } from "fs";
import { join } from "path";
import { loadPeriod, savePeriod, loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";
import {
  buildPublishedSnapshotBundle,
  savePublishedSnapshotBundle,
} from "../lib/ai-demand-positioning/published-snapshot.js";
import {
  HOTEL_PHILLIPS_PROPERTY_ID,
  HOTEL_PHILLIPS_BASELINE_MARKER,
  loadFrozenContractHash,
} from "../lib/ai-demand-positioning/execution/hotel-phillips-baseline-period-001-v1.js";
import { MEASUREMENT_CONTRACT_VERSION } from "../lib/ai-demand-positioning/contracts/adp-measurement-contract-v1.js";

const args = process.argv.slice(2);
const fromRun = args.includes("--from-run-report");
let periodId = null;
const periodArg = args.find((a) => a.startsWith("--period="));
if (periodArg) periodId = periodArg.slice("--period=".length);
const periodIdx = args.indexOf("--period");
if (periodIdx >= 0 && args[periodIdx + 1]) periodId = args[periodIdx + 1];

const runPath = join(
  process.cwd(),
  "reports/ai-demand-positioning/adp-hotel-phillips-baseline-period-001-run.json"
);

if (fromRun) {
  if (!existsSync(runPath)) {
    console.error("Missing run report", runPath);
    process.exit(1);
  }
  const run = JSON.parse(readFileSync(runPath, "utf8"));
  periodId = run.PERIOD_ID;
}

if (!periodId) {
  console.error("Provide --from-run-report or --period <id>");
  process.exit(1);
}

const period = loadPeriod(periodId);
if (!period) {
  console.error("PERIOD_NOT_FOUND", periodId);
  process.exit(1);
}

const hash = loadFrozenContractHash();
period.certified = true;
period.certifiedAt = new Date().toISOString();
period.baselineMarker = HOTEL_PHILLIPS_BASELINE_MARKER;
period.firstOfficialPropertyPeriod = true;
period.officialPeriod = true;
period.measurementPhase = "OFFICIAL_PRODUCTION";
period.customerTrendEligible = true;
period.customerVisible = true;
period.fullProperty = true;
period.measurementContractVersion = MEASUREMENT_CONTRACT_VERSION;
period.measurementContractHash = hash;
period.priorComparablePeriod = null;
savePeriod(period);

// Enable dropdown visibility on profile fixture
const profilePath = join(
  process.cwd(),
  "fixtures/ai-demand-positioning/hotel-phillips-kansas-city-property-profile.json"
);
const profile = JSON.parse(readFileSync(profilePath, "utf8"));
profile.customerDropdownVisible = true;
writeFileSync(profilePath, JSON.stringify(profile, null, 2) + "\n");

const liveProfile = loadPropertyProfile(HOTEL_PHILLIPS_PROPERTY_ID);
const bundle = buildPublishedSnapshotBundle({ period, profile: liveProfile });
let published = null;
if (bundle.ok) {
  bundle.manifest.officialPeriod = true;
  bundle.manifest.baselineMarker = HOTEL_PHILLIPS_BASELINE_MARKER;
  bundle.manifest.firstOfficialPropertyPeriod = true;
  bundle.manifest.measurementContractVersion = MEASUREMENT_CONTRACT_VERSION;
  bundle.manifest.measurementContractHash = hash;
  bundle.manifest.certified = true;
  bundle.report.payload.period = {
    ...(bundle.report.payload.period || {}),
    officialPeriod: true,
    baselineMarker: HOTEL_PHILLIPS_BASELINE_MARKER,
    firstOfficialPropertyPeriod: true,
    measurementContractVersion: MEASUREMENT_CONTRACT_VERSION,
    measurementContractHash: hash,
    certified: true,
    priorComparablePeriod: null,
  };
  published = savePublishedSnapshotBundle(bundle, { seed: false });
}

const out = {
  ok: true,
  CERTIFIED: true,
  MARKER: "ADP_HOTEL_PHILLIPS_BASELINE_PERIOD_001_CERTIFIED",
  propertyId: HOTEL_PHILLIPS_PROPERTY_ID,
  PERIOD_ID: periodId,
  HOTEL_PHILLIPS_VISIBLE: true,
  published,
  measurementContractHash: hash,
};
mkdirSync(join(process.cwd(), "reports/ai-demand-positioning"), { recursive: true });
writeFileSync(
  join(process.cwd(), "reports/ai-demand-positioning/adp-hotel-phillips-baseline-period-001-certify.json"),
  JSON.stringify(out, null, 2) + "\n"
);
console.log(JSON.stringify(out, null, 2));
