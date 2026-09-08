#!/usr/bin/env node
/**
 * Certify CALA six-hotel first-official ADP baselines + enable dropdown.
 *
 * Usage:
 *   node scripts/certify-adp-cala-six-baseline-period-001.mjs --from-batch-run
 *   node scripts/certify-adp-cala-six-baseline-period-001.mjs --property adp_st_regis_mexico_city --period <id>
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
  CALA_SIX_PROPERTY_IDS,
  CALA_SIX_BASELINE_MARKERS,
  loadFrozenContractHash,
} from "../lib/ai-demand-positioning/execution/cala-six-baseline-period-001-v1.js";
import { MEASUREMENT_CONTRACT_VERSION } from "../lib/ai-demand-positioning/contracts/adp-measurement-contract-v1.js";
import { getCensusRecordIdForAdpProperty } from "../lib/ai-demand-positioning/census-link-registry.js";

const PROFILE_PATHS = {
  adp_st_regis_mexico_city: "fixtures/ai-demand-positioning/st-regis-mexico-city-property-profile.json",
  adp_st_regis_cap_cana: "fixtures/ai-demand-positioning/st-regis-cap-cana-property-profile.json",
  adp_jw_marriott_santo_domingo:
    "fixtures/ai-demand-positioning/jw-marriott-santo-domingo-property-profile.json",
  adp_radisson_santo_domingo: "fixtures/ai-demand-positioning/radisson-santo-domingo-property-profile.json",
  adp_hotel_caribe_faranda_grand:
    "fixtures/ai-demand-positioning/hotel-caribe-faranda-grand-property-profile.json",
  adp_faranda_collection_bogota:
    "fixtures/ai-demand-positioning/faranda-collection-bogota-property-profile.json",
};

const args = process.argv.slice(2);
const fromBatch = args.includes("--from-batch-run");
const onlyIdx = args.indexOf("--property");
const only =
  onlyIdx >= 0 && args[onlyIdx + 1]
    ? args[onlyIdx + 1]
    : args.find((a) => a.startsWith("--property="))?.slice("--property=".length) || null;
const periodArg = args.find((a) => a.startsWith("--period="));
let singlePeriod = periodArg ? periodArg.slice("--period=".length) : null;
const periodIdx = args.indexOf("--period");
if (periodIdx >= 0 && args[periodIdx + 1]) singlePeriod = args[periodIdx + 1];

const batchPath = join(
  process.cwd(),
  "reports/ai-demand-positioning/adp-cala-six-baseline-period-001-batch-run.json"
);

/** @type {{ propertyId: string, periodId: string }[]} */
let jobs = [];

if (fromBatch) {
  if (!existsSync(batchPath)) {
    console.error("Missing batch run report", batchPath);
    process.exit(1);
  }
  const batch = JSON.parse(readFileSync(batchPath, "utf8"));
  jobs = (batch.results || [])
    .filter((r) => r.ok && r.PERIOD_ID)
    .map((r) => ({ propertyId: r.propertyId, periodId: r.PERIOD_ID }));
  if (only) jobs = jobs.filter((j) => j.propertyId === only);
} else if (only && singlePeriod) {
  jobs = [{ propertyId: only, periodId: singlePeriod }];
} else {
  console.error("Provide --from-batch-run or --property <id> --period <id>");
  process.exit(1);
}

if (!jobs.length) {
  console.error("No jobs to certify");
  process.exit(1);
}

const hash = loadFrozenContractHash();
const certified = [];

for (const job of jobs) {
  const { propertyId, periodId } = job;
  if (!CALA_SIX_PROPERTY_IDS.includes(propertyId)) {
    console.error("Unknown property", propertyId);
    process.exit(1);
  }
  const baselineMarker = CALA_SIX_BASELINE_MARKERS[propertyId];
  const period = loadPeriod(periodId);
  if (!period) {
    console.error("PERIOD_NOT_FOUND", periodId);
    process.exit(1);
  }

  period.certified = true;
  period.certifiedAt = new Date().toISOString();
  period.baselineMarker = baselineMarker;
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

  const profilePath = join(process.cwd(), PROFILE_PATHS[propertyId]);
  const profile = JSON.parse(readFileSync(profilePath, "utf8"));
  profile.customerDropdownVisible = true;
  writeFileSync(profilePath, JSON.stringify(profile, null, 2) + "\n");

  const liveProfile = loadPropertyProfile(propertyId);
  const censusId = getCensusRecordIdForAdpProperty(propertyId);
  const bundle = buildPublishedSnapshotBundle({ period, profile: liveProfile });
  let published = null;
  if (bundle.ok) {
    bundle.manifest.officialPeriod = true;
    bundle.manifest.baselineMarker = baselineMarker;
    bundle.manifest.firstOfficialPropertyPeriod = true;
    bundle.manifest.measurementContractVersion = MEASUREMENT_CONTRACT_VERSION;
    bundle.manifest.measurementContractHash = hash;
    bundle.manifest.certified = true;
    bundle.manifest.certificationStatus = "CERTIFIED";
    if (censusId) bundle.manifest.censusRecordId = censusId;
    bundle.report.payload.period = {
      ...(bundle.report.payload.period || {}),
      officialPeriod: true,
      baselineMarker,
      firstOfficialPropertyPeriod: true,
      measurementContractVersion: MEASUREMENT_CONTRACT_VERSION,
      measurementContractHash: hash,
      certified: true,
      priorComparablePeriod: null,
    };
    published = savePublishedSnapshotBundle(bundle, { seed: false });
  }

  certified.push({
    ok: true,
    CERTIFIED: true,
    MARKER: `${baselineMarker}_CERTIFIED`,
    propertyId,
    PERIOD_ID: periodId,
    DROPDOWN_VISIBLE: true,
    censusRecordId: censusId,
    published,
    measurementContractHash: hash,
  });
}

const out = {
  ok: true,
  CERTIFIED_COUNT: certified.length,
  properties: certified.map((c) => c.propertyId),
  results: certified,
};
mkdirSync(join(process.cwd(), "reports/ai-demand-positioning"), { recursive: true });
const outPath = join(
  process.cwd(),
  "reports/ai-demand-positioning/adp-cala-six-baseline-period-001-certify.json"
);
writeFileSync(outPath, JSON.stringify(out, null, 2) + "\n");
console.log(JSON.stringify(out, null, 2));
