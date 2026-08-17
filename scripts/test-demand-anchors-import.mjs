#!/usr/bin/env node
/**
 * Demand Anchors import workflow tests.
 *   node scripts/test-demand-anchors-import.mjs
 */
import "../load-env.js";
import { readFileSync } from "fs";
import { fileURLToPath } from "url";
import {
  previewDemandAnchorsImport,
  commitDemandAnchorsImport,
} from "../lib/demand-anchors/import-commit.js";
import {
  validateImportPoint,
  buildDemandAnchorsImportPreview,
  isDuplicateCandidate,
  coordsWithinTolerance,
  coerceImportPoint,
  filterCommitRecords,
} from "../lib/demand-anchors/import-validation.js";
import { applyPointTypeDefaults } from "../lib/demand-anchors/point-type-defaults.js";
import { buildDemandAnchorAirtableFields } from "../lib/demand-anchors/import-airtable-fields.js";
import { DEMAND_ANCHORS_FIELDS as DA_F, POINT_TYPES } from "../lib/demand-anchors/airtable-demand-anchors-fields.js";
import {
  validateTravelInfraImportPoint,
  buildTravelInfraImportPreview,
} from "../lib/travel-infrastructure/import-validation.js";

let failed = 0;
function assert(cond, msg) {
  if (!cond) {
    console.error("FAIL:", msg);
    failed += 1;
  } else console.log("ok:", msg);
}

function validPoint(overrides = {}) {
  return {
    name: "Test Medical Campus",
    pointType: "Medical Campus",
    city: "San Juan",
    country: "Puerto Rico",
    latitude: 18.41,
    longitude: -66.06,
    ...overrides,
  };
}

// Required field validation
const missingName = validateImportPoint({ pointType: "Medical Campus", city: "X", country: "Y", latitude: 1, longitude: 2 }, 0);
assert(!missingName.valid && missingName.errors.some((e) => /Name/i.test(e)), "missing name fails");

const missingCoords = validateImportPoint(
  { name: "X", pointType: "Medical Campus", city: "San Juan", country: "PR" },
  0
);
assert(!missingCoords.valid, "missing coordinates fails");
assert(missingCoords.missingCoordinates, "missingCoordinates flag");

const invalidType = validateImportPoint(validPoint({ pointType: "Invalid Type" }), 0);
assert(!invalidType.valid, "invalid point type fails");

// Defaults
const withDefaults = validateImportPoint(validPoint(), 0, { market: "San Juan", region: "Caribbean" });
assert(withDefaults.valid, "valid point passes");
assert(withDefaults.normalized.demandSegment === "Medical", "defaults apply demand segment");

// Duplicate detection
const existing = {
  id: "recEXIST",
  name: "Test Medical Campus",
  city: "San Juan",
  country: "Puerto Rico",
  pointType: "Medical Campus",
  latitude: 18.41,
  longitude: -66.06,
};
const dup = isDuplicateCandidate(validPoint(), existing, { market: "San Juan" });
assert(dup.duplicate, "duplicate same name city country");

const coordDup = isDuplicateCandidate(
  validPoint({ name: "Nearby Clinic Annex", latitude: 18.4101, longitude: -66.0601 }),
  existing
);
assert(coordDup.duplicate && coordDup.reason === "same_coordinates", "coordinate duplicate");

const preview = buildDemandAnchorsImportPreview([validPoint(), validPoint({ name: "Other Sports", pointType: "Sports Venue" })], [existing], {
  market: "San Juan",
  country: "Puerto Rico",
});
assert(preview.summary.valid === 2, "preview valid count");
assert(preview.duplicates.length >= 1, "flags duplicate against existing");

const batchDup = buildDemandAnchorsImportPreview(
  [validPoint(), validPoint({ name: "Test Medical Campus", pointType: "Medical Campus" })],
  [],
  { market: "San Juan", country: "Puerto Rico" }
);
assert(batchDup.duplicates.length >= 1, "batch duplicate detection");

// Skip duplicate filter
const toSave = filterCommitRecords(preview.preview, true);
assert(toSave.length < preview.preview.length || preview.duplicates.length === 0, "skip duplicates reduces commit set");

// Travel infra additional types
const tiValid = validateTravelInfraImportPoint(
  { name: "Bus Terminal", pointType: "Bus Terminal", city: "Cancun", country: "Mexico", latitude: 21.17, longitude: -86.85 },
  0,
  { market: "Cancun" }
);
assert(tiValid.valid, "travel infra bus terminal valid");

const tiInvalid = validateTravelInfraImportPoint(
  { name: "Airport", pointType: "Airport", city: "Cancun", country: "Mexico", latitude: 1, longitude: 2 },
  0
);
assert(!tiInvalid.valid, "airport rejected on additional-types workflow");

assert(coordsWithinTolerance(18.41, -66.06, 18.4105, -66.0605), "coord tolerance");

const applied = applyPointTypeDefaults(coerceImportPoint(validPoint()));
assert(applied.hotelDemandRationale, "defaults rationale");

assert(POINT_TYPES.includes("Convention Center"), "point types list");

const emptyCommit = await commitDemandAnchorsImport([], {});
assert(!emptyCommit.ok && emptyCommit.error === "validation_failed", "empty commit rejected");

const fixturePayload = JSON.parse(
  readFileSync(new URL("../fixtures/demand-anchors-san-juan.json", import.meta.url), "utf8")
);
const fixturePreview = await previewDemandAnchorsImport({
  market: fixturePayload.market,
  country: fixturePayload.country,
  region: fixturePayload.region,
  points: fixturePayload.points,
});
assert(fixturePreview.ok && fixturePreview.summary.valid === fixturePayload.points.length, "san juan fixture preview");

const submarketFields = buildDemandAnchorAirtableFields(
  validPoint({ submarket: "San Juan Metro" })
);
assert(submarketFields[DA_F.submarket] === "San Juan Metro", "import maps optional submarket");

if (failed) {
  console.error("\n" + failed + " failed");
  process.exit(1);
}
console.log("\nAll demand anchors import tests passed.");
