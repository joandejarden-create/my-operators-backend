#!/usr/bin/env node
/**
 * Travel Infrastructure backfill logic tests.
 *   node scripts/test-travel-infrastructure-backfill.mjs
 */
import {
  buildBackfillPatch,
  buildBackfillTargetFields,
  inferBackfillPointSubtype,
  inferBackfillDataConfidence,
  hasValidCoordinates,
  resolveBackfillPointType,
  sanitizeBackfillPatchForSchema,
} from "../lib/travel-infrastructure/backfill-radar-fields.js";
import { TRAVEL_INFRASTRUCTURE_FIELDS as F } from "../lib/travel-infrastructure/airtable-travel-infrastructure-fields.js";

let failed = 0;

function assert(cond, msg) {
  if (!cond) {
    console.error("FAIL:", msg);
    failed += 1;
  } else {
    console.log("ok:", msg);
  }
}

function rec(fields, id = "recTEST") {
  return { id, fields };
}

function testAirportSubtypeInference() {
  const intl = inferBackfillPointSubtype(
    {
      [F.infrastructureRole]: "International Hub (Airport)",
      [F.scaleTier]: "Major",
    },
    "Airport"
  );
  assert(intl === "International Airport", "international hub → International Airport");

  const regional = inferBackfillPointSubtype(
    { [F.airportType]: "Small Airport", [F.infrastructureRole]: "Secondary / Spoke (Airport)" },
    "Airport"
  );
  assert(regional === "Regional Airport", "small/spoke → Regional Airport");
}

function testCruisePortDefaults() {
  const t = buildBackfillTargetFields({ [F.type]: "Cruise Port", [F.lat]: 1, [F.lng]: 2 });
  assert(t[F.pointType] === "Cruise Port", "cruise point type");
  assert(t[F.pointSubtype] === "Cruise Terminal", "cruise subtype");
  assert(t[F.demandRelevance] === "High", "cruise high relevance");
  assert(t[F.mapIconType] === "Cruise Port", "cruise icon");
}

function testConventionCenterDefaults() {
  const t = buildBackfillTargetFields({
    [F.type]: "Convention Center",
    [F.lat]: 10,
    [F.lng]: 20,
  });
  assert(t[F.mapIconType] === "Event", "convention → Event icon");
  assert(t[F.demandRelevance] === "High", "convention high relevance");
  assert(t[F.demandPattern].includes("Event-Based"), "convention event-based pattern");
  assert(t[F.relevantHotelTypes].includes("Upper-Upscale"), "convention hotel types");
}

function testMissingCoordinates() {
  const t = buildBackfillTargetFields({ [F.type]: "Airport" });
  assert(t[F.includeOnRadarMap] === false, "no coords → includeOnRadarMap false");
  assert(inferBackfillDataConfidence({ [F.type]: "Airport" }) === "Low", "no coords → low confidence");
  assert(!hasValidCoordinates({ [F.lat]: 0, [F.lng]: 0 }), "0,0 not valid coords");
}

function testSourceUrlCopy() {
  const t = buildBackfillTargetFields({
    [F.type]: "Airport",
    [F.lat]: 1,
    [F.lng]: 2,
    [F.sourceUrl]: "https://example.com/airport",
  });
  assert(t[F.sourceReference] === "https://example.com/airport", "source URL copied to reference");
  assert(t[F.dataConfidence] === "High", "url + coords → high confidence");
  assert(t[F.source] === "Existing Dataset", "default source Existing Dataset");
}

function testNoOverwriteWithoutForce() {
  const r = buildBackfillPatch(
    rec({
      [F.type]: "Airport",
      [F.lat]: 1,
      [F.lng]: 2,
      [F.pointType]: "Airport",
      [F.pointSubtype]: "International Airport",
      [F.demandRelevance]: "Low",
    }),
    { force: false }
  );
  assert(!r.patch[F.pointSubtype], "does not overwrite populated subtype");
  assert(!r.patch[F.demandRelevance], "does not overwrite populated demand relevance");
  assert(r.patch[F.radarCategory] === "Travel Infrastructure", "still fills empty radar category");
}

function testForceOverwrite() {
  const r = buildBackfillPatch(
    rec({
      [F.type]: "Airport",
      [F.lat]: 1,
      [F.lng]: 2,
      [F.demandRelevance]: "Low",
    }),
    { force: true }
  );
  assert(r.patch[F.demandRelevance] === "High", "force overwrites demand relevance");
}

function testDataConfidenceInference() {
  assert(
    inferBackfillDataConfidence({ [F.lat]: 5, [F.lng]: 6, [F.sourceUrl]: "https://x.com" }) === "High",
    "high confidence"
  );
  assert(
    inferBackfillDataConfidence({ [F.lat]: 5, [F.lng]: 6 }) === "Medium",
    "medium confidence"
  );
  assert(inferBackfillDataConfidence({}) === "Low", "low confidence");
}

function testSchemaSanitizeEventIcon() {
  const schema = new Set([F.mapIconType, F.demandPattern]);
  const out = sanitizeBackfillPatchForSchema(
    {
      [F.mapIconType]: "Event",
      [F.demandPattern]: ["Group", "Event-Based"],
    },
    schema
  );
  assert(out[F.mapIconType] === "Convention", "Event icon falls back to Convention in schema");
  assert(out[F.demandPattern].includes("Group"), "Event-Based pattern mapped for schema");
}

function testResolveLegacyType() {
  assert(resolveBackfillPointType({ [F.type]: "Airport" }) === "Airport", "legacy airport type");
  assert(
    resolveBackfillPointType({ [F.pointType]: "Train Station", [F.type]: "Airport" }) === "Train Station",
    "existing point type wins"
  );
}

function main() {
  testAirportSubtypeInference();
  testCruisePortDefaults();
  testConventionCenterDefaults();
  testMissingCoordinates();
  testSourceUrlCopy();
  testNoOverwriteWithoutForce();
  testForceOverwrite();
  testDataConfidenceInference();
  testSchemaSanitizeEventIcon();
  testResolveLegacyType();

  if (failed) {
    console.error("\n" + failed + " test(s) failed");
    process.exit(1);
  }
  console.log("\nAll travel infrastructure backfill tests passed.");
}

main();
