#!/usr/bin/env node
/**
 * Radar submarket field tests.
 *   node scripts/test-radar-submarket.mjs
 */
import {
  extractSubmarketFromNotes,
  normalizeSubmarketLabel,
  inferSubmarketFromCity,
  resolveSubmarketForImport,
  getSubmarketOptionsForCountry,
  PUERTO_RICO_SUBMARKET_OPTIONS,
} from "../lib/radar-submarket.js";
import {
  buildSubmarketBackfillPatch,
  summarizeSubmarketBackfill,
} from "../lib/radar-submarket-backfill.js";
import { normalizeDemandAnchorToRadarPoint } from "../lib/demand-anchors/normalize-demand-anchor.js";
import { normalizeTravelInfrastructureToRadarPoint } from "../lib/travel-infrastructure/normalize-radar-map-point.js";
import { buildDemandAnchorAirtableFields } from "../lib/demand-anchors/import-airtable-fields.js";
import { buildTravelInfraAirtableFields } from "../lib/travel-infrastructure/import-airtable-fields.js";
import { DEMAND_ANCHORS_FIELDS as DA_F } from "../lib/demand-anchors/airtable-demand-anchors-fields.js";
import { TRAVEL_INFRASTRUCTURE_FIELDS as TI_F } from "../lib/travel-infrastructure/airtable-travel-infrastructure-fields.js";

let failed = 0;
function assert(cond, msg) {
  if (!cond) {
    console.error("FAIL:", msg);
    failed += 1;
  } else {
    console.log("ok:", msg);
  }
}

assert(
  extractSubmarketFromNotes("Submarket: East Coast / Island Access. Ferry district centroid.") ===
    "East Coast / Island Access",
  "extract submarket from notes prefix"
);

assert(
  normalizeSubmarketLabel("vieques / culebra") === "Vieques / Culebra",
  "normalize submarket case-insensitive"
);

const daPoint = normalizeDemandAnchorToRadarPoint({
  id: "recDA",
  fields: {
    [DA_F.name]: "Test Beach",
    [DA_F.pointType]: "Beach / Waterfront",
    [DA_F.submarket]: "North Coast Resort Corridor",
    [DA_F.country]: "Puerto Rico",
    [DA_F.lat]: 18.4,
    [DA_F.lng]: -66.1,
  },
});
assert(daPoint.submarket === "North Coast Resort Corridor", "demand anchor normalizer includes submarket");

const tiPoint = normalizeTravelInfrastructureToRadarPoint({
  id: "recTI",
  fields: {
    [TI_F.name]: "Ceiba Ferry",
    [TI_F.type]: "Ferry Terminal",
    [TI_F.submarket]: "East Coast / Island Access",
    [TI_F.country]: "Puerto Rico",
    [TI_F.lat]: 18.2,
    [TI_F.lng]: -65.6,
  },
});
assert(tiPoint.submarket === "East Coast / Island Access", "travel infra normalizer includes submarket");

const daFields = buildDemandAnchorAirtableFields({
  name: "Test",
  pointType: "Beach / Waterfront",
  city: "Dorado",
  country: "Puerto Rico",
  latitude: 18.4,
  longitude: -66.2,
  submarket: "North Coast Resort Corridor",
});
assert(
  daFields[DA_F.submarket] === "North Coast Resort Corridor",
  "demand anchor import maps submarket"
);

const tiFields = buildTravelInfraAirtableFields({
  name: "Ferry",
  pointType: "Ferry Terminal",
  city: "Ceiba",
  country: "Puerto Rico",
  latitude: 18.2,
  longitude: -65.6,
  submarket: "East Coast / Island Access",
});
assert(
  tiFields[TI_F.submarket] === "East Coast / Island Access",
  "travel infra import maps submarket"
);

assert(
  resolveSubmarketForImport({ submarket: "Miraflores", country: "Peru" }) === "Miraflores",
  "peru submarket resolves for import"
);

assert(
  resolveSubmarketForImport({ submarket: "Punta Cana / Bávaro / Cap Cana", country: "Dominican Republic" }) ===
    "Punta Cana / Bávaro / Cap Cana",
  "DR submarket resolves for import"
);

const peruOptions = getSubmarketOptionsForCountry("Peru");
assert(peruOptions.includes("Miraflores"), "peru registry includes Miraflores");
assert(peruOptions.includes("Sacred Valley"), "peru registry includes Sacred Valley");

const patch = buildSubmarketBackfillPatch(
  {
    id: "rec1",
    fields: {
      Name: "Palmas del Mar",
      Country: "Puerto Rico",
      Notes: "Submarket: East Coast / Island Access. Gated resort.",
      Submarket: "",
    },
  },
  {
    nameField: "Name",
    notesField: "Notes",
    submarketField: "Submarket",
    countryField: "Country",
  }
);
assert(patch.needsUpdate && patch.target === "East Coast / Island Access", "backfill patch from notes");

const skip = buildSubmarketBackfillPatch(
  {
    id: "rec2",
    fields: {
      Name: "Existing",
      Country: "Puerto Rico",
      Notes: "Submarket: San Juan Metro.",
      Submarket: "San Juan Metro",
    },
  },
  {
    nameField: "Name",
    notesField: "Notes",
    submarketField: "Submarket",
    countryField: "Country",
  }
);
assert(skip.reason === "already_populated", "skip when submarket already set");

const summary = summarizeSubmarketBackfill([patch, skip]);
assert(summary.needingUpdate === 1, "backfill summary counts updates");

assert(PUERTO_RICO_SUBMARKET_OPTIONS.length === 10, "ten PR submarket options");

assert(
  inferSubmarketFromCity("San Juan") === "San Juan Metro",
  "infer submarket from city"
);

if (failed) {
  console.error("\n" + failed + " test(s) failed");
  process.exit(1);
}
console.log("\nAll radar submarket tests passed.");
