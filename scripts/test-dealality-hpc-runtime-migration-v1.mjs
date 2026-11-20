#!/usr/bin/env node
/**
 * Dealality-wide HPC runtime migration gates (no production writes).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  mapHpcRecordToScoutCensusShape,
  buildHpcGeographyFormula,
  HPC_RUNTIME_TABLE_ID,
  SCOUT_HPC_SELECT_FIELDS,
} from "../lib/hotel-census/hpc-scout-record-adapter-v1.js";
import {
  DEALALITY_HPC_IDENTITY_RESOLVER_V1,
  resolveDealalityHotelIdentity,
  HPC_CANONICAL_TABLE_ID,
} from "../lib/hotel-census/dealality-hpc-identity-resolver-v1.js";
import {
  assertLegacyHotelCensusAccess,
  LEGACY_HOTEL_CENSUS_TABLE_ID,
  migrationReadAllowed,
} from "../lib/hotel-intelligence/hpc-population/legacy-hotel-census-access-guard-v1.js";
import {
  HOTEL_PROPERTY_CENSUS_TABLE,
  HOTEL_PROPERTY_CENSUS_TABLE_ID,
} from "../lib/hotel-census/platform-base.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");

delete process.env.DEALALITY_LEGACY_HOTEL_CENSUS_MIGRATION_READ;
delete process.env.HE_LEGACY_HOTEL_CENSUS_MIGRATION_READ;
assert.equal(migrationReadAllowed({}), false);
assert.equal(HPC_RUNTIME_TABLE_ID, "tbl9aY5ijiuIzzWam");
assert.equal(HOTEL_PROPERTY_CENSUS_TABLE_ID, "tbl9aY5ijiuIzzWam");
assert.equal(HPC_CANONICAL_TABLE_ID, "tbl9aY5ijiuIzzWam");
assert.equal(DEALALITY_HPC_IDENTITY_RESOLVER_V1, "DEALALITY_HPC_IDENTITY_RESOLVER_V1");

assert.throws(
  () =>
    assertLegacyHotelCensusAccess(LEGACY_HOTEL_CENSUS_TABLE_ID, {
      method: "GET",
      workflow: "dealality-runtime-gate",
      forceBlock: true,
    }),
  (err) => err?.code === "LEGACY_HOTEL_CENSUS_ACCESS_BLOCKED"
);

const adapted = mapHpcRecordToScoutCensusShape({
  id: "recLuxvwwxID7U2B8",
  fields: {
    "Property Name": "Bethesda Marriott",
    "Current Brand": "Marriott Hotels",
    "Radar Status": "Open",
    "Rooms / Keys": 407,
    Country: "United States",
    City: "Bethesda",
    Market: "Washington DC",
    Latitude: 38.98,
    Longitude: -77.09,
  },
});
assert.equal(adapted.fields.name, "Bethesda Marriott");
assert.equal(adapted.fields.Affiliation, "Marriott Hotels");
assert.equal(adapted.fields.status, "Open");
assert.equal(adapted.fields.rooms, 407);
assert.equal(adapted.fields.country, "United States");
assert.equal(adapted._sourceTableId, "tbl9aY5ijiuIzzWam");

const formula = buildHpcGeographyFormula({ country: "Costa Rica", market: "Guanacaste" });
assert.match(formula, /\{Country\}='Costa Rica'/);
assert.match(formula, /\{Market\}='Guanacaste'/);
assert.ok(SCOUT_HPC_SELECT_FIELDS.includes("Property Name"));
assert.ok(!SCOUT_HPC_SELECT_FIELDS.includes("Affiliation"));

const beth = await resolveDealalityHotelIdentity({
  hotelKey: "recLuxvwwxID7U2B8",
  liveLookup: false,
});
assert.equal(beth.canonicalHotelId, "recLuxvwwxID7U2B8");
assert.equal(beth.legacyHotelCensusRuntimeDependency, false);

// Source files must not hard-code Hotel Census as Scout/brand-presence runtime table
const scoutCache = fs.readFileSync(
  path.join(root, "lib/scout/census-read-cache.js"),
  "utf8"
);
assert.ok(scoutCache.includes("Hotel Property Census") || scoutCache.includes("HPC_RUNTIME_TABLE_NAME"));
assert.ok(!/base\(HOTEL_CENSUS_TABLE\)/.test(scoutCache));

const brandPresence = fs.readFileSync(path.join(root, "api/brand-presence.js"), "utf8");
assert.ok(brandPresence.includes("HOTEL_PROPERTY_CENSUS_TABLE"));
assert.ok(brandPresence.includes('tableId: "tbl9aY5ijiuIzzWam"'));
assert.ok(!/table:\s*"Hotel Census"/.test(brandPresence));

const operators = fs.readFileSync(
  path.join(root, "api/operators-by-brand-region.js"),
  "utf8"
);
assert.ok(operators.includes('table: "Hotel Property Census"'));
assert.ok(!/table:\s*"Hotel Census"/.test(operators));

const aggregate = fs.readFileSync(
  path.join(root, "lib/hotel-census/aggregate-presence-summary.js"),
  "utf8"
);
assert.ok(aggregate.includes("HOTEL_PROPERTY_CENSUS_TABLE"));
assert.ok(!/base\(HOTEL_CENSUS_TABLE\)/.test(aggregate));

const brandFit = fs.readFileSync(path.join(root, "api/brand-fit-analyzer.js"), "utf8");
assert.ok(brandFit.includes('table: "Hotel Property Census"'));

assert.equal(HOTEL_PROPERTY_CENSUS_TABLE, "Hotel Property Census");

console.log("test:dealality-hpc-runtime-migration-v1 PASS");
