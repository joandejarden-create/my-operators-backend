#!/usr/bin/env node
/**
 * HE property identity — RECORD_ID ownership (Census vs HPC).
 * Live Airtable when credentials present; otherwise structural asserts only.
 */
import assert from "node:assert/strict";
import { createRequire } from "node:module";
import {
  resolvePropertyCensusOwnership,
  HOTEL_CENSUS_TABLE,
  PROPERTY_IDENTITY_CONTRACT,
} from "../lib/hotel-intelligence/property-census-identity-v1.js";
import { HOTEL_PROPERTY_CENSUS_TABLE } from "../lib/hotel-intelligence/radar-internal-preview-hotels-v1.js";

const require = createRequire(import.meta.url);
try {
  require("dotenv").config({ path: "C:/Dev/dealality/my-operators-backend/.env" });
} catch {
  /* optional */
}

assert.equal(PROPERTY_IDENTITY_CONTRACT, "HE_PROPERTY_CENSUS_IDENTITY_V1");
assert.equal(HOTEL_CENSUS_TABLE, "Hotel Census");
assert.equal(HOTEL_PROPERTY_CENSUS_TABLE, "Hotel Property Census");

const key = process.env.AIRTABLE_API_KEY;
const baseId = process.env.AIRTABLE_BASE_ID_ALT;
if (!key || baseId !== "appCCUsuGsE1ifoLk") {
  console.log("SKIP live identity — AIRTABLE_BASE_ID_ALT/credentials not available");
  console.log("OK — HE property census identity (structural)");
  process.exit(0);
}

const Airtable = (await import("airtable")).default;
const base = new Airtable({ apiKey: key }).base(baseId);

const kgpv = await resolvePropertyCensusOwnership(base, "recUNycnMwOVFX0hc");
assert.equal(kgpv.ok, true);
assert.equal(kgpv.owningTableKind, "HOTEL_CENSUS");
assert.equal(kgpv.owningTable, HOTEL_CENSUS_TABLE);
assert.equal(kgpv.hpcFound, false);
assert.equal(kgpv.censusFound, true);

const beth = await resolvePropertyCensusOwnership(base, "recLuxvwwxID7U2B8");
assert.equal(beth.ok, true);
assert.equal(beth.owningTableKind, "HPC");
assert.equal(beth.owningTable, HOTEL_PROPERTY_CENSUS_TABLE);
assert.equal(beth.hpcFound, true);
assert.equal(beth.censusFound, false);

console.log("OK — HE property census identity (live)");
