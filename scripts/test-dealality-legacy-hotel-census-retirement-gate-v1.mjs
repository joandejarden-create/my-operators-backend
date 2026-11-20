#!/usr/bin/env node
/**
 * DEALALITY_LEGACY_HOTEL_CENSUS_RETIREMENT_GATE
 *
 * Fails when:
 * - Active CLI table still defaults to legacy Hotel Census
 * - Platform base allows legacy writes without migration opt-in
 * - Shared resolver claims legacy runtime dependency
 * - Field map missing core identity fields
 */
import assert from "node:assert/strict";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS, LEGACY_HOTEL_CENSUS_TABLE_NAME } from "../lib/hotel-census/fields.js";
import {
  CLI_DISPOSITION,
  classifyCensusCliPath,
  mapLegacyCensusFieldsToHpc,
  resolveCensusCliTable,
  assertCliDoesNotWriteLegacyHotelCensus,
  HPC_CLI_TABLE_ID,
  HPC_CLI_TABLE_NAME,
} from "../lib/hotel-census/dealality-census-cli-migration-v1.js";
import {
  assertLegacyHotelCensusAccess,
  resetLegacyCensusAccessLog,
  getLegacyCensusAccessLog,
  LEGACY_HOTEL_CENSUS_TABLE_ID,
} from "../lib/hotel-intelligence/hpc-population/legacy-hotel-census-access-guard-v1.js";
import { resolveDealalityHotelIdentity } from "../lib/hotel-census/dealality-hpc-identity-resolver-v1.js";
import { RECON_DISPOSITION } from "../lib/hotel-census/dealality-legacy-hpc-reconciliation-v1.js";

export const DEALALITY_LEGACY_HOTEL_CENSUS_RETIREMENT_GATE =
  "DEALALITY_LEGACY_HOTEL_CENSUS_RETIREMENT_GATE";

assert.notEqual(HOTEL_CENSUS_TABLE, LEGACY_HOTEL_CENSUS_TABLE_NAME);
assert.equal(HOTEL_CENSUS_TABLE, HPC_CLI_TABLE_NAME);
assert.equal(CENSUS_FIELDS.name, "Property Name");
assert.equal(CENSUS_FIELDS.rooms, "Rooms / Keys");
assert.equal(CENSUS_FIELDS.status, "Radar Status");

const mapped = mapLegacyCensusFieldsToHpc({
  name: "Test Hotel",
  rooms: 100,
  status: "Open",
  Affiliation: "Test Brand",
  unknownLegacyField: "drop-me",
});
assert.equal(mapped.fields["Property Name"], "Test Hotel");
assert.equal(mapped.fields["Rooms / Keys"], 100);
assert.ok(mapped.dropped.includes("unknownLegacyField"));

const cliTarget = resolveCensusCliTable({ purpose: "cli", method: "GET" });
assert.equal(cliTarget.tableId, HPC_CLI_TABLE_ID);
assert.equal(cliTarget.mode, "HPC_CLI");

resetLegacyCensusAccessLog();
assert.throws(
  () =>
    assertCliDoesNotWriteLegacyHotelCensus(LEGACY_HOTEL_CENSUS_TABLE_ID, {
      method: "PATCH",
      workflow: "retirement-gate",
    }),
  /LEGACY_HOTEL_CENSUS/
);

assert.throws(
  () =>
    assertLegacyHotelCensusAccess(LEGACY_HOTEL_CENSUS_TABLE_ID, {
      method: "POST",
      workflow: "retirement-gate",
      forceBlock: true,
    }),
  /LEGACY_HOTEL_CENSUS_ACCESS_BLOCKED/
);

assert.equal(
  classifyCensusCliPath("scripts/apply-hilton-census-enrichment.mjs"),
  CLI_DISPOSITION.RETARGET_HPC_NULL_FILL
);
assert.equal(
  classifyCensusCliPath("scripts/apply-str-census-import.mjs"),
  CLI_DISPOSITION.RETARGET_HPC_AUTOPILOT
);

const resolved = await resolveDealalityHotelIdentity({
  hotelKey: "recLuxvwwxID7U2B8",
  liveLookup: false,
});
assert.equal(resolved.legacyHotelCensusRuntimeDependency, false);

assert.ok(RECON_DISPOSITION.MATCHED_HPC_VERIFIED);
assert.ok(RECON_DISPOSITION.HPC_CREATE_CANDIDATE);
assert.ok(RECON_DISPOSITION.AMBIGUOUS_MATCH);

const log = getLegacyCensusAccessLog();
assert.ok(log.some((e) => e.decision === "BLOCK"));

console.log(
  JSON.stringify(
    {
      gate: DEALALITY_LEGACY_HOTEL_CENSUS_RETIREMENT_GATE,
      cliTable: HOTEL_CENSUS_TABLE,
      hpcTableId: HPC_CLI_TABLE_ID,
      blockedLegacyWrites: true,
      ok: true,
    },
    null,
    2
  )
);
console.log("test:dealality-legacy-hotel-census-retirement-gate-v1 PASS");
