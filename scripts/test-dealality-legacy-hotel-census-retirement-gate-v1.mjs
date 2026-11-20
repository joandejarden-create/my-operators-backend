#!/usr/bin/env node
/**
 * DEALALITY_LEGACY_HOTEL_CENSUS_RETIREMENT_GATE — HPC-only founder correction.
 *
 * Fails when:
 * - CLI table defaults to legacy Hotel Census
 * - Migration-read is still allowed
 * - Legacy recon jobs are executable
 * - Shared resolver / CLI can target legacy table
 */
import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS, LEGACY_HOTEL_CENSUS_TABLE_NAME } from "../lib/hotel-census/fields.js";
import {
  classifyCensusCliPath,
  mapLegacyCensusFieldsToHpc,
  resolveCensusCliTable,
  assertCliDoesNotWriteLegacyHotelCensus,
  HPC_CLI_TABLE_ID,
  HPC_CLI_TABLE_NAME,
  CLI_DISPOSITION,
} from "../lib/hotel-census/dealality-census-cli-migration-v1.js";
import {
  assertLegacyHotelCensusAccess,
  migrationReadAllowed,
  resetLegacyCensusAccessLog,
  getLegacyCensusAccessLog,
  LEGACY_HOTEL_CENSUS_TABLE_ID,
  LEGACY_HOTEL_CENSUS_DATA_ACCESS_PROHIBITED,
} from "../lib/hotel-intelligence/hpc-population/legacy-hotel-census-access-guard-v1.js";
import {
  LEGACY_RECONCILIATION_DISABLED,
  DEPRECATION_BANNER,
} from "../lib/hotel-census/dealality-legacy-hpc-reconciliation-v1.js";
import { resolveDealalityHotelIdentity } from "../lib/hotel-census/dealality-hpc-identity-resolver-v1.js";

export const DEALALITY_LEGACY_HOTEL_CENSUS_RETIREMENT_GATE =
  "DEALALITY_LEGACY_HOTEL_CENSUS_RETIREMENT_GATE";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");

assert.equal(LEGACY_HOTEL_CENSUS_DATA_ACCESS_PROHIBITED, true);
assert.equal(LEGACY_RECONCILIATION_DISABLED, true);
assert.match(DEPRECATION_BANNER, /PROHIBITED LEGACY/);

assert.equal(migrationReadAllowed({}), false);
assert.equal(migrationReadAllowed({ allowLegacyHotelCensusRead: true }), false);
process.env.DEALALITY_LEGACY_HOTEL_CENSUS_MIGRATION_READ = "1";
process.env.HE_LEGACY_HOTEL_CENSUS_MIGRATION_READ = "1";
assert.equal(migrationReadAllowed({}), false, "env flags must not re-enable legacy reads");
delete process.env.DEALALITY_LEGACY_HOTEL_CENSUS_MIGRATION_READ;
delete process.env.HE_LEGACY_HOTEL_CENSUS_MIGRATION_READ;

assert.notEqual(HOTEL_CENSUS_TABLE, LEGACY_HOTEL_CENSUS_TABLE_NAME);
assert.equal(HOTEL_CENSUS_TABLE, HPC_CLI_TABLE_NAME);
assert.equal(CENSUS_FIELDS.name, "Property Name");

assert.throws(
  () =>
    assertLegacyHotelCensusAccess(LEGACY_HOTEL_CENSUS_TABLE_ID, {
      method: "GET",
      workflow: "retirement-gate",
    }),
  (err) => err?.code === "LEGACY_HOTEL_CENSUS_ACCESS_BLOCKED"
);

assert.throws(
  () =>
    resolveCensusCliTable({ purpose: "migration-read", method: "GET" }),
  (err) => err?.code === "LEGACY_HOTEL_CENSUS_ACCESS_BLOCKED"
);

assert.throws(
  () =>
    assertCliDoesNotWriteLegacyHotelCensus(LEGACY_HOTEL_CENSUS_TABLE_ID, {
      method: "GET",
      workflow: "retirement-gate",
    }),
  /LEGACY_HOTEL_CENSUS/
);

const mapped = mapLegacyCensusFieldsToHpc({
  name: "Test Hotel",
  rooms: 100,
  unknownLegacyField: "drop-me",
});
assert.equal(mapped.fields["Property Name"], "Test Hotel");
assert.ok(mapped.dropped.includes("unknownLegacyField"));

const cliTarget = resolveCensusCliTable({ purpose: "cli", method: "GET" });
assert.equal(cliTarget.tableId, HPC_CLI_TABLE_ID);
assert.equal(cliTarget.mode, "HPC_CLI");

assert.equal(
  classifyCensusCliPath("scripts/apply-hilton-census-enrichment.mjs"),
  CLI_DISPOSITION.RETARGET_HPC_NULL_FILL
);

const resolved = await resolveDealalityHotelIdentity({
  hotelKey: "recLuxvwwxID7U2B8",
  liveLookup: false,
});
assert.equal(resolved.legacyHotelCensusRuntimeDependency, false);

// Disabled recon / compare / mutation-from-legacy jobs must exit nonzero
for (const script of [
  "scripts/reconcile-legacy-census-to-hpc-full-v1.mjs",
  "scripts/reconcile-legacy-census-to-hpc-dry-run-v1.mjs",
  "scripts/compare-legacy-hpc-population-v1.mjs",
  "scripts/prepare-legacy-to-hpc-mutation-package-v1.mjs",
]) {
  const r = spawnSync(process.execPath, [path.join(root, script)], {
    encoding: "utf8",
  });
  assert.notEqual(r.status, 0, `${script} must be disabled`);
  assert.match(
    String(r.stderr || r.stdout || ""),
    /LEGACY_RECONCILIATION_DISABLED|LEGACY_DATA_IMPORT_BLOCKED|PROHIBITED LEGACY/
  );
}

resetLegacyCensusAccessLog();
assert.throws(() =>
  assertLegacyHotelCensusAccess(LEGACY_HOTEL_CENSUS_TABLE_ID, {
    method: "GET",
    workflow: "log-check",
  })
);
assert.ok(getLegacyCensusAccessLog().some((e) => e.decision === "BLOCK"));

console.log(
  JSON.stringify(
    {
      gate: DEALALITY_LEGACY_HOTEL_CENSUS_RETIREMENT_GATE,
      policy: "HPC_ONLY_ZERO_LEGACY_DATA_ACCESS",
      cliTable: HOTEL_CENSUS_TABLE,
      migrationReadAllowed: false,
      legacyReconciliationDisabled: true,
      ok: true,
    },
    null,
    2
  )
);
console.log("test:dealality-legacy-hotel-census-retirement-gate-v1 PASS");
