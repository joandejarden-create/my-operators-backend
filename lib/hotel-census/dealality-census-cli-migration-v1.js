/**
 * Dealality Census CLI migration — Phase 2.
 *
 * Ordinary production-intended enrichment/import CLIs must target Hotel Property
 * Census (HPC), never legacy Hotel Census writes. Legacy reads remain opt-in only
 * for explicit migration tooling.
 *
 * Do not treat this as a silent table-ID swap: field names are remapped via
 * LEGACY_CENSUS_TO_HPC_FIELD_MAP / mapLegacyCensusFieldsToHpc.
 */

import {
  assertLegacyHotelCensusAccess,
  isLegacyHotelCensusTableRef,
  LEGACY_HOTEL_CENSUS_TABLE_ID,
  LEGACY_HOTEL_CENSUS_TABLE_NAME,
  migrationReadAllowed,
} from "../hotel-intelligence/hpc-population/legacy-hotel-census-access-guard-v1.js";
export const DEALALITY_CENSUS_CLI_MIGRATION_V1 = "DEALALITY_CENSUS_CLI_MIGRATION_V1";
export const HPC_CLI_TABLE_NAME = "Hotel Property Census";
export const HPC_CLI_TABLE_ID = "tbl9aY5ijiuIzzWam";

/** Disposition vocabulary for CLI catalog / retirement gate. */
export const CLI_DISPOSITION = Object.freeze({
  RETARGET_HPC_AUTOPILOT: "RETARGET_HPC_AUTOPILOT",
  RETARGET_HPC_UPSERT: "RETARGET_HPC_UPSERT",
  RETARGET_HPC_NULL_FILL: "RETARGET_HPC_NULL_FILL",
  DISABLE_MIGRATION_ONLY: "DISABLE_MIGRATION_ONLY",
  RETIRED_OBSOLETE: "RETIRED_OBSOLETE",
  ALREADY_HPC: "ALREADY_HPC",
  TEST_ONLY: "TEST_ONLY",
  AUDIT_ONLY: "AUDIT_ONLY",
});

/**
 * Legacy Hotel Census field → HPC field.
 * Only map fields that have a stable canonical destination.
 */
export const LEGACY_CENSUS_TO_HPC_FIELD_MAP = Object.freeze({
  name: "Property Name",
  Affiliation: "Current Brand",
  "Parent Company": "Parent Company",
  status: "Radar Status",
  rooms: "Rooms / Keys",
  country: "Country",
  city: "City",
  Market: "Market",
  Submarket: "Submarket",
  Region: "Continent",
  "Chain Scale": "Hotel Class / Segment",
  "Operation Type": "Operation Type",
  "Management Company": "Operator / Management Company",
  "Property Type": "Asset Context",
  Website: "Official Property URL",
  website: "Official Property URL",
  Latitude: "Latitude",
  Longitude: "Longitude",
  Address: "Address",
  Phone: "Phone",
});

/** HPC field names used by remapped CLI payloads. */
export const HPC_CLI_FIELDS = Object.freeze({
  name: "Property Name",
  affiliation: "Current Brand",
  parentCompany: "Parent Company",
  status: "Radar Status",
  rooms: "Rooms / Keys",
  country: "Country",
  city: "City",
  market: "Market",
  submarket: "Submarket",
  region: "Continent",
  chainScale: "Hotel Class / Segment",
  operationType: "Operation Type",
  managementCompany: "Operator / Management Company",
  propertyType: "Asset Context",
  website: "Official Property URL",
});

/**
 * Resolve the Airtable table name ordinary CLIs must use.
 * Always HPC unless explicitly migration-read-only against legacy.
 */
export function resolveCensusCliTable(opts = {}) {
  const purpose = String(opts.purpose || "cli").toLowerCase();
  const method = String(opts.method || "GET").toUpperCase();

  if (opts.forceLegacyMigrationRead === true || purpose === "migration-read") {
    if (method !== "GET") {
      const err = new Error(
        "LEGACY_HOTEL_CENSUS_WRITE_BLOCKED: migration tooling may only GET legacy Hotel Census"
      );
      err.code = "LEGACY_HOTEL_CENSUS_WRITE_BLOCKED";
      throw err;
    }
    assertLegacyHotelCensusAccess(LEGACY_HOTEL_CENSUS_TABLE_ID, {
      method: "GET",
      workflow: opts.workflow || "census-cli-migration-read",
      allowLegacyHotelCensusRead: true,
    });
    if (!migrationReadAllowed({ allowLegacyHotelCensusRead: true })) {
      const err = new Error("LEGACY_HOTEL_CENSUS_ACCESS_BLOCKED: migration read flag required");
      err.code = "LEGACY_HOTEL_CENSUS_ACCESS_BLOCKED";
      throw err;
    }
    return {
      tableName: LEGACY_HOTEL_CENSUS_TABLE_NAME,
      tableId: LEGACY_HOTEL_CENSUS_TABLE_ID,
      mode: "MIGRATION_READ",
      fieldMap: null,
    };
  }

  return {
    tableName: HPC_CLI_TABLE_NAME,
    tableId: HPC_CLI_TABLE_ID,
    mode: "HPC_CLI",
    fieldMap: LEGACY_CENSUS_TO_HPC_FIELD_MAP,
  };
}

/**
 * Remap a legacy-shaped fields object to HPC field names.
 * Unknown keys are dropped (not silently written under legacy names).
 */
export function mapLegacyCensusFieldsToHpc(fields = {}) {
  const out = {};
  const dropped = [];
  for (const [key, value] of Object.entries(fields || {})) {
    const dest = LEGACY_CENSUS_TO_HPC_FIELD_MAP[key];
    if (!dest) {
      dropped.push(key);
      continue;
    }
    if (value === undefined) continue;
    out[dest] = value;
  }
  return { fields: out, dropped };
}

/**
 * Fail closed if a CLI attempts to target legacy Hotel Census for write/mutation.
 */
export function assertCliDoesNotWriteLegacyHotelCensus(tableRef, opts = {}) {
  if (!isLegacyHotelCensusTableRef(tableRef)) {
    return { ok: true };
  }
  const method = String(opts.method || "PATCH").toUpperCase();
  if (method === "GET" && migrationReadAllowed(opts)) {
    assertLegacyHotelCensusAccess(tableRef, { ...opts, method: "GET" });
    return { ok: true, mode: "MIGRATION_READ" };
  }
  const err = new Error(
    `LEGACY_HOTEL_CENSUS_WRITE_BLOCKED: CLI must not ${method} legacy Hotel Census (${LEGACY_HOTEL_CENSUS_TABLE_ID}). Use HPC ${HPC_CLI_TABLE_ID} via shared upsert / Autopilot.`
  );
  err.code = "LEGACY_HOTEL_CENSUS_WRITE_BLOCKED";
  throw err;
}

/**
 * Classify a CLI script path into a migration disposition (heuristic catalog).
 */
export function classifyCensusCliPath(filePath = "") {
  const f = String(filePath || "").replace(/\\/g, "/").toLowerCase();
  if (!f) return CLI_DISPOSITION.AUDIT_ONLY;
  if (/\/test-|\.test\.|\/probe-/.test(f)) return CLI_DISPOSITION.TEST_ONLY;
  if (/audit-|inventory|reconcile-|compare-/.test(f)) return CLI_DISPOSITION.AUDIT_ONLY;
  if (/str-import|str-census|wave\d|manual-additions|directory-creates/.test(f)) {
    return CLI_DISPOSITION.RETARGET_HPC_AUTOPILOT;
  }
  if (/backfill-|apply-.*enrichment|apply-.*amenities|apply-.*descriptions|geocode/.test(f)) {
    return CLI_DISPOSITION.RETARGET_HPC_NULL_FILL;
  }
  if (/apply-he-cala|apply-.*affiliation|apply-.*property-type|apply-.*room/.test(f)) {
    return CLI_DISPOSITION.RETARGET_HPC_UPSERT;
  }
  if (/apply-census-steward|apply-hotel-census-market|sync-.*census/.test(f)) {
    return CLI_DISPOSITION.RETARGET_HPC_NULL_FILL;
  }
  if (/obsolete|deprecated|legacy-only/.test(f)) return CLI_DISPOSITION.RETIRED_OBSOLETE;
  return CLI_DISPOSITION.DISABLE_MIGRATION_ONLY;
}

/**
 * Soft-disable wrapper: enrichment CLIs that have not been field-retargeted
 * must refuse live apply against legacy Census.
 */
export function enforceCensusCliHpcOnlyOrDryRun(opts = {}) {
  const dryRun = opts.dryRun === true || process.argv.includes("--dry-run");
  const apply = opts.apply === true || process.argv.includes("--apply");
  const target = resolveCensusCliTable({
    purpose: "cli",
    method: apply && !dryRun ? "PATCH" : "GET",
    workflow: opts.workflow || "census-cli",
  });

  if (isLegacyHotelCensusTableRef(target.tableName) || isLegacyHotelCensusTableRef(target.tableId)) {
    assertCliDoesNotWriteLegacyHotelCensus(target.tableId, {
      method: apply && !dryRun ? "PATCH" : "GET",
      workflow: opts.workflow,
    });
  }

  if (apply && !dryRun && target.mode !== "HPC_CLI") {
    const err = new Error("CENSUS_CLI_APPLY_REQUIRES_HPC: live apply only against Hotel Property Census");
    err.code = "CENSUS_CLI_APPLY_REQUIRES_HPC";
    throw err;
  }

  return {
    contract: DEALALITY_CENSUS_CLI_MIGRATION_V1,
    dryRun: dryRun || !apply,
    applyAuthorized: apply && !dryRun,
    tableName: target.tableName,
    tableId: target.tableId,
    mode: target.mode,
    fieldMap: target.fieldMap,
  };
}
