/**
 * Dominican Republic State / Region backfill for Hotel Property Census.
 * Dry-run by default; patches only High-confidence city→province proposals.
 */

import {
  resolveDominicanRepublicStateRegion,
  DR_STATE_REGION_MAP_VERSION,
} from "./dominican-republic-state-region.js";
import {
  resolvePat,
  resolveTargetBase,
} from "../research-engine-v2/production-census-schema-create.js";
import { TABLE_IDS } from "../research-engine-v2/production-census-write.js";
import {
  assertProductionCensusWriteTarget,
  productionHotelPropertyCensus,
  PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
} from "../research-engine-v2/production-census-source-of-truth.js";
import { INTAKE_APPLY_CONFIRMS } from "./intake-autopilot-controlled.js";
import { checkIntakeApplyEnv } from "./intake-autopilot-apply.js";

export const DR_STATE_BACKFILL_VERSION = "census-dr-state-region-backfill-v1";

const CENSUS_TABLE_ID =
  TABLE_IDS["Hotel Property Census"] || PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID;

const READ_FIELDS = [
  "Property Name",
  "Property Identity Key",
  "City",
  "State / Region",
  "Country",
  "Current Brand",
];

async function listDominicanRepublicCensus(baseId, token) {
  const out = [];
  let offset;
  const baseParams = new URLSearchParams({
    filterByFormula: "{Country}='Dominican Republic'",
  });
  for (const f of READ_FIELDS) baseParams.append("fields[]", f);

  do {
    const params = new URLSearchParams(baseParams);
    if (offset) params.set("offset", offset);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(CENSUS_TABLE_ID)}?${params}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) {
      throw new Error(`HPC list ${res.status}: ${JSON.stringify(json.error || json)}`);
    }
    out.push(...(json.records || []));
    offset = json.offset;
  } while (offset);
  return out;
}

async function patchRecords(baseId, token, records) {
  const updated = [];
  for (let i = 0; i < records.length; i += 10) {
    const chunk = records.slice(i, i + 10);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(CENSUS_TABLE_ID)}`,
      {
        method: "PATCH",
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ records: chunk, typecast: true }),
      }
    );
    const json = await res.json();
    if (!res.ok) {
      throw new Error(`HPC patch ${res.status}: ${JSON.stringify(json.error || json)}`);
    }
    updated.push(...(json.records || []));
  }
  return updated;
}

/**
 * @param {object} opts
 * @param {'all'|'osm_intake_only'|'blank_only'} [opts.scope]
 * @param {boolean} [opts.doWrite]
 * @param {boolean} [opts.overwriteExisting]
 */
export async function runDominicanRepublicStateRegionBackfill(opts = {}) {
  const scope = opts.scope || "blank_only";
  const overwriteExisting = opts.overwriteExisting === true;
  const envCheck = checkIntakeApplyEnv(opts.env || process.env);
  const confirms = opts.confirms || {};
  const allConfirmsOk =
    opts.allConfirmsOk != null
      ? opts.allConfirmsOk
      : INTAKE_APPLY_CONFIRMS.every((f) => confirms[f]);
  const doWrite = Boolean(opts.doWrite && allConfirmsOk && envCheck.allOk);

  const writeTarget = assertProductionCensusWriteTarget({
    baseName: productionHotelPropertyCensus.baseName,
    tableName: productionHotelPropertyCensus.tableName,
    tableId: CENSUS_TABLE_ID,
  });
  if (!writeTarget.ok) {
    return {
      version: DR_STATE_BACKFILL_VERSION,
      status: "blocked",
      blocked_reason: "wrong_write_target",
      airtable_writes: false,
    };
  }

  const token = opts.token || resolvePat();
  const baseId = opts.baseId || resolveTargetBase()?.target_base_id;
  if (!token || !baseId) {
    return {
      version: DR_STATE_BACKFILL_VERSION,
      status: "blocked",
      blocked_reason: "missing_airtable_credentials",
      airtable_writes: false,
    };
  }

  const records = opts.records || (await listDominicanRepublicCensus(baseId, token));
  const proposals = [];
  const steward = [];
  const skipped = [];

  for (const rec of records) {
    const f = rec.fields || {};
    const identityKey = String(f["Property Identity Key"] || "").trim();
    const city = String(f.City || "").trim();
    const existingState = String(f["State / Region"] || "").trim();
    const isOsm = identityKey.startsWith("osm_do_");

    if (scope === "osm_intake_only" && !isOsm) {
      skipped.push({ record_id: rec.id, reason: "scope_osm_only" });
      continue;
    }
    if (scope === "blank_only" && existingState && !overwriteExisting) {
      skipped.push({
        record_id: rec.id,
        reason: "state_already_set",
        state: existingState,
      });
      continue;
    }
    if (existingState && !overwriteExisting && scope !== "blank_only") {
      // all scope but don't overwrite
      skipped.push({
        record_id: rec.id,
        reason: "state_already_set",
        state: existingState,
      });
      continue;
    }

    const resolved = resolveDominicanRepublicStateRegion(city);
    if (!resolved.ok || resolved.confidence !== "High") {
      steward.push({
        record_id: rec.id,
        property_name: f["Property Name"],
        identity_key: identityKey,
        city,
        reason: resolved.reason,
        osm_intake: isOsm,
      });
      continue;
    }

    /** @type {Record<string, string>} */
    const patch = { "State / Region": resolved.province };
    // Optional city cleanup when city field embedded province
    if (resolved.suggest_city_cleanup && resolved.suggest_city_cleanup !== city) {
      patch.City = resolved.city_canonical || resolved.suggest_city_cleanup;
    }

    proposals.push({
      record_id: rec.id,
      property_name: f["Property Name"],
      identity_key: identityKey,
      city_before: city,
      state_before: existingState || null,
      state_after: resolved.province,
      city_after: patch.City || city,
      reason: resolved.reason,
      confidence: resolved.confidence,
      osm_intake: isOsm,
      patch,
    });
  }

  let patched = [];
  const errors = [];
  if (doWrite && proposals.length) {
    try {
      patched = await patchRecords(
        baseId,
        token,
        proposals.map((p) => ({ id: p.record_id, fields: p.patch }))
      );
    } catch (err) {
      errors.push(err?.message || String(err));
    }
  }

  return {
    version: DR_STATE_BACKFILL_VERSION,
    map_version: DR_STATE_REGION_MAP_VERSION,
    status: doWrite ? (errors.length ? "partial" : "applied") : "dry_run",
    generated_at: new Date().toISOString(),
    scope,
    overwrite_existing: overwriteExisting,
    apply_executed: doWrite,
    airtable_writes: Boolean(doWrite && patched.length),
    legacy_hotel_census_used: false,
    write_target: {
      base: productionHotelPropertyCensus.baseName,
      table: productionHotelPropertyCensus.tableName,
      table_id: CENSUS_TABLE_ID,
    },
    input_count: records.length,
    proposal_count: proposals.length,
    steward_count: steward.length,
    skipped_count: skipped.length,
    patched_count: patched.length,
    patched_record_ids: patched.map((r) => r.id),
    errors,
    proposals,
    steward_sample: steward.slice(0, 40),
    steward,
    note: doWrite
      ? "DR State / Region High-confidence backfill applied"
      : "Dry-run only — High-confidence city→province proposals",
  };
}
