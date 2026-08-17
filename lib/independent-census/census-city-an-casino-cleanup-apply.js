/**
 * Apply City cleanup for An & Casino / Unknown DR rows + State fill.
 */

import { buildAnCasinoCityCleanupProposal } from "./census-city-an-casino-cleanup.js";
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

export const CITY_CASINO_CLEANUP_APPLY_VERSION =
  "census-city-an-casino-cleanup-apply-v1";

const CENSUS_TABLE_ID =
  TABLE_IDS["Hotel Property Census"] || PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID;

const READ_FIELDS = [
  "Property Name",
  "City",
  "State / Region",
  "Country",
  "Current Brand",
  "Brand Family",
  "Family / Source Family",
  "Official Property URL",
  "Property Identity Key",
];

function isTargetBadCity(city) {
  const c = String(city || "").trim();
  return (
    !c ||
    /^unknown$/i.test(c) ||
    /^an\s*&/i.test(c) ||
    (/casino/i.test(c) && !/punta cana|bavaro|romana/i.test(c)) ||
    /adults?\s*only/i.test(c)
  );
}

async function listDr(baseId, token) {
  const out = [];
  let offset;
  const base = new URLSearchParams({
    filterByFormula: "{Country}='Dominican Republic'",
  });
  for (const f of READ_FIELDS) base.append("fields[]", f);
  do {
    const params = new URLSearchParams(base);
    if (offset) params.set("offset", offset);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(CENSUS_TABLE_ID)}?${params}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(JSON.stringify(json.error || json));
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
    if (!res.ok) throw new Error(JSON.stringify(json.error || json));
    updated.push(...(json.records || []));
  }
  return updated;
}

export async function runAnCasinoCityCleanup(opts = {}) {
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
      version: CITY_CASINO_CLEANUP_APPLY_VERSION,
      status: "blocked",
      blocked_reason: "wrong_write_target",
      airtable_writes: false,
    };
  }

  const token = opts.token || resolvePat();
  const baseId = opts.baseId || resolveTargetBase()?.target_base_id;
  if (!token || !baseId) {
    return {
      version: CITY_CASINO_CLEANUP_APPLY_VERSION,
      status: "blocked",
      blocked_reason: "missing_airtable_credentials",
      airtable_writes: false,
    };
  }

  const records = opts.records || (await listDr(baseId, token));
  const targets = records.filter((r) => isTargetBadCity(r.fields?.City));
  const proposals = [];
  const failed = [];

  for (const rec of targets) {
    const fields = { Country: "Dominican Republic", ...(rec.fields || {}) };
    const proposal = buildAnCasinoCityCleanupProposal(fields);
    if (!proposal.ok) {
      failed.push({
        record_id: rec.id,
        property_name: fields["Property Name"],
        city: fields.City,
        reason: proposal.reason,
      });
      continue;
    }
    proposals.push({
      record_id: rec.id,
      property_name: fields["Property Name"],
      identity_key: fields["Property Identity Key"],
      city_before: proposal.city_before,
      city_after: proposal.city_after,
      state_after: proposal.patch["State / Region"] || null,
      brand_after: proposal.patch["Current Brand"] || null,
      reason: proposal.reason,
      patch: proposal.patch,
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
    version: CITY_CASINO_CLEANUP_APPLY_VERSION,
    status: doWrite ? (errors.length ? "partial" : "applied") : "dry_run",
    generated_at: new Date().toISOString(),
    apply_executed: doWrite,
    airtable_writes: Boolean(doWrite && patched.length),
    legacy_hotel_census_used: false,
    write_target: {
      base: productionHotelPropertyCensus.baseName,
      table: productionHotelPropertyCensus.tableName,
      table_id: CENSUS_TABLE_ID,
    },
    targets_found: targets.length,
    proposal_count: proposals.length,
    failed_count: failed.length,
    patched_count: patched.length,
    patched_record_ids: patched.map((r) => r.id),
    proposals,
    failed,
    errors,
    note: doWrite
      ? "An & Casino / Unknown City cleanup applied"
      : "Dry-run only",
  };
}
