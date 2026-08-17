/**
 * Remediate DR intake apply format issues (Family / Brand Family / State / hostels).
 * Default: dry-run. Live patch/delete requires confirms + env.
 */

import {
  buildIntakeCensusFormatRemediationPatch,
  isHostelOrHostalProperty,
} from "./intake-census-field-normalize.js";
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

export const INTAKE_FORMAT_REMEDIATION_VERSION =
  "census-intake-format-remediation-v1";

const CENSUS_TABLE_ID =
  TABLE_IDS["Hotel Property Census"] || PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID;

const REMEDIATION_FIELDS = [
  "Property Name",
  "Property Identity Key",
  "Current Brand",
  "Brand Family",
  "Family / Source Family",
  "Affiliation Status",
  "State / Region",
  "Country",
  "City",
  "Official Property URL",
  "Source URL",
  "VIC Freeze Hash",
];

async function listByIdentityKeys(baseId, token, identityKeys) {
  const keySet = new Set(identityKeys.map((k) => String(k).trim()).filter(Boolean));
  const out = [];
  let offset;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    for (const f of REMEDIATION_FIELDS) params.append("fields[]", f);
    const url = `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(CENSUS_TABLE_ID)}?${params}`;
    const res = await fetch(url, {
      headers: { Authorization: `Bearer ${token}` },
    });
    const json = await res.json();
    if (!res.ok) {
      throw new Error(`HPC list ${res.status}: ${JSON.stringify(json.error || json)}`);
    }
    for (const rec of json.records || []) {
      const key = String(rec.fields?.["Property Identity Key"] || "").trim();
      if (keySet.has(key)) out.push(rec);
    }
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

async function deleteRecords(baseId, token, recordIds) {
  const deleted = [];
  for (let i = 0; i < recordIds.length; i += 10) {
    const chunk = recordIds.slice(i, i + 10);
    const params = new URLSearchParams();
    for (const id of chunk) params.append("records[]", id);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(CENSUS_TABLE_ID)}?${params}`,
      {
        method: "DELETE",
        headers: { Authorization: `Bearer ${token}` },
      }
    );
    const json = await res.json();
    if (!res.ok) {
      throw new Error(`HPC delete ${res.status}: ${JSON.stringify(json.error || json)}`);
    }
    deleted.push(...(json.records || []).map((r) => r.id));
  }
  return deleted;
}

/**
 * @param {object} opts
 * @param {string[]} opts.identityKeys
 * @param {boolean} [opts.doWrite]
 */
export async function runIntakeFormatRemediation(opts = {}) {
  const identityKeys = opts.identityKeys || [];
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
      version: INTAKE_FORMAT_REMEDIATION_VERSION,
      status: "blocked",
      blocked_reason: "wrong_write_target",
      airtable_writes: false,
    };
  }

  const token = opts.token || resolvePat();
  const baseId = opts.baseId || resolveTargetBase()?.target_base_id;
  if (!token || !baseId) {
    return {
      version: INTAKE_FORMAT_REMEDIATION_VERSION,
      status: "blocked",
      blocked_reason: "missing_airtable_credentials",
      airtable_writes: false,
    };
  }

  const records = opts.records || (await listByIdentityKeys(baseId, token, identityKeys));
  const proposals = [];
  for (const rec of records) {
    const fields = { ...(rec.fields || {}) };
    // Prefer Embassy Suites canonical brand when name says so
    if (
      /embassy suites/i.test(fields["Property Name"] || "") &&
      /^hilton hotels/i.test(fields["Current Brand"] || "")
    ) {
      fields["Current Brand"] = "Embassy Suites by Hilton";
    }
    const remediation = buildIntakeCensusFormatRemediationPatch(fields);
    proposals.push({
      record_id: rec.id,
      property_name: fields["Property Name"],
      identity_key: fields["Property Identity Key"],
      before: {
        family: fields["Family / Source Family"] || null,
        brand_family: fields["Brand Family"] || null,
        state: fields["State / Region"] || null,
        current_brand: rec.fields?.["Current Brand"] || null,
      },
      patch: remediation.patch,
      delete_record: remediation.delete_record,
      reasons: remediation.reasons,
    });
  }

  const toPatch = proposals.filter(
    (p) => !p.delete_record && Object.keys(p.patch).length > 0
  );
  const toDelete = proposals.filter((p) => p.delete_record);
  const noOp = proposals.filter(
    (p) => !p.delete_record && Object.keys(p.patch).length === 0
  );

  let patched = [];
  let deleted = [];
  const errors = [];

  if (doWrite) {
    try {
      if (toPatch.length) {
        patched = await patchRecords(
          baseId,
          token,
          toPatch.map((p) => {
            const fields = { ...p.patch };
            // Also fix Current Brand when Embassy Suites name matched
            if (
              /embassy suites/i.test(p.property_name || "") &&
              p.before.current_brand &&
              /^hilton hotels/i.test(p.before.current_brand)
            ) {
              fields["Current Brand"] = "Embassy Suites by Hilton";
            }
            return { id: p.record_id, fields };
          })
        );
      }
      if (toDelete.length) {
        deleted = await deleteRecords(
          baseId,
          token,
          toDelete.map((p) => p.record_id)
        );
      }
    } catch (err) {
      errors.push(err?.message || String(err));
    }
  }

  return {
    version: INTAKE_FORMAT_REMEDIATION_VERSION,
    status: doWrite
      ? errors.length
        ? "partial"
        : "applied"
      : "dry_run",
    generated_at: new Date().toISOString(),
    apply_executed: doWrite,
    airtable_writes: Boolean(doWrite && (patched.length || deleted.length)),
    legacy_hotel_census_used: false,
    write_target: {
      base: productionHotelPropertyCensus.baseName,
      table: productionHotelPropertyCensus.tableName,
      table_id: CENSUS_TABLE_ID,
    },
    input_identity_keys: identityKeys.length,
    records_found: records.length,
    patch_count: toPatch.length,
    delete_count: toDelete.length,
    noop_count: noOp.length,
    patched_record_ids: patched.map((r) => r.id),
    deleted_record_ids: deleted,
    errors,
    proposals,
    note: doWrite
      ? "Format remediation applied to Hotel Property Census"
      : "Dry-run only — review patches/deletes before --apply",
  };
}

export { isHostelOrHostalProperty };
