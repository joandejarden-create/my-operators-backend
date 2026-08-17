/**
 * Peru MINCETUR steward insert apply — dry-run first; live writes fully gated.
 *
 * Writes only Hotel Property Census (tbl9aY5ijiuIzzWam).
 * Never writes Owner Name / Operator / RUC onto Owner Name.
 * Never writes legacy Hotel Census.
 */

import {
  AUTOPILOT_FORBIDDEN_FIELDS,
} from "./census-autopilot-field-allowlist.js";
import {
  INSERT_ALLOWED_FIELDS,
  INSERT_FORBIDDEN_FIELDS,
  sanitizeInsertFields,
} from "./census-autopilot-source-discovery.js";
import {
  createHotelPropertyCensusRecords,
  resolveLiveInsertContext,
} from "./census-autopilot-discovery-insert-apply.js";
import {
  assertProductionCensusWriteTarget,
  BLOCKED_WRONG_CENSUS_TARGET,
  productionHotelPropertyCensus,
} from "./production-census-source-of-truth.js";
import { TABLE_IDS } from "./production-census-write.js";
import { resolveContinentSubContinentFromCountry } from "./census-region-market-map.js";
import { MAP_PERU_MINCETUR } from "./peru-mincetur-open-data-adapter.js";
import {
  PERU_MINCETUR_REQUIRED_APPLY_CONFIRMS,
  PERU_MINCETUR_STEWARD_PACK_VERSION,
} from "./peru-mincetur-steward-review-pack.js";
import {
  loadHotelPropertyCensusReadOnly,
  matchAllCandidatesToHotelPropertyCensus,
} from "../independent-census/match-hotel-property-census.js";

export const PERU_MINCETUR_STEWARD_APPLY_VERSION = "peru-mincetur-steward-insert-apply-v1";

export const PERU_MINCETUR_APPLY_STATUS = Object.freeze({
  DRY_RUN: "peru_mincetur_steward_insert_dry_run",
  BLOCKED: "peru_mincetur_steward_insert_blocked",
  CLEAN: "peru_mincetur_steward_insert_applied",
  PARTIAL: "peru_mincetur_steward_insert_partial",
});

const CENSUS_TABLE_ID = TABLE_IDS["Hotel Property Census"];

/**
 * @param {string[]} argv
 */
export function parsePeruMinceturStewardApplyArgs(argv = process.argv.slice(2)) {
  const get = (flag, fallback = null) => {
    const i = argv.indexOf(flag);
    if (i >= 0 && argv[i + 1] && !String(argv[i + 1]).startsWith("--")) return argv[i + 1];
    return fallback;
  };
  const has = (flag) => argv.includes(flag);
  const confirms = {
    peruStewardInsert: has("--confirm-peru-mincetur-steward-insert"),
    noOwnerOperator:
      has("--confirm-no-owner-operator-writes") || has("--confirm-no-owner-operator"),
    hotelPropertyCensusOnly: has("--confirm-hotel-property-census-only"),
    noLegacyCensus: has("--confirm-no-legacy-census-writes"),
  };
  const allConfirmsOk = Object.values(confirms).every(Boolean);
  return {
    packPath: get("--pack") || get("--input") || "",
    pilotLimit: Number(get("--pilot-limit", "25")) || 25,
    enableProductionWrites: has("--enable-production-writes"),
    apply: has("--apply") || has("--enable-production-writes"),
    confirms,
    allConfirmsOk,
    help: has("--help") || has("-h"),
  };
}

/**
 * Prepare allowlisted insert fields from a steward proposed insert.
 * @param {object} proposed
 */
export function preparePeruMinceturStewardInsertFields(proposed = {}) {
  const failed = [];
  const raw = { ...(proposed.fields || {}) };

  for (const field of AUTOPILOT_FORBIDDEN_FIELDS) {
    if (raw[field] != null && raw[field] !== "") {
      failed.push(`forbidden_field:${field}`);
      delete raw[field];
    }
  }
  for (const field of INSERT_FORBIDDEN_FIELDS) {
    if (raw[field] != null && raw[field] !== "") {
      failed.push(`forbidden_field:${field}`);
      delete raw[field];
    }
  }
  // Ownership never lands on Owner Name
  delete raw["Owner Name"];
  delete raw["Operator / Management Company"];

  const name = String(raw["Property Name"] || proposed.property_name || "").trim();
  const identity = String(
    raw["Property Identity Key"] || proposed.identity_key || ""
  ).trim();
  const country = String(raw.Country || "Peru").trim();

  if (!name) failed.push("required_property_name");
  if (!identity.startsWith("gov_pe_mincetur_")) failed.push("required_identity_key");
  if (country !== "Peru") failed.push("required_country_peru");

  const geo = resolveContinentSubContinentFromCountry("Peru");
  const today = new Date().toISOString().slice(0, 10);

  /** @type {Record<string, unknown>} */
  const enriched = {
    ...raw,
    "Property Name": name,
    "Canonical Property Name": raw["Canonical Property Name"] || name,
    "Property Identity Key": identity,
    Country: "Peru",
    Continent: geo?.continent || "South America",
    "Sub-Continent": geo?.subContinent || "South America",
    "Current Brand": raw["Current Brand"] || "Independent / Unconfirmed",
    "Affiliation Status": raw["Affiliation Status"] || "Unknown",
    "Production Use Status": raw["Production Use Status"] || "Candidate",
    "Human Review Required": true,
    "Enrichment Status": raw["Enrichment Status"] || "Not Started",
    "Discovery Date": raw["Discovery Date"] || today,
    "Last Reviewed Date": raw["Last Reviewed Date"] || today,
    "Data Eligible": raw["Data Eligible"] ?? true,
  };

  // Drop non-insert fields (Phone, VIC Freeze Hash, etc.)
  const sanitized = sanitizeInsertFields(enriched);
  for (const d of sanitized.dropped) {
    if (d.reason === "forbidden_on_insert") failed.push(`forbidden_field:${d.field}`);
  }

  // ownership_signal must never be in Airtable payload
  if (Object.prototype.hasOwnProperty.call(sanitized.fields, "Owner Name")) {
    failed.push("owner_name_must_not_be_written");
    delete sanitized.fields["Owner Name"];
  }

  const hardFails = failed.filter(
    (f) =>
      f.startsWith("required_") ||
      f === "owner_name_must_not_be_written" ||
      f.startsWith("forbidden_field:Owner") ||
      f.startsWith("forbidden_field:Operator")
  );

  return {
    ok:
      hardFails.length === 0 &&
      Boolean(sanitized.fields["Property Name"]) &&
      Boolean(sanitized.fields["Property Identity Key"]),
    failed: [...new Set(failed)],
    fields: sanitized.fields,
    dropped: sanitized.dropped,
    ownership_signal: proposed.ownership_signal || null,
    field_mapping: MAP_PERU_MINCETUR,
    allowlist: INSERT_ALLOWED_FIELDS,
    sanitized_payload_preview: sanitized.fields,
  };
}

/**
 * Re-dedupe proposed inserts against live HPC match results.
 * @param {object[]} prepared — { identity_key, fields, ... }
 * @param {object} hpc — loadHotelPropertyCensusReadOnly result
 */
export function rededupePeruMinceturInserts(prepared = [], hpc) {
  const censusData = {
    rows: hpc?.rows || [],
    byIdentityKey: hpc?.byIdentityKey || new Map(),
    byCountry: hpc?.byCountry || new Map(),
    tableId: hpc?.tableId,
    totalLoaded: hpc?.totalLoaded ?? 0,
  };
  const matchInputs = prepared.map((p) => ({
    sourceRecordId: p.identity_key,
    rawHotelName: p.fields?.["Property Name"] || "",
    rawCity: p.fields?.City || "",
    rawCountry: "Peru",
    rawWebsite: p.fields?.["Official Property URL"] || "",
    rawPhone: "",
    proposedIdentityKey: p.identity_key,
  }));
  const { rows: matches, summary } = matchAllCandidatesToHotelPropertyCensus(
    matchInputs,
    censusData,
    { identityKeyFn: (c) => c.proposedIdentityKey || "" }
  );
  const byKey = new Map(matches.map((m) => [String(m.sourceRecordId || m.proposedIdentityKey || ""), m]));

  const writable = [];
  const blocked = [];
  for (const p of prepared) {
    const m = byKey.get(String(p.identity_key)) || {};
    const action = m.recommendedAction;
    if (action === "likely_existing" || action === "possible_duplicate_review") {
      blocked.push({
        ...p,
        block_reason: action === "likely_existing" ? "hpc_likely_existing" : "hpc_possible_duplicate",
        hpc_match: m,
      });
      continue;
    }
    writable.push({ ...p, hpc_match: m });
  }
  return { writable, blocked, match_summary: summary };
}

/**
 * @param {object} opts
 */
export async function runPeruMinceturStewardInsertApply(opts = {}) {
  const args = opts.args || parsePeruMinceturStewardApplyArgs(opts.argv || []);
  const pack = opts.pack || null;

  if (!pack) {
    return {
      version: PERU_MINCETUR_STEWARD_APPLY_VERSION,
      status: PERU_MINCETUR_APPLY_STATUS.BLOCKED,
      apply_executed: false,
      airtable_writes: false,
      blocked_reason: "pack_missing",
    };
  }

  const writeTarget = assertProductionCensusWriteTarget({
    baseName: productionHotelPropertyCensus.baseName,
    tableName: productionHotelPropertyCensus.tableName,
    tableId: CENSUS_TABLE_ID,
  });
  if (!writeTarget.ok || CENSUS_TABLE_ID !== productionHotelPropertyCensus.tableId) {
    return {
      version: PERU_MINCETUR_STEWARD_APPLY_VERSION,
      status: PERU_MINCETUR_APPLY_STATUS.BLOCKED,
      apply_executed: false,
      airtable_writes: false,
      blocked_reason: BLOCKED_WRONG_CENSUS_TARGET,
      write_target: writeTarget,
    };
  }

  const proposed =
    pack.proposed_inserts ||
    pack.approval_bundle?.proposed_inserts ||
    [];
  const limited = proposed.slice(0, Math.max(1, args.pilotLimit || 25));

  const prepared = [];
  const invalid = [];
  for (const row of limited) {
    const prep = preparePeruMinceturStewardInsertFields(row);
    if (!prep.ok) {
      invalid.push({ identity_key: row.identity_key, failed: prep.failed });
      continue;
    }
    prepared.push({
      identity_key: row.identity_key,
      steward_tier: row.steward_tier,
      fields: prep.fields,
      ownership_signal: prep.ownership_signal,
      validation: prep,
    });
  }

  const hpc =
    opts.hpc ||
    (await loadHotelPropertyCensusReadOnly({ countryFilter: "Peru" }));
  const rededupe = rededupePeruMinceturInserts(prepared, hpc);

  const wantWrite = Boolean(args.enableProductionWrites || args.apply);
  const doWrite = Boolean(
    wantWrite && args.allConfirmsOk && opts.allowLiveWrite !== false && rededupe.writable.length
  );

  if (wantWrite && !args.allConfirmsOk) {
    return {
      version: PERU_MINCETUR_STEWARD_APPLY_VERSION,
      status: PERU_MINCETUR_APPLY_STATUS.BLOCKED,
      apply_executed: false,
      airtable_writes: false,
      blocked_reason: "missing_required_confirms",
      required_confirms: [...PERU_MINCETUR_REQUIRED_APPLY_CONFIRMS],
      confirms: args.confirms,
      dry_run_preview: {
        proposed: limited.length,
        prepared: prepared.length,
        invalid: invalid.length,
        writable: rededupe.writable.length,
        blocked_duplicates: rededupe.blocked.length,
      },
    };
  }

  const created = [];
  const createErrors = [];
  if (doWrite) {
    let createRecords = opts.createRecords || null;
    if (!createRecords) {
      const ctx = resolveLiveInsertContext();
      if (!ctx.token || !ctx.bases?.target_base_id) {
        return {
          version: PERU_MINCETUR_STEWARD_APPLY_VERSION,
          status: PERU_MINCETUR_APPLY_STATUS.BLOCKED,
          apply_executed: false,
          airtable_writes: false,
          blocked_reason: "missing_airtable_credentials",
        };
      }
      createRecords = async (rows) =>
        createHotelPropertyCensusRecords(ctx.bases.target_base_id, ctx.token, rows);
    }
    try {
      const result = await createRecords(
        rededupe.writable.map((r) => ({ fields: r.fields }))
      );
      created.push(...(result.created || []));
    } catch (err) {
      createErrors.push({ error: err?.message || String(err) });
    }
  }

  const status = !doWrite
    ? PERU_MINCETUR_APPLY_STATUS.DRY_RUN
    : createErrors.length
      ? PERU_MINCETUR_APPLY_STATUS.PARTIAL
      : rededupe.blocked.length || invalid.length
        ? PERU_MINCETUR_APPLY_STATUS.PARTIAL
        : PERU_MINCETUR_APPLY_STATUS.CLEAN;

  return {
    version: PERU_MINCETUR_STEWARD_APPLY_VERSION,
    pack_version: pack.version || PERU_MINCETUR_STEWARD_PACK_VERSION,
    status,
    dry_run: !doWrite,
    apply_executed: Boolean(doWrite),
    airtable_writes: Boolean(doWrite && created.length),
    ownership_writes: false,
    brand_explorer_writes: false,
    legacy_hotel_census_writes: false,
    production_target: productionHotelPropertyCensus,
    table_id: CENSUS_TABLE_ID,
    required_confirms: [...PERU_MINCETUR_REQUIRED_APPLY_CONFIRMS],
    confirms: args.confirms,
    summary: {
      proposed_in_pack_slice: limited.length,
      prepared_ok: prepared.length,
      invalid: invalid.length,
      writable_after_rededupe: rededupe.writable.length,
      blocked_duplicates: rededupe.blocked.length,
      created: created.length,
      create_errors: createErrors.length,
    },
    hpc: {
      tableId: hpc.tableId,
      matching_pool: hpc.rows?.length ?? 0,
      total_loaded: hpc.totalLoaded,
    },
    hpc_match_summary: rededupe.match_summary,
    invalid_sample: invalid.slice(0, 25),
    blocked_sample: rededupe.blocked.slice(0, 25).map((b) => ({
      identity_key: b.identity_key,
      block_reason: b.block_reason,
      matched_record_id: b.hpc_match?.matchedCensusRecordId || null,
    })),
    writable_preview: rededupe.writable.slice(0, 25).map((w) => ({
      identity_key: w.identity_key,
      property_name: w.fields?.["Property Name"],
      city: w.fields?.City,
      rooms: w.fields?.["Rooms / Keys"] ?? null,
      official_property_url: w.fields?.["Official Property URL"] || null,
      ownership_signal_ruc: w.ownership_signal?.tax_id || null,
      field_keys: Object.keys(w.fields || {}),
    })),
    created_record_ids: created.map((r) => r.id).filter(Boolean),
    create_errors: createErrors,
    note: doWrite
      ? createErrors.length
        ? "Peru MINCETUR steward insert partial — see create_errors"
        : "Peru MINCETUR steward insert applied to Hotel Property Census only"
      : "Dry-run only. Pass --enable-production-writes and all four confirms to write.",
  };
}
