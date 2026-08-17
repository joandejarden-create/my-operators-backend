/**
 * Approval-bundle-bound Hotel Property Census INSERT apply for source_discovery.
 *
 * Build-only in this task — do not run production apply.
 * Writes only to Hotel Property Census (tbl9aY5ijiuIzzWam).
 * Re-dedupes before insert; stops on duplicate risk / wrong target.
 */

import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import {
  resolvePat,
  resolveTargetBase,
} from "./production-census-schema-create.js";
import { TABLE_IDS } from "./production-census-write.js";
import {
  assertProductionCensusWriteTarget,
  BLOCKED_WRONG_CENSUS_TARGET,
  productionHotelPropertyCensus,
} from "./production-census-source-of-truth.js";
import {
  INSERT_ALLOWED_FIELDS,
  INSERT_FORBIDDEN_FIELDS,
  MATCH_CLASS,
  SOURCE_DISCOVERY_QUEUE_ID,
  SOURCE_DISCOVERY_VERSION,
  indexHotelPropertyCensus,
  matchDiscoveredProperty,
  sanitizeInsertFields,
} from "./census-autopilot-source-discovery.js";
import { MAP_FIRST_PASS } from "./production-census-first-pass-enrichment.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "../..");

export const DISCOVERY_INSERT_APPLY_VERSION =
  "census-autopilot-discovery-insert-apply-v1";

export const INSERT_APPLY_STATUS = Object.freeze({
  CLEAN: "production_census_discovery_insert_apply_clean",
  PARTIAL: "production_census_discovery_insert_apply_partial_needs_review",
  BLOCKED: "production_census_discovery_insert_apply_blocked",
  DRY_RUN: "production_census_discovery_insert_apply_dry_run",
});

const CENSUS_TABLE_ID = TABLE_IDS["Hotel Property Census"];

function writeJson(fp, data) {
  mkdirSync(dirname(fp), { recursive: true });
  writeFileSync(fp, JSON.stringify(data, null, 2), "utf8");
}

/**
 * Parse insert-apply CLI args.
 * @param {string[]} argv
 */
export function parseDiscoveryInsertApplyArgs(argv = process.argv.slice(2)) {
  const get = (flag, fallback = null) => {
    const i = argv.indexOf(flag);
    if (i >= 0 && argv[i + 1] && !argv[i + 1].startsWith("--")) return argv[i + 1];
    return fallback;
  };
  const has = (flag) => argv.includes(flag);
  const confirms = {
    safeWrites: has("--confirm-safe-writes"),
    writeToProductionCensus: has("--confirm-write-to-production-census"),
    noBrandExplorer: has("--confirm-no-brand-explorer-writes"),
    noOwnerOperator: has("--confirm-no-owner-operator"),
    noDateWrites: has("--confirm-no-date-writes"),
    noRecentMomentum: has("--confirm-no-recent-momentum"),
    noCompanyValidation: has("--confirm-no-company-validation"),
    webhoundNotProduction: has("--confirm-webhound-not-production-source"),
  };
  const allConfirmsOk = Object.values(confirms).every(Boolean);
  return {
    mode: get("--mode", "controlled"),
    apply: has("--enable-production-writes") && get("--mode") === "apply",
    approvalBundlePath: get("--approval-bundle"),
    batchSize: Number(get("--batch-size", "100")) || 100,
    region: get("--region", "CALA"),
    queue: get("--queue", SOURCE_DISCOVERY_QUEUE_ID),
    confirms,
    allConfirmsOk,
  };
}

export function checkDiscoveryInsertApplyEnv(env = process.env) {
  const flags = {
    ALLOW_CENSUS_AUTOPILOT_APPLY: String(env.ALLOW_CENSUS_AUTOPILOT_APPLY || "").trim() === "1",
    CONFIRM_WRITE_TO_PRODUCTION_CENSUS:
      String(env.CONFIRM_WRITE_TO_PRODUCTION_CENSUS || "").trim() === "1",
    CONFIRM_NO_BRAND_EXPLORER_WRITES:
      String(env.CONFIRM_NO_BRAND_EXPLORER_WRITES || "").trim() === "1",
    CONFIRM_NO_OWNER_OPERATOR_WRITES:
      String(env.CONFIRM_NO_OWNER_OPERATOR_WRITES || "").trim() === "1",
  };
  return {
    flags,
    allOk: Object.values(flags).every(Boolean),
    missing: Object.entries(flags)
      .filter(([, v]) => !v)
      .map(([k]) => k),
  };
}

/**
 * Load insert proposals from discovery approval bundle.
 */
export function loadDiscoveryInsertApprovalBundle(bundlePath) {
  const abs = resolve(bundlePath);
  if (!existsSync(abs)) {
    return { ok: false, error: "approval_bundle_missing", path: abs };
  }
  let bundle;
  try {
    bundle = JSON.parse(readFileSync(abs, "utf8"));
  } catch (err) {
    return { ok: false, error: `approval_bundle_parse:${err.message}` };
  }
  if (bundle.type !== "hotel_property_census_insert_approval_bundle") {
    return { ok: false, error: "not_insert_approval_bundle", type: bundle.type };
  }
  if (bundle.queue && bundle.queue !== SOURCE_DISCOVERY_QUEUE_ID) {
    return { ok: false, error: "wrong_queue", queue: bundle.queue };
  }
  const inserts = bundle.proposed_inserts || [];
  const fieldViolations = [];
  for (const row of inserts) {
    for (const k of Object.keys(row.fields || {})) {
      if (INSERT_FORBIDDEN_FIELDS.includes(k)) {
        fieldViolations.push({ identity_key: row.identity_key, field: k, reason: "forbidden" });
      }
      if (!INSERT_ALLOWED_FIELDS.includes(k)) {
        fieldViolations.push({
          identity_key: row.identity_key,
          field: k,
          reason: "not_allowlisted",
        });
      }
    }
  }
  if (fieldViolations.length) {
    return { ok: false, error: "forbidden_or_non_allowlisted_fields", fieldViolations };
  }
  return { ok: true, bundle, inserts, path: abs };
}

/**
 * Re-dedupe inserts against live (or injected) Hotel Property Census.
 */
export function rededupeInsertsAgainstCensus(inserts = [], censusRecords = []) {
  const index = indexHotelPropertyCensus(censusRecords);
  const writable = [];
  const blocked = [];
  const steward = [];

  for (const row of inserts) {
    const discovered = {
      identity_key: row.identity_key,
      official_property_id: row.official_property_id || row.discovery?.official_property_id,
      property_name: row.property_name,
      brand: row.brand || row.fields?.["Current Brand"],
      city: row.fields?.City || row.discovery?.city,
      country: row.fields?.Country || row.discovery?.country,
      address: row.fields?.Address,
      official_property_url:
        row.fields?.["Official Property URL"] || row.discovery?.official_property_url,
      source_family: row.source_family,
      identity_confidence: "High",
      source_confidence: "High",
    };
    const match = matchDiscoveredProperty(discovered, index);
    if (
      match.classification === MATCH_CLASS.EXISTING_EXACT ||
      match.classification === MATCH_CLASS.DUPLICATE_RISK
    ) {
      blocked.push({
        ...row,
        block_reason: "duplicate_detected_on_rededupe",
        match,
      });
      continue;
    }
    if (
      match.classification === MATCH_CLASS.EXISTING_PROBABLE ||
      match.classification === MATCH_CLASS.STEWARD
    ) {
      steward.push({ ...row, steward_reason: match.classification, match });
      continue;
    }
    const sanitized = sanitizeInsertFields(row.fields || {});
    if (!sanitized.fields["Property Identity Key"] || !sanitized.fields["Property Name"]) {
      blocked.push({ ...row, block_reason: "invalid_insert_payload", dropped: sanitized.dropped });
      continue;
    }
    writable.push({ ...row, fields: sanitized.fields });
  }

  return { writable, blocked, steward, stop_on_duplicate: blocked.some((b) => b.block_reason === "duplicate_detected_on_rededupe") };
}

/**
 * Dry-run or apply discovery inserts (apply gated — not invoked in this task).
 * @param {object} opts
 */
export async function runDiscoveryInsertApply(opts = {}) {
  const args = opts.args || parseDiscoveryInsertApplyArgs(opts.argv || []);
  const envCheck = checkDiscoveryInsertApplyEnv(opts.env || process.env);
  const doWrite = Boolean(opts.doWrite && args.apply && args.allConfirmsOk && envCheck.allOk);

  const loaded = loadDiscoveryInsertApprovalBundle(args.approvalBundlePath || opts.bundlePath);
  if (!loaded.ok) {
    return {
      version: DISCOVERY_INSERT_APPLY_VERSION,
      status: INSERT_APPLY_STATUS.BLOCKED,
      apply_executed: false,
      blocked_reason: loaded.error,
      fieldViolations: loaded.fieldViolations || null,
      airtable_writes: false,
    };
  }

  const writeTarget = assertProductionCensusWriteTarget({
    baseName: productionHotelPropertyCensus.baseName,
    tableName: productionHotelPropertyCensus.tableName,
    tableId: CENSUS_TABLE_ID,
  });
  if (!writeTarget.ok || CENSUS_TABLE_ID !== productionHotelPropertyCensus.tableId) {
    return {
      version: DISCOVERY_INSERT_APPLY_VERSION,
      status: INSERT_APPLY_STATUS.BLOCKED,
      apply_executed: false,
      blocked_reason: BLOCKED_WRONG_CENSUS_TARGET,
      write_target: writeTarget,
      airtable_writes: false,
    };
  }

  if (doWrite && (!args.allConfirmsOk || !envCheck.allOk)) {
    return {
      version: DISCOVERY_INSERT_APPLY_VERSION,
      status: INSERT_APPLY_STATUS.BLOCKED,
      apply_executed: false,
      blocked_reason: "confirmation_or_env_missing",
      confirms: args.confirms,
      env_flags: envCheck.flags,
      airtable_writes: false,
    };
  }

  const censusRecords = opts.censusRecords || [];
  const rededupe = rededupeInsertsAgainstCensus(loaded.inserts, censusRecords);

  // Founder intent: steward/block duplicates and continue with clean inserts.
  // Only hard-stop the whole apply when nothing is writable and duplicates exist,
  // or when an injected create adapter is missing for a live write request.
  if (doWrite && rededupe.writable.length === 0) {
    return {
      version: DISCOVERY_INSERT_APPLY_VERSION,
      status:
        rededupe.blocked.length || rededupe.steward.length
          ? INSERT_APPLY_STATUS.BLOCKED
          : INSERT_APPLY_STATUS.BLOCKED,
      apply_executed: false,
      blocked_reason:
        rededupe.blocked.length || rededupe.steward.length
          ? "no_writable_inserts_after_rededupe"
          : "empty_writable_set",
      blocked: rededupe.blocked,
      steward: rededupe.steward,
      airtable_writes: false,
      inserts_in_bundle: loaded.inserts.length,
      writable_after_rededupe: 0,
    };
  }

  const batchSize = args.batchSize || 100;
  const batches = [];
  for (let i = 0; i < rededupe.writable.length; i += batchSize) {
    batches.push(rededupe.writable.slice(i, i + batchSize));
  }

  /** Injected create adapter for tests; live Airtable create when doWrite + no inject. */
  let createRecords = opts.createRecords || null;
  if (doWrite && !createRecords && opts.useLiveAirtable) {
    const ctx = resolveLiveInsertContext();
    if (!ctx.token || !ctx.bases?.target_base_id) {
      return {
        version: DISCOVERY_INSERT_APPLY_VERSION,
        status: INSERT_APPLY_STATUS.BLOCKED,
        apply_executed: false,
        blocked_reason: "missing_airtable_credentials",
        airtable_writes: false,
      };
    }
    const liveTarget = assertProductionCensusWriteTarget({
      baseName: productionHotelPropertyCensus.baseName,
      baseId: ctx.bases.target_base_id,
      tableName: productionHotelPropertyCensus.tableName,
      tableId: CENSUS_TABLE_ID,
    });
    if (!liveTarget.ok) {
      return {
        version: DISCOVERY_INSERT_APPLY_VERSION,
        status: INSERT_APPLY_STATUS.BLOCKED,
        apply_executed: false,
        blocked_reason: BLOCKED_WRONG_CENSUS_TARGET,
        write_target: liveTarget,
        airtable_writes: false,
      };
    }
    createRecords = async (rows) =>
      createHotelPropertyCensusRecords(ctx.bases.target_base_id, ctx.token, rows);
  }

  const created = [];
  const createErrors = [];
  let checkpoint = {
    batches_completed: 0,
    identity_keys_created: [],
    record_ids_created: [],
    stopped: false,
    blocked_duplicates: rededupe.blocked.map((b) => b.identity_key),
    steward_routed: rededupe.steward.map((b) => b.identity_key),
  };

  if (doWrite && createRecords) {
    for (let bi = 0; bi < batches.length; bi += 1) {
      const batch = batches[bi];
      try {
        const result = await createRecords(batch.map((r) => ({ fields: r.fields })));
        for (const r of result.created || []) {
          created.push(r);
          checkpoint.identity_keys_created.push(
            r.fields?.[MAP_FIRST_PASS.identityKey] || r.id
          );
          if (r.id) checkpoint.record_ids_created.push(r.id);
        }
        checkpoint.batches_completed = bi + 1;
        if (opts.checkpointDir) {
          writeJson(join(opts.checkpointDir, "insert-checkpoint.json"), checkpoint);
          writeJson(join(opts.checkpointDir, "checkpoint.json"), {
            ...checkpoint,
            mode: "apply",
            queue: SOURCE_DISCOVERY_QUEUE_ID,
            table_id: CENSUS_TABLE_ID,
          });
        }
      } catch (err) {
        createErrors.push({ batch: bi, error: err?.message || String(err) });
        checkpoint.stopped = true;
        if (opts.checkpointDir) {
          writeJson(join(opts.checkpointDir, "insert-checkpoint.json"), checkpoint);
        }
        break;
      }
    }
  } else if (doWrite && !createRecords) {
    return {
      version: DISCOVERY_INSERT_APPLY_VERSION,
      status: INSERT_APPLY_STATUS.BLOCKED,
      apply_executed: false,
      blocked_reason: "create_adapter_missing",
      airtable_writes: false,
      note: "Live apply requires useLiveAirtable or injected createRecords",
    };
  }

  const status = !doWrite
    ? INSERT_APPLY_STATUS.DRY_RUN
    : createErrors.length
      ? INSERT_APPLY_STATUS.PARTIAL
      : rededupe.blocked.length || rededupe.steward.length
        ? INSERT_APPLY_STATUS.PARTIAL
        : INSERT_APPLY_STATUS.CLEAN;

  return {
    version: DISCOVERY_INSERT_APPLY_VERSION,
    source_discovery_version: SOURCE_DISCOVERY_VERSION,
    status,
    apply_executed: Boolean(doWrite),
    airtable_writes: Boolean(doWrite && created.length),
    brand_explorer_writes: false,
    brand_setup_writes: false,
    vic_writes: false,
    production_target: productionHotelPropertyCensus,
    table_id: CENSUS_TABLE_ID,
    inserts_in_bundle: loaded.inserts.length,
    writable_after_rededupe: rededupe.writable.length,
    blocked_duplicates: rededupe.blocked.length,
    steward_routed: rededupe.steward.length,
    blocked: rededupe.blocked,
    steward: rededupe.steward,
    batches_planned: batches.length,
    created_count: created.length,
    created_record_ids: created.map((r) => r.id).filter(Boolean),
    create_errors: createErrors,
    checkpoint,
    note: doWrite
      ? createErrors.length
        ? "Insert apply partial — see create_errors"
        : rededupe.blocked.length || rededupe.steward.length
          ? "Insert apply completed; duplicates/steward cases skipped"
          : "Insert apply executed to Hotel Property Census"
      : "Insert apply dry-run; production writes not executed",
  };
}

/**
 * Live Airtable create helper (exported for future apply — not called by default).
 */
export async function createHotelPropertyCensusRecords(baseId, token, records = []) {
  const target = assertProductionCensusWriteTarget({
    baseName: productionHotelPropertyCensus.baseName,
    baseId,
    tableName: productionHotelPropertyCensus.tableName,
    tableId: CENSUS_TABLE_ID,
  });
  if (!target.ok) {
    throw new Error(BLOCKED_WRONG_CENSUS_TARGET);
  }
  const created = [];
  for (let i = 0; i < records.length; i += 10) {
    const chunk = records.slice(i, i + 10);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(CENSUS_TABLE_ID)}`,
      {
        method: "POST",
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ records: chunk, typecast: true }),
      }
    );
    const json = await res.json();
    if (!res.ok) {
      throw new Error(`airtable_create_${res.status}:${JSON.stringify(json.error || json)}`);
    }
    created.push(...(json.records || []));
  }
  return { created };
}

export function resolveLiveInsertContext() {
  return {
    token: resolvePat(),
    bases: resolveTargetBase(),
    tableId: CENSUS_TABLE_ID,
  };
}
