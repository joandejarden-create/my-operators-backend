/**
 * Census Intake Autopilot — gated Hotel Property Census INSERT apply.
 *
 * Requires approval bundle from controlled dry-run + full confirms + env flags.
 * Writes only Hotel Property Census (tbl9aY5ijiuIzzWam). Legacy Hotel Census forbidden.
 */

import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { TABLE_IDS } from "../research-engine-v2/production-census-write.js";
import {
  assertProductionCensusWriteTarget,
  BLOCKED_WRONG_CENSUS_TARGET,
  productionHotelPropertyCensus,
  PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
} from "../research-engine-v2/production-census-source-of-truth.js";
import {
  createHotelPropertyCensusRecords,
  resolveLiveInsertContext,
} from "../research-engine-v2/census-autopilot-discovery-insert-apply.js";
import {
  INTAKE_APPLY_CONFIRMS,
  INTAKE_CONTROLLED_VERSION,
  INTAKE_INSERT_ALLOWED_FIELDS,
  validateIntakeApplyRow,
} from "./intake-autopilot-controlled.js";
import { isForbiddenAutopilotField } from "../research-engine-v2/census-autopilot-field-allowlist.js";
import { loadHotelPropertyCensusReadOnly } from "./match-hotel-property-census.js";

export { INTAKE_APPLY_CONFIRMS };
const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "../..");

export const INTAKE_APPLY_VERSION = "census-intake-autopilot-apply-v1";

export const INTAKE_APPLY_STATUS = Object.freeze({
  CLEAN: "census_intake_apply_clean",
  PARTIAL: "census_intake_apply_partial_needs_review",
  BLOCKED: "census_intake_apply_blocked",
  DRY_RUN: "census_intake_apply_dry_run",
});

const CENSUS_TABLE_ID =
  TABLE_IDS["Hotel Property Census"] || PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID;

function writeJson(fp, data) {
  mkdirSync(dirname(fp), { recursive: true });
  writeFileSync(fp, JSON.stringify(data, null, 2), "utf8");
}

/**
 * @param {string[]} [argv]
 */
export function parseIntakeApplyArgs(argv = process.argv.slice(2)) {
  const get = (flag, fallback = null) => {
    const i = argv.indexOf(flag);
    if (i >= 0 && argv[i + 1] && !argv[i + 1].startsWith("--")) return argv[i + 1];
    return fallback;
  };
  const has = (flag) => argv.includes(flag);

  const confirms = {};
  for (const flag of INTAKE_APPLY_CONFIRMS) {
    confirms[flag] = has(flag);
  }
  const allConfirmsOk = Object.values(confirms).every(Boolean);

  return {
    approvalBundlePath: get("--approval-bundle"),
    batchSize: Number(get("--batch-size", "10")) || 10,
    maxRecords: get("--max-records") != null ? Number(get("--max-records")) : null,
    cohort: get("--cohort", "no_hr"),
    output: get("--output", ""),
    dryRun: !has("--apply"),
    apply: has("--apply") && has("--enable-production-writes"),
    confirms,
    allConfirmsOk,
    confirmFlags: [...INTAKE_APPLY_CONFIRMS],
  };
}

export function checkIntakeApplyEnv(env = process.env) {
  const flags = {
    ALLOW_CENSUS_AUTOPILOT_APPLY:
      String(env.ALLOW_CENSUS_AUTOPILOT_APPLY || "").trim() === "1",
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
 * Load intake controlled approval bundle.
 * @param {string} bundlePath
 */
export function loadIntakeApprovalBundle(bundlePath) {
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
  if (bundle.version && !String(bundle.version).includes("intake")) {
    // allow controlled version string
  }
  if (bundle.legacy_hotel_census_used === true) {
    return { ok: false, error: "legacy_hotel_census_forbidden" };
  }
  if (bundle.approval_bundle_ready !== true) {
    return { ok: false, error: "approval_bundle_not_ready" };
  }
  if (bundle.write_target?.table_id && bundle.write_target.table_id !== CENSUS_TABLE_ID) {
    return {
      ok: false,
      error: "wrong_write_target",
      table_id: bundle.write_target.table_id,
    };
  }
  const inserts = Array.isArray(bundle.inserts) ? bundle.inserts : [];
  if (!inserts.length) {
    return { ok: false, error: "empty_inserts" };
  }

  const fieldViolations = [];
  for (const row of inserts) {
    for (const k of Object.keys(row.fields || {})) {
      if (isForbiddenAutopilotField(k)) {
        fieldViolations.push({
          source_record_id: row.source_record_id,
          field: k,
          reason: "forbidden",
        });
      }
      if (!INTAKE_INSERT_ALLOWED_FIELDS.includes(k)) {
        fieldViolations.push({
          source_record_id: row.source_record_id,
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
 * Block inserts whose Property Identity Key already exists in HPC.
 * @param {object[]} inserts
 * @param {object[]} censusRecords — Airtable records OR mapped HPC rows
 * @param {{ cohort?: 'no_hr'|'hr_only'|'all' }} [opts]
 */
export function rededupeIntakeInsertsByIdentityKey(
  inserts = [],
  censusRecords = [],
  opts = {}
) {
  const existingKeys = new Set();
  for (const rec of censusRecords) {
    const key = String(
      rec.identityKey ||
        rec.fields?.["Property Identity Key"] ||
        rec["Property Identity Key"] ||
        ""
    )
      .trim()
      .toLowerCase();
    if (key) existingKeys.add(key);
  }

  const writable = [];
  const blocked = [];
  const cohort = opts.cohort || "no_hr";

  for (const row of inserts) {
    const fields = { ...(row.fields || {}) };
    const identityKey = String(fields["Property Identity Key"] || "").trim();
    if (!identityKey) {
      blocked.push({ ...row, block_reason: "missing_property_identity_key" });
      continue;
    }
    if (existingKeys.has(identityKey.toLowerCase())) {
      blocked.push({
        ...row,
        block_reason: "property_identity_key_already_in_hpc",
        identity_key: identityKey,
      });
      continue;
    }

    const validation = validateIntakeApplyRow(fields, {
      lane: row.lane,
      intake_class: row.intake_class,
      hpc_recommended_action: "likely_new_candidate",
      quality_score: row.quality_score ?? null,
      cohort,
    });
    if (!validation.pass) {
      blocked.push({
        ...row,
        block_reason: "validation_failed",
        validation_failures: validation.failures,
      });
      continue;
    }

    writable.push({
      ...row,
      fields,
      identity_key: identityKey,
    });
  }

  return { writable, blocked };
}

/**
 * @param {object} opts
 */
export async function runIntakeAutopilotApply(opts = {}) {
  const args = opts.args || parseIntakeApplyArgs(opts.argv || []);
  const envCheck = checkIntakeApplyEnv(opts.env || process.env);
  const doWrite = Boolean(
    opts.doWrite && args.apply && args.allConfirmsOk && envCheck.allOk
  );

  if (!args.approvalBundlePath && !opts.bundlePath) {
    return {
      version: INTAKE_APPLY_VERSION,
      status: INTAKE_APPLY_STATUS.BLOCKED,
      apply_executed: false,
      blocked_reason: "missing_approval_bundle",
      airtable_writes: false,
    };
  }

  const loaded = loadIntakeApprovalBundle(
    args.approvalBundlePath || opts.bundlePath
  );
  if (!loaded.ok) {
    return {
      version: INTAKE_APPLY_VERSION,
      status: INTAKE_APPLY_STATUS.BLOCKED,
      apply_executed: false,
      blocked_reason: loaded.error,
      fieldViolations: loaded.fieldViolations || null,
      airtable_writes: false,
    };
  }

  if (args.cohort && loaded.bundle.cohort && args.cohort !== loaded.bundle.cohort) {
    return {
      version: INTAKE_APPLY_VERSION,
      status: INTAKE_APPLY_STATUS.BLOCKED,
      apply_executed: false,
      blocked_reason: `cohort_mismatch:cli=${args.cohort}:bundle=${loaded.bundle.cohort}`,
      airtable_writes: false,
    };
  }

  if (loaded.bundle.cohort && !["no_hr", "hr_only"].includes(loaded.bundle.cohort)) {
    return {
      version: INTAKE_APPLY_VERSION,
      status: INTAKE_APPLY_STATUS.BLOCKED,
      apply_executed: false,
      blocked_reason: "cohort_not_allowed_for_apply_use_no_hr_or_hr_only",
      bundle_cohort: loaded.bundle.cohort,
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
      version: INTAKE_APPLY_VERSION,
      status: INTAKE_APPLY_STATUS.BLOCKED,
      apply_executed: false,
      blocked_reason: BLOCKED_WRONG_CENSUS_TARGET,
      write_target: writeTarget,
      airtable_writes: false,
    };
  }

  if (doWrite && (!args.allConfirmsOk || !envCheck.allOk)) {
    return {
      version: INTAKE_APPLY_VERSION,
      status: INTAKE_APPLY_STATUS.BLOCKED,
      apply_executed: false,
      blocked_reason: "confirmation_or_env_missing",
      confirms: args.confirms,
      env_flags: envCheck.flags,
      env_missing: envCheck.missing,
      airtable_writes: false,
    };
  }

  let inserts = loaded.inserts;
  if (args.maxRecords != null && Number.isFinite(args.maxRecords)) {
    inserts = inserts.slice(0, args.maxRecords);
  }

  let censusRecords = opts.censusRecords || null;
  if (!censusRecords) {
    if (opts.skipLiveCensusRead) {
      censusRecords = [];
    } else {
      try {
        const loadedHpc = await loadHotelPropertyCensusReadOnly({});
        censusRecords = loadedHpc.rows || loadedHpc.records || [];
      } catch (err) {
        return {
          version: INTAKE_APPLY_VERSION,
          status: INTAKE_APPLY_STATUS.BLOCKED,
          apply_executed: false,
          blocked_reason: `hpc_rededupe_read_failed:${err.message}`,
          airtable_writes: false,
        };
      }
    }
  }

  const rededupe = rededupeIntakeInsertsByIdentityKey(inserts, censusRecords, {
    cohort: args.cohort || loaded.bundle.cohort || "no_hr",
  });

  if (doWrite && rededupe.writable.length === 0) {
    return {
      version: INTAKE_APPLY_VERSION,
      status: INTAKE_APPLY_STATUS.BLOCKED,
      apply_executed: false,
      blocked_reason: "no_writable_inserts_after_rededupe",
      blocked: rededupe.blocked,
      inserts_in_bundle: inserts.length,
      writable_after_rededupe: 0,
      airtable_writes: false,
    };
  }

  const batchSize = Math.min(10, args.batchSize || 10);
  const batches = [];
  for (let i = 0; i < rededupe.writable.length; i += batchSize) {
    batches.push(rededupe.writable.slice(i, i + batchSize));
  }

  let createRecords = opts.createRecords || null;
  if (doWrite && !createRecords && opts.useLiveAirtable !== false) {
    const ctx = resolveLiveInsertContext();
    if (!ctx.token || !ctx.bases?.target_base_id) {
      return {
        version: INTAKE_APPLY_VERSION,
        status: INTAKE_APPLY_STATUS.BLOCKED,
        apply_executed: false,
        blocked_reason: "missing_airtable_credentials",
        airtable_writes: false,
      };
    }
    createRecords = async (rows) =>
      createHotelPropertyCensusRecords(ctx.bases.target_base_id, ctx.token, rows);
  }

  const created = [];
  const createErrors = [];
  const checkpoint = {
    batches_completed: 0,
    identity_keys_created: [],
    record_ids_created: [],
    stopped: false,
  };

  if (doWrite && createRecords) {
    for (let bi = 0; bi < batches.length; bi += 1) {
      const batch = batches[bi];
      try {
        const result = await createRecords(batch.map((r) => ({ fields: r.fields })));
        for (const r of result.created || []) {
          created.push(r);
          const key = r.fields?.["Property Identity Key"];
          if (key) checkpoint.identity_keys_created.push(key);
          if (r.id) checkpoint.record_ids_created.push(r.id);
        }
        checkpoint.batches_completed = bi + 1;
        if (opts.checkpointDir) {
          writeJson(join(opts.checkpointDir, "intake-insert-checkpoint.json"), checkpoint);
        }
      } catch (err) {
        createErrors.push({ batch: bi, error: err?.message || String(err) });
        checkpoint.stopped = true;
        if (opts.checkpointDir) {
          writeJson(join(opts.checkpointDir, "intake-insert-checkpoint.json"), checkpoint);
        }
        break;
      }
    }
  } else if (doWrite && !createRecords) {
    return {
      version: INTAKE_APPLY_VERSION,
      status: INTAKE_APPLY_STATUS.BLOCKED,
      apply_executed: false,
      blocked_reason: "create_adapter_missing",
      airtable_writes: false,
    };
  }

  const status = !doWrite
    ? INTAKE_APPLY_STATUS.DRY_RUN
    : createErrors.length
      ? INTAKE_APPLY_STATUS.PARTIAL
      : rededupe.blocked.length
        ? INTAKE_APPLY_STATUS.PARTIAL
        : INTAKE_APPLY_STATUS.CLEAN;

  return {
    version: INTAKE_APPLY_VERSION,
    controlled_version: INTAKE_CONTROLLED_VERSION,
    status,
    apply_executed: Boolean(doWrite),
    airtable_writes: Boolean(doWrite && created.length),
    brand_explorer_writes: false,
    brand_setup_writes: false,
    legacy_hotel_census_used: false,
    write_target: {
      base: productionHotelPropertyCensus.baseName,
      table: productionHotelPropertyCensus.tableName,
      table_id: CENSUS_TABLE_ID,
    },
    batch_id: loaded.bundle.batch_id || null,
    cohort: loaded.bundle.cohort || args.cohort,
    approval_bundle: loaded.path,
    inserts_in_bundle: inserts.length,
    writable_after_rededupe: rededupe.writable.length,
    blocked_count: rededupe.blocked.length,
    blocked: rededupe.blocked.map((b) => ({
      source_record_id: b.source_record_id,
      identity_key: b.identity_key || b.fields?.["Property Identity Key"],
      property_name: b.fields?.["Property Name"],
      block_reason: b.block_reason,
      validation_failures: b.validation_failures || null,
    })),
    writable_preview: rededupe.writable.map((r) => ({
      source_record_id: r.source_record_id,
      identity_key: r.identity_key,
      property_name: r.fields["Property Name"],
      current_brand: r.fields["Current Brand"],
      city: r.fields.City,
      official_property_url: r.fields["Official Property URL"],
    })),
    batches_planned: batches.length,
    created_count: created.length,
    created_record_ids: created.map((r) => r.id).filter(Boolean),
    create_errors: createErrors,
    checkpoint,
    confirms_ok: args.allConfirmsOk,
    env_ok: envCheck.allOk,
    note: doWrite
      ? createErrors.length
        ? "Intake apply partial — see create_errors"
        : "Intake apply executed to Hotel Property Census (no_hr cohort)"
      : "Intake apply dry-run; production writes not executed",
  };
}

export function defaultIntakeApplyReportPath(batchId) {
  return join(
    ROOT,
    "reports",
    `census-intake-autopilot-apply-${batchId || "intake"}.json`
  );
}
