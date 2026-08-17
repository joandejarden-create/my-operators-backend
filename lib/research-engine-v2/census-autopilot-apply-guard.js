/**
 * Autopilot apply guard v2 — confirms, env, proposal guards.
 * Fail-closed: production writes only to Hotel Property Census (tbl9aY5ijiuIzzWam).
 */

import { isWritableConfidence, normalizeConfidence } from "./census-autopilot-confidence.js";
import {
  AUTOPILOT_FORBIDDEN_FIELDS,
  AUTOPILOT_PROTECTED_BRAND_EXPLORER_HINTS,
  AUTOPILOT_TARGET_BASE_LABEL,
  AUTOPILOT_TARGET_TABLE,
  AUTOPILOT_TARGET_TABLE_ID,
  isForbiddenAutopilotField,
  sanitizeAutopilotPatch,
} from "./census-autopilot-field-allowlist.js";
import { validateCoordinatePatch } from "./census-autopilot-idempotent-writer.js";
import {
  assertProductionCensusWriteTarget,
  BLOCKED_WRONG_CENSUS_TARGET,
  productionHotelPropertyCensus,
} from "./production-census-source-of-truth.js";

export {
  AUTOPILOT_FORBIDDEN_FIELDS,
  AUTOPILOT_PROTECTED_BRAND_EXPLORER_HINTS,
} from "./census-autopilot-field-allowlist.js";

export {
  assertProductionCensusWriteTarget,
  BLOCKED_WRONG_CENSUS_TARGET,
  productionHotelPropertyCensus,
};

export const DEFAULT_APPLY_CONFIRMS = Object.freeze([
  "--confirm-safe-writes",
  "--confirm-write-to-production-census",
  "--confirm-no-brand-explorer-writes",
  "--confirm-no-owner-operator",
  "--confirm-no-date-writes",
  "--confirm-no-recent-momentum",
  "--confirm-no-company-validation",
  "--confirm-webhound-not-production-source",
]);

/**
 * Parse Autopilot CLI args (v2 batch model).
 * @param {string[]} argv
 */
export function parseAutopilotArgs(argv = process.argv.slice(2)) {
  const get = (flag, fallback = null) => {
    const i = argv.indexOf(flag);
    if (i >= 0 && argv[i + 1] && !argv[i + 1].startsWith("--")) return argv[i + 1];
    return fallback;
  };
  const has = (flag) => argv.includes(flag);
  const getNum = (flag, fallback = null) => {
    const raw = get(flag, null);
    if (raw == null) return fallback;
    const n = Number(raw);
    return Number.isFinite(n) ? n : fallback;
  };

  const modeRaw = String(get("--mode", "plan") || "plan").toLowerCase();
  const mode = [
    "plan",
    "dry-run",
    "apply",
    "controlled",
    "schema-apply",
    "production-cycle",
    "mission",
  ].includes(modeRaw)
    ? modeRaw
    : "plan";

  const resume = get("--resume", null);
  const runUntilComplete = has("--run-until-complete");
  const batchSizeDefault =
    mode === "apply" || mode === "production-cycle" || mode === "mission"
      ? 100
      : 250;
  let batchSize = getNum("--batch-size", null);
  const maxRecords = getNum("--max-records", null);
  const maxPasses = getNum("--max-passes", null);
  const legacyLimit = getNum("--limit", null);
  const warnings = [];
  const objective = get("--objective", null);

  if (legacyLimit != null) {
    warnings.push(
      "--limit is deprecated for production scope; use --batch-size (chunk) and --run-until-complete (full parent/region). --limit treated as dry-run/test alias only."
    );
    if (batchSize == null && (mode === "dry-run" || mode === "controlled" || mode === "plan")) {
      batchSize = legacyLimit;
    }
  }
  if (batchSize == null) batchSize = batchSizeDefault;

  const confirms = {
    safeWrites: has("--confirm-safe-writes"),
    writeToProductionCensus: has("--confirm-write-to-production-census"),
    noOwnerOperator:
      has("--confirm-no-owner-operator") || has("--confirm-no-owner-operator-writes"),
    noDateWrites: has("--confirm-no-date-writes") || has("--confirm-no-room-date-writes"),
    noBrandExplorer: has("--confirm-no-brand-explorer-writes"),
    noRecentMomentum: has("--confirm-no-recent-momentum"),
    noCompanyValidation: has("--confirm-no-company-validation"),
    webhoundNotProduction: has("--confirm-webhound-not-production-source"),
    schemaApply: has("--confirm-schema-v114-rooms-provenance"),
    approvalBundleBound: has("--confirm-approval-bundle-bound"),
  };

  const allApplyConfirms =
    confirms.safeWrites &&
    confirms.writeToProductionCensus &&
    confirms.noOwnerOperator &&
    confirms.noDateWrites &&
    confirms.noBrandExplorer &&
    confirms.noRecentMomentum &&
    confirms.noCompanyValidation &&
    confirms.webhoundNotProduction;

  const parentCompany = get("--parent-company") || get("--parent") || null;
  const scopeRaw = String(
    get("--scope", parentCompany ? "parent-company" : "official-parent-inventory") || ""
  )
    .trim()
    .toLowerCase();
  const scope = [
    "active-brand-setup",
    "parent-company",
    "official-parent-inventory",
    "full-cala-universe",
  ].includes(scopeRaw)
    ? scopeRaw
    : parentCompany
      ? "parent-company"
      : "official-parent-inventory";
  const strategyRaw = String(get("--strategy", "fastest-safe") || "fastest-safe")
    .trim()
    .toLowerCase();
  const strategy = strategyRaw || "fastest-safe";
  const country = get("--country", null);
  const countriesRaw = get("--countries", null);
  const countries = countriesRaw
    ? String(countriesRaw)
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean)
    : country
      ? [country]
      : null;
  const maxInserts = getNum("--max-inserts", null);
  const queueRaw = get("--queue", null);
  const queueStr = queueRaw ? String(queueRaw).trim() : null;
  /** @type {string[]} */
  const queues = queueStr
    ? queueStr
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean)
    : [];
  /** Primary queue (first) for legacy single-queue callers. */
  const queue = queues.length === 1 ? queues[0] : queues.length > 1 ? queues.join(",") : null;
  const cleanupExistingOnly = has("--cleanup-existing-only");
  const approvalBundle = get("--approval-bundle", null);
  const recordSet = get("--record-set", null);
  const recordId = get("--record-id", null);
  const propertyCode = get("--property-code", null);

  const censusModeRaw = String(get("--census-mode", "growth") || "growth")
    .trim()
    .toLowerCase();
  const censusMode = [
    "growth",
    "field-completion-only",
    "governance-only",
    "universe-shell",
    "identity-linkage-only",
    "shell-backfill",
    "census_growth",
    "census-growth",
  ].includes(censusModeRaw)
    ? censusModeRaw === "census_growth" || censusModeRaw === "census-growth"
      ? "growth"
      : censusModeRaw
    : "growth";

  return {
    parentCompany,
    region: get("--region") || "CALA",
    country: country || get("--country") || null,
    countries,
    maxInserts,
    brand: get("--brand") || null,
    scope,
    strategy,
    queue,
    queues,
    cleanupExistingOnly,
    censusMode,
    mode,
    objective,
    resume,
    runUntilComplete,
    batchSize,
    maxRecords,
    maxPasses,
    /** @deprecated use batchSize / maxRecords / runUntilComplete */
    limit: legacyLimit,
    legacyLimit,
    confidenceThreshold: get("--confidence-threshold", "High"),
    provider: get("--provider") || null,
    concurrency: getNum("--concurrency", 2),
    skipBeGates: has("--skip-be-gates"),
    live: has("--live"),
    approvalBundle,
    recordSet,
    recordId,
    propertyCode,
    confirms,
    allApplyConfirms,
    warnings,
  };
}

/**
 * Validate apply environment flags.
 * @param {NodeJS.ProcessEnv} [env]
 */
export function checkAutopilotApplyEnv(env = process.env) {
  const flags = {
    ALLOW_CENSUS_AUTOPILOT_APPLY: String(env.ALLOW_CENSUS_AUTOPILOT_APPLY || "").trim() === "1",
    CONFIRM_NO_BRAND_EXPLORER_WRITES:
      String(env.CONFIRM_NO_BRAND_EXPLORER_WRITES || "").trim() === "1",
    CONFIRM_NO_OWNER_OPERATOR_WRITES:
      String(env.CONFIRM_NO_OWNER_OPERATOR_WRITES || "").trim() === "1",
    CONFIRM_WRITE_TO_PRODUCTION_CENSUS:
      String(env.CONFIRM_WRITE_TO_PRODUCTION_CENSUS || "").trim() === "1",
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
 * Inspect a patch object for forbidden / protected fields.
 * @param {Record<string, unknown>} patch
 */
export function findForbiddenPatchFields(patch = {}) {
  const hits = [];
  for (const key of Object.keys(patch || {})) {
    if (isForbiddenAutopilotField(key)) {
      hits.push({ field: key, reason: "forbidden_field" });
    }
  }
  return hits;
}

/**
 * Guard a single proposal before apply.
 * @param {object} proposal
 * @param {{ threshold?: string, allowGeocode?: boolean, schemaV114Ready?: boolean }} [opts]
 */
export function guardProposal(proposal, opts = {}) {
  const errors = [];
  const softBlocks = [];
  const rawFields = proposal.patch || proposal.fields || {};
  const forbidden = findForbiddenPatchFields(rawFields);
  if (forbidden.length) {
    errors.push(...forbidden.map((f) => `${f.reason}:${f.field}`));
  }

  const sanitized = sanitizeAutopilotPatch(rawFields, {
    allowGeocode: opts.allowGeocode,
    schemaV114Ready: opts.schemaV114Ready,
  });
  for (const d of sanitized.dropped) {
    if (d.reason === "provider_decision_needed") softBlocks.push(`provider_decision_needed:${d.field}`);
    else if (d.reason === "forbidden") errors.push(`forbidden_field:${d.field}`);
  }

  if (proposal.held || proposal.human_review_required) errors.push("held_record");
  if (proposal.brand_unconfirmed) errors.push("brand_unconfirmed");
  if (proposal.source_missing) errors.push("source_support_missing");
  if (proposal.room_count_ambiguous) errors.push("room_count_ambiguous");
  if (proposal.coordinate_invalid) errors.push("coordinate_invalid");

  const coordCheck = validateCoordinatePatch(sanitized.fields);
  if (!coordCheck.ok) errors.push(coordCheck.reason);

  // Geocode-only proposal without provider → soft route, not hard stop of whole run
  const onlyGeocodeSoft =
    Object.keys(sanitized.fields).length === 0 &&
    softBlocks.length > 0 &&
    errors.length === 0;

  const conf = normalizeConfidence(proposal.confidence || "Low");
  if (
    Object.keys(sanitized.fields).length &&
    !isWritableConfidence(conf, { threshold: opts.threshold })
  ) {
    errors.push(`confidence_below_threshold:${conf}`);
  }
  if (proposal.source === "webhound" || proposal.webhound_direct_write) {
    errors.push("webhound_direct_write_forbidden");
  }

  return {
    ok: errors.length === 0 && Object.keys(sanitized.fields).length > 0,
    errors,
    soft_blocks: softBlocks,
    provider_decision_needed: onlyGeocodeSoft || softBlocks.some((s) => s.startsWith("provider_")),
    confidence: conf,
    sanitized_fields: sanitized.fields,
  };
}

/**
 * Guard a batch of proposals; returns writable vs blocked vs provider-routed.
 * Protected-field patches stop the whole apply (safety).
 * Provider soft-blocks do NOT stop the run.
 * @param {object[]} proposals
 * @param {object} [opts]
 */
export function guardApplyBatch(proposals = [], opts = {}) {
  const writable = [];
  const blocked = [];
  const provider_decision_needed = [];

  for (const p of proposals) {
    const g = guardProposal(p, opts);
    if (g.ok) {
      writable.push({ ...p, patch: g.sanitized_fields, guard: g });
    } else if (g.provider_decision_needed && g.errors.length === 0) {
      provider_decision_needed.push({ ...p, guard: g });
    } else {
      blocked.push({ ...p, guard: g });
    }
  }

  const stop_all = proposals.some((p) => {
    const fields = p.patch || p.fields || {};
    return findForbiddenPatchFields(fields).length > 0;
  });

  return {
    writable,
    blocked,
    provider_decision_needed,
    ok: !stop_all,
    stop_all,
  };
}

/**
 * Validate Autopilot write target before any production patch.
 * Fail closed with blocked_wrong_census_target when unverified / wrong / ambiguous.
 *
 * @param {{
 *   baseName?: string,
 *   base?: string,
 *   baseId?: string,
 *   tableName?: string,
 *   table?: string,
 *   tableId?: string,
 *   table_id?: string,
 * }} [target]
 * @param {{ requireLiveBaseId?: boolean }} [opts]
 */
export function guardProductionCensusWriteTarget(target = {}, opts = {}) {
  const resolved = {
    baseName: target.baseName || target.base || AUTOPILOT_TARGET_BASE_LABEL,
    baseId: target.baseId,
    tableName: target.tableName || target.table || AUTOPILOT_TARGET_TABLE,
    tableId: target.tableId || target.table_id || AUTOPILOT_TARGET_TABLE_ID,
  };
  const check = assertProductionCensusWriteTarget(resolved, opts);
  return {
    ...check,
    ok: check.ok,
    stop_apply: !check.ok,
    stop_reason: check.ok ? null : BLOCKED_WRONG_CENSUS_TARGET,
  };
}

/**
 * Preflight for Autopilot apply mode.
 * @param {ReturnType<typeof parseAutopilotArgs>} args
 * @param {object} [envCheck]
 * @param {{ writeTarget?: object }} [opts]
 */
/** Modes that may perform production Hotel Property Census writes when confirms+env set. */
export function isProductionWriteMode(mode) {
  return mode === "apply" || mode === "production-cycle" || mode === "mission";
}

export function applyPreflight(args, envCheck = checkAutopilotApplyEnv(), opts = {}) {
  const blockers = [];
  const writeMode = isProductionWriteMode(args.mode);
  if (writeMode && !args.allApplyConfirms) {
    blockers.push("missing_cli_confirm_flags");
    const missing = DEFAULT_APPLY_CONFIRMS.filter((flag) => {
      const map = {
        "--confirm-safe-writes": args.confirms.safeWrites,
        "--confirm-write-to-production-census": args.confirms.writeToProductionCensus,
        "--confirm-no-brand-explorer-writes": args.confirms.noBrandExplorer,
        "--confirm-no-owner-operator": args.confirms.noOwnerOperator,
        "--confirm-no-date-writes": args.confirms.noDateWrites,
        "--confirm-no-recent-momentum": args.confirms.noRecentMomentum,
        "--confirm-no-company-validation": args.confirms.noCompanyValidation,
        "--confirm-webhound-not-production-source": args.confirms.webhoundNotProduction,
      };
      return !map[flag];
    });
    if (missing.length) blockers.push(`missing_flags:${missing.join(",")}`);
  }
  if (writeMode && !envCheck.allOk) {
    blockers.push(`missing_env:${envCheck.missing.join(",")}`);
  }
  if (
    !args.resume &&
    args.scope !== "active-brand-setup" &&
    args.scope !== "official-parent-inventory" &&
    !args.parentCompany
  ) {
    blockers.push("missing_parent_company");
  }
  if (!args.resume && !args.region) blockers.push("missing_region");

  const writeTargetGuard = guardProductionCensusWriteTarget(
    opts.writeTarget || {
      baseName: AUTOPILOT_TARGET_BASE_LABEL,
      tableName: AUTOPILOT_TARGET_TABLE,
      tableId: AUTOPILOT_TARGET_TABLE_ID,
    },
    { requireLiveBaseId: Boolean(opts.requireLiveBaseId) }
  );
  if (writeMode && !writeTargetGuard.ok) {
    blockers.push(BLOCKED_WRONG_CENSUS_TARGET);
    for (const e of writeTargetGuard.errors || []) blockers.push(e);
  }

  return {
    ok: blockers.length === 0,
    blockers,
    env: envCheck,
    confirms: args.confirms,
    warnings: args.warnings || [],
    write_target: writeTargetGuard,
    production_census: productionHotelPropertyCensus,
  };
}
