/**
 * Approval-bundle-bound Property Name cleanup apply.
 * Freezes High proposals from controlled dry-run; never re-plans.
 */

import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
  resolvePat,
  resolveTargetBase,
} from "./production-census-schema-create.js";
import { TABLE_IDS } from "./production-census-write.js";
import { AUTOPILOT_FORBIDDEN_FIELDS } from "./census-autopilot-field-allowlist.js";
import { compareFieldValues } from "./census-autopilot-idempotent-writer.js";
import { classifyPropertyNameProblems } from "./production-census-property-name-cleanup-extractor.js";
import { MAP_FIRST_PASS } from "./production-census-first-pass-enrichment.js";
import {
  assertProductionCensusWriteTarget,
  BLOCKED_WRONG_CENSUS_TARGET,
  productionHotelPropertyCensus,
} from "./production-census-source-of-truth.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "../..");

export const NAME_CLEANUP_APPLY_VERSION =
  "census-autopilot-property-name-cleanup-approval-bundle-apply-v1";

export const STATUS = Object.freeze({
  CLEAN: "production_census_property_name_cleanup_apply_clean",
  PARTIAL: "production_census_property_name_cleanup_apply_partial_needs_review",
  BLOCKED: "production_census_property_name_cleanup_apply_blocked",
});

export const APPROVED_NAME_PATCH_FIELDS = Object.freeze([
  "Property Name",
  "Last Reviewed Date",
  "Enrichment Status",
  "Enrichment Priority",
]);

/** Founder-approved exact mapping (identity_key → proposed name). */
export const FOUNDER_APPROVED_NAME_MAP = Object.freeze({
  ind_ihg_mx_tijav: "avid hotels Tijuana - Otay",
  ind_ihg_mx_zclav: "avid hotels Fresnillo",
  ind_ihg_mx_gdlet: "avid hotels Guadalajara Aeropuerto Norte",
  ind_ihg_mx_qroav: "avid hotels Queretaro Centro Sur",
  ind_ihg_mx_gdlav: "avid hotels Guadalajara Av Vallarta Pte",
});

const CENSUS_TABLE_ID = TABLE_IDS["Hotel Property Census"];
const EXPECTED_RECORD_COUNT = 666;
const EXPECTED_APPLY_COUNT = 5;
const EXPECTED_ROOMS_FILLED = 5;

const DEFAULT_RUN =
  "reports/research-engine-v2/autopilot/2026-08-05_20-40-47-CALA-active-brands";

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function isBlank(v) {
  return v == null || v === "" || (typeof v === "string" && !v.trim());
}
function mask(id) {
  if (!id || id.length < 10) return id ? "***" : null;
  return `${id.slice(0, 6)}…${id.slice(-4)}`;
}

export function parseNameCleanupApplyArgs(argv = process.argv.slice(2)) {
  const flags = new Set(argv.filter((a) => a.startsWith("--")));
  const get = (name) => {
    const i = argv.indexOf(name);
    return i >= 0 ? argv[i + 1] : null;
  };
  const confirms = {
    approvalBundleBound: flags.has("--confirm-approval-bundle-bound"),
    propertyNameOnly: flags.has("--confirm-property-name-only"),
    fiveRecordsOnly: flags.has("--confirm-five-records-only"),
    noReplan: flags.has("--confirm-no-replan"),
    noBrandExplorer: flags.has("--confirm-no-brand-explorer-writes"),
    noBrandSetup: flags.has("--confirm-no-brand-setup-writes"),
    noOwnerOperator:
      flags.has("--confirm-no-owner-operator") || flags.has("--confirm-no-owner-operator-writes"),
    noDateWrites: flags.has("--confirm-no-date-writes"),
    noRooms: flags.has("--confirm-no-rooms-writes"),
    noGeocode: flags.has("--confirm-no-geocode-writes"),
    noDescriptions: flags.has("--confirm-no-description-writes"),
    writeToProduction: flags.has("--confirm-write-to-production-census"),
    safeWrites: flags.has("--confirm-safe-writes"),
  };
  return {
    dryRun: flags.has("--dry-run") || !flags.has("--apply"),
    apply: flags.has("--apply"),
    runDir: get("--run-dir"),
    approvalBundlePath: get("--approval-bundle"),
    confirms,
    allConfirmsOk: Object.values(confirms).every(Boolean),
  };
}

export function checkNameCleanupApplyEnv(env = process.env) {
  const flags = {
    ALLOW_CENSUS_AUTOPILOT_APPLY: String(env.ALLOW_CENSUS_AUTOPILOT_APPLY || "").trim() === "1",
    CONFIRM_WRITE_TO_PRODUCTION_CENSUS:
      String(env.CONFIRM_WRITE_TO_PRODUCTION_CENSUS || "").trim() === "1",
    CONFIRM_NO_BRAND_EXPLORER_WRITES:
      String(env.CONFIRM_NO_BRAND_EXPLORER_WRITES || "").trim() === "1",
    CONFIRM_NO_OWNER_OPERATOR_WRITES:
      String(env.CONFIRM_NO_OWNER_OPERATOR_WRITES || "").trim() === "1",
  };
  return { allOk: Object.values(flags).every(Boolean), flags };
}

export function loadNameCleanupFrozenProposals(opts = {}) {
  const runDir = resolve(opts.runDir || join(ROOT, DEFAULT_RUN));
  const bundlePath = resolve(
    opts.approvalBundlePath || join(runDir, "approval-bundle.json")
  );
  const dryPath = join(runDir, "dry-run.json");
  if (!existsSync(bundlePath)) return { ok: false, error: `approval_bundle_missing:${bundlePath}` };
  if (!existsSync(dryPath)) return { ok: false, error: `dry_run_missing:${dryPath}` };

  const bundle = JSON.parse(readFileSync(bundlePath, "utf8"));
  const dry = JSON.parse(readFileSync(dryPath, "utf8"));
  const proposals = (dry.proposals || []).filter(
    (p) =>
      p.queue === "property_name_cleanup" &&
      p.action === "propose_high_write" &&
      p.confidence === "High" &&
      p.write_allowed_now
  );

  if (proposals.length !== EXPECTED_APPLY_COUNT) {
    return {
      ok: false,
      error: `expected_${EXPECTED_APPLY_COUNT}_high_proposals_got_${proposals.length}`,
      runDir,
    };
  }

  const frozen = [];
  const contractErrors = [];
  for (const p of proposals) {
    const expectedName = FOUNDER_APPROVED_NAME_MAP[p.identity_key];
    if (!expectedName) {
      contractErrors.push({ identity_key: p.identity_key, reason: "not_in_founder_approved_map" });
      continue;
    }
    if (String(p.proposed_property_name) !== expectedName) {
      contractErrors.push({
        identity_key: p.identity_key,
        reason: "proposed_name_mismatch_founder_map",
        dry_run: p.proposed_property_name,
        founder: expectedName,
      });
      continue;
    }
    if (!/json_ld|og_title|h1|page_data/i.test(String(p.method || ""))) {
      contractErrors.push({
        identity_key: p.identity_key,
        reason: "method_not_official_name_source",
        method: p.method,
      });
      continue;
    }
    /** @type {Record<string, unknown>} */
    const patch = {};
    for (const k of APPROVED_NAME_PATCH_FIELDS) {
      if (k in (p.patch || {})) patch[k] = p.patch[k];
    }
    patch["Property Name"] = expectedName;
    for (const k of Object.keys(patch)) {
      if (!APPROVED_NAME_PATCH_FIELDS.includes(k)) {
        contractErrors.push({ identity_key: p.identity_key, reason: `unapproved_field:${k}` });
      }
      if (AUTOPILOT_FORBIDDEN_FIELDS.includes(k)) {
        contractErrors.push({ identity_key: p.identity_key, reason: `forbidden_field:${k}` });
      }
    }
    frozen.push({
      record_id: p.record_id,
      identity_key: p.identity_key,
      current_property_name: p.current_property_name,
      proposed_property_name: expectedName,
      confidence: p.confidence,
      method: p.method,
      source_url: p.source_url,
      reason: p.reason,
      name_problems: p.name_problems,
      patch,
      patch_fields: Object.keys(patch),
      queue: "property_name_cleanup",
    });
  }

  if (contractErrors.length || frozen.length !== EXPECTED_APPLY_COUNT) {
    return { ok: false, error: "founder_contract_violation", contractErrors, runDir };
  }

  return { ok: true, runDir, bundlePath, dryPath, bundle, dry, frozen };
}

async function airtableGet(baseId, token, tableId, recordId) {
  const res = await fetch(
    `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}/${encodeURIComponent(recordId)}`,
    { headers: { Authorization: `Bearer ${token}` } }
  );
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(`get ${recordId} ${res.status}: ${JSON.stringify(json.error || json)}`);
  return json;
}

async function airtablePatch(baseId, token, tableId, recordId, fields) {
  const res = await fetch(
    `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}/${encodeURIComponent(recordId)}`,
    {
      method: "PATCH",
      headers: {
        Authorization: `Bearer ${token}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ fields, typecast: false }),
    }
  );
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(`patch ${recordId} ${res.status}: ${JSON.stringify(json.error || json)}`);
  return json;
}

async function listAllRecords(baseId, token, tableId, fields = []) {
  const out = [];
  let offset;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    for (const f of fields) params.append("fields[]", f);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}?${params}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(`list ${res.status}: ${JSON.stringify(json.error || json)}`);
    out.push(...(json.records || []));
    offset = json.offset;
    await sleep(120);
  } while (offset);
  return out;
}

export function preflightNameCleanupProposal(frozen, liveRecord) {
  const errors = [];
  const fields = liveRecord?.fields || {};
  if (!liveRecord?.id) errors.push("record_not_found");
  if (frozen.confidence !== "High") errors.push("confidence_not_high");

  const current = String(fields[MAP_FIRST_PASS.propertyName] || "");
  const problems = classifyPropertyNameProblems(current);
  if (!problems.malformed) errors.push("current_name_not_malformed");

  if (fields[MAP_FIRST_PASS.humanReview] === true) errors.push("human_review_required");
  if (String(fields[MAP_FIRST_PASS.affiliationStatus] || "") === "Brand-Unconfirmed") {
    errors.push("brand_unconfirmed");
  }

  if (String(frozen.patch["Property Name"]) !== frozen.proposed_property_name) {
    errors.push("patch_name_mismatch");
  }

  for (const k of Object.keys(frozen.patch)) {
    if (!APPROVED_NAME_PATCH_FIELDS.includes(k)) errors.push(`unapproved_field:${k}`);
    if (AUTOPILOT_FORBIDDEN_FIELDS.includes(k)) errors.push(`forbidden_field:${k}`);
  }

  // Property Name cleanup is an intentional overwrite of malformed values (not blank-only).
  /** @type {Record<string, unknown>} */
  const fieldsToWrite = {};
  if (problems.malformed && current !== String(frozen.proposed_property_name)) {
    fieldsToWrite["Property Name"] = frozen.proposed_property_name;
  } else if (problems.malformed && current === String(frozen.proposed_property_name)) {
    // already cleaned — nothing to write for name
  } else if (!problems.malformed) {
    // already flagged above
  }

  for (const k of ["Last Reviewed Date", "Enrichment Status", "Enrichment Priority"]) {
    if (!(k in frozen.patch)) continue;
    const cmp = compareFieldValues(fields[k], frozen.patch[k]);
    if (cmp === "write") fieldsToWrite[k] = frozen.patch[k];
    // match → skip; conflict on enrichment → skip (do not block name overwrite)
  }

  if (!Object.keys(fieldsToWrite).length && problems.malformed) {
    errors.push("no_fields_to_write");
  }

  return {
    ok: errors.length === 0 && Boolean(fieldsToWrite["Property Name"]),
    errors,
    idempotent: {
      action: fieldsToWrite["Property Name"] ? "write" : "skip",
      fields: fieldsToWrite,
      reason: "malformed_property_name_overwrite",
    },
    live_snapshot: {
      property_name: fields["Property Name"] ?? null,
      rooms_keys: fields["Rooms / Keys"] ?? null,
      latitude: fields.Latitude ?? null,
      longitude: fields.Longitude ?? null,
      description: fields["Hotel Description - Source Text"] ?? null,
      amenities: fields["Amenities - Source Text"] ?? null,
      property_type: fields["Property Type"] ?? null,
      owner: fields["Owner Name"] ?? null,
      operator: fields["Operator / Management Company"] ?? null,
      name_problems: problems,
    },
  };
}

export async function runPropertyNameCleanupApprovalBundleApply(
  argv = process.argv.slice(2),
  env = process.env
) {
  const args = parseNameCleanupApplyArgs(argv);
  const envCheck = checkNameCleanupApplyEnv(env);
  const started = Date.now();

  const loaded = loadNameCleanupFrozenProposals({
    runDir: args.runDir,
    approvalBundlePath: args.approvalBundlePath,
  });
  if (!loaded.ok) {
    return {
      version: NAME_CLEANUP_APPLY_VERSION,
      generated_at: new Date().toISOString(),
      status: STATUS.BLOCKED,
      apply_executed: false,
      blocked_reason: loaded.error,
      contractErrors: loaded.contractErrors || null,
    };
  }

  if (args.apply && (!args.allConfirmsOk || !envCheck.allOk)) {
    return {
      version: NAME_CLEANUP_APPLY_VERSION,
      generated_at: new Date().toISOString(),
      status: STATUS.BLOCKED,
      apply_executed: false,
      blocked_reason: "confirmation_or_env_missing",
      confirms: args.confirms,
      env_flags: envCheck.flags,
    };
  }

  const token = resolvePat();
  const bases = resolveTargetBase();
  if (!token || !bases?.target_base_id) {
    return {
      version: NAME_CLEANUP_APPLY_VERSION,
      generated_at: new Date().toISOString(),
      status: STATUS.BLOCKED,
      apply_executed: false,
      blocked_reason: "missing_airtable_credentials",
    };
  }

  const writeTargetCheck = assertProductionCensusWriteTarget({
    baseName: productionHotelPropertyCensus.baseName,
    baseId: bases.target_base_id,
    tableName: productionHotelPropertyCensus.tableName,
    tableId: CENSUS_TABLE_ID,
  });
  if (!writeTargetCheck.ok || CENSUS_TABLE_ID !== productionHotelPropertyCensus.tableId) {
    return {
      version: NAME_CLEANUP_APPLY_VERSION,
      generated_at: new Date().toISOString(),
      status: STATUS.BLOCKED,
      apply_executed: false,
      blocked_reason: BLOCKED_WRONG_CENSUS_TARGET,
      write_target: writeTargetCheck,
    };
  }

  const censusBefore = await listAllRecords(bases.target_base_id, token, CENSUS_TABLE_ID, [
    "Property Identity Key",
    "Property Name",
    "Rooms / Keys",
    "Latitude",
    "Longitude",
  ]);
  if (censusBefore.length !== EXPECTED_RECORD_COUNT) {
    return {
      version: NAME_CLEANUP_APPLY_VERSION,
      generated_at: new Date().toISOString(),
      status: STATUS.BLOCKED,
      apply_executed: false,
      blocked_reason: `unexpected_census_count_${censusBefore.length}`,
    };
  }

  const roomsFilledBefore = censusBefore.filter((r) => !isBlank(r.fields?.["Rooms / Keys"])).length;
  const namesSnapshotBefore = Object.fromEntries(
    censusBefore.map((r) => [
      r.id,
      String(r.fields?.["Property Name"] ?? ""),
    ])
  );

  const preflightRows = [];
  for (const frozen of loaded.frozen) {
    const live = await airtableGet(bases.target_base_id, token, CENSUS_TABLE_ID, frozen.record_id);
    await sleep(150);
    const pf = preflightNameCleanupProposal(frozen, live);
    preflightRows.push({
      record_id: frozen.record_id,
      identity_key: frozen.identity_key,
      proposed_property_name: frozen.proposed_property_name,
      method: frozen.method,
      source_url: frozen.source_url,
      ok: pf.ok,
      errors: pf.errors,
      fields_to_write: pf.idempotent.fields,
      live_snapshot: pf.live_snapshot,
    });
  }

  if (!preflightRows.every((r) => r.ok)) {
    return {
      version: NAME_CLEANUP_APPLY_VERSION,
      generated_at: new Date().toISOString(),
      mode: args.apply ? "apply_blocked_preflight" : "dry-run",
      status: STATUS.BLOCKED,
      apply_executed: false,
      run_dir: loaded.runDir,
      preflight: preflightRows,
      rooms_filled_before: roomsFilledBefore,
      brand_explorer_writes: false,
      brand_setup_writes: false,
    };
  }

  if (!args.apply) {
    return {
      version: NAME_CLEANUP_APPLY_VERSION,
      generated_at: new Date().toISOString(),
      mode: "dry-run",
      status: STATUS.CLEAN,
      apply_executed: false,
      dry_run_pass: true,
      run_dir: loaded.runDir,
      frozen_count: loaded.frozen.length,
      would_update: preflightRows.map((r) => ({
        record_id: r.record_id,
        identity_key: r.identity_key,
        proposed_property_name: r.proposed_property_name,
        fields: Object.keys(r.fields_to_write || {}),
      })),
      preflight: preflightRows,
      rooms_filled_before: roomsFilledBefore,
      brand_explorer_writes: false,
      brand_setup_writes: false,
    };
  }

  const writeResults = [];
  for (const row of preflightRows) {
    try {
      await airtablePatch(
        bases.target_base_id,
        token,
        CENSUS_TABLE_ID,
        row.record_id,
        row.fields_to_write
      );
      writeResults.push({
        record_id: row.record_id,
        identity_key: row.identity_key,
        ok: true,
        fields_written: Object.keys(row.fields_to_write || {}),
        proposed_property_name: row.proposed_property_name,
      });
      await sleep(200);
    } catch (err) {
      writeResults.push({
        record_id: row.record_id,
        identity_key: row.identity_key,
        ok: false,
        error: err?.message || String(err),
      });
    }
  }

  const writesOk = writeResults.filter((w) => w.ok).length;
  const writesFail = writeResults.filter((w) => !w.ok).length;

  const postVerify = [];
  for (const frozen of loaded.frozen) {
    const live = await airtableGet(bases.target_base_id, token, CENSUS_TABLE_ID, frozen.record_id);
    await sleep(120);
    const f = live.fields || {};
    const pre = preflightRows.find((r) => r.record_id === frozen.record_id);
    postVerify.push({
      record_id: frozen.record_id,
      identity_key: frozen.identity_key,
      property_name: f["Property Name"] ?? null,
      name_match_expected: String(f["Property Name"]) === frozen.proposed_property_name,
      rooms_unchanged:
        String(f["Rooms / Keys"] ?? "") === String(pre?.live_snapshot?.rooms_keys ?? ""),
      coords_unchanged:
        String(f.Latitude ?? "") === String(pre?.live_snapshot?.latitude ?? "") &&
        String(f.Longitude ?? "") === String(pre?.live_snapshot?.longitude ?? ""),
      description_unchanged:
        String(f["Hotel Description - Source Text"] ?? "") ===
        String(pre?.live_snapshot?.description ?? ""),
      amenities_unchanged:
        String(f["Amenities - Source Text"] ?? "") === String(pre?.live_snapshot?.amenities ?? ""),
      property_type_unchanged:
        String(f["Property Type"] ?? "") === String(pre?.live_snapshot?.property_type ?? ""),
      owner_still_blank: isBlank(f["Owner Name"]),
      operator_still_blank: isBlank(f["Operator / Management Company"]),
    });
  }

  const censusAfter = await listAllRecords(bases.target_base_id, token, CENSUS_TABLE_ID, [
    "Property Identity Key",
    "Property Name",
    "Rooms / Keys",
    "Latitude",
    "Longitude",
  ]);
  const roomsFilledAfter = censusAfter.filter((r) => !isBlank(r.fields?.["Rooms / Keys"])).length;
  const approvedIds = new Set(loaded.frozen.map((f) => f.record_id));
  let otherNameChanges = 0;
  for (const r of censusAfter) {
    if (approvedIds.has(r.id)) continue;
    const before = namesSnapshotBefore[r.id] ?? "";
    const after = String(r.fields?.["Property Name"] ?? "");
    if (before !== after) otherNameChanges += 1;
  }

  const fiveUpdated =
    writesOk === EXPECTED_APPLY_COUNT &&
    postVerify.every(
      (v) =>
        v.name_match_expected &&
        v.rooms_unchanged &&
        v.coords_unchanged &&
        v.description_unchanged &&
        v.owner_still_blank &&
        v.operator_still_blank
    );
  const roomsOk = roomsFilledAfter === EXPECTED_ROOMS_FILLED && roomsFilledAfter === roomsFilledBefore;

  let status = STATUS.PARTIAL;
  if (writesFail > 0 && writesOk === 0) status = STATUS.BLOCKED;
  else if (fiveUpdated && roomsOk && otherNameChanges === 0 && writesFail === 0) {
    status = STATUS.CLEAN;
  }

  return {
    version: NAME_CLEANUP_APPLY_VERSION,
    generated_at: new Date().toISOString(),
    mode: "apply",
    apply_executed: true,
    status,
    duration_ms: Date.now() - started,
    run_dir: loaded.runDir,
    approval_bundle: loaded.bundlePath,
    base_id_masked: mask(bases.target_base_id),
    table_id: CENSUS_TABLE_ID,
    frozen_count: loaded.frozen.length,
    records_updated: writesOk,
    records_failed: writesFail,
    write_results: writeResults,
    preflight: preflightRows.map((r) => ({
      record_id: r.record_id,
      identity_key: r.identity_key,
      ok: r.ok,
      fields_to_write: Object.keys(r.fields_to_write || {}),
    })),
    post_verify: postVerify,
    census_validation: {
      record_count_before: censusBefore.length,
      record_count_after: censusAfter.length,
      rooms_filled_before: roomsFilledBefore,
      rooms_filled_after: roomsFilledAfter,
      rooms_remain_at_5: roomsOk,
      other_property_name_changes: otherNameChanges,
    },
    brand_explorer_writes: false,
    brand_setup_writes: false,
    fields_written_union: [...new Set(writeResults.flatMap((w) => w.fields_written || []))],
    next_step:
      status === STATUS.CLEAN
        ? "Property Name cleanup apply clean. Continue Autopilot controlled queues; geocode still blocked."
        : "Review post_verify before further apply.",
  };
}

export function renderNameCleanupApplyMarkdown(r) {
  return `# Property Name Cleanup Apply (Approval-Bundle-Bound)

**Status:** \`${r.status}\`  
**Generated:** ${r.generated_at}  
**Apply executed:** ${r.apply_executed}

## Summary

- Records updated: **${r.records_updated ?? 0}**
- Records failed: **${r.records_failed ?? 0}**
- Rooms filled: **${r.census_validation?.rooms_filled_before} → ${r.census_validation?.rooms_filled_after}**
- Other Property Name changes: **${r.census_validation?.other_property_name_changes ?? "—"}**
- Brand Explorer / Brand Setup writes: **false**

## Write results

\`\`\`json
${JSON.stringify(r.write_results || r.would_update || [], null, 2)}
\`\`\`

## Post-verify

\`\`\`json
${JSON.stringify(r.post_verify || [], null, 2)}
\`\`\`

## Next

${r.next_step || ""}
`;
}

export function writeJson(path, obj) {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, JSON.stringify(obj, null, 2), "utf8");
}
export function writeMd(path, text) {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, text, "utf8");
}
