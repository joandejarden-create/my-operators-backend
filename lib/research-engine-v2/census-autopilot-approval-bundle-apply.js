/**
 * Approval-bundle-bound Autopilot apply — first live Rooms / Keys write.
 * Applies ONLY frozen High proposals from a controlled-run dry-run.json.
 * Never re-plans. Never writes Brand Explorer / Brand Setup / owner / geocode / descriptions.
 */

import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import {
  resolvePat,
  resolveTargetBase,
} from "./production-census-schema-create.js";
import { TABLE_IDS } from "./production-census-write.js";
import { AUTOPILOT_FORBIDDEN_FIELDS, AUTOPILOT_ALLOWED_WRITE_FIELDS, sanitizeAutopilotPatch } from "./census-autopilot-field-allowlist.js";
import { buildIdempotentPatch, compareFieldValues } from "./census-autopilot-idempotent-writer.js";
import { MAP_ROOMS } from "./production-census-rooms-keys-queue.js";
import {
  assertProductionCensusWriteTarget,
  BLOCKED_WRONG_CENSUS_TARGET,
  productionHotelPropertyCensus,
} from "./production-census-source-of-truth.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "../..");

export const FIRST_ROOMS_APPLY_VERSION = "census-autopilot-first-rooms-approval-bundle-apply-v1";

export const STATUS = Object.freeze({
  CLEAN: "production_census_autopilot_first_rooms_apply_clean",
  PARTIAL: "production_census_autopilot_first_rooms_apply_partial_needs_review",
  BLOCKED: "production_census_autopilot_first_rooms_apply_blocked",
});

/** Strict field set for this founder-approved apply. */
export const APPROVED_ROOMS_PATCH_FIELDS = Object.freeze([
  "Rooms / Keys",
  "Rooms Confidence",
  "Rooms Source URL",
  "Rooms Source Type",
  "Rooms Reviewed Date",
  "Rooms Notes",
  "Enrichment Status",
  "Enrichment Priority",
  "Last Reviewed Date",
]);

export const REQUIRED_ROOMS_PATCH_FIELDS = Object.freeze([
  "Rooms / Keys",
  "Rooms Confidence",
  "Rooms Source URL",
  "Rooms Source Type",
  "Rooms Reviewed Date",
  "Enrichment Status",
  "Enrichment Priority",
  "Last Reviewed Date",
]);

const CENSUS_TABLE_ID = TABLE_IDS["Hotel Property Census"];
const EXPECTED_RECORD_COUNT = 666;
const EXPECTED_APPLY_COUNT = 5;

const READ_FIELDS = [
  "Property Identity Key",
  "Property Name",
  "Current Brand",
  "Rooms / Keys",
  "Rooms Confidence",
  "Rooms Source URL",
  "Rooms Source Type",
  "Rooms Reviewed Date",
  "Rooms Notes",
  "Enrichment Status",
  "Enrichment Priority",
  "Last Reviewed Date",
  "Owner Name",
  "Operator / Management Company",
  "Developer Name",
  "Opening Date",
  "Renovation / Conversion Date",
  "Latitude",
  "Longitude",
  "Mixed-Use Flag",
  "Branded Residences Flag",
  "Hotel Description - Source Text",
  "Amenities - Source Text",
  "Property Type",
];

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
function isSafeHttpUrl(url) {
  try {
    const u = new URL(String(url || ""));
    return u.protocol === "http:" || u.protocol === "https:";
  } catch {
    return false;
  }
}

export function parseFirstRoomsApplyArgs(argv = process.argv.slice(2)) {
  const flags = new Set(argv.filter((a) => a.startsWith("--")));
  const get = (name) => {
    const i = argv.indexOf(name);
    return i >= 0 ? argv[i + 1] : null;
  };
  const confirms = {
    approvalBundleBound: flags.has("--confirm-approval-bundle-bound"),
    roomsKeysOnly: flags.has("--confirm-rooms-keys-only"),
    fiveRecordsOnly: flags.has("--confirm-five-records-only"),
    noReplan: flags.has("--confirm-no-replan"),
    noBrandExplorer: flags.has("--confirm-no-brand-explorer-writes"),
    noBrandSetup: flags.has("--confirm-no-brand-setup-writes"),
    noOwnerOperator: flags.has("--confirm-no-owner-operator") || flags.has("--confirm-no-owner-operator-writes"),
    noDateWrites: flags.has("--confirm-no-date-writes"),
    noGeocode: flags.has("--confirm-no-geocode-writes"),
    noDescriptions: flags.has("--confirm-no-description-writes"),
    writeToProduction: flags.has("--confirm-write-to-production-census"),
    safeWrites: flags.has("--confirm-safe-writes"),
  };
  const allOk = Object.values(confirms).every(Boolean);
  return {
    dryRun: flags.has("--dry-run") || !flags.has("--apply"),
    apply: flags.has("--apply"),
    approvalBundlePath: get("--approval-bundle"),
    runDir: get("--run-dir"),
    confirms,
    allConfirmsOk: allOk,
  };
}

export function checkFirstRoomsApplyEnv(env = process.env) {
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

/**
 * Resolve run dir + load frozen proposals from dry-run.json (approval-bundle companion).
 */
export function loadApprovalBundleProposals(opts = {}) {
  const bundlePath = resolve(
    opts.approvalBundlePath ||
      join(
        ROOT,
        "reports/research-engine-v2/autopilot/2026-08-05_20-24-38-CALA-active-brands/approval-bundle.json"
      )
  );
  const runDir = opts.runDir
    ? resolve(opts.runDir)
    : dirname(bundlePath);
  const dryPath = join(runDir, "dry-run.json");
  if (!existsSync(bundlePath)) {
    return { ok: false, error: `approval_bundle_missing:${bundlePath}` };
  }
  if (!existsSync(dryPath)) {
    return { ok: false, error: `dry_run_missing:${dryPath}` };
  }

  const bundle = JSON.parse(readFileSync(bundlePath, "utf8"));
  const dry = JSON.parse(readFileSync(dryPath, "utf8"));
  const proposals = (dry.proposals || []).filter(
    (p) => p.action === "propose_high_write" && p.confidence === "High" && p.write_allowed_now
  );

  if (proposals.length !== EXPECTED_APPLY_COUNT) {
    return {
      ok: false,
      error: `expected_${EXPECTED_APPLY_COUNT}_high_proposals_got_${proposals.length}`,
      runDir,
      bundlePath,
      dryPath,
    };
  }

  const frozen = proposals.map((p) => freezeRoomsProposal(p));
  const fieldViolations = [];
  for (const f of frozen) {
    for (const k of Object.keys(f.patch)) {
      if (!APPROVED_ROOMS_PATCH_FIELDS.includes(k)) {
        fieldViolations.push({ record_id: f.record_id, field: k });
      }
    }
    for (const req of REQUIRED_ROOMS_PATCH_FIELDS) {
      if (!(req in f.patch)) {
        fieldViolations.push({ record_id: f.record_id, field: req, reason: "missing_required" });
      }
    }
  }
  if (fieldViolations.length) {
    return { ok: false, error: "patch_field_contract_violation", fieldViolations, runDir };
  }

  return {
    ok: true,
    runDir,
    bundlePath,
    dryPath,
    bundle,
    dry,
    frozen,
  };
}

export function freezeRoomsProposal(p) {
  const src = p.patch || {};
  /** @type {Record<string, unknown>} */
  const patch = {};
  for (const k of APPROVED_ROOMS_PATCH_FIELDS) {
    if (k === "Rooms Notes") {
      if (!isBlank(src[k])) patch[k] = src[k];
      continue;
    }
    if (k in src) patch[k] = src[k];
  }
  return {
    record_id: p.record_id,
    identity_key: p.identity_key,
    property_name: p.property_name,
    brand: p.brand,
    family: p.family,
    confidence: p.confidence,
    method: p.method,
    source_url: p.source_url || src["Rooms Source URL"],
    source_type: p.source_type || src["Rooms Source Type"],
    proposed_rooms_keys: p.proposed_rooms_keys ?? src["Rooms / Keys"],
    mixed_use_risk: Boolean(p.mixed_use_risk),
    notes: p.notes || null,
    patch,
    patch_fields: Object.keys(patch),
    queue: "rooms_keys",
    approval_source: "dry-run.json:propose_high_write",
  };
}

/**
 * Load multi-queue approval bundle for apply mode.
 * Prefers approval-bundle.json proposed_writes; falls back to dry-run.json High proposals.
 * Does not require a fixed proposal count (unlike first-rooms apply).
 */
export function loadMultiQueueApprovalBundleProposals(opts = {}) {
  const bundlePath = resolve(
    opts.approvalBundlePath ||
      join(ROOT, "reports/research-engine-v2/autopilot/latest/approval-bundle.json")
  );
  const runDir = opts.runDir ? resolve(opts.runDir) : dirname(bundlePath);
  const dryPath = join(runDir, "dry-run.json");

  if (!existsSync(bundlePath)) {
    return { ok: false, error: `approval_bundle_missing:${bundlePath}` };
  }

  const bundle = JSON.parse(readFileSync(bundlePath, "utf8"));
  let raw = [];
  if (Array.isArray(bundle.proposed_writes) && bundle.proposed_writes.length) {
    raw = bundle.proposed_writes;
  } else if (existsSync(dryPath)) {
    const dry = JSON.parse(readFileSync(dryPath, "utf8"));
    raw = (dry.proposals || []).filter(
      (p) =>
        (p.action === "propose_high_write" || p.write_allowed_now) &&
        p.confidence === "High"
    );
  } else {
    return { ok: false, error: `no_proposed_writes_in_bundle_and_dry_run_missing:${dryPath}` };
  }

  const frozen = [];
  const fieldViolations = [];
  for (const p of raw) {
    const { fields: patch, dropped } = sanitizeAutopilotPatch(p.patch || {}, {
      schemaV114Ready: opts.schemaV114Ready !== false,
      allowGeocode: Boolean(opts.allowGeocode),
    });
    for (const d of dropped) {
      fieldViolations.push({ record_id: p.record_id, field: d.field, reason: d.reason });
    }
    for (const k of Object.keys(p.patch || {})) {
      if (AUTOPILOT_FORBIDDEN_FIELDS.includes(k)) {
        fieldViolations.push({ record_id: p.record_id, field: k, reason: "forbidden" });
      }
    }
    if (!Object.keys(patch).length) continue;
    frozen.push({
      record_id: p.record_id,
      identity_key: p.identity_key,
      property_name: p.property_name,
      queue: p.queue || "unknown",
      confidence: "High",
      patch,
      patch_fields: Object.keys(patch),
      approval_source: bundle.proposed_writes?.length
        ? "approval-bundle.json:proposed_writes"
        : "dry-run.json:propose_high_write",
    });
  }

  if (fieldViolations.some((v) => v.reason === "forbidden")) {
    return { ok: false, error: "forbidden_fields_in_bundle", fieldViolations, runDir, bundlePath };
  }

  return {
    ok: true,
    multi_queue: true,
    runDir,
    bundlePath,
    dryPath: existsSync(dryPath) ? dryPath : null,
    bundle,
    frozen,
    queues: [...new Set(frozen.map((f) => f.queue))],
    records_proposed: frozen.length,
    field_violations_non_fatal: fieldViolations.filter((v) => v.reason !== "forbidden"),
    allowed_fields: AUTOPILOT_ALLOWED_WRITE_FIELDS,
  };
}

async function airtableGet(baseId, token, tableId, recordId, _fields = []) {
  // Fetch full record (fields[] on single-record GET can 422 with large/odd field sets).
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

function loadGeocodeIdentityKeys() {
  const path = join(ROOT, "reports/research-engine-v2/production-census-address-geocode-resolver-dry-run.json");
  if (!existsSync(path)) return [];
  const g = JSON.parse(readFileSync(path, "utf8"));
  return (g.proposed_updates || [])
    .map((u) => u.identity_key)
    .filter(Boolean);
}

/**
 * Preflight one frozen proposal against live Airtable record.
 */
export function preflightFrozenProposal(frozen, liveRecord) {
  const errors = [];
  const fields = liveRecord?.fields || {};

  if (!liveRecord?.id) errors.push("record_not_found");
  if (frozen.confidence !== "High") errors.push("confidence_not_high");
  if (Number(frozen.patch["Rooms / Keys"]) !== Number(frozen.proposed_rooms_keys)) {
    errors.push("rooms_count_mismatch_in_frozen_patch");
  }
  if (!isSafeHttpUrl(frozen.patch["Rooms Source URL"] || frozen.source_url)) {
    errors.push("missing_or_invalid_source_url");
  }
  if (frozen.patch["Rooms Confidence"] !== "High") errors.push("patch_confidence_not_high");
  if (frozen.mixed_use_risk) errors.push("mixed_use_risk_flagged");
  if (fields["Mixed-Use Flag"] === true || fields["Mixed-Use Flag"] === "true") {
    errors.push("live_mixed_use_flag");
  }
  if (fields["Branded Residences Flag"] === true || fields["Branded Residences Flag"] === "true") {
    errors.push("live_residences_flag");
  }

  for (const k of Object.keys(frozen.patch)) {
    if (AUTOPILOT_FORBIDDEN_FIELDS.includes(k)) errors.push(`forbidden_field:${k}`);
    if (!APPROVED_ROOMS_PATCH_FIELDS.includes(k)) errors.push(`unapproved_field:${k}`);
  }

  const idempotent = buildIdempotentPatch(fields, frozen.patch, {
    confidence: "High",
    allowGeocode: false,
    schemaV114Ready: true,
    threshold: "High",
  });
  if (idempotent.action === "conflict") errors.push("idempotent_conflict");
  if (idempotent.action === "no_write") errors.push(`idempotent_no_write:${idempotent.reason}`);
  if (idempotent.conflicts?.length) {
    for (const c of idempotent.conflicts) errors.push(`conflict:${c.field}`);
  }

  // Rooms / Keys must be blank or match
  const roomsCmp = compareFieldValues(fields["Rooms / Keys"], frozen.patch["Rooms / Keys"]);
  if (roomsCmp === "conflict") errors.push("rooms_keys_already_filled_different");

  return {
    ok: errors.length === 0,
    errors,
    idempotent,
    live_snapshot: {
      rooms_keys: fields["Rooms / Keys"] ?? null,
      rooms_confidence: fields["Rooms Confidence"] ?? null,
      rooms_source_url: fields["Rooms Source URL"] ?? null,
      latitude: fields.Latitude ?? null,
      longitude: fields.Longitude ?? null,
      owner: fields["Owner Name"] ?? null,
      operator: fields["Operator / Management Company"] ?? null,
      description: fields["Hotel Description - Source Text"] ?? null,
      amenities: fields["Amenities - Source Text"] ?? null,
      property_type: fields["Property Type"] ?? null,
    },
  };
}

/**
 * Run approval-bundle-bound first rooms apply.
 */
export async function runFirstRoomsApprovalBundleApply(argv = process.argv.slice(2), env = process.env) {
  const args = parseFirstRoomsApplyArgs(argv);
  const envCheck = checkFirstRoomsApplyEnv(env);
  const started = Date.now();

  const loaded = loadApprovalBundleProposals({
    approvalBundlePath: args.approvalBundlePath,
    runDir: args.runDir,
  });
  if (!loaded.ok) {
    return {
      version: FIRST_ROOMS_APPLY_VERSION,
      generated_at: new Date().toISOString(),
      status: STATUS.BLOCKED,
      apply_executed: false,
      blocked_reason: loaded.error,
      fieldViolations: loaded.fieldViolations || null,
    };
  }

  if (args.apply && (!args.allConfirmsOk || !envCheck.allOk)) {
    return {
      version: FIRST_ROOMS_APPLY_VERSION,
      generated_at: new Date().toISOString(),
      status: STATUS.BLOCKED,
      apply_executed: false,
      blocked_reason: "confirmation_or_env_missing",
      confirms: args.confirms,
      env_flags: envCheck.flags,
      frozen_count: loaded.frozen.length,
    };
  }

  const token = resolvePat();
  const bases = resolveTargetBase();
  if (!token || !bases?.target_base_id) {
    return {
      version: FIRST_ROOMS_APPLY_VERSION,
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
      version: FIRST_ROOMS_APPLY_VERSION,
      generated_at: new Date().toISOString(),
      status: STATUS.BLOCKED,
      apply_executed: false,
      blocked_reason: BLOCKED_WRONG_CENSUS_TARGET,
      write_target: writeTargetCheck,
    };
  }

  // Pre-census snapshot: rooms filled + coords for geocode keys
  const censusBefore = await listAllRecords(bases.target_base_id, token, CENSUS_TABLE_ID, [
    "Property Identity Key",
    "Rooms / Keys",
    "Latitude",
    "Longitude",
    "Owner Name",
    "Operator / Management Company",
  ]);
  if (censusBefore.length !== EXPECTED_RECORD_COUNT) {
    return {
      version: FIRST_ROOMS_APPLY_VERSION,
      generated_at: new Date().toISOString(),
      status: STATUS.BLOCKED,
      apply_executed: false,
      blocked_reason: `unexpected_census_count_${censusBefore.length}`,
    };
  }

  const roomsFilledBefore = censusBefore.filter((r) => !isBlank(r.fields?.["Rooms / Keys"])).length;
  const geocodeKeys = new Set(loadGeocodeIdentityKeys());
  const geocodeCoordsBefore = censusBefore
    .filter((r) => geocodeKeys.has(r.fields?.["Property Identity Key"]))
    .map((r) => ({
      identity_key: r.fields?.["Property Identity Key"],
      latitude: r.fields?.Latitude ?? null,
      longitude: r.fields?.Longitude ?? null,
      has_coords: !isBlank(r.fields?.Latitude) && !isBlank(r.fields?.Longitude),
    }));
  const geocodeStillBlankBefore = geocodeCoordsBefore.filter((g) => !g.has_coords).length;

  const preflightRows = [];
  for (const frozen of loaded.frozen) {
    const live = await airtableGet(
      bases.target_base_id,
      token,
      CENSUS_TABLE_ID,
      frozen.record_id,
      READ_FIELDS
    );
    await sleep(150);
    const pf = preflightFrozenProposal(frozen, live);
    preflightRows.push({
      record_id: frozen.record_id,
      identity_key: frozen.identity_key,
      proposed_rooms_keys: frozen.proposed_rooms_keys,
      source_url: frozen.source_url,
      ok: pf.ok,
      errors: pf.errors,
      idempotent_action: pf.idempotent.action,
      fields_to_write: pf.idempotent.fields,
      live_snapshot: pf.live_snapshot,
      frozen_patch: frozen.patch,
    });
  }

  const preflightPass = preflightRows.every((r) => r.ok);
  if (!preflightPass) {
    return {
      version: FIRST_ROOMS_APPLY_VERSION,
      generated_at: new Date().toISOString(),
      mode: args.apply ? "apply_blocked_preflight" : "dry-run",
      status: STATUS.BLOCKED,
      apply_executed: false,
      run_dir: loaded.runDir,
      frozen_count: loaded.frozen.length,
      preflight: preflightRows,
      rooms_filled_before: roomsFilledBefore,
      geocode_proposed_keys: geocodeKeys.size,
      geocode_still_blank_before: geocodeStillBlankBefore,
      brand_explorer_writes: false,
      brand_setup_writes: false,
    };
  }

  if (!args.apply) {
    return {
      version: FIRST_ROOMS_APPLY_VERSION,
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
        rooms: r.proposed_rooms_keys,
        fields: Object.keys(r.fields_to_write || {}),
      })),
      preflight: preflightRows,
      rooms_filled_before: roomsFilledBefore,
      expected_rooms_filled_after: roomsFilledBefore + EXPECTED_APPLY_COUNT,
      geocode_proposed_keys: geocodeKeys.size,
      geocode_still_blank_before: geocodeStillBlankBefore,
      brand_explorer_writes: false,
      brand_setup_writes: false,
      next_step:
        "Founder confirms → re-run with --apply + env/confirm flags (approval-bundle-bound).",
    };
  }

  // LIVE APPLY — only the 5 frozen patches
  const writeResults = [];
  for (const row of preflightRows) {
    try {
      const updated = await airtablePatch(
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
        rooms: row.proposed_rooms_keys,
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

  // Post-verify: re-read 5 + full census rooms/coords snapshot
  const postVerify = [];
  for (const frozen of loaded.frozen) {
    const live = await airtableGet(
      bases.target_base_id,
      token,
      CENSUS_TABLE_ID,
      frozen.record_id,
      READ_FIELDS
    );
    await sleep(120);
    const f = live.fields || {};
    const pre = preflightRows.find((r) => r.record_id === frozen.record_id);
    postVerify.push({
      record_id: frozen.record_id,
      identity_key: frozen.identity_key,
      rooms_keys: f["Rooms / Keys"] ?? null,
      rooms_confidence: f["Rooms Confidence"] ?? null,
      rooms_source_url: f["Rooms Source URL"] ?? null,
      rooms_source_type: f["Rooms Source Type"] ?? null,
      rooms_reviewed_date: f["Rooms Reviewed Date"] ?? null,
      rooms_notes: f["Rooms Notes"] ?? null,
      enrichment_status: f["Enrichment Status"] ?? null,
      rooms_match_expected: Number(f["Rooms / Keys"]) === Number(frozen.proposed_rooms_keys),
      confidence_high: f["Rooms Confidence"] === "High",
      source_ok: String(f["Rooms Source URL"] || "") === String(frozen.patch["Rooms Source URL"] || ""),
      coords_unchanged:
        String(f.Latitude ?? "") === String(pre?.live_snapshot?.latitude ?? "") &&
        String(f.Longitude ?? "") === String(pre?.live_snapshot?.longitude ?? ""),
      owner_still_blank: isBlank(f["Owner Name"]),
      operator_still_blank: isBlank(f["Operator / Management Company"]),
      description_unchanged:
        String(f["Hotel Description - Source Text"] ?? "") ===
        String(pre?.live_snapshot?.description ?? ""),
      amenities_unchanged:
        String(f["Amenities - Source Text"] ?? "") === String(pre?.live_snapshot?.amenities ?? ""),
      property_type_unchanged:
        String(f["Property Type"] ?? "") === String(pre?.live_snapshot?.property_type ?? ""),
    });
  }

  const censusAfter = await listAllRecords(bases.target_base_id, token, CENSUS_TABLE_ID, [
    "Property Identity Key",
    "Rooms / Keys",
    "Latitude",
    "Longitude",
  ]);
  const roomsFilledAfter = censusAfter.filter((r) => !isBlank(r.fields?.["Rooms / Keys"])).length;
  const geocodeCoordsAfter = censusAfter
    .filter((r) => geocodeKeys.has(r.fields?.["Property Identity Key"]))
    .map((r) => ({
      identity_key: r.fields?.["Property Identity Key"],
      has_coords: !isBlank(r.fields?.Latitude) && !isBlank(r.fields?.Longitude),
    }));
  const geocodeStillBlankAfter = geocodeCoordsAfter.filter((g) => !g.has_coords).length;

  const fiveUpdated =
    writesOk === EXPECTED_APPLY_COUNT &&
    postVerify.every(
      (v) =>
        v.rooms_match_expected &&
        v.confidence_high &&
        v.source_ok &&
        v.coords_unchanged &&
        v.owner_still_blank &&
        v.operator_still_blank
    );
  const roomsDeltaOk = roomsFilledAfter === roomsFilledBefore + EXPECTED_APPLY_COUNT;
  const geocodeStillBlocked =
    geocodeKeys.size === 34 && geocodeStillBlankAfter === geocodeStillBlankBefore;
  const noExtraCensusWrites = censusAfter.length === EXPECTED_RECORD_COUNT && roomsDeltaOk;

  let status = STATUS.PARTIAL;
  if (writesFail > 0 && writesOk === 0) status = STATUS.BLOCKED;
  else if (fiveUpdated && noExtraCensusWrites && geocodeStillBlocked && writesFail === 0) {
    status = STATUS.CLEAN;
  }

  return {
    version: FIRST_ROOMS_APPLY_VERSION,
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
      rooms_delta: roomsFilledAfter - roomsFilledBefore,
      expected_delta: EXPECTED_APPLY_COUNT,
      rooms_delta_ok: roomsDeltaOk,
    },
    geocode_gate: {
      proposed_keys: geocodeKeys.size,
      still_blank_before: geocodeStillBlankBefore,
      still_blank_after: geocodeStillBlankAfter,
      remain_blocked: geocodeStillBlocked,
      note: "34 geocode proposals remain unapplied (provider/storage not confirmed)",
    },
    brand_explorer_writes: false,
    brand_setup_writes: false,
    fields_written_union: [
      ...new Set(writeResults.flatMap((w) => w.fields_written || [])),
    ],
    rooms_notes_written: false,
    next_step:
      status === STATUS.CLEAN
        ? "First rooms apply clean. Continue Autopilot controlled on next queue; do not apply geocode until provider decision."
        : "Review post_verify / write_results before further Autopilot apply.",
  };
}

export function renderFirstRoomsApplyMarkdown(r) {
  return `# First Autopilot Rooms / Keys Apply (Approval-Bundle-Bound)

**Status:** \`${r.status}\`  
**Generated:** ${r.generated_at}  
**Mode:** ${r.mode}  
**Apply executed:** ${r.apply_executed}

## Summary

- Frozen proposals: **${r.frozen_count ?? "—"}**
- Records updated: **${r.records_updated ?? 0}**
- Records failed: **${r.records_failed ?? 0}**
- Rooms filled: **${r.census_validation?.rooms_filled_before ?? "—"} → ${r.census_validation?.rooms_filled_after ?? "—"}** (Δ ${r.census_validation?.rooms_delta ?? "—"})
- Geocode 34 still blocked: **${r.geocode_gate?.remain_blocked ?? "—"}** (${r.geocode_gate?.still_blank_after ?? "—"} blank)
- Brand Explorer writes: **false**
- Brand Setup writes: **false**

## Write results

\`\`\`json
${JSON.stringify(r.write_results || r.would_update || [], null, 2)}
\`\`\`

## Post-verify

\`\`\`json
${JSON.stringify(r.post_verify || r.preflight || [], null, 2)}
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
