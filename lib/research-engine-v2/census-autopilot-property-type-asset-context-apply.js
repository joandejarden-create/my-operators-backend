/**
 * Approval-bundle-bound Property Type / Asset Context apply.
 * Applies only frozen High property_type_asset_context updates.
 * Never inserts. Never re-plans. Never writes Address/geocode/rooms/names/BE/Brand Setup.
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
import { buildIdempotentPatch, compareFieldValues } from "./census-autopilot-idempotent-writer.js";
import {
  assertProductionCensusWriteTarget,
  BLOCKED_WRONG_CENSUS_TARGET,
  productionHotelPropertyCensus,
} from "./production-census-source-of-truth.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "../..");

export const PTAC_APPLY_VERSION =
  "census-autopilot-property-type-asset-context-approval-bundle-apply-v1";

export const STATUS = Object.freeze({
  CLEAN: "production_census_property_type_asset_context_apply_clean",
  PARTIAL: "production_census_property_type_asset_context_apply_partial_needs_review",
  BLOCKED: "production_census_property_type_asset_context_apply_blocked",
});

/** Allowed patch fields for this founder-approved apply. */
export const PTAC_LANE_FIELDS = Object.freeze([
  "Property Type",
  "Asset Context",
  "Market",
  "Submarket",
  "Market / Submarket",
  "Last Reviewed Date",
  "Enrichment Status",
  "Enrichment Priority",
]);

export const FORBIDDEN_IN_PATCH = Object.freeze([
  "Address",
  "Address Confidence",
  "Address Source URL",
  "Latitude",
  "Longitude",
  "Coordinate Source Type",
  "Coordinate Confidence",
  "Geocode Provider",
  "Geocode Method",
  "Geocode Reviewed Date",
  "Source URL",
  "Source Family",
  "Hotel Description - Source Text",
  "Hotel Description - AI Summary",
  "Amenities - Source Text",
  "Amenities - Structured Tags",
  "Rooms / Keys",
  "Rooms Confidence",
  "Rooms Source URL",
  "Rooms Source Type",
  "Rooms Reviewed Date",
  "Rooms Notes",
  "Property Name",
  "Brand",
  "Current Brand",
  "Owner Name",
  "Developer",
  "Developer Name",
  "Operator / Management Company",
  "Opening Date",
  "Renovation Date",
  "Renovation / Conversion Date",
  "Affiliation Start Date",
  "Recent Momentum",
  "Company Validated",
  "Company Validation Date",
  "Brand Verified",
  "Brand Status",
]);

const PROPERTY_TYPE_ALLOWED = new Set([
  "Hotel",
  "Resort",
  "Boutique Hotel",
  "Extended Stay",
  "All-Inclusive",
  "Serviced Apartment",
  "Mixed-Use",
  "Other",
  "Unknown",
]);

const ASSET_CONTEXT_ALLOWED = new Set([
  "Urban",
  "Airport",
  "Suburban",
  "Beach / Waterfront",
  "Resort Destination",
  "Highway / Transit",
  "Campus / Medical",
  "Other",
  "Unknown",
]);

const CENSUS_TABLE_ID = TABLE_IDS["Hotel Property Census"];
const EXPECTED_RECORD_COUNT = 741;
const EXPECTED_PROPOSAL_COUNT = 24;

const READ_FIELDS = [
  "Property Identity Key",
  "Property Name",
  "Current Brand",
  "City",
  "Country",
  "Address",
  "Property Type",
  "Asset Context",
  "Market",
  "Submarket",
  "Market / Submarket",
  "Amenities - Source Text",
  "Affiliation Status",
  "Human Review Required",
  "Enrichment Status",
  "Enrichment Priority",
  "Last Reviewed Date",
  "Latitude",
  "Longitude",
  "Coordinate Source Type",
  "Rooms / Keys",
  "Hotel Description - Source Text",
  "Owner Name",
  "Operator / Management Company",
  "Developer Name",
  "Opening Date",
  "Renovation / Conversion Date",
  "Mixed-Use Flag",
  "Branded Residences Flag",
  "Recent Momentum",
  "Company Validated",
  "Brand Verified",
  "Brand Status",
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
function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}
function norm(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/\p{M}/gu, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

export function writeJson(path, obj) {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, JSON.stringify(obj, null, 2), "utf8");
}
export function writeMd(path, text) {
  mkdirSync(dirname(path), { recursive: true });
  writeFileSync(path, text, "utf8");
}

export function parsePtacApplyArgs(argv = process.argv.slice(2)) {
  const flags = new Set(argv.filter((a) => a.startsWith("--")));
  const get = (name) => {
    const i = argv.indexOf(name);
    return i >= 0 && argv[i + 1] && !argv[i + 1].startsWith("--") ? argv[i + 1] : null;
  };
  const confirms = {
    safeWrites: flags.has("--confirm-safe-writes"),
    writeToProduction: flags.has("--confirm-write-to-production-census"),
    noBrandExplorer: flags.has("--confirm-no-brand-explorer-writes"),
    noOwnerOperator:
      flags.has("--confirm-no-owner-operator") || flags.has("--confirm-no-owner-operator-writes"),
    noDateWrites: flags.has("--confirm-no-date-writes"),
    noRecentMomentum: flags.has("--confirm-no-recent-momentum"),
    noCompanyValidation: flags.has("--confirm-no-company-validation"),
    webhoundNotProduction: flags.has("--confirm-webhound-not-production-source"),
    approvalBundleBound:
      flags.has("--confirm-approval-bundle-bound") || flags.has("--approval-bundle"),
  };
  return {
    mode: get("--mode") || "preflight",
    runDir: get("--run-dir"),
    approvalBundlePath: get("--approval-bundle"),
    batchSize: Number(get("--batch-size") || 50) || 50,
    queue: get("--queue"),
    confirms,
    allConfirmsOk: Object.values(confirms).every(Boolean),
    enableProductionWrites: flags.has("--enable-production-writes"),
  };
}

export function checkPtacApplyEnv(env = process.env) {
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

function supportHaystack(fields, frozen) {
  return norm(
    [
      fields["Property Name"] || frozen.property_name,
      fields["Current Brand"] || frozen.brand,
      fields.City,
      fields["Amenities - Source Text"],
    ].join(" ")
  );
}

function propertyTypeSupported(value, hay) {
  const v = String(value || "");
  if (v === "Extended Stay") {
    return /extended stay|homewood|staybridge|candlewood|suites by hilton/.test(hay);
  }
  if (v === "All-Inclusive") return /all inclusive|all-inclusive|emotions/.test(hay);
  if (v === "Boutique Hotel") {
    return /boutique|tapestry|curio|kimpton|autograph|design hotels/.test(hay);
  }
  if (v === "Resort") return /resort|leisure|spa|playa|all inclusive/.test(hay);
  if (v === "Hotel") return true;
  return false;
}

function assetContextSupported(value, hay) {
  const v = String(value || "");
  if (v === "Airport") return /airport|aeropuerto/.test(hay);
  if (v === "Beach / Waterfront") {
    return /beach|waterfront|riviera|cabo|vallarta|tulum|cozumel|playa/.test(hay);
  }
  if (v === "Resort Destination") {
    return /resort destination|all inclusive|all-inclusive|emotions|iberostar/.test(hay);
  }
  if (v === "Urban") return /urban|centro|downtown|polanco|santa fe/.test(hay);
  if (v === "Suburban") return /suburban|suburb/.test(hay);
  return false;
}

/**
 * Load only High property_type_asset_context update proposals from an approval bundle.
 */
export function loadPtacFrozenProposals(opts = {}) {
  const bundlePath = resolve(opts.approvalBundlePath || "");
  if (!bundlePath || !existsSync(bundlePath)) {
    return { ok: false, error: `approval_bundle_missing:${bundlePath}` };
  }
  const runDir = opts.runDir ? resolve(opts.runDir) : dirname(bundlePath);
  const dryPath = join(runDir, "dry-run.json");
  const bundle = JSON.parse(readFileSync(bundlePath, "utf8"));
  const byQueue = bundle.proposed_writes_by_queue || {};

  // Reject insert / discovery payloads
  if (
    Array.isArray(bundle.inserts) ||
    Array.isArray(bundle.insert_candidates) ||
    byQueue.source_discovery?.length
  ) {
    return { ok: false, error: "bundle_contains_source_discovery_or_inserts" };
  }
  if (byQueue.address_confirmation?.length) {
    return { ok: false, error: "bundle_contains_address_confirmation_proposals" };
  }

  const assetRaw = byQueue.property_type_asset_context || [];
  if (!assetRaw.length) {
    return { ok: false, error: "no_property_type_asset_context_proposals" };
  }

  const dry = existsSync(dryPath) ? JSON.parse(readFileSync(dryPath, "utf8")) : null;
  const dryById = new Map(
    (dry?.proposals || [])
      .filter((p) => p.queue === "property_type_asset_context")
      .map((p) => [p.record_id, p])
  );

  const frozen = [];
  const fieldViolations = [];

  for (const p of assetRaw) {
    if (p.confidence !== "High") {
      fieldViolations.push({
        record_id: p.record_id,
        field: "confidence",
        reason: "not_high",
      });
      continue;
    }
    if (p.action === "insert" || p.type === "insert") {
      return { ok: false, error: `insert_proposal_in_ptac_bundle:${p.record_id}` };
    }

    const dryP = dryById.get(p.record_id);
    const patch = { ...(p.patch || {}) };
    // Drop empty keys
    for (const k of Object.keys(patch)) {
      if (isBlank(patch[k])) delete patch[k];
    }
    for (const k of Object.keys(patch)) {
      if (FORBIDDEN_IN_PATCH.includes(k) || AUTOPILOT_FORBIDDEN_FIELDS.includes(k)) {
        fieldViolations.push({ record_id: p.record_id, field: k, reason: "forbidden" });
      }
      if (!PTAC_LANE_FIELDS.includes(k)) {
        fieldViolations.push({ record_id: p.record_id, field: k, reason: "outside_ptac_lane" });
      }
    }
    if (!patch["Property Type"] && !patch["Asset Context"]) {
      fieldViolations.push({
        record_id: p.record_id,
        field: "(none)",
        reason: "missing_property_type_and_asset_context",
      });
      continue;
    }
    if (!("Last Reviewed Date" in patch)) patch["Last Reviewed Date"] = todayIsoDate();

    frozen.push({
      record_id: p.record_id,
      identity_key: p.identity_key,
      property_name: p.property_name || dryP?.property_name,
      brand: dryP?.brand || null,
      family: dryP?.family || null,
      queue: "property_type_asset_context",
      lane: "property_type_asset_context",
      confidence: "High",
      method: dryP?.method || "lane2:property_type_asset_context",
      patch,
      patch_fields: Object.keys(patch),
      source_url: dryP?.source_url || null,
      approval_source: "approval-bundle.json:property_type_asset_context",
    });
  }

  if (fieldViolations.some((v) => v.reason === "forbidden")) {
    return { ok: false, error: "forbidden_fields_in_bundle", fieldViolations, runDir, bundlePath };
  }
  if (frozen.length !== EXPECTED_PROPOSAL_COUNT && !opts.allowCountMismatch) {
    // Soft warn — still proceed if filtered intentionally, but flag
  }

  return {
    ok: true,
    runDir,
    bundlePath,
    dryPath: existsSync(dryPath) ? dryPath : null,
    bundle,
    dry,
    frozen,
    proposal_count: frozen.length,
    field_violations_non_fatal: fieldViolations.filter((v) => v.reason !== "forbidden"),
  };
}

export function preflightPtacProposal(frozen, liveRecord) {
  const errors = [];
  const steward_reasons = [];
  const fields = liveRecord?.fields || {};

  if (!liveRecord?.id) errors.push("record_not_found");
  if (frozen.confidence !== "High") errors.push("confidence_not_high");
  if (fields["Human Review Required"] === true || fields["Human Review Required"] === "true") {
    errors.push("held_human_review_required");
  }
  if (String(fields["Affiliation Status"] || "") === "Brand-Unconfirmed") {
    errors.push("brand_unconfirmed");
  }

  for (const k of Object.keys(frozen.patch)) {
    if (FORBIDDEN_IN_PATCH.includes(k) || AUTOPILOT_FORBIDDEN_FIELDS.includes(k)) {
      errors.push(`forbidden_field:${k}`);
    }
    if (!PTAC_LANE_FIELDS.includes(k)) errors.push(`unapproved_field:${k}`);
  }

  const hay = supportHaystack(fields, frozen);
  const pt = frozen.patch["Property Type"];
  const ac = frozen.patch["Asset Context"];

  if (pt != null) {
    if (!PROPERTY_TYPE_ALLOWED.has(String(pt))) errors.push("property_type_not_in_allowed_options");
    if (!propertyTypeSupported(pt, hay)) {
      steward_reasons.push("property_type_weak_name_brand_amenity_support");
    }
  }
  if (ac != null) {
    if (!ASSET_CONTEXT_ALLOWED.has(String(ac))) errors.push("asset_context_not_in_allowed_options");
    if (!assetContextSupported(ac, hay)) {
      steward_reasons.push("asset_context_weak_name_city_support");
    }
  }

  if (fields["Mixed-Use Flag"] === true || fields["Branded Residences Flag"] === true) {
    errors.push("unsupported_mixed_use_or_residences_flag");
  }

  if (pt != null) {
    const cmp = compareFieldValues(fields["Property Type"], pt);
    if (cmp === "conflict") errors.push("property_type_already_filled_different");
  }
  if (ac != null) {
    const cmp = compareFieldValues(fields["Asset Context"], ac);
    if (cmp === "conflict") errors.push("asset_context_already_filled_different");
  }

  // Content fields only for idempotent compare. Governance dates may already be set
  // from insert/prior enrichment — bump Last Reviewed Date on successful content write.
  const contentPatch = { ...frozen.patch };
  const governanceKeys = ["Last Reviewed Date", "Enrichment Status", "Enrichment Priority"];
  for (const gk of governanceKeys) delete contentPatch[gk];

  const idempotent = buildIdempotentPatch(fields, contentPatch, {
    confidence: "High",
    allowGeocode: false,
    schemaV114Ready: true,
    threshold: "High",
  });
  if (idempotent.action === "conflict") errors.push("idempotent_conflict");
  if (idempotent.conflicts?.length) {
    for (const c of idempotent.conflicts) errors.push(`conflict:${c.field}`);
  }
  if (idempotent.action === "write" && Object.keys(idempotent.fields || {}).length) {
    idempotent.fields["Last Reviewed Date"] = frozen.patch["Last Reviewed Date"] || todayIsoDate();
  }

  const hardFail = errors.length > 0;
  const steward = !hardFail && steward_reasons.length > 0;
  const pass = !hardFail && !steward && (idempotent.action === "write" || idempotent.action === "skip");

  return {
    ok: pass,
    apply: pass && idempotent.action === "write",
    skip_matching: pass && idempotent.action === "skip",
    steward,
    blocked: hardFail,
    errors,
    steward_reasons,
    idempotent,
    live_snapshot: {
      identity_key: fields["Property Identity Key"] ?? null,
      property_name: fields["Property Name"] ?? null,
      brand: fields["Current Brand"] ?? null,
      city: fields.City ?? null,
      country: fields.Country ?? null,
      address: fields.Address ?? null,
      property_type: fields["Property Type"] ?? null,
      asset_context: fields["Asset Context"] ?? null,
      latitude: fields.Latitude ?? null,
      longitude: fields.Longitude ?? null,
      coordinate_source_type: fields["Coordinate Source Type"] ?? null,
      rooms_keys: fields["Rooms / Keys"] ?? null,
      description: fields["Hotel Description - Source Text"] ?? null,
      amenities: fields["Amenities - Source Text"] ?? null,
      owner: fields["Owner Name"] ?? null,
      operator: fields["Operator / Management Company"] ?? null,
      developer: fields["Developer Name"] ?? null,
      opening_date: fields["Opening Date"] ?? null,
      recent_momentum: fields["Recent Momentum"] ?? null,
      company_validated: fields["Company Validated"] ?? null,
      brand_verified: fields["Brand Verified"] ?? null,
      brand_status: fields["Brand Status"] ?? null,
    },
  };
}

function familyFromKey(identityKey, family) {
  if (family) return family;
  const id = String(identityKey || "");
  if (id.includes("_hilton_")) return "Hilton";
  if (id.includes("_choice_")) return "Choice";
  if (id.includes("_marriott_")) return "Marriott";
  if (id.includes("_ihg_")) return "IHG";
  return "Other";
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

export async function runPtacPreflight(opts = {}) {
  const loaded = loadPtacFrozenProposals(opts);
  if (!loaded.ok) {
    return {
      version: PTAC_APPLY_VERSION,
      generated_at: new Date().toISOString(),
      status: STATUS.BLOCKED,
      ok: false,
      error: loaded.error,
      fieldViolations: loaded.fieldViolations || null,
    };
  }

  const token = resolvePat();
  const bases = resolveTargetBase();
  if (!token || !bases?.target_base_id) {
    return {
      version: PTAC_APPLY_VERSION,
      generated_at: new Date().toISOString(),
      status: STATUS.BLOCKED,
      ok: false,
      error: "missing_airtable_credentials",
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
      version: PTAC_APPLY_VERSION,
      generated_at: new Date().toISOString(),
      status: STATUS.BLOCKED,
      ok: false,
      error: BLOCKED_WRONG_CENSUS_TARGET,
      write_target: writeTargetCheck,
    };
  }

  const rows = [];
  for (const frozen of loaded.frozen) {
    const live = await airtableGet(
      bases.target_base_id,
      token,
      CENSUS_TABLE_ID,
      frozen.record_id
    );
    await sleep(120);
    const pf = preflightPtacProposal(frozen, live);
    rows.push({
      record_id: frozen.record_id,
      identity_key: frozen.identity_key,
      property_name: frozen.property_name,
      family: familyFromKey(frozen.identity_key, frozen.family),
      lane: frozen.lane,
      queue: frozen.queue,
      confidence: frozen.confidence,
      method: frozen.method,
      proposed_property_type: frozen.patch["Property Type"] || null,
      proposed_asset_context: frozen.patch["Asset Context"] || null,
      ok: pf.ok,
      apply: pf.apply,
      skip_matching: pf.skip_matching,
      steward: pf.steward,
      blocked: pf.blocked,
      errors: pf.errors,
      steward_reasons: pf.steward_reasons,
      fields_to_write: pf.apply ? pf.idempotent.fields : {},
      live_snapshot: pf.live_snapshot,
      frozen_patch: frozen.patch,
    });
  }

  const passing = rows.filter((r) => r.apply);
  const skipMatching = rows.filter((r) => r.skip_matching);
  const steward = rows.filter((r) => r.steward);
  const blocked = rows.filter((r) => r.blocked);
  const parents = {};
  for (const r of rows) parents[r.family] = (parents[r.family] || 0) + 1;

  return {
    version: PTAC_APPLY_VERSION,
    generated_at: new Date().toISOString(),
    ok: true,
    run_dir: loaded.runDir,
    approval_bundle: loaded.bundlePath,
    base_id_masked: mask(bases.target_base_id),
    table_id: CENSUS_TABLE_ID,
    table_name: "Hotel Property Census",
    proposal_count: loaded.proposal_count,
    parent_family_breakdown: parents,
    records_passing_preflight: passing.length + skipMatching.length,
    records_ready_to_apply: passing.length,
    records_already_matching: skipMatching.length,
    records_routed_to_steward_review: steward.length,
    records_hard_blocked: blocked.length,
    exact_apply_count_after_preflight: passing.length,
    rows,
    passing_ids: passing.map((r) => r.record_id),
    steward_queue: steward.map((r) => ({
      record_id: r.record_id,
      identity_key: r.identity_key,
      property_name: r.property_name,
      lane: r.lane,
      reasons: r.steward_reasons,
      proposed_property_type: r.proposed_property_type,
      proposed_asset_context: r.proposed_asset_context,
    })),
    blocked_queue: blocked.map((r) => ({
      record_id: r.record_id,
      identity_key: r.identity_key,
      property_name: r.property_name,
      lane: r.lane,
      errors: r.errors,
    })),
    inserts_excluded: true,
    source_discovery_excluded: true,
  };
}

export async function runPtacApprovalBundleApply(argv = process.argv.slice(2), env = process.env) {
  const args = parsePtacApplyArgs(argv);
  const envCheck = checkPtacApplyEnv(env);
  const started = Date.now();

  const preflight = await runPtacPreflight({
    approvalBundlePath: args.approvalBundlePath,
    runDir: args.runDir,
  });
  if (!preflight.ok) {
    return {
      ...preflight,
      apply_executed: false,
      status: STATUS.BLOCKED,
      duration_ms: Date.now() - started,
    };
  }

  const applyRequested = args.enableProductionWrites || args.mode === "apply";
  if (applyRequested && (!args.allConfirmsOk || !envCheck.allOk)) {
    return {
      version: PTAC_APPLY_VERSION,
      generated_at: new Date().toISOString(),
      status: STATUS.BLOCKED,
      apply_executed: false,
      blocked_reason: "confirmation_or_env_missing",
      confirms: args.confirms,
      env_flags: envCheck.flags,
      preflight,
      duration_ms: Date.now() - started,
    };
  }

  if (!applyRequested) {
    return {
      version: PTAC_APPLY_VERSION,
      generated_at: new Date().toISOString(),
      mode: "preflight_only",
      status:
        preflight.exact_apply_count_after_preflight > 0
          ? preflight.records_routed_to_steward_review > 0
            ? STATUS.PARTIAL
            : STATUS.CLEAN
          : STATUS.BLOCKED,
      apply_executed: false,
      airtable_writes: false,
      brand_explorer_writes: false,
      brand_setup_writes: false,
      preflight,
      duration_ms: Date.now() - started,
    };
  }

  const token = resolvePat();
  const bases = resolveTargetBase();

  const writeTargetCheck = assertProductionCensusWriteTarget({
    baseName: productionHotelPropertyCensus.baseName,
    baseId: bases.target_base_id,
    tableName: productionHotelPropertyCensus.tableName,
    tableId: CENSUS_TABLE_ID,
  });
  if (!writeTargetCheck.ok || CENSUS_TABLE_ID !== productionHotelPropertyCensus.tableId) {
    return {
      version: PTAC_APPLY_VERSION,
      generated_at: new Date().toISOString(),
      status: STATUS.BLOCKED,
      apply_executed: false,
      blocked_reason: BLOCKED_WRONG_CENSUS_TARGET,
      write_target: writeTargetCheck,
      preflight,
      duration_ms: Date.now() - started,
    };
  }

  const censusBefore = await listAllRecords(bases.target_base_id, token, CENSUS_TABLE_ID, [
    "Property Identity Key",
    "Property Type",
    "Asset Context",
    "Rooms / Keys",
    "Latitude",
    "Longitude",
    "Hotel Description - Source Text",
    "Owner Name",
    "Operator / Management Company",
    "Property Name",
  ]);
  if (censusBefore.length !== EXPECTED_RECORD_COUNT) {
    return {
      version: PTAC_APPLY_VERSION,
      generated_at: new Date().toISOString(),
      status: STATUS.BLOCKED,
      apply_executed: false,
      blocked_reason: `unexpected_census_count_${censusBefore.length}_expected_${EXPECTED_RECORD_COUNT}`,
      preflight,
      duration_ms: Date.now() - started,
    };
  }

  const roomsFilledBefore = censusBefore.filter((r) => !isBlank(r.fields?.["Rooms / Keys"])).length;
  const toWrite = (preflight.rows || []).filter(
    (r) => r.apply && Object.keys(r.fields_to_write || {}).length
  );

  const writeResults = [];
  for (const row of toWrite) {
    const live = await airtableGet(bases.target_base_id, token, CENSUS_TABLE_ID, row.record_id);
    await sleep(100);
    const recheck = preflightPtacProposal(
      {
        record_id: row.record_id,
        identity_key: row.identity_key,
        property_name: row.property_name,
        patch: row.frozen_patch,
        confidence: "High",
        lane: row.lane,
        queue: row.queue,
      },
      live
    );
    if (recheck.blocked) {
      writeResults.push({
        record_id: row.record_id,
        identity_key: row.identity_key,
        ok: false,
        skipped: true,
        reason: recheck.errors.join(",") || "recheck_blocked",
      });
      continue;
    }
    if (!recheck.apply && recheck.skip_matching) {
      writeResults.push({
        record_id: row.record_id,
        identity_key: row.identity_key,
        ok: true,
        skipped: true,
        reason: "already_matching",
      });
      continue;
    }
    if (recheck.steward) {
      writeResults.push({
        record_id: row.record_id,
        identity_key: row.identity_key,
        ok: false,
        skipped: true,
        reason: (recheck.steward_reasons || []).join(",") || "recheck_steward",
      });
      continue;
    }

    const fields =
      recheck.apply && Object.keys(recheck.idempotent.fields || {}).length
        ? { ...recheck.idempotent.fields }
        : { ...row.fields_to_write };

    for (const k of Object.keys(fields)) {
      if (
        FORBIDDEN_IN_PATCH.includes(k) ||
        AUTOPILOT_FORBIDDEN_FIELDS.includes(k) ||
        !PTAC_LANE_FIELDS.includes(k)
      ) {
        delete fields[k];
      }
    }
    if (!Object.keys(fields).length) {
      writeResults.push({
        record_id: row.record_id,
        identity_key: row.identity_key,
        ok: true,
        skipped: true,
        reason: "empty_patch_after_sanitize",
      });
      continue;
    }

    try {
      await airtablePatch(bases.target_base_id, token, CENSUS_TABLE_ID, row.record_id, fields);
      writeResults.push({
        record_id: row.record_id,
        identity_key: row.identity_key,
        lane: row.lane,
        ok: true,
        skipped: false,
        fields_written: Object.keys(fields),
        patch: fields,
      });
      await sleep(180);
    } catch (err) {
      writeResults.push({
        record_id: row.record_id,
        identity_key: row.identity_key,
        lane: row.lane,
        ok: false,
        error: err?.message || String(err),
      });
    }
  }

  const writesOk = writeResults.filter((w) => w.ok && !w.skipped).length;
  const writesFail = writeResults.filter((w) => !w.ok && !w.skipped).length;
  const writesSkipped = writeResults.filter((w) => w.skipped).length;

  const postVerify = [];
  for (const row of toWrite) {
    const written = writeResults.find((w) => w.record_id === row.record_id && w.ok && !w.skipped);
    if (!written) continue;
    const live = await airtableGet(bases.target_base_id, token, CENSUS_TABLE_ID, row.record_id);
    await sleep(100);
    const f = live.fields || {};
    const snap = row.live_snapshot || {};
    postVerify.push({
      record_id: row.record_id,
      identity_key: row.identity_key,
      property_type_applied: row.proposed_property_type
        ? String(f["Property Type"] || "") === String(row.proposed_property_type)
        : null,
      asset_context_applied: row.proposed_asset_context
        ? String(f["Asset Context"] || "") === String(row.proposed_asset_context)
        : null,
      coords_unchanged:
        String(f.Latitude ?? "") === String(snap.latitude ?? "") &&
        String(f.Longitude ?? "") === String(snap.longitude ?? ""),
      description_unchanged:
        String(f["Hotel Description - Source Text"] ?? "") === String(snap.description ?? ""),
      amenities_unchanged:
        String(f["Amenities - Source Text"] ?? "") === String(snap.amenities ?? ""),
      rooms_unchanged: String(f["Rooms / Keys"] ?? "") === String(snap.rooms_keys ?? ""),
      property_name_unchanged:
        String(f["Property Name"] ?? "") === String(snap.property_name ?? ""),
      owner_still_blank: isBlank(f["Owner Name"]),
      operator_still_blank: isBlank(f["Operator / Management Company"]),
      company_validated_unchanged:
        String(f["Company Validated"] ?? "") === String(snap.company_validated ?? ""),
      brand_verified_unchanged:
        String(f["Brand Verified"] ?? "") === String(snap.brand_verified ?? ""),
      brand_status_unchanged:
        String(f["Brand Status"] ?? "") === String(snap.brand_status ?? ""),
    });
  }

  const censusAfter = await listAllRecords(bases.target_base_id, token, CENSUS_TABLE_ID, [
    "Property Identity Key",
    "Rooms / Keys",
  ]);
  const roomsFilledAfter = censusAfter.filter((r) => !isBlank(r.fields?.["Rooms / Keys"])).length;

  const verifyOk =
    postVerify.length === 0 ||
    postVerify.every(
      (v) =>
        (v.property_type_applied === null || v.property_type_applied) &&
        (v.asset_context_applied === null || v.asset_context_applied) &&
        v.coords_unchanged &&
        v.description_unchanged &&
        v.amenities_unchanged &&
        v.rooms_unchanged &&
        v.property_name_unchanged &&
        v.owner_still_blank &&
        v.operator_still_blank &&
        v.company_validated_unchanged &&
        v.brand_verified_unchanged &&
        v.brand_status_unchanged
    );
  const censusOk = censusAfter.length === EXPECTED_RECORD_COUNT;
  const roomsOk = roomsFilledAfter === roomsFilledBefore;

  let status = STATUS.PARTIAL;
  if (writesOk === 0 && (writesFail > 0 || preflight.exact_apply_count_after_preflight === 0)) {
    status = STATUS.BLOCKED;
  } else if (
    writesFail === 0 &&
    verifyOk &&
    roomsOk &&
    censusOk &&
    preflight.records_routed_to_steward_review === 0 &&
    writesOk + writesSkipped === preflight.exact_apply_count_after_preflight + preflight.records_already_matching &&
    writesOk === preflight.exact_apply_count_after_preflight
  ) {
    status = STATUS.CLEAN;
  } else if (writesOk > 0 && writesFail === 0 && verifyOk && censusOk) {
    status = STATUS.PARTIAL;
  } else if (writesFail > 0 && writesOk === 0) {
    status = STATUS.BLOCKED;
  }

  return {
    version: PTAC_APPLY_VERSION,
    generated_at: new Date().toISOString(),
    mode: "apply",
    apply_executed: true,
    status,
    airtable_writes: writesOk > 0,
    brand_explorer_writes: false,
    brand_setup_writes: false,
    inserts_applied: 0,
    source_discovery_untouched: true,
    run_dir: preflight.run_dir,
    approval_bundle: preflight.approval_bundle,
    table_id: CENSUS_TABLE_ID,
    census_record_count_before: censusBefore.length,
    census_record_count_after: censusAfter.length,
    rooms_filled_before: roomsFilledBefore,
    rooms_filled_after: roomsFilledAfter,
    records_updated: writesOk,
    records_skipped: writesSkipped,
    records_failed: writesFail,
    steward_count: preflight.records_routed_to_steward_review,
    blocked_count: preflight.records_hard_blocked,
    fields_written_union: [
      ...new Set(writeResults.flatMap((w) => w.fields_written || [])),
    ],
    write_results: writeResults,
    post_verify: postVerify,
    post_verify_ok: verifyOk,
    census_count_ok: censusOk,
    rooms_unchanged_ok: roomsOk,
    preflight,
    steward_queue: preflight.steward_queue,
    blocked_queue: preflight.blocked_queue,
    duration_ms: Date.now() - started,
  };
}

export function renderPtacPreflightMarkdown(report) {
  return [
    `# Production Census — Property Type / Asset Context Preflight`,
    ``,
    `- Generated: ${report.generated_at}`,
    `- Approval bundle: \`${report.approval_bundle || ""}\``,
    `- Proposals: ${report.proposal_count}`,
    `- Ready to apply: **${report.exact_apply_count_after_preflight}**`,
    `- Already matching: ${report.records_already_matching}`,
    `- Steward: ${report.records_routed_to_steward_review}`,
    `- Hard blocked: ${report.records_hard_blocked}`,
    `- Inserts excluded: ${report.inserts_excluded}`,
    `- Source discovery excluded: ${report.source_discovery_excluded}`,
    ``,
    `## Parent / family`,
    ``,
    "```json",
    JSON.stringify(report.parent_family_breakdown || {}, null, 2),
    "```",
    ``,
  ].join("\n");
}

export function renderPtacApplyMarkdown(report) {
  return [
    `# Production Census — Property Type / Asset Context Apply`,
    ``,
    `- Status: **${report.status}**`,
    `- Generated: ${report.generated_at}`,
    `- Apply executed: ${report.apply_executed}`,
    `- Records updated: ${report.records_updated ?? 0}`,
    `- Records skipped: ${report.records_skipped ?? 0}`,
    `- Records failed: ${report.records_failed ?? 0}`,
    `- Steward review: ${report.steward_count ?? 0}`,
    `- Hard blocked: ${report.blocked_count ?? 0}`,
    `- Census count: ${report.census_record_count_after} (before ${report.census_record_count_before})`,
    `- Rooms filled unchanged: ${report.rooms_filled_before} → ${report.rooms_filled_after}`,
    `- Brand Explorer writes: ${report.brand_explorer_writes}`,
    `- Brand Setup writes: ${report.brand_setup_writes}`,
    `- Inserts applied: ${report.inserts_applied ?? 0}`,
    `- Fields written: ${(report.fields_written_union || []).join(", ") || "(none)"}`,
    `- Post-verify OK: ${report.post_verify_ok}`,
    ``,
    `## Preflight summary`,
    ``,
    "```json",
    JSON.stringify(
      {
        proposal_count: report.preflight?.proposal_count,
        records_ready_to_apply: report.preflight?.records_ready_to_apply,
        records_already_matching: report.preflight?.records_already_matching,
        records_routed_to_steward_review: report.preflight?.records_routed_to_steward_review,
        records_hard_blocked: report.preflight?.records_hard_blocked,
        parent_family_breakdown: report.preflight?.parent_family_breakdown,
      },
      null,
      2
    ),
    "```",
    ``,
  ].join("\n");
}

export function isPtacOnlyApprovalBundle(bundlePath) {
  if (!bundlePath || !existsSync(bundlePath)) return false;
  try {
    const bundle = JSON.parse(readFileSync(bundlePath, "utf8"));
    const byQueue = bundle.proposed_writes_by_queue || {};
    const queues = Object.keys(byQueue).filter((q) => (byQueue[q] || []).length > 0);
    return (
      queues.length === 1 &&
      queues[0] === "property_type_asset_context" &&
      !bundle.inserts &&
      !bundle.insert_candidates
    );
  } catch {
    return false;
  }
}
