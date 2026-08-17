/**
 * Apply approved Production Census schema v1.1.1 cleanup (renames + optional views).
 * No record writes. No field deletes. No Brand Explorer writes.
 */

import { resolvePat, resolveTargetBase } from "./production-census-schema-create.js";
import { EXPECTED_FREEZE, PRODUCTION_USE_STATUS } from "./production-census-write.js";

export const APPLY_VERSION = "production-census-schema-v111-cleanup-apply-v1";

export const STATUS = Object.freeze({
  APPLIED: "production_census_schema_v111_cleanup_applied_ready_for_contract_and_enrichment",
  PARTIAL: "production_census_schema_v111_cleanup_partial_manual_view_steps_needed",
  BLOCKED: "production_census_schema_v111_cleanup_blocked_by_conflict",
  CONFIRMATION_MISSING: "production_census_schema_v111_cleanup_confirmation_missing",
});

export const RENAMES = Object.freeze([
  { from: "Last Verified Date", to: "Last Reviewed Date" },
  { from: "Resort Amenities Flag", to: "Resort / Leisure Flag" },
  { from: "Extended Stay Amenity Flag", to: "Extended Stay Flag" },
]);

export const KEEP_UNCHANGED = Object.freeze([
  "Rooms / Keys",
  "Operator / Management Company",
  "Owner Name",
  "Source URL",
  "State / Region",
]);

export const HIDE_AMENITY_FLAGS = Object.freeze([
  "Fitness Flag",
  "Pool Flag",
  "Parking Flag",
  "Airport Shuttle Flag",
  "Spa Flag",
  "Beach / Waterfront Flag",
]);

export const SUGGESTED_VIEWS = Object.freeze([
  {
    name: "Census - Core Identity",
    fields: [
      "Property Name",
      "Current Brand",
      "City",
      "State / Region",
      "Country",
      "Affiliation Status",
      "Production Use Status",
      "Data Confidence Tier",
      "Enrichment Status",
      "Human Review Required",
    ],
  },
  {
    name: "Census - Enrichment",
    fields: [
      "Property Name",
      "Hotel Description - Source Text",
      "Hotel Description - AI Summary",
      "Amenities - Source Text",
      "Amenities - Structured Tags",
      "Property Type",
      "Asset Context",
      "Market / Submarket",
      "Enrichment Status",
      "Enrichment Priority",
    ],
  },
  {
    name: "Census - Owner Operator",
    fields: [
      "Property Name",
      "Owner Name",
      "Owner Confidence",
      "Operator / Management Company",
      "Operator Confidence",
      "Ownership Review Status",
      "Operator Review Status",
    ],
  },
  {
    name: "Census - Steward Review",
    fields: [
      "Property Name",
      "Human Review Required",
      "Notes for Steward",
      "Brand-Unassigned Reason",
      "Enrichment Priority",
    ],
  },
]);

function mask(id) {
  if (!id || id.length < 10) return id ? "***" : null;
  return `${id.slice(0, 6)}…${id.slice(-4)}`;
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

export function checkV111ApplyEnv() {
  const flags = {
    ALLOW_PRODUCTION_CENSUS_SCHEMA_V111: process.env.ALLOW_PRODUCTION_CENSUS_SCHEMA_V111 === "1",
    CONFIRM_NO_RECORD_WRITES: process.env.CONFIRM_NO_RECORD_WRITES === "1",
    CONFIRM_NO_FIELD_DELETES: process.env.CONFIRM_NO_FIELD_DELETES === "1",
    CONFIRM_NO_BRAND_EXPLORER_WRITES: process.env.CONFIRM_NO_BRAND_EXPLORER_WRITES === "1",
  };
  return { allOk: Object.values(flags).every(Boolean), flags };
}

export function parseV111ApplyArgs(argv = process.argv.slice(2)) {
  const flags = new Set(argv.filter((a) => a.startsWith("--")));
  return {
    dryRun: flags.has("--dry-run") || !flags.has("--apply"),
    apply: flags.has("--apply"),
    confirms: {
      v111: flags.has("--confirm-production-census-schema-v111"),
      renamesOnly: flags.has("--confirm-approved-renames-only"),
      noDeletes: flags.has("--confirm-no-field-deletes"),
      noRecords: flags.has("--confirm-no-record-writes"),
      noBe: flags.has("--confirm-no-brand-explorer-writes"),
      noCv: flags.has("--confirm-no-company-validation-writes"),
      noVerified: flags.has("--confirm-no-brand-verified-writes"),
      noMomentum: flags.has("--confirm-no-recent-momentum-writes"),
    },
  };
}

export function allV111ApplyConfirms(args) {
  return Object.values(args.confirms).every(Boolean);
}

async function metaFetch(baseId, token, path, init = {}) {
  const url = `https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}${path}`;
  const res = await fetch(url, {
    ...init,
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      ...(init.headers || {}),
    },
  });
  const text = await res.text();
  let json;
  try {
    json = text ? JSON.parse(text) : {};
  } catch {
    json = { raw: text };
  }
  return { res, json };
}

async function listTables(baseId, token, includeVisible = false) {
  const q = includeVisible ? "?include%5B%5D=visibleFieldIds" : "";
  const { res, json } = await metaFetch(baseId, token, `/tables${q}`);
  if (!res.ok) throw new Error(`meta tables ${res.status}`);
  return json.tables || [];
}

async function listAll(baseId, token, tableId, fields) {
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
    await sleep(100);
  } while (offset);
  return out;
}

export async function runV111Precheck() {
  const token = resolvePat();
  const bases = resolveTargetBase();
  const tables = await listTables(bases.target_base_id, token, true);
  const census = tables.find((t) => t.name === "Hotel Property Census");
  if (!census) {
    return { ok: false, status: STATUS.BLOCKED, conflicts: [{ code: "census_table_missing" }] };
  }
  const byName = Object.fromEntries((census.fields || []).map((f) => [f.name, f]));
  const conflicts = [];

  for (const r of RENAMES) {
    if (!byName[r.from]) conflicts.push({ code: "rename_source_missing", field: r.from });
    if (byName[r.to]) conflicts.push({ code: "rename_target_exists", field: r.to });
  }
  for (const k of KEEP_UNCHANGED) {
    if (!byName[k]) conflicts.push({ code: "keep_field_missing", field: k });
  }
  for (const k of HIDE_AMENITY_FLAGS) {
    if (!byName[k]) conflicts.push({ code: "hide_field_missing", field: k });
  }

  const amenityCheckFields = [...HIDE_AMENITY_FLAGS];
  const rows = await listAll(bases.target_base_id, token, census.id, [
    "Property Identity Key",
    ...amenityCheckFields,
  ]);
  const filled = {};
  for (const name of amenityCheckFields) filled[name] = 0;
  for (const row of rows) {
    for (const name of amenityCheckFields) {
      if (row.fields?.[name] === true) filled[name] += 1;
    }
  }
  const anyFilled = Object.values(filled).some((n) => n > 0);

  return {
    ok: conflicts.length === 0 && !anyFilled && rows.length === 666,
    status: conflicts.length || anyFilled ? STATUS.BLOCKED : null,
    conflicts,
    table_id: census.id,
    field_count: census.fields.length,
    record_count: rows.length,
    amenity_filled: filled,
    views: (census.views || []).map((v) => ({
      id: v.id,
      name: v.name,
      type: v.type,
      visible_count: v.visibleFieldIds?.length ?? null,
    })),
    byName,
    census,
  };
}

async function renameField(baseId, token, tableId, fieldId, newName) {
  return metaFetch(baseId, token, `/tables/${tableId}/fields/${fieldId}`, {
    method: "PATCH",
    body: JSON.stringify({ name: newName }),
  });
}

async function tryCreateView(baseId, token, tableId, viewSpec, fieldIdByName) {
  const visibleFieldIds = [];
  const missing = [];
  for (const name of viewSpec.fields) {
    const id = fieldIdByName[name];
    if (!id) missing.push(name);
    else visibleFieldIds.push(id);
  }
  if (!visibleFieldIds.length) {
    return { ok: false, status: 0, missing, error: "no_resolvable_fields" };
  }
  const { res, json } = await metaFetch(baseId, token, `/tables/${tableId}/views`, {
    method: "POST",
    body: JSON.stringify({
      name: viewSpec.name,
      type: "grid",
      visibleFieldIds,
    }),
  });
  return {
    ok: res.ok,
    status: res.status,
    json,
    missing,
    visibleFieldIds,
  };
}

function manualHideInstructions() {
  return {
    supported_via_api: false,
    reason:
      "Airtable Meta API can read visibleFieldIds but does not expose a supported endpoint to hide fields on existing views.",
    ui_steps: [
      "Open Deal Capture Platform → Hotel Property Census → Grid view",
      "Hide fields: Fitness Flag, Pool Flag, Parking Flag, Airport Shuttle Flag, Spa Flag, Beach / Waterfront Flag",
      "Optional: create founder views listed in this report with only recommended columns visible",
    ],
  };
}

function manualViewInstructions(views) {
  return views.map((v) => ({
    name: v.name,
    fields: v.fields,
    note: "Create manually in Airtable if API view create is unavailable",
  }));
}

export async function runV111CleanupApply(argv = process.argv.slice(2)) {
  const args = parseV111ApplyArgs(argv);
  const env = checkV111ApplyEnv();
  const started = Date.now();

  const pre = await runV111Precheck();
  if (!pre.ok) {
    return {
      version: APPLY_VERSION,
      generated_at: new Date().toISOString(),
      mode: "apply_blocked",
      apply_executed: false,
      status: STATUS.BLOCKED,
      precheck: pre,
    };
  }

  if (!args.apply) {
    return {
      version: APPLY_VERSION,
      generated_at: new Date().toISOString(),
      mode: "dry-run",
      apply_executed: false,
      status: pre.ok
        ? "production_census_schema_v111_cleanup_dry_run_pass"
        : STATUS.BLOCKED,
      precheck: {
        ok: pre.ok,
        record_count: pre.record_count,
        field_count: pre.field_count,
        amenity_filled: pre.amenity_filled,
        conflicts: pre.conflicts,
        views: pre.views,
      },
      planned_renames: RENAMES,
      keep_unchanged: KEEP_UNCHANGED,
      hide_fields: HIDE_AMENITY_FLAGS,
      planned_views: SUGGESTED_VIEWS,
      env_ok_for_apply: env.allOk,
      env_flags: env.flags,
      next_recommended_step:
        "Re-run with --apply and confirmation env/flags to rename approved fields.",
    };
  }

  if (!env.allOk || !allV111ApplyConfirms(args)) {
    return {
      version: APPLY_VERSION,
      generated_at: new Date().toISOString(),
      mode: "apply_blocked",
      apply_executed: false,
      status: STATUS.CONFIRMATION_MISSING,
      env_flags: env.flags,
      confirms: args.confirms,
    };
  }

  const token = resolvePat();
  const bases = resolveTargetBase();
  const tableId = pre.table_id;
  const renamesApplied = [];
  const renameErrors = [];

  for (const r of RENAMES) {
    const field = pre.byName[r.from];
    const { res, json } = await renameField(bases.target_base_id, token, tableId, field.id, r.to);
    if (!res.ok) {
      renameErrors.push({ from: r.from, to: r.to, status: res.status, error: json.error || json });
      break;
    }
    renamesApplied.push({ from: r.from, to: r.to, field_id: field.id });
    await sleep(250);
  }

  if (renameErrors.length) {
    return {
      version: APPLY_VERSION,
      generated_at: new Date().toISOString(),
      mode: "apply",
      apply_executed: true,
      status: STATUS.BLOCKED,
      renames_applied: renamesApplied,
      rename_errors: renameErrors,
    };
  }

  // Refresh field map after renames
  const tablesAfterRename = await listTables(bases.target_base_id, token, true);
  const censusAfter = tablesAfterRename.find((t) => t.id === tableId);
  const fieldIdByName = Object.fromEntries((censusAfter.fields || []).map((f) => [f.name, f.id]));

  const viewsCreated = [];
  const viewsFailed = [];
  let viewApiSupported = null;

  for (const spec of SUGGESTED_VIEWS) {
    // Skip if view already exists
    const existing = (censusAfter.views || []).find((v) => v.name === spec.name);
    if (existing) {
      viewsCreated.push({ name: spec.name, id: existing.id, already_existed: true });
      continue;
    }
    const result = await tryCreateView(bases.target_base_id, token, tableId, spec, fieldIdByName);
    if (viewApiSupported === null) viewApiSupported = result.ok || result.status !== 404;
    if (result.ok) {
      viewsCreated.push({
        name: spec.name,
        id: result.json?.id,
        visible_fields: spec.fields,
        missing_fields: result.missing,
      });
    } else {
      viewsFailed.push({
        name: spec.name,
        status: result.status,
        error: result.json?.error || result.error,
        missing_fields: result.missing,
      });
      // If first create is 404/405, stop trying
      if (result.status === 404 || result.status === 405 || result.status === 403) break;
    }
    await sleep(300);
  }

  const hideResult = manualHideInstructions();

  // Post validation
  const tablesFinal = await listTables(bases.target_base_id, token, true);
  const censusFinal = tablesFinal.find((t) => t.id === tableId);
  const namesFinal = new Set((censusFinal.fields || []).map((f) => f.name));

  const validationFields = [
    "Property Identity Key",
    "Enrichment Status",
    "Human Review Required",
    "Production Use Status",
    "Hotel Description - Source Text",
    "Amenities - Source Text",
    "Owner Name",
    "Operator / Management Company",
    "Rooms / Keys",
    "Opening Date",
    "Latitude",
    "Longitude",
    ...HIDE_AMENITY_FLAGS,
  ];
  // Use renamed names where applicable
  const rows = await listAll(bases.target_base_id, token, tableId, validationFields);

  const amenityFilled = {};
  for (const n of HIDE_AMENITY_FLAGS) amenityFilled[n] = 0;
  for (const row of rows) {
    for (const n of HIDE_AMENITY_FLAGS) {
      if (row.fields?.[n] === true) amenityFilled[n] += 1;
    }
  }

  const validation = {
    record_count: rows.length,
    field_count: censusFinal.fields.length,
    duplicates: (() => {
      const m = new Map();
      for (const r of rows) {
        const k = r.fields?.["Property Identity Key"];
        m.set(k, (m.get(k) || 0) + 1);
      }
      return [...m.values()].filter((n) => n > 1).length;
    })(),
    renames_present: RENAMES.every((r) => namesFinal.has(r.to) && !namesFinal.has(r.from)),
    old_names_gone: RENAMES.every((r) => !namesFinal.has(r.from)),
    hide_fields_still_exist: HIDE_AMENITY_FLAGS.every((n) => namesFinal.has(n)),
    amenity_filled: amenityFilled,
    enrichment_not_started: rows.filter((r) => r.fields?.["Enrichment Status"] === "Not Started").length,
    human_review_true: rows.filter((r) => r.fields?.["Human Review Required"] === true).length,
    production_use_ok: rows.filter((r) => r.fields?.["Production Use Status"] === PRODUCTION_USE_STATUS)
      .length,
    description_filled: rows.filter((r) => Boolean(r.fields?.["Hotel Description - Source Text"]))
      .length,
    amenities_filled: rows.filter((r) => Boolean(r.fields?.["Amenities - Source Text"])).length,
    owner_filled: rows.filter((r) => Boolean(r.fields?.["Owner Name"])).length,
    operator_filled: rows.filter((r) => Boolean(r.fields?.["Operator / Management Company"]))
      .length,
    rooms_filled: rows.filter((r) => r.fields?.["Rooms / Keys"] != null).length,
    opening_filled: rows.filter((r) => Boolean(r.fields?.["Opening Date"])).length,
    zero_zero: rows.filter((r) => r.fields?.Latitude === 0 && r.fields?.Longitude === 0).length,
    no_fields_deleted: censusFinal.fields.length >= pre.field_count,
  };

  const validationPass =
    validation.record_count === 666 &&
    validation.duplicates === 0 &&
    validation.renames_present &&
    validation.hide_fields_still_exist &&
    validation.enrichment_not_started === 666 &&
    validation.human_review_true === 4 &&
    validation.production_use_ok === 666 &&
    validation.description_filled === 0 &&
    validation.amenities_filled === 0 &&
    validation.owner_filled === 0 &&
    validation.operator_filled === 0 &&
    validation.rooms_filled === 0 &&
    validation.opening_filled === 0 &&
    validation.zero_zero === 0 &&
    Object.values(amenityFilled).every((n) => n === 0);

  const viewsNeedManual = viewsFailed.length > 0 || viewsCreated.filter((v) => !v.already_existed && v.id).length < SUGGESTED_VIEWS.length;
  const hideNeedsManual = true; // API cannot hide on existing Grid view

  let status = STATUS.APPLIED;
  if (!validationPass) status = STATUS.BLOCKED;
  else if (viewsNeedManual || hideNeedsManual) status = STATUS.PARTIAL;

  return {
    version: APPLY_VERSION,
    generated_at: new Date().toISOString(),
    mode: "apply",
    apply_executed: true,
    status,
    duration_ms: Date.now() - started,
    token_masked: mask(token),
    base_id_masked: mask(bases.target_base_id),
    table_id: tableId,
    approved_decisions_applied: {
      A: "renamed Last Verified Date → Last Reviewed Date",
      B: "kept Rooms / Keys",
      C: "kept Operator / Management Company",
      D: "kept Owner Name",
      E: "kept Source URL",
      F: "kept State / Region",
      G: "hide over-modeled amenity flags — manual UI (API unsupported)",
      H: "renamed Resort Amenities Flag → Resort / Leisure Flag; Extended Stay Amenity Flag → Extended Stay Flag",
    },
    fields_renamed: renamesApplied,
    fields_kept_unchanged: KEEP_UNCHANGED,
    amenity_hide: {
      fields: HIDE_AMENITY_FLAGS,
      deleted: false,
      hidden_via_api: false,
      manual: hideResult,
    },
    views: {
      api_supported: viewApiSupported,
      created: viewsCreated,
      failed: viewsFailed,
      manual_instructions: viewsNeedManual ? manualViewInstructions(SUGGESTED_VIEWS) : [],
    },
    validation,
    validation_pass: validationPass,
    remaining_cleanup_items: [
      ...(hideNeedsManual
        ? ["Manually hide 6 over-modeled amenity flags from Grid view / founder views"]
        : []),
      ...(viewsNeedManual ? ["Manually create 4 founder review views if API create failed"] : []),
      "Update internal scripts referencing Last Verified Date / Resort Amenities Flag / Extended Stay Amenity Flag",
    ],
    next_recommended_step:
      "Complete manual hide + view setup if needed, then freeze Census field contract and start descriptions + amenities + property type enrichment.",
    freeze_hash: EXPECTED_FREEZE,
  };
}

export function renderV111ApplyMarkdown(r) {
  return [
    `# Production Census Schema v1.1.1 Cleanup Apply`,
    ``,
    `**Status:** \`${r.status}\``,
    `**Mode:** ${r.mode}`,
    `**Generated:** ${r.generated_at}`,
    ``,
    `## 1. Executive summary`,
    ``,
    `- Apply executed: ${r.apply_executed}`,
    `- Renames: ${(r.fields_renamed || []).length}`,
    `- Validation pass: ${r.validation_pass}`,
    `- Views API: ${r.views?.api_supported}`,
    `- Amenity hide via API: ${r.amenity_hide?.hidden_via_api}`,
    ``,
    `## 2. Approved founder decisions applied`,
    ``,
    "```json",
    JSON.stringify(r.approved_decisions_applied || {}, null, 2),
    "```",
    ``,
    `## 3. Fields renamed`,
    ``,
    "```json",
    JSON.stringify(r.fields_renamed || [], null, 2),
    "```",
    ``,
    `## 4. Fields kept unchanged`,
    ``,
    ...(r.fields_kept_unchanged || []).map((f) => `- ${f}`),
    ``,
    `## 5. Amenity fields hidden / manual instructions`,
    ``,
    "```json",
    JSON.stringify(r.amenity_hide || {}, null, 2),
    "```",
    ``,
    `## 6. Views created / manual instructions`,
    ``,
    "```json",
    JSON.stringify(r.views || {}, null, 2),
    "```",
    ``,
    `## 7. Census validation`,
    ``,
    "```json",
    JSON.stringify(r.validation || r.precheck || {}, null, 2),
    "```",
    ``,
    `## 8. Brand Explorer safety result`,
    ``,
    "```json",
    JSON.stringify(r.brand_explorer_safety || { pending: true }, null, 2),
    "```",
    ``,
    `## 9. Remaining cleanup items`,
    ``,
    ...(r.remaining_cleanup_items || []).map((x) => `- ${x}`),
    ``,
    `## 10. Next recommended step`,
    ``,
    r.next_recommended_step || "",
    ``,
  ].join("\n");
}
