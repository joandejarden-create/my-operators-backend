/**
 * Production Census schema v1.1.2 — add Radar/public display fields only.
 * Schema create only. No record writes. No Brand Explorer writes.
 */

import { resolvePat, resolveTargetBase } from "./production-census-schema-create.js";
import { PRODUCTION_USE_STATUS, TABLE_IDS } from "./production-census-write.js";

export const V112_VERSION = "production-census-schema-v112-radar-fields-v1";
export const CENSUS_TABLE = "Hotel Property Census";
export const CENSUS_TABLE_ID = TABLE_IDS["Hotel Property Census"];

export const STATUS = Object.freeze({
  CONFIRMATION_MISSING: "production_census_schema_v112_confirmation_missing",
  DRY_RUN_PASS: "production_census_schema_v112_dry_run_pass",
  DRY_RUN_FAIL: "production_census_schema_v112_dry_run_fail",
  APPLIED: "production_census_schema_v112_radar_fields_added_ready_for_radar_readiness_classification",
  PARTIAL: "production_census_schema_v112_partial_fields_added_needs_review",
  BLOCKED: "production_census_schema_v112_blocked_by_schema_conflict",
});

const RADAR_DISPLAY_STATUS_OPTIONS = [
  "Public Map Eligible",
  "Public List Eligible",
  "Internal Only",
  "Hold",
];
const RADAR_GEOGRAPHY_STATUS_OPTIONS = [
  "Coordinates Available",
  "City-Level Only",
  "Address Available No Coordinates",
  "Geography Insufficient",
  "Hold",
];
const PUBLIC_CENSUS_ELIGIBILITY_OPTIONS = [
  "Eligible",
  "Eligible With Limits",
  "Not Eligible",
  "Hold",
];
const PUBLIC_DISPLAY_CONFIDENCE_OPTIONS = ["High", "Medium", "Low", "Hold"];
const PUBLIC_DISPLAY_REVIEW_STATUS_OPTIONS = [
  "Auto-Classified",
  "Needs Review",
  "Approved",
  "Hold",
];

function choices(names) {
  return names.map((name) => ({ name: String(name) }));
}
function singleSelect(name, optionNames, description) {
  return {
    name,
    type: "singleSelect",
    description,
    options: { choices: choices(optionNames) },
  };
}
function longText(name, description) {
  return { name, type: "multilineText", description };
}

export function buildV112RadarFieldSpecs() {
  return [
    singleSelect(
      "Radar Display Status",
      RADAR_DISPLAY_STATUS_OPTIONS,
      "Radar map/list eligibility — leave blank until readiness classification"
    ),
    longText(
      "Radar Display Reason",
      "Why a property is eligible/held for Radar display — leave blank until classification"
    ),
    singleSelect(
      "Radar Geography Status",
      RADAR_GEOGRAPHY_STATUS_OPTIONS,
      "Geography readiness for public map — leave blank until classification"
    ),
    singleSelect(
      "Public Census Eligibility",
      PUBLIC_CENSUS_ELIGIBILITY_OPTIONS,
      "Whether Census row may surface publicly — leave blank until classification"
    ),
    singleSelect(
      "Public Display Confidence",
      PUBLIC_DISPLAY_CONFIDENCE_OPTIONS,
      "Confidence in public-facing Census display — leave blank until classification"
    ),
    singleSelect(
      "Public Display Review Status",
      PUBLIC_DISPLAY_REVIEW_STATUS_OPTIONS,
      "Review state for public display — leave blank until classification"
    ),
  ];
}

export function checkV112EnvFlags() {
  const flags = {
    ALLOW_PRODUCTION_CENSUS_SCHEMA_V112: process.env.ALLOW_PRODUCTION_CENSUS_SCHEMA_V112 === "1",
    CONFIRM_SCHEMA_ONLY_NO_RECORD_WRITES:
      process.env.CONFIRM_SCHEMA_ONLY_NO_RECORD_WRITES === "1",
    CONFIRM_NO_BRAND_EXPLORER_WRITES: process.env.CONFIRM_NO_BRAND_EXPLORER_WRITES === "1",
  };
  return { allOk: Object.values(flags).every(Boolean), flags };
}

export function parseV112Args(argv = process.argv.slice(2)) {
  const flags = new Set(argv.filter((a) => a.startsWith("--")));
  // Task contract confirms (preferred) + legacy v1.1-style confirms (accepted).
  const addRadarOnly =
    flags.has("--confirm-add-radar-public-fields-only") ||
    flags.has("--confirm-production-census-schema-v112");
  const noRecordWrites =
    flags.has("--confirm-no-record-writes") ||
    flags.has("--confirm-schema-only-no-record-writes");
  const noBe = flags.has("--confirm-no-brand-explorer-writes");
  const noFieldDeletes = flags.has("--confirm-no-field-deletes");
  const noFieldRenames = flags.has("--confirm-no-field-renames");
  const taskContractOk =
    flags.has("--confirm-add-radar-public-fields-only") &&
    flags.has("--confirm-no-record-writes") &&
    noBe &&
    noFieldDeletes &&
    noFieldRenames;
  const legacyOk =
    flags.has("--confirm-production-census-schema-v112") &&
    flags.has("--confirm-census-table-only") &&
    flags.has("--confirm-schema-only-no-record-writes") &&
    noBe &&
    flags.has("--confirm-no-brand-status-writes") &&
    flags.has("--confirm-no-company-validation-writes") &&
    flags.has("--confirm-no-brand-verified-writes") &&
    flags.has("--confirm-no-recent-momentum-writes") &&
    flags.has("--confirm-no-radar-field-population") &&
    flags.has("--confirm-no-enrichment-writes");
  return {
    dryRun: flags.has("--dry-run") || !flags.has("--apply"),
    apply: flags.has("--apply"),
    confirms: {
      addRadarOnly,
      noRecordWrites,
      noBe,
      noFieldDeletes,
      noFieldRenames,
      taskContractOk,
      legacyOk,
    },
  };
}

export function allV112ConfirmsPresent(args) {
  const c = args.confirms || {};
  return Boolean(c.taskContractOk || c.legacyOk);
}

function mask(id) {
  if (!id || id.length < 10) return id ? "***" : null;
  return `${id.slice(0, 6)}…${id.slice(-4)}`;
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
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

async function listTables(baseId, token) {
  const { res, json } = await metaFetch(baseId, token, "/tables");
  if (!res.ok) throw new Error(`meta tables ${res.status}: ${JSON.stringify(json.error || json)}`);
  return json.tables || [];
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
    if (!res.ok) throw new Error(`list ${tableId} ${res.status}: ${JSON.stringify(json.error || json)}`);
    out.push(...(json.records || []));
    offset = json.offset;
    await sleep(120);
  } while (offset);
  return out;
}

async function validateCensusState(baseId, token, tableId, expectedFieldCount) {
  const tables = await listTables(baseId, token);
  const census = tables.find((t) => t.id === tableId || t.name === CENSUS_TABLE);
  const names = new Set((census?.fields || []).map((f) => f.name));
  const radarNames = buildV112RadarFieldSpecs().map((s) => s.name);

  const rows = await listAllRecords(baseId, token, tableId, [
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
    "Renovation / Conversion Date",
    "Affiliation Start Date",
    "Latitude",
    "Longitude",
    ...radarNames.filter((n) => names.has(n)),
  ]);

  const keyCounts = new Map();
  for (const r of rows) {
    const k = r.fields?.["Property Identity Key"];
    keyCounts.set(k, (keyCounts.get(k) || 0) + 1);
  }

  const radarPopulated = {};
  for (const n of radarNames) {
    if (!names.has(n)) continue;
    radarPopulated[n] = rows.filter((r) => {
      const v = r.fields?.[n];
      return v != null && v !== "" && v !== false;
    }).length;
  }

  const validation = {
    record_count: rows.length,
    field_count: census?.fields?.length ?? 0,
    expected_field_count: expectedFieldCount,
    duplicates: [...keyCounts.values()].filter((n) => n > 1).length,
    radar_fields_present: radarNames.filter((n) => names.has(n)),
    radar_fields_missing: radarNames.filter((n) => !names.has(n)),
    radar_populated_counts: radarPopulated,
    enrichment_not_started: rows.filter((r) => r.fields?.["Enrichment Status"] === "Not Started")
      .length,
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
    renovation_filled: rows.filter((r) => Boolean(r.fields?.["Renovation / Conversion Date"]))
      .length,
    affiliation_start_filled: rows.filter((r) => Boolean(r.fields?.["Affiliation Start Date"]))
      .length,
    zero_zero: rows.filter((r) => r.fields?.Latitude === 0 && r.fields?.Longitude === 0).length,
    renames_present: {
      "Last Reviewed Date": names.has("Last Reviewed Date"),
      "Resort / Leisure Flag": names.has("Resort / Leisure Flag"),
      "Extended Stay Flag": names.has("Extended Stay Flag"),
    },
    old_names_absent: {
      "Last Verified Date": !names.has("Last Verified Date"),
      "Resort Amenities Flag": !names.has("Resort Amenities Flag"),
      "Extended Stay Amenity Flag": !names.has("Extended Stay Amenity Flag"),
    },
  };

  validation.pass =
    validation.record_count === 666 &&
    validation.duplicates === 0 &&
    validation.radar_fields_missing.length === 0 &&
    validation.field_count === expectedFieldCount &&
    validation.enrichment_not_started === 666 &&
    validation.human_review_true === 4 &&
    validation.production_use_ok === 666 &&
    validation.description_filled === 0 &&
    validation.amenities_filled === 0 &&
    validation.owner_filled === 0 &&
    validation.operator_filled === 0 &&
    validation.rooms_filled === 0 &&
    validation.opening_filled === 0 &&
    validation.renovation_filled === 0 &&
    validation.affiliation_start_filled === 0 &&
    validation.zero_zero === 0 &&
    Object.values(validation.renames_present).every(Boolean) &&
    Object.values(validation.old_names_absent).every(Boolean) &&
    Object.values(radarPopulated).every((n) => n === 0);

  return validation;
}

export async function runV112DryRun() {
  const token = resolvePat();
  const bases = resolveTargetBase();
  const env = checkV112EnvFlags();
  const specs = buildV112RadarFieldSpecs();

  const tables = await listTables(bases.target_base_id, token);
  const census = tables.find((t) => t.name === CENSUS_TABLE || t.id === CENSUS_TABLE_ID);
  if (!census) {
    return {
      version: V112_VERSION,
      generated_at: new Date().toISOString(),
      mode: "dry-run",
      status: STATUS.BLOCKED,
      blocked_reason: "Hotel Property Census not found",
    };
  }

  const existingNames = new Set((census.fields || []).map((f) => f.name));
  const toAdd = [];
  const alreadyExist = [];
  for (const spec of specs) {
    if (existingNames.has(spec.name)) alreadyExist.push(spec.name);
    else toAdd.push(spec);
  }

  const rows = await listAllRecords(bases.target_base_id, token, census.id, [
    "Property Identity Key",
    "Enrichment Status",
    "Human Review Required",
    "Production Use Status",
  ]);

  const dryPass =
    rows.length === 666 &&
    toAdd.length + alreadyExist.length === 6 &&
    (alreadyExist.length === 0 || toAdd.length > 0 || alreadyExist.length === 6);

  // Conflict only if somehow wrong — missing table already handled
  const conflicts = [];
  if (rows.length !== 666) conflicts.push({ code: "unexpected_record_count", count: rows.length });

  return {
    version: V112_VERSION,
    generated_at: new Date().toISOString(),
    mode: "dry-run",
    status: conflicts.length ? STATUS.DRY_RUN_FAIL : STATUS.DRY_RUN_PASS,
    dry_run_pass: conflicts.length === 0 && toAdd.length === 6 && alreadyExist.length === 0,
    base_id_masked: mask(bases.target_base_id),
    table_id: census.id,
    field_count_before: census.fields.length,
    expected_field_count_after: census.fields.length + toAdd.length,
    fields_before: {
      radar_present: alreadyExist,
      radar_missing: toAdd.map((s) => s.name),
    },
    fields_to_add: toAdd.map((s) => ({ name: s.name, type: s.type, options: s.options?.choices?.map((c) => c.name) || null })),
    fields_already_existed: alreadyExist,
    conflicts,
    census_record_count: rows.length,
    env_ok_for_apply: env.allOk,
    env_flags: env.flags,
  };
}

export async function runV112Apply(argv = process.argv.slice(2)) {
  const args = parseV112Args(argv);
  const env = checkV112EnvFlags();

  if (!args.apply) return runV112DryRun();

  if (!env.allOk || !allV112ConfirmsPresent(args)) {
    return {
      version: V112_VERSION,
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
  const specs = buildV112RadarFieldSpecs();
  const started = Date.now();

  const tablesBefore = await listTables(bases.target_base_id, token);
  const censusBefore = tablesBefore.find((t) => t.name === CENSUS_TABLE || t.id === CENSUS_TABLE_ID);
  if (!censusBefore) {
    return {
      version: V112_VERSION,
      generated_at: new Date().toISOString(),
      mode: "apply",
      apply_executed: false,
      status: STATUS.BLOCKED,
      blocked_reason: "Hotel Property Census not found",
    };
  }

  const existingNames = new Set((censusBefore.fields || []).map((f) => f.name));
  const fieldsFoundBefore = specs.filter((s) => existingNames.has(s.name)).map((s) => s.name);
  const fieldsMissingBefore = specs.filter((s) => !existingNames.has(s.name)).map((s) => s.name);

  if (fieldsMissingBefore.length === 0) {
    return {
      version: V112_VERSION,
      generated_at: new Date().toISOString(),
      mode: "apply",
      apply_executed: false,
      status: STATUS.BLOCKED,
      blocked_reason: "All 6 Radar/public fields already exist — nothing to create",
      fields_found_before: fieldsFoundBefore,
      fields_already_existing: fieldsFoundBefore,
      field_count_before: censusBefore.fields.length,
    };
  }

  const fieldsAdded = [];
  const fieldsSkipped = [];
  const fieldErrors = [];

  for (const spec of specs) {
    if (existingNames.has(spec.name)) {
      fieldsSkipped.push({ name: spec.name, reason: "already_exists" });
      continue;
    }
    const body = {
      name: spec.name,
      type: spec.type,
      ...(spec.description ? { description: spec.description } : {}),
      ...(spec.options ? { options: spec.options } : {}),
    };
    let attempt = 0;
    let ok = false;
    while (attempt < 5 && !ok) {
      attempt += 1;
      const { res, json } = await metaFetch(
        bases.target_base_id,
        token,
        `/tables/${encodeURIComponent(censusBefore.id)}/fields`,
        { method: "POST", body: JSON.stringify(body) }
      );
      if (res.status === 429) {
        await sleep(1000 * attempt);
        continue;
      }
      if (!res.ok) {
        fieldErrors.push({ name: spec.name, status: res.status, error: json.error || json });
        break;
      }
      fieldsAdded.push({
        name: spec.name,
        type: spec.type,
        id: json.id,
        options: spec.options?.choices?.map((c) => c.name) || null,
      });
      existingNames.add(spec.name);
      ok = true;
    }
    await sleep(250);
  }

  const expectedFieldCount = censusBefore.fields.length + fieldsAdded.length;
  const validation = await validateCensusState(
    bases.target_base_id,
    token,
    censusBefore.id,
    expectedFieldCount
  );

  let status = STATUS.APPLIED;
  if (fieldErrors.length > 0 || fieldsAdded.length !== fieldsMissingBefore.length) {
    status = STATUS.PARTIAL;
  }
  if (!validation.pass && fieldsAdded.length === 0) {
    status = STATUS.BLOCKED;
  }
  if (fieldsAdded.length === 6 && validation.pass) {
    status = STATUS.APPLIED;
  } else if (fieldsAdded.length > 0 && fieldsAdded.length < 6) {
    status = STATUS.PARTIAL;
  } else if (fieldsAdded.length === 0 && fieldErrors.length) {
    status = STATUS.BLOCKED;
  }

  return {
    version: V112_VERSION,
    generated_at: new Date().toISOString(),
    mode: "apply",
    apply_executed: true,
    status,
    duration_ms: Date.now() - started,
    token_masked: mask(token),
    base_id_masked: mask(bases.target_base_id),
    table_id: censusBefore.id,
    field_count_before: censusBefore.fields.length,
    field_count_after: validation.field_count,
    fields_found_before: {
      radar_present: fieldsFoundBefore,
      radar_missing: fieldsMissingBefore,
    },
    fields_created: fieldsAdded,
    fields_already_existing: fieldsSkipped,
    field_errors: fieldErrors,
    validation,
    validation_pass: validation.pass,
    radar_public_readiness_classification_can_run_next:
      status === STATUS.APPLIED && validation.pass,
    next_recommended_step:
      status === STATUS.APPLIED && validation.pass
        ? "Run Radar/public readiness classification (separate task) or proceed with first enrichment lane under the frozen v1.1.1 contract."
        : "Review field_errors / validation failures before continuing.",
  };
}

export function renderV112DryRunMarkdown(r) {
  return [
    `# Production Census Schema v1.1.2 Radar Fields — Dry Run`,
    ``,
    `**Status:** \`${r.status}\``,
    `**Generated:** ${r.generated_at}`,
    ``,
    `- Field count before: ${r.field_count_before}`,
    `- Expected after: ${r.expected_field_count_after}`,
    `- To add: ${r.fields_to_add?.length}`,
    `- Already exist: ${r.fields_already_existed?.length}`,
    `- Census records: ${r.census_record_count}`,
    ``,
    `## Fields to add`,
    ``,
    "```json",
    JSON.stringify(r.fields_to_add || [], null, 2),
    "```",
    ``,
  ].join("\n");
}

export function renderV112ApplyMarkdown(r) {
  return [
    `# Production Census Schema v1.1.2 — Radar/Public Display Fields Apply`,
    ``,
    `**Status:** \`${r.status}\``,
    `**Mode:** ${r.mode}`,
    `**Generated:** ${r.generated_at}`,
    ``,
    `## 1. Executive summary`,
    ``,
    `- Apply executed: ${r.apply_executed}`,
    `- Fields created: ${(r.fields_created || []).length}`,
    `- Fields already existing: ${(r.fields_already_existing || []).length}`,
    `- Field count: ${r.field_count_before} → ${r.field_count_after}`,
    `- Validation pass: ${r.validation_pass}`,
    `- Ready for Radar readiness classification: ${r.radar_public_readiness_classification_can_run_next}`,
    ``,
    `## 2. Fields found before apply`,
    ``,
    "```json",
    JSON.stringify(r.fields_found_before || {}, null, 2),
    "```",
    ``,
    `## 3. Fields created`,
    ``,
    "```json",
    JSON.stringify(r.fields_created || [], null, 2),
    "```",
    ``,
    `## 4. Fields already existing`,
    ``,
    "```json",
    JSON.stringify(r.fields_already_existing || [], null, 2),
    "```",
    ``,
    `## 5. Final field count`,
    ``,
    `- Before: ${r.field_count_before}`,
    `- After: ${r.field_count_after}`,
    ``,
    `## 6. Census validation`,
    ``,
    "```json",
    JSON.stringify(r.validation || {}, null, 2),
    "```",
    ``,
    `## 7. Brand Explorer safety result`,
    ``,
    "```json",
    JSON.stringify(r.brand_explorer_safety || { pending: true }, null, 2),
    "```",
    ``,
    `## 8. Whether Radar/public readiness classification can run next`,
    ``,
    String(r.radar_public_readiness_classification_can_run_next),
    ``,
    `## Next`,
    ``,
    r.next_recommended_step || "",
    ``,
    r.field_errors?.length
      ? `## Field errors\n\n\`\`\`json\n${JSON.stringify(r.field_errors, null, 2)}\n\`\`\`\n`
      : "",
  ].join("\n");
}
