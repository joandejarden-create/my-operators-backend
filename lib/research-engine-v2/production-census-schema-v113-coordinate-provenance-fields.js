/**
 * Production Census schema v1.1.3 — address + coordinate provenance fields.
 * Schema create only. No record writes. No Brand Explorer writes.
 */

import { resolvePat, resolveTargetBase } from "./production-census-schema-create.js";
import { TABLE_IDS } from "./production-census-write.js";

export const V113_VERSION = "production-census-schema-v113-coordinate-provenance-v1";
export const CENSUS_TABLE = "Hotel Property Census";
export const CENSUS_TABLE_ID = TABLE_IDS["Hotel Property Census"];
export const EXPECTED_RECORD_COUNT = 666;
export const EXPECTED_FIELD_COUNT_BEFORE = 101;
export const EXPECTED_FIELD_COUNT_AFTER_ALL_SEVEN = 108;

export const STATUS = Object.freeze({
  CONFIRMATION_MISSING: "production_census_schema_v113_confirmation_missing",
  DRY_RUN_PASS: "production_census_schema_v113_dry_run_pass",
  DRY_RUN_FAIL: "production_census_schema_v113_dry_run_fail",
  APPLIED: "production_census_schema_v113_coordinate_provenance_added_ready_for_provider_decision",
  PARTIAL: "production_census_schema_v113_partial_fields_added_needs_review",
  BLOCKED: "production_census_schema_v113_blocked_by_schema_conflict",
});

const ADDRESS_CONFIDENCE = ["High", "Medium", "Low", "Hold"];
const COORDINATE_SOURCE_TYPE = [
  "official_coordinates",
  "official_address_geocode",
  "existing_source",
  "structured_data_extraction",
  "embedded_map_extraction",
  "blocked_low_confidence",
  "blocked_no_official_address",
  "steward_review",
];
const COORDINATE_CONFIDENCE = ["High", "Medium", "Low", "Hold"];
const GEOCODE_PROVIDER = [
  "Mapbox",
  "Google",
  "Official Page",
  "Existing Source",
  "Manual Review",
  "None",
];
const GEOCODE_METHOD = [
  "official_coordinates",
  "official_address_geocode",
  "structured_data_extraction",
  "embedded_map_extraction",
  "manual_review",
  "none",
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
function urlField(name, description) {
  return { name, type: "url", description };
}
function dateField(name, description) {
  return { name, type: "date", description, options: { dateFormat: { name: "iso" } } };
}

export function buildV113ProvenanceFieldSpecs() {
  return [
    singleSelect(
      "Address Confidence",
      ADDRESS_CONFIDENCE,
      "Confidence in official street address — leave blank until address/geocode lane writes"
    ),
    urlField(
      "Address Source URL",
      "Official URL that supplied the street address — leave blank until address lane writes"
    ),
    singleSelect(
      "Coordinate Source Type",
      COORDINATE_SOURCE_TYPE,
      "How Latitude/Longitude were obtained — leave blank until coordinate lane writes"
    ),
    singleSelect(
      "Coordinate Confidence",
      COORDINATE_CONFIDENCE,
      "Confidence in property-level coordinates — leave blank until coordinate lane writes"
    ),
    singleSelect(
      "Geocode Provider",
      GEOCODE_PROVIDER,
      "Approved geocode/source provider — leave blank until coordinate lane writes"
    ),
    singleSelect(
      "Geocode Method",
      GEOCODE_METHOD,
      "Method used to resolve coordinates — leave blank until coordinate lane writes"
    ),
    dateField(
      "Geocode Reviewed Date",
      "Date coordinates/address provenance were last reviewed — leave blank until lane writes"
    ),
  ];
}

export const V113_FIELD_NAMES = Object.freeze(
  buildV113ProvenanceFieldSpecs().map((s) => s.name)
);

export function checkV113EnvFlags() {
  const flags = {
    ALLOW_PRODUCTION_CENSUS_SCHEMA_V113: process.env.ALLOW_PRODUCTION_CENSUS_SCHEMA_V113 === "1",
    CONFIRM_SCHEMA_ONLY_NO_RECORD_WRITES:
      process.env.CONFIRM_SCHEMA_ONLY_NO_RECORD_WRITES === "1",
    CONFIRM_NO_BRAND_EXPLORER_WRITES: process.env.CONFIRM_NO_BRAND_EXPLORER_WRITES === "1",
  };
  return { allOk: Object.values(flags).every(Boolean), flags };
}

export function parseV113Args(argv = process.argv.slice(2)) {
  const flags = new Set(argv.filter((a) => a.startsWith("--")));
  const addProvenanceOnly = flags.has("--confirm-add-coordinate-provenance-fields-only");
  const noRecordWrites = flags.has("--confirm-no-record-writes");
  const noBe = flags.has("--confirm-no-brand-explorer-writes");
  const noFieldDeletes = flags.has("--confirm-no-field-deletes");
  const noFieldRenames = flags.has("--confirm-no-field-renames");
  const noPopulate = flags.has("--confirm-no-field-population");
  const taskContractOk =
    addProvenanceOnly &&
    noRecordWrites &&
    noBe &&
    noFieldDeletes &&
    noFieldRenames &&
    noPopulate;
  return {
    dryRun: flags.has("--dry-run") || !flags.has("--apply"),
    apply: flags.has("--apply"),
    skipBeGates: flags.has("--skip-be-gates"),
    confirms: {
      addProvenanceOnly,
      noRecordWrites,
      noBe,
      noFieldDeletes,
      noFieldRenames,
      noPopulate,
      taskContractOk,
    },
  };
}

export function allV113ConfirmsPresent(args) {
  return Boolean(args?.confirms?.taskContractOk);
}

function mask(id) {
  if (!id || id.length < 10) return id ? "***" : null;
  return `${id.slice(0, 6)}…${id.slice(-4)}`;
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function isBlank(v) {
  return v == null || v === "" || (typeof v === "string" && !v.trim());
}
function isValidCoordPair(lat, lng) {
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) return false;
  if (lat === 0 && lng === 0) return false;
  if (lat < -90 || lat > 90 || lng < -180 || lng > 180) return false;
  return true;
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

/**
 * Post–first-pass aware validation: enrichment may already be Partial; provenance must stay blank.
 */
export async function validateCensusStateV113(baseId, token, tableId, expectedFieldCount) {
  const tables = await listTables(baseId, token);
  const census = tables.find((t) => t.id === tableId || t.name === CENSUS_TABLE);
  const names = (census?.fields || []).map((f) => f.name);
  const nameSet = new Set(names);
  const provenancePresent = V113_FIELD_NAMES.filter((n) => nameSet.has(n));
  const provenanceMissing = V113_FIELD_NAMES.filter((n) => !nameSet.has(n));

  // Detect duplicate field names on table meta
  const nameCounts = new Map();
  for (const n of names) nameCounts.set(n, (nameCounts.get(n) || 0) + 1);
  const duplicateFieldNames = [...nameCounts.entries()]
    .filter(([, c]) => c > 1)
    .map(([n]) => n);

  const fieldList = [
    "Property Identity Key",
    "Human Review Required",
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
    "Address",
    ...provenancePresent,
  ];

  const rows = await listAllRecords(baseId, token, tableId, fieldList);

  const keyCounts = new Map();
  for (const r of rows) {
    const k = r.fields?.["Property Identity Key"];
    keyCounts.set(k, (keyCounts.get(k) || 0) + 1);
  }

  const provenancePopulated = {};
  for (const n of provenancePresent) {
    provenancePopulated[n] = rows.filter((r) => !isBlank(r.fields?.[n])).length;
  }

  const coordsFilled = rows.filter((r) =>
    isValidCoordPair(Number(r.fields?.Latitude), Number(r.fields?.Longitude))
  ).length;

  const validation = {
    record_count: rows.length,
    field_count: census?.fields?.length ?? 0,
    expected_field_count: expectedFieldCount,
    duplicate_identity_keys: [...keyCounts.values()].filter((n) => n > 1).length,
    duplicate_field_names: duplicateFieldNames,
    provenance_fields_present: provenancePresent,
    provenance_fields_missing: provenanceMissing,
    provenance_populated_counts: provenancePopulated,
    provenance_any_populated: Object.values(provenancePopulated).some((n) => n > 0),
    human_review_true: rows.filter((r) => r.fields?.["Human Review Required"] === true).length,
    coords_filled: coordsFilled,
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
  };

  validation.pass =
    validation.record_count === EXPECTED_RECORD_COUNT &&
    validation.duplicate_identity_keys === 0 &&
    validation.duplicate_field_names.length === 0 &&
    validation.provenance_fields_missing.length === 0 &&
    validation.field_count === expectedFieldCount &&
    validation.human_review_true === 4 &&
    validation.provenance_any_populated === false &&
    validation.owner_filled === 0 &&
    validation.operator_filled === 0 &&
    validation.rooms_filled === 0 &&
    validation.opening_filled === 0 &&
    validation.renovation_filled === 0 &&
    validation.affiliation_start_filled === 0 &&
    validation.zero_zero === 0 &&
    // First-pass already wrote coords/amenities; descriptions remain 0
    validation.coords_filled === 132 &&
    validation.description_filled === 0;

  return validation;
}

export async function runV113DryRun() {
  const token = resolvePat();
  const bases = resolveTargetBase();
  const env = checkV113EnvFlags();
  const specs = buildV113ProvenanceFieldSpecs();

  const tables = await listTables(bases.target_base_id, token);
  const census = tables.find((t) => t.name === CENSUS_TABLE || t.id === CENSUS_TABLE_ID);
  if (!census) {
    return {
      version: V113_VERSION,
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
    "Human Review Required",
  ]);

  const conflicts = [];
  if (rows.length !== EXPECTED_RECORD_COUNT) {
    conflicts.push({ code: "unexpected_record_count", count: rows.length });
  }
  if (census.fields.length !== EXPECTED_FIELD_COUNT_BEFORE && alreadyExist.length === 0) {
    conflicts.push({
      code: "unexpected_field_count_before",
      count: census.fields.length,
      expected: EXPECTED_FIELD_COUNT_BEFORE,
      note: "Proceed if intentional; report for founder review",
    });
  }

  const dryPass =
    conflicts.filter((c) => c.code === "unexpected_record_count").length === 0 &&
    toAdd.length + alreadyExist.length === 7;

  return {
    version: V113_VERSION,
    generated_at: new Date().toISOString(),
    mode: "dry-run",
    status: dryPass ? STATUS.DRY_RUN_PASS : STATUS.DRY_RUN_FAIL,
    dry_run_pass: dryPass && toAdd.length === 7 && alreadyExist.length === 0,
    base_id_masked: mask(bases.target_base_id),
    table_id: census.id,
    field_count_before: census.fields.length,
    expected_field_count_after: census.fields.length + toAdd.length,
    fields_to_add: toAdd.map((s) => ({
      name: s.name,
      type: s.type,
      options: s.options?.choices?.map((c) => c.name) || null,
    })),
    fields_already_existed: alreadyExist,
    conflicts,
    census_record_count: rows.length,
    human_review_true: rows.filter((r) => r.fields?.["Human Review Required"] === true).length,
    env_ok_for_apply: env.allOk,
    env_flags: env.flags,
  };
}

export async function runV113Apply(argv = process.argv.slice(2)) {
  const args = parseV113Args(argv);
  const env = checkV113EnvFlags();

  if (!args.apply) return runV113DryRun();

  if (!env.allOk || !allV113ConfirmsPresent(args)) {
    return {
      version: V113_VERSION,
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
  const specs = buildV113ProvenanceFieldSpecs();
  const started = Date.now();

  const tablesBefore = await listTables(bases.target_base_id, token);
  const censusBefore = tablesBefore.find((t) => t.name === CENSUS_TABLE || t.id === CENSUS_TABLE_ID);
  if (!censusBefore) {
    return {
      version: V113_VERSION,
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
    const validation = await validateCensusStateV113(
      bases.target_base_id,
      token,
      censusBefore.id,
      censusBefore.fields.length
    );
    return {
      version: V113_VERSION,
      generated_at: new Date().toISOString(),
      mode: "apply",
      apply_executed: false,
      status: STATUS.BLOCKED,
      blocked_reason: "All 7 provenance fields already exist — nothing to create (no duplicates)",
      fields_already_existing: fieldsFoundBefore,
      field_count_before: censusBefore.fields.length,
      field_count_after: validation.field_count,
      validation,
      validation_pass: validation.pass,
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
  const validation = await validateCensusStateV113(
    bases.target_base_id,
    token,
    censusBefore.id,
    expectedFieldCount
  );

  let status = STATUS.PARTIAL;
  if (
    fieldsAdded.length === fieldsMissingBefore.length &&
    fieldErrors.length === 0 &&
    validation.pass &&
    fieldsAdded.length === 7
  ) {
    status = STATUS.APPLIED;
  } else if (fieldsAdded.length === 0 && fieldErrors.length) {
    status = STATUS.BLOCKED;
  } else if (fieldsAdded.length > 0 && fieldsAdded.length < 7) {
    status = STATUS.PARTIAL;
  } else if (fieldsAdded.length === 7 && !validation.pass) {
    status = STATUS.PARTIAL;
  } else if (fieldsAdded.length === 7 && validation.pass) {
    status = STATUS.APPLIED;
  }

  return {
    version: V113_VERSION,
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
      provenance_present: fieldsFoundBefore,
      provenance_missing: fieldsMissingBefore,
    },
    fields_created: fieldsAdded,
    fields_already_existing: fieldsSkipped,
    field_errors: fieldErrors,
    validation,
    validation_pass: validation.pass,
    next_recommended_step:
      status === STATUS.APPLIED && validation.pass
        ? "Founder provider/storage decision (Mapbox Permanent recommended) → re-run address-geocode dry-run → approved apply with provenance writes."
        : "Review field_errors / validation failures before continuing.",
  };
}

export function renderV113DryRunMarkdown(r) {
  return `# Production Census Schema v1.1.3 — Provenance Fields Dry Run

**Status:** \`${r.status}\`  
**Generated:** ${r.generated_at}

- Field count before: **${r.field_count_before}**
- Expected after: **${r.expected_field_count_after}**
- To add: **${r.fields_to_add?.length}**
- Already exist: **${r.fields_already_existed?.length}**
- Census records: **${r.census_record_count}**

## Fields to add

\`\`\`json
${JSON.stringify(r.fields_to_add || [], null, 2)}
\`\`\`

## Already existing

\`\`\`json
${JSON.stringify(r.fields_already_existed || [], null, 2)}
\`\`\`
`;
}

export function renderV113ApplyMarkdown(r) {
  return `# Production Census Schema v1.1.3 — Address & Coordinate Provenance Fields

**Status:** \`${r.status}\`  
**Mode:** ${r.mode}  
**Generated:** ${r.generated_at}  
**Apply executed:** ${r.apply_executed}

## 1. Executive summary

- Fields created: **${(r.fields_created || []).length}**
- Fields already existing: **${(r.fields_already_existing || []).length}**
- Field count: **${r.field_count_before} → ${r.field_count_after}**
- Census records: **${r.validation?.record_count ?? "—"}**
- Validation pass: **${r.validation_pass}**
- Provenance populated: **${r.validation?.provenance_any_populated ? "YES (unexpected)" : "no (expected)"}**

## 2. Fields created

\`\`\`json
${JSON.stringify(r.fields_created || [], null, 2)}
\`\`\`

## 3. Fields already existing

\`\`\`json
${JSON.stringify(r.fields_already_existing || [], null, 2)}
\`\`\`

## 4. Final Census field count

- Before: ${r.field_count_before}
- After: ${r.field_count_after}
- Expected if all 7 created from 101: ${EXPECTED_FIELD_COUNT_AFTER_ALL_SEVEN}

## 5. Census validation

\`\`\`json
${JSON.stringify(r.validation || {}, null, 2)}
\`\`\`

## 6. Brand Explorer safety result

\`\`\`json
${JSON.stringify(r.brand_explorer_safety || { pending: true }, null, 2)}
\`\`\`

## 7. Learning ledger update

\`\`\`json
${JSON.stringify(r.learning_ledger_update || { pending: true }, null, 2)}
\`\`\`

## 8. Recommended next step

${r.next_recommended_step || ""}

${
  r.field_errors?.length
    ? `## Field errors\n\n\`\`\`json\n${JSON.stringify(r.field_errors, null, 2)}\n\`\`\`\n`
    : ""
}
`;
}
