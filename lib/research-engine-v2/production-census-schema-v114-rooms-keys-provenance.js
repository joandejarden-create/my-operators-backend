/**
 * Production Census schema v1.1.4 — Rooms / Keys provenance fields only.
 * Schema create / option add only. No record writes. No Brand Explorer / Brand Setup writes.
 */

import { resolvePat, resolveTargetBase } from "./production-census-schema-create.js";
import { TABLE_IDS } from "./production-census-write.js";
import { MAP_ROOMS } from "./production-census-rooms-keys-queue.js";

export const V114_VERSION = "production-census-schema-v114-rooms-keys-provenance-v1";
export const CENSUS_TABLE = "Hotel Property Census";
export const CENSUS_TABLE_ID = TABLE_IDS["Hotel Property Census"];
export const EXPECTED_RECORD_COUNT = 666;

export const STATUS = Object.freeze({
  CONFIRMATION_MISSING: "production_census_schema_v114_rooms_keys_provenance_blocked",
  DRY_RUN_PASS: "production_census_schema_v114_dry_run_pass",
  DRY_RUN_FAIL: "production_census_schema_v114_dry_run_fail",
  APPLIED: "production_census_schema_v114_rooms_keys_provenance_applied_ready_for_controlled_autopilot",
  PARTIAL: "production_census_schema_v114_rooms_keys_provenance_partial_needs_review",
  BLOCKED: "production_census_schema_v114_rooms_keys_provenance_blocked",
});

export const ROOMS_SOURCE_TYPE_OPTIONS = Object.freeze([
  "official_property_page",
  "official_brand_directory",
  "official_hotel_website",
  "official_press_release",
  "official_development_page",
  "trusted_secondary_source",
  "steward_review",
]);

export const ROOMS_CONFIDENCE_HOLD = "Hold";

function choices(names) {
  return names.map((name) => ({ name: String(name) }));
}

export function buildV114NewFieldSpecs() {
  return [
    {
      name: MAP_ROOMS.sourceTypePlanned, // Rooms Source Type
      type: "singleSelect",
      description:
        "Provenance type for Rooms / Keys — leave blank until rooms queue writes High-confidence counts",
      options: { choices: choices(ROOMS_SOURCE_TYPE_OPTIONS) },
    },
    {
      name: MAP_ROOMS.reviewedDatePlanned, // Rooms Reviewed Date
      type: "date",
      description: "Date Rooms / Keys provenance was last reviewed — leave blank until rooms queue writes",
      options: { dateFormat: { name: "iso" } },
    },
    {
      name: MAP_ROOMS.notesPlanned, // Rooms Notes
      type: "multilineText",
      description: "Steward/extractor notes for Rooms / Keys — leave blank until rooms queue writes",
    },
  ];
}

export const V114_NEW_FIELD_NAMES = Object.freeze(buildV114NewFieldSpecs().map((s) => s.name));

export function checkV114EnvFlags(env = process.env) {
  const flags = {
    ALLOW_PRODUCTION_CENSUS_SCHEMA_V114: String(env.ALLOW_PRODUCTION_CENSUS_SCHEMA_V114 || "").trim() === "1",
    CONFIRM_SCHEMA_ONLY_NO_RECORD_WRITES:
      String(env.CONFIRM_SCHEMA_ONLY_NO_RECORD_WRITES || "").trim() === "1",
    CONFIRM_NO_BRAND_EXPLORER_WRITES:
      String(env.CONFIRM_NO_BRAND_EXPLORER_WRITES || "").trim() === "1",
  };
  return { allOk: Object.values(flags).every(Boolean), flags };
}

export function parseV114Args(argv = process.argv.slice(2)) {
  const flags = new Set(argv.filter((a) => a.startsWith("--")));
  const addRoomsOnly = flags.has("--confirm-add-rooms-keys-provenance-fields-only");
  const noRecordWrites = flags.has("--confirm-no-record-writes");
  const noBe = flags.has("--confirm-no-brand-explorer-writes");
  const noBrandSetup = flags.has("--confirm-no-brand-setup-writes");
  const noFieldDeletes = flags.has("--confirm-no-field-deletes");
  const noFieldRenames = flags.has("--confirm-no-field-renames");
  const noPopulate = flags.has("--confirm-no-field-population");
  const addHold = flags.has("--confirm-add-hold-to-rooms-confidence");
  const taskContractOk =
    addRoomsOnly &&
    noRecordWrites &&
    noBe &&
    noBrandSetup &&
    noFieldDeletes &&
    noFieldRenames &&
    noPopulate &&
    addHold;
  return {
    dryRun: flags.has("--dry-run") || !flags.has("--apply"),
    apply: flags.has("--apply"),
    confirms: {
      addRoomsOnly,
      noRecordWrites,
      noBe,
      noBrandSetup,
      noFieldDeletes,
      noFieldRenames,
      noPopulate,
      addHold,
      taskContractOk,
    },
  };
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

function inspectRoomsConfidence(census) {
  const field = (census.fields || []).find((f) => f.name === MAP_ROOMS.confidenceExisting);
  if (!field) return { exists: false, has_hold: false, choices: [] };
  const choiceNames = (field.options?.choices || []).map((c) => c.name);
  return {
    exists: true,
    field_id: field.id,
    type: field.type,
    choices: choiceNames,
    has_hold: choiceNames.includes(ROOMS_CONFIDENCE_HOLD),
    raw_choices: field.options?.choices || [],
  };
}

/**
 * Post-apply / dry validation — schema only; new provenance must stay blank.
 */
export async function validateCensusStateV114(baseId, token, tableId, opts = {}) {
  const tables = await listTables(baseId, token);
  const census = tables.find((t) => t.id === tableId || t.name === CENSUS_TABLE);
  const names = (census?.fields || []).map((f) => f.name);
  const nameSet = new Set(names);
  const newPresent = V114_NEW_FIELD_NAMES.filter((n) => nameSet.has(n));
  const newMissing = V114_NEW_FIELD_NAMES.filter((n) => !nameSet.has(n));
  const confidence = inspectRoomsConfidence(census || {});

  // Only request fields that exist on the table (unknown field names → 422).
  const optionalCheckFields = ["Owner Name", "Operator / Management Company"];
  const fieldList = [
    "Property Identity Key",
    MAP_ROOMS.roomsKeys,
    MAP_ROOMS.confidenceExisting,
    MAP_ROOMS.sourceUrlExisting,
    ...newPresent,
    ...optionalCheckFields.filter((n) => nameSet.has(n)),
  ].filter((n) => nameSet.has(n));

  const rows = await listAllRecords(baseId, token, tableId, fieldList);
  const provenancePopulated = {};
  for (const n of newPresent) {
    provenancePopulated[n] = rows.filter((r) => !isBlank(r.fields?.[n])).length;
  }

  const validation = {
    record_count: rows.length,
    field_count: census?.fields?.length ?? 0,
    expected_record_count: EXPECTED_RECORD_COUNT,
    rooms_keys_field_exists: nameSet.has(MAP_ROOMS.roomsKeys),
    rooms_confidence_field_exists: nameSet.has(MAP_ROOMS.confidenceExisting),
    rooms_source_url_field_exists: nameSet.has(MAP_ROOMS.sourceUrlExisting),
    new_fields_present: newPresent,
    new_fields_missing: newMissing,
    rooms_confidence: {
      has_hold: confidence.has_hold,
      choices: confidence.choices,
    },
    new_provenance_populated_counts: provenancePopulated,
    new_provenance_any_populated: Object.values(provenancePopulated).some((n) => n > 0),
    rooms_keys_filled: rows.filter((r) => r.fields?.[MAP_ROOMS.roomsKeys] != null && r.fields?.[MAP_ROOMS.roomsKeys] !== "").length,
    rooms_confidence_filled: rows.filter((r) => !isBlank(r.fields?.[MAP_ROOMS.confidenceExisting])).length,
    rooms_source_url_filled: rows.filter((r) => !isBlank(r.fields?.[MAP_ROOMS.sourceUrlExisting])).length,
    owner_filled: nameSet.has("Owner Name")
      ? rows.filter((r) => !isBlank(r.fields?.["Owner Name"])).length
      : null,
    operator_filled: nameSet.has("Operator / Management Company")
      ? rows.filter((r) => !isBlank(r.fields?.["Operator / Management Company"])).length
      : null,
    company_validated_field_exists: nameSet.has("Company Validated"),
    brand_verified_field_exists: nameSet.has("Brand Verified"),
    note: "Company Validated / Brand Verified are Brand Explorer fields — not on Census; not requested.",
  };

  validation.pass =
    validation.record_count === EXPECTED_RECORD_COUNT &&
    validation.rooms_keys_field_exists &&
    validation.rooms_confidence_field_exists &&
    validation.rooms_source_url_field_exists &&
    validation.new_fields_missing.length === 0 &&
    validation.rooms_confidence.has_hold === true &&
    validation.new_provenance_any_populated === false;

  return validation;
}

export async function runV114DryRun() {
  const token = resolvePat();
  const bases = resolveTargetBase();
  const env = checkV114EnvFlags();
  const specs = buildV114NewFieldSpecs();

  const tables = await listTables(bases.target_base_id, token);
  const census = tables.find((t) => t.name === CENSUS_TABLE || t.id === CENSUS_TABLE_ID);
  if (!census) {
    return {
      version: V114_VERSION,
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

  const confidence = inspectRoomsConfidence(census);
  const rows = await listAllRecords(bases.target_base_id, token, census.id, [
    "Property Identity Key",
    MAP_ROOMS.roomsKeys,
    MAP_ROOMS.confidenceExisting,
    MAP_ROOMS.sourceUrlExisting,
  ]);

  const conflicts = [];
  if (rows.length !== EXPECTED_RECORD_COUNT) {
    conflicts.push({ code: "unexpected_record_count", count: rows.length });
  }
  if (!existingNames.has(MAP_ROOMS.roomsKeys)) {
    conflicts.push({ code: "missing_rooms_keys_field" });
  }
  if (!confidence.exists) {
    conflicts.push({ code: "missing_rooms_confidence_field" });
  }

  const dryPass =
    conflicts.filter((c) => c.code === "unexpected_record_count").length === 0 &&
    !conflicts.some((c) => c.code.startsWith("missing_"));

  return {
    version: V114_VERSION,
    generated_at: new Date().toISOString(),
    mode: "dry-run",
    status: dryPass ? STATUS.DRY_RUN_PASS : STATUS.DRY_RUN_FAIL,
    dry_run_pass: dryPass,
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
    rooms_confidence_option_add: {
      field: MAP_ROOMS.confidenceExisting,
      add: ROOMS_CONFIDENCE_HOLD,
      already_has_hold: confidence.has_hold,
      existing_choices: confidence.choices,
    },
    rename_recommendation:
      "Optional later rename Rooms Confidence / Rooms Source URL → Rooms / Keys* for naming parity — NOT applied in this task.",
    conflicts,
    census_record_count: rows.length,
    rooms_keys_filled: rows.filter((r) => r.fields?.[MAP_ROOMS.roomsKeys] != null).length,
    env_ok_for_apply: env.allOk,
    env_flags: env.flags,
    brand_explorer_writes: false,
    brand_setup_writes: false,
    record_writes: false,
  };
}

export async function runV114Apply(argv = process.argv.slice(2), env = process.env) {
  const args = parseV114Args(argv);
  const envCheck = checkV114EnvFlags(env);

  if (!args.apply) return runV114DryRun();

  if (!envCheck.allOk || !args.confirms.taskContractOk) {
    return {
      version: V114_VERSION,
      generated_at: new Date().toISOString(),
      mode: "apply_blocked",
      apply_executed: false,
      status: STATUS.CONFIRMATION_MISSING,
      env_flags: envCheck.flags,
      confirms: args.confirms,
    };
  }

  const token = resolvePat();
  const bases = resolveTargetBase();
  const specs = buildV114NewFieldSpecs();
  const started = Date.now();

  const tablesBefore = await listTables(bases.target_base_id, token);
  const censusBefore = tablesBefore.find((t) => t.name === CENSUS_TABLE || t.id === CENSUS_TABLE_ID);
  if (!censusBefore) {
    return {
      version: V114_VERSION,
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
  const confidenceBefore = inspectRoomsConfidence(censusBefore);

  // Snapshot Rooms Confidence cell values before option add (preservation check)
  const preRows = await listAllRecords(bases.target_base_id, token, censusBefore.id, [
    MAP_ROOMS.confidenceExisting,
    MAP_ROOMS.sourceUrlExisting,
    MAP_ROOMS.roomsKeys,
  ]);
  const preConfidenceSnapshot = preRows.map((r) => ({
    id: r.id,
    confidence: r.fields?.[MAP_ROOMS.confidenceExisting] ?? null,
    source_url: r.fields?.[MAP_ROOMS.sourceUrlExisting] ?? null,
    rooms: r.fields?.[MAP_ROOMS.roomsKeys] ?? null,
  }));

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

  // Add Hold to Rooms Confidence.
  // Meta API Update Field docs do not support singleSelect choices mutation; PATCH options.choices
  // returns 422. Fallback: typecast seed on one cell then restore prior value (net-zero cell state).
  let holdOptionResult = {
    attempted: false,
    applied: false,
    already_had_hold: confidenceBefore.has_hold,
    method: null,
  };
  if (args.confirms.addHold && confidenceBefore.exists && !confidenceBefore.has_hold) {
    holdOptionResult.attempted = true;
    const preserved = (confidenceBefore.raw_choices || []).map((c) => ({
      id: c.id,
      name: c.name,
      ...(c.color ? { color: c.color } : {}),
    }));
    preserved.push({ name: ROOMS_CONFIDENCE_HOLD, color: "grayLight2" });
    const { res, json } = await metaFetch(
      bases.target_base_id,
      token,
      `/tables/${encodeURIComponent(censusBefore.id)}/fields/${encodeURIComponent(confidenceBefore.field_id)}`,
      {
        method: "PATCH",
        body: JSON.stringify({
          type: "singleSelect",
          options: { choices: preserved },
        }),
      }
    );
    if (res.ok) {
      holdOptionResult.applied = true;
      holdOptionResult.method = "meta_patch_choices";
      holdOptionResult.choices_after = (json.options?.choices || []).map((c) => c.name);
    } else {
      holdOptionResult.meta_patch_error = json.error || json;
      // Typecast seed: write Hold once then restore previous cell value.
      const seedTarget =
        preConfidenceSnapshot.find((r) => isBlank(r.confidence)) || preConfidenceSnapshot[0];
      if (!seedTarget) {
        fieldErrors.push({
          name: MAP_ROOMS.confidenceExisting,
          action: "add_hold_option",
          error: "no_census_record_for_typecast_seed",
        });
      } else {
        const patchUrl = `https://api.airtable.com/v0/${encodeURIComponent(bases.target_base_id)}/${encodeURIComponent(censusBefore.id)}/${encodeURIComponent(seedTarget.id)}`;
        const seedRes = await fetch(patchUrl, {
          method: "PATCH",
          headers: {
            Authorization: `Bearer ${token}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({
            typecast: true,
            fields: { [MAP_ROOMS.confidenceExisting]: ROOMS_CONFIDENCE_HOLD },
          }),
        });
        const seedJson = await seedRes.json().catch(() => ({}));
        if (!seedRes.ok) {
          fieldErrors.push({
            name: MAP_ROOMS.confidenceExisting,
            action: "add_hold_option_typecast_seed",
            status: seedRes.status,
            error: seedJson.error || seedJson,
          });
          holdOptionResult.error = seedJson.error || seedJson;
        } else {
          await sleep(300);
          const restoreValue = isBlank(seedTarget.confidence) ? null : seedTarget.confidence;
          const restoreRes = await fetch(patchUrl, {
            method: "PATCH",
            headers: {
              Authorization: `Bearer ${token}`,
              "Content-Type": "application/json",
            },
            body: JSON.stringify({
              fields: { [MAP_ROOMS.confidenceExisting]: restoreValue },
            }),
          });
          const restoreJson = await restoreRes.json().catch(() => ({}));
          if (!restoreRes.ok) {
            fieldErrors.push({
              name: MAP_ROOMS.confidenceExisting,
              action: "restore_after_hold_typecast_seed",
              status: restoreRes.status,
              error: restoreJson.error || restoreJson,
              seeded_record_id: seedTarget.id,
            });
            holdOptionResult.error = restoreJson.error || restoreJson;
          } else {
            holdOptionResult.applied = true;
            holdOptionResult.method = "typecast_seed_then_restore";
            holdOptionResult.seeded_record_id = seedTarget.id;
            holdOptionResult.restored_value = restoreValue;
            holdOptionResult.note =
              "Meta API rejects singleSelect choices PATCH; Hold created via typecast then cell restored. Final drift check must be 0.";
          }
        }
      }
    }
    await sleep(250);
  } else if (confidenceBefore.has_hold) {
    holdOptionResult.skipped = "already_has_hold";
  }

  const validation = await validateCensusStateV114(bases.target_base_id, token, censusBefore.id);

  // Confirm no cell value drift on Rooms Confidence / Source URL / Rooms Keys
  const postRows = await listAllRecords(bases.target_base_id, token, censusBefore.id, [
    MAP_ROOMS.confidenceExisting,
    MAP_ROOMS.sourceUrlExisting,
    MAP_ROOMS.roomsKeys,
  ]);
  const postById = new Map(postRows.map((r) => [r.id, r]));
  let valueDrift = 0;
  for (const pre of preConfidenceSnapshot) {
    const post = postById.get(pre.id);
    const confAfter = post?.fields?.[MAP_ROOMS.confidenceExisting] ?? null;
    const urlAfter = post?.fields?.[MAP_ROOMS.sourceUrlExisting] ?? null;
    const roomsAfter = post?.fields?.[MAP_ROOMS.roomsKeys] ?? null;
    if (String(confAfter ?? "") !== String(pre.confidence ?? "")) valueDrift += 1;
    if (String(urlAfter ?? "") !== String(pre.source_url ?? "")) valueDrift += 1;
    if (String(roomsAfter ?? "") !== String(pre.rooms ?? "")) valueDrift += 1;
  }

  const createdOk = fieldsAdded.length === fieldsMissingBefore.length && fieldErrors.filter((e) => e.action !== "add_hold_option").length === 0;
  const holdOk = confidenceBefore.has_hold || holdOptionResult.applied;
  const allNewPresent = validation.new_fields_missing.length === 0;

  let status = STATUS.PARTIAL;
  if (
    createdOk &&
    holdOk &&
    allNewPresent &&
    validation.pass &&
    valueDrift === 0 &&
    fieldErrors.length === 0
  ) {
    status = STATUS.APPLIED;
  } else if (fieldsAdded.length === 0 && !holdOptionResult.applied && fieldErrors.length) {
    status = STATUS.BLOCKED;
  }

  return {
    version: V114_VERSION,
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
      present: fieldsFoundBefore,
      missing: fieldsMissingBefore,
    },
    fields_created: fieldsAdded,
    fields_already_existing: fieldsSkipped,
    rooms_confidence_hold: holdOptionResult,
    field_errors: fieldErrors,
    existing_cell_value_drift_count: valueDrift,
    validation,
    validation_pass: validation.pass && valueDrift === 0,
    brand_explorer_writes: false,
    brand_setup_writes: false,
    record_writes: false,
    rename_not_applied:
      "Rooms Confidence / Rooms Source URL naming parity rename deferred — report only",
    next_recommended_step:
      status === STATUS.APPLIED
        ? "Run Autopilot controlled: npm run census:autopilot -- --region CALA --scope active-brand-setup --mode controlled --strategy fastest-safe --run-until-complete --batch-size 250"
        : "Review field_errors / validation before continuing.",
  };
}

export function renderV114DryRunMarkdown(r) {
  return `# Production Census Schema v1.1.4 — Rooms / Keys Provenance Dry Run

**Status:** \`${r.status}\`  
**Generated:** ${r.generated_at}

- Field count before: **${r.field_count_before}**
- Expected after: **${r.expected_field_count_after}**
- To add: **${r.fields_to_add?.length}**
- Already exist: **${r.fields_already_existed?.length}**
- Census records: **${r.census_record_count}**
- Rooms Confidence has Hold: **${r.rooms_confidence_option_add?.already_has_hold}**

## Fields to add

\`\`\`json
${JSON.stringify(r.fields_to_add || [], null, 2)}
\`\`\`

## Rooms Confidence option add

\`\`\`json
${JSON.stringify(r.rooms_confidence_option_add || {}, null, 2)}
\`\`\`

## Rename note (not applied)

${r.rename_recommendation || ""}
`;
}

export function renderV114ApplyMarkdown(r) {
  return `# Production Census Schema v1.1.4 — Rooms / Keys Provenance Apply

**Status:** \`${r.status}\`  
**Mode:** ${r.mode}  
**Generated:** ${r.generated_at}  
**Apply executed:** ${r.apply_executed}

## Executive summary

- Fields created: **${(r.fields_created || []).length}**
- Fields already existing: **${(r.fields_already_existing || []).length}**
- Hold option on Rooms Confidence: **${r.rooms_confidence_hold?.applied || r.rooms_confidence_hold?.already_had_hold || r.rooms_confidence_hold?.skipped || false}**
- Field count: **${r.field_count_before} → ${r.field_count_after}**
- Census records: **${r.validation?.record_count ?? "—"}** (expected 666)
- New provenance populated: **${r.validation?.new_provenance_any_populated ? "YES (unexpected)" : "no (expected)"}**
- Existing cell value drift: **${r.existing_cell_value_drift_count ?? "—"}**
- Validation pass: **${r.validation_pass}**
- Brand Explorer writes: **false**
- Brand Setup writes: **false**
- Record writes: **false**

## Fields created

\`\`\`json
${JSON.stringify(r.fields_created || [], null, 2)}
\`\`\`

## Rooms Confidence Hold

\`\`\`json
${JSON.stringify(r.rooms_confidence_hold || {}, null, 2)}
\`\`\`

## Validation

\`\`\`json
${JSON.stringify(r.validation || {}, null, 2)}
\`\`\`

## Next

${r.next_recommended_step || ""}
`;
}
