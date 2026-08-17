/**
 * HBX Census schema repair + identity/provenance linkage v1.
 * Creates missing HBX + phone provenance fields, then writes linkage only
 * for existing_match_high. No inserts. No rooms/coords/media/owner.
 *
 * Objective: hbx-census-schema-and-identity-linkage-v1
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  resolvePat,
  resolveTargetBase,
} from "./production-census-schema-create.js";
import { TABLE_IDS } from "./production-census-write.js";
import {
  productionHotelPropertyCensus,
  assertProductionCensusWriteTarget,
  PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
} from "./production-census-source-of-truth.js";
import { isForbiddenAutopilotField } from "./census-autopilot-field-allowlist.js";
import { normalizePhoneNumber } from "./census-phone-number-enrichment.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.join(__dirname, "../..");

export const HBX_SCHEMA_LINKAGE_OBJECTIVE =
  "hbx-census-schema-and-identity-linkage-v1";
export const HBX_SCHEMA_LINKAGE_VERSION =
  "hbx-census-schema-and-identity-linkage-v1";

export const HBX_SCHEMA_LINKAGE_STATUS = Object.freeze({
  COMPLETE:
    "production_census_hbx_census_schema_and_identity_linkage_v1_complete",
  PARTIAL_MANUAL:
    "production_census_hbx_census_schema_and_identity_linkage_v1_partial_schema_manual_action_needed",
  PARTIAL_SCHEMA:
    "production_census_hbx_census_schema_and_identity_linkage_v1_partial_schema_remaining",
  BLOCKED:
    "production_census_hbx_census_schema_and_identity_linkage_v1_blocked",
});

const CENSUS_TABLE_ID =
  TABLE_IDS["Hotel Property Census"] ||
  productionHotelPropertyCensus.tableId ||
  PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID;

const CANDIDATE_PACK_PATH = path.join(
  ROOT,
  "reports/research-engine-v2/hbx-cala-wave1-candidate-pack.json"
);

const LINKAGE_WRITE_FIELDS = new Set([
  "HBX Hotel Code",
  "HBX Chain Code",
  "HBX Category Code",
  "HBX Category Name",
  "HBX Accommodation Type",
  "HBX License / Registration Number",
  "HBX Last Update",
  "HBX Linkage Confidence",
  "HBX Source Status",
  "HBX Content Review Status",
  "Phone Confidence",
  "Phone Source Type",
  "Phone Source URL",
  "Phone Review Status",
  "Phone Reviewed Date",
  "Phone Notes",
  "Last Reviewed Date",
]);

const FORBIDDEN = new Set([
  "Rooms / Keys",
  "Latitude",
  "Longitude",
  "Address",
  "Official Property URL",
  "Phone",
  "Owner Name",
  "Operator / Management Company",
  "Developer Name",
  "Opening Date",
  "Renovation / Conversion Date",
  "Affiliation Start Date",
  "Recent Momentum",
  "Company Validated",
  "Brand Verified",
  "Brand Status",
  "Current Brand",
]);

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
function text(name, description) {
  return { name, type: "singleLineText", description };
}
function longText(name, description) {
  return { name, type: "multilineText", description };
}
function urlField(name, description) {
  return { name, type: "url", description };
}
function dateField(name, description) {
  return {
    name,
    type: "date",
    description,
    options: { dateFormat: { name: "iso" } },
  };
}

export function buildHbxSchemaFieldSpecs() {
  return [
    text(
      "HBX Hotel Code",
      "Hotelbeds Content API hotel code — internal identity linkage"
    ),
    text("HBX Chain Code", "Hotelbeds chainCode — not Current Brand"),
    text("HBX Category Code", "Hotelbeds categoryCode"),
    text("HBX Category Name", "Hotelbeds category description when available"),
    text(
      "HBX Accommodation Type",
      "Hotelbeds accommodationTypeCode — filter non-hotel"
    ),
    text(
      "HBX License / Registration Number",
      "Hotelbeds license string when present"
    ),
    dateField("HBX Last Update", "Hotelbeds lastUpdate content timestamp"),
    singleSelect(
      "HBX Linkage Confidence",
      ["High", "Medium", "Low", "Review Needed"],
      "Confidence of HBX↔Census match"
    ),
    singleSelect(
      "HBX Source Status",
      ["Active", "Matched", "Candidate", "Held", "Conflict", "Needs Review"],
      "HBX linkage workflow status"
    ),
    singleSelect(
      "HBX Content Review Status",
      [
        "Internal Only",
        "Needs Review",
        "License Review Needed",
        "Approved Internal",
        "Hold",
      ],
      "Content storage/display review gate"
    ),
    singleSelect(
      "Phone Confidence",
      ["High", "Medium", "Low", "Needs Review"],
      "Confidence in Phone value"
    ),
    singleSelect(
      "Phone Source Type",
      [
        "hbx_content_api",
        "dataforseo",
        "official_property_page",
        "official_brand_directory",
        "steward_review",
        "other",
      ],
      "Provenance for Phone"
    ),
    urlField("Phone Source URL", "URL supporting the Phone value when available"),
    singleSelect(
      "Phone Review Status",
      ["Internal Only", "Needs Review", "Approved", "Hold"],
      "Phone review / exposure gate"
    ),
    dateField("Phone Reviewed Date", "Date phone provenance last reviewed"),
    longText("Phone Notes", "Phone provenance notes (internal)"),
  ];
}

export const HBX_SCHEMA_FIELD_NAMES = Object.freeze(
  buildHbxSchemaFieldSpecs().map((s) => s.name)
);

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function writeJson(fp, data) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, `${JSON.stringify(data, null, 2)}\n`, "utf8");
}
function writeMd(fp, md) {
  fs.mkdirSync(path.dirname(fp), { recursive: true });
  fs.writeFileSync(fp, md.endsWith("\n") ? md : `${md}\n`, "utf8");
}
function isBlank(v) {
  return v == null || !String(v).trim();
}
function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}

export function resolveHbxSchemaLinkageGates(env = process.env) {
  const flag = (k) => String(env[k] || "0").trim() === "1";
  const blockers = [];
  if (flag("ENABLE_HBX_INSERTS")) blockers.push("ENABLE_HBX_INSERTS_must_be_0");
  if (flag("ENABLE_HBX_NEW_CANDIDATE_INSERTS")) {
    blockers.push("ENABLE_HBX_NEW_CANDIDATE_INSERTS_must_be_0");
  }
  if (flag("ENABLE_HBX_COORDINATE_WRITES")) blockers.push("ENABLE_HBX_COORDINATE_WRITES_must_be_0");
  if (flag("ENABLE_HBX_IMAGE_WRITES")) blockers.push("ENABLE_HBX_IMAGE_WRITES_must_be_0");
  if (flag("ENABLE_HBX_DESCRIPTION_WRITES")) blockers.push("ENABLE_HBX_DESCRIPTION_WRITES_must_be_0");
  if (flag("ENABLE_HBX_FACILITY_WRITES")) blockers.push("ENABLE_HBX_FACILITY_WRITES_must_be_0");
  if (flag("ENABLE_HBX_ROOM_WRITES")) blockers.push("ENABLE_HBX_ROOM_WRITES_must_be_0");
  return {
    ok: blockers.length === 0,
    blockers,
    schema_repair: flag("ENABLE_HBX_SCHEMA_REPAIR"),
    identity_linkage: flag("ENABLE_HBX_IDENTITY_LINKAGE_WRITES"),
    census_writes: flag("ENABLE_HBX_CENSUS_WRITES"),
    existing_match_high: flag("ENABLE_HBX_EXISTING_MATCH_HIGH_WRITES"),
  };
}

async function metaFetch(baseId, token, pathAndQuery, init = {}) {
  const url = `https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}${pathAndQuery}`;
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

async function listCensusTable(baseId, token) {
  const { res, json } = await metaFetch(baseId, token, "/tables");
  if (!res.ok) throw new Error(`meta_tables_failed:${res.status}`);
  const table = (json.tables || []).find(
    (t) => t.id === CENSUS_TABLE_ID || t.name === "Hotel Property Census"
  );
  if (!table) throw new Error("hotel_property_census_not_found");
  return table;
}

/**
 * Create missing HBX/phone fields on Hotel Property Census.
 */
export async function ensureHbxSchemaFields({
  baseId,
  token,
  apply = false,
  log = () => {},
} = {}) {
  const table = await listCensusTable(baseId, token);
  const existingNames = new Set((table.fields || []).map((f) => f.name));
  const specs = buildHbxSchemaFieldSpecs();
  const found = [];
  const missing = [];
  const toCreate = [];
  for (const spec of specs) {
    if (existingNames.has(spec.name)) found.push(spec.name);
    else {
      missing.push(spec.name);
      toCreate.push(spec);
    }
  }

  const created = [];
  const errors = [];
  const manual_instructions = [];

  if (!apply) {
    for (const spec of toCreate) {
      manual_instructions.push({
        table: "Hotel Property Census",
        field: spec.name,
        type: spec.type,
        options: spec.options?.choices?.map((c) => c.name) || null,
        description: spec.description || null,
      });
    }
    return {
      found,
      missing,
      created,
      errors,
      dry_run: true,
      manual_instructions,
      field_count_before: table.fields.length,
    };
  }

  for (const spec of toCreate) {
    let ok = false;
    for (let attempt = 1; attempt <= 4 && !ok; attempt += 1) {
      const body = {
        name: spec.name,
        type: spec.type,
        description: spec.description,
        ...(spec.options ? { options: spec.options } : {}),
      };
      const { res, json } = await metaFetch(
        baseId,
        token,
        `/tables/${encodeURIComponent(table.id)}/fields`,
        { method: "POST", body: JSON.stringify(body) }
      );
      if (res.status === 429) {
        await sleep(1000 * attempt);
        continue;
      }
      if (res.status === 403 || res.status === 401) {
        errors.push({
          name: spec.name,
          status: res.status,
          error: json.error || json,
          permission: true,
        });
        manual_instructions.push({
          table: "Hotel Property Census",
          field: spec.name,
          type: spec.type,
          options: spec.options?.choices?.map((c) => c.name) || null,
          description: spec.description || null,
          reason: "schema_write_permission_missing",
        });
        break;
      }
      if (!res.ok) {
        errors.push({ name: spec.name, status: res.status, error: json.error || json });
        manual_instructions.push({
          table: "Hotel Property Census",
          field: spec.name,
          type: spec.type,
          options: spec.options?.choices?.map((c) => c.name) || null,
          description: spec.description || null,
          reason: `create_failed_${res.status}`,
        });
        break;
      }
      created.push({ name: spec.name, type: spec.type, id: json.id });
      existingNames.add(spec.name);
      log(`[hbx-schema] created ${spec.name}`);
      ok = true;
    }
    await sleep(280);
  }

  return {
    found,
    missing: missing.filter((n) => !created.some((c) => c.name === n)),
    created,
    errors,
    dry_run: false,
    manual_instructions,
    field_count_before: table.fields.length,
    field_count_after: table.fields.length + created.length,
  };
}

function loadExistingMatchHigh() {
  if (!fs.existsSync(CANDIDATE_PACK_PATH)) {
    throw new Error(`missing_candidate_pack:${CANDIDATE_PACK_PATH}`);
  }
  const pack = JSON.parse(fs.readFileSync(CANDIDATE_PACK_PATH, "utf8"));
  return (pack.candidates || []).filter(
    (c) => c.match_class === "existing_match_high" && c.census_record_id
  );
}

async function fetchCensusByIds(baseId, token, tableId, ids) {
  const out = [];
  const unique = [...new Set(ids.filter(Boolean))];
  for (let i = 0; i < unique.length; i += 20) {
    const chunk = unique.slice(i, i + 20);
    const filterByFormula = `OR(${chunk.map((id) => `RECORD_ID()='${id}'`).join(",")})`;
    const params = new URLSearchParams({ pageSize: "100", filterByFormula });
    for (const f of [
      "Property Name",
      "Phone",
      "Official Property URL",
      "Notes for Steward",
      "Last Reviewed Date",
      ...HBX_SCHEMA_FIELD_NAMES,
    ]) {
      params.append("fields[]", f);
    }
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}?${params}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(`census_read_failed:${res.status}`);
    out.push(...(json.records || []));
    await sleep(120);
  }
  return out;
}

/**
 * Build linkage-only patch. Does not rewrite Address/Phone/URL values.
 */
export function buildIdentityLinkagePatch(candidate, censusFields, fieldSet) {
  const patch = {};
  const skipped = [];
  const conflicts = [];

  const setIfBlank = (field, value) => {
    if (!fieldSet.has(field)) {
      skipped.push({ field, reason: "schema_missing" });
      return;
    }
    if (value == null || value === "") {
      skipped.push({ field, reason: "no_value" });
      return;
    }
    const existing = censusFields[field];
    if (!isBlank(existing)) {
      // Allow overwrite only if same value; else conflict hold
      if (String(existing).trim() === String(value).trim()) {
        skipped.push({ field, reason: "already_same" });
      } else {
        conflicts.push({ field, existing, candidate: value });
      }
      return;
    }
    patch[field] = value;
  };

  setIfBlank(
    "HBX Hotel Code",
    candidate.hbx_hotel_code != null ? String(candidate.hbx_hotel_code) : null
  );
  setIfBlank("HBX Chain Code", candidate.chain_code || null);
  setIfBlank("HBX Category Code", candidate.category || null);
  // Category name / accommodation / license / lastUpdate often absent from Wave1 pack
  setIfBlank("HBX Category Name", candidate.category_name || null);
  setIfBlank("HBX Accommodation Type", candidate.accommodation_type || null);
  setIfBlank(
    "HBX License / Registration Number",
    candidate.license || candidate.registration_number || null
  );
  if (candidate.last_update && fieldSet.has("HBX Last Update")) {
    const d = String(candidate.last_update).slice(0, 10);
    setIfBlank("HBX Last Update", /^\d{4}-\d{2}-\d{2}$/.test(d) ? d : null);
  }

  setIfBlank("HBX Linkage Confidence", "High");
  setIfBlank("HBX Source Status", "Matched");
  setIfBlank("HBX Content Review Status", "Internal Only");

  // Phone provenance only when Phone already present (do not rewrite Phone)
  if (!isBlank(censusFields.Phone)) {
    const phoneNorm =
      normalizePhoneNumber(censusFields.Phone) || String(censusFields.Phone).trim();
    const candPhone =
      normalizePhoneNumber(candidate.phonehotel) ||
      (candidate.phonehotel ? String(candidate.phonehotel).trim() : null);
    const samePhone =
      !candPhone ||
      String(phoneNorm).replace(/[^\d]/g, "").endsWith(
        String(candPhone).replace(/[^\d]/g, "").slice(-8)
      ) ||
      String(candPhone).replace(/[^\d]/g, "").endsWith(
        String(phoneNorm).replace(/[^\d]/g, "").slice(-8)
      );

    if (samePhone || !candPhone) {
      setIfBlank("Phone Confidence", "Medium");
      setIfBlank("Phone Source Type", "hbx_content_api");
      setIfBlank("Phone Review Status", "Internal Only");
      setIfBlank("Phone Reviewed Date", todayIsoDate());
      const web = candidate.website
        ? String(candidate.website).startsWith("http")
          ? candidate.website
          : `https://${candidate.website}`
        : null;
      if (web && !/cvent\.com/i.test(web)) {
        try {
          // validate URL
          // eslint-disable-next-line no-new
          new URL(web);
          setIfBlank("Phone Source URL", web);
        } catch {
          skipped.push({ field: "Phone Source URL", reason: "invalid_url" });
        }
      }
      setIfBlank(
        "Phone Notes",
        `phone_provenance | confidence=Medium | source=hbx_content_api | hbx_hotel_code=${candidate.hbx_hotel_code} | match=existing_match_high | public_exposure=false | reviewed=${todayIsoDate()}`
      );
    } else {
      conflicts.push({
        field: "Phone provenance",
        reason: "census_phone_differs_from_hbx_phonehotel",
        existing: censusFields.Phone,
        candidate: candidate.phonehotel,
      });
      setIfBlank("Phone Confidence", "Needs Review");
      setIfBlank("Phone Review Status", "Needs Review");
    }
  } else {
    skipped.push({ field: "Phone provenance", reason: "census_phone_blank_skip" });
  }

  if (Object.keys(patch).length) {
    patch["Last Reviewed Date"] = todayIsoDate();
  }

  // Strip forbidden
  for (const k of Object.keys(patch)) {
    if (FORBIDDEN.has(k) || isForbiddenAutopilotField(k) || !LINKAGE_WRITE_FIELDS.has(k)) {
      delete patch[k];
    }
  }

  return {
    ok: Object.keys(patch).length > 0,
    patch,
    skipped,
    conflicts,
    notes_left_in_place: true,
  };
}

async function applyPatches(proposals, { baseId, token, tableId, log }) {
  let updatesApplied = 0;
  const writeErrors = [];
  const counts = {
    hbx_hotel_code: 0,
    hbx_chain_code: 0,
    hbx_category_code: 0,
    phone_provenance: 0,
  };

  for (let i = 0; i < proposals.length; i += 10) {
    const chunk = proposals.slice(i, i + 10);
    const records = chunk
      .map((p) => {
        const fields = {};
        for (const [k, v] of Object.entries(p.patch || {})) {
          if (!LINKAGE_WRITE_FIELDS.has(k)) continue;
          if (FORBIDDEN.has(k) || isForbiddenAutopilotField(k)) continue;
          if (v === undefined || v === null || v === "") continue;
          fields[k] = v;
        }
        if (fields["HBX Hotel Code"]) counts.hbx_hotel_code += 1;
        if (fields["HBX Chain Code"]) counts.hbx_chain_code += 1;
        if (fields["HBX Category Code"]) counts.hbx_category_code += 1;
        if (
          fields["Phone Confidence"] ||
          fields["Phone Source Type"] ||
          fields["Phone Review Status"]
        ) {
          counts.phone_provenance += 1;
        }
        return { id: p.record_id, fields };
      })
      .filter((u) => Object.keys(u.fields).length > 0);

    if (!records.length) continue;

    const tryWrite = async (recs) => {
      const res = await fetch(
        `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}`,
        {
          method: "PATCH",
          headers: {
            Authorization: `Bearer ${token}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({ records: recs, typecast: true }),
        }
      );
      const json = await res.json().catch(() => ({}));
      return { res, json };
    };

    const { res, json } = await tryWrite(records);
    if (!res.ok) {
      writeErrors.push({ status: res.status, error: json.error || json, batch: true });
      log?.(`[hbx-linkage] batch ${res.status}; retrying one-by-one`);
      for (const rec of records) {
        const one = await tryWrite([rec]);
        if (!one.res.ok) {
          writeErrors.push({
            status: one.res.status,
            error: one.json.error || one.json,
            record_id: rec.id,
          });
        } else updatesApplied += 1;
        await sleep(150);
      }
    } else {
      updatesApplied += records.length;
    }
    await sleep(200);
  }

  return { updatesApplied, writeErrors, counts };
}

function renderMd(report) {
  return `# HBX Census Schema + Identity Linkage v1

**Status:** \`${report.status}\`  
**Objective:** \`${report.objective}\`  
**Generated:** ${report.generated_at}  
**Dry run:** ${report.dry_run}  
**Airtable writes:** **${report.airtable_writes}**

## Schema
- Fields present after: **${(report.schema?.found || []).length}**
- Fields created this run: **${(report.schema?.created || []).length}**
- Fields still missing: **${(report.schema?.missing || []).length}**
${(report.schema?.missing || []).map((f) => `  - \`${f}\``).join("\n") || ""}
${(report.schema?.created || []).map((f) => `- Created: \`${f.name}\``).join("\n")}

## Linkage
- existing_match_high reviewed: **${report.records_reviewed}**
- records updated: **${report.records_updated}**
- HBX Hotel Codes written: **${report.hbx_hotel_codes_written}**
- HBX Chain Codes written: **${report.hbx_chain_codes_written}**
- HBX Category Codes written: **${report.hbx_category_codes_written}**
- category/accommodation/license: Wave1 pack has category codes; accommodation/license/lastUpdate usually absent (skipped)
- Phone provenance fields touched: **${report.phone_provenance_writes}**
- Notes for Steward: left in place (not stripped)
- Conflicts: **${report.conflicts_count}**
  (typically census Phone differs from HBX phonehotel → Needs Review path)

## Confirmations
- No inserts: **true**
- No Rooms / Keys / coords / images / descriptions / facilities: **true**
- No Address / Phone / Official Property URL rewrites: **true**
- Hotel Property Census only: **true**

${
  report.manual_instructions?.length
    ? `## Manual schema action needed\n${report.manual_instructions
        .map(
          (m) =>
            `- Create \`${m.field}\` (${m.type})${m.options ? ` options: ${m.options.join(", ")}` : ""}`
        )
        .join("\n")}`
    : ""
}
`;
}

/**
 * @param {object} opts
 */
export async function runHbxCensusSchemaAndIdentityLinkageV1(opts = {}) {
  const env = opts.env || process.env;
  const log = opts.log || (() => {});
  const generated_at = new Date().toISOString();
  const gates = resolveHbxSchemaLinkageGates(env);

  if (!gates.ok) {
    const report = {
      ok: false,
      status: HBX_SCHEMA_LINKAGE_STATUS.BLOCKED,
      objective: HBX_SCHEMA_LINKAGE_OBJECTIVE,
      generated_at,
      reason: "gate_blockers",
      blockers: gates.blockers,
      airtable_writes: 0,
      dry_run: true,
    };
    persist(report);
    return report;
  }

  const enableSchemaApply = Boolean(
    opts.enableProductionWrites && gates.schema_repair
  );
  const enableLinkageWrites = Boolean(
    opts.enableProductionWrites &&
      gates.identity_linkage &&
      gates.census_writes &&
      gates.existing_match_high
  );

  let token;
  let baseId;
  try {
    token = resolvePat();
    const base = resolveTargetBase();
    baseId =
      base?.target_base_id || base?.baseId || env.AIRTABLE_BASE_ID_ALT;
    assertProductionCensusWriteTarget({
      tableId: CENSUS_TABLE_ID,
      tableName: "Hotel Property Census",
    });
  } catch (err) {
    const report = {
      ok: false,
      status: HBX_SCHEMA_LINKAGE_STATUS.BLOCKED,
      objective: HBX_SCHEMA_LINKAGE_OBJECTIVE,
      generated_at,
      reason: String(err?.message || err).slice(0, 300),
      airtable_writes: 0,
      dry_run: true,
    };
    persist(report);
    return report;
  }

  log(`[hbx-schema-linkage] schema_repair apply=${enableSchemaApply}`);
  const schemaResult = await ensureHbxSchemaFields({
    baseId,
    token,
    apply: enableSchemaApply,
    log,
  });

  // Re-read schema after create
  const tableAfter = await listCensusTable(baseId, token);
  const fieldSet = new Set((tableAfter.fields || []).map((f) => f.name));
  const stillMissing = HBX_SCHEMA_FIELD_NAMES.filter((n) => !fieldSet.has(n));

  if (stillMissing.includes("HBX Hotel Code") && !enableLinkageWrites) {
    // If schema couldn't be created and we're not writing, return manual action
    if (schemaResult.errors?.some((e) => e.permission) || stillMissing.length) {
      const report = {
        ok: true,
        status: HBX_SCHEMA_LINKAGE_STATUS.PARTIAL_MANUAL,
        objective: HBX_SCHEMA_LINKAGE_OBJECTIVE,
        generated_at,
        dry_run: !enableSchemaApply,
        airtable_writes: 0,
        inserts: 0,
        schema: {
          found: schemaResult.found,
          created: schemaResult.created,
          missing: stillMissing,
        },
        manual_instructions: schemaResult.manual_instructions,
        records_reviewed: 0,
        records_updated: 0,
        hbx_hotel_codes_written: 0,
        hbx_chain_codes_written: 0,
        hbx_category_codes_written: 0,
        phone_provenance_writes: 0,
        conflicts_count: 0,
        confirmations: {
          no_inserts: true,
          no_restricted_fields: true,
        },
      };
      persist(report);
      return report;
    }
  }

  if (stillMissing.includes("HBX Hotel Code") && enableLinkageWrites) {
    const report = {
      ok: true,
      status: HBX_SCHEMA_LINKAGE_STATUS.PARTIAL_MANUAL,
      objective: HBX_SCHEMA_LINKAGE_OBJECTIVE,
      generated_at,
      dry_run: false,
      airtable_writes: schemaResult.created?.length || 0,
      inserts: 0,
      reason: "HBX Hotel Code still missing — cannot run identity linkage writes",
      schema: {
        found: [...fieldSet].filter((n) => HBX_SCHEMA_FIELD_NAMES.includes(n)),
        created: schemaResult.created,
        missing: stillMissing,
      },
      manual_instructions: schemaResult.manual_instructions,
      records_reviewed: 0,
      records_updated: 0,
      hbx_hotel_codes_written: 0,
      hbx_chain_codes_written: 0,
      hbx_category_codes_written: 0,
      phone_provenance_writes: 0,
      conflicts_count: 0,
      confirmations: { no_inserts: true, no_restricted_fields: true },
    };
    persist(report);
    return report;
  }

  const high = loadExistingMatchHigh();
  // Dedupe by census_record_id (prefer first)
  const byRec = new Map();
  for (const c of high) {
    if (!byRec.has(c.census_record_id)) byRec.set(c.census_record_id, c);
  }
  const uniqueHigh = [...byRec.values()];

  log(`[hbx-schema-linkage] reviewing ${uniqueHigh.length} unique existing_match_high`);
  const censusRecords = await fetchCensusByIds(
    baseId,
    token,
    CENSUS_TABLE_ID,
    uniqueHigh.map((c) => c.census_record_id)
  );
  const byId = new Map(censusRecords.map((r) => [r.id, r]));

  const proposals = [];
  const conflicts = [];
  for (const c of uniqueHigh) {
    const rec = byId.get(c.census_record_id);
    if (!rec) continue;
    const built = buildIdentityLinkagePatch(c, rec.fields || {}, fieldSet);
    for (const conf of built.conflicts) {
      conflicts.push({ ...conf, record_id: c.census_record_id, hbx_hotel_code: c.hbx_hotel_code });
    }
    if (!built.ok) continue;
    proposals.push({
      record_id: c.census_record_id,
      hbx_hotel_code: c.hbx_hotel_code,
      patch: built.patch,
    });
  }

  let updatesApplied = 0;
  let writeErrors = [];
  let counts = {
    hbx_hotel_code: 0,
    hbx_chain_code: 0,
    hbx_category_code: 0,
    phone_provenance: 0,
  };

  if (enableLinkageWrites && proposals.length) {
    const applied = await applyPatches(proposals, {
      baseId,
      token,
      tableId: CENSUS_TABLE_ID,
      log,
    });
    updatesApplied = applied.updatesApplied;
    writeErrors = applied.writeErrors;
    counts = applied.counts;
  } else if (!enableLinkageWrites) {
    for (const p of proposals) {
      if (p.patch["HBX Hotel Code"]) counts.hbx_hotel_code += 1;
      if (p.patch["HBX Chain Code"]) counts.hbx_chain_code += 1;
      if (p.patch["HBX Category Code"]) counts.hbx_category_code += 1;
      if (
        p.patch["Phone Confidence"] ||
        p.patch["Phone Source Type"] ||
        p.patch["Phone Review Status"]
      ) {
        counts.phone_provenance += 1;
      }
    }
  }

  let status = HBX_SCHEMA_LINKAGE_STATUS.COMPLETE;
  if (stillMissing.length) status = HBX_SCHEMA_LINKAGE_STATUS.PARTIAL_SCHEMA;
  if (schemaResult.manual_instructions?.length && stillMissing.includes("HBX Hotel Code")) {
    status = HBX_SCHEMA_LINKAGE_STATUS.PARTIAL_MANUAL;
  }
  if (writeErrors.length && updatesApplied === 0 && enableLinkageWrites) {
    status = HBX_SCHEMA_LINKAGE_STATUS.BLOCKED;
  }

  const report = {
    ok: status !== HBX_SCHEMA_LINKAGE_STATUS.BLOCKED,
    status,
    objective: HBX_SCHEMA_LINKAGE_OBJECTIVE,
    version: HBX_SCHEMA_LINKAGE_VERSION,
    generated_at,
    dry_run: !enableSchemaApply && !enableLinkageWrites,
    airtable_writes: updatesApplied,
    inserts: 0,
    schema: {
      found: [...fieldSet].filter((n) => HBX_SCHEMA_FIELD_NAMES.includes(n)),
      created: schemaResult.created,
      missing: stillMissing,
      field_count: tableAfter.fields.length,
    },
    manual_instructions: schemaResult.manual_instructions,
    records_reviewed: uniqueHigh.length,
    proposals_ready: proposals.length,
    records_updated: enableLinkageWrites ? updatesApplied : 0,
    hbx_hotel_codes_written: counts.hbx_hotel_code,
    hbx_chain_codes_written: counts.hbx_chain_code,
    hbx_category_codes_written: counts.hbx_category_code,
    phone_provenance_writes: counts.phone_provenance,
    conflicts_count: conflicts.length,
    conflicts: conflicts.slice(0, 40),
    write_errors: writeErrors.slice(0, 15),
    notes_migrated_or_left: "left_in_place",
    confirmations: {
      no_inserts: true,
      no_rooms_keys: true,
      no_coords_images_descriptions_facilities: true,
      no_address_phone_url_rewrites: true,
      hotel_property_census_only: true,
      no_brand_explorer: true,
      no_brand_setup: true,
    },
  };

  persist(report);
  log(
    `[hbx-schema-linkage] status=${report.status} created=${schemaResult.created?.length || 0} updated=${report.records_updated}`
  );
  return report;
}

function persist(report) {
  const reportsDir = path.join(ROOT, "reports/research-engine-v2");
  const docsDir = path.join(ROOT, "docs/data-intelligence");
  writeJson(
    path.join(reportsDir, "hbx-census-schema-and-identity-linkage-v1.json"),
    report
  );
  const md = renderMd(report);
  writeMd(path.join(reportsDir, "hbx-census-schema-and-identity-linkage-v1.md"), md);
  writeMd(path.join(docsDir, "hbx-census-schema-and-identity-linkage-v1.md"), md);
}
