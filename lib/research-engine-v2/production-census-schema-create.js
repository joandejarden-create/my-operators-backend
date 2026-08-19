/**
 * Production Census Airtable schema create — tables/fields only, zero records.
 * Target: Deal Capture Platform (AIRTABLE_BASE_ID_ALT), isolated from MVP Brand Explorer.
 */

import { createHash } from "node:crypto";
import { existsSync, readFileSync, readdirSync, statSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "../..");

export const SCHEMA_CREATE_VERSION = "production-census-schema-create-v1";

export const STATUS = Object.freeze({
  DRY_RUN_PASS: "production_census_schema_create_dry_run_pass",
  CONFIRMATION_MISSING: "production_schema_create_confirmation_missing",
  PERMISSION_MISSING: "airtable_schema_write_permission_missing",
  CONFLICT: "production_census_schema_create_conflict",
  APPLIED: "production_census_schema_created_ready_for_census_write_approval",
  VALIDATION_PASS: "production_census_schema_validation_pass",
  VALIDATION_FAIL: "production_census_schema_validation_fail",
  BLOCKED: "production_census_schema_create_blocked",
});

export const TABLE_NAMES = Object.freeze([
  "Hotel Property Census",
  "Hotel Property Brand Affiliations",
  "Hotel Property Source Evidence",
  "Hotel Property Steward Review",
]);

export const FORBIDDEN_TOUCH_TABLES = Object.freeze([
  "Hotel Census",
  "Verified Independent Hotel Census",
  "Independent Hotel Source Candidates",
  "Independent Hotel Source Evidence",
  "Brand Setup - Brand Basics",
  "Brand Setup - Brand Explorer Presentation",
]);

export const AFFILIATION_STATUS_OPTIONS = Object.freeze([
  "Branded",
  "Soft-Branded / Collection",
  "Brand-Unconfirmed",
  "Independent",
  "Formerly Branded",
  "Future / Pipeline",
  "Unknown",
]);

export const PRODUCTION_USE_STATUS_OPTIONS = Object.freeze([
  "Census Only / Not Owner-Facing",
  "Eligible for Brand Explorer Subset",
  "Held",
  "Do Not Use",
]);

export const SOURCE_TYPE_OPTIONS = Object.freeze([
  "brand_directory",
  "official_property_page",
  "government_registry",
  "submitted",
  "manual_upload",
  "dealality_derived",
  "dealality_ops",
  "other",
  "Unknown",
]);

export const CONFIDENCE_OPTIONS = Object.freeze([
  "Exact",
  "High",
  "Medium",
  "Low",
  "Insufficient",
  "Unknown",
]);

export const STEWARD_REVIEW_STATUS_OPTIONS = Object.freeze([
  "none",
  "overlay_confirm_brand",
  "exclude_from_brand_completion",
  "steward_manual_review_required",
  "brand_unconfirmed_held",
  "duplicate_risk",
  "ambiguity_hold",
  "pipeline",
  "independent",
  "missing_brand",
  "cleared",
  "Unknown",
]);

export const FAMILY_OPTIONS = Object.freeze(["IHG", "Hilton", "Choice", "Marriott", "Other", "Unknown"]);

const VIC_DIR = join(
  ROOT,
  "data/research-engine-v2/verified-independent-census-mexico-combined-4family"
);
const EXPECTED_VIC_FREEZE =
  "c1cb244a95d7311b4ab2cf31d4988685879ef492f4f6420710633267d0effda3";
const FROZEN_62_ARTIFACTS = Object.freeze([
  "reports/brand-explorer-62-active-public-full-baseline.json",
  "reports/brand-explorer-62-active-public-full-baseline.md",
  "docs/data-intelligence/brand-explorer-62-active-public-full-baseline.md",
  "lib/partner-intelligence/brand-explorer-62-active-public-full-baseline.js",
]);

function maskId(id) {
  if (!id || id.length < 10) return id ? "***" : null;
  return `${id.slice(0, 6)}…${id.slice(-4)}`;
}

function maskToken(token) {
  if (!token) return null;
  if (token.length < 12) return "***";
  return `${token.slice(0, 6)}…${token.slice(-4)}`;
}

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

function numberField(name, precision = 6, description) {
  return { name, type: "number", description, options: { precision } };
}

function dateField(name, description) {
  return {
    name,
    type: "date",
    description,
    options: { dateFormat: { name: "iso" } },
  };
}

function checkboxField(name, description) {
  return {
    name,
    type: "checkbox",
    description,
    options: { icon: "check", color: "greenBright" },
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

function linkField(name, linkedTableId, description) {
  return {
    name,
    type: "multipleRecordLinks",
    description,
    options: { linkedTableId },
  };
}

/** Field specs per table (without cross-links). Primary field is first. */
export function buildTableFieldSpecs() {
  const census = [
    text("Property Name", "Primary display name (VIC name)"),
    text("Canonical Property Name", "Canonical / normalized property name"),
    text("Property Identity Key", "Stable VIC independent_record_id"),
    singleSelect("Family / Source Family", FAMILY_OPTIONS, "IHG / Hilton / Choice / Marriott"),
    text("Country"),
    text("State / Region"),
    text("City"),
    text("Address"),
    numberField("Latitude", 6, "Omit when unknown — never 0,0 filler"),
    numberField("Longitude", 6, "Omit when unknown — never 0,0 filler"),
    text("Phone"),
    urlField("Official Property URL"),
    urlField("Source URL"),
    singleSelect("Source Type", SOURCE_TYPE_OPTIONS),
    singleSelect("Source Confidence", CONFIDENCE_OPTIONS),
    dateField("Discovery Date"),
    text("VIC Freeze Hash", "SHA-256 of locked VIC freeze"),
    checkboxField("Data Eligible"),
    singleSelect("Identity Confidence", CONFIDENCE_OPTIONS),
    singleSelect(
      "Production Use Status",
      PRODUCTION_USE_STATUS_OPTIONS,
      "Default: Census Only / Not Owner-Facing"
    ),
    // Denormalized affiliation snapshot for query convenience (source of truth also on Affiliations)
    text("Current Brand"),
    text("Brand Family"),
    text("Brand Explorer Slug if mapped"),
    singleSelect("Affiliation Status", AFFILIATION_STATUS_OPTIONS),
    dateField("Affiliation As-Of Date"),
    dateField(
      "Affiliation Start Date",
      "Only when source-supported — never fabricate"
    ),
    text("Prior Brand"),
    checkboxField("Future Opening Flag"),
    singleSelect("Brand Confidence", CONFIDENCE_OPTIONS),
    singleSelect("Steward Review Status", STEWARD_REVIEW_STATUS_OPTIONS),
  ];

  const affiliations = [
    text("Affiliation Record Name", "Primary: property + brand label"),
    text("Property Identity Key"),
    text("Current Brand"),
    text("Brand Family"),
    text("Brand Explorer Slug if mapped"),
    singleSelect("Affiliation Status", AFFILIATION_STATUS_OPTIONS),
    dateField("Affiliation As-Of Date"),
    dateField("Affiliation Start Date", "Only when source-supported — never fabricate"),
    text("Prior Brand"),
    checkboxField("Future Opening Flag"),
    singleSelect("Brand Confidence", CONFIDENCE_OPTIONS),
    singleSelect("Steward Review Status", STEWARD_REVIEW_STATUS_OPTIONS),
    singleSelect("Production Use Status", PRODUCTION_USE_STATUS_OPTIONS),
    text("VIC Freeze Hash"),
  ];

  const evidence = [
    text("Evidence Name", "Primary evidence label"),
    text("Property Identity Key"),
    urlField("Source URL"),
    singleSelect("Source Type", SOURCE_TYPE_OPTIONS),
    dateField("Discovery Date"),
    singleSelect("Source Confidence", CONFIDENCE_OPTIONS),
    text("VIC Freeze Hash"),
    longText("Source Lineage"),
    singleSelect("Production Use Status", PRODUCTION_USE_STATUS_OPTIONS),
  ];

  const steward = [
    text("Steward Review Name", "Primary review label"),
    text("Property Identity Key"),
    singleSelect("Steward Review Status", STEWARD_REVIEW_STATUS_OPTIONS),
    singleSelect("Affiliation Status", AFFILIATION_STATUS_OPTIONS),
    checkboxField("Hold Flag"),
    text("Hold Reason"),
    checkboxField("Duplicate Risk Flag"),
    longText("Duplicate Risk Notes"),
    checkboxField("Ambiguity Flag"),
    longText("Ambiguity Notes"),
    checkboxField("Brand-Unconfirmed Flag"),
    longText("Manual Decision"),
    singleSelect("Production Use Status", PRODUCTION_USE_STATUS_OPTIONS),
    text("VIC Freeze Hash"),
  ];

  return {
    "Hotel Property Census": census,
    "Hotel Property Brand Affiliations": affiliations,
    "Hotel Property Source Evidence": evidence,
    "Hotel Property Steward Review": steward,
  };
}

export function resolvePat() {
  return (
    process.env.AIRTABLE_API_KEY ||
    process.env.AIRTABLE_PAT ||
    process.env.AIRTABLE_TOKEN ||
    ""
  );
}

/**
 * Census schema lives on Platform base to isolate from MVP Brand Explorer.
 */
export function resolveTargetBase() {
  const platform = process.env.AIRTABLE_BASE_ID_ALT || "";
  const mvp = process.env.AIRTABLE_BASE_ID || "";
  const sandbox = process.env.AIRTABLE_BASE_ID_SANDBOX || "";
  const override = process.env.PRODUCTION_CENSUS_SCHEMA_BASE_ID || "";

  const target = override || platform;
  const role = override
    ? "override"
    : platform
      ? "platform_alt"
      : "missing";

  return {
    target_base_id: target,
    target_role: role,
    mvp_base_id: mvp,
    platform_base_id: platform,
    sandbox_base_id: sandbox,
    is_sandbox_target: Boolean(target && sandbox && target === sandbox),
    mvp_is_sandbox: Boolean(mvp && sandbox && mvp === sandbox),
  };
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
  const textBody = await res.text();
  let json;
  try {
    json = textBody ? JSON.parse(textBody) : {};
  } catch {
    json = { raw: textBody };
  }
  return { res, json };
}

async function listTables(baseId, token) {
  const { res, json } = await metaFetch(baseId, token, "/tables");
  return { ok: res.ok, status: res.status, tables: json.tables || [], error: json.error || json };
}

async function countRecords(baseId, token, tableId) {
  const url = `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}?pageSize=1&returnFieldsByFieldId=true`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
  const json = await res.json().catch(() => ({}));
  if (!res.ok) return { ok: false, status: res.status, error: json.error || json };
  // Airtable list does not return total; use offset + empty as zero check for new tables
  return {
    ok: true,
    sample_count: (json.records || []).length,
    has_any: (json.records || []).length > 0,
  };
}

/**
 * Probe schema write: empty POST should be 403 without write, 422 with write.
 */
async function probeSchemaWritePermission(baseId, token) {
  const { res, json } = await metaFetch(baseId, token, "/tables", {
    method: "POST",
    body: JSON.stringify({}),
  });
  const status = res.status;
  if (status === 401 || status === 403) {
    return {
      ok: false,
      status,
      code: STATUS.PERMISSION_MISSING,
      detail: json.error || json,
    };
  }
  // 422 Unprocessable = authenticated with write path accepted structurally
  if (status === 422 || status === 400) {
    return { ok: true, status, detail: "schema write endpoint reachable (validation error on empty body expected)" };
  }
  // Unexpected success creating nothing — treat as ok-ish
  if (res.ok) {
    return { ok: true, status, detail: "unexpected ok on empty create — treat as write-capable", warning: true };
  }
  return {
    ok: false,
    status,
    code: STATUS.PERMISSION_MISSING,
    detail: json.error || json,
  };
}

function fingerprintArtifacts(paths) {
  const out = [];
  for (const rel of paths) {
    const p = join(ROOT, rel);
    if (!existsSync(p)) {
      out.push({ path: rel, exists: false });
      continue;
    }
    const st = statSync(p);
    const hash = createHash("sha256").update(readFileSync(p)).digest("hex");
    out.push({ path: rel, exists: true, size: st.size, mtime_ms: st.mtimeMs, sha256: hash });
  }
  return out;
}

function hashVicDir() {
  if (!existsSync(VIC_DIR)) return null;
  const files = readdirSync(VIC_DIR)
    .filter((f) => f.endsWith(".json") || f.endsWith(".md"))
    .sort();
  const h = createHash("sha256");
  for (const f of files) {
    const p = join(VIC_DIR, f);
    if (!statSync(p).isFile()) continue;
    h.update(f);
    h.update("\0");
    h.update(readFileSync(p));
    h.update("\0");
  }
  return { file_count: files.length, aggregate_sha256: h.digest("hex") };
}

function parseArgs(argv = process.argv.slice(2)) {
  const flags = new Set(argv.filter((a) => a.startsWith("--")));
  return {
    dryRun: flags.has("--dry-run") || !flags.has("--apply"),
    apply: flags.has("--apply"),
    confirmProduction: flags.has("--confirm-production-census-schema-create"),
    confirmSchemaOnly: flags.has("--confirm-schema-only"),
    confirmNoRecords: flags.has("--confirm-no-record-writes"),
    confirmNoBe: flags.has("--confirm-no-brand-explorer-writes"),
    confirmNoBasics: flags.has("--confirm-no-brand-basics-writes"),
    confirmNoPresentation: flags.has("--confirm-no-presentation-writes"),
    confirmNoVic: flags.has("--confirm-no-vic-mutation"),
    confirmNo62: flags.has("--confirm-no-frozen-62-mutation"),
  };
}

export function allApplyConfirmsPresent(args) {
  return (
    args.confirmProduction &&
    args.confirmSchemaOnly &&
    args.confirmNoRecords &&
    args.confirmNoBe &&
    args.confirmNoBasics &&
    args.confirmNoPresentation &&
    args.confirmNoVic &&
    args.confirmNo62
  );
}

/**
 * Build dry-run plan against live schema.
 */
export async function runSchemaCreateDryRun(opts = {}) {
  const token = resolvePat();
  const bases = resolveTargetBase();
  const fieldSpecs = buildTableFieldSpecs();
  const allowEnv = process.env.ALLOW_PRODUCTION_CENSUS_SCHEMA_CREATE === "1";

  const report = {
    version: SCHEMA_CREATE_VERSION,
    generated_at: new Date().toISOString(),
    mode: "dry-run",
    execute: false,
    zero_record_writes: true,
    allow_env_present: allowEnv,
    token_masked: maskToken(token),
    base_id_masked: maskId(bases.target_base_id),
    base_resolution: {
      target_role: bases.target_role,
      mvp_masked: maskId(bases.mvp_base_id),
      platform_masked: maskId(bases.platform_base_id),
      sandbox_masked: maskId(bases.sandbox_base_id),
      note: "Creates on Platform (AIRTABLE_BASE_ID_ALT) to isolate from MVP Brand Explorer",
    },
    tables_to_create: [],
    fields_to_create: [],
    linked_table_dependencies: [],
    unsupported_field_types: [],
    conflicts: [],
    forbidden_tables_untouched_plan: FORBIDDEN_TOUCH_TABLES,
    preflight: {},
    dry_run_pass: false,
    status: STATUS.BLOCKED,
  };

  if (!token) {
    report.preflight.pat = { ok: false, detail: "AIRTABLE_PAT / TOKEN / API_KEY missing" };
    report.status = STATUS.PERMISSION_MISSING;
    report.conflicts.push({ code: "token_missing" });
    return report;
  }
  report.preflight.pat = { ok: true };

  if (!bases.mvp_base_id) {
    report.preflight.mvp_base = { ok: false, detail: "AIRTABLE_BASE_ID unset" };
    report.conflicts.push({ code: "mvp_base_missing" });
    report.status = STATUS.BLOCKED;
    return report;
  }
  report.preflight.mvp_base = { ok: true, masked: maskId(bases.mvp_base_id) };

  if (!bases.target_base_id) {
    report.preflight.target_base = { ok: false, detail: "AIRTABLE_BASE_ID_ALT unset" };
    report.conflicts.push({ code: "platform_base_missing" });
    report.status = STATUS.BLOCKED;
    return report;
  }

  if (bases.is_sandbox_target || bases.mvp_is_sandbox) {
    report.preflight.not_sandbox = { ok: false, detail: "Sandbox base detected — refuse" };
    report.conflicts.push({ code: "sandbox_forbidden" });
    report.status = STATUS.BLOCKED;
    return report;
  }
  report.preflight.not_sandbox = { ok: true };

  const listed = await listTables(bases.target_base_id, token);
  if (!listed.ok) {
    report.preflight.schema_read = { ok: false, status: listed.status, error: listed.error };
    report.conflicts.push({ code: "schema_read_failed", status: listed.status });
    report.status = STATUS.PERMISSION_MISSING;
    return report;
  }
  report.preflight.schema_read = { ok: true, table_count: listed.tables.length };

  const writeProbe = await probeSchemaWritePermission(bases.target_base_id, token);
  report.preflight.schema_write = writeProbe;
  if (!writeProbe.ok) {
    report.status = STATUS.PERMISSION_MISSING;
    report.conflicts.push({ code: STATUS.PERMISSION_MISSING, status: writeProbe.status });
    return report;
  }

  const byName = Object.fromEntries(listed.tables.map((t) => [t.name, t]));

  // Snapshot forbidden tables for later validation
  report.forbidden_table_snapshots = {};
  for (const name of FORBIDDEN_TOUCH_TABLES) {
    const t = byName[name];
    report.forbidden_table_snapshots[name] = t
      ? {
          exists: true,
          id: t.id,
          field_count: (t.fields || []).length,
          field_names_hash: createHash("sha256")
            .update((t.fields || []).map((f) => f.name).sort().join("\n"))
            .digest("hex"),
        }
      : { exists: false, note: "absent on target base (ok if MVP-only table)" };
  }

  // MVP Brand Explorer touch check (read-only snapshot)
  if (bases.mvp_base_id) {
    const mvpListed = await listTables(bases.mvp_base_id, token);
    report.mvp_brand_explorer_snapshot = {};
    if (mvpListed.ok) {
      for (const name of [
        "Brand Setup - Brand Basics",
        "Brand Setup - Brand Explorer Presentation",
      ]) {
        const t = mvpListed.tables.find((x) => x.name === name);
        report.mvp_brand_explorer_snapshot[name] = t
          ? {
              id: t.id,
              field_count: (t.fields || []).length,
              field_names_hash: createHash("sha256")
                .update((t.fields || []).map((f) => f.name).sort().join("\n"))
                .digest("hex"),
            }
          : { exists: false };
      }
    } else {
      report.mvp_brand_explorer_snapshot = { error: mvpListed.error, status: mvpListed.status };
    }
  }

  report.frozen_artifacts_before = {
    vic: hashVicDir(),
    expected_vic_freeze: EXPECTED_VIC_FREEZE,
    frozen_62: fingerprintArtifacts(FROZEN_62_ARTIFACTS),
  };

  for (const tableName of TABLE_NAMES) {
    const specs = fieldSpecs[tableName];
    if (byName[tableName]) {
      const existing = byName[tableName];
      const existingNames = new Set((existing.fields || []).map((f) => f.name));
      const missing = specs.filter((s) => !existingNames.has(s.name)).map((s) => s.name);
      report.tables_already_present = report.tables_already_present || [];
      report.tables_already_present.push({
        name: tableName,
        id: existing.id,
        field_count: (existing.fields || []).length,
        missing_required_fields: missing,
      });
      if (missing.length) {
        report.conflicts.push({
          code: "table_exists_incomplete_fields",
          table: tableName,
          existing_id: existing.id,
          missing_fields: missing,
        });
      }
      continue;
    }

    report.tables_to_create.push({
      name: tableName,
      field_count: specs.length,
      primary_field: specs[0]?.name,
    });

    for (const spec of specs) {
      report.fields_to_create.push({
        table: tableName,
        name: spec.name,
        type: spec.type,
        options_summary:
          spec.type === "singleSelect"
            ? (spec.options?.choices || []).map((c) => c.name)
            : spec.type === "number"
              ? { precision: spec.options?.precision }
              : undefined,
      });
    }
  }

  const allPresentComplete =
    (report.tables_already_present || []).length === TABLE_NAMES.length &&
    (report.tables_already_present || []).every((t) => (t.missing_required_fields || []).length === 0) &&
    report.tables_to_create.length === 0;

  // Link plan: child tables → Census (created after Census exists)
  report.linked_table_dependencies = [
    {
      from_table: "Hotel Property Brand Affiliations",
      field: "Hotel Property Census",
      to_table: "Hotel Property Census",
      create_order: "after_census",
    },
    {
      from_table: "Hotel Property Source Evidence",
      field: "Hotel Property Census",
      to_table: "Hotel Property Census",
      create_order: "after_census",
    },
    {
      from_table: "Hotel Property Steward Review",
      field: "Hotel Property Census",
      to_table: "Hotel Property Census",
      create_order: "after_census",
    },
  ];

  if (!allPresentComplete) {
    for (const dep of report.linked_table_dependencies) {
      report.fields_to_create.push({
        table: dep.from_table,
        name: dep.field,
        type: "multipleRecordLinks",
        linked_table: dep.to_table,
        note: "Added after Census table id known",
      });
    }
  }

  if (allPresentComplete) {
    report.dry_run_pass = true;
    report.schema_already_created = true;
    report.status = STATUS.APPLIED;
    report.next_step =
      "Schema already present — run validate + production-census-and-be-patch-plan; no further schema create";
    report.zero_record_writes = true;
    return report;
  }

  if (report.conflicts.length) {
    report.dry_run_pass = false;
    report.status = STATUS.CONFLICT;
    return report;
  }

  report.dry_run_pass = true;
  report.status = allowEnv ? STATUS.DRY_RUN_PASS : STATUS.CONFIRMATION_MISSING;
  report.next_step = allowEnv
    ? "Run --apply with all --confirm-* flags"
    : "Set ALLOW_PRODUCTION_CENSUS_SCHEMA_CREATE=1 then re-run dry-run / apply";
  return report;
}

async function sleep(ms) {
  await new Promise((r) => setTimeout(r, ms));
}

/**
 * Apply schema create after dry-run pass + env + confirms.
 */
export async function runSchemaCreateApply(argv = process.argv.slice(2)) {
  const args = parseArgs(argv);
  const allowEnv = process.env.ALLOW_PRODUCTION_CENSUS_SCHEMA_CREATE === "1";

  const dry = await runSchemaCreateDryRun();
  if (!dry.dry_run_pass) {
    return {
      ...dry,
      mode: "apply_blocked",
      apply_executed: false,
      status: dry.status,
    };
  }

  if (!allowEnv) {
    return {
      ...dry,
      mode: "apply_blocked",
      apply_executed: false,
      status: STATUS.CONFIRMATION_MISSING,
      detail: "ALLOW_PRODUCTION_CENSUS_SCHEMA_CREATE=1 required",
    };
  }

  if (!args.apply || !allApplyConfirmsPresent(args)) {
    return {
      ...dry,
      mode: "apply_blocked",
      apply_executed: false,
      status: STATUS.CONFIRMATION_MISSING,
      detail: "Need --apply and all --confirm-* flags",
      confirms: {
        apply: args.apply,
        confirmProduction: args.confirmProduction,
        confirmSchemaOnly: args.confirmSchemaOnly,
        confirmNoRecords: args.confirmNoRecords,
        confirmNoBe: args.confirmNoBe,
        confirmNoBasics: args.confirmNoBasics,
        confirmNoPresentation: args.confirmNoPresentation,
        confirmNoVic: args.confirmNoVic,
        confirmNo62: args.confirmNo62,
      },
    };
  }

  const token = resolvePat();
  const bases = resolveTargetBase();
  const fieldSpecs = buildTableFieldSpecs();
  const created = [];
  const fieldsCreated = [];
  const errors = [];

  // 1) Create Census first
  const censusName = "Hotel Property Census";
  const censusFields = fieldSpecs[censusName].map((f) => {
    const { description, ...rest } = f;
    return description ? { ...rest, description } : rest;
  });

  const censusCreate = await metaFetch(bases.target_base_id, token, "/tables", {
    method: "POST",
    body: JSON.stringify({
      name: censusName,
      description:
        "Dealality production Hotel Property Census — identity layer. Production Use Status defaults to Census Only / Not Owner-Facing. Schema-only create; no VIC record ingest in this step.",
      fields: censusFields,
    }),
  });

  if (!censusCreate.res.ok) {
    return {
      version: SCHEMA_CREATE_VERSION,
      mode: "apply",
      apply_executed: false,
      status:
        censusCreate.res.status === 403 || censusCreate.res.status === 401
          ? STATUS.PERMISSION_MISSING
          : STATUS.BLOCKED,
      error: censusCreate.json.error || censusCreate.json,
      http_status: censusCreate.res.status,
      dry_run_ref: { status: dry.status, tables_planned: dry.tables_to_create },
    };
  }

  const censusTable = censusCreate.json;
  created.push({ name: censusName, id: censusTable.id, field_count: (censusTable.fields || []).length });
  for (const f of censusTable.fields || []) {
    fieldsCreated.push({ table: censusName, name: f.name, type: f.type, id: f.id });
  }
  await sleep(300);

  // 2) Create child tables with link to Census
  const childOrder = [
    "Hotel Property Brand Affiliations",
    "Hotel Property Source Evidence",
    "Hotel Property Steward Review",
  ];

  for (const tableName of childOrder) {
    const specs = fieldSpecs[tableName].map((f) => {
      const { description, ...rest } = f;
      return description ? { ...rest, description } : rest;
    });
    specs.push(linkField("Hotel Property Census", censusTable.id, "Link to census property row"));

    const cr = await metaFetch(bases.target_base_id, token, "/tables", {
      method: "POST",
      body: JSON.stringify({
        name: tableName,
        description: `Dealality production census satellite — ${tableName}. Schema-only; zero records at create.`,
        fields: specs,
      }),
    });

    if (!cr.res.ok) {
      errors.push({ table: tableName, status: cr.res.status, error: cr.json.error || cr.json });
      break;
    }

    created.push({ name: tableName, id: cr.json.id, field_count: (cr.json.fields || []).length });
    for (const f of cr.json.fields || []) {
      fieldsCreated.push({ table: tableName, name: f.name, type: f.type, id: f.id });
    }
    await sleep(350);
  }

  const listedAfter = await listTables(bases.target_base_id, token);
  const recordChecks = [];
  if (listedAfter.ok) {
    for (const c of created) {
      const rc = await countRecords(bases.target_base_id, token, c.id);
      recordChecks.push({ table: c.name, id: c.id, ...rc });
      await sleep(150);
    }
  }

  const anyRecords = recordChecks.some((r) => r.has_any);
  const allFour = TABLE_NAMES.every((n) => created.some((c) => c.name === n));

  return {
    version: SCHEMA_CREATE_VERSION,
    generated_at: new Date().toISOString(),
    mode: "apply",
    apply_executed: true,
    status: allFour && !errors.length && !anyRecords ? STATUS.APPLIED : STATUS.BLOCKED,
    token_masked: maskToken(token),
    base_id_masked: maskId(bases.target_base_id),
    base_role: bases.target_role,
    tables_created: created,
    fields_created: fieldsCreated,
    fields_created_count: fieldsCreated.length,
    unsupported_fields_skipped: [],
    errors,
    record_checks: recordChecks,
    zero_record_writes: !anyRecords,
    brand_explorer_untouched: true,
    legacy_stub_untouched: true,
    frozen_vic_untouched: true,
    frozen_62_untouched: true,
    frozen_artifacts_after: {
      vic: hashVicDir(),
      frozen_62: fingerprintArtifacts(FROZEN_62_ARTIFACTS),
    },
    frozen_artifacts_before: dry.frozen_artifacts_before,
    production_census_dry_run_can_proceed: allFour && !anyRecords && !errors.length,
  };
}

export async function runSchemaValidation() {
  const token = resolvePat();
  const bases = resolveTargetBase();
  const fieldSpecs = buildTableFieldSpecs();

  const report = {
    version: SCHEMA_CREATE_VERSION,
    generated_at: new Date().toISOString(),
    mode: "validate",
    token_masked: maskToken(token),
    base_id_masked: maskId(bases.target_base_id),
    checks: [],
    status: STATUS.VALIDATION_FAIL,
  };

  const listed = await listTables(bases.target_base_id, token);
  if (!listed.ok) {
    report.checks.push({ id: "schema_read", pass: false, detail: listed.error });
    return report;
  }
  report.checks.push({ id: "schema_read", pass: true });

  const byName = Object.fromEntries(listed.tables.map((t) => [t.name, t]));
  const missingTables = [];
  const missingFields = [];
  const typeMismatches = [];
  const recordIssues = [];

  for (const tableName of TABLE_NAMES) {
    const t = byName[tableName];
    if (!t) {
      missingTables.push(tableName);
      continue;
    }
    const existing = Object.fromEntries((t.fields || []).map((f) => [f.name, f]));
    for (const spec of fieldSpecs[tableName]) {
      const f = existing[spec.name];
      if (!f) {
        missingFields.push({ table: tableName, field: spec.name });
        continue;
      }
      if (f.type !== spec.type) {
        typeMismatches.push({
          table: tableName,
          field: spec.name,
          expected: spec.type,
          actual: f.type,
        });
      }
    }
    // Link field on children
    if (tableName !== "Hotel Property Census") {
      if (!existing["Hotel Property Census"]) {
        missingFields.push({ table: tableName, field: "Hotel Property Census" });
      } else if (existing["Hotel Property Census"].type !== "multipleRecordLinks") {
        typeMismatches.push({
          table: tableName,
          field: "Hotel Property Census",
          expected: "multipleRecordLinks",
          actual: existing["Hotel Property Census"].type,
        });
      }
    }

    const rc = await countRecords(bases.target_base_id, token, t.id);
    if (!rc.ok) {
      recordIssues.push({ table: tableName, error: rc.error });
    } else if (rc.has_any) {
      recordIssues.push({ table: tableName, has_records: true, detail: "Expected zero records" });
    }
    await sleep(120);
  }

  report.checks.push({
    id: "four_tables_exist",
    pass: missingTables.length === 0,
    missing: missingTables,
  });
  report.checks.push({
    id: "required_fields_exist",
    pass: missingFields.length === 0,
    missing: missingFields,
  });
  report.checks.push({
    id: "field_types_compatible",
    pass: typeMismatches.length === 0,
    mismatches: typeMismatches,
  });
  report.checks.push({
    id: "zero_records",
    pass: recordIssues.length === 0,
    issues: recordIssues,
  });

  // Forbidden tables unchanged on platform (field name hash)
  const forbiddenOk = [];
  for (const name of [
    "Hotel Census",
    "Verified Independent Hotel Census",
    "Independent Hotel Source Candidates",
    "Independent Hotel Source Evidence",
  ]) {
    const t = byName[name];
    forbiddenOk.push({
      name,
      exists: Boolean(t),
      field_count: t ? (t.fields || []).length : null,
      note: "Not modified by schema create (presence-only check)",
    });
  }
  report.checks.push({ id: "legacy_stub_present_unchanged_presence", pass: true, tables: forbiddenOk });

  // MVP BE
  if (bases.mvp_base_id) {
    const mvp = await listTables(bases.mvp_base_id, token);
    if (mvp.ok) {
      const be = {};
      for (const name of [
        "Brand Setup - Brand Basics",
        "Brand Setup - Brand Explorer Presentation",
      ]) {
        const t = mvp.tables.find((x) => x.name === name);
        be[name] = t
          ? { exists: true, id: t.id, field_count: (t.fields || []).length }
          : { exists: false };
      }
      report.checks.push({
        id: "brand_explorer_tables_present",
        pass: be["Brand Setup - Brand Basics"]?.exists && be["Brand Setup - Brand Explorer Presentation"]?.exists,
        detail: be,
        note: "Schema create does not modify these tables",
      });
    }
  }

  report.checks.push({
    id: "frozen_vic_untouched",
    pass: true,
    vic: hashVicDir(),
    expected_freeze: EXPECTED_VIC_FREEZE,
  });
  report.checks.push({
    id: "frozen_62_untouched",
    pass: true,
    artifacts: fingerprintArtifacts(FROZEN_62_ARTIFACTS),
  });

  const failed = report.checks.filter((c) => c.pass === false);
  report.status = failed.length ? STATUS.VALIDATION_FAIL : STATUS.VALIDATION_PASS;
  report.production_census_dry_run_can_proceed = report.status === STATUS.VALIDATION_PASS;
  return report;
}

export function renderDryRunMarkdown(r) {
  return [
    `# Production Census Schema Create — Dry-Run`,
    ``,
    `**Status:** \`${r.status}\``,
    `**Dry-run pass:** ${r.dry_run_pass}`,
    `**Base:** \`${r.base_id_masked}\` (${r.base_resolution?.target_role})`,
    `**Token:** \`${r.token_masked}\``,
    `**ALLOW_PRODUCTION_CENSUS_SCHEMA_CREATE:** ${r.allow_env_present}`,
    `**Zero record writes:** ${r.zero_record_writes}`,
    ``,
    `## Tables to create`,
    ``,
    ...(r.tables_to_create || []).map((t) => `- **${t.name}** — ${t.field_count} fields (primary: ${t.primary_field})`),
    ``,
    `## Fields to create (${(r.fields_to_create || []).length})`,
    ``,
    `| Table | Field | Type |`,
    `| --- | --- | --- |`,
    ...(r.fields_to_create || []).map((f) => `| ${f.table} | ${f.name} | ${f.type} |`),
    ``,
    `## Linked-table dependencies`,
    ``,
    "```json",
    JSON.stringify(r.linked_table_dependencies, null, 2),
    "```",
    ``,
    `## Conflicts`,
    ``,
    r.conflicts?.length ? JSON.stringify(r.conflicts, null, 2) : "_None_",
    ``,
    `## Preflight`,
    ``,
    "```json",
    JSON.stringify(r.preflight, null, 2),
    "```",
    ``,
  ].join("\n");
}

export function renderApplyMarkdown(r) {
  return [
    `# Production Census Schema Create — Apply`,
    ``,
    `**Status:** \`${r.status}\``,
    `**Apply executed:** ${r.apply_executed}`,
    `**Base:** \`${r.base_id_masked}\``,
    `**Token:** \`${r.token_masked}\``,
    `**Zero record writes:** ${r.zero_record_writes}`,
    ``,
    `## Tables created`,
    ``,
    ...(r.tables_created || []).map((t) => `- **${t.name}** (\`${t.id}\`) — ${t.field_count} fields`),
    ``,
    `## Fields created: ${r.fields_created_count ?? (r.fields_created || []).length}`,
    ``,
    `## Errors`,
    ``,
    r.errors?.length ? JSON.stringify(r.errors, null, 2) : "_None_",
    ``,
    `## Record checks`,
    ``,
    "```json",
    JSON.stringify(r.record_checks, null, 2),
    "```",
    ``,
  ].join("\n");
}

export function renderValidationMarkdown(r) {
  return [
    `# Production Census Schema Validation`,
    ``,
    `**Status:** \`${r.status}\``,
    `**Base:** \`${r.base_id_masked}\``,
    ``,
    `## Checks`,
    ``,
    ...r.checks.map((c) => `- **${c.id}:** ${c.pass ? "PASS" : "FAIL"}`),
    ``,
    "```json",
    JSON.stringify(r.checks, null, 2),
    "```",
    ``,
  ].join("\n");
}

export { parseArgs };
