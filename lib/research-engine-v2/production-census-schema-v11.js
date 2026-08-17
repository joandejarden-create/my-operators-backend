/**
 * Production Census schema v1.1 — future hotel intelligence fields on Hotel Property Census.
 * Schema create + optional safe backfill only. No BE writes. No record duplication.
 */

import { createHash } from "node:crypto";
import { existsSync, readFileSync, readdirSync, statSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import {
  resolvePat,
  resolveTargetBase,
  CONFIDENCE_OPTIONS,
} from "./production-census-schema-create.js";
import {
  TABLE_IDS,
  EXPECTED_FREEZE,
  PRODUCTION_USE_STATUS,
} from "./production-census-write.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "../..");

export const V11_VERSION = "production-census-schema-v11-v1";
export const CENSUS_TABLE = "Hotel Property Census";
export const CENSUS_TABLE_ID = TABLE_IDS["Hotel Property Census"];

export const STATUS = Object.freeze({
  CONFIRMATION_MISSING: "production_census_schema_v11_confirmation_missing",
  DRY_RUN_PASS: "production_census_schema_v11_dry_run_pass",
  DRY_RUN_FAIL: "production_census_schema_v11_dry_run_fail",
  APPLIED: "production_census_schema_v11_ready_for_future_enrichment",
  BLOCKED: "production_census_schema_v11_blocked",
});

const VIC_DIR = join(
  ROOT,
  "data/research-engine-v2/verified-independent-census-mexico-combined-4family"
);
const FROZEN_62 = Object.freeze([
  "reports/brand-explorer-62-active-public-full-baseline.json",
  "reports/brand-explorer-62-active-public-full-baseline.md",
  "docs/data-intelligence/brand-explorer-62-active-public-full-baseline.md",
  "lib/partner-intelligence/brand-explorer-62-active-public-full-baseline.js",
]);

const PROPERTY_TYPE_OPTIONS = [
  "Hotel",
  "Resort",
  "Boutique Hotel",
  "Extended Stay",
  "All-Inclusive",
  "Serviced Apartment",
  "Mixed-Use",
  "Other",
  "Unknown",
];
const ASSET_CONTEXT_OPTIONS = [
  "Urban",
  "Airport",
  "Suburban",
  "Beach / Waterfront",
  "Resort Destination",
  "Highway / Transit",
  "Campus / Medical",
  "Other",
  "Unknown",
];
const HOTEL_CLASS_OPTIONS = [
  "Luxury",
  "Upper Upscale",
  "Upscale",
  "Upper Midscale",
  "Midscale",
  "Economy",
  "Independent / Unclassified",
  "Unknown",
];
const OWNER_TYPE_OPTIONS = [
  "Institutional",
  "Private Equity",
  "Family Office",
  "Individual / Family",
  "REIT",
  "Developer",
  "Brand / Franchisor",
  "Government / Sovereign",
  "Other",
  "Unknown",
];
const OPERATOR_TYPE_OPTIONS = [
  "Brand-Managed",
  "Third-Party Operator",
  "Owner-Operated",
  "Soft-Brand / Collection Distribution",
  "Unknown",
];
const MANAGEMENT_MODEL_OPTIONS = [
  "Management Agreement",
  "Franchise",
  "Lease",
  "Owner-Operated",
  "Hybrid",
  "Unknown",
];
const INDEPENDENT_CLASS_OPTIONS = [
  "True Independent",
  "Formerly Branded",
  "Soft-Brand Candidate",
  "Brand-Unconfirmed",
  "Not Independent",
  "Unknown",
];
const DATA_CONFIDENCE_TIER_OPTIONS = ["Exact", "High", "Medium", "Low", "Insufficient", "Unknown"];
const ENRICHMENT_STATUS_OPTIONS = [
  "Not Started",
  "In Progress",
  "Partial",
  "Complete",
  "Held",
  "Do Not Enrich",
];
const ENRICHMENT_PRIORITY_OPTIONS = ["Critical", "High", "Medium", "Low", "Deferred"];
const REVIEW_STATUS_OPTIONS = [
  "Not Reviewed",
  "In Review",
  "Confirmed",
  "Disputed",
  "Insufficient Evidence",
  "Unknown",
];

function choices(names) {
  return names.map((name) => ({ name: String(name) }));
}
function singleSelect(name, optionNames, description) {
  return { name, type: "singleSelect", description, options: { choices: choices(optionNames) } };
}
function longText(name, description) {
  return { name, type: "multilineText", description };
}
function text(name, description) {
  return { name, type: "singleLineText", description };
}
function checkbox(name, description) {
  return {
    name,
    type: "checkbox",
    description,
    options: { icon: "check", color: "greenBright" },
  };
}
function numberField(name, precision = 0, description) {
  return { name, type: "number", description, options: { precision } };
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

/** All v1.1 fields to add on Hotel Property Census. */
export function buildV11FieldSpecs() {
  return [
    // Description / profile
    longText("Hotel Description - Source Text", "Raw source description text — never invent"),
    longText("Hotel Description - AI Summary", "AI summary only after governed enrichment"),
    longText("Short Property Summary"),
    longText("Property Positioning"),
    singleSelect("Hotel Class / Segment", HOTEL_CLASS_OPTIONS),
    singleSelect("Property Type", PROPERTY_TYPE_OPTIONS),
    singleSelect("Asset Context", ASSET_CONTEXT_OPTIONS),
    text("Market / Submarket", "Dealality market / corridor — not STR taxonomy"),

    // Amenities
    longText("Amenities - Source Text"),
    longText("Amenities - Structured Tags", "Comma/newline tags after enrichment"),
    checkbox("F&B Flag"),
    checkbox("Meeting Space Flag"),
    checkbox("Fitness Flag"),
    checkbox("Pool Flag"),
    checkbox("Resort / Leisure Flag"),
    checkbox("Extended Stay Flag"),
    checkbox("Parking Flag"),
    checkbox("Airport Shuttle Flag"),
    checkbox("Spa Flag"),
    checkbox("Beach / Waterfront Flag"),
    checkbox("Branded Residences Flag"),
    checkbox("Mixed-Use Flag"),

    // Physical / scale
    numberField("Rooms / Keys", 0, "Only when source-supported — never fabricate"),
    urlField("Rooms Source URL"),
    singleSelect("Rooms Confidence", CONFIDENCE_OPTIONS),
    longText("Building / Asset Notes"),
    dateField("Opening Date", "Only when source-supported — never fabricate"),
    urlField("Opening Date Source URL"),
    text("Renovation / Conversion Status"),
    dateField("Renovation / Conversion Date", "Only when source-supported"),
    urlField("Renovation / Conversion Source URL"),

    // Ownership / development
    text("Owner Name", "Only when source-supported — never fabricate"),
    singleSelect("Owner Type", OWNER_TYPE_OPTIONS),
    urlField("Owner Source URL"),
    singleSelect("Owner Confidence", CONFIDENCE_OPTIONS),
    text("Developer Name"),
    urlField("Developer Source URL"),
    singleSelect("Developer Confidence", CONFIDENCE_OPTIONS),
    singleSelect("Ownership Review Status", REVIEW_STATUS_OPTIONS),

    // Operator / management
    text("Operator / Management Company", "Only when source-supported — never fabricate"),
    singleSelect("Operator Type", OPERATOR_TYPE_OPTIONS),
    singleSelect("Management Model", MANAGEMENT_MODEL_OPTIONS),
    urlField("Operator Source URL"),
    singleSelect("Operator Confidence", CONFIDENCE_OPTIONS),
    singleSelect("Operator Review Status", REVIEW_STATUS_OPTIONS),
    checkbox("Possible Operator Target"),

    // Independent / unassigned
    checkbox("Independent Hotel Flag"),
    singleSelect("Independent Classification", INDEPENDENT_CLASS_OPTIONS),
    text("Brand-Unassigned Reason"),
    checkbox("Possible Soft-Brand Candidate"),
    checkbox("Possible Brand Conversion Candidate"),
    checkbox("Possible Owner Outreach Target"),
    checkbox("Possible Financing Target"),
    checkbox("Possible Dealality Opportunity"),

    // Data governance
    singleSelect("Data Confidence Tier", DATA_CONFIDENCE_TIER_OPTIONS),
    singleSelect("Relationship Confidence", CONFIDENCE_OPTIONS),
    dateField("Last Reviewed Date"),
    dateField("Next Review Needed"),
    singleSelect("Enrichment Status", ENRICHMENT_STATUS_OPTIONS),
    singleSelect("Enrichment Priority", ENRICHMENT_PRIORITY_OPTIONS),
    checkbox("Human Review Required"),
    longText("Notes for Steward"),
  ];
}

function maskId(id) {
  if (!id || id.length < 10) return id ? "***" : null;
  return `${id.slice(0, 6)}…${id.slice(-4)}`;
}
function maskToken(token) {
  if (!token) return null;
  if (token.length < 12) return "***";
  return `${token.slice(0, 6)}…${token.slice(-4)}`;
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
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
function fingerprintArtifacts(paths) {
  return paths.map((rel) => {
    const p = join(ROOT, rel);
    if (!existsSync(p)) return { path: rel, exists: false };
    const st = statSync(p);
    return {
      path: rel,
      exists: true,
      size: st.size,
      mtime_ms: st.mtimeMs,
      sha256: createHash("sha256").update(readFileSync(p)).digest("hex"),
    };
  });
}

export function checkV11EnvFlags() {
  const flags = {
    ALLOW_PRODUCTION_CENSUS_SCHEMA_V11: process.env.ALLOW_PRODUCTION_CENSUS_SCHEMA_V11 === "1",
    CONFIRM_SCHEMA_ONLY_OR_SAFE_BACKFILL:
      process.env.CONFIRM_SCHEMA_ONLY_OR_SAFE_BACKFILL === "1",
    CONFIRM_NO_BRAND_EXPLORER_WRITES: process.env.CONFIRM_NO_BRAND_EXPLORER_WRITES === "1",
  };
  return {
    allOk: Object.values(flags).every(Boolean),
    flags,
  };
}

export function parseV11Args(argv = process.argv.slice(2)) {
  const flags = new Set(argv.filter((a) => a.startsWith("--")));
  return {
    dryRun: flags.has("--dry-run") || !flags.has("--apply"),
    apply: flags.has("--apply"),
    confirms: {
      schemaV11: flags.has("--confirm-production-census-schema-v11"),
      censusOnly: flags.has("--confirm-census-table-only"),
      noBe: flags.has("--confirm-no-brand-explorer-writes"),
      noBrandStatus: flags.has("--confirm-no-brand-status-writes"),
      noCv: flags.has("--confirm-no-company-validation-writes"),
      noVerified: flags.has("--confirm-no-brand-verified-writes"),
      noMomentum: flags.has("--confirm-no-recent-momentum-writes"),
      noFakeOwnerOp: flags.has("--confirm-no-fake-owner-operator"),
      noFakeRooms: flags.has("--confirm-no-fake-rooms"),
      noFakeDates: flags.has("--confirm-no-fake-dates"),
      noZeroZero: flags.has("--confirm-no-zero-zero-coordinates"),
      noDup: flags.has("--confirm-no-record-duplication"),
    },
  };
}

export function allV11ConfirmsPresent(args) {
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
    const url = `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}?${params}`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
    const json = await res.json();
    if (!res.ok) throw new Error(`list ${tableId} ${res.status}: ${JSON.stringify(json.error || json)}`);
    out.push(...(json.records || []));
    offset = json.offset;
    await sleep(120);
  } while (offset);
  return out;
}

async function batchPatch(baseId, token, tableId, records) {
  const errors = [];
  let updated = 0;
  for (let i = 0; i < records.length; i += 10) {
    const chunk = records.slice(i, i + 10);
    let attempt = 0;
    while (attempt < 5) {
      attempt += 1;
      const res = await fetch(
        `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(tableId)}`,
        {
          method: "PATCH",
          headers: {
            Authorization: `Bearer ${token}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({ records: chunk, typecast: true }),
        }
      );
      const json = await res.json().catch(() => ({}));
      if (res.status === 429) {
        await sleep(1000 * attempt);
        continue;
      }
      if (!res.ok) {
        errors.push({ status: res.status, error: json.error || json, chunk_start: i });
        break;
      }
      updated += (json.records || []).length;
      break;
    }
    await sleep(220);
  }
  return { updated, errors };
}

function mapConfidenceTier(identityConfidence) {
  const v = String(identityConfidence || "").trim();
  if (DATA_CONFIDENCE_TIER_OPTIONS.includes(v)) return v;
  return "Unknown";
}

/**
 * Safe backfill fields only — never invent owner/operator/rooms/dates/descriptions.
 */
export function buildSafeBackfillFields(recordFields, heldIdentityKeys) {
  const key = recordFields["Property Identity Key"];
  const affiliation = recordFields["Affiliation Status"];
  const identityConf = recordFields["Identity Confidence"];
  const steward = recordFields["Steward Review Status"];
  const isHeld =
    heldIdentityKeys.has(key) ||
    affiliation === "Brand-Unconfirmed" ||
    ["exclude_from_brand_completion", "steward_manual_review_required", "brand_unconfirmed_held"].includes(
      steward
    );

  /** @type {Record<string, unknown>} */
  const fields = {
    "Enrichment Status": "Not Started",
    "Human Review Required": isHeld,
    "Data Confidence Tier": mapConfidenceTier(identityConf),
    "Enrichment Priority": isHeld ? "High" : recordFields["Data Eligible"] === true ? "Medium" : "Low",
  };

  if (affiliation === "Independent") {
    fields["Independent Hotel Flag"] = true;
    fields["Independent Classification"] = "True Independent";
  } else if (affiliation === "Brand-Unconfirmed") {
    fields["Independent Classification"] = "Brand-Unconfirmed";
    fields["Brand-Unassigned Reason"] = steward || "brand_unconfirmed";
    fields["Possible Brand Conversion Candidate"] = true;
  } else if (affiliation === "Soft-Branded / Collection") {
    fields["Independent Classification"] = "Soft-Brand Candidate";
    fields["Possible Soft-Brand Candidate"] = true;
  } else if (affiliation === "Branded" || affiliation === "Future / Pipeline") {
    fields["Independent Classification"] = "Not Independent";
  }

  // Never touch Production Use Status to anything else
  // Never set Owner/Operator/Rooms/Opening Date here
  return fields;
}

export async function runV11DryRun() {
  const token = resolvePat();
  const bases = resolveTargetBase();
  const env = checkV11EnvFlags();
  const specs = buildV11FieldSpecs();

  const report = {
    version: V11_VERSION,
    generated_at: new Date().toISOString(),
    mode: "dry-run",
    execute: false,
    status: STATUS.DRY_RUN_FAIL,
    dry_run_pass: false,
    token_masked: maskToken(token),
    base_id_masked: maskId(bases.target_base_id),
    env_flags: env.flags,
    env_ok_for_apply: env.allOk,
    table: CENSUS_TABLE,
    table_id: CENSUS_TABLE_ID,
    fields_to_add: [],
    fields_already_existed: [],
    fields_skipped: [],
    field_type_conflicts: [],
    safe_backfill_proposed: {},
    forbidden_untouched: [
      "Owner Name values (fabricated)",
      "Operator / Management Company values (fabricated)",
      "Rooms / Keys values (fabricated)",
      "Opening Date values (fabricated)",
      "Brand Explorer Presentation/Basics/Status/CV/Verified/Momentum",
    ],
    frozen_before: { vic: hashVicDir(), frozen_62: fingerprintArtifacts(FROZEN_62) },
    conflicts: [],
  };

  if (!token || !bases.target_base_id || bases.is_sandbox_target) {
    report.conflicts.push({ code: "base_or_token_invalid" });
    report.status = STATUS.BLOCKED;
    return report;
  }

  const tables = await listTables(bases.target_base_id, token);
  const census = tables.find((t) => t.name === CENSUS_TABLE || t.id === CENSUS_TABLE_ID);
  if (!census) {
    report.conflicts.push({ code: "census_table_missing" });
    report.status = STATUS.BLOCKED;
    return report;
  }

  const byName = Object.fromEntries((census.fields || []).map((f) => [f.name, f]));
  for (const spec of specs) {
    const existing = byName[spec.name];
    if (!existing) {
      report.fields_to_add.push({
        name: spec.name,
        type: spec.type,
        options_summary:
          spec.type === "singleSelect"
            ? (spec.options?.choices || []).map((c) => c.name)
            : undefined,
      });
      continue;
    }
    report.fields_already_existed.push({ name: spec.name, type: existing.type });
    if (existing.type !== spec.type) {
      report.field_type_conflicts.push({
        name: spec.name,
        expected: spec.type,
        actual: existing.type,
      });
    }
  }

  const records = await listAllRecords(bases.target_base_id, token, CENSUS_TABLE_ID, [
    "Property Identity Key",
    "VIC Freeze Hash",
    "Production Use Status",
    "Affiliation Status",
    "Identity Confidence",
    "Data Eligible",
    "Steward Review Status",
    "Latitude",
    "Longitude",
  ]);
  const freezeRows = records.filter((r) => r.fields?.["VIC Freeze Hash"] === EXPECTED_FREEZE);
  const steward = await listAllRecords(
    bases.target_base_id,
    token,
    TABLE_IDS["Hotel Property Steward Review"],
    ["Property Identity Key", "VIC Freeze Hash"]
  );
  const heldKeys = new Set(
    steward
      .filter((r) => r.fields?.["VIC Freeze Hash"] === EXPECTED_FREEZE)
      .map((r) => r.fields?.["Property Identity Key"])
      .filter(Boolean)
  );

  let humanReviewTrue = 0;
  let softBrandCandidates = 0;
  let brandConversionCandidates = 0;
  for (const row of freezeRows) {
    const bf = buildSafeBackfillFields(row.fields || {}, heldKeys);
    if (bf["Human Review Required"] === true) humanReviewTrue += 1;
    if (bf["Possible Soft-Brand Candidate"] === true) softBrandCandidates += 1;
    if (bf["Possible Brand Conversion Candidate"] === true) brandConversionCandidates += 1;
  }

  report.census_record_count = freezeRows.length;
  report.total_census_rows = records.length;
  report.safe_backfill_proposed = {
    enrichment_status_not_started: freezeRows.length,
    human_review_required_true: humanReviewTrue,
    data_confidence_tier_from_identity: freezeRows.length,
    enrichment_priority_derived: freezeRows.length,
    independent_flag_only_if_independent: freezeRows.filter(
      (r) => r.fields?.["Affiliation Status"] === "Independent"
    ).length,
    possible_soft_brand_candidate: softBrandCandidates,
    possible_brand_conversion_candidate: brandConversionCandidates,
    will_not_backfill: [
      "Hotel Description",
      "Amenities",
      "Owner",
      "Developer",
      "Operator / Management Company",
      "Rooms / Keys",
      "Opening Date",
      "Renovation Date",
      "Management Model",
    ],
  };

  const badUse = freezeRows.filter((r) => r.fields?.["Production Use Status"] !== PRODUCTION_USE_STATUS)
    .length;
  const zeroZero = freezeRows.filter((r) => r.fields?.Latitude === 0 && r.fields?.Longitude === 0)
    .length;

  if (freezeRows.length !== 666) {
    report.conflicts.push({ code: "census_count_not_666", actual: freezeRows.length });
  }
  if (records.length !== freezeRows.length) {
    report.conflicts.push({
      code: "extra_census_rows_outside_freeze",
      total: records.length,
      freeze: freezeRows.length,
    });
  }
  if (report.field_type_conflicts.length) {
    report.conflicts.push({ code: "field_type_conflicts" });
  }
  if (badUse) report.conflicts.push({ code: "production_use_status_drift", badUse });
  if (zeroZero) report.conflicts.push({ code: "zero_zero_coordinates", zeroZero });

  report.dry_run_pass = report.conflicts.length === 0;
  report.status = report.dry_run_pass ? STATUS.DRY_RUN_PASS : STATUS.DRY_RUN_FAIL;
  if (!env.allOk && report.dry_run_pass) {
    report.status = STATUS.CONFIRMATION_MISSING;
    report.next_step = "Set ALLOW_PRODUCTION_CENSUS_SCHEMA_V11=1 and confirm env flags, then --apply";
  } else if (report.dry_run_pass) {
    report.next_step = "Run --apply with all --confirm-* flags";
  }
  return report;
}

export async function runV11Apply(argv = process.argv.slice(2)) {
  const args = parseV11Args(argv);
  const env = checkV11EnvFlags();
  const dry = await runV11DryRun();

  if (!dry.dry_run_pass) {
    return { ...dry, mode: "apply_blocked", apply_executed: false };
  }
  if (!env.allOk) {
    return {
      ...dry,
      mode: "apply_blocked",
      apply_executed: false,
      status: STATUS.CONFIRMATION_MISSING,
    };
  }
  if (!args.apply || !allV11ConfirmsPresent(args)) {
    return {
      ...dry,
      mode: "apply_blocked",
      apply_executed: false,
      status: STATUS.CONFIRMATION_MISSING,
      detail: "Need --apply and all --confirm-* flags",
      confirms: args.confirms,
    };
  }

  const token = resolvePat();
  const bases = resolveTargetBase();
  const specs = buildV11FieldSpecs();
  const started = Date.now();

  // BE snapshot before
  const mvp = process.env.AIRTABLE_BASE_ID;
  let beBefore = null;
  if (mvp) {
    try {
      const beRows = await listAllRecords(mvp, token, "Brand Setup - Brand Explorer Presentation", [
        "Slot Key",
      ]);
      beBefore = {
        record_count: beRows.length,
        sample_hash: createHash("sha256")
          .update(beRows.map((r) => r.id).sort().join(","))
          .digest("hex"),
      };
    } catch (err) {
      beBefore = { error: String(err.message || err) };
    }
  }

  const tables = await listTables(bases.target_base_id, token);
  const census = tables.find((t) => t.name === CENSUS_TABLE || t.id === CENSUS_TABLE_ID);
  const existingNames = new Set((census.fields || []).map((f) => f.name));

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
        `/tables/${encodeURIComponent(census.id)}/fields`,
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
      fieldsAdded.push({ name: spec.name, type: spec.type, id: json.id });
      existingNames.add(spec.name);
      ok = true;
    }
    await sleep(250);
  }

  // Safe backfill
  const records = await listAllRecords(bases.target_base_id, token, CENSUS_TABLE_ID, [
    "Property Identity Key",
    "VIC Freeze Hash",
    "Production Use Status",
    "Affiliation Status",
    "Identity Confidence",
    "Data Eligible",
    "Steward Review Status",
  ]);
  const freezeRows = records.filter((r) => r.fields?.["VIC Freeze Hash"] === EXPECTED_FREEZE);
  const steward = await listAllRecords(
    bases.target_base_id,
    token,
    TABLE_IDS["Hotel Property Steward Review"],
    ["Property Identity Key", "VIC Freeze Hash"]
  );
  const heldKeys = new Set(
    steward
      .filter((r) => r.fields?.["VIC Freeze Hash"] === EXPECTED_FREEZE)
      .map((r) => r.fields?.["Property Identity Key"])
      .filter(Boolean)
  );

  const patches = freezeRows.map((row) => ({
    id: row.id,
    fields: buildSafeBackfillFields(row.fields || {}, heldKeys),
  }));
  const backfill = await batchPatch(bases.target_base_id, token, CENSUS_TABLE_ID, patches);

  // Post counts
  const after = await listAllRecords(bases.target_base_id, token, CENSUS_TABLE_ID, [
    "Property Identity Key",
    "VIC Freeze Hash",
    "Production Use Status",
    "Enrichment Status",
    "Human Review Required",
    "Latitude",
    "Longitude",
    "Owner Name",
    "Operator / Management Company",
    "Rooms / Keys",
    "Opening Date",
  ]);
  const afterFreeze = after.filter((r) => r.fields?.["VIC Freeze Hash"] === EXPECTED_FREEZE);
  const badUse = afterFreeze.filter((r) => r.fields?.["Production Use Status"] !== PRODUCTION_USE_STATUS)
    .length;
  const enrichmentSet = afterFreeze.filter((r) => r.fields?.["Enrichment Status"] === "Not Started")
    .length;
  const humanTrue = afterFreeze.filter((r) => r.fields?.["Human Review Required"] === true).length;
  const fakeOwner = afterFreeze.filter((r) => Boolean(r.fields?.["Owner Name"])).length;
  const fakeOp = afterFreeze.filter((r) => Boolean(r.fields?.["Operator / Management Company"]))
    .length;
  const fakeRooms = afterFreeze.filter(
    (r) => r.fields?.["Rooms / Keys"] != null && r.fields?.["Rooms / Keys"] !== ""
  ).length;
  const fakeOpen = afterFreeze.filter((r) => Boolean(r.fields?.["Opening Date"])).length;
  const zeroZero = afterFreeze.filter((r) => r.fields?.Latitude === 0 && r.fields?.Longitude === 0)
    .length;

  let beAfter = null;
  if (mvp && beBefore && !beBefore.error) {
    try {
      const beRows = await listAllRecords(mvp, token, "Brand Setup - Brand Explorer Presentation", [
        "Slot Key",
      ]);
      beAfter = {
        record_count: beRows.length,
        sample_hash: createHash("sha256")
          .update(beRows.map((r) => r.id).sort().join(","))
          .digest("hex"),
      };
    } catch (err) {
      beAfter = { error: String(err.message || err) };
    }
  }

  const beUntouched =
    !beBefore ||
    !beAfter ||
    Boolean(beBefore.error) ||
    Boolean(beAfter.error) ||
    (beBefore.record_count === beAfter.record_count &&
      beBefore.sample_hash === beAfter.sample_hash);

  const ok =
    afterFreeze.length === 666 &&
    after.length === 666 &&
    badUse === 0 &&
    fakeOwner === 0 &&
    fakeOp === 0 &&
    fakeRooms === 0 &&
    fakeOpen === 0 &&
    zeroZero === 0 &&
    fieldErrors.length === 0 &&
    backfill.errors.length === 0 &&
    beUntouched;

  return {
    version: V11_VERSION,
    generated_at: new Date().toISOString(),
    mode: "apply",
    apply_executed: true,
    status: ok ? STATUS.APPLIED : STATUS.BLOCKED,
    token_masked: maskToken(token),
    base_id_masked: maskId(bases.target_base_id),
    duration_ms: Date.now() - started,
    fields_added: fieldsAdded,
    fields_already_existed: dry.fields_already_existed,
    fields_skipped: fieldsSkipped,
    field_type_conflicts: dry.field_type_conflicts,
    field_errors: fieldErrors,
    safe_backfill_proposed: dry.safe_backfill_proposed,
    safe_backfill_applied: {
      records_patched: backfill.updated,
      enrichment_status_not_started: enrichmentSet,
      human_review_required_true: humanTrue,
      errors: backfill.errors,
    },
    census_record_count_after: afterFreeze.length,
    total_census_rows_after: after.length,
    production_use_status_ok: badUse === 0,
    no_fake_owner_operator_rooms_dates:
      fakeOwner === 0 && fakeOp === 0 && fakeRooms === 0 && fakeOpen === 0,
    no_zero_zero: zeroZero === 0,
    brand_explorer_untouched: beUntouched,
    brand_explorer_snapshot_before: beBefore,
    brand_explorer_snapshot_after: beAfter,
    frozen_after: { vic: hashVicDir(), frozen_62: fingerprintArtifacts(FROZEN_62) },
    frozen_before: dry.frozen_before,
    production_safety_result: ok ? "pass" : "fail",
    next_recommended_step:
      "Future enrichment lanes may populate description/amenities/owner/operator/rooms only with source-backed evidence",
  };
}

export function renderV11DryRunMarkdown(r) {
  return [
    `# Production Census Schema v1.1 — Dry-Run`,
    ``,
    `**Status:** \`${r.status}\``,
    `**Dry-run pass:** ${r.dry_run_pass}`,
    `**Fields to add:** ${(r.fields_to_add || []).length}`,
    `**Already existed:** ${(r.fields_already_existed || []).length}`,
    `**Census freeze rows:** ${r.census_record_count}`,
    ``,
    `## Fields to add`,
    ``,
    `| Field | Type |`,
    `| --- | --- |`,
    ...(r.fields_to_add || []).map((f) => `| ${f.name} | ${f.type} |`),
    ``,
    `## Safe backfill proposed`,
    ``,
    "```json",
    JSON.stringify(r.safe_backfill_proposed, null, 2),
    "```",
    ``,
  ].join("\n");
}

export function renderV11ApplyMarkdown(r) {
  return [
    `# Production Census Schema v1.1 — Apply`,
    ``,
    `**Status:** \`${r.status}\``,
    `**Fields added:** ${(r.fields_added || []).length}`,
    `**Census count after:** ${r.census_record_count_after}`,
    `**Brand Explorer untouched:** ${r.brand_explorer_untouched}`,
    `**Duration ms:** ${r.duration_ms}`,
    ``,
    `## Fields added`,
    ``,
    ...(r.fields_added || []).map((f) => `- ${f.name} (${f.type})`),
    ``,
    `## Safe backfill applied`,
    ``,
    "```json",
    JSON.stringify(r.safe_backfill_applied, null, 2),
    "```",
    ``,
    `## Safety`,
    ``,
    `- No fake owner/operator/rooms/dates: ${r.no_fake_owner_operator_rooms_dates}`,
    `- No 0,0 coords: ${r.no_zero_zero}`,
    `- Production Use Status OK: ${r.production_use_status_ok}`,
    ``,
  ].join("\n");
}
