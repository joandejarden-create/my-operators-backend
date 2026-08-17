#!/usr/bin/env node
/**
 * Insert Donoma Las Terrenas (Autograph) into Hotel Property Census — only after
 * live census re-check proves it is not already present.
 *
 * Coverage allowlist only (no address/phone/rooms/lat/lng).
 * Default dry-run. Live: --apply --enable-production-writes + confirms + env.
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  buildCoverageInsertFields,
  COVERAGE_INSERT_ALLOWED_FIELDS,
  COVERAGE_INSERT_NEVER_FIELDS,
} from "../lib/research-engine-v2/census-autopilot-coverage-reconciliation.js";
import { createHotelPropertyCensusRecords } from "../lib/research-engine-v2/census-autopilot-discovery-insert-apply.js";
import { INTAKE_APPLY_CONFIRMS } from "../lib/independent-census/intake-autopilot-controlled.js";
import { checkIntakeApplyEnv } from "../lib/independent-census/intake-autopilot-apply.js";
import {
  resolvePat,
  resolveTargetBase,
} from "../lib/research-engine-v2/production-census-schema-create.js";
import {
  assertProductionCensusWriteTarget,
  productionHotelPropertyCensus,
  PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
} from "../lib/research-engine-v2/production-census-source-of-truth.js";
import { TABLE_IDS } from "../lib/research-engine-v2/production-census-write.js";
import { buildCanonicalBrandDictionary } from "../lib/research-engine-v2/census-brand-canonical-dictionary.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const CENSUS_TABLE_ID =
  TABLE_IDS["Hotel Property Census"] || PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID;

const DONOMA = {
  property_name:
    "Donoma Las Terrenas Resort & Villas, Autograph Collection",
  brand: "Autograph Collection",
  parent_company: "Marriott",
  source_family: "Marriott",
  city: "Las Terrenas",
  state_region: "Samaná",
  country: "Dominican Republic",
  official_property_url:
    "https://www.marriott.com/en-us/hotels/azsak-donoma-las-terrenas-resort-and-villas-autograph-collection/overview",
  official_directory_url:
    "https://www.marriott.com/en-us/hotel-sitemap/dominican-republic-hotel-sitemap",
  identity_key: "ind_marriott_do_azsak",
  official_property_id: "azsak",
  identity_confidence: "High",
  source_confidence: "High",
  distinctive_tokens: ["donoma", "azsak", "popak"],
};

function norm(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function parseArgs(argv = process.argv.slice(2)) {
  const confirms = {};
  for (const f of INTAKE_APPLY_CONFIRMS) confirms[f] = argv.includes(f);
  return {
    apply: argv.includes("--apply") && argv.includes("--enable-production-writes"),
    confirms,
    allConfirmsOk: Object.values(confirms).every(Boolean),
  };
}

async function listDrCensus(baseId, token) {
  const fields = [
    "Property Name",
    "Canonical Property Name",
    "Official Property URL",
    "Property Identity Key",
    "Current Brand",
    "City",
  ];
  const formula = "AND({Country}='Dominican Republic')";
  const out = [];
  let offset;
  do {
    const p = new URLSearchParams({ filterByFormula: formula, pageSize: "100" });
    for (const f of fields) p.append("fields[]", f);
    if (offset) p.set("offset", offset);
    const res = await fetch(
      `https://api.airtable.com/v0/${baseId}/${CENSUS_TABLE_ID}?${p}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(JSON.stringify(json.error || json));
    out.push(...(json.records || []));
    offset = json.offset;
  } while (offset);
  return out;
}

function findExisting(records) {
  const hits = [];
  for (const r of records) {
    const f = r.fields || {};
    const hay = norm(
      `${f["Property Name"]} ${f["Canonical Property Name"]} ${f["Official Property URL"]} ${f["Property Identity Key"]}`
    );
    const reasons = [];
    for (const t of DONOMA.distinctive_tokens) {
      if (hay.includes(norm(t))) reasons.push(`token:${t}`);
    }
    if (norm(f["Property Identity Key"]) === norm(DONOMA.identity_key)) {
      reasons.push("exact_identity_key");
    }
    if (!reasons.length) continue;
    hits.push({
      id: r.id,
      property_name: f["Property Name"],
      city: f.City,
      url: f["Official Property URL"],
      key: f["Property Identity Key"],
      reasons,
    });
  }
  return hits;
}

async function main() {
  const args = parseArgs();
  const writeTarget = assertProductionCensusWriteTarget({
    baseName: productionHotelPropertyCensus.baseName,
    tableName: productionHotelPropertyCensus.tableName,
    tableId: CENSUS_TABLE_ID,
  });
  if (!writeTarget.ok) {
    console.error(JSON.stringify({ ok: false, blocked: "wrong_write_target" }));
    process.exit(1);
  }

  const token = resolvePat();
  const baseId = resolveTargetBase()?.target_base_id;
  console.log("[donoma] live census re-check…");
  const records = await listDrCensus(baseId, token);
  const existing = findExisting(records);
  if (existing.length) {
    const report = {
      status: "blocked_already_in_census",
      airtable_writes: false,
      existing,
      rule: "Census re-check found distinctive match — insert aborted",
    };
    mkdirSync(join(root, "reports"), { recursive: true });
    writeFileSync(
      join(root, "reports/census-dr-donoma-insert-blocked-existing.json"),
      JSON.stringify(report, null, 2)
    );
    console.log(JSON.stringify({ ok: true, ...report }, null, 2));
    return;
  }

  const brandDictionary = buildCanonicalBrandDictionary({});
  const built = buildCoverageInsertFields(DONOMA, {
    brandDictionary,
    human_review_required: false,
  });
  // buildCoverageInsertFields returns sanitizeCoverageInsertFields → { fields, dropped }
  const fields = built?.fields && typeof built.fields === "object" ? built.fields : built;
  const dropped = built?.dropped || [];

  // Hard strip never-fields
  for (const k of COVERAGE_INSERT_NEVER_FIELDS) delete fields[k];
  for (const k of Object.keys(fields)) {
    if (!COVERAGE_INSERT_ALLOWED_FIELDS.includes(k)) delete fields[k];
  }

  const validation = {
    pass: true,
    failed: [],
  };
  for (const req of [
    "Property Name",
    "Property Identity Key",
    "Current Brand",
    "Country",
    "City",
    "Official Property URL",
  ]) {
    if (!fields[req]) {
      validation.pass = false;
      validation.failed.push(`missing_${req}`);
    }
  }
  if (norm(fields.City) === "unknown") {
    validation.pass = false;
    validation.failed.push("city_unknown");
  }

  const envCheck = checkIntakeApplyEnv();
  const doWrite = Boolean(
    args.apply && args.allConfirmsOk && envCheck.allOk && validation.pass
  );

  const report = {
    status: doWrite ? "applied" : "dry_run",
    airtable_writes: doWrite,
    census_recheck: {
      dr_rows_scanned: records.length,
      existing_hits: existing.length,
      decision: "true_missing_safe_to_insert",
    },
    validation,
    field_mapping: Object.fromEntries(
      Object.keys(fields).map((k) => [k, `coverage_insert:${k}`])
    ),
    sanitized_payload_preview: fields,
    dropped_fields: dropped,
    identity_key: DONOMA.identity_key,
  };

  let created = [];
  if (doWrite) {
    const createResult = await createHotelPropertyCensusRecords(baseId, token, [
      { fields },
    ]);
    created = Array.isArray(createResult)
      ? createResult
      : createResult?.created || [];
    report.created_ids = created.map((r) => r.id);
    report.patched_count = created.length;
  }

  mkdirSync(join(root, "reports"), { recursive: true });
  const out = doWrite
    ? "reports/census-dr-donoma-insert-applied.json"
    : "reports/census-dr-donoma-insert-dry-run.json";
  writeFileSync(join(root, out), JSON.stringify(report, null, 2));
  console.log(
    JSON.stringify(
      {
        ok: true,
        status: report.status,
        output: out,
        airtable_writes: doWrite,
        validation,
        identity_key: DONOMA.identity_key,
        city: fields.City,
        brand: fields["Current Brand"],
        created: report.created_ids || [],
      },
      null,
      2
    )
  );
  if (!validation.pass) process.exitCode = 1;
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
