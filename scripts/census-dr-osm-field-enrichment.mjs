#!/usr/bin/env node
/**
 * Enrich DR OSM Hotel Property Census rows:
 * Continent, Sub-Continent, Market, Submarket, Address, Phone, Rooms / Keys,
 * and brand-homepage → property Official Property URL fixes.
 *
 * Default dry-run. Live: --apply --enable-production-writes + confirms + env.
 */
import "../load-env.js";
import { mkdirSync, readFileSync, writeFileSync, existsSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import { buildDrOsmFieldEnrichmentProposal } from "../lib/independent-census/dr-osm-hpc-field-enrichment.js";
import { INTAKE_APPLY_CONFIRMS } from "../lib/independent-census/intake-autopilot-controlled.js";
import { checkIntakeApplyEnv } from "../lib/independent-census/intake-autopilot-apply.js";
import {
  resolvePat,
  resolveTargetBase,
} from "../lib/research-engine-v2/production-census-schema-create.js";
import { TABLE_IDS } from "../lib/research-engine-v2/production-census-write.js";
import {
  assertProductionCensusWriteTarget,
  productionHotelPropertyCensus,
  PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
} from "../lib/research-engine-v2/production-census-source-of-truth.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const CENSUS_TABLE_ID =
  TABLE_IDS["Hotel Property Census"] || PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID;

const READ_FIELDS = [
  "Property Name",
  "Current Brand",
  "Country",
  "City",
  "State / Region",
  "Address",
  "Phone",
  "Continent",
  "Sub-Continent",
  "Market",
  "Submarket",
  "Rooms / Keys",
  "Official Property URL",
  "Property Identity Key",
  "VIC Freeze Hash",
];

function parseArgs(argv = process.argv.slice(2)) {
  const get = (name, fb = "") => {
    const i = argv.indexOf(name);
    return i >= 0 ? argv[i + 1] : fb;
  };
  const confirms = {};
  for (const f of INTAKE_APPLY_CONFIRMS) confirms[f] = argv.includes(f);
  return {
    apply: argv.includes("--apply") && argv.includes("--enable-production-writes"),
    osmPath: get(
      "--osm",
      "reports/independent-census-osm-dry-run-osm-dominican-republic-hotel-focused-2026-08-07.json"
    ),
    googlePath: get(
      "--google",
      "reports/census-intake-google-places-url-dry-run-osm-dominican-republic-hotel-focused-2026-08-07.json"
    ),
    confirms,
    allConfirmsOk: Object.values(confirms).every(Boolean),
  };
}

function loadOsmById(rel) {
  const p = join(root, rel);
  if (!existsSync(p)) return new Map();
  const json = JSON.parse(readFileSync(p, "utf8"));
  const map = new Map();
  for (const c of json.candidates || []) {
    if (c.sourceRecordId) map.set(String(c.sourceRecordId), c);
  }
  return map;
}

function loadGoogleById(rel) {
  const p = join(root, rel);
  if (!existsSync(p)) return new Map();
  const json = JSON.parse(readFileSync(p, "utf8"));
  const map = new Map();
  for (const r of json.results || []) {
    if (r.source_record_id) map.set(String(r.source_record_id), r);
  }
  return map;
}

async function listOsmDr(baseId, token) {
  const out = [];
  let offset;
  const formula =
    "AND({Country}='Dominican Republic',FIND('independent_census_dr_osm',{VIC Freeze Hash}&''))";
  const base = new URLSearchParams({ filterByFormula: formula });
  for (const f of READ_FIELDS) base.append("fields[]", f);
  do {
    const params = new URLSearchParams(base);
    if (offset) params.set("offset", offset);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(CENSUS_TABLE_ID)}?${params}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(JSON.stringify(json.error || json));
    out.push(...(json.records || []));
    offset = json.offset;
  } while (offset);
  return out;
}

async function patchRecords(baseId, token, records) {
  const updated = [];
  for (let i = 0; i < records.length; i += 10) {
    const chunk = records.slice(i, i + 10);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(CENSUS_TABLE_ID)}`,
      {
        method: "PATCH",
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ records: chunk, typecast: true }),
      }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(JSON.stringify(json.error || json));
    updated.push(...(json.records || []));
  }
  return updated;
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

  const osmBySourceId = loadOsmById(args.osmPath);
  const googleBySourceId = loadGoogleById(args.googlePath);
  const token = resolvePat();
  const baseId = resolveTargetBase()?.target_base_id;
  const rows = await listOsmDr(baseId, token);

  const proposals = [];
  const fieldHits = {
    Continent: 0,
    "Sub-Continent": 0,
    Market: 0,
    Submarket: 0,
    Address: 0,
    Phone: 0,
    "Rooms / Keys": 0,
    "Official Property URL": 0,
  };
  const stewardUrl = [];

  for (const rec of rows) {
    const built = buildDrOsmFieldEnrichmentProposal(rec.fields || {}, {
      osmBySourceId,
      googleBySourceId,
    });
    if (built.reasons.includes("official_url_brand_homepage_needs_steward")) {
      stewardUrl.push({
        id: rec.id,
        n: rec.fields["Property Name"],
        url: rec.fields["Official Property URL"],
      });
    }
    if (!built.ok) continue;
    for (const k of Object.keys(built.patch)) {
      if (fieldHits[k] != null) fieldHits[k]++;
    }
    proposals.push({
      id: rec.id,
      property_name: rec.fields["Property Name"],
      identity_key: rec.fields["Property Identity Key"],
      patch: built.patch,
      reasons: built.reasons,
    });
  }

  const envCheck = checkIntakeApplyEnv();
  const doWrite = Boolean(args.apply && args.allConfirmsOk && envCheck.allOk);
  let patched = [];
  if (doWrite && proposals.length) {
    patched = await patchRecords(
      baseId,
      token,
      proposals.map((p) => ({ id: p.id, fields: p.patch }))
    );
  }

  const report = {
    status: doWrite ? "applied" : "dry_run",
    scanned: rows.length,
    proposal_count: proposals.length,
    patched_count: patched.length,
    field_hits: fieldHits,
    steward_brand_homepage_remaining: stewardUrl,
    airtable_writes: doWrite,
    proposals: proposals.slice(0, 200),
  };
  mkdirSync(join(root, "reports"), { recursive: true });
  const out = doWrite
    ? "reports/census-dr-osm-field-enrichment-applied.json"
    : "reports/census-dr-osm-field-enrichment-dry-run.json";
  writeFileSync(join(root, out), JSON.stringify(report, null, 2));
  console.log(
    JSON.stringify(
      {
        ok: true,
        status: report.status,
        output: out,
        scanned: report.scanned,
        proposal_count: report.proposal_count,
        patched_count: report.patched_count,
        field_hits: report.field_hits,
        steward_brand_homepage_remaining:
          report.steward_brand_homepage_remaining.length,
        airtable_writes: report.airtable_writes,
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
