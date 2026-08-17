#!/usr/bin/env node
/**
 * Remediate OSM DR intake rows:
 * - Brand Family short aliases → census-canonical display (IHG Hotels & Resorts, …)
 * - City=Unknown → extract from name/URL or clear; fill State / Region when High
 *
 * Default dry-run. Live: --apply --enable-production-writes + confirms + env.
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import { buildIntakeCensusFormatRemediationPatch } from "../lib/independent-census/intake-census-field-normalize.js";
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
  "Brand Family",
  "Family / Source Family",
  "City",
  "State / Region",
  "Country",
  "Official Property URL",
  "Source URL",
  "Property Identity Key",
  "VIC Freeze Hash",
  "Affiliation Status",
];

function parseArgs(argv = process.argv.slice(2)) {
  const confirms = {};
  for (const f of INTAKE_APPLY_CONFIRMS) confirms[f] = argv.includes(f);
  return {
    apply: argv.includes("--apply") && argv.includes("--enable-production-writes"),
    osmOnly: !argv.includes("--all-dr"),
    confirms,
    allConfirmsOk: Object.values(confirms).every(Boolean),
  };
}

async function listDr(baseId, token, osmOnly) {
  const out = [];
  let offset;
  const formula = osmOnly
    ? "AND({Country}='Dominican Republic',FIND('independent_census_dr_osm',{VIC Freeze Hash}&''))"
    : "{Country}='Dominican Republic'";
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
  const envCheck = checkIntakeApplyEnv();
  if (args.apply && !(args.allConfirmsOk && envCheck.allOk)) {
    console.error("Apply blocked", {
      confirms: Object.entries(args.confirms)
        .filter(([, v]) => !v)
        .map(([k]) => k),
      env: envCheck.missing,
    });
    process.exit(1);
  }

  const writeTarget = assertProductionCensusWriteTarget({
    baseName: productionHotelPropertyCensus.baseName,
    tableName: productionHotelPropertyCensus.tableName,
    tableId: CENSUS_TABLE_ID,
  });
  if (!writeTarget.ok) {
    console.error("Wrong write target");
    process.exit(1);
  }

  const token = resolvePat();
  const baseId = resolveTargetBase()?.target_base_id;
  const records = await listDr(baseId, token, args.osmOnly);
  const proposals = [];

  for (const rec of records) {
    const fields = { Country: "Dominican Republic", ...(rec.fields || {}) };
    const { patch, reasons } = buildIntakeCensusFormatRemediationPatch(fields);
    if (!Object.keys(patch).length) continue;
    proposals.push({
      record_id: rec.id,
      property_name: fields["Property Name"],
      brand: fields["Current Brand"],
      patch,
      reasons,
      before: {
        city: fields.City || null,
        state: fields["State / Region"] || null,
        brand_family: fields["Brand Family"] || null,
        family: fields["Family / Source Family"] || null,
      },
    });
  }

  let patched = [];
  const errors = [];
  if (args.apply && proposals.length) {
    try {
      patched = await patchRecords(
        baseId,
        token,
        proposals.map((p) => ({ id: p.record_id, fields: p.patch }))
      );
    } catch (err) {
      errors.push(err?.message || String(err));
    }
  }

  const report = {
    version: "census-intake-brand-city-remediation-v1",
    status: args.apply ? (errors.length ? "partial" : "applied") : "dry_run",
    apply_executed: Boolean(args.apply),
    airtable_writes: Boolean(args.apply && patched.length),
    scanned: records.length,
    proposal_count: proposals.length,
    patched_count: patched.length,
    city_fixes: proposals.filter((p) => p.reasons.city_fixed).length,
    brand_family_fixes: proposals.filter((p) => p.reasons.brand_family_fixed)
      .length,
    state_fixes: proposals.filter((p) => p.reasons.state_fixed).length,
    proposals: proposals.slice(0, 80),
    errors,
  };

  const out = `reports/census-intake-brand-city-remediation-${args.apply ? "applied" : "dry-run"}.json`;
  mkdirSync(dirname(join(root, out)), { recursive: true });
  writeFileSync(join(root, out), JSON.stringify(report, null, 2));
  writeFileSync(
    join(root, "docs/data-intelligence/census-intake-brand-city-remediation.md"),
    [
      `# Intake Brand Family + City remediation`,
      ``,
      `**Status:** \`${report.status}\``,
      ``,
      `| Metric | Count |`,
      `| --- | ---: |`,
      `| Scanned | ${report.scanned} |`,
      `| Proposals | ${report.proposal_count} |`,
      `| City fixes | ${report.city_fixes} |`,
      `| Brand Family fixes | ${report.brand_family_fixes} |`,
      `| State fixes | ${report.state_fixes} |`,
      `| Patched | ${report.patched_count} |`,
      ``,
    ].join("\n")
  );

  console.log(
    JSON.stringify(
      {
        ok: true,
        status: report.status,
        output: out,
        scanned: report.scanned,
        proposal_count: report.proposal_count,
        city_fixes: report.city_fixes,
        brand_family_fixes: report.brand_family_fixes,
        state_fixes: report.state_fixes,
        patched_count: report.patched_count,
        sample: proposals.slice(0, 12).map((p) => ({
          n: p.property_name,
          patch: p.patch,
          before: p.before,
        })),
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
