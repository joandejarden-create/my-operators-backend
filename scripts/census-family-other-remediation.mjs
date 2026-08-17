#!/usr/bin/env node
/**
 * Remediate Family / Source Family = Other on DR OSM intake rows
 * → proper regional / global family names.
 *
 * Default dry-run. Live: --apply --enable-production-writes + confirms + env.
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  resolveIntakeParentFamily,
} from "../lib/independent-census/intake-census-field-normalize.js";
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
  "Country",
  "Property Identity Key",
  "VIC Freeze Hash",
];

function parseArgs(argv = process.argv.slice(2)) {
  const confirms = {};
  for (const f of INTAKE_APPLY_CONFIRMS) confirms[f] = argv.includes(f);
  return {
    apply: argv.includes("--apply") && argv.includes("--enable-production-writes"),
    confirms,
    allConfirmsOk: Object.values(confirms).every(Boolean),
  };
}

async function listDr(baseId, token) {
  const out = [];
  let offset;
  const base = new URLSearchParams({
    filterByFormula: "{Country}='Dominican Republic'",
  });
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
  const records = await listDr(baseId, token);
  const proposals = [];
  const failed = [];

  for (const rec of records) {
    const fields = rec.fields || {};
    const fam = String(fields["Family / Source Family"] || "").trim();
    if (!/^other$/i.test(fam)) continue;

    const brand = String(fields["Current Brand"] || "").trim();
    const resolved = resolveIntakeParentFamily(brand, {
      propertyName: fields["Property Name"],
    });
    if (!resolved.sourceFamily || /^other$/i.test(resolved.sourceFamily)) {
      failed.push({
        record_id: rec.id,
        property_name: fields["Property Name"],
        brand,
        reason: "could_not_resolve_family",
      });
      continue;
    }

    proposals.push({
      record_id: rec.id,
      property_name: fields["Property Name"],
      brand,
      family_before: fam,
      family_after: resolved.sourceFamily,
      brand_family_after: resolved.brandFamily,
      patch: {
        "Family / Source Family": resolved.sourceFamily,
        "Brand Family": resolved.brandFamily,
      },
    });
  }

  let patched = [];
  const errors = [];
  if (args.apply && proposals.length) {
    try {
      // typecast:true creates missing Family / Source Family select options
      // when Meta API field PATCH is blocked on this base/token.
      patched = await patchRecords(
        baseId,
        token,
        proposals.map((p) => ({ id: p.record_id, fields: p.patch }))
      );
    } catch (err) {
      errors.push(err?.message || String(err));
    }
  }

  const byFamily = {};
  for (const p of proposals) {
    byFamily[p.family_after] = (byFamily[p.family_after] || 0) + 1;
  }

  const report = {
    version: "census-family-other-remediation-v1",
    status: args.apply ? (errors.length ? "partial" : "applied") : "dry_run",
    apply_executed: Boolean(args.apply),
    airtable_writes: Boolean(args.apply && patched.length),
    targets_other: proposals.length + failed.length,
    proposal_count: proposals.length,
    failed_count: failed.length,
    patched_count: patched.length,
    by_family_after: byFamily,
    proposals,
    failed,
    errors,
  };

  const out = `reports/census-family-other-remediation-${args.apply ? "applied" : "dry-run"}.json`;
  mkdirSync(dirname(join(root, out)), { recursive: true });
  writeFileSync(join(root, out), JSON.stringify(report, null, 2));
  writeFileSync(
    join(root, "docs/data-intelligence/census-family-other-remediation.md"),
    [
      `# Family / Source Family — Other remediation`,
      ``,
      `**Status:** \`${report.status}\``,
      ``,
      `| Family after | Count |`,
      `| --- | ---: |`,
      ...Object.entries(byFamily)
        .sort((a, b) => b[1] - a[1])
        .map(([k, v]) => `| ${k} | ${v} |`),
      ``,
    ].join("\n")
  );

  console.log(
    JSON.stringify(
      {
        ok: report.status !== "blocked",
        status: report.status,
        output: out,
        proposal_count: report.proposal_count,
        failed_count: report.failed_count,
        patched_count: report.patched_count,
        by_family_after: byFamily,
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
