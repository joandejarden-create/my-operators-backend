#!/usr/bin/env node
/**
 * Plan (and optionally apply) fill-blank geography enrichment for Hotel Census.
 *
 * Fills: Region, Sub-Continent (from country map only).
 * Does NOT fill: Market, Submarket (STR import path).
 *
 * Usage:
 *   node scripts/plan-hotel-census-geography-enrichment.mjs
 *   node scripts/plan-hotel-census-geography-enrichment.mjs --apply
 */

import fs from "node:fs";
import path from "node:path";
import "../load-env.js";
import { getPlatformBase } from "../lib/hotel-census/platform-base.js";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import {
  CENSUS_SUB_CONTINENT_FIELD,
  MAP_GEOGRAPHY_ENRICHMENT,
  proposeGeographyEnrichment,
  validateGeographyProposal,
} from "../lib/hotel-census/geography-enrichment-contract.js";

const APPLY = process.argv.includes("--apply");
const REPORT_DIR = path.join(process.cwd(), "reports");
const STAMP = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);

const SELECT_FIELDS = [
  CENSUS_FIELDS.name,
  CENSUS_FIELDS.country,
  CENSUS_FIELDS.city,
  CENSUS_FIELDS.region,
  CENSUS_SUB_CONTINENT_FIELD,
  CENSUS_FIELDS.market,
  "Submarket",
  "STR Number",
];

async function main() {
  const base = getPlatformBase();
  const records = await base(HOTEL_CENSUS_TABLE)
    .select({ fields: SELECT_FIELDS, pageSize: 100 })
    .all();

  const plans = [];
  const summary = {
    total: records.length,
    proposeRegion: 0,
    proposeSubContinent: 0,
    proposeAny: 0,
    skippedNoCountry: 0,
    validationErrors: 0,
    applied: 0,
    applyErrors: 0,
  };

  for (const rec of records) {
    const row = rec.fields;
    if (!String(row[CENSUS_FIELDS.country] ?? "").trim()) {
      summary.skippedNoCountry++;
      continue;
    }

    const { fields, sources, skipped } = proposeGeographyEnrichment(row);
    if (!Object.keys(fields).length) continue;

    const validation = validateGeographyProposal(fields);
    if (!validation.pass) {
      summary.validationErrors++;
      plans.push({
        recordId: rec.id,
        name: row[CENSUS_FIELDS.name],
        country: row[CENSUS_FIELDS.country],
        status: "validation_error",
        errors: validation.errors.join("; "),
        proposed: fields,
      });
      continue;
    }

    if (fields[CENSUS_FIELDS.region]) summary.proposeRegion++;
    if (fields[CENSUS_SUB_CONTINENT_FIELD]) summary.proposeSubContinent++;
    summary.proposeAny++;

    plans.push({
      recordId: rec.id,
      name: row[CENSUS_FIELDS.name],
      country: row[CENSUS_FIELDS.country],
      city: row[CENSUS_FIELDS.city],
      existingRegion: row[CENSUS_FIELDS.region] ?? "",
      existingSubContinent: row[CENSUS_SUB_CONTINENT_FIELD] ?? "",
      proposed: fields,
      sources,
      skipped,
      status: "ready",
    });
  }

  fs.mkdirSync(REPORT_DIR, { recursive: true });
  const jsonPath = path.join(REPORT_DIR, `hotel-census-geography-plan-${STAMP}.json`);
  const csvPath = path.join(REPORT_DIR, `hotel-census-geography-plan-${STAMP}.csv`);

  fs.writeFileSync(
    jsonPath,
    JSON.stringify({ generatedAt: new Date().toISOString(), apply: APPLY, summary, plans }, null, 2)
  );

  const csvHeader =
    "recordId,name,country,city,existingRegion,existingSubContinent,proposedRegion,proposedSubContinent,status";
  const csvRows = plans.map((p) => {
    const pr = p.proposed?.[CENSUS_FIELDS.region] ?? "";
    const ps = p.proposed?.[CENSUS_SUB_CONTINENT_FIELD] ?? "";
    const esc = (v) => `"${String(v ?? "").replace(/"/g, '""')}"`;
    return [
      p.recordId,
      esc(p.name),
      esc(p.country),
      esc(p.city),
      esc(p.existingRegion),
      esc(p.existingSubContinent),
      esc(pr),
      esc(ps),
      p.status,
    ].join(",");
  });
  fs.writeFileSync(csvPath, [csvHeader, ...csvRows].join("\n"));

  console.log("Hotel Census geography plan");
  console.log("  Total rows:", summary.total);
  console.log("  Propose Region:", summary.proposeRegion);
  console.log("  Propose Sub-Continent:", summary.proposeSubContinent);
  console.log("  Rows with any proposal:", summary.proposeAny);
  console.log("  Skipped (no country):", summary.skippedNoCountry);
  console.log("  Validation errors:", summary.validationErrors);
  console.log("  Reports:", jsonPath, csvPath);

  if (!APPLY) {
    console.log("\nDry run only. Pass --apply to write fill-blank Region / Sub-Continent.");
    return;
  }

  const ready = plans.filter((p) => p.status === "ready");
  const BATCH = 10;
  for (let i = 0; i < ready.length; i += BATCH) {
    const chunk = ready.slice(i, i + BATCH);
    try {
      await base(HOTEL_CENSUS_TABLE).update(
        chunk.map((p) => ({ id: p.recordId, fields: p.proposed }))
      );
      summary.applied += chunk.length;
    } catch (err) {
      summary.applyErrors += chunk.length;
      console.error("Apply batch failed:", err?.message || err);
    }
  }

  console.log("\nApply complete:", summary.applied, "updated,", summary.applyErrors, "errors");
  fs.writeFileSync(
    path.join(REPORT_DIR, `hotel-census-geography-apply-${STAMP}.json`),
    JSON.stringify({ generatedAt: new Date().toISOString(), summary }, null, 2)
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
