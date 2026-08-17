#!/usr/bin/env node
/**
 * Create missing SLH CALA Hotel Census rows from slh.com directory gaps.
 * Also sets Affiliation on ambiguous duplicate-name steward matches.
 *
 *   node scripts/apply-slh-census-manual-additions.mjs --dry-run
 *   node scripts/apply-slh-census-manual-additions.mjs --apply
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import Airtable from "airtable";
import {
  planSlhCensusManualAdditions,
  rowToAirtableFields,
  findDuplicateCandidates,
  validateSlhCensusManualRow,
  HOTEL_CENSUS_TABLE,
} from "../lib/hotel-census/plan-slh-census-manual-additions.js";
import { CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { SLH_AFFILIATION, SLH_PARENT_COMPANY, isCalaCountry } from "../lib/slh-census-enrichment.js";

const REPORT = join("reports", "slh-census-manual-additions-log.json");
const APPLY = process.argv.includes("--apply");
const DRY_RUN = process.argv.includes("--dry-run") || !APPLY;

async function main() {
  mkdirSync("reports", { recursive: true });
  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID_ALT
  );

  console.log(`=== SLH CALA census creates (${DRY_RUN ? "DRY RUN" : "LIVE"}) ===\n`);

  const censusRows = await base(HOTEL_CENSUS_TABLE)
    .select({
      fields: [
        "name",
        "Website",
        CENSUS_FIELDS.affiliation,
        CENSUS_FIELDS.parentCompany,
        CENSUS_FIELDS.country,
        CENSUS_FIELDS.city,
      ],
      pageSize: 100,
    })
    .all();
  const calaRows = censusRows.filter((r) => isCalaCountry(r.fields[CENSUS_FIELDS.country]));
  console.log("CALA census rows:", calaRows.length);

  const plan = await planSlhCensusManualAdditions(calaRows, {
    onProgress: (msg) => console.log(" ", msg),
  });

  console.log("Create candidates:", plan.createRows.length);
  console.log("Duplicate affiliation updates:", plan.duplicateAffiliationUpdates.length);

  const log = {
    generatedAt: new Date().toISOString(),
    dryRun: DRY_RUN,
    createRows: [],
    affiliationUpdates: [],
    skipped: [],
  };

  let created = 0;
  let skipped = 0;
  let affiliationUpdated = 0;

  for (const row of plan.createRows) {
    const v = validateSlhCensusManualRow(row);
    if (!v.pass) {
      skipped++;
      log.skipped.push({ portfolioKey: row.portfolioKey, reason: "validation", errors: v.errors });
      continue;
    }
    const fields = rowToAirtableFields(row);
    const dupes = findDuplicateCandidates(censusRows, row);
    if (dupes.length) {
      skipped++;
      log.skipped.push({
        portfolioKey: row.portfolioKey,
        action: "skip_duplicate",
        recordIds: dupes.map((d) => d.id),
        duplicateNames: dupes.map((d) => d.fields.name),
      });
      console.log(`SKIP duplicate ${row.name} → ${dupes.map((d) => d.id).join(", ")}`);
      continue;
    }

    if (DRY_RUN) {
      log.createRows.push({ portfolioKey: row.portfolioKey, action: "create_dry_run", fields });
      console.log(`[dry-run] CREATE ${row.name} (${row.country})`);
      continue;
    }

    const [rec] = await base(HOTEL_CENSUS_TABLE).create([{ fields }], { typecast: true });
    created++;
    censusRows.push(rec);
    log.createRows.push({
      portfolioKey: row.portfolioKey,
      action: "create",
      recordId: rec.id,
      fields,
    });
    console.log(`CREATED ${row.name} → ${rec.id}`);
  }

  // Ambiguous exact-name duplicates: set Affiliation on both census rows
  for (const row of plan.duplicateAffiliationUpdates) {
    const name = String(row.censusName || "").trim();
    const country = String(row.censusCountry || "").trim();
    const matches = censusRows.filter(
      (r) =>
        String(r.fields.name || "").trim() === name &&
        String(r.fields[CENSUS_FIELDS.country] || "").trim() === country
    );
    const targets = matches.length ? matches : censusRows.filter((r) => r.id === row.censusRecordId);

    for (const rec of targets) {
      const current = String(rec.fields[CENSUS_FIELDS.affiliation] || "").trim();
      if (current === SLH_AFFILIATION) {
        log.affiliationUpdates.push({ recordId: rec.id, action: "already_slh", name });
        continue;
      }
      /** @type {Record<string, string>} */
      const fields = {
        [CENSUS_FIELDS.affiliation]: SLH_AFFILIATION,
      };
      if (!String(rec.fields[CENSUS_FIELDS.parentCompany] || "").trim()) {
        fields[CENSUS_FIELDS.parentCompany] = SLH_PARENT_COMPANY;
      }
      if (!String(rec.fields.Website || "").trim() && row.propertyUrl) {
        fields.Website = row.propertyUrl;
      }

      if (DRY_RUN) {
        log.affiliationUpdates.push({ recordId: rec.id, action: "update_dry_run", name, fields });
        console.log(`[dry-run] AFFILIATION ${name} (${rec.id})`);
        continue;
      }

      await base(HOTEL_CENSUS_TABLE).update(rec.id, fields, { typecast: true });
      affiliationUpdated++;
      log.affiliationUpdates.push({ recordId: rec.id, action: "update", name, fields });
      console.log(`AFFILIATION ${name} → ${rec.id}`);
    }
  }

  writeFileSync(REPORT, JSON.stringify(log, null, 2));
  console.log(`\nCreated: ${created} | Affiliation updates: ${affiliationUpdated} | Skipped: ${skipped}`);
  console.log("Report:", REPORT);
  if (DRY_RUN) console.log("\nDry-run only — pass --apply to write Airtable.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
