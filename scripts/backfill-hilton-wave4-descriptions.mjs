#!/usr/bin/env node
/**
 * Wave 4: fill-blank Hotel Description for Curio + Tapestry from Hilton GraphQL
 * using existing census Property ID (ctyhocn).
 *
 *   node scripts/backfill-hilton-wave4-descriptions.mjs
 *   node scripts/backfill-hilton-wave4-descriptions.mjs --apply
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { CENSUS_PROPERTY_ID_FIELD } from "../lib/hotel-census/hilton-property-id-contract.js";
import {
  CENSUS_DESCRIPTION_FIELD,
} from "../lib/hotel-census/hilton-description-enrichment-contract.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import {
  fetchHiltonHotelDescription,
  pickPrimaryHiltonDescription,
} from "../lib/hilton-hotel-description-fetch.js";
import { isCalaCountry } from "../lib/design-hotels-census-enrichment.js";

const APPLY = process.argv.includes("--apply");
const AFFS = ["Curio Collection by Hilton", "Tapestry Collection by Hilton"];
const DELAY = 250;

function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}

async function main() {
  mkdirSync("reports", { recursive: true });
  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID_ALT
  );
  const formula = `OR(${AFFS.map((a) => `{${CENSUS_FIELDS.affiliation}}="${a}"`).join(",")})`;
  const records = await base(HOTEL_CENSUS_TABLE)
    .select({
      fields: [
        "name",
        CENSUS_FIELDS.affiliation,
        CENSUS_FIELDS.country,
        CENSUS_PROPERTY_ID_FIELD,
        CENSUS_DESCRIPTION_FIELD,
        "Website",
      ],
      filterByFormula: formula,
      pageSize: 100,
    })
    .all();

  const planRows = [];
  const skipped = [];
  let n = 0;
  for (const rec of records) {
    if (!isCalaCountry(rec.fields[CENSUS_FIELDS.country])) continue;
    if (!isBlankCensusValue(rec.fields[CENSUS_DESCRIPTION_FIELD])) {
      skipped.push({ id: rec.id, reason: "description_present" });
      continue;
    }
    const pid = String(rec.fields[CENSUS_PROPERTY_ID_FIELD] || "")
      .trim()
      .toUpperCase();
    if (!/^[A-Z0-9]{4,8}$/.test(pid)) {
      skipped.push({ id: rec.id, name: rec.fields.name, reason: "no_ctyhocn" });
      continue;
    }
    n++;
    console.log(` [${n}] ${rec.fields.name} ${pid}`);
    try {
      const gql = await fetchHiltonHotelDescription(pid);
      await sleep(DELAY);
      const desc = pickPrimaryHiltonDescription(gql);
      if (!desc || desc.length < 40) {
        skipped.push({ id: rec.id, name: rec.fields.name, pid, reason: "empty_description" });
        continue;
      }
      planRows.push({
        censusRecordId: rec.id,
        censusName: rec.fields.name,
        affiliation: rec.fields[CENSUS_FIELDS.affiliation],
        propertyId: pid,
        applyFields: { [CENSUS_DESCRIPTION_FIELD]: desc },
      });
    } catch (err) {
      skipped.push({
        id: rec.id,
        name: rec.fields.name,
        pid,
        reason: "graphql_error",
        error: String(err?.message || err),
      });
    }
  }

  writeFileSync(
    "reports/hilton-wave4-descriptions-plan.json",
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        mode: APPLY ? "apply" : "dry-run",
        readyToApply: planRows.length,
        planRows,
        skipped,
      },
      null,
      2
    )
  );
  console.log("\nReady:", planRows.length, "Skipped:", skipped.length);

  if (!APPLY) {
    console.log("DRY-RUN");
    return;
  }
  let updated = 0;
  let batch = [];
  async function flush() {
    if (!batch.length) return;
    await base(HOTEL_CENSUS_TABLE).update(batch, { typecast: true });
    updated += batch.length;
    batch = [];
  }
  for (const row of planRows) {
    batch.push({ id: row.censusRecordId, fields: row.applyFields });
    if (batch.length >= 10) await flush();
  }
  await flush();
  writeFileSync(
    "reports/hilton-wave4-descriptions-apply-log.json",
    JSON.stringify({ generatedAt: new Date().toISOString(), updated, planRows }, null, 2)
  );
  console.log("Updated:", updated);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
