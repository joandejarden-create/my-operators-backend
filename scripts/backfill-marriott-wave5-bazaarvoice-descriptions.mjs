#!/usr/bin/env node
/**
 * Wave 5: fill-blank Hotel Description for Autograph / Tribute / Design Hotels
 * from Marriott Bazaarvoice products API (works when marriott.com is Akamai-blocked).
 *
 *   node scripts/backfill-marriott-wave5-bazaarvoice-descriptions.mjs
 *   node scripts/backfill-marriott-wave5-bazaarvoice-descriptions.mjs --apply
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { CENSUS_DESCRIPTION_FIELD } from "../lib/hotel-census/hilton-description-enrichment-contract.js";
import { CENSUS_PROPERTY_ID_FIELD } from "../lib/hotel-census/hilton-property-id-contract.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import { isCalaCountry } from "../lib/design-hotels-census-enrichment.js";
import { fetchMarriottBazaarvoiceProducts } from "../lib/marriott-bazaarvoice-content-fetch.js";
import { marshaFromMarriottWebsite } from "../lib/marriott-brand-directory-extract.js";

const APPLY = process.argv.includes("--apply");
const AFFS = ["Autograph Collection", "Tribute Portfolio", "Design Hotels"];

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
        "Website",
        CENSUS_PROPERTY_ID_FIELD,
        CENSUS_DESCRIPTION_FIELD,
      ],
      filterByFormula: formula,
      pageSize: 100,
    })
    .all();

  /** @type {{ rec: import('airtable').Record, marsha: string }[]} */
  const need = [];
  for (const rec of records) {
    if (!isCalaCountry(rec.fields[CENSUS_FIELDS.country])) continue;
    if (!isBlankCensusValue(rec.fields[CENSUS_DESCRIPTION_FIELD])) continue;
    const marsha =
      String(rec.fields[CENSUS_PROPERTY_ID_FIELD] || "")
        .trim()
        .toUpperCase() || marshaFromMarriottWebsite(rec.fields.Website);
    if (!marsha || !/^[A-Z0-9]{4,6}$/.test(marsha)) continue;
    need.push({ rec, marsha });
  }

  console.log("Blank description with MARSHA:", need.length);
  const byMarsha = await fetchMarriottBazaarvoiceProducts(need.map((n) => n.marsha));

  const planRows = [];
  const skipped = [];
  const seenRec = new Set();
  for (const { rec, marsha } of need) {
    if (seenRec.has(rec.id)) continue;
    seenRec.add(rec.id);
    const bv = byMarsha.get(marsha);
    const desc = String(bv?.description || "").replace(/\s+/g, " ").trim();
    if (!desc || desc.length < 40) {
      skipped.push({ id: rec.id, name: rec.fields.name, marsha, reason: "no_bv_description" });
      continue;
    }
    planRows.push({
      censusRecordId: rec.id,
      censusName: rec.fields.name,
      affiliation: rec.fields[CENSUS_FIELDS.affiliation],
      marsha,
      source: "marriott_bazaarvoice",
      applyFields: { [CENSUS_DESCRIPTION_FIELD]: desc },
    });
  }

  writeFileSync(
    "reports/marriott-wave5-bazaarvoice-descriptions-plan.json",
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
  console.log("Ready:", planRows.length);
  for (const r of planRows) {
    console.log(" ", r.marsha, r.censusName, "→", r.applyFields[CENSUS_DESCRIPTION_FIELD].slice(0, 90));
  }

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
    "reports/marriott-wave5-bazaarvoice-descriptions-apply-log.json",
    JSON.stringify({ generatedAt: new Date().toISOString(), updated, planRows }, null, 2)
  );
  console.log("Updated:", updated);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
