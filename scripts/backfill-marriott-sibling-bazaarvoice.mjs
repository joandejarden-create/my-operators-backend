#!/usr/bin/env node
/**
 * Sibling Marriott: fill-blank Hotel Description (+ Website if blank) for CALA
 * Marriott-parent rows that are NOT Active soft brands, via Bazaarvoice products API.
 *
 *   node scripts/backfill-marriott-sibling-bazaarvoice.mjs
 *   node scripts/backfill-marriott-sibling-bazaarvoice.mjs --apply --limit=120
 *   node scripts/backfill-marriott-sibling-bazaarvoice.mjs --affiliations="Ritz-Carlton|Courtyard"
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import Airtable from "airtable";
import { BRAND_STATUS_ACTIVE_FORMULA } from "../lib/brand-status-active.js";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { CENSUS_DESCRIPTION_FIELD } from "../lib/hotel-census/hilton-description-enrichment-contract.js";
import { CENSUS_PROPERTY_ID_FIELD } from "../lib/hotel-census/hilton-property-id-contract.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import { isCalaCountry } from "../lib/design-hotels-census-enrichment.js";
import { fetchMarriottBazaarvoiceProducts } from "../lib/marriott-bazaarvoice-content-fetch.js";
import { marshaFromMarriottWebsite } from "../lib/marriott-brand-directory-extract.js";

const APPLY = process.argv.includes("--apply");
const limitArg = process.argv.find((a) => a.startsWith("--limit="));
const LIMIT = limitArg ? Number(limitArg.split("=")[1]) : 120;
const affArg = process.argv.find((a) => a.startsWith("--affiliations="))?.split("=")[1];
const AFF_FILTER = affArg
  ? affArg.split("|").map((s) => s.trim()).filter(Boolean)
  : null;

async function main() {
  mkdirSync("reports", { recursive: true });
  const apiKey = process.env.AIRTABLE_API_KEY;
  const mvp = new Airtable({ apiKey }).base(process.env.AIRTABLE_BASE_ID);
  const plat = new Airtable({ apiKey }).base(process.env.AIRTABLE_BASE_ID_ALT);

  const active = new Set(
    (
      await mvp("Brand Setup - Brand Basics")
        .select({ fields: ["Brand Name"], filterByFormula: BRAND_STATUS_ACTIVE_FORMULA })
        .all()
    )
      .map((r) => String(r.fields["Brand Name"] || "").trim())
      .filter(Boolean)
  );

  const records = await plat(HOTEL_CENSUS_TABLE)
    .select({
      fields: [
        "name",
        CENSUS_FIELDS.affiliation,
        CENSUS_FIELDS.parentCompany,
        CENSUS_FIELDS.country,
        "Website",
        CENSUS_PROPERTY_ID_FIELD,
        CENSUS_DESCRIPTION_FIELD,
      ],
      filterByFormula: `FIND("Marriott", {${CENSUS_FIELDS.parentCompany}})`,
      pageSize: 100,
    })
    .all();

  /** @type {{ rec: import('airtable').Record, marsha: string }[]} */
  const need = [];
  for (const rec of records) {
    if (!isCalaCountry(rec.fields[CENSUS_FIELDS.country])) continue;
    const aff = String(rec.fields[CENSUS_FIELDS.affiliation] || "").trim();
    if (!aff || active.has(aff)) continue;
    if (AFF_FILTER && !AFF_FILTER.some((a) => a.toLowerCase() === aff.toLowerCase())) continue;
    if (!isBlankCensusValue(rec.fields[CENSUS_DESCRIPTION_FIELD])) continue;
    const marsha =
      String(rec.fields[CENSUS_PROPERTY_ID_FIELD] || "")
        .trim()
        .toUpperCase() || marshaFromMarriottWebsite(rec.fields.Website);
    if (!marsha || !/^[A-Z0-9]{4,6}$/.test(marsha)) continue;
    need.push({ rec, marsha });
  }

  const work = need.slice(0, LIMIT);
  console.log(`Marriott sibling blank-desc with MARSHA: ${need.length}; working: ${work.length}`);

  const byMarsha = await fetchMarriottBazaarvoiceProducts(work.map((n) => n.marsha));
  const planRows = [];
  const skipped = [];
  const seenRec = new Set();

  for (const { rec, marsha } of work) {
    if (seenRec.has(rec.id)) continue;
    seenRec.add(rec.id);
    const bv = byMarsha.get(marsha);
    const desc = String(bv?.description || "").replace(/\s+/g, " ").trim();
    if (!desc || desc.length < 40) {
      skipped.push({ id: rec.id, name: rec.fields.name, marsha, reason: "no_bv_description" });
      continue;
    }
    /** @type {Record<string, string>} */
    const applyFields = { [CENSUS_DESCRIPTION_FIELD]: desc };
    if (isBlankCensusValue(rec.fields.Website) && bv?.website) {
      applyFields.Website = bv.website;
    }
    if (isBlankCensusValue(rec.fields[CENSUS_PROPERTY_ID_FIELD])) {
      applyFields[CENSUS_PROPERTY_ID_FIELD] = marsha;
    }
    planRows.push({
      censusRecordId: rec.id,
      censusName: rec.fields.name,
      affiliation: rec.fields[CENSUS_FIELDS.affiliation],
      marsha,
      source: "marriott_bazaarvoice",
      applyFields,
    });
  }

  const byAff = {};
  for (const r of planRows) byAff[r.affiliation] = (byAff[r.affiliation] || 0) + 1;

  writeFileSync(
    "reports/marriott-sibling-bazaarvoice-plan.json",
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        mode: APPLY ? "apply" : "dry-run",
        limit: LIMIT,
        readyToApply: planRows.length,
        byAffiliation: byAff,
        planRows,
        skipped,
      },
      null,
      2
    )
  );
  console.log("Ready:", planRows.length, byAff);
  for (const r of planRows.slice(0, 15)) {
    console.log(" ", r.marsha, r.affiliation, "|", r.censusName);
  }
  if (planRows.length > 15) console.log(`  … +${planRows.length - 15} more`);

  if (!APPLY) {
    console.log("DRY-RUN");
    return;
  }

  let updated = 0;
  let batch = [];
  async function flush() {
    if (!batch.length) return;
    await plat(HOTEL_CENSUS_TABLE).update(batch, { typecast: true });
    updated += batch.length;
    batch = [];
  }
  for (const row of planRows) {
    batch.push({ id: row.censusRecordId, fields: row.applyFields });
    if (batch.length >= 10) await flush();
  }
  await flush();
  writeFileSync(
    "reports/marriott-sibling-bazaarvoice-apply-log.json",
    JSON.stringify({ generatedAt: new Date().toISOString(), updated, byAff, planRows }, null, 2)
  );
  console.log("Updated:", updated);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
