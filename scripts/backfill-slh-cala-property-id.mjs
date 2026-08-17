#!/usr/bin/env node
/**
 * Fill-blank Property ID for SLH CALA census from official slh.com catalog.
 * Match via Website slug / URL; Property ID = SLH API hotel id (official).
 *
 *   node scripts/backfill-slh-cala-property-id.mjs
 *   node scripts/backfill-slh-cala-property-id.mjs --apply
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "node:fs";
import Airtable from "airtable";
import {
  fetchSlhCalaProperties,
  SLH_AFFILIATION,
  isCalaCountry,
} from "../lib/slh-census-enrichment.js";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { CENSUS_PROPERTY_ID_FIELD } from "../lib/hotel-census/hilton-property-id-contract.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";

const APPLY = process.argv.includes("--apply");
const MAP = {
  website: "Website",
  propertyId: CENSUS_PROPERTY_ID_FIELD,
  affiliation: CENSUS_FIELDS.affiliation,
};

function slugFromWebsite(url) {
  const m = String(url || "").match(/slh\.com\/hotels\/([^/?#]+)/i);
  return m ? m[1].toLowerCase() : "";
}

async function main() {
  mkdirSync("reports", { recursive: true });
  console.log("=== SLH CALA Property ID backfill ===\n");

  const catalog = await fetchSlhCalaProperties({
    onProgress: (m) => console.log(" ", m),
  });
  const bySlug = new Map();
  const byId = new Map();
  for (const h of catalog) {
    if (h.slug) bySlug.set(h.slug.toLowerCase(), h);
    if (h.id) byId.set(String(h.id), h);
  }
  console.log("Catalog CALA:", catalog.length);

  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID_ALT
  );
  const records = await base(HOTEL_CENSUS_TABLE)
    .select({
      fields: ["name", MAP.website, MAP.propertyId, MAP.affiliation, CENSUS_FIELDS.country],
      filterByFormula: `{${MAP.affiliation}}="${SLH_AFFILIATION}"`,
      pageSize: 100,
    })
    .all();

  const cala = records.filter((r) => isCalaCountry(r.fields[CENSUS_FIELDS.country]));
  /** @type {object[]} */
  const planRows = [];
  /** @type {object[]} */
  const skipped = [];

  for (const rec of cala) {
    if (!isBlankCensusValue(rec.fields[MAP.propertyId])) {
      skipped.push({ censusRecordId: rec.id, reason: "property_id_present" });
      continue;
    }
    const website = String(rec.fields[MAP.website] || "").trim();
    const slug = slugFromWebsite(website);
    if (!slug) {
      skipped.push({
        censusRecordId: rec.id,
        censusName: rec.fields.name,
        reason: "no_slh_website_slug",
      });
      continue;
    }
    const hit = bySlug.get(slug);
    if (!hit?.id) {
      skipped.push({
        censusRecordId: rec.id,
        censusName: rec.fields.name,
        slug,
        reason: "slug_not_in_cala_catalog",
      });
      continue;
    }

    planRows.push({
      censusRecordId: rec.id,
      censusName: rec.fields.name,
      slug,
      propertyId: String(hit.id),
      propertyUrl: hit.propertyUrl,
      applyFields: { [MAP.propertyId]: String(hit.id) },
      fieldMapping: MAP,
      validation: { pass: /^\d+$/.test(String(hit.id)) || String(hit.id).length >= 4 },
    });
  }

  // Prefer numeric/stable IDs; if id looks like GUID keep it (official)
  const validated = planRows.filter((r) => r.validation.pass && r.applyFields[MAP.propertyId]);

  writeFileSync(
    "reports/slh-cala-property-id-backfill-plan.json",
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        mode: APPLY ? "apply" : "dry-run",
        catalogCala: catalog.length,
        censusCala: cala.length,
        readyToApply: validated.length,
        planRows: validated,
        skipped,
        fieldMapping: MAP,
        note: "Property ID = official SLH API hotel id matched via Website slug.",
      },
      null,
      2
    )
  );

  console.log("Census CALA:", cala.length);
  console.log("Ready:", validated.length, "Skipped:", skipped.length);
  console.log("Sample:", validated.slice(0, 5).map((r) => `${r.censusName} → ${r.propertyId}`));

  if (!APPLY) {
    console.log("\nDRY-RUN — re-run with --apply after review.");
    return;
  }

  let updated = 0;
  let batch = [];
  const log = [];
  async function flush() {
    if (!batch.length) return;
    await base(HOTEL_CENSUS_TABLE).update(batch, { typecast: true });
    updated += batch.length;
    batch = [];
  }
  for (const row of validated) {
    log.push(row);
    batch.push({ id: row.censusRecordId, fields: row.applyFields });
    if (batch.length >= 10) await flush();
  }
  await flush();
  writeFileSync(
    "reports/slh-cala-property-id-backfill-apply-log.json",
    JSON.stringify({ generatedAt: new Date().toISOString(), updated, rows: log }, null, 2)
  );
  console.log("Updated:", updated);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
