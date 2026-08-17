#!/usr/bin/env node
/**
 * MGallery Collection CALA — match Accor catalog (brand=MGA) to Hotel Census,
 * update Affiliation / Parent Company / Website / Property ID, create gaps if any.
 *
 *   node scripts/apply-mgallery-cala-census-affiliation.mjs --dry-run
 *   node scripts/apply-mgallery-cala-census-affiliation.mjs --apply
 */
import "../load-env.js";
import { mkdirSync, writeFileSync, appendFileSync, existsSync } from "node:fs";
import { join } from "node:path";
import Airtable from "airtable";
import {
  fetchMgalleryCalaCatalog,
  planMgalleryAffiliationUpdates,
  buildMgalleryCensusPatch,
  MGALLERY_AFFILIATION,
  MGALLERY_PARENT_COMPANY,
  isCalaCountry,
} from "../lib/mgallery-census-enrichment.js";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { countryToDealalityRegion } from "../lib/hotel-census/region.js";
import { countryToSubContinent } from "../lib/hotel-census/geography-enrichment-contract.js";
import { CENSUS_PROPERTY_ID_FIELD } from "../lib/hotel-census/hilton-property-id-contract.js";
import { writeCsv } from "../lib/str-census-import/report-utils.mjs";

const APPLY = process.argv.includes("--apply");
const PLAN_JSON = join("reports", "mgallery-cala-affiliation-apply-plan.json");
const LOG_CSV = join("reports", "mgallery-cala-affiliation-applies.csv");

async function loadCensusCalaRows() {
  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID_ALT
  );
  const records = await base(HOTEL_CENSUS_TABLE)
    .select({
      fields: [
        "name",
        "Website",
        CENSUS_PROPERTY_ID_FIELD,
        CENSUS_FIELDS.affiliation,
        CENSUS_FIELDS.parentCompany,
        CENSUS_FIELDS.country,
        CENSUS_FIELDS.city,
      ],
    })
    .all();
  return records.filter((r) => isCalaCountry(r.fields[CENSUS_FIELDS.country]));
}

function createFieldsFromCatalog(source) {
  return {
    [CENSUS_FIELDS.name]: source.name,
    [CENSUS_FIELDS.city]: source.city || source.censusCountry,
    [CENSUS_FIELDS.country]: source.censusCountry || source.country,
    [CENSUS_FIELDS.region]: countryToDealalityRegion(source.censusCountry || source.country),
    [CENSUS_FIELDS.subContinent]: countryToSubContinent(source.censusCountry || source.country),
    [CENSUS_FIELDS.affiliation]: MGALLERY_AFFILIATION,
    [CENSUS_FIELDS.parentCompany]: MGALLERY_PARENT_COMPANY,
    [CENSUS_FIELDS.status]: ["Open"],
    [CENSUS_FIELDS.projectPhase]: "Open",
    Website: source.propertyUrl,
    [CENSUS_PROPERTY_ID_FIELD]: source.propertyId,
    Latitude: source.latitude,
    Longitude: source.longitude,
    "Address 1": source.address1 || undefined,
    [CENSUS_FIELDS.operationType]: "Branded",
    [CENSUS_FIELDS.chainScale]: "Upper Upscale",
  };
}

async function main() {
  mkdirSync("reports", { recursive: true });
  console.log("Fetching Accor catalog brand=MGA for CALA…");
  const catalog = await fetchMgalleryCalaCatalog({ onProgress: (m) => console.log(" ", m) });
  console.log(`Catalog CALA MGallery hotels: ${catalog.length}`);
  catalog.forEach((h) => console.log(" ", h.propertyId, "|", h.censusCountry, "|", h.name));

  console.log("Loading CALA census…");
  const censusRows = await loadCensusCalaRows();
  console.log(`CALA census rows: ${censusRows.length}`);

  const planned = planMgalleryAffiliationUpdates(censusRows, catalog, { minScore: 80 });
  const ready = [];
  const skipped = [];

  for (const row of planned.matches) {
    const fields = buildMgalleryCensusPatch(row);
    if (!Object.keys(fields).length) {
      skipped.push({ ...row, skipReason: "already_complete" });
      continue;
    }
    ready.push({ ...row, fields });
  }

  const alreadyAffiliated = censusRows.filter(
    (r) => r.fields[CENSUS_FIELDS.affiliation] === MGALLERY_AFFILIATION
  );
  const matchedIds = new Set(planned.matches.map((m) => m.censusRecordId));
  const stewardExtras = alreadyAffiliated
    .filter((r) => !matchedIds.has(r.id))
    .map((r) => ({
      censusRecordId: r.id,
      censusName: r.fields.name,
      country: r.fields[CENSUS_FIELDS.country],
      propertyId: r.fields[CENSUS_PROPERTY_ID_FIELD] || "",
      reason: "census_mgallery_not_on_current_accor_catalog",
    }));

  const report = {
    generatedAt: new Date().toISOString(),
    apply: APPLY,
    affiliationTarget: MGALLERY_AFFILIATION,
    source: "Accor Catalog API brand=MGA + mgallery.accor.com destinations",
    catalogCount: catalog.length,
    matchedRows: planned.matches.length,
    readyToApply: ready.length,
    skippedAlreadyComplete: skipped.length,
    unmatchedCatalog: planned.unmatchedSources,
    stewardExtras,
    ready,
    skipped,
  };

  writeFileSync(PLAN_JSON, JSON.stringify(report, null, 2));
  writeCsv(
    join("reports", "mgallery-cala-affiliation-apply-ready.csv"),
    ready.map((r) => ({
      censusRecordId: r.censusRecordId,
      censusName: r.censusName,
      censusCountry: r.censusCountry,
      propertyId: r.source.propertyId,
      currentAffiliation: r.currentAffiliation,
      matchScore: r.matchScore,
      fields: JSON.stringify(r.fields),
    })),
    [
      "censusRecordId",
      "censusName",
      "censusCountry",
      "propertyId",
      "currentAffiliation",
      "matchScore",
      "fields",
    ]
  );

  console.log(`\nMatched: ${planned.matches.length}`);
  console.log(`Ready to apply: ${ready.length}`);
  console.log(`Already complete: ${skipped.length}`);
  console.log(`Unmatched catalog (need create): ${planned.unmatchedSources.length}`);
  console.log(`Steward extras (census MGallery not on catalog): ${stewardExtras.length}`);
  stewardExtras.forEach((s) => console.log("  EXTRA", s.censusName, s.country));

  if (planned.unmatchedSources.length) {
    console.log("\nWould CREATE:");
    for (const s of planned.unmatchedSources) {
      console.log(" ", s.propertyId, s.name, s.censusCountry);
    }
  }

  for (const r of ready.slice(0, 15)) {
    console.log(
      `  ${r.censusName} ← ${r.source.propertyId} [${r.matchScore}] → ${JSON.stringify(r.fields)}`
    );
  }

  if (!APPLY) {
    console.log("\nDry-run only — pass --apply to write Airtable.");
    return;
  }

  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID_ALT
  );

  if (!existsSync(LOG_CSV)) {
    appendFileSync(
      LOG_CSV,
      "appliedAt,action,censusRecordId,censusName,propertyId,fieldsJson\n"
    );
  }

  let updated = 0;
  let created = 0;
  let failed = 0;

  for (const row of ready) {
    try {
      await base(HOTEL_CENSUS_TABLE).update(row.censusRecordId, row.fields, { typecast: true });
      updated++;
      appendFileSync(
        LOG_CSV,
        `${new Date().toISOString()},update,${row.censusRecordId},"${String(row.censusName).replace(/"/g, '""')}",${row.source.propertyId},"${JSON.stringify(row.fields).replace(/"/g, '""')}"\n`
      );
      console.log("Updated", row.censusRecordId, row.censusName);
    } catch (err) {
      failed++;
      console.error("FAIL update", row.censusRecordId, err?.message || err);
    }
  }

  for (const source of planned.unmatchedSources) {
    try {
      const fields = createFieldsFromCatalog(source);
      // remove undefined
      for (const k of Object.keys(fields)) {
        if (fields[k] === undefined || fields[k] === null || fields[k] === "") delete fields[k];
      }
      const [rec] = await base(HOTEL_CENSUS_TABLE).create([{ fields }], { typecast: true });
      created++;
      appendFileSync(
        LOG_CSV,
        `${new Date().toISOString()},create,${rec.id},"${String(source.name).replace(/"/g, '""')}",${source.propertyId},"${JSON.stringify(fields).replace(/"/g, '""')}"\n`
      );
      console.log("Created", rec.id, source.name);
    } catch (err) {
      failed++;
      console.error("FAIL create", source.propertyId, err?.message || err);
    }
  }

  console.log(`\nUpdated: ${updated} Created: ${created} Failed: ${failed}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
