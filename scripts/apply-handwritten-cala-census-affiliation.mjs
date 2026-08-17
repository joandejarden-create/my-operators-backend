#!/usr/bin/env node
/**
 * Handwritten Collection CALA — Accor catalog brand=SOU → Hotel Census.
 *
 *   node scripts/apply-handwritten-cala-census-affiliation.mjs
 *   node scripts/apply-handwritten-cala-census-affiliation.mjs --apply
 */
import "../load-env.js";
import { mkdirSync, writeFileSync, appendFileSync, existsSync } from "node:fs";
import { join } from "node:path";
import Airtable from "airtable";
import {
  fetchHandwrittenCalaCatalog,
  planHandwrittenAffiliationUpdates,
  buildHandwrittenCensusPatch,
  fetchHandwrittenAmenities,
  HANDWRITTEN_AFFILIATION,
  HANDWRITTEN_PARENT_COMPANY,
  HANDWRITTEN_BRAND_CODE,
  HANDWRITTEN_CALA_PIPELINE_STEWARD,
  MAP_HANDWRITTEN_CENSUS,
  isCalaCountry,
} from "../lib/handwritten-census-enrichment.js";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";
import { CENSUS_PROPERTY_ID_FIELD } from "../lib/hotel-census/hilton-property-id-contract.js";
import { CENSUS_AMENITIES_TEXT_FIELD } from "../lib/hilton-amenity-map.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import { countryToDealalityRegion } from "../lib/hotel-census/region.js";
import { countryToSubContinent } from "../lib/hotel-census/geography-enrichment-contract.js";
import { writeCsv } from "../lib/str-census-import/report-utils.mjs";

const APPLY = process.argv.includes("--apply");
const PLAN_JSON = join("reports", "handwritten-cala-affiliation-apply-plan.json");
const LOG_CSV = join("reports", "handwritten-cala-affiliation-applies.csv");
const AMENITIES_LOG = join("reports", "handwritten-cala-amenities-apply-log.json");

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
        CENSUS_AMENITIES_TEXT_FIELD,
      ],
    })
    .all();
  return records.filter((r) => isCalaCountry(r.fields[CENSUS_FIELDS.country]));
}

function createFieldsFromCatalog(source) {
  return {
    [CENSUS_FIELDS.name]: source.name.replace(/\s+Handwritten Collection\s*$/i, "").trim() || source.name,
    [CENSUS_FIELDS.city]: source.city || source.censusCountry,
    [CENSUS_FIELDS.country]: source.censusCountry || source.country,
    [CENSUS_FIELDS.region]: countryToDealalityRegion(source.censusCountry || source.country),
    [CENSUS_FIELDS.subContinent]: countryToSubContinent(source.censusCountry || source.country),
    [CENSUS_FIELDS.affiliation]: HANDWRITTEN_AFFILIATION,
    [CENSUS_FIELDS.parentCompany]: HANDWRITTEN_PARENT_COMPANY,
    [CENSUS_FIELDS.status]: ["Open"],
    [CENSUS_FIELDS.projectPhase]: "Open",
    Website: source.propertyUrl,
    [CENSUS_PROPERTY_ID_FIELD]: source.propertyId,
    Latitude: source.latitude,
    Longitude: source.longitude,
    "Address 1": source.address1 || undefined,
    [CENSUS_FIELDS.operationType]: "Branded",
  };
}

/**
 * @param {Record<string, string>} fields
 * @param {object} row
 */
function validatePatch(fields, row) {
  /** @type {string[]} */
  const failed = [];
  if (fields[CENSUS_FIELDS.affiliation] && fields[CENSUS_FIELDS.affiliation] !== HANDWRITTEN_AFFILIATION) {
    failed.push("invalid_affiliation");
  }
  if (
    fields[CENSUS_FIELDS.parentCompany] &&
    fields[CENSUS_FIELDS.parentCompany] !== HANDWRITTEN_PARENT_COMPANY
  ) {
    failed.push("invalid_parent");
  }
  if (fields.Website && !/^https:\/\/all\.accor\.com\/hotel\//i.test(fields.Website)) {
    failed.push("invalid_website");
  }
  if (fields[CENSUS_PROPERTY_ID_FIELD] && !/^[A-Z0-9]+$/i.test(fields[CENSUS_PROPERTY_ID_FIELD])) {
    failed.push("invalid_property_id");
  }
  if (!Object.keys(fields).length) failed.push("empty_patch");
  return {
    pass: failed.length === 0,
    failed,
    fieldMapping: MAP_HANDWRITTEN_CENSUS,
    sanitizedPayloadPreview: fields,
    match: { id: row.censusRecordId, score: row.matchScore, reason: row.matchReason },
  };
}

async function main() {
  mkdirSync("reports", { recursive: true });
  console.log(`Fetching Accor catalog brand=${HANDWRITTEN_BRAND_CODE} (Handwritten Collection) for CALA…`);
  const catalog = await fetchHandwrittenCalaCatalog({ onProgress: (m) => console.log(" ", m) });
  console.log(`Catalog CALA Handwritten open hotels: ${catalog.length}`);
  catalog.forEach((h) => console.log(" ", h.propertyId, "|", h.censusCountry, "|", h.name));

  console.log("\nPipeline steward (not in open catalog — no invent / no Open create):");
  for (const p of HANDWRITTEN_CALA_PIPELINE_STEWARD) {
    console.log(`  ${p.name} | ${p.city}, ${p.country} | ${p.expectedOpen} | ${p.status}`);
  }

  console.log("\nLoading CALA census…");
  const censusRows = await loadCensusCalaRows();
  const { matches, unmatchedSources } = planHandwrittenAffiliationUpdates(censusRows, catalog, {
    minScore: 80,
  });

  /** @type {object[]} */
  const ready = [];
  /** @type {object[]} */
  const creates = [];

  for (const row of matches) {
    const fields = buildHandwrittenCensusPatch(row);
    const validation = validatePatch(fields, row);
    ready.push({ ...row, fields, validation });
  }
  for (const source of unmatchedSources) {
    creates.push({
      source,
      fields: createFieldsFromCatalog(source),
      action: "create",
    });
  }

  const plan = {
    generatedAt: new Date().toISOString(),
    brandCode: HANDWRITTEN_BRAND_CODE,
    affiliation: HANDWRITTEN_AFFILIATION,
    parentCompany: HANDWRITTEN_PARENT_COMPANY,
    fieldMapping: MAP_HANDWRITTEN_CENSUS,
    catalogCount: catalog.length,
    matchCount: matches.length,
    createCount: creates.length,
    pipelineSteward: HANDWRITTEN_CALA_PIPELINE_STEWARD,
    ready,
    creates,
    unmatchedSources,
  };
  writeFileSync(PLAN_JSON, JSON.stringify(plan, null, 2));
  writeCsv(
    join("reports", "handwritten-cala-affiliation-apply-ready.csv"),
    ready.map((r) => ({
      censusRecordId: r.censusRecordId,
      censusName: r.censusName,
      country: r.censusCountry,
      matchScore: r.matchScore,
      matchReason: r.matchReason,
      propertyId: r.source?.propertyId,
      propertyUrl: r.source?.propertyUrl,
      patchKeys: Object.keys(r.fields || {}).join("|"),
      validation: r.validation?.pass ? "pass" : (r.validation?.failed || []).join("|"),
    }))
  );

  console.log(`\nReady updates: ${ready.length}  Creates: ${creates.length}`);
  for (const r of ready) {
    console.log(
      `  ${r.censusRecordId} ${r.censusName} ← ${r.source.propertyId} (${r.matchReason}/${r.matchScore})`,
      r.fields
    );
  }
  for (const c of creates) {
    console.log(`  CREATE ${c.source.propertyId} ${c.source.name}`);
  }

  if (!APPLY) {
    console.log(`\nDry-run only — plan: ${PLAN_JSON}`);
    console.log("Pass --apply to write Airtable (typecast:true).");
    return;
  }

  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID_ALT
  );
  if (!existsSync(LOG_CSV)) {
    appendFileSync(
      LOG_CSV,
      "ts,action,recordId,propertyId,patchKeys,amenitiesCount,ok,error\n"
    );
  }

  let applied = 0;
  let failed = 0;
  /** @type {object[]} */
  const amenityLog = [];

  for (const row of ready) {
    if (!row.validation?.pass) {
      console.log("Skip invalid", row.censusRecordId, row.validation?.failed);
      failed++;
      continue;
    }
    try {
      await base(HOTEL_CENSUS_TABLE).update(row.censusRecordId, row.fields, { typecast: true });
      console.log("Updated", row.censusRecordId, row.fields);
      applied++;
      appendFileSync(
        LOG_CSV,
        `${new Date().toISOString()},update,${row.censusRecordId},${row.source.propertyId},"${Object.keys(row.fields).join("|")}",,1,\n`
      );

      // Amenities fill-blank from official Accor page
      const existingAmen = censusRows.find((r) => r.id === row.censusRecordId)?.fields?.[
        CENSUS_AMENITIES_TEXT_FIELD
      ];
      if (isBlankCensusValue(existingAmen) && row.source.propertyUrl) {
        const am = await fetchHandwrittenAmenities(row.source.propertyUrl);
        const text = (am.amenities || []).join("; ");
        if (am.ok && text) {
          await base(HOTEL_CENSUS_TABLE).update(
            row.censusRecordId,
            { [CENSUS_AMENITIES_TEXT_FIELD]: text },
            { typecast: true }
          );
          console.log(`  Amenities ${am.amenities.length} labels`);
          amenityLog.push({
            recordId: row.censusRecordId,
            propertyId: row.source.propertyId,
            count: am.amenities.length,
            ok: true,
          });
          appendFileSync(
            LOG_CSV,
            `${new Date().toISOString()},amenities,${row.censusRecordId},${row.source.propertyId},Amenities,${am.amenities.length},1,\n`
          );
        } else {
          amenityLog.push({
            recordId: row.censusRecordId,
            propertyId: row.source.propertyId,
            ok: false,
            error: am.error || "empty",
          });
        }
      }
    } catch (err) {
      failed++;
      console.error("FAIL", row.censusRecordId, err?.message || err);
      appendFileSync(
        LOG_CSV,
        `${new Date().toISOString()},update,${row.censusRecordId},${row.source.propertyId},,,0,"${String(err?.message || err).replace(/"/g, "'")}"\n`
      );
    }
  }

  for (const c of creates) {
    try {
      const created = await base(HOTEL_CENSUS_TABLE).create(c.fields, { typecast: true });
      console.log("Created", created.id, c.source.propertyId);
      applied++;
      appendFileSync(
        LOG_CSV,
        `${new Date().toISOString()},create,${created.id},${c.source.propertyId},"${Object.keys(c.fields).join("|")}",,1,\n`
      );
    } catch (err) {
      failed++;
      console.error("CREATE FAIL", c.source.propertyId, err?.message || err);
    }
  }

  writeFileSync(
    AMENITIES_LOG,
    JSON.stringify({ generatedAt: new Date().toISOString(), amenityLog }, null, 2)
  );
  console.log(`\nApplied: ${applied}  Failed: ${failed}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
