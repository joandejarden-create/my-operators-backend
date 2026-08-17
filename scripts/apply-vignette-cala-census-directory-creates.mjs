#!/usr/bin/env node
/**
 * Vignette Collection CALA — create open IHG directory hotels missing from census;
 * steward Mexico pipeline rows not on the public open directory.
 *
 * Fill-blank Website + Property ID on create. Dry-run by default.
 *
 *   node scripts/apply-vignette-cala-census-directory-creates.mjs
 *   node scripts/apply-vignette-cala-census-directory-creates.mjs --apply
 */
import "../load-env.js";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import {
  CENSUS_FIELDS,
  HOTEL_CENSUS_TABLE,
  STATUS_OPEN,
} from "../lib/hotel-census/fields.js";
import { countryToDealalityRegion } from "../lib/hotel-census/region.js";
import { countryToSubContinent } from "../lib/hotel-census/geography-enrichment-contract.js";
import { CENSUS_PROPERTY_ID_FIELD } from "../lib/hotel-census/hilton-property-id-contract.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORTS = join(__dirname, "..", "reports");
const EXTRACT_JSON = join(REPORTS, "ihg-cala-directory-extract.json");

export const VIGNETTE_AFFILIATION = "Vignette Collection";
export const VIGNETTE_PARENT = "IHG Hotels & Resorts";

const MAP_VIGNETTE_CREATE = {
  name: CENSUS_FIELDS.name,
  affiliation: CENSUS_FIELDS.affiliation,
  parentCompany: CENSUS_FIELDS.parentCompany,
  status: CENSUS_FIELDS.status,
  city: CENSUS_FIELDS.city,
  country: CENSUS_FIELDS.country,
  region: CENSUS_FIELDS.region,
  subContinent: CENSUS_FIELDS.subContinent,
  market: CENSUS_FIELDS.market,
  website: "Website",
  propertyId: CENSUS_PROPERTY_ID_FIELD,
  projectPhase: CENSUS_FIELDS.projectPhase,
};

function parseArgs() {
  return { apply: process.argv.includes("--apply") };
}

function titleCity(city) {
  return String(city || "")
    .trim()
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

/**
 * @param {object} dir
 */
function validateCreateFields(fields) {
  const errors = [];
  if (!fields[MAP_VIGNETTE_CREATE.name]) errors.push("missing name");
  if (fields[MAP_VIGNETTE_CREATE.affiliation] !== VIGNETTE_AFFILIATION) {
    errors.push("Affiliation must be exact Brand Setup name Vignette Collection");
  }
  const website = fields[MAP_VIGNETTE_CREATE.website];
  if (!website || !/^https:\/\/www\.ihg\.com\/vignettecollection\//i.test(website)) {
    errors.push("Website must be official vignettecollection hoteldetail URL");
  }
  if (!/\/hoteldetail\/?$/i.test(String(website || "").replace(/\/$/, ""))) {
    errors.push("Website must end with /hoteldetail");
  }
  const pid = fields[MAP_VIGNETTE_CREATE.propertyId];
  if (!pid || !/^[A-Z0-9]{4,6}$/.test(String(pid))) {
    errors.push("Property ID must be 4–6 alphanumeric IHG mnemonic");
  }
  return { pass: errors.length === 0, errors };
}

async function main() {
  const opts = parseArgs();
  mkdirSync(REPORTS, { recursive: true });

  if (!existsSync(EXTRACT_JSON)) {
    throw new Error(`Missing ${EXTRACT_JSON}. Run: node scripts/extract-ihg-cala-directory.mjs`);
  }

  const extract = JSON.parse(readFileSync(EXTRACT_JSON, "utf8"));
  const vignetteDir = (extract.propertyRows || []).filter(
    (r) =>
      String(r.brand || "").toLowerCase() === "vignettecollection" ||
      /\/vignettecollection\//i.test(r.propertyUrl || "")
  );

  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID_ALT
  );

  const census = await base(HOTEL_CENSUS_TABLE)
    .select({
      fields: [
        CENSUS_FIELDS.name,
        CENSUS_FIELDS.affiliation,
        CENSUS_FIELDS.country,
        CENSUS_FIELDS.city,
        CENSUS_FIELDS.status,
        "Website",
        CENSUS_PROPERTY_ID_FIELD,
      ],
      filterByFormula: `{${CENSUS_FIELDS.affiliation}}="${VIGNETTE_AFFILIATION}"`,
      pageSize: 100,
    })
    .all();

  const claimedIds = new Set();
  const claimedUrls = new Set();
  for (const r of census) {
    const pid = r.fields?.[CENSUS_PROPERTY_ID_FIELD];
    if (!isBlankCensusValue(pid)) claimedIds.add(String(pid).toUpperCase());
    const web = r.fields?.Website;
    if (!isBlankCensusValue(web)) claimedUrls.add(String(web).toLowerCase().replace(/\/$/, ""));
  }

  /** @type {object[]} */
  const createRows = [];
  /** @type {object[]} */
  const skippedCreates = [];

  for (const dir of vignetteDir) {
    const pid = String(dir.propertyId || dir.mnemonic || "").toUpperCase();
    const url = String(dir.propertyUrl || dir.website || "")
      .trim()
      .replace(/\/$/, "");
    if (!pid || !url) {
      skippedCreates.push({ dir, reason: "missing_id_or_url" });
      continue;
    }
    if (claimedIds.has(pid) || claimedUrls.has(url.toLowerCase())) {
      skippedCreates.push({ propertyId: pid, propertyUrl: url, reason: "already_on_census" });
      continue;
    }

    const city = titleCity(dir.city || dir.citySlug);
    const country = String(dir.country || "").trim();
    const fields = {
      [MAP_VIGNETTE_CREATE.name]: String(dir.name || dir.inferredHotelName).trim(),
      [MAP_VIGNETTE_CREATE.affiliation]: VIGNETTE_AFFILIATION,
      [MAP_VIGNETTE_CREATE.parentCompany]: VIGNETTE_PARENT,
      [MAP_VIGNETTE_CREATE.status]: [STATUS_OPEN],
      [MAP_VIGNETTE_CREATE.projectPhase]: "Open",
      [MAP_VIGNETTE_CREATE.city]: city,
      [MAP_VIGNETTE_CREATE.country]: country,
      [MAP_VIGNETTE_CREATE.region]: countryToDealalityRegion(country),
      [MAP_VIGNETTE_CREATE.subContinent]: countryToSubContinent(country),
      [MAP_VIGNETTE_CREATE.market]: city || country,
      [MAP_VIGNETTE_CREATE.website]: url,
      [MAP_VIGNETTE_CREATE.propertyId]: pid,
    };

    const v = validateCreateFields(fields);
    if (!v.pass) {
      skippedCreates.push({ propertyId: pid, reason: "validation", errors: v.errors });
      continue;
    }

    createRows.push({
      propertyId: pid,
      propertyUrl: url,
      directoryName: dir.name || dir.inferredHotelName,
      country,
      city,
      fields,
      fieldMapping: MAP_VIGNETTE_CREATE,
      validation: v,
    });
  }

  const stewardPipeline = census
    .filter((r) => {
      const st = r.fields?.[CENSUS_FIELDS.status];
      const arr = Array.isArray(st) ? st : [st];
      return arr.some((s) => /pipeline/i.test(String(s || "")));
    })
    .map((r) => ({
      censusRecordId: r.id,
      censusName: r.fields?.[CENSUS_FIELDS.name],
      country: r.fields?.[CENSUS_FIELDS.country],
      city: r.fields?.[CENSUS_FIELDS.city],
      status: r.fields?.[CENSUS_FIELDS.status],
      reason: "pipeline_not_on_public_open_directory",
      note: "Do not invent Website/Property ID; wait for IHG open listing.",
    }));

  const planPath = join(REPORTS, "vignette-cala-census-create-plan.json");
  const stewardPath = join(REPORTS, "vignette-cala-pipeline-steward.csv");
  writeFileSync(
    planPath,
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        mode: opts.apply ? "apply" : "dry-run",
        affiliation: VIGNETTE_AFFILIATION,
        directoryVignetteCount: vignetteDir.length,
        censusVignetteCount: census.length,
        createCandidates: createRows.length,
        createRows,
        skippedCreates,
        stewardPipeline,
        fieldMapping: MAP_VIGNETTE_CREATE,
      },
      null,
      2
    )
  );

  const csvEsc = (v) => {
    const s = String(v ?? "");
    return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
  };
  writeFileSync(
    stewardPath,
    [
      "censusRecordId,censusName,country,city,status,reason",
      ...stewardPipeline.map((r) =>
        [r.censusRecordId, r.censusName, r.country, r.city, JSON.stringify(r.status), r.reason]
          .map(csvEsc)
          .join(",")
      ),
    ].join("\n")
  );

  console.log("=== Vignette Collection CALA directory creates ===\n");
  console.log("Directory vignette:", vignetteDir.length);
  console.log("Census vignette:", census.length);
  console.log("Create candidates:", createRows.length);
  console.log("Steward pipeline:", stewardPipeline.length);
  console.log("Plan:", planPath);
  console.log("Steward CSV:", stewardPath);

  if (!opts.apply) {
    console.log("\nDRY-RUN — no writes. Re-run with --apply after review.");
    for (const row of createRows) {
      console.log(`  would create: ${row.fields.name} (${row.country}) ${row.propertyId}`);
    }
    return;
  }

  /** @type {object[]} */
  const log = [];
  let created = 0;
  let errors = 0;
  for (const row of createRows) {
    try {
      const [rec] = await base(HOTEL_CENSUS_TABLE).create([{ fields: row.fields }], {
        typecast: true,
      });
      created++;
      log.push({ action: "created", recordId: rec.id, ...row });
      console.log(`CREATED ${rec.id} ${row.fields.name}`);
    } catch (err) {
      errors++;
      log.push({ action: "error", error: String(err?.message || err), ...row });
      console.error("Create failed:", err?.message || err);
    }
  }

  writeFileSync(
    join(REPORTS, "vignette-cala-census-create-apply-log.json"),
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        created,
        errors,
        fieldMapping: MAP_VIGNETTE_CREATE,
        rows: log,
      },
      null,
      2
    )
  );
  console.log("\nCreated:", created, "Errors:", errors);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
