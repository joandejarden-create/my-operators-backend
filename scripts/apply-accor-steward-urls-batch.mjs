#!/usr/bin/env node
/**
 * Apply steward-verified Accor canonical URLs to Hotel Census rows.
 * Usage: node scripts/apply-accor-steward-urls-batch.mjs [--dry-run]
 */
import "../load-env.js";
import { appendFileSync, existsSync } from "node:fs";
import { join } from "node:path";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";
import { fetchAccorHotelAmenities } from "../lib/accor-hotel-content-fetch.js";
import { accorCanonicalPropertyUrl } from "../lib/hotel-census/accor-directory-name-normalize.js";
import { fetchAccorCatalogByIds } from "../lib/accor-catalog-api.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";

const DRY_RUN = process.argv.includes("--dry-run");
const LOG_PATH = join("reports", "accor-steward-verified-applies.csv");

/** @type {{ id: string; code: string; name: string; fixWrongMatch?: boolean }[]} */
const STEWARD = [
  {
    id: "recDKfOQsBa3THBPT",
    code: "C0S2",
    name: "ibis Styles Presidente Prudente",
    fixWrongMatch: true,
  },
  {
    id: "recUwZHVRH0GZFPIp",
    code: "A232",
    name: "Sofitel Las Bovedas",
    fixWrongMatch: false,
  },
  {
    id: "recg0GRXkYL1d43np",
    code: "C0S2",
    name: "Hotel Ibis Styles Portal D'oeste",
    fixWrongMatch: false,
  },
  {
    id: "reclMnLTkNiXtNXp9",
    code: "B0P5",
    name: "Sofitel Baru Calablanca",
    fixWrongMatch: false,
  },
  {
    id: "rec3LfgNdx7xvFCnQ",
    code: "C2A2",
    name: "Armony Resort future MGallery",
    fixWrongMatch: false,
  },
];

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID_ALT
);

function appendLog(row, url, amenityCount) {
  if (!existsSync(LOG_PATH)) {
    appendFileSync(
      LOG_PATH,
      "appliedAt,censusRecordId,censusName,propertyId,propertyUrl,source,amenityCount\n"
    );
  }
  const appliedAt = new Date().toISOString();
  appendFileSync(
    LOG_PATH,
    `${appliedAt},${row.id},"${row.name}",${row.code},${url},steward_verified_url,${amenityCount}\n`
  );
}

for (const row of STEWARD) {
  const rec = await base(HOTEL_CENSUS_TABLE).find(row.id);
  const f = rec.fields || {};
  const url = accorCanonicalPropertyUrl(row.code);
  const fetched = await fetchAccorHotelAmenities(url);
  const catalog = await fetchAccorCatalogByIds([row.code]);
  const cat = catalog.hotels.find((h) => h.propertyId === row.code) || catalog.hotels[0] || {};

  const fields = {};
  const wrongPid =
    row.fixWrongMatch &&
    String(f["Property ID"] || "").toUpperCase() !== row.code.toUpperCase() &&
    !isBlankCensusValue(f["Property ID"]);

  if (wrongPid) {
    fields.Website = url;
    fields["Property ID"] = row.code;
    fields.Amenities = fetched.amenitiesText;
    if (cat.telephone) fields.Telephone = cat.telephone;
    if (cat.address1) fields["Address 1"] = cat.address1;
    if (cat.postalCode) fields["Postal Code"] = cat.postalCode;
    if (cat.latitude != null) fields.Latitude = cat.latitude;
    if (cat.longitude != null) fields.Longitude = cat.longitude;
  } else {
    if (isBlankCensusValue(f.Website)) fields.Website = url;
    if (isBlankCensusValue(f["Property ID"])) fields["Property ID"] = row.code;
    if (isBlankCensusValue(f.Amenities) && fetched.amenitiesText) {
      fields.Amenities = fetched.amenitiesText;
    }
    if (isBlankCensusValue(f.Telephone) && cat.telephone) fields.Telephone = cat.telephone;
    if (isBlankCensusValue(f["Address 1"]) && cat.address1) fields["Address 1"] = cat.address1;
    if (isBlankCensusValue(f["Postal Code"]) && cat.postalCode) {
      fields["Postal Code"] = cat.postalCode;
    }
    if (isBlankCensusValue(f.Latitude) && cat.latitude != null) {
      fields.Latitude = cat.latitude;
    }
    if (isBlankCensusValue(f.Longitude) && cat.longitude != null) {
      fields.Longitude = cat.longitude;
    }
  }

  console.log(`\n${row.name} (${row.code})`);
  console.log("Before:", {
    Website: f.Website,
    PID: f["Property ID"],
    Amenities: (f.Amenities || "").slice(0, 80),
  });
  console.log(wrongPid ? "Mode: steward fix wrong match" : "Mode: fill-blank");
  console.log("Apply:", fields);

  if (!Object.keys(fields).length) {
    console.log("Skip: nothing to apply");
    continue;
  }

  if (DRY_RUN) {
    console.log("Dry-run: no write");
    continue;
  }

  await base(HOTEL_CENSUS_TABLE).update(row.id, fields, { typecast: true });
  const after = await base(HOTEL_CENSUS_TABLE).find(row.id);
  console.log("After:", {
    Website: after.fields.Website,
    PID: after.fields["Property ID"],
    Amenities: (after.fields.Amenities || "").slice(0, 100),
  });
  const count = fields.Amenities ? fields.Amenities.split(";").length : 0;
  appendLog(row, url, count);
}
