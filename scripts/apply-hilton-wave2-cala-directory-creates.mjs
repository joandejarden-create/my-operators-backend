#!/usr/bin/env node
/**
 * Create CALA Hilton Curio/Tapestry directory hotels missing from census.
 * Dry-run by default.
 *
 *   node scripts/apply-hilton-wave2-cala-directory-creates.mjs
 *   node scripts/apply-hilton-wave2-cala-directory-creates.mjs --apply
 */
import "../load-env.js";
import { readFileSync, writeFileSync, mkdirSync } from "fs";
import Airtable from "airtable";
import {
  HOTEL_CENSUS_TABLE,
  CENSUS_FIELDS,
  STATUS_OPEN,
} from "../lib/hotel-census/fields.js";
import { countryToDealalityRegion } from "../lib/hotel-census/region.js";
import { countryToSubContinent } from "../lib/hotel-census/geography-enrichment-contract.js";
import { CENSUS_PROPERTY_ID_FIELD } from "../lib/hotel-census/hilton-property-id-contract.js";
import { CENSUS_DESCRIPTION_FIELD } from "../lib/hotel-census/hilton-description-enrichment-contract.js";
import { isBlankCensusValue } from "../lib/hotel-census/brand-directory-enrichment-contract.js";
import { fetchHiltonHotelDescription } from "../lib/hilton-hotel-description-fetch.js";

const APPLY = process.argv.includes("--apply");

/** Verified CALA open directory gaps (from Hilton crawl unmatched_directory). */
const CREATE_CANDIDATES = [
  {
    brand: "Curio Collection by Hilton",
    planFile: "reports/hilton-census-enrichment-plan-curio-collection-by-hilton.json",
    nameIncludes: ["Amare Cancun", "Punta Sal", "York Medellin"],
  },
  {
    brand: "Tapestry Collection by Hilton",
    planFile: "reports/hilton-census-enrichment-plan-tapestry-by-hilton.json",
    nameIncludes: ["Chelsea Bogota", "Perla La Paz"],
  },
];

const COUNTRY_FROM_NAME = [
  { re: /cancun|méxico|mexico|la paz/i, country: "Mexico" },
  { re: /medell[ií]n|bogot[aá]|colombia/i, country: "Colombia" },
  { re: /punta sal|peru|lima/i, country: "Peru" },
];

function inferCountry(name, sourceUrl) {
  const blob = `${name} ${sourceUrl}`;
  for (const row of COUNTRY_FROM_NAME) {
    if (row.re.test(blob)) return row.country;
  }
  // hilton URL path /en/hotels/... often has city
  if (/\/mx\//i.test(sourceUrl) || /mexico/i.test(sourceUrl)) return "Mexico";
  if (/\/co\//i.test(sourceUrl) || /colombia|bogota|medellin/i.test(sourceUrl)) return "Colombia";
  if (/\/pe\//i.test(sourceUrl) || /peru/i.test(sourceUrl)) return "Peru";
  return "";
}

function cityFromName(name) {
  if (/cancun/i.test(name)) return "Cancun";
  if (/medell/i.test(name)) return "Medellin";
  if (/bogot/i.test(name)) return "Bogota";
  if (/la paz/i.test(name)) return "La Paz";
  if (/punta sal/i.test(name)) return "Punta Sal";
  return "";
}

async function main() {
  mkdirSync("reports", { recursive: true });
  const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
    process.env.AIRTABLE_BASE_ID_ALT
  );

  /** @type {object[]} */
  const createRows = [];
  /** @type {object[]} */
  const skipped = [];

  for (const cfg of CREATE_CANDIDATES) {
    const plan = JSON.parse(readFileSync(cfg.planFile, "utf8"));
    const ud = (plan.planRows || []).filter((r) => r.status === "unmatched_directory");
    for (const needle of cfg.nameIncludes) {
      const row = ud.find((r) =>
        String(r.directoryName || "").toLowerCase().includes(needle.toLowerCase())
      );
      if (!row) {
        skipped.push({ brand: cfg.brand, needle, reason: "not_in_plan" });
        continue;
      }
      const name = String(row.directoryName || "").trim();
      const pid = String(row.directoryBrandPropertyCode || "").trim().toUpperCase();
      let website = "";
      let description = "";
      try {
        const gql = await fetchHiltonHotelDescription(pid);
        website = String(gql.website || "").trim().replace(/\/$/, "");
        description = String(gql.shortDesc || "").trim();
        if (gql.name) {
          // Prefer GraphQL formal name when present
        }
      } catch (err) {
        skipped.push({
          brand: cfg.brand,
          name,
          reason: "graphql_fetch_failed",
          error: String(err?.message || err),
        });
        continue;
      }
      const country = inferCountry(name, website);
      const city = cityFromName(name);
      if (!pid || !website || !country) {
        skipped.push({ brand: cfg.brand, name, reason: "missing_pid_url_or_country", pid, website, country });
        continue;
      }
      if (!/^https:\/\/www\.hilton\.com\/en\/hotels\//i.test(website)) {
        skipped.push({ brand: cfg.brand, name, reason: "website_not_hilton_property", website });
        continue;
      }

      // Duplicate check
      const existing = await base(HOTEL_CENSUS_TABLE)
        .select({
          filterByFormula: `OR({${CENSUS_PROPERTY_ID_FIELD}}="${pid}", FIND("${pid.toLowerCase()}", LOWER({Website})))`,
          fields: ["name", "Website", CENSUS_PROPERTY_ID_FIELD],
          maxRecords: 3,
        })
        .all();
      if (existing.length) {
        skipped.push({
          brand: cfg.brand,
          name,
          reason: "already_on_census",
          recordIds: existing.map((r) => r.id),
        });
        continue;
      }

      const affiliation =
        cfg.brand === "Tapestry Collection by Hilton"
          ? "Tapestry Collection by Hilton"
          : "Curio Collection by Hilton";

      const fields = {
        [CENSUS_FIELDS.name]: name.replace(/, Curio by Hilton$/i, ", Curio Collection by Hilton"),
        [CENSUS_FIELDS.affiliation]: affiliation,
        [CENSUS_FIELDS.parentCompany]: "Hilton Worldwide",
        [CENSUS_FIELDS.status]: [STATUS_OPEN],
        [CENSUS_FIELDS.projectPhase]: "Open",
        [CENSUS_FIELDS.country]: country,
        [CENSUS_FIELDS.city]: city || country,
        [CENSUS_FIELDS.region]: countryToDealalityRegion(country),
        [CENSUS_FIELDS.subContinent]: countryToSubContinent(country),
        [CENSUS_FIELDS.market]: city || country,
        Website: website,
        [CENSUS_PROPERTY_ID_FIELD]: pid,
      };

      if (description && description.length >= 40) {
        fields[CENSUS_DESCRIPTION_FIELD] = description;
      }

      // Amenities text if suggested and non-empty
      if (row.amenitiesTextSuggested && !isBlankCensusValue(row.amenitiesTextSuggested)) {
        fields.Amenities = row.amenitiesTextSuggested;
      }

      createRows.push({
        brand: affiliation,
        directoryName: name,
        propertyId: pid,
        propertyUrl: fields.Website,
        fields,
        validation: {
          pass: Boolean(fields.Website && fields[CENSUS_PROPERTY_ID_FIELD] && fields[CENSUS_FIELDS.affiliation]),
        },
      });
    }
  }

  writeFileSync(
    "reports/hilton-wave2-cala-directory-create-plan.json",
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        mode: APPLY ? "apply" : "dry-run",
        createCandidates: createRows.length,
        createRows,
        skipped,
      },
      null,
      2
    )
  );

  console.log("Create candidates:", createRows.length);
  for (const r of createRows) console.log(" ", r.fields.name, r.propertyId, r.fields.country);
  console.log("Skipped:", skipped.length);

  if (!APPLY) {
    console.log("DRY-RUN — re-run with --apply after review.");
    return;
  }

  const log = [];
  for (const row of createRows) {
    const [rec] = await base(HOTEL_CENSUS_TABLE).create([{ fields: row.fields }], {
      typecast: true,
    });
    log.push({ recordId: rec.id, ...row });
    console.log("CREATED", rec.id, row.fields.name);
  }
  writeFileSync(
    "reports/hilton-wave2-cala-directory-create-apply-log.json",
    JSON.stringify({ generatedAt: new Date().toISOString(), created: log.length, log }, null, 2)
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
