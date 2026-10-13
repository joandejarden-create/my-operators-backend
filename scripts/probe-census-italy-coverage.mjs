/**
 * Census coverage probe — Italy / Europe presence (read-only).
 */
import "../load-env.js";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";

const apiKey = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
const baseId = process.env.AIRTABLE_BASE_ID_ALT || process.env.AIRTABLE_BASE_ID;

async function main() {
  const base = new Airtable({ apiKey }).base(baseId);
  let total = 0;
  const countries = {};
  const italy = [];
  await base(HOTEL_CENSUS_TABLE)
    .select({
      pageSize: 100,
      fields: [
        CENSUS_FIELDS.name,
        CENSUS_FIELDS.country,
        CENSUS_FIELDS.city,
        CENSUS_FIELDS.affiliation,
        CENSUS_FIELDS.parentCompany,
      ],
    })
    .eachPage((records, next) => {
      for (const r of records) {
        total += 1;
        const c = String(r.fields[CENSUS_FIELDS.country] || "UNKNOWN").trim() || "UNKNOWN";
        countries[c] = (countries[c] || 0) + 1;
        if (/italy|italia/i.test(c)) {
          italy.push({
            id: r.id,
            name: r.fields[CENSUS_FIELDS.name],
            city: r.fields[CENSUS_FIELDS.city],
            affiliation: r.fields[CENSUS_FIELDS.affiliation],
          });
        }
      }
      next();
    });
  const top = Object.entries(countries)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 20);
  console.log(
    JSON.stringify(
      {
        baseId,
        table: HOTEL_CENSUS_TABLE,
        total,
        topCountries: top,
        italyCount: italy.length,
        italySample: italy.slice(0, 15),
        italyW: italy.filter((h) => /\bW\b|W Hotel|W Rome/i.test(`${h.name} ${h.affiliation || ""}`)),
      },
      null,
      2
    )
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
