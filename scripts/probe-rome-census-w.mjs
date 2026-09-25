/**
 * Broader Rome census probe + Marriott MARSHA ROMWV lookup (read-only).
 */
import "../load-env.js";
import Airtable from "airtable";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";

const apiKey = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
const baseId = process.env.AIRTABLE_BASE_ID_ALT || process.env.AIRTABLE_BASE_ID;
const outDir = path.join(
  path.dirname(fileURLToPath(import.meta.url)),
  "../reports/group-demand-intelligence/cross-market-replication-v1"
);

async function main() {
  fs.mkdirSync(outDir, { recursive: true });
  const base = new Airtable({ apiKey }).base(baseId);
  const rome = [];
  await base(HOTEL_CENSUS_TABLE)
    .select({ pageSize: 100 })
    .eachPage((records, next) => {
      for (const r of records) {
        const f = r.fields || {};
        const city = String(f[CENSUS_FIELDS.city] || "").toLowerCase();
        const country = String(f[CENSUS_FIELDS.country] || "").toLowerCase();
        const name = String(f[CENSUS_FIELDS.name] || "");
        if (city.includes("rome") || country === "italy" && /rome|roma/i.test(name)) {
          rome.push({
            id: r.id,
            name,
            affiliation: f[CENSUS_FIELDS.affiliation],
            parent: f[CENSUS_FIELDS.parentCompany],
            city: f[CENSUS_FIELDS.city],
            country: f[CENSUS_FIELDS.country],
            rooms: f[CENSUS_FIELDS.rooms],
            status: f[CENSUS_FIELDS.status],
          });
        }
      }
      next();
    });

  const wish = rome.filter((h) =>
    /\bW\b|W Hotel|W Rome|ROMWV/i.test(`${h.name} ${h.affiliation || ""}`)
  );
  const marriottRome = rome.filter((h) =>
    /marriott|bonvoy|sheraton|westin|renaissance|autograph|tribute|edition|st\.?\s*regis|ritz|jw /i.test(
      `${h.name} ${h.affiliation || ""} ${h.parent || ""}`
    )
  );

  const report = {
    baseId,
    table: HOTEL_CENSUS_TABLE,
    romeCityCount: rome.length,
    wLike: wish,
    marriottRomeSample: marriottRome.slice(0, 30),
    marriottRomeCount: marriottRome.length,
    officialFactsExternal: {
      name: "W Rome",
      marsha: "ROMWV",
      website: "https://www.marriott.com/en-us/hotels/romwv-w-rome/overview/",
      address: "26/36 Via Liguria, Rome, Italy, 00187",
      rooms: 148,
      meetingRooms: 2,
      meetingSpaceSqM: 60,
      meetingSpaceSqFt: 646,
      largestTheater: 30,
      largestReception: 40,
      brand: "W Hotels",
      parent: "Marriott International",
      source: "marriott.com events page + cvent",
    },
    censusStatus: wish.length ? "FOUND" : "MISSING_FROM_CENSUS",
  };
  fs.writeFileSync(path.join(outDir, "PHASE0_ROME_CENSUS_PROBE.json"), JSON.stringify(report, null, 2));
  console.log(
    JSON.stringify(
      {
        ok: true,
        romeCityCount: rome.length,
        wLike: wish.length,
        marriottRomeCount: marriottRome.length,
        censusStatus: report.censusStatus,
        wLikeHits: wish,
        marriottSample: marriottRome.slice(0, 10),
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
