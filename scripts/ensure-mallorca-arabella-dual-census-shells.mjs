#!/usr/bin/env node
/**
 * Ensure Hotel Property Census shells for Mallorca Arabella / Son Vida dual hotels.
 * Independent identity keys — do not merge Castillo ↔ Sheraton.
 *
 *   node scripts/ensure-mallorca-arabella-dual-census-shells.mjs --dry-run
 *   node scripts/ensure-mallorca-arabella-dual-census-shells.mjs --apply
 */
import "dotenv/config";
import Airtable from "airtable";

const APPLY = process.argv.includes("--apply");
const key = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
const baseId = process.env.AIRTABLE_BASE_ID_ALT;
if (!key || !baseId) {
  console.error("Missing AIRTABLE_API_KEY/PAT or AIRTABLE_BASE_ID_ALT");
  process.exit(1);
}

const base = new Airtable({ apiKey: key }).base(baseId);
const TABLE = "Hotel Property Census";

const HOTELS = [
  {
    slug: "castillo_hotel_son_vida",
    identityKey: "ind_marriott_es_pmilc",
    findFormula:
      "OR(FIND('Castillo Hotel Son Vida', {Property Name}), {Property Identity Key}='ind_marriott_es_pmilc')",
    payload: {
      "Property Name": "Castillo Hotel Son Vida, a Luxury Collection Hotel, Mallorca",
      "Canonical Property Name": "Castillo Hotel Son Vida, a Luxury Collection Hotel, Mallorca",
      City: "Palma de Mallorca",
      Country: "Spain",
      Continent: "Europe",
      Market: "Mallorca",
      Submarket: "Son Vida / Palma",
      "Postal Code": "07013",
      Address: "C/Raixa 2, Urbanización Son Vida, 07013 Palma de Mallorca, Spain",
      Latitude: 39.5928,
      Longitude: 2.5936,
      "Rooms / Keys": 164,
      "Rooms Confidence": "High",
      "Rooms Source Type": "Official / Brand",
      "Rooms Source URL":
        "https://www.marriott.com/en-us/hotels/pmilc-castillo-hotel-son-vida-a-luxury-collection-hotel-mallorca/rooms/",
      "Official Property URL":
        "https://www.marriott.com/en-us/hotels/pmilc-castillo-hotel-son-vida-a-luxury-collection-hotel-mallorca/overview/",
      "Source URL":
        "https://www.marriott.com/en-us/hotels/pmilc-castillo-hotel-son-vida-a-luxury-collection-hotel-mallorca/overview/",
      "Current Brand": "Luxury Collection",
      "Brand Family": "Marriott International",
      "Identity Confidence": "High",
      "Production Use Status": "Census Only / Not Owner-Facing",
      "Property Identity Key": "ind_marriott_es_pmilc",
      "Source Type": "Official / Brand",
      "Source Confidence": "High",
      "Address Confidence": "High",
      "Notes for Steward":
        "ADP+GDI Mallorca dual e2e 2026-10-08. Marriott PMILC. 164 rooms; adults-only Luxury Collection castle hotel in Son Vida. Distinct from Sheraton Mallorca Arabella Golf (PMISI). Region: Illes Balears.",
    },
  },
  {
    slug: "sheraton_mallorca_arabella_golf",
    identityKey: "ind_marriott_es_pmisi",
    findFormula:
      "OR(FIND('Sheraton Mallorca Arabella', {Property Name}), {Property Identity Key}='ind_marriott_es_pmisi')",
    payload: {
      "Property Name": "Sheraton Mallorca Arabella Golf Hotel",
      "Canonical Property Name": "Sheraton Mallorca Arabella Golf Hotel",
      City: "Palma de Mallorca",
      Country: "Spain",
      Continent: "Europe",
      Market: "Mallorca",
      Submarket: "Son Vida / Palma",
      "Postal Code": "07013",
      Address: "Carrer de la Vinagrella, Urbanización Son Vida, 07013 Palma de Mallorca, Spain",
      Latitude: 39.5912,
      Longitude: 2.5958,
      "Rooms / Keys": 93,
      "Rooms Confidence": "High",
      "Rooms Source Type": "Official / Brand",
      "Rooms Source URL":
        "https://www.marriott.com/en-us/hotels/pmisi-sheraton-mallorca-arabella-golf-hotel/rooms/",
      "Official Property URL":
        "https://www.marriott.com/en-us/hotels/pmisi-sheraton-mallorca-arabella-golf-hotel/overview/",
      "Source URL":
        "https://www.marriott.com/en-us/hotels/pmisi-sheraton-mallorca-arabella-golf-hotel/overview/",
      "Current Brand": "Sheraton",
      "Brand Family": "Marriott International",
      "Identity Confidence": "High",
      "Production Use Status": "Census Only / Not Owner-Facing",
      "Property Identity Key": "ind_marriott_es_pmisi",
      "Source Type": "Official / Brand",
      "Source Confidence": "High",
      "Address Confidence": "High",
      "Notes for Steward":
        "ADP+GDI Mallorca dual e2e 2026-10-08. Marriott PMISI. 93 rooms; golf resort in Son Vida. Distinct from Castillo Hotel Son Vida Luxury Collection (PMILC). Region: Illes Balears.",
    },
  },
];

async function findExisting(hotel) {
  const rows = [];
  await base(TABLE)
    .select({ maxRecords: 5, filterByFormula: hotel.findFormula })
    .eachPage((r, n) => {
      rows.push(...r);
      n();
    });
  return rows;
}

const results = [];
for (const hotel of HOTELS) {
  const existing = await findExisting(hotel);
  if (existing.length) {
    results.push({
      slug: hotel.slug,
      action: "REUSE",
      censusRecordId: existing[0].id,
      name: existing[0].fields["Property Name"],
      identityKey: hotel.identityKey,
      apply: APPLY,
    });
    continue;
  }

  if (!APPLY) {
    results.push({
      slug: hotel.slug,
      action: "DRY_RUN_CREATE",
      identityKey: hotel.identityKey,
      payload: hotel.payload,
    });
    continue;
  }

  const created = await base(TABLE).create([{ fields: hotel.payload }], { typecast: true });
  results.push({
    slug: hotel.slug,
    action: "CREATED",
    censusRecordId: created[0].id,
    name: created[0].fields["Property Name"],
    identityKey: hotel.identityKey,
  });
}

console.log(JSON.stringify({ ok: true, apply: APPLY, results }, null, 2));
