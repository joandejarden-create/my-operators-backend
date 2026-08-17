#!/usr/bin/env node
import "../load-env.js";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";
import { CENSUS_AMENITIES_TEXT_FIELD } from "../lib/hilton-amenity-map.js";
import { parseMarriottAmenitiesText } from "../lib/marriott-amenity-format.js";

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID_ALT
);
const recs = await base(HOTEL_CENSUS_TABLE)
  .select({
    filterByFormula: `FIND("Marriott", {Parent Company})`,
    fields: ["name", CENSUS_AMENITIES_TEXT_FIELD],
    pageSize: 100,
  })
  .all();

const withAm = recs.filter((r) => String(r.get(CENSUS_AMENITIES_TEXT_FIELD) || "").trim());
const lens = withAm.map((r) => parseMarriottAmenitiesText(r.get(CENSUS_AMENITIES_TEXT_FIELD)).length);
const hist = {};
for (const n of lens) hist[n] = (hist[n] || 0) + 1;

console.log("With amenities:", withAm.length, "/", recs.length);
console.log("Distribution (# amenities per hotel):", hist);
console.log("\nSamples with 4-6 amenities:");
for (const r of withAm) {
  const items = parseMarriottAmenitiesText(r.get(CENSUS_AMENITIES_TEXT_FIELD));
  if (items.length >= 4 && items.length <= 6) {
    console.log("-", r.get("name"), "→", items.join("; "));
  }
}
