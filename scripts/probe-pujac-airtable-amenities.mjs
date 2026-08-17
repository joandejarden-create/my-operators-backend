#!/usr/bin/env node
import "../load-env.js";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE } from "../lib/hotel-census/fields.js";
import { CENSUS_AMENITIES_TEXT_FIELD } from "../lib/hilton-amenity-map.js";
import { buildHiltonAmenitiesDisplay } from "../lib/hilton-amenity-display.js";
import { parseMarriottAmenitiesText } from "../lib/marriott-amenity-format.js";

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID_ALT
);
const recs = await base(HOTEL_CENSUS_TABLE)
  .select({
    filterByFormula: `FIND("AC Hotel Punta Cana", {name})`,
    fields: ["name", CENSUS_AMENITIES_TEXT_FIELD, "Property ID"],
    maxRecords: 5,
  })
  .all();

for (const r of recs) {
  const amenities = r.get(CENSUS_AMENITIES_TEXT_FIELD);
  const parsed = parseMarriottAmenitiesText(amenities);
  const display = buildHiltonAmenitiesDisplay({ amenities });
  console.log("\nRecord:", r.id, r.get("name"));
  console.log("Property ID:", r.get("Property ID"));
  console.log("Raw Amenities field length:", String(amenities || "").length);
  console.log("Parsed count:", parsed.length);
  console.log("Parsed:", parsed.join(" | "));
  console.log("Display count:", display.length);
  console.log("Display:", display.map((d) => d.label).join(" | "));
}
