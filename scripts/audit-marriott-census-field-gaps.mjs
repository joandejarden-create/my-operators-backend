#!/usr/bin/env node
import "../load-env.js";
import Airtable from "airtable";
import { HOTEL_CENSUS_TABLE, CENSUS_FIELDS } from "../lib/hotel-census/fields.js";

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID_ALT);
const formula = `FIND("Marriott", {${CENSUS_FIELDS.parentCompany}})`;
const fields = ["name", CENSUS_FIELDS.affiliation, CENSUS_FIELDS.parentCompany, "Website", "Hotel Description", "Amenities", "Open Date", "Property ID", "country", CENSUS_FIELDS.status];
const rows = await base(HOTEL_CENSUS_TABLE).select({ filterByFormula: formula, fields, pageSize: 100 }).all();

function blank(v) {
  return v == null || String(v).trim() === "";
}
function hasMarriottSite(w) {
  return /marriott\.com/i.test(String(w || ""));
}
function marshaFromWebsite(url) {
  const m = String(url || "").match(/\/hotels\/([a-z0-9]+)-/i);
  return m ? m[1].toUpperCase() : "";
}

const blanks = { description: 0, amenities: 0, website: 0, openDate: 0, propertyId: 0 };
let withSite = 0;
let withMarsha = 0;
for (const r of rows) {
  const w = r.get("Website");
  if (hasMarriottSite(w)) withSite++;
  if (marshaFromWebsite(w)) withMarsha++;
  if (blank(r.get("Hotel Description"))) blanks.description++;
  if (blank(r.get("Amenities"))) blanks.amenities++;
  if (blank(w)) blanks.website++;
  if (blank(r.get("Open Date"))) blanks.openDate++;
  if (blank(r.get("Property ID"))) blanks.propertyId++;
}

console.log("Marriott-parent census rows:", rows.length);
console.log("With marriott.com website:", withSite);
console.log("With MARSHA from website:", withMarsha);
console.log("Blank counts:", blanks);
