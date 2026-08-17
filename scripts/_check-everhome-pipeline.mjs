import "../load-env.js";
import Airtable from "airtable";
import { buildBrandCensusSummary } from "../lib/hotel-census/build-brand-census-summary.js";

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID
);
const b = await base("Brand Setup - Brand Basics")
  .select({ filterByFormula: '{Brand Name} = "Everhome Suites"', maxRecords: 1 })
  .firstPage();
const fpId = b[0]?.fields["Brand Setup - Brand Footprint"]?.[0];
if (fpId) {
  const fp = await base("Brand Setup - Brand Footprint").find(fpId);
  console.log("Brand Footprint pipeline fields:");
  for (const k of Object.keys(fp.fields).sort()) {
    if (/pipeline|Pipeline|New Build|Conversion|Total Distribution/i.test(k)) {
      console.log(" ", k, ":", fp.fields[k]);
    }
  }
  console.log(" totalPipelineHotels-ish:", fp.fields["Total Pipeline Hotels"] ?? fp.fields["AM Pipeline Hotel"]);
}

const census = await buildBrandCensusSummary("Everhome Suites");
console.log("\nCensus summary metrics:", census?.metrics);
console.log("Census fallbackRecommended:", census?.fallbackRecommended);
