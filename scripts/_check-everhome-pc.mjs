import "../load-env.js";
import Airtable from "airtable";

const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID
);
const r = await base("Brand Setup - Brand Basics")
  .select({ filterByFormula: '{Brand Name} = "Everhome Suites"', maxRecords: 1 })
  .all();
console.log(r[0]?.get("Parent Company"), r[0]?.id);
