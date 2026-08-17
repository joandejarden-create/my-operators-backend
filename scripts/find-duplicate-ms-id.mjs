import "../load-env.js";
import Airtable from "airtable";
import { INTAKE_USERS_UNIQUE_WEBFLOW_ID } from "../api/schemas/intake-deal-fields.js";
import { cellToString } from "../lib/airtable-utils.js";

const ms = process.argv[2] || "mem_cmquujoc500160sl4hfskh22t";
const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(
  process.env.AIRTABLE_BASE_ID
);

const esc = ms.replace(/\\/g, "\\\\").replace(/'/g, "\\'");
const formula = `OR({${INTAKE_USERS_UNIQUE_WEBFLOW_ID}} = '${esc}', {Slug} = '${esc}')`;
console.log("Searching for:", ms);

const rows = await base("tbl6shiyz2wdUqE5F")
  .select({ filterByFormula: formula, maxRecords: 20 })
  .firstPage();

console.log("Matches:", rows.length);
for (const r of rows) {
  const f = r.fields || {};
  console.log("---", r.id);
  console.log(" Email:", cellToString(f.Email));
  console.log(
    " Unique Webflow ID:",
    cellToString(f["Unique Webflow ID"]) || cellToString(f[INTAKE_USERS_UNIQUE_WEBFLOW_ID])
  );
  console.log(" Slug:", cellToString(f.Slug));
  console.log(" Deals:", (f.Deals || []).length);
  console.log(" Company:", (f["Company Profile"] || [])[0] || "none");
}
