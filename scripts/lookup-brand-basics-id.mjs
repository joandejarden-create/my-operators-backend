import "../load-env.js";
import Airtable from "airtable";

const name = process.argv[2] || "Curio Collection by Hilton";
const base = new Airtable({ apiKey: process.env.AIRTABLE_API_KEY }).base(process.env.AIRTABLE_BASE_ID);
const formula = `{Brand Name} = "${name.replace(/"/g, '\\"')}"`;
const rows = await base("Brand Setup - Brand Basics")
  .select({ filterByFormula: formula, maxRecords: 5, fields: ["Brand Name", "Parent Company"] })
  .all();
console.log(JSON.stringify(rows.map((r) => ({ id: r.id, name: r.fields["Brand Name"], parent: r.fields["Parent Company"] })), null, 2));
