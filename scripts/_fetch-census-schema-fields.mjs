#!/usr/bin/env node
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";

const token = (
  process.env.AIRTABLE_PAT ||
  process.env.AIRTABLE_TOKEN ||
  process.env.AIRTABLE_API_KEY ||
  ""
).trim();
const baseId = (
  process.env.AIRTABLE_BASE_ID_ALT ||
  process.env.AIRTABLE_BASE_ID ||
  ""
).trim();
const tableId = "tbl9aY5ijiuIzzWam";
if (!token || !baseId) {
  console.error("missing Airtable credentials");
  process.exit(1);
}

const url = `https://api.airtable.com/v0/meta/bases/${baseId}/tables`;
const res = await fetch(url, {
  headers: { Authorization: `Bearer ${token}` },
});
const j = await res.json();
if (!res.ok) {
  console.error(JSON.stringify(j).slice(0, 500));
  process.exit(1);
}
const table = (j.tables || []).find(
  (t) => t.id === tableId || t.name === "Hotel Property Census"
);
if (!table) {
  console.error(
    "table not found",
    (j.tables || []).map((t) => t.name).slice(0, 30)
  );
  process.exit(1);
}
const fields = (table.fields || []).map((f) => ({
  id: f.id,
  name: f.name,
  type: f.type,
}));
const outDir = path.join(
  "data/hotel-intelligence/tripadvisor-census-profile-pack-v1"
);
fs.mkdirSync(outDir, { recursive: true });
fs.writeFileSync(
  path.join(outDir, "census-schema-fields.json"),
  `${JSON.stringify(
    {
      tableId: table.id,
      tableName: table.name,
      fieldCount: fields.length,
      fields,
    },
    null,
    2
  )}\n`
);
const names = fields.map((f) => f.name);
const needles = [
  "Email",
  "Phone",
  "Official Property URL",
  "Rooms / Keys",
  "Rooms Confidence",
  "Rooms Source",
  "Latitude",
  "Longitude",
  "Address",
  "City",
  "State / Region",
  "Country",
  "Postal",
  "Current Brand",
  "Property Type",
  "Hotel Class",
  "Star",
  "Class",
  "Amenit",
  "Website",
  "Tripadvisor",
  "GIATA",
];
const hits = needles.map((q) => ({
  q,
  matches: names.filter((n) => n.toLowerCase().includes(q.toLowerCase())),
}));
console.log(JSON.stringify({ fieldCount: fields.length, hits }, null, 2));
