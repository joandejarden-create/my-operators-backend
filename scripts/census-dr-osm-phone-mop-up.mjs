#!/usr/bin/env node
/** Phone-only mop-up from Google Places wave report for blank Phone rows. */
import "../load-env.js";
import { readFileSync } from "fs";
import {
  resolvePat,
  resolveTargetBase,
} from "../lib/research-engine-v2/production-census-schema-create.js";
import { PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID } from "../lib/research-engine-v2/production-census-source-of-truth.js";

const googlePath =
  process.argv[2] ||
  "reports/census-dr-osm-google-places-contact-refresh-wave2.json";
const g = JSON.parse(readFileSync(googlePath, "utf8"));
const token = resolvePat();
const baseId = resolveTargetBase().target_base_id;
const tid = PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID;
const formula =
  "AND({Country}='Dominican Republic',FIND('independent_census_dr_osm',{VIC Freeze Hash}&''))";

let offset;
const rows = [];
do {
  const p = new URLSearchParams({ filterByFormula: formula, pageSize: "100" });
  for (const f of ["Property Name", "Phone", "Property Identity Key"]) {
    p.append("fields[]", f);
  }
  if (offset) p.set("offset", offset);
  const res = await fetch(`https://api.airtable.com/v0/${baseId}/${tid}?${p}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const j = await res.json();
  if (!res.ok) throw new Error(JSON.stringify(j));
  rows.push(...(j.records || []));
  offset = j.offset;
} while (offset);

const byKey = new Map(
  rows.map((r) => [r.fields["Property Identity Key"], r])
);
const byName = new Map(rows.map((r) => [r.fields["Property Name"], r]));
const patches = [];
for (const r of g.results || []) {
  if (!r.place?.google_phone) continue;
  const rec =
    (r.property_identity_key && byKey.get(r.property_identity_key)) ||
    byName.get(r.property_name);
  if (!rec) continue;
  if (String(rec.fields.Phone || "").trim()) continue;
  patches.push({
    id: rec.id,
    n: r.property_name,
    fields: { Phone: r.place.google_phone },
  });
}

console.log(
  JSON.stringify(
    {
      blank_phone_now: rows.filter((r) => !String(r.fields.Phone || "").trim())
        .length,
      phone_patch_count: patches.length,
      names: patches.map((p) => p.n),
    },
    null,
    2
  )
);

if (!patches.length) process.exit(0);

const res = await fetch(`https://api.airtable.com/v0/${baseId}/${tid}`, {
  method: "PATCH",
  headers: {
    Authorization: `Bearer ${token}`,
    "Content-Type": "application/json",
  },
  body: JSON.stringify({
    records: patches.map((p) => ({ id: p.id, fields: p.fields })),
    typecast: true,
  }),
});
const j = await res.json();
if (!res.ok) throw new Error(JSON.stringify(j));
console.log(JSON.stringify({ phone_applied: (j.records || []).length }, null, 2));
