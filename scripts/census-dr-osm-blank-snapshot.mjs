#!/usr/bin/env node
/** Quick DR OSM HPC blank-field snapshot (avoids PowerShell formula escaping). */
import "../load-env.js";
import {
  resolvePat,
  resolveTargetBase,
} from "../lib/research-engine-v2/production-census-schema-create.js";
import { PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID } from "../lib/research-engine-v2/production-census-source-of-truth.js";

const token = resolvePat();
const baseId = resolveTargetBase().target_base_id;
const tid = PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID;
const formula =
  "AND({Country}='Dominican Republic',FIND('independent_census_dr_osm',{VIC Freeze Hash}&''))";
let offset;
const rows = [];
do {
  const p = new URLSearchParams({ filterByFormula: formula, pageSize: "100" });
  for (const f of [
    "Property Name",
    "Official Property URL",
    "Rooms / Keys",
    "Phone",
    "Address",
  ]) {
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

const blank = (k) =>
  rows.filter((r) => {
    const v = r.fields[k];
    return v == null || v === "" || (typeof v === "string" && !v.trim());
  });

console.log(
  JSON.stringify(
    {
      total: rows.length,
      blank_rooms: blank("Rooms / Keys").length,
      blank_phone: blank("Phone").length,
      blank_address: blank("Address").length,
      blank_phone_names: blank("Phone").map((r) => r.fields["Property Name"]),
    },
    null,
    2
  )
);
