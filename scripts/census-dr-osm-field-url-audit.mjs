#!/usr/bin/env node
/** Audit DR OSM HPC: missing geo/contact fields + brand-homepage Official URLs. */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import {
  resolvePat,
  resolveTargetBase,
} from "../lib/research-engine-v2/production-census-schema-create.js";
import { PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID } from "../lib/research-engine-v2/production-census-source-of-truth.js";
import {
  classifyOfficialPropertyUrl,
  isBrandHomepageOfficialUrl,
} from "../lib/independent-census/official-property-url-quality.js";

const token = resolvePat();
const baseId = resolveTargetBase()?.target_base_id;
const tid = PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID;
const formula =
  "AND({Country}='Dominican Republic',FIND('independent_census_dr_osm',{VIC Freeze Hash}&''))";
const fields = [
  "Property Name",
  "Current Brand",
  "Official Property URL",
  "Address",
  "Phone",
  "Continent",
  "Sub-Continent",
  "Market",
  "Submarket",
  "Rooms / Keys",
  "City",
  "State / Region",
  "Property Identity Key",
  "Human Review Required",
  "Latitude",
  "Longitude",
];

let offset;
const rows = [];
do {
  const p = new URLSearchParams({ filterByFormula: formula, pageSize: "100" });
  for (const f of fields) p.append("fields[]", f);
  if (offset) p.set("offset", offset);
  const res = await fetch(`https://api.airtable.com/v0/${baseId}/${tid}?${p}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const json = await res.json();
  if (!res.ok) throw new Error(JSON.stringify(json.error || json));
  rows.push(...(json.records || []));
  offset = json.offset;
} while (offset);

const missing = {
  address: 0,
  phone: 0,
  continent: 0,
  subcontinent: 0,
  market: 0,
  submarket: 0,
  rooms: 0,
  lat: 0,
};
const weak = [];
for (const r of rows) {
  const f = r.fields || {};
  if (!String(f.Address || "").trim()) missing.address++;
  if (!String(f.Phone || "").trim()) missing.phone++;
  if (!String(f.Continent || "").trim()) missing.continent++;
  if (!String(f["Sub-Continent"] || "").trim()) missing.subcontinent++;
  if (!String(f.Market || "").trim()) missing.market++;
  if (!String(f.Submarket || "").trim()) missing.submarket++;
  if (f["Rooms / Keys"] == null || f["Rooms / Keys"] === "") missing.rooms++;
  if (f.Latitude == null || f.Longitude == null) missing.lat++;
  const url = String(f["Official Property URL"] || "").trim();
  if (!url) {
    weak.push({
      id: r.id,
      n: f["Property Name"],
      b: f["Current Brand"],
      url,
      issue: "missing_url",
      key: f["Property Identity Key"],
    });
    continue;
  }
  if (isBrandHomepageOfficialUrl(url)) {
    const c = classifyOfficialPropertyUrl(url);
    weak.push({
      id: r.id,
      n: f["Property Name"],
      b: f["Current Brand"],
      url,
      host: c.host,
      path: c.path,
      issue: c.reason,
      key: f["Property Identity Key"],
    });
  }
}

const out = {
  total: rows.length,
  missing,
  weak_url_count: weak.length,
  weak_by_issue: weak.reduce((m, x) => {
    m[x.issue] = (m[x.issue] || 0) + 1;
    return m;
  }, {}),
  weak,
};
mkdirSync("reports", { recursive: true });
writeFileSync(
  "reports/census-dr-osm-field-url-audit.json",
  JSON.stringify(out, null, 2)
);
console.log(
  JSON.stringify(
    {
      total: out.total,
      missing: out.missing,
      weak_url_count: out.weak_url_count,
      weak_by_issue: out.weak_by_issue,
      weak_sample: weak.slice(0, 25),
    },
    null,
    2
  )
);
