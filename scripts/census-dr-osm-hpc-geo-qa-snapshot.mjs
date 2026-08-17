#!/usr/bin/env node
/** Snapshot DR OSM HPC geography + sample QA. */
import "../load-env.js";
import { writeFileSync, mkdirSync } from "fs";
import { resolvePat, resolveTargetBase } from "../lib/research-engine-v2/production-census-schema-create.js";
import { PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID } from "../lib/research-engine-v2/production-census-source-of-truth.js";

const token = resolvePat();
const baseId = resolveTargetBase()?.target_base_id;
const tid = PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID;
const formula =
  "AND({Country}='Dominican Republic',FIND('independent_census_dr_osm',{VIC Freeze Hash}&''))";
const fields = [
  "Property Name",
  "Current Brand",
  "Brand Family",
  "Family / Source Family",
  "City",
  "State / Region",
  "Official Property URL",
  "Property Identity Key",
  "Human Review Required",
  "VIC Freeze Hash",
  "Affiliation Status",
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

const blankCity = rows.filter(
  (r) => !String(r.fields.City || "").trim() || /^unknown$/i.test(r.fields.City)
);
const blankState = rows.filter(
  (r) =>
    String(r.fields.City || "").trim() &&
    !/^unknown$/i.test(r.fields.City) &&
    !String(r.fields["State / Region"] || "").trim()
);
const hr = rows.filter((r) => r.fields["Human Review Required"] === true);

// Quick QA checks on HR sample
const qaIssues = [];
for (const r of hr.slice(0, 40)) {
  const f = r.fields || {};
  const url = String(f["Official Property URL"] || "");
  const city = String(f.City || "");
  const fam = String(f["Family / Source Family"] || "");
  const bf = String(f["Brand Family"] || "");
  if (!url) qaIssues.push({ id: r.id, n: f["Property Name"], issue: "missing_url" });
  if (/^unknown$/i.test(city)) qaIssues.push({ id: r.id, n: f["Property Name"], issue: "city_unknown" });
  if (fam === "Other") qaIssues.push({ id: r.id, n: f["Property Name"], issue: "family_other" });
  if (/^(IHG|Marriott|Choice)$/i.test(bf))
    qaIssues.push({ id: r.id, n: f["Property Name"], issue: "brand_family_short_alias", bf });
  if (/hostel|hostal/i.test(f["Property Name"] || ""))
    qaIssues.push({ id: r.id, n: f["Property Name"], issue: "hostel_name" });
}

const out = {
  total: rows.length,
  hr: hr.length,
  blankCity: blankCity.length,
  blankState: blankState.length,
  blankCityRows: blankCity.map((r) => ({
    id: r.id,
    n: r.fields["Property Name"],
    b: r.fields["Current Brand"],
    city: r.fields.City || null,
    url: r.fields["Official Property URL"] || "",
    key: r.fields["Property Identity Key"],
  })),
  blankStateRows: blankState.map((r) => ({
    id: r.id,
    n: r.fields["Property Name"],
    city: r.fields.City,
    state: r.fields["State / Region"] || null,
  })),
  qa_issues_sample: qaIssues,
};

mkdirSync("reports", { recursive: true });
writeFileSync("reports/census-dr-osm-hpc-geo-qa-snapshot.json", JSON.stringify(out, null, 2));
console.log(
  JSON.stringify(
    {
      total: out.total,
      hr: out.hr,
      blankCity: out.blankCity,
      blankState: out.blankState,
      qa_issue_count: qaIssues.length,
      qa_by: qaIssues.reduce((m, x) => {
        m[x.issue] = (m[x.issue] || 0) + 1;
        return m;
      }, {}),
    },
    null,
    2
  )
);
