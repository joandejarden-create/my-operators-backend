#!/usr/bin/env node
/** List DR OSM rows missing Market or Submarket. */
import "../load-env.js";
import { writeFileSync } from "fs";
import {
  resolvePat,
  resolveTargetBase,
} from "../lib/research-engine-v2/production-census-schema-create.js";
import { PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID } from "../lib/research-engine-v2/production-census-source-of-truth.js";
import { resolveMarketFromCity } from "../lib/research-engine-v2/census-region-market-map.js";
import { proposeCensusSubmarketCorridor } from "../lib/hotel-census/census-dealality-submarket.js";

const token = resolvePat();
const baseId = resolveTargetBase()?.target_base_id;
const tid = PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID;
const formula =
  "AND({Country}='Dominican Republic',FIND('independent_census_dr_osm',{VIC Freeze Hash}&''))";
const fields = [
  "Property Name",
  "City",
  "State / Region",
  "Market",
  "Submarket",
  "Address",
  "Property Identity Key",
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

const gaps = [];
for (const r of rows) {
  const f = r.fields || {};
  const city = String(f.City || "").trim();
  const market = String(f.Market || "").trim();
  const sub = String(f.Submarket || "").trim();
  if (market && sub) continue;
  const m = !market
    ? resolveMarketFromCity({ city, country: "Dominican Republic" })
    : { ok: true, market };
  const proposedMarket = m.ok ? m.market : null;
  const subProp = proposeCensusSubmarketCorridor(
    {
      country: "Dominican Republic",
      city,
      market: proposedMarket || market || "",
      Market: proposedMarket || market || "",
      Submarket: "",
      name: f["Property Name"] || "",
    },
    { minConfidence: "Medium", normalizeLabels: true }
  );
  gaps.push({
    id: r.id,
    n: f["Property Name"],
    city,
    state: f["State / Region"] || null,
    market: market || null,
    submarket: sub || null,
    propose_market: proposedMarket,
    propose_market_ok: m.ok,
    propose_submarket: subProp.submarket || null,
    propose_sub_conf: subProp.confidence || null,
    propose_sub_reason: subProp.reason || null,
    key: f["Property Identity Key"],
  });
}

writeFileSync(
  "reports/census-dr-osm-market-submarket-gaps.json",
  JSON.stringify({ total: rows.length, gap_count: gaps.length, gaps }, null, 2)
);
console.log(JSON.stringify({ total: rows.length, gap_count: gaps.length, gaps }, null, 2));
