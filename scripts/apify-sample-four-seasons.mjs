#!/usr/bin/env node
/**
 * Sample only newly inventoried first-party Actors (Four Seasons).
 * Does not re-run Hilton/Marriott/Choice/IHG.
 */
import {
  resolvePat,
  resolveTargetBase,
} from "../lib/research-engine-v2/production-census-schema-create.js";
import { PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID } from "../lib/research-engine-v2/production-census-source-of-truth.js";
import { MAP_MASTER } from "../lib/research-engine-v2/master-census-enrichment-v1.js";
import { MAP_BRAND } from "../lib/research-engine-v2/master-brand-portfolio-validation-v1.js";
import { inventoryAndSampleApifyActors } from "../lib/research-engine-v2/apify-first-party-acquisition-v1.js";

const READ_FIELDS = [
  MAP_MASTER.propertyName,
  MAP_MASTER.canonicalName,
  MAP_MASTER.country,
  MAP_MASTER.city,
  MAP_MASTER.address,
  MAP_MASTER.postalCode,
  MAP_MASTER.latitude,
  MAP_MASTER.longitude,
  MAP_MASTER.currentBrand,
  MAP_MASTER.officialUrl,
  MAP_MASTER.phone,
  MAP_MASTER.roomsKeys,
  MAP_BRAND.candidateBrand,
];

async function listCensus(baseId, token) {
  const records = [];
  let offset;
  do {
    const params = new URLSearchParams({ pageSize: "100" });
    if (offset) params.set("offset", offset);
    for (const f of READ_FIELDS) params.append("fields[]", f);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID)}?${params}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(`census list ${res.status}`);
    records.push(...(json.records || []));
    offset = json.offset;
  } while (offset);
  return records;
}

const token = resolvePat();
const base = resolveTargetBase();
const baseId = base?.target_base_id || base?.baseId;
console.log("[apify] listing census for Four Seasons sample…");
const censusRecords = await listCensus(baseId, token);
const sampled = await inventoryAndSampleApifyActors({
  censusRecords,
  log: (m) => console.log(m),
  onlyCompanies: ["Four Seasons"],
});
const t = sampled.tested?.[0];
console.log(
  JSON.stringify(
    {
      status: t?.summary?.gate?.status,
      n: t?.summary?.metrics?.SAMPLE_SIZE,
      high: t?.summary?.metrics?.HIGH_MATCHES,
      identity: t?.summary?.metrics?.IDENTITY_ACCURACY,
      brand: t?.summary?.metrics?.BRAND_ACCURACY,
      cost: sampled.TOTAL_APIFY_COST,
      reasons: t?.summary?.gate?.reasons,
    },
    null,
    2
  )
);
