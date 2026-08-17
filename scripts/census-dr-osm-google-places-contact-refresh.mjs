#!/usr/bin/env node
/**
 * Google Places refresh for DR OSM HPC rows missing street Address and/or Phone.
 * Report-only. Does not write Airtable (feed into census:dr-osm-contact-enrichment).
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import { resolveGoogleApiKey } from "../lib/location-verification/google-api-config.js";
import { runGooglePlacesHotelUrlLookupBatch } from "../lib/independent-census/google-places-hotel-url-lookup.js";
import { identityKeyToOsmSourceId } from "../lib/independent-census/dr-osm-hpc-field-enrichment.js";
import {
  resolvePat,
  resolveTargetBase,
} from "../lib/research-engine-v2/production-census-schema-create.js";
import { PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID } from "../lib/research-engine-v2/production-census-source-of-truth.js";
import { isStreetLevelAddress } from "../lib/research-engine-v2/production-census-geocoding-providers.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");

function parseArgs(argv = process.argv.slice(2)) {
  const get = (name, fb = "") => {
    const i = argv.indexOf(name);
    return i >= 0 ? argv[i + 1] : fb;
  };
  return {
    limit: Number(get("--limit", "50")) || 50,
    maxRequests: Number(get("--max-requests", "50")) || 50,
    delayMs: Number(get("--delay-ms", "300")) || 300,
    output: get(
      "--output",
      "reports/census-dr-osm-google-places-contact-refresh.json"
    ),
  };
}

async function listNeedContact(baseId, token) {
  const fields = [
    "Property Name",
    "Current Brand",
    "City",
    "Country",
    "Address",
    "Phone",
    "Latitude",
    "Longitude",
    "Property Identity Key",
  ];
  const formula =
    "AND({Country}='Dominican Republic',FIND('independent_census_dr_osm',{VIC Freeze Hash}&''))";
  const out = [];
  let offset;
  do {
    const p = new URLSearchParams({ filterByFormula: formula, pageSize: "100" });
    for (const f of fields) p.append("fields[]", f);
    if (offset) p.set("offset", offset);
    const res = await fetch(
      `https://api.airtable.com/v0/${baseId}/${PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID}?${p}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(JSON.stringify(json.error || json));
    out.push(...(json.records || []));
    offset = json.offset;
  } while (offset);

  return out.filter((r) => {
    const f = r.fields || {};
    const needAddr = !isStreetLevelAddress(f.Address || "");
    const needPhone = !String(f.Phone || "").trim();
    return needAddr || needPhone;
  });
}

async function main() {
  const args = parseArgs();
  const apiKey = resolveGoogleApiKey();
  if (!apiKey) {
    console.error("Missing GOOGLE_PLACES_API_KEY / GOOGLE_MAPS_API_KEY");
    process.exit(1);
  }

  const token = resolvePat();
  const baseId = resolveTargetBase()?.target_base_id;
  const need = await listNeedContact(baseId, token);

  const rows = need.map((r) => {
    const f = r.fields || {};
    const sid = identityKeyToOsmSourceId(f["Property Identity Key"]);
    return {
      source_record_id: sid || r.id,
      airtable_record_id: r.id,
      property_name: f["Property Name"],
      current_brand: f["Current Brand"] || "",
      city: f.City || "",
      country: f.Country || "Dominican Republic",
      latitude: f.Latitude ?? null,
      longitude: f.Longitude ?? null,
      property_identity_key: f["Property Identity Key"],
      need_address: !isStreetLevelAddress(f.Address || ""),
      need_phone: !String(f.Phone || "").trim(),
    };
  });

  console.log(
    JSON.stringify(
      {
        phase: "google_places_contact_refresh",
        need_count: rows.length,
        limit: args.limit,
        max_requests: args.maxRequests,
      },
      null,
      2
    )
  );

  const batch = await runGooglePlacesHotelUrlLookupBatch(rows, {
    limit: args.limit,
    maxRequests: args.maxRequests,
    delayMs: args.delayMs,
    apiKey,
  });

  // Attach airtable ids for downstream apply
  const bySid = new Map(rows.map((r) => [r.source_record_id, r]));
  for (const res of batch.results || []) {
    const meta = bySid.get(res.source_record_id);
    if (meta) {
      res.airtable_record_id = meta.airtable_record_id;
      res.property_identity_key = meta.property_identity_key;
      res.need_address = meta.need_address;
      res.need_phone = meta.need_phone;
    }
  }

  const usable = (batch.results || []).filter((r) => {
    if (r.status !== "matched") return false;
    const conf = String(r.match_confidence || "").toLowerCase();
    if (conf !== "high" && conf !== "medium") return false;
    const addr = r.place?.google_formatted_address || "";
    const phone = r.place?.google_phone || "";
    return (
      (r.need_address && isStreetLevelAddress(addr)) ||
      (r.need_phone && phone)
    );
  });

  const report = {
    ...batch,
    mode: "report_only",
    airtable_write: false,
    need_input_count: rows.length,
    usable_contact_proposals: usable.length,
    usable_sample: usable.slice(0, 20).map((r) => ({
      n: r.property_name,
      conf: r.match_confidence,
      addr: r.place?.google_formatted_address || null,
      phone: r.place?.google_phone || null,
    })),
  };

  mkdirSync(dirname(join(root, args.output)), { recursive: true });
  writeFileSync(join(root, args.output), JSON.stringify(report, null, 2));
  console.log(
    JSON.stringify(
      {
        ok: true,
        output: args.output,
        need_input_count: report.need_input_count,
        processed: report.processed_count,
        matched: report.matched,
        usable_contact_proposals: report.usable_contact_proposals,
        request_count: report.request_count,
        airtable_write: false,
      },
      null,
      2
    )
  );
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
