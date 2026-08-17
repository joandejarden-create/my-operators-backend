#!/usr/bin/env node
/**
 * Fill blank Choice HPC Address from OSM Nominatim hotel matches (brand+city aligned).
 * Never stores Nominatim lat/lng — Address text only; Mapbox remains coord path.
 * Default dry-run. Live: --apply --enable-production-writes + confirms + env.
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import { isStreetLevelAddress } from "../lib/research-engine-v2/production-census-geocoding-providers.js";
import {
  resolveMapboxCoordinates,
  MAPBOX_COORDINATE_STATUSES,
} from "../lib/research-engine-v2/census-mapbox-coordinate-provider.js";
import { INTAKE_APPLY_CONFIRMS } from "../lib/independent-census/intake-autopilot-controlled.js";
import { checkIntakeApplyEnv } from "../lib/independent-census/intake-autopilot-apply.js";
import {
  resolvePat,
  resolveTargetBase,
} from "../lib/research-engine-v2/production-census-schema-create.js";
import { TABLE_IDS } from "../lib/research-engine-v2/production-census-write.js";
import {
  assertProductionCensusWriteTarget,
  productionHotelPropertyCensus,
  PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID,
} from "../lib/research-engine-v2/production-census-source-of-truth.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const CENSUS_TABLE_ID =
  TABLE_IDS["Hotel Property Census"] || PRODUCTION_HOTEL_PROPERTY_CENSUS_TABLE_ID;

function todayIsoDate() {
  return new Date().toISOString().slice(0, 10);
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function blank(v) {
  return v == null || !String(v).trim();
}
function norm(s) {
  return String(s || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .replace(/[^a-z0-9]+/g, " ")
    .trim();
}

function parseArgs(argv = process.argv.slice(2)) {
  const confirms = {};
  for (const f of INTAKE_APPLY_CONFIRMS) confirms[f] = argv.includes(f);
  return {
    apply: argv.includes("--apply") && argv.includes("--enable-production-writes"),
    withMapbox: !argv.includes("--skip-mapbox"),
    confirms,
    allConfirmsOk: Object.values(confirms).every(Boolean),
  };
}

function brandTokens(fields) {
  const brand = norm(fields["Current Brand"]);
  const name = norm(fields["Property Name"]);
  const tokens = [];
  for (const t of [
    "sleep inn",
    "comfort inn",
    "quality inn",
    "radisson",
    "park inn",
    "ascend",
    "fiesta americana",
    "faranda",
  ]) {
    if (brand.includes(t) || name.includes(t)) tokens.push(t);
  }
  return tokens;
}

function cityAligned(city, hit) {
  const cityCue = norm(city);
  const hay = norm(
    [hit.display_name, hit.address?.city, hit.address?.town, hit.address?.municipality, hit.address?.state]
      .filter(Boolean)
      .join(" ")
  );
  if (!cityCue) return false;
  if (hay.includes(cityCue)) return true;
  if (cityCue === "cabo san lucas" && hay.includes("los cabos")) return true;
  if (cityCue === "tuxtla gutierrez" && hay.includes("tuxtla")) return true;
  return false;
}

function brandAligned(fields, hit) {
  const hay = norm(hit.display_name);
  const tokens = brandTokens(fields);
  if (!tokens.length) return false;
  return tokens.some((t) => hay.includes(t));
}

function formatOsmAddress(hit) {
  const a = hit.address || {};
  const road = a.road || a.pedestrian || a.highway || "";
  const house = a.house_number || "";
  const suburb = a.suburb || a.neighbourhood || a.quarter || "";
  const city = a.city || a.town || a.municipality || "";
  const state = a.state || "";
  const postcode = a.postcode || "";
  const country = a.country || "Mexico";
  const line1 = [house, road].filter(Boolean).join(" ").trim() || road;
  if (!line1) {
    // fallback: strip hotel name prefix from display_name
    const parts = String(hit.display_name || "").split(",").map((s) => s.trim());
    return parts.slice(1).join(", ");
  }
  return [line1, suburb, postcode, city, state, country].filter(Boolean).join(", ");
}

async function nominatimHotel(query) {
  const url =
    "https://nominatim.openstreetmap.org/search?format=json&limit=5&addressdetails=1&q=" +
    encodeURIComponent(query);
  const res = await fetch(url, {
    headers: { "User-Agent": "DealalityCensusBot/1.0 (+https://dealality.com; research)" },
  });
  if (!res.ok) return [];
  const json = await res.json();
  return (json || []).filter((h) => h.class === "tourism" || h.type === "hotel");
}

async function listBlankAddress(baseId, token) {
  const formula =
    "AND({Brand Family}='Choice Hotels International',OR({Address}='',{Address}=BLANK()))";
  let offset;
  const out = [];
  do {
    const p = new URLSearchParams({ pageSize: "100", filterByFormula: formula });
    for (const f of [
      "Property Name",
      "Current Brand",
      "City",
      "Country",
      "State / Region",
      "Address",
      "Official Property URL",
      "Property Identity Key",
      "Latitude",
      "Longitude",
    ]) {
      p.append("fields[]", f);
    }
    if (offset) p.set("offset", offset);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(CENSUS_TABLE_ID)}?${p}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(JSON.stringify(json.error || json));
    out.push(...(json.records || []));
    offset = json.offset;
  } while (offset);
  return out;
}

async function patchRecords(baseId, token, records) {
  const updated = [];
  for (let i = 0; i < records.length; i += 10) {
    const chunk = records.slice(i, i + 10);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(CENSUS_TABLE_ID)}`,
      {
        method: "PATCH",
        headers: {
          Authorization: `Bearer ${token}`,
          "Content-Type": "application/json",
        },
        body: JSON.stringify({ records: chunk, typecast: true }),
      }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(JSON.stringify(json.error || json));
    updated.push(...(json.records || []));
  }
  return updated;
}

async function main() {
  const args = parseArgs();
  const writeTarget = assertProductionCensusWriteTarget({
    baseName: productionHotelPropertyCensus.baseName,
    tableName: productionHotelPropertyCensus.tableName,
    tableId: CENSUS_TABLE_ID,
  });
  if (!writeTarget.ok) {
    console.error(JSON.stringify({ ok: false, blocked: "wrong_write_target" }));
    process.exit(1);
  }

  const token = resolvePat();
  const baseId = resolveTargetBase()?.target_base_id;
  const rows = await listBlankAddress(baseId, token);
  const proposals = [];
  const steward = [];
  let mapboxLookups = 0;

  for (const rec of rows) {
    const f = rec.fields || {};
    const city = String(f.City || "").trim();
    const country = String(f.Country || "Mexico").trim();
    const name = String(f["Property Name"] || "").trim();
    const brand = String(f["Current Brand"] || "").trim();
    // Alternate queries: Nominatim often misses "by Choice" brand strings;
    // try hotel-normalized names (e.g. Radisson Hotel Monterrey San Jeronimo).
    const nameNoByChoice = name.replace(/\s+by\s+Choice\b/gi, "").trim();
    const brandCore = brand
      .replace(/\s+by\s+Choice\b/gi, "")
      .replace(/\s+Hotels?\s+International\b/gi, "")
      .trim();
    const queries = [
      `${name}, ${city}, ${country}`,
      `${nameNoByChoice}, ${city}, ${country}`,
      `${brand} ${city}, ${country}`,
      `${brandCore} ${city}, ${country}`,
      name.includes("Radisson")
        ? `Radisson Hotel ${city} San Jeronimo`
        : "",
      name.includes("Radisson") ? `Radisson Hotel ${city}` : "",
      name,
      nameNoByChoice,
    ].filter(Boolean);

    let chosen = null;
    let queryUsed = "";
    for (const q of queries) {
      const hits = await nominatimHotel(q);
      await sleep(1100);
      queryUsed = q;
      const ok = hits.find((h) => brandAligned(f, h) && cityAligned(city, h));
      if (ok) {
        chosen = ok;
        break;
      }
    }

    if (!chosen) {
      steward.push({
        id: rec.id,
        name,
        key: f["Property Identity Key"],
        city,
        reasons: ["osm_no_brand_city_hotel_match"],
      });
      continue;
    }

    const address = formatOsmAddress(chosen);
    if (!address || address.length < 12) {
      steward.push({
        id: rec.id,
        name,
        key: f["Property Identity Key"],
        city,
        reasons: ["osm_address_too_weak", chosen.display_name],
      });
      continue;
    }

    /** @type {Record<string, unknown>} */
    const patch = {
      Address: address,
      "Address Confidence": isStreetLevelAddress(address) ? "Medium" : "Medium",
      "Address Source URL": chosen.osm_type
        ? `https://www.openstreetmap.org/${chosen.osm_type}/${chosen.osm_id}`
        : "https://nominatim.openstreetmap.org/",
      "Last Reviewed Date": todayIsoDate(),
    };
    const reasons = [
      `address_from_osm_nominatim_hotel:${chosen.type || "hotel"}`,
      `query:${queryUsed}`,
    ];

    if (args.withMapbox && isStreetLevelAddress(address)) {
      const mb = await resolveMapboxCoordinates(
        {
          propertyName: name,
          brand,
          address,
          city,
          stateRegion: f["State / Region"],
          country,
        },
        { omitPropertyName: true }
      );
      mapboxLookups += 1;
      if (mb.status === MAPBOX_COORDINATE_STATUSES.RESOLVED_HIGH) {
        patch.Latitude = mb.latitude;
        patch.Longitude = mb.longitude;
        patch["Coordinate Source Type"] = "official_address_geocode";
        patch["Coordinate Confidence"] = "High";
        patch["Geocode Provider"] = "Mapbox";
        patch["Geocode Method"] = "permanent_geocoding_osm_address";
        patch["Geocode Reviewed Date"] = todayIsoDate();
        reasons.push(`coords_from_mapbox:${mb.reason}`);
      } else {
        reasons.push(`mapbox_${mb.status}:${mb.reason}`);
      }
    }

    proposals.push({
      id: rec.id,
      property_name: name,
      identity_key: f["Property Identity Key"],
      city,
      country,
      osm_display: chosen.display_name,
      patch,
      reasons,
    });
  }

  const envCheck = checkIntakeApplyEnv();
  const doWrite = Boolean(args.apply && args.allConfirmsOk && envCheck.allOk);
  let patched = [];
  if (doWrite && proposals.length) {
    patched = await patchRecords(
      baseId,
      token,
      proposals.map((p) => ({ id: p.id, fields: p.patch }))
    );
  }

  const report = {
    status: doWrite ? "applied" : "dry_run",
    hard_rule:
      "OSM hotel node Address text only when brand+city align; never Nominatim coords; Mapbox for lat/lng",
    generated_at: new Date().toISOString(),
    scanned: rows.length,
    proposal_count: proposals.length,
    patched_count: patched.length,
    steward_count: steward.length,
    mapbox_lookups: mapboxLookups,
    airtable_writes: doWrite,
    proposals,
    steward,
  };
  mkdirSync(join(root, "reports"), { recursive: true });
  const outRel = doWrite
    ? "reports/census-choice-osm-address-applied.json"
    : "reports/census-choice-osm-address-dry-run.json";
  writeFileSync(join(root, outRel), JSON.stringify(report, null, 2));
  console.log(
    JSON.stringify(
      {
        ok: true,
        status: report.status,
        output: outRel,
        scanned: report.scanned,
        proposal_count: report.proposal_count,
        steward_count: report.steward_count,
        mapbox_lookups: report.mapbox_lookups,
        sample: proposals.map((p) => ({
          n: p.property_name,
          city: p.city,
          addr: p.patch.Address,
          lat: p.patch.Latitude,
          reasons: p.reasons,
        })),
        steward: steward.map((s) => ({ n: s.name, city: s.city, reasons: s.reasons })),
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
