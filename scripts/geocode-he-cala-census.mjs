/**
 * Set Latitude / Longitude (and optional Address 1) for Hotel Equities (CALA) census rows.
 *
 * Coordinates from OSM, property websites, and Nominatim (verified queries).
 *
 * Usage:
 *   node scripts/geocode-he-cala-census.mjs --dry-run
 *   node scripts/geocode-he-cala-census.mjs
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import {
  HE_MGMT,
  HOTEL_CENSUS_TABLE,
  MAP_HE_CALA_CENSUS,
} from "../lib/hotel-census/he-cala-census-apply.js";
import { CENSUS_FIELDS } from "../lib/hotel-census/fields.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const REPORT = join(__dirname, "..", "reports", "he-cala-census-geocode-log.csv");

const F_LAT = "Latitude";
const F_LNG = "Longitude";
const F_ADDR = "Address 1";

/**
 * Curated WGS84 coordinates keyed by Airtable record id (stable after create).
 * source: osm | website | nominatim | existing_census
 */
export const HE_CALA_GEO_BY_RECORD_ID = {
  recUF12aRBJxaDdIU: {
    lat: 18.019279,
    lng: -76.792706,
    address1: "17 Waterloo Road",
    source: "existing_census",
  },
  recq7PdwcUkBDDRbU: {
    lat: 9.6210309,
    lng: -84.6380492,
    address1: "Calle Jardín, Jacó",
    source: "osm",
  },
  recETlBd8ctQnHx4L: {
    lat: 18.9189,
    lng: -99.2394,
    address1: "Km 87.5 Carretera México-Acapulco, Col. Flores Magón",
    source: "website",
  },
  recnV6adKBfss0VfR: {
    lat: 11.2480089,
    lng: -60.5854305,
    address1: "Windward Road, Roxborough",
    source: "nominatim",
  },
  recgeikvPO8zuKTyJ: {
    lat: 11.9972063,
    lng: -61.7698353,
    address1: "1 True Blue Beach",
    source: "nominatim",
  },
  rec8shi9qzHM6fjsf: {
    lat: 12.1066695,
    lng: -68.9351307,
    address1: "Willemstad (pipeline — site TBD)",
    source: "nominatim_city",
  },
  recT41S0j01asWiK8: {
    lat: 15.335229,
    lng: -61.3330422,
    address1: "Providence Estate, Laudat",
    source: "nominatim",
  },
  recDtetmqOGsJGcxK: {
    lat: 19.3249067,
    lng: -69.5219281,
    address1: "Playa Portillo, El Portillo",
    source: "nominatim",
  },
  recUfg2NwXX3DvDWF: {
    lat: 10.425,
    lng: -61.38,
    address1: "South Park, Tarouba Link Road",
    source: "approximate_tarouba",
  },
  recdeNV2tBUjf4zr2: {
    lat: 12.2261784,
    lng: -61.6087201,
    address1: "Levera, St Patrick",
    source: "nominatim",
  },
  recFsXj5d1l3VwxTz: {
    lat: 19.3227153,
    lng: -69.5530726,
    source: "existing_census",
  },
  recGKuT86TJI05QAA: {
    lat: 21.7970415,
    lng: -72.1847277,
    source: "existing_census",
  },
  recHBTqnDeXJOc7FZ: {
    lat: 18.470829,
    lng: -69.886546,
    address1: "252 Padre Billini",
    source: "existing_census",
  },
  recr3DXLHdh09J8mi: {
    lat: 18.33303,
    lng: -64.92134,
    source: "existing_census",
  },
  recscC7og2NEHYfbr: {
    lat: 20.7056748,
    lng: -87.0103064,
    address1: "Km 54 Carretera Cancun Tulum",
    source: "existing_census",
  },
};

function parseArgs() {
  return { dryRun: process.argv.includes("--dry-run"), force: process.argv.includes("--force") };
}

function hasCoords(f) {
  const lat = f[F_LAT];
  const lng = f[F_LNG];
  return Number.isFinite(lat) && Number.isFinite(lng);
}

function csvEscape(v) {
  const s = String(v ?? "");
  return /[",\n]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

async function main() {
  const { dryRun, force } = parseArgs();
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID_ALT;
  if (!apiKey || !baseId) throw new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID_ALT");

  console.log(`=== HE CALA geocode (${dryRun ? "DRY RUN" : "LIVE"}) ===\n`);

  const base = new Airtable({ apiKey }).base(baseId);
  const selectFields = [
    CENSUS_FIELDS.name,
    CENSUS_FIELDS.managementCompany,
    F_LAT,
    F_LNG,
    F_ADDR,
  ];

  const recs = await base(HOTEL_CENSUS_TABLE)
    .select({
      filterByFormula: `FIND('${HE_MGMT.replace(/'/g, "\\'")}', {${MAP_HE_CALA_CENSUS.mgmt}})>0`,
      fields: selectFields,
      pageSize: 100,
    })
    .all();

  const logRows = [];
  const updates = [];

  for (const rec of recs) {
    const geo = HE_CALA_GEO_BY_RECORD_ID[rec.id];
    if (!geo) {
      logRows.push({
        recordId: rec.id,
        name: rec.fields.name,
        action: "skip",
        status: "no_geo_map",
        lat: "",
        lng: "",
        source: "",
      });
      continue;
    }

    const f = rec.fields;
    if (hasCoords(f) && !force) {
      logRows.push({
        recordId: rec.id,
        name: f.name,
        action: "skip",
        status: "already_has_coords",
        lat: f[F_LAT],
        lng: f[F_LNG],
        source: geo.source,
      });
      continue;
    }

    const fields = {
      [F_LAT]: geo.lat,
      [F_LNG]: geo.lng,
    };
    if (geo.address1 && !f[F_ADDR]) fields[F_ADDR] = geo.address1;

    updates.push({ id: rec.id, fields, name: f.name, geo });
    logRows.push({
      recordId: rec.id,
      name: f.name,
      action: "update",
      status: "queued",
      lat: geo.lat,
      lng: geo.lng,
      source: geo.source,
    });
  }

  console.log(`HE CALA rows: ${recs.length}; updates queued: ${updates.length}\n`);

  let updated = 0;
  let errors = 0;

  if (!dryRun) {
    for (const u of updates) {
      try {
        await base(HOTEL_CENSUS_TABLE).update([{ id: u.id, fields: u.fields }], {
          typecast: true,
        });
        updated++;
        const lr = logRows.find((r) => r.recordId === u.id);
        if (lr) lr.status = "ok";
        console.log(`  ${u.name} → ${u.geo.lat}, ${u.geo.lng} (${u.geo.source})`);
      } catch (err) {
        errors++;
        console.error(`  FAILED ${u.name}:`, err.message);
        const lr = logRows.find((r) => r.recordId === u.id);
        if (lr) lr.status = `error: ${err.message}`;
      }
    }
  } else {
    for (const u of updates) {
      console.log(`  [dry-run] ${u.name} → ${u.geo.lat}, ${u.geo.lng}`, u.fields);
    }
    updated = updates.length;
  }

  mkdirSync(dirname(REPORT), { recursive: true });
  const headers = ["recordId", "name", "action", "status", "lat", "lng", "source"];
  writeFileSync(
    REPORT,
    `${headers.join(",")}\n${logRows.map((r) => headers.map((h) => csvEscape(r[h])).join(",")).join("\n")}\n`,
    "utf8"
  );

  console.log(`\nDone. Updated: ${updated}; Errors: ${errors}`);
  console.log(`Log: ${REPORT}`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
