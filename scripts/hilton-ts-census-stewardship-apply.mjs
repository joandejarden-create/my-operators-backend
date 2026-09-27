/**
 * Hilton New York Times Square (NYCTSHH) census stewardship — evidence + gated HPC create.
 *
 * Usage:
 *   node scripts/hilton-ts-census-stewardship-apply.mjs
 *   node scripts/hilton-ts-census-stewardship-apply.mjs --apply \
 *     --confirm-hilton-ts-census-steward-insert \
 *     --confirm-hotel-property-census-only \
 *     --confirm-no-legacy-census-writes \
 *     --confirm-no-owner-operator \
 *     --enable-production-writes
 *
 * Target: AIRTABLE_BASE_ID_ALT / Hotel Property Census only.
 * Hotel facts are HOTEL_SPECIFIC_DATA — no Hilton/NYC production switches.
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import {
  createHotelPropertyCensusRecords,
  resolveLiveInsertContext,
} from "../lib/research-engine-v2/census-autopilot-discovery-insert-apply.js";
import { sanitizeInsertFields } from "../lib/research-engine-v2/census-autopilot-source-discovery.js";
import {
  assertProductionCensusWriteTarget,
  productionHotelPropertyCensus,
} from "../lib/research-engine-v2/production-census-source-of-truth.js";
import { resolvePat, resolveTargetBase } from "../lib/research-engine-v2/production-census-schema-create.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(
  ROOT,
  "reports/group-demand-intelligence/hotel4-hilton-times-square-v1"
);
const APPLY =
  process.argv.includes("--apply") ||
  process.argv.includes("--enable-production-writes");
const ENABLE = process.argv.includes("--enable-production-writes");

const CONFIRMS = {
  stewardInsert: process.argv.includes("--confirm-hilton-ts-census-steward-insert"),
  hpcOnly: process.argv.includes("--confirm-hotel-property-census-only"),
  noLegacy: process.argv.includes("--confirm-no-legacy-census-writes"),
  noOwner: process.argv.includes("--confirm-no-owner-operator"),
};

const IDENTITY_KEY = "ind_hilton_us_nyctshh";
const OFFICIAL_URL =
  "https://www.hilton.com/en/hotels/nyctshh-hilton-times-square/";
const EVENTS_URL =
  "https://www.hilton.com/en/hotels/nyctshh-hilton-times-square/events/";
const HOTEL_INFO_URL =
  "https://www.hilton.com/en/hotels/nyctshh-hilton-times-square/hotel-info/";

/**
 * Event-space interpretation (do not flatten lounge into formal meeting total):
 * - Hilton summary: 1 meeting room / 300 sq ft "total event space" = formal meeting inventory
 * - Private Dining Room: 300 sq ft, conference capacity 15
 * - 42nd And Sky Bar & Lounge: 2,000 sq ft, reception 100 = social / private-event venue
 * Canonical formalMeetingSpaceSqFt = 300; socialEventVenueSqFt = 2000 (separate).
 */
export const HILTON_TS_EVIDENCE_PACK = Object.freeze({
  version: "hilton_ts_census_stewardship_evidence_v1",
  hotel: "Hilton New York Times Square",
  propertyCode: "NYCTSHH",
  fields: [
    {
      field: "hotelName",
      value: "Hilton New York Times Square",
      sourceUrl: OFFICIAL_URL,
      sourceType: "official_property_page",
      authority: "FIRST_PARTY",
      status: "VERIFIED_FIRST_PARTY",
      confidence: "HIGH",
    },
    {
      field: "brand",
      value: "Hilton Hotels & Resorts",
      sourceUrl: OFFICIAL_URL,
      sourceType: "official_property_page",
      authority: "FIRST_PARTY",
      status: "VERIFIED_FIRST_PARTY",
      confidence: "HIGH",
    },
    {
      field: "brandFamily",
      value: "Hilton",
      sourceUrl: OFFICIAL_URL,
      sourceType: "official_property_page",
      authority: "FIRST_PARTY",
      status: "VERIFIED_FIRST_PARTY",
      confidence: "HIGH",
    },
    {
      field: "propertyCode",
      value: "NYCTSHH",
      sourceUrl: OFFICIAL_URL,
      sourceType: "official_property_page",
      authority: "FIRST_PARTY",
      status: "VERIFIED_FIRST_PARTY",
      confidence: "HIGH",
      note: "Hilton code in official URL path /nyctshh-hilton-times-square/",
    },
    {
      field: "address",
      value: "234 West 42nd Street",
      sourceUrl: OFFICIAL_URL,
      sourceType: "official_property_page",
      authority: "FIRST_PARTY",
      status: "VERIFIED_FIRST_PARTY",
      confidence: "HIGH",
    },
    {
      field: "city",
      value: "New York",
      sourceUrl: OFFICIAL_URL,
      sourceType: "official_property_page",
      authority: "FIRST_PARTY",
      status: "VERIFIED_FIRST_PARTY",
      confidence: "HIGH",
    },
    {
      field: "state",
      value: "New York",
      sourceUrl: OFFICIAL_URL,
      sourceType: "official_property_page",
      authority: "FIRST_PARTY",
      status: "VERIFIED_FIRST_PARTY",
      confidence: "HIGH",
    },
    {
      field: "postalCode",
      value: "10036",
      sourceUrl: OFFICIAL_URL,
      sourceType: "official_property_page",
      authority: "FIRST_PARTY",
      status: "VERIFIED_FIRST_PARTY",
      confidence: "HIGH",
    },
    {
      field: "country",
      value: "United States",
      sourceUrl: OFFICIAL_URL,
      sourceType: "official_property_page",
      authority: "FIRST_PARTY",
      status: "VERIFIED_FIRST_PARTY",
      confidence: "HIGH",
    },
    {
      field: "phone",
      value: "+1 212-913-9488",
      sourceUrl: OFFICIAL_URL,
      sourceType: "official_property_page",
      authority: "FIRST_PARTY",
      status: "VERIFIED_FIRST_PARTY",
      confidence: "HIGH",
    },
    {
      field: "email",
      value: "NYCTS_HOTEL@hilton.com",
      sourceUrl: OFFICIAL_URL,
      sourceType: "official_property_page",
      authority: "FIRST_PARTY",
      status: "VERIFIED_FIRST_PARTY",
      confidence: "HIGH",
    },
    {
      field: "rooms",
      value: 478,
      sourceUrl: EVENTS_URL,
      sourceType: "official_property_page",
      authority: "FIRST_PARTY",
      status: "VERIFIED_FIRST_PARTY",
      confidence: "HIGH",
      note: "Hilton events page JSON totalRooms/Guest rooms = 478",
    },
    {
      field: "formalMeetingSpaceSqFt",
      value: 300,
      sourceUrl: EVENTS_URL,
      sourceType: "official_property_page",
      authority: "FIRST_PARTY",
      status: "VERIFIED_FIRST_PARTY",
      confidence: "HIGH",
      note: "Hilton lists 1 meeting room / 300 sq ft total event space = Private Dining Room only",
    },
    {
      field: "socialEventVenueSqFt",
      value: 2000,
      sourceUrl: EVENTS_URL,
      sourceType: "official_property_page",
      authority: "FIRST_PARTY",
      status: "VERIFIED_FIRST_PARTY",
      confidence: "HIGH",
      note: "42nd And Sky Bar & Lounge — social/private-event venue; NOT folded into formal meeting total",
    },
    {
      field: "officialUrl",
      value: OFFICIAL_URL,
      sourceUrl: OFFICIAL_URL,
      sourceType: "official_property_page",
      authority: "FIRST_PARTY",
      status: "VERIFIED_FIRST_PARTY",
      confidence: "HIGH",
    },
    {
      field: "owner",
      value: null,
      status: "UNKNOWN",
      confidence: "UNKNOWN",
      note: "Not asserted from first-party Hilton pages; ownership research deferred",
    },
    {
      field: "operator",
      value: null,
      status: "UNKNOWN",
      confidence: "UNKNOWN",
      note: "Hilton brand management likely but not evidenced as legal operator on property page — left unknown",
    },
  ],
  eventSpaceInterpretation: {
    formalMeetingRooms: 1,
    formalMeetingSpaceSqFt: 300,
    formalRooms: [
      {
        name: "Private Dining Room",
        sqFt: 300,
        conferenceCapacity: 15,
      },
    ],
    socialPrivateEventVenues: [
      {
        name: "42nd And Sky Bar & Lounge",
        sqFt: 2000,
        receptionCapacity: 100,
      },
    ],
    hiltonSummaryTotalEventSpaceSqFt: 300,
    note: "Do not treat Hilton summary 300 sq ft as excluding the lounge existence — it counts formal meeting inventory only. Lounge is separate social-event product.",
  },
  identityConfidence: "HIGH",
  duplicateDecision: "NO_EXISTING_CANONICAL_MATCH",
  networkCostUsd: 0,
});

/** Rooftop-ish Times Square block — Mapbox preferred when permanent geocoding authorized. */
const FALLBACK_LAT = 40.75698;
const FALLBACK_LNG = -73.98901;

async function maybeGeocode() {
  if (String(process.env.MAPBOX_PERMANENT_GEOCODING || "") !== "1") {
    return {
      lat: FALLBACK_LAT,
      lng: FALLBACK_LNG,
      source: "manual_times_square_block_approx",
      confidence: "Medium",
      note: "MAPBOX_PERMANENT_GEOCODING not set; using verified-address approximate coordinates for Times Square block — refine when permanent geocoding authorized",
    };
  }
  const token = process.env.MAPBOX_ACCESS_TOKEN || process.env.MAPBOX_TOKEN;
  if (!token) {
    return {
      lat: FALLBACK_LAT,
      lng: FALLBACK_LNG,
      source: "manual_times_square_block_approx",
      confidence: "Medium",
    };
  }
  const q = encodeURIComponent(
    "234 West 42nd Street, New York, NY 10036, USA"
  );
  const url = `https://api.mapbox.com/geocoding/v5/mapbox.places/${q}.json?access_token=${token}&limit=1`;
  const r = await fetch(url);
  const j = await r.json();
  const c = j?.features?.[0]?.center;
  if (!c) {
    return {
      lat: FALLBACK_LAT,
      lng: FALLBACK_LNG,
      source: "manual_times_square_block_approx",
      confidence: "Medium",
    };
  }
  return {
    lat: c[1],
    lng: c[0],
    source: "official_address_geocode",
    confidence: "High",
  };
}

function buildInsertFields(geo) {
  const today = new Date().toISOString().slice(0, 10);
  const raw = {
    "Property Name": "Hilton New York Times Square",
    "Canonical Property Name": "Hilton New York Times Square",
    "Property Identity Key": IDENTITY_KEY,
    "Current Brand": "Hilton Hotels & Resorts",
    "Brand Family": "Hilton",
    "Affiliation Status": "Branded",
    City: "New York",
    "State / Region": "New York",
    Country: "United States",
    Continent: "North America",
    "Sub-Continent": "Northern America",
    Market: "New York",
    Submarket: "Times Square / Midtown West",
    Address: "234 West 42nd Street",
    "Postal Code": "10036",
    "Address Confidence": "High",
    "Address Source URL": OFFICIAL_URL,
    Latitude: geo.lat,
    Longitude: geo.lng,
    "Coordinate Source Type": geo.source,
    "Coordinate Confidence": geo.confidence,
    Phone: "+1 212-913-9488",
    "Rooms / Keys": 478,
    "Rooms Confidence": "High",
    "Rooms Source URL": EVENTS_URL,
    "Rooms Source Type": "official_property_page",
    "Rooms Reviewed Date": today,
    "Official Property URL": OFFICIAL_URL,
    "Source URL": OFFICIAL_URL,
    "Family / Source Family": "Hilton",
    "Source Type": "official_property_page",
    "Source Confidence": "High",
    "Identity Confidence": "High",
    "Data Eligible": true,
    "Production Use Status": "Census Only / Not Owner-Facing",
    "Public Display Review Status": "Hold",
    "Radar Display Status": "Hold",
    "Radar Geography Status": "Coordinates Available",
    "Enrichment Status": "Identity Seeded — pending enrichment",
    "Enrichment Priority": "High",
    "Discovery Date": today,
    "Last Reviewed Date": today,
  };
  const sanitized = sanitizeInsertFields(raw);
  return { raw, sanitized };
}

async function reDedup(baseId, token) {
  const base = new Airtable({ apiKey: token }).base(baseId);
  const hits = [];
  let scanned = 0;
  await base("Hotel Property Census")
    .select({ pageSize: 100 })
    .eachPage((recs, next) => {
      for (const r of recs) {
        scanned += 1;
        const f = r.fields || {};
        const blob = JSON.stringify(f).toLowerCase();
        const key = String(f["Property Identity Key"] || "");
        if (
          key === IDENTITY_KEY ||
          /nyctshh|hilton new york times square|hilton times square|234 west 42/.test(
            blob
          ) ||
          String(f["Official Property URL"] || "")
            .toLowerCase()
            .includes("nyctshh")
        ) {
          hits.push({
            id: r.id,
            name: f["Canonical Property Name"] || f["Property Name"],
            key,
            city: f.City,
            country: f.Country,
            rooms: f["Rooms / Keys"],
          });
        }
      }
      next();
    });
  return { scanned, hits };
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  fs.writeFileSync(
    path.join(OUT, "EVIDENCE_PACK.json"),
    JSON.stringify(HILTON_TS_EVIDENCE_PACK, null, 2)
  );

  const geo = await maybeGeocode();
  const { raw, sanitized } = buildInsertFields(geo);
  const dropped = (sanitized.dropped || []).map((d) => d.field);
  const preview = {
    generatedAt: new Date().toISOString(),
    dryRun: !APPLY,
    identityConfidence: "HIGH",
    duplicateDecision: "NO_EXISTING_CANONICAL_MATCH",
    target: productionHotelPropertyCensus,
    confirms: CONFIRMS,
    geo,
    eventSpaceInterpretation: HILTON_TS_EVIDENCE_PACK.eventSpaceInterpretation,
    fieldCountRaw: Object.keys(raw).length,
    fieldCountSanitized: Object.keys(sanitized.fields).length,
    droppedFromAllowlist: dropped,
    payloadPreview: sanitized.fields,
  };
  fs.writeFileSync(path.join(OUT, "WRITE_PREVIEW.json"), JSON.stringify(preview, null, 2));

  if (!APPLY) {
    console.log(
      JSON.stringify(
        {
          ok: true,
          dryRun: true,
          outDir: OUT,
          identityKey: IDENTITY_KEY,
          fields: Object.keys(sanitized.fields).length,
          dropped,
          geo,
        },
        null,
        2
      )
    );
    return;
  }

  const allConfirms = Object.values(CONFIRMS).every(Boolean);
  if (!allConfirms || !ENABLE) {
    console.error(
      JSON.stringify({
        ok: false,
        blocked: true,
        reason: "missing_confirms_or_enable_flag",
        confirms: CONFIRMS,
        enableProductionWrites: ENABLE,
      })
    );
    process.exit(2);
  }

  const ctx = resolveLiveInsertContext();
  const bases = ctx.bases || resolveTargetBase();
  const resolvedBaseId =
    (typeof bases === "string" ? bases : null) ||
    bases?.target_base_id ||
    bases?.baseId ||
    process.env.AIRTABLE_BASE_ID_ALT;
  const token = ctx.token || resolvePat();
  if (!resolvedBaseId || !token) {
    console.error(JSON.stringify({ ok: false, reason: "missing_base_or_token" }));
    process.exit(2);
  }
  assertProductionCensusWriteTarget({
    baseId: resolvedBaseId,
    tableName: "Hotel Property Census",
  });

  const before = await reDedup(resolvedBaseId, token);
  if (before.hits.length) {
    console.error(
      JSON.stringify({
        ok: false,
        blocked: true,
        reason: "duplicate_exists",
        hits: before.hits,
      })
    );
    process.exit(3);
  }

  const created = await createHotelPropertyCensusRecords(
    resolvedBaseId,
    token,
    [{ fields: sanitized.fields }]
  );

  const after = await reDedup(resolvedBaseId, token);
  const recordId = created?.created?.[0]?.id || after.hits[0]?.id || null;
  const result = {
    ok: Boolean(recordId),
    applied: true,
    baseId: resolvedBaseId,
    identityKey: IDENTITY_KEY,
    created,
    afterHits: after.hits,
    hpcId: recordId,
    eventSpaceInterpretation: HILTON_TS_EVIDENCE_PACK.eventSpaceInterpretation,
  };
  fs.writeFileSync(path.join(OUT, "APPLY_RESULT.json"), JSON.stringify(result, null, 2));
  console.log(JSON.stringify(result, null, 2));
  if (!recordId) process.exit(4);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
