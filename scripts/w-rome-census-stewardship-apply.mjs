/**
 * W Rome census stewardship — evidence pack + dry-run / gated HPC create.
 *
 * Usage:
 *   node scripts/w-rome-census-stewardship-apply.mjs
 *   node scripts/w-rome-census-stewardship-apply.mjs --apply \
 *     --confirm-w-rome-census-steward-insert \
 *     --confirm-hotel-property-census-only \
 *     --confirm-no-legacy-census-writes \
 *     --confirm-no-owner-operator \
 *     --enable-production-writes
 *
 * Target: AIRTABLE_BASE_ID_ALT / tbl9aY5ijiuIzzWam only.
 * No W/Italy/Rome production switches — hotel facts are HOTEL_SPECIFIC_DATA.
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import crypto from "crypto";
import { fileURLToPath } from "url";
import Airtable from "airtable";
import {
  createHotelPropertyCensusRecords,
  resolveLiveInsertContext,
} from "../lib/research-engine-v2/census-autopilot-discovery-insert-apply.js";
import {
  sanitizeInsertFields,
  INSERT_ALLOWED_FIELDS,
} from "../lib/research-engine-v2/census-autopilot-source-discovery.js";
import {
  assertProductionCensusWriteTarget,
  productionHotelPropertyCensus,
} from "../lib/research-engine-v2/production-census-source-of-truth.js";
import { resolvePat, resolveTargetBase } from "../lib/research-engine-v2/production-census-schema-create.js";
import { TABLE_IDS } from "../lib/research-engine-v2/production-census-write.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(
  ROOT,
  "reports/group-demand-intelligence/w-rome-census-stewardship-v1"
);
const APPLY = process.argv.includes("--apply") || process.argv.includes("--enable-production-writes");
const ENABLE = process.argv.includes("--enable-production-writes");

const CONFIRMS = {
  stewardInsert: process.argv.includes("--confirm-w-rome-census-steward-insert"),
  hpcOnly: process.argv.includes("--confirm-hotel-property-census-only"),
  noLegacy: process.argv.includes("--confirm-no-legacy-census-writes"),
  noOwner: process.argv.includes("--confirm-no-owner-operator"),
};

const IDENTITY_KEY = "ind_marriott_it_romwv";
const OFFICIAL_URL = "https://www.marriott.com/en-us/hotels/romwv-w-rome/overview/";
const ROOMS_URL = "https://www.marriott.com/en-us/hotels/romwv-w-rome/rooms/";
const MARRIOTT_CN_URL = "https://www.marriott.com.cn/hotels/romwv-w-rome/overview/";
const W_BRAND_URL = "https://w-hotels.marriott.com/hotel/w-rome/";
const CVENT_URL =
  "https://www.cvent.com/venues/rome/hotel/w-rome/venue-e167d237-46bb-406e-ba16-b5dc215bf6f2";

/** Field-level evidence — first-party preferred. */
export const W_ROME_EVIDENCE_PACK = Object.freeze({
  version: "w_rome_census_stewardship_evidence_v1",
  hotel: "W Rome",
  propertyCode: "ROMWV",
  fields: [
    {
      field: "hotelName",
      value: "W Rome",
      sourceUrl: OFFICIAL_URL,
      sourceType: "official_property_page",
      authority: "FIRST_PARTY",
      status: "VERIFIED_FIRST_PARTY",
      direct: true,
      confidence: "HIGH",
    },
    {
      field: "brand",
      value: "W Hotels",
      sourceUrl: W_BRAND_URL,
      sourceType: "official_property_page",
      authority: "FIRST_PARTY",
      status: "VERIFIED_FIRST_PARTY",
      direct: true,
      confidence: "HIGH",
    },
    {
      field: "brandFamily",
      value: "Marriott International",
      sourceUrl: OFFICIAL_URL,
      sourceType: "official_property_page",
      authority: "FIRST_PARTY",
      status: "VERIFIED_FIRST_PARTY",
      direct: true,
      confidence: "HIGH",
    },
    {
      field: "propertyCode",
      value: "ROMWV",
      sourceUrl: OFFICIAL_URL,
      sourceType: "official_property_page",
      authority: "FIRST_PARTY",
      status: "VERIFIED_FIRST_PARTY",
      direct: true,
      confidence: "HIGH",
      note: "MARSHA in Marriott URL path /romwv-w-rome/",
    },
    {
      field: "address",
      value: "26/36 Via Liguria",
      sourceUrl: ROOMS_URL,
      sourceType: "official_property_page",
      authority: "FIRST_PARTY",
      status: "VERIFIED_FIRST_PARTY",
      direct: true,
      confidence: "HIGH",
    },
    {
      field: "city",
      value: "Rome",
      sourceUrl: ROOMS_URL,
      sourceType: "official_property_page",
      authority: "FIRST_PARTY",
      status: "VERIFIED_FIRST_PARTY",
      direct: true,
      confidence: "HIGH",
    },
    {
      field: "postalCode",
      value: "00187",
      sourceUrl: ROOMS_URL,
      sourceType: "official_property_page",
      authority: "FIRST_PARTY",
      status: "VERIFIED_FIRST_PARTY",
      direct: true,
      confidence: "HIGH",
    },
    {
      field: "country",
      value: "Italy",
      sourceUrl: ROOMS_URL,
      sourceType: "official_property_page",
      authority: "FIRST_PARTY",
      status: "VERIFIED_FIRST_PARTY",
      direct: true,
      confidence: "HIGH",
    },
    {
      field: "phone",
      value: "+39 06-894121",
      sourceUrl: ROOMS_URL,
      sourceType: "official_property_page",
      authority: "FIRST_PARTY",
      status: "VERIFIED_FIRST_PARTY",
      direct: true,
      confidence: "HIGH",
    },
    {
      field: "rooms",
      value: 148,
      sourceUrl: MARRIOTT_CN_URL,
      sourceType: "official_property_page",
      authority: "FIRST_PARTY",
      status: "VERIFIED_FIRST_PARTY",
      direct: true,
      confidence: "HIGH",
      note: "Marriott first-party CN overview states 148 rooms and suites; Cvent corroborates 148",
      corroborationUrl: CVENT_URL,
    },
    {
      field: "officialUrl",
      value: OFFICIAL_URL,
      sourceUrl: OFFICIAL_URL,
      sourceType: "official_property_page",
      authority: "FIRST_PARTY",
      status: "VERIFIED_FIRST_PARTY",
      direct: true,
      confidence: "HIGH",
    },
    {
      field: "latitude",
      value: 41.906211,
      sourceUrl: "mapbox_geocode",
      sourceType: "official_address_geocode",
      authority: "DERIVED",
      status: "VERIFIED_CORROBORATED",
      direct: false,
      confidence: "HIGH",
      note: "Mapbox rooftop geocode of validated first-party address; permanent storage authorized via MAPBOX_PERMANENT_GEOCODING=1",
    },
    {
      field: "longitude",
      value: 12.488489,
      sourceUrl: "mapbox_geocode",
      sourceType: "official_address_geocode",
      authority: "DERIVED",
      status: "VERIFIED_CORROBORATED",
      direct: false,
      confidence: "HIGH",
    },
    {
      field: "region",
      value: null,
      status: "UNKNOWN",
      confidence: "UNKNOWN",
      note: "Lazio not stated on first-party address line; left blank",
    },
    {
      field: "operatingStatus",
      value: "Open",
      sourceUrl: OFFICIAL_URL,
      sourceType: "official_property_page",
      authority: "FIRST_PARTY",
      status: "VERIFIED_FIRST_PARTY",
      direct: true,
      confidence: "HIGH",
      note: "Live bookable Marriott property page",
    },
  ],
  conflicts: [
    {
      field: "rooms",
      sourceA: MARRIOTT_CN_URL,
      valueA: 148,
      sourceB: "hotelspedia.org (weak)",
      valueB: "148–162",
      resolution: 148,
      reason: "Prefer Marriott first-party + Cvent corroboration over tertiary range",
    },
  ],
  identityConfidence: "HIGH",
  duplicateDecision: "NO_EXISTING_CANONICAL_MATCH",
  networkCostUsd: 0,
});

function buildInsertFields() {
  const today = new Date().toISOString().slice(0, 10);
  const raw = {
    "Property Name": "W Rome",
    "Canonical Property Name": "W Rome",
    "Property Identity Key": IDENTITY_KEY,
    "Current Brand": "W Hotels",
    "Brand Family": "Marriott International",
    "Affiliation Status": "Branded",
    City: "Rome",
    Country: "Italy",
    Continent: "Europe",
    // Sub-Continent: no Europe option in live schema — leave blank
    Market: "Rome",
    Address: "26/36 Via Liguria",
    "Postal Code": "00187",
    "Address Confidence": "High",
    "Address Source URL": ROOMS_URL,
    Latitude: 41.906211,
    Longitude: 12.488489,
    "Coordinate Source Type": "official_address_geocode",
    "Coordinate Confidence": "High",
    Phone: "+39 06-894121",
    "Rooms / Keys": 148,
    "Rooms Confidence": "High",
    "Rooms Source URL": MARRIOTT_CN_URL,
    "Rooms Source Type": "official_property_page",
    "Rooms Reviewed Date": today,
    "Official Property URL": OFFICIAL_URL,
    "Source URL": OFFICIAL_URL,
    "Family / Source Family": "Marriott",
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
          /romwv|w rome|w roma|via liguria/.test(blob) ||
          String(f["Official Property URL"] || "")
            .toLowerCase()
            .includes("romwv")
        ) {
          hits.push({
            id: r.id,
            name: f["Canonical Property Name"] || f["Property Name"],
            key,
            city: f.City,
            country: f.Country,
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
    JSON.stringify(W_ROME_EVIDENCE_PACK, null, 2)
  );

  const { raw, sanitized } = buildInsertFields();
  const dropped = (sanitized.dropped || []).map((d) => d.field);
  const preview = {
    generatedAt: new Date().toISOString(),
    dryRun: !APPLY,
    identityConfidence: "HIGH",
    duplicateDecision: "NO_EXISTING_CANONICAL_MATCH",
    target: productionHotelPropertyCensus,
    confirms: CONFIRMS,
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
          identityConfidence: "HIGH",
          fields: Object.keys(sanitized.fields).length,
          dropped,
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
    tableId: TABLE_IDS["Hotel Property Census"],
  });

  const dedup = await reDedup(resolvedBaseId, token);
  fs.writeFileSync(path.join(OUT, "PREWRITE_DEDUP.json"), JSON.stringify(dedup, null, 2));
  if (dedup.hits.length > 0) {
    console.error(
      JSON.stringify({
        ok: false,
        blocked: true,
        reason: "duplicate_found_prewrite",
        hits: dedup.hits,
      })
    );
    process.exit(3);
  }

  const created = await createHotelPropertyCensusRecords(resolvedBaseId, token, [
    { fields: sanitized.fields },
  ]);
  const recordId = created?.created?.[0]?.id || null;

  const base = new Airtable({ apiKey: token }).base(resolvedBaseId);
  const readback = recordId
    ? await base("Hotel Property Census").find(recordId)
    : null;

  const result = {
    created: Boolean(recordId),
    recordId,
    createResponse: created,
    readback: readback
      ? {
          id: readback.id,
          fields: Object.fromEntries(
            Object.entries(readback.fields || {}).filter(
              ([k]) =>
                INSERT_ALLOWED_FIELDS.includes(k) ||
                ["Canonical Property Name", "Property Identity Key"].includes(k)
            )
          ),
        }
      : null,
    mismatchCount: 0,
  };

  if (readback) {
    const checks = [
      ["Canonical Property Name", "W Rome"],
      ["Property Identity Key", IDENTITY_KEY],
      ["Country", "Italy"],
      ["City", "Rome"],
      ["Current Brand", "W Hotels"],
      ["Rooms / Keys", 148],
    ];
    let mismatches = 0;
    for (const [k, expected] of checks) {
      const got = readback.fields?.[k];
      if (got !== expected && Number(got) !== Number(expected)) {
        mismatches += 1;
        result.mismatches = result.mismatches || [];
        result.mismatches.push({ field: k, expected, got });
      }
    }
    result.mismatchCount = mismatches;
  }

  fs.writeFileSync(path.join(OUT, "WRITE_RESULT.json"), JSON.stringify(result, null, 2));
  console.log(
    JSON.stringify(
      {
        ok: result.created && result.mismatchCount === 0,
        recordId,
        mismatchCount: result.mismatchCount,
        outDir: OUT,
      },
      null,
      2
    )
  );
  if (!result.created || result.mismatchCount > 0) process.exit(4);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
