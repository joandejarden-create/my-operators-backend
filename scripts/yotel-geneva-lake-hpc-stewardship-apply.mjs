/**
 * YOTEL Geneva Lake census stewardship — evidence pack + dry-run / gated HPC create.
 *
 * Usage:
 *   node scripts/yotel-geneva-lake-hpc-stewardship-apply.mjs
 *   node scripts/yotel-geneva-lake-hpc-stewardship-apply.mjs --apply \
 *     --confirm-yotel-geneva-census-steward-insert \
 *     --confirm-hotel-property-census-only \
 *     --confirm-no-legacy-census-writes \
 *     --confirm-no-owner-operator \
 *     --enable-production-writes
 *
 * Target: AIRTABLE_BASE_ID_ALT / Hotel Property Census only.
 * Never writes to forbidden legacy base appvtnDurnMSjINP6.
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
import {
  sanitizeInsertFields,
} from "../lib/research-engine-v2/census-autopilot-source-discovery.js";
import {
  assertProductionCensusWriteTarget,
  productionHotelPropertyCensus,
} from "../lib/research-engine-v2/production-census-source-of-truth.js";
import { resolvePat, resolveTargetBase } from "../lib/research-engine-v2/production-census-schema-create.js";
import { TABLE_IDS } from "../lib/research-engine-v2/production-census-write.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(ROOT, "reports/hotel-census/yotel-geneva-lake-onboarding-v1");
const APPLY =
  process.argv.includes("--apply") || process.argv.includes("--enable-production-writes");
const ENABLE = process.argv.includes("--enable-production-writes");

const CONFIRMS = {
  stewardInsert: process.argv.includes("--confirm-yotel-geneva-census-steward-insert"),
  hpcOnly: process.argv.includes("--confirm-hotel-property-census-only"),
  noLegacy: process.argv.includes("--confirm-no-legacy-census-writes"),
  noOwner: process.argv.includes("--confirm-no-owner-operator"),
};

const IDENTITY_KEY = "yotel_ch_geneva_lake_founex";
const OFFICIAL_URL = "https://www.yotel.com/en/hotels/yotel-geneva-lake";
const MEETINGS_URL =
  "https://www.yotel.com/en/hotels/yotel-geneva-lake/meetings-conferences-events";
const PRESS_URL =
  "https://www.yotel.com/en/press/yotel-geneva-lake-officially-opens-its-doors";

export const YOTEL_GENEVA_EVIDENCE_PACK = Object.freeze({
  version: "yotel_geneva_lake_census_stewardship_evidence_v1",
  hotel: "YOTEL Geneva Lake",
  duplicateDecision: "NO_EXISTING_CANONICAL_MATCH",
  identityConfidence: "HIGH",
  geographyNote:
    "Founex / Canton of Vaud / La Côte — not downtown Geneva city proper",
  fields: [
    {
      field: "hotelName",
      value: "YOTEL Geneva Lake",
      sourceUrl: OFFICIAL_URL,
      authority: "FIRST_PARTY",
      confidence: "HIGH",
    },
    {
      field: "brand",
      value: "YOTEL",
      sourceUrl: OFFICIAL_URL,
      authority: "FIRST_PARTY",
      confidence: "HIGH",
    },
    {
      field: "address",
      value: "Chemin Ballessert 1",
      sourceUrl: OFFICIAL_URL,
      authority: "FIRST_PARTY",
      confidence: "HIGH",
    },
    {
      field: "city",
      value: "Founex",
      sourceUrl: OFFICIAL_URL,
      authority: "FIRST_PARTY",
      confidence: "HIGH",
    },
    {
      field: "country",
      value: "Switzerland",
      sourceUrl: OFFICIAL_URL,
      authority: "FIRST_PARTY",
      confidence: "HIGH",
    },
    {
      field: "rooms",
      value: 237,
      sourceUrl: PRESS_URL,
      authority: "FIRST_PARTY",
      confidence: "HIGH",
    },
    {
      field: "meetingRooms",
      value: 6,
      sourceUrl: MEETINGS_URL,
      authority: "FIRST_PARTY",
      confidence: "HIGH",
    },
  ],
});

function buildInsertFields() {
  const today = new Date().toISOString().slice(0, 10);
  const raw = {
    "Property Name": "YOTEL Geneva Lake",
    "Canonical Property Name": "YOTEL Geneva Lake",
    "Property Identity Key": IDENTITY_KEY,
    "Current Brand": "YOTEL",
    "Brand Family": "YOTEL",
    "Affiliation Status": "Branded",
    City: "Founex",
    "State / Region": "Vaud",
    Country: "Switzerland",
    Continent: "Europe",
    Market: "Lake Geneva / La Côte",
    Submarket: "Founex / Nyon corridor",
    Address: "Chemin Ballessert 1",
    "Postal Code": "1297",
    "Address Confidence": "High",
    "Address Source URL": OFFICIAL_URL,
    Phone: "+41 22 960 78 00",
    "Rooms / Keys": 237,
    "Rooms Confidence": "High",
    "Rooms Source URL": PRESS_URL,
    "Rooms Source Type": "official_press",
    "Rooms Reviewed Date": today,
    "Official Property URL": OFFICIAL_URL,
    "Source URL": OFFICIAL_URL,
    "Family / Source Family": "YOTEL",
    "Source Type": "official_property_page",
    "Source Confidence": "High",
    "Identity Confidence": "High",
    "Data Eligible": true,
    "Production Use Status": "Census Only / Not Owner-Facing",
    "Public Display Review Status": "Hold",
    "Radar Display Status": "Hold",
    "Radar Geography Status": "Address Only",
    "Enrichment Status": "Identity Seeded — pending enrichment",
    "Enrichment Priority": "High",
    "Discovery Date": today,
    "Last Reviewed Date": today,
    "Meeting Space Flag": true,
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
          (/yotel/.test(blob) && /founex|geneva lake/.test(blob)) ||
          String(f["Official Property URL"] || "")
            .toLowerCase()
            .includes("yotel-geneva-lake")
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
  const { raw, sanitized } = buildInsertFields();
  fs.writeFileSync(
    path.join(OUT, "EVIDENCE_PACK.json"),
    JSON.stringify(YOTEL_GENEVA_EVIDENCE_PACK, null, 2)
  );
  const preview = {
    generatedAt: new Date().toISOString(),
    dryRun: !APPLY,
    identityConfidence: "HIGH",
    duplicateDecision: "NO_EXISTING_CANONICAL_MATCH",
    identityKey: IDENTITY_KEY,
    target: productionHotelPropertyCensus,
    confirms: CONFIRMS,
    fieldCountRaw: Object.keys(raw).length,
    fieldCountSanitized: Object.keys(sanitized.fields).length,
    droppedFromAllowlist: (sanitized.dropped || []).map((d) => d.field),
    payloadPreview: sanitized.fields,
    note: "Coordinates deferred — address-only until permanent geocode terms reviewed",
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
          dropped: preview.droppedFromAllowlist,
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
  if (resolvedBaseId === "appvtnDurnMSjINP6") {
    console.error(JSON.stringify({ ok: false, reason: "forbidden_legacy_base" }));
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
          name: readback.fields?.["Canonical Property Name"],
          city: readback.fields?.City,
          rooms: readback.fields?.["Rooms / Keys"],
          key: readback.fields?.["Property Identity Key"],
        }
      : null,
  };
  fs.writeFileSync(path.join(OUT, "WRITE_RESULT.json"), JSON.stringify(result, null, 2));
  console.log(JSON.stringify({ ok: result.created, recordId, outDir: OUT }, null, 2));
  if (!result.created) process.exit(4);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
