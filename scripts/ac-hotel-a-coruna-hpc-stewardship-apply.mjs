/**
 * AC Hotel A Coruña census stewardship — evidence pack + dry-run / gated HPC create.
 *
 * Usage:
 *   node scripts/ac-hotel-a-coruna-hpc-stewardship-apply.mjs
 *   node scripts/ac-hotel-a-coruna-hpc-stewardship-apply.mjs --apply \
 *     --confirm-ac-coruna-census-steward-insert \
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
const OUT = path.join(ROOT, "reports/hotel-census/ac-hotel-a-coruna-onboarding-v1");
const APPLY =
  process.argv.includes("--apply") || process.argv.includes("--enable-production-writes");
const ENABLE = process.argv.includes("--enable-production-writes");

const CONFIRMS = {
  stewardInsert: process.argv.includes("--confirm-ac-coruna-census-steward-insert"),
  hpcOnly: process.argv.includes("--confirm-hotel-property-census-only"),
  noLegacy: process.argv.includes("--confirm-no-legacy-census-writes"),
  noOwner: process.argv.includes("--confirm-no-owner-operator"),
};

const IDENTITY_KEY = "ind_marriott_es_lcgco";
const OFFICIAL_URL_ES =
  "https://www.marriott.com/es/hotels/lcgco-ac-hotel-a-coruna/overview/";
const OFFICIAL_URL_EN =
  "https://www.marriott.com/en-us/hotels/lcgco-ac-hotel-a-coruna/overview/";
const EVENTS_URL =
  "https://www.marriott.com/en-us/hotels/lcgco-ac-hotel-a-coruna/events/";
const ROOMS_URL =
  "https://www.marriott.com/es/hotels/lcgco-ac-hotel-a-coruna/rooms/";
const ACHM_REFORM_URL = "https://achmhotels.com/reforma-ac-hotel-a-coruna/";

function loadGeocode() {
  const p = path.join(OUT, "PHASE2_GEOCODE.json");
  if (!fs.existsSync(p)) return null;
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return null;
  }
}

export const AC_CORUNA_EVIDENCE_PACK = Object.freeze({
  version: "ac_hotel_a_coruna_census_stewardship_evidence_v1",
  hotel: "AC Hotel A Coruña",
  propertyCode: "LCGCO",
  duplicateDecision: "NO_EXISTING_CANONICAL_MATCH",
  fields: [
    {
      field: "hotelName",
      value: "AC Hotel A Coruña",
      sourceUrl: EVENTS_URL,
      authority: "FIRST_PARTY",
      confidence: "HIGH",
    },
    {
      field: "brand",
      value: "AC Hotels by Marriott",
      sourceUrl: EVENTS_URL,
      authority: "FIRST_PARTY",
      confidence: "HIGH",
    },
    {
      field: "brandFamily",
      value: "Marriott International",
      sourceUrl: EVENTS_URL,
      authority: "FIRST_PARTY",
      confidence: "HIGH",
    },
    {
      field: "propertyCode",
      value: "LCGCO",
      sourceUrl: EVENTS_URL,
      authority: "FIRST_PARTY",
      confidence: "HIGH",
      note: "MARSHA in Marriott URL path /lcgco-ac-hotel-a-coruna/",
    },
    {
      field: "address",
      value: "Enrique Mariñas 36",
      sourceUrl: EVENTS_URL,
      authority: "FIRST_PARTY",
      confidence: "HIGH",
    },
    {
      field: "city",
      value: "A Coruña",
      sourceUrl: EVENTS_URL,
      authority: "FIRST_PARTY",
      confidence: "HIGH",
    },
    {
      field: "stateRegion",
      value: "Galicia",
      sourceUrl: EVENTS_URL,
      authority: "FIRST_PARTY",
      confidence: "MEDIUM",
      note: "Autonomous community — Galicia; corroborated by market geography",
    },
    {
      field: "postalCode",
      value: "15009",
      sourceUrl: EVENTS_URL,
      authority: "FIRST_PARTY",
      confidence: "HIGH",
    },
    {
      field: "country",
      value: "Spain",
      sourceUrl: EVENTS_URL,
      authority: "FIRST_PARTY",
      confidence: "HIGH",
    },
    {
      field: "phone",
      value: "+34 981-175490",
      sourceUrl: EVENTS_URL,
      authority: "FIRST_PARTY",
      confidence: "HIGH",
    },
    {
      field: "rooms",
      value: 116,
      sourceUrl: ROOMS_URL,
      authority: "FIRST_PARTY",
      confidence: "HIGH",
      note: "Marriott rooms page + ACHM reform article both state 116 habitaciones",
      corroborationUrl: ACHM_REFORM_URL,
    },
    {
      field: "meetingRooms",
      value: 6,
      sourceUrl: EVENTS_URL,
      authority: "FIRST_PARTY",
      confidence: "HIGH",
      note: "Not written on HPC insert if field not allowlisted — held for HI/ADP layers",
    },
    {
      field: "totalEventSpaceSqM",
      value: 674,
      sourceUrl: EVENTS_URL,
      authority: "FIRST_PARTY",
      confidence: "HIGH",
    },
    {
      field: "largestEventCapacity",
      value: 290,
      sourceUrl: EVENTS_URL,
      authority: "FIRST_PARTY",
      confidence: "HIGH",
      note: "Banquets largest reception capacity from Marriott events product",
    },
    {
      field: "officialUrl",
      value: OFFICIAL_URL_ES,
      sourceUrl: OFFICIAL_URL_ES,
      authority: "FIRST_PARTY",
      confidence: "HIGH",
    },
    {
      field: "operator",
      value: "ACHM Hotels by Marriott / ACHM Spain Management SL",
      sourceUrl: ACHM_REFORM_URL,
      authority: "OPERATOR_OFFICIAL",
      confidence: "HIGH",
      note: "ACHM states they manage AC Hotel A Coruña; not written to HPC owner/operator fields (gated)",
    },
    {
      field: "owner",
      value: null,
      status: "UNKNOWN",
      confidence: "UNKNOWN",
      note: "ACHM manages third-party owned hotels; specific propco for A Coruña not confirmed — UNKNOWN",
    },
  ],
  identityConfidence: "HIGH",
});

function buildInsertFields(geo) {
  const today = new Date().toISOString().slice(0, 10);
  const raw = {
    "Property Name": "AC Hotel A Coruña",
    "Canonical Property Name": "AC Hotel A Coruña",
    "Property Identity Key": IDENTITY_KEY,
    "Current Brand": "AC Hotels by Marriott",
    "Brand Family": "Marriott International",
    "Affiliation Status": "Branded",
    City: "A Coruña",
    "State / Region": "Galicia",
    Country: "Spain",
    Continent: "Europe",
    Market: "A Coruña",
    Address: "Enrique Mariñas 36",
    "Postal Code": "15009",
    "Address Confidence": "High",
    "Address Source URL": EVENTS_URL,
    Phone: "+34 981-175490",
    "Rooms / Keys": 116,
    "Rooms Confidence": "High",
    "Rooms Source URL": ROOMS_URL,
    "Rooms Source Type": "official_property_page",
    "Rooms Reviewed Date": today,
    "Official Property URL": OFFICIAL_URL_ES,
    "Source URL": EVENTS_URL,
    "Family / Source Family": "Marriott",
    "Source Type": "official_property_page",
    "Source Confidence": "High",
    "Identity Confidence": "High",
    "Data Eligible": true,
    "Production Use Status": "Census Only / Not Owner-Facing",
    "Public Display Review Status": "Hold",
    "Radar Display Status": "Hold",
    "Radar Geography Status":
      geo?.ok && geo.permanentFlag ? "Coordinates Available" : "Address Only",
    "Enrichment Status": "Identity Seeded — pending enrichment",
    "Enrichment Priority": "High",
    "Discovery Date": today,
    "Last Reviewed Date": today,
    "Meeting Space Flag": true,
  };

  if (geo?.ok && geo.permanentFlag && geo.latitude != null && geo.longitude != null) {
    raw.Latitude = geo.latitude;
    raw.Longitude = geo.longitude;
    raw["Coordinate Source Type"] = "official_address_geocode";
    raw["Coordinate Confidence"] = "High";
  }

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
          /lcgco|ac hotel a coru|enrique mari/.test(blob) ||
          String(f["Official Property URL"] || "")
            .toLowerCase()
            .includes("lcgco")
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
  const geo = loadGeocode();
  fs.writeFileSync(
    path.join(OUT, "EVIDENCE_PACK.json"),
    JSON.stringify(AC_CORUNA_EVIDENCE_PACK, null, 2)
  );
  fs.writeFileSync(
    path.join(OUT, "OWNERSHIP_OPERATOR_RESEARCH.json"),
    JSON.stringify(
      {
        generatedAt: new Date().toISOString(),
        owner: null,
        ownerConfidence: "UNKNOWN",
        ownerNotes:
          "ACHM manages hotels owned by third parties; specific A Coruña propco not confirmed from public sources this pass.",
        operator: "ACHM Hotels by Marriott (ACHM Spain Management SL, CIF B86107406)",
        operatorConfidence: "HIGH",
        operatorSources: [
          ACHM_REFORM_URL,
          "https://achmhotels.com/en/about-us/",
          "https://www.empresia.es/empresa/achm-spain-management/",
        ],
        leaseOrManagementStructure:
          "Public ACHM materials describe management/franchise of third-party owned hotels under AC Hotels by Marriott — structure UNKNOWN at property level",
        unresolvedGaps: ["property_owner_legal_entity", "lease_vs_management_contract_terms"],
      },
      null,
      2
    )
  );

  const { raw, sanitized } = buildInsertFields(geo);
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
    geocodeAttached: Boolean(geo?.ok && geo.permanentFlag),
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
      ["Canonical Property Name", "AC Hotel A Coruña"],
      ["Property Identity Key", IDENTITY_KEY],
      ["Country", "Spain"],
      ["City", "A Coruña"],
      ["Current Brand", "AC Hotels by Marriott"],
      ["Rooms / Keys", 116],
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
    // Marriott code persistence via identity key + official URL
    const url = String(readback.fields?.["Official Property URL"] || "");
    const key = String(readback.fields?.["Property Identity Key"] || "");
    if (!/lcgco/i.test(url) || !/lcgco/i.test(key)) {
      mismatches += 1;
      result.mismatches = result.mismatches || [];
      result.mismatches.push({
        field: "marriott_code_lcgco",
        expected: "LCGCO in identity key + URL",
        got: { key, url },
      });
    }
    result.mismatchCount = mismatches;
  }

  fs.writeFileSync(path.join(OUT, "WRITE_RESULT.json"), JSON.stringify(result, null, 2));
  console.log(
    JSON.stringify(
      {
        ok: result.created && result.mismatchCount === 0,
        recordId,
        mismatches: result.mismatches || [],
        outDir: OUT,
      },
      null,
      2
    )
  );
  if (!(result.created && result.mismatchCount === 0)) process.exit(4);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
