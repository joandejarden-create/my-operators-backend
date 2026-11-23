/**
 * Memmo Príncipe Real — HPC stewardship enroll (identity seed).
 *
 * Usage:
 *   node scripts/memmo-principe-real-hpc-stewardship-apply.mjs
 *   node scripts/memmo-principe-real-hpc-stewardship-apply.mjs --apply \
 *     --confirm-memmo-principe-real-census-steward-insert \
 *     --confirm-hotel-property-census-only \
 *     --confirm-no-legacy-census-writes \
 *     --confirm-no-owner-operator \
 *     --enable-production-writes
 *
 * Target: AIRTABLE_BASE_ID_ALT / Hotel Property Census only.
 * Founder V1.2 authorized stewarded enrollment when conclusively absent.
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
import { TABLE_IDS } from "../lib/research-engine-v2/production-census-write.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT = path.join(
  ROOT,
  "reports/gdi-design-hotels-operational-pilot-v1/memmo-hpc-enrollment-v1"
);

const APPLY =
  process.argv.includes("--apply") || process.argv.includes("--enable-production-writes");
const ENABLE = process.argv.includes("--enable-production-writes");

const CONFIRMS = {
  stewardInsert: process.argv.includes("--confirm-memmo-principe-real-census-steward-insert"),
  hpcOnly: process.argv.includes("--confirm-hotel-property-census-only"),
  noLegacy: process.argv.includes("--confirm-no-legacy-census-writes"),
  noOwner: process.argv.includes("--confirm-no-owner-operator"),
};

const IDENTITY_KEY = "memmo_pt_lisbon_principe_real_lisdm";
const OFFICIAL_DH_URL =
  "https://www.designhotels.com/hotels/portugal/lisbon/memmo-principe-real/";
const OFFICIAL_MARRIOTT_URL =
  "https://www.marriott.com/en-us/hotels/lisdm-memmo-principe-real-lisbon-a-member-of-design-hotels/overview/";
const MARSHA = "LISDM";
const ADDRESS = "Rua D. Pedro V 56 J, 1250-094 Lisbon, Portugal";

export const MEMMO_EVIDENCE_PACK = Object.freeze({
  version: "memmo_principe_real_census_stewardship_evidence_v1",
  hotel: "Memmo Príncipe Real",
  duplicateDecision: "NO_EXISTING_CANONICAL_MATCH",
  identityConfidence: "HIGH",
  scanEvidence: {
    fullTableScanned: 19580,
    portugalRecords: 0,
    lisdmHits: 0,
    memmoHits: 0,
    report: "phase1-portugal-scan.json",
  },
  fields: [
    {
      field: "hotelName",
      value: "Memmo Príncipe Real",
      sourceUrl: OFFICIAL_DH_URL,
      authority: "FIRST_PARTY",
      confidence: "HIGH",
    },
    {
      field: "brand",
      value: "Design Hotels",
      sourceUrl: OFFICIAL_DH_URL,
      authority: "FIRST_PARTY",
      confidence: "HIGH",
    },
    {
      field: "marsha",
      value: MARSHA,
      sourceUrl: OFFICIAL_MARRIOTT_URL,
      authority: "FIRST_PARTY",
      confidence: "HIGH",
    },
    {
      field: "address",
      value: ADDRESS,
      sourceUrl: OFFICIAL_DH_URL,
      authority: "FIRST_PARTY",
      confidence: "HIGH",
    },
    {
      field: "city",
      value: "Lisbon",
      sourceUrl: OFFICIAL_DH_URL,
      authority: "FIRST_PARTY",
      confidence: "HIGH",
    },
    {
      field: "country",
      value: "Portugal",
      sourceUrl: OFFICIAL_DH_URL,
      authority: "FIRST_PARTY",
      confidence: "HIGH",
    },
    {
      field: "rooms",
      value: 41,
      sourceUrl: OFFICIAL_DH_URL,
      authority: "FIRST_PARTY",
      confidence: "HIGH",
      corroborationUrl: OFFICIAL_MARRIOTT_URL,
    },
    {
      field: "meetingFacilities",
      value: "Boutique / limited meetings — not advertised as multi-event conference plant",
      sourceUrl: OFFICIAL_DH_URL,
      authority: "FIRST_PARTY",
      confidence: "HIGH",
      note: "Meeting rooms not required for GDI eligibility",
    },
  ],
});

function buildInsertFields() {
  const today = new Date().toISOString().slice(0, 10);
  const raw = {
    "Property Name": "Memmo Príncipe Real",
    "Canonical Property Name": "Memmo Príncipe Real",
    "Property Identity Key": IDENTITY_KEY,
    "Current Brand": "Design Hotels",
    "Brand Family": "Marriott International",
    "Affiliation Status": "Branded",
    City: "Lisbon",
    "State / Region": "Lisbon",
    Country: "Portugal",
    Continent: "Europe",
    Market: "Lisbon",
    Submarket: "Príncipe Real",
    Address: ADDRESS,
    "Postal Code": "1250-094",
    "Address Confidence": "High",
    "Address Source URL": OFFICIAL_DH_URL,
    "Rooms / Keys": 41,
    "Rooms Confidence": "High",
    "Rooms Source URL": OFFICIAL_DH_URL,
    "Rooms Source Type": "official_property_page",
    "Rooms Reviewed Date": today,
    "Official Property URL": OFFICIAL_DH_URL,
    "Source URL": OFFICIAL_MARRIOTT_URL,
    "Family / Source Family": "Marriott",
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
    "Property Type": "Hotel",
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
        const propertyId = String(f["Property ID"] || "");
        if (
          key === IDENTITY_KEY ||
          propertyId.toUpperCase() === MARSHA ||
          /lisdm/.test(blob) ||
          (/memmo/.test(blob) && /pr[ií]ncipe|principe/.test(blob)) ||
          String(f["Official Property URL"] || "")
            .toLowerCase()
            .includes("memmo-principe-real")
        ) {
          hits.push({
            id: r.id,
            name: f["Canonical Property Name"] || f["Property Name"] || f.name || null,
            key,
            propertyId: propertyId || null,
            city: f.City || f.city || null,
            country: f.Country || f.country || null,
          });
        }
      }
      next();
    });
  return { scanned, hits };
}

/**
 * Best-effort legacy Design Hotels field parity (name / Affiliation / Property ID).
 * Failures are reported; canonical Property Name insert remains authoritative.
 */
async function patchLegacyDesignHotelsFields(baseId, token, recordId) {
  const base = new Airtable({ apiKey: token }).base(baseId);
  const legacy = {
    name: "Memmo Príncipe Real",
    Affiliation: "Design Hotels",
    "Property ID": MARSHA,
    city: "Lisbon",
    country: "Portugal",
    rooms: 41,
    Website: OFFICIAL_DH_URL,
    "Address 1": "Rua D. Pedro V 56 J",
    "Postal Code": "1250-094",
    Market: "Lisbon",
    Submarket: "Príncipe Real",
    Continent: "Europe",
    "Parent Company": "Marriott International",
    "Conference (Y/N)": "N",
    "Boutique (Y/N)": "Y",
    status: ["Open"],
    project_phase: "Open",
  };
  try {
    const updated = await base("Hotel Property Census").update(recordId, legacy);
    return { ok: true, id: updated.id, writtenKeys: Object.keys(legacy) };
  } catch (err) {
    return {
      ok: false,
      error: err.message || String(err),
      status: err.statusCode || err.status || null,
      attemptedKeys: Object.keys(legacy),
    };
  }
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const { raw, sanitized } = buildInsertFields();
  fs.writeFileSync(
    path.join(OUT, "EVIDENCE_PACK.json"),
    JSON.stringify(MEMMO_EVIDENCE_PACK, null, 2)
  );
  const preview = {
    generatedAt: new Date().toISOString(),
    dryRun: !APPLY,
    identityConfidence: "HIGH",
    duplicateDecision: "NO_EXISTING_CANONICAL_MATCH",
    identityKey: IDENTITY_KEY,
    marsha: MARSHA,
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
    const reuseId = dedup.hits[0].id;
    fs.writeFileSync(
      path.join(OUT, "ENROLLMENT_RESULT.json"),
      JSON.stringify(
        {
          created: false,
          reusedExisting: true,
          recordId: reuseId,
          hits: dedup.hits,
          reason: "prewrite_dedup_hit_reuse_existing",
        },
        null,
        2
      )
    );
    console.log(
      JSON.stringify({
        ok: true,
        created: false,
        reusedExisting: true,
        recordId: reuseId,
        hits: dedup.hits,
      })
    );
    return;
  }

  const created = await createHotelPropertyCensusRecords(resolvedBaseId, token, [
    { fields: sanitized.fields },
  ]);
  const recordId = created?.created?.[0]?.id || null;
  if (!recordId) {
    console.error(JSON.stringify({ ok: false, reason: "create_returned_no_id", created }));
    process.exit(4);
  }

  const legacyPatch = await patchLegacyDesignHotelsFields(resolvedBaseId, token, recordId);

  const base = new Airtable({ apiKey: token }).base(resolvedBaseId);
  const readback = await base("Hotel Property Census").find(recordId);
  const f = readback.fields || {};

  // Idempotency re-check
  const postDedup = await reDedup(resolvedBaseId, token);

  const result = {
    created: true,
    recordId,
    createResponse: created,
    legacyPatch,
    readback: {
      id: readback.id,
      propertyName: f["Property Name"] || f.name || null,
      canonical: f["Canonical Property Name"] || null,
      identityKey: f["Property Identity Key"] || null,
      currentBrand: f["Current Brand"] || null,
      affiliation: f.Affiliation || null,
      propertyId: f["Property ID"] || null,
      city: f.City || f.city || null,
      country: f.Country || f.country || null,
      market: f.Market || null,
      submarket: f.Submarket || null,
      rooms: f["Rooms / Keys"] ?? f.rooms ?? null,
      officialUrl: f["Official Property URL"] || f.Website || null,
    },
    postDedupHitCount: postDedup.hits.length,
    postDedupIds: postDedup.hits.map((h) => h.id),
    idempotentSingleRecord:
      postDedup.hits.length === 1 && postDedup.hits[0].id === recordId,
  };
  fs.writeFileSync(path.join(OUT, "ENROLLMENT_RESULT.json"), JSON.stringify(result, null, 2));
  console.log(
    JSON.stringify(
      {
        ok: true,
        created: true,
        recordId,
        readback: result.readback,
        legacyPatchOk: legacyPatch.ok,
        idempotentSingleRecord: result.idempotentSingleRecord,
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
