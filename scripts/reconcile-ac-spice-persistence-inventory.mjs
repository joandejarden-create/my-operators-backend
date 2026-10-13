/**
 * Live persistence inventory for AC A Coruña + Spice Island.
 * Read-only. Does not write.
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import {
  CANONICAL_INTELLIGENCE_BASE_ID,
  LEGACY_DEAL_CAPTURE_MVP_BASE_ID,
  getGdiOpportunitiesAirtableBaseId,
  assertNotLegacyMvpCanonicalBase,
} from "../lib/decision-outcomes/airtable-base.js";
import { loadHotelIntelligenceFromAirtable } from "../lib/hotel-intelligence/schema/hi-airtable-store.js";
import { HI_TABLES } from "../lib/hotel-intelligence/schema/hotel-intelligence-schema-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT_DIR = path.join(ROOT, "reports", "hotel-census", "ac-spice-persistence-reconciliation-v1");

const AC_ID = "rec2PVBDavppGpenm";
const SPICE_ID = "recKRJjcPnb4tVDDS";

const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
const ALT = process.env.AIRTABLE_BASE_ID_ALT;
const intel =
  getGdiOpportunitiesAirtableBaseId() ||
  process.env.ADP_AIRTABLE_BASE_ID ||
  CANONICAL_INTELLIGENCE_BASE_ID;
assertNotLegacyMvpCanonicalBase(intel, { surface: "ac-spice-reconciliation" });

if (!token) throw new Error("missing_airtable_token");
if (intel !== CANONICAL_INTELLIGENCE_BASE_ID) {
  throw new Error(`wrong_intel_base:${intel}`);
}

const hpcBase = new Airtable({ apiKey: token }).base(ALT);
const intelBase = new Airtable({ apiKey: token }).base(intel);

function esc(s) {
  return String(s || "").replace(/"/g, '\\"');
}

async function getFull(id) {
  const r = await hpcBase("Hotel Property Census").find(id);
  const f = r.fields;
  return {
    id: r.id,
    propertyName: f["Property Name"],
    canonical: f["Canonical Property Name"],
    address: f.Address,
    city: f.City,
    postal: f["Postal Code"],
    country: f.Country,
    rooms: f["Rooms / Keys"],
    lat: f.Latitude,
    lon: f.Longitude,
    brand: f["Current Brand"],
    brandFamily: f["Brand Family"],
    operator: f["Operator / Management Company"],
    owner: f["Owner Name"],
    ownerType: f["Owner Type"],
    ownerStatus: f["Owner Confidence"] || f["Production Use Status"],
    website: f["Official Property URL"],
    identityKey: f["Property Identity Key"],
    market: f.Market,
    state: f["State / Region"],
    affiliation: f["Affiliation Status"],
  };
}

async function searchHpc(terms) {
  const raw = [];
  for (const t of terms) {
    const formula = `OR(FIND("${esc(t)}",{Property Name}),FIND("${esc(t)}",{Canonical Property Name}),FIND("${esc(t)}",{Address}),FIND("${esc(t)}",{City}))`;
    try {
      await hpcBase("Hotel Property Census")
        .select({
          filterByFormula: formula,
          maxRecords: 15,
          fields: [
            "Property Name",
            "Canonical Property Name",
            "Address",
            "City",
            "Country",
            "Rooms / Keys",
            "Property Identity Key",
            "Current Brand",
            "Official Property URL",
          ],
        })
        .eachPage((recs, next) => {
          for (const r of recs) {
            raw.push({
              term: t,
              id: r.id,
              name: r.fields["Property Name"] || r.fields["Canonical Property Name"],
              city: r.fields.City,
              country: r.fields.Country,
              rooms: r.fields["Rooms / Keys"],
              key: r.fields["Property Identity Key"],
              brand: r.fields["Current Brand"],
              url: r.fields["Official Property URL"],
            });
          }
          next();
        });
    } catch (e) {
      raw.push({ term: t, error: String(e.message || e) });
    }
  }
  const byId = new Map();
  for (const r of raw) if (r.id) byId.set(r.id, r);
  return { hits: [...byId.values()], rawCount: raw.length };
}

async function listByHpc(tableName, hpcField, hpcId, extraFields = []) {
  const rows = [];
  try {
    await intelBase(tableName)
      .select({
        filterByFormula: `{${hpcField}} = "${esc(hpcId)}"`,
        pageSize: 100,
      })
      .eachPage((recs, next) => {
        for (const r of recs) {
          rows.push({
            id: r.id,
            fields: Object.fromEntries(
              Object.entries(r.fields || {}).filter(([k]) =>
                extraFields.length ? extraFields.includes(k) || /key|name|title|status|hotel/i.test(k) : true
              )
            ),
          });
        }
        next();
      });
  } catch (e) {
    return { error: String(e.message || e), rows: [] };
  }
  return { rows, count: rows.length };
}

async function listGdiByHotel(tableName, hotelFieldCandidates, hotelId) {
  const errors = [];
  for (const field of hotelFieldCandidates) {
    try {
      const rows = [];
      await intelBase(tableName)
        .select({
          filterByFormula: `{${field}} = "${esc(hotelId)}"`,
          pageSize: 100,
        })
        .eachPage((recs, next) => {
          for (const r of recs) rows.push({ id: r.id, fields: r.fields });
          next();
        });
      return { fieldUsed: field, count: rows.length, ids: rows.map((r) => r.id), rows };
    } catch (e) {
      errors.push({ field, error: String(e.message || e) });
    }
  }
  // Also try FIND on linked text fields holding hotel id in notes
  try {
    const rows = [];
    await intelBase(tableName)
      .select({
        filterByFormula: `OR(FIND("${esc(hotelId)}",{Hotel ID}),FIND("${esc(hotelId)}",{HPC Hotel ID}),FIND("${esc(hotelId)}",{Canonical Hotel ID}),FIND("${esc(hotelId)}",{Dealality Hotel ID}))`,
        pageSize: 100,
      })
      .eachPage((recs, next) => {
        for (const r of recs) rows.push({ id: r.id, fields: r.fields });
        next();
      });
    return { fieldUsed: "OR_FIND", count: rows.length, ids: rows.map((r) => r.id), rows };
  } catch (e) {
    errors.push({ field: "OR_FIND", error: String(e.message || e) });
  }
  return { error: "all_field_attempts_failed", errors, count: 0, ids: [], rows: [] };
}

const GDI_TABLES = [
  { name: "Demand Generators", fields: ["Hotel ID", "HPC Hotel ID", "Canonical Hotel ID", "Dealality Hotel ID"] },
  { name: "Demand Programs", fields: ["Hotel ID", "HPC Hotel ID", "Canonical Hotel ID"] },
  { name: "Hotel Demand Generator Fit", fields: ["Hotel ID", "HPC Hotel ID", "Canonical Hotel ID", "Dealality Hotel ID"] },
  { name: "Demand Generator Signals", fields: ["Hotel ID", "HPC Hotel ID", "Canonical Hotel ID"] },
  { name: "GDI Opportunities", fields: ["Hotel ID", "HPC Hotel ID", "Canonical Hotel ID", "Dealality Hotel ID"] },
  { name: "GDI Research Targets", fields: ["Hotel ID", "HPC Hotel ID", "Canonical Hotel ID", "Dealality Hotel ID"] },
  { name: "GDI Research Runs", fields: ["Hotel ID", "HPC Hotel ID", "Canonical Hotel ID"] },
  { name: "GDI Research Target Runs", fields: ["Hotel ID", "HPC Hotel ID", "Canonical Hotel ID"] },
  { name: "Private Event Venues", fields: ["Hotel ID", "HPC Hotel ID"] },
  { name: "Hotel Venue Fit", fields: ["Hotel ID", "HPC Hotel ID"] },
  { name: "Private Event Signals", fields: ["Hotel ID", "HPC Hotel ID"] },
  { name: "Hotel ADP Attributes", fields: ["HPC Hotel ID"] },
];

async function inventoryHotel(label, hpcId, terms) {
  const [full, search, hi] = await Promise.all([
    getFull(hpcId),
    searchHpc(terms),
    loadHotelIntelligenceFromAirtable(hpcId).catch((e) => ({ ok: false, error: String(e.message || e) })),
  ]);

  const gdi = {};
  for (const t of GDI_TABLES) {
    gdi[t.name] = await listGdiByHotel(t.name, t.fields, hpcId);
  }

  // HI tables via known field
  const hiTables = {};
  for (const [key, name] of Object.entries(HI_TABLES)) {
    hiTables[key] = await listByHpc(name, "HPC Hotel ID", hpcId);
  }

  return { label, hpcId, full, search, hi, hiTables, gdi };
}

const ac = await inventoryHotel("AC Hotel A Coruña", AC_ID, [
  "AC Hotel A Coruña",
  "AC Hotel A Coruna",
  "AC Coruña",
  "AC Coruna",
  "LCGCO",
  "Enrique Mariñas",
  "Enrique Marinas",
]);

const spice = await inventoryHotel("Spice Island Beach Resort", SPICE_ID, [
  "Spice Island Beach Resort",
  "Spice Island Resort",
  "Spice Island Beach",
  "Grand Anse",
  "spiceislandbeachresort",
]);

function classifyAc(packet) {
  const ids = packet.search.hits.map((h) => h.id);
  if (ids.length === 1 && ids[0] === AC_ID) return "EXACT_CANONICAL_MATCH";
  if (ids.includes(AC_ID) && ids.length > 1) return "DUPLICATE_FOUND";
  if (!ids.includes(AC_ID)) return "MISSING_EXPECTED_RECORD";
  return "AMBIGUOUS";
}

function classifySpice(packet) {
  const spiceHits = packet.search.hits.filter((h) => /spice island/i.test(h.name || ""));
  if (spiceHits.length === 1 && spiceHits[0].id === SPICE_ID) return "EXACT_CANONICAL_MATCH";
  if (spiceHits.length > 1) return "LIKELY_DUPLICATE";
  if (spiceHits.length === 0 && packet.search.hits.length === 0) return "NO_EXISTING_CANONICAL_MATCH";
  if (spiceHits.length === 1) return "EXACT_CANONICAL_MATCH";
  return "AMBIGUOUS";
}

const report = {
  generatedAt: new Date().toISOString(),
  branchExpected: "deploy/gdi-pe-v1-7-customer-closure",
  bases: {
    hpcAlt: ALT,
    intelligence: intel,
    canonicalIntelligence: CANONICAL_INTELLIGENCE_BASE_ID,
    forbiddenLegacy: LEGACY_DEAL_CAPTURE_MVP_BASE_ID,
    legacyGuardActive: true,
    wrongBaseRisk: intel === LEGACY_DEAL_CAPTURE_MVP_BASE_ID ? "FAIL" : "PASS",
  },
  ac: {
    ...ac,
    classify: classifyAc(ac),
    duplicates: ac.search.hits.filter((h) => h.id !== AC_ID),
  },
  spice: {
    ...spice,
    classify: classifySpice(spice),
    duplicates: spice.search.hits.filter((h) => h.id !== SPICE_ID && /spice island/i.test(h.name || "")),
  },
};

fs.mkdirSync(OUT_DIR, { recursive: true });
const outPath = path.join(OUT_DIR, "PHASE0_1_INVENTORY.json");
fs.writeFileSync(outPath, JSON.stringify(report, null, 2) + "\n");

function summarize(p) {
  return {
    classify: p.classify,
    hpcExists: Boolean(p.full?.id),
    rooms: p.full?.rooms,
    brand: p.full?.brand,
    website: p.full?.website,
    searchHitCount: p.search.hits.length,
    duplicateCount: p.duplicates.length,
    hi: {
      commercial: p.hi?.commercial?.airtableRecordId || null,
      eventSpaces: (p.hi?.eventSpaces || []).length,
      demandNodes: (p.hi?.demandNodes || []).length,
      seasonality: (p.hi?.seasonality || []).length,
      needPeriods: (p.hi?.needPeriods || []).length,
      evidence: (p.hi?.evidence || []).length,
      error: p.hi?.error || null,
    },
    gdiCounts: Object.fromEntries(
      Object.entries(p.gdi || {}).map(([k, v]) => [k, { count: v.count || 0, fieldUsed: v.fieldUsed || null, error: v.error || null, sampleIds: (v.ids || []).slice(0, 5) }])
    ),
  };
}

console.log(
  JSON.stringify(
    {
      outPath,
      bases: report.bases,
      ac: summarize(report.ac),
      spice: summarize(report.spice),
    },
    null,
    2
  )
);
