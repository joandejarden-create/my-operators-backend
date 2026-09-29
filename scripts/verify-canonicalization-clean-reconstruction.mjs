/**
 * Clean-process reconstruction check for Bethesda + AC + Spice
 * after Airtable canonicalization audit (read-only).
 */
import "../load-env.js";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import Airtable from "airtable";
import { loadHotelIntelligenceFromAirtable } from "../lib/hotel-intelligence/schema/hi-airtable-store.js";
import { buildHotelIntelligenceProfile } from "../lib/hotel-intelligence/adp-attributes/build-hotel-intelligence-profile.js";
import { buildAdpHotelAttributes } from "../lib/hotel-intelligence/adp-attributes/build-adp-hotel-attributes.js";
import { loadHotelDemandConfig } from "../lib/group-demand-intelligence/hotel-profile.js";
import { resolveCanonicalHotelId } from "../lib/hotel-census/adp-gdi-canonical-identity.js";
import { MAP_HOTEL_ADP_ATTRIBUTE as ATTR } from "../lib/hotel-intelligence/adp-attributes/field-map.js";
import { MAP_HOTEL_GENERATOR_FIT as FIT } from "../lib/group-demand-intelligence/demand-generators/airtable-field-map.js";
import { MAP_RESEARCH_TARGET as TGT } from "../lib/group-demand-intelligence/research-coverage/airtable-field-map.js";
import {
  CANONICAL_INTELLIGENCE_BASE_ID,
  assertNotLegacyMvpCanonicalBase,
} from "../lib/decision-outcomes/airtable-base.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const OUT_DIR = path.join(ROOT, "reports", "airtable-canonicalization-v1");
const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
const baseId = CANONICAL_INTELLIGENCE_BASE_ID;
assertNotLegacyMvpCanonicalBase(baseId, { surface: "canonicalization-reconstruct" });
const base = new Airtable({ apiKey: token }).base(baseId);

function esc(s) {
  return String(s || "").replace(/"/g, '\\"');
}

async function count(table, field, hotelId) {
  let n = 0;
  await base(table)
    .select({ filterByFormula: `{${field}} = "${esc(hotelId)}"`, pageSize: 100 })
    .eachPage((recs, next) => {
      n += recs.length;
      next();
    });
  return n;
}

async function activeAttrMultiKey(hotelId) {
  const rows = [];
  await base("Hotel ADP Attributes")
    .select({
      filterByFormula: `AND({${ATTR.hpcHotelId}} = "${esc(hotelId)}", {${ATTR.active}} = 1)`,
      pageSize: 100,
      fields: [ATTR.dedupeKey, ATTR.attributeKey, ATTR.active],
    })
    .eachPage((recs, next) => {
      for (const r of recs) rows.push(r.fields?.[ATTR.dedupeKey] || r.fields?.[ATTR.attributeKey]);
      next();
    });
  const m = new Map();
  for (const k of rows) {
    if (!k) continue;
    m.set(k, (m.get(k) || 0) + 1);
  }
  return [...m.values()].filter((c) => c > 1).length;
}

const hotels = [
  { label: "Bethesda", hpcId: "recLuxvwwxID7U2B8", adp: "adp_bethesda_marriott" },
  { label: "AC", hpcId: "rec2PVBDavppGpenm", adp: "adp_ac_hotel_a_coruna" },
  { label: "Spice", hpcId: "recKRJjcPnb4tVDDS", adp: "adp_spice_island_beach_resort" },
];

const results = {};
for (const h of hotels) {
  const hi = await loadHotelIntelligenceFromAirtable(h.hpcId);
  const profile = await buildHotelIntelligenceProfile(h.hpcId);
  const attrs = await buildAdpHotelAttributes(h.hpcId, { profile });
  const config = loadHotelDemandConfig(h.hpcId);
  const multi = await activeAttrMultiKey(h.hpcId);
  const fits = await count("Hotel Demand Generator Fit", FIT.hotelId, h.hpcId);
  const targets = await count("GDI Research Targets", TGT.hotelId, h.hpcId);
  let runs = 0;
  try {
    runs = await count("GDI Research Runs", "Hotel ID", h.hpcId);
  } catch {
    runs = 0;
  }
  const pub = path.join(ROOT, "data/ai-demand-positioning/published", h.adp, "manifest.json");
  const manifest = fs.existsSync(pub) ? JSON.parse(fs.readFileSync(pub, "utf8")) : null;
  const pass =
    resolveCanonicalHotelId(h.adp) === h.hpcId &&
    Boolean(hi.commercial?.airtableRecordId) &&
    multi === 0 &&
    Boolean(config) &&
    fits > 0 &&
    targets > 0 &&
    manifest?.publishStatus === "Live";

  results[h.label] = {
    pass,
    hpc: resolveCanonicalHotelId(h.adp) === h.hpcId,
    hiCommercial: hi.commercial?.airtableRecordId || null,
    eventSpaces: (hi.eventSpaces || []).length,
    demandNodes: (hi.demandNodes || []).length,
    evidence: (hi.evidence || []).length,
    fromHiAirtable: profile.commercialProfile?.fromHiAirtable === true,
    adpAttrBuilt: attrs.counts?.total,
    activeAttrMultiKey: multi,
    gdiConfig: Boolean(config),
    fits,
    targets,
    runs,
    adpPublished: manifest?.publishStatus || null,
    adpPeriod: manifest?.latestPeriodId || null,
  };
}

fs.mkdirSync(OUT_DIR, { recursive: true });
fs.writeFileSync(
  path.join(OUT_DIR, "CLEAN_RECONSTRUCTION.json"),
  JSON.stringify({ generatedAt: new Date().toISOString(), results }, null, 2) + "\n"
);
console.log(JSON.stringify(results, null, 2));
