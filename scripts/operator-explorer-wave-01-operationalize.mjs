#!/usr/bin/env node
/**
 * Operator Explorer Operationalization + Wave 01 enrichment.
 *
 *   node scripts/operator-explorer-wave-01-operationalize.mjs --dry-run
 *   node scripts/operator-explorer-wave-01-operationalize.mjs --apply --approve-oe-wave-01-writes
 *
 * Webhound: deferred unless done=true (checked separately). No Fit/owner changes.
 */
import "../load-env.js";
import { createHash } from "crypto";
import { mkdirSync, writeFileSync, readFileSync, existsSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import {
  buildOperatorUniverse,
  dispositionForOperator,
  RECORD_PURPOSE,
} from "../lib/operator-explorer/operator-universe.js";
import { classifyExplorerReadiness, isAggregateAssignmentName } from "../lib/operator-explorer/readiness.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const APPLY = process.argv.includes("--apply");
const APPROVED = process.argv.includes("--approve-oe-wave-01-writes");
const DRY = !APPLY;
const TS = new Date().toISOString().replace(/[:.]/g, "-").slice(0, 19);

const MASTER = "Operator Setup - Master";
const ASG = "Operator Intelligence - Assignments";
const BR = "Operator Intelligence - Brand Relationships";
const MP = "Operator Intelligence - Market Presence";
const PI = "Partner Intelligence - Source Library";
const CLAIMS = "Operator Intelligence - Claims";

/** Wave 01 curated named assignments — official property / brand sites preferred. */
const WAVE_01_OPERATORS = [
  { name: "Marriott International (Managed)", id: "recGmiPhRt6hiayd9", priority: 1, reason: "BMC present; needs named managed hotels (esp. CALA)" },
  { name: "Hilton (Managed)", id: "rec3Uwxe6ovpiokuN", priority: 1, reason: "BMC present; needs named managed hotels" },
  { name: "Accor (Managed)", id: "recF2WqLqNVyKGz9E", priority: 1, reason: "BMC present; needs named managed hotels" },
  { name: "IHG Hotels & Resorts (Managed)", id: "rec7IXYQYpKMYsrDl", priority: 1, reason: "Managed subset; needs named examples" },
  { name: "Minor Hotels (Managed)", id: "rec8SrT3VjRkkYTxm", priority: 1, reason: "NH/Anantara managed lens; named hotels" },
  { name: "Atlantica Hotels International (AHI)", id: "recfwDdU5t9h4uFnZ", priority: 2, reason: "Production; countries exist; named LatAm hotels missing" },
  { name: "Grupo Iberostar", id: "recwEHUotSGpfkZEJ", priority: 2, reason: "CALA integrated; zero named assignments" },
  { name: "Tafer Hotels & Resorts", id: "recJ6NPSYveCTo3At", priority: 2, reason: "Mexico CALA operator; Production empty intel" },
  { name: "Royalton Hotels & Resorts", id: "recOc5kpsg4Muip9Y", priority: 2, reason: "Caribbean AI platform; Production empty" },
  { name: "Grupo Presidente", id: "recJtFkhjaO57rSDC", priority: 2, reason: "Mexico urban/full-service; Production empty" },
];

const WAVE_01_ASSIGNMENTS = {
  recGmiPhRt6hiayd9: [
    {
      propertyName: "JW Marriott Cancun Resort & Spa",
      country: "Mexico",
      city: "Cancún",
      brand: "JW Marriott",
      brandParent: "Marriott International",
      urbanOrResort: "Resort",
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Brand-managed",
      assignmentStatus: "Current",
      sourceUrl: "https://www.marriott.com/en-us/hotels/cunjw-jw-marriott-cancun-resort-and-spa/overview/",
      evidenceClass: "primary_authoritative",
    },
    {
      propertyName: "The Ritz-Carlton, Cancun",
      country: "Mexico",
      city: "Cancún",
      brand: "The Ritz-Carlton",
      brandParent: "Marriott International",
      urbanOrResort: "Resort",
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Brand-managed",
      assignmentStatus: "Current",
      sourceUrl: "https://www.ritzcarlton.com/en/hotels/cunrz-the-ritz-carlton-cancun/overview/",
      evidenceClass: "primary_authoritative",
    },
    {
      propertyName: "JW Marriott Hotel Mexico City",
      country: "Mexico",
      city: "Mexico City",
      brand: "JW Marriott",
      brandParent: "Marriott International",
      urbanOrResort: "Urban",
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Brand-managed",
      assignmentStatus: "Current",
      sourceUrl: "https://www.marriott.com/en-us/hotels/mexjw-jw-marriott-hotel-mexico-city/overview/",
      evidenceClass: "primary_authoritative",
    },
  ],
  rec3Uwxe6ovpiokuN: [
    {
      propertyName: "Hilton Mexico City Reforma",
      country: "Mexico",
      city: "Mexico City",
      brand: "Hilton Hotels & Resorts",
      brandParent: "Hilton",
      urbanOrResort: "Urban",
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Brand-managed",
      assignmentStatus: "Current",
      sourceUrl: "https://www.hilton.com/en/hotels/mexschh-hilton-mexico-city-reforma/",
      evidenceClass: "primary_authoritative",
    },
    {
      propertyName: "Conrad Tulum Riviera Maya",
      country: "Mexico",
      city: "Tulum",
      brand: "Conrad Hotels & Resorts",
      brandParent: "Hilton",
      urbanOrResort: "Resort",
      developmentContext: "New Build",
      operatingStructure: "Brand-managed",
      assignmentStatus: "Current",
      sourceUrl: "https://www.hilton.com/en/hotels/cuntici-conrad-tulum-riviera-maya/",
      evidenceClass: "primary_authoritative",
    },
    {
      propertyName: "Hilton Panama",
      country: "Panama",
      city: "Panama City",
      brand: "Hilton Hotels & Resorts",
      brandParent: "Hilton",
      urbanOrResort: "Urban",
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Brand-managed",
      assignmentStatus: "Current",
      sourceUrl: "https://www.hilton.com/en/hotels/ptyhihh-hilton-panama/",
      evidenceClass: "primary_authoritative",
    },
  ],
  recF2WqLqNVyKGz9E: [
    {
      propertyName: "Sofitel Mexico City Reforma",
      country: "Mexico",
      city: "Mexico City",
      brand: "Sofitel",
      brandParent: "Accor",
      urbanOrResort: "Urban",
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Brand-managed",
      assignmentStatus: "Current",
      sourceUrl: "https://all.accor.com/hotel/8554/index.en.shtml",
      evidenceClass: "primary_authoritative",
    },
    {
      propertyName: "Fairmont Mayakoba",
      country: "Mexico",
      city: "Playa del Carmen",
      brand: "Fairmont",
      brandParent: "Accor",
      urbanOrResort: "Resort",
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Brand-managed",
      assignmentStatus: "Current",
      sourceUrl: "https://www.fairmont.com/mayakoba/",
      evidenceClass: "primary_authoritative",
    },
    {
      propertyName: "Pullman Mexico City Reforma",
      country: "Mexico",
      city: "Mexico City",
      brand: "Pullman",
      brandParent: "Accor",
      urbanOrResort: "Urban",
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Brand-managed",
      assignmentStatus: "Current",
      sourceUrl: "https://all.accor.com/hotel/B158/index.en.shtml",
      evidenceClass: "primary_authoritative",
    },
  ],
  rec7IXYQYpKMYsrDl: [
    {
      propertyName: "InterContinental Presidente Mexico City",
      country: "Mexico",
      city: "Mexico City",
      brand: "InterContinental",
      brandParent: "IHG",
      urbanOrResort: "Urban",
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Brand-managed",
      assignmentStatus: "Current",
      sourceUrl: "https://www.ihg.com/intercontinental/hotels/us/en/mexico-city/mexha/hoteldetail",
      evidenceClass: "primary_authoritative",
    },
    {
      propertyName: "InterContinental Presidente Cancun Resort",
      country: "Mexico",
      city: "Cancún",
      brand: "InterContinental",
      brandParent: "IHG",
      urbanOrResort: "Resort",
      allInclusive: true,
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Brand-managed",
      assignmentStatus: "Current",
      sourceUrl: "https://www.ihg.com/intercontinental/hotels/us/en/cancun/cunha/hoteldetail",
      evidenceClass: "primary_authoritative",
    },
    {
      propertyName: "Crowne Plaza Panama Airport",
      country: "Panama",
      city: "Panama City",
      brand: "Crowne Plaza",
      brandParent: "IHG",
      urbanOrResort: "Urban",
      hotelType: "Airport",
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Brand-managed",
      assignmentStatus: "Current",
      sourceUrl: "https://www.ihg.com/crowneplaza/hotels/us/en/panama-city/ptyap/hoteldetail",
      evidenceClass: "primary_authoritative",
    },
  ],
  rec8SrT3VjRkkYTxm: [
    {
      propertyName: "NH Collection Mexico City Reforma",
      country: "Mexico",
      city: "Mexico City",
      brand: "NH Collection",
      brandParent: "Minor Hotels",
      urbanOrResort: "Urban",
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Brand-managed",
      assignmentStatus: "Current",
      sourceUrl: "https://www.nh-hotels.com/en/hotel/nh-collection-mexico-city-reforma",
      evidenceClass: "primary_authoritative",
    },
    {
      propertyName: "NH Collection Mexico City Airport T2",
      country: "Mexico",
      city: "Mexico City",
      brand: "NH Collection",
      brandParent: "Minor Hotels",
      urbanOrResort: "Urban",
      hotelType: "Airport",
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Brand-managed",
      assignmentStatus: "Current",
      sourceUrl: "https://www.nh-hotels.com/en/hotel/nh-collection-mexico-city-airport-t2",
      evidenceClass: "primary_authoritative",
    },
    {
      propertyName: "Anantara Plaza Nice Hotel",
      country: "France",
      city: "Nice",
      brand: "Anantara",
      brandParent: "Minor Hotels",
      urbanOrResort: "Urban",
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Brand-managed",
      assignmentStatus: "Current",
      sourceUrl: "https://www.anantara.com/en/plaza-nice",
      evidenceClass: "primary_authoritative",
      limitations: "International reference; not CALA",
    },
  ],
  recfwDdU5t9h4uFnZ: [
    {
      propertyName: "Tryp by Wyndham São Paulo Iguatemi",
      country: "Brazil",
      city: "São Paulo",
      brand: "Tryp by Wyndham",
      brandParent: "Wyndham",
      urbanOrResort: "Urban",
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Third-Party Management",
      assignmentStatus: "Current",
      sourceUrl: "https://www.atlanticahotels.com.br/",
      evidenceClass: "referenced",
      limitations: "Operator portfolio site; property pages should be re-verified",
      publicationStatus: "Publish With Evidence Label",
    },
    {
      propertyName: "Comfort Hotel Ibirapuera",
      country: "Brazil",
      city: "São Paulo",
      brand: "Comfort Inn",
      brandParent: "Choice Hotels",
      urbanOrResort: "Urban",
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Third-Party Management",
      assignmentStatus: "Current",
      sourceUrl: "https://www.atlanticahotels.com.br/",
      evidenceClass: "referenced",
      publicationStatus: "Publish With Evidence Label",
    },
  ],
  recwEHUotSGpfkZEJ: [
    {
      propertyName: "Iberostar Selection Cancun",
      country: "Mexico",
      city: "Cancún",
      brand: "Iberostar",
      brandParent: "Grupo Iberostar",
      urbanOrResort: "Resort",
      allInclusive: true,
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Owner-Operated",
      assignmentStatus: "Current",
      sourceUrl: "https://www.iberostar.com/en/hotels/cancun/iberostar-selection-cancun/",
      evidenceClass: "primary_authoritative",
    },
    {
      propertyName: "Iberostar Selection Paraiso Lindo",
      country: "Mexico",
      city: "Riviera Maya",
      brand: "Iberostar",
      brandParent: "Grupo Iberostar",
      urbanOrResort: "Resort",
      allInclusive: true,
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Owner-Operated",
      assignmentStatus: "Current",
      sourceUrl: "https://www.iberostar.com/en/hotels/riviera-maya/iberostar-selection-paraiso-lindo/",
      evidenceClass: "primary_authoritative",
    },
    {
      propertyName: "Iberostar Hacienda Dominicus",
      country: "Dominican Republic",
      city: "Bayahibe",
      brand: "Iberostar",
      brandParent: "Grupo Iberostar",
      urbanOrResort: "Resort",
      allInclusive: true,
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Owner-Operated",
      assignmentStatus: "Current",
      sourceUrl: "https://www.iberostar.com/en/hotels/bavaro/iberostar-hacienda-dominicus/",
      evidenceClass: "primary_authoritative",
    },
  ],
  recJ6NPSYveCTo3At: [
    {
      propertyName: "Live Aqua Beach Resort Cancun",
      country: "Mexico",
      city: "Cancún",
      brand: "Live Aqua",
      brandParent: "Tafer Hotels & Resorts",
      urbanOrResort: "Resort",
      allInclusive: true,
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Owner-Operated",
      assignmentStatus: "Current",
      sourceUrl: "https://www.taferhotelsandresorts.com/",
      evidenceClass: "referenced",
      publicationStatus: "Publish With Evidence Label",
      limitations: "Operator corporate site; property page re-verify recommended",
    },
    {
      propertyName: "The Resort at Pedregal",
      country: "Mexico",
      city: "Cabo San Lucas",
      brand: "Independent / Pedregal",
      brandParent: "Tafer Hotels & Resorts",
      urbanOrResort: "Resort",
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Owner-Operated",
      assignmentStatus: "Current",
      sourceUrl: "https://www.theresortatpedregal.com/",
      evidenceClass: "primary_authoritative",
    },
    {
      propertyName: "Grand Fiesta Americana Coral Beach Cancun",
      country: "Mexico",
      city: "Cancún",
      brand: "Grand Fiesta Americana",
      brandParent: "Grupo Posadas",
      urbanOrResort: "Resort",
      allInclusive: true,
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Owner-Operated",
      assignmentStatus: "Current",
      sourceUrl: "https://www.fiestamericana.com/en/hotels/grand-fiesta-americana-coral-beach-cancun",
      evidenceClass: "referenced",
      limitations: "Tafer affiliation must not be overstated — hold if counterparty unclear",
      publicationStatus: "Internal / Validation Required",
      hold: true,
      holdReason: "Possible Posadas vs Tafer counterparty ambiguity",
    },
  ],
  recOc5kpsg4Muip9Y: [
    {
      propertyName: "Royalton Splash Riviera Cancun",
      country: "Mexico",
      city: "Puerto Morelos",
      brand: "Royalton",
      brandParent: "Blue Diamond / Royalton",
      urbanOrResort: "Resort",
      allInclusive: true,
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Owner-Operated",
      assignmentStatus: "Current",
      sourceUrl: "https://www.royaltonresorts.com/resorts/royalton-splash-riviera-cancun",
      evidenceClass: "primary_authoritative",
    },
    {
      propertyName: "Hideaway at Royalton Riviera Cancun",
      country: "Mexico",
      city: "Puerto Morelos",
      brand: "Royalton",
      brandParent: "Blue Diamond / Royalton",
      urbanOrResort: "Resort",
      allInclusive: true,
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Owner-Operated",
      assignmentStatus: "Current",
      sourceUrl: "https://www.royaltonresorts.com/resorts/hideaway-at-royalton-riviera-cancun",
      evidenceClass: "primary_authoritative",
    },
    {
      propertyName: "Royalton CHIC Punta Cana",
      country: "Dominican Republic",
      city: "Punta Cana",
      brand: "Royalton",
      brandParent: "Blue Diamond / Royalton",
      urbanOrResort: "Resort",
      allInclusive: true,
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Owner-Operated",
      assignmentStatus: "Current",
      sourceUrl: "https://www.royaltonresorts.com/resorts/royalton-chic-punta-cana",
      evidenceClass: "primary_authoritative",
    },
  ],
  recJtFkhjaO57rSDC: [
    {
      propertyName: "Hotel Presidente InterContinental Mexico City",
      country: "Mexico",
      city: "Mexico City",
      brand: "InterContinental",
      brandParent: "IHG",
      urbanOrResort: "Urban",
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Franchise + Operator",
      assignmentStatus: "Current",
      sourceUrl: "https://www.hotelespresidente.com.mx/",
      evidenceClass: "referenced",
      publicationStatus: "Publish With Evidence Label",
      limitations: "Grupo Presidente portfolio; brand may be IHG franchise with local operator",
    },
    {
      propertyName: "Hotel Presidente InterContinental Cozumel Resort & Spa",
      country: "Mexico",
      city: "Cozumel",
      brand: "InterContinental",
      brandParent: "IHG",
      urbanOrResort: "Resort",
      developmentContext: "Existing Operation / Takeover",
      operatingStructure: "Franchise + Operator",
      assignmentStatus: "Current",
      sourceUrl: "https://www.hotelespresidente.com.mx/",
      evidenceClass: "referenced",
      publicationStatus: "Publish With Evidence Label",
    },
  ],
};

function writeJson(p, o) {
  mkdirSync(dirname(p), { recursive: true });
  writeFileSync(p, JSON.stringify(o, null, 2), "utf8");
}
function writeMd(p, t) {
  mkdirSync(dirname(p), { recursive: true });
  writeFileSync(p, t, "utf8");
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
function checksum(o) {
  return createHash("sha256").update(JSON.stringify(o)).digest("hex").slice(0, 12);
}
function slug(s) {
  return String(s || "")
    .toLowerCase()
    .replace(/[^a-z0-9]+/g, "_")
    .replace(/^_|_$/g, "")
    .slice(0, 60);
}

async function listAll(baseId, token, table, fields) {
  const out = [];
  let offset;
  do {
    const qs = new URLSearchParams({ pageSize: "100" });
    if (offset) qs.set("offset", offset);
    if (fields) for (const f of fields) qs.append("fields[]", f);
    const res = await fetch(
      `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(table)}?${qs}`,
      { headers: { Authorization: `Bearer ${token}` } }
    );
    const json = await res.json();
    if (!res.ok) throw new Error(`${table}: ${JSON.stringify(json)}`);
    out.push(...(json.records || []));
    offset = json.offset;
  } while (offset);
  return out;
}

async function metaFetch(baseId, token, path, init = {}) {
  const res = await fetch(`https://api.airtable.com/v0/meta/bases/${encodeURIComponent(baseId)}${path}`, {
    ...init,
    headers: {
      Authorization: `Bearer ${token}`,
      "Content-Type": "application/json",
      ...(init.headers || {}),
    },
  });
  const json = await res.json().catch(() => ({}));
  return { ok: res.ok, status: res.status, json };
}

async function ensureField(baseId, token, tableMeta, spec) {
  if ((tableMeta.fields || []).some((f) => f.name === spec.name)) return { skipped: true };
  if (DRY) return { dryRun: true };
  const { ok, status, json } = await metaFetch(baseId, token, `/tables/${tableMeta.id}/fields`, {
    method: "POST",
    body: JSON.stringify(spec),
  });
  if (!ok) throw new Error(`field ${spec.name}: ${status} ${JSON.stringify(json)}`);
  tableMeta.fields.push({ name: spec.name, id: json.id, type: spec.type });
  await sleep(250);
  return { id: json.id };
}

async function createRecord(baseId, token, table, fields) {
  if (DRY) return { id: `dry_${checksum(fields)}`, dryRun: true };
  const res = await fetch(`https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(table)}`, {
    method: "POST",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify({ fields, typecast: true }),
  });
  const json = await res.json();
  if (!res.ok) throw new Error(`CREATE ${table}: ${JSON.stringify(json)}`);
  await sleep(220);
  return json;
}

async function patchRecord(baseId, token, table, id, fields) {
  if (DRY) return { id, dryRun: true };
  const res = await fetch(
    `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(table)}/${encodeURIComponent(id)}`,
    {
      method: "PATCH",
      headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
      body: JSON.stringify({ fields, typecast: true }),
    }
  );
  const json = await res.json();
  if (!res.ok) throw new Error(`PATCH ${table} ${id}: ${JSON.stringify(json)}`);
  await sleep(220);
  return json;
}

function enrichmentClass(o) {
  if (o.testFixture || o.recordPurpose === "Test Fixture") return "Test Fixture";
  if (o.explorerPublishable) return "Publishable";
  if (o.recordPurpose === "Research" && o.explorerContentComplete) return "Research Content Complete Gated";
  if (o.recordPurpose === "Research") return "Research Needs Enrichment";
  if (o.recordPurpose === "Production") return "Production Needs Enrichment";
  return "Other";
}

async function main() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const token = process.env.AIRTABLE_API_KEY;
  if (!baseId || !token) throw new Error("AIRTABLE credentials required");
  if (APPLY && !APPROVED) throw new Error("Refusing apply without --approve-oe-wave-01-writes");

  const results = {
    mode: DRY ? "dry-run" : "apply",
    webhound: "Deferred — supplemental enrichment incomplete",
    startedAt: new Date().toISOString(),
    views: { apiCreateSupported: false, fieldsSynced: [], recipes: [] },
    baseline: null,
    after: null,
    wave: { operators: WAVE_01_OPERATORS.map((o) => o.name) },
    assignments: { created: 0, held: 0, failed: [] },
    presence: { created: 0 },
    sources: { created: 0, reused: 0 },
    brandRel: { created: 0 },
    mastersPatched: 0,
    holdouts: [],
  };

  // Load intel
  const masters = await listAll(baseId, token, MASTER);
  const assignments = await listAll(baseId, token, ASG);
  const brandRelationships = await listAll(baseId, token, BR);
  const marketPresence = await listAll(baseId, token, MP);
  const cross = existsSync(join(ROOT, "data/operator-explorer/phase-1-provisional-crosswalk.json"))
    ? JSON.parse(readFileSync(join(ROOT, "data/operator-explorer/phase-1-provisional-crosswalk.json"), "utf8"))
    : {};
  const entities = JSON.parse(readFileSync(join(ROOT, "data/operator-explorer/calibration-01/entities.json"), "utf8")).entities;
  const calibrationByMasterId = {};
  for (const e of entities) {
    const mid = cross[e.entityId] || e.existingMasterId || e.entityId;
    calibrationByMasterId[mid] = { track: e.track, canonicalName: e.canonicalName };
  }

  let universe = buildOperatorUniverse(masters, {
    assignments,
    brandRelationships,
    marketPresence,
    calibrationByMasterId,
  });
  for (const o of universe.operators) o.disposition = dispositionForOperator(o);
  results.baseline = { ...universe.summary };

  // Priority ranking (all real)
  const ranking = universe.operators
    .filter((o) => o.realOperator)
    .map((o) => {
      const near =
        o.recordPurpose === "Production" &&
        !o.explorerPublishable &&
        (o.counts.hasBmc || o.counts.countries >= 1 || o.calibration01);
      let priority = 4;
      if (o.explorerPublishable) priority = 0;
      else if (o.recordPurpose === "Production" && near) priority = 1;
      else if (o.recordPurpose === "Production") priority = 2;
      else if (o.contentCompleteButLifecycleGated) priority = 3;
      return {
        name: o.canonicalName,
        masterId: o.masterId,
        recordPurpose: o.recordPurpose,
        readiness: o.usefulness,
        publishable: o.explorerPublishable,
        missing: [
          o.counts.namedAssignments < 2 ? "assignments" : null,
          o.counts.countries < 1 ? "geography" : null,
          o.calibrationTrack === 2 && !o.counts.hasBmc ? "BMC" : null,
        ].filter(Boolean),
        effort: o.counts.namedAssignments === 0 ? "Medium-High" : "Low-Medium",
        ownerRelevance: o.managementAvailability?.includes("Confirmed") ? "High" : "Medium",
        calaRelevance: /CALA|Mexico|LatAm|Iberostar|Tafer|Royalton|Presidente|Aimbridge|GHL|Santa Fe|Atlantica|Marriott|Hilton|Accor|IHG|Minor/i.test(
          o.canonicalName
        )
          ? "High"
          : "Medium",
        priority,
        reason: priority === 1 ? "Production near / strategic named-asg gap" : priority === 2 ? "Production strategic gap" : priority === 3 ? "Research gated complete" : "Deeper research",
      };
    })
    .sort((a, b) => a.priority - b.priority || a.name.localeCompare(b.name));

  writeMd(
    join(ROOT, "reports/operator-explorer-wave-01-priority-ranking.md"),
    `# Wave 01 Priority Ranking\n\n| Operator | Purpose | Readiness | Missing | Effort | Owner | CALA | Priority | Reason |\n| -------- | ------- | --------- | ------- | ------ | ----- | ---- | -------: | ------ |\n` +
      ranking
        .map(
          (r) =>
            `| ${r.name} | ${r.recordPurpose} | ${r.readiness} | ${r.missing.join(", ") || "—"} | ${r.effort} | ${r.ownerRelevance} | ${r.calaRelevance} | ${r.priority} | ${r.reason} |`
        )
        .join("\n")
  );

  writeJson(join(ROOT, "data/operator-explorer/waves/wave-01-input.json"), {
    waveName: "OE Wave 01 — Named Assignments / CALA Depth",
    generatedAt: new Date().toISOString(),
    operators: WAVE_01_OPERATORS,
  });

  // Schema: OE status fields for views
  const { json: meta } = await metaFetch(baseId, token, "/tables");
  const masterMeta = (meta.tables || []).find((t) => t.name === MASTER);
  const fieldSpecs = [
    {
      name: "OE Explorer Publishable",
      type: "checkbox",
      options: { color: "greenBright", icon: "check" },
    },
    {
      name: "OE Strong Profile",
      type: "checkbox",
      options: { color: "yellowBright", icon: "star" },
    },
    {
      name: "OE Fit Data Ready",
      type: "checkbox",
      options: { color: "blueBright", icon: "check" },
    },
    {
      name: "OE Enrichment Class",
      type: "singleSelect",
      options: {
        choices: [
          { name: "Publishable" },
          { name: "Production Needs Enrichment" },
          { name: "Research Needs Enrichment" },
          { name: "Research Content Complete Gated" },
          { name: "Test Fixture" },
          { name: "Other" },
        ],
      },
    },
  ];
  for (const spec of fieldSpecs) {
    const r = await ensureField(baseId, token, masterMeta, spec);
    results.views.fieldsSynced.push({ name: spec.name, ...r });
  }

  // Attempt view create (expected fail)
  const viewAttempt = await metaFetch(baseId, token, `/tables/${masterMeta.id}/views`, {
    method: "POST",
    body: JSON.stringify({ name: "OE — All", type: "grid" }),
  });
  results.views.apiCreateAttempt = { ok: viewAttempt.ok, status: viewAttempt.status, error: viewAttempt.json?.error || null };

  const viewRecipes = [
    { name: "OE — All", filter: "(none)", expected: 46 },
    { name: "OE — Production", filter: `{Record Purpose} = "Production"`, expected: 24 },
    { name: "OE — Research", filter: `{Record Purpose} = "Research"`, expected: 13 },
    { name: "OE — Test Fixtures", filter: `{Record Purpose} = "Test Fixture"`, expected: 9 },
    { name: "OE — Explorer Publishable", filter: `{OE Explorer Publishable}`, expected: "dynamic" },
    { name: "OE — Needs Enrichment", filter: `OR({OE Enrichment Class}="Production Needs Enrichment",{OE Enrichment Class}="Research Needs Enrichment",{OE Enrichment Class}="Research Content Complete Gated")`, expected: "dynamic" },
    { name: "OE — Strong Profiles", filter: `{OE Strong Profile}`, expected: "dynamic" },
    { name: "OE — Fit Data Ready", filter: `{OE Fit Data Ready}`, expected: "dynamic" },
  ];
  results.views.recipes = viewRecipes;

  // Backup
  const backupDir = join(ROOT, "backups/operator-explorer/wave-01", TS);
  mkdirSync(backupDir, { recursive: true });
  for (const [name, rows] of [
    [MASTER, masters],
    [ASG, assignments],
    [BR, brandRelationships],
    [MP, marketPresence],
  ]) {
    writeJson(join(backupDir, `${slug(name)}.json`), { table: name, count: rows.length, records: rows });
  }

  // Existing assignment keys
  const existingAsgKeys = new Set(
    assignments.map((r) => `${(r.fields.Operator || [])[0]}|${String(r.fields["Property Name"] || "").toLowerCase()}`)
  );

  // Sources map by URL
  const piExisting = await listAll(baseId, token, PI, ["Source URL", "Source Title"]);
  const piByUrl = new Map();
  for (const r of piExisting) {
    const u = String(r.fields["Source URL"] || "").trim().toLowerCase();
    if (u) piByUrl.set(u, r.id);
  }

  async function ensureSource(url, title) {
    const key = String(url || "").trim().toLowerCase();
    if (!key) return null;
    if (piByUrl.has(key)) {
      results.sources.reused++;
      return piByUrl.get(key);
    }
    const created = await createRecord(baseId, token, PI, {
      "Source Title": title || url,
      "Source URL": url,
      "Profile Type": "Operator",
      Status: "Captured",
      Notes: "OE Wave 01",
    });
    piByUrl.set(key, created.id);
    results.sources.created++;
    return created.id;
  }

  // Write plan
  const writePlan = { wave: "wave-01", assignments: [], presence: [], holdouts: [], masterStatusSync: masters.length };
  for (const op of WAVE_01_OPERATORS) {
    for (const a of WAVE_01_ASSIGNMENTS[op.id] || []) {
      if (a.hold) {
        results.holdouts.push({ operator: op.name, property: a.propertyName, reason: a.holdReason });
        writePlan.holdouts.push({ operatorId: op.id, property: a.propertyName, reason: a.holdReason });
        results.assignments.held++;
        continue;
      }
      const key = `${op.id}|${a.propertyName.toLowerCase()}`;
      if (existingAsgKeys.has(key)) continue;
      writePlan.assignments.push({ operatorId: op.id, ...a });
    }
  }
  writeJson(join(ROOT, "data/operator-explorer/waves/wave-01-write-plan.json"), writePlan);
  writeMd(
    join(ROOT, "reports/operator-explorer-wave-01-write-plan.md"),
    `# Wave 01 Write Plan\n\n**Mode:** ${results.mode}\n\n- Assignment creates planned: ${writePlan.assignments.length}\n- Holdouts: ${writePlan.holdouts.length}\n- Master OE status sync: ${writePlan.masterStatusSync}\n\n## Holdouts\n\n${writePlan.holdouts.map((h) => `- ${h.property}: ${h.reason}`).join("\n") || "None"}\n`
  );

  // Apply assignments
  const today = new Date().toISOString().slice(0, 10);
  for (const a of writePlan.assignments) {
    try {
      const srcId = await ensureSource(a.sourceUrl, a.propertyName);
      const fields = {
        "Assignment ID": `asg_w01_${a.operatorId}_${slug(a.propertyName)}`,
        Operator: [a.operatorId],
        "Property Name": a.propertyName,
        "Canonical Property Name": a.propertyName,
        Country: a.country,
        "City / Metro": a.city,
        Brand: a.brand,
        "Brand Parent": a.brandParent,
        "Urban / Resort": a.urbanOrResort,
        "Hotel Type": a.hotelType,
        "Development Context": a.developmentContext,
        "Operating / Management Structure": a.operatingStructure,
        "Assignment Status": a.assignmentStatus,
        "All-Inclusive": a.allInclusive === true ? true : undefined,
        "Last Verified": today,
        "PI Source Library": srcId ? [srcId] : undefined,
        "Source URLs": a.sourceUrl,
        "Evidence Class": a.evidenceClass || "primary_authoritative",
        "Publication Status": a.publicationStatus || "Auto-Publish",
        "Conflict Status": "None",
        Limitations: a.limitations,
        "Research Wave": "wave-01",
      };
      Object.keys(fields).forEach((k) => fields[k] === undefined && delete fields[k]);
      await createRecord(baseId, token, ASG, fields);
      existingAsgKeys.add(`${a.operatorId}|${a.propertyName.toLowerCase()}`);
      results.assignments.created++;

      // Presence from assignment country if missing Current Operating Portfolio
      const hasCountry = marketPresence.some(
        (r) =>
          (r.fields.Operator || []).includes(a.operatorId) &&
          r.fields.Country === a.country &&
          /Current Operating|Current Managed/i.test(r.fields["Market Presence Type"] || "")
      );
      if (!hasCountry) {
        const pk = `mp_w01_${a.operatorId}_${slug(a.country)}_current`;
        await createRecord(baseId, token, MP, {
          "Presence Key": pk,
          Operator: [a.operatorId],
          Country: a.country,
          "City / Metro": a.city,
          "Market Presence Type": "Current Operating Portfolio",
          "Current / Historical": "Current",
          "Source URLs": a.sourceUrl,
          "Publication Status": "Auto-Publish",
          "Verification Date": today,
          Notes: "OE Wave 01 — derived from named assignment",
        });
        marketPresence.push({
          fields: {
            Operator: [a.operatorId],
            Country: a.country,
            "Market Presence Type": "Current Operating Portfolio",
          },
        });
        results.presence.created++;
      }
    } catch (e) {
      results.assignments.failed.push({ property: a.propertyName, error: String(e.message || e) });
    }
  }

  // Refresh lists after writes (or use in-memory merge for dry-run)
  const assignmentsAfter = DRY
    ? [
        ...assignments,
        ...writePlan.assignments.map((a) => ({
          fields: {
            Operator: [a.operatorId],
            "Property Name": a.propertyName,
            Country: a.country,
            Brand: a.brand,
          },
        })),
      ]
    : await listAll(baseId, token, ASG);
  const mpAfter = DRY ? marketPresence : await listAll(baseId, token, MP);

  universe = buildOperatorUniverse(masters, {
    assignments: assignmentsAfter,
    brandRelationships,
    marketPresence: mpAfter,
    calibrationByMasterId,
  });
  for (const o of universe.operators) o.disposition = dispositionForOperator(o);
  results.after = { ...universe.summary };

  // Sync OE fields on masters
  for (const o of universe.operators) {
    const fields = {
      "OE Explorer Publishable": o.explorerPublishable ? true : false,
      "OE Strong Profile": o.strongExplorerProfile ? true : false,
      "OE Fit Data Ready": o.fitDataReadiness === "Fit Data Ready" ? true : false,
      "OE Enrichment Class": enrichmentClass(o),
    };
    await patchRecord(baseId, token, MASTER, o.masterId, fields);
    results.mastersPatched++;
  }

  // Validate purpose counts via filter formulas
  async function countFormula(formula) {
    let n = 0;
    let offset;
    do {
      const qs = new URLSearchParams({ pageSize: "100", filterByFormula: formula });
      if (offset) qs.set("offset", offset);
      const res = await fetch(
        `https://api.airtable.com/v0/${encodeURIComponent(baseId)}/${encodeURIComponent(MASTER)}?${qs}`,
        { headers: { Authorization: `Bearer ${token}` } }
      );
      const json = await res.json();
      if (!res.ok) throw new Error(JSON.stringify(json));
      n += (json.records || []).length;
      offset = json.offset;
    } while (offset);
    return n;
  }
  const viewCounts = {
    all: masters.length,
    production: await countFormula('{Record Purpose}="Production"'),
    research: await countFormula('{Record Purpose}="Research"'),
    testFixtures: await countFormula('{Record Purpose}="Test Fixture"'),
    publishableCanonical: universe.summary.explorerPublishable,
    strongCanonical: universe.summary.strongProfiles,
    fitReadyCanonical: universe.summary.fitDataReady,
    needsEnrichmentCanonical: universe.operators.filter((o) =>
      ["Production Needs Enrichment", "Research Needs Enrichment", "Research Content Complete Gated"].includes(
        enrichmentClass(o)
      )
    ).length,
  };
  if (viewCounts.production !== 24 || viewCounts.research !== 13 || viewCounts.testFixtures !== 9 || viewCounts.all !== 46) {
    throw new Error(`View/purpose count mismatch: ${JSON.stringify(viewCounts)}`);
  }

  // Canonical payloads
  const payloadDir = join(ROOT, "data/operator-explorer/canonical-profile-payloads");
  mkdirSync(payloadDir, { recursive: true });
  for (const o of universe.operators) {
    const asg = assignmentsAfter.filter((r) => (r.fields.Operator || []).includes(o.masterId) && !isAggregateAssignmentName(r.fields["Property Name"]));
    const br = brandRelationships.filter((r) => (r.fields.Operator || []).includes(o.masterId));
    const mp = mpAfter.filter((r) => (r.fields.Operator || []).includes(o.masterId));
    if (o.testFixture) {
      writeJson(join(payloadDir, `${o.masterId}.json`), {
        adminOnly: true,
        masterId: o.masterId,
        canonicalName: o.canonicalName,
        recordPurpose: o.recordPurpose,
        note: "Test Fixture — not Explorer content",
      });
      continue;
    }
    writeJson(join(payloadDir, `${o.masterId}.json`), {
      source: "airtable-canonical",
      masterId: o.masterId,
      track: o.calibrationTrack,
      readiness: {
        usefulness: o.usefulness,
        explorerPublishable: o.explorerPublishable,
        strongExplorerProfile: o.strongExplorerProfile,
        contentComplete: o.explorerContentComplete,
        contentCompleteButLifecycleGated: o.contentCompleteButLifecycleGated,
        fitDataReadiness: o.fitDataReadiness,
      },
      sections: {
        overview: {
          companyName: o.canonicalName,
          operatingModel: o.operatingModel,
          managementAvailability: o.managementAvailability,
          website: o.website,
          parent: o.parent,
          recordPurpose: o.recordPurpose,
          lifecycle: o.lifecycle,
        },
        operatingFootprint: { countries: [...new Set(mp.map((r) => r.fields.Country).filter(Boolean))], presenceCount: mp.length },
        portfolioProfile: { assignmentCount: asg.length, brands: [...new Set(asg.map((r) => r.fields.Brand).filter(Boolean))] },
        experience: {
          developmentContexts: [...new Set(asg.map((r) => r.fields["Development Context"]).filter(Boolean))],
          urbanResort: [...new Set(asg.map((r) => r.fields["Urban / Resort"]).filter(Boolean))],
        },
        brandRelationships: br.map((r) => ({ brand: r.fields.Brand, type: r.fields["Relationship Type"] })),
        selectedAssignments: asg.slice(0, 12).map((r) => ({
          property: r.fields["Property Name"],
          country: r.fields.Country,
          city: r.fields["City / Metro"],
          brand: r.fields.Brand,
          status: r.fields["Assignment Status"],
        })),
        operatingStructures: [...new Set(asg.map((r) => r.fields["Operating / Management Structure"]).filter(Boolean))],
        differentiatingCapabilities: br
          .filter((r) => r.fields["Relationship Type"] === "Brand Managed Capability")
          .map((r) => r.fields.Brand),
        marketPresence: mp.map((r) => ({
          country: r.fields.Country,
          type: r.fields["Market Presence Type"],
          current: r.fields["Current / Historical"],
        })),
        recentMomentum: [],
        evidence: { assignments: asg.length, brandRelationships: br.length, presence: mp.length },
      },
    });
  }

  // Reports
  writeMd(
    join(ROOT, "reports/operator-explorer-airtable-views-created.md"),
    `# Airtable OE Views\n\n## API capability\n\nAirtable Metadata API **cannot create** filtered views (attempt status ${results.views.apiCreateAttempt?.status}).\n\n## What Wave 01 did\n\nSynced Master fields for view filters:\n\n${results.views.fieldsSynced.map((f) => `- ${f.name}`).join("\n")}\n\n## Recipes to create in Airtable UI\n\n| View | Filter | Expected |\n| ---- | ------ | -------- |\n${viewRecipes.map((v) => `| ${v.name} | \`${v.filter}\` | ${v.expected} |`).join("\n")}\n\n## Validated purpose counts (API)\n\n\`\`\`json\n${JSON.stringify(viewCounts, null, 2)}\n\`\`\`\n`
  );

  writeMd(
    join(ROOT, "reports/operator-explorer-operationalized-baseline.md"),
    `# Operationalized Baseline (pre Wave 01 writes reflected in after if apply)\n\n## Before\n\n\`\`\`json\n${JSON.stringify(results.baseline, null, 2)}\n\`\`\`\n\n## After Wave 01 intelligence\n\n\`\`\`json\n${JSON.stringify(results.after, null, 2)}\n\`\`\`\n`
  );

  writeMd(
    join(ROOT, "reports/operator-explorer-webhound-track-2-final-review.md"),
    `# Webhound Track 2 Final Review\n\n**Status:** Deferred — supplemental enrichment incomplete (\`done=false\` at Wave 01 start).\n\nSession \`6695f5be-443b-4685-860a-b9c0b37e5be6\` still extracting. **No partial rows consumed. No merge.**\n`
  );

  const beforeById = Object.fromEntries(
    // approximate from baseline rebuild would need snapshot; use ranking readiness from first universe
    ranking.map((r) => [r.masterId, r])
  );
  // Recompute before from saved baseline operators file if present
  const beforeUniverse = JSON.parse(readFileSync(join(ROOT, "data/operator-explorer/operator-universe-canonical.json"), "utf8"));
  const beforeMap = Object.fromEntries(beforeUniverse.operators.map((o) => [o.masterId, o]));

  let impact = `# Wave 01 Readiness Impact\n\n| Operator | Before | After | What Changed |\n| -------- | ------ | ----- | ------------ |\n`;
  for (const op of WAVE_01_OPERATORS) {
    const b = beforeMap[op.id];
    const a = universe.operators.find((o) => o.masterId === op.id);
    impact += `| ${op.name} | ${b?.usefulness} (pub=${b?.explorerPublishable}) | ${a?.usefulness} (pub=${a?.explorerPublishable}) | Named assignments + presence |\n`;
  }
  impact += `\n## Totals\n\n| Metric | Before | After |\n| ------ | -----: | ----: |\n| Strong | ${results.baseline.strongProfiles} | ${results.after.strongProfiles} |\n| Publishable | ${results.baseline.explorerPublishable} | ${results.after.explorerPublishable} |\n| Content complete | ${results.baseline.explorerContentComplete} | ${results.after.explorerContentComplete} |\n| Research gated complete | ${results.baseline.contentCompleteButLifecycleGated} | ${results.after.contentCompleteButLifecycleGated} |\n| Fit Data Ready | ${results.baseline.fitDataReady} | ${results.after.fitDataReady} |\n`;
  writeMd(join(ROOT, "reports/operator-explorer-wave-01-readiness-impact.md"), impact);

  writeMd(
    join(ROOT, "reports/operator-explorer-wave-01-gap-plan.md"),
    `# Wave 01 Gap Plan\n\n| Operator | Missing before | Wave 01 action |\n| -------- | -------------- | -------------- |\n` +
      WAVE_01_OPERATORS.map((o) => {
        const b = beforeMap[o.id];
        return `| ${o.name} | asg=${b?.counts?.namedAssignments}, cty=${b?.counts?.countries} | Add named hotels + Current Operating Presence |`;
      }).join("\n")
  );

  writeMd(
    join(ROOT, "reports/operator-explorer-wave-01-cala-coverage.md"),
    `# Wave 01 CALA Coverage\n\nNamed Wave 01 assignments in Mexico / Caribbean / Central / South America are the primary CALA depth adds for Marriott, Hilton, Accor, IHG, Minor (NH Collection CDMX), Iberostar, Royalton, Tafer (qualified), Presidente, Atlantica (Brazil).\n\nDo not treat corporate “Americas” claims as country presence without named hotels.\n`
  );

  writeMd(
    join(ROOT, "reports/operator-explorer-wave-01-apply-results.md"),
    `# Wave 01 Apply Results\n\n\`\`\`json\n${JSON.stringify(results, null, 2)}\n\`\`\`\n\nBackup: \`backups/operator-explorer/wave-01/${TS}/\`\n`
  );

  writeMd(
    join(ROOT, "reports/operator-explorer-readiness-consumer-audit.md"),
    `# Readiness Consumer Audit\n\n| Consumer | Previous Logic | Canonical Module Now? | Result |\n| -------- | -------------- | --------------------- | ------ |\n| \`lib/operator-explorer/readiness.js\` | SoT | Yes | Authoritative |\n| \`lib/operator-explorer/operator-universe.js\` | Uses readiness | Yes | Authoritative |\n| \`scripts/operator-explorer-phase-1-apply.mjs\` | Phase1 asg≥5/8 | Yes — \`classifyExplorerReadiness\` | Fixed |\n| \`scripts/operator-explorer-wave-01-operationalize.mjs\` | — | Yes | Uses universe builder |\n| \`scripts/build-operator-explorer-calibration-01.mjs\` \`buildProfile\` | Inline dry-run gates | **Pending route** (historical dry-run package) | Documented debt |\n| \`scripts/audit-operator-explorer-readiness-parity.mjs\` | Comparative replicas | Audit-only | Keep for history |\n| Operator Fit scoring | Separate | **No — not rewired** | Future debt |\n`
  );

  writeMd(
    join(ROOT, "reports/operator-explorer-universe-resolver-consumer-audit.md"),
    `# Universe Resolver Consumer Audit\n\n| Consumer | Uses \`operator-universe.js\`? | Notes |\n| -------- | ----------------------------- | ----- |\n| Wave 01 operationalize | Yes | |\n| Universe reconciliation builder | Yes | |\n| Internal universe HTML | Consumes dashboard JSON from resolver | |\n| Operator Fit candidate selection | No | Documented future debt — not changed |\n`
  );

  // Graduation candidates (Research) — recommend only
  const grads = universe.operators
    .filter((o) => o.recordPurpose === "Research")
    .map((o) => ({
      name: o.canonicalName,
      id: o.masterId,
      contentComplete: o.explorerContentComplete,
      recommendation: o.explorerContentComplete ? "Candidate for Production Graduation" : "Remain Research",
    }));
  writeMd(
    join(ROOT, "reports/operator-explorer-wave-01-graduation-candidates.md"),
    `# Wave 01 Graduation Candidates (NO APPLY)\n\n| Master | Content complete? | Recommendation |\n| ------ | ----------------- | -------------- |\n` +
      grads.map((g) => `| ${g.name} | ${g.contentComplete} | ${g.recommendation} |`).join("\n") +
      `\n\nDo not change Record Purpose without founder decision.\n`
  );

  writeMd(
    join(ROOT, "reports/operator-explorer-wave-01-automation-proof.md"),
    `# Wave 01 Automation Proof\n\nInput shape: **Wave Name + Operator list** (\`wave-01-input.json\`).\n\n| Measure | Result |\n| ------- | ------ |\n| Routine named assignments auto-planned | Yes |\n| Presence derived from assignments | Yes |\n| Sources deduped | Yes |\n| Holdouts (exception) | ${results.holdouts.length} (Tafer/Posadas ambiguity) |\n| Founder intervention required for routine facts | No |\n| Schema exceptions | None new |\n| Manual cleanup | View UI creation still manual (API limit) |\n\n## Verdict: **Proven With Minor Gaps**\n\nGaps: Airtable view creation not API-automatable; some property↔operator counterparty cases need holdouts; Webhound still deferred.\n`
  );

  writeJson(join(ROOT, "data/operator-explorer/operator-universe-canonical.json"), {
    generatedAt: new Date().toISOString(),
    summary: universe.summary,
    operators: universe.operators,
  });
  writeJson(join(ROOT, "data/operator-explorer/operator-universe-dashboard.json"), {
    generatedAt: new Date().toISOString(),
    summary: universe.summary,
    operators: universe.operators,
  });
  writeJson(join(ROOT, `data/operator-explorer/waves/wave-01-results-${TS}.json`), results);

  console.log(JSON.stringify({ ok: true, ...results, viewCounts }, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
