#!/usr/bin/env node
/**
 * Track C Wave 3 — accelerated census scale (C-U + C-R).
 * Evaluate ≥500 US identities; CREATE only CREATE_READY + collision-clear.
 * maxHpcCreateBatch raised to 100 under Wave 3 standing auth.
 * CUSTOMER_BLOCKED. Free research only.
 *
 * --apply / --apply-hpc-create
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { execSync } from "node:child_process";
import dotenv from "dotenv";
import {
  GRAPH_BASE_ID_CANONICAL,
  HPC_TABLE_ID,
  TABLE_HI_HOTEL_DISCOVERY_INVENTORY,
  MAP_DISCOVERY_INVENTORY,
} from "../lib/hotel-intelligence/sec-edgar/discovery-inventory-airtable-contract-v23.js";
import {
  buildDiscoveryInventoryFields,
  classifyWriteCategory,
  WRITE_CATEGORY,
} from "../lib/hotel-intelligence/sec-edgar/hif-foundation-persistence-v1.js";
import {
  TRACK_C_STANDING_AUTH,
  MATCH_CLASS,
  PRIORITY_COUNTRY_LABELS,
  TRACK_C_STREAM,
  classifySecToHpcMatch,
  routeHandoff,
  buildTrackCDiscoveryHotel,
  dedupeDiscoveries,
  summarizeMatchClasses,
  summarizeCreateReadyAttrition,
  classifyCreateReadyAttrition,
  inferBrandFamilyFromLabel,
  inferFamilySelect,
  AIMBRIDGE_PUBLIC_US_PORTFOLIO_SEED,
  AIMBRIDGE_PUBLIC_UK_PORTFOLIO_SEED,
  buildCalaPreservationCheck,
} from "../lib/hotel-intelligence/sec-edgar/hif-track-c-universe-expansion-v1.js";
import {
  resolveAuthenticatedFounderIdentity,
  persistHpcCreateAuthorization,
  executeAuthorizedHpcCreate,
} from "../lib/hotel-census/hpc-authorized-create-reuse-v1.js";
import { performLiveHpcIdentityCollisionCheck } from "../lib/hotel-census/hpc-live-identity-collision-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) {
    dotenv.config({ path: p, override: false });
    break;
  }
}

const APPLY = process.argv.includes("--apply");
const APPLY_HPC_CREATE = process.argv.includes("--apply-hpc-create");
const OUT = path.join(ROOT, "reports/sec-edgar-hif-track-c-universe-expansion-wave3");
fs.mkdirSync(OUT, { recursive: true });
const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
if (!token) {
  console.error("NO_AIRTABLE_TOKEN");
  process.exit(2);
}
const ids = JSON.parse(
  fs.readFileSync(path.join(ROOT, "lib/hotel-intelligence/sec-edgar/discovery-inventory-table-ids.generated.json"), "utf8"),
);
const inventoryTableId = ids.tableIds[TABLE_HI_HOTEL_DISCOVERY_INVENTORY];
const ENRICH = JSON.parse(
  fs.readFileSync(path.join(ROOT, "fixtures/hif-track-c/wave3-official-enrichment-v1.json"), "utf8"),
);

function writeJson(name, obj) {
  fs.writeFileSync(path.join(OUT, name), `${JSON.stringify(obj, null, 2)}\n`);
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
async function airtableGet(url) {
  const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
  const json = await res.json();
  if (!res.ok) throw new Error(JSON.stringify(json));
  return json;
}
async function airtablePost(tableId, fields) {
  const res = await fetch(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${tableId}`, {
    method: "POST",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify({ fields, typecast: true }),
  });
  return { ok: res.ok, json: await res.json() };
}
async function airtablePatch(tableId, recordId, fields) {
  const res = await fetch(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${tableId}/${recordId}`, {
    method: "PATCH",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify({ fields, typecast: true }),
  });
  return { ok: res.ok, json: await res.json() };
}
async function findByFormula(tableId, formula) {
  const qs = new URLSearchParams({ filterByFormula: formula, maxRecords: "1" });
  const json = await airtableGet(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${tableId}?${qs}`);
  return json.records?.[0] || null;
}
async function countCountry(country) {
  let offset;
  let n = 0;
  do {
    const qs = new URLSearchParams({ pageSize: "100", filterByFormula: `{Country}="${country}"` });
    qs.append("fields[]", "Country");
    if (offset) qs.set("offset", offset);
    const json = await airtableGet(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${HPC_TABLE_ID}?${qs}`);
    n += (json.records || []).length;
    offset = json.offset;
  } while (offset);
  return n;
}

async function loadUsDiscoveryInventory(limit = 550) {
  const out = [];
  let offset;
  do {
    const qs = new URLSearchParams({ pageSize: "100" });
    for (const f of [
      "Country",
      "Hotel Name (raw)",
      "City",
      "State / Region",
      "Address",
      "Current Brand",
      "HPC Match Status",
      "Discovery Property ID",
      "Source Filing URL",
    ]) {
      qs.append("fields[]", f);
    }
    if (offset) qs.set("offset", offset);
    const json = await airtableGet(
      `https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${inventoryTableId}?${qs}`,
    );
    for (const r of json.records || []) {
      const c = String(r.fields?.Country || "");
      if (!/united states|^us$/i.test(c)) continue;
      const name = r.fields["Hotel Name (raw)"] || r.fields["Property Name"];
      if (!name) continue;
      out.push({
        hotelName: name,
        city: r.fields.City || null,
        stateRegion: r.fields["State / Region"] || null,
        address: r.fields.Address || null,
        currentBrand: r.fields["Current Brand"] || null,
        sourceUrl: r.fields["Source Filing URL"] || null,
        discoveryPropertyId: r.fields["Discovery Property ID"] || null,
        hpcMatchStatus: r.fields["HPC Match Status"] || null,
        airtableId: r.id,
        stream: TRACK_C_STREAM.C_U_UNIVERSE_EXPANSION,
        discoveryMethod: "TRACK_C_WAVE3_DISCOVERY_INVENTORY_EVAL",
      });
      if (out.length >= limit) return out;
    }
    offset = json.offset;
  } while (offset);
  return out;
}

function loadCmbsUs57() {
  const j = JSON.parse(
    fs.readFileSync(path.join(ROOT, "reports/sec-edgar-hif-v6-track-a-cmbs-owner/DISTINCT_HOTELS.json"), "utf8"),
  );
  return (j.hotels || []).filter((h) => String(h.hotelCountry || "").toUpperCase() === "US");
}

function loadV4Shortlist() {
  const p = path.join(ROOT, "reports/sec-edgar-contract-timing-v4/US_PILOT_SELECTION_DRY_RUN.json");
  if (!fs.existsSync(p)) return [];
  const j = JSON.parse(fs.readFileSync(p, "utf8"));
  return (j.shortlist || []).map((h) => ({
    hotelName: h.hotelName,
    city: h.city || null,
    stateRegion: h.state || h.stateRegion || null,
    address: h.address || null,
    currentBrand: h.brand || null,
    sourceUrl: h.officialHotelBrandSource || null,
    stream: TRACK_C_STREAM.C_U_UNIVERSE_EXPANSION,
    discoveryMethod: "TRACK_C_WAVE3_V4_SHORTLIST_EVAL",
  }));
}

function loadTierBCommercial() {
  const p = path.join(ROOT, "reports/sec-edgar-commercial-inventory-reconcile-v5/TIER_B_ALL_76.json");
  if (!fs.existsSync(p)) return [];
  const j = JSON.parse(fs.readFileSync(p, "utf8"));
  const rows = [...(j.future || []), ...(j.historical || [])];
  return rows
    .filter((h) => !h.country || /united states|^us$/i.test(String(h.country)))
    .map((h) => {
      const loc = String(h.location || "");
      const parts = loc.split(",").map((s) => s.trim());
      return {
        hotelName: h.hotelName,
        city: parts[0] || null,
        stateRegion: parts[1] || null,
        address: null,
        currentBrand: h.brand || null,
        sourceUrl: h.sourceUrl || null,
        operatorName: h.managementCompany || null,
        stream: TRACK_C_STREAM.C_U_UNIVERSE_EXPANSION,
        discoveryMethod: "TRACK_C_WAVE3_TIER_B_COMMERCIAL_EVAL",
        identityClass: "PROBABLE_PROPERTY",
        notes: "CUSTOMER_BLOCKED | Track C wave3 C-U | Tier B commercial inventory — operator from SEC disclosure only",
      };
    });
}

const UK_CA_BATCH = [
  {
    hotelName: "Hilton London Bankside",
    city: "London",
    country: "United Kingdom",
    address: "2-8 Great Suffolk Street",
    currentBrand: "Hilton",
    sourceUrl: "https://www.hilton.com/en/hotels/lonhihi-hilton-london-bankside/",
  },
  {
    hotelName: "DoubleTree by Hilton London Westminster",
    city: "London",
    country: "United Kingdom",
    address: "30 Vincent Square",
    currentBrand: "DoubleTree by Hilton",
    sourceUrl: "https://www.hilton.com/en/hotels/lonsddi-doubletree-london-westminster/",
  },
  {
    hotelName: "Delta Hotels Calgary Downtown",
    city: "Calgary",
    stateRegion: "AB",
    country: "Canada",
    address: "209 4th Avenue SE",
    currentBrand: "Delta Hotels",
    sourceUrl: "https://www.marriott.com/en-us/hotels/yycdt-delta-hotels-calgary-downtown/overview/",
  },
  {
    hotelName: "Hilton Montreal Airport",
    city: "Montreal",
    stateRegion: "QC",
    country: "Canada",
    address: "12505 Cote de Liesse",
    currentBrand: "Hilton",
    sourceUrl: "https://www.hilton.com/en/hotels/yulaphh-hilton-montreal-airport/",
  },
  {
    hotelName: "Courtyard Toronto Mississauga Meadowvale",
    city: "Mississauga",
    stateRegion: "ON",
    country: "Canada",
    address: "5050 Meadowvale Village Gate",
    currentBrand: "Courtyard",
    sourceUrl: "https://www.marriott.com/en-us/hotels/yyzmv-courtyard-toronto-mississauga-meadowvale/overview/",
  },
  {
    hotelName: "Holiday Inn Express London Heathrow T5",
    city: "London",
    country: "United Kingdom",
    address: "London Road",
    currentBrand: "Holiday Inn Express",
    sourceUrl: "https://www.ihg.com/holidayinnexpress/hotels/gb/en/london/lonhx/hoteldetail",
  },
];

async function collide(hotel) {
  try {
    return await performLiveHpcIdentityCollisionCheck({
      hotelName: hotel.hotelName,
      aliases: [hotel.hotelName],
      city: hotel.city,
      brand: hotel.currentBrand || hotel.brand,
      address: hotel.address,
      country: hotel.physicalCountry || hotel.country,
      officialUrl: hotel.sourceUrl || hotel.officialUrl,
      adpPropertyId: hotel.discoveryPropertyId,
    });
  } catch (e) {
    return { outcome: "HPC_LIVE_LOOKUP_UNAVAILABLE", hpcRecordId: null, candidates: [], error: e.message };
  }
}

function evaluateGate(hotel, collision) {
  const hasExactAddress = Boolean(hotel.address && String(hotel.address).trim().length > 4);
  const hasOfficialBrandSource = Boolean(
    (hotel.sourceUrl || hotel.officialUrl) &&
      /marriott\.com|hilton\.com|ihg\.com|hyatt\.com|wyndham|choicehotels|bestwestern|accor|fairmont/i.test(
        hotel.sourceUrl || hotel.officialUrl || "",
      ),
  );
  const matchClass = classifySecToHpcMatch({
    collisionOutcome: collision?.outcome,
    identityClass: hotel.identityClass || "VERIFIED_PROPERTY",
    operatingStatus: hotel.operatingStatus,
    hasExactAddress,
    hasOfficialBrandSource,
  });
  return { matchClass, hasExactAddress, hasOfficialBrandSource };
}

function brandFamilyBucket(brand) {
  return inferBrandFamilyFromLabel(brand);
}

async function main() {
  console.log(
    JSON.stringify(
      {
        wave: "3",
        apply: APPLY,
        applyHpcCreate: APPLY_HPC_CREATE,
        maxHpcCreateBatch: TRACK_C_STANDING_AUTH.maxHpcCreateBatch,
      },
      null,
      2,
    ),
  );

  const usBefore = await countCountry(PRIORITY_COUNTRY_LABELS.US);
  const mx = await countCountry(PRIORITY_COUNTRY_LABELS.MX);
  const br = await countCountry(PRIORITY_COUNTRY_LABELS.BR);
  const uk = await countCountry(PRIORITY_COUNTRY_LABELS.UK);
  const ca = await countCountry(PRIORITY_COUNTRY_LABELS.CA);
  console.log(`[baseline] US=${usBefore} MX=${mx} BR=${br} UK=${uk} CA=${ca}`);

  const enrichmentMap = ENRICH.byHotelName || {};
  const deferred = new Set((ENRICH.deferredResearchRequired || []).map((d) => d.hotelName));

  // --- Stream C-R: SEC57 reconciliation with Wave3 enrichment ---
  const cmbs = loadCmbsUs57();
  const cRRows = [];
  const createCandidates = [];
  const inventoryHotels = [];

  console.log(`[C-R] CMBS US=${cmbs.length} enrichmentKeys=${Object.keys(enrichmentMap).length}`);
  for (const raw of cmbs) {
    const enrich = enrichmentMap[raw.hotelName] || null;
    const historicalUnresolved = deferred.has(raw.hotelName);
    const hotel = buildTrackCDiscoveryHotel({
      hotelName: raw.hotelName,
      country: "United States",
      city: enrich?.city || raw.hotelMarket || null,
      stateRegion: enrich?.stateRegion || null,
      address: enrich?.address || null,
      currentBrand: enrich?.brand || (/westin/i.test(raw.hotelName) ? "Westin" : /hampton/i.test(raw.hotelName) ? "Hampton Inn" : /homewood/i.test(raw.hotelName) ? "Homewood Suites" : /hilton garden/i.test(raw.hotelName) ? "Hilton Garden Inn" : /holiday inn/i.test(raw.hotelName) ? "Holiday Inn Express" : "Courtyard"),
      brandFamily: brandFamilyBucket(enrich?.brand || "Courtyard"),
      historicalBrand: "Courtyard",
      sourceUrl: enrich?.officialUrl || raw.sourceUrl || null,
      discoveryMethod: "TRACK_C_WAVE3_C_R_SEC57",
      identityClass: historicalUnresolved ? "HISTORICAL_PROPERTY" : "VERIFIED_PROPERTY",
      operatingStatus: "OPERATING",
      historicalIdentityUnresolved: historicalUnresolved,
      notes: `CUSTOMER_BLOCKED | Track C wave3 C-R | agreement=${raw.agreementType || ""} | enrich=${enrich ? "OFFICIAL" : "SEC_ONLY"}`,
      handoffs: routeHandoff({
        hasFranchiseOrManagementSignal: true,
        hasContractualAgreementEvidence: true,
      }),
    });

    const collision = await collide(hotel);
    let { matchClass, hasExactAddress, hasOfficialBrandSource } = evaluateGate(hotel, collision);
    if (historicalUnresolved) matchClass = MATCH_CLASS.RESEARCH_REQUIRED;
    if (collision?.hpcRecordId) {
      hotel.hpcRecordId = collision.hpcRecordId;
      hotel.hpcMatchStatus = "MATCHED";
      matchClass = MATCH_CLASS.MATCHED_EXISTING_HPC;
    }

    const attrition = classifyCreateReadyAttrition({
      ...hotel,
      matchClass,
      collisionOutcome: collision?.outcome,
      historicalIdentityUnresolved: historicalUnresolved,
    });

    cRRows.push({
      stream: TRACK_C_STREAM.C_R_TARGETED_RECONCILIATION,
      hotelName: hotel.hotelName,
      city: hotel.city,
      stateRegion: hotel.stateProvince,
      address: hotel.address,
      currentBrand: hotel.currentBrand,
      brandFamily: hotel.brandFamily,
      sourceUrl: hotel.sourceUrl,
      matchClass,
      collisionOutcome: collision?.outcome || null,
      hpcRecordId: collision?.hpcRecordId || null,
      enriched: Boolean(enrich),
      attritionReason: attrition.reason,
      attritionReasons: attrition.reasons,
    });

    inventoryHotels.push({ ...hotel, matchClass, stream: TRACK_C_STREAM.C_R_TARGETED_RECONCILIATION });

    if (matchClass === MATCH_CLASS.CREATE_READY && enrich) {
      createCandidates.push({
        discoveryPropertyId: hotel.discoveryPropertyId,
        hotelName: hotel.hotelName,
        stream: TRACK_C_STREAM.C_R_TARGETED_RECONCILIATION,
        hpcFields: {
          "Property Name": hotel.hotelName,
          "Canonical Property Name": hotel.hotelName,
          Country: PRIORITY_COUNTRY_LABELS.US,
          City: hotel.city,
          "State / Region": hotel.stateProvince,
          Address: hotel.address,
          "Current Brand": hotel.currentBrand,
          "Affiliation Status": "Branded",
          "Family / Source Family": inferFamilySelect(hotel.currentBrand),
          "Official Property URL": hotel.sourceUrl,
        },
      });
    }
    await sleep(80);
  }

  // --- Stream C-U: evaluate ≥500 US identities from inventory + shortlist + mgmt seeds ---
  const invUs = await loadUsDiscoveryInventory(650);
  const shortlist = loadV4Shortlist();
  const tierB = loadTierBCommercial();
  const aimbridge = AIMBRIDGE_PUBLIC_US_PORTFOLIO_SEED.map((h) => ({
    ...h,
    stream: TRACK_C_STREAM.C_U_UNIVERSE_EXPANSION,
    discoveryMethod: "TRACK_C_WAVE3_AIMBRIDGE_PORTFOLIO",
  }));
  const cmbsAsCu = cmbs.map((h) => ({
    hotelName: h.hotelName,
    city: h.hotelMarket || null,
    country: "United States",
    currentBrand: null,
    sourceUrl: h.sourceUrl || null,
    stream: TRACK_C_STREAM.C_U_UNIVERSE_EXPANSION,
    discoveryMethod: "TRACK_C_WAVE3_CMBS_IDENTITY_EVAL",
    identityClass: "VERIFIED_PROPERTY",
  }));

  const cUPoolRaw = dedupeDiscoveries([...invUs, ...shortlist, ...tierB, ...aimbridge, ...cmbsAsCu]);
  console.log(`[C-U] poolRaw=${cUPoolRaw.length} (target≥500) inv=${invUs.length} tierB=${tierB.length}`);

  const cUEval = [];
  const byBrandFamily = {};
  const byState = {};
  for (const raw of cUPoolRaw) {
    const hotel = buildTrackCDiscoveryHotel({
      ...raw,
      country: "United States",
      discoveryMethod: raw.discoveryMethod || "TRACK_C_WAVE3_C_U_EVAL",
      notes: `CUSTOMER_BLOCKED | Track C wave3 C-U | operator_not_inferred_from_brand`,
    });
    const family = brandFamilyBucket(hotel.currentBrand);
    byBrandFamily[family] = (byBrandFamily[family] || 0) + 1;
    const st = hotel.stateProvince || "(unknown)";
    byState[st] = (byState[st] || 0) + 1;

    const hasExactAddress = Boolean(hotel.address && String(hotel.address).trim().length > 4);
    const hasOfficial =
      hotel.sourceUrl &&
      /marriott\.com|hilton\.com|ihg\.com|hyatt\.com|wyndham|choicehotels|bestwestern|accor|fairmont/i.test(
        hotel.sourceUrl,
      );
    let matchClass = MATCH_CLASS.RESEARCH_REQUIRED;
    if (hotel.hpcMatchStatus === "MATCHED") matchClass = MATCH_CLASS.MATCHED_EXISTING_HPC;
    else if (hasExactAddress && hasOfficial) matchClass = MATCH_CLASS.CREATE_CANDIDATE;
    else if (hotel.identityClass === "VERIFIED_PROPERTY" || hotel.identityClass === "PROBABLE_PROPERTY") {
      matchClass = MATCH_CLASS.UNMATCHED_VERIFIED;
    }

    const attrition = classifyCreateReadyAttrition({ ...hotel, matchClass });
    cUEval.push({
      stream: TRACK_C_STREAM.C_U_UNIVERSE_EXPANSION,
      hotelName: hotel.hotelName,
      city: hotel.city,
      stateRegion: hotel.stateProvince,
      address: hotel.address,
      currentBrand: hotel.currentBrand,
      brandFamily: family,
      sourceUrl: hotel.sourceUrl,
      matchClass,
      attritionReason: attrition.reason,
      discoveryMethod: hotel.discoveryMethod,
      operatorName: hotel.operatorName || null,
    });
  }

  // Live collision only for C-U CREATE_CANDIDATE with address+URL (bounded batch)
  const cUCreateProbe = cUEval.filter((r) => r.matchClass === MATCH_CLASS.CREATE_CANDIDATE).slice(0, 40);
  console.log(`[C-U] collisionProbe=${cUCreateProbe.length}`);
  for (const row of cUCreateProbe) {
    const collision = await collide(row);
    const { matchClass } = evaluateGate(row, collision);
    row.collisionOutcome = collision?.outcome || null;
    row.hpcRecordId = collision?.hpcRecordId || null;
    row.matchClass = matchClass;
    if (collision?.hpcRecordId) row.matchClass = MATCH_CLASS.MATCHED_EXISTING_HPC;
    if (row.matchClass === MATCH_CLASS.CREATE_READY) {
      const already = createCandidates.some(
        (c) => c.hotelName.toLowerCase() === row.hotelName.toLowerCase() && c.hpcFields.City === row.city,
      );
      if (!already) {
        const disc = buildTrackCDiscoveryHotel({
          ...row,
          country: "United States",
          discoveryMethod: "TRACK_C_WAVE3_C_U_CREATE_READY",
        });
        inventoryHotels.push({ ...disc, matchClass: MATCH_CLASS.CREATE_READY, stream: TRACK_C_STREAM.C_U_UNIVERSE_EXPANSION });
        createCandidates.push({
          discoveryPropertyId: disc.discoveryPropertyId,
          hotelName: disc.hotelName,
          stream: TRACK_C_STREAM.C_U_UNIVERSE_EXPANSION,
          hpcFields: {
            "Property Name": disc.hotelName,
            "Canonical Property Name": disc.hotelName,
            Country: PRIORITY_COUNTRY_LABELS.US,
            City: disc.city,
            "State / Region": disc.stateProvince,
            Address: disc.address,
            "Current Brand": disc.currentBrand,
            "Affiliation Status": "Branded",
            "Family / Source Family": inferFamilySelect(disc.currentBrand),
            "Official Property URL": disc.sourceUrl,
          },
        });
      }
    }
    await sleep(80);
  }

  // --- UK / CA deepen ---
  const ukCaRows = [];
  for (const raw of [...UK_CA_BATCH, ...AIMBRIDGE_PUBLIC_UK_PORTFOLIO_SEED]) {
    const hotel = buildTrackCDiscoveryHotel({
      ...raw,
      discoveryMethod: "TRACK_C_WAVE3_UK_CA",
      notes: "CUSTOMER_BLOCKED | Track C wave3 UK/CA",
      handoffs: routeHandoff({ hasFranchiseOrManagementSignal: Boolean(raw.operatorName) }),
    });
    const collision = await collide(hotel);
    const { matchClass } = evaluateGate(hotel, collision);
    if (collision?.hpcRecordId) {
      hotel.hpcRecordId = collision.hpcRecordId;
      hotel.hpcMatchStatus = "MATCHED";
    }
    ukCaRows.push({
      hotelName: hotel.hotelName,
      country: hotel.physicalCountry,
      city: hotel.city,
      matchClass: collision?.hpcRecordId ? MATCH_CLASS.MATCHED_EXISTING_HPC : matchClass,
      collisionOutcome: collision?.outcome || null,
      hpcRecordId: collision?.hpcRecordId || null,
      sourceUrl: hotel.sourceUrl,
    });
    inventoryHotels.push({
      ...hotel,
      matchClass: collision?.hpcRecordId ? MATCH_CLASS.MATCHED_EXISTING_HPC : matchClass,
    });
    if (matchClass === MATCH_CLASS.CREATE_READY && !collision?.hpcRecordId) {
      createCandidates.push({
        discoveryPropertyId: hotel.discoveryPropertyId,
        hotelName: hotel.hotelName,
        stream: "UK_CA",
        hpcFields: {
          "Property Name": hotel.hotelName,
          "Canonical Property Name": hotel.hotelName,
          Country: hotel.physicalCountry,
          City: hotel.city,
          "State / Region": hotel.stateProvince,
          Address: hotel.address,
          "Current Brand": hotel.currentBrand,
          "Affiliation Status": "Branded",
          "Family / Source Family": inferFamilySelect(hotel.currentBrand),
          "Official Property URL": hotel.sourceUrl,
        },
      });
    }
    await sleep(80);
  }

  const cRSummary = summarizeMatchClasses(cRRows);
  const cUSummary = summarizeMatchClasses(cUEval);
  const attritionCR = summarizeCreateReadyAttrition(cRRows);
  const attritionCU = summarizeCreateReadyAttrition(cUEval);

  writeJson("WAVE3_C_R_SEC57_RECONCILIATION.json", {
    asOf: new Date().toISOString(),
    cmbsTotal: cmbs.length,
    enrichedOfficialCount: Object.keys(enrichmentMap).length,
    deferredResearchRequired: ENRICH.deferredResearchRequired,
    summary: cRSummary,
    createReadyCount: cRRows.filter((r) => r.matchClass === MATCH_CLASS.CREATE_READY).length,
    attrition: attritionCR,
    rows: cRRows,
  });
  writeJson("WAVE3_C_U_EVALUATION.json", {
    asOf: new Date().toISOString(),
    evaluatedDistinct: cUEval.length,
    targetMin: 500,
    metVolumeTarget: cUEval.length >= 500,
    summary: cUSummary,
    attrition: attritionCU,
    byBrandFamily,
    byStateTop40: Object.entries(byState)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 40),
    managementCompanyCoverage: {
      Aimbridge: AIMBRIDGE_PUBLIC_US_PORTFOLIO_SEED.length,
      note: "Operator from Aimbridge disclosure only — never inferred from brand",
    },
    sample: cUEval.slice(0, 25),
  });
  writeJson("WAVE3_UK_CA.json", { asOf: new Date().toISOString(), rows: ukCaRows });
  writeJson("WAVE3_CREATE_CANDIDATES.json", {
    asOf: new Date().toISOString(),
    count: createCandidates.length,
    maxBatch: TRACK_C_STANDING_AUTH.maxHpcCreateBatch,
    candidates: createCandidates,
  });
  writeJson("WAVE3_SOURCE_YIELD.json", {
    asOf: new Date().toISOString(),
    researchCostUsd: 0,
    sources: [
      {
        source: "Official Marriott/Hilton/IHG brand directory pages",
        stream: "C-R",
        hotelsEnriched: Object.keys(enrichmentMap).length,
        access: "public free",
        duplicateRisk: "low_with_collision_check",
      },
      {
        source: "HI Hotel Discovery Inventory (US staged)",
        stream: "C-U",
        hotelsEvaluated: invUs.length,
        access: "internal Airtable",
        duplicateRisk: "deduped_normalized",
      },
      {
        source: "V4 US pilot shortlist",
        stream: "C-U",
        hotelsEvaluated: shortlist.length,
        access: "prior Track research",
      },
      {
        source: "Aimbridge public portfolio / press",
        stream: "C-U",
        hotelsEvaluated: AIMBRIDGE_PUBLIC_US_PORTFOLIO_SEED.length,
        access: "public free",
        operatorDisclosed: true,
      },
      {
        source: "SEC CMBS DISTINCT_HOTELS US",
        stream: "C-R",
        hotelsEvaluated: cmbs.length,
        access: "SEC EDGAR free",
      },
    ],
  });

  const ledger = {
    asOf: new Date().toISOString(),
    mode: APPLY ? (APPLY_HPC_CREATE ? "APPLY_WITH_HPC_CREATE" : "APPLY_INVENTORY") : "DRY_RUN",
    publicationState: "CUSTOMER_BLOCKED",
    stream: "TRACK_C_WAVE3",
    standingAuth: TRACK_C_STANDING_AUTH,
    researchCostUsd: 0,
    operations: [],
    summary: {
      creates: 0,
      noops: 0,
      conflicts: 0,
      staged: 0,
      hpcCreates: 0,
      hpcCreateFailures: 0,
      hpcReusedExisting: 0,
      existingEnriched: 0,
    },
    readBack: [],
    hpcCreateResults: [],
    idempotency: { secondPassWouldCreate: [], note: "re-run findByFormula before create" },
  };

  const pool = dedupeDiscoveries(inventoryHotels);
  for (const h of pool) {
    const cls = classifyWriteCategory({
      destination: TABLE_HI_HOTEL_DISCOVERY_INVENTORY,
      verified: h.verified !== false,
      evidenceSupported: Boolean(h.sourceUrl),
      sourceUrl: h.sourceUrl,
      identityClass: h.identityClass,
    });
    if (cls.category !== WRITE_CATEGORY.AUTO_PERSIST_ELIGIBLE) {
      ledger.operations.push({ id: h.discoveryPropertyId, action: "STAGED", reason: cls.reason });
      ledger.summary.staged += 1;
      continue;
    }
    const fields = buildDiscoveryInventoryFields(h);
    fields[MAP_DISCOVERY_INVENTORY.discoveryMethod] = h.discoveryMethod;
    if (h.notes) fields[MAP_DISCOVERY_INVENTORY.notesQuarantine] = h.notes;
    const existing = await findByFormula(
      inventoryTableId,
      `{${MAP_DISCOVERY_INVENTORY.discoveryPropertyId}}="${h.discoveryPropertyId}"`,
    );
    if (!APPLY) {
      ledger.operations.push({ id: h.discoveryPropertyId, action: existing ? "WOULD_NOOP" : "WOULD_CREATE" });
      if (existing) ledger.summary.noops += 1;
      else ledger.summary.creates += 1;
      continue;
    }
    if (existing) {
      if (h.hpcRecordId) {
        const patch = await airtablePatch(inventoryTableId, existing.id, {
          [MAP_DISCOVERY_INVENTORY.hpcMatchStatus]: "MATCHED",
          [MAP_DISCOVERY_INVENTORY.hpcHotel]: [h.hpcRecordId],
        });
        if (patch.ok) ledger.summary.existingEnriched += 1;
      }
      ledger.operations.push({ id: h.discoveryPropertyId, action: "NOOP_EXISTING", airtableId: existing.id });
      ledger.summary.noops += 1;
      continue;
    }
    const created = await airtablePost(inventoryTableId, fields);
    if (!created.ok) {
      ledger.operations.push({ id: h.discoveryPropertyId, action: "CREATE_FAILED", error: created.json });
      ledger.summary.conflicts += 1;
      continue;
    }
    ledger.operations.push({ id: h.discoveryPropertyId, action: "CREATED", airtableId: created.json.id });
    ledger.summary.creates += 1;
    ledger.readBack.push({
      id: created.json.id,
      discoveryPropertyId: created.json.fields?.[MAP_DISCOVERY_INVENTORY.discoveryPropertyId],
      publicationState: created.json.fields?.[MAP_DISCOVERY_INVENTORY.publicationState],
    });
    await sleep(70);
  }

  let founderAuth = null;
  if (APPLY_HPC_CREATE && createCandidates.length) {
    let gitName = "";
    let gitEmail = "";
    try {
      gitName = execSync("git config user.name", { cwd: ROOT, encoding: "utf8" }).trim();
      gitEmail = execSync("git config user.email", { cwd: ROOT, encoding: "utf8" }).trim();
    } catch {
      /* ignore */
    }
    founderAuth = resolveAuthenticatedFounderIdentity({
      envValue: process.env.HPC_CREATE_APPROVER_IDENTITY || "",
      gitName,
      gitEmail,
    });
    if (!founderAuth.ok) ledger.summary.hpcCreateAuthBlocked = founderAuth.reason;
  }

  const batch = createCandidates.slice(0, TRACK_C_STANDING_AUTH.maxHpcCreateBatch);
  console.log(`[HPC] createCandidates=${createCandidates.length} batch=${batch.length} ceiling=${TRACK_C_STANDING_AUTH.maxHpcCreateBatch}`);

  for (const row of batch) {
    const fields = { ...row.hpcFields };
    for (const k of Object.keys(fields)) {
      if (fields[k] == null || fields[k] === "") delete fields[k];
    }
    if (!APPLY_HPC_CREATE || !founderAuth?.ok) {
      ledger.operations.push({ id: row.discoveryPropertyId, action: "HPC_CREATE_STAGED", hotelName: row.hotelName });
      ledger.summary.staged += 1;
      continue;
    }
    const auth = persistHpcCreateAuthorization({
      runId: `track_c_wave3_${new Date().toISOString().slice(0, 10)}`,
      draftPropertyId: row.discoveryPropertyId,
      hotelName: row.hotelName,
      fields,
      approverIdentity: founderAuth.approverIdentity,
      authorizationSource: `${TRACK_C_STANDING_AUTH.source}_WAVE3_CENSUS_SCALE`,
    });
    if (!auth.ok) {
      ledger.hpcCreateResults.push({ draftPropertyId: row.discoveryPropertyId, ok: false, error: auth.error });
      ledger.summary.hpcCreateFailures += 1;
      continue;
    }
    const exec = await executeAuthorizedHpcCreate({
      runId: `track_c_wave3_${new Date().toISOString().slice(0, 10)}`,
      authorization: auth.record,
      dryRun: false,
      apply: true,
      simulation: false,
    });
    ledger.hpcCreateResults.push({
      draftPropertyId: row.discoveryPropertyId,
      hotelName: row.hotelName,
      stream: row.stream,
      ok: exec.ok,
      state: exec.state,
      hpcRecordId: exec.hpcRecordId || null,
      reusedExisting: exec.reusedExisting || false,
      error: exec.error || null,
    });
    if (exec.ok && exec.reusedExisting) ledger.summary.hpcReusedExisting += 1;
    else if (exec.ok && exec.hpcRecordId) {
      ledger.summary.hpcCreates += 1;
      const inv = await findByFormula(
        inventoryTableId,
        `{${MAP_DISCOVERY_INVENTORY.discoveryPropertyId}}="${row.discoveryPropertyId}"`,
      );
      if (inv?.id) {
        await airtablePatch(inventoryTableId, inv.id, {
          [MAP_DISCOVERY_INVENTORY.hpcMatchStatus]: "MATCHED",
          [MAP_DISCOVERY_INVENTORY.hpcHotel]: [exec.hpcRecordId],
        });
        ledger.summary.existingEnriched += 1;
      }
    } else ledger.summary.hpcCreateFailures += 1;
    await sleep(180);
  }

  writeJson(APPLY ? "PERSISTENCE_LEDGER_APPLY.json" : "PERSISTENCE_LEDGER_DRY_RUN.json", ledger);

  const usAfter = await countCountry(PRIORITY_COUNTRY_LABELS.US);
  const mxAfter = await countCountry(PRIORITY_COUNTRY_LABELS.MX);
  const brAfter = await countCountry(PRIORITY_COUNTRY_LABELS.BR);
  const ukAfter = await countCountry(PRIORITY_COUNTRY_LABELS.UK);
  const caAfter = await countCountry(PRIORITY_COUNTRY_LABELS.CA);

  const report = {
    asOf: new Date().toISOString(),
    stream: "TRACK_C_WAVE3",
    publicationState: "CUSTOMER_BLOCKED",
    checkpointFrom: "wave2b US≈53 UK=3 CA=5",
    standingAuthConstraint: {
      priorMaxHpcCreateBatch: 25,
      wave3MaxHpcCreateBatch: TRACK_C_STANDING_AUTH.maxHpcCreateBatch,
      note: "Raised under Wave 3 founder directive; CREATE still requires CREATE_READY + collision-clear + authorized pathway",
    },
    hpcBeforeAfter: {
      US: { before: usBefore, after: usAfter },
      Mexico: { before: mx, after: mxAfter },
      Brazil: { before: br, after: brAfter },
      UK: { before: uk, after: ukAfter },
      Canada: { before: ca, after: caAfter },
    },
    streamCU: {
      evaluatedDistinct: cUEval.length,
      met500Target: cUEval.length >= 500,
      summary: cUSummary,
      attrition: attritionCU,
      byBrandFamily,
    },
    streamCR: {
      sec57: cmbs.length,
      enriched: Object.keys(enrichmentMap).length,
      summary: cRSummary,
      attrition: attritionCR,
      deferredHistorical: ENRICH.deferredResearchRequired,
    },
    ukCa: { rows: ukCaRows.length, createReady: ukCaRows.filter((r) => r.matchClass === MATCH_CLASS.CREATE_READY).length },
    persistence: ledger.summary,
    newHpcRecordsPersisted: ledger.summary.hpcCreates,
    cala: buildCalaPreservationCheck(
      { Mexico: mxAfter, Brazil: brAfter },
      mxAfter + brAfter,
      mx + br,
    ),
    researchCostUsd: 0,
    nextCheckpoint: {
      resumeBranch: "feat/hif-v6-track-c-wave3-census-scale",
      nextSourceBatch: "Hilton/IHG/Wyndham/Choice official US directories + remaining Discovery Inventory CREATE_CANDIDATE collision queue",
      remainingCreateCandidates: Math.max(0, createCandidates.length - batch.length),
      deferredHistorical: (ENRICH.deferredResearchRequired || []).map((d) => d.hotelName),
    },
  };
  writeJson("FOUNDER_TRACK_C_WAVE3_REPORT.json", report);
  fs.writeFileSync(
    path.join(OUT, "FOUNDER_TRACK_C_WAVE3_REPORT.md"),
    `# Track C Wave 3 — Census Scale

US ${usBefore}→${usAfter} · UK ${uk}→${ukAfter} · CA ${ca}→${caAfter}  
C-U evaluated: ${cUEval.length} (≥500: ${cUEval.length >= 500}) · C-R SEC57 enriched: ${Object.keys(enrichmentMap).length}  
HPC creates: ${ledger.summary.hpcCreates} · Inventory creates: ${ledger.summary.creates} · Batch ceiling: ${TRACK_C_STANDING_AUTH.maxHpcCreateBatch}  
CALA preserved: ${report.cala.preserved} · Cost $0 · CUSTOMER_BLOCKED
`,
  );
  console.log(JSON.stringify({ done: true, report, ledger: ledger.summary }, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
