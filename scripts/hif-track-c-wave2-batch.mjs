#!/usr/bin/env node
/**
 * HIF Track C — Wave 2 resumable batch.
 * Comfort CREATE_READY corroboration + Aimbridge address enrichment +
 * SEC CMBS collision sample + UK/CA + controlled HPC/inventory persist.
 *
 * Default dry-run. --apply / --apply-hpc-create for live writes.
 * CUSTOMER_BLOCKED.
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
  TABLE_HI_DISCOVERY_FINDINGS,
  MAP_DISCOVERY_INVENTORY,
  MAP_DISCOVERY_FINDINGS,
} from "../lib/hotel-intelligence/sec-edgar/discovery-inventory-airtable-contract-v23.js";
import {
  buildDiscoveryInventoryFields,
  buildCommercialFindingFields,
  classifyWriteCategory,
  WRITE_CATEGORY,
} from "../lib/hotel-intelligence/sec-edgar/hif-foundation-persistence-v1.js";
import {
  TRACK_C_STANDING_AUTH,
  MATCH_CLASS,
  PRIORITY_COUNTRY_LABELS,
  normalizeCountry,
  classifySecToHpcMatch,
  routeHandoff,
  buildTrackCDiscoveryHotel,
  mapCreateReadyRowToTrackC,
  dedupeDiscoveries,
  summarizeMatchClasses,
  inferBrandFamilyFromLabel,
  inferFamilySelect,
  AIMBRIDGE_PUBLIC_UK_PORTFOLIO_SEED,
} from "../lib/hotel-intelligence/sec-edgar/hif-track-c-universe-expansion-v1.js";
import {
  resolveAuthenticatedFounderIdentity,
  persistHpcCreateAuthorization,
  executeAuthorizedHpcCreate,
} from "../lib/hotel-census/hpc-authorized-create-reuse-v1.js";
import { performLiveHpcIdentityCollisionCheck } from "../lib/hotel-census/hpc-live-identity-collision-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) {
    dotenv.config({ path: p, override: false });
    break;
  }
}

const APPLY = process.argv.includes("--apply");
const APPLY_HPC_CREATE = process.argv.includes("--apply-hpc-create");
const OUT = path.join(ROOT, "reports/sec-edgar-hif-track-c-universe-expansion-wave2");
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
const findingsTableId = ids.tableIds[TABLE_HI_DISCOVERY_FINDINGS];

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
async function getRecord(tableId, recordId) {
  return airtableGet(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${tableId}/${recordId}`);
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

/** Wave2 CREATE_READY Comfort cohort — official Choice URLs corroborated 2026-11-21 */
const COMFORT_CREATE_READY = [
  {
    candidateId: "hpc_pilot_012",
    hotelName: "Comfort Inn & Suites Paris",
    brand: "Comfort Inn",
    currentOperationEvidence: "OPERATING",
    officialHotelBrandSource: "https://www.choicehotels.com/texas/paris/comfort-inn-hotels/txc53",
    historicalSecCmbsSource: "https://www.sec.gov/Archives/edgar/data/1013611/000153949717001773/n1063_424h-x5.htm",
    relevantContractMilestone: { date: "2009-12-31", meaning: "FRANCHISE_EXPIRATION" },
    intendedHpcFields: {
      "Property Name": "Comfort Inn & Suites Paris",
      Country: "United States",
      City: "Paris",
      "State / Region": "TX",
      "Street Address": "3035 Northeast Loop 286",
      "Current Brand": "Comfort Inn",
    },
    recommendation: "CREATE",
  },
  {
    candidateId: "hpc_pilot_013",
    hotelName: "Comfort Suites Buda Austin South",
    brand: "Comfort Suites",
    currentOperationEvidence: "OPERATING",
    officialHotelBrandSource: "https://www.choicehotels.com/texas/buda/comfort-suites-hotels/txc51",
    historicalSecCmbsSource: "https://www.sec.gov/Archives/edgar/data/1013611/000153949717001773/n1063_424h-x5.htm",
    relevantContractMilestone: { date: "2009-12-31", meaning: "FRANCHISE_EXPIRATION" },
    intendedHpcFields: {
      "Property Name": "Comfort Suites Buda Austin South",
      Country: "United States",
      City: "Buda",
      "State / Region": "TX",
      "Street Address": "15295 South Interstate 35 Building 800",
      "Current Brand": "Comfort Suites",
    },
    recommendation: "CREATE",
  },
  {
    candidateId: "hpc_pilot_014",
    hotelName: "Comfort Suites Gastonia",
    brand: "Comfort Suites",
    currentOperationEvidence: "OPERATING",
    officialHotelBrandSource: "https://www.choicehotels.com/north-carolina/gastonia/comfort-suites-hotels/nc028",
    historicalSecCmbsSource: "https://www.sec.gov/Archives/edgar/data/1013611/000153949717001152/n980_424h-x6.htm",
    relevantContractMilestone: { date: "1996-12-31", meaning: "FRANCHISE_EXPIRATION" },
    intendedHpcFields: {
      "Property Name": "Comfort Suites Gastonia",
      Country: "United States",
      City: "Gastonia",
      "State / Region": "NC",
      "Street Address": "1874 Remount Road",
      "Current Brand": "Comfort Suites",
    },
    recommendation: "CREATE",
  },
];

/** Aimbridge US hotels with street address + official brand URL (wave2 enrichment) */
const AIMBRIDGE_ADDRESS_ENRICHED = [
  {
    hotelName: "Residence Inn Scottsdale North",
    city: "Scottsdale",
    stateRegion: "AZ",
    country: "United States",
    address: "17011 North Scottsdale Road",
    currentBrand: "Residence Inn",
    operatorName: "Aimbridge Hospitality",
    sourceUrl: "https://www.marriott.com/en-us/hotels/phxrn-residence-inn-scottsdale-north/overview/",
    operatorSourceUrl: "https://context.aimbridgehospitality.com/llms.txt",
    identityClass: "VERIFIED_PROPERTY",
    evidenceConfidence: "HIGH",
    roomCount: 120,
  },
  {
    hotelName: "Hilton New York Fashion District",
    city: "New York",
    stateRegion: "NY",
    country: "United States",
    address: "152 West 26th Street",
    currentBrand: "Hilton",
    operatorName: "Aimbridge Hospitality",
    sourceUrl: "https://www.hilton.com/en/hotels/nycfdhf-hilton-new-york-fashion-district/hotel-location/",
    operatorSourceUrl: "https://context.aimbridgehospitality.com/llms.txt",
    identityClass: "VERIFIED_PROPERTY",
    evidenceConfidence: "HIGH",
  },
  {
    hotelName: "voco Sandpiper All-Inclusive Resort",
    city: "Port St. Lucie",
    stateRegion: "FL",
    country: "United States",
    currentBrand: "voco",
    operatorName: "Aimbridge Hospitality",
    sourceUrl:
      "https://www.aimbridgehospitality.com/news/aimbridge-welcomes-voco-sandpiper-all-inclusive-resort-to-its-expanding-portfolio/",
    identityClass: "VERIFIED_PROPERTY",
    evidenceConfidence: "HIGH",
    roomCount: 335,
    notes: "Address not on press release — inventory-only until street address corroborated",
    inventoryOnly: true,
  },
  {
    hotelName: "Delta Hotels Orlando Celebration",
    city: "Kissimmee",
    stateRegion: "FL",
    country: "United States",
    currentBrand: "Delta Hotels",
    operatorName: "Aimbridge Hospitality",
    sourceUrl:
      "https://www.aimbridgehospitality.com/news/aimbridge-hospitality-strengthens-portfolio-with-718-key-delta-marriott-orlando-celebration/",
    identityClass: "VERIFIED_PROPERTY",
    evidenceConfidence: "HIGH",
    roomCount: 718,
    notes: "Address not on press release — inventory-only until street address corroborated",
    inventoryOnly: true,
  },
  {
    hotelName: "AC Hotel Atlanta Downtown",
    city: "Atlanta",
    stateRegion: "GA",
    country: "United States",
    currentBrand: "AC Hotels",
    operatorName: "Aimbridge Hospitality",
    sourceUrl: "https://context.aimbridgehospitality.com/llms.txt",
    identityClass: "PROBABLE_PROPERTY",
    evidenceConfidence: "MEDIUM",
    notes: "No street address yet — inventory-only",
    inventoryOnly: true,
  },
];

/** UK Aimbridge + Canada strengthen */
const UK_CA_BATCH = [
  ...AIMBRIDGE_PUBLIC_UK_PORTFOLIO_SEED.map((h) => ({
    ...h,
    inventoryOnly: true,
    notes: "CUSTOMER_BLOCKED | Track C wave2 UK — address corroboration pending",
  })),
  {
    hotelName: "Fairmont Royal York",
    city: "Toronto",
    stateRegion: "ON",
    country: "Canada",
    address: "100 Front Street West",
    currentBrand: "Fairmont",
    sourceUrl: "https://www.fairmont.com/en/hotels/toronto/fairmont-royal-york.html",
    identityClass: "VERIFIED_PROPERTY",
    evidenceConfidence: "HIGH",
  },
  {
    hotelName: "The Savoy London",
    city: "London",
    country: "United Kingdom",
    address: "Strand",
    currentBrand: "Fairmont",
    sourceUrl: "https://www.thesavoylondon.com/",
    identityClass: "VERIFIED_PROPERTY",
    evidenceConfidence: "HIGH",
  },
];

function loadCmbsUsSample(limit = 20) {
  const fp = path.join(ROOT, "reports/sec-edgar-hif-v6-track-a-cmbs-owner/DISTINCT_HOTELS.json");
  if (!fs.existsSync(fp)) return [];
  const j = JSON.parse(fs.readFileSync(fp, "utf8"));
  return (j.hotels || [])
    .filter((h) => String(h.hotelCountry || "").toUpperCase() === "US")
    .slice(0, limit)
    .map((h) =>
      buildTrackCDiscoveryHotel({
        hotelName: h.hotelName,
        country: "United States",
        city: h.hotelMarket || null,
        sourceUrl: h.sourceUrl,
        identityClass: h.identityClass || "PROBABLE_PROPERTY",
        evidenceConfidence: "MEDIUM",
        discoveryMethod: "TRACK_C_WAVE2_SEC_CMBS_RECONCILE",
        matchClass: MATCH_CLASS.RESEARCH_REQUIRED,
        handoffs: routeHandoff({
          hasFranchiseOrManagementSignal: Boolean(h.agreementType),
          hasContractualAgreementEvidence: true,
        }),
        notes: `CUSTOMER_BLOCKED | Track C wave2 CMBS reconcile | agreement=${h.agreementType || ""} | exp=${h.expirationDate || ""}`,
        operatorName: h.operatorName || null,
      }),
    );
}

async function collide(hotel) {
  try {
    return await performLiveHpcIdentityCollisionCheck({
      hotelName: hotel.hotelName,
      aliases: [hotel.hotelName],
      city: hotel.city,
      brand: hotel.currentBrand,
      address: hotel.address,
      country: hotel.physicalCountry || hotel.country,
      officialUrl: hotel.sourceUrl || hotel.officialHotelBrandSource,
      adpPropertyId: hotel.discoveryPropertyId,
    });
  } catch (e) {
    return { outcome: "HPC_LIVE_LOOKUP_UNAVAILABLE", hpcRecordId: null, candidates: [], error: e.message };
  }
}

async function persistInventory(hotels, ledger) {
  for (const h of hotels) {
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
    fields[MAP_DISCOVERY_INVENTORY.discoveryMethod] = h.discoveryMethod || "TRACK_C_WAVE2";
    if (h.brandFamily) fields[MAP_DISCOVERY_INVENTORY.brandFamily] = h.brandFamily;
    if (h.notes) fields[MAP_DISCOVERY_INVENTORY.notesQuarantine] = h.notes;

    const existing = await findByFormula(
      inventoryTableId,
      `{${MAP_DISCOVERY_INVENTORY.discoveryPropertyId}}="${h.discoveryPropertyId}"`,
    );
    if (!APPLY) {
      ledger.operations.push({
        id: h.discoveryPropertyId,
        action: existing ? "WOULD_NOOP" : "WOULD_CREATE",
        existingId: existing?.id || null,
      });
      if (existing) ledger.summary.noops += 1;
      else ledger.summary.creates += 1;
      continue;
    }
    if (existing) {
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
    const rb = await getRecord(inventoryTableId, created.json.id);
    ledger.readBack.push({
      table: "inventory",
      id: created.json.id,
      ok: Boolean(rb?.id),
      discoveryPropertyId: rb?.fields?.[MAP_DISCOVERY_INVENTORY.discoveryPropertyId],
      country: rb?.fields?.[MAP_DISCOVERY_INVENTORY.country],
      publicationState: rb?.fields?.[MAP_DISCOVERY_INVENTORY.publicationState],
    });
    await sleep(80);

    if ((h.handoffs || []).includes("TRACK_A_COMMERCIAL") && h.sourceUrl) {
      const findingId = `find_tc_w2_${h.discoveryPropertyId.replace(/^disc_tc_/, "")}`.slice(0, 64);
      const existingFind = await findByFormula(
        findingsTableId,
        `{${MAP_DISCOVERY_FINDINGS.findingId}}="${findingId}"`,
      );
      if (!existingFind) {
        const fFields = buildCommercialFindingFields(
          {
            findingId,
            commercialCategory: "BCT",
            commercialThesis: `Track C wave2 handoff: ${h.hotelName}`,
            commercialRelevance: "IDENTITY_LINKED_COMMERCIAL_SIGNAL",
            evidencePathway: "TRACK_C_TO_TRACK_A",
            sourceUrl: h.sourceUrl,
            confidence: h.evidenceConfidence || "MEDIUM",
          },
          created.json.id,
        );
        fFields[MAP_DISCOVERY_FINDINGS.findingId] = findingId;
        fFields[MAP_DISCOVERY_FINDINGS.notes] = "CUSTOMER_BLOCKED | Track C wave2 → Track A";
        const fCreated = await airtablePost(findingsTableId, fFields);
        if (fCreated.ok) ledger.summary.findingsCreated = (ledger.summary.findingsCreated || 0) + 1;
      }
    }
  }
}

async function main() {
  console.log(JSON.stringify({ wave: 2, apply: APPLY, applyHpcCreate: APPLY_HPC_CREATE }, null, 2));

  const usBefore = await countCountry(PRIORITY_COUNTRY_LABELS.US);
  const mx = await countCountry(PRIORITY_COUNTRY_LABELS.MX);
  const br = await countCountry(PRIORITY_COUNTRY_LABELS.BR);
  const uk = await countCountry(PRIORITY_COUNTRY_LABELS.UK);
  const ca = await countCountry(PRIORITY_COUNTRY_LABELS.CA);
  console.log(`[baseline] US=${usBefore} MX=${mx} BR=${br} UK=${uk} CA=${ca}`);

  const comfortMapped = COMFORT_CREATE_READY.map(mapCreateReadyRowToTrackC);
  const aimbridgeHotels = AIMBRIDGE_ADDRESS_ENRICHED.map((h) =>
    buildTrackCDiscoveryHotel({
      ...h,
      brandFamily: inferBrandFamilyFromLabel(h.currentBrand),
      discoveryMethod: "TRACK_C_WAVE2_AIMBRIDGE_ENRICHED",
      handoffs: routeHandoff({ hasFranchiseOrManagementSignal: Boolean(h.operatorName) }),
      matchClass: h.inventoryOnly ? MATCH_CLASS.UNMATCHED_VERIFIED : MATCH_CLASS.CREATE_CANDIDATE,
    }),
  );
  const ukCaHotels = UK_CA_BATCH.map((h) =>
    buildTrackCDiscoveryHotel({
      ...h,
      brandFamily: inferBrandFamilyFromLabel(h.currentBrand),
      discoveryMethod: "TRACK_C_WAVE2_UK_CA",
      handoffs: routeHandoff({ hasFranchiseOrManagementSignal: Boolean(h.operatorName) }),
      matchClass: h.address ? MATCH_CLASS.CREATE_CANDIDATE : MATCH_CLASS.UNMATCHED_VERIFIED,
    }),
  );
  const cmbs = loadCmbsUsSample(20);

  const reconRows = [];
  const createCandidates = [];

  // Comfort live collision
  for (const m of comfortMapped) {
    process.stderr.write(`[w2] collision Comfort: ${m.hotel.hotelName}\n`);
    const collision = await collide(m.hotel);
    const matchClass = classifySecToHpcMatch({
      collisionOutcome: collision.outcome,
      identityClass: m.hotel.identityClass,
      operatingStatus: m.hotel.operatingStatus,
      hasExactAddress: Boolean(m.hotel.address),
      hasOfficialBrandSource: Boolean(m.officialHotelBrandSource),
    });
    const row = {
      cohort: "COMFORT_CREATE_READY",
      candidateId: m.candidateId,
      hotelName: m.hotel.hotelName,
      discoveryPropertyId: m.hotel.discoveryPropertyId,
      collisionOutcome: collision.outcome,
      hpcRecordId: collision.hpcRecordId || null,
      matchClass,
      hpcFields: m.hpcFields,
      officialHotelBrandSource: m.officialHotelBrandSource,
      hotel: m.hotel,
    };
    reconRows.push(row);
    if (matchClass === MATCH_CLASS.CREATE_READY) createCandidates.push(row);
    await sleep(120);
  }

  // Aimbridge with address — collision + maybe create
  for (const h of aimbridgeHotels.filter((x) => x.address && !AIMBRIDGE_ADDRESS_ENRICHED.find((a) => a.hotelName === x.hotelName)?.inventoryOnly)) {
    process.stderr.write(`[w2] collision Aimbridge: ${h.hotelName}\n`);
    const collision = await collide(h);
    const matchClass = classifySecToHpcMatch({
      collisionOutcome: collision.outcome,
      identityClass: h.identityClass,
      hasExactAddress: Boolean(h.address),
      hasOfficialBrandSource: Boolean(h.sourceUrl),
    });
    h.matchClass = matchClass;
    h.hpcMatchStatus =
      matchClass === MATCH_CLASS.MATCHED_EXISTING_HPC ? "MATCHED" : "DISCOVERED_NOT_IN_HPC";
    if (collision.hpcRecordId) h.hpcRecordId = collision.hpcRecordId;
    const hpcFields = {
      "Property Name": h.hotelName,
      "Canonical Property Name": h.hotelName,
      Country: "United States",
      City: h.city,
      "State / Region": h.stateProvince,
      Address: h.address,
      "Current Brand": h.currentBrand,
      "Affiliation Status": "Branded",
      "Family / Source Family": inferFamilySelect(h.currentBrand),
      "Official Property URL": h.sourceUrl,
    };
    if (h.operatorName) hpcFields["Operator / Management Company"] = h.operatorName;
    if (h.roomCount) hpcFields["Rooms / Keys"] = h.roomCount;
    const row = {
      cohort: "AIMBRIDGE_ENRICHED",
      candidateId: null,
      hotelName: h.hotelName,
      discoveryPropertyId: h.discoveryPropertyId,
      collisionOutcome: collision.outcome,
      hpcRecordId: collision.hpcRecordId || null,
      matchClass,
      hpcFields,
      officialHotelBrandSource: h.sourceUrl,
      hotel: h,
    };
    reconRows.push(row);
    if (matchClass === MATCH_CLASS.CREATE_READY || matchClass === MATCH_CLASS.CREATE_CANDIDATE) {
      if (matchClass === MATCH_CLASS.CREATE_READY) createCandidates.push(row);
    }
    await sleep(120);
  }

  // Fairmont Royal York + Savoy — UK/CA collision
  for (const h of ukCaHotels.filter((x) => x.address)) {
    process.stderr.write(`[w2] collision UK/CA: ${h.hotelName}\n`);
    const collision = await collide(h);
    const matchClass = classifySecToHpcMatch({
      collisionOutcome: collision.outcome,
      identityClass: h.identityClass,
      hasExactAddress: Boolean(h.address),
      hasOfficialBrandSource: Boolean(h.sourceUrl),
    });
    h.matchClass = matchClass;
    if (collision.hpcRecordId) {
      h.hpcRecordId = collision.hpcRecordId;
      h.hpcMatchStatus = "MATCHED";
    }
    reconRows.push({
      cohort: "UK_CA",
      hotelName: h.hotelName,
      discoveryPropertyId: h.discoveryPropertyId,
      collisionOutcome: collision.outcome,
      hpcRecordId: collision.hpcRecordId || null,
      matchClass,
      hotel: h,
    });
    if (matchClass === MATCH_CLASS.CREATE_READY) {
      createCandidates.push({
        cohort: "UK_CA",
        hotelName: h.hotelName,
        discoveryPropertyId: h.discoveryPropertyId,
        collisionOutcome: collision.outcome,
        matchClass,
        hpcFields: {
          "Property Name": h.hotelName,
          "Canonical Property Name": h.hotelName,
          Country: normalizeCountry(h.physicalCountry),
          City: h.city,
          "State / Region": h.stateProvince || null,
          Address: h.address,
          "Current Brand": h.currentBrand,
          "Affiliation Status": "Branded",
          "Family / Source Family": inferFamilySelect(h.currentBrand),
          "Official Property URL": h.sourceUrl,
        },
        hotel: h,
        officialHotelBrandSource: h.sourceUrl,
      });
    }
    await sleep(120);
  }

  // CMBS sample — collision only (no create without address)
  let cmbsMatched = 0;
  let cmbsUnmatched = 0;
  for (const h of cmbs.slice(0, 12)) {
    process.stderr.write(`[w2] collision CMBS: ${h.hotelName}\n`);
    const collision = await collide(h);
    const matchClass = classifySecToHpcMatch({
      collisionOutcome: collision.outcome,
      identityClass: h.identityClass,
      hasExactAddress: false,
      hasOfficialBrandSource: false,
    });
    if (matchClass === MATCH_CLASS.MATCHED_EXISTING_HPC) cmbsMatched += 1;
    else cmbsUnmatched += 1;
    reconRows.push({
      cohort: "SEC_CMBS",
      hotelName: h.hotelName,
      discoveryPropertyId: h.discoveryPropertyId,
      collisionOutcome: collision.outcome,
      hpcRecordId: collision.hpcRecordId || null,
      matchClass,
      hotel: h,
    });
    if (collision.hpcRecordId) {
      h.hpcRecordId = collision.hpcRecordId;
      h.hpcMatchStatus = "MATCHED";
      h.matchClass = MATCH_CLASS.MATCHED_EXISTING_HPC;
    }
    await sleep(100);
  }

  const reconSummary = summarizeMatchClasses(reconRows);
  writeJson("WAVE2_RECONCILIATION.json", { asOf: new Date().toISOString(), summary: reconSummary, rows: reconRows, cmbsMatched, cmbsUnmatched });

  const inventoryPool = dedupeDiscoveries([
    ...comfortMapped.map((m) => m.hotel),
    ...aimbridgeHotels,
    ...ukCaHotels,
    ...cmbs,
  ]);
  writeJson("WAVE2_DISCOVERY_POOL.json", {
    asOf: new Date().toISOString(),
    count: inventoryPool.length,
    hotels: inventoryPool,
  });

  const ledger = {
    asOf: new Date().toISOString(),
    mode: APPLY ? (APPLY_HPC_CREATE ? "APPLY_WITH_HPC_CREATE" : "APPLY_INVENTORY") : "DRY_RUN",
    publicationState: "CUSTOMER_BLOCKED",
    stream: "TRACK_C_WAVE2",
    standingAuth: TRACK_C_STANDING_AUTH,
    researchCostUsd: 0,
    operations: [],
    summary: {
      creates: 0,
      updates: 0,
      noops: 0,
      conflicts: 0,
      staged: 0,
      hpcCreates: 0,
      hpcCreateFailures: 0,
      hpcReusedExisting: 0,
      findingsCreated: 0,
      existingEnriched: 0,
    },
    readBack: [],
    hpcCreateResults: [],
  };

  await persistInventory(inventoryPool, ledger);

  // Link matched CMBS/UK/CA to HPC on inventory when matched
  for (const row of reconRows) {
    if (row.matchClass !== MATCH_CLASS.MATCHED_EXISTING_HPC || !row.hpcRecordId) continue;
    if (!APPLY) {
      ledger.operations.push({ id: row.discoveryPropertyId, action: "WOULD_LINK_EXISTING", hpcRecordId: row.hpcRecordId });
      continue;
    }
    const inv = await findByFormula(
      inventoryTableId,
      `{${MAP_DISCOVERY_INVENTORY.discoveryPropertyId}}="${row.discoveryPropertyId}"`,
    );
    if (!inv?.id) continue;
    const link = await airtablePatch(inventoryTableId, inv.id, {
      [MAP_DISCOVERY_INVENTORY.hpcMatchStatus]: "MATCHED",
      [MAP_DISCOVERY_INVENTORY.hpcHotel]: [row.hpcRecordId],
    });
    if (link.ok) ledger.summary.existingEnriched += 1;
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
    if (!founderAuth.ok) {
      ledger.summary.hpcCreateAuthBlocked = founderAuth.reason;
    }
  }

  const createBatch = createCandidates.slice(0, TRACK_C_STANDING_AUTH.maxHpcCreateBatch);
  for (const row of createBatch) {
    const fields = { ...row.hpcFields };
    for (const k of Object.keys(fields)) {
      if (fields[k] == null || fields[k] === "") delete fields[k];
    }
    const draftPropertyId = row.discoveryPropertyId;
    if (!APPLY_HPC_CREATE || !founderAuth?.ok) {
      ledger.operations.push({ id: draftPropertyId, action: "HPC_CREATE_STAGED", fields, matchClass: row.matchClass });
      ledger.summary.staged += 1;
      continue;
    }
    const auth = persistHpcCreateAuthorization({
      runId: `track_c_wave2_${new Date().toISOString().slice(0, 10)}`,
      draftPropertyId,
      hotelName: row.hotelName,
      fields,
      approverIdentity: founderAuth.approverIdentity,
      authorizationSource: `${TRACK_C_STANDING_AUTH.source}_WAVE2`,
    });
    if (!auth.ok) {
      ledger.hpcCreateResults.push({ draftPropertyId, ok: false, error: auth.error });
      ledger.summary.hpcCreateFailures += 1;
      continue;
    }
    const exec = await executeAuthorizedHpcCreate({
      runId: `track_c_wave2_${new Date().toISOString().slice(0, 10)}`,
      authorization: auth.record,
      dryRun: false,
      apply: true,
      simulation: false,
    });
    ledger.hpcCreateResults.push({
      draftPropertyId,
      hotelName: row.hotelName,
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
        `{${MAP_DISCOVERY_INVENTORY.discoveryPropertyId}}="${draftPropertyId}"`,
      );
      if (inv?.id) {
        await airtablePatch(inventoryTableId, inv.id, {
          [MAP_DISCOVERY_INVENTORY.hpcMatchStatus]: "MATCHED",
          [MAP_DISCOVERY_INVENTORY.hpcHotel]: [exec.hpcRecordId],
        });
        ledger.summary.existingEnriched += 1;
      }
    } else ledger.summary.hpcCreateFailures += 1;
    await sleep(200);
  }

  writeJson(APPLY ? "PERSISTENCE_LEDGER_APPLY.json" : "PERSISTENCE_LEDGER_DRY_RUN.json", ledger);

  const usAfter = await countCountry(PRIORITY_COUNTRY_LABELS.US);
  const mxAfter = await countCountry(PRIORITY_COUNTRY_LABELS.MX);
  const brAfter = await countCountry(PRIORITY_COUNTRY_LABELS.BR);
  const ukAfter = await countCountry(PRIORITY_COUNTRY_LABELS.UK);
  const caAfter = await countCountry(PRIORITY_COUNTRY_LABELS.CA);

  const report = {
    asOf: new Date().toISOString(),
    stream: "TRACK_C_WAVE2",
    publicationState: "CUSTOMER_BLOCKED",
    mergeOwner: "Integration Controller",
    usHotelsBefore: usBefore,
    usHotelsAfter: usAfter,
    priorityCountries: {
      before: { US: usBefore, Mexico: mx, Brazil: br, UK: uk, Canada: ca },
      after: { US: usAfter, Mexico: mxAfter, Brazil: brAfter, UK: ukAfter, Canada: caAfter },
    },
    calaPreserved: mxAfter === mx && brAfter === br,
    distinctNewIdentitiesInPool: inventoryPool.length,
    createCandidates: createBatch.map((r) => r.hotelName),
    newHpcRecordsPersisted: ledger.summary.hpcCreates,
    existingEnriched: ledger.summary.existingEnriched,
    discoveryInventoryCreates: ledger.summary.creates,
    discoveryInventoryNoops: ledger.summary.noops,
    secToHpc: {
      ...reconSummary,
      cmbsSampleMatched: cmbsMatched,
      cmbsSampleUnmatched: cmbsUnmatched,
    },
    ukCanada: {
      ukHpcBefore: uk,
      ukHpcAfter: ukAfter,
      caHpcBefore: ca,
      caHpcAfter: caAfter,
      discoverySeeds: ukCaHotels.length,
    },
    researchCostUsd: 0,
    ledgerSummary: ledger.summary,
  };
  writeJson("FOUNDER_TRACK_C_WAVE2_REPORT.json", report);

  const md = `# HIF Track C — Wave 2 Report

**As of:** ${report.asOf}  
**Publication:** CUSTOMER_BLOCKED  

## US hotels
- Before: ${usBefore}
- After: ${usAfter}
- New HPC creates: ${ledger.summary.hpcCreates}
- Discovery inventory creates: ${ledger.summary.creates}

## SEC→HPC
- Recon rows: ${reconRows.length}
- Match rate: ${reconSummary.matchRate}
- CMBS sample matched/unmatched: ${cmbsMatched}/${cmbsUnmatched}

## UK/Canada
- UK HPC: ${uk} → ${ukAfter}
- CA HPC: ${ca} → ${caAfter}

## CALA
- MX ${mx}→${mxAfter} · BR ${br}→${brAfter} · preserved=${report.calaPreserved}

## Cost
- $0
`;
  fs.writeFileSync(path.join(OUT, "FOUNDER_TRACK_C_WAVE2_REPORT.md"), md);
  console.log(JSON.stringify({ done: true, report, ledger: ledger.summary }, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
