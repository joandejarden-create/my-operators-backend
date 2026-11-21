#!/usr/bin/env node
/**
 * Track C Wave 2b — full 57 SEC CMBS US → HPC reconciliation.
 * CREATE only when independently CREATE_READY (address + official brand URL + collision absent).
 * Also deepen UK/CA. CUSTOMER_BLOCKED.
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
  normalizeCountry,
  classifySecToHpcMatch,
  routeHandoff,
  buildTrackCDiscoveryHotel,
  dedupeDiscoveries,
  summarizeMatchClasses,
  inferFamilySelect,
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
const OUT = path.join(ROOT, "reports/sec-edgar-hif-track-c-universe-expansion-wave2b");
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

/** Independently corroborated Marriott/Hilton/IHG pages — 2026-11-21 research */
const OFFICIAL_ENRICHMENT = {
  "Courtyard Larkspur Landing Marin County": {
    city: "Larkspur",
    stateRegion: "CA",
    address: "2500 Larkspur Landing Circle",
    officialUrl: "https://www.marriott.com/en-us/hotels/sfoll-courtyard-san-francisco-larkspur-landing-marin-county/overview/",
    brand: "Courtyard",
  },
  "Courtyard San Mateo Foster City": {
    city: "Foster City",
    stateRegion: "CA",
    address: "550 Shell Boulevard",
    officialUrl: "https://www.marriott.com/en-us/hotels/sfofc-courtyard-san-mateo-foster-city/overview/",
    brand: "Courtyard",
  },
  "Courtyard San Jose Cupertino": {
    city: "Cupertino",
    stateRegion: "CA",
    address: "10605 North Wolfe Road",
    officialUrl: "https://www.marriott.com/en-us/hotels/sjccu-courtyard-san-jose-cupertino/overview/",
    brand: "Courtyard",
  },
  "Courtyard Boulder": {
    city: "Boulder",
    stateRegion: "CO",
    address: "4710 Pearl East Circle",
    officialUrl: "https://www.marriott.com/en-us/hotels/denbd-courtyard-boulder/overview/",
    brand: "Courtyard",
  },
  "Courtyard Los Angeles Torrance Palos Verdes": {
    city: "Torrance",
    stateRegion: "CA",
    address: "2633 Sepulveda Boulevard",
    officialUrl: "https://www.marriott.com/en-us/hotels/laxto-courtyard-los-angeles-torrance-palos-verdes/overview/",
    brand: "Courtyard",
  },
  "Courtyard Los Angeles Hacienda Heights": {
    city: "Hacienda Heights",
    stateRegion: "CA",
    address: "1905 S Azusa Avenue",
    officialUrl: "https://www.marriott.com/en-us/hotels/laxhh-courtyard-los-angeles-hacienda-heights-orange-county/overview/",
    brand: "Courtyard",
  },
  "Courtyard Seattle South Center": {
    city: "Tukwila",
    stateRegion: "WA",
    address: "400 Andover Park West",
    officialUrl: "https://www.marriott.com/en-us/hotels/seasc-courtyard-seattle-southcenter/overview/",
    brand: "Courtyard",
  },
  "Courtyard Nashville Airport": {
    city: "Nashville",
    stateRegion: "TN",
    address: "2508 Elm Hill Pike",
    officialUrl: "https://www.marriott.com/en-us/hotels/bnaca-courtyard-nashville-airport/overview/",
    brand: "Courtyard",
  },
  "Courtyard Palm Springs": {
    city: "Palm Springs",
    stateRegion: "CA",
    address: "1300 Tahquitz Canyon Way",
    officialUrl: "https://www.marriott.com/en-us/hotels/pspch-courtyard-palm-springs/overview/",
    brand: "Courtyard",
  },
  "Courtyard Portland Beaverton": {
    city: "Beaverton",
    stateRegion: "OR",
    address: "8500 SW Nimbus Avenue",
    officialUrl: "https://www.marriott.com/en-us/hotels/pdxcy-courtyard-portland-beaverton/overview/",
    brand: "Courtyard",
  },
  "Courtyard Atlanta Perimeter Center": {
    city: "Atlanta",
    stateRegion: "GA",
    address: "6250 Peachtree-Dunwoody Rd",
    officialUrl: "https://www.marriott.com/en-us/hotels/atlpe-courtyard-atlanta-perimeter-center/overview/",
    brand: "Courtyard",
  },
  "Courtyard Detroit Livonia": {
    city: "Livonia",
    stateRegion: "MI",
    address: "17200 N Laurel Park Drive",
    officialUrl: "https://www.marriott.com/en-us/hotels/detlv-courtyard-detroit-livonia/overview/",
    brand: "Courtyard",
  },
  "Courtyard St. Louis Creve Coeur": {
    city: "Creve Coeur",
    stateRegion: "MO",
    address: "828 N New Ballas Road",
    officialUrl: "https://www.marriott.com/en-us/hotels/stlcc-courtyard-st-louis-creve-coeur/overview/",
    brand: "Courtyard",
  },
  "Courtyard Rye": {
    city: "Rye",
    stateRegion: "NY",
    address: "631 Midland Avenue",
    officialUrl: "https://www.marriott.com/en-us/hotels/hpnry-courtyard-rye/overview/",
    brand: "Courtyard",
  },
  "Courtyard Fresno": {
    city: "Fresno",
    stateRegion: "CA",
    address: "140 East Shaw Avenue",
    officialUrl: "https://www.marriott.com/en-us/hotels/fatch-courtyard-fresno/overview/",
    brand: "Courtyard",
  },
  "Courtyard Tampa Westshore": {
    city: "Tampa",
    stateRegion: "FL",
    address: "3805 West Cypress",
    officialUrl: "https://www.marriott.com/en-us/hotels/tpawt-courtyard-tampa-westshore-airport/overview/",
    brand: "Courtyard",
  },
  "Courtyard Boston Andover": {
    city: "Andover",
    stateRegion: "MA",
    address: "10 Campanelli Drive",
    officialUrl: "https://www.marriott.com/en-us/hotels/mhtch-courtyard-boston-andover/overview/",
    brand: "Courtyard",
  },
  "Courtyard Detroit Metro Airport": {
    city: "Romulus",
    stateRegion: "MI",
    address: "30653 Flynn Drive",
    officialUrl: "https://www.marriott.com/en-us/hotels/dtwca-courtyard-detroit-metro-airport-romulus/overview/",
    brand: "Courtyard",
  },
};

const UK_CA_DEEPEN = [
  {
    hotelName: "Delta Hotels Toronto",
    city: "Toronto",
    stateRegion: "ON",
    country: "Canada",
    address: "75 Lower Simcoe Street",
    currentBrand: "Delta Hotels",
    sourceUrl: "https://www.marriott.com/en-us/hotels/yyzdl-delta-hotels-toronto/overview/",
  },
  {
    hotelName: "Hilton Toronto",
    city: "Toronto",
    stateRegion: "ON",
    country: "Canada",
    address: "145 Richmond Street West",
    currentBrand: "Hilton",
    sourceUrl: "https://www.hilton.com/en/hotels/torhihh-hilton-toronto/",
  },
  {
    hotelName: "Fairmont Banff Springs",
    city: "Banff",
    stateRegion: "AB",
    country: "Canada",
    address: "405 Spray Avenue",
    currentBrand: "Fairmont",
    sourceUrl: "https://www.fairmont.com/en/hotels/banff/fairmont-banff-springs/location-contact.html",
  },
  {
    hotelName: "Hilton London Metropole",
    city: "London",
    country: "United Kingdom",
    address: "225 Edgware Road",
    currentBrand: "Hilton",
    sourceUrl: "https://www.hilton.com/en/hotels/lonmetw-hilton-london-metropole/hotel-location/",
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

function loadCmbsUs57() {
  const j = JSON.parse(
    fs.readFileSync(path.join(ROOT, "reports/sec-edgar-hif-v6-track-a-cmbs-owner/DISTINCT_HOTELS.json"), "utf8"),
  );
  return (j.hotels || []).filter((h) => String(h.hotelCountry || "").toUpperCase() === "US");
}

async function main() {
  console.log(JSON.stringify({ wave: "2b", apply: APPLY, applyHpcCreate: APPLY_HPC_CREATE }, null, 2));
  const usBefore = await countCountry(PRIORITY_COUNTRY_LABELS.US);
  const mx = await countCountry(PRIORITY_COUNTRY_LABELS.MX);
  const br = await countCountry(PRIORITY_COUNTRY_LABELS.BR);
  const uk = await countCountry(PRIORITY_COUNTRY_LABELS.UK);
  const ca = await countCountry(PRIORITY_COUNTRY_LABELS.CA);
  console.log(`[baseline] US=${usBefore} MX=${mx} BR=${br} UK=${uk} CA=${ca}`);

  const cmbs = loadCmbsUs57();
  console.log(`[cmbs] US hotels=${cmbs.length}`);

  const reconRows = [];
  const createCandidates = [];
  const inventoryHotels = [];

  let i = 0;
  for (const raw of cmbs) {
    i += 1;
    const enrich = OFFICIAL_ENRICHMENT[raw.hotelName] || null;
    const hotel = buildTrackCDiscoveryHotel({
      hotelName: raw.hotelName,
      country: "United States",
      city: enrich?.city || raw.hotelMarket || null,
      stateRegion: enrich?.stateRegion || null,
      address: enrich?.address || null,
      currentBrand: enrich?.brand || "Courtyard",
      brandFamily: "MARRIOTT",
      sourceUrl: enrich?.officialUrl || raw.sourceUrl,
      identityClass: enrich ? "VERIFIED_PROPERTY" : "PROBABLE_PROPERTY",
      evidenceConfidence: enrich ? "HIGH" : "MEDIUM",
      discoveryMethod: "TRACK_C_WAVE2B_SEC57_RECONCILE",
      handoffs: routeHandoff({
        hasFranchiseOrManagementSignal: Boolean(raw.agreementType),
        hasContractualAgreementEvidence: true,
      }),
      historicalBrand: "Courtyard",
      notes: `CUSTOMER_BLOCKED | Track C wave2b SEC57 | agreement=${raw.agreementType || ""} | exp=${raw.expirationDate || ""} | historical_brand_preserved=Courtyard | enrich=${enrich ? "OFFICIAL_MARRIOTT" : "SEC_ONLY"}`,
      operatingStatus: "OPERATING",
    });

    process.stderr.write(`[2b] ${i}/${cmbs.length} ${raw.hotelName}\n`);
    const collision = await collide(hotel);
    const matchClass = classifySecToHpcMatch({
      collisionOutcome: collision.outcome,
      identityClass: hotel.identityClass,
      operatingStatus: "OPERATING",
      hasExactAddress: Boolean(enrich?.address),
      hasOfficialBrandSource: Boolean(enrich?.officialUrl),
    });
    hotel.matchClass = matchClass;
    if (collision.hpcRecordId) {
      hotel.hpcRecordId = collision.hpcRecordId;
      hotel.hpcMatchStatus = "MATCHED";
    }

    const row = {
      cohort: "SEC_CMBS_57",
      hotelName: raw.hotelName,
      discoveryPropertyId: hotel.discoveryPropertyId,
      collisionOutcome: collision.outcome,
      hpcRecordId: collision.hpcRecordId || null,
      matchClass,
      enriched: Boolean(enrich),
      historicalSecUrl: raw.sourceUrl,
      officialUrl: enrich?.officialUrl || null,
      address: enrich?.address || null,
    };
    reconRows.push(row);
    inventoryHotels.push(hotel);

    if (matchClass === MATCH_CLASS.CREATE_READY && enrich) {
      createCandidates.push({
        hotelName: raw.hotelName,
        discoveryPropertyId: hotel.discoveryPropertyId,
        matchClass,
        collisionOutcome: collision.outcome,
        hpcFields: {
          "Property Name": raw.hotelName,
          "Canonical Property Name": raw.hotelName,
          Country: "United States",
          City: enrich.city,
          "State / Region": enrich.stateRegion,
          Address: enrich.address,
          "Current Brand": enrich.brand,
          "Affiliation Status": "Branded",
          "Family / Source Family": "Marriott",
          "Official Property URL": enrich.officialUrl,
        },
        hotel,
      });
    }
    await sleep(90);
  }

  // UK/CA deepen
  for (const u of UK_CA_DEEPEN) {
    const hotel = buildTrackCDiscoveryHotel({
      ...u,
      identityClass: "VERIFIED_PROPERTY",
      evidenceConfidence: "HIGH",
      discoveryMethod: "TRACK_C_WAVE2B_UK_CA_DEEPEN",
      handoffs: routeHandoff({ hasFranchiseOrManagementSignal: false }),
      notes: "CUSTOMER_BLOCKED | Track C wave2b UK/CA deepen",
    });
    process.stderr.write(`[2b] UK/CA ${hotel.hotelName}\n`);
    const collision = await collide(hotel);
    const matchClass = classifySecToHpcMatch({
      collisionOutcome: collision.outcome,
      identityClass: "VERIFIED_PROPERTY",
      hasExactAddress: Boolean(u.address),
      hasOfficialBrandSource: Boolean(u.sourceUrl),
    });
    hotel.matchClass = matchClass;
    reconRows.push({
      cohort: "UK_CA_DEEPEN",
      hotelName: hotel.hotelName,
      discoveryPropertyId: hotel.discoveryPropertyId,
      collisionOutcome: collision.outcome,
      hpcRecordId: collision.hpcRecordId || null,
      matchClass,
      enriched: true,
      officialUrl: u.sourceUrl,
      address: u.address,
    });
    inventoryHotels.push(hotel);
    if (matchClass === MATCH_CLASS.CREATE_READY) {
      createCandidates.push({
        hotelName: hotel.hotelName,
        discoveryPropertyId: hotel.discoveryPropertyId,
        matchClass,
        collisionOutcome: collision.outcome,
        hpcFields: {
          "Property Name": hotel.hotelName,
          "Canonical Property Name": hotel.hotelName,
          Country: normalizeCountry(u.country),
          City: u.city,
          "State / Region": u.stateRegion || null,
          Address: u.address,
          "Current Brand": u.currentBrand,
          "Affiliation Status": "Branded",
          "Family / Source Family": inferFamilySelect(u.currentBrand),
          "Official Property URL": u.sourceUrl,
        },
        hotel,
      });
    }
    await sleep(100);
  }

  const summary = summarizeMatchClasses(reconRows);
  writeJson("WAVE2B_SEC57_RECONCILIATION.json", {
    asOf: new Date().toISOString(),
    cmbsTotal: cmbs.length,
    enrichedOfficialCount: Object.keys(OFFICIAL_ENRICHMENT).length,
    summary,
    createReadyCount: createCandidates.length,
    rows: reconRows,
  });

  const ledger = {
    asOf: new Date().toISOString(),
    mode: APPLY ? (APPLY_HPC_CREATE ? "APPLY_WITH_HPC_CREATE" : "APPLY_INVENTORY") : "DRY_RUN",
    publicationState: "CUSTOMER_BLOCKED",
    stream: "TRACK_C_WAVE2B",
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
      // Non-destructive enrich: update match status / notes if matched
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
      runId: `track_c_wave2b_${new Date().toISOString().slice(0, 10)}`,
      draftPropertyId: row.discoveryPropertyId,
      hotelName: row.hotelName,
      fields,
      approverIdentity: founderAuth.approverIdentity,
      authorizationSource: `${TRACK_C_STANDING_AUTH.source}_WAVE2B_SEC57`,
    });
    if (!auth.ok) {
      ledger.hpcCreateResults.push({ draftPropertyId: row.discoveryPropertyId, ok: false, error: auth.error });
      ledger.summary.hpcCreateFailures += 1;
      continue;
    }
    const exec = await executeAuthorizedHpcCreate({
      runId: `track_c_wave2b_${new Date().toISOString().slice(0, 10)}`,
      authorization: auth.record,
      dryRun: false,
      apply: true,
      simulation: false,
    });
    ledger.hpcCreateResults.push({
      draftPropertyId: row.discoveryPropertyId,
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

  const unresolved = reconRows.filter((r) =>
    [MATCH_CLASS.UNMATCHED_VERIFIED, MATCH_CLASS.RESEARCH_REQUIRED, MATCH_CLASS.AMBIGUOUS, MATCH_CLASS.UNCERTAIN].includes(
      r.matchClass,
    ),
  );

  const report = {
    asOf: new Date().toISOString(),
    stream: "TRACK_C_WAVE2B",
    publicationState: "CUSTOMER_BLOCKED",
    checkpointFrom: "wave2 US=35 UK=2 CA=2",
    usHotelsBefore: usBefore,
    usHotelsAfter: usAfter,
    priority: {
      before: { US: usBefore, Mexico: mx, Brazil: br, UK: uk, Canada: ca },
      after: { US: usAfter, Mexico: mxAfter, Brazil: brAfter, UK: ukAfter, Canada: caAfter },
    },
    sec57: {
      total: cmbs.length,
      officiallyEnriched: Object.keys(OFFICIAL_ENRICHMENT).length,
      createReadyAttempted: batch.length,
      matchedExisting: summary.matchedExistingHpc,
      unresolvedCount: unresolved.length,
      summary,
    },
    newHpcRecordsPersisted: ledger.summary.hpcCreates,
    existingEnriched: ledger.summary.existingEnriched,
    discoveryInventoryCreates: ledger.summary.creates,
    discoveryInventoryNoops: ledger.summary.noops,
    researchCostUsd: 0,
    ledgerSummary: ledger.summary,
    calaPreserved: mxAfter === mx && brAfter === br,
  };
  writeJson("FOUNDER_TRACK_C_WAVE2B_REPORT.json", report);
  fs.writeFileSync(
    path.join(OUT, "FOUNDER_TRACK_C_WAVE2B_REPORT.md"),
    `# Track C Wave 2b — SEC57 + UK/CA

US ${usBefore}→${usAfter} · UK ${uk}→${ukAfter} · CA ${ca}→${caAfter}  
SEC57: ${cmbs.length} reconciled · ${Object.keys(OFFICIAL_ENRICHMENT).length} official-enriched · HPC creates ${ledger.summary.hpcCreates}  
Unresolved (inventory/research): ${unresolved.length}  
CALA preserved: ${report.calaPreserved} · Cost $0 · CUSTOMER_BLOCKED
`,
  );
  console.log(JSON.stringify({ done: true, report, ledger: ledger.summary }, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
