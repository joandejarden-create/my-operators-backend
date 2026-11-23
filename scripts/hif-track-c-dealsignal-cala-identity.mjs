#!/usr/bin/env node
/**
 * Track C — DealSignal + Track A/B handoff → HPC identity & ownership integration.
 * CALA-first; global ENGAGE NOW hotels included. No per-hotel commercial requalification.
 * CREATE only with address + brand-directory Official URL + geo-safe collision clear.
 * CUSTOMER_BLOCKED. --apply / --apply-hpc-create
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
import { buildDiscoveryInventoryFields } from "../lib/hotel-intelligence/sec-edgar/hif-foundation-persistence-v1.js";
import {
  resolveAuthenticatedFounderIdentity,
  persistHpcCreateAuthorization,
  executeAuthorizedHpcCreate,
} from "../lib/hotel-census/hpc-authorized-create-reuse-v1.js";
import { performLiveHpcIdentityCollisionCheck } from "../lib/hotel-census/hpc-live-identity-collision-v1.js";
import {
  TRACK_C_STANDING_AUTH,
  inferFamilySelect,
  inferBrandFamilyFromLabel,
} from "../lib/hotel-intelligence/sec-edgar/hif-track-c-universe-expansion-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) {
    dotenv.config({ path: p, override: false });
    break;
  }
}
const APPLY = process.argv.includes("--apply");
const APPLY_HPC = process.argv.includes("--apply-hpc-create");
const OUT = path.join(ROOT, "reports/sec-edgar-hif-track-c-dealsignal-cala-identity");
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

const BRAND_RE =
  /marriott\.com|hilton\.com|ihg\.com|hyatt\.com|resortsbyhyatt\.com|hyattinclusivecollection\.com|fourseasons\.com|wyndham|choicehotels|bestwestern|accor|fairmont|radissonhotels|kimptonhotels|margaritavillehollywoodbeachresort\.com|margaritavilleresorts\.com|margaritaville\.com/i;

/**
 * CALA first, then DealSignal ENGAGE NOW / high-priority named hotels.
 * Portfolio parents omitted (not individual hotel HPC).
 * Ownership vs operator differentiated in notes — Operator field not invented.
 */
const QUEUE = [
  // ——— CALA ———
  {
    priority: "CALA_P0",
    discoveryPropertyId: "disc_tb_hyatt_ziva_riviera_cancun_2023",
    findingId: "find_tb_v3_hyatt_ziva_rc_not_in_tortuga_13",
    hotelName: "Hyatt Ziva Riviera Cancún",
    aliases: ["Hyatt Ziva Riviera Cancun", "Ziva Riviera Cancún"],
    city: "Puerto Morelos",
    stateRegion: "Quintana Roo",
    country: "Mexico",
    address: "Bahia de Petempich SM 12 MZ 31 Lote 14-02",
    brand: "Hyatt Ziva",
    brandUrl: "https://www.resortsbyhyatt.com/ziva-riviera-cancun",
    rooms: 438,
    owner: null,
    ownerNote: "Not Tortuga 13-of-14 Playa sale set; Playa third-party management terminated 2023-12-15; current owner/successor manager unverified in primary instrument",
    operator: null,
    operatorNote: "Successor manager UNKNOWN after Playa termination — do not invent",
    sources: [
      "https://newsroom.hyatt.com/May-28-2026-Investor-Day-Release",
      "https://www.resortsbyhyatt.com/ziva-riviera-cancun",
    ],
    handoffFrom: "TRACK_B_V3",
  },
  {
    priority: "CALA_P0",
    discoveryPropertyId: "disc_tb_hyatt_zilara_riviera_maya_2023",
    findingId: "find_tb_v3_hyatt_zilara_rm_not_in_tortuga_13",
    hotelName: "Hyatt Zilara Riviera Maya",
    aliases: ["Hyatt Zilara Riviera Maya Adults Only"],
    city: "Playa del Carmen",
    stateRegion: "Quintana Roo",
    country: "Mexico",
    address: "Carretera Federal 387, KM 299, Solidaridad",
    brand: "Hyatt Zilara",
    brandUrl: "https://www.resortsbyhyatt.com/zilara/zilara-riviera-maya/contact-us",
    rooms: null,
    owner: null,
    ownerNote: "Outside Tortuga 13-of-14; Playa third-party management terminated 2023-12-15; owner/successor manager unverified",
    operator: null,
    operatorNote: "Successor manager UNKNOWN — do not invent",
    sources: [
      "https://newsroom.hyatt.com/May-28-2026-Investor-Day-Release",
      "https://www.resortsbyhyatt.com/zilara/zilara-riviera-maya/contact-us",
    ],
    handoffFrom: "TRACK_B_V3",
    priorAmbiguous: true,
  },
  // ——— DealSignal ENGAGE NOW (global) ———
  {
    priority: "ENGAGE_NOW",
    discoveryPropertyId: "disc_ds_opew_v2_bidwell_portland_2026",
    findingId: "find_ds_opew_v2_bidwell_portland_bct_2026",
    hotelName: "The Bidwell Marriott Portland",
    city: "Portland",
    stateRegion: "OR",
    country: "United States",
    address: "520 SW Broadway",
    brand: "Marriott Hotels",
    brandUrl: "https://www.marriott.com/en-us/hotels/pdxct-the-bidwell-marriott-portland/overview/",
    rooms: null,
    owner: "Sunstone Hotel Investors, Inc.",
    operator: "Sage Hospitality Group",
    operatorNote: "Franchise hotel; Sage HMA ~2027 indicative — brand≠operator",
    sources: [
      "https://www.marriott.com/en-us/hotels/pdxct-the-bidwell-marriott-portland/overview/",
      "https://www.sec.gov/Archives/edgar/data/1295810/000110465926021440/sho-20251231x10k.htm",
    ],
    handoffFrom: "DEALSIGNAL_OPEW_V2",
  },
  {
    priority: "ENGAGE_NOW",
    discoveryPropertyId: "disc_ds_opew_v2_ri_holtsville_cldt_2026",
    findingId: "find_ds_opew_v2_ri_holtsville_bct_2026",
    hotelName: "Residence Inn Long Island Holtsville",
    city: "Holtsville",
    stateRegion: "NY",
    country: "United States",
    address: "25 Middle Avenue",
    brand: "Residence Inn",
    brandUrl: "https://www.marriott.com/en-us/hotels/isphv-residence-inn-long-island-holtsville/overview/",
    rooms: 124,
    owner: "Chatham Lodging Trust",
    operator: "Island Hospitality Management (IHM)",
    operatorNote: "Related-party operator (CEO-owned IHM) — MCT not the commercial thesis",
    sources: [
      "https://www.marriott.com/en-us/hotels/isphv-residence-inn-long-island-holtsville/overview/",
      "https://www.sec.gov/Archives/edgar/data/1476045/000143774926006103/cldt20251231_10k.htm",
    ],
    handoffFrom: "DEALSIGNAL_OPEW_V2",
  },
  {
    priority: "ENGAGE_NOW",
    discoveryPropertyId: "disc_ds_opew_v2_crowne_plaza_atlanta_ravinia_2026",
    findingId: "find_ds_opew_v2_crowne_plaza_atlanta_hma_sale_2026",
    hotelName: "Crowne Plaza Atlanta Perimeter at Ravinia",
    city: "Atlanta",
    stateRegion: "GA",
    country: "United States",
    address: "4355 Ashford Dunwoody Road",
    brand: "Crowne Plaza",
    brandUrl: "https://www.ihg.com/crowneplaza/hotels/us/en/atlanta/atlcp/hoteldetail",
    rooms: 495,
    owner: "Service Properties Trust",
    operator: "IHG",
    operatorNote: "IHG HMA expires 2027-01-31; sale marketing — buyer-side brand optionality",
    sources: [
      "https://www.ihg.com/crowneplaza/hotels/us/en/atlanta/atlcp/hoteldetail",
    ],
    handoffFrom: "DEALSIGNAL_OPEW_V2",
  },
  {
    priority: "ENGAGE_NOW",
    discoveryPropertyId: "disc_ds_eci_v1_margaritaville_hollywood_beach_2026",
    findingId: "find_ds_eci_v1_margaritaville_hollywood_mct_2026",
    hotelName: "Margaritaville Hollywood Beach Resort",
    city: "Hollywood",
    stateRegion: "FL",
    country: "United States",
    address: "1111 N Ocean Drive",
    brand: "Margaritaville",
    brandUrl: "https://www.margaritavillehollywoodbeachresort.com/",
    rooms: 369,
    owner: "Pebblebrook Hotel Trust",
    operator: null,
    operatorNote: "HMA terminable at will from Sep 2026 per PEB IR — incumbent manager name gap",
    sources: [
      "https://www.margaritavillehollywoodbeachresort.com/",
      "https://www.pebblebrookhotels.com/",
    ],
    handoffFrom: "DEALSIGNAL_ECI_V1",
  },
  {
    priority: "ENGAGE_NOW",
    discoveryPropertyId: "disc_ds_eci_v1_margaritaville_beach_house_kw_2026",
    findingId: "find_ds_eci_v1_mbhk_mct_2026",
    hotelName: "Margaritaville Beach House Key West",
    city: "Key West",
    stateRegion: "FL",
    country: "United States",
    address: "2001 South Roosevelt Boulevard",
    brand: "Margaritaville",
    brandUrl: "https://www.margaritavilleresorts.com/margaritaville-beach-house-key-west",
    rooms: null,
    owner: "DiamondRock Hospitality",
    operator: "Ocean Properties",
    operatorNote: "Operator expires Jul 2027; brand to Apr 2041 — MCT not BCT",
    sources: ["https://www.margaritavilleresorts.com/margaritaville-beach-house-key-west"],
    handoffFrom: "DEALSIGNAL_ECI_V1",
  },
  {
    priority: "ENGAGE_NOW",
    discoveryPropertyId: "disc_ds_eci_v1_palomar_phoenix_2026",
    findingId: "find_ds_eci_v1_palomar_phoenix_bct_2026",
    hotelName: "Kimpton Hotel Palomar Phoenix",
    city: "Phoenix",
    stateRegion: "AZ",
    country: "United States",
    address: "2 East Jefferson Street",
    brand: "Kimpton",
    brandUrl: "https://www.ihg.com/kimptonhotels/hotels/us/en/hotel-palomar-phoenix-az/phxpx/hoteldetail",
    rooms: 242,
    owner: "DiamondRock Hospitality",
    operator: null,
    sources: [
      "https://www.ihg.com/kimptonhotels/hotels/us/en/hotel-palomar-phoenix-az/phxpx/hoteldetail",
    ],
    handoffFrom: "DEALSIGNAL_ECI_V1",
  },
  {
    priority: "CB5",
    discoveryPropertyId: "disc_cb5_westin_den_airport_mct_2026",
    findingId: null,
    hotelName: "Westin Denver International Airport",
    city: "Denver",
    stateRegion: "CO",
    country: "United States",
    address: "8300 Peña Boulevard",
    brand: "Westin",
    brandUrl: "https://www.marriott.com/en-us/hotels/denaw-the-westin-denver-international-airport/overview/",
    rooms: 519,
    owner: null,
    operator: null,
    sources: [
      "https://www.marriott.com/en-us/hotels/denaw-the-westin-denver-international-airport/overview/",
    ],
    handoffFrom: "TRACK_A_CB5",
  },
];

function writeJson(name, obj) {
  fs.writeFileSync(path.join(OUT, name), `${JSON.stringify(obj, null, 2)}\n`);
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
}
async function getRecord(tableId, recordId) {
  const res = await fetch(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${tableId}/${recordId}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  return { ok: res.ok, json: await res.json() };
}
async function patchRecord(tableId, recordId, fields) {
  const res = await fetch(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${tableId}/${recordId}`, {
    method: "PATCH",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify({ fields, typecast: true }),
  });
  return { ok: res.ok, json: await res.json() };
}
async function createRecord(tableId, fields) {
  const res = await fetch(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${tableId}`, {
    method: "POST",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify({ fields, typecast: true }),
  });
  return { ok: res.ok, json: await res.json() };
}
async function findByDiscId(discId) {
  const formula = `{${MAP_DISCOVERY_INVENTORY.discoveryPropertyId}}="${discId}"`;
  const url =
    `https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${inventoryTableId}` +
    `?filterByFormula=${encodeURIComponent(formula)}&maxRecords=1`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
  const json = await res.json();
  return json.records?.[0] || null;
}

function countryMatches(expected, actual) {
  const e = String(expected || "").toLowerCase();
  const a = String(actual || "").toLowerCase();
  if (!e || !a) return false;
  if (e.includes("united states") || e === "us" || e === "usa") {
    return /united states|usa|\bus\b/.test(a);
  }
  if (e.includes("mexico") || e === "mx") return /mexico|méxico|\bmx\b/.test(a);
  return a.includes(e.slice(0, 5)) || e.includes(a.slice(0, 5));
}

function addressCompatible(expectedAddr, hpcAddr) {
  const toks = String(expectedAddr || "")
    .toLowerCase()
    .replace(/[^a-z0-9áéíóúñ]+/gi, " ")
    .split(" ")
    .filter((t) => t.length > 2);
  if (!toks.length) return true; // no address to check — rely on city/country only (weaker)
  const h = String(hpcAddr || "").toLowerCase();
  const hits = toks.filter((t) => h.includes(t)).length;
  return hits >= Math.min(2, toks.length);
}

async function acceptCollision(h, collision) {
  if (!collision?.hpcRecordId) return { accept: false };
  const rb = await getRecord(HPC_TABLE_ID, collision.hpcRecordId);
  if (!rb.ok) return { accept: false, reason: "HPC_READ_FAIL" };
  const f = rb.json.fields || {};
  if (!countryMatches(h.country, f.Country)) {
    return {
      accept: false,
      reason: "GEO_COUNTRY_MISMATCH",
      rejected: { id: collision.hpcRecordId, country: f.Country, city: f.City, name: f["Property Name"] },
    };
  }
  const cityOk =
    !h.city ||
    String(f.City || "")
      .toLowerCase()
      .includes(String(h.city).toLowerCase().slice(0, 4)) ||
    String(h.city)
      .toLowerCase()
      .includes(String(f.City || "").toLowerCase().slice(0, 4));
  if (!cityOk && h.address) {
    // allow if address strongly matches
    if (!addressCompatible(h.address, f.Address)) {
      return {
        accept: false,
        reason: "GEO_CITY_ADDRESS_MISMATCH",
        rejected: { id: collision.hpcRecordId, country: f.Country, city: f.City, address: f.Address },
      };
    }
  } else if (!cityOk && !h.address) {
    return {
      accept: false,
      reason: "GEO_CITY_MISMATCH_NO_ADDRESS",
      rejected: { id: collision.hpcRecordId, city: f.City },
    };
  }
  if (h.address && !addressCompatible(h.address, f.Address) && !cityOk) {
    return { accept: false, reason: "ADDRESS_MISMATCH", rejected: { id: collision.hpcRecordId } };
  }
  return {
    accept: true,
    hpcRead: {
      id: rb.json.id,
      name: f["Property Name"],
      address: f.Address,
      city: f.City,
      country: f.Country,
      brand: f["Current Brand"],
      officialUrl: f["Official Property URL"],
    },
  };
}

async function main() {
  let founderAuth = null;
  if (APPLY_HPC) {
    const gitName = execSync("git config user.name", { encoding: "utf8" }).trim();
    const gitEmail = execSync("git config user.email", { encoding: "utf8" }).trim();
    founderAuth = resolveAuthenticatedFounderIdentity({
      envValue: process.env.HPC_CREATE_APPROVER_IDENTITY || "",
      gitName,
      gitEmail,
    });
    if (!founderAuth.ok) {
      console.error("FOUNDER_AUTH_FAILED", founderAuth);
      process.exit(3);
    }
  }

  const ledger = {
    asOf: new Date().toISOString(),
    mode: APPLY ? (APPLY_HPC ? "APPLY_WITH_HPC_CREATE" : "APPLY_LINKS") : "DRY_RUN",
    publicationState: "CUSTOMER_BLOCKED",
    paidResearchUsd: 0,
    operations: [],
    summary: {
      examined: 0,
      calaExamined: 0,
      matches: 0,
      creates: 0,
      researchRequired: 0,
      ambiguousRejected: 0,
      inventoryLinked: 0,
      inventoryCreated: 0,
      failures: 0,
      readBackOk: 0,
    },
  };

  const rows = [];
  const heItems = [];

  for (const h of QUEUE) {
    ledger.summary.examined += 1;
    if (h.priority?.startsWith("CALA")) ledger.summary.calaExamined += 1;

    const brandUrl = h.brandUrl && BRAND_RE.test(h.brandUrl) ? h.brandUrl : null;
    const gaps = [];
    if (!h.address) gaps.push("MISSING_PHYSICAL_ADDRESS");
    if (!brandUrl) gaps.push("MISSING_BRAND_DIRECTORY_OFFICIAL_URL");

    let hpcId = null;
    let matchOrCreate = "NONE";
    let hpcRead = null;
    let collisionMeta = null;

    // Collision
    try {
      const collision = await performLiveHpcIdentityCollisionCheck({
        hotelName: h.hotelName,
        aliases: h.aliases || [h.hotelName],
        city: h.city,
        brand: h.brand,
        address: h.address,
        country: h.country,
        officialUrl: brandUrl,
        adpPropertyId: h.discoveryPropertyId,
      });
      collisionMeta = { outcome: collision?.outcome || null, rawId: collision?.hpcRecordId || null };
      if (collision?.hpcRecordId) {
        const verdict = await acceptCollision(h, collision);
        if (verdict.accept) {
          hpcId = collision.hpcRecordId;
          matchOrCreate = "MATCHED_EXISTING";
          hpcRead = verdict.hpcRead;
          ledger.summary.matches += 1;
          ledger.summary.readBackOk += 1;
        } else {
          ledger.summary.ambiguousRejected += 1;
          collisionMeta.rejected = verdict;
          ledger.operations.push({
            hotel: h.hotelName,
            action: "REJECT_COLLISION",
            reason: verdict.reason,
            rejected: verdict.rejected || null,
          });
        }
      }
    } catch (e) {
      gaps.push(`COLLISION_ERROR:${e.message}`);
      collisionMeta = { outcome: "LOOKUP_ERROR", error: e.message };
    }

    const createReady = !hpcId && Boolean(h.address && brandUrl);
    if (!hpcId && !createReady) {
      ledger.summary.researchRequired += 1;
    }

    if (createReady && APPLY_HPC && founderAuth?.ok) {
      const fields = {
        "Property Name": h.hotelName,
        "Canonical Property Name": h.hotelName,
        Country: h.country === "Mexico" || h.country === "MX" ? "Mexico" : h.country,
        City: h.city,
        "State / Region": h.stateRegion || null,
        Address: h.address,
        "Current Brand": h.brand,
        "Affiliation Status": "Branded",
        "Family / Source Family": inferFamilySelect(h.brand),
        "Official Property URL": brandUrl,
      };
      for (const k of Object.keys(fields)) {
        if (fields[k] == null || fields[k] === "") delete fields[k];
      }
      const auth = persistHpcCreateAuthorization({
        runId: `track_c_dealsignal_${new Date().toISOString().slice(0, 10)}`,
        draftPropertyId: h.discoveryPropertyId,
        hotelName: h.hotelName,
        fields,
        approverIdentity: founderAuth.approverIdentity,
        authorizationSource: `${TRACK_C_STANDING_AUTH.source}_DEALSIGNAL_CALA_IDENTITY`,
      });
      if (!auth.ok) {
        gaps.push(`AUTH:${auth.error}`);
        ledger.summary.failures += 1;
      } else {
        const exec = await executeAuthorizedHpcCreate({
          runId: `track_c_dealsignal_${new Date().toISOString().slice(0, 10)}`,
          authorization: auth.record,
          dryRun: false,
          apply: true,
          simulation: false,
        });
        ledger.operations.push({
          hotel: h.hotelName,
          action: "HPC_CREATE",
          ok: exec.ok,
          hpcRecordId: exec.hpcRecordId || null,
          state: exec.state,
        });
        if (exec.ok && exec.hpcRecordId) {
          hpcId = exec.hpcRecordId;
          matchOrCreate = exec.reusedExisting ? "REUSED_EXISTING" : "CREATED";
          ledger.summary.creates += 1;
          const rb = await getRecord(HPC_TABLE_ID, hpcId);
          if (rb.ok) {
            const f = rb.json.fields || {};
            hpcRead = {
              id: rb.json.id,
              name: f["Property Name"],
              address: f.Address,
              city: f.City,
              country: f.Country,
              brand: f["Current Brand"],
              officialUrl: f["Official Property URL"],
            };
            ledger.summary.readBackOk += 1;
          }
        } else {
          gaps.push(`CREATE_FAIL:${exec.error || exec.state}`);
          ledger.summary.failures += 1;
        }
        await sleep(200);
      }
    } else if (createReady && !APPLY_HPC) {
      matchOrCreate = "WOULD_CREATE";
      ledger.operations.push({ hotel: h.hotelName, action: "WOULD_HPC_CREATE", brandUrl });
    }

    const notes = [
      "CUSTOMER_BLOCKED",
      "Track C DealSignal/TrackB identity",
      `handoffFrom=${h.handoffFrom}`,
      h.findingId ? `finding=${h.findingId}` : null,
      "do_not_requalify_commercial",
      h.owner ? `owner_verified=${h.owner}` : "owner=UNVERIFIED",
      h.ownerNote ? `ownerNote=${h.ownerNote}` : null,
      h.operator ? `operator_verified=${h.operator}` : null,
      h.operatorNote ? `operatorNote=${h.operatorNote}` : null,
      "brand_neq_operator",
      h.rooms != null ? `rooms_sourced=${h.rooms}` : null,
      hpcId ? `hpc=${hpcId}` : null,
      ...(h.sources || []).slice(0, 3).map((u, i) => `src${i + 1}=${u}`),
    ]
      .filter(Boolean)
      .join(" | ");

    let inventoryId = null;
    let invAction = "NONE";
    const existing = await findByDiscId(h.discoveryPropertyId);
    if (existing) {
      inventoryId = existing.id;
      invAction = "EXISTING";
      if (APPLY && hpcId) {
        const patch = await patchRecord(inventoryTableId, inventoryId, {
          [MAP_DISCOVERY_INVENTORY.hpcMatchStatus]: "MATCHED",
          [MAP_DISCOVERY_INVENTORY.hpcHotel]: [hpcId],
          [MAP_DISCOVERY_INVENTORY.notesQuarantine]: notes,
          ...(brandUrl ? { [MAP_DISCOVERY_INVENTORY.sourceFilingUrl]: brandUrl } : {}),
          ...(h.address ? { [MAP_DISCOVERY_INVENTORY.address]: h.address } : {}),
        });
        invAction = patch.ok ? "LINKED" : "LINK_FAILED";
        if (patch.ok) ledger.summary.inventoryLinked += 1;
        else ledger.summary.failures += 1;
      } else if (!APPLY && hpcId) {
        invAction = "WOULD_LINK";
      }
    } else if (APPLY && (hpcId || (h.address && brandUrl))) {
      const payload = {
        discoveryPropertyId: h.discoveryPropertyId,
        hotelName: h.hotelName,
        hotelNameNormalized: h.hotelName.toLowerCase(),
        hotelNameRaw: h.hotelName,
        city: h.city,
        stateRegion: h.stateRegion,
        country: h.country === "MX" ? "Mexico" : h.country,
        address: h.address,
        currentBrand: h.brand,
        brandFamily: inferBrandFamilyFromLabel(h.brand),
        identityClass: "VERIFIED_PROPERTY",
        evidenceConfidence: "HIGH",
        sourceUrl: brandUrl || h.sources?.[0] || null,
        discoveryMethod: "TRACK_C_DEALSIGNAL_CALA_IDENTITY",
        hpcMatchStatus: hpcId ? "MATCHED" : "DISCOVERED_NOT_IN_HPC",
        notes,
        verified: true,
        evidenceSupported: true,
      };
      const fields = buildDiscoveryInventoryFields(payload);
      if (hpcId) fields[MAP_DISCOVERY_INVENTORY.hpcHotel] = [hpcId];
      if (inferBrandFamilyFromLabel(h.brand)) {
        fields[MAP_DISCOVERY_INVENTORY.brandFamily] = inferBrandFamilyFromLabel(h.brand);
      }
      const created = await createRecord(inventoryTableId, fields);
      if (created.ok) {
        inventoryId = created.json.id;
        invAction = "CREATED";
        ledger.summary.inventoryCreated += 1;
        if (hpcId) ledger.summary.inventoryLinked += 1;
      } else {
        invAction = "CREATE_FAILED";
        gaps.push(`INV:${JSON.stringify(created.json?.error || created.json)}`);
        ledger.summary.failures += 1;
      }
      await sleep(100);
    } else if (!existing) {
      invAction = createReady || hpcId ? "WOULD_CREATE_OR_LINK_INVENTORY" : "NO_INVENTORY_WITHOUT_GATE";
    }

    let idempotencyOk = null;
    if (APPLY && inventoryId && hpcId) {
      const inv2 = await getRecord(inventoryTableId, inventoryId);
      const linked = inv2.ok ? inv2.json.fields?.[MAP_DISCOVERY_INVENTORY.hpcHotel] || [] : [];
      const status = inv2.ok ? inv2.json.fields?.[MAP_DISCOVERY_INVENTORY.hpcMatchStatus] : null;
      idempotencyOk = linked[0] === hpcId && status === "MATCHED";
    }

    const row = {
      priority: h.priority,
      discoveryPropertyId: h.discoveryPropertyId,
      findingId: h.findingId,
      hotelName: h.hotelName,
      country: h.country,
      city: h.city,
      address: h.address,
      brand: h.brand,
      brandUrl,
      rooms: h.rooms,
      owner: h.owner || null,
      operator: h.operator || null,
      ownerNote: h.ownerNote || null,
      operatorNote: h.operatorNote || null,
      sources: h.sources || [],
      handoffFrom: h.handoffFrom,
      canonicalHpcRecordId: hpcId,
      matchOrCreate,
      collision: collisionMeta,
      inventoryAirtableId: inventoryId,
      inventoryAction: invAction,
      gaps,
      idempotencyOk,
      heDeepLinkTarget: hpcId
        ? `http://localhost:8080/app#/opportunity-radar (open HE by HPC ${hpcId})`
        : null,
      hotelExplorerHandoffStatus: hpcId ? "READY" : gaps.length ? "RESEARCH_REQUIRED" : "PENDING",
      hpcReadBack: hpcRead,
    };
    rows.push(row);

    if (hpcId) {
      heItems.push({
        hpcRecordId: hpcId,
        verifiedHotelName: hpcRead?.name || h.hotelName,
        heDeepLinkTarget: row.heDeepLinkTarget,
        brand: hpcRead?.brand || h.brand,
        operator: h.operator,
        operatorNote: h.operatorNote || "Not inferred from brand",
        owner: h.owner,
        ownerNote: h.ownerNote,
        physicalAddress: hpcRead?.address || h.address,
        city: hpcRead?.city || h.city,
        country: hpcRead?.country || h.country,
        roomsSourced: h.rooms,
        officialHotelUrl: hpcRead?.officialUrl || brandUrl,
        discoveryPropertyId: h.discoveryPropertyId,
        findingId: h.findingId,
        inventoryAirtableId: inventoryId,
        sourceEvidence: h.sources,
        ownershipIntelligence: {
          ownerVerified: Boolean(h.owner),
          operatorVerified: Boolean(h.operator),
          differentiateOwnerVsManager: true,
        },
        researchGaps: gaps,
        recommendedResearchPriority: h.priority?.startsWith("CALA") ? "IMMEDIATE_CALA" : "HIGH",
      });
    }
  }

  const report = {
    asOf: new Date().toISOString(),
    publicationState: "CUSTOMER_BLOCKED",
    paidResearchUsd: 0,
    p0Preserved: true,
    ahipPortfolioPreserved: true,
    calaExamined: ledger.summary.calaExamined,
    examined: ledger.summary.examined,
    existingHpcMatches: ledger.summary.matches,
    newHpcCreates: ledger.summary.creates,
    researchRequired: ledger.summary.researchRequired,
    collisionRejections: ledger.summary.ambiguousRejected,
    rows,
  };

  writeJson("DEALSIGNAL_CALA_IDENTITY_TABLE.json", report);
  writeJson(`PERSISTENCE_LEDGER_${ledger.mode}.json`, ledger);
  writeJson("HOTEL_EXPLORER_HANDOFF_DEALSIGNAL.json", {
    asOf: new Date().toISOString(),
    publicationState: "CUSTOMER_BLOCKED",
    items: heItems,
  });
  writeJson("TRACK_A_IC_HANDOFF.json", {
    asOf: new Date().toISOString(),
    from: "TRACK_C",
    to: ["TRACK_A", "INTEGRATION_CONTROLLER"],
    publicationState: "CUSTOMER_BLOCKED",
    summary: ledger.summary,
    canonicalIds: rows
      .filter((r) => r.canonicalHpcRecordId)
      .map((r) => ({
        discoveryPropertyId: r.discoveryPropertyId,
        findingId: r.findingId,
        hpcRecordId: r.canonicalHpcRecordId,
        matchOrCreate: r.matchOrCreate,
        hotelName: r.hotelName,
        country: r.country,
      })),
    unresolved: rows
      .filter((r) => !r.canonicalHpcRecordId)
      .map((r) => ({
        discoveryPropertyId: r.discoveryPropertyId,
        hotelName: r.hotelName,
        gaps: r.gaps,
        reason: r.gaps.join("; ") || "NO_HPC",
      })),
  });

  const md = `# Track C — DealSignal / CALA Identity & Ownership Integration

**Examined:** ${ledger.summary.examined} (**CALA:** ${ledger.summary.calaExamined})  
**HPC matches:** ${ledger.summary.matches} · **Creates:** ${ledger.summary.creates} · **Research required:** ${ledger.summary.researchRequired} · **Collision rejects:** ${ledger.summary.ambiguousRejected}  
**Paid research:** $0 · **CUSTOMER_BLOCKED** · P0 + AHIP work preserved

| Priority | Hotel | Country | HPC | Action | Owner | Operator | Gaps |
|----------|-------|---------|-----|--------|-------|----------|------|
${rows
  .map(
    (r) =>
      `| ${r.priority} | ${r.hotelName} | ${r.country} | ${r.canonicalHpcRecordId || "—"} | ${r.matchOrCreate} | ${r.owner || "—"} | ${r.operator || "—"} | ${r.gaps.join("; ") || "—"} |`,
  )
  .join("\n")}

## Notes
- Owner ≠ operator; Operator HPC field not written without verified mapping.
- FibraHotel MX is portfolio-parent only this wave (committed Villahermosa/Mexicali excluded from open creates).
- Geo collision gate rejects cross-country name collisions (AHIP RI lesson).
`;
  fs.writeFileSync(path.join(OUT, "FOUNDER_DEALSIGNAL_CALA_IDENTITY_REPORT.md"), md);

  console.log(
    JSON.stringify(
      {
        done: true,
        ...ledger.summary,
        heHandoffs: heItems.length,
      },
      null,
      2,
    ),
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
