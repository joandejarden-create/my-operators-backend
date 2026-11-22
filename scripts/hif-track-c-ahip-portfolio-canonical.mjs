#!/usr/bin/env node
/**
 * Track C — AHIP remaining portfolio identity expansion (Aimbridge MCT parent).
 * Parent inventory: recWQQKIVI89cJnSV · Finding: recxh3z8kPr3I8n1r
 * Does NOT create per-hotel commercial opportunities (Track A owns qualification).
 * Does NOT invent a single fictional portfolio hotel HPC.
 * P0 five remain higher priority. Exa ≤$10 cumulative. CUSTOMER_BLOCKED.
 *
 * Portfolio SoT: AHIP AIF YE2025 (30 hotels as of ~2026-03-30) minus completed
 * 2026 dispositions through 2026-11-13 → 19 remaining (matches ahipreit.com "19 Hotels").
 * MD&A Q3 2026: Aimbridge/ONE Lodging management extended to 2027-04-30.
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
import { createExaContractTimingClient } from "../lib/hotel-intelligence/sec-edgar/exa-contract-timing-client-v1.js";
import { createContractTimingV2Budget } from "../lib/hotel-intelligence/sec-edgar/contract-timing-v2-budget-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) {
    dotenv.config({ path: p, override: false });
    break;
  }
}
const APPLY = process.argv.includes("--apply");
const APPLY_HPC = process.argv.includes("--apply-hpc-create");
const OUT = path.join(ROOT, "reports/sec-edgar-hif-track-c-ahip-portfolio-canonical");
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

const PARENT = {
  inventoryAirtableId: "recWQQKIVI89cJnSV",
  findingAirtableId: "recxh3z8kPr3I8n1r",
  discoveryPropertyId: "disc_cb4_ahip_remaining_portfolio_mct_2026",
  findingId: "find_cb4_ahip_aimbridge_mct_2026",
  terminationDate: "2027-04-30",
  currentOperator: "ONE Lodging Management LLC (Aimbridge Hospitality subsidiary)",
  owner: "American Hotel Income Properties REIT LP (AHIP)",
  sourceAif:
    "https://newsfile.futunn.com/public/NN-PersistNoticeAttachment/7781/20260331/SEDAR_PLUS/CSA_SEDAR_PLUS_NOTICE_RECORD_ID_2365254.pdf",
  sourceSettlement:
    "https://www.globenewswire.com/news-release/2026/04/20/3276955/0/en/american-hotel-income-properties-reit-lp-announces-settlement-with-its-hotel-management-company.html",
  sourceMdaQ3: "https://www.otcmarkets.com/filing/608809/content",
};

/** Completed 2026 dispositions (excluded from affected remaining portfolio). */
const SOLD_2026 = [
  "TownePlace Suites Pittsburgh Airport Robinson Township",
  "Embassy Suites Cleveland Rockside",
  "Embassy Suites Columbus Dublin",
  "SpringHill Suites Arundel Mills BWI Airport",
  "Hilton Garden Inn Baltimore White Marsh",
  "Homewood Suites Dover Rockaway",
  "Homewood Suites Allentown West Fogelsville",
  "Embassy Suites Tempe",
  "Holiday Inn Express and Suites Fort Myers East",
  "Residence Inn Chattanooga",
  "TownePlace Suites Chattanooga",
  "Fairfield Inn & Suites Jacksonville West",
];

/**
 * Remaining 19 AHIP-owned hotels as of ~2026-11-21 (AIF 30 − 11 sold from AIF list;
 * TPS Pittsburgh sold before AIF date). Room sum 2,018 = Q3 2,303 − Q4 closed 285.
 * Operator relationship: Aimbridge/ONE Lodging until 2027-04-30 (portfolio MHMA).
 */
const REMAINING = [
  {
    slug: "hampton_asheboro_nc",
    hotelName: "Hampton Inn Asheboro",
    brand: "Hampton Inn",
    address: "1137 E. Dixie Drive",
    city: "Asheboro",
    state: "NC",
    rooms: 111,
    brandUrl: "https://www.hilton.com/en/hotels/asbnchx-hampton-asheboro/",
  },
  {
    slug: "courtyard_bismarck_nd",
    hotelName: "Courtyard by Marriott Bismarck North",
    brand: "Courtyard",
    address: "3319 North 14th Street",
    city: "Bismarck",
    state: "ND",
    rooms: 89,
    brandUrl: "https://www.marriott.com/en-us/hotels/biscn-courtyard-bismarck-north/overview/",
  },
  {
    slug: "embassy_covington_ky",
    hotelName: "Embassy Suites by Hilton Cincinnati RiverCenter",
    brand: "Embassy Suites",
    address: "10 E. Rivercenter Boulevard",
    city: "Covington",
    state: "KY",
    rooms: 227,
    brandUrl: "https://www.hilton.com/en/hotels/cvgcves-embassy-suites-cincinnati-rivercenter/",
  },
  {
    slug: "hampton_emporia_va",
    hotelName: "Hampton Inn Emporia",
    brand: "Hampton Inn",
    address: "898 Wiggins Road",
    city: "Emporia",
    state: "VA",
    rooms: 85,
    brandUrl: "https://www.hilton.com/en/hotels/emvwehx-hampton-emporia/",
  },
  {
    slug: "hampton_suites_arundel_mills_md",
    hotelName: "Hampton Inn & Suites Arundel Mills/Baltimore",
    brand: "Hampton Inn & Suites",
    address: "7027 Arundel Mills Circle",
    city: "Hanover",
    state: "MD",
    rooms: 130,
    brandUrl: "https://www.hilton.com/en/hotels/balamhx-hampton-suites-arundel-mills-baltimore/",
  },
  {
    slug: "hampton_harrisonburg_south_va",
    hotelName: "Hampton Inn Harrisonburg - South",
    brand: "Hampton Inn",
    address: "43 Covenant Drive",
    city: "Harrisonburg",
    state: "VA",
    rooms: 90,
    brandUrl: "https://www.hilton.com/en/hotels/hsbsohx-hampton-harrisonburg-south/",
  },
  {
    slug: "staybridge_midland_tx",
    hotelName: "Staybridge Suites Midland",
    brand: "Staybridge Suites",
    address: "1505 Catalina Drive",
    city: "Midland",
    state: "TX",
    rooms: 98,
    brandUrl: "https://www.ihg.com/staybridge/hotels/us/en/midland/mafsb/hoteldetail",
  },
  {
    slug: "home2_midland_tx",
    hotelName: "Home2 Suites by Hilton Midland",
    brand: "Home2 Suites",
    address: "1401 Catalina Drive",
    city: "Midland",
    state: "TX",
    rooms: 93,
    brandUrl: "https://www.hilton.com/en/hotels/mafdcht-home2-suites-midland/",
  },
  {
    slug: "hgi_milford_ct",
    hotelName: "Hilton Garden Inn Milford",
    brand: "Hilton Garden Inn",
    address: "291 Old Gate Lane",
    city: "Milford",
    state: "CT",
    rooms: 120,
    brandUrl: "https://www.hilton.com/en/hotels/mifgigi-hilton-garden-inn-milford/",
  },
  {
    slug: "residence_inn_mt_laurel_nj",
    hotelName: "Residence Inn by Marriott Mt. Laurel at Bishop's Gate",
    brand: "Residence Inn",
    address: "1000 Bishops Gate Boulevard",
    addressAlias: "1000 Sunburst Lane (brand directory)",
    city: "Mount Laurel",
    state: "NJ",
    rooms: 144,
    brandUrl: "https://www.marriott.com/en-us/hotels/phlrl-residence-inn-mt-laurel-at-bishops-gate/overview/",
  },
  {
    slug: "residence_inn_neptune_nj",
    hotelName: "Residence Inn by Marriott Neptune at Gateway Centre",
    brand: "Residence Inn",
    address: "230 Jumping Brook Road",
    city: "Neptune",
    state: "NJ",
    rooms: 105,
    brandUrl: "https://www.marriott.com/en-us/hotels/ewrnp-residence-inn-neptune-at-gateway-centre/overview/",
  },
  {
    slug: "fairfield_orlando_ocoee_fl",
    hotelName: "Fairfield Inn & Suites Orlando Ocoee",
    brand: "Fairfield Inn & Suites",
    address: "10971 W. Colonial Drive",
    city: "Ocoee",
    state: "FL",
    rooms: 80,
    brandUrl: "https://www.marriott.com/en-us/hotels/mcoof-fairfield-inn-and-suites-orlando-ocoee/overview/",
  },
  {
    slug: "fairfield_pittsburgh_robinson_pa",
    hotelName: "Fairfield Inn & Suites Pittsburgh Airport/Robinson Township",
    brand: "Fairfield Inn & Suites",
    address: "1004 Sutherland Drive",
    city: "Pittsburgh",
    state: "PA",
    rooms: 103,
    brandUrl:
      "https://www.marriott.com/en-us/hotels/pitwf-fairfield-inn-and-suites-pittsburgh-airport-robinson-township/overview/",
  },
  {
    slug: "home2_san_angelo_tx",
    hotelName: "Home2 Suites by Hilton San Angelo",
    brand: "Home2 Suites",
    address: "2569 Southwest Boulevard",
    city: "San Angelo",
    state: "TX",
    rooms: 93,
    brandUrl: "https://www.hilton.com/en/hotels/sjtwhht-home2-suites-san-angelo/",
  },
  {
    slug: "residence_inn_san_angelo_tx",
    hotelName: "Residence Inn by Marriott San Angelo",
    brand: "Residence Inn",
    address: "2545 Southwest Boulevard",
    city: "San Angelo",
    state: "TX",
    rooms: 92,
    brandUrl: "https://www.marriott.com/en-us/hotels/sjtri-residence-inn-san-angelo/overview/",
  },
  {
    slug: "fairfield_south_hill_va",
    hotelName: "Fairfield Inn & Suites South Hill I-85",
    brand: "Fairfield Inn & Suites",
    address: "150 Arnold Drive",
    city: "South Hill",
    state: "VA",
    rooms: 68,
    brandUrl: "https://www.marriott.com/en-us/hotels/ricst-fairfield-inn-and-suites-south-hill-i-85/overview/",
  },
  {
    slug: "courtyard_tampa_north_fl",
    hotelName: "Courtyard by Marriott Tampa North/I-75 Fletcher",
    brand: "Courtyard",
    address: "13575 Cypress Glen Lane",
    city: "Tampa",
    state: "FL",
    rooms: 81,
    brandUrl: "https://www.marriott.com/en-us/hotels/tpacy-courtyard-tampa-north-i-75-fletcher/overview/",
  },
  {
    slug: "fairfield_titusville_fl",
    hotelName: "Fairfield Inn & Suites Titusville Kennedy Space Center",
    brand: "Fairfield Inn & Suites",
    address: "4735 Helen Hauser Boulevard",
    city: "Titusville",
    state: "FL",
    rooms: 96,
    brandUrl:
      "https://www.marriott.com/en-us/hotels/tixfi-fairfield-inn-and-suites-titusville-kennedy-space-center/overview/",
  },
  {
    slug: "courtyard_wall_nj",
    hotelName: "Courtyard by Marriott Wall at Monmouth Shores Corporate Park",
    brand: "Courtyard",
    address: "1302 Campus Parkway",
    city: "Wall Township",
    state: "NJ",
    rooms: 113,
    brandUrl:
      "https://www.marriott.com/en-us/hotels/ewrwt-courtyard-wall-at-monmouth-shores-corporate-park/overview/",
  },
];

const BRAND_RE =
  /marriott\.com|hilton\.com|ihg\.com|hyatt\.com|fourseasons\.com|wyndham|choicehotels|bestwestern|accor|fairmont|radissonhotels/i;

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

async function main() {
  const spendBefore = (() => {
    const p = path.join(ROOT, "data/sec-edgar/hif-track-c-wave3b-exa-budget-ledger.json");
    if (!fs.existsSync(p)) return 0;
    return Number(JSON.parse(fs.readFileSync(p, "utf8")).spentUsd || 0);
  })();
  const budget = createContractTimingV2Budget({
    ceilingUsd: 10,
    ledgerPath: path.join(ROOT, "data/sec-edgar/hif-track-c-wave3b-exa-budget-ledger.json"),
    cwd: ROOT,
  });
  const exa = createExaContractTimingClient({
    budget,
    cacheDir: path.join(ROOT, "data/sec-edgar/hif-track-c-wave3b-exa-cache"),
  });

  const parentRb = await getRecord(inventoryTableId, PARENT.inventoryAirtableId);
  const parentFindingOk = parentRb.ok;

  let founderAuth = null;
  if (APPLY_HPC) {
    let gitName = "";
    let gitEmail = "";
    try {
      gitName = execSync("git config user.name", { encoding: "utf8" }).trim();
      gitEmail = execSync("git config user.email", { encoding: "utf8" }).trim();
    } catch {
      /* ignore */
    }
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
    mode: APPLY ? (APPLY_HPC ? "APPLY_WITH_HPC_CREATE" : "APPLY_INVENTORY") : "DRY_RUN",
    publicationState: "CUSTOMER_BLOCKED",
    parent: PARENT,
    soldExcluded: SOLD_2026,
    remainingCount: REMAINING.length,
    roomsSupportedSum: REMAINING.reduce((s, h) => s + (h.rooms || 0), 0),
    exaSpendBeforeUsd: spendBefore,
    operations: [],
    summary: {
      inventoryCreates: 0,
      inventoryNoops: 0,
      hpcMatches: 0,
      hpcCreates: 0,
      researchRequired: 0,
      linkOk: 0,
      failures: 0,
      readBackOk: 0,
    },
  };

  const rows = [];
  const heItems = [];

  for (const h of REMAINING) {
    const discoveryPropertyId = `disc_cb4_ahip_${h.slug}_2026`;
    let brandUrl = h.brandUrl || null;
    let exaRequests = 0;
    let exaCost = 0;

    if (!brandUrl || !BRAND_RE.test(brandUrl)) {
      if (budget.remaining() > 0.02) {
        const search = await exa.search({
          query: `${h.hotelName} ${h.city} ${h.state} ${h.address} official hotel`,
          type: "lite",
          numResults: 5,
        });
        exaRequests += 1;
        if (search.ok && !search.fromCache) exaCost += Number(search.costDollars?.total || 0.005);
        const hit = (search.results || []).find((r) => BRAND_RE.test(r.url || ""));
        if (hit?.url) brandUrl = hit.url;
      }
    }

    const createReadyUrl = brandUrl && BRAND_RE.test(brandUrl) ? brandUrl : null;
    let hpcId = null;
    let matchOrCreate = "NONE";
    let collision = null;
    let hpcRead = null;
    let gaps = [];

    try {
      collision = await performLiveHpcIdentityCollisionCheck({
        hotelName: h.hotelName,
        aliases: [h.hotelName, h.brand].filter(Boolean),
        city: h.city,
        brand: h.brand,
        address: h.address,
        country: "United States",
        officialUrl: createReadyUrl,
        adpPropertyId: discoveryPropertyId,
      });
      if (collision?.hpcRecordId) {
        const rb = await getRecord(HPC_TABLE_ID, collision.hpcRecordId);
        const f = rb.ok ? rb.json.fields || {} : {};
        const countryOk = /united states|usa/i.test(String(f.Country || ""));
        const cityOk =
          !h.city ||
          String(f.City || "")
            .toLowerCase()
            .includes(String(h.city).toLowerCase().slice(0, 5));
        const addrTok = String(h.address || "")
          .toLowerCase()
          .replace(/[^a-z0-9]+/g, " ")
          .split(" ")
          .filter((t) => t.length > 2);
        const hpcAddr = String(f.Address || "").toLowerCase();
        const addrHit = addrTok.filter((t) => hpcAddr.includes(t)).length;
        const addressOk = !addrTok.length || addrHit >= Math.min(2, addrTok.length);
        if (countryOk && cityOk && addressOk) {
          hpcId = collision.hpcRecordId;
          matchOrCreate = "MATCHED_EXISTING";
          ledger.summary.hpcMatches += 1;
          hpcRead = {
            id: rb.json.id,
            name: f["Property Name"],
            address: f.Address,
            brand: f["Current Brand"],
            officialUrl: f["Official Property URL"],
            country: f.Country,
          };
          ledger.summary.readBackOk += 1;
        } else {
          collision = {
            ...collision,
            outcome: "REJECTED_FALSE_POSITIVE_GEO_MISMATCH",
            rejectedHpcId: collision.hpcRecordId,
            rejectedCountry: f.Country || null,
            rejectedCity: f.City || null,
          };
          ledger.operations.push({
            hotel: h.hotelName,
            action: "REJECT_COLLISION_GEO_MISMATCH",
            rejectedHpcId: collision.hpcRecordId,
            rejectedCountry: f.Country || null,
          });
        }
      }
    } catch (e) {
      collision = { outcome: "LOOKUP_ERROR", error: e.message };
      gaps.push(`COLLISION_ERROR:${e.message}`);
    }

    if (!hpcId && createReadyUrl && h.address) {
      if (APPLY_HPC && founderAuth?.ok) {
        const fields = {
          "Property Name": h.hotelName,
          "Canonical Property Name": h.hotelName,
          Country: "United States",
          City: h.city,
          "State / Region": h.state,
          Address: h.address,
          "Current Brand": h.brand,
          "Affiliation Status": "Branded",
          "Family / Source Family": inferFamilySelect(h.brand),
          "Official Property URL": createReadyUrl,
        };
        const auth = persistHpcCreateAuthorization({
          runId: `track_c_ahip_${new Date().toISOString().slice(0, 10)}`,
          draftPropertyId: discoveryPropertyId,
          hotelName: h.hotelName,
          fields,
          approverIdentity: founderAuth.approverIdentity,
          authorizationSource: `${TRACK_C_STANDING_AUTH.source}_AHIP_PORTFOLIO_CANONICAL`,
        });
        if (!auth.ok) {
          gaps.push(`AUTH_FAIL:${auth.error}`);
          ledger.summary.failures += 1;
        } else {
          const exec = await executeAuthorizedHpcCreate({
            runId: `track_c_ahip_${new Date().toISOString().slice(0, 10)}`,
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
            error: exec.error || null,
          });
          if (exec.ok && exec.hpcRecordId) {
            hpcId = exec.hpcRecordId;
            matchOrCreate = exec.reusedExisting ? "REUSED_EXISTING" : "CREATED";
            ledger.summary.hpcCreates += 1;
            const rb = await getRecord(HPC_TABLE_ID, hpcId);
            if (rb.ok) {
              hpcRead = {
                id: rb.json.id,
                name: rb.json.fields?.["Property Name"],
                address: rb.json.fields?.Address,
                brand: rb.json.fields?.["Current Brand"],
                officialUrl: rb.json.fields?.["Official Property URL"],
              };
              ledger.summary.readBackOk += 1;
            }
          } else {
            gaps.push(`CREATE_FAIL:${exec.error || exec.state}`);
            ledger.summary.failures += 1;
          }
          await sleep(200);
        }
      } else if (!APPLY_HPC) {
        matchOrCreate = "WOULD_CREATE";
        ledger.operations.push({ hotel: h.hotelName, action: "WOULD_HPC_CREATE", brandUrl: createReadyUrl });
      }
    } else if (!hpcId && !createReadyUrl) {
      gaps.push("MISSING_BRAND_DIRECTORY_OFFICIAL_URL");
      ledger.summary.researchRequired += 1;
    }

    const notes = [
      "CUSTOMER_BLOCKED",
      "Track C AHIP portfolio component",
      `parentInventory=${PARENT.inventoryAirtableId}`,
      `parentDisc=${PARENT.discoveryPropertyId}`,
      `parentFinding=${PARENT.findingId}`,
      `parentFindingAirtable=${PARENT.findingAirtableId}`,
      "NOT_INDIVIDUAL_COMMERCIAL_OPPORTUNITY",
      "do_not_requalify_without_Track_A",
      `operator=${PARENT.currentOperator}`,
      `owner=${PARENT.owner}`,
      `mct_termination=${PARENT.terminationDate}`,
      `rooms_aif=${h.rooms}`,
      h.addressAlias ? `addressAlias=${h.addressAlias}` : null,
      hpcId ? `hpc=${hpcId}` : null,
    ]
      .filter(Boolean)
      .join(" | ");

    const invPayload = {
      discoveryPropertyId,
      hotelName: h.hotelName,
      hotelNameNormalized: h.hotelName.toLowerCase(),
      hotelNameRaw: h.hotelName,
      city: h.city,
      stateRegion: h.state,
      country: "United States",
      address: h.address,
      currentBrand: h.brand,
      brandFamily: inferBrandFamilyFromLabel(h.brand),
      identityClass: "VERIFIED_PROPERTY",
      evidenceConfidence: "HIGH",
      sourceUrl: createReadyUrl || PARENT.sourceSettlement,
      discoveryMethod: "TRACK_C_AHIP_PORTFOLIO_COMPONENT",
      hpcMatchStatus: hpcId ? "MATCHED" : "DISCOVERED_NOT_IN_HPC",
      notes,
      verified: true,
      evidenceSupported: true,
    };

    let inventoryId = null;
    let invAction = "NONE";
    const existing = await findByDiscId(discoveryPropertyId);
    if (existing) {
      inventoryId = existing.id;
      invAction = "EXISTING";
      ledger.summary.inventoryNoops += 1;
      if (APPLY && hpcId) {
        const patch = await patchRecord(inventoryTableId, inventoryId, {
          [MAP_DISCOVERY_INVENTORY.hpcMatchStatus]: "MATCHED",
          [MAP_DISCOVERY_INVENTORY.hpcHotel]: [hpcId],
          [MAP_DISCOVERY_INVENTORY.notesQuarantine]: notes,
          [MAP_DISCOVERY_INVENTORY.sourceFilingUrl]: createReadyUrl || PARENT.sourceSettlement,
        });
        invAction = patch.ok ? "LINKED_EXISTING" : "LINK_FAILED";
        if (patch.ok) ledger.summary.linkOk += 1;
        else ledger.summary.failures += 1;
      }
    } else if (APPLY) {
      const fields = buildDiscoveryInventoryFields(invPayload);
      if (hpcId) {
        fields[MAP_DISCOVERY_INVENTORY.hpcMatchStatus] = "MATCHED";
        fields[MAP_DISCOVERY_INVENTORY.hpcHotel] = [hpcId];
      }
      if (inferBrandFamilyFromLabel(h.brand)) {
        fields[MAP_DISCOVERY_INVENTORY.brandFamily] = inferBrandFamilyFromLabel(h.brand);
      }
      const created = await createRecord(inventoryTableId, fields);
      if (created.ok) {
        inventoryId = created.json.id;
        invAction = "CREATED";
        ledger.summary.inventoryCreates += 1;
        if (hpcId) ledger.summary.linkOk += 1;
        const rb = await getRecord(inventoryTableId, inventoryId);
        if (rb.ok) ledger.summary.readBackOk += 1;
      } else {
        invAction = "CREATE_FAILED";
        gaps.push(`INV_CREATE_FAIL:${JSON.stringify(created.json?.error || created.json)}`);
        ledger.summary.failures += 1;
      }
      await sleep(120);
    } else {
      invAction = "WOULD_CREATE_INVENTORY";
      ledger.summary.inventoryCreates += 1;
    }

    // Idempotency read-back when linked
    let idempotencyOk = null;
    if (APPLY && inventoryId && hpcId) {
      const inv2 = await getRecord(inventoryTableId, inventoryId);
      const linked = inv2.ok ? inv2.json.fields?.[MAP_DISCOVERY_INVENTORY.hpcHotel] || [] : [];
      const status = inv2.ok ? inv2.json.fields?.[MAP_DISCOVERY_INVENTORY.hpcMatchStatus] : null;
      idempotencyOk = Array.isArray(linked) && linked[0] === hpcId && status === "MATCHED";
      ledger.operations.push({
        hotel: h.hotelName,
        action: "IDEMPOTENCY_READBACK",
        ok: idempotencyOk,
        inventoryId,
        hpcId,
      });
    }

    const row = {
      discoveryPropertyId,
      hotelName: h.hotelName,
      brand: h.brand,
      address: h.address,
      city: h.city,
      state: h.state,
      roomsAif: h.rooms,
      brandUrl: createReadyUrl,
      currentOperator: PARENT.currentOperator,
      owner: PARENT.owner,
      parentPortfolioLink: {
        inventoryAirtableId: PARENT.inventoryAirtableId,
        findingAirtableId: PARENT.findingAirtableId,
        discoveryPropertyId: PARENT.discoveryPropertyId,
        findingId: PARENT.findingId,
        linkageMethod: "NOTES_PARENT_IDS_NO_PER_HOTEL_COMMERCIAL_FINDING",
      },
      inventoryAirtableId: inventoryId,
      inventoryAction: invAction,
      canonicalHpcRecordId: hpcId,
      matchOrCreate,
      collisionOutcome: collision?.outcome || null,
      hpcReadBack: hpcRead,
      idempotencyOk,
      gaps,
      exaRequests,
      exaCostUsd: exaCost,
      hotelExplorerHandoffStatus: hpcId ? "READY" : gaps.length ? "RESEARCH_REQUIRED" : "PENDING",
    };
    rows.push(row);

    if (hpcId) {
      heItems.push({
        hpcRecordId: hpcId,
        verifiedHotelName: hpcRead?.name || h.hotelName,
        brand: hpcRead?.brand || h.brand,
        operator: null,
        operatorEvidenceNote: `${PARENT.currentOperator} until ${PARENT.terminationDate} — not written as HPC Operator field (brand≠operator; omitted unverified)`,
        physicalAddress: hpcRead?.address || h.address,
        officialHotelUrl: hpcRead?.officialUrl || createReadyUrl,
        roomsAifSupported: h.rooms,
        commercialOpportunityId: PARENT.findingId,
        commercialNote: "Portfolio-level MCT only — do not invent per-hotel opportunity",
        discoveryPropertyId,
        inventoryAirtableId: inventoryId,
        parentInventoryAirtableId: PARENT.inventoryAirtableId,
        parentFindingAirtableId: PARENT.findingAirtableId,
        ownershipEvidence: PARENT.owner,
        contractualTrigger: `Master Hotel Management Agreement termination ${PARENT.terminationDate}`,
        decisionTiming: "Replacement manager retention before 2027-04-30",
        sourceEvidence: createReadyUrl,
        hotelExplorerAvailability: "UNKNOWN_NOT_QUERIED_FROM_TRACK_C",
        missingHotelExplorerAttributes: ["full_profile", "owner_operator_validated", "decision_maker", "PIP_status"],
        recommendedResearchPriority: "HIGH_AFTER_P0_FIVE",
      });
    }
  }

  // Patch parent inventory notes with child count (non-destructive)
  if (APPLY && parentFindingOk) {
    const parentNotes =
      `CUSTOMER_BLOCKED | FIRST_100_MCT | OPEN_OPERATOR_REPLACEMENT | Track C children=${rows.filter((r) => r.inventoryAirtableId).length}/${REMAINING.length} | hpcLinked=${rows.filter((r) => r.canonicalHpcRecordId).length} | roomsSum=${ledger.roomsSupportedSum} | termination=${PARENT.terminationDate} | NOT_A_SINGLE_HOTEL_HPC`;
    await patchRecord(inventoryTableId, PARENT.inventoryAirtableId, {
      [MAP_DISCOVERY_INVENTORY.notesQuarantine]: parentNotes,
    });
  }

  const spendAfter = budget.spentUsd();
  const report = {
    asOf: new Date().toISOString(),
    publicationState: "CUSTOMER_BLOCKED",
    p0Preserved: true,
    verifiedAffectedHotelCount: REMAINING.length,
    roomsSupportedSum: ledger.roomsSupportedSum,
    methodology: {
      aifBaselineHotels: 30,
      sold2026Excluded: SOLD_2026.length,
      websiteCorroboration: "ahipreit.com homepage ~19 Hotels / 10 States",
      mdaQ3Count: "22 hotels / 2,303 rooms as of 2026-09-30; Q4 closes reduce to 19 / 2,018",
      underContractNote:
        "MD&A lists 9 PSAs at 2026-11-13 ($137.3M / 947 keys) unnamed in summary — still owned until close; remain in affected set",
      noFictionalPortfolioHpc: true,
      noPerHotelCommercialOpportunity: true,
    },
    parent: PARENT,
    hpcMatches: ledger.summary.hpcMatches,
    hpcCreates: ledger.summary.hpcCreates,
    researchRequired: rows.filter((r) => r.gaps.length).length,
    inventoryCreates: ledger.summary.inventoryCreates,
    exa: {
      hardCapUsd: 10,
      spendBeforeUsd: spendBefore,
      spendAfterUsd: spendAfter,
      thisRunUsd: Number((spendAfter - spendBefore).toFixed(4)),
      additionalAllocation: false,
    },
    rows,
  };

  writeJson("AHIP_PORTFOLIO_CANONICAL_TABLE.json", report);
  writeJson(`PERSISTENCE_LEDGER_${ledger.mode}.json`, ledger);
  writeJson("HOTEL_EXPLORER_HANDOFF_AHIP.json", {
    asOf: new Date().toISOString(),
    publicationState: "CUSTOMER_BLOCKED",
    priorityNote: "After original five P0 commercial hotels",
    items: heItems,
  });

  const md = `# AHIP Portfolio — Opportunity-First Canonical Identity

**Verified remaining hotels: ${REMAINING.length}** · Rooms (AIF-supported): **${ledger.roomsSupportedSum}**  
**HPC matches: ${ledger.summary.hpcMatches}** · **Creates: ${ledger.summary.hpcCreates}** · **Unresolved: ${rows.filter((r) => r.gaps.length).length}**  
**Exa: $${spendAfter.toFixed(4)} / $10 cumulative** (this run $${Number(spendAfter - spendBefore).toFixed(4)})  
**CUSTOMER_BLOCKED** · Parent MCT finding \`${PARENT.findingId}\` · P0 five preserved above this queue

| Hotel | Rooms | Brand | HPC | Match/Create | Inventory | Gaps |
|-------|------:|-------|-----|--------------|-----------|------|
${rows
  .map(
    (r) =>
      `| ${r.hotelName} | ${r.roomsAif} | ${r.brand} | ${r.canonicalHpcRecordId || "—"} | ${r.matchOrCreate} | ${r.inventoryAction} | ${r.gaps.join("; ") || "—"} |`,
  )
  .join("\n")}

## Portfolio linkage
- Parent inventory \`${PARENT.inventoryAirtableId}\` / finding \`${PARENT.findingAirtableId}\`
- Child notes carry parent IDs; **no** per-hotel commercial findings
- Operator: ${PARENT.currentOperator} until ${PARENT.terminationDate}
`;
  fs.writeFileSync(path.join(OUT, "FOUNDER_AHIP_PORTFOLIO_REPORT.md"), md);

  console.log(
    JSON.stringify(
      {
        done: true,
        verifiedAffected: REMAINING.length,
        rooms: ledger.roomsSupportedSum,
        hpcMatches: ledger.summary.hpcMatches,
        hpcCreates: ledger.summary.hpcCreates,
        inventoryCreates: ledger.summary.inventoryCreates,
        unresolved: rows.filter((r) => r.gaps.length).length,
        heHandoffs: heItems.length,
        exa: report.exa,
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
