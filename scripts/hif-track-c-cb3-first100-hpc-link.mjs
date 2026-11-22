#!/usr/bin/env node
/**
 * Track C — CB3 first-100 BCT/MCT priority queue: verify HPC, link discovery inventory, HE handoff.
 * Does not change commercial qualification. Cumulative Exa budget (no new $10). CUSTOMER_BLOCKED.
 * --apply to patch discovery inventory ↔ HPC links.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import dotenv from "dotenv";
import {
  GRAPH_BASE_ID_CANONICAL,
  HPC_TABLE_ID,
  TABLE_HI_HOTEL_DISCOVERY_INVENTORY,
  MAP_DISCOVERY_INVENTORY,
} from "../lib/hotel-intelligence/sec-edgar/discovery-inventory-airtable-contract-v23.js";
import { performLiveHpcIdentityCollisionCheck } from "../lib/hotel-census/hpc-live-identity-collision-v1.js";
import {
  resolveAuthenticatedFounderIdentity,
  persistHpcCreateAuthorization,
  executeAuthorizedHpcCreate,
} from "../lib/hotel-census/hpc-authorized-create-reuse-v1.js";
import { TRACK_C_STANDING_AUTH, inferFamilySelect } from "../lib/hotel-intelligence/sec-edgar/hif-track-c-universe-expansion-v1.js";
import { execSync } from "node:child_process";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) {
    dotenv.config({ path: p, override: false });
    break;
  }
}
const APPLY = process.argv.includes("--apply");
const APPLY_HPC_CREATE = process.argv.includes("--apply-hpc-create");
const OUT = path.join(ROOT, "reports/sec-edgar-hif-track-c-opportunity-exa-v1");
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

/** IC-qualified first-100 from PR #544 CB3 — do not re-qualify. */
const FIRST100 = [
  {
    hotelName: "Lakeville Inn",
    aliases: ["Lakeville Inn", "Hampton Inn Lakeville"],
    city: "Lakeville",
    stateRegion: "MN",
    country: "United States",
    address: "20851 Keokuk Avenue",
    currentBrand: "Independent",
    historicalBrand: "Hampton Inn",
    officialUrl: "https://visitlakeville.org/explore/stay/hotels/lakeville-inn/",
    brandDirectoryUrl: null,
    opportunityType: "BCT",
    qualificationTier: "FIRST_100_QUALIFIED_OPEN_BCT",
    findingId: "find_cb1_lakeville_bct_brand_search_2026",
    discoveryPropertyId: "disc_cb1_lakeville_inn_mn_2026",
    knownHpcId: null,
  },
  {
    hotelName: "Kimpton Shorebreak Huntington Beach Resort",
    aliases: ["Kimpton Shorebreak", "Shorebreak Huntington Beach"],
    city: "Huntington Beach",
    stateRegion: "CA",
    country: "United States",
    address: "500 Pacific Coast Highway",
    currentBrand: "Kimpton",
    officialUrl:
      "https://www.ihg.com/kimptonhotels/hotels/us/en/shorebreak-hotel-huntington-beach-ca/laxsb/hoteldetail",
    opportunityType: "BCT",
    qualificationTier: "FIRST_100_QUALIFIED_OPEN_BCT",
    findingId: "find_cb2b_shorebreak_huntington_bct_2026",
    discoveryPropertyId: "disc_cb2b_shorebreak_huntington_2026",
    knownHpcId: "reckTzVQUT2H8hbug",
  },
  {
    hotelName: "Courtyard by Marriott Denver Downtown",
    aliases: ["Courtyard Denver Downtown", "Courtyard by Marriott Denver Downtown"],
    city: "Denver",
    stateRegion: "CO",
    country: "United States",
    address: "934 16th Street",
    currentBrand: "Courtyard",
    officialUrl: "https://www.marriott.com/en-us/hotels/dencd-courtyard-denver-downtown/overview/",
    opportunityType: "BCT",
    qualificationTier: "FIRST_100_QUALIFIED_OPEN_BCT",
    findingId: "find_cb2b_courtyard_denver_bct_2026",
    discoveryPropertyId: "disc_cb2b_courtyard_denver_downtown_2026",
    knownHpcId: "recvn447rdN53NgZ7",
  },
  {
    hotelName: "Hilton Trinidad & Conference Centre",
    aliases: ["Hilton Trinidad", "Hilton Trinidad & Conference Centre"],
    city: "Port of Spain",
    country: "Trinidad and Tobago",
    address: "1B Lady Young Road, Belmont",
    currentBrand: "Hilton",
    officialUrl: "https://www.hilton.com/en/hotels/poshihh-hilton-trinidad-and-conference-centre/",
    opportunityType: "MCT",
    qualificationTier: "FIRST_100_QUALIFIED_OPEN_MCT",
    findingId: "find_cfa_hilton_trinidad_mct_hma_sale_2026",
    discoveryPropertyId: "disc_cfa_hilton_trinidad_pos_2026",
    knownHpcId: "recFNfTvcph0fYQnm",
  },
  {
    hotelName: "Four Seasons Hotel Bengaluru at Embassy ONE",
    aliases: ["Four Seasons Hotel Bengaluru", "Embassy ONE", "Embassy One Bengaluru"],
    city: "Bengaluru",
    country: "India",
    address: "8 Bellary Road, Ganganagar",
    currentBrand: "Four Seasons",
    officialUrl: "https://www.fourseasons.com/bengaluru/",
    opportunityType: "MCT",
    qualificationTier: "FIRST_100_QUALIFIED_OPEN_MCT",
    findingId: "find_cb3_embassy_one_bengaluru_mct_2026",
    discoveryPropertyId: "disc_cb3_embassy_one_bengaluru_2026",
    knownHpcId: "recD8mIAMUyQ9ilka",
  },
];

const EXPANDED = [
  {
    hotelName: "Legacy Hotel",
    city: "Green Bay",
    stateRegion: "WI",
    country: "United States",
    address: "1004 Brett Favre Pass",
    opportunityType: "BCT_INTELLIGENCE",
    qualificationTier: "PREFERRED_BRAND_NOT_OPEN_COMPETITIVE",
    discoveryPropertyId: "disc_cb2_legacy_hotel_green_bay_2026",
    findingId: "find_cb2_legacy_hotel_green_bay_bct_2026",
    knownHpcId: "recRNGJk5lpaHeeo6",
    doNotPromote: true,
  },
  {
    hotelName: "Gideon Putnam Hotel",
    city: "Saratoga Springs",
    stateRegion: "NY",
    country: "United States",
    address: "24 Gideon Putnam Road",
    officialUrl: "https://www.gideonputnam.com/",
    opportunityType: "CONCESSION",
    qualificationTier: "EXPANDED_PIPELINE_ADJACENT",
    discoveryPropertyId: "disc_tc_gideon_putnam_ny",
    knownHpcId: null,
    independentOfficial: true,
  },
  {
    hotelName: "Marriott Marquis Chicago",
    city: "Chicago",
    stateRegion: "IL",
    country: "United States",
    address: "2121 South Prairie Avenue",
    officialUrl: "https://www.marriott.com/en-us/hotels/chimq-marriott-marquis-chicago/overview/",
    opportunityType: "MCT",
    qualificationTier: "AWARD_PENDING",
    discoveryPropertyId: "disc_cb3_marriott_marquis_chicago_2026",
    findingId: "find_cb3_marriott_marquis_chicago_mct_2026",
    knownHpcId: "rec43YKxOBdvxnpd9",
  },
  {
    hotelName: "Honey Creek Resort",
    city: "Moravia",
    stateRegion: "IA",
    country: "United States",
    address: "12633 Resort Drive",
    officialUrl: "https://www.honeycreekresort.com/",
    opportunityType: "CONCESSION",
    qualificationTier: "AWARD_PENDING",
    discoveryPropertyId: "disc_cb3_honey_creek_resort_ia_2026",
    knownHpcId: null,
    independentOfficial: true,
  },
  {
    hotelName: "Hotel do Mel – Casa da Costeira",
    city: "Vila Nova do Ceira",
    country: "Portugal",
    opportunityType: "HOTEL_DEVELOPMENT",
    qualificationTier: "EXPANDED_PIPELINE_ADJACENT",
    discoveryPropertyId: "disc_cb2_hotel_do_mel_gois_2026",
    projectLevelNoPhysicalHotelYet: true,
    knownHpcId: null,
  },
];

function writeJson(name, obj) {
  fs.writeFileSync(path.join(OUT, name), `${JSON.stringify(obj, null, 2)}\n`);
}
async function airtableGet(url) {
  const res = await fetch(url, { headers: { Authorization: `Bearer ${token}` } });
  const json = await res.json();
  if (!res.ok) throw new Error(JSON.stringify(json));
  return json;
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
async function readHpc(id) {
  if (!id) return null;
  const res = await fetch(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${HPC_TABLE_ID}/${id}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const json = await res.json();
  if (!res.ok) return { error: json, id };
  return {
    id: json.id,
    name: json.fields?.["Property Name"] || json.fields?.["Canonical Property Name"],
    city: json.fields?.City,
    country: json.fields?.Country,
    address: json.fields?.Address,
    brand: json.fields?.["Current Brand"],
    officialUrl: json.fields?.["Official Property URL"],
  };
}

async function collide(h) {
  try {
    return await performLiveHpcIdentityCollisionCheck({
      hotelName: h.hotelName,
      aliases: h.aliases || [h.hotelName],
      city: h.city,
      brand: h.currentBrand,
      address: h.address,
      country: h.country,
      officialUrl: h.officialUrl,
      adpPropertyId: h.discoveryPropertyId,
    });
  } catch (e) {
    return { outcome: "HPC_LIVE_LOOKUP_UNAVAILABLE", error: e.message };
  }
}

function brandDirectoryOk(url) {
  return /marriott\.com|hilton\.com|ihg\.com|hyatt\.com|fourseasons\.com|accor|wyndham|choicehotels|bestwestern|fairmont|radissonhotels/i.test(
    String(url || ""),
  );
}

async function main() {
  const budgetPath = path.join(ROOT, "data/sec-edgar/hif-track-c-wave3b-exa-budget-ledger.json");
  const exaSpent = fs.existsSync(budgetPath)
    ? Number(JSON.parse(fs.readFileSync(budgetPath, "utf8")).spentUsd || 0)
    : 0.121;

  const rows = [];
  const ledger = {
    asOf: new Date().toISOString(),
    mode: APPLY ? "APPLY" : "DRY_RUN",
    publicationState: CUSTOMER_BLOCKED_LABEL(),
    exaCumulativeSpendUsd: exaSpent,
    exaHardCapUsd: 10,
    newExaSpendThisRunUsd: 0,
    note: "No additional $10 allocation — cumulative pilot budget",
    operations: [],
    summary: { inventoryLinked: 0, inventoryNoops: 0, hpcCreates: 0, hpcCreateFailures: 0, readBackOk: 0 },
  };

  function CUSTOMER_BLOCKED_LABEL() {
    return "CUSTOMER_BLOCKED";
  }

  for (const h of [...FIRST100, ...EXPANDED]) {
    const collision = await collide(h);
    let hpcId = h.knownHpcId || collision?.hpcRecordId || null;
    if (hpcId) {
      const rb = await readHpc(hpcId);
      if (rb?.error) hpcId = null;
      else ledger.summary.readBackOk += 1;
    }

    let status = "RESEARCH_REQUIRED";
    if (h.projectLevelNoPhysicalHotelYet) status = "PROJECT_LEVEL_NO_HPC";
    else if (hpcId) status = "MATCHED_OR_CREATED";
    else if (h.address && brandDirectoryOk(h.officialUrl)) status = "CREATE_READY";
    else if (h.address && (h.independentOfficial || !brandDirectoryOk(h.officialUrl))) {
      status = "UNRESOLVED_INDEPENDENT_URL_GATE";
    }

    // Link discovery inventory when we have HPC + discoveryPropertyId
    if (h.discoveryPropertyId && hpcId) {
      const inv = await findByFormula(
        inventoryTableId,
        `{${MAP_DISCOVERY_INVENTORY.discoveryPropertyId}}="${h.discoveryPropertyId}"`,
      );
      if (!inv) {
        ledger.operations.push({
          hotelName: h.hotelName,
          action: "INVENTORY_MISSING",
          discoveryPropertyId: h.discoveryPropertyId,
          hpcRecordId: hpcId,
        });
      } else if (!APPLY) {
        ledger.operations.push({
          hotelName: h.hotelName,
          action: "WOULD_LINK_INVENTORY",
          inventoryId: inv.id,
          hpcRecordId: hpcId,
        });
        ledger.summary.inventoryLinked += 1;
      } else {
        const patch = await airtablePatch(inventoryTableId, inv.id, {
          [MAP_DISCOVERY_INVENTORY.hpcMatchStatus]: "MATCHED",
          [MAP_DISCOVERY_INVENTORY.hpcHotel]: [hpcId],
          [MAP_DISCOVERY_INVENTORY.notesQuarantine]: `CUSTOMER_BLOCKED | Track C CB3 first-100 link | finding=${h.findingId || ""} | tier=${h.qualificationTier || ""} | do_not_requalify`,
        });
        ledger.operations.push({
          hotelName: h.hotelName,
          action: patch.ok ? "LINKED_INVENTORY" : "LINK_FAILED",
          inventoryId: inv.id,
          hpcRecordId: hpcId,
          error: patch.ok ? null : patch.json,
        });
        if (patch.ok) ledger.summary.inventoryLinked += 1;
      }
    } else if (h.discoveryPropertyId && !hpcId) {
      ledger.summary.inventoryNoops += 1;
    }

    rows.push({
      hotelName: h.hotelName,
      first100: FIRST100.some((x) => x.hotelName === h.hotelName),
      opportunityType: h.opportunityType,
      qualificationTier: h.qualificationTier,
      findingId: h.findingId || null,
      discoveryPropertyId: h.discoveryPropertyId || null,
      status,
      hpcRecordId: hpcId,
      collisionOutcome: collision?.outcome || null,
      address: h.address || null,
      officialUrl: h.officialUrl || null,
      projectLevel: Boolean(h.projectLevelNoPhysicalHotelYet),
      doNotPromote: Boolean(h.doNotPromote),
      readBack: hpcId ? await readHpc(hpcId) : null,
    });
  }

  // Optional: Lakeville CREATE only if brand-directory URL available — currently not.
  // Do not create with DMO URL without founder gate authorization.

  const first100 = rows.filter((r) => r.first100);
  const linked = first100.filter((r) => r.hpcRecordId).length;
  const report = {
    asOf: new Date().toISOString(),
    publicationState: "CUSTOMER_BLOCKED",
    icSource: "PR #544 MERGED — CB3 first-100 = 5; Legacy excluded from open competitive",
    first100Queue: first100,
    first100HpcLinkageRate: Number((linked / first100.length).toFixed(4)),
    first100Linked: linked,
    first100Total: first100.length,
    unresolvedFirst100: first100.filter((r) => !r.hpcRecordId),
    expandedPipeline: rows.filter((r) => !r.first100),
    exa: {
      cumulativeSpendUsd: exaSpent,
      hardCapUsd: 10,
      remainingUsd: Number((10 - exaSpent).toFixed(4)),
      additionalAllocationThisInstruction: false,
      newSpendThisRunUsd: 0,
      note: "Free-source verification only this run; Exa not required for already-resolved first-100 brand URLs",
    },
    ledgerSummary: ledger.summary,
    heHandoffPath: "HOTEL_EXPLORER_HANDOFF_CB3_FIRST100.json",
  };

  const heHandoff = {
    asOf: new Date().toISOString(),
    publicationState: "CUSTOMER_BLOCKED",
    priority: "IMMEDIATE",
    note: "Track C identity handoff only — HE worktree not modified; do not publish",
    items: first100.map((r) => ({
      recommendedResearchPriority: r.hpcRecordId ? "HE_ENRICHMENT_READY" : "BLOCKED_PENDING_HPC",
      hotelName: r.hotelName,
      market: r.readBack?.city || null,
      hpcRecordId: r.hpcRecordId,
      discoveryFindingId: r.findingId,
      discoveryPropertyId: r.discoveryPropertyId,
      opportunityType: r.opportunityType,
      opportunityQualificationTier: r.qualificationTier,
      commercialStatus: r.qualificationTier,
      identityStatus: r.status,
      officialUrl: r.officialUrl,
      missingIntelligence: [
        !r.hpcRecordId ? "hpc_link" : null,
        r.status === "UNRESOLVED_INDEPENDENT_URL_GATE" ? "brand_directory_or_authorized_independent_url" : null,
      ].filter(Boolean),
    })),
  };

  writeJson("CB3_FIRST100_HPC_LINKAGE.json", report);
  writeJson("HOTEL_EXPLORER_HANDOFF_CB3_FIRST100.json", heHandoff);
  writeJson(APPLY ? "CB3_FIRST100_PERSISTENCE_APPLY.json" : "CB3_FIRST100_PERSISTENCE_DRY_RUN.json", ledger);
  writeJson("FOUNDER_CB3_FIRST100_PRIORITY_REPORT.json", {
    ...report,
    successCriterion: "proportion of commercially relevant opportunities connected to verified HPC",
    actualProportion: `${linked}/${first100.length}`,
    lakevilleBlocker:
      "Independent property with verified address; Official Property URL gate requires brand-directory domain. Founder recommendation pending: accept DMO/independent official site.",
  });

  console.log(
    JSON.stringify(
      {
        done: true,
        first100Linked: `${linked}/${first100.length}`,
        unresolved: report.unresolvedFirst100.map((r) => r.hotelName),
        exaRemaining: report.exa.remainingUsd,
        ledger: ledger.summary,
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
