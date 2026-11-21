#!/usr/bin/env node
/**
 * Track C — P0 commercial opportunity canonical population (5 first-100).
 * Preserve opportunity IDs. Link inventory+findings. CREATE only when gate-clear.
 * Cumulative Exa $10. CUSTOMER_BLOCKED. --apply / --apply-hpc-create
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
  resolveAuthenticatedFounderIdentity,
  persistHpcCreateAuthorization,
  executeAuthorizedHpcCreate,
} from "../lib/hotel-census/hpc-authorized-create-reuse-v1.js";
import { performLiveHpcIdentityCollisionCheck } from "../lib/hotel-census/hpc-live-identity-collision-v1.js";
import { TRACK_C_STANDING_AUTH, inferFamilySelect } from "../lib/hotel-intelligence/sec-edgar/hif-track-c-universe-expansion-v1.js";
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
const OUT = path.join(ROOT, "reports/sec-edgar-hif-track-c-p0-canonical-population");
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
// Findings table id from generated map if present
const findingsTableId =
  ids.tableIds["HI Hotel Discovery Findings"] ||
  ids.tableIds.HI_HOTEL_DISCOVERY_FINDINGS ||
  Object.entries(ids.tableIds || {}).find(([k]) => /finding/i.test(k))?.[1] ||
  null;

const P0 = [
  {
    id: "P0-1",
    hotelName: "Lakeville Inn",
    opportunity: "BCT",
    discoveryPropertyId: "disc_cb1_lakeville_inn_mn_2026",
    findingId: "find_cb1_lakeville_bct_brand_search_2026",
    inventoryAirtableId: "recPXlLWkd63mLK5h",
    findingAirtableId: "recjL2vx3ExNlReF1",
    city: "Lakeville",
    stateRegion: "MN",
    country: "United States",
    address: "20851 Keokuk Avenue",
    currentBrand: "Independent",
    historicalBrand: "Hampton Inn",
    // Independent post-Hampton: property-owned official site (address-corroborated).
    // Brand-directory N/A while unaffiliated; DMO is supporting evidence only.
    officialUrl: "https://lakevilleinnmn.com/",
    brandDirectoryUrl: null,
    independentOfficialUrl: "https://lakevilleinnmn.com/",
    supportingUrls: [
      "https://visitlakeville.org/explore/stay/hotels/lakeville-inn/",
      "https://lakevillechamber.org/member-news-lakeville-inn/",
      "https://www.startribune.com/hotel-sold-at-discount-lost-hampton-inn-flag-during-ice-surge/601664022",
    ],
    contractualTrigger: "Owner/CEO brand-search for new franchise by end-2026",
    decisionTiming: "2026 brand agreement goal",
    ownershipEvidence: "Elevate Hospitality LLC / JAMP Hotels CEO Jaymin Patel (Star Tribune 2026-04-09; Chamber 2026-04-13; sale ~2026-03-26 $8.5M)",
  },
  {
    id: "P0-2",
    hotelName: "Kimpton Shorebreak Huntington Beach Resort",
    opportunity: "BCT",
    discoveryPropertyId: "disc_cb2b_shorebreak_huntington_2026",
    findingId: "find_cb2b_shorebreak_huntington_bct_2026",
    inventoryAirtableId: "recTnywid3vtCErTu",
    findingAirtableId: "recrgvSQeupLLKe0B",
    city: "Huntington Beach",
    stateRegion: "CA",
    country: "United States",
    address: "500 Pacific Coast Highway",
    currentBrand: "Kimpton",
    officialUrl:
      "https://www.ihg.com/kimptonhotels/hotels/us/en/shorebreak-hotel-huntington-beach-ca/laxsb/hoteldetail",
    brandDirectoryUrl:
      "https://www.ihg.com/kimptonhotels/hotels/us/en/shorebreak-hotel-huntington-beach-ca/laxsb/hoteldetail",
    priorClaimedHpc: "reckTzVQUT2H8hbug",
    contractualTrigger: "Kimpton / IHG soft-brand conversion diligence (BCT)",
    decisionTiming: "Open competitive first-100 BCT window",
    ownershipEvidence: "See Finding find_cb2b_shorebreak_huntington_bct_2026 (qualification unchanged)",
  },
  {
    id: "P0-3",
    hotelName: "Courtyard by Marriott Denver Downtown",
    opportunity: "BCT",
    discoveryPropertyId: "disc_cb2b_courtyard_denver_downtown_2026",
    findingId: "find_cb2b_courtyard_denver_bct_2026",
    inventoryAirtableId: "recmomZidWqHS7vTv",
    findingAirtableId: "recBL4VUbarrWCUDS",
    city: "Denver",
    stateRegion: "CO",
    country: "United States",
    address: "934 16th Street",
    currentBrand: "Courtyard",
    officialUrl: "https://www.marriott.com/en-us/hotels/dencd-courtyard-denver-downtown/overview/",
    brandDirectoryUrl: "https://www.marriott.com/en-us/hotels/dencd-courtyard-denver-downtown/overview/",
    priorClaimedHpc: "recvn447rdN53NgZ7",
    contractualTrigger: "Courtyard / Marriott brand-conversion diligence (BCT)",
    decisionTiming: "Open competitive first-100 BCT window",
    ownershipEvidence: "See Finding find_cb2b_courtyard_denver_bct_2026 (qualification unchanged)",
  },
  {
    id: "P0-4",
    hotelName: "Hilton Trinidad & Conference Centre",
    opportunity: "MCT",
    discoveryPropertyId: "disc_cfa_hilton_trinidad_pos_2026",
    findingId: "find_cfa_hilton_trinidad_mct_hma_sale_2026",
    inventoryAirtableId: "rec0f9E8genYc3tI2",
    findingAirtableId: "recDRyNUriKMAl58C",
    city: "Port of Spain",
    country: "Trinidad and Tobago",
    address: "1B Lady Young Road, Belmont",
    currentBrand: "Hilton",
    officialUrl: "https://www.hilton.com/en/hotels/poshihh-hilton-trinidad-and-conference-centre/",
    brandDirectoryUrl: "https://www.hilton.com/en/hotels/poshihh-hilton-trinidad-and-conference-centre/",
    priorClaimedHpc: "recFNfTvcph0fYQnm",
    contractualTrigger: "HMA / management-contract timing sale signal (MCT)",
    decisionTiming: "Open competitive first-100 MCT window",
    ownershipEvidence: "See Finding find_cfa_hilton_trinidad_mct_hma_sale_2026 (qualification unchanged)",
  },
  {
    id: "P0-5",
    hotelName: "Four Seasons Hotel Bengaluru at Embassy ONE",
    opportunity: "MCT",
    discoveryPropertyId: "disc_cb3_embassy_one_bengaluru_2026",
    findingId: "find_cb3_embassy_one_bengaluru_mct_2026",
    inventoryAirtableId: "recoF4KfmJj2LYRET",
    findingAirtableId: "recQmiSbQOyMmzalO",
    city: "Bengaluru",
    country: "India",
    address: "8 Bellary Road, Ganganagar",
    currentBrand: "Four Seasons",
    officialUrl: "https://www.fourseasons.com/bengaluru/",
    brandDirectoryUrl: "https://www.fourseasons.com/bengaluru/",
    priorClaimedHpc: "recD8mIAMUyQ9ilka",
    contractualTrigger: "Embassy ONE / Four Seasons management-contract timing (MCT)",
    decisionTiming: "Open competitive first-100 MCT window",
    ownershipEvidence: "See Finding find_cb3_embassy_one_bengaluru_mct_2026 (qualification unchanged)",
  },
];

const BRAND_RE =
  /marriott\.com|hilton\.com|ihg\.com|hyatt\.com|fourseasons\.com|wyndham|choicehotels|bestwestern|accor|fairmont|radissonhotels/i;
/** Aggregators / DMO / booking engines — not property Official Property URL for CREATE. */
const NON_PROPERTY_OFFICIAL_RE =
  /visitlakeville\.org|booking\.com|expedia\.com|hotels\.com|tripadvisor\.com|google\.com\/travel|asiwebres\.com|kayak\.com|trivago/i;

/**
 * Official Property URL for CREATE_READY:
 * - Branded: brand-directory domain required
 * - Independent: property-owned official hotel website (not DMO/aggregator), address already verified separately
 * Does not invent brand-directory URLs for independents.
 */
function resolveGateUrl(h) {
  const brandUrl = h.brandDirectoryUrl || (BRAND_RE.test(h.officialUrl || "") ? h.officialUrl : null);
  if (brandUrl && BRAND_RE.test(brandUrl)) return { gateUrl: brandUrl, gateKind: "BRAND_DIRECTORY" };
  const independent =
    /independent/i.test(h.currentBrand || "") || Boolean(h.independentOfficialUrl);
  if (!independent) return { gateUrl: null, gateKind: null };
  const cand = h.independentOfficialUrl || h.officialUrl || null;
  if (!cand || NON_PROPERTY_OFFICIAL_RE.test(cand) || BRAND_RE.test(cand)) {
    return { gateUrl: null, gateKind: null };
  }
  try {
    const u = new URL(cand);
    if (!/^https?:$/i.test(u.protocol)) return { gateUrl: null, gateKind: null };
    return { gateUrl: cand, gateKind: "INDEPENDENT_PROPERTY_OFFICIAL" };
  } catch {
    return { gateUrl: null, gateKind: null };
  }
}

function writeJson(name, obj) {
  fs.writeFileSync(path.join(OUT, name), `${JSON.stringify(obj, null, 2)}\n`);
}
async function getRecord(tableId, recordId) {
  const res = await fetch(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${tableId}/${recordId}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const json = await res.json();
  return { ok: res.ok, status: res.status, json };
}
async function patchRecord(tableId, recordId, fields) {
  const res = await fetch(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${tableId}/${recordId}`, {
    method: "PATCH",
    headers: { Authorization: `Bearer ${token}`, "Content-Type": "application/json" },
    body: JSON.stringify({ fields, typecast: true }),
  });
  return { ok: res.ok, json: await res.json() };
}
function sleep(ms) {
  return new Promise((r) => setTimeout(r, ms));
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

  const rows = [];
  const ledger = {
    asOf: new Date().toISOString(),
    mode: APPLY ? (APPLY_HPC ? "APPLY_WITH_HPC_CREATE" : "APPLY_LINKS") : "DRY_RUN",
    publicationState: "CUSTOMER_BLOCKED",
    exaSpendBeforeUsd: spendBefore,
    operations: [],
    summary: { links: 0, creates: 0, failures: 0, noops: 0, readBackOk: 0 },
  };

  let founderAuth = null;
  if (APPLY_HPC) {
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
  }

  for (const h of P0) {
    const inv = await getRecord(inventoryTableId, h.inventoryAirtableId);
    const invFields = inv.ok ? inv.json.fields || {} : {};
    const invMatchStatus = invFields[MAP_DISCOVERY_INVENTORY.hpcMatchStatus] || invFields["HPC Match Status"] || null;
    const invHpcLinked = invFields[MAP_DISCOVERY_INVENTORY.hpcHotel] || invFields["HPC Hotel"] || [];
    const invLinkedId = Array.isArray(invHpcLinked) ? invHpcLinked[0] : null;

    // Verify claimed / linked HPC exists
    let hpcId = invLinkedId || h.priorClaimedHpc || null;
    let hpcRead = null;
    let matchOrCreate = "NONE";
    if (hpcId) {
      const rb = await getRecord(HPC_TABLE_ID, hpcId);
      if (rb.ok) {
        hpcRead = {
          id: rb.json.id,
          name: rb.json.fields?.["Property Name"] || rb.json.fields?.["Canonical Property Name"],
          city: rb.json.fields?.City,
          country: rb.json.fields?.Country,
          address: rb.json.fields?.Address,
          brand: rb.json.fields?.["Current Brand"],
          officialUrl: rb.json.fields?.["Official Property URL"],
        };
        matchOrCreate = invLinkedId ? "EXISTING_LINKED" : "EXISTING_UNLINKED";
        ledger.summary.readBackOk += 1;
      } else {
        hpcId = null;
      }
    }

    // Collision if still missing
    let collision = null;
    if (!hpcId) {
      try {
        collision = await performLiveHpcIdentityCollisionCheck({
          hotelName: h.hotelName,
          aliases: [h.hotelName, h.historicalBrand].filter(Boolean),
          city: h.city,
          brand: h.currentBrand,
          address: h.address,
          country: h.country,
          officialUrl: h.brandDirectoryUrl || h.officialUrl,
          adpPropertyId: h.discoveryPropertyId,
        });
        if (collision?.hpcRecordId) {
          hpcId = collision.hpcRecordId;
          matchOrCreate = "MATCHED_COLLISION";
          const rb = await getRecord(HPC_TABLE_ID, hpcId);
          if (rb.ok) {
            hpcRead = {
              id: rb.json.id,
              name: rb.json.fields?.["Property Name"],
              city: rb.json.fields?.City,
              country: rb.json.fields?.Country,
              address: rb.json.fields?.Address,
              brand: rb.json.fields?.["Current Brand"],
              officialUrl: rb.json.fields?.["Official Property URL"],
            };
            ledger.summary.readBackOk += 1;
          }
        }
      } catch (e) {
        collision = { outcome: "LOOKUP_ERROR", error: e.message };
      }
    }

    // Exa only if still no gate URL and no HPC (Lakeville brand hunt)
    let exaRequests = 0;
    let exaCostThisHotel = 0;
    let { gateUrl, gateKind } = resolveGateUrl(h);
    if (!hpcId && !gateUrl && budget.remaining() > 0.01) {
      const search = await exa.search({
        query: `${h.hotelName} ${h.city || ""} ${h.address || ""} hotel official website OR brand directory`,
        type: "lite",
        numResults: 5,
      });
      exaRequests += 1;
      if (search.ok && !search.fromCache) {
        exaCostThisHotel += Number(search.costDollars?.total || 0.005);
      }
      const brandHit = (search.results || []).find((r) => BRAND_RE.test(r.url || ""));
      if (brandHit?.url) {
        h.brandDirectoryUrl = brandHit.url;
      } else {
        const indepHit = (search.results || []).find(
          (r) =>
            r.url &&
            !NON_PROPERTY_OFFICIAL_RE.test(r.url) &&
            !BRAND_RE.test(r.url) &&
            /lakevilleinn|official/i.test(`${r.url} ${r.title || ""}`),
        );
        if (indepHit?.url && /independent/i.test(h.currentBrand || "")) {
          h.independentOfficialUrl = indepHit.url;
          h.officialUrl = indepHit.url;
        }
      }
      ({ gateUrl, gateKind } = resolveGateUrl(h));
    }

    const createReady = !hpcId && Boolean(h.address && gateUrl && gateKind);
    let createError = null;

    if (createReady && APPLY_HPC && founderAuth?.ok) {
      const fields = {
        "Property Name": h.hotelName,
        "Canonical Property Name": h.hotelName,
        Country: h.country,
        City: h.city,
        "State / Region": h.stateRegion || null,
        Address: h.address,
        "Current Brand": h.currentBrand,
        "Affiliation Status": /independent/i.test(h.currentBrand || "") ? "Independent" : "Branded",
        "Family / Source Family": inferFamilySelect(h.currentBrand),
        "Official Property URL": gateUrl,
      };
      for (const k of Object.keys(fields)) {
        if (fields[k] == null || fields[k] === "") delete fields[k];
      }
      const auth = persistHpcCreateAuthorization({
        runId: `track_c_p0_${new Date().toISOString().slice(0, 10)}`,
        draftPropertyId: h.discoveryPropertyId,
        hotelName: h.hotelName,
        fields,
        approverIdentity: founderAuth.approverIdentity,
        authorizationSource: `${TRACK_C_STANDING_AUTH.source}_P0_COMMERCIAL_CANONICAL`,
      });
      if (!auth.ok) {
        createError = auth.error;
        ledger.summary.failures += 1;
      } else {
        const exec = await executeAuthorizedHpcCreate({
          runId: `track_c_p0_${new Date().toISOString().slice(0, 10)}`,
          authorization: auth.record,
          dryRun: false,
          apply: true,
          simulation: false,
        });
        ledger.operations.push({
          hotel: h.hotelName,
          action: "HPC_CREATE",
          ok: exec.ok,
          state: exec.state,
          hpcRecordId: exec.hpcRecordId || null,
          error: exec.error || null,
        });
        if (exec.ok && exec.hpcRecordId) {
          hpcId = exec.hpcRecordId;
          matchOrCreate = exec.reusedExisting ? "REUSED_EXISTING" : "CREATED";
          ledger.summary.creates += 1;
          const rb = await getRecord(HPC_TABLE_ID, hpcId);
          if (rb.ok) {
            hpcRead = {
              id: rb.json.id,
              name: rb.json.fields?.["Property Name"],
              city: rb.json.fields?.City,
              country: rb.json.fields?.Country,
              address: rb.json.fields?.Address,
              brand: rb.json.fields?.["Current Brand"],
              officialUrl: rb.json.fields?.["Official Property URL"],
            };
            ledger.summary.readBackOk += 1;
          }
        } else {
          createError = exec.error || exec.state;
          ledger.summary.failures += 1;
        }
        await sleep(180);
      }
    } else if (createReady && !APPLY_HPC) {
      ledger.operations.push({ hotel: h.hotelName, action: "WOULD_HPC_CREATE" });
    } else if (!hpcId && !createReady) {
      createError =
        !h.address
          ? "MISSING_ADDRESS"
          : !gateUrl
            ? /independent/i.test(h.currentBrand || "")
              ? "MISSING_INDEPENDENT_PROPERTY_OFFICIAL_URL"
              : "MISSING_BRAND_DIRECTORY_OFFICIAL_URL"
            : "GATE_NOT_MET";
    }

    // Link inventory (and findings notes) when we have HPC
    let linkStatus = "NO_HPC";
    if (hpcId) {
      if (!APPLY) {
        linkStatus = invLinkedId === hpcId ? "ALREADY_LINKED" : "WOULD_LINK";
        if (linkStatus === "ALREADY_LINKED") ledger.summary.noops += 1;
        else ledger.summary.links += 1;
      } else {
        const patchFields = {
          [MAP_DISCOVERY_INVENTORY.hpcMatchStatus]: "MATCHED",
          [MAP_DISCOVERY_INVENTORY.hpcHotel]: [hpcId],
        };
        // Also set notes with opportunity preservation
        patchFields[MAP_DISCOVERY_INVENTORY.notesQuarantine] =
          `CUSTOMER_BLOCKED | Track C P0 canonical | finding=${h.findingId} | opportunity=${h.opportunity} | do_not_requalify | linked=${hpcId}`;
        const patch = await patchRecord(inventoryTableId, h.inventoryAirtableId, patchFields);
        linkStatus = patch.ok ? "LINKED" : "LINK_FAILED";
        ledger.operations.push({
          hotel: h.hotelName,
          action: "LINK_INVENTORY",
          inventoryId: h.inventoryAirtableId,
          hpcRecordId: hpcId,
          ok: patch.ok,
          error: patch.ok ? null : patch.json,
        });
        if (patch.ok) ledger.summary.links += 1;
        else ledger.summary.failures += 1;

        // Idempotency: re-read inventory
        const inv2 = await getRecord(inventoryTableId, h.inventoryAirtableId);
        const linked2 = inv2.ok ? inv2.json.fields?.[MAP_DISCOVERY_INVENTORY.hpcHotel] || [] : [];
        const status2 = inv2.ok ? inv2.json.fields?.[MAP_DISCOVERY_INVENTORY.hpcMatchStatus] : null;
        ledger.operations.push({
          hotel: h.hotelName,
          action: "IDEMPOTENCY_READBACK",
          matchStatus: status2,
          hpcLinked: linked2,
          ok: Array.isArray(linked2) && linked2[0] === hpcId && status2 === "MATCHED",
        });
      }
    }

    const missingEvidence = [];
    if (!hpcId) missingEvidence.push(createError || "NO_HPC");
    if (!hpcId && h.id === "P0-1" && !gateUrl) {
      missingEvidence.push("INDEPENDENT_PROPERTY_OFFICIAL_URL_OR_FUTURE_BRAND_DIRECTORY");
    }

    rows.push({
      p0Id: h.id,
      commercialOpportunity: h.opportunity,
      hotelName: h.hotelName,
      discoveryPropertyId: h.discoveryPropertyId,
      findingId: h.findingId,
      inventoryAirtableId: h.inventoryAirtableId,
      findingAirtableId: h.findingAirtableId,
      inventoryMatchStatusBefore: invMatchStatus,
      inventoryLinkedHpcBefore: invLinkedId,
      canonicalHpcRecordId: hpcId,
      matchOrCreate,
      discoveryLinkage: linkStatus,
      gateKind: gateKind || null,
      gateUrl: gateUrl || null,
      hotelExplorerHandoffStatus: hpcId ? "READY" : "BLOCKED_PENDING_HPC",
      address: h.address,
      officialUrl: gateUrl || h.officialUrl,
      brand: h.currentBrand,
      hpcReadBack: hpcRead,
      collisionOutcome: collision?.outcome || null,
      createReady,
      missingEvidence,
      exaRequests,
      exaCostUsd: exaCostThisHotel,
      sourceVerification: gateKind || (gateUrl ? "OFFICIAL_URL" : "INSUFFICIENT"),
      contractualTrigger: h.contractualTrigger || null,
      decisionTiming: h.decisionTiming || null,
      ownershipEvidence: h.ownershipEvidence || null,
    });
  }

  const linkedCount = rows.filter((r) => r.canonicalHpcRecordId).length;
  const spendAfter = budget.spentUsd();

  const heHandoff = {
    asOf: new Date().toISOString(),
    publicationState: "CUSTOMER_BLOCKED",
    note: "Hotel Explorer owns enrichment — Track C identity handoff only",
    items: rows
      .filter((r) => r.canonicalHpcRecordId)
      .map((r) => ({
        hpcRecordId: r.canonicalHpcRecordId,
        verifiedHotelName: r.hpcReadBack?.name || r.hotelName,
        brand: r.hpcReadBack?.brand || r.brand,
        operator: null,
        operatorNote: "Not inferred from brand",
        physicalAddress: r.hpcReadBack?.address || r.address,
        officialHotelUrl: r.hpcReadBack?.officialUrl || r.officialUrl,
        commercialOpportunityType: r.commercialOpportunity,
        discoveryPropertyId: r.discoveryPropertyId,
        findingId: r.findingId,
        inventoryAirtableId: r.inventoryAirtableId,
        findingAirtableId: r.findingAirtableId,
        ownershipEvidence: r.ownershipEvidence,
        contractualTrigger: r.contractualTrigger,
        decisionTiming: r.decisionTiming,
        sourceEvidence: r.officialUrl,
        hotelExplorerAvailability: "UNKNOWN_NOT_QUERIED_FROM_TRACK_C",
        missingHotelExplorerAttributes: ["full_profile", "owner_operator_validated", "decision_maker"],
        recommendedResearchPriority: "IMMEDIATE",
      })),
  };

  const report = {
    asOf: new Date().toISOString(),
    publicationState: "CUSTOMER_BLOCKED",
    target: "5/5 verified canonical hotel links",
    achieved: `${linkedCount}/5`,
    rows,
    secondary: {
      legacyGreenBay: "Below P0 — preferred-brand conversion intelligence, not open first-100",
      aenaNordeste: "Project-level four-site program — do not force into existing-hotel HPC",
    },
    exa: {
      hardCapUsd: 10,
      spendBeforeUsd: spendBefore,
      spendAfterUsd: spendAfter,
      thisRunUsd: Number((spendAfter - spendBefore).toFixed(6)),
      additionalAllocation: false,
    },
    ledgerSummary: ledger.summary,
  };

  writeJson("P0_CANONICAL_TABLE.json", report);
  writeJson("HOTEL_EXPLORER_HANDOFF_P0.json", heHandoff);
  writeJson(APPLY ? "PERSISTENCE_LEDGER_APPLY.json" : "PERSISTENCE_LEDGER_DRY_RUN.json", ledger);
  writeJson("FOUNDER_P0_CANONICAL_REPORT.json", report);

  const md = `# P0 Canonical Population

Achieved **${linkedCount}/5** · Exa $${spendAfter.toFixed(4)} / $10 cumulative · CUSTOMER_BLOCKED

| P0 | Hotel | Opp | HPC ID | Match/Create | Linkage | HE | Gaps |
|----|-------|-----|--------|--------------|---------|----|------|
${rows
  .map(
    (r) =>
      `| ${r.p0Id} | ${r.hotelName} | ${r.commercialOpportunity} | ${r.canonicalHpcRecordId || "—"} | ${r.matchOrCreate} | ${r.discoveryLinkage} | ${r.hotelExplorerHandoffStatus} | ${(r.missingEvidence || []).join("; ") || "—"} |`,
  )
  .join("\n")}
`;
  fs.writeFileSync(path.join(OUT, "FOUNDER_P0_CANONICAL_REPORT.md"), md);

  console.log(JSON.stringify({ done: true, achieved: `${linkedCount}/5`, rows: rows.map((r) => ({ id: r.p0Id, hpc: r.canonicalHpcRecordId, link: r.discoveryLinkage, gap: r.missingEvidence })), exa: report.exa, ledger: ledger.summary }, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
