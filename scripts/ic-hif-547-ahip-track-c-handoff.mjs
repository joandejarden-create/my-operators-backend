#!/usr/bin/env node
/**
 * IC #547 — opportunity-first AHIP portfolio Track C handoff (read-only Airtable).
 * One portfolio MCT parent; property-level identities preserved separately.
 * Does NOT create a fictional HPC hotel for the portfolio.
 */
import dotenv from "dotenv";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  GRAPH_BASE_ID_CANONICAL,
  TABLE_HI_DISCOVERY_FINDINGS,
  TABLE_HI_HOTEL_DISCOVERY_INVENTORY,
} from "../lib/hotel-intelligence/sec-edgar/discovery-inventory-airtable-contract-v23.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
for (const p of [path.join(ROOT, ".env"), "C:/Dev/dealality/my-operators-backend/.env"]) {
  if (fs.existsSync(p)) {
    dotenv.config({ path: p, override: false });
    break;
  }
}

const OUT_DIR = path.join(ROOT, "reports/sec-edgar-hif-v6-track-a-cb4-global-commercial-expansion");
const inv = JSON.parse(fs.readFileSync(path.join(OUT_DIR, "INVESTIGATED_HOTELS.json"), "utf8"));
const readback = JSON.parse(fs.readFileSync(path.join(OUT_DIR, "AIRTABLE_READBACK_IDS.json"), "utf8"));
const he = JSON.parse(fs.readFileSync(path.join(OUT_DIR, "HOTEL_EXPLORER_RESEARCH_PRIORITIES.json"), "utf8"));

const token = process.env.AIRTABLE_API_KEY || process.env.AIRTABLE_PAT;
const ids = JSON.parse(
  fs.readFileSync(path.join(ROOT, "lib/hotel-intelligence/sec-edgar/discovery-inventory-table-ids.generated.json"), "utf8"),
);
const invTable = ids.tableIds[TABLE_HI_HOTEL_DISCOVERY_INVENTORY];
const findingsTable = ids.tableIds[TABLE_HI_DISCOVERY_FINDINGS];

async function findByFormula(tableId, formula) {
  if (!token) return null;
  const qs = new URLSearchParams({ filterByFormula: formula, maxRecords: "1" });
  const res = await fetch(`https://api.airtable.com/v0/${GRAPH_BASE_ID_CANONICAL}/${tableId}?${qs}`, {
    headers: { Authorization: `Bearer ${token}` },
  });
  const json = await res.json();
  if (!res.ok) throw new Error(JSON.stringify(json));
  return json.records?.[0] || null;
}

const ahipHotels = (inv.hotels || []).filter((h) => h.sourceFamily === "OWNER_EARNINGS_AHIP_AIMBRIDGE_MCT");
const remainingNamed = ahipHotels.filter(
  (h) =>
    h.disposition === "PRIVATE_OPPORTUNITY_PROBABLE_ACCESS" &&
    !/AHIP Remaining Portfolio/i.test(h.hotelName || ""),
);
const disposedNamed = ahipHotels.filter((h) => h.disposition === "COMPLETED_OR_HISTORICAL");

const PARENT_DISC = "disc_cb4_ahip_remaining_portfolio_mct_2026";
const PARENT_FIND = "find_cb4_ahip_aimbridge_mct_2026";

const parentInv = await findByFormula(invTable, `{Discovery Property ID}="${PARENT_DISC}"`);
const parentFind = await findByFormula(findingsTable, `{Finding ID}="${PARENT_FIND}"`);
const pFields = parentInv?.fields || {};
const parentLinked =
  pFields["Hotel Property Census"] || pFields["Linked HPC"] || pFields["HPC"] || null;

const propertyIdentities = remainingNamed.map((h) => ({
  hotelName: h.hotelName,
  city: h.city || null,
  country: h.country || "US",
  disposition: h.disposition,
  identityRole: "AFFECTED_PORTFOLIO_PROPERTY",
  hpcMatchStatus: "PENDING_TRACK_C_PROPERTY_MATCH",
  hpcRecordId: null,
  note: "Do not invent portfolio-level HPC; match/create property HPC only when Track C identity work runs",
}));

const handoff = {
  asOf: new Date().toISOString(),
  publicationState: "CUSTOMER_BLOCKED",
  from: "IC_PR_547",
  to: "TRACK_C",
  first100AfterCb4: "6/100",
  ownership: {
    canonicalIdentityAndHpc: "TRACK_C",
    comprehensivePropertyIntelligence: "HOTEL_EXPLORER",
    doNotRequireGeneralUsCensusCompletion: true,
    doNotCreateFictionalPortfolioHpcHotel: true,
  },
  ahipCommercialQualification: {
    opportunityId: "ahip-remaining-portfolio-aimbridge-mct",
    opportunityType: "MCT",
    countsAsOneQualifiedMct: true,
    separateIndependentlyActionableDecisionsSupported: false,
    decisionParty: "American Hotel Income Properties REIT LP (AHIP REIT LP) via GP American Hotel Income Properties REIT (GP) Inc.",
    currentManager: "ONE Lodging Management LLC (Aimbridge / ONE Lodging Holdings)",
    terminationEffective: "2027-04-30",
    replacementManagementRequired: true,
    selectionStillOpen: true,
    replacementAgreementsAwarded: false,
    oneVsMultipleManagers:
      "DISCLOSED_AS_SINGULAR_ASSUMPTION_ONLY — FLI/settlement assume retain 'a replacement hotel manager'; Master HMA + individual HMAs terminate together; no explicit multi-manager award or RFP disclosed",
    portfolioScope: {
      asOfSept30_2026: { hotels: 22, rooms: 2303, states: 11, source: "Q3 2026 MD&A 2026-11-13" },
      remainingAffectedNamedProperties: remainingNamed.length,
      disposedInvestigatedForScopeCompleteness: disposedNamed.length,
      note: "Not all historical AHIP hotels are affected; only remaining Aimbridge-managed portfolio after 2026 dispositions",
    },
    primarySources: [
      {
        url: "https://www.globenewswire.com/news-release/2026/04/20/3276955/0/en/american-hotel-income-properties-reit-lp-announces-settlement-with-its-hotel-management-company.html",
        publicationDate: "2026-04-20",
        role: "ORIGINAL_SETTLEMENT_DISCLOSURE",
      },
      {
        url: "https://www.otcmarkets.com/filing/608809/content",
        publicationDate: "2026-11-13",
        role: "Q3_2026_MDA_EXTENSION_TO_2027-04-30",
      },
    ],
  },
  parentOpportunity: {
    opportunityId: "ahip-remaining-portfolio-aimbridge-mct",
    discoveryPropertyId: PARENT_DISC,
    discoveryInventoryRecordId: parentInv?.id || readback.inventory[PARENT_DISC] || null,
    discoveryFindingId: PARENT_FIND,
    discoveryFindingRecordId: parentFind?.id || readback.findings[PARENT_FIND] || null,
    hpcMatchStatus: pFields["HPC Match Status"] || "PORTFOLIO_PARENT_NOT_AN_HPC_HOTEL",
    hpcRecordId: parentLinked,
    missingCanonicalIdentityOrLink: true,
    policy:
      "Parent discovery/finding rows represent the portfolio MCT decision. They are NOT a substitute HPC hotel. Track C must resolve property-level HPC identities for affected hotels only.",
  },
  affectedHotelIdentities: propertyIdentities,
  disposedOutOfScopeIdentities: disposedNamed.map((h) => ({
    hotelName: h.hotelName,
    city: h.city || null,
    note: "2026 disposition — investigated for MCT scope completeness; not remaining affected portfolio",
  })),
  existingHpcMatches: propertyIdentities.filter((p) => p.hpcRecordId),
  missingHpcIdentities: propertyIdentities.filter((p) => !p.hpcRecordId),
  hotelExplorerPriorities: he.priorities,
  adjacentHandoffs: [
    {
      hotelIdentity: "Thimarafushi City Hotel development plot",
      discoveryPropertyId: "disc_cb4_maldives_thimarafushi_city_hotel_2026",
      opportunityClass: "OPEN_COMPETITIVE_DEVELOPMENT",
      countsTowardFirst100: false,
    },
    {
      hotelIdentity: "Fonadhoo Tourist Hotel plots A/B/C",
      discoveryPropertyIds: [
        "disc_cb4_maldives_fonadhoo_plot_a_2026",
        "disc_cb4_maldives_fonadhoo_plot_b_2026",
        "disc_cb4_maldives_fonadhoo_plot_c_2026",
      ],
      opportunityClass: "OPEN_COMPETITIVE_DEVELOPMENT",
      countsTowardFirst100: false,
    },
    {
      hotelIdentity: "Cedar Lake Outdoor Center",
      discoveryPropertyId: "disc_cb4_cedar_lake_outdoor_center_mi_2026",
      opportunityClass: "OPEN_CONCESSION",
      countsTowardFirst100: false,
    },
    {
      hotelIdentity: "Radisson Hotel La Crosse",
      discoveryPropertyId: "disc_cb4_radisson_la_crosse_wi_2026",
      opportunityClass: "ACQUISITION_RELATED_FUTURE_SELECTION",
      countsTowardFirst100: false,
      note: "Not active owner solicitation; post-close optionality watch",
    },
    {
      hotelIdentity: "Grand Teton CC-GRTE001-27",
      discoveryPropertyId: "disc_cb4_grand_teton_cc_grte001_27_2026",
      opportunityClass: "AWARD_PENDING_NOT_OPEN",
      countsTowardFirst100: false,
      note: "Submissions closed — award-watch only",
    },
  ],
  remainingQualificationGaps: [
    "Exact remaining hotel list vs post-Sept 30 dispositions / ahipreit.com (~19) — reconcile property-by-property with latest owner website + any subsequent closings",
    "Whether AHIP will appoint one master manager vs multiple managers — not explicitly decided in disclosures (singular FLI phrasing only)",
    "No public RFP / shortlist / award yet — outreach_ready remains false",
    "Property-level HPC matches all PENDING — Track C identity work required before HE depth on each asset",
    "CUSTOMER_BLOCKED — no customer publication / weekly automation",
  ],
};

const trackCPath = path.join(OUT_DIR, "TRACK_C_HANDOFF.json");
const trackC = {
  asOf: "2026-11-21",
  from: "TRACK_A_CB4",
  to: "TRACK_C",
  publicationState: "CUSTOMER_BLOCKED",
  icVerified: true,
  icPr: 547,
  ahipPortfolioOpportunityFirst: {
    parentOpportunityId: handoff.parentOpportunity.opportunityId,
    parentDiscoveryPropertyId: handoff.parentOpportunity.discoveryPropertyId,
    parentDiscoveryInventoryRecordId: handoff.parentOpportunity.discoveryInventoryRecordId,
    parentFindingId: handoff.parentOpportunity.discoveryFindingId,
    parentFindingRecordId: handoff.parentOpportunity.discoveryFindingRecordId,
    countsAsOneMct: true,
    doNotCreateFictionalPortfolioHpc: true,
    remainingAffectedNamedProperties: remainingNamed.length,
    affectedHotelIdentities: propertyIdentities.map((p) => ({
      hotelName: p.hotelName,
      city: p.city,
      hpcMatchStatus: p.hpcMatchStatus,
    })),
    existingHpcMatches: [],
    missingHpcIdentities: propertyIdentities.map((p) => p.hotelName),
    hotelExplorerPriorityRank: 1,
  },
  handoffs: [
    {
      hotelIdentity: "AHIP Remaining Premium Branded Portfolio (Aimbridge MCT replacement)",
      discoveryPropertyId: PARENT_DISC,
      opportunityClass: "PRIVATE_OPEN_MCT",
      evidence:
        "Aimbridge master HMA + individual HMAs terminate 2027-04-30 after Oct 21 2026 extension; AHIP must retain replacement manager; selection open; no award disclosed",
      country: "US",
      recommendedTrackCAction:
        "Opportunity-first: keep one parent MCT; match property-level HPC for remaining named assets only; no portfolio HPC fiction; HE depth on affected hotels",
      affectedPropertyCount: remainingNamed.length,
    },
    {
      hotelIdentity: "Thimarafushi City Hotel development plot",
      discoveryPropertyId: "disc_cb4_maldives_thimarafushi_city_hotel_2026",
      opportunityClass: "OPEN_COMPETITIVE_DEVELOPMENT",
      evidence: "MOTCA 50y lease bid due 2027-01-07",
      country: "MV",
      recommendedTrackCAction: "Emerging development site — identity staging only",
    },
    {
      hotelIdentity: "Fonadhoo Tourist Hotel plots A/B/C",
      discoveryPropertyIds: [
        "disc_cb4_maldives_fonadhoo_plot_a_2026",
        "disc_cb4_maldives_fonadhoo_plot_b_2026",
        "disc_cb4_maldives_fonadhoo_plot_c_2026",
      ],
      opportunityClass: "OPEN_COMPETITIVE_DEVELOPMENT",
      evidence: "MOTCA three-plot tourist hotel leases; bids due 2027-01-07",
      country: "MV",
      recommendedTrackCAction: "Emerging development sites — identity staging only",
    },
    {
      hotelIdentity: "Cedar Lake Outdoor Center",
      discoveryPropertyId: "disc_cb4_cedar_lake_outdoor_center_mi_2026",
      opportunityClass: "OPEN_CONCESSION",
      evidence: "Michigan DNR lodging/day-camp/event lease; bids due 2027-02-03",
      country: "US",
      recommendedTrackCAction: "Concession universe watch",
    },
    {
      hotelIdentity: "Radisson Hotel La Crosse",
      discoveryPropertyId: "disc_cb4_radisson_la_crosse_wi_2026",
      opportunityClass: "ACQUISITION_RELATED_FUTURE_SELECTION",
      evidence: "Sale listing; Choice management expires 2026-12-31; Hilton PIP completed",
      country: "US",
      recommendedTrackCAction: "Post-close brand/operator optionality watch — not current owner solicitation",
    },
    {
      hotelIdentity: "Grand Teton CC-GRTE001-27",
      discoveryPropertyId: "disc_cb4_grand_teton_cc_grte001_27_2026",
      opportunityClass: "AWARD_PENDING_NOT_OPEN",
      evidence: "NPS solicitation submissions closed — award-watch only",
      country: "US",
      recommendedTrackCAction: "Do not classify as active competitive opportunity",
    },
  ],
};

fs.writeFileSync(trackCPath, `${JSON.stringify(trackC, null, 2)}\n`);
const outPath = path.join(OUT_DIR, "IC_AHIP_PORTFOLIO_TRACK_C_HANDOFF.json");
fs.writeFileSync(outPath, `${JSON.stringify(handoff, null, 2)}\n`);
console.log(
  JSON.stringify(
    {
      ok: true,
      outPath,
      remainingAffected: remainingNamed.length,
      disposed: disposedNamed.length,
      parentInv: handoff.parentOpportunity.discoveryInventoryRecordId,
      parentFind: handoff.parentOpportunity.discoveryFindingRecordId,
      existingHpcMatches: handoff.existingHpcMatches.length,
      missingHpc: handoff.missingHpcIdentities.length,
    },
    null,
    2,
  ),
);
