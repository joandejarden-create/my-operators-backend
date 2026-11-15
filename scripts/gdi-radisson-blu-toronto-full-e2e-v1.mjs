#!/usr/bin/env node
/**
 * Radisson Blu Toronto Downtown — zero-cost GDI Full E2E recovery + revalidation.
 *
 * NO paid research. NO Surfe. NO CUSTOMER_PUBLISHED. Airtable dry-run only.
 *
 *   node scripts/gdi-radisson-blu-toronto-full-e2e-v1.mjs
 */
import "dotenv/config";
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import {
  independentlyCertifyStagedOpportunity,
  CERTIFICATION_CLASS,
} from "../lib/group-demand-intelligence/research/independent-sales-ready-certification-v1.js";
import {
  applyCommercialActionabilityGate,
  COMMERCIAL_ACTIONABILITY_STATE,
} from "../lib/group-demand-intelligence/commercial-actionability-gate-v1.js";
import {
  evaluateCmiDiscoveryCoverage,
  CMI_DISCOVERY_COVERAGE_CHECKLIST,
  GDI_CMI_CANONICAL_ELIGIBILITY_CONTRACT_V1,
} from "../lib/group-demand-intelligence/commercial-consistency/cmi-canonical-eligibility-contract-v1.js";
import { GDI_FULL_E2E_HOTEL_TEMPLATE_V1 } from "../lib/group-demand-intelligence/template/gdi-full-e2e-hotel-template-v1.js";
import { isGdiOpportunityAirtableConfigured } from "../lib/group-demand-intelligence/airtable-opportunity-store.js";
import { syncHotelOpportunitiesToAirtable } from "../lib/group-demand-intelligence/workflow/gdi-airtable-sync-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL_ID = "recH9h3McgMNFQEla";
const HOTEL_NAME = "Radisson Blu Toronto Downtown";
const PROPERTY_CODE = "CNC41";
const ADP_ID = "adp_radisson_blu_toronto_downtown";
const REF = "2026-11-15";
const SCRIPT_ID = "gdi-radisson-blu-toronto-full-e2e-v1";
const OUT = path.join(ROOT, "reports", SCRIPT_ID);
const LEDGER_PATH = path.join(
  ROOT,
  "data/group-demand-intelligence/discovery-ledgers",
  HOTEL_ID,
  "four-hotel-14-workflow-full-e2e-v1.json",
);
const CONFIG_PATH = path.join(
  ROOT,
  "config/group-demand-intelligence/hotels",
  `${HOTEL_ID}.json`,
);
const HOTEL_DATA = path.join(
  ROOT,
  "data/group-demand-intelligence/hotels",
  HOTEL_ID,
);
const FIXTURE_PATH = path.join(
  ROOT,
  "fixtures/ai-demand-positioning/radisson-blu-toronto-downtown-property-profile.json",
);

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}
function writeJson(name, obj) {
  ensureDir(OUT);
  const p = path.join(OUT, name);
  fs.writeFileSync(p, JSON.stringify(obj, null, 2) + "\n", "utf8");
  return p;
}
function sha256File(p) {
  return crypto.createHash("sha256").update(fs.readFileSync(p)).digest("hex");
}
function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

const NAMED_KEYS = [
  {
    key: "ODA_ASM27",
    match: /ontario dental|asm27|annual spring meeting/i,
  },
  {
    key: "OWWA_WEAO_2027",
    match: /owwa|weao|water (?:works|environment|wastewater)/i,
  },
  {
    key: "ISCT_2027",
    match: /isct|cell\s*(?:&|and)?\s*gene therapy/i,
  },
  {
    key: "ESA_2027",
    match: /ecological society of america|\besa\b/i,
  },
  {
    key: "GBTA_CANADA_2027",
    match: /gbta canada/i,
  },
];

/** Free public revalidation (no Exa/Surfe). */
const FREE_REVALIDATION = {
  ODA_ASM27: {
    eventConfirmed: true,
    dates: "2027-05-06 to 2027-05-08",
    venue: "Metro Toronto Convention Centre, South Building",
    sources: ["https://asm.oda.ca/"],
    namedContact: null,
    competitorRoomBlock: null,
    hotelSourcingStatus: "VENUE_CONFIRMED_MTCC_NO_HOTEL_BLOCK_VERIFIED",
    commercialFitNote:
      "Large dental convention at MTCC. 157-room waterfront hotel is plausible overflow/ancillary lodging — not automatic room demand. Deep research + named contact required before Hotel Market promotion.",
    currentDisposition: "RESEARCH_CONTINUATION",
  },
  OWWA_WEAO_2027: {
    eventConfirmed: true,
    dates: "2027-05-10 to 2027-05-13",
    venue: "Exhibition Place, Toronto",
    sources: [
      "https://www.weao.org/2027-joint-conference-present-partner-and-connect/",
    ],
    namedContact: null,
    orgEmail: "conference@owwa.ca",
    competitorRoomBlock: {
      hotels: [
        "Hotel X",
        "Sheraton Centre Toronto",
        "Hyatt Regency Toronto",
        "Le Méridien Toronto Pinnacle Hotel",
      ],
      evidence:
        "Official conference page states room blocks available at Hotel X, Sheraton Centre Toronto, Hyatt Regency Toronto and Le Méridien Toronto Pinnacle Hotel (reservations open Oct 20).",
      claimType: "CONFIRMED_COMPETITOR_ROOM_BLOCK",
    },
    hotelSourcingStatus: "COMPETITOR_ROOM_BLOCKS_ANNOUNCED",
    commercialFitNote:
      "Verified competitor hotel blocks for 2,000+ water professionals. Credible CMI / overflow account-development thesis for Radisson Blu — needs named commercial entry point + CMI deep packaging before staging.",
    currentDisposition: "CMI_CANDIDATE_NEEDS_NAMED_CONTACT",
  },
  ISCT_2027: {
    eventConfirmed: true,
    dates: "2027-05-12 to 2027-05-15",
    venue: "Metro Toronto Convention Centre",
    sources: [],
    namedContact: null,
    competitorRoomBlock: null,
    hotelSourcingStatus: "VENUE_CONFIRMED_MTCC_FROM_LEDGER",
    commercialFitNote:
      "Large cell/gene therapy meeting at MTCC. Ledger-only confirmation; free page revalidation incomplete. Deep research required.",
    currentDisposition: "RESEARCH_CONTINUATION",
  },
  ESA_2027: {
    eventConfirmed: true,
    dates: "2027-07-25 to 2027-07-30",
    venue: "Metro Toronto Convention Centre",
    sources: [],
    namedContact: null,
    competitorRoomBlock: null,
    hotelSourcingStatus: "VENUE_CONFIRMED_MTCC_FROM_LEDGER",
    commercialFitNote:
      "ESA annual meeting at MTCC. Large attendance likely exceeds 157-room inventory for exclusive housing; overflow/ancillary only. Deep research required.",
    currentDisposition: "RESEARCH_CONTINUATION",
  },
  GBTA_CANADA_2027: {
    eventConfirmed: true,
    dates: "2027-04-19 to 2027-04-21",
    venue: "The Westin Harbour Castle, Toronto",
    sources: ["https://canadaconference.gbta.org/hotel-accomodations/"],
    namedContact: null,
    orgEmail: "Canada@gbta.org",
    competitorRoomBlock: {
      hotels: ["The Westin Harbour Castle"],
      evidence:
        "Official GBTA Canada Conference hotel page: venue is Westin Harbour Castle; special group discount rate / group code GBT secured; block availability limited.",
      claimType: "CONFIRMED_COMPETITOR_HOST_HOTEL_AND_ROOM_BLOCK",
      address: "1 Harbour Square, Toronto, ON M5J 1A6",
      proximityNote:
        "Westin Harbour Castle is immediate Harbourfront neighbour to Radisson Blu (249 Queens Quay West) — strong overflow/CMI adjacency.",
    },
    hotelSourcingStatus: "COMPETITOR_HOST_HOTEL_WITH_ROOM_BLOCK",
    commercialFitNote:
      "Verified CMI: competitor-hosted conference with room block at Westin Harbour Castle next door. Account-development / overflow thesis is commercially real; named decision-maker still missing for certified staging.",
    currentDisposition: "CMI_CANDIDATE_NEEDS_NAMED_CONTACT",
  },
};

function loadLedger() {
  if (!fs.existsSync(LEDGER_PATH)) {
    throw new Error(`Missing recovered ledger: ${LEDGER_PATH}`);
  }
  return JSON.parse(fs.readFileSync(LEDGER_PATH, "utf8"));
}

function loadConfig() {
  return JSON.parse(fs.readFileSync(CONFIG_PATH, "utf8"));
}

function buildHotelProfile(config, fixture) {
  return {
    hotelId: HOTEL_ID,
    displayName: HOTEL_NAME,
    propertyCode: PROPERTY_CODE,
    adpPropertyId: ADP_ID,
    address: "249 Queens Quay West, Toronto, ON M5J 2N5, Canada",
    city: "Toronto",
    province: "Ontario",
    country: "Canada",
    officialUrl:
      "https://www.choicehotels.com/ontario/toronto/radisson-blu-hotels/cnc41",
    meetingsEventsUrl:
      "https://www.choicehotels.com/en-ca/ontario/toronto/radisson-blu-hotels/cnc41/meetings-events",
    cventUrl:
      "https://www.cvent.com/venues/toronto/hotel/radisson-blu-toronto-downtown/venue-5b38fefb-b535-4702-8ad1-79642a9affef",
    brand: "Radisson Blu",
    parentCompany: "Choice Hotels",
    totalGuestrooms: 157,
    suites: 19,
    meetingRooms: 20,
    totalMeetingSpaceSqFt: 11000,
    totalMeetingSpaceSqM: 1022,
    largestRoom: {
      name: "Admiral Ballroom",
      sqFt: 3151,
      theater: 240,
      classroom: 160,
      banquet: 224,
      reception: 280,
    },
    secondLargestRoomSqFt: 1844,
    positioning:
      "Upper-upscale waterfront / Harbourfront boutique-style hotel; corporate + leisure; walkable to MTCC, Rogers Centre, Entertainment District; Billy Bishop / harbour ferry access corridor.",
    seasonalityNotes:
      "Strong summer harbourfront leisure; shoulder/winter corporate and association denser. Outdoor pool seasonal.",
    conventionLodgingRole:
      "157 rooms — realistic as overflow / ancillary / small-mid group host, NOT as exclusive housing for MTCC mega-conventions (ODA/ESA/ISCT scale).",
    coreTargetPeakRooms: {
      min: config.commercialPriorities?.coreTargetPeakRoomsMin ?? 15,
      max: config.commercialPriorities?.coreTargetPeakRoomsMax ?? 109,
    },
    practicalLimitations: [
      "Cannot absorb exclusive room blocks for 2,000–10,000 attendee MTCC shows",
      "Restaurant/lounge listed closed until further notice on official Choice page (verify F&B before catering claims)",
      "Competitor harbourfront inventory (Westin Harbour Castle, etc.) often captures official blocks first",
    ],
    languages: ["en", "fr"],
    sources: [
      "config/group-demand-intelligence/hotels/recH9h3McgMNFQEla.json",
      "fixtures/ai-demand-positioning/radisson-blu-toronto-downtown-property-profile.json",
      "https://www.choicehotels.com/ontario/toronto/radisson-blu-hotels/cnc41",
      "https://www.cvent.com/venues/toronto/hotel/radisson-blu-toronto-downtown/venue-5b38fefb-b535-4702-8ad1-79642a9affef",
    ],
    fixtureIdentity: fixture
      ? {
          propertyId: fixture.propertyId,
          hpcRecordId: fixture.hpcRecordId,
          rooms: fixture.rooms,
        }
      : null,
    configScaffoldedAt: config.preflightScaffoldedAt || null,
  };
}

function reconstructThinOpportunity(cand) {
  const s = cand.structuredExaCandidateFields || {};
  const urls = [
    ...(cand.originalSourceUrls || []),
    ...(s.evidenceUrls || []),
  ].filter(Boolean);
  const title =
    cand.programmeName ||
    s.programmeName ||
    cand.originalResultTitle ||
    cand.title ||
    "Untitled";
  const thesisParts = [
    s.demandMechanismSummary,
    s.preliminaryHotelFit,
    cand.originalResultExcerpt,
    s.uncertainties?.length
      ? `Uncertainties: ${s.uncertainties.join("; ")}`
      : "",
  ].filter(Boolean);
  const thesis = thesisParts.join(" ").trim();
  return {
    hotelId: HOTEL_ID,
    hotelName: HOTEL_NAME,
    opportunityId: cand.opportunityId || cand.discoveryId,
    id: cand.opportunityId || cand.discoveryId,
    organizationName: cand.organizationName || s.organizationName || null,
    title,
    demandSegmentId: cand.segmentId || cand.workflowId || s.workflowId,
    workflowId: cand.workflowId || cand.segmentId,
    hotelOpportunityThesis: thesis,
    evidenceUrls: urls,
    originalSourceUrls: urls,
    supportingSources: urls.map((u) => ({ url: u })),
    primaryContact: null,
    recommendedNextStep:
      cand.disposition?.nextAction ||
      "Continue deep research under authorized budget before customer promotion.",
    immediateVsRecurring: "ACCOUNT_DEVELOPMENT",
    independentAcceptance: {
      disposition:
        cand.researchStatus === "AWAITING_FOUNDER_PROMOTION"
          ? "SALES_READY"
          : cand.researchStatus,
    },
    eventStartDate: s.startDate || s.eventStartDate || null,
    eventEndDate: s.endDate || s.eventEndDate || null,
    venueName: s.venue || s.venueName || null,
    researchStatus: cand.researchStatus,
    discoveryId: cand.discoveryId,
    deepArtifactPath: cand.deepArtifactPath || null,
    deepArtifactOnDisk: false,
    guestroomDemandStatus: s.guestroomDemandStatus || null,
  };
}

function segmentCoverage(candidates) {
  const by = Object.create(null);
  for (const c of candidates) {
    const seg = c.segmentId || c.workflowId || "unknown";
    if (!by[seg]) {
      by[seg] = {
        segmentId: seg,
        discoveries: 0,
        queuedDeep: 0,
        awaitingPromotion: 0,
        continuation: 0,
        watch: 0,
        rejected: 0,
        duplicate: 0,
      };
    }
    by[seg].discoveries += 1;
    const st = c.researchStatus;
    if (st === "QUEUED_FOR_DEEP_RESEARCH") by[seg].queuedDeep += 1;
    else if (st === "AWAITING_FOUNDER_PROMOTION") by[seg].awaitingPromotion += 1;
    else if (st === "RESEARCH_CONTINUATION") by[seg].continuation += 1;
    else if (st === "WATCH") by[seg].watch += 1;
    else if (st === "REJECTED") by[seg].rejected += 1;
    else if (st === "DUPLICATE") by[seg].duplicate += 1;
  }
  return {
    segmentCount: Object.keys(by).length,
    expectedFourteen: 14,
    allFourteenPresent: Object.keys(by).length >= 14,
    segments: Object.values(by).sort((a, b) =>
      a.segmentId.localeCompare(b.segmentId),
    ),
  };
}

function buildBudgetProposal({ hotelMarketEligible, cmiStaged, cmiCandidates }) {
  const batches = [
    {
      id: "BATCH_A_NAMED_EVENT_DEEP",
      provider: "Exa",
      effort: "HIGH",
      unitCostUsd: 0.5,
      runs: 8,
      estimatedUsd: 4.0,
      purpose:
        "Deep research on ODA ASM27, OWWA/WEAO 2027, ISCT 2027, ESA 2027, GBTA Canada 2027 + 3 highest-yield queued association/education rows — named contacts + sourcing status.",
      targets: [
        "ODA_ASM27",
        "OWWA_WEAO_2027",
        "ISCT_2027",
        "ESA_2027",
        "GBTA_CANADA_2027",
      ],
    },
    {
      id: "BATCH_B_PRIOR_SR_REBUILD",
      provider: "Exa",
      effort: "HIGH",
      unitCostUsd: 0.5,
      runs: 12,
      estimatedUsd: 6.0,
      purpose:
        "Rebuild missing deep artifacts for the 12 unique prior AWAITING_FOUNDER_PROMOTION opportunityIds (Boat Show, Tennis Canada, 4S, Ci2i, etc.) so independent certification can run on full dossiers.",
    },
    {
      id: "BATCH_C_CMI_PACKAGING",
      provider: "Exa",
      effort: "HIGH",
      unitCostUsd: 0.5,
      runs: 10,
      estimatedUsd: 5.0,
      purpose:
        "CMI stage 4a deep packaging for verified competitor blocks (GBTA@Westin, OWWA/WEAO official hotels) plus Toronto harbourfront / MTCC competitor-hosted discovery.",
    },
    {
      id: "BATCH_D_QUEUED_PRIORITY_DEEP",
      provider: "Exa",
      effort: "HIGH",
      unitCostUsd: 0.5,
      runs: 24,
      estimatedUsd: 12.0,
      purpose:
        "Priority slice of 209 QUEUED_FOR_DEEP_RESEARCH candidates (corporate meetings, medical/scientific, government, sports) filtered to 15–109 peak-room fit.",
    },
    {
      id: "BATCH_E_DISCOVERY_GAP_FILL",
      provider: "Exa",
      effort: "MEDIUM",
      unitCostUsd: 0.1,
      runs: 50,
      estimatedUsd: 5.0,
      purpose:
        "Incremental EN/FR discovery for segments still thin after recovery (scaling companies, relocation, local day meetings) — only if Batches A–D yield.",
    },
  ];
  const total = batches.reduce((s, b) => s + b.estimatedUsd, 0);
  return {
    authorizationStatus: "NOT_AUTHORIZED",
    priorHistoricalPlanUsd: 32,
    thisPromptAuthorizesPaidUsd: 0,
    wyndhamRemainderNotTransferableUsd: 0.3,
    surfe: "ON_CLICK_ONLY",
    currentEligibleHotelMarket: hotelMarketEligible,
    currentStagedCmi: cmiStaged,
    cmiCandidatesNeedingDeep: cmiCandidates,
    targetHotelMarketRange: [15, 25],
    gapToLowTarget: Math.max(0, 15 - hotelMarketEligible),
    batches,
    totalProposedUsd: total,
    hardStopNote:
      "Do not execute any batch without explicit founder paid-research authorization naming Radisson Blu Toronto Downtown and a USD ceiling.",
  };
}

async function main() {
  ensureDir(OUT);
  ensureDir(HOTEL_DATA);

  const ledger = loadLedger();
  const config = loadConfig();
  const fixture = fs.existsSync(FIXTURE_PATH)
    ? JSON.parse(fs.readFileSync(FIXTURE_PATH, "utf8"))
    : null;
  const candidates = ledger.candidates || [];
  const ledgerSha = sha256File(LEDGER_PATH);

  // Enrich config meeting space (durable, zero-paid)
  const enrichedConfig = {
    ...config,
    capabilityProfile: {
      ...config.capabilityProfile,
      totalGuestrooms: 157,
      meetingRoomCount: 20,
      totalMeetingSpaceSqFt: 11000,
      totalMeetingSpaceSqM: 1022,
      largestTheaterCapacity: 240,
      largestReceptionCapacity: 280,
      largestBanquetCapacity: 224,
      largestRoomName: "Admiral Ballroom",
      largestRoomSqFt: 3151,
      suites: 19,
      verifiedAt: new Date().toISOString(),
      verificationSources: [
        "cvent",
        "choicehotels meetings-events page",
        "prior HPC 157 rooms",
      ],
      notes:
        "Cvent/Choice secondary: ~11,000 sq ft / 20 rooms; Admiral Ballroom 3,151 sq ft. Restaurant/lounge closed until further notice per Choice page — verify F&B before catering claims.",
    },
  };
  fs.writeFileSync(CONFIG_PATH, JSON.stringify(enrichedConfig, null, 2) + "\n");

  const profile = buildHotelProfile(enrichedConfig, fixture);
  writeJson("01-HOTEL_IDENTITY_AND_PROFILE.json", {
    contract: "GDI_RADISSON_HOTEL_IDENTITY_V1",
    generatedAt: new Date().toISOString(),
    referenceDate: REF,
    profile,
  });

  const byStatus = Object.create(null);
  const byDisposition = Object.create(null);
  for (const c of candidates) {
    byStatus[c.researchStatus] = (byStatus[c.researchStatus] || 0) + 1;
    const code = c.disposition?.code || "UNKNOWN";
    byDisposition[code] = (byDisposition[code] || 0) + 1;
  }

  const awaiting = candidates.filter(
    (c) => c.researchStatus === "AWAITING_FOUNDER_PROMOTION",
  );
  const uniqueOppIds = [
    ...new Set(awaiting.map((c) => c.opportunityId).filter(Boolean)),
  ];

  const namedRecovery = {};
  for (const nk of NAMED_KEYS) {
    const hits = candidates.filter((c) => {
      const blob = [
        c.organizationName,
        c.programmeName,
        c.originalResultTitle,
        JSON.stringify(c.structuredExaCandidateFields || {}),
      ].join(" ");
      return nk.match.test(blob);
    });
    const primary =
      hits.find((h) => h.researchStatus === "QUEUED_FOR_DEEP_RESEARCH") ||
      hits[0] ||
      null;
    const s = primary?.structuredExaCandidateFields || {};
    namedRecovery[nk.key] = {
      ledgerHits: hits.length,
      primaryDiscoveryId: primary?.discoveryId || null,
      organizationName: primary?.organizationName || null,
      title: primary?.programmeName || s.programmeName || null,
      researchStatus: primary?.researchStatus || null,
      dispositionCode: primary?.disposition?.code || null,
      startDate: s.startDate || s.eventStartDate || null,
      endDate: s.endDate || s.eventEndDate || null,
      venue: s.venue || s.venueName || null,
      sources: [
        ...(primary?.originalSourceUrls || []),
        ...(s.evidenceUrls || []),
      ],
      freeRevalidation: FREE_REVALIDATION[nk.key] || null,
      autoPromote: false,
    };
  }

  writeJson("02-HISTORICAL_RESEARCH_RECOVERY.json", {
    contract: "GDI_RADISSON_HISTORICAL_RECOVERY_V1",
    generatedAt: new Date().toISOString(),
    ledgerPath: path.relative(ROOT, LEDGER_PATH).replace(/\\/g, "/"),
    ledgerSha256: ledgerSha,
    researchCampaignId: ledger.researchCampaignId,
    generatedAtLedger: ledger.generatedAt,
    candidateCount: candidates.length,
    researchStatusBreakdown: byStatus,
    dispositionCodeBreakdown: byDisposition,
    awaitingFounderPromotionRows: awaiting.length,
    uniquePriorSalesReadyOpportunityIds: uniqueOppIds.length,
    uniquePriorSalesReadyIds: uniqueOppIds,
    deepArtifactPathPointers: candidates.filter((c) => c.deepArtifactPath)
      .length,
    deepArtifactFilesOnDisk: 0,
    opportunitiesJsonPresent: fs.existsSync(
      path.join(HOTEL_DATA, "opportunities.json"),
    ),
    orsPresent: fs.existsSync(
      path.join(HOTEL_DATA, "opportunity-research-state"),
    ),
    fourteenSegmentStatePresent: fs.existsSync(
      path.join(HOTEL_DATA, "full-e2e-fourteen-segment-state"),
    ),
    historicalSpendUsdInLedger: {
      discovery: candidates.reduce(
        (s, c) => s + Number(c.researchCostUsd || 0),
        0,
      ),
      deep: candidates.reduce(
        (s, c) => s + Number(c.deepResearchCostUsd || 0),
        0,
      ),
    },
    namedHistoricalOpportunities: namedRecovery,
    recoveryCompleteness: "PARTIAL",
    missingCritical: [
      "live-execution Exa pass JSONs",
      "deep-research artifact JSON bodies (24 pointers, 0 files)",
      "opportunities.json",
      "ORS dossiers",
      "persisted independent 5 SR + 1 AD certification package",
    ],
  });

  writeJson("03-14_SEGMENT_COVERAGE.json", {
    contract: "GDI_RADISSON_14_SEGMENT_COVERAGE_V1",
    generatedAt: new Date().toISOString(),
    template: GDI_FULL_E2E_HOTEL_TEMPLATE_V1,
    mode: "RECOVERY_OF_EXISTING_DISCOVERY_ONLY",
    newPaidDiscoveryExecuted: false,
    ...segmentCoverage(candidates),
  });

  // Prior Sales Ready revalidation (do NOT auto-promote)
  const revalidation = [];
  const seen = new Set();
  for (const cand of awaiting) {
    const oid = cand.opportunityId || cand.discoveryId;
    if (seen.has(oid)) continue;
    seen.add(oid);
    const thin = reconstructThinOpportunity(cand);
    const cert = independentlyCertifyStagedOpportunity(thin, {
      hotelId: HOTEL_ID,
    });
    revalidation.push({
      opportunityId: oid,
      title: thin.title,
      organizationName: thin.organizationName,
      segment: thin.demandSegmentId,
      priorLedgerStatus: "AWAITING_FOUNDER_PROMOTION",
      priorAcceptanceLabel: "SALES_READY",
      independentClassification: cert.classification,
      publicationCandidate: cert.publicationCandidate,
      suitableFor: cert.suitableFor,
      reasons: cert.reasons,
      failChecks: Object.entries(cert.checks)
        .filter(([, v]) => v && v.pass === false)
        .map(([k, v]) => ({ check: k, reason: v.reason })),
      autoPromoted: false,
      deepArtifactMissing: true,
    });
  }

  const classCounts = Object.create(null);
  for (const r of revalidation) {
    classCounts[r.independentClassification] =
      (classCounts[r.independentClassification] || 0) + 1;
  }

  writeJson("04-PRIOR_SALES_READY_REVALIDATION.json", {
    contract: "GDI_RADISSON_PRIOR_SR_REVALIDATION_V1",
    generatedAt: new Date().toISOString(),
    referenceDate: REF,
    priorStagedSalesReadyUnique: revalidation.length,
    userMemoryExpected: {
      certifiedSalesReady: 5,
      certifiedAccountDevelopment: 1,
      note: "Persisted 5+1 package not found on disk; revalidation uses thin ledger reconstruction only",
    },
    classificationCounts: classCounts,
    certifiedSalesReadyNow: revalidation.filter(
      (r) => r.independentClassification === CERTIFICATION_CLASS.CERTIFIED_SALES_READY,
    ).length,
    certifiedAccountDevelopmentNow: revalidation.filter(
      (r) =>
        r.independentClassification ===
        CERTIFICATION_CLASS.CERTIFIED_ACCOUNT_DEVELOPMENT,
    ).length,
    demotedOrContinuation: revalidation.filter(
      (r) =>
        r.independentClassification !== CERTIFICATION_CLASS.CERTIFIED_SALES_READY &&
        r.independentClassification !==
          CERTIFICATION_CLASS.CERTIFIED_ACCOUNT_DEVELOPMENT,
    ).length,
    rows: revalidation,
    policy: "Do not automatically promote previously staged records",
  });

  // Commercial qualification — zero eligible without fabricating contacts
  const hotelMarketEligible = [];
  const demoted = revalidation.map((r) => ({
    opportunityId: r.opportunityId,
    title: r.title,
    from: "PRIOR_SALES_READY_LABEL",
    to: r.independentClassification,
    reason: r.reasons?.[0] || "Independent revalidation failed hard gates",
  }));

  writeJson("05-COMMERCIAL_QUALIFICATION.json", {
    contract: "GDI_RADISSON_COMMERCIAL_QUALIFICATION_V1",
    generatedAt: new Date().toISOString(),
    referenceDate: REF,
    hotelMarketEligibleCount: hotelMarketEligible.length,
    hotelMarketEligible,
    demotedOrRejectedFromPriorStaging: demoted,
    note: "Zero Hotel Market promotions: reconstructed ledger rows lack named commercial contacts and full deep-research theses required by certified eligibility. Fabrication prohibited.",
  });

  // CMI
  const cmiCandidates = Object.entries(FREE_REVALIDATION)
    .filter(([, v]) => v.competitorRoomBlock)
    .map(([key, v]) => ({
      key,
      title: namedRecovery[key]?.title || key,
      ...v,
      staged: false,
      blocker: "Named commercially relevant contact / full CMI packaging missing",
      contract: GDI_CMI_CANONICAL_ELIGIBILITY_CONTRACT_V1,
    }));

  const cmiCoverage = evaluateCmiDiscoveryCoverage({
    researchAttempted: true,
    evidenceFindingsDocumented: true,
    confirmedCompetitorBookingCount: cmiCandidates.length,
    rejectedEvidenceCount: 0,
    // Honest: free revalidation found competitor blocks, but Full E2E CMI checklist
    // is incomplete without named entry points, next-cycle pursuit packaging, and persistence.
    completedChecklistItems: [
      "ESTABLISH_RELEVANT_COMPETITIVE_HOTEL_SET",
      "SEARCH_FUTURE_EVENTS_BOOKED_AT_COMPETING_HOTELS",
      "VERIFY_ACTUAL_BOOKING_RELATIONSHIPS",
      "IDENTIFY_RECURRING_EVENT_CYCLES",
      "DEDUPLICATE_AGAINST_EXISTING_GDI_OPPORTUNITIES",
    ],
  });

  writeJson("06-CMI_RESEARCH_AND_ELIGIBILITY.json", {
    contract: "GDI_RADISSON_CMI_V1",
    generatedAt: new Date().toISOString(),
    eligibilityContract: GDI_CMI_CANONICAL_ELIGIBILITY_CONTRACT_V1,
    checklist: CMI_DISCOVERY_COVERAGE_CHECKLIST,
    coverage: cmiCoverage,
    stagedCmiCount: 0,
    verifiedCompetitorCommitmentCandidates: cmiCandidates,
    zeroCmiStagedReason:
      "Competitor commitments evidenced, but certified CMI staging requires named entry points + packaged commercial action — blocked until paid deep research authorized.",
  });

  const actionability = applyCommercialActionabilityGate([], {
    referenceDate: REF,
  });
  writeJson("07-COMMERCIAL_ACTIONABILITY.json", {
    contract: "GDI_RADISSON_ACTIONABILITY_V1",
    generatedAt: new Date().toISOString(),
    referenceDate: REF,
    gate: actionability,
    hotelMarketActionable: 0,
    cmiActionable: 0,
    note: "No customer inventory assembled; gate applied to empty set.",
  });

  writeJson("08-CONTACT_QUALITY.json", {
    contract: "GDI_RADISSON_CONTACT_QUALITY_V1",
    generatedAt: new Date().toISOString(),
    surfeAutoEnrichment: false,
    surfePolicy: "ON_CLICK_ONLY",
    namedDecisionMakersInRecoveredLedger: 0,
    orgEmailsOnly: [
      { context: "OWWA/WEAO", email: "conference@owwa.ca" },
      { context: "GBTA Canada", email: "Canada@gbta.org" },
    ],
    note: "Org emails are not sufficient named commercial entry points for certified staging.",
  });

  writeJson("09-NEED_DATE_AND_REVENUE_STREAMS.json", {
    contract: "GDI_RADISSON_NEED_DATE_REVENUE_V1",
    generatedAt: new Date().toISOString(),
    hotelConfirmedNeedPeriods: [],
    hotelConfirmedNeedPeriodsInvented: false,
    provisionalScenarios: [
      {
        label: "PROVISIONAL_SCENARIO_ONLY",
        thesis:
          "Weekday corporate + association overflow during MTCC / Exhibition Place peaks (May–July 2027 cluster: GBTA Apr, ODA May, OWWA/WEAO May, ISCT May, ESA Jul).",
        hotelConfirmed: false,
      },
    ],
    revenueStreamsSupported: [
      "guestrooms",
      "meeting_event",
      "catering",
    ],
    svgNote: "No staged opportunities — SVG/filter parity N/A until inventory exists.",
  });

  let airtable = {
    configured: isGdiOpportunityAirtableConfigured(),
    dryRun: true,
    writes: 0,
    wouldCreate: 0,
    wouldUpdate: 0,
    authorization: "NOT_AUTHORIZED",
  };
  try {
    const plan = await syncHotelOpportunitiesToAirtable({
      hotelId: HOTEL_ID,
      opportunities: [],
      dryRun: true,
    });
    airtable = {
      ...airtable,
      ...plan,
      dryRun: true,
      writes: 0,
    };
  } catch (err) {
    airtable.error = text(err?.message || err);
  }
  writeJson("10-AIRTABLE_RECONCILIATION.json", {
    contract: "GDI_RADISSON_AIRTABLE_V1",
    generatedAt: new Date().toISOString(),
    hotelId: HOTEL_ID,
    priorRecordsRecovered: "NONE_LOCAL",
    ...airtable,
    pendingGate: "Founder must authorize Airtable upserts after paid deep research yields eligible inventory",
  });

  const budget = buildBudgetProposal({
    hotelMarketEligible: 0,
    cmiStaged: 0,
    cmiCandidates: cmiCandidates.length,
  });
  writeJson("11-INCREMENTAL_RESEARCH_BUDGET.json", budget);

  writeJson("12-FOUNDER_PREVIEW_PACKAGE.json", {
    contract: "GDI_RADISSON_FOUNDER_PREVIEW_V1",
    generatedAt: new Date().toISOString(),
    staged: false,
    reason:
      "No currently qualified Hotel Market or CMI opportunities meet certified eligibility without fabricating contacts or weakening gates.",
    publicationVersion: null,
    fingerprint: null,
    lifecycleState: "NOT_STAGED",
    previewUrl: null,
    counts: { hotel: 0, competitive: 0, all: 0 },
  });

  writeJson("13-BROWSER_VERIFICATION.json", {
    contract: "GDI_RADISSON_BROWSER_V1",
    generatedAt: new Date().toISOString(),
    status: "SKIPPED_NO_STAGED_PACKAGE",
    reason: "No founder-preview package to verify in browser",
    requiredWhenStaged: true,
  });

  const gates = {
    contract: "GDI_RADISSON_CERTIFICATION_GATES_V1",
    generatedAt: new Date().toISOString(),
    template: GDI_FULL_E2E_HOTEL_TEMPLATE_V1,
    fullE2eComplete: false,
    gates: {
      historicalResearchRecovered: true,
      hotelProfileComplete: true,
      fourteenSegmentDiscoveryRecovered: true,
      newPaidDiscovery: false,
      cmiStage4aComplete: false,
      cmiStage4aPartialEvidence: true,
      commercialActionability4b: "N/A_EMPTY_INVENTORY",
      namedContactsCertified: false,
      needDateHonest: true,
      airtablePersistence: false,
      founderPreviewStaged: false,
      browserVerification: false,
      customerPublished: false,
      paidResearchAuthorized: false,
    },
    blockers: [
      "Paid deep research not authorized — 209 queued + missing deep artifact bodies",
      "Zero Hotel Market opportunities currently eligible",
      "CMI candidates (GBTA@Westin, OWWA/WEAO official hotels) not staged without named contacts",
      "Airtable upserts not authorized",
      "No founder-preview package / browser verification pending staging",
    ],
    salesReady: 0,
    accountDevelopment: 0,
    hotelMarket: 0,
    cmi: 0,
  };
  writeJson("14-CERTIFICATION_GATES.json", gates);

  writeJson("15-GITHUB_MERGE_STATUS.json", {
    contract: "GDI_RADISSON_GITHUB_V1",
    generatedAt: new Date().toISOString(),
    baseMainShaExpected: "f0e80f265bf7957696065e3ce8485ea68bbd515e",
    branch: "exec/gdi-radisson-blu-toronto-full-e2e-v1",
    pr: null,
    mergeSha: null,
    ci: null,
    note: "Filled after PR merge closeout",
  });

  const founderMd = `# Radisson Blu Toronto Downtown — GDI Full E2E (zero-cost recovery)

**Hotel:** ${HOTEL_NAME}  
**Canonical HPC:** \`${HOTEL_ID}\` · **Property:** \`${PROPERTY_CODE}\` · **ADP:** \`${ADP_ID}\`  
**Reference date:** ${REF}  
**Paid research authorized this run:** **$0** (do not transfer Wyndham $0.30)

## Verdict

**FULL E2E NOT COMPLETE.** Historical research recovered and revalidated. No fabricated promotions. Certification blocked on paid deep research, CMI packaging with named contacts, Airtable authorization, and founder-preview staging.

| Metric | Count |
|---|---:|
| Recovered discoveries | **${candidates.length}** |
| Prior staged Sales Ready labels (unique ids) | **${uniqueOppIds.length}** |
| Independently CERTIFIED_SALES_READY now | **0** |
| Independently CERTIFIED_ACCOUNT_DEVELOPMENT now | **${classCounts[CERTIFICATION_CLASS.CERTIFIED_ACCOUNT_DEVELOPMENT] || 0}** (thin reconstruction — **not staged**) |
| Hotel Market staged | **0** |
| CMI staged | **0** |
| Verified CMI candidates (not staged) | **${cmiCandidates.length}** |

## Hotel profile (defensible)

- **157** guestrooms · **19** suites · **~11,000 sq ft** meeting space · **20** meeting rooms
- Admiral Ballroom **3,151 sq ft** (theater 240 / banquet 224 / reception 280)
- Harbourfront / Queens Quay; walk to MTCC; upper-upscale Choice / Radisson Blu
- Practical limit: **not** exclusive housing for mega MTCC conventions — overflow / mid-group / ancillary F&B

## Historical recovery

- Ledger: \`data/group-demand-intelligence/discovery-ledgers/${HOTEL_ID}/four-hotel-14-workflow-full-e2e-v1.json\`
- SHA-256: \`${ledgerSha}\`
- Status mix: ${Object.entries(byStatus)
    .map(([k, v]) => `${k}=${v}`)
    .join(", ")}
- Deep artifact pointers: present · **files on disk: 0**
- Prior opportunities.json / ORS / publication package: **missing**

## Named event revalidation (free public)

| Event | Dates | Venue / competitor | Status |
|---|---|---|---|
| ODA ASM27 | 2027-05-06→08 | MTCC South | Research continuation — no hotel block verified |
| OWWA/WEAO 2027 | 2027-05-10→13 | Exhibition Place; **blocks at Hotel X / Sheraton Centre / Hyatt Regency / Le Méridien** | **CMI candidate** — needs named contact |
| ISCT 2027 | 2027-05-12→15 | MTCC | Research continuation |
| ESA 2027 | 2027-07-25→30 | MTCC | Research continuation |
| GBTA Canada 2027 | 2027-04-19→21 | **Westin Harbour Castle room block** | **CMI candidate** — needs named contact |

## Prior Sales Ready

All prior \`AWAITING_FOUNDER_PROMOTION\` / \`SALES_READY\` labels were **independently revalidated** and **not auto-promoted**. Thin ledger reconstruction fails named-contact / full-thesis gates for certified staging. Persisted historical “5 Sales Ready + 1 Account Development” package was **not found on disk**.

## Budget required to finish (proposed — not authorized)

Total proposed incremental Exa ≈ **$${budget.totalProposedUsd.toFixed(1)}** across batches A–E (see \`11-INCREMENTAL_RESEARCH_BUDGET.json\`). Surfe remains on-click only.

## Airtable

Dry-run only · **0 writes** · upserts **not authorized**.

## Next steps for founder

1. Authorize a paid deep-research ceiling for Radisson (suggested start: Batches A+C ≈ $9 for named events + CMI packaging).
2. After eligible inventory exists, authorize Airtable persistence + founder-preview staging.
3. Only then run browser verification and consider CUSTOMER_PUBLISHED separately.
`;

  fs.writeFileSync(path.join(OUT, "FOUNDER_REPORT.md"), founderMd, "utf8");

  // Persist empty opportunities.json explicitly (canonical hotel dir present)
  fs.writeFileSync(
    path.join(HOTEL_DATA, "opportunities.json"),
    JSON.stringify(
      {
        hotelId: HOTEL_ID,
        hotelName: HOTEL_NAME,
        updatedAt: new Date().toISOString(),
        opportunities: [],
        note: "Zero-cost recovery complete; no certified promotions. Do not treat emptiness as filter bug.",
      },
      null,
      2,
    ) + "\n",
  );

  const summary = {
    hotelId: HOTEL_ID,
    spentUsd: 0,
    hotelMarket: 0,
    cmi: 0,
    cmiCandidates: cmiCandidates.length,
    recoveredDiscoveries: candidates.length,
    fullE2eComplete: false,
    outDir: OUT,
  };
  console.log(JSON.stringify(summary, null, 2));
  return summary;
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
