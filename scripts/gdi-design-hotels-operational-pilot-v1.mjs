#!/usr/bin/env node
/**
 * Design Hotels Operational Pilot V1 / V1.1 / V1.2 — $0 preflight + cohort package.
 * Does not spend, write GDI opportunity Airtable, or publish.
 *
 * V1.2: Memmo Príncipe Real (Lisbon) enrolled as HPC rec1t35e3sPerzxYu;
 * Good Hotel Antigua removed from runnable cohort only (HPC retained).
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  runHotelOperationalPreflightBatch,
  HOTEL_PROCESSING_CLASS,
  DEMAND_MIX_POSTURE,
} from "../lib/group-demand-intelligence/workflow/gdi-hotel-operational-preflight-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-design-hotels-operational-pilot-v1");

export const MEMMO_PRINCIPE_REAL_TARGET_V1_1 = Object.freeze({
  name: "Memmo Príncipe Real",
  postureIntent: "BOUTIQUE_LIMITED_MEETINGS",
  country: "Portugal",
  city: "Lisbon",
  hpcId: "rec1t35e3sPerzxYu",
  marriottMarsha: "LISDM",
  affiliation: "Design Hotels",
  roomsOfficial: 41,
  designHotelsUrl:
    "https://www.designhotels.com/hotels/portugal/lisbon/memmo-principe-real/",
  marriottUrl:
    "https://www.marriott.com/en-us/hotels/lisdm-memmo-principe-real-lisbon-a-member-of-design-hotels/overview/",
  status: "HPC_VERIFIED_ENROLLED",
  rationale:
    "European boutique / limited-meetings Design Hotels member. Steward-enrolled after conclusive HPC absence (full-table scan).",
});

/** Final runnable cohort — Mexico / Grenada / Portugal. */
export const DESIGN_HOTELS_PILOT_COHORT_V1 = Object.freeze([
  {
    hpcId: "rece0NdV9FCnaFlVS",
    name: "Condesa DF Hotel",
    postureIntent: "URBAN_MEETINGS",
    country: "Mexico",
    city: "Mexico City",
    cohortRole: "HOTEL_1_RETAINED",
    rationale:
      "Urban CDMX Design Hotels member with documented multi-event meeting inventory (7 event spaces).",
  },
  {
    hpcId: "recaokE8psIbuVeuH",
    name: "LaLuna Boutique Hotels & Villas",
    postureIntent: "LEISURE_DESTINATION",
    country: "Grenada",
    city: "St. George's",
    cohortRole: "HOTEL_2_RETAINED",
    rationale:
      "Caribbean leisure / destination villa resort — room-night and leisure-group demand; geographic diversification outside Mexico.",
  },
  {
    hpcId: "rec1t35e3sPerzxYu",
    name: "Memmo Príncipe Real",
    postureIntent: "BOUTIQUE_LIMITED_MEETINGS",
    country: "Portugal",
    city: "Lisbon",
    cohortRole: "HOTEL_3_EUROPE_VERIFIED",
    marsha: "LISDM",
    roomsOfficial: 41,
    rationale:
      "Verified European Design Hotels property (Lisbon). Replaces Good Hotel Antigua in this pilot cohort only.",
  },
]);

/** Target equals runnable as of V1.2. */
export const DESIGN_HOTELS_PILOT_TARGET_COHORT_V1_1 = DESIGN_HOTELS_PILOT_COHORT_V1;

export const ANTIGUA_REMOVED_FROM_COHORT_ONLY = Object.freeze({
  hpcId: "recGL1epYhEle5SDM",
  name: "Good Hotel Antigua",
  action: "REMOVED_FROM_PILOT_COHORT_ONLY",
  hpcRecordDeleted: false,
  researchDeleted: false,
});

export const AFFILIATION_FLAGS = Object.freeze([
  {
    hpcId: "recOIzFTFK8vTDTJp",
    name: "Tulum, a Tribute Portfolio Hotel",
    flag: "AFFILIATION_UNCERTAIN",
    note: "Appears on Design Hotels sitemap match set but name indicates Tribute Portfolio — do not treat as Design Hotels without steward confirm.",
  },
  {
    hpcId: "recdz5f9TrLZHDVb0",
    name: "City Express by Marriott La Paz",
    flag: "AFFILIATION_UNCERTAIN",
    note: "City Express select-service brand — unlikely Design Hotels soft collection; excluded from pilot cohort.",
  },
  {
    hpcId: "rec2ZOABX5Kz1Ax55",
    name: "Banyan Tree Veya Valle de Guadalupe",
    flag: "AFFILIATION_UNCERTAIN",
    note: "Banyan Tree brand on Design Hotels sitemap row — confirm Design Hotels membership before GDI branding.",
  },
]);

export function buildDesignHotelsPilotPackage(opts = {}) {
  const root = opts.root || ROOT;
  const out = opts.out || OUT;
  fs.mkdirSync(out, { recursive: true });
  const hotelIds = DESIGN_HOTELS_PILOT_COHORT_V1.map((h) => h.hpcId);
  const batch = runHotelOperationalPreflightBatch(hotelIds, {
    root,
    budgetUsd: 0,
  });

  const hotelsFile = path.join(out, "design-hotels-pilot-cohort-hotels.txt");
  fs.writeFileSync(hotelsFile, `${hotelIds.join("\n")}\n`, "utf8");

  const identityPath = path.join(
    root,
    "fixtures/gdi-design-hotels-operational-pilot-v1/memmo-principe-real-identity-v1.json"
  );
  const memmoIdentity = fs.existsSync(identityPath)
    ? JSON.parse(fs.readFileSync(identityPath, "utf8"))
    : MEMMO_PRINCIPE_REAL_TARGET_V1_1;

  const memmoPreflight = batch.results.find((r) => r.hotelId === "rec1t35e3sPerzxYu");

  const packageJson = {
    version: "gdi-design-hotels-operational-pilot-v1-2",
    generatedAt: new Date().toISOString(),
    paidResearch: false,
    airtableOpportunityWrites: false,
    customerPublication: false,
    europeReplacement: {
      preferred: MEMMO_PRINCIPE_REAL_TARGET_V1_1,
      hpcVerified: true,
      hpcId: "rec1t35e3sPerzxYu",
      enrollmentMode: "NEW_RECORD_CREATED",
      europeanAlternativesWithVerifiedHpcIds: [],
      antiguaRemovedFromCohortOnly: ANTIGUA_REMOVED_FROM_COHORT_ONLY,
    },
    prBaseline: {
      designHotelsOperationalPilotV1: { pr: 571, status: "OPEN" },
      portfolioCertificationV1: { pr: 562, status: "MERGED" },
      paidPilotControlsV1_1: { pr: 564, status: "OPEN" },
      conversionRecoveryV1_2: { pr: 566, status: "OPEN" },
      evidenceIntegrityV1_3: { pr: 567, status: "OPEN" },
      competitiveIntelligenceIntegrationV1: { pr: 570, status: "OPEN" },
      marketScopeToggle: { pr: 224, status: "MERGED" },
    },
    runnableCohort: DESIGN_HOTELS_PILOT_COHORT_V1,
    targetCohortV1_1: DESIGN_HOTELS_PILOT_TARGET_COHORT_V1_1,
    memmoIdentity,
    memmoDemandMix: memmoPreflight?.demandMixPosture || DEMAND_MIX_POSTURE.BOUTIQUE_LIMITED_MEETINGS,
    affiliationFlags: AFFILIATION_FLAGS,
    preflight: batch,
    powershell: {
      preview: `.\\scripts\\Invoke-GdiPortfolioProcessing.ps1 -Mode Preview -HotelsFile reports\\gdi-design-hotels-operational-pilot-v1\\design-hotels-pilot-cohort-hotels.txt`,
      rehearsal: `.\\scripts\\Invoke-GdiPortfolioProcessing.ps1 -Mode Rehearsal -HotelsFile reports\\gdi-design-hotels-operational-pilot-v1\\design-hotels-pilot-cohort-hotels.txt`,
      resume:
        ".\\scripts\\Invoke-GdiPortfolioProcessing.ps1 -Mode Resume -ResumeState <state.json> -HotelsFile reports\\gdi-design-hotels-operational-pilot-v1\\design-hotels-pilot-cohort-hotels.txt",
      singleHotelPreview:
        ".\\scripts\\Invoke-GdiPortfolioProcessing.ps1 -HotelId rec1t35e3sPerzxYu -Mode Preview",
      paidFullE2eNotAuthorized:
        ".\\scripts\\Invoke-GdiPortfolioProcessing.ps1 -Mode FullE2E -HotelsFile reports\\gdi-design-hotels-operational-pilot-v1\\design-hotels-pilot-cohort-hotels.txt -BudgetUSD <N> -AuthorizePaidResearch -AllowLivePilot",
      note: "Same PowerShell command for all three hotels. Paid FullE2E requires separate founder GO.",
    },
    projectedCostPerHotelUsd: {
      preflight: 0,
      rehearsal: 0,
      proposedLivePilotBand: "8–25 (founder authorize; not executed)",
      aggregateThreeHotelCeilingSuggestedUsd: 60,
      note: "Informed by V1.1 $2 Bethesda/YOTEL pilot economics — that yield was not commercially certified.",
    },
    engineReady: {
      dryRunPreview: batch.readyCount === batch.count,
      paidExecution: false,
      europeSlotReady: true,
      reason:
        batch.readyCount === batch.count
          ? "Final Mexico / Grenada / Portugal cohort passes $0 preflight. Paid execution blocked until founder budget GO; prefer IC merge of #564/#566/#567/#570 first."
          : "One or more hotels blocked — see preflight.results",
    },
    processingClassCounts: batch.byClass,
  };

  fs.writeFileSync(
    path.join(out, "pilot-package.json"),
    JSON.stringify(packageJson, null, 2)
  );
  fs.writeFileSync(
    path.join(out, "preflight-results.json"),
    JSON.stringify(batch, null, 2)
  );
  return { out, hotelsFile, packageJson, batch };
}

const isMain =
  process.argv[1] &&
  path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);
if (isMain) {
  const { out, hotelsFile, packageJson, batch } = buildDesignHotelsPilotPackage();
  console.log(
    JSON.stringify(
      {
        out,
        version: packageJson.version,
        readyCount: batch.readyCount,
        blockedCount: batch.blockedCount,
        byClass: batch.byClass,
        hotelsFile,
        europeSlotReady: packageJson.engineReady.europeSlotReady,
        memmoHpcId: packageJson.europeReplacement.hpcId,
        cohort: DESIGN_HOTELS_PILOT_COHORT_V1.map((h) => h.hpcId),
        engineReadyDryRun: packageJson.engineReady.dryRunPreview,
        NEW_HOTEL: HOTEL_PROCESSING_CLASS.NEW_HOTEL,
      },
      null,
      2
    )
  );
}
