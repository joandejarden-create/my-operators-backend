#!/usr/bin/env node
/**
 * Design Hotels Operational Pilot V1 / V1.1 — $0 preflight + cohort package.
 * Does not spend, write Airtable, or publish.
 *
 * V1.1: Preferred European replacement = Memmo Príncipe Real (Lisbon).
 * No Dealality HPC ID verified → do NOT invent; Antigua remains interim Hotel 3.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  runHotelOperationalPreflightBatch,
  HOTEL_PROCESSING_CLASS,
} from "../lib/group-demand-intelligence/workflow/gdi-hotel-operational-preflight-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/gdi-design-hotels-operational-pilot-v1");

/** Preferred Europe replacement — official identity only until HPC exists. */
export const MEMMO_PRINCIPE_REAL_TARGET_V1_1 = Object.freeze({
  name: "Memmo Príncipe Real",
  postureIntent: "BOUTIQUE_LIMITED_MEETINGS",
  country: "Portugal",
  city: "Lisbon",
  hpcId: null,
  marriottMarsha: "LISDM",
  affiliation: "Design Hotels",
  roomsOfficial: 41,
  designHotelsUrl:
    "https://www.designhotels.com/hotels/portugal/lisbon/memmo-principe-real/",
  marriottUrl:
    "https://www.marriott.com/en-us/hotels/lisdm-memmo-principe-real-lisbon-a-member-of-design-hotels/overview/",
  status: "BLOCKED_PENDING_VERIFIED_HPC_ID",
  rationale:
    "Preferred European boutique / limited-meetings replacement. Official Design Hotels + Marriott (LISDM) verified; Dealality HPC ID not found in accessible inventory.",
});

/**
 * Runnable cohort for $0 Preview/Rehearsal.
 * Hotel 3 remains Good Hotel Antigua until Memmo HPC is steward-verified.
 * Antigua HPC is NOT deleted — interim only for this pilot slot.
 */
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
    hpcId: "recGL1epYhEle5SDM",
    name: "Good Hotel Antigua",
    postureIntent: "BOUTIQUE_LIMITED_MEETINGS",
    country: "Guatemala",
    city: "Antigua Guatemala",
    cohortRole: "HOTEL_3_INTERIM_PENDING_MEMMO_HPC",
    rationale:
      "INTERIM only — retains boutique/limited-meetings posture for $0 certification until Memmo Príncipe Real has a verified HPC ID. Not deleted from census.",
  },
]);

/** Target cohort after Memmo HPC enrollment (not executable until hpcId set). */
export const DESIGN_HOTELS_PILOT_TARGET_COHORT_V1_1 = Object.freeze([
  DESIGN_HOTELS_PILOT_COHORT_V1[0],
  DESIGN_HOTELS_PILOT_COHORT_V1[1],
  {
    hpcId: null,
    name: MEMMO_PRINCIPE_REAL_TARGET_V1_1.name,
    postureIntent: MEMMO_PRINCIPE_REAL_TARGET_V1_1.postureIntent,
    country: MEMMO_PRINCIPE_REAL_TARGET_V1_1.country,
    city: MEMMO_PRINCIPE_REAL_TARGET_V1_1.city,
    cohortRole: "HOTEL_3_PREFERRED_EUROPE",
    status: MEMMO_PRINCIPE_REAL_TARGET_V1_1.status,
    marriottMarsha: MEMMO_PRINCIPE_REAL_TARGET_V1_1.marriottMarsha,
    rationale: MEMMO_PRINCIPE_REAL_TARGET_V1_1.rationale,
  },
]);

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

  const packageJson = {
    version: "gdi-design-hotels-operational-pilot-v1-1",
    generatedAt: new Date().toISOString(),
    paidResearch: false,
    airtableWrites: false,
    customerPublication: false,
    europeReplacement: {
      preferred: MEMMO_PRINCIPE_REAL_TARGET_V1_1,
      hpcVerified: false,
      europeanAlternativesWithVerifiedHpcIds: [],
      interimHotel3: DESIGN_HOTELS_PILOT_COHORT_V1[2],
      stopReason:
        "Memmo Príncipe Real lacks a verifiable Dealality HPC ID. No speculative substitution. Antigua retained as interim Hotel 3 only.",
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
    affiliationFlags: AFFILIATION_FLAGS,
    preflight: batch,
    powershell: {
      preview: `.\\scripts\\Invoke-GdiPortfolioProcessing.ps1 -Mode Preview -HotelsFile reports\\gdi-design-hotels-operational-pilot-v1\\design-hotels-pilot-cohort-hotels.txt`,
      rehearsal: `.\\scripts\\Invoke-GdiPortfolioProcessing.ps1 -Mode Rehearsal -HotelsFile reports\\gdi-design-hotels-operational-pilot-v1\\design-hotels-pilot-cohort-hotels.txt`,
      resume:
        ".\\scripts\\Invoke-GdiPortfolioProcessing.ps1 -Mode Resume -ResumeState <state.json> -HotelsFile reports\\gdi-design-hotels-operational-pilot-v1\\design-hotels-pilot-cohort-hotels.txt",
      singleHotelPreview:
        ".\\scripts\\Invoke-GdiPortfolioProcessing.ps1 -HotelId rece0NdV9FCnaFlVS -Mode Preview",
      paidFullE2eNotAuthorized:
        ".\\scripts\\Invoke-GdiPortfolioProcessing.ps1 -Mode FullE2E -HotelsFile reports\\gdi-design-hotels-operational-pilot-v1\\design-hotels-pilot-cohort-hotels.txt -BudgetUSD <N> -AuthorizePaidResearch -AllowLivePilot",
      note: "Same command for all runnable hotels. Paid FullE2E requires separate founder GO. Swap Antigua→Memmo only after verified HPC ID.",
    },
    projectedCostPerHotelUsd: {
      preflight: 0,
      rehearsal: 0,
      proposedLivePilotBand: "8–25 (founder authorize; not executed)",
      aggregateThreeHotelCeilingSuggestedUsd: 60,
      note: "Informed by V1.1 $2 Bethesda/YOTEL pilot economics — that yield was not commercially certified. Enforce portfolio BudgetUSD.",
    },
    engineReady: {
      dryRunPreview: batch.readyCount === batch.count,
      paidExecution: false,
      europeSlotReady: false,
      reason:
        batch.readyCount === batch.count
          ? "Runnable interim cohort (Mexico / Grenada / Antigua interim) passes $0 preflight. Europe Memmo slot BLOCKED pending HPC enrollment. Paid execution blocked until founder hotel+budget GO; prefer IC merge of #564/#566/#567/#570 first."
          : "One or more runnable hotels blocked — see preflight.results",
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
        memmoHpcId: packageJson.europeReplacement.preferred.hpcId,
        interimHotel3: packageJson.europeReplacement.interimHotel3.hpcId,
        engineReadyDryRun: packageJson.engineReady.dryRunPreview,
        NEW_HOTEL: HOTEL_PROCESSING_CLASS.NEW_HOTEL,
      },
      null,
      2
    )
  );
}
