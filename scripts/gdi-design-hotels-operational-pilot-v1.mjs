#!/usr/bin/env node
/**
 * Design Hotels Operational Pilot V1 — $0 preflight + cohort package.
 * Does not spend, write Airtable, or publish.
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

/** Founder-review cohort — contrasting commercial postures + geo diversity. */
export const DESIGN_HOTELS_PILOT_COHORT_V1 = Object.freeze([
  {
    hpcId: "rece0NdV9FCnaFlVS",
    name: "Condesa DF Hotel",
    postureIntent: "URBAN_MEETINGS",
    country: "Mexico",
    city: "Mexico City",
    rationale:
      "Urban CDMX Design Hotels member with documented multi-event meeting inventory (7 event spaces).",
  },
  {
    hpcId: "recaokE8psIbuVeuH",
    name: "LaLuna Boutique Hotels & Villas",
    postureIntent: "LEISURE_DESTINATION",
    country: "Grenada",
    city: "St. George's",
    rationale:
      "Caribbean leisure / destination villa resort — room-night and leisure-group demand; geographic diversification outside Mexico.",
  },
  {
    hpcId: "recGL1epYhEle5SDM",
    name: "Good Hotel Antigua",
    postureIntent: "BOUTIQUE_LIMITED_MEETINGS",
    country: "Guatemala",
    city: "Antigua Guatemala",
    rationale:
      "Independent boutique with co-working (limited meetings) — validates room-night GDI without requiring conference plant.",
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

  const packageJson = {
    version: "gdi-design-hotels-operational-pilot-v1",
    generatedAt: new Date().toISOString(),
    paidResearch: false,
    airtableWrites: false,
    customerPublication: false,
    prBaseline: {
      portfolioCertificationV1: { pr: 562, status: "MERGED" },
      paidPilotControlsV1_1: { pr: 564, status: "OPEN" },
      conversionRecoveryV1_2: { pr: 566, status: "OPEN" },
      evidenceIntegrityV1_3: { pr: 567, status: "OPEN" },
      competitiveIntelligenceIntegrationV1: { pr: 570, status: "OPEN" },
      marketScopeToggle: { pr: 224, status: "MERGED" },
    },
    cohort: DESIGN_HOTELS_PILOT_COHORT_V1,
    affiliationFlags: AFFILIATION_FLAGS,
    preflight: batch,
    powershell: {
      preview: `.\\scripts\\Invoke-GdiPortfolioProcessing.ps1 -Mode Preview -HotelsFile reports\\gdi-design-hotels-operational-pilot-v1\\design-hotels-pilot-cohort-hotels.txt`,
      rehearsal: `.\\scripts\\Invoke-GdiPortfolioProcessing.ps1 -Mode Rehearsal -HotelsFile reports\\gdi-design-hotels-operational-pilot-v1\\design-hotels-pilot-cohort-hotels.txt`,
      resume:
        ".\\scripts\\Invoke-GdiPortfolioProcessing.ps1 -Mode Resume -ResumeState <state.json> -HotelsFile reports\\gdi-design-hotels-operational-pilot-v1\\design-hotels-pilot-cohort-hotels.txt",
      note: "Same command for all three hotels. Paid FullE2E requires separate founder GO + -AllowLivePilot -AuthorizePaidResearch -BudgetUSD.",
    },
    projectedCostPerHotelUsd: {
      preflight: 0,
      rehearsal: 0,
      proposedLivePilotBand: "8–25 (founder authorize; not executed)",
      note: "Subject to Exa/agent rates and MaxCandidates; enforce portfolio BudgetUSD ceiling.",
    },
    engineReady: {
      dryRunPreview: batch.readyCount === batch.count,
      paidExecution: false,
      reason:
        batch.readyCount === batch.count
          ? "All three hotels pass $0 operational preflight / dry-run readiness. Paid execution blocked until founder approves hotels + budget. Upstream PRs #564/#566/#567/#570 remain OPEN — IC merge recommended before paid."
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
        readyCount: batch.readyCount,
        blockedCount: batch.blockedCount,
        byClass: batch.byClass,
        hotelsFile,
        engineReadyDryRun: packageJson.engineReady.dryRunPreview,
        NEW_HOTEL: HOTEL_PROCESSING_CLASS.NEW_HOTEL,
      },
      null,
      2
    )
  );
}