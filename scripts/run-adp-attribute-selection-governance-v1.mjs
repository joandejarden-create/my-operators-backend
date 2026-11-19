#!/usr/bin/env node
/**
 * Phase 1C / PR-2 — attribute selection governance suite (no Airtable writes).
 */

import { mkdirSync, writeFileSync } from "fs";
import { join } from "path";
import { buildPhase1cGovernanceSuite } from "../lib/ai-demand-positioning/verified-context/index.js";

function summarize(report) {
  if (!report?.ok) return { propertyId: report?.propertyId, error: report?.error };
  return {
    propertyId: report.propertyId,
    coverageStatus: report.coverageStatus,
    selectedRawCount: report.selectedRawCount,
    uniqueEligibleConceptCount: report.uniqueEligibleConceptCount,
    shortlistCount: report.shortlistCount,
    proposedExclusions: report.proposedExclusions,
    validationSummary: report.validationSummary,
    topActions: (report.topRecommendations || []).slice(0, 12).map((r) => ({
      attribute: r.attribute,
      action: r.recommendedAction,
      selected: r.currentlySelected,
      eligible: r.eligibilityForMeasurement,
      score: r.rankingScore,
    })),
    shortlist: (report.shortlist?.items || []).map((i) => i.attribute),
  };
}

const suite = buildPhase1cGovernanceSuite();
const outDir = join(
  process.cwd(),
  "reports/ai-demand-positioning/attribute-selection-governance"
);
mkdirSync(outDir, { recursive: true });

const closeout = {
  contractVersion: suite.contractVersion,
  generatedAt: suite.generatedAt,
  scope: "PHASE_1C_PR2_DIAGNOSTIC_ONLY",
  airtableWrites: false,
  pilots: (suite.pilots || []).map(summarize),
  regression: (suite.regression || []).map(summarize),
  yotelAirportReview: suite.yotelAirportReview,
  isolation: suite.isolation,
  airtableContractSummary: {
    status: suite.airtableContract.status,
    persistenceClassification: suite.airtableContract.persistenceClassification,
    productionWritesAuthorized: suite.airtableContract.productionWritesAuthorized,
    tables: Object.keys(suite.airtableContract.layers.strategicSelection.tables),
  },
  methodologyCompatibility: suite.methodologyCompatibility,
  founderDecisionsBeforePr3: [
    "Authorize Airtable schema creation for Option B tables",
    "Authorize production writes / fixture REVIEW_REQUIRED migration",
    "Confirm YOTEL Y1 defs+parser after steward evidence confirmation",
    "Confirm whether broader/narrower pairs may count separately by default",
    "Approve any future research-blocking min/max enforcement",
  ],
};

const stamp = new Date().toISOString().replace(/[:.]/g, "-");
writeFileSync(join(outDir, `phase-1c-governance-full-${stamp}.json`), JSON.stringify(suite, null, 2) + "\n");
writeFileSync(join(outDir, "phase-1c-governance-full-latest.json"), JSON.stringify(suite, null, 2) + "\n");
writeFileSync(join(outDir, `phase-1c-governance-closeout-${stamp}.json`), JSON.stringify(closeout, null, 2) + "\n");
writeFileSync(join(outDir, "phase-1c-governance-closeout-latest.json"), JSON.stringify(closeout, null, 2) + "\n");

console.log(JSON.stringify({ ok: true, closeout: {
  pilots: closeout.pilots,
  yotel: closeout.yotelAirportReview?.rows,
  airtable: closeout.airtableContractSummary,
} }, null, 2));
