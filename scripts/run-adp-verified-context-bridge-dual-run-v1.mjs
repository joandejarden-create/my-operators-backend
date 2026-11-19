#!/usr/bin/env node
/**
 * Phase 1B PR-1 — Verified Context Bridge dual-run diagnostics (no Airtable writes).
 *
 * Usage:
 *   node scripts/run-adp-verified-context-bridge-dual-run-v1.mjs
 *   node scripts/run-adp-verified-context-bridge-dual-run-v1.mjs --property adp_bethesda_marriott
 */

import { mkdirSync, writeFileSync } from "fs";
import { join } from "path";
import {
  buildVerifiedContextBridgeDiagnostic,
  buildPilotSuiteDiagnostics,
  PILOT_HOTELS,
  REGRESSION_HOTELS,
} from "../lib/ai-demand-positioning/verified-context/index.js";
import { STRATEGIC_SELECTION_PERSISTENCE_PROPOSAL } from "../lib/ai-demand-positioning/verified-context/adp-strategic-selection-airtable-contract-v1.js";

function parseArgs(argv) {
  const out = { property: null };
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === "--property" && argv[i + 1]) {
      out.property = argv[++i];
    }
  }
  return out;
}

function summarizeHotel(diag) {
  if (!diag?.ok) return { propertyId: diag?.propertyId, error: diag?.error };
  return {
    propertyId: diag.propertyId,
    hpcHotelId: diag.hpcHotelId,
    attributeCoverageValidation: diag.attributeCoverageValidation?.status || null,
    measurementComparisonStatus: diag.measurementComparison?.status || null,
    dualRunScoresValid: diag.measurementComparison?.dualRunScoresValid === true,
    selectedCount: diag.selectedAttributes.length,
    eligibleCount: diag.eligibility.eligibleCount,
    eligibleUniqueConceptCount: diag.eligibility.eligibleUniqueConceptCount,
    needsVerification: diag.eligibility.needsVerificationKeys,
    undefinedKeys: diag.eligibility.undefinedKeys,
    conflictingKeys: diag.eligibility.conflictingKeys,
    semanticOverlaps: diag.eligibility.semanticOverlaps,
    minimumCoverage: {
      meetsMinimum: diag.minimumCoverage.meetsMinimum,
      eligibleCount: diag.minimumCoverage.eligibleCount,
      shortfall: diag.minimumCoverage.shortfall,
      flag: diag.minimumCoverage.flag,
      countedAs: "eligibleUniqueConceptCount",
    },
    fixtureDenom: diag.dualRun.fixtureBaseline.realityGap?.totalAttributes,
    bridgedDenom: diag.dualRun.bridgedEligible.realityGap?.totalAttributes,
    fixtureGapScore: diag.measurementComparison?.dualRunScoresValid
      ? diag.dualRun.fixtureBaseline.realityGap?.gapScore
      : null,
    bridgedGapScore: diag.measurementComparison?.dualRunScoresValid
      ? diag.dualRun.bridgedEligible.realityGap?.gapScore
      : null,
    artifactWarning: diag.dualRun.difference.artifactWarning,
    publishedReference: diag.measurementComparison?.publishedReference
      ? {
          periodId: diag.measurementComparison.publishedReference.periodId,
          gapScore: diag.measurementComparison.publishedReference.realityGap?.gapScore,
          dualRunValid: false,
          note: diag.measurementComparison.publishedReference.note,
        }
      : null,
    topSuggestions: (diag.suggestions.topSuggestions || []).slice(0, 5).map((s) => ({
      attribute: s.attribute,
      status: s.suggestedInclusionStatus,
      verifiedSupport: s.verifiedSupport,
      reason: s.reason,
    })),
    researchPeriodId: diag.inventoryPin.researchPeriodId,
  };
}

async function main() {
  const args = parseArgs(process.argv.slice(2));
  const outDir = join(
    process.cwd(),
    "reports/ai-demand-positioning/verified-context-bridge"
  );
  mkdirSync(outDir, { recursive: true });

  let suite;
  if (args.property) {
    const diag = buildVerifiedContextBridgeDiagnostic(args.property);
    suite = {
      contractVersion: diag.contractVersion,
      suite: "SINGLE_PROPERTY",
      pilots: PILOT_HOTELS.some((p) => p.propertyId === args.property) ? [diag] : [],
      regression: REGRESSION_HOTELS.some((p) => p.propertyId === args.property)
        ? [diag]
        : PILOT_HOTELS.some((p) => p.propertyId === args.property)
          ? []
          : [diag],
      isolation: [],
      generatedAt: new Date().toISOString(),
    };
  } else {
    suite = buildPilotSuiteDiagnostics();
  }

  const closeout = {
    contractVersion: suite.contractVersion,
    generatedAt: new Date().toISOString(),
    scope: "PHASE_1B_PR1_DIAGNOSTIC_ONLY",
    airtableWrites: false,
    customerFacingCutover: false,
    observationBackedHotels: suite.observationBackedHotels || [],
    attributeCoverageOnlyHotels: suite.attributeCoverageOnlyHotels || [],
    pilots: (suite.pilots || []).map(summarizeHotel),
    regression: (suite.regression || []).map(summarizeHotel),
    isolation: suite.isolation,
    airtableSelectionPersistenceProposal: STRATEGIC_SELECTION_PERSISTENCE_PROPOSAL,
    founderGatesBeforePr2OrWrites: [
      "Approve OPTION_B schema implementation details before Airtable creation/writes",
      "Fixture selections migrate as review-required — not auto-APPROVED",
      "Approve YOTEL Y1 defs+parser only after factual/strategic/recognition review (D2)",
      "Do not activate production research-blocking minimum-12 gate without pilot review",
      "Bridged RG remains diagnostic-only (D6)",
    ],
  };

  const stamp = new Date().toISOString().replace(/[:.]/g, "-");
  const fullPath = join(outDir, `phase-1b-dual-run-full-${stamp}.json`);
  const closeoutPath = join(outDir, `phase-1b-dual-run-closeout-${stamp}.json`);
  const latestFull = join(outDir, "phase-1b-dual-run-full-latest.json");
  const latestCloseout = join(outDir, "phase-1b-dual-run-closeout-latest.json");

  writeFileSync(fullPath, JSON.stringify(suite, null, 2) + "\n", "utf8");
  writeFileSync(closeoutPath, JSON.stringify(closeout, null, 2) + "\n", "utf8");
  writeFileSync(latestFull, JSON.stringify(suite, null, 2) + "\n", "utf8");
  writeFileSync(latestCloseout, JSON.stringify(closeout, null, 2) + "\n", "utf8");

  console.log(JSON.stringify({ ok: true, closeoutPath, latestCloseout, summary: closeout.pilots }, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
