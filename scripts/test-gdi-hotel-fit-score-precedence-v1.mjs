#!/usr/bin/env node
/**
 * Focused: withheld/unscored states beat stale numeric fields on import + display.
 * Uses shared resolveStructuredHotelFitNumericScore — not importer-only rules.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { resolveStructuredHotelFitNumericScore } from "../lib/group-demand-intelligence/workflow/hotel-fit-assessment-v1.js";
import { reconcileHotelFitScoreForDisplay } from "../lib/group-demand-intelligence/customer-presentation-sanitize-v1.js";
import { toOpportunityListDto } from "../lib/group-demand-intelligence/opportunity-list-dto.js";
import {
  clearGdiRepositoryReadCache,
  saveHotelProfile,
  saveOpportunities,
  loadOpportunities,
} from "../lib/group-demand-intelligence/repository.js";
const tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-fit-prec-"));
process.env.GDI_DATA_ROOT = tmpRoot;
process.env.GDI_OPPORTUNITIES_PERSISTENCE = "filesystem";
clearGdiRepositoryReadCache();

const hotelId = "recFitPrecTestHotel";
saveHotelProfile(hotelId, {
  hotelId,
  identity: { hotelName: "Fit Precedence Test Hotel" },
});

function caseResolve(name, assessment, expect) {
  const r = resolveStructuredHotelFitNumericScore(assessment);
  assert.equal(r.accept, expect.accept, `${name}.accept`);
  assert.equal(r.score, expect.score, `${name}.score`);
  if (expect.reason) assert.equal(r.reason, expect.reason, `${name}.reason`);
  console.log(`PASS resolve:${name}`);
}

// 1. Version present, scored:false, stale numeric
caseResolve(
  "version_scored_false_stale",
  {
    scoringVersion: "gdi-hotel-fit-v2-renormalized",
    scored: false,
    hotelFitScore: 55,
    numericScoreEmitted: false,
    overallStatus: "insufficient_evidence",
    calculationBasis: {
      numericScoreEmitted: false,
      reason: "Overall numeric withheld — insufficient assessed applicable dimensions (no invented defaults).",
    },
    summary:
      "Dimension-level hotel-fit assessment — overall numeric withheld until more components are evidenced.",
  },
  { accept: false, score: null, reason: "withheld_or_insufficient" }
);

// 2. Explicit withheld overall + stale flat
caseResolve(
  "withheld_stale_flat",
  {
    scored: false,
    hotelFitScore: 61,
    numericScoreEmitted: false,
    calculationBasis: { numericScoreEmitted: false, reason: "Overall numeric withheld" },
  },
  { accept: false, score: null, reason: "withheld_or_insufficient" }
);

// 3. Insufficient evidence + stale legacy
caseResolve(
  "insufficient_stale_legacy",
  {
    overallStatus: "insufficient_evidence",
    hotelFitScore: 48,
    scoringVersion: "gdi-hotel-fit-v1",
  },
  { accept: false, score: null, reason: "withheld_or_insufficient" }
);

// 4. Valid assessed
caseResolve(
  "valid_assessed",
  {
    scored: true,
    numericScoreEmitted: true,
    hotelFitScore: 74,
    scoringVersion: "gdi-hotel-fit-v2-renormalized",
    provisional: false,
    overallStatus: "assessed",
  },
  { accept: true, score: 74 }
);

// 5. Valid provisional
caseResolve(
  "valid_provisional",
  {
    numericScoreEmitted: true,
    hotelFitScore: 63,
    scoringVersion: "gdi-hotel-fit-v2-renormalized-provisional",
    provisional: true,
    overallStatus: "provisional",
    calculationBasis: { numericScoreEmitted: true, provisional: true },
  },
  { accept: true, score: 63 }
);

// 6. Legitimate zero
caseResolve(
  "legitimate_zero",
  {
    numericScoreEmitted: true,
    hotelFitScore: 0,
    scoringVersion: "gdi-hotel-fit-v2-renormalized",
    overallStatus: "assessed",
  },
  { accept: true, score: 0 }
);

// 7. Version alone must not authorize
caseResolve(
  "version_alone",
  {
    scoringVersion: "gdi-hotel-fit-v2-renormalized",
    hotelFitScore: 55,
  },
  { accept: false, score: null, reason: "version_without_numeric_emit" }
);

// Display + list DTO agree with withheld
{
  const opp = {
    id: "gdi_fit_prec_withheld",
    hotelId,
    title: "Withheld Fit Case",
    organizationName: "Example Org",
    hotelFitScore: 55,
    hotelFitAssessment: {
      scoringVersion: "gdi-hotel-fit-v2-renormalized",
      scored: false,
      numericScoreEmitted: false,
      hotelFitScore: 55,
      overallStatus: "insufficient_evidence",
      calculationBasis: {
        numericScoreEmitted: false,
        reason: "Overall numeric withheld — insufficient assessed applicable dimensions.",
      },
      summary: "Overall numeric withheld — insufficient assessed applicable dimensions.",
    },
    hotelFitOverallStatus: "insufficient_evidence",
    hotelFitCalculationBasis: { numericScoreEmitted: false },
  };
  assert.equal(reconcileHotelFitScoreForDisplay(opp), null);
  const dto = toOpportunityListDto(opp);
  assert.equal(dto.hotelFitScore, null);
  console.log("PASS display_dto_withheld");
}

// Isolated persistence: save withheld package then re-load — score stays null
{
  const opp = {
    id: "gdi_fit_prec_persist",
    hotelId,
    title: "Persist Withheld",
    organizationName: "Persist Org",
    hotelFitScore: null,
    hotelFitAssessment: {
      version: "gdi-hotel-fit-assessment-v2",
      scoringVersion: "gdi-hotel-fit-v2-renormalized",
      scored: false,
      numericScoreEmitted: false,
      hotelFitScore: 55,
      overallStatus: "insufficient_evidence",
      calculationBasis: {
        numericScoreEmitted: false,
        reason: "Overall numeric withheld — insufficient assessed applicable dimensions.",
      },
      summary: "Overall numeric withheld",
      dimensions: {},
    },
    hotelFitOverallStatus: "insufficient_evidence",
    hotelFitCalculationBasis: { numericScoreEmitted: false },
  };
  saveOpportunities(hotelId, { hotelId, opportunities: [opp] });
  clearGdiRepositoryReadCache();
  const loaded = loadOpportunities(hotelId);
  const row = (loaded.opportunities || []).find((o) => o.id === opp.id);
  assert.ok(row);
  assert.equal(reconcileHotelFitScoreForDisplay(row), null);
  // Reimport-shaped overlay must not revive stale 55 via version-alone gate
  const resolved = resolveStructuredHotelFitNumericScore({
    ...row.hotelFitAssessment,
    scoringVersion: "gdi-hotel-fit-v2-renormalized",
    hotelFitScore: 55,
  });
  assert.equal(resolved.accept, false);
  assert.equal(resolved.score, null);
  console.log("PASS persist_reimport_withheld");
}

// Valid provisional survives display
{
  const opp = {
    hotelFitScore: 63,
    hotelFitAssessment: {
      numericScoreEmitted: true,
      hotelFitScore: 63,
      provisional: true,
      scoringVersion: "gdi-hotel-fit-v2-renormalized-provisional",
      overallStatus: "provisional",
      calculationBasis: { numericScoreEmitted: true, provisional: true },
    },
    hotelFitOverallStatus: "provisional",
    hotelFitCalculationBasis: { numericScoreEmitted: true, provisional: true },
  };
  assert.equal(reconcileHotelFitScoreForDisplay(opp), 63);
  console.log("PASS display_provisional_kept");
}

console.log("OK gdi-hotel-fit-score-precedence-v1");
