#!/usr/bin/env node
/**
 * Focused drawer hotel-fit display contract:
 * - one overall fit tile (no duplicate Hotel fit meta row)
 * - withheld vs provisional vs numeric consistency
 * - dimension states are labeled (never blank assessed area)
 * - no provider metadata in rendered HTML
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import vm from "node:vm";
import { createRequire } from "node:module";
import { reconcileHotelFitScoreForDisplay } from "../lib/group-demand-intelligence/opportunity-list-dto.js";

const require = createRequire(import.meta.url);
const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const uiPath = path.join(
  ROOT,
  "public/js/group-demand-intelligence/dealality-gdi-ui.js"
);

const code = fs.readFileSync(uiPath, "utf8");
const sandbox = { window: {}, console };
sandbox.window = sandbox;
vm.runInNewContext(code, sandbox);
const UI = sandbox.DealalityGdiUi;
if (!UI || !UI.hotelFitAssessmentSectionHtml) {
  console.error("FAIL: DealalityGdiUi.hotelFitAssessmentSectionHtml missing");
  process.exit(1);
}

let failed = 0;
function pass(name) {
  console.log("PASS", name);
}
function fail(name, detail) {
  failed += 1;
  console.error("FAIL", name, detail || "");
}

// DTO reconcile: withheld clears stale flat score
{
  const n = reconcileHotelFitScoreForDisplay({
    hotelFitScore: 61,
    hotelFitOverallStatus: "insufficient_evidence",
    hotelFitCalculationBasis: {
      numericScoreEmitted: false,
      reason: "Overall numeric withheld — insufficient assessed applicable dimensions",
    },
  });
  if (n === null) pass("dto_withheld_clears_stale_score");
  else fail("dto_withheld_clears_stale_score", n);
}

{
  const n = reconcileHotelFitScoreForDisplay({
    hotelFitScore: 68,
    hotelFitOverallStatus: "provisional",
    hotelFitProvisional: true,
    hotelFitCalculationBasis: { numericScoreEmitted: true, provisional: true },
  });
  if (n === 68) pass("dto_provisional_keeps_numeric");
  else fail("dto_provisional_keeps_numeric", n);
}

const withheldOpp = {
  hotelFitScore: 61,
  hotelFitOverallStatus: "insufficient_evidence",
  hotelFitRationale:
    "Overall numeric withheld — insufficient assessed applicable dimensions (no invented defaults).",
  hotelFitCalculationBasis: {
    numericScoreEmitted: false,
    reason: "Overall numeric withheld — insufficient assessed applicable dimensions",
    insufficientKeys: ["physicalFit", "commercialValue"],
    notApplicableKeys: ["historicalFit"],
  },
  hotelFitDimensionAssessments: {
    physicalFit: { status: "insufficient_evidence", score: null, rationale: "Need room count evidence" },
    geographyFit: { status: "assessed", score: 70, rationale: "In territory", assessmentDate: "2026-10-01", ruleVersion: "gdi-hotel-fit-dim-rules-v2" },
    timing: { status: "assessed", score: 55, rationale: "Ongoing" },
    commercialValue: { status: "insufficient_evidence", score: null },
    historicalFit: { status: "not_applicable", score: null, rationale: "No prior stay pattern claimed" },
    competitiveAccessibility: { status: "assessed", score: 50, rationale: "Open market" },
    contactability: { status: "assessed", score: 80, rationale: "Named path" },
  },
  opportunityQualificationLabel: "Qualify further",
  evidenceConfidence: 38,
  evidenceConfidenceExplanation: "Thin public sourcing.",
  recommendedAction: "Introductory outreach via organization route",
  bookingWindowLabel: "Ongoing relationship",
};

const htmlWithheld = UI.hotelFitAssessmentSectionHtml(withheldOpp, {});
if ((htmlWithheld.match(/Overall Hotel Fit/g) || []).length === 1) {
  pass("single_overall_label");
} else fail("single_overall_label", (htmlWithheld.match(/Overall Hotel Fit/g) || []).length);
if (!/>\s*Hotel fit\s*</i.test(htmlWithheld) && !/gdi-meta-label">Hotel fit</i.test(htmlWithheld)) {
  pass("no_duplicate_hotel_fit_meta");
} else fail("no_duplicate_hotel_fit_meta");
if (/Withheld/i.test(htmlWithheld) && !/>\s*61\s*</.test(htmlWithheld)) {
  pass("withheld_not_stale_61");
} else fail("withheld_not_stale_61", htmlWithheld.slice(0, 400));
if (/Needs verification/.test(htmlWithheld) && /Not applicable/.test(htmlWithheld)) {
  pass("dimension_states_labeled");
} else fail("dimension_states_labeled");
if (/Sales posture/.test(htmlWithheld) && /Evidence confidence/.test(htmlWithheld) && /Sales action/.test(htmlWithheld)) {
  pass("posture_confidence_action_separate");
} else fail("posture_confidence_action_separate");
if (/38 \/ 100/.test(htmlWithheld)) pass("evidence_confidence_scale");
else fail("evidence_confidence_scale");
if (!/Surfe|Webhound|requestId|providerCost/i.test(htmlWithheld)) {
  pass("no_provider_metadata");
} else fail("no_provider_metadata");

const provisionalOpp = {
  hotelFitScore: 61,
  hotelFitOverallStatus: "provisional",
  hotelFitProvisional: true,
  hotelFitRationale: "Provisional hotel-fit score 61 from 4 assessed dimensions.",
  hotelFitCalculationBasis: {
    numericScoreEmitted: true,
    provisional: true,
    assessedKeys: ["geographyFit", "timing", "competitiveAccessibility", "contactability"],
    insufficientKeys: ["physicalFit"],
  },
  hotelFitDimensionAssessments: withheldOpp.hotelFitDimensionAssessments,
  opportunityQualificationLabel: "Exploratory",
  evidenceConfidence: 42,
  recommendedAction: "Qualify further",
};
const htmlProv = UI.hotelFitAssessmentSectionHtml(provisionalOpp, {});
if (/>\s*61\s*</.test(htmlProv) && /provisional/i.test(htmlProv) && !/Withheld/i.test(htmlProv)) {
  pass("provisional_shows_numeric");
} else fail("provisional_shows_numeric");

const opening = UI.whyThisHotelOpeningHtml({
  summaryWhat: "Growing federal software company expanding DC footprint.",
  whyThisHotel:
    "Mayflower's downtown Connecticut Avenue location and meeting inventory fit visiting client workshops without inventing a room block.",
  venueSourcingStatusLabel: "Hotel sourcing unconfirmed",
});
if (/Connecticut Avenue/.test(opening) && /Sourcing status/.test(opening)) {
  pass("why_this_hotel_hotel_specific");
} else fail("why_this_hotel_hotel_specific", opening);

const genericOpen = UI.whyThisHotelOpeningHtml({
  summaryWhat: "Growing federal software company expanding DC footprint.",
  whyThisHotel: "Growing federal software company expanding DC footprint.",
});
if (/not yet established/i.test(genericOpen)) pass("why_this_hotel_rejects_generic_dup");
else fail("why_this_hotel_rejects_generic_dup", genericOpen);

if (failed) {
  console.error(`\n${failed} failed`);
  process.exit(1);
}
console.log("\ntest-gdi-drawer-hotel-fit-display-v1: PASS");
