#!/usr/bin/env node
/**
 * Sanitize v2: never fall back hotel-thesis fields to occasion summary;
 * strip assessment-status prose from sales surfaces when provisional numeric exists.
 */
import {
  sanitizeOpportunityForCustomerPresentation,
  reconcileHotelFitScoreForDisplay,
  CUSTOMER_PRESENTATION_SANITIZE_VERSION,
} from "../lib/group-demand-intelligence/customer-presentation-sanitize-v1.js";

let failed = 0;
function pass(n) {
  console.log("PASS", n);
}
function fail(n, d) {
  failed += 1;
  console.error("FAIL", n, d || "");
}

if (
  CUSTOMER_PRESENTATION_SANITIZE_VERSION.includes("v2") ||
  CUSTOMER_PRESENTATION_SANITIZE_VERSION.includes("v3")
) {
  pass("sanitize_version_v2_or_v3");
} else fail("sanitize_version_v2_or_v3", CUSTOMER_PRESENTATION_SANITIZE_VERSION);

const occasion =
  "HCAOA Annual Meeting 2027 is dated in Washington, DC advocacy day — hotel TBA with hotel sourcing unconfirmed — a plausible group-lodging conversation for The Mayflower Hotel, Autograph Collection, not proof of an open room block or RFP. Hotel/host is not confirmed — do not treat as open buying.";

const hcaoaLike = {
  opportunityId: "gdi_opp_prospect_mv8fnmjz_832ce0cf",
  title: "HCAOA Annual Meeting 2027",
  summaryWhat: occasion,
  whyHotelDemand: occasion,
  hotelOpportunityThesis:
    "Dimension-level hotel-fit assessment for The Mayflower Hotel, Autograph Collection — overall numeric score withheld until more components are evidenced.",
  summaryWhyHotel:
    "Dimension-level hotel-fit assessment for The Mayflower Hotel, Autograph Collection — overall numeric score withheld until more components are evidenced.",
  fitExplanation:
    "Dimension-level hotel-fit assessment for The Mayflower Hotel, Autograph Collection — overall numeric score withheld until more components are evidenced.",
  hotelFitRationale:
    "Provisional hotel-fit score 63 from 4 assessed dimensions (weights renormalized). Applicable dimensions still lacking evidence: physicalFit, commercialValue, historicalFit.",
  hotelFitScore: 63,
  hotelFitOverallStatus: "provisional",
  hotelFitProvisional: true,
  hotelFitCalculationBasis: {
    numericScoreEmitted: true,
    provisional: true,
  },
  evidenceConfidence: 36,
};

const out = sanitizeOpportunityForCustomerPresentation(hcaoaLike);

if (out.hotelFitScore === 63) pass("provisional_score_kept");
else fail("provisional_score_kept", out.hotelFitScore);

if (!out.hotelOpportunityThesis && !out.summaryWhyHotel && !out.whyThisHotel) {
  pass("no_occasion_fallback_for_why_hotel");
} else {
  fail("no_occasion_fallback_for_why_hotel", {
    hotelOpportunityThesis: out.hotelOpportunityThesis,
    summaryWhyHotel: out.summaryWhyHotel,
    whyThisHotel: out.whyThisHotel,
  });
}

if (!out.fitExplanation) pass("stale_withheld_fitExplanation_cleared");
else fail("stale_withheld_fitExplanation_cleared", out.fitExplanation);

if (!out.whyHotelDemand) pass("whyHotelDemand_not_occasion_dup");
else fail("whyHotelDemand_not_occasion_dup", out.whyHotelDemand);

if (/provisional hotel-fit score 63/i.test(out.hotelFitRationale || "")) {
  pass("hotelFitRationale_preserved");
} else fail("hotelFitRationale_preserved", out.hotelFitRationale);

const withheld = reconcileHotelFitScoreForDisplay({
  hotelFitScore: 61,
  hotelFitOverallStatus: "insufficient_evidence",
  hotelFitCalculationBasis: { numericScoreEmitted: false },
});
if (withheld === null) pass("withheld_still_clears_flat_score");
else fail("withheld_still_clears_flat_score", withheld);

if (failed) {
  console.error(`\n${failed} failed`);
  process.exit(1);
}
console.log("\ntest-gdi-customer-presentation-sanitize-v2: PASS");
