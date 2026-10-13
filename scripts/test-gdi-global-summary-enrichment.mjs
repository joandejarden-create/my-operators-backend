/**
 * Tests: global GDI summary enrichment + readiness gate.
 */
import assert from "node:assert/strict";
import {
  buildGdiOpportunitySummary,
  evaluateGdiSummaryQuality,
  isTitleDuplicateSummary,
  SUMMARY_QUALITY,
  applyGdiSummaryEnrichment,
} from "../lib/group-demand-intelligence/opportunity-summary-v1.js";
import {
  classifyWhoHowPath,
  whoResearchAttempted,
  applyGdiWhoHowResolution,
  WHO_PATH_CLASS,
} from "../lib/group-demand-intelligence/opportunity-who-resolution-v1.js";
import {
  isGdiCustomerOpportunityReady,
  READINESS_HOLD,
} from "../lib/group-demand-intelligence/customer-readiness-gate-v1.js";
import {
  evaluateOpportunityEnrichmentNextStep,
  ENRICHMENT_NEXT_STEP,
} from "../lib/group-demand-intelligence/jev-opportunity-enrichment-v1.js";

assert.equal(isTitleDuplicateSummary("AMWA Meeting", "AMWA Meeting"), true);
assert.equal(
  isTitleDuplicateSummary(
    "AMWA annual meeting announced for DC area; hotel TBA.",
    "AMWA Meeting"
  ),
  false
);

{
  const thin = evaluateGdiSummaryQuality({
    title: "76th Annual Meeting",
    summaryWhat: "76th Annual Meeting",
    organizationName: "Society for the Study of Social Problems",
  });
  assert.equal(thin.quality, SUMMARY_QUALITY.THIN);
}

{
  const built = buildGdiOpportunitySummary({
    title: "76th Annual Meeting",
    summaryWhat: "76th Annual Meeting",
    organizationName: "Society for the Study of Social Problems",
    eventStartDate: "2027-08-01",
    eventEndDate: "2027-08-31",
    destinationStatus: "New York, NY",
    opportunityType: "OVERFLOW_HOUSING",
    venueStatus: "NOT_ANNOUNCED",
  });
  assert.ok(built.summaryWhat);
  assert.notEqual(built.summaryWhat, "76th Annual Meeting");
  assert.ok(
    built.summaryQuality === SUMMARY_QUALITY.ADEQUATE ||
      built.summaryQuality === SUMMARY_QUALITY.STRONG
  );
}

{
  const strongKeep = applyGdiSummaryEnrichment({
    title: "NADO 2028",
    organizationName: "NADO",
    summaryWhat:
      "NADO & DDAA Washington Conference March 19–22, 2028 in Arlington; hotel TBA. Housing remains open for early engagement.",
    eventStartDate: "2028-03-19",
    eventEndDate: "2028-03-22",
    destinationStatus: "Arlington, VA",
    venueStatus: "hotel TBA",
  });
  assert.equal(strongKeep.result.changed, false);
  assert.equal(strongKeep.opportunity.summaryQuality, SUMMARY_QUALITY.STRONG);
}

{
  const who = classifyWhoHowPath({
    primaryContact: { name: "Jane Smith", email: "jane@nado.org", role: "Meetings" },
  });
  assert.equal(who.pathClass, WHO_PATH_CLASS.NAMED_DIRECT);
  assert.equal(whoResearchAttempted({ primaryContact: { name: "Jane Smith", email: "a@b.c" } }), true);
  assert.equal(whoResearchAttempted({ title: "x" }), false);
}

{
  const stamped = applyGdiWhoHowResolution(
    { title: "x", organizationName: "Org" },
    { markAttempted: true, ceilingReason: "PUBLIC_DATA_CEILING" }
  );
  assert.equal(stamped.classified.pathClass, WHO_PATH_CLASS.NO_CONTACT_AFTER_RESEARCH);
  assert.equal(whoResearchAttempted(stamped.opportunity), true);
}

{
  const jev = evaluateOpportunityEnrichmentNextStep({
    whoNotResearched: true,
    organizationName: "Org",
    eventTiming: true,
  });
  assert.equal(jev.decision, ENRICHMENT_NEXT_STEP.RESEARCH_WHO);
  assert.equal(jev.safeApply, true);
}

{
  // Insufficient facts → not ready for customer summary fabrication
  const thin = buildGdiOpportunitySummary({ title: "Mystery" });
  assert.equal(thin.summaryInsufficient, true);
}

{
  // Readiness: thin summary + not researched WHO → not ready
  const readyThin = isGdiCustomerOpportunityReady({
    title: "X",
    organizationName: "Org",
    summaryWhat: "X",
    officialSource: "https://example.org",
    eventEndDate: "2028-06-01",
    hotelFitScore: 70,
    whyNow: "Dates approaching",
    recommendedAction: "Monitor housing",
  });
  assert.equal(readyThin.ok, false);
  assert.ok(
    readyThin.failed.includes("summary_quality") ||
      readyThin.failed.includes("who_research_not_attempted") ||
      readyThin.state === READINESS_HOLD.DQ
  );
}

{
  // Readiness: ADEQUATE summary + researched WHO (ceiling) can pass ancillary if present
  const built = applyGdiSummaryEnrichment({
    title: "Advocacy Summit 2027",
    organizationName: "AHIMA",
    eventStartDate: "2027-03-15",
    eventEndDate: "2027-03-16",
    destinationStatus: "Washington, DC",
    venueStatus: "hotel TBA",
    officialSource: "https://example.org/ahima",
    hotelFitScore: 72,
    whyNow: "Housing not announced",
    recommendedAction: "Watch hotel TBA",
  });
  const whoDone = applyGdiWhoHowResolution(built.opportunity, {
    markAttempted: true,
    ceilingReason: "EVENT_CONTACT_NOT_PUBLISHED",
  });
  const ready = isGdiCustomerOpportunityReady(whoDone.opportunity);
  assert.equal(whoDone.classified.pathClass, WHO_PATH_CLASS.NO_CONTACT_AFTER_RESEARCH);
  assert.ok(whoResearchAttempted(whoDone.opportunity));
  // May still fail surface eligibility without full V5A fields — but WHO+summary gates clear
  assert.equal(ready.failed.includes("who_research_not_attempted"), false);
  assert.equal(ready.failed.includes("summary_quality"), false);
}

console.log("test:gdi-global-summary-enrichment OK");
