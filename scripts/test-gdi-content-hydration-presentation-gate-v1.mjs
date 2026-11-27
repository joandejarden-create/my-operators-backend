#!/usr/bin/env node
/**
 * Regression: content hydration gate + evidence-backed titles.
 */
import assert from "node:assert/strict";
import {
  isResearchParagraphTitle,
  resolveEvidenceBackedCommercialTitle,
} from "../lib/group-demand-intelligence/presentation/gdi-evidence-backed-commercial-title-v1.js";
import {
  auditHotelContentHydration,
  auditOpportunityContentHydration,
} from "../lib/group-demand-intelligence/presentation/gdi-content-hydration-presentation-gate-v1.js";
import { buildWyndhamHydratedLocked26 } from "../lib/group-demand-intelligence/publication-package/gdi-wyndham-content-hydration-rematerialize-v1.js";

assert.equal(
  isResearchParagraphTitle(
    "The airport identifies Turner-Flatiron as the design-build JV; Phase 1B begins early 2026",
  ),
  true,
);
assert.equal(isResearchParagraphTitle("Esri User Conference 2027"), false);

const saronic = resolveEvidenceBackedCommercialTitle({
  id: "disc_ecba7545037a",
  organizationName: "Saronic Technologies, Inc.",
  title:
    "Opened an 80,000+ sq ft downtown San Diego facility in October 2025; it is an operations, training and depot facility",
  activityHeadline:
    "Saronic announced a new 80,000+ square-foot San Diego operations facility",
});
assert.match(saronic.displayTitle, /Saronic Technologies/);
assert.match(saronic.displayTitle, /San Diego Expansion/);

const researchSentence = auditOpportunityContentHydration({
  id: "x1",
  organizationName: "Test Co",
  title: "Opened a facility and began production of widgets in January",
  whatIsHappening: "Expansion underway",
  potentialHotelBusiness: "Transient demand possible",
  primaryContact: { name: "Jane Doe", title: "Travel Manager" },
  demandSegmentId: "corporate_accounts_scaling",
  opportunityTypeFilterCategory: "SCALING_COMPANY",
});
assert.equal(researchSentence.pass, false);
assert.ok(
  researchSentence.findings.some((f) => f.code === "RESEARCH_PARAGRAPH_TITLE"),
);

const built = buildWyndhamHydratedLocked26();
assert.equal(built.count, 26);
assert.equal(built.afterAudit.pass, true, "hydrated locked-26 must pass gate");
assert.ok(
  built.titleBeforeAfter.some((r) =>
    /Saronic Technologies – San Diego Expansion/.test(r.after),
  ),
);
assert.ok(
  built.titleBeforeAfter.some((r) =>
    /Bora Biologics – FDA Facility Expansion/.test(r.after),
  ),
);
assert.ok(
  built.titleBeforeAfter.some((r) =>
    /Bora Biologics – GMP Capacity Expansion/.test(r.after),
  ),
);
assert.ok(
  built.titleBeforeAfter.some((r) =>
    /Stacy Witbeck – MTS Rail Improvement/.test(r.after),
  ),
);

const hotelAudit = auditHotelContentHydration(built.opportunities, {
  hotelId: "recnPB3Q02NO8Ma1o",
});
assert.equal(hotelAudit.certification, "CONTENT_HYDRATION_PASS");

console.log(
  JSON.stringify(
    {
      ok: true,
      hydratedCount: built.count,
      afterBlocked: built.afterAudit.blockedCount,
      certification: hotelAudit.certification,
    },
    null,
    2,
  ),
);
