#!/usr/bin/env node
/**
 * GDI Executive Summary V2 regression — metric integrity + audience gating.
 */
import assert from "node:assert/strict";
import {
  buildGdiExecutiveSummaryV2,
  stripResearchIntelligencePerspective,
  buildDemandComposition,
  resolveCanonicalDemandSegmentLabel,
  EXEC_SUMMARY_PERSPECTIVE,
  GDI_EXECUTIVE_SUMMARY_V2,
} from "../lib/group-demand-intelligence/executive-summary/gdi-executive-summary-v2.js";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/index.js";

const store = createGdiPublicationPackageStore({});
const ptr = store.readPointer("recnPB3Q02NO8Ma1o", "STAGED");
assert.ok(ptr?.publicationVersion, "Wyndham STAGED required for V2 test");
const loaded = store.loadPackage("recnPB3Q02NO8Ma1o", ptr.publicationVersion);
assert.equal(loaded.ok, true);
const opps = loaded.package.opportunities;
assert.equal(opps.length, 26);

const composition = buildDemandComposition(opps);
assert.equal(composition.unspecifiedCount, 0, "no Unspecified when taxonomy hydrated");
assert.ok(composition.rows.some((r) => /Scaling/i.test(r.name)));
assert.ok(composition.rows.some((r) => /Projects/i.test(r.name)));

for (const o of opps) {
  const label = resolveCanonicalDemandSegmentLabel(o);
  assert.notEqual(label, "Unspecified");
}

const withIntel = buildGdiExecutiveSummaryV2({
  hotelId: "recnPB3Q02NO8Ma1o",
  hotelName: "Wyndham San Diego Bayside",
  opportunities: opps,
  founderPreview: true,
  customerAuthorizedInventory: false,
  dataState: "FOUNDER_PREVIEW",
  publicationMeta: {
    publicationVersion: ptr.publicationVersion,
    createdAt: loaded.package.createdAt,
  },
  includeResearchIntelligence: true,
});

assert.equal(withIntel.version, GDI_EXECUTIVE_SUMMARY_V2);
assert.ok(
  withIntel.perspectives.available.includes(
    EXEC_SUMMARY_PERSPECTIVE.RESEARCH_INTELLIGENCE,
  ),
);
assert.ok(withIntel.sections.researchIntelligence);
assert.equal(
  withIntel.sections.executiveCommercialSnapshot.priorityDemandSegments.some(
    (s) => /Unspecified/i.test(s.name),
  ),
  false,
);
assert.equal(
  withIntel.sections.executiveCommercialSnapshot.reportingPeriod.kind,
  "STAGED_PACKAGE",
);
assert.notEqual(
  withIntel.sections.executiveCommercialSnapshot.reportingPeriod.kind,
  "FOUNDER_PREVIEW",
);
assert.equal(
  /gdi_hotel_wide_fqo_|Publication gdi_/i.test(
    withIntel.sections.executiveCommercialSnapshot.reportingPeriod.label || "",
  ),
  false,
  "reporting period label must not expose raw publication package ids",
);
assert.match(
  withIntel.sections.executiveCommercialSnapshot.reportingPeriod.label || "",
  /Research updated [A-Z][a-z]+ \d{1,2}, \d{4}/,
);
assert.equal(
  /STAGED|CURRENT/i.test(
    withIntel.sections.executiveCommercialSnapshot.reportingPeriod.label || "",
  ),
  false,
  "customer-facing reporting period must not embed STAGED/CURRENT jargon",
);
assert.equal(
  withIntel.sections.executiveCommercialSnapshot.reportingPeriod.founderStatusLabel,
  "Staged",
);

const receipt = withIntel.sections.researchReceipt.metrics;
const evidence = receipt.find((m) => m.key === "evidenceSourcesReviewed");
assert.ok(evidence.value > 0, "evidence sources should hydrate from package");

const scorecard = withIntel.sections.researchIntelligence.contactCoverageScorecard;
assert.ok(scorecard, "Admin RI must include contact coverage scorecard");
assert.equal(scorecard.customerFacing, false);
assert.equal(scorecard.opportunityLevel.totalQualifiedOpportunities, 26);
assert.equal(scorecard.opportunityLevel.withVerifiedNamedContacts, 26);
assert.ok(scorecard.uniquePersonLevel.uniquePeople >= 26);
assert.ok(scorecard.uniquePersonLevel.withVerifiedPersonalLinkedIn >= 14);
assert.ok(scorecard.uniquePersonLevel.withUsableProfessionalOutreachRoute >= 26);
assert.ok(
  withIntel.sections.researchIntelligence.metrics.some(
    (m) => m.key === "contactCoverageUsableRoute",
  ),
);

const pri = withIntel.sections.priorityCommercialOpportunities.items[0];
assert.ok(pri.name);
assert.notEqual(pri.segment, "Segment TBD");
assert.equal(
  /CONTACT_IDENTIFICATION_REQUIRED/.test(
    String(pri.contactActionabilityLabel || ""),
  ),
  false,
);

const stripped = stripResearchIntelligencePerspective(withIntel);
assert.equal(stripped.sections.researchIntelligence, undefined);
assert.equal(stripped.sections.salesPerformance, undefined);
assert.equal(
  stripped.perspectives.available.includes(
    EXEC_SUMMARY_PERSPECTIVE.RESEARCH_INTELLIGENCE,
  ),
  false,
);
assert.equal(
  stripped.perspectives.available.includes(
    EXEC_SUMMARY_PERSPECTIVE.SALES_PERFORMANCE,
  ),
  false,
);
assert.deepEqual(stripped.perspectives.available, [
  EXEC_SUMMARY_PERSPECTIVE.EXECUTIVE_OVERVIEW,
]);

const customerish = buildGdiExecutiveSummaryV2({
  hotelId: "recnPB3Q02NO8Ma1o",
  opportunities: opps,
  founderPreview: false,
  customerAuthorizedInventory: true,
  dataState: "CURRENT_AVAILABLE",
  includeResearchIntelligence: false,
});
assert.equal(customerish.sections.researchIntelligence, undefined);

console.log(
  JSON.stringify(
    {
      ok: true,
      version: GDI_EXECUTIVE_SUMMARY_V2,
      segments: composition.rows,
      evidenceSources: evidence.value,
      perspectives: withIntel.perspectives.available,
    },
    null,
    2,
  ),
);
