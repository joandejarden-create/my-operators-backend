#!/usr/bin/env node
/**
 * Regression: internal provider metadata must not reach customer DTOs.
 * Uses the actual Renaissance leak shape (evidence.researchProvider + webhoundUsed
 * + phoneSource WEBHOUND_*) — not a special-cased opportunity ID.
 */
import assert from "node:assert/strict";
import {
  sanitizeOpportunityForCustomerPresentation,
  scrubChannelSourceLabel,
  CUSTOMER_PRESENTATION_SANITIZE_VERSION,
} from "../lib/group-demand-intelligence/customer-presentation-sanitize-v1.js";
import { detectProviderMetadataLeaks } from "../lib/group-demand-intelligence/workflow/full-e2e-verification-adapters-v1.js";
import { assertPublicationWriteReadiness } from "../lib/group-demand-intelligence/workflow/full-e2e-publication-quality-v1.js";

assert.ok(
  CUSTOMER_PRESENTATION_SANITIZE_VERSION.includes("v3"),
  "sanitize version must be v3+"
);

const leakingLegacyShape = {
  id: "gdi_opp_example_legacy_assoc_1",
  title: "Example Annual Meeting",
  organizationName: "Example Association",
  summaryWhat: "Credible overflow housing conversation in Midtown.",
  hotelOpportunityThesis:
    "Times Square location supports overflow when the host hotel fills.",
  hotelFitRationale: "Geography and product fit assessed for Midtown overflow.",
  localConnection: "Event footprint is Times Square / Midtown Manhattan.",
  whyNow: "Next cycle dated; host/overflow not locked.",
  recommendedAction: "Confirm housing list path with meeting manager.",
  webhoundUsed: false,
  ampfyUsed: false,
  phoneSource: "WEBHOUND_FIRST_PARTY",
  primaryContact: {
    name: "Alex Meeting Manager",
    email: "meetings@example.org",
    phone: "212-555-0100",
    phoneSource: "WEBHOUND_FIRST_PARTY",
    whyThisContact: "Named on the official annual meeting page.",
    provider: "webhound",
    surfeRequestId: "should-not-leak",
  },
  evidence: [
    {
      id: "ev1",
      field: "source",
      value: "Official annual meeting page",
      claimKind: "FACT",
      sourceUrl: "https://www.example.org/annual-meeting",
      sourceTitle: "Official Annual Meeting",
      sourceAuthority: "Tier_B",
      accessDate: "2026-09-16",
      researchProvider: "webhound",
      researchLevel: "L5_WEBHOUND",
    },
  ],
  sources: [
    {
      name: "Official Annual Meeting",
      url: "https://www.example.org/annual-meeting",
      date: "2026-09-16",
    },
  ],
};

const cleaned = sanitizeOpportunityForCustomerPresentation(leakingLegacyShape);
const leak = detectProviderMetadataLeaks(cleaned);
assert.equal(leak.leaked, false, `provider leak after sanitize: ${leak.match}`);
assert.equal(cleaned.webhoundUsed, undefined);
assert.equal(cleaned.ampfyUsed, undefined);
assert.ok(!cleaned.evidence?.[0]?.researchProvider);
assert.ok(!cleaned.evidence?.[0]?.researchLevel);
assert.equal(
  cleaned.evidence?.[0]?.sourceUrl,
  "https://www.example.org/annual-meeting",
  "legitimate source URL must be preserved"
);
assert.equal(cleaned.sources?.[0]?.url, "https://www.example.org/annual-meeting");
assert.equal(cleaned.phoneSource, "FIRST_PARTY");
assert.equal(cleaned.primaryContact?.phoneSource, "FIRST_PARTY");
assert.equal(cleaned.primaryContact?.provider, undefined);
assert.equal(cleaned.primaryContact?.surfeRequestId, undefined);
assert.equal(scrubChannelSourceLabel("WEBHOUND_FIRST_PARTY"), "FIRST_PARTY");

// Publication readiness: provisional / insufficient after assessment is OK;
// missing assessment is unfinished work.
const provisionalReady = assertPublicationWriteReadiness({
  ...cleaned,
  hotelFitAssessment: {
    version: "gdi-hotel-fit-assessment-v2",
    assessedAt: "2026-10-16",
    overallStatus: "provisional",
    dimensions: {
      geographyFit: { status: "assessed", score: 70 },
      physicalFit: { status: "insufficient_evidence" },
      guestRooms: { status: "not_applicable" },
    },
  },
  hotelFitRationale: "Geography assessed; rooms N/A for LMC-style overflow pitch.",
  localConnection: "Midtown event footprint.",
  summaryWhat: cleaned.summaryWhat,
  whyThisMatters: cleaned.summaryWhat,
  proposedHotelUse: "Overflow / housing list participation",
  opportunityType: "OVERFLOW_HOUSING",
  primaryContact: cleaned.primaryContact,
  contactEmail: "meetings@example.org",
  recommendedAction: cleaned.recommendedAction,
  whyNow: cleaned.whyNow,
  sources: cleaned.sources,
});
// Drawer contract may still fail on thin packages — assert assessment path alone
// does not treat provisional overall as unfinished.
assert.equal(
  provisionalReady.blockers.some((b) => b.code === "structured_hotel_fit_never_executed"),
  false
);
assert.equal(
  provisionalReady.blockers.some((b) => b.code === "structured_hotel_fit_unknown_state"),
  false
);

const neverAssessed = assertPublicationWriteReadiness({
  ...cleaned,
  hotelFitAssessment: { incomplete: true, overallStatus: "UNKNOWN" },
  hotelFitRationale: "placeholder",
  localConnection: "Midtown",
});
assert.equal(neverAssessed.ok, false);
assert.equal(neverAssessed.incomplete, true);
assert.ok(
  neverAssessed.blockers.some((b) => b.code === "structured_hotel_fit_never_executed")
);

const missingFit = assertPublicationWriteReadiness({
  ...cleaned,
  hotelFitAssessment: null,
  hotelFitRationale: "placeholder",
  localConnection: "Midtown",
});
assert.equal(missingFit.ok, false);
assert.equal(missingFit.incomplete, true);
assert.ok(missingFit.blockers.some((b) => b.code === "structured_hotel_fit_missing"));

console.log("test-gdi-customer-provider-leak-sanitize-v3: PASS");
