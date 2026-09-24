/**
 * Unit checks for PE V1.6 customer detail enrichment helpers.
 */
import assert from "node:assert/strict";
import {
  normalizePeCustomerSources,
  isPrivateEventOpportunity,
  isVenuePartnership,
  enrichPrivateEventOpportunityDetail,
} from "../lib/group-demand-intelligence/private-events/customer-detail-enrichment.js";

assert.equal(
  isPrivateEventOpportunity({
    opportunityType: "VENUE_PARTNERSHIP",
    demandFamily: "PRIVATE_EVENTS",
  }),
  true
);
assert.equal(isVenuePartnership({ opportunityType: "VENUE_PARTNERSHIP" }), true);
assert.equal(isPrivateEventOpportunity({ opportunityType: "PRIMARY_PURSUIT" }), false);

const sources = normalizePeCustomerSources(
  [
    { url: "https://www.womansclubofbethesda.org/venue/", type: "official_venue", title: "Official" },
    { url: "https://www.bethesdacountryclub.org/", type: "official_venue", title: "Wrong host" },
    { url: "https://www.womansclubofbethesda.org/", type: "OFFICIAL_WEDDING_PAGE", title: "Wedding" },
    { url: "https://www.womansclubofbethesda.org/", type: "dup" },
  ],
  { officialDomain: "womansclubofbethesda.org" }
);
assert.equal(sources.length, 2);
assert.ok(sources.every((s) => s.url.includes("womansclubofbethesda.org")));
assert.ok(sources[0].name);

const stub = {
  id: "gdi_pe_test_detail",
  opportunityType: "VENUE_PARTNERSHIP",
  demandFamily: "PRIVATE_EVENTS",
  organizationName: "Stub Venue",
  commercialContactPath: "ORGANIZATION_PATH",
  onSiteLodgingStatus: "NO_LODGING",
  partnerStatus: "NO_PUBLIC_PARTNER_FOUND",
  eventActivityEvidenceStatus: "STRONG_REPEATED_ACTIVITY",
  lodgingCatchmentFit: "CORE",
  productFit: "STRONG",
  partnershipPotential: "HIGH",
  lodgingCapturePotential: "HIGH",
  distanceMiles: 1.4,
  evidenceConfidence: 85,
  sources: [
    {
      url: "https://stub-venue.example/venue/",
      type: "official_venue",
      title: "Official venue",
    },
  ],
  officialDomain: "stub-venue.example",
  recommendedAction: "Call events team",
  summaryWhyMatters: "Thesis once.",
};

const { opportunity: enriched, peEnriched } = await enrichPrivateEventOpportunityDetail(
  stub,
  { hydrate: false }
);
assert.equal(peEnriched, true);
assert.equal(enriched.detailLayout, "VENUE_PARTNERSHIP");
assert.equal(enriched.eventDateDisplay, "Ongoing partnership opportunity");
assert.ok(enriched.primaryContact?.name);
assert.ok((enriched.sources || []).length >= 1);
assert.ok((enriched.knownVsEstimated?.verified || []).length >= 1);
assert.match(enriched.evidenceConfidenceExplanation, /verified/i);
assert.ok(!/0 verified/i.test(enriched.evidenceConfidenceExplanation));

console.log("test:gdi-private-events-v1-6 OK");
