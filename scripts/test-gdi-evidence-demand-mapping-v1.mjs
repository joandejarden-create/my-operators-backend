import assert from "node:assert/strict";
import { classifyCommercialDemandThesis } from "../lib/group-demand-intelligence/commercial-demand-thesis-standard-v1.js";
import { classifyContactEligibility } from "../lib/group-demand-intelligence/contact-eligibility-standard-v1.js";
import { rehydratePersistedExaEvidence } from "../lib/group-demand-intelligence/workflow/gdi-persisted-exa-evidence-rehydrate-v1.js";
import { listGdiSelectableHotels } from "../lib/group-demand-intelligence/repository.js";
import { RESEARCH_ONLY_HOTELS_V1 } from "../lib/group-demand-intelligence/research/gdi-research-only-hotel-inventory-v1.js";

const direct = classifyCommercialDemandThesis({
  evidence: [{ supportsFact: "hotel_block", sourceTitle: "Official housing page", sourceUrl: "https://example.org/housing" }],
});
assert.equal(direct.thesisStandard, "CONFIRMED_DEMAND");

const inferred = classifyCommercialDemandThesis({
  sources: [{ title: "Regional oncology conference", url: "https://example.org/conference" }],
  evidence: [
    { supportsFact: "event_dates", sourceTitle: "2026 conference dates" },
    { supportsFact: "venue", sourceTitle: "Downtown convention venue" },
    { supportsFact: "2026_attendance", sourceTitle: "1200 delegates expected" },
  ],
});
assert.equal(inferred.thesisStandard, "RELIABLE_INFERRED_DEMAND");

const unsupported = classifyCommercialDemandThesis({
  sources: [{ title: "Example Association", url: "https://example.org/about", supportsFact: "source" }],
  hotelOpportunityThesis: "This organization will need a guestroom block.",
});
assert.equal(unsupported.thesisStandard, "SPECULATIVE_DEMAND");

const intermediary = classifyContactEligibility(
  { name: "Alex Morgan", role: "Destination Management Company Director", email: "alex@example.org" },
  { evidenceQuote: "Alex Morgan at Example DMC is arranging the conference program.", organizationName: "Example DMC" }
);
assert.notEqual(intermediary.class, "DIRECT_BUYER");
assert.equal(/direct hotel-booking remit|direct booking authority/i.test(JSON.stringify(intermediary)), false);

const unrelated = classifyContactEligibility(
  { name: "Pat Lee", role: "Unrelated Accountant", email: "pat@example.org" },
  { organizationName: "Example Association" }
);
assert.notEqual(unrelated.class, "DIRECT_BUYER");
assert.notEqual(unrelated.class, "RELEVANT_COMMERCIAL_ENTRY_POINT");

const rehydrated = rehydratePersistedExaEvidence({
  evidence: [{ sourceTitle: "Conference housing block", sourceUrl: "https://example.org/block", supportsFact: "hotel_block" }],
});
assert.equal(rehydrated.opportunity.evidence[0].quote, "Conference housing block");

const selectable = new Set(listGdiSelectableHotels().map((hotel) => hotel.hotelId));
for (const hotel of RESEARCH_ONLY_HOTELS_V1) {
  assert.equal(hotel.visibility, "RESEARCH_ONLY");
  assert.equal(selectable.has(hotel.hotelId), false);
}

console.log(JSON.stringify({ ok: true }));
