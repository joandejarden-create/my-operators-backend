import assert from "node:assert/strict";
import { classifyCommercialDemandThesis } from "../lib/group-demand-intelligence/commercial-demand-thesis-standard-v1.js";
import { classifyContactEligibility } from "../lib/group-demand-intelligence/contact-eligibility-standard-v1.js";
import { rehydratePersistedExaEvidence } from "../lib/group-demand-intelligence/workflow/gdi-persisted-exa-evidence-rehydrate-v1.js";
import { listGdiSelectableHotels } from "../lib/group-demand-intelligence/repository.js";
import { RESEARCH_ONLY_HOTELS_V1 } from "../lib/group-demand-intelligence/research/gdi-research-only-hotel-inventory-v1.js";

const factCodeOnly = classifyCommercialDemandThesis({
  evidence: [{ supportsFact: "hotel_block", sourceTitle: "Official housing page", sourceUrl: "https://example.org/housing" }],
});
assert.equal(factCodeOnly.thesisStandard, "SPECULATIVE_DEMAND");

const direct = classifyCommercialDemandThesis({
  evidence: [{ supportsFact: "stay_to_play", value: "Teams outside 100 miles must book through the housing system." }],
});
assert.equal(direct.thesisStandard, "CONFIRMED_DEMAND");

const contradictory = classifyCommercialDemandThesis({
  evidence: [{ supportsFact: "hotel_block", value: "No dedicated hotel housing block found on official pages." }],
});
assert.equal(contradictory.thesisStandard, "CONTRADICTORY_OR_NEGATIVE");

const absentLanguage = classifyCommercialDemandThesis({
  evidence: [{ supportsFact: "no_housing_partner", value: "No housing partner or stay-to-play language found in the published rules." }],
});
assert.equal(absentLanguage.thesisStandard, "CONTRADICTORY_OR_NEGATIVE");

const inferred = classifyCommercialDemandThesis({
  evidence: [
    { value: "The 2026 oncology conference is held at the downtown convention center." },
    { value: "1200 delegates are expected." },
  ],
});
assert.equal(inferred.thesisStandard, "RELIABLE_INFERRED_DEMAND");

const inferredCityDateScale = classifyCommercialDemandThesis({
  evidence: [
    { supportsFact: "dates_city", value: "April 20–23, 2027 — Washington, DC", sourceTitle: "ACTS Translational Science" },
    { supportsFact: "peak_rooms_estimate", value: "150–250 peak rooms (from historical ~500–800 attendance)" },
  ],
  estimatedAttendance: "500-800",
  estimatedPeakRooms: "150-250",
});
assert.equal(inferredCityDateScale.thesisStandard, "RELIABLE_INFERRED_DEMAND");

const namedHostHotel = classifyCommercialDemandThesis({
  evidence: [
    { supportsFact: "host_hotel", value: "Crystal Gateway Marriott named for March 7–10, 2027" },
    { supportsFact: "sold_out_precedent", value: "2026 Crystal Gateway block sold out with no official overflow hotel" },
  ],
});
assert.equal(namedHostHotel.thesisStandard, "CONFIRMED_DEMAND");

const historicalHostInferred = classifyCommercialDemandThesis({
  evidence: [
    { supportsFact: "2027_city_dates", value: "Washington, DC — March 15–16, 2027", sourceTitle: "AHIMA Advocacy Summit" },
    { supportsFact: "2026_hotel", value: "Hilton Washington DC National Mall (2026)" },
  ],
});
assert.equal(historicalHostInferred.thesisStandard, "RELIABLE_INFERRED_DEMAND");

const announcedLater = classifyCommercialDemandThesis({
  evidence: [
    { value: "June 7–9, 2027 conference." },
    { value: "Location to be announced." },
    { value: "Over 400 participants." },
  ],
});
assert.equal(announcedLater.thesisStandard, "SPECULATIVE_DEMAND");

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

const meetingsHousingRemit = classifyContactEligibility(
  { name: "Jamie McCormick", role: "Meetings / housing", organization: "NADO" },
  {
    organizationName: "NADO",
    title: "NADO Arlington Washington Conference 2027",
    relationshipToOpportunity: "Influences hotel selection, housing, or room-block inclusion",
  }
);
assert.equal(meetingsHousingRemit.class, "DIRECT_BUYER");

const rehydrated = rehydratePersistedExaEvidence({
  evidence: [{ value: "Teams must book through the housing system.", sourceTitle: "Conference housing block", supportsFact: "hotel_block" }],
});
assert.equal(rehydrated.opportunity.evidence[0].quote, "Teams must book through the housing system.");

const placeholderRehydrated = rehydratePersistedExaEvidence({
  evidence: [{ value: "Official source", sourceTitle: "SSSP 2026 Annual Meeting", supportsFact: "source" }],
});
assert.equal(placeholderRehydrated.opportunity.evidence[0].quote, "SSSP 2026 Annual Meeting");

const selectable = new Set(listGdiSelectableHotels().map((hotel) => hotel.hotelId));
for (const hotel of RESEARCH_ONLY_HOTELS_V1) {
  assert.equal(hotel.visibility, "RESEARCH_ONLY");
  assert.equal(selectable.has(hotel.hotelId), false);
}

console.log(JSON.stringify({ ok: true }));
