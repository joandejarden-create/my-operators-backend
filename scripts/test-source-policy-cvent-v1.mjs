#!/usr/bin/env node
/**
 * source-policy-v1 Cvent gates — unit proofs (no Airtable writes).
 *
 *   node scripts/test-source-policy-cvent-v1.mjs
 */
import assert from "node:assert/strict";
import {
  SourceRole,
  SourceContentDomain,
  canPersistAsCanonical,
  canUseForScoring,
  canDisplayToCustomer,
  requiresIndependentVerification,
  resolveMixedProvenance,
  createDiscoveryResearchCandidate,
  classifySourceContentDomain,
  isCventVenueHotelSource,
  isCventEventPlatformSource,
  evaluateSourceGates,
  SOURCE_POLICY_VERSION,
} from "../lib/data-intelligence/source-policy/v1/index.js";
import { isFieldApprovedForSource } from "../lib/research-engine-v2/external-hotel-source-policy.js";
import { applyCventDiscoveryOnlyAdpGuard } from "../lib/hotel-intelligence/adp-attributes/build-adp-hotel-attributes.js";
import { isHiValueScoringEligible } from "../lib/group-demand-intelligence/requalification/hi-enriched-hotel-profile-v1.js";

const CVENT_VENUE =
  "https://www.cvent.com/venues/bethesda/hotel/bethesda-marriott/venue-759e1222-3f0f-4739-9375-e9cb7962c6a4";
const CVENT_EVENT =
  "https://web.cvent.com/event/225a615f-cd58-4c72-8191-38a489105c2d/summary";
const MARRIOTT =
  "https://www.marriott.com/en-us/hotels/wasbm-bethesda-marriott/overview/";

let passed = 0;
function ok(name) {
  passed += 1;
  console.log(`PASS ${name}`);
}

// 1. Cvent venue Rooms cannot persist canonical
{
  const g = canPersistAsCanonical({
    url: CVENT_VENUE,
    field: "Rooms / Keys",
    candidateValue: 407,
  });
  assert.equal(g.ok, false);
  assert.equal(classifySourceContentDomain(CVENT_VENUE), SourceContentDomain.CVENT_VENUE_HOTEL);
  ok("1_cvent_rooms_cannot_persist_canonical");
}

// 2. Cvent venue meeting-space cannot persist HIGH / canonical
{
  const g = canPersistAsCanonical({
    url: CVENT_VENUE,
    field: "totalMeetingSpaceSqFt",
    candidateValue: 19000,
  });
  assert.equal(g.ok, false);
  assert.equal(requiresIndependentVerification({ url: CVENT_VENUE }).required, true);
  ok("2_cvent_meeting_cannot_persist_canonical");
}

// 3. Cvent description prose cannot display as verified customer truth
{
  const g = canDisplayToCustomer({
    url: CVENT_VENUE,
    field: "Hotel Description - Source Text",
    candidateValue: "Located in the heart of downtown...",
  });
  assert.equal(g.ok, false);
  ok("3_cvent_description_not_customer_display");
}

// 4. Cvent-only HI fact cannot activate verified ADP attribute
{
  const guarded = applyCventDiscoveryOnlyAdpGuard({
    attributeName: "Rooms",
    attributeValue: "407",
    sourceUrl: CVENT_VENUE,
    sourceName: "Cvent venue profile",
    usedInAdp: true,
    active: true,
    confidence: "HIGH",
  });
  assert.equal(guarded.usedInAdp, false);
  assert.equal(guarded.needsSourceReview, true);
  assert.equal(guarded.sourceRole, SourceRole.DISCOVERY_ONLY);
  ok("4_adp_cvent_only_attribute_blocked");
}

// 5. Cvent-only hotel capability cannot influence verified GDI fit score
{
  const elig = isHiValueScoringEligible({
    sourceUrl: CVENT_VENUE,
    notes: "sourceRole=DISCOVERY_ONLY",
    field: "roomsKeys",
  });
  assert.equal(elig.ok, false);
  const score = canUseForScoring({
    url: CVENT_VENUE,
    field: "roomsKeys",
  });
  assert.equal(score.ok, false);
  ok("5_gdi_cvent_only_hotel_fit_scoring_blocked");
}

// 6. Cvent event evidence is not globally blocked
{
  assert.equal(
    classifySourceContentDomain(CVENT_EVENT),
    SourceContentDomain.CVENT_EVENT_PLATFORM
  );
  assert.equal(isCventEventPlatformSource(CVENT_EVENT), true);
  assert.equal(isCventVenueHotelSource(CVENT_EVENT), false);
  const evt = canUseForScoring({
    url: CVENT_EVENT,
    field: "event_registration_url",
  });
  assert.equal(evt.ok, true);
  ok("6_cvent_event_evidence_still_allowed");
}

// 7. Cvent discovery can create research candidate
{
  const cand = createDiscoveryResearchCandidate({
    url: CVENT_VENUE,
    field: "Rooms / Keys",
    candidateValue: 407,
  });
  assert.equal(cand.canPersistAsCanonical, false);
  assert.equal(cand.requiresIndependentVerification, true);
  assert.equal(cand.sourceRole, SourceRole.DISCOVERY_ONLY);
  ok("7_discovery_research_candidate_created");
}

// 8. Independent first-party verification can promote candidate
{
  const mixed = resolveMixedProvenance(
    { url: CVENT_VENUE, field: "Rooms / Keys", candidateValue: 407 },
    { url: MARRIOTT, field: "Rooms / Keys", candidateValue: 407 }
  );
  assert.equal(mixed.ok, true);
  assert.equal(mixed.canonicalSource.contentDomain, SourceContentDomain.FIRST_PARTY_HOTEL);
  assert.equal(mixed.discoverySourceRole, SourceRole.DISCOVERY_ONLY);
  const persist = canPersistAsCanonical({
    url: MARRIOTT,
    field: "Rooms / Keys",
    candidateValue: 407,
  });
  assert.equal(persist.ok, true);
  ok("8_independent_first_party_can_promote");
}

// 9. Mixed Cvent + verified follows verified source provenance
{
  const mixed = resolveMixedProvenance(
    { url: CVENT_VENUE, field: "totalMeetingSpaceSqFt", candidateValue: 19000 },
    { url: MARRIOTT, field: "totalMeetingSpaceSqFt", candidateValue: 18976 }
  );
  assert.equal(mixed.ok, true);
  assert.equal(mixed.canonicalSource.candidateValue, 18976);
  assert.notEqual(mixed.canonicalSource.url, CVENT_VENUE);
  ok("9_mixed_provenance_follows_verified");
}

// 10. Unknown remains unknown when verification absent
{
  const gates = evaluateSourceGates({
    url: CVENT_VENUE,
    field: "Rooms / Keys",
    candidateValue: 407,
  });
  assert.equal(gates.canPersistAsCanonical.ok, false);
  assert.equal(gates.canUseForScoring.ok, false);
  assert.equal(gates.canDisplayToCustomer.ok, false);
  assert.equal(gates.requiresIndependentVerification.required, true);
  // External policy also blocks
  const ext = isFieldApprovedForSource("Rooms / Keys", "cvent");
  assert.equal(ext.ok, false);
  ok("10_unknown_stays_unknown_without_verification");
}

console.log(
  JSON.stringify(
    {
      ok: true,
      passed,
      sourcePolicyVersion: SOURCE_POLICY_VERSION,
    },
    null,
    2
  )
);
