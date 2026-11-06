#!/usr/bin/env node
/**
 * Full-field GDI presentation parity + integrity regressions (fixture-only).
 * Partial tile/drawer matches must not be labeled FULL_PARITY_PASS.
 */
import assert from "node:assert/strict";
import {
  dedupeOpportunityContacts,
  validateCustomerPresentationIntegrity,
} from "../lib/group-demand-intelligence/presentation/gdi-customer-presentation-integrity-v1.js";
import {
  buildAuthoritativeCustomerPresentation,
  compareFullCustomerPresentations,
  labelSubsetParityPass,
  PARITY_RESULT,
} from "../lib/group-demand-intelligence/presentation/gdi-full-presentation-parity-v1.js";
import { freezePublishedPresentationFields } from "../lib/group-demand-intelligence/durable-publication/gdi-one-click-hotel-publish-v1.js";

const AYESHA = {
  id: "gdi_s4912_cb2d71ab1b15",
  hotelId: "recKRJjcPnb4tVDDS",
  title: "A'yesha Gaetán Collectives LLC",
  organizationName: "A'yesha Gaetán Collectives LLC",
  commercialInventoryClass: "ACCOUNT_DEVELOPMENT",
  commercialClassification: "ACCOUNT_DEVELOPMENT",
  priority: "WATCHLIST",
  priorityFilterKey: "WATCHLIST",
  salesActionKey: "ENGAGE_PLANNER",
  salesActionPillLabel: "Engage Planner",
  hotelSourcingStatusNormalized: "HOTEL_SOURCING_NOT_CONFIRMED",
  hotelSourcingStatusLabel: "Hotel Sourcing Unconfirmed",
  whatIsHappening:
    "Intermediary destination-wedding planner relationship development — no verified live couple brief.",
  potentialHotelBusiness:
    "Account-development pathway for intimate luxury destination weddings; no production forecast.",
  whyThisHotel: "Strong qualitative fit for intimate luxury all-inclusive destination weddings.",
  whyActNow: "Develop the intermediary relationship before a live programme exists.",
  establishedFacts: [
    "A'yesha is a certified wedding planner and travel designer.",
    "She has personally toured Spice Island Beach Resort.",
  ],
  uncertainties: [
    "No verified live couple brief or room block.",
    "Direct email and contracting authority not verified.",
  ],
  recommendedNextStep:
    "Develop the intermediary account first; ask whether an active Grenada couple brief exists.",
  qualificationQuestions: [
    "Do you currently have a Grenada couple brief?",
  ],
  sources: [
    {
      url: "https://ayeshagaetan.com/spice-island-beach-resort-grenada-review/",
      label: "Spice Island review",
    },
  ],
  primaryContact: {
    name: "A'yesha Gaetán",
    role: "Certified Wedding Planner and Travel Designer",
    title: "Certified Wedding Planner and Travel Designer",
  },
  secondaryContact: {
    name: "Adrienne Brown",
    role: "Owner and Lead Travel Advisor",
    title: "Owner and Lead Travel Advisor",
    organization: "Breathless Getaways",
    whyThisContact: "Host-agency owner supporting independent advisors.",
    contactRelevanceLabel: "INFLUENCER",
  },
  backupContacts: [
    {
      name: "Adrienne Brown",
      role: "Owner and Lead Travel Advisor",
      organization: "Breathless Getaways",
    },
  ],
};

function testDedupeRemovesDuplicateAdrienne() {
  const d = dedupeOpportunityContacts(AYESHA);
  assert.equal(d.primaryContact.name, "A'yesha Gaetán");
  assert.equal(d.secondaryContact.name, "Adrienne Brown");
  assert.equal((d.backupContacts || []).length, 0);
}

function testNamedPrimaryBlocksUnidentifiedCeiling() {
  const bad = {
    ...AYESHA,
    customerContactDrawer: {
      publicCeilingCopy: "Named event contact not publicly identified yet",
    },
  };
  const gate = validateCustomerPresentationIntegrity(bad);
  assert.equal(gate.ok, false);
  assert.ok(
    gate.fails.includes(
      "contacts:named_primary_present_but_classified_unidentified",
    ),
  );
}

function testFreezeBuildsTwoContactAndHash() {
  const frozen = freezePublishedPresentationFields(AYESHA, {
    opportunityId: AYESHA.id,
    hotelId: AYESHA.hotelId,
    establishedFacts: AYESHA.establishedFacts,
    sources: AYESHA.sources,
    whatIsHappening: AYESHA.whatIsHappening,
    potentialHotelBusiness: AYESHA.potentialHotelBusiness,
    whyThisHotel: AYESHA.whyThisHotel,
    whyActNow: AYESHA.whyActNow,
    recommendedNextStep: AYESHA.recommendedNextStep,
    uncertainties: AYESHA.uncertainties,
  });
  assert.equal(frozen.publishedPresentationLocked, true);
  assert.equal(frozen.primaryContact?.name, "A'yesha Gaetán");
  assert.equal(frozen.secondaryContact?.name, "Adrienne Brown");
  assert.equal((frozen.backupContacts || []).length, 0);
  assert.ok(frozen.scalingTwoContactPresentation?.primary?.customerFacingNamed);
  assert.equal(
    frozen.scalingTwoContactPresentation.primary.name,
    "A'yesha Gaetán",
  );
  assert.ok(frozen.customerFacingPresentationHash);
  assert.equal(frozen.presentationIntegrity?.ok, true);
}

function testFullParityPassAndSubsetLabel() {
  const a = buildAuthoritativeCustomerPresentation(AYESHA, { lock: true });
  const b = buildAuthoritativeCustomerPresentation(
    { ...AYESHA, publishedPresentationLocked: true },
    { lock: true },
  );
  const cmp = compareFullCustomerPresentations(a.opportunity, b.opportunity);
  assert.equal(cmp.result, PARITY_RESULT.FULL_PARITY_PASS);
  assert.equal(cmp.ok, true);

  const subset = labelSubsetParityPass("tile_drawer");
  assert.equal(subset.result, PARITY_RESULT.TILE_DRAWER_FIELD_SUBSET_PASS);
  assert.equal(subset.upgradesToFullParity, false);
}

function testFullParityFailsOnContactDrift() {
  const drifted = {
    ...AYESHA,
    primaryContact: {
      name: "Adrienne Brown",
      role: "Owner and Lead Travel Advisor",
    },
  };
  const cmp = compareFullCustomerPresentations(AYESHA, drifted);
  assert.equal(cmp.ok, false);
  assert.ok(
    cmp.result === PARITY_RESULT.FULL_PARITY_FAIL ||
      cmp.result === PARITY_RESULT.INTEGRITY_BLOCKED,
  );
}

const tests = [
  ["dedupe_removes_duplicate_adrienne", testDedupeRemovesDuplicateAdrienne],
  [
    "named_primary_blocks_unidentified_ceiling",
    testNamedPrimaryBlocksUnidentifiedCeiling,
  ],
  ["freeze_builds_two_contact_and_hash", testFreezeBuildsTwoContactAndHash],
  ["full_parity_pass_and_subset_label", testFullParityPassAndSubsetLabel],
  ["full_parity_fails_on_contact_drift", testFullParityFailsOnContactDrift],
];

const results = [];
for (const [name, fn] of tests) {
  try {
    fn();
    results.push({ name, ok: true });
  } catch (err) {
    results.push({ name, ok: false, error: String(err?.message || err) });
  }
}

const ok = results.every((r) => r.ok);
console.log(
  JSON.stringify(
    {
      ok,
      contract: "GDI_FULL_PRESENTATION_PARITY_V1",
      results,
    },
    null,
    2,
  ),
);
process.exit(ok ? 0 : 1);
