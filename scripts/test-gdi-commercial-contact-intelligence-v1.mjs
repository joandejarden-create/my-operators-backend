#!/usr/bin/env node
/**
 * Unit gates for Commercial Contact Intelligence V1.
 */
import assert from "node:assert/strict";
import {
  COMMERCIAL_CONTACT_CLASS,
  classifyCommercialContact,
  rejectBuyerPromotionWithoutEvidence,
} from "../lib/group-demand-intelligence/presentation/gdi-commercial-contact-classification-v1.js";
import {
  assertScoresDoNotOverrideAuthorityGates,
  scoreContactChannelRecovery,
  scoreCommercialContactRelevance,
} from "../lib/group-demand-intelligence/presentation/gdi-commercial-contact-scoring-v1.js";
import {
  selectBestCommercialContacts,
  compareBaselineToEnhanced,
} from "../lib/group-demand-intelligence/presentation/gdi-commercial-contact-selection-v1.js";
import {
  inferBuyingEcosystemTargets,
  buildBuyingEcosystemQueryPlan,
} from "../lib/group-demand-intelligence/presentation/gdi-commercial-contact-buying-ecosystem-v1.js";
import {
  defaultEvidenceForKnownFixtures,
  deriveContactLevelChanges,
  CONTACT_LEVEL_CHANGE_TYPES,
  buildContactLevelWeeklyAnnotation,
} from "../lib/group-demand-intelligence/presentation/gdi-commercial-contact-intelligence-v1.js";

// --- Classification gates ---
{
  const titleOnly = classifyCommercialContact(
    { name: "Jane Doe", title: "Hotel Buyer" },
    { evidence: { currentOrganizationAndRole: true } },
  );
  assert.notEqual(titleOnly.class, COMMERCIAL_CONTACT_CLASS.DIRECT_HOTEL_BUYER);
  assert.equal(titleOnly.buyerAuthorityClaimed, false);
}

{
  const verified = classifyCommercialContact(
    { name: "Sam Buyer", title: "Travel Manager" },
    {
      evidence: {
        connectedToDemandActivity: true,
        currentOrganizationAndRole: true,
        confirmedHotelSourcingResponsibility: true,
        supportingSourceUrls: ["https://example.org/policy"],
      },
    },
  );
  assert.equal(verified.class, COMMERCIAL_CONTACT_CLASS.DIRECT_HOTEL_BUYER);
}

{
  const alejandra = classifyCommercialContact(
    {
      name: "Alejandra Vázquez",
      title: "Sales Manager",
      organization: "IGECO",
    },
    {
      evidence: defaultEvidenceForKnownFixtures({
        name: "Alejandra Vázquez",
        title: "Sales Manager",
      }),
    },
  );
  assert.equal(
    alejandra.class,
    COMMERCIAL_CONTACT_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
  );
  assert.match(alejandra.salesFriendlyWhy, /not yet confirmed/i);
}

{
  const rejected = rejectBuyerPromotionWithoutEvidence({
    buyerAuthorityClaimed: true,
    class: COMMERCIAL_CONTACT_CLASS.DIRECT_HOTEL_BUYER,
    personName: "Fake",
    evidence: {},
  });
  assert.equal(rejected.buyerAuthorityClaimed, false);
}

// --- Scores independent of authority ---
{
  const gate = assertScoresDoNotOverrideAuthorityGates();
  assert.equal(gate.ok, true);
  assert.ok(gate.scored.channelRecovery.score >= 40);
}

{
  const ch = scoreContactChannelRecovery({
    email: "a@b.co",
    phone: "+52 33 3819 2333",
    phoneExtensions: ["42333"],
    channelSourceUrl: "https://example.org",
    contactDataOrigin: "PUBLIC_SOURCE",
    channelLastVerifiedAt: new Date().toISOString().slice(0, 10),
  });
  assert.equal(ch.score, 100);
}

// --- Selection: buyer without email kept; reachable entry separate ---
{
  const sel = selectBestCommercialContacts([
    {
      name: "Verified Buyer",
      title: "Travel Manager",
      email: null,
      commercialEvidence: {
        connectedToDemandActivity: true,
        currentOrganizationAndRole: true,
        confirmedHotelSourcingResponsibility: true,
      },
    },
    {
      name: "Easy Email",
      title: "Reception",
      email: "info@example.org",
      commercialEvidence: {
        connectedToDemandActivity: false,
        currentOrganizationAndRole: true,
      },
    },
    {
      name: "Referral Path",
      title: "Sales Manager",
      email: "sales@example.org",
      commercialEvidence: {
        connectedToDemandActivity: true,
        currentOrganizationAndRole: true,
        relevantOperationalOrCommercialInfluence: true,
        credibleReferralRoute: true,
      },
    },
  ]);
  assert.equal(sel.primary.name, "Verified Buyer");
  assert.ok(sel.bestAccessibleEntryPoint);
  assert.notEqual(sel.bestAccessibleEntryPoint.name, "Verified Buyer");
  assert.ok(sel.bestAccessibleEntryPoint.email);
}

{
  const cmp = compareBaselineToEnhanced(
    { name: "Old Person" },
    {
      primary: {
        name: "New Entry",
        classification: COMMERCIAL_CONTACT_CLASS.RELEVANT_COMMERCIAL_ENTRY_POINT,
      },
    },
  );
  assert.equal(cmp.meaningfulCommercialUpgrade, true);
  assert.equal(cmp.newPersonIsNotAutomaticImprovement, true);
}

// --- Ecosystem ---
{
  const eco = inferBuyingEcosystemTargets({
    title: "Ecomondo Mexico 2027",
    organizationName: "IGECO",
    segment: "Trade Exhibition / Association Events",
  });
  assert.ok(eco.flags.isEvent);
  assert.ok(eco.targets.some((t) => t.id === "housing_coordinator"));
  const plan = buildBuyingEcosystemQueryPlan({
    title: "Ecomondo Mexico 2027",
    organizationName: "IGECO",
  });
  assert.ok(plan.queries.some((q) => /hotel|hospedaje|housing|hoteles/i.test(q.query)));
}

// --- Contact-level weekly changes (no demand duplicate) ---
{
  const changes = deriveContactLevelChanges(
    { name: "A", email: null },
    { primaryContact: { name: "A", email: "a@b.co" } },
    null,
    { upgradeType: "SAME_PERSON_CHANNEL_IMPROVED" },
  );
  assert.ok(changes.includes(CONTACT_LEVEL_CHANGE_TYPES.CONTACT_EMAIL_RECOVERED));
  const ann = buildContactLevelWeeklyAnnotation({
    contactChanges: changes,
    runId: "test",
  });
  assert.equal(ann.doesNotCreateDemandOpportunity, true);
  assert.equal(ann.materialContactChange, true);
}

console.log("OK: test-gdi-commercial-contact-intelligence-v1");
