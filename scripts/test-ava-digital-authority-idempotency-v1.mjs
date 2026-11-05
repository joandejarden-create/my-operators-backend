/**
 * Digital Authority merge idempotency regression
 *   node scripts/test-ava-digital-authority-idempotency-v1.mjs
 */
import assert from "node:assert/strict";
import {
  appendDigitalAuthorityActions,
  finalizeDigitalAuthorityBlock,
  DIGITAL_AUTHORITY_FINDING_CLASSES,
} from "../lib/ai-visibility-audit-action-plan/digital-authority-v1.js";
import { resolveAvaResearchAuthorization } from "../lib/ai-visibility-audit-action-plan/research-authorization-v1.js";

const da = finalizeDigitalAuthorityBlock({
  propertyId: "adp_test",
  hotelName: "Test",
  officialWebsite: "https://example.test/",
  executiveFinding: "Test finding",
  sources: [
    {
      referringOrganization: "Visit Test",
      linkingPageUrl: "https://visit.test/hotel",
      hotelDestinationUrl: "https://example.test/",
      findingClass: DIGITAL_AUTHORITY_FINDING_CLASSES.VERIFIED_BACKLINK,
      evidenceUrl: "https://visit.test/hotel",
    },
    // duplicate source
    {
      referringOrganization: "Visit Test",
      linkingPageUrl: "https://visit.test/hotel",
      hotelDestinationUrl: "https://example.test/",
      findingClass: DIGITAL_AUTHORITY_FINDING_CLASSES.VERIFIED_BACKLINK,
      evidenceUrl: "https://visit.test/hotel",
    },
  ],
  opportunities: [
    {
      opportunityId: "opp-1",
      title: "Seek CVB link",
      findingClass: DIGITAL_AUTHORITY_FINDING_CLASSES.POTENTIAL_BACKLINK_OPPORTUNITY,
      scoreFactors: {
        demandRelevance: 8,
        sourceCredibility: 8,
        competitiveDifferentiation: 7,
        implementationLikelihood: 6,
        discoveryValue: 7,
      },
    },
    { opportunityId: "opp-1", title: "Seek CVB link duplicate" },
  ],
  actions: [
    {
      actionId: "AVA-TEST-DA-001",
      headline: "Pursue CVB backlink",
      priority: "HIGH",
      whyItMatters: "Demand",
      accountableOwner: "Hotel",
    },
  ],
});

assert.equal(da.sources.length, 1, "duplicate sources removed");
assert.equal(da.opportunities.length, 1, "duplicate opportunities removed");
assert.equal(da.paidProviderUsed, false);
assert.equal(da.paidResearchUsd, 0);

let payload = {
  property: { propertyId: "adp_test", name: "Test" },
  actions: [
    {
      actionId: "AVA-TEST-001",
      headline: "Core action",
      hotelDecision: "APPROVED",
    },
  ],
  actionOverview: [
    { actionId: "AVA-TEST-001", headline: "Core action" },
  ],
};

payload = appendDigitalAuthorityActions(payload, da);
payload = appendDigitalAuthorityActions(payload, da);
payload = appendDigitalAuthorityActions(payload, da);

const daActions = payload.actions.filter((a) => a.actionId === "AVA-TEST-DA-001");
assert.equal(daActions.length, 1, "no duplicate DA actions after repeat merge");
assert.equal(
  payload.actionOverview.filter((a) => a.actionId === "AVA-TEST-DA-001").length,
  1,
  "no duplicate actionOverview rows"
);
assert.equal(payload.actions.filter((a) => a.actionId === "AVA-TEST-001").length, 1);

// Preserve founder decision on refresh
payload.actions = payload.actions.map((a) =>
  a.actionId === "AVA-TEST-DA-001"
    ? { ...a, hotelDecision: "APPROVED", decisionAt: "2026-11-01" }
    : a
);
const refreshed = appendDigitalAuthorityActions(payload, {
  ...da,
  actions: [
    {
      actionId: "AVA-TEST-DA-001",
      headline: "Pursue CVB backlink (updated copy)",
      priority: "HIGH",
      whyItMatters: "Updated",
    },
  ],
});
const kept = refreshed.actions.find((a) => a.actionId === "AVA-TEST-DA-001");
assert.equal(kept.hotelDecision, "APPROVED");
assert.ok(kept.revisionNote);

const auth = resolveAvaResearchAuthorization({});
assert.equal(auth.paidResearchAuthorized, false);

console.log(
  JSON.stringify(
    {
      ok: true,
      sources: da.sources.length,
      opportunities: da.opportunities.length,
      actionsAfterTripleMerge: daActions.length,
      paidDenied: !auth.paidResearchAuthorized,
    },
    null,
    2
  )
);
