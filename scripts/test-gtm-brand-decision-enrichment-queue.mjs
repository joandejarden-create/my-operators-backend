import test from "node:test";
import assert from "node:assert/strict";
import {
  deriveContactEnrichmentGaps,
  deriveEnrichmentPriority,
  suggestEnrichmentAction,
  toEnrichmentQueueItem,
} from "../lib/gtm-owner-target/branding-decision-target-rows.js";

const outreachReadyRow = {
  outreachReady: true,
  priorityTier: "A",
  intentScore: 80,
  contact: { hasVerifiedPersonEmail: true, email: "ceo@test.com", name: "CEO" },
};

const needsEmailRow = {
  outreachReady: false,
  priorityTier: "A",
  intentScore: 75,
  ownerTargetId: "rec1",
  ownerName: "Test Owner",
  outreachTrack: "asset_owner",
  contact: {
    name: "",
    email: "",
    linkedIn: "",
    isNonPersonMailbox: false,
    hasVerifiedPersonEmail: false,
    hasVerifiedPersonPhone: false,
    hasVerifiedContact: false,
  },
};

test("deriveContactEnrichmentGaps flags missing channels", () => {
  const gaps = deriveContactEnrichmentGaps(needsEmailRow);
  assert.ok(gaps.includes("missing_contact_channel"));
  assert.ok(gaps.includes("missing_contact_name"));
});

test("deriveEnrichmentPriority ranks outreach ready as P0", () => {
  assert.equal(deriveEnrichmentPriority(outreachReadyRow), "P0_outreach_ready");
  assert.equal(deriveEnrichmentPriority(needsEmailRow), "P1_high_intent_tier_a");
});

test("toEnrichmentQueueItem maps enrichment fields", () => {
  const item = toEnrichmentQueueItem({
    ...needsEmailRow,
    outreachScore: 70,
    primaryDealTrigger: "new_build",
    brandDecisionEligiblePropertyCount: 3,
    topProperties: [{ buildingName: "Hotel X", brandAffiliation: "Independent", city: "Cancun" }],
  });
  assert.equal(item.needsEnrichment, true);
  assert.equal(item.topLeadAsset, "Hotel X");
  assert.ok(item.suggestedAction.length > 10);
});
