#!/usr/bin/env node
/**
 * Smoke tests for GDI full semantic commercial audit module (no paid LLM).
 */
import assert from "node:assert/strict";
import {
  GDI_FULL_SEMANTIC_COMMERCIAL_AUDIT_V1,
  buildSemanticAuditInventory,
  renderCustomerFacingPack,
  detectCrossSectionSemanticIssues,
  computeScorecard,
  SEMANTIC_SECTION_IDS,
} from "../lib/group-demand-intelligence/customer-consistency/gdi-full-semantic-commercial-audit-v1.js";
import { sanitizeDuplicatedActionNarrative } from "../lib/group-demand-intelligence/commercial-consistency/commercial-presentation-sanitizer-v1.js";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";

assert.equal(
  GDI_FULL_SEMANTIC_COMMERCIAL_AUDIT_V1,
  "gdi-full-semantic-commercial-audit-v1"
);
assert.ok(SEMANTIC_SECTION_IDS.includes("what_is_happening"));
assert.ok(SEMANTIC_SECTION_IDS.includes("hotel_validation"));

const inventory = await buildSemanticAuditInventory();
assert.ok(inventory.totals.opportunities >= 600, "expected large operational inventory");
assert.equal(inventory.totals.hotelsWithInventory, 17);
assert.ok(
  (inventory.excludedParityHotels || []).length >= 1,
  "selectable hotels without stores should be documented"
);
console.log("PASS inventory_reconciles_17_hotels_and_exclusions");

const hotelId = "recLuxvwwxID7U2B8";
const doc = await loadOpportunitiesCanonical(hotelId);
const raw = (doc.opportunities || []).find((o) =>
  String(o.id || "").includes("nado_ddaa_washcon_2027")
);
assert.ok(raw, "NADO fixture opportunity present");
const scrubbed = sanitizeDuplicatedActionNarrative({
  ...raw,
  hotelOpportunityThesis:
    "NADO Arlington Washington Conference 2027 (overflow) is dated in Host hotel contracted — Crystal Gateway Marriott with hotel sourcing unconfirmed — a plausible group-lodging conversation.",
});
assert.ok(
  !/dated in Host hotel contracted/i.test(
    scrubbed.hotelOpportunityThesis || ""
  ),
  "thesis field Host-hotel-contracted glue must sanitize"
);
console.log("PASS host_contracted_glue_sanitizer_covers_thesis_fields");

const pack = renderCustomerFacingPack(raw, hotelId, "Bethesda Marriott");
assert.ok(pack.opportunityId);
assert.ok(pack.displayTitle);
assert.ok((pack.populatedSections || []).length >= 8);
assert.ok(!/dated in Host hotel contracted/i.test(pack.fullDrawerText || ""));
console.log("PASS render_customer_facing_pack_nado_clean");

const falseHistorical = detectCrossSectionSemanticIssues({
  sections: {
    what_is_happening: "Conference planned for 2027",
    why_act_now: "Historical attendance (~400) supports fit",
    recommended_next_step: "Pursue Open Business",
    card_sales_action: "Pursue Open Business",
  },
  card: { title: "X" },
});
assert.ok(
  !falseHistorical.some((f) => f.code === "SEM_HISTORICAL_VS_PURSUE_NOW"),
  "historical attendance must not trip historical-vs-pursue heuristic"
);
console.log("PASS historical_attendance_false_positive_guard");

const incomplete = computeScorecard({
  packs: [pack, pack],
  reviews: [
    {
      hotelId,
      opportunityId: pack.opportunityId,
      reviewStatus: "HEURISTIC_ASSIST_ONLY",
      scores: {},
    },
  ],
  researchRequiredCount: 1,
  technicalConsistencyScore: 100,
});
assert.equal(incomplete.overall, null);
assert.match(incomplete.overallStatus, /INCOMPLETE/);
console.log("PASS overall_withheld_until_semantic_complete");

console.log("\nAll full semantic commercial audit smoke checks passed");
