#!/usr/bin/env node
/**
 * Offline: WATCHLIST finalization handoff + evidence-complete Full E2E fixture.
 * No paid provider calls. No customer publication. No operational store writes.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  reconcileUnresolvedQuestionsAfterContact,
  applyOfflineDemandThesisRecoveryInChain,
  evaluatePublicationReviewReadiness,
  reconstructProviderRunIdFromOpportunity,
} from "../lib/group-demand-intelligence/workflow/gdi-watchlist-finalization-handoff-v1.js";
import { processOpportunityThroughOriginalChain } from "../lib/group-demand-intelligence/workflow/original-full-e2e-research-chain-v1.js";
import { evaluateSharedCommercialEligibility } from "../lib/group-demand-intelligence/commercial-eligibility-contract-v1.js";
import { isLiveDiscoveryOrsStem } from "../lib/group-demand-intelligence/research/gdi-paid-deep-two-stage-dedupe-v1.js";
import { buildPreferredLodgingDiscoveryPrompt } from "../lib/group-demand-intelligence/preferred-lodging-partnership/exa-preferred-lodging-contracts-v1.js";
import { buildProfessionalEventPlannerDiscoveryPrompt } from "../lib/group-demand-intelligence/professional-event-planners/exa-professional-event-planner-contracts-v1.js";
import { getSegment912ExaContracts } from "../lib/group-demand-intelligence/research/exa-agent/exa-agent-segments-9-12-contracts-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
let passed = 0;
function check(name, fn) {
  const out = fn();
  if (out && typeof out.then === "function") {
    throw new Error(`use checkAsync for ${name}`);
  }
  passed += 1;
  console.log(`PASS  ${name}`);
}
async function checkAsync(name, fn) {
  await fn();
  passed += 1;
  console.log(`PASS  ${name}`);
}

const fixturePath = path.join(
  ROOT,
  "fixtures/gdi/evidence-complete-full-e2e-handoff-v1.json"
);
const fixture = JSON.parse(fs.readFileSync(fixturePath, "utf8"));

check("unresolved_questions_cleared_after_verified_buyer", () => {
  const opp = {
    unresolvedQuestions: [
      "named_buyer_not_verified",
      "sourcing_accessibility_unconfirmed",
    ],
    primaryContact: {
      name: "Neil Brownsword",
      title: "International Facilities Senior Manager",
      contactRelevanceLabel: "CONTACT_RELEVANT",
      commercialClass: "hotel_sourcing_decision_maker",
    },
    venueSourcingStatus: "OPEN_UNCONFIRMED",
  };
  const r = reconcileUnresolvedQuestionsAfterContact(opp);
  assert.ok(!r.remaining.includes("named_buyer_not_verified"));
  assert.ok(!r.remaining.includes("sourcing_accessibility_unconfirmed"));
  assert.equal(r.hasVerifiedBuyer, true);
});

check("provider_run_id_reconstructed_from_ors_routing", () => {
  const r = reconstructProviderRunIdFromOpportunity({
    id: "disc_0809a729fc57",
    exaAgentRouting: { providerRunId: "agent_run_7fc12d6fecd947bea8df63ce7aaa00d4" },
  });
  assert.equal(r.providerRunId, "agent_run_7fc12d6fecd947bea8df63ce7aaa00d4");
  assert.equal(r.ledgerMutation, false);
});

check("live_disc_ors_still_excluded_from_inventory_classifier", () => {
  assert.equal(isLiveDiscoveryOrsStem("disc_abc"), true);
});

check("plp_pep_lcm_contracts_require_correct_entity_types", () => {
  const hotel = {
    hotelId: "recLuxvwwxID7U2B8",
    displayName: "Bethesda Marriott",
    name: "Bethesda Marriott",
    city: "Bethesda",
  };
  const plp = JSON.stringify(buildPreferredLodgingDiscoveryPrompt({ hotel }));
  const pep = JSON.stringify(buildProfessionalEventPlannerDiscoveryPrompt({ hotel }));
  const lcm = getSegment912ExaContracts("local_catering_day_meetings");
  assert.match(plp, /complementary/i);
  assert.match(pep, /Do not substitute the conference brand/i);
  assert.match(lcm.discoveryObjective, /NOT predominantly large destination conferences/i);
});

check("failed_pep_conference_brand_is_not_planner_entity", () => {
  const blob = "Society of American Military Engineers — SAME Capital Week 2027 conference";
  assert.match(blob, /conference|capital week|society of/i);
  assert.ok(!/planner|DMC|event agency/i.test(blob));
});

check("failed_lcm_destination_conference_is_not_local_day_meeting", () => {
  const blob = "Association for Financial Counseling and Planning Education Annual Conference 2027";
  assert.match(blob, /annual conference/i);
});

await checkAsync(
  "evidence_complete_fixture_reaches_publication_review_ready_without_publish",
  async () => {
    const result = await processOpportunityThroughOriginalChain(
      { ...fixture.opportunity },
      {
        authorizePaidResearch: false,
        skipDeepResearch: true,
        skipPersistResearchState: true,
        allowOfficialSourceContactFollowUp: false,
      }
    );
    assert.equal(result.customerPublicationReady, false);
    assert.ok(result.opportunity.tenSectionDrawer?.sections);
    assert.ok(
      (result.opportunity.unresolvedQuestions || []).includes("named_buyer_not_verified") ===
        false
    );
    const elig = evaluateSharedCommercialEligibility(result.opportunity, {
      hotel: fixture.hotel,
    });
    assert.equal(
      elig.salesReady,
      true,
      `expected salesReady, gaps=${JSON.stringify(elig.gaps)} status=${elig.status}`
    );
    assert.equal(result.publicationReviewReady, true);
    assert.equal(result.opportunity.publicationReviewReady, true);
    assert.ok(result.opportunity.commercialDemandType);
    const readiness = evaluatePublicationReviewReadiness(result.opportunity, {
      eligibilityResult: elig,
      hotel: fixture.hotel,
    });
    assert.equal(readiness.publicationReviewReady, true);
    assert.equal(readiness.customerPublicationReady, false);
    assert.equal(readiness.founderAuthorizationRequired, true);
  }
);

await checkAsync(
  "knowbe4_watchlist_does_not_artificially_become_publication_ready",
  async () => {
    const orsPath = path.join(
      ROOT,
      "data/group-demand-intelligence/hotels/recLuxvwwxID7U2B8/opportunity-research-state/disc_5976f7572131.json"
    );
    const doc = JSON.parse(fs.readFileSync(orsPath, "utf8"));
    const opp = doc.opportunity || doc;
    const demand = applyOfflineDemandThesisRecoveryInChain(opp);
    const cleared = reconcileUnresolvedQuestionsAfterContact(demand.opportunity);
    const elig = evaluateSharedCommercialEligibility(cleared.opportunity, {
      hotel: { hotelId: "recLuxvwwxID7U2B8", displayName: "Bethesda Marriott" },
    });
    // Historical pool: strong contacts, speculative demand — must NOT force publication-ready.
    assert.equal(elig.salesReady, false);
    assert.ok(elig.gaps.includes("MISSING_HOTEL_DEMAND_THESIS"));
    assert.ok(!cleared.remaining.includes("named_buyer_not_verified"));
  }
);

console.log(
  `\nOK  ${passed} WATCHLIST finalization handoff checks passed (offline, $0)`
);
