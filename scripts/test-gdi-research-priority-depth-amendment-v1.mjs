#!/usr/bin/env node
import assert from "node:assert/strict";
import {
  buildResearchContinuationPacket,
  classifyResearchStopFromExistingSignals,
  RESEARCH_STOP_CLASS,
} from "../lib/group-demand-intelligence/research/gdi-progressive-research-depth-contract-v1.js";
import {
  compareResearchPriorityPolicies,
  inventoryRowToSelectionCandidate,
  scoreProposedResearchPriority,
} from "../lib/group-demand-intelligence/research/gdi-research-priority-evaluation-v1.js";
import { buildResearchEconomicsLedger } from "../lib/group-demand-intelligence/workflow/gdi-research-economics-ledger-v1.js";
import { listFourteenWorkflowSegmentIds } from "../lib/group-demand-intelligence/research/universal-contact-research-contract-v1.js";

const cont = buildResearchContinuationPacket({
  opportunityId: "opp_1",
  unresolvedQuestion: "Is lodging solicitation open?",
  expectedCommercialBenefit: "Confirm overflow pathway",
  proposedNextAction: "Fetch official housing page",
  stoppingCondition: "Stop if housing closed or competitor host confirmed",
});
assert.equal(cont.continuationAuthorized, true);

const incomplete = buildResearchContinuationPacket({
  opportunityId: "opp_2",
  unresolvedQuestion: "Need more?",
});
assert.equal(incomplete.continuationAuthorized, false);

assert.equal(
  classifyResearchStopFromExistingSignals({ tooEarly: true }),
  RESEARCH_STOP_CLASS.TOO_EARLY_MONITOR,
);

const planned = listFourteenWorkflowSegmentIds();
const candidates = planned.map((segmentId, i) =>
  inventoryRowToSelectionCandidate({
    id: `cand_${i}`,
    organizationName: `Org ${i}`,
    title: `Program ${2027 + (i % 3)}`,
    researchSegment: segmentId,
    hotelOpportunityThesis: `${segmentId} local Bethesda demand thesis with housing language`,
    evidence: [{ url: `https://example.org/${segmentId}` }],
    priority: i > 10 ? "HIGH" : "WATCHLIST",
  }),
);

const comparison = compareResearchPriorityPolicies({
  candidates,
  maxCandidates: 8,
  fairMinPerSegment: 1,
  hotelProfile: { city: "Bethesda" },
});
assert.equal(comparison.productionPolicyChanged, false);
assert.equal(comparison.structuralFinding.fairPassCanConsumeEntireCap, true);
assert.equal(
  comparison.policies.currentProductionFairFloorThenScore.selectedCount,
  8,
);

const emerging = scoreProposedResearchPriority({
  organizationName: "Emerging Biotech",
  title: "Bethesda lab expansion 2028",
  hotelOpportunityThesis:
    "Scaling company announced Bethesda expansion; lodging need not yet published.",
  evidence: [{ url: "https://example.org/press" }],
});
assert.ok(emerging.proposedScore > 30);

const economics = buildResearchEconomicsLedger({
  discoveryYieldCounts: { HISTORICAL_REPLAY: 5, LIVE_NEW: 0 },
  retainedCandidates: candidates,
  selectionRegister: {
    selectedCount: 8,
    deferredCount: 6,
    register: comparison.policies.currentProductionFairFloorThenScore.registerSample || [],
  },
  processed: [],
  spentUsd: 0,
});
assert.equal(economics.zeroYieldResearch, true);
assert.equal(economics.newUniqueCandidates, 0);

console.log("test-gdi-research-priority-depth-amendment-v1: PASS");
