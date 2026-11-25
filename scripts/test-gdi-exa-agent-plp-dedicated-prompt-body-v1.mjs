#!/usr/bin/env node
/**
 * $0 — Preferred Lodging Agent must use dedicated PLP prompt + schema,
 * not Scaling Companies mission under a PLP H1.
 */
import assert from "node:assert/strict";
import { buildPreferredLodgingExaAgentCreatePayload } from "../lib/group-demand-intelligence/preferred-lodging-partnership/preferred-lodging-exa-payload-compat-v1.js";
import {
  EXA_PREFERRED_LODGING_DISCOVERY_CONTRACT_V1,
  EXA_PREFERRED_LODGING_DEEP_RESEARCH_CONTRACT_V1,
  buildExaPreferredLodgingAgentCreatePayload,
  redactExaPreferredLodgingPayloadForReport,
} from "../lib/group-demand-intelligence/research/exa-agent/exa-agent-preferred-lodging-contracts-v1.js";
import { extractExaQueryHeading } from "../lib/group-demand-intelligence/research/exa-agent/gdi-exa-segment-request-titles-v1.js";
import {
  buildPreferredLodgingDeepResearchContextBlock,
  executeExaPreferredLodgingOrsStrategy,
  EXA_PREFERRED_LODGING_ORS_STRATEGY_V1,
} from "../lib/group-demand-intelligence/research/exa-agent/exa-agent-preferred-lodging-ors-strategy-v1.js";
import { EXA_PREFERRED_LODGING_RESEARCH_STRATEGIES } from "../lib/group-demand-intelligence/research/exa-agent/gdi-segment-aware-exa-agent-routing-v1.js";
import { EXA_AGENT_DEEP_RESEARCH_CONTRACT_V1 } from "../lib/group-demand-intelligence/research/exa-agent/exa-agent-contracts-v1.js";

const results = [];

function check(name, fn) {
  try {
    fn();
    results.push({ name, ok: true });
  } catch (err) {
    results.push({ name, ok: false, error: String(err?.message || err) });
  }
}

async function checkAsync(name, fn) {
  try {
    await fn();
    results.push({ name, ok: true });
  } catch (err) {
    results.push({ name, ok: false, error: String(err?.message || err) });
  }
}

check("deep_payload_uses_plp_contract_not_scaling", () => {
  const pack = buildPreferredLodgingExaAgentCreatePayload({
    mode: "deep_research",
    contextBlock: "Focus venue: The Lane\nHotel ID: recTest",
    hotel: { displayName: "Wyndham San Diego Bayside" },
    venue: { name: "The Lane" },
    segmentId: "preferred_lodging_partnerships",
    opportunityName: "The Lane",
  });
  const q = String(pack.body.query || "");
  assert.equal(pack.meta.contractId, EXA_PREFERRED_LODGING_DEEP_RESEARCH_CONTRACT_V1);
  assert.equal(pack.meta.scalingPromptUsed, false);
  assert.match(q, /gdi-exa-preferred-lodging-partnership-deep-research-v1/);
  assert.doesNotMatch(q, /gdi-scaling-exa-agent-/i);
  assert.notEqual(pack.body.outputSchema?.$id, "gdi-scaling-exa-agent-deep-research-structured-output-v1");
  assert.equal(
    pack.body.outputSchema?.$id,
    "gdi-exa-preferred-lodging-partnership-deep-research-structured-output-v1",
  );
  assert.equal(
    pack.body.outputSchema?.properties?.contractId?.const,
    EXA_PREFERRED_LODGING_DEEP_RESEARCH_CONTRACT_V1,
  );
  assert.equal(
    extractExaQueryHeading(q),
    "# PREFERRED LODGING / PARTNERSHIPS | DEEP RESEARCH | The Lane",
  );
  // applyExaSegmentRequestTitle rewrites the first H1; mission body must remain PLP.
  assert.match(q, /Agent Stage B — Adaptive deep investigation of a selected Preferred Lodging venue/);
  assert.match(q, /referral|overflow|preferred-hotel/i);
  assert.match(q, /Venue owner|Wedding director|Event sales director/);
  assert.match(q, /Do \*\*not\*\* use Scaling Companies growth/);
  assert.notEqual(EXA_AGENT_DEEP_RESEARCH_CONTRACT_V1, pack.meta.contractId);
});

check("discovery_payload_uses_plp_contract_not_scaling", () => {
  const pack = buildPreferredLodgingExaAgentCreatePayload({
    mode: "discovery",
    contextBlock: "Hotel ID: recTest\nCandidate limit: 12",
    hotel: { displayName: "Wyndham San Diego Bayside" },
    segmentId: "preferred_lodging_partnerships",
  });
  const q = String(pack.body.query || "");
  assert.equal(pack.meta.contractId, EXA_PREFERRED_LODGING_DISCOVERY_CONTRACT_V1);
  assert.match(q, /gdi-exa-preferred-lodging-partnership-discovery-v1/);
  assert.doesNotMatch(q, /gdi-scaling-exa-agent-/i);
  assert.equal(
    pack.body.outputSchema?.$id,
    "gdi-exa-preferred-lodging-partnership-discovery-structured-output-v1",
  );
  assert.match(q, /weddings|banquets|referral/i);
  assert.match(q, /Venue owner|Wedding director/);
  const redacted = redactExaPreferredLodgingPayloadForReport(pack);
  assert.equal(redacted.body.contractIdInQuery, true);
  assert.equal(redacted.body.scalingContractAbsent, true);
  assert.equal(redacted.body.venueReferralMissionPresent, true);
  assert.equal(redacted.body.stakeholderRolesPresent, true);
});

check("direct_contracts_builder_matches_compat", () => {
  const direct = buildExaPreferredLodgingAgentCreatePayload({
    mode: "deep_research",
    contextBlock: "venue",
    opportunityName: "Rock Creek Mansion",
  });
  const compat = buildPreferredLodgingExaAgentCreatePayload({
    mode: "deep_research",
    contextBlock: "venue",
    venue: { name: "Rock Creek Mansion" },
    opportunityName: "Rock Creek Mansion",
  });
  assert.equal(direct.meta.contractId, compat.meta.contractId);
  assert.equal(direct.body.outputSchema.$id, compat.body.outputSchema.$id);
  assert.equal(
    extractExaQueryHeading(direct.body.query),
    extractExaQueryHeading(compat.body.query),
  );
});

check("deep_context_block_preserves_plp_family_directive", () => {
  const block = buildPreferredLodgingDeepResearchContextBlock({
    hotel: { hotelId: "recX", name: "Test Hotel" },
    opportunity: { venueName: "The Cordell", id: "opp1" },
  });
  assert.match(block, /Preferred Lodging Partnerships/);
  assert.match(block, /Do NOT use Scaling Companies/);
  assert.match(block, /Venue owner|Wedding director|Event sales/i);
});

await checkAsync("ors_executor_outbound_payload_is_dedicated_plp", async () => {
  const mockStructured = {
    contractId: EXA_PREFERRED_LODGING_DEEP_RESEARCH_CONTRACT_V1,
    venueIdentity: "The Lane",
    findings: {
      onsiteLodgingStatus: "UNKNOWN",
      hotelSpecificFit: "Complementary overflow watch",
      recommendedSalesAction: "Introduce lodging partnership conversation",
    },
    remainingUncertainties: ["Confirm preferred-hotel language"],
    supportingSources: [
      { claim: "Venue hosts weddings", url: "https://example.com/lane" },
    ],
    venueContactCandidates: [],
    researchDisposition: "CONTINUE_RESEARCH",
  };

  const result = await executeExaPreferredLodgingOrsStrategy({
    strategyId:
      EXA_PREFERRED_LODGING_RESEARCH_STRATEGIES.EXA_AGENT_PREFERRED_LODGING_DEEP_RESEARCH,
    hotel: {
      hotelId: "recTestHotel",
      displayName: "Wyndham San Diego Bayside",
      address: "1355 N Harbor Dr, San Diego, CA",
    },
    opportunity: {
      id: "opp_lane",
      venueName: "The Lane",
      title: "The Lane",
      researchSegment: "preferred_lodging_partnerships",
    },
    segmentId: "preferred_lodging_partnerships",
    opportunityName: "The Lane",
    forceMockOnly: true,
    mockStructured,
    authorizeExaAgentLive: false,
  });

  assert.equal(result.version, EXA_PREFERRED_LODGING_ORS_STRATEGY_V1);
  assert.equal(
    result.requestPayloadMeta.contractId,
    EXA_PREFERRED_LODGING_DEEP_RESEARCH_CONTRACT_V1,
  );
  assert.equal(result.requestPayloadMeta.agentFamily, "PREFERRED_LODGING");
  assert.equal(result.run?.ok, true);
  assert.equal(result.run?.contractId, EXA_PREFERRED_LODGING_DEEP_RESEARCH_CONTRACT_V1);
  // Reconstruct what was submitted via createPayloadOverride path
  const preview = buildPreferredLodgingExaAgentCreatePayload({
    mode: "deep_research",
    contextBlock: buildPreferredLodgingDeepResearchContextBlock({
      hotel: {
        hotelId: "recTestHotel",
        displayName: "Wyndham San Diego Bayside",
      },
      opportunity: { venueName: "The Lane", id: "opp_lane" },
      segmentId: "preferred_lodging_partnerships",
    }),
    segmentId: "preferred_lodging_partnerships",
    opportunityName: "The Lane",
  });
  const q = preview.body.query;
  assert.match(q, /gdi-exa-preferred-lodging-partnership-deep-research-v1/);
  assert.doesNotMatch(q, /gdi-scaling-exa-agent-deep-research-v1/);
  assert.equal(
    preview.body.outputSchema.required.includes("venueIdentity"),
    true,
  );
  assert.equal(
    preview.body.outputSchema.required.includes("companyIdentity"),
    false,
  );
  assert.ok(result.validation?.ok);
});

const failed = results.filter((r) => !r.ok);
console.log(JSON.stringify({ ok: failed.length === 0, results }, null, 2));
if (failed.length) process.exit(1);
