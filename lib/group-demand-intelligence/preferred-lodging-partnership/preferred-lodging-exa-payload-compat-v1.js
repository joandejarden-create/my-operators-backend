/**
 * Compatibility adapter for Preferred Lodging Exa Agent create payloads.
 *
 * Builds the dedicated Preferred Lodging Agent mission (prompt + schema).
 * Does NOT wrap Scaling Companies prompt/schema under a PLP heading.
 * Does not call a provider.
 */
import {
  EXA_PREFERRED_LODGING_DISCOVERY_CONTRACT_V1,
  EXA_PREFERRED_LODGING_DEEP_RESEARCH_CONTRACT_V1,
  buildExaPreferredLodgingAgentCreatePayload,
} from "../research/exa-agent/exa-agent-preferred-lodging-contracts-v1.js";
import {
  buildPreferredLodgingDiscoveryPrompt,
  buildPreferredLodgingDeepResearchPrompt,
  EXA_PREFERRED_LODGING_DISCOVERY_V1,
  EXA_PREFERRED_LODGING_DEEP_V1,
} from "./exa-preferred-lodging-contracts-v1.js";

export {
  EXA_PREFERRED_LODGING_DISCOVERY_V1,
  EXA_PREFERRED_LODGING_DEEP_V1,
  EXA_PREFERRED_LODGING_DISCOVERY_CONTRACT_V1,
  EXA_PREFERRED_LODGING_DEEP_RESEARCH_CONTRACT_V1,
};

/**
 * Historical Aimbridge-era entry. Now emits dedicated PLP Agent payloads.
 * `hotel` / `venue` remain accepted for callers that still pass them; the
 * ORS strategy already embeds hotel/venue into `contextBlock`.
 */
export function buildPreferredLodgingExaAgentCreatePayload({
  mode = "discovery",
  contextBlock = "",
  effort = "medium",
  maxCostDollars = null,
  hotel = {},
  venue = {},
  exclusion = null,
  segmentId = "preferred_lodging_partnerships",
  opportunityName = null,
} = {}) {
  // Keep structured prompt objects available for non-Agent callers / audits.
  const structuredPrompt =
    mode === "deep_research"
      ? buildPreferredLodgingDeepResearchPrompt({ hotel, venue })
      : buildPreferredLodgingDiscoveryPrompt({ hotel });

  const venueName =
    opportunityName ||
    venue.venueName ||
    venue.name ||
    venue.title ||
    venue.organizationName ||
    null;

  const pack = buildExaPreferredLodgingAgentCreatePayload({
    mode: mode === "deep_research" ? "deep_research" : "discovery",
    contextBlock,
    effort,
    maxCostDollars,
    exclusion,
    segmentId: segmentId || "preferred_lodging_partnerships",
    opportunityName: venueName,
  });

  return {
    body: pack.body,
    meta: {
      ...pack.meta,
      contractId:
        mode === "deep_research"
          ? EXA_PREFERRED_LODGING_DEEP_V1
          : EXA_PREFERRED_LODGING_DISCOVERY_V1,
      compatibility: "dedicated-preferred-lodging-agent-prompt-v1",
      structuredPromptVersion: structuredPrompt?.version || null,
      scalingPromptUsed: false,
    },
  };
}
