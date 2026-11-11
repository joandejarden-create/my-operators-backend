/**
 * Compatibility adapter for the historical Aimbridge preferred-lodging payload
 * builder. Current mainline contracts expose prompts, not create-payload helpers.
 * This does not call a provider.
 */
import { buildExaAgentCreatePayload } from "../research/exa-agent/exa-agent-provider-adapter-v1.js";
import {
  buildPreferredLodgingDiscoveryPrompt,
  buildPreferredLodgingDeepResearchPrompt,
  EXA_PREFERRED_LODGING_DISCOVERY_V1,
  EXA_PREFERRED_LODGING_DEEP_V1,
} from "./exa-preferred-lodging-contracts-v1.js";

export {
  EXA_PREFERRED_LODGING_DISCOVERY_V1,
  EXA_PREFERRED_LODGING_DEEP_V1,
};

export function buildPreferredLodgingExaAgentCreatePayload({
  mode = "discovery",
  contextBlock = "",
  effort = "medium",
  maxCostDollars = null,
  hotel = {},
  venue = {},
} = {}) {
  const prompt =
    mode === "deep_research"
      ? buildPreferredLodgingDeepResearchPrompt({ hotel, venue })
      : buildPreferredLodgingDiscoveryPrompt({ hotel });
  const body = buildExaAgentCreatePayload({
    mode: mode === "deep_research" ? "deep_research" : "discovery",
    contextBlock: [contextBlock, JSON.stringify(prompt)].filter(Boolean).join("\n"),
    effort,
    maxCostDollars,
  });
  return {
    body,
    meta: {
      contractId:
        mode === "deep_research"
          ? EXA_PREFERRED_LODGING_DEEP_V1
          : EXA_PREFERRED_LODGING_DISCOVERY_V1,
      mode,
      effort,
      compatibility: "mainline-preferred-lodging-prompt-v1",
    },
  };
}
