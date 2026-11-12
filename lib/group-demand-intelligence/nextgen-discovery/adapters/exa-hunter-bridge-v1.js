/**
 * Thin Exa bridge for specialist hunters — reuses existing Exa agent adapter.
 * Default: readiness inspect + quote only. Never executes paid calls unless
 * founder-authorized live flags are explicitly set (still blocked by hunter-base).
 */

import {
  quoteExaAgentRun,
  EXA_AGENT_EFFORT,
  isExaAgentLiveAuthorized,
} from "../../research/exa-agent/exa-agent-provider-adapter-v1.js";
import { inspectExaLiveDiscoveryReadiness } from "../../research/exa-agent/exa-live-discovery-provider-bridge-v1.js";

export const EXA_HUNTER_BRIDGE_V1 = "gdi-nextgen-exa-hunter-bridge-v1";

/**
 * Plan Exa gap research without executing.
 */
export function planExaEvidenceGapResearch({
  hunterId,
  discoveries = [],
  budgetUsd = 0,
  effort = EXA_AGENT_EFFORT.MEDIUM,
  authorizePaid = false,
} = {}) {
  const quote = quoteExaAgentRun({ effort, maxCostDollars: budgetUsd || null });
  const readiness = inspectExaLiveDiscoveryReadiness({
    authorizePaidDiscovery: authorizePaid === true,
    authorizeExaAgentLive: authorizePaid === true,
  });
  const liveEnv = isExaAgentLiveAuthorized({
    authorizeExaAgentLive: authorizePaid === true,
  });

  const questions = (discoveries || []).flatMap((d) => {
    const gaps = d.unresolvedEvidenceGaps || d.uncertainties || [];
    if (!gaps.length) return [];
    return [
      {
        hunterId,
        organizationName: d.organizationName,
        activityTitle: d.activityTitle,
        priorityGaps: gaps.slice(0, 3),
        skipIfEvidenceSufficient: Boolean(
          d.hasRealisticHotelThesis && (d.evidenceUrls || []).length
        ),
      },
    ];
  });

  return {
    schema: EXA_HUNTER_BRIDGE_V1,
    provider: "exa",
    execute: false,
    paidAuthorized: false,
    authorizePaidRequested: authorizePaid === true,
    liveEnvAuthorized: liveEnv === true,
    readiness,
    quote,
    plannedQuestions: questions,
    note:
      "Exa bridge plans targeted gap questions only. Paid execution remains fail-closed until founder authorization.",
  };
}
