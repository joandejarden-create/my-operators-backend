/**
 * ORS strategy selection for Exa Agent — available capability, not a new supervisor.
 *
 * Prefer cheaper Exa Search / Contents / direct_public when they can answer.
 * Use Agent only when gap value + authorization + budget justify multi-step research.
 */
import {
  runExaAgentResearch,
  quoteExaAgentRun,
  EXA_AGENT_EFFORT,
  isExaAgentLiveAuthorized,
} from "./exa-agent-provider-adapter-v1.js";
import {
  validateDiscoveryStructured,
  validateDeepResearchStructured,
} from "./exa-agent-evidence-validation-v1.js";
import { applyValidatedDeepResearchToScalingBrief } from "./exa-agent-to-cib-mapping-v1.js";
import {
  buildAuthoritativeHotelResearchContext,
  formatHotelResearchContextBlock,
} from "../authoritative-hotel-research-context-v1.js";
import { buildUniversalContactResearchPromptBlock } from "../universal-contact-research-contract-v1.js";
import {
  firstSafeDisplayText,
  resolveCompanyIdentityName,
} from "../safe-display-text-v1.js";

export const EXA_AGENT_ORS_STRATEGY_V1 = "gdi-scaling-exa-agent-ors-strategy-v1";

export const EXA_AGENT_RESEARCH_STRATEGIES = Object.freeze({
  EXA_AGENT_SCALING_DISCOVERY: "EXA_AGENT_SCALING_DISCOVERY",
  EXA_AGENT_SCALING_DEEP_RESEARCH: "EXA_AGENT_SCALING_DEEP_RESEARCH",
});

/**
 * Decide whether Exa Agent is appropriate vs cheaper providers.
 */
export function selectExaAgentStrategy({
  gap = {},
  opportunity = {},
  authorizeExaAgentLive = false,
  remainingBudgetUsd = 0,
  priorExaSearchAttempts = 0,
  priorAgentAttempts = 0,
} = {}) {
  const section = String(gap.drawerSection || gap.sectionId || "");
  const question = String(gap.commercialQuestion || gap.question || "");
  const complex =
    /named buyer|chronology|mexico|office|travel owner|sourcing|competitive|multi-hop|deep/i.test(
      `${section} ${question}`,
    );

  if (priorAgentAttempts >= 2) {
    return {
      useExaAgent: false,
      reason: "agent_attempt_cap",
      prefer: "ORS_CONTINUATION_OR_STOP",
    };
  }
  if (!authorizeExaAgentLive || !isExaAgentLiveAuthorized({ authorizeExaAgentLive })) {
    return {
      useExaAgent: false,
      reason: "live_not_authorized",
      prefer: priorExaSearchAttempts < 2 ? "EXA_SEARCH" : "DIRECT_PUBLIC_OR_CONTENTS",
      strategyId: null,
    };
  }
  if (Number(remainingBudgetUsd) < 0.05) {
    return {
      useExaAgent: false,
      reason: "insufficient_budget",
      prefer: "STOP_OR_DIRECT_PUBLIC",
    };
  }
  if (!complex && priorExaSearchAttempts < 1) {
    return {
      useExaAgent: false,
      reason: "try_cheaper_search_first",
      prefer: "EXA_SEARCH",
    };
  }

  const mode = /discover|new company|candidate/i.test(`${section} ${question}`)
    ? "discovery"
    : "deep_research";
  return {
    useExaAgent: true,
    reason: "complex_gap_authorized",
    mode,
    strategyId:
      mode === "discovery"
        ? EXA_AGENT_RESEARCH_STRATEGIES.EXA_AGENT_SCALING_DISCOVERY
        : EXA_AGENT_RESEARCH_STRATEGIES.EXA_AGENT_SCALING_DEEP_RESEARCH,
    suggestedEffort:
      Number(remainingBudgetUsd) >= 1
        ? EXA_AGENT_EFFORT.AUTO
        : EXA_AGENT_EFFORT.MEDIUM,
    maxCostDollars:
      Number(remainingBudgetUsd) >= 1
        ? Math.min(5, Number(remainingBudgetUsd))
        : null,
  };
}

export function buildDiscoveryContextBlock({
  hotel = {},
  exclusions = [],
  limit = 8,
  additionalQueryDirectives = [],
} = {}) {
  const ctx = buildAuthoritativeHotelResearchContext(hotel, {
    contextAddress: hotel.address || null,
  });
  const extra = (Array.isArray(additionalQueryDirectives)
    ? additionalQueryDirectives
    : []
  )
    .map((d) => String(d || "").replace(/\s+/g, " ").trim())
    .filter(Boolean);
  return [
    formatHotelResearchContextBlock(ctx),
    `Candidate limit: ${limit} (prefer fewer strong hospitality-demand candidates over weak growth stories).`,
    `Exclude / avoid duplicates: ${JSON.stringify(exclusions).slice(0, 4000)}`,
    `Segment: Scaling Companies corporate accounts only (not Association Events).`,
    `Discovery objective: identify companies whose verified growth/expansion/operating activity creates a credible potential need for guestrooms, meetings, training, or local catering near this hotel.`,
    `Funding size, valuation, headcount, and media fame are supporting signals only — not primary ranking criteria.`,
    `For each candidate investigate the hospitality-demand mechanism (who travels, why, where, when) and set gate1OperationalActivity + gate2HospitalityDemand honestly.`,
    `Do not ACCEPT_FOR_DEEP_RESEARCH solely because the company operates in the same city.`,
    ...extra,
  ].join("\n");
}

export function buildDeepResearchContextBlock({
  hotel = {},
  opportunity = {},
  sectionGaps = [],
} = {}) {
  const ctx = buildAuthoritativeHotelResearchContext(
    { ...hotel, hotelId: hotel.hotelId || opportunity.hotelId },
    { contextAddress: hotel.address || null },
  );
  const targetCompany = firstSafeDisplayText(
    opportunity.organizationName,
    opportunity.companyName,
    resolveCompanyIdentityName(opportunity),
    opportunity.displayTitle,
    opportunity.title,
    opportunity.programmeName,
  );
  const activitySummary = firstSafeDisplayText(
    opportunity.whatIsHappening,
    opportunity.activityHeadline,
    opportunity.verifiedBusinessActivity,
    opportunity.demandMechanismSummary,
  );
  return [
    formatHotelResearchContextBlock(ctx),
    `Opportunity ID: ${opportunity.id || opportunity.opportunityId || ""}`,
    `Target company: ${targetCompany}`,
    `Existing activity summary: ${activitySummary.slice(0, 800)}`,
    `Section gaps: ${JSON.stringify(sectionGaps).slice(0, 3000)}`,
    `Segment: Scaling Companies — corporate travel / visiting staff / meetings; no Association PCO framing.`,
    `Contact standard: minimum one CONTACT_RELEVANT named person; preferred two distinct named contacts. Do not fabricate a second contact.`,
    buildUniversalContactResearchPromptBlock({
      segmentId: "corporate_accounts_scaling",
      segmentLabel: "Scaling Companies",
    }),
  ].join("\n");
}

/**
 * Execute strategy with mocks or live (gated). Returns ORS-compatible result bag.
 */
export async function executeExaAgentOrsStrategy({
  strategyId,
  hotel = {},
  opportunity = {},
  exclusions = [],
  sectionGaps = [],
  authorizeExaAgentLive = false,
  mockStructured = null,
  mockStatus = "completed",
  effort = EXA_AGENT_EFFORT.MEDIUM,
  maxCostDollars = null,
  additionalQueryDirectives = [],
} = {}) {
  const mode =
    strategyId === EXA_AGENT_RESEARCH_STRATEGIES.EXA_AGENT_SCALING_DISCOVERY
      ? "discovery"
      : "deep_research";

  const contextBlock =
    mode === "discovery"
      ? buildDiscoveryContextBlock({
          hotel,
          exclusions,
          additionalQueryDirectives,
        })
      : buildDeepResearchContextBlock({ hotel, opportunity, sectionGaps });

  const quote = quoteExaAgentRun({ effort, maxCostDollars });
  // High-effort deep research routinely exceeds the adapter default (~4 min).
  // Hotel-generic: allow up to ~15 minutes before declaring not_completed.
  const highDeep = mode === "deep_research" && /high/i.test(String(effort || ""));
  const run = await runExaAgentResearch({
    mode,
    contextBlock,
    effort,
    maxCostDollars,
    exclusion: exclusions,
    authorizeExaAgentLive,
    mockStructured,
    mockStatus,
    pollUntilFinished: true,
    pollIntervalMs: highDeep ? 5000 : 4000,
    maxPolls: highDeep ? 180 : 90,
  });

  if (!run.ok) {
    return {
      version: EXA_AGENT_ORS_STRATEGY_V1,
      strategyId,
      mode,
      quote,
      run,
      validation: null,
      opportunity: opportunity || null,
      continueResearch: true,
    };
  }

  if (mode === "discovery") {
    const validation = validateDiscoveryStructured(run.structured);
    return {
      version: EXA_AGENT_ORS_STRATEGY_V1,
      strategyId,
      mode,
      quote,
      run,
      validation,
      opportunity: null,
      continueResearch: true,
      discoveryCandidates: validation.acceptedCandidates || [],
    };
  }

  const validation = validateDeepResearchStructured(run.structured);
  if (!validation.ok) {
    return {
      version: EXA_AGENT_ORS_STRATEGY_V1,
      strategyId,
      mode,
      quote,
      run,
      validation,
      opportunity,
      continueResearch: true,
    };
  }

  const applied = applyValidatedDeepResearchToScalingBrief(
    opportunity,
    validation.sanitized,
    hotel,
  );
  return {
    version: EXA_AGENT_ORS_STRATEGY_V1,
    strategyId,
    mode,
    quote,
    run,
    validation,
    opportunity: applied.opportunity,
    assessment: applied.assessment,
    continueResearch:
      validation.sanitized.researchDisposition ===
        "RESEARCH_CONTINUATION_REQUIRED" ||
      validation.sanitized.researchDisposition === "WATCHLIST_OR_MONITOR",
  };
}
