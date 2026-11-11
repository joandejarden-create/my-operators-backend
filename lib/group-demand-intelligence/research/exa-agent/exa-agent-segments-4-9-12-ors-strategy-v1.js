/**
 * Exa Agent ORS strategy for Segments 4 / 9 / 12 — live-ready payload builders.
 * Dispatch only via shared exa-agent-provider-adapter-v1 (createPayloadOverride).
 * Live HTTP requires authorizeExaAgentLive + GDI_EXA_AGENT_LIVE=1.
 */
import {
  buildAuthoritativeHotelResearchContext,
  formatHotelResearchContextBlock,
} from "../authoritative-hotel-research-context-v1.js";
import {
  getSegment912ExaContracts,
  SEGMENTS_9_12_CONTRACTS_BY_ID,
} from "./exa-agent-segments-9-12-contracts-v1.js";
import { buildUniversalContactResearchPromptBlock } from "../universal-contact-research-contract-v1.js";

export const EXA_SEGMENTS_4_9_12_ORS_STRATEGY_V1 =
  "gdi-exa-segments-4-9-12-ors-strategy-v1";

function segmentContract({
  discoveryContractId,
  deepResearchContractId,
  segmentLabel,
  discoveryObjective,
  deepResearchObjective,
  preferredRevenueTypes,
  guestroomRequired = false,
}) {
  return Object.freeze({
    version: EXA_SEGMENTS_4_9_12_ORS_STRATEGY_V1,
    discoveryContractId,
    deepResearchContractId,
    segmentLabel,
    discoveryObjective,
    deepResearchObjective,
    preferredRevenueTypes: Object.freeze([...preferredRevenueTypes]),
    guestroomRequired,
  });
}

export const SEGMENT_3_MEETINGS_INCENTIVES_CONTRACTS = segmentContract({
  discoveryContractId: "gdi-exa-meetings-incentives-discovery-v1",
  deepResearchContractId: "gdi-exa-meetings-incentives-deep-research-v1",
  segmentLabel: "Corporate Meetings & Incentive Travel",
  discoveryObjective:
    "Discover verified corporate meetings, training, offsites, or incentive travel programmes that could create realistic hotel lodging and/or meeting demand near this hotel's market — without inventing RFPs or committed bookings.",
  deepResearchObjective:
    "Verify programme identity, destination/dates, meeting/lodging needs, buyer roles, named commercially relevant contact, and a legitimate commercial approach.",
  preferredRevenueTypes: [
    "guestrooms",
    "group_guestrooms",
    "meeting_space_rental",
    "local_catering",
  ],
});

export const SEGMENT_4_GOVERNMENT_CONTRACTS = segmentContract({
  discoveryContractId: "gdi-exa-government-discovery-v1",
  deepResearchContractId: "gdi-exa-government-deep-research-v1",
  segmentLabel: "Government & Public Sector",
  discoveryObjective:
    "Discover verified government or public-sector programmes that could create realistic hotel lodging, meeting, or training demand — without assuming private-hotel procurement access.",
  deepResearchObjective:
    "Verify programme identity, destination/dates, meeting/lodging needs, procurement constraints, venue sourcing, named programme/procurement contact, and a legitimate commercial approach.",
  preferredRevenueTypes: [
    "guestrooms",
    "group_guestrooms",
    "meeting_space_rental",
    "local_catering",
  ],
});

export const SEGMENT_5_SPORTS_ENTERTAINMENT_CONTRACTS = segmentContract({
  discoveryContractId: "gdi-exa-sports-entertainment-discovery-v1",
  deepResearchContractId: "gdi-exa-sports-entertainment-deep-research-v1",
  segmentLabel: "Sports & Entertainment",
  discoveryObjective:
    "Discover verified sports teams, tournaments, entertainment productions, or touring programmes that could create realistic hotel lodging demand near this hotel — without inventing room blocks.",
  deepResearchObjective:
    "Verify event/programme identity, dates, lodging needs, production/travel buyer roles, named contact, and a legitimate commercial approach.",
  preferredRevenueTypes: ["guestrooms", "group_guestrooms", "banquets"],
});

export const SEGMENT_6_EDUCATION_SCIENTIFIC_CONTRACTS = segmentContract({
  discoveryContractId: "gdi-exa-education-scientific-discovery-v1",
  deepResearchContractId: "gdi-exa-education-scientific-deep-research-v1",
  segmentLabel: "Education, Academic & Scientific Groups",
  discoveryObjective:
    "Discover verified academic, scientific, medical-education, or university programmes that could create realistic hotel lodging and/or meeting demand near this hotel.",
  deepResearchObjective:
    "Verify programme identity, destination/dates, lodging/meeting needs, named organiser contact, and a legitimate commercial approach.",
  preferredRevenueTypes: [
    "guestrooms",
    "group_guestrooms",
    "meeting_space_rental",
    "local_catering",
  ],
});

export const SEGMENT_7_PROJECTS_INFRASTRUCTURE_CONTRACTS = segmentContract({
  discoveryContractId: "gdi-exa-projects-infrastructure-discovery-v1",
  deepResearchContractId: "gdi-exa-projects-infrastructure-deep-research-v1",
  segmentLabel: "Projects, Construction & Infrastructure",
  discoveryObjective:
    "Discover verified construction, infrastructure, or project-deployment activity that may require temporary lodging near this hotel — not general hiring announcements.",
  deepResearchObjective:
    "Verify project identity, duration, lodging needs, mobility/project contact, and distinguish temporary lodging from permanent housing.",
  preferredRevenueTypes: ["extended_stay", "guestrooms", "group_guestrooms"],
});

export const SEGMENT_8_SOCIAL_SPECIAL_EVENTS_CONTRACTS = segmentContract({
  discoveryContractId: "gdi-exa-social-special-events-discovery-v1",
  deepResearchContractId: "gdi-exa-social-special-events-deep-research-v1",
  segmentLabel: "Social Groups & Special Events",
  discoveryObjective:
    "Discover verified social, wedding, private celebration, or special-event programmes that could create realistic hotel banquet and/or lodging demand near this hotel.",
  deepResearchObjective:
    "Verify event identity, dates, banquet/lodging needs, planner/couple/organizer contact path, and a legitimate commercial approach.",
  preferredRevenueTypes: [
    "banquets",
    "local_catering",
    "guestrooms",
    "private_dining",
  ],
});

const SEGMENT_CONTRACTS = Object.freeze({
  corporate_meetings_incentives: SEGMENT_3_MEETINGS_INCENTIVES_CONTRACTS,
  government_public_sector: SEGMENT_4_GOVERNMENT_CONTRACTS,
  sports_entertainment: SEGMENT_5_SPORTS_ENTERTAINMENT_CONTRACTS,
  education_scientific: SEGMENT_6_EDUCATION_SCIENTIFIC_CONTRACTS,
  projects_infrastructure: SEGMENT_7_PROJECTS_INFRASTRUCTURE_CONTRACTS,
  social_special_events: SEGMENT_8_SOCIAL_SPECIAL_EVENTS_CONTRACTS,
  ...SEGMENTS_9_12_CONTRACTS_BY_ID,
});

function text(v) {
  return String(v || "")
    .replace(/\s+/g, " ")
    .trim();
}

export function getLiveSegmentContract(segmentId) {
  return (
    SEGMENT_CONTRACTS[segmentId] ||
    getSegment912ExaContracts(segmentId) ||
    null
  );
}

function hotelBlock(hotel = {}) {
  const ctx = buildAuthoritativeHotelResearchContext(hotel);
  return formatHotelResearchContextBlock(ctx);
}

/**
 * Build Agent create payload for discovery (no live HTTP).
 */
export function buildSegmentDiscoveryAgentPayloadPreview(
  segmentId,
  hotel = {},
  {
    exclusions = [],
    limit = 10,
    effort = "medium",
    maxCostDollars = 0.5,
    additionalQueryDirectives = [],
  } = {},
) {
  const contract = getLiveSegmentContract(segmentId);
  if (!contract) throw new Error(`unknown_live_segment:${segmentId}`);
  const ctx = hotelBlock(hotel);
  const excl = (Array.isArray(exclusions) ? exclusions : [])
    .map((x) => text(typeof x === "string" ? x : x?.id || x?.title))
    .filter(Boolean)
    .slice(0, 40);
  const extra = (Array.isArray(additionalQueryDirectives)
    ? additionalQueryDirectives
    : []
  )
    .map((d) => text(d))
    .filter(Boolean);
  const query = [
    `GDI ${contract.segmentLabel} Discovery.`,
    contract.discoveryObjective,
    `Hotel research context:\n${ctx}`,
    `Return up to ${limit} candidates with organizationName, programmeName, demandMechanismSummary, revenueTypes, guestroomDemandStatus (NONE_CONFIRMED|UNKNOWN|EVIDENCE_SUPPORTED), relevantBuyerRoles, evidenceUrls, uncertainties.`,
    `Guestrooms are optional when catering/meeting/banquet revenue is credible.`,
    `Do not invent buyers, volumes, RFPs, or procurement access.`,
    excl.length ? `Exclude known opportunity keys: ${excl.join("; ")}` : "",
    `Prefer official and recent sources. Multilingual research OK.`,
    ...extra,
  ]
    .filter(Boolean)
    .join("\n\n");

  const body = {
    query,
    effort,
    outputSchema: {
      type: "object",
      required: ["candidates"],
      properties: {
        candidates: {
          type: "array",
          items: {
            type: "object",
            required: [
              "organizationName",
              "demandMechanismSummary",
              "evidenceUrls",
            ],
            properties: {
              organizationName: { type: "string" },
              programmeName: { type: "string" },
              demandMechanismSummary: { type: "string" },
              revenueTypes: { type: "array", items: { type: "string" } },
              guestroomDemandStatus: {
                type: "string",
                enum: ["NONE_CONFIRMED", "UNKNOWN", "EVIDENCE_SUPPORTED"],
              },
              relevantBuyerRoles: { type: "array", items: { type: "string" } },
              evidenceUrls: { type: "array", items: { type: "string" } },
              uncertainties: { type: "array", items: { type: "string" } },
            },
          },
        },
      },
    },
  };
  // Exa: budget.maxCostDollars only for metered efforts (auto/max), and must be >= 1.
  const e = String(effort).toLowerCase();
  if ((e === "auto" || e === "max" || e === "ultra") && maxCostDollars != null) {
    body.budget = { maxCostDollars: Math.max(1, Number(maxCostDollars)) };
  }
  return {
    contractId: contract.discoveryContractId,
    contextBlock: ctx,
    body,
  };
}

export function buildSegmentDeepResearchAgentPayloadPreview(
  segmentId,
  hotel = {},
  candidate = {},
  { effort = "high", maxCostDollars = 1.0 } = {},
) {
  const contract = getLiveSegmentContract(segmentId);
  if (!contract) throw new Error(`unknown_live_segment:${segmentId}`);
  const ctx = hotelBlock(hotel);
  const org = text(candidate.organizationName || candidate.companyName);
  const programme = text(candidate.programmeName || candidate.title);
  const query = [
    `GDI ${contract.segmentLabel} Deep Research.`,
    contract.deepResearchObjective || contract.discoveryObjective,
    `Candidate: ${org} — ${programme}`,
    `Prior summary: ${text(candidate.demandMechanismSummary)}`,
    `Hotel research context:\n${ctx}`,
    `Return eventIdentity/programmeIdentity, hotelDemandThesis, namedContacts (name, title, whyRelevant), revenueTypes, guestroomDemandStatus, procurementOrAccessibilityNotes, recommendedNextStep, qualificationQuestions, supportingSources[{claim,url}], establishedFacts, uncertainties.`,
    `Do not invent room nights, budgets, or contracting authority.`,
    buildUniversalContactResearchPromptBlock({
      segmentId: segmentId,
      segmentLabel: contract.segmentLabel,
    }),
  ].join("\n\n");

  const body = {
    query,
    effort,
    outputSchema: {
      type: "object",
      required: ["programmeIdentity", "supportingSources"],
      properties: {
        programmeIdentity: { type: "string" },
        hotelDemandThesis: { type: "string" },
        namedContacts: { type: "array" },
        revenueTypes: { type: "array", items: { type: "string" } },
        guestroomDemandStatus: { type: "string" },
        procurementOrAccessibilityNotes: { type: "string" },
        recommendedNextStep: { type: "string" },
        qualificationQuestions: { type: "array", items: { type: "string" } },
        supportingSources: {
          type: "array",
          items: {
            type: "object",
            required: ["claim", "url"],
            properties: {
              claim: { type: "string" },
              url: { type: "string" },
            },
          },
        },
        establishedFacts: { type: "array", items: { type: "string" } },
        uncertainties: { type: "array", items: { type: "string" } },
      },
    },
  };
  const e = String(effort).toLowerCase();
  if ((e === "auto" || e === "max" || e === "ultra") && maxCostDollars != null) {
    body.budget = { maxCostDollars: Math.max(1, Number(maxCostDollars)) };
  }
  return {
    contractId: contract.deepResearchContractId,
    contextBlock: ctx,
    body,
  };
}

/**
 * Execute discovery or deep research via shared adapter.
 */
export async function executeSegment4912ExaAgentOrsStrategy({
  segmentId,
  mode = "discovery",
  hotel = {},
  candidate = {},
  exclusions = [],
  authorizeExaAgentLive = false,
  forceMockOnly = false,
  mockStructured = null,
  effort = null,
  maxCostDollars = null,
  limit = 10,
  pollIntervalMs = 4000,
  maxPolls = 180,
  additionalQueryDirectives = [],
} = {}) {
  const { runExaAgentResearch, quoteExaAgentRun } = await import(
    "./exa-agent-provider-adapter-v1.js"
  );

  const resolvedEffort =
    effort || (mode === "deep_research" ? "high" : "medium");
  const resolvedMax =
    maxCostDollars != null
      ? Number(maxCostDollars)
      : mode === "deep_research"
        ? 1.0
        : 0.5;

  const preview =
    mode === "deep_research"
      ? buildSegmentDeepResearchAgentPayloadPreview(segmentId, hotel, candidate, {
          effort: resolvedEffort,
          maxCostDollars: resolvedMax,
        })
      : buildSegmentDiscoveryAgentPayloadPreview(segmentId, hotel, {
          exclusions,
          limit,
          effort: resolvedEffort,
          maxCostDollars: resolvedMax,
          additionalQueryDirectives,
        });

  const quote = quoteExaAgentRun({
    effort: resolvedEffort,
    maxCostDollars: resolvedMax,
  });

  const run = await runExaAgentResearch({
    mode: mode === "deep_research" ? "deep_research" : "discovery",
    contextBlock: preview.contextBlock,
    effort: resolvedEffort,
    maxCostDollars: resolvedMax,
    authorizeExaAgentLive,
    forceMockOnly: forceMockOnly === true || mockStructured != null,
    mockStructured,
    createPayloadOverride: preview.body,
    contractIdOverride: preview.contractId,
    pollIntervalMs,
    maxPolls,
  });

  return {
    version: EXA_SEGMENTS_4_9_12_ORS_STRATEGY_V1,
    segmentId,
    mode,
    quote,
    run,
    requestPayloadMeta: {
      contractId: preview.contractId,
      queryChars: String(preview.body?.query || "").length,
      hasOutputSchema: Boolean(preview.body?.outputSchema),
      liveAuthorized: authorizeExaAgentLive === true,
      forceMockOnly: forceMockOnly === true || mockStructured != null,
    },
  };
}
