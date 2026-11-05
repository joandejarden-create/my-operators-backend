/**
 * Events Exa Agent ORS context builders — hotel-adaptive, no hotel hardcodes in prompts.
 */
import {
  buildAuthoritativeHotelResearchContext,
  formatHotelResearchContextBlock,
} from "../authoritative-hotel-research-context-v1.js";
import {
  buildExaEventsAgentCreatePayload,
  EXA_EVENTS_DISCOVERY_CONTRACT_V1,
  EXA_EVENTS_DEEP_RESEARCH_CONTRACT_V1,
} from "./exa-agent-events-contracts-v1.js";
import { buildUniversalContactResearchPromptBlock } from "../universal-contact-research-contract-v1.js";

export const EXA_EVENTS_ORS_STRATEGY_V1 = "gdi-exa-events-ors-strategy-v1";

export const EXA_EVENTS_RESEARCH_STRATEGIES = Object.freeze({
  EXA_AGENT_EVENTS_DISCOVERY: "EXA_AGENT_EVENTS_DISCOVERY",
  EXA_AGENT_EVENTS_DEEP_RESEARCH: "EXA_AGENT_EVENTS_DEEP_RESEARCH",
});

export function buildEventsDiscoveryContextBlock({
  hotel = {},
  exclusions = [],
  limit = 12,
  horizonMonths = 18,
  additionalQueryDirectives = [],
} = {}) {
  const ctx = buildAuthoritativeHotelResearchContext(hotel, {
    contextAddress: hotel.address || null,
  });
  const extra = (Array.isArray(additionalQueryDirectives)
    ? additionalQueryDirectives
    : []
  )
    .map((d) => text(d))
    .filter(Boolean);
  return [
    formatHotelResearchContextBlock(ctx),
    "",
    `Hotel ID: ${hotel.hotelId || hotel.id || "(unknown)"}`,
    `Candidate limit: ${limit} (prefer quality over volume; 10–15 max when market supports).`,
    `Discovery horizon: ${horizonMonths} months forward + historical editions as recurrence/relationship evidence.`,
    `Segment: Association / Conference / Events (research axis associations_government).`,
    `Do NOT use Scaling Companies discovery rules. Route ongoing corporate-account theses via ROUTE_TO_SCALING.`,
    `Assign commercialOpportunityTrack TRACK_A|B|C|D honestly.`,
    exclusions.length
      ? `Exclude / de-duplicate known entities: ${exclusions.slice(0, 40).join("; ")}`
      : "No exclusion list supplied.",
    ...extra,
  ].join("\n");
}

export function buildEventsDeepResearchContextBlock({
  hotel = {},
  candidate = {},
} = {}) {
  const ctx = buildAuthoritativeHotelResearchContext(hotel, {
    contextAddress: hotel.address || null,
  });
  return [
    formatHotelResearchContextBlock(ctx),
    "",
    `Focus candidate: ${candidate.eventIdentity || candidate.eventName || candidate.title || "(unnamed)"}`,
    `Organizer: ${candidate.organizingEntity || candidate.organizationName || "(unknown)"}`,
    `Track: ${candidate.commercialOpportunityTrack || "(unset)"}`,
    `Prior disposition: ${candidate.discoveryDisposition || "(unset)"}`,
    `Investigate only material uncertainties for commercial qualification.`,
    `Preserve current-edition vs future-edition assessments independently.`,
    buildUniversalContactResearchPromptBlock({
      segmentId: "associations_conferences_events",
      segmentLabel: "Association / Conference / Events",
    }),
  ].join("\n");
}

export function buildEventsDiscoveryAgentPayloadPreview(hotel = {}, opts = {}) {
  const contextBlock = buildEventsDiscoveryContextBlock({ hotel, ...opts });
  const pack = buildExaEventsAgentCreatePayload({
    mode: "discovery",
    contextBlock,
    effort: opts.effort || "medium",
    maxCostDollars: opts.maxCostDollars ?? null,
    exclusion: opts.exclusions || null,
  });
  return {
    version: EXA_EVENTS_ORS_STRATEGY_V1,
    strategyId: EXA_EVENTS_RESEARCH_STRATEGIES.EXA_AGENT_EVENTS_DISCOVERY,
    contractId: EXA_EVENTS_DISCOVERY_CONTRACT_V1,
    contextBlock,
    ...pack,
  };
}

export function buildEventsDeepResearchAgentPayloadPreview(
  hotel = {},
  candidate = {},
  opts = {},
) {
  const contextBlock = buildEventsDeepResearchContextBlock({ hotel, candidate });
  const pack = buildExaEventsAgentCreatePayload({
    mode: "deep_research",
    contextBlock,
    effort: opts.effort || "medium",
    maxCostDollars: opts.maxCostDollars ?? null,
  });
  return {
    version: EXA_EVENTS_ORS_STRATEGY_V1,
    strategyId: EXA_EVENTS_RESEARCH_STRATEGIES.EXA_AGENT_EVENTS_DEEP_RESEARCH,
    contractId: EXA_EVENTS_DEEP_RESEARCH_CONTRACT_V1,
    contextBlock,
    ...pack,
  };
}

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

/**
 * Validate Events Discovery structured output (contract shape + commercial gates).
 * Does not invent fields. Rejects fabricated future editions without evidence.
 */
export function validateEventsDiscoveryStructured(structured = {}) {
  const issues = [];
  if (!structured || typeof structured !== "object") {
    return { ok: false, issues: [{ severity: "P0", code: "missing_structured" }], acceptedCandidates: [] };
  }
  if (structured.contractId !== EXA_EVENTS_DISCOVERY_CONTRACT_V1) {
    issues.push({
      severity: "P0",
      code: "wrong_contract",
      message: `Expected ${EXA_EVENTS_DISCOVERY_CONTRACT_V1}, got ${structured.contractId}`,
    });
  }
  const candidates = Array.isArray(structured.candidates) ? structured.candidates : [];
  const acceptedCandidates = [];
  for (let i = 0; i < candidates.length; i += 1) {
    const c = candidates[i];
    const path = `candidates[${i}]`;
    let reject = false;
    if (!text(c?.eventIdentity)) {
      issues.push({ severity: "P0", code: "event_identity_missing", path });
      reject = true;
    }
    if (!text(c?.organizingEntity)) {
      issues.push({ severity: "P0", code: "organizer_missing", path });
      reject = true;
    }
    if (!text(c?.commercialOpportunityTrack)) {
      issues.push({ severity: "P0", code: "track_missing", path });
      reject = true;
    }
    if (!Array.isArray(c?.supportingSources) || !c.supportingSources.length) {
      issues.push({ severity: "P1", code: "sources_missing", path });
    }
    // False-future: historical_completed must not claim verified upcoming dates as open booking
    if (
      c.eventStatus === "historical_completed" &&
      c.verifiedDates &&
      c.commercialOpportunityTrack === "TRACK_A_UPCOMING_VENUE"
    ) {
      issues.push({
        severity: "P0",
        code: "historical_as_upcoming_track_a",
        path,
        message: "Historical event cannot be TRACK_A with verifiedDates as open venue",
      });
      reject = true;
    }
    if (c.discoveryDisposition === "ROUTE_TO_SCALING") {
      // Keep for routing audit; not accepted into Events deep research
      issues.push({
        severity: "P1",
        code: "route_to_scaling",
        path,
        message: text(c.eventIdentity),
      });
      reject = true;
    }
    if (!reject) acceptedCandidates.push(c);
  }
  const p0 = issues.filter((x) => x.severity === "P0");
  return {
    ok: p0.length === 0,
    issues,
    acceptedCandidates,
    candidateCount: candidates.length,
  };
}

export function validateEventsDeepResearchStructured(structured = {}) {
  const issues = [];
  if (!structured || typeof structured !== "object") {
    return { ok: false, issues: [{ severity: "P0", code: "missing_structured" }] };
  }
  if (structured.contractId !== EXA_EVENTS_DEEP_RESEARCH_CONTRACT_V1) {
    issues.push({
      severity: "P0",
      code: "wrong_contract",
      message: structured.contractId,
    });
  }
  if (!text(structured.eventIdentity)) {
    issues.push({ severity: "P0", code: "event_identity_missing" });
  }
  if (!Array.isArray(structured.supportingSources) || !structured.supportingSources.length) {
    issues.push({ severity: "P0", code: "sources_missing" });
  }
  for (const s of structured.supportingSources || []) {
    if (!text(s?.claim) || !text(s?.url)) {
      issues.push({ severity: "P0", code: "claim_url_incomplete" });
    }
  }
  const p0 = issues.filter((x) => x.severity === "P0");
  return { ok: p0.length === 0, issues, structured };
}

/**
 * Execute Events Exa Agent via the shared provider adapter.
 * Live HTTP still requires dual auth; pass mockStructured + forceMockOnly for zero-paid FullE2E.
 */
export async function executeExaEventsAgentOrsStrategy({
  strategyId,
  hotel = {},
  candidate = {},
  exclusions = [],
  authorizeExaAgentLive = false,
  mockStructured = null,
  mockStatus = "completed",
  effort = "medium",
  maxCostDollars = null,
  forceMockOnly = false,
  limit = 12,
  pollIntervalMs = 4000,
  maxPolls = 60,
  additionalQueryDirectives = [],
} = {}) {
  const { runExaAgentResearch, quoteExaAgentRun } = await import(
    "./exa-agent-provider-adapter-v1.js"
  );

  const mode =
    strategyId === EXA_EVENTS_RESEARCH_STRATEGIES.EXA_AGENT_EVENTS_DEEP_RESEARCH
      ? "deep_research"
      : "discovery";

  const preview =
    mode === "discovery"
      ? buildEventsDiscoveryAgentPayloadPreview(hotel, {
          exclusions,
          limit,
          effort,
          maxCostDollars,
          additionalQueryDirectives,
        })
      : buildEventsDeepResearchAgentPayloadPreview(hotel, candidate, {
          effort,
          maxCostDollars,
        });

  const quote = quoteExaAgentRun({ effort, maxCostDollars });
  const run = await runExaAgentResearch({
    mode,
    contextBlock: preview.contextBlock,
    effort,
    maxCostDollars,
    authorizeExaAgentLive,
    mockStructured,
    mockStatus,
    forceMockOnly: forceMockOnly === true || mockStructured != null,
    createPayloadOverride: preview.body,
    contractIdOverride: preview.contractId,
    pollIntervalMs,
    maxPolls,
  });

  if (!run.ok) {
    return {
      version: EXA_EVENTS_ORS_STRATEGY_V1,
      strategyId:
        strategyId ||
        (mode === "discovery"
          ? EXA_EVENTS_RESEARCH_STRATEGIES.EXA_AGENT_EVENTS_DISCOVERY
          : EXA_EVENTS_RESEARCH_STRATEGIES.EXA_AGENT_EVENTS_DEEP_RESEARCH),
      mode,
      quote,
      run,
      validation: null,
      requestPayloadMeta: {
        contractId: preview.contractId,
        queryChars: String(preview.body?.query || "").length,
        hasOutputSchema: Boolean(preview.body?.outputSchema),
      },
      continueResearch: true,
    };
  }

  if (mode === "discovery") {
    const validation = validateEventsDiscoveryStructured(run.structured);
    return {
      version: EXA_EVENTS_ORS_STRATEGY_V1,
      strategyId: EXA_EVENTS_RESEARCH_STRATEGIES.EXA_AGENT_EVENTS_DISCOVERY,
      mode,
      quote,
      run,
      validation,
      discoveryCandidates: validation.acceptedCandidates || [],
      requestPayloadMeta: {
        contractId: preview.contractId,
        queryChars: String(preview.body?.query || "").length,
        hasOutputSchema: Boolean(preview.body?.outputSchema),
      },
      continueResearch: true,
    };
  }

  const validation = validateEventsDeepResearchStructured(run.structured);
  return {
    version: EXA_EVENTS_ORS_STRATEGY_V1,
    strategyId: EXA_EVENTS_RESEARCH_STRATEGIES.EXA_AGENT_EVENTS_DEEP_RESEARCH,
    mode,
    quote,
    run,
    validation,
    deepResearch: run.structured,
    requestPayloadMeta: {
      contractId: preview.contractId,
      queryChars: String(preview.body?.query || "").length,
      hasOutputSchema: Boolean(preview.body?.outputSchema),
    },
    continueResearch: Boolean(run.structured?.suggestedOrsFollowUps?.length),
  };
}
