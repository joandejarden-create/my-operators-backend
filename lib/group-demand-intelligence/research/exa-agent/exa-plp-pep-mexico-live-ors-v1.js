/**
 * Live Exa ORS payload builders for Mexico Preferred Lodging + Professional Event Planners.
 * Dispatch only via exa-agent-provider-adapter-v1 (createPayloadOverride).
 */
import {
  buildAuthoritativeHotelResearchContext,
  formatHotelResearchContextBlock,
} from "../authoritative-hotel-research-context-v1.js";
import {
  EXA_PREFERRED_LODGING_DISCOVERY_V1,
  EXA_PREFERRED_LODGING_DEEP_V1,
  buildPreferredLodgingDiscoveryPrompt,
} from "../../preferred-lodging-partnership/exa-preferred-lodging-contracts-v1.js";
import {
  EXA_PROFESSIONAL_EVENT_PLANNER_DISCOVERY_V1,
  EXA_PROFESSIONAL_EVENT_PLANNER_DEEP_V1,
  buildProfessionalEventPlannerDiscoveryPrompt,
} from "../../professional-event-planners/exa-professional-event-planner-contracts-v1.js";
import { buildUniversalContactResearchPromptBlock } from "../universal-contact-research-contract-v1.js";

export const EXA_PLP_PEP_MEXICO_LIVE_ORS_V1 = "gdi-exa-plp-pep-mexico-live-ors-v1";

function text(v) {
  return String(v || "")
    .replace(/\s+/g, " ")
    .trim();
}

function hotelBlock(hotel = {}) {
  const ctx = buildAuthoritativeHotelResearchContext(hotel);
  return formatHotelResearchContextBlock(ctx);
}

function omitBudgetForFixedEffort(body, effort) {
  const e = String(effort || "").toLowerCase();
  if (e === "auto" || e === "max" || e === "ultra") return body;
  const next = { ...body };
  delete next.budget;
  return next;
}

export function buildPlpDiscoveryPayload(hotel = {}, { exclusions = [], limit = 10, effort = "medium" } = {}) {
  const contract = buildPreferredLodgingDiscoveryPrompt({ hotel });
  const ctx = hotelBlock(hotel);
  const excl = (exclusions || []).map(text).filter(Boolean).slice(0, 40);
  const query = [
    `GDI Preferred Lodging Partnership Discovery.`,
    ...contract.instructions,
    `Hotel research context:\n${ctx}`,
    `Return up to ${limit} venues with venueName, venueAddress, website, venueType, eventTypes, onsiteLodgingMention, preferredHotelLanguage, evidenceUrls, uncertainties.`,
    `Do not invent distances, drive times, annual wedding counts, or room-night forecasts.`,
    `Never infer NO_ONSITE lodging from silence alone — use UNKNOWN when not evidenced.`,
    excl.length ? `Exclude known venue/opportunity keys: ${excl.join("; ")}` : "",
    `Use Spanish and English local terminology for ${hotel.market || hotel.city || "Mexico"}.`,
  ]
    .filter(Boolean)
    .join("\n\n");

  const body = omitBudgetForFixedEffort(
    {
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
              required: ["venueName", "evidenceUrls"],
              properties: {
                venueName: { type: "string" },
                venueAddress: { type: "string" },
                website: { type: "string" },
                venueType: { type: "string" },
                eventTypes: { type: "string" },
                onsiteLodgingMention: { type: "string" },
                preferredHotelLanguage: { type: "string" },
                evidenceUrls: { type: "array", items: { type: "string" } },
                uncertainties: { type: "array", items: { type: "string" } },
              },
            },
          },
        },
      },
    },
    effort,
  );
  return { contractId: EXA_PREFERRED_LODGING_DISCOVERY_V1, contextBlock: ctx, body };
}

export function buildPlpDeepPayload(hotel = {}, venue = {}, { effort = "high" } = {}) {
  const ctx = hotelBlock(hotel);
  const name = text(venue.venueName || venue.name);
  const query = [
    `GDI Preferred Lodging Partnership Deep Research.`,
    `Deep-research venue "${name}" as a Preferred Lodging Partnership prospect for hotel "${hotel.name || hotel.displayName}".`,
    `Prior discovery notes: ${text(venue.eventTypes)} · lodging mention: ${text(venue.onsiteLodgingMention)}`,
    `Hotel research context:\n${ctx}`,
    `Return venueIdentity, venueAddress, website, onSiteLodgingStatus (NO_ONSITE_LODGING|LIMITED_ONSITE_LODGING|ONSITE_LODGING_AVAILABLE|UNKNOWN), eventActivitySummary, capacityIfPublished, namedContacts[{name,title,whyRelevant,sourceUrl}], hotelFitThesis, advantages, limitations, recommendedNextStep, qualificationQuestions, supportingSources[{claim,url}], establishedFacts, uncertainties.`,
    `Do not invent partnership agreements, rates, drive times, or volumes.`,
    buildUniversalContactResearchPromptBlock({
      segmentId: "preferred_lodging_partnerships",
      segmentLabel: "Preferred Lodging Partnerships",
    }),
  ].join("\n\n");

  const body = omitBudgetForFixedEffort(
    {
      query,
      effort,
      outputSchema: {
        type: "object",
        required: ["venueIdentity", "supportingSources"],
        properties: {
          venueIdentity: { type: "string" },
          venueAddress: { type: "string" },
          website: { type: "string" },
          onSiteLodgingStatus: { type: "string" },
          eventActivitySummary: { type: "string" },
          capacityIfPublished: { type: "string" },
          namedContacts: { type: "array" },
          hotelFitThesis: { type: "string" },
          advantages: { type: "array", items: { type: "string" } },
          limitations: { type: "array", items: { type: "string" } },
          recommendedNextStep: { type: "string" },
          qualificationQuestions: { type: "array", items: { type: "string" } },
          supportingSources: {
            type: "array",
            items: {
              type: "object",
              required: ["claim", "url"],
              properties: { claim: { type: "string" }, url: { type: "string" } },
            },
          },
          establishedFacts: { type: "array", items: { type: "string" } },
          uncertainties: { type: "array", items: { type: "string" } },
        },
      },
    },
    effort,
  );
  return { contractId: EXA_PREFERRED_LODGING_DEEP_V1, contextBlock: ctx, body };
}

export function buildPepDiscoveryPayload(hotel = {}, { exclusions = [], limit = 10, effort = "medium" } = {}) {
  const contract = buildProfessionalEventPlannerDiscoveryPrompt({ hotel });
  const ctx = hotelBlock(hotel);
  const excl = (exclusions || []).map(text).filter(Boolean).slice(0, 40);
  const terms = (contract.localTerminology || []).slice(0, 8).join("; ");
  const query = [
    `GDI Professional Event Planner Partnership Discovery.`,
    ...contract.instructions,
    `Hotel research context:\n${ctx}`,
    `Local terminology seeds: ${terms}`,
    `Return up to ${limit} planners with plannerCompanyName, officialWebsite, officeLocation, operatingMarkets, specialization, eventTypes, destinationEvidence, hotelInfluenceSignals, namedContactHints, evidenceUrls, uncertainties.`,
    `Do not invent annual event volumes or hotel contracts.`,
    `Exclude decorators/florists/photographers/equipment/entertainment vendors unless hotel-sourcing influence is evidenced.`,
    excl.length ? `Exclude known planner/opportunity keys: ${excl.join("; ")}` : "",
  ]
    .filter(Boolean)
    .join("\n\n");

  const body = omitBudgetForFixedEffort(
    {
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
              required: ["plannerCompanyName", "evidenceUrls"],
              properties: {
                plannerCompanyName: { type: "string" },
                officialWebsite: { type: "string" },
                officeLocation: { type: "string" },
                operatingMarkets: { type: "array", items: { type: "string" } },
                specialization: { type: "string" },
                eventTypes: { type: "string" },
                destinationEvidence: { type: "string" },
                hotelInfluenceSignals: { type: "string" },
                namedContactHints: { type: "array", items: { type: "string" } },
                evidenceUrls: { type: "array", items: { type: "string" } },
                uncertainties: { type: "array", items: { type: "string" } },
              },
            },
          },
        },
      },
    },
    effort,
  );
  return {
    contractId: EXA_PROFESSIONAL_EVENT_PLANNER_DISCOVERY_V1,
    contextBlock: ctx,
    body,
  };
}

export function buildPepDeepPayload(hotel = {}, planner = {}, { effort = "high" } = {}) {
  const ctx = hotelBlock(hotel);
  const name = text(planner.plannerCompanyName || planner.name);
  const query = [
    `GDI Professional Event Planner Partnership Deep Research.`,
    `Deep-research planner/agency "${name}" as a Professional Event Planner Partnership prospect for hotel "${hotel.name || hotel.displayName}".`,
    `Prior discovery: ${text(planner.specialization)} · ${text(planner.destinationEvidence)} · influence: ${text(planner.hotelInfluenceSignals)}`,
    `Hotel research context:\n${ctx}`,
    `Return plannerIdentity, website, officeLocation, operatingMarkets, specialization, eventPortfolioSummary, destinationEvidence, revenueMechanisms, commercialInfluenceSummary, namedContacts[{name,title,whyRelevant,sourceUrl}], hotelFitThesis, advantages, limitations, recommendedNextStep, qualificationQuestions, supportingSources[{claim,url}], establishedFacts, uncertainties.`,
    `Do not invent volumes, exclusivity, or booking authority solely from planner title.`,
    buildUniversalContactResearchPromptBlock({
      segmentId: "professional_event_planners",
      segmentLabel: "Professional Event Planners",
    }),
  ].join("\n\n");

  const body = omitBudgetForFixedEffort(
    {
      query,
      effort,
      outputSchema: {
        type: "object",
        required: ["plannerIdentity", "supportingSources"],
        properties: {
          plannerIdentity: { type: "string" },
          website: { type: "string" },
          officeLocation: { type: "string" },
          operatingMarkets: { type: "array", items: { type: "string" } },
          specialization: { type: "string" },
          eventPortfolioSummary: { type: "string" },
          destinationEvidence: { type: "string" },
          revenueMechanisms: { type: "array", items: { type: "string" } },
          commercialInfluenceSummary: { type: "string" },
          namedContacts: { type: "array" },
          hotelFitThesis: { type: "string" },
          advantages: { type: "array", items: { type: "string" } },
          limitations: { type: "array", items: { type: "string" } },
          recommendedNextStep: { type: "string" },
          qualificationQuestions: { type: "array", items: { type: "string" } },
          supportingSources: {
            type: "array",
            items: {
              type: "object",
              required: ["claim", "url"],
              properties: { claim: { type: "string" }, url: { type: "string" } },
            },
          },
          establishedFacts: { type: "array", items: { type: "string" } },
          uncertainties: { type: "array", items: { type: "string" } },
        },
      },
    },
    effort,
  );
  return {
    contractId: EXA_PROFESSIONAL_EVENT_PLANNER_DEEP_V1,
    contextBlock: ctx,
    body,
  };
}

export async function executePlpPepExaLive({
  workflow = "plp",
  mode = "discovery",
  hotel = {},
  candidate = {},
  exclusions = [],
  authorizeExaAgentLive = false,
  effort = null,
  limit = 10,
  pollIntervalMs = 4000,
  maxPolls = 180,
} = {}) {
  const { runExaAgentResearch, quoteExaAgentRun } = await import(
    "./exa-agent-provider-adapter-v1.js"
  );
  const resolvedEffort =
    effort || (mode === "deep_research" ? "high" : "medium");
  const preview =
    workflow === "pep"
      ? mode === "deep_research"
        ? buildPepDeepPayload(hotel, candidate, { effort: resolvedEffort })
        : buildPepDiscoveryPayload(hotel, { exclusions, limit, effort: resolvedEffort })
      : mode === "deep_research"
        ? buildPlpDeepPayload(hotel, candidate, { effort: resolvedEffort })
        : buildPlpDiscoveryPayload(hotel, { exclusions, limit, effort: resolvedEffort });

  const quote = quoteExaAgentRun({ effort: resolvedEffort });
  const run = await runExaAgentResearch({
    mode: mode === "deep_research" ? "deep_research" : "discovery",
    contextBlock: preview.contextBlock,
    effort: resolvedEffort,
    authorizeExaAgentLive,
    forceMockOnly: false,
    createPayloadOverride: preview.body,
    contractIdOverride: preview.contractId,
    pollIntervalMs,
    maxPolls,
  });

  return {
    version: EXA_PLP_PEP_MEXICO_LIVE_ORS_V1,
    workflow,
    mode,
    quote,
    run,
    requestPayloadMeta: {
      contractId: preview.contractId,
      queryChars: String(preview.body?.query || "").length,
      liveAuthorized: authorizeExaAgentLive === true,
    },
  };
}
