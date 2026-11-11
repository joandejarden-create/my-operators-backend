/**
 * Bridge: segment discovery executor provider ↔ existing Exa Agent adapter.
 *
 * Default: refuse live HTTP (dual-auth required).
 * Does not invent a second Exa client — reuses runExaAgentResearch.
 *
 * Pilot readiness inspection is free. Paid calls require:
 *   authorizeExaAgentLive === true AND process.env.GDI_EXA_AGENT_LIVE === "1"
 *   AND a separate founder authorization flag for the $40 pilot.
 */

import {
  isExaAgentLiveAuthorized,
  quoteExaAgentRun,
  runExaAgentResearch,
  buildExaAgentCreatePayload,
  EXA_AGENT_EFFORT,
  EXA_AGENT_PROVIDER_ADAPTER_V1,
} from "./exa-agent-provider-adapter-v1.js";
import {
  buildExaEventsAgentCreatePayload,
} from "./exa-agent-events-contracts-v1.js";
import {
  EXA_AGENT_DISCOVERY_CONTRACT_ACTIVE,
} from "./exa-agent-contracts-v1.js";
import {
  resolveExaAgentRoutingForSegment,
  buildExaOperationRoutingMetadata,
  EXA_AGENT_FAMILY,
  ROUTING_SUPPORT,
} from "./gdi-segment-aware-exa-agent-routing-v1.js";
import {
  buildPreferredLodgingExaAgentCreatePayload,
  EXA_PREFERRED_LODGING_DISCOVERY_V1,
} from "../../preferred-lodging-partnership/preferred-lodging-exa-payload-compat-v1.js";

export const EXA_LIVE_DISCOVERY_PROVIDER_BRIDGE_V1 =
  "gdi-exa-live-discovery-provider-bridge-v1";

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function hotelContextBlock(hotel = {}, segmentId = "") {
  return [
    "HOTEL RESEARCH CONTEXT",
    `Hotel ID: ${hotel.hotelId || ""}`,
    `Hotel name: ${hotel.displayName || hotel.name || ""}`,
    `City: ${hotel.city || ""}`,
    `Country: ${hotel.country || ""}`,
    `Demand territory: ${hotel.demandTerritory?.label || ""}`,
    `Segment: ${segmentId}`,
    "Languages: Spanish and English sources both in-scope when market is LATAM/MX/DR.",
    "Return evidence-backed demand signals only. Do not invent buyer contacts.",
  ].join("\n");
}

/**
 * Readiness probe — no network. Returns auth + quote + payload shape.
 */
export function inspectExaLiveDiscoveryReadiness({
  hotel = {},
  segmentId = "associations_conferences_events",
  effort = EXA_AGENT_EFFORT.MEDIUM,
  maxCostDollars = null,
  authorizeExaAgentLive = false,
} = {}) {
  const liveAuthorized = isExaAgentLiveAuthorized({ authorizeExaAgentLive });
  const hasKey = Boolean(process.env.EXA_API_KEY || process.env.EXA_KEY);
  const quote = quoteExaAgentRun({ effort, maxCostDollars });
  const routing = resolveExaAgentRoutingForSegment(segmentId, {
    stage: "DISCOVERY",
  });
  const routingMeta = buildExaOperationRoutingMetadata({
    hotel,
    segmentId,
    stage: "DISCOVERY",
  });
  const contextBlock = [
    hotelContextBlock(hotel, segmentId),
    `Agent family: ${routing.agentName || "NONE"}`,
    `Routing support: ${routing.support}`,
  ].join("\n");

  let payloadPack = null;
  let contractId = null;
  let contractLoadError = null;
  try {
    if (!routing.paidExaAllowed || !routing.discovery) {
      contractLoadError =
        routing.blockReason || "AGENT_FAMILY_UNSUPPORTED_FOR_SEGMENT";
      payloadPack = { body: { query: contextBlock, effort }, meta: null };
    } else if (routing.agentFamily === EXA_AGENT_FAMILY.ASSOCIATION_EVENTS) {
      payloadPack = buildExaEventsAgentCreatePayload({
        mode: "discovery",
        contextBlock,
        effort,
        maxCostDollars,
      });
      contractId = routing.discovery.contractId;
    } else if (routing.agentFamily === EXA_AGENT_FAMILY.SCALING_COMPANIES) {
      const body = buildExaAgentCreatePayload({
        mode: "discovery",
        contextBlock,
        effort,
        maxCostDollars,
      });
      payloadPack = {
        body,
        meta: {
          contractId: routing.discovery.contractId || EXA_AGENT_DISCOVERY_CONTRACT_ACTIVE,
          mode: "discovery",
          effort,
          segment: segmentId,
          agentFamily: routing.agentFamily,
          liveCall: false,
        },
      };
      contractId = routing.discovery.contractId || EXA_AGENT_DISCOVERY_CONTRACT_ACTIVE;
    } else if (routing.agentFamily === EXA_AGENT_FAMILY.PREFERRED_LODGING) {
      const pack = buildPreferredLodgingExaAgentCreatePayload({
        mode: "discovery",
        contextBlock,
        effort,
        maxCostDollars,
        hotel,
      });
      payloadPack = {
        body: pack.body,
        meta: {
          ...pack.meta,
          segment: segmentId,
          agentFamily: routing.agentFamily,
        },
      };
      contractId =
        routing.discovery.contractId || EXA_PREFERRED_LODGING_DISCOVERY_V1;
    } else {
      contractLoadError = "NO_EXECUTABLE_DISCOVERY_AGENT_FAMILY";
      payloadPack = { body: { query: contextBlock, effort }, meta: null };
    }
  } catch (err) {
    contractLoadError = String(err?.message || err);
    payloadPack = { body: { query: contextBlock, effort }, meta: null };
  }

  const query = String(payloadPack?.body?.query || "");
  const unsupported =
    routing.support === ROUTING_SUPPORT.UNSUPPORTED || !routing.paidExaAllowed;
  return {
    version: EXA_LIVE_DISCOVERY_PROVIDER_BRIDGE_V1,
    adapterVersion: EXA_AGENT_PROVIDER_ADAPTER_V1,
    segmentId,
    contractId,
    agentRouting: routingMeta,
    agentFamily: routing.agentFamily,
    agentName: routing.agentName,
    routingSupport: routing.support,
    liveAuthorized,
    credentialPresent: hasKey,
    // Authenticated ≠ successful live retrieval
    authenticatedDoesNotProveLiveRetrieval: true,
    quote,
    effort,
    maxCostDollars,
    payload: {
      effort: payloadPack?.body?.effort || effort,
      hasOutputSchema: Boolean(payloadPack?.body?.outputSchema),
      queryChars: query.length,
      hotelContextPresent: /Hotel ID:|HOTEL RESEARCH CONTEXT/i.test(query),
      spanishEnglishScopePresent: /Spanish and English/i.test(query),
      contractIdInQuery: Boolean(contractId && query.includes(contractId)),
      agentFamilyInQuery: Boolean(
        routing.agentName && query.includes(String(routing.agentName).slice(0, 12))
      ),
    },
    readyForPaidPilot:
      !unsupported &&
      quote.bounded === true &&
      hasKey &&
      Boolean(payloadPack?.body?.outputSchema) &&
      query.length > 100,
    blockers: [
      unsupported ? `AGENT_FAMILY_UNSUPPORTED:${segmentId}` : null,
      !hasKey ? "EXA_API_KEY_ABSENT" : null,
      !quote.bounded ? `QUOTE_UNBOUNDED:${quote.reason}` : null,
      !payloadPack?.body?.outputSchema ? "OUTPUT_SCHEMA_MISSING" : null,
      contractLoadError ? `CONTRACT_LOAD_ERROR:${contractLoadError}` : null,
    ].filter(Boolean),
    note:
      "Do not treat credential presence or readiness=true as authorization to spend. Founder must authorize paid research separately.",
  };
}

/**
 * Create an injectable provider for runSegmentDiscoveryExecutor.
 * Live path refused unless dual-auth + opts.authorizePaidDiscovery.
 */
export function createExaLiveDiscoveryProvider(opts = {}) {
  const effort = opts.effort || EXA_AGENT_EFFORT.MEDIUM;
  const maxCostDollars = opts.maxCostDollars ?? null;
  const authorizePaidDiscovery = opts.authorizePaidDiscovery === true;
  const authorizeExaAgentLive = opts.authorizeExaAgentLive === true;
  const forceMockOnly = opts.forceMockOnly !== false && !authorizePaidDiscovery;
  const segmentIdHint = opts.segmentId || null;

  return {
    id: EXA_LIVE_DISCOVERY_PROVIDER_BRIDGE_V1,
    isLive: authorizePaidDiscovery && authorizeExaAgentLive && !forceMockOnly,
    async search({ queries, hotel, hotelId, segmentId }) {
      const seg = segmentId || segmentIdHint;
      if (!authorizePaidDiscovery || forceMockOnly) {
        return {
          providerCallCount: 0,
          freshRetrievals: 0,
          cacheHits: 0,
          costUsd: 0,
          hits: [],
          mode: "BLOCKED_PAID",
          blockedReason: "PAID_PROVIDER_REQUIRES_FOUNDER_AUTHORIZATION",
          readiness: inspectExaLiveDiscoveryReadiness({
            hotel: { ...hotel, hotelId },
            segmentId: seg,
            effort,
            maxCostDollars,
            authorizeExaAgentLive,
          }),
        };
      }

      const routing = resolveExaAgentRoutingForSegment(seg, {
        stage: "DISCOVERY",
      });
      if (!routing.paidExaAllowed || !routing.discovery) {
        return {
          providerCallCount: 0,
          freshRetrievals: 0,
          cacheHits: 0,
          costUsd: 0,
          hits: [],
          mode: "BLOCKED_UNSUPPORTED_AGENT_FAMILY",
          blockedReason:
            routing.blockReason || "AGENT_FAMILY_UNSUPPORTED_FOR_SEGMENT",
          agentRouting: buildExaOperationRoutingMetadata({
            hotel: { ...hotel, hotelId },
            segmentId: seg,
            stage: "DISCOVERY",
          }),
        };
      }

      const contextBlock = [
        hotelContextBlock({ ...hotel, hotelId }, seg),
        `Agent family: ${routing.agentName}`,
        `Routing support: ${routing.support}`,
        "",
        "QUERY FAMILY (from segment adapter):",
        ...(queries || [])
          .slice(0, 8)
          .map((q, i) => `${i + 1}. [${q.intent || q.id || "q"}] ${text(q.query).slice(0, 240)}`),
      ].join("\n");

      let createPayloadOverride = null;
      let contractIdOverride = null;
      if (routing.agentFamily === EXA_AGENT_FAMILY.ASSOCIATION_EVENTS) {
        const pack = buildExaEventsAgentCreatePayload({
          mode: "discovery",
          contextBlock,
          effort,
          maxCostDollars,
        });
        createPayloadOverride = pack.body;
        contractIdOverride = pack.meta.contractId;
      } else if (routing.agentFamily === EXA_AGENT_FAMILY.SCALING_COMPANIES) {
        const pack = buildExaAgentCreatePayload({
          mode: "discovery",
          contextBlock,
          effort,
          maxCostDollars,
        });
        createPayloadOverride = pack;
        contractIdOverride = routing.discovery.contractId;
      } else if (routing.agentFamily === EXA_AGENT_FAMILY.PREFERRED_LODGING) {
        const pack = buildPreferredLodgingExaAgentCreatePayload({
          mode: "discovery",
          contextBlock,
          effort,
          maxCostDollars,
          hotel: { ...hotel, hotelId },
        });
        createPayloadOverride = pack.body;
        contractIdOverride =
          routing.discovery.contractId || EXA_PREFERRED_LODGING_DISCOVERY_V1;
      }

      const result = await runExaAgentResearch({
        mode: "discovery",
        contextBlock,
        effort,
        maxCostDollars,
        authorizeExaAgentLive,
        forceMockOnly: false,
        createPayloadOverride,
        contractIdOverride,
      });

      if (!result.ok) {
        return {
          providerCallCount: result.status?.startsWith("refused") ? 0 : 1,
          freshRetrievals: 0,
          cacheHits: 0,
          costUsd: Number(result.costDollars?.total || 0),
          hits: [],
          mode: "PROVIDER_ERROR",
          blockedReason: result.status || result.reason || "EXA_AGENT_FAILED",
          providerRunId: result.runId || null,
          error: result.error || null,
        };
      }

      const structured = result.structured || {};
      const rows =
        structured.candidates ||
        structured.events ||
        structured.companies ||
        structured.results ||
        [];
      const hits = (Array.isArray(rows) ? rows : []).map((row, i) => {
        const company =
          row.companyIdentity && typeof row.companyIdentity === "object"
            ? row.companyIdentity
            : null;
        const urls = [
          row.url,
          row.sourceUrl,
          ...(row.sourceUrls || []),
          ...(row.evidenceUrls || []),
          ...((row.supportingSources || []).map((s) => s?.url || s?.sourceUrl)),
          ...((row.supportingEvidence || []).map((s) => s?.url || s?.sourceUrl)),
          ...((row.evidenceClaims || []).map((s) => s?.url || s?.sourceUrl)),
          company?.website,
          company?.officialWebsite,
        ].filter((u) => /^https?:\/\//i.test(String(u || "")));
        const organizationName =
          text(
            row.organizationName ||
              row.organizingEntity ||
              row.corporateOrAssociationOwner ||
              row.companyName ||
              row.associationName ||
              company?.canonicalName ||
              company?.legalOrTradingName ||
              row.name ||
              ""
          ) || null;
        const title =
          text(
            row.title ||
              row.verifiedEventName ||
              row.eventIdentity ||
              row.eventName ||
              row.verifiedBusinessActivity ||
              row.businessTrigger ||
              organizationName ||
              ""
          ) || null;
        return {
          title,
          organizationName,
          url: urls[0] || null,
          evidenceUrls: [...new Set(urls)],
          excerpt: text(
            row.summary ||
              row.excerpt ||
              row.rationale ||
              row.rankScoreExplain ||
              row.hotelDemandHypothesis ||
              row.hotelRelevanceHypothesis ||
              ""
          ).slice(0, 400),
          queryId: queries?.[i % Math.max(queries.length, 1)]?.id || null,
          intent: queries?.[i % Math.max(queries.length, 1)]?.intent || null,
          rawSchemaHints: {
            demandFamily: row.demandFamily || row.growthSignalType || null,
            discoveryDisposition: row.discoveryDisposition || null,
          },
        };
      });

      return {
        providerCallCount: 1,
        freshRetrievals: 1,
        cacheHits: 0,
        costUsd: Number(result.costDollars?.total || 0),
        hits,
        mode: "FRESH_RETRIEVAL",
        providerRunId: result.runId || null,
        contractId: result.contractId || contractIdOverride,
      };
    },
  };
}

export default {
  EXA_LIVE_DISCOVERY_PROVIDER_BRIDGE_V1,
  inspectExaLiveDiscoveryReadiness,
  createExaLiveDiscoveryProvider,
};
