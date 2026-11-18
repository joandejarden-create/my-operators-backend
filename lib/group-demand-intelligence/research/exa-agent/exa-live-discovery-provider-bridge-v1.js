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
import { observeProviderCall } from "../../observability/gdi-execution-run-context-v1.js";
import {
  GDI_PROVIDER_IDS,
  PROVIDER_CALL_STATUS,
} from "../../observability/gdi-execution-ledger-store-v1.js";
import { buildUpgradedExaDiscoveryContextBlock } from "./gdi-fourteen-segment-exa-mission-upgrades-v1.js";

export const EXA_LIVE_DISCOVERY_PROVIDER_BRIDGE_V1 =
  "gdi-exa-live-discovery-provider-bridge-v1";

function observeExaDiscoveryResult(seg, payload) {
  try {
    if (!payload) return;
    const refused = String(payload.blockedReason || payload.status || "").includes(
      "refused",
    );
    if (refused && !(payload.providerCallCount > 0)) return;
    observeProviderCall({
      providerId: GDI_PROVIDER_IDS.EXA_AGENT,
      segmentId: seg,
      researchStage: "DISCOVERY",
      operation: "DISCOVERY",
      status:
        payload.mode === "FRESH_RETRIEVAL" || payload.providerCallCount > 0
          ? payload.mode === "PROVIDER_ERROR"
            ? PROVIDER_CALL_STATUS.FAILED
            : PROVIDER_CALL_STATUS.COMPLETED
          : refused
            ? PROVIDER_CALL_STATUS.BLOCKED
            : PROVIDER_CALL_STATUS.SKIPPED,
      providerRequestId: payload.providerRunId || null,
      actualCostUsd: Number(payload.costUsd || 0) || null,
      candidateCount: Array.isArray(payload.hits) ? payload.hits.length : 0,
      resultCount: Array.isArray(payload.hits) ? payload.hits.length : 0,
      errorCode: payload.error || payload.blockedReason || null,
      skipReason: refused ? payload.blockedReason || payload.status : null,
    });
  } catch {
    /* observability must never break research */
  }
}

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function segmentTargetingOverlay(segmentId = "") {
  const sid = String(segmentId || "");
  if (sid === "professional_event_planners") {
    return [
      "SEGMENT TARGETING — Professional Event Planners:",
      "- Discover actual planning companies, DMCs, meeting planners, and event-management businesses.",
      "- Conferences/galas are evidence of planner activity — identify the planner/PCO/DMC; do not substitute the conference brand as the commercial target.",
      "- Prefer firms that influence hotel room blocks, catering, or venue selection.",
    ].join("\n");
  }
  if (sid === "local_catering_day_meetings") {
    return [
      "SEGMENT TARGETING — Local Catering & Day Meetings:",
      "- Target local corporate functions, training, association CHAPTER meetings, receptions, private events, and recurring day programmes.",
      "- Do NOT predominantly return large destination / national / multi-day conferences (route those to Associations / Education).",
      "- Guestroom demand is optional; meeting-space and catering revenue are primary.",
    ].join("\n");
  }
  if (sid === "preferred_lodging_partnerships") {
    return [
      "SEGMENT TARGETING — Preferred Lodging Partnerships:",
      "- Prioritize venues/businesses that can refer hotel guestrooms or groups.",
      "- Do not reject venues merely for offering meeting or catering space — complementary lodging partnerships are in-scope.",
    ].join("\n");
  }
  return "";
}

function hotelContextBlock(hotel = {}, segmentId = "", opts = {}) {
  const overlay = segmentTargetingOverlay(segmentId);
  const upgraded = buildUpgradedExaDiscoveryContextBlock(hotel, segmentId, opts);
  // Preserve legacy segment overlays + LATAM language note on top of upgraded mission/catchment.
  return [upgraded, overlay || null, "Languages: Spanish and English sources both in-scope when market is LATAM/MX/DR."]
    .filter((line) => line != null && line !== "")
    .join("\n");
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
  researchTrack = null,
  researchTrackContextBlock = null,
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
    hotelContextBlock(hotel, segmentId, {
      researchTrack,
      researchTrackContextBlock,
    }),
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
        segmentId,
      });
      contractId = routing.discovery.contractId;
    } else if (routing.agentFamily === EXA_AGENT_FAMILY.SCALING_COMPANIES) {
      const body = buildExaAgentCreatePayload({
        mode: "discovery",
        contextBlock,
        effort,
        maxCostDollars,
        segmentId,
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
      hotelCatchmentPresent: /HOTEL CATCHMENT/i.test(query),
      segmentMissionPresent: /SEGMENT MISSION/i.test(query),
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
  const researchTrack = opts.researchTrack || null;
  const researchTrackContextBlock = opts.researchTrackContextBlock || null;

  return {
    id: EXA_LIVE_DISCOVERY_PROVIDER_BRIDGE_V1,
    isLive: authorizePaidDiscovery && authorizeExaAgentLive && !forceMockOnly,
    async search({ queries, hotel, hotelId, segmentId }) {
      const seg = segmentId || segmentIdHint;
      const contextOpts = { researchTrack, researchTrackContextBlock };
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
            ...contextOpts,
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
        hotelContextBlock({ ...hotel, hotelId }, seg, contextOpts),
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
          segmentId: seg,
        });
        createPayloadOverride = pack.body;
        contractIdOverride = pack.meta.contractId;
      } else if (routing.agentFamily === EXA_AGENT_FAMILY.SCALING_COMPANIES) {
        const pack = buildExaAgentCreatePayload({
          mode: "discovery",
          contextBlock,
          effort,
          maxCostDollars,
          segmentId: seg,
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
          segmentId: seg,
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
        segmentId: seg,
      });

      if (!result.ok) {
        const errPayload = {
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
        observeExaDiscoveryResult(seg, errPayload);
        return errPayload;
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

      const okPayload = {
        providerCallCount: 1,
        freshRetrievals: 1,
        cacheHits: 0,
        costUsd: Number(result.costDollars?.total || 0),
        hits,
        mode: "FRESH_RETRIEVAL",
        providerRunId: result.runId || null,
        contractId: result.contractId || contractIdOverride,
      };
      observeExaDiscoveryResult(seg, okPayload);
      return okPayload;
    },
  };
}

export default {
  EXA_LIVE_DISCOVERY_PROVIDER_BRIDGE_V1,
  inspectExaLiveDiscoveryReadiness,
  createExaLiveDiscoveryProvider,
};
