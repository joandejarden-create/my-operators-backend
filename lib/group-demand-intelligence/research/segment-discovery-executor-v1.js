/**
 * Canonical segment discovery executor V1.
 *
 * Resolves hotels via GDI config + HPC (never ADP 31-matrix).
 * Builds segment-specific queries, invokes an injectable provider adapter,
 * extracts Stage A candidates with evidence provenance.
 *
 * Live paid providers require authorizePaidDiscovery=true.
 * Fixture / historical providers never report as live retrieval.
 */

import { createHash } from "node:crypto";
import { resolveHotelGdiProfileForResearch } from "../hotel-gdi-profile-resolution-v1.js";
import { buildSegmentDiscoveryQueries } from "./segment-discovery-adapters-v1.js";

export const SEGMENT_DISCOVERY_EXECUTOR_V1 = "gdi-segment-discovery-executor-v1";

/** Canonical FullE2E discovery execution statuses (may map to existing enums). */
export const DISCOVERY_EXECUTION_STATUS = Object.freeze({
  EXECUTOR_NOT_IMPLEMENTED: "EXECUTOR_NOT_IMPLEMENTED",
  EXECUTOR_READY: "EXECUTOR_READY",
  EXECUTION_BLOCKED: "EXECUTION_BLOCKED",
  EXECUTION_PARTIAL: "EXECUTION_PARTIAL",
  DISCOVERY_EXECUTED: "DISCOVERY_EXECUTED",
  DISCOVERY_COMPLETED: "DISCOVERY_COMPLETED",
  QUALIFICATION_PARTIAL: "QUALIFICATION_PARTIAL",
  QUALIFICATION_COMPLETED: "QUALIFICATION_COMPLETED",
});

function sha12(s) {
  return createHash("sha1").update(String(s || "")).digest("hex").slice(0, 12);
}

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function hasHttp(url) {
  return /^https?:\/\//i.test(String(url || ""));
}

/**
 * Deterministic fixture provider for CI — processes query list into labeled
 * fixture hits without network I/O. Never claims live retrieval.
 */
export function createFixtureDiscoveryProvider(opts = {}) {
  const hitFactory =
    opts.hitFactory ||
    ((query, hotel) => {
      const city = text(hotel.city || hotel.demandTerritory?.label || "market");
      return {
        title: `Fixture demand signal — ${query.intent || query.segmentId} — ${city}`,
        url: `https://example.invalid/fixture/${query.id || sha12(query.query)}`,
        excerpt: `Deterministic fixture hit for query: ${text(query.query).slice(0, 160)}`,
        fixture: true,
      };
    });
  return {
    id: "fixture_discovery_provider_v1",
    isLive: false,
    async search({ queries, hotel }) {
      const hits = [];
      for (const q of queries || []) {
        const hit = hitFactory(q, hotel);
        if (hit) hits.push({ ...hit, queryId: q.id, intent: q.intent, query: q.query });
      }
      return {
        providerCallCount: 0,
        freshRetrievals: 0,
        cacheHits: queries?.length || 0,
        costUsd: 0,
        hits,
        mode: "FIXTURE",
      };
    },
  };
}

/**
 * Historical ledger hit provider — maps prior Exa rows into provider hits.
 * Marks retrieval as HISTORICAL_REPLAY (not live).
 */
export function createHistoricalHitProvider(rows = []) {
  return {
    id: "historical_ledger_provider_v1",
    isLive: false,
    async search({ queries }) {
      const hits = (rows || [])
        .map((row) => {
          const urls = [
            ...(row.originalSourceUrls || []),
            ...(row.evidenceUrls || []),
            row.sourceUrl,
            row.url,
          ].filter(hasHttp);
          return {
            title:
              text(row.title || row.programmeName || row.organizationName) ||
              "historical_candidate",
            url: urls[0] || null,
            excerpt: text(row.excerpt || row.summary || "").slice(0, 400),
            organizationName: row.organizationName || row.orgName || null,
            candidateId: row.candidateId || null,
            historical: true,
            queryId: queries?.[0]?.id || null,
            evidenceUrls: urls,
          };
        })
        .filter((h) => h.organizationName || h.title);
      return {
        providerCallCount: 0,
        freshRetrievals: 0,
        cacheHits: hits.length,
        costUsd: 0,
        hits,
        mode: "HISTORICAL_REPLAY",
      };
    },
  };
}

/**
 * Paid-blocked provider — records that live retrieval was requested but blocked.
 */
export function createBlockedPaidProvider() {
  return {
    id: "blocked_paid_provider_v1",
    isLive: false,
    async search() {
      return {
        providerCallCount: 0,
        freshRetrievals: 0,
        cacheHits: 0,
        costUsd: 0,
        hits: [],
        mode: "BLOCKED_PAID",
        blockedReason: "PAID_PROVIDER_REQUIRES_FOUNDER_AUTHORIZATION",
      };
    },
  };
}

function extractCandidatesFromHits({ hits, hotelId, segmentId, retrievalMode }) {
  const byKey = new Map();
  const exclusions = [];
  for (const hit of hits || []) {
    const org = text(hit.organizationName || hit.title);
    if (!org) {
      exclusions.push({ reason: "MISSING_ORG_OR_TITLE", hit });
      continue;
    }
    const urls = [
      hit.url,
      ...(hit.evidenceUrls || []),
    ].filter(hasHttp);
    const key = org.toLowerCase().replace(/[^a-z0-9]+/g, " ").trim();
    const candidateId =
      hit.candidateId || `disc_${sha12(`${hotelId}|${segmentId}|${key}`)}`;
    const isFixtureUrl = /example\.invalid/i.test(urls[0] || "");
    const hasSourceEvidence = urls.length > 0 && !isFixtureUrl;
    if (!byKey.has(key)) {
      byKey.set(key, {
        candidateId,
        hotelId,
        segmentId,
        organizationName: hit.organizationName || null,
        title: hit.title || null,
        stage: "A_DISCOVERY",
        sourceFamily: retrievalMode,
        evidenceUrls: urls,
        hasSourceEvidence,
        unsupportedNameOnly: !hasSourceEvidence,
        fixtureOnly: Boolean(hit.fixture) || isFixtureUrl,
        historical: Boolean(hit.historical),
        provenance: {
          queryId: hit.queryId || null,
          intent: hit.intent || null,
          retrievalMode,
          excerpt: text(hit.excerpt).slice(0, 280) || null,
        },
      });
    } else if (hasSourceEvidence && !byKey.get(key).hasSourceEvidence) {
      byKey.set(key, {
        ...byKey.get(key),
        evidenceUrls: urls,
        hasSourceEvidence: true,
        unsupportedNameOnly: false,
      });
    }
  }
  return {
    candidates: [...byKey.values()],
    exclusions,
  };
}

/**
 * Main entry — run segment discovery for one hotel×segment.
 */
export async function runSegmentDiscoveryExecutor(raw = {}) {
  const startedAt = new Date().toISOString();
  const hotelId = text(raw.hotelId);
  const segmentId = text(raw.segmentId);
  const authorizePaidDiscovery = raw.authorizePaidDiscovery === true;
  const skipHpc = raw.skipHpc !== false; // default skip live HPC for offline/CI

  if (!hotelId || !segmentId) {
    return {
      ok: false,
      version: SEGMENT_DISCOVERY_EXECUTOR_V1,
      classification: DISCOVERY_EXECUTION_STATUS.EXECUTOR_NOT_IMPLEMENTED,
      executionStatus: DISCOVERY_EXECUTION_STATUS.EXECUTOR_NOT_IMPLEMENTED,
      stopReason: "INVALID_ARGS",
      executorInvoked: true,
      providerCalls: 0,
      queryCount: 0,
      candidates: [],
      startedAt,
      finishedAt: new Date().toISOString(),
    };
  }

  // Resolve hotel via GDI config / HPC — never ADP 31-matrix.
  let resolved;
  try {
    resolved = await resolveHotelGdiProfileForResearch(hotelId, {
      skipHpc,
      preferLive: false,
    });
  } catch (err) {
    resolved = { ok: false, error: String(err?.message || err) };
  }
  const hotel =
    resolved?.hotel ||
    resolved?.profile ||
    (resolved?.ok === false ? null : resolved);

  if (!hotel || typeof hotel !== "object") {
    return {
      ok: false,
      version: SEGMENT_DISCOVERY_EXECUTOR_V1,
      hotelId,
      segmentId,
      classification: DISCOVERY_EXECUTION_STATUS.EXECUTION_BLOCKED,
      executionStatus: DISCOVERY_EXECUTION_STATUS.EXECUTION_BLOCKED,
      stopReason: "HOTEL_PROFILE_UNRESOLVED",
      executorInvoked: true,
      providerCalls: 0,
      queryCount: 0,
      candidates: [],
      profileResolution: resolved?.resolution || resolved || null,
      startedAt,
      finishedAt: new Date().toISOString(),
    };
  }
  if (!hotel.hotelId) hotel.hotelId = hotelId;
  if (!hotel.city && hotel.demandTerritory?.label) {
    // best-effort city from territory label for query builders
    hotel.city = String(hotel.demandTerritory.label).split(/[/,]/)[0].trim();
  }

  const built = buildSegmentDiscoveryQueries(segmentId, hotel);
  if (built.readiness === "EXECUTOR_NOT_IMPLEMENTED" || !built.queries?.length) {
    return {
      ok: false,
      version: SEGMENT_DISCOVERY_EXECUTOR_V1,
      hotelId,
      segmentId,
      hotel: { hotelId, displayName: hotel.displayName, city: hotel.city, country: hotel.country },
      classification: DISCOVERY_EXECUTION_STATUS.EXECUTOR_NOT_IMPLEMENTED,
      executionStatus: DISCOVERY_EXECUTION_STATUS.EXECUTOR_NOT_IMPLEMENTED,
      stopReason: "NO_SEGMENT_QUERIES",
      executorInvoked: true,
      providerCalls: 0,
      queryCount: 0,
      queries: [],
      builder: built.builder,
      candidates: [],
      startedAt,
      finishedAt: new Date().toISOString(),
    };
  }

  // Provider selection
  let provider = raw.provider || null;
  let providerSelection = "injected";
  if (!provider) {
    if (Array.isArray(raw.historicalRows) && raw.historicalRows.length) {
      provider = createHistoricalHitProvider(raw.historicalRows);
      providerSelection = "historical_ledger";
    } else if (authorizePaidDiscovery) {
      // Caller must inject a live provider; we refuse to invent Exa calls here.
      provider = createBlockedPaidProvider();
      providerSelection = "paid_requested_but_no_live_adapter_injected";
    } else if (raw.useFixtureProvider === true) {
      provider = createFixtureDiscoveryProvider(raw.fixtureOpts || {});
      providerSelection = "fixture";
    } else {
      provider = createBlockedPaidProvider();
      providerSelection = "zero_cost_blocked_awaiting_auth_or_fixture";
    }
  }

  const searchResult = await provider.search({
    queries: built.queries,
    hotel,
    hotelId,
    segmentId,
    authorizePaidDiscovery,
  });

  const retrievalMode = searchResult.mode || (provider.isLive ? "FRESH_RETRIEVAL" : "NON_LIVE");
  const { candidates, exclusions } = extractCandidatesFromHits({
    hits: searchResult.hits || [],
    hotelId,
    segmentId,
    retrievalMode,
  });

  const providerCalls = Number(searchResult.providerCallCount || 0);
  const freshRetrievals = Number(searchResult.freshRetrievals || 0);
  const liveExecuted = provider.isLive === true && freshRetrievals > 0;

  let executionStatus = DISCOVERY_EXECUTION_STATUS.EXECUTOR_READY;
  if (searchResult.blockedReason || retrievalMode === "BLOCKED_PAID") {
    executionStatus = DISCOVERY_EXECUTION_STATUS.EXECUTION_BLOCKED;
  } else if (liveExecuted) {
    // Live retrieval ran — executed even if extraction yielded zero (honest empty).
    executionStatus = DISCOVERY_EXECUTION_STATUS.DISCOVERY_EXECUTED;
  } else if (retrievalMode === "FIXTURE" && candidates.length) {
    executionStatus = DISCOVERY_EXECUTION_STATUS.EXECUTION_PARTIAL;
  } else if (retrievalMode === "HISTORICAL_REPLAY" && candidates.length) {
    executionStatus = DISCOVERY_EXECUTION_STATUS.EXECUTION_PARTIAL;
  } else if (!candidates.length && !liveExecuted) {
    executionStatus = DISCOVERY_EXECUTION_STATUS.EXECUTION_BLOCKED;
  }

  // Never claim DISCOVERY_COMPLETED from fixture/historical alone.
  const discoveryCompleted = false;

  return {
    ok: true,
    version: SEGMENT_DISCOVERY_EXECUTOR_V1,
    hotelId,
    segmentId,
    hotel: {
      hotelId,
      displayName: hotel.displayName || null,
      city: hotel.city || null,
      country: hotel.country || null,
    },
    builder: built.builder,
    queries: built.queries,
    queryCount: built.queries.length,
    providerId: provider.id,
    providerSelection,
    providerCalls,
    freshRetrievals,
    cacheHits: Number(searchResult.cacheHits || 0),
    rawResults: (searchResult.hits || []).length,
    extractedCandidates: candidates.length,
    validatedCandidates: candidates.filter((c) => c.hasSourceEvidence).length,
    deduplicatedCandidates: candidates.length,
    persistedCandidates: 0,
    researchCostUsd: Number(searchResult.costUsd || 0),
    candidates,
    exclusions: exclusions.map((e) => ({ reason: e.reason })),
    classification: executionStatus,
    executionStatus,
    discoveryCompleted,
    retrievalMode,
    stopReason:
      searchResult.blockedReason ||
      (liveExecuted
        ? "LIVE_PROVIDER_EXECUTED"
        : retrievalMode === "FIXTURE"
          ? "FIXTURE_PROVIDER_ONLY"
          : retrievalMode === "HISTORICAL_REPLAY"
            ? "HISTORICAL_REPLAY_ONLY"
            : "PAID_PROVIDER_REQUIRES_FOUNDER_AUTHORIZATION"),
    executorInvoked: true,
    profileResolution: resolved?.resolution || null,
    startedAt,
    finishedAt: new Date().toISOString(),
  };
}

export default runSegmentDiscoveryExecutor;
