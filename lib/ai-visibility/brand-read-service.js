/**
 * Brand AI Visibility read service — portfolio + detail modules.
 * Reads via storage abstraction only. No provider calls. Authorization required by caller.
 */

import { ACCESS_DEPTH } from "./access-depth.js";
import {
  AVAILABILITY,
  classifyMetricAvailability,
  normalizeMetricKey,
} from "./availability-states.js";
import { toBenchmarkSafeEntityView } from "./authorized-reads.js";
import { resolveAiIntelligenceAccess } from "./authorization.js";
import { filterEvidenceByAccessDepth } from "./evidence-access.js";
import { buildEvidenceDescriptors, mapRecommendationRoleToBrandStatus } from "./role-copy.js";
import { loadPeerSetConfig, resolvePeerSetMembership, peerSetBrandNamesById, PEER_SET_ID_V2 } from "./peer-sets.js";
import { buildBrandDetailIntelligence, buildExecutiveFindings, executiveFindingsToInsightBoxes } from "./executive-finding-engine.js";
import {
  DEFAULT_AI_VISIBILITY_PROVIDER,
  formatProviderLabel,
  isAllProvidersSelector,
  KNOWN_AI_VISIBILITY_PROVIDER_IDS,
  normalizeProviderId,
  pickScaffoldDataProvider,
  providersMatch,
  resolveProviderId,
  resolveProviderReadContext,
} from "./provider-dimension.js";
import {
  buildCrossProviderPresenceIntelligence,
  buildSourceOverlapBetweenProviders,
} from "./cross-provider-presence.js";
import {
  buildLanguageFilterContract,
  listLanguagesFromMonitoringRecords,
  normalizeLanguage,
  recordMatchesLanguage,
  requireSupportedLanguage,
  resolveReadLanguage,
  resolveRecordLanguage,
} from "./language-dimension.js";
import { compareTrendObservations, TREND_LANGUAGE_MATCH_REQUIRED, computeComparablePresenceDelta } from "./trend-comparability.js";
import {
  loadObservationsFromBatchSummary,
  resolveEvidenceObservationLanguage,
} from "./cohort-observations.js";
import { buildMonitoringFreshness } from "./monitoring-freshness.js";
import {
  EVIDENCE_RESOLUTION_MODES,
  buildEvidenceResolutionIndex,
  resolveEvidenceForRun,
} from "./evidence-resolution.js";
import { applySignalPublicationGate } from "./signal-architecture/publication-gate.js";
import {
  computeAiPresenceRate,
  computeRecommendationShare,
  computeRecommendationRate,
  computeTop3RecommendationRate,
  computeFirstRecommendationRate,
  computeQuestionsWon,
  computeQuestionsMissing,
  computeCitationRate,
} from "./metrics.js";
import { computeBrandQuestionMetrics } from "./portfolio-question-metrics.js";
import {
  computeBrandCrossProviderQuestionsMissing,
  loadObservationsByProviderForCohort,
} from "./cross-provider-questions.js";
import { CLIENT_METRIC_DEFINITIONS } from "./client-metric-definitions.js";
import { filterFixtureContaminatedSources } from "./fixture-domain-guard.js";
import {
  buildDiscoverabilityDetailPlaceholder,
  buildOpenAiDiscoverabilityDetailPlaceholder,
} from "./future-discoverability.js";
import { buildProviderPresencePanel } from "./provider-presence-panel.js";
import {
  buildBrandDetailInsightBoxes,
  crossProviderFromProviderPresencePanel,
} from "./brand-executive-insights.js";
import {
  buildDiscoverabilityProductPayload,
  resolveOwnedDomainsForBrand,
} from "./brand-website-wiring.js";
import { computeResponseCitationRates } from "./citation-intelligence.js";
import {
  buildPeerPresentSubjectMissing,
  buildPromptFamilyMissingRollup,
  groupQuestionsMissingWatchlist,
} from "./questions-missing-intelligence.js";
import { buildEvidenceFootprint } from "./evidence-footprint.js";
import {
  buildCitedSourceIntelligence,
  providerEvidenceAssociationMap,
} from "./cited-source-intelligence.js";
import { normalizeAiVisibilityViewerContext } from "./viewer-context.js";
import {
  isMultiSlotBatchSummary,
  isWave1MultiMetricSnapshot,
  listLanguagesFromMultiSlotSummary,
  listMatchingSlots,
  projectMultiSlotSummaryForRead,
  runMatchesSlotFilter,
  deriveRunSlotKey,
} from "./multi-slot-geography.js";
import { filterSummariesForClientPublication } from "./validation/publication-gate.js";
import { enrichRowWithPromptOriginFromLibrary } from "./prompt-provenance.js";

/** Lazy import avoids brand-read-service ↔ hotel-decision-visibility cycle. */
async function loadHdvPayload(args) {
  const { getHotelDecisionVisibilityPayload } = await import("./hotel-decision-visibility.js");
  return getHotelDecisionVisibilityPayload(args);
}

export const BRAND_READ_SERVICE_VERSION = "ai_visibility_brand_read_v1";

export const HEADLINE_GEOGRAPHIES = Object.freeze([
  { key: "Global", geographyScope: "Global", commercialRegion: null, country: null },
  { key: "North America", geographyScope: "Region", commercialRegion: "North America", country: null },
  { key: "CALA", geographyScope: "Region", commercialRegion: "CALA", country: null },
  { key: "Europe", geographyScope: "Region", commercialRegion: "Europe", country: null },
  { key: "MEA", geographyScope: "Region", commercialRegion: "Middle East & Africa", country: null },
  { key: "Asia Pacific", geographyScope: "Region", commercialRegion: "APAC", country: null },
]);

/** Stable display order for Regional / Market View (and related headline loops). */
export const HEADLINE_GEOGRAPHY_ORDER = Object.freeze(
  HEADLINE_GEOGRAPHIES.map((g) => g.key)
);

export function headlineGeographyByKey(key) {
  const want = String(key || "").trim().toLowerCase();
  return HEADLINE_GEOGRAPHIES.find((g) => String(g.key).toLowerCase() === want) || null;
}

/**
 * Parse UI/API geography query into cohort filter.
 * @param {{ geography?: string, geographyScope?: string, region?: string, commercialRegion?: string, country?: string }} q
 */
export function parseGeographyQuery(q = {}) {
  if (q.geographyScope) {
    return {
      geographyScope: q.geographyScope,
      commercialRegion: q.commercialRegion || q.region || null,
      country: q.country || null,
      key: q.geography || q.commercialRegion || q.region || q.geographyScope,
    };
  }
  const raw = String(q.geography || q.region || "").trim();
  if (!raw || /^portfolio$/i.test(raw)) {
    return { geographyScope: null, commercialRegion: null, country: null, key: "all" };
  }
  if (/^global$/i.test(raw)) {
    return { geographyScope: "Global", commercialRegion: null, country: null, key: "Global" };
  }
  if (/^cala$/i.test(raw)) {
    return { geographyScope: "Region", commercialRegion: "CALA", country: null, key: "CALA" };
  }
  if (/^europe$/i.test(raw)) {
    return { geographyScope: "Region", commercialRegion: "Europe", country: null, key: "Europe" };
  }
  if (/^north\s*america$/i.test(raw)) {
    return {
      geographyScope: "Region",
      commercialRegion: "North America",
      country: null,
      key: "North America",
    };
  }
  if (/^(mea|middle\s*east(\s*&\s*|\s+and\s+)?africa)$/i.test(raw)) {
    return {
      geographyScope: "Region",
      commercialRegion: "Middle East & Africa",
      country: null,
      key: "MEA",
    };
  }
  if (/^(apac|asia\s*pacific)$/i.test(raw)) {
    return {
      geographyScope: "Region",
      commercialRegion: "APAC",
      country: null,
      key: "Asia Pacific",
    };
  }
  // Country scope
  return {
    geographyScope: "Country",
    commercialRegion: q.commercialRegion || null,
    country: raw,
    key: raw,
  };
}

/**
 * Parse language query. Empty → unresolved (caller resolves via available languages).
 * @param {{ language?: string }} q
 */
export function parseLanguageQuery(q = {}) {
  if (q.language == null || String(q.language).trim() === "") {
    return { language: null, explicit: false };
  }
  const req = requireSupportedLanguage(q.language);
  if (!req.ok) {
    return { language: null, explicit: true, error: req.reasonCode, message: req.message };
  }
  return { language: req.language, explicit: true };
}

/**
 * Data-driven available languages for a provider × geography from completed summaries.
 * Multi-slot baselines: derive from matched slot languages (CALA_EN / CALA_ES), never
 * only the parent summary language stamp (often forced to "en" when includeAllLanguages).
 */
export async function listAvailableAiVisibilityLanguages({
  store,
  provider,
  geography,
  geographyFilter = null,
} = {}) {
  const geo = geographyFilter || parseGeographyQuery({ geography });
  const summaries = await findMatchingSummaries(store, geo, provider, {
    language: null,
    includeAllLanguages: true,
  });
  const fromSlots = new Set();
  const legacyRecords = [];
  for (const s of summaries) {
    if (isMultiSlotBatchSummary(s)) {
      for (const lang of listLanguagesFromMultiSlotSummary(s, geo)) {
        fromSlots.add(lang);
      }
      continue;
    }
    legacyRecords.push({
      status: s.status,
      language: s.language ?? s.cohort?.language,
    });
  }
  const available = [
    ...new Set([
      ...fromSlots,
      ...listLanguagesFromMonitoringRecords(legacyRecords),
    ]),
  ].sort();
  return {
    provider: resolveProviderId(provider),
    geography: geo,
    availableLanguages: available,
    filterContract: buildLanguageFilterContract(available),
  };
}

/**
 * Resolve effective monitoring language for a read.
 * Never silently remaps es→en when Spanish is unmonitored.
 */
export async function resolveMonitoringLanguageForRead({
  store,
  provider,
  geography,
  geographyFilter = null,
  language: languageArg = null,
} = {}) {
  const geo = geographyFilter || parseGeographyQuery({ geography });
  const avail = await listAvailableAiVisibilityLanguages({
    store,
    provider,
    geographyFilter: geo,
  });
  const resolved = resolveReadLanguage({
    requested: languageArg,
    availableLanguages: avail.availableLanguages,
  });
  return {
    ...resolved,
    geography: geo,
    availableLanguages: avail.availableLanguages,
    filterContract: avail.filterContract,
    SILENT_LANGUAGE_FALLBACK: false,
  };
}

function matchesGeography(rowGeo, filter) {
  if (!filter.geographyScope) return true;
  const scope = String(rowGeo.geographyScope || "").toLowerCase();
  const want = String(filter.geographyScope).toLowerCase();
  if (scope !== want) return false;
  if (filter.commercialRegion) {
    if (
      String(rowGeo.commercialRegion || rowGeo.region || "").toLowerCase() !==
      String(filter.commercialRegion).toLowerCase()
    ) {
      return false;
    }
  }
  if (filter.country) {
    if (String(rowGeo.country || rowGeo.countryName || "").toLowerCase() !== String(filter.country).toLowerCase()) {
      return false;
    }
  }
  return true;
}

function latestByBatchDate(rows) {
  return [...rows].sort((a, b) =>
    String(b.batchDate || b.completedAt || b.savedAt || "").localeCompare(
      String(a.batchDate || a.completedAt || a.savedAt || "")
    )
  );
}

/**
 * @param {object} store
 * @param {string} entityId
 * @param {object} geoFilter
 * @param {string} provider
 */
async function loadEntityMetricMap(
  store,
  entityId,
  geoFilter,
  provider,
  language = "en",
  opts = {}
) {
  const snaps = await store.listMetricSnapshots({
    entityId,
    geographyScope: geoFilter.geographyScope || undefined,
    region: geoFilter.commercialRegion || undefined,
    provider,
    language: language || "en",
  });
  let filtered = snaps.filter((s) =>
    matchesGeography(
      {
        geographyScope: s.geographyScope,
        commercialRegion: s.commercialRegion || s.region,
        country: s.country,
      },
      geoFilter
    )
  );

  // Four-provider baseline snapshots are tagged wave1_multi / language multi.
  // When a multi-slot summary already matched this geography, accept those snaps.
  if (!filtered.length && opts.allowWave1MultiSnapshots) {
    const broad = await store.listMetricSnapshots({
      entityId,
      provider,
    });
    filtered = broad.filter((s) => isWave1MultiMetricSnapshot(s));
  }

  const byMetric = {};
  const periods = new Set();
  for (const snap of latestByBatchDate(filtered)) {
    const key = normalizeMetricKey(snap.metric);
    periods.add(snap.batchId || snap.batchDate);
    if (!byMetric[key]) {
      byMetric[key] = snap;
    }
  }
  return { byMetric, snaps: filtered, periodCount: periods.size, monitored: filtered.length > 0 };
}

function rateFromSnap(snap) {
  if (!snap) return null;
  if (typeof snap.value === "number") return snap.value;
  return null;
}

/** Questions Won / Missing are counts. With cohort size: "40% (2)". */
function formatQuestionsCountDisplay(count, denominator) {
  if (count == null || !Number.isFinite(Number(count))) return null;
  const n = Math.round(Number(count));
  if (typeof denominator === "number" && Number.isFinite(denominator) && denominator > 0) {
    // Never render >100% — if count exceeds denom, show count only (invariant failure signal).
    if (n > denominator) return String(n);
    const pct = Math.round((n / denominator) * 1000) / 10;
    const pctLabel = `${pct}`.replace(/\.0$/, "");
    return `${pctLabel}% (${n})`;
  }
  return String(n);
}

function questionsCountMetric(count, denominator, monitored) {
  if (count == null) {
    return {
      availability: monitored ? AVAILABILITY.UNAVAILABLE : AVAILABILITY.NOT_MONITORED,
      value: null,
      display: monitored ? "Unavailable" : "Not Monitored",
      unit: "count",
      denominator: denominator ?? null,
    };
  }
  const n = Math.round(Number(count));
  return {
    availability: AVAILABILITY.OBSERVED,
    value: n,
    display: formatQuestionsCountDisplay(n, denominator),
    unit: "count",
    denominator: typeof denominator === "number" ? denominator : null,
  };
}

function buildDelta(currentSnap, priorSnap) {
  if (!currentSnap || !priorSnap) {
    return {
      availability: AVAILABILITY.INSUFFICIENT_HISTORY,
      display: "Insufficient History",
      absolute: null,
      value: null,
    };
  }
  if (currentSnap.batchId && priorSnap.batchId && currentSnap.batchId === priorSnap.batchId) {
    return {
      availability: AVAILABILITY.INSUFFICIENT_HISTORY,
      display: "Insufficient History",
      absolute: null,
      value: null,
    };
  }
  const cur = rateFromSnap(currentSnap);
  const prior = rateFromSnap(priorSnap);
  const geoKey =
    currentSnap.commercialRegion || currentSnap.geographyScope || null;
  const deltaResult = computeComparablePresenceDelta(
    {
      value: prior,
      provider: priorSnap.provider,
      geographyKey: priorSnap.commercialRegion || priorSnap.geographyScope || geoKey,
      language: priorSnap.language || currentSnap.language || "en",
      metric: "aiPresenceRate",
      metricVersion: priorSnap.metricVersion || null,
      peerSetVersion: priorSnap.peerSetVersion || null,
      promptFamily: priorSnap.promptFamily || null,
      methodology: priorSnap.methodology || priorSnap.metricVersion || null,
    },
    {
      value: cur,
      provider: currentSnap.provider,
      geographyKey: currentSnap.commercialRegion || currentSnap.geographyScope || geoKey,
      language: currentSnap.language || priorSnap.language || "en",
      metric: "aiPresenceRate",
      metricVersion: currentSnap.metricVersion || null,
      peerSetVersion: currentSnap.peerSetVersion || null,
      promptFamily: currentSnap.promptFamily || null,
      methodology: currentSnap.methodology || currentSnap.metricVersion || null,
    }
  );
  if (!deltaResult.ok) {
    return {
      availability: deltaResult.status,
      display: deltaResult.display,
      absolute: null,
      value: null,
      reasonCode: deltaResult.reasonCode,
      INVALID_DELTA_BLOCKED: true,
    };
  }
  return {
    availability: AVAILABILITY.OBSERVED,
    absolute: deltaResult.delta,
    value: deltaResult.delta,
    absoluteDeltaPp: deltaResult.deltaPp,
    display: deltaResult.display,
    priorValue: prior,
    priorBatchDate: priorSnap.batchDate || null,
    unit: "rate_points",
    INVALID_DELTA_BLOCKED: false,
  };
}

/** Per-store in-flight cache so parallel geo/brand resolves share one listBatchSummaries read. */
const summaryListCacheByStore = new WeakMap();

export async function findMatchingSummaries(store, geoFilter, provider, languageOpts = {}) {
  if (typeof store.listBatchSummaries !== "function") return [];
  let byKey = summaryListCacheByStore.get(store);
  if (!byKey) {
    byKey = new Map();
    summaryListCacheByStore.set(store, byKey);
  }
  const wantLanguage = languageOpts.includeAllLanguages
    ? null
    : normalizeLanguage(languageOpts.language) ||
      (languageOpts.language === null || languageOpts.language === undefined
        ? undefined
        : null);
  const cacheKey = [
    resolveProviderId(provider),
    geoFilter?.geographyScope || "",
    geoFilter?.commercialRegion || "",
    geoFilter?.country || "",
    languageOpts.includeAllLanguages ? "*" : wantLanguage || "en",
  ].join("|");
  if (!byKey.has(cacheKey)) {
    byKey.set(
      cacheKey,
      (async () => {
        const rows = await store.listBatchSummaries({
          provider,
          geographyScope: geoFilter.geographyScope || undefined,
          commercialRegion: geoFilter.commercialRegion || undefined,
          country: geoFilter.country || undefined,
          // When includeAllLanguages, omit language filter at store; filter in JS for legacy.
          language: languageOpts.includeAllLanguages
            ? undefined
            : wantLanguage || "en",
        });
        const out = [];
        for (const s of rows) {
          if (!(s.status === "completed" || s.status === "partial")) continue;

          if (isMultiSlotBatchSummary(s)) {
            const matchedSlots = listMatchingSlots(
              s,
              geoFilter,
              languageOpts.includeAllLanguages
                ? { includeAllLanguages: true }
                : { language: wantLanguage || "en" }
            );
            if (!matchedSlots.length) continue;
            out.push(
              projectMultiSlotSummaryForRead(s, matchedSlots, wantLanguage || "en")
            );
            continue;
          }

          if (
            !matchesGeography(
              {
                geographyScope: s.cohort?.geographyScope,
                commercialRegion: s.cohort?.commercialRegion,
                country: s.cohort?.country,
              },
              geoFilter
            )
          ) {
            continue;
          }
          if (languageOpts.includeAllLanguages) {
            out.push(s);
            continue;
          }
          const lang = resolveRecordLanguage(
            { language: s.language ?? s.cohort?.language },
            { treatMissingAsEn: true }
          );
          const want = wantLanguage || "en";
          if (lang === want) out.push(s);
        }
        const gated = filterSummariesForClientPublication(out);
        const publishable = gated.publishable;
        Object.defineProperty(publishable, "_publicationGate", {
          value: {
            latestBatchFailedValidation: gated.latestBatchFailedValidation,
            latestExcludedBatchId: gated.latestExcludedBatchId,
            fallbackBatchId: gated.fallbackBatchId,
            excludedCount: gated.excluded.length,
          },
          enumerable: false,
          configurable: true,
        });
        return publishable;
      })()
    );
  }
  return byKey.get(cacheKey);
}

function entityMetricsFromSummary(summary, entityId) {
  const byEntity = summary?.metrics?.byEntity || {};
  if (byEntity[entityId]) return byEntity[entityId];
  for (const row of Object.values(byEntity)) {
    if (row?.id === entityId || row?.entityId === entityId) return row;
  }
  return null;
}

function competitiveRankFromSummary(summary, entityId) {
  const peers = summary?.metrics?.competitivePosition?.peers || [];
  const hit = peers.find((p) => p.entityId === entityId);
  if (!hit) return null;
  return {
    rank: hit.rank ?? hit.position ?? null,
    peerCount: peers.length,
    presence: hit.presence ?? hit.aiPresenceRate ?? null,
    name: hit.name || null,
  };
}

/**
 * Was this entity in the governed monitored/peer universe for a completed batch?
 * Snapshot absence alone does not mean out-of-scope.
 */
export function entityInMonitoredUniverse(summary, entityId, peerSetEntityIds = []) {
  if (!entityId || !summary) return false;
  if (entityMetricsFromSummary(summary, entityId)) return true;
  const peers = summary?.metrics?.competitivePosition?.peers || [];
  if (peers.some((p) => p.entityId === entityId)) return true;
  const fromPeerSet = Array.isArray(peerSetEntityIds) ? peerSetEntityIds : [];
  if (fromPeerSet.includes(entityId)) return true;
  const cohortIds = summary?.cohort?.entityIds || summary?.entityUniverseIds || [];
  if (Array.isArray(cohortIds) && cohortIds.includes(entityId)) return true;
  return false;
}

export const MONITORING_STATE = Object.freeze({
  OBSERVED: "observed",
  ZERO: "zero",
  NO_BATCH: "no_batch",
  VALIDATION_UNAVAILABLE: "validation_unavailable",
  OUT_OF_SCOPE: "out_of_scope",
  UNAVAILABLE: "unavailable",
});

/** Per-signal client gate — unavailable metrics stay null (never coerced to 0). */
function gateClientMetric(metricKey, metric) {
  return applySignalPublicationGate(metricKey, metric);
}

function monitoringMessage(code, geographyKey, providerId = null) {
  const geo = geographyKey || "this geography";
  const providerBit = providerId
    ? ` (${formatProviderLabel(providerId)})`
    : "";
  if (code === MONITORING_STATE.VALIDATION_UNAVAILABLE) {
    return "Validated monitoring data is not currently available.";
  }
  if (code === MONITORING_STATE.NO_BATCH) {
    return `No monitoring data is available for this brand in ${geo} yet${providerBit}.`;
  }
  if (code === MONITORING_STATE.OUT_OF_SCOPE) {
    return `This brand has not yet been included in the ${geo} monitoring cohort${providerBit}.`;
  }
  if (code === MONITORING_STATE.UNAVAILABLE) {
    return "Monitoring data could not be loaded.";
  }
  return null;
}

/**
 * Resolve monitoring availability for a brand — geography.
 * Order: batch exists → entity in universe → snapshot/summary metrics → zero vs not monitored.
 */
export async function resolveBrandGeographyMonitoringState(args = {}) {
  const {
    store,
    brandId,
    geoFilter,
    provider: providerArg = DEFAULT_AI_VISIBILITY_PROVIDER,
    language: languageArg = "en",
  } = args;
  const provider = resolveProviderId(providerArg);
  const language = normalizeLanguage(languageArg) || "en";
  const geographyKey = geoFilter?.key || geoFilter?.commercialRegion || geoFilter?.geographyScope;

  let summaries;
  try {
    summaries =
      args.summaries ||
      (await findMatchingSummaries(store, geoFilter, provider, { language }));
  } catch (err) {
    return {
      code: MONITORING_STATE.UNAVAILABLE,
      monitored: false,
      inScope: false,
      message: monitoringMessage(MONITORING_STATE.UNAVAILABLE, geographyKey, provider),
      error: err.message,
      language,
    };
  }

  if (!summaries.length) {
    const gate = summaries._publicationGate || null;
    const validationBlocked = Boolean(gate && gate.excludedCount > 0);
    return {
      code: validationBlocked
        ? MONITORING_STATE.VALIDATION_UNAVAILABLE
        : MONITORING_STATE.NO_BATCH,
      monitored: false,
      inScope: false,
      message: monitoringMessage(
        validationBlocked
          ? MONITORING_STATE.VALIDATION_UNAVAILABLE
          : MONITORING_STATE.NO_BATCH,
        geographyKey,
        provider
      ),
      latestSummary: null,
      metricPack: { byMetric: {}, snaps: [], periodCount: 0, monitored: false },
      fromSummary: null,
      rank: null,
      presenceVal: null,
      shareVal: null,
      firstVal: null,
      citationVal: null,
      questionsWon: null,
      questionsMissing: null,
      promptDenominator: null,
      language,
      LATEST_BATCH_FAILED_VALIDATION: gate?.latestBatchFailedValidation || false,
      FALLBACK_VALIDATED_BATCH_ID: gate?.fallbackBatchId || null,
    };
  }

  const latestSummary = summaries[0];
  const publicationGate = summaries._publicationGate || null;
  let peerSetEntityIds = [];
  try {
    const peerSetId = latestSummary?.peerSet?.peerSetId;
    if (peerSetId) {
      const membership = resolvePeerSetMembership({
        peerSetId,
        commercialRegion: geoFilter?.commercialRegion || null,
      });
      peerSetEntityIds = membership?.entityIds || [];
    }
  } catch {
    peerSetEntityIds = [];
  }

  const inScope = entityInMonitoredUniverse(latestSummary, brandId, peerSetEntityIds);
  const fromSummary = entityMetricsFromSummary(latestSummary, brandId);
  const rank = competitiveRankFromSummary(latestSummary, brandId);

  let metricPack;
  try {
    metricPack = await loadEntityMetricMap(store, brandId, geoFilter, provider, language, {
      allowWave1MultiSnapshots: isMultiSlotBatchSummary(latestSummary),
    });
  } catch (err) {
    return {
      code: MONITORING_STATE.UNAVAILABLE,
      monitored: false,
      inScope,
      message: monitoringMessage(MONITORING_STATE.UNAVAILABLE, geographyKey),
      error: err.message,
      latestSummary,
      language,
    };
  }

  if (!inScope && !metricPack.monitored) {
    return {
      code: MONITORING_STATE.OUT_OF_SCOPE,
      monitored: false,
      inScope: false,
      message: monitoringMessage(MONITORING_STATE.OUT_OF_SCOPE, geographyKey),
      latestSummary,
      metricPack,
      fromSummary: null,
      rank: null,
      presenceVal: null,
      shareVal: null,
      firstVal: null,
      citationVal: null,
      questionsWon: null,
      questionsMissing: null,
      promptDenominator: latestSummary?.cohort?.promptCount ?? null,
    };
  }

  let promptDenominator =
    fromSummary?.presenceDetail?.denominator ??
    fromSummary?.denominatorPresence ??
    latestSummary?.cohort?.promptCount ??
    null;

  let presenceVal =
    rateFromSnap(metricPack.byMetric.aiPresenceRate) ??
    (typeof fromSummary?.presence === "number" ? fromSummary.presence : null);
  let shareVal =
    rateFromSnap(metricPack.byMetric.recommendationShare) ??
    (typeof fromSummary?.recommendationShare === "number"
      ? fromSummary.recommendationShare
      : null);
  let recommendationRateVal =
    rateFromSnap(metricPack.byMetric.recommendationRate) ??
    (typeof fromSummary?.recommendationRate === "number"
      ? fromSummary.recommendationRate
      : null);
  let top3RecommendationRateVal =
    rateFromSnap(metricPack.byMetric.top3RecommendationRate) ??
    (typeof fromSummary?.top3RecommendationRate === "number"
      ? fromSummary.top3RecommendationRate
      : null);
  let firstVal =
    rateFromSnap(metricPack.byMetric.firstRecommendationRate) ??
    (typeof fromSummary?.firstRecommendationRate === "number"
      ? fromSummary.firstRecommendationRate
      : null);
  let citationVal =
    rateFromSnap(metricPack.byMetric.citationRate) ??
    (typeof fromSummary?.citationRate === "number" ? fromSummary.citationRate : null);

  let questionsWon =
    fromSummary?.questionsWon != null ? fromSummary.questionsWon : null;
  let questionsMissing =
    fromSummary?.questionsMissing != null ? fromSummary.questionsMissing : null;
  let questionsPresent = null;
  let slotObservationCount = null;
  let slotMetricScope = null;

  // Multi-slot parent summaries store 84-prompt aggregates. When geography/language
  // matched specific slots, recompute KPIs from slot-filtered observations only.
  // Never pair slot-scoped MONITORED_N with parent-aggregate questionsMissing.
  const matchedSlots = latestSummary?._matchedSlotKeys || null;
  const parentAggregateScope =
    latestSummary?._metricScope === "wave1_parent_aggregate" ||
    Boolean(matchedSlots?.length);
  const needsSlotScope =
    parentAggregateScope ||
    recommendationRateVal == null ||
    top3RecommendationRateVal == null ||
    shareVal == null ||
    firstVal == null;

  if (latestSummary && needsSlotScope) {
    try {
      const { observations } = await loadObservationsFromBatchSummary(store, latestSummary, {
        matchedSlotKeys: matchedSlots?.length ? matchedSlots : undefined,
        language: language || undefined,
      });
      slotObservationCount = observations.length;
      if (observations.length) {
        const promptIds = [...new Set(observations.map((o) => o.promptId).filter(Boolean))];
        promptDenominator = promptIds.length;
        const q = computeBrandQuestionMetrics(observations, brandId);
        questionsWon = q.questionsWonCount;
        questionsMissing = q.questionsMissingCount;
        questionsPresent = q.questionsPresentCount;
        // Unique-prompt Presence so Present + Missing = Monitored and rate matches.
        presenceVal =
          typeof q.presenceRate === "number"
            ? q.presenceRate
            : computeAiPresenceRate(observations, brandId).value;
        shareVal = computeRecommendationShare(observations, brandId).value;
        recommendationRateVal = computeRecommendationRate(observations, brandId).value;
        top3RecommendationRateVal = computeTop3RecommendationRate(observations, brandId).value;
        firstVal = computeFirstRecommendationRate(observations, brandId).value;
        citationVal = computeCitationRate(observations, brandId).value;
        slotMetricScope = matchedSlots?.length ? "slot_filtered" : "batch_observations";
        // Prefer slot-scoped values over parent wave1_multi snapshots.
        metricPack = {
          ...metricPack,
          monitored: true,
          periodCount: Math.max(metricPack.periodCount || 0, 1),
          snaps: metricPack.snaps || [],
          byMetric: {
            ...metricPack.byMetric,
            aiPresenceRate: {
              ...(metricPack.byMetric.aiPresenceRate || {}),
              value: presenceVal,
              entityId: brandId,
              geographyScope: geoFilter?.geographyScope || null,
              commercialRegion: geoFilter?.commercialRegion || null,
              batchId: latestSummary.batchId,
              metricScope: slotMetricScope,
            },
          },
        };
      } else if (parentAggregateScope) {
        // Honest partial: do not keep parent Missing with slot denominator.
        questionsMissing = null;
        questionsWon = null;
        questionsPresent = null;
        slotMetricScope = "partial_data_no_slot_observations";
      }
    } catch (err) {
      if (typeof console !== "undefined" && console.warn) {
        console.warn("[brand-read] slot-scoped metric recompute failed:", err.message);
      }
      if (parentAggregateScope) {
        questionsMissing = null;
        questionsWon = null;
        questionsPresent = null;
        slotMetricScope = "partial_data_recompute_error";
      }
    }
  }

  // Final guard: never allow Missing / Present to exceed Monitored when all three exist.
  if (
    typeof promptDenominator === "number" &&
    promptDenominator > 0 &&
    typeof questionsMissing === "number" &&
    questionsMissing > promptDenominator
  ) {
    if (typeof console !== "undefined" && console.warn) {
      console.warn("[brand-read] questionsMissing exceeds denominator — clearing unsafe parent value", {
        questionsMissing,
        promptDenominator,
        brandId,
        batchId: latestSummary?.batchId,
      });
    }
    questionsMissing = null;
    questionsPresent = null;
  }

  // Guard: question counts must never exceed eligible unique prompts for display rates.
  if (
    typeof promptDenominator === "number" &&
    promptDenominator > 0 &&
    typeof questionsWon === "number" &&
    questionsWon > promptDenominator
  ) {
    if (typeof console !== "undefined" && console.warn) {
      console.warn(
        "[brand-read] questionsWon exceeds denominator after recompute",
        { questionsWon, promptDenominator, brandId, batchId: latestSummary?.batchId }
      );
    }
  }

  // In-scope entity with no appearances: valid zero / full Questions Missing.
  if (inScope && presenceVal == null && !metricPack.monitored) {
    presenceVal = 0;
    if (shareVal == null) shareVal = 0;
    if (recommendationRateVal == null) recommendationRateVal = 0;
    if (top3RecommendationRateVal == null) top3RecommendationRateVal = 0;
    if (firstVal == null) firstVal = 0;
    if (questionsWon == null) questionsWon = 0;
    if (questionsMissing == null && typeof promptDenominator === "number") {
      questionsMissing = promptDenominator;
      questionsPresent = 0;
    }
  }

  // Derive Present from Monitored − Missing when both known and Present unset.
  if (
    questionsPresent == null &&
    typeof promptDenominator === "number" &&
    typeof questionsMissing === "number" &&
    questionsMissing <= promptDenominator
  ) {
    questionsPresent = promptDenominator - questionsMissing;
  }

  const monitored = true;
  const code =
    presenceVal === 0 && (metricPack.monitored || inScope)
      ? MONITORING_STATE.ZERO
      : MONITORING_STATE.OBSERVED;

  return {
    code,
    monitored,
    inScope: true,
    message: null,
    latestSummary,
    priorSummary: summaries[1] || null,
    metricPack,
    fromSummary,
    rank,
    presenceVal,
    shareVal,
    recommendationRateVal,
    top3RecommendationRateVal,
    firstVal,
    citationVal,
    questionsWon,
    questionsPresent,
    questionsMissing,
    promptDenominator,
    slotObservationCount,
    slotMetricScope,
    language,
    validationStatus: "PASS",
    validatedAt: latestSummary?.validatedAt || null,
    batchId: latestSummary?.batchId || null,
    publishable: true,
    LATEST_BATCH_FAILED_VALIDATION: publicationGate?.latestBatchFailedValidation || false,
    FALLBACK_VALIDATED_BATCH_ID:
      publicationGate?.latestBatchFailedValidation
        ? latestSummary?.batchId || null
        : null,
  };
}

/**
 * Resolve store scaffold when UI selector is All Providers (derived — never provider id "all").
 * @param {{ store: object, provider: string, effectiveGeo: object }} args
 */
async function resolveAllProvidersReadScaffold(args = {}) {
  const provider = resolveProviderId(args.provider);
  const effectiveGeo = args.effectiveGeo || {};
  const providerContext = await resolveProviderReadContext({
    store: args.store,
    requestedProvider: provider,
    geographyScope: effectiveGeo.geographyScope || null,
    commercialRegion: effectiveGeo.commercialRegion || null,
    country: effectiveGeo.country || null,
  });
  const allProvidersMode = isAllProvidersSelector(provider);
  const dataProvider = allProvidersMode
    ? pickScaffoldDataProvider(providerContext.availableProviders)
    : provider;
  return { provider, providerContext, allProvidersMode, dataProvider };
}

/**
 * Portfolio list for entitled brands only.
 */
export async function getBrandPortfolioPayload(args = {}) {
  const {
    dealalityUser,
    viewerContext,
    entitlementGraph,
    store,
    provider: providerArg = DEFAULT_AI_VISIBILITY_PROVIDER,
    geography,
    language: languageArg = null,
    brandNamesById = {},
  } = args;

  const viewer = viewerContext || normalizeAiVisibilityViewerContext(dealalityUser);
  const geo = parseGeographyQuery({ geography, ...args });
  const effectiveGeo = geo.geographyScope
    ? geo
    : { geographyScope: "Region", commercialRegion: "CALA", country: null, key: "CALA" };
  const provider = resolveProviderId(providerArg);
  const providerContext = await resolveProviderReadContext({
    store,
    requestedProvider: provider,
    geographyScope: geo.geographyScope || null,
    commercialRegion: geo.commercialRegion || null,
    country: geo.country || null,
  });
  const allProvidersMode = isAllProvidersSelector(provider);
  /** Provider-specific scaffold for reads; All Providers never becomes a store provider id. */
  const dataProvider = allProvidersMode
    ? pickScaffoldDataProvider(providerContext.availableProviders)
    : provider;
  const brandIds = entitlementGraph?.entitledBrandIds || [];

  const langResolved = await resolveMonitoringLanguageForRead({
    store,
    provider: dataProvider,
    geographyFilter: effectiveGeo,
    language: languageArg,
  });
  if (!langResolved.ok) {
    return {
      ok: false,
      reasonCode: langResolved.reasonCode,
      message: langResolved.message,
      availableLanguages: langResolved.availableLanguages,
      languageFilterContract: langResolved.filterContract,
      SILENT_LANGUAGE_FALLBACK: false,
      provider,
      providerLabel: providerContext.providerLabel,
      providerMode: providerContext.providerMode,
      availableProviders: providerContext.availableProviders,
      providerSelectorOptions: providerContext.providerSelectorOptions,
      providerHasCompletedData: providerContext.providerHasCompletedData,
      ALL_PROVIDERS_DERIVED: allProvidersMode,
      ALL_PROVIDERS_RUN: false,
      NO_SILENT_PROVIDER_FALLBACK: true,
      geography: geo,
      brands: [],
      brandReadServiceVersion: BRAND_READ_SERVICE_VERSION,
    };
  }
  const language = langResolved.language;
  if (langResolved.status === "not_monitored" && languageArg) {
    return {
      ok: true,
      productSurface: "AI Visibility",
      view: "portfolio",
      brands: [],
      availability: AVAILABILITY.NOT_MONITORED,
      availabilityReason: MONITORING_STATE.NO_BATCH,
      availabilityMessage: `No ${language === "es" ? "Spanish" : "English"} monitoring data is available in ${effectiveGeo.key} yet.`,
      language,
      availableLanguages: langResolved.availableLanguages,
      languageFilterContract: langResolved.filterContract,
      SILENT_LANGUAGE_FALLBACK: false,
      provider,
      providerLabel: providerContext.providerLabel,
      providerMode: providerContext.providerMode,
      availableProviders: providerContext.availableProviders,
      providerSelectorOptions: providerContext.providerSelectorOptions,
      providerHasCompletedData: providerContext.providerHasCompletedData,
      ALL_PROVIDERS_DERIVED: allProvidersMode,
      ALL_PROVIDERS_RUN: false,
      NO_SILENT_PROVIDER_FALLBACK: true,
      geography: geo,
      portfolioCompositeScore: null,
      opportunityQueue: futureOpportunityQueue(),
      brandReadServiceVersion: BRAND_READ_SERVICE_VERSION,
    };
  }

  if (!brandIds.length) {
    return {
      ok: true,
      productSurface: "AI Visibility",
      view: "portfolio",
      brands: [],
      emptyReason: "NO_ENTITLED_BRANDS",
      language: language || "en",
      availableLanguages: langResolved.availableLanguages,
      languageFilterContract: langResolved.filterContract,
      SILENT_LANGUAGE_FALLBACK: false,
      provider,
      providerLabel: providerContext.providerLabel,
      providerMode: providerContext.providerMode,
      availableProviders: providerContext.availableProviders,
      providerSelectorOptions: providerContext.providerSelectorOptions,
      providerHasCompletedData: providerContext.providerHasCompletedData,
      ALL_PROVIDERS_DERIVED: allProvidersMode,
      ALL_PROVIDERS_RUN: false,
      NO_SILENT_PROVIDER_FALLBACK: true,
      geography: geo,
      opportunityQueue: futureOpportunityQueue(),
      brandReadServiceVersion: BRAND_READ_SERVICE_VERSION,
    };
  }

  const deepBrandIds = brandIds.filter((brandId) => {
    const access = resolveAiIntelligenceAccess({
      viewerContext: viewer,
      subject: { subjectType: "brand", subjectEntityId: brandId },
      entitlementGraph,
    });
    return access.allowed && access.accessDepth === ACCESS_DEPTH.DEEP;
  });

  let brands = await Promise.all(
    deepBrandIds.map(async (brandId) => {
      const geoFilter = geo.geographyScope
        ? geo
        : { geographyScope: "Region", commercialRegion: "CALA", country: null, key: "CALA" };

      let mon;
      try {
        mon = await resolveBrandGeographyMonitoringState({
          store,
          brandId,
          geoFilter,
          provider: dataProvider,
          language: language || "en",
        });
        if (!geo.geographyScope && mon.code === MONITORING_STATE.NO_BATCH) {
          const europeFilter =
            headlineGeographyByKey("Europe") || {
              key: "Europe",
              geographyScope: "Region",
              commercialRegion: "Europe",
              country: null,
            };
          const europe = await resolveBrandGeographyMonitoringState({
            store,
            brandId,
            geoFilter: europeFilter,
            provider: dataProvider,
            language: language || "en",
          });
          if (europe.monitored) mon = europe;
        }
      } catch {
        return {
          brandId,
          brandName: brandNamesById[brandId] || null,
          availability: AVAILABILITY.UNAVAILABLE,
          availabilityReason: MONITORING_STATE.UNAVAILABLE,
          aiPresence: classifyMetricAvailability({ monitored: false, unavailable: true }),
        };
      }

      const fromSummary = mon.fromSummary;
      const rank = mon.rank;
      const presenceVal = mon.presenceVal;

      return {
        brandId,
        brandName:
          brandNamesById[brandId] ||
          mon.metricPack?.byMetric?.aiPresenceRate?.entityName ||
          fromSummary?.name ||
          null,
        accessDepth: ACCESS_DEPTH.DEEP,
        availabilityReason: mon.code,
        availabilityMessage: mon.message,
        aiPresence: gateClientMetric(
          "aiPresence",
          classifyMetricAvailability({
            monitored: !!mon.monitored,
            value: presenceVal,
          })
        ),
        competitivePosition: gateClientMetric(
          "competitivePosition",
          rank
            ? {
                availability: AVAILABILITY.OBSERVED,
                rank: rank.rank,
                peerCount: rank.peerCount,
                display: rank.rank != null ? `#${rank.rank} of ${rank.peerCount}` : "—",
              }
            : {
                availability: mon.monitored ? AVAILABILITY.UNAVAILABLE : AVAILABILITY.NOT_MONITORED,
                rank: null,
                peerCount: null,
                display: mon.monitored ? "Unavailable" : "Not Monitored",
              }
        ),
        questionsMissing: gateClientMetric(
          "questionsMissing",
          questionsCountMetric(mon.questionsMissing, mon.promptDenominator, !!mon.monitored)
        ),
        latestMonitoring:
          mon.latestSummary?.completedAt ||
          mon.metricPack?.byMetric?.aiPresenceRate?.batchDate ||
          null,
        geographyUsed: geo.geographyScope
          ? geo
          : {
              geographyScope:
                mon.metricPack?.snaps?.[0]?.geographyScope || geoFilter.geographyScope,
              commercialRegion:
                mon.metricPack?.snaps?.[0]?.commercialRegion || geoFilter.commercialRegion,
            },
      };
    })
  );

  // All Providers: overlay Presence from pure derived cross-provider measures (no scaffold KPI).
  let portfolioCrossProvider = null;
  if (allProvidersMode && brands.length) {
    const measured = providerContext.availableProviders || [];
    const derivedBrands = [];
    for (const b of brands) {
      const providerRows = [];
      for (const p of measured) {
        const pid = p.id || p;
        try {
          const mon = await resolveBrandGeographyMonitoringState({
            store,
            brandId: b.brandId,
            geoFilter: effectiveGeo,
            provider: pid,
            language: language || "en",
          });
          providerRows.push({
            provider: pid,
            monitored: !!mon.monitored,
            availability: mon.monitored ? "observed" : "not_monitored",
            presenceRate: typeof mon.presenceVal === "number" ? mon.presenceVal : null,
            geography: effectiveGeo.key,
            language: language || "en",
            monitoringWindow:
              mon.latestSummary?.monitoringPeriodId ||
              mon.latestSummary?.periodId ||
              (mon.latestSummary?.completedAt
                ? String(mon.latestSummary.completedAt).slice(0, 10)
                : null) ||
              mon.latestSummary?.batchId ||
              null,
            promptCohortKey: [
              effectiveGeo.key,
              language || "en",
              mon.latestSummary?.peerSet?.peerSetId ||
                mon.latestSummary?.peerSetId ||
                "peers_uu_collection_lifestyle_owner_decision_v2",
              mon.latestSummary?.metricVersion || "ai_visibility_metrics_v1",
            ].join("|"),
          });
        } catch {
          providerRows.push({
            provider: pid,
            monitored: false,
            availability: "not_monitored",
            presenceRate: null,
            geography: effectiveGeo.key,
            language: language || "en",
          });
        }
      }
      const xp = buildCrossProviderPresenceIntelligence({
        entityId: b.brandId,
        geography: effectiveGeo.key,
        language: language || "en",
        providers: providerRows,
      });
      if (!portfolioCrossProvider) portfolioCrossProvider = xp;
      const avg = xp.CROSS_PROVIDER_AVERAGE_OBSERVED_PRESENCE;
      const presenceMetric =
        xp.NOT_COMPARABLE || avg == null
          ? classifyMetricAvailability({
              monitored: true,
              notComparable: true,
            })
          : classifyMetricAvailability({
              monitored: true,
              success: true,
              value: avg,
              denominator: 1,
            });
      derivedBrands.push({
        brandId: b.brandId,
        brandName: b.brandName,
        accessDepth: b.accessDepth,
        availabilityReason: b.availabilityReason,
        availabilityMessage: b.availabilityMessage,
        aiPresence: gateClientMetric("aiPresence", presenceMetric),
        competitivePosition: gateClientMetric("competitivePosition", {
          availability: AVAILABILITY.NOT_COMPARABLE,
          rank: null,
          peerCount: null,
          display: "Use a specific provider for peer rank",
        }),
        questionsMissing: b.questionsMissing,
        latestMonitoring: b.latestMonitoring,
        geographyUsed: b.geographyUsed,
        crossProviderPresence: xp,
        ALL_PROVIDERS_DERIVED: true,
        SYNTHETIC_PRIMARY_PROVIDER_DEPENDENCY_REMOVED: true,
      });
    }
    brands = derivedBrands;
  }

  return {
    ok: true,
    productSurface: "AI Visibility",
    view: "portfolio",
    brands,
    provider,
    providerLabel: providerContext.providerLabel,
    providerMode: providerContext.providerMode || "PROVIDER_SPECIFIC",
    availableProviders: providerContext.availableProviders,
    providerSelectorOptions: providerContext.providerSelectorOptions,
    providerHasCompletedData: providerContext.providerHasCompletedData,
    ALL_PROVIDERS_DERIVED: allProvidersMode,
    ALL_PROVIDERS_RUN: false,
    ALL_PROVIDERS_PROVIDER_RECORD: false,
    SYNTHETIC_PRIMARY_PROVIDER_DEPENDENCY_REMOVED: allProvidersMode === true,
    scaffoldProvider: allProvidersMode ? dataProvider : null,
    crossProviderPresence: portfolioCrossProvider,
    NO_SILENT_PROVIDER_FALLBACK: true,
    geography: geo,
    language: language || "en",
    availableLanguages: langResolved.availableLanguages,
    languageFilterContract: langResolved.filterContract,
    SILENT_LANGUAGE_FALLBACK: false,
    portfolioCompositeScore: null,
    opportunityQueue: futureOpportunityQueue(),
    brandReadServiceVersion: BRAND_READ_SERVICE_VERSION,
  };
}

/**
 * Brand detail overview (KPIs + secondary metrics + regional position).
 */
export async function getBrandOverviewPayload(args = {}) {
  const {
    dealalityUser,
    viewerContext,
    entitlementGraph,
    store,
    brandId,
    provider: providerArg = DEFAULT_AI_VISIBILITY_PROVIDER,
    geography,
    language: languageArg = null,
    brandNamesById = {},
    brandBasicsById = {},
  } = args;
  const provider = resolveProviderId(providerArg);

  const viewer = viewerContext || normalizeAiVisibilityViewerContext(dealalityUser);
  const access = resolveAiIntelligenceAccess({
    viewerContext: viewer,
    subject: { subjectType: "brand", subjectEntityId: brandId },
    entitlementGraph,
  });

  if (!access.allowed) {
    return {
      ok: false,
      allowed: false,
      reasonCode: access.reasonCode,
      accessDepth: ACCESS_DEPTH.NONE,
    };
  }

  const geo = parseGeographyQuery({ geography, ...args });
  const effectiveGeo = geo.geographyScope
    ? geo
    : { geographyScope: "Region", commercialRegion: "CALA", country: null, key: "CALA" };

  const providerContext = await resolveProviderReadContext({
    store,
    requestedProvider: provider,
    geographyScope: effectiveGeo.geographyScope || null,
    commercialRegion: effectiveGeo.commercialRegion || null,
    country: effectiveGeo.country || null,
  });
  const allProvidersMode = isAllProvidersSelector(provider);
  /** All Providers is derived — never query store with provider id "all". */
  const dataProvider = allProvidersMode
    ? pickScaffoldDataProvider(providerContext.availableProviders)
    : provider;

  const langResolved = await resolveMonitoringLanguageForRead({
    store,
    provider: dataProvider,
    geographyFilter: effectiveGeo,
    language: languageArg,
  });
  if (!langResolved.ok) {
    return {
      ok: false,
      allowed: true,
      accessDepth: access.accessDepth,
      reasonCode: langResolved.reasonCode,
      message: langResolved.message,
      availableLanguages: langResolved.availableLanguages,
      languageFilterContract: langResolved.filterContract,
      SILENT_LANGUAGE_FALLBACK: false,
      provider,
      providerLabel: providerContext.providerLabel,
      providerMode: providerContext.providerMode,
      ALL_PROVIDERS_DERIVED: allProvidersMode,
    };
  }
  const language = langResolved.language;
  if (langResolved.status === "not_monitored" && languageArg) {
    return {
      ok: true,
      allowed: true,
      accessDepth: access.accessDepth,
      productSurface: "AI Visibility",
      brandId,
      provider,
      providerLabel: providerContext.providerLabel,
      providerMode: providerContext.providerMode,
      ALL_PROVIDERS_DERIVED: allProvidersMode,
      geography: effectiveGeo,
      language,
      availableLanguages: langResolved.availableLanguages,
      languageFilterContract: langResolved.filterContract,
      SILENT_LANGUAGE_FALLBACK: false,
      availability: AVAILABILITY.NOT_MONITORED,
      availabilityReason: MONITORING_STATE.NO_BATCH,
      availabilityMessage: `No ${language === "es" ? "Spanish" : "English"} monitoring data is available for this brand in ${effectiveGeo.key} yet.`,
      kpis: {},
      secondary: {},
      decisionPatterns: null,
      reviewItems: [],
      brandReadServiceVersion: BRAND_READ_SERVICE_VERSION,
    };
  }

  let mon;
  try {
    mon = await resolveBrandGeographyMonitoringState({
      store,
      brandId,
      geoFilter: effectiveGeo,
      provider: dataProvider,
      language: language || "en",
    });
  } catch (err) {
    return {
      ok: false,
      allowed: true,
      accessDepth: access.accessDepth,
      availability: AVAILABILITY.UNAVAILABLE,
      availabilityReason: MONITORING_STATE.UNAVAILABLE,
      availabilityMessage: monitoringMessage(MONITORING_STATE.UNAVAILABLE, effectiveGeo.key),
      error: "store_unavailable",
      message: err.message,
      provider,
      ALL_PROVIDERS_DERIVED: allProvidersMode,
    };
  }

  if (mon.code === MONITORING_STATE.UNAVAILABLE) {
    return {
      ok: false,
      allowed: true,
      accessDepth: access.accessDepth,
      availability: AVAILABILITY.UNAVAILABLE,
      availabilityReason: MONITORING_STATE.UNAVAILABLE,
      availabilityMessage: mon.message,
      error: "store_unavailable",
      message: mon.error || mon.message,
    };
  }

  const metricPack = mon.metricPack || {
    byMetric: {},
    snaps: [],
    periodCount: 0,
    monitored: false,
  };
  const latestSummary = mon.latestSummary || null;
  const priorSummary = mon.priorSummary || null;
  const fromSummary = mon.fromSummary || null;
  const rank = mon.rank || null;
  const monitored = !!mon.monitored;
  const presenceVal = mon.presenceVal;
  const citationVal = mon.citationVal;

  const presenceSnaps = latestByBatchDate(
    (metricPack.snaps || []).filter((s) => normalizeMetricKey(s.metric) === "aiPresenceRate")
  );
  const presenceDelta = buildDelta(presenceSnaps[0], presenceSnaps[1]);

  // Comparative: benchmark-safe KPIs only — no questions missing diagnostics / opportunity path depth
  if (access.accessDepth === ACCESS_DEPTH.COMPARATIVE) {
    return {
      ok: true,
      allowed: true,
      accessDepth: ACCESS_DEPTH.COMPARATIVE,
      productSurface: "AI Visibility",
      brandId,
      brandName: brandNamesById[brandId] || metricPack.byMetric.aiPresenceRate?.entityName || null,
      provider,
      geography: effectiveGeo,
      availabilityReason: mon.code,
      availabilityMessage: mon.message,
      kpis: {
        aiPresence: gateClientMetric(
          "aiPresence",
          classifyMetricAvailability({ monitored, value: presenceVal })
        ),
        competitivePosition: gateClientMetric(
          "competitivePosition",
          rank
            ? {
                availability: AVAILABILITY.OBSERVED,
                rank: rank.rank,
                peerCount: rank.peerCount,
                display: `#${rank.rank} of ${rank.peerCount}`,
                delta: null,
              }
            : {
                availability: monitored ? AVAILABILITY.UNAVAILABLE : AVAILABILITY.NOT_MONITORED,
                display: monitored ? "Unavailable" : "Not Monitored",
                delta: null,
              }
        ),
      },
      secondary: {
        citationRate: {
          availability: AVAILABILITY.PARTIAL,
          value: null,
          display: "Partial",
          readiness: "PARTIAL",
        },
      },
      regionalPosition: [],
      competitiveBenchmarkOnly: true,
      opportunityQueue: futureOpportunityQueue(),
      evidenceStrength: {
        status: AVAILABILITY.FUTURE_READY,
        note: "FUTURE-READY — NO ARBITRARY CONFIDENCE LABELS",
      },
      brandReadServiceVersion: BRAND_READ_SERVICE_VERSION,
    };
  }

  const kpis = {
    aiPresence: gateClientMetric("aiPresence", {
      ...classifyMetricAvailability({ monitored, value: presenceVal }),
      delta: presenceDelta,
      geography: effectiveGeo,
      provider,
    }),
    competitivePosition: gateClientMetric(
      "competitivePosition",
      rank
        ? {
            availability: AVAILABILITY.OBSERVED,
            rank: rank.rank,
            peerCount: rank.peerCount,
            display: `#${rank.rank} of ${rank.peerCount}`,
            delta: null,
            helper: "Rank by AI Presence Rate within the governed peer set.",
          }
        : {
            availability: monitored ? AVAILABILITY.UNAVAILABLE : AVAILABILITY.NOT_MONITORED,
            rank: null,
            peerCount: null,
            display: monitored ? "Unavailable" : "Not Monitored",
            delta: null,
          }
    ),
    questionsMissing: gateClientMetric("questionsMissing", {
      ...questionsCountMetric(mon.questionsMissing, mon.promptDenominator, monitored),
      helper: "Monitored owner questions where your brand was absent. Missing is not automatically a poor fit.",
      delta: null,
    }),
  };
  // Recommendation-dependent metrics intentionally omitted from client KPIs (BLOCKED).

  const secondary = {
    citationRate: {
      ...classifyMetricAvailability({
        monitored,
        value: citationVal,
        partial: true,
      }),
      readiness: "PARTIAL",
    },
  };

  // Decision Patterns + regional presence — parallel (shared file-store cache).
  let decisionPatterns = null;
  let aiVsDealalityContext = null;
  let reviewItems = [];
  const entitledIds = entitlementGraph?.entitledBrandIds || [];
  const [hdvSettled, regionalPosition] = await Promise.all([
    (async () => {
      try {
        return await loadHdvPayload({
          dealalityUser,
          viewerContext: viewer,
          entitlementGraph,
          store,
          provider: dataProvider,
          geography: effectiveGeo.key || geography,
          language: language || "en",
          brandId,
          brandNamesById,
          entitledBrandIds: entitledIds,
          // Skip nested portfolio + per-brand trend (was emptying / timing out this merge).
          includePortfolioOverview: false,
          includeTrendEnrichment: false,
        });
      } catch (err) {
        if (typeof console !== "undefined" && console.warn) {
          console.warn("[brand-read] HDV detail merge skipped:", err.message);
        }
        return null;
      }
    })(),
    Promise.all(
      HEADLINE_GEOGRAPHIES.map(async (g) => {
        const regionMon = await resolveBrandGeographyMonitoringState({
          store,
          brandId,
          geoFilter: g,
          provider: dataProvider,
          language: language || "en",
        });
        return {
          geography: g.key,
          geographyScope: g.geographyScope,
          commercialRegion: g.commercialRegion,
          availabilityReason: regionMon.code,
          aiPresence: classifyMetricAvailability({
            monitored: !!regionMon.monitored,
            value: regionMon.presenceVal,
          }),
          competitivePosition: allProvidersMode
            ? {
                availability: AVAILABILITY.NOT_COMPARABLE,
                rank: null,
                peerCount: null,
                display: "Use a specific provider for peer rank",
              }
            : regionMon.rank
            ? {
                availability: AVAILABILITY.OBSERVED,
                rank: regionMon.rank.rank,
                peerCount: regionMon.rank.peerCount,
                display: `#${regionMon.rank.rank}`,
              }
            : {
                availability: regionMon.monitored
                  ? AVAILABILITY.UNAVAILABLE
                  : AVAILABILITY.NOT_MONITORED,
              display: regionMon.monitored ? "Unavailable" : "Not Monitored",
            },
      };
      })
    ),
  ]);

  const hdv = hdvSettled;
  if (hdv?.ok !== false && hdv?.allowed !== false && hdv) {
    decisionPatterns = {
      ownerIntentCoverage: hdv.ownerIntentCoverage || null,
      topDecisionTerritory: hdv.headline?.topDecisionTerritory || null,
    };
    aiVsDealalityContext = hdv.aiVsDealalityContext || null;
    reviewItems = hdv.reviewItems || [];
  } else if (hdv && typeof console !== "undefined" && console.warn) {
    console.warn(
      "[brand-read] HDV detail merge denied:",
      hdv?.reasonCode || "unknown"
    );
  }

  const discoverabilityBusinessImpact = buildDiscoverabilityDetailPlaceholder();
  const openAiDiscoverability = buildOpenAiDiscoverabilityDetailPlaceholder();

  // Provider Presence panel (all known providers; missing ≠ 0%)
  const measuredProviders = providerContext.availableProviders || [];
  const providerRows = [];
  const providerIds = [
    ...new Set([
      ...KNOWN_AI_VISIBILITY_PROVIDER_IDS,
      ...measuredProviders.map((p) => p.id || p),
    ]),
  ];
  for (const pid of providerIds) {
    try {
      const pMon = await resolveBrandGeographyMonitoringState({
        store,
        brandId,
        geoFilter: effectiveGeo,
        provider: pid,
        language: language || "en",
      });
      const presentN =
        typeof pMon.questionsPresent === "number"
          ? pMon.questionsPresent
          : typeof pMon.presenceVal === "number" && typeof pMon.promptDenominator === "number"
            ? Math.round(pMon.presenceVal * pMon.promptDenominator)
            : null;
      providerRows.push({
        provider: pid,
        monitored: !!pMon.monitored,
        availability: pMon.monitored
          ? pMon.presenceVal === 0
            ? AVAILABILITY.NO_PRESENCE_OBSERVED
            : AVAILABILITY.OBSERVED
          : AVAILABILITY.NOT_MONITORED,
        presenceRate: typeof pMon.presenceVal === "number" ? pMon.presenceVal : null,
        presentN,
        monitoredN: pMon.promptDenominator ?? null,
        questionsMissingN:
          typeof pMon.questionsMissing === "number" ? pMon.questionsMissing : null,
        citationRate: typeof pMon.citationVal === "number" ? pMon.citationVal : null,
        currentPeriod:
          pMon.latestSummary?.monitoringPeriodId ||
          pMon.latestSummary?.periodId ||
          (pMon.latestSummary?.completedAt
            ? String(pMon.latestSummary.completedAt).slice(0, 10)
            : null),
        priorPeriod:
          pMon.priorSummary?.monitoringPeriodId ||
          pMon.priorSummary?.periodId ||
          (pMon.priorSummary?.completedAt
            ? String(pMon.priorSummary.completedAt).slice(0, 10)
            : null),
        priorPresenceRate: (() => {
          const priorSnaps = latestByBatchDate(
            (pMon.metricPack?.snaps || []).filter(
              (s) => normalizeMetricKey(s.metric) === "aiPresenceRate"
            )
          );
          const prior = priorSnaps[1];
          return typeof prior?.value === "number" ? prior.value : null;
        })(),
        geography: effectiveGeo.key,
        language: language || "en",
      });
    } catch (err) {
      if (typeof console !== "undefined" && console.warn) {
        console.warn("[brand-read] provider presence row skipped:", pid, err.message);
      }
      providerRows.push({
        provider: pid,
        monitored: false,
        availability: AVAILABILITY.NOT_MONITORED,
        presenceRate: null,
        geography: effectiveGeo.key,
        language: language || "en",
      });
    }
  }

  const { owned } = resolveOwnedDomainsForBrand(brandId, {
    brandNamesById,
    brandBasicsById,
  });
  // Attach owned citation when owned domains exist (display-layer only; no Presence rescore)
  if (owned.OWNED_DOMAIN_STATUS === "CONFIGURED") {
    for (const row of providerRows) {
      row.ownedDomainStatus = owned.OWNED_DOMAIN_STATUS;
    }
  } else {
    for (const row of providerRows) {
      row.ownedDomainStatus = "MISSING_GOVERNED_SOURCE";
    }
  }

  const providerPresencePanel = buildProviderPresencePanel({
    brandId,
    providers: providerRows,
    currentPeriod: latestSummary?.completedAt || presenceSnaps[0]?.batchDate || null,
    priorPeriod: priorSummary?.completedAt || presenceSnaps[1]?.batchDate || null,
  });

  // All Providers: overlay Presence from derived cross-provider average; peer rank stays provider-specific.
  let overviewCrossProvider = null;
  /** @type {object|null} */
  let crossProviderQuestions = null;
  if (allProvidersMode) {
    overviewCrossProvider = buildCrossProviderPresenceIntelligence({
      entityId: brandId,
      geography: effectiveGeo.key,
      language: language || "en",
      providers: providerRows.map((r) => ({
        provider: r.provider,
        monitored: !!r.monitored,
        availability: r.availability,
        presenceRate: r.presenceRate,
        geography: r.geography || effectiveGeo.key,
        language: r.language || language || "en",
        monitoringWindow: r.currentPeriod || null,
        promptCohortKey: [
          effectiveGeo.key,
          language || "en",
          latestSummary?.peerSet?.peerSetId ||
            latestSummary?.peerSetId ||
            "peers_uu_collection_lifestyle_owner_decision_v2",
          latestSummary?.metricVersion || "ai_visibility_metrics_v1",
        ].join("|"),
      })),
    });
    const avg = overviewCrossProvider.CROSS_PROVIDER_AVERAGE_OBSERVED_PRESENCE;
    const presenceMetric =
      overviewCrossProvider.NOT_COMPARABLE || avg == null
        ? classifyMetricAvailability({
            monitored: true,
            notComparable: true,
          })
        : classifyMetricAvailability({
            monitored: true,
            success: true,
            value: avg,
            denominator: 1,
          });
    kpis.aiPresence = gateClientMetric("aiPresence", {
      ...presenceMetric,
      delta: null,
      geography: effectiveGeo,
      provider,
      ALL_PROVIDERS_DERIVED: true,
    });
    kpis.competitivePosition = gateClientMetric("competitivePosition", {
      availability: AVAILABILITY.NOT_COMPARABLE,
      rank: null,
      peerCount: null,
      display: "Use a specific provider for peer rank",
      delta: null,
      helper: "Peer rank is provider-specific. Select OpenAI, Gemini, Perplexity, or Claude.",
      ALL_PROVIDERS_PEER_RANK: false,
    });

    try {
      const measuredIds = (providerContext.availableProviders || [])
        .map((p) => p.id || p)
        .filter(Boolean);
      const byProvider = await loadObservationsByProviderForCohort({
        store,
        geoFilter: effectiveGeo,
        language: language || "en",
        providers: measuredIds.length ? measuredIds : [...KNOWN_AI_VISIBILITY_PROVIDER_IDS],
      });
      crossProviderQuestions = computeBrandCrossProviderQuestionsMissing({
        byProvider,
        subjectBrandId: brandId,
      });
      if (crossProviderQuestions.denominator > 0) {
        kpis.questionsMissing = gateClientMetric("questionsMissing", {
          ...questionsCountMetric(
            crossProviderQuestions.questionsMissingCount,
            crossProviderQuestions.denominator,
            true
          ),
          aggregation: crossProviderQuestions.aggregation,
          OPENAI_SCAFFOLD: false,
          helper: CLIENT_METRIC_DEFINITIONS.QUESTIONS_MISSING.allProvidersOneLiner,
          CROSS_PROVIDER_STATE_COUNTS: {
            MISSING_ACROSS_ALL_N: crossProviderQuestions.MISSING_ACROSS_ALL_N,
            PRESENT_ON_ANY_N: crossProviderQuestions.PRESENT_ON_ANY_N,
            PRESENT_ACROSS_ALL_N: crossProviderQuestions.PRESENT_ACROSS_ALL_N,
            PROVIDER_DISAGREEMENT_N: crossProviderQuestions.PROVIDER_DISAGREEMENT_N,
          },
        });
      }
    } catch (err) {
      if (typeof console !== "undefined" && console.warn) {
        console.warn("[brand-read] cross-provider QM failed:", err.message);
      }
    }
  }

  const publicDiscoverability = buildDiscoverabilityProductPayload(brandId, {
    brandNamesById,
    brandBasicsById,
  });

  const monitoringFreshness = await buildMonitoringFreshness({
    store,
    geographyScope: effectiveGeo.geographyScope || null,
    commercialRegion: effectiveGeo.commercialRegion || effectiveGeo.key || null,
    country: effectiveGeo.country || null,
    language: language || "en",
    availableProviders: providerContext.availableProviders || [],
    provider,
  });

  // Enrich owned citation rates on secondary when possible from latest batch observations
  let ownedCitationSecondary = null;
  try {
    if (owned.ownedDomainList?.length && latestSummary) {
      const overviewMatchedSlots = latestSummary._matchedSlotKeys || null;
      const loaded = await loadObservationsFromBatchSummary(store, latestSummary, {
        matchedSlotKeys: overviewMatchedSlots?.length ? overviewMatchedSlots : undefined,
        language: language || undefined,
      });
      const obs = (loaded.observations || []).filter((o) => {
        if (!language) return true;
        return recordMatchesLanguage(
          { language: o.language ?? o.payload?.language },
          language,
          { treatMissingAsEn: true }
        );
      });
      const rates = computeResponseCitationRates(obs || [], {
        ownedDomains: owned.ownedDomainList,
      });
      ownedCitationSecondary = {
        citationRate: rates.CITATION_RATE,
        ownedSourceCitationRate: rates.OWNED_SOURCE_CITATION_RATE,
        thirdPartyCitationRate: rates.THIRD_PARTY_CITATION_RATE,
        ownedDomainsCited: rates.OWNED_DOMAINS_CITED,
        OWNED_DOMAIN_STATUS: owned.OWNED_DOMAIN_STATUS,
      };
      if (rates.CITATION_RATE?.value != null) {
        secondary.citationRate = {
          ...rates.CITATION_RATE,
          readiness: "OBSERVED",
        };
      }
      if (rates.OWNED_SOURCE_CITATION_RATE) {
        secondary.ownedSourceCitationRate = rates.OWNED_SOURCE_CITATION_RATE;
      }
      if (rates.THIRD_PARTY_CITATION_RATE) {
        secondary.thirdPartyCitationRate = rates.THIRD_PARTY_CITATION_RATE;
      }
    } else if (owned.OWNED_DOMAIN_STATUS === "MISSING_GOVERNED_SOURCE") {
      secondary.ownedSourceCitationRate = {
        availability: AVAILABILITY.UNAVAILABLE,
        value: null,
        display: "No official brand website has been configured.",
        OWNED_DOMAIN_STATUS: "MISSING_GOVERNED_SOURCE",
      };
    }
  } catch (err) {
    if (typeof console !== "undefined" && console.warn) {
      console.warn("[brand-read] owned citation secondary skipped:", err.message);
    }
  }

  const brandName =
    brandNamesById[brandId] || metricPack.byMetric.aiPresenceRate?.entityName || null;
  let competitiveGap = null;
  try {
    const peerRows = latestSummary?.metrics?.competitivePosition?.peers || [];
    const subjectPresence =
      typeof presenceVal === "number" && Number.isFinite(presenceVal)
        ? presenceVal
        : null;
    let leadingPeer = null;
    for (const peer of peerRows) {
      if (!peer || peer.entityId === brandId) continue;
      const p =
        typeof peer.presence === "number"
          ? peer.presence
          : typeof peer.aiPresenceRate === "number"
            ? peer.aiPresenceRate
            : null;
      if (p == null || !Number.isFinite(p)) continue;
      if (!leadingPeer || p > leadingPeer.presence) {
        leadingPeer = {
          entityId: peer.entityId,
          name:
            peer.name ||
            brandNamesById[peer.entityId] ||
            peer.entityId ||
            null,
          presence: p,
        };
      }
    }
    if (subjectPresence != null && leadingPeer) {
      competitiveGap = {
        subjectName: brandName,
        subjectPresence,
        peerName: leadingPeer.name,
        peerPresence: leadingPeer.presence,
      };
    }
  } catch (err) {
    if (typeof console !== "undefined" && console.warn) {
      console.warn("[brand-read] detail competitive gap skipped:", err.message);
    }
  }

  const intentRows = decisionPatterns?.ownerIntentCoverage?.rows || [];
  let topMissingPromptFamily = null;
  if (intentRows.length) {
    const weakest = [...intentRows]
      .filter((r) => typeof r.value === "number" && Number.isFinite(r.value))
      .sort((a, b) => a.value - b.value)[0];
    if (weakest) {
      const monitored =
        weakest.monitoredN ?? weakest.MONITORED_N ?? weakest.denominator ?? null;
      const present =
        weakest.presentN ?? weakest.PRESENT_N ?? weakest.numerator ?? null;
      const missing =
        monitored != null && present != null
          ? Math.max(0, monitored - present)
          : null;
      topMissingPromptFamily = {
        promptFamily: weakest.intentTerritory || null,
        intentTerritory: weakest.intentTerritory || null,
        display: weakest.display || null,
        QUESTIONS_MISSING: missing,
        MONITORED_QUESTIONS: monitored,
        missingN: missing,
        presentN: present,
      };
    }
  }

  const kpiPresenceDelta = kpis?.aiPresence?.delta || null;
  const presenceChange =
    kpiPresenceDelta &&
    typeof kpiPresenceDelta.absolute === "number" &&
    kpiPresenceDelta.availability !== AVAILABILITY.NOT_COMPARABLE &&
    kpiPresenceDelta.availability !== AVAILABILITY.INSUFFICIENT_HISTORY
      ? {
          comparable: true,
          deltaPp: Math.round(kpiPresenceDelta.absolute * 1000) / 10,
          brandName,
        }
      : null;

  const detailExecutiveInsights = buildBrandDetailInsightBoxes({
    brandId,
    brandName,
    geographyKey: effectiveGeo.key || geography,
    provider,
    providerLabel: formatProviderLabel(provider),
    language: language || "en",
    presenceValue:
      typeof kpis?.aiPresence?.value === "number" ? kpis.aiPresence.value : presenceVal,
    presenceDisplay: kpis?.aiPresence?.display || null,
    rankDisplay: kpis?.competitivePosition?.display || null,
    competitiveGap,
    questionsMissing: {
      value:
        typeof kpis?.questionsMissing?.value === "number"
          ? kpis.questionsMissing.value
          : typeof mon.questionsMissing === "number"
            ? mon.questionsMissing
            : null,
      denominator:
        kpis?.questionsMissing?.denominator ??
        mon.promptDenominator ??
        null,
      display: kpis?.questionsMissing?.display || null,
    },
    topMissingPromptFamily,
    presenceChange: allProvidersMode ? null : presenceChange,
    crossProvider: crossProviderFromProviderPresencePanel(providerPresencePanel),
  });

  let detailIntelligence = null;
  let brandExecutiveFindings = null;
  try {
    const peerNames = {
      ...peerSetBrandNamesById(PEER_SET_ID_V2),
      [brandId]: brandName,
    };
    detailIntelligence = await buildBrandDetailIntelligence({
      store,
      brandId,
      brandName,
      brandNamesById: peerNames,
      geographyKey: effectiveGeo.key || "CALA",
      language: language || "en",
      provider,
      crossProvider: crossProviderFromProviderPresencePanel(providerPresencePanel),
      peerSetId: PEER_SET_ID_V2,
    });
    brandExecutiveFindings = await buildExecutiveFindings({
      store,
      brandIds: [brandId],
      brandNamesById: peerNames,
      geographyKey: effectiveGeo.key || "CALA",
      language: language || "en",
      scope: "brand",
      subjectBrandId: brandId,
      crossProvider: crossProviderFromProviderPresencePanel(providerPresencePanel),
      presenceChange: allProvidersMode ? null : presenceChange,
      peerSetId: PEER_SET_ID_V2,
    });
  } catch (err) {
    if (typeof console !== "undefined" && console.warn) {
      console.warn("[brand-read] detailIntelligence skipped:", err.message);
    }
    detailIntelligence = { ok: false, reason: "engine_error" };
  }

  return {
    ok: true,
    allowed: true,
    accessDepth: access.accessDepth,
    productSurface: "AI Visibility",
    brandId,
    brandName,
    provider,
    providerLabel: providerContext.providerLabel,
    providerMode: providerContext.providerMode || (allProvidersMode ? "DERIVED" : "PROVIDER_SPECIFIC"),
    availableProviders: providerContext.availableProviders,
    providerSelectorOptions: providerContext.providerSelectorOptions,
    ALL_PROVIDERS_DERIVED: allProvidersMode,
    ALL_PROVIDERS_RUN: false,
    SYNTHETIC_PRIMARY_PROVIDER_DEPENDENCY_REMOVED: allProvidersMode === true,
    scaffoldProvider: allProvidersMode ? dataProvider : null,
    OPENAI_SCAFFOLD_REMOVED_FOR_QM: allProvidersMode === true,
    crossProviderPresence: overviewCrossProvider,
    crossProviderQuestions,
    geography: effectiveGeo,
    language: language || "en",
    availableLanguages: langResolved.availableLanguages,
    languageFilterContract: langResolved.filterContract,
    SILENT_LANGUAGE_FALLBACK: false,
    availabilityReason: mon.code,
    availabilityMessage: mon.message,
    kpis,
    secondary,
    ownedDomainResolution: owned,
    ownedCitationSecondary,
    providerPresencePanel,
    publicDiscoverability,
    decisionPatterns,
    aiVsDealalityContext,
    reviewItems,
    discoverabilityBusinessImpact,
    openAiDiscoverability,
    regionalPosition,
    detailExecutiveInsights,
    detailIntelligence,
    brandExecutiveFindings,
    brandExecutiveIntelligenceInsights: executiveFindingsToInsightBoxes(brandExecutiveFindings),
    monitoringFreshness,
    latestMonitoring: latestSummary?.completedAt || presenceSnaps[0]?.batchDate || null,
    priorMonitoring: priorSummary?.completedAt || presenceSnaps[1]?.batchDate || null,
    opportunityQueue: futureOpportunityQueue(),
    evidenceStrength: {
      status: AVAILABILITY.FUTURE_READY,
      note: "FUTURE-READY — NO ARBITRARY CONFIDENCE LABELS",
      descriptorsSupported: true,
    },
    brandReadServiceVersion: BRAND_READ_SERVICE_VERSION,
    COMPLETION_WAVE: 2,
  };
}

/**
 * Presence over time — actual monitoring points only.
 */
export async function getBrandTrendPayload(args = {}) {
  const {
    dealalityUser,
    viewerContext,
    entitlementGraph,
    store,
    brandId,
    provider: providerArg = DEFAULT_AI_VISIBILITY_PROVIDER,
    geography,
    language: languageArg = null,
    range,
  } = args;

  const provider = resolveProviderId(providerArg);

  const viewer = viewerContext || normalizeAiVisibilityViewerContext(dealalityUser);
  const access = resolveAiIntelligenceAccess({
    viewerContext: viewer,
    subject: { subjectType: "brand", subjectEntityId: brandId },
    entitlementGraph,
  });
  if (!access.allowed) {
    return { ok: false, allowed: false, reasonCode: access.reasonCode };
  }

  const geo = parseGeographyQuery({ geography, ...args });
  const effectiveGeo = geo.geographyScope
    ? geo
    : { geographyScope: "Region", commercialRegion: "CALA", key: "CALA" };

  const { allProvidersMode, dataProvider } = await resolveAllProvidersReadScaffold({
    store,
    provider,
    effectiveGeo,
  });

  const langResolved = await resolveMonitoringLanguageForRead({
    store,
    provider: dataProvider,
    geographyFilter: effectiveGeo,
    language: languageArg,
  });
  if (!langResolved.ok) {
    return {
      ok: false,
      allowed: true,
      accessDepth: access.accessDepth,
      reasonCode: langResolved.reasonCode,
      message: langResolved.message,
      availableLanguages: langResolved.availableLanguages,
      languageFilterContract: langResolved.filterContract,
      SILENT_LANGUAGE_FALLBACK: false,
      ALL_PROVIDERS_DERIVED: allProvidersMode,
    };
  }
  const language = langResolved.language;
  if (langResolved.status === "not_monitored" && languageArg) {
    return {
      ok: true,
      allowed: true,
      accessDepth: access.accessDepth,
      brandId,
      provider,
      geography: effectiveGeo,
      language,
      availableLanguages: langResolved.availableLanguages,
      languageFilterContract: langResolved.filterContract,
      SILENT_LANGUAGE_FALLBACK: false,
      availability: AVAILABILITY.NOT_MONITORED,
      availabilityReason: MONITORING_STATE.NO_BATCH,
      availabilityMessage: `No ${language === "es" ? "Spanish" : "English"} monitoring data is available for this brand in ${effectiveGeo.key} yet.`,
      points: [],
      pointCount: 0,
      message: `No ${language === "es" ? "Spanish" : "English"} monitoring data is available for this brand in ${effectiveGeo.key} yet.`,
      timeRanges: evaluateTimeRangeSupport([]),
      selectedRange: null,
      FAKE_INTERMEDIATE_POINTS: "NONE",
      ALL_PROVIDERS_DERIVED: allProvidersMode,
      brandReadServiceVersion: BRAND_READ_SERVICE_VERSION,
    };
  }

  // Prefer geography-matched batch summaries (incl. multi-slot wave1) so trend
  // points are slot-scoped — never inherit wave1_multi parent aggregates as geo KPIs.
  const summaries = await findMatchingSummaries(store, effectiveGeo, dataProvider, {
    language: language || "en",
  });
  const summaryPoints = [];
  const trendLanguage = language || "en";
  for (const summary of latestByBatchDate(summaries).reverse()) {
    try {
      const matchedSlots = summary._matchedSlotKeys || null;
      const projectedLang = normalizeLanguage(summary.language) || trendLanguage;
      if (projectedLang !== trendLanguage) continue;
      const { observations } = await loadObservationsFromBatchSummary(store, summary, {
        matchedSlotKeys: matchedSlots?.length ? matchedSlots : undefined,
        language: trendLanguage,
      });
      if (!observations.length) continue;
      const presence = computeAiPresenceRate(observations, brandId).value;
      if (presence == null || !Number.isFinite(presence)) continue;
      summaryPoints.push({
        batchId: summary.batchId,
        date: summary.completedAt || summary.startedAt || summary.savedAt,
        value: presence,
        availability: presence === 0 ? AVAILABILITY.ZERO : AVAILABILITY.OBSERVED,
        metricScope: matchedSlots?.length ? "slot_filtered" : "batch_observations",
        matchedSlotKeys: matchedSlots || null,
        language: trendLanguage,
        provider,
        geographyKey: effectiveGeo.key,
      });
    } catch (err) {
      if (typeof console !== "undefined" && console.warn) {
        console.warn("[brand-read] trend point from summary failed:", err.message);
      }
    }
  }

  let points = summaryPoints.filter(
    (p) => normalizeLanguage(p.language) === trendLanguage
  );

  // Legacy path: geography-scoped snapshot files (Phase 2E single-geo cohorts).
  if (!points.length) {
    const snaps = await store.listMetricSnapshots({
      entityId: brandId,
      geographyScope: effectiveGeo.geographyScope,
      region: effectiveGeo.commercialRegion || undefined,
      provider,
      metric: "ai_presence_rate",
      language: language || "en",
    });
    points = latestByBatchDate(
      snaps.filter(
        (s) =>
          matchesGeography(s, effectiveGeo) &&
          normalizeMetricKey(s.metric) === "aiPresenceRate" &&
          recordMatchesLanguage(
            { language: s.language },
            trendLanguage,
            { treatMissingAsEn: true }
          )
      )
    )
      .map((s) => ({
        batchId: s.batchId,
        date: s.batchDate || s.savedAt,
        value: s.value,
        availability: s.value === 0 ? AVAILABILITY.ZERO : AVAILABILITY.OBSERVED,
        language: trendLanguage,
        provider,
        geographyKey: effectiveGeo.key,
      }))
      .reverse();
  }

  if (!points.length) {
    const snaps2 = await store.listMetricSnapshots({
      entityId: brandId,
      geographyScope: effectiveGeo.geographyScope,
      region: effectiveGeo.commercialRegion || undefined,
      provider,
      language: language || "en",
    });
    points = latestByBatchDate(
      snaps2.filter(
        (s) =>
          matchesGeography(s, effectiveGeo) &&
          normalizeMetricKey(s.metric) === "aiPresenceRate" &&
          recordMatchesLanguage(
            { language: s.language },
            trendLanguage,
            { treatMissingAsEn: true }
          )
      )
    )
      .map((s) => ({
        batchId: s.batchId,
        date: s.batchDate || s.savedAt,
        value: s.value,
        availability: s.value === 0 ? AVAILABILITY.ZERO : AVAILABILITY.OBSERVED,
        language: trendLanguage,
        provider,
        geographyKey: effectiveGeo.key,
      }))
      .reverse();
  }

  const rangeSupport = evaluateTimeRangeSupport(points);
  const selectedRange = range && rangeSupport[range] === true ? range : null;

  let renderState = "NO_CURRENT_MONITORING";
  let message = `Not Monitored for this geography yet — no AI Presence trend points for ${formatProviderLabel(provider)}.`;
  let pointCountComparable = 0;
  let pairwiseBlocked = false;
  if (points.length === 1) {
    renderState = "ONE_VALID_POINT";
    message =
      "Insufficient History — not enough comparable periods exist for trend.";
  } else if (points.length >= 2) {
    // Pairwise comparability before treating series as chartable deltas.
    let comparablePairs = 0;
    for (let i = 1; i < points.length; i++) {
      const cmp = compareTrendObservations(
        {
          ...points[i - 1],
          provider,
          geographyKey: effectiveGeo.key,
          language: trendLanguage,
          metric: "aiPresenceRate",
        },
        {
          ...points[i],
          provider,
          geographyKey: effectiveGeo.key,
          language: trendLanguage,
          metric: "aiPresenceRate",
        }
      );
      if (cmp.comparable) comparablePairs += 1;
      else pairwiseBlocked = true;
    }
    pointCountComparable = comparablePairs + (comparablePairs > 0 ? 1 : 0);
    if (comparablePairs === 0) {
      renderState = "NOT_COMPARABLE";
      message = "Not Comparable — trend observations cannot validly be compared.";
    } else {
      renderState = "TWO_OR_MORE_COMPARABLE_POINTS";
      message = pairwiseBlocked
        ? "Some periods are not comparable; deltas only use compatible pairs."
        : null;
    }
  }

  return {
    ok: true,
    allowed: true,
    accessDepth: access.accessDepth,
    brandId,
    provider,
    providerLabel: formatProviderLabel(provider),
    geography: effectiveGeo,
    language: trendLanguage,
    availableLanguages: langResolved.availableLanguages,
    languageFilterContract: langResolved.filterContract,
    SILENT_LANGUAGE_FALLBACK: false,
    TREND_LANGUAGE_MATCH_REQUIRED,
    PAIRWISE_COMPARABILITY: true,
    points,
    pointCount: points.length,
    pointCountComparable,
    renderState,
    availability:
      points.length === 0
        ? AVAILABILITY.NOT_MONITORED
        : renderState === "NOT_COMPARABLE"
          ? AVAILABILITY.NOT_COMPARABLE
          : points.length === 1
            ? AVAILABILITY.INSUFFICIENT_HISTORY
            : AVAILABILITY.OBSERVED,
    message,
    timeRanges: rangeSupport,
    selectedRange,
    FAKE_INTERMEDIATE_POINTS: "NONE",
    INVALID_DELTAS_BLOCKED: pairwiseBlocked || renderState === "NOT_COMPARABLE",
    brandReadServiceVersion: BRAND_READ_SERVICE_VERSION,
  };
}

function evaluateTimeRangeSupport(points) {
  if (!points.length) {
    return {
      "30D": "NOT YET AVAILABLE — INSUFFICIENT HISTORY",
      "90D": "NOT YET AVAILABLE — INSUFFICIENT HISTORY",
      "6M": "NOT YET AVAILABLE — INSUFFICIENT HISTORY",
      "1Y": "NOT YET AVAILABLE — INSUFFICIENT HISTORY",
    };
  }
  const dates = points.map((p) => Date.parse(p.date)).filter(Number.isFinite);
  const spanMs = dates.length >= 2 ? Math.max(...dates) - Math.min(...dates) : 0;
  const day = 86400000;
  return {
    "30D":
      points.length >= 2 && spanMs >= 7 * day
        ? true
        : "NOT YET AVAILABLE — INSUFFICIENT HISTORY",
    "90D":
      points.length >= 2 && spanMs >= 30 * day
        ? true
        : "NOT YET AVAILABLE — INSUFFICIENT HISTORY",
    "6M":
      points.length >= 2 && spanMs >= 90 * day
        ? true
        : "NOT YET AVAILABLE — INSUFFICIENT HISTORY",
    "1Y":
      points.length >= 2 && spanMs >= 180 * day
        ? true
        : "NOT YET AVAILABLE — INSUFFICIENT HISTORY",
  };
}

/**
 * Owner questions table for brand subject.
 */
export async function getBrandQuestionsPayload(args = {}) {
  const {
    dealalityUser,
    viewerContext,
    entitlementGraph,
    store,
    brandId,
    provider: providerArg = DEFAULT_AI_VISIBILITY_PROVIDER,
    geography,
    language: languageArg = null,
    filter = "all",
    intentTerritory,
    limit: limitArg = 50,
    offset: offsetArg = 0,
    brandNamesById = {},
    watchlistMode = null,
    groupBy = null,
  } = args;

  const provider = resolveProviderId(providerArg);

  const viewer = viewerContext || normalizeAiVisibilityViewerContext(dealalityUser);
  const access = resolveAiIntelligenceAccess({
    viewerContext: viewer,
    subject: { subjectType: "brand", subjectEntityId: brandId },
    entitlementGraph,
  });
  if (!access.allowed || access.accessDepth !== ACCESS_DEPTH.DEEP) {
    return {
      ok: false,
      allowed: false,
      reasonCode: access.reasonCode || "SUBJECT_NOT_ENTITLED",
      accessDepth: access.accessDepth,
    };
  }

  const geo = parseGeographyQuery({ geography, ...args });
  const effectiveGeo = geo.geographyScope
    ? geo
    : { geographyScope: "Region", commercialRegion: "CALA", key: "CALA" };

  const { allProvidersMode, dataProvider, providerContext } =
    await resolveAllProvidersReadScaffold({
      store,
      provider,
      effectiveGeo,
    });

  const langResolved = await resolveMonitoringLanguageForRead({
    store,
    provider: dataProvider,
    geographyFilter: effectiveGeo,
    language: languageArg,
  });
  if (!langResolved.ok) {
    return {
      ok: false,
      allowed: true,
      accessDepth: access.accessDepth,
      reasonCode: langResolved.reasonCode,
      message: langResolved.message,
      availableLanguages: langResolved.availableLanguages,
      languageFilterContract: langResolved.filterContract,
      SILENT_LANGUAGE_FALLBACK: false,
      ALL_PROVIDERS_DERIVED: allProvidersMode,
    };
  }
  const language = langResolved.language;
  if (langResolved.status === "not_monitored" && languageArg) {
    return {
      ok: true,
      allowed: true,
      accessDepth: access.accessDepth,
      brandId,
      provider,
      geography: effectiveGeo,
      language,
      availableLanguages: langResolved.availableLanguages,
      languageFilterContract: langResolved.filterContract,
      SILENT_LANGUAGE_FALLBACK: false,
      availability: AVAILABILITY.NOT_MONITORED,
      availabilityReason: MONITORING_STATE.NO_BATCH,
      availabilityMessage: `No ${language === "es" ? "Spanish" : "English"} monitoring data is available for this brand in ${effectiveGeo.key} yet.`,
      filter,
      questions: [],
      ALL_PROVIDERS_DERIVED: allProvidersMode,
      brandReadServiceVersion: BRAND_READ_SERVICE_VERSION,
    };
  }

  const summaries = await findMatchingSummaries(store, effectiveGeo, dataProvider, {
    language: language || "en",
  });
  const questions = [];

  for (const summary of summaries.slice(0, 5)) {
    const runs = (await store.listBatchRuns(summary.batchId)) || [];
    const matchedSlots = summary._matchedSlotKeys || null;
    const providerForIndex =
      summary.provider?.name || summary.provider || dataProvider || null;
    const evidenceIndex = await buildEvidenceResolutionIndex(store, {
      batchId: summary.batchId,
      provider: providerForIndex,
    });
    for (const run of runs) {
      if (run.status !== "completed") continue;
      if (!runMatchesSlotFilter(run, matchedSlots)) continue;
      const resolved = await resolveEvidenceForRun(store, run, {
        index: evidenceIndex,
        batchId: summary.batchId,
        provider: providerForIndex,
      });
      if (
        resolved.mode === EVIDENCE_RESOLUTION_MODES.AMBIGUOUS_EVIDENCE_LINK ||
        !resolved.evidence
      ) {
        continue;
      }
      const evidence = resolved.evidence;
      const rowLanguage = resolveEvidenceObservationLanguage(
        evidence,
        deriveRunSlotKey(run) || run.slot
      );
      if (
        language &&
        !recordMatchesLanguage({ language: rowLanguage }, language, {
          treatMissingAsEn: true,
        })
      ) {
        continue;
      }
      const mentions = evidence.payload?.mentions || [];
      const subjectMentions = mentions.filter(
        (m) =>
          m.entityId === brandId ||
          m.resolvedEntityId === brandId ||
          m.canonicalEntityId === brandId
      );
      // Presence-led client status (recommendation roles remain internal only).
      const presenceObserved = subjectMentions.length > 0;
      const status = presenceObserved ? "Present" : "Missing";

      const competitors = mentions
        .filter(
          (m) =>
            (m.entityId || m.resolvedEntityId) &&
            (m.entityId || m.resolvedEntityId) !== brandId &&
            presenceObserved
        )
        .map((m) => ({
          entityId: m.entityId || m.resolvedEntityId,
          entityName: m.entityName || m.name || null,
          role: m.role,
        }))
        .slice(0, 5);

      const intent = evidence.intentTerritory || null;
      if (intentTerritory && intent && intent !== intentTerritory) continue;

      const citations = evidence.payload?.citations || [];
      const row = {
        promptId: evidence.promptId,
        promptVersion: evidence.promptVersion,
        question: evidence.promptText || evidence.promptId,
        brandStatus: status,
        presenceObserved,
        presenceLabel: presenceObserved ? "Present" : "Missing",
        topCompetitor: competitors[0] || null,
        intentTerritory: intent,
        geographyScope: evidence.geographyScope,
        commercialRegion: evidence.regionName || summary.cohort?.commercialRegion,
        evidenceId: evidence.evidenceId,
        responseId: evidence.responseId || run.responseId || evidence.payload?.responseId || null,
        batchId: summary.batchId,
        batchDate: summary.completedAt || evidence.timestamp,
        provider: evidence.provider || provider,
        language: rowLanguage || language || "en",
        evidenceResolutionMode: resolved.mode,
        evidenceDescriptors: buildEvidenceDescriptors({
          promptCount: 1,
          periodCount: 1,
          citationCount: citations.length,
          observationCount: 1,
        }),
      };

      const isPresent = presenceObserved;
      const isMissing = !presenceObserved;
      if (filter === "present" && !isPresent) continue;
      if (filter === "won" && !isPresent) continue; // legacy alias → present
      if (filter === "missing" && !isMissing) continue;
      questions.push(row);
    }
  }

  // Deduplicate by promptId keeping latest
  const byPrompt = new Map();
  for (const q of questions) {
    const prev = byPrompt.get(q.promptId);
    if (!prev || String(q.batchDate) > String(prev.batchDate)) byPrompt.set(q.promptId, q);
  }
  let allRows = [...byPrompt.values()];

  /** @type {object|null} */
  let crossProviderQuestions = null;
  if (allProvidersMode) {
    try {
      const measuredIds = (providerContext?.availableProviders || [])
        .map((p) => p.id || p)
        .filter(Boolean);
      const byProviderObs = await loadObservationsByProviderForCohort({
        store,
        geoFilter: effectiveGeo,
        language: language || "en",
        providers: measuredIds.length
          ? measuredIds
          : [...KNOWN_AI_VISIBILITY_PROVIDER_IDS],
      });
      crossProviderQuestions = computeBrandCrossProviderQuestionsMissing({
        byProvider: byProviderObs,
        subjectBrandId: brandId,
      });
      // Replace scaffold-only question rows with cross-provider classified rows.
      allRows = (crossProviderQuestions.rows || []).map((r) => ({
        promptId: r.promptId,
        question: r.QUESTION,
        brandStatus:
          r.CROSS_PROVIDER_STATE === "MISSING_ACROSS_ALL_PROVIDERS"
            ? "Missing"
            : r.CROSS_PROVIDER_STATE === "PRESENT_ACROSS_ALL_COMPARABLE"
              ? "Present"
              : "Mixed",
        presenceObserved: r.PRESENT_ON_ANY_PROVIDER === true,
        presenceLabel: r.CROSS_PROVIDER_STATE,
        CROSS_PROVIDER_STATE: r.CROSS_PROVIDER_STATE,
        PROVIDERS_PRESENT: r.PROVIDERS_PRESENT,
        PROVIDERS_MISSING: r.PROVIDERS_MISSING,
        PROVIDERS_MONITORED: r.PROVIDERS_MONITORED,
        intentTerritory: r.PROMPT_FAMILY,
        provider: "all",
        language: language || "en",
        commercialRegion: effectiveGeo.key,
        evidenceId: null,
        responseId: null,
        batchDate: null,
      }));
      if (filter === "present" || filter === "won") {
        allRows = allRows.filter((q) => q.presenceObserved);
      } else if (filter === "missing") {
        allRows = allRows.filter(
          (q) => q.CROSS_PROVIDER_STATE === "MISSING_ACROSS_ALL_PROVIDERS"
        );
      }
    } catch (err) {
      if (typeof console !== "undefined" && console.warn) {
        console.warn("[brand-read] all-providers questions failed:", err.message);
      }
    }
  }

  allRows = allRows.map((q) => enrichRowWithPromptOriginFromLibrary(q));

  const limit = Math.min(Math.max(Number(limitArg) || 50, 1), 100);
  const offset = Math.max(Number(offsetArg) || 0, 0);
  const page = allRows.slice(offset, offset + limit);

  // Questions Missing watchlist (+ peer-present / subject-missing mode)
  const missingRows = allProvidersMode && crossProviderQuestions
    ? (crossProviderQuestions.watchlistRows || []).map((r) => ({
        QUESTION: r.QUESTION,
        PROMPT_FAMILY: r.PROMPT_FAMILY,
        PROVIDER: "all",
        PROVIDERS_PRESENT: r.PROVIDERS_PRESENT,
        PROVIDERS_MISSING: r.PROVIDERS_MISSING,
        CROSS_PROVIDER_STATE: r.CROSS_PROVIDER_STATE,
        REGION: effectiveGeo.key,
        LANGUAGE: language || "en",
        SUBJECT_PRESENCE: "MISSING_ACROSS_ALL_PROVIDERS",
        PEERS_PRESENT: [],
        MONITORING_DATE: null,
        EVIDENCE: null,
        promptId: r.promptId,
        evidenceId: null,
        responseId: null,
      }))
    : allRows
        .filter((q) => !q.presenceObserved)
        .map((q) => ({
          QUESTION: q.question,
          PROMPT_FAMILY: q.intentTerritory || "Unspecified",
          PROVIDER: q.provider,
          REGION: q.commercialRegion || effectiveGeo.key,
          LANGUAGE: q.language || language || "en",
          SUBJECT_PRESENCE: "NOT OBSERVED",
          PEERS_PRESENT: q.topCompetitor
            ? [{ entityId: q.topCompetitor.entityId, entityName: q.topCompetitor.entityName }]
            : [],
          MONITORING_DATE: q.batchDate,
          EVIDENCE: q.evidenceId,
          promptId: q.promptId,
          evidenceId: q.evidenceId,
          responseId: q.responseId,
        }));

  const peerIds = entitlementGraph?.peerBrandIds || [];
  let peerPresentSubjectMissing = {
    rows: [],
    PEER_PRESENT_SUBJECT_MISSING_N: 0,
    READY: false,
  };
  try {
    const latest = summaries[0];
    if (latest && peerIds.length) {
      const matchedSlots = latest._matchedSlotKeys || null;
      const loaded = await loadObservationsFromBatchSummary(store, latest, {
        matchedSlotKeys: matchedSlots?.length ? matchedSlots : undefined,
        language: language || "en",
      });
      peerPresentSubjectMissing = buildPeerPresentSubjectMissing(
        loaded.observations || [],
        {
          subjectBrandId: brandId,
          peerEntityIds: peerIds,
          peerNamesById: {
            ...peerSetBrandNamesById(PEER_SET_ID_V2),
            ...(brandNamesById || {}),
          },
        }
      );
    }
  } catch (err) {
    if (typeof console !== "undefined" && console.warn) {
      console.warn("[brand-read] peer-present watchlist skipped:", err.message);
    }
  }

  const watchlistRows =
    watchlistMode === "peers_present_subject_missing" ||
    watchlistMode === "peer_gaps"
      ? (peerPresentSubjectMissing.rows || []).map((r) => ({
          ...r,
          SUBJECT_PRESENCE: "NOT OBSERVED",
        }))
      : missingRows;

  const originWatchlistRows = watchlistRows.map((r) =>
    enrichRowWithPromptOriginFromLibrary(r)
  );

  const groupDimension = groupBy || null;
  const watchlistGrouped = groupDimension
    ? groupQuestionsMissingWatchlist(originWatchlistRows, groupDimension)
    : null;

  return {
    ok: true,
    allowed: true,
    accessDepth: access.accessDepth,
    brandId,
    provider,
    geography: effectiveGeo,
    language: language || "en",
    availableLanguages: langResolved.availableLanguages,
    languageFilterContract: langResolved.filterContract,
    SILENT_LANGUAGE_FALLBACK: false,
    filter,
    questions: page,
    pagination: {
      limit,
      offset,
      total: allRows.length,
      hasMore: offset + limit < allRows.length,
    },
    questionsMissingWatchlist: {
      mode:
        watchlistMode === "peers_present_subject_missing" ||
        watchlistMode === "peer_gaps"
          ? "Peers Appearing Where We Are Not"
          : allProvidersMode
            ? "Questions Missing Across All Providers"
            : "Questions Missing",
      CLIENT_COPY: allProvidersMode
        ? "Comparable owner questions where this brand was missing on every monitored provider. Disagreement rows are listed separately when present on some providers only."
        : "Subject Presence on this view is normally Not Observed. Absence is derived from successful monitoring and validated entity resolution — not fabricated evidence of absence.",
      rows: originWatchlistRows,
      disagreementRows: allProvidersMode
        ? crossProviderQuestions?.disagreementRows || []
        : [],
      grouped: watchlistGrouped,
      filters: {
        promptFamily: true,
        provider: !allProvidersMode,
        region: true,
        language: true,
        peerBrand: true,
      },
      peerPresentSubjectMissing,
      crossProviderQuestions: allProvidersMode ? crossProviderQuestions : null,
      OPENAI_SCAFFOLD: allProvidersMode ? false : undefined,
      promptFamilyRollup: buildPromptFamilyMissingRollup(
        // lightweight: from missing rows only
        missingRows.map((r) => ({
          promptFamily: r.PROMPT_FAMILY,
          success: true,
          presentEntityIds: [],
          promptId: r.promptId,
        })),
        brandId
      ),
      ARBITRARY_PRIORITY_SCORE: false,
    },
    ALL_PROVIDERS_DERIVED: allProvidersMode,
    OPENAI_SCAFFOLD_REMOVED_FOR_QM: allProvidersMode === true,
    brandReadServiceVersion: BRAND_READ_SERVICE_VERSION,
    COMPLETION_WAVE: 2,
  };
}

/**
 * Competitive context — subject deep + peers comparative-safe.
 */
export async function getBrandCompetitorsPayload(args = {}) {
  const {
    dealalityUser,
    viewerContext,
    entitlementGraph,
    store,
    brandId,
    provider: providerArg = DEFAULT_AI_VISIBILITY_PROVIDER,
    geography,
    language: languageArg = null,
    brandNamesById = {},
  } = args;

  const provider = resolveProviderId(providerArg);

  const viewer = viewerContext || normalizeAiVisibilityViewerContext(dealalityUser);
  const access = resolveAiIntelligenceAccess({
    viewerContext: viewer,
    subject: { subjectType: "brand", subjectEntityId: brandId },
    entitlementGraph,
  });
  if (!access.allowed) {
    return { ok: false, allowed: false, reasonCode: access.reasonCode };
  }

  const geo = parseGeographyQuery({ geography, ...args });
  const effectiveGeo = geo.geographyScope
    ? geo
    : { geographyScope: "Region", commercialRegion: "CALA", key: "CALA" };

  const { allProvidersMode, dataProvider } = await resolveAllProvidersReadScaffold({
    store,
    provider,
    effectiveGeo,
  });

  const langResolved = await resolveMonitoringLanguageForRead({
    store,
    provider: dataProvider,
    geographyFilter: effectiveGeo,
    language: languageArg,
  });
  if (!langResolved.ok) {
    return {
      ok: false,
      allowed: true,
      accessDepth: access.accessDepth,
      reasonCode: langResolved.reasonCode,
      message: langResolved.message,
      availableLanguages: langResolved.availableLanguages,
      languageFilterContract: langResolved.filterContract,
      SILENT_LANGUAGE_FALLBACK: false,
      ALL_PROVIDERS_DERIVED: allProvidersMode,
    };
  }
  const language = langResolved.language;
  if (langResolved.status === "not_monitored" && languageArg) {
    return {
      ok: true,
      allowed: true,
      accessDepth: access.accessDepth,
      brandId,
      geography: effectiveGeo,
      provider,
      language,
      availableLanguages: langResolved.availableLanguages,
      languageFilterContract: langResolved.filterContract,
      SILENT_LANGUAGE_FALLBACK: false,
      availability: AVAILABILITY.NOT_MONITORED,
      availabilityReason: MONITORING_STATE.NO_BATCH,
      availabilityMessage: `No ${language === "es" ? "Spanish" : "English"} monitoring data is available for this brand in ${effectiveGeo.key} yet.`,
      competitors: [],
      ALL_PROVIDERS_DERIVED: allProvidersMode,
      brandReadServiceVersion: BRAND_READ_SERVICE_VERSION,
    };
  }

  const summaries = await findMatchingSummaries(store, effectiveGeo, dataProvider, {
    language: language || "en",
  });
  const latest = summaries[0];
  const prior = summaries[1] || null;
  if (!latest) {
    return {
      ok: true,
      allowed: true,
      accessDepth: access.accessDepth,
      brandId,
      geography: effectiveGeo,
      provider,
      language: language || "en",
      availableLanguages: langResolved.availableLanguages,
      languageFilterContract: langResolved.filterContract,
      SILENT_LANGUAGE_FALLBACK: false,
      competitors: [],
      availability: AVAILABILITY.NOT_MONITORED,
      ALL_PROVIDERS_DERIVED: allProvidersMode,
      brandReadServiceVersion: BRAND_READ_SERVICE_VERSION,
    };
  }

  function presenceFromSummary(summary, entityId) {
    if (!summary || !entityId) return null;
    const rank = competitiveRankFromSummary(summary, entityId);
    if (typeof rank?.presence === "number") return rank.presence;
    const em = entityMetricsFromSummary(summary, entityId);
    const v = em?.presence ?? em?.aiPresenceRate ?? null;
    return typeof v === "number" ? v : null;
  }

  function buildPeerPresenceChange(entityId) {
    const cur = presenceFromSummary(latest, entityId);
    const prev = prior ? presenceFromSummary(prior, entityId) : null;
    if (cur == null || prev == null) {
      return {
        availability: AVAILABILITY.NOT_MONITORED,
        value: null,
        absoluteDeltaPp: null,
        direction: null,
        display: "INSUFFICIENT_COMPARABLE_HISTORY",
        code: "INSUFFICIENT_COMPARABLE_HISTORY",
        note: "INSUFFICIENT_COMPARABLE_HISTORY — not zero change. Require same provider, geography, language, prompt cohort, and compatible methodology.",
      };
    }
    const absolute = cur - prev;
    const absoluteDeltaPp = Math.round(absolute * 1000) / 10;
    return {
      availability: AVAILABILITY.OBSERVED,
      value: absolute,
      absoluteDeltaPp,
      direction: absolute > 0 ? "up" : absolute < 0 ? "down" : "flat",
      display:
        absoluteDeltaPp === 0
          ? "0 pp"
          : `${absoluteDeltaPp > 0 ? "+" : "−"}${Math.abs(absoluteDeltaPp)} pp`,
    };
  }

  const peers = latest.metrics?.competitivePosition?.peers || [];
  const rows = [];
  for (const peer of peers) {
    const peerAccess = resolveAiIntelligenceAccess({
      viewerContext: viewer,
      subject: { subjectType: "brand", subjectEntityId: peer.entityId },
      entitlementGraph,
    });
    const em = entityMetricsFromSummary(latest, peer.entityId);
    const base = {
      entityId: peer.entityId,
      entityName: peer.name || em?.name || brandNamesById?.[peer.entityId] || null,
      aiPresenceRate: peer.presence ?? peer.aiPresenceRate ?? em?.presence ?? em?.aiPresenceRate ?? null,
      aiPresenceChange: buildPeerPresenceChange(peer.entityId),
      competitivePosition: peer.rank ?? peer.position ?? null,
      recommendationShare:
        peer.recommendationShare ?? em?.recommendationShare ?? null,
      recommendationRate: em?.recommendationRate ?? peer.recommendationRate ?? null,
      top3RecommendationRate: em?.top3RecommendationRate ?? peer.top3RecommendationRate ?? null,
      firstRecommendationRate: em?.firstRecommendationRate ?? peer.firstRecommendationRate ?? null,
      isSubject: peer.entityId === brandId,
      accessDepth: peerAccess.accessDepth,
      deepLinkAllowed: peerAccess.accessDepth === ACCESS_DEPTH.DEEP,
    };
    if (peer.entityId === brandId || peerAccess.accessDepth === ACCESS_DEPTH.DEEP) {
      rows.push(base);
    } else if (peerAccess.accessDepth === ACCESS_DEPTH.COMPARATIVE) {
      rows.push(toBenchmarkSafeEntityView(base));
    }
    // none → omit
  }

  // Fallback peer set membership if summary lacks competitivePosition
  if (!rows.length && entitlementGraph?.peerBrandIds?.length) {
    const cfg = loadPeerSetConfig();
    const membership = resolvePeerSetMembership(
      {
        peerSetId: "peers_upper_upscale_brands_global_v1",
        commercialRegion: effectiveGeo.commercialRegion,
      },
      cfg
    );
    for (const id of membership.entityIds || []) {
      const peerAccess = resolveAiIntelligenceAccess({
        viewerContext: viewer,
        subject: { subjectType: "brand", subjectEntityId: id },
        entitlementGraph,
      });
      if (peerAccess.accessDepth === ACCESS_DEPTH.NONE) continue;
      const em = entityMetricsFromSummary(latest, id);
      const row = {
        entityId: id,
        entityName: brandNamesById?.[id] || null,
        aiPresenceRate: em?.presence ?? null,
        aiPresenceChange: buildPeerPresenceChange(id),
        competitivePosition: null,
        recommendationShare: em?.recommendationShare ?? null,
        isSubject: id === brandId,
        accessDepth: peerAccess.accessDepth,
        deepLinkAllowed: peerAccess.accessDepth === ACCESS_DEPTH.DEEP,
      };
      rows.push(
        peerAccess.accessDepth === ACCESS_DEPTH.COMPARATIVE ? toBenchmarkSafeEntityView(row) : row
      );
    }
  }

  return {
    ok: true,
    allowed: true,
    accessDepth: access.accessDepth,
    brandId,
    geography: effectiveGeo,
    provider,
    language: language || "en",
    availableLanguages: langResolved.availableLanguages,
    languageFilterContract: langResolved.filterContract,
    SILENT_LANGUAGE_FALLBACK: false,
    competitors: rows,
    peerSetId: latest.peerSet?.peerSetId || null,
    brandReadServiceVersion: BRAND_READ_SERVICE_VERSION,
  };
}

/**
 * Recurring sources from provider citations (non-causal).
 */
export async function getBrandSourcesPayload(args = {}) {
  const {
    dealalityUser,
    viewerContext,
    entitlementGraph,
    store,
    brandId,
    provider: providerArg = DEFAULT_AI_VISIBILITY_PROVIDER,
    geography,
    language: languageArg = null,
  } = args;

  const provider = resolveProviderId(providerArg);

  const viewer = viewerContext || normalizeAiVisibilityViewerContext(dealalityUser);
  const access = resolveAiIntelligenceAccess({
    viewerContext: viewer,
    subject: { subjectType: "brand", subjectEntityId: brandId },
    entitlementGraph,
  });
  if (!access.allowed) {
    return { ok: false, allowed: false, reasonCode: access.reasonCode };
  }

  const geo = parseGeographyQuery({ geography, ...args });
  const effectiveGeo = geo.geographyScope
    ? geo
    : { geographyScope: "Region", commercialRegion: "CALA", key: "CALA" };

  const { allProvidersMode, dataProvider } = await resolveAllProvidersReadScaffold({
    store,
    provider,
    effectiveGeo,
  });

  const langResolved = await resolveMonitoringLanguageForRead({
    store,
    provider: dataProvider,
    geographyFilter: effectiveGeo,
    language: languageArg,
  });
  if (!langResolved.ok) {
    return {
      ok: false,
      allowed: true,
      accessDepth: access.accessDepth,
      reasonCode: langResolved.reasonCode,
      message: langResolved.message,
      availableLanguages: langResolved.availableLanguages,
      languageFilterContract: langResolved.filterContract,
      SILENT_LANGUAGE_FALLBACK: false,
      ALL_PROVIDERS_DERIVED: allProvidersMode,
    };
  }
  const language = langResolved.language;
  if (langResolved.status === "not_monitored" && languageArg) {
    return {
      ok: true,
      allowed: true,
      accessDepth: access.accessDepth,
      brandId,
      provider,
      geography: effectiveGeo,
      language,
      availableLanguages: langResolved.availableLanguages,
      languageFilterContract: langResolved.filterContract,
      SILENT_LANGUAGE_FALLBACK: false,
      availability: AVAILABILITY.NOT_MONITORED,
      availabilityReason: MONITORING_STATE.NO_BATCH,
      availabilityMessage: `No ${language === "es" ? "Spanish" : "English"} monitoring data is available for this brand in ${effectiveGeo.key} yet.`,
      title: "Sources Appearing in AI Answers",
      sources: [],
      CAUSAL_LANGUAGE_USED: false,
      INFLUENCE_SCORE_CREATED: false,
      citationRateReadiness: "PARTIAL",
      ALL_PROVIDERS_DERIVED: allProvidersMode,
      brandReadServiceVersion: BRAND_READ_SERVICE_VERSION,
    };
  }

  const summaries = await findMatchingSummaries(store, effectiveGeo, dataProvider, {
    language: language || "en",
  });
  const domainMap = new Map();
  const footprintRows = [];

  for (const summary of summaries.slice(0, 5)) {
    const runs = (await store.listBatchRuns(summary.batchId)) || [];
    const matchedSlots = summary._matchedSlotKeys || null;
    const providerForIndex =
      summary.provider?.name || summary.provider || dataProvider || null;
    const evidenceIndex = await buildEvidenceResolutionIndex(store, {
      batchId: summary.batchId,
      provider: providerForIndex,
    });
    for (const run of runs) {
      if (run.status !== "completed") continue;
      if (!runMatchesSlotFilter(run, matchedSlots)) continue;
      const resolved = await resolveEvidenceForRun(store, run, {
        index: evidenceIndex,
        batchId: summary.batchId,
        provider: providerForIndex,
      });
      if (
        resolved.mode === EVIDENCE_RESOLUTION_MODES.AMBIGUOUS_EVIDENCE_LINK ||
        !resolved.evidence
      ) {
        continue;
      }
      const evidence = resolved.evidence;
      if (
        language &&
        !recordMatchesLanguage(
          { language: evidence.language ?? evidence.payload?.language ?? run.language },
          language,
          { treatMissingAsEn: true }
        )
      ) {
        continue;
      }
      const citations = evidence.payload?.citations || [];
      const mentions = evidence.payload?.mentions || [];
      const searchResults = evidence.payload?.searchResults || [];
      footprintRows.push({
        responseId: evidence.responseId || run.responseId,
        runId: run.runId,
        promptId: evidence.promptId || run.promptId,
        provider: dataProvider,
        language: evidence.language || run.language,
        geographyKey: run.geographyKey,
        intent: run.intent || evidence.intent,
        mentions,
        citations,
        searchResults,
      });
      for (const c of citations) {
        const domain = c.domain || c.sourceDomain || null;
        if (!domain) continue;
        const key = String(domain).toLowerCase();
        const prev = domainMap.get(key) || {
          domain: key,
          occurrenceCount: 0,
          relatedPromptIds: new Set(),
          evidenceIds: new Set(),
          associatedEntityIds: new Set(),
          responseIds: new Set(),
        };
        prev.occurrenceCount += 1;
        if (evidence.promptId) prev.relatedPromptIds.add(evidence.promptId);
        prev.evidenceIds.add(evidence.evidenceId);
        if (evidence.responseId || run.responseId) {
          prev.responseIds.add(evidence.responseId || run.responseId);
        }
        if (c.associatedEntityId) prev.associatedEntityIds.add(c.associatedEntityId);
        domainMap.set(key, prev);
      }
    }
  }

  const sources = filterFixtureContaminatedSources(
    [...domainMap.values()]
      .map((s) => ({
        domain: s.domain,
        occurrenceCount: s.occurrenceCount,
        responsesAppearingIn: s.responseIds?.size || s.evidenceIds.size,
        relatedPromptIds: [...s.relatedPromptIds],
        evidenceIds: [...s.evidenceIds],
        associatedEntityIds: [...s.associatedEntityIds],
        label: "Appearing in AI answers",
      }))
      .sort((a, b) => {
        const ra = a.responsesAppearingIn || 0;
        const rb = b.responsesAppearingIn || 0;
        if (rb !== ra) return rb - ra;
        return b.occurrenceCount - a.occurrenceCount;
      })
  );

  const evidenceFootprint = buildEvidenceFootprint(footprintRows, { entityId: brandId });
  const comparableResponses = footprintRows.length;
  const citedSourceIntelligence = buildCitedSourceIntelligence(
    footprintRows.map((r) => ({ ...r, success: true })),
    { comparableResponses }
  );
  const domainsByProvider = {};
  for (const row of footprintRows) {
    const pid = normalizeProviderId(row.provider) || provider;
    if (!domainsByProvider[pid]) domainsByProvider[pid] = [];
    for (const c of row.citations || []) {
      const d = (c.domain || "").toLowerCase();
      if (d) domainsByProvider[pid].push(d);
    }
  }
  for (const pid of Object.keys(domainsByProvider)) {
    domainsByProvider[pid] = [...new Set(domainsByProvider[pid])];
  }
  const sourceOverlap = buildSourceOverlapBetweenProviders(domainsByProvider);

  return {
    ok: true,
    allowed: true,
    accessDepth: access.accessDepth,
    brandId,
    provider,
    geography: effectiveGeo,
    language: language || "en",
    availableLanguages: langResolved.availableLanguages,
    languageFilterContract: langResolved.filterContract,
    SILENT_LANGUAGE_FALLBACK: false,
    title: "Sources Appearing in AI Answers",
    sources,
    evidenceFootprint,
    citedSourceIntelligence: {
      topCitedSources: (citedSourceIntelligence.TOP_CITED_SOURCES || []).slice(0, 20),
      sortMethod: citedSourceIntelligence.SORT_METHOD,
      ownedClassificationReady: citedSourceIntelligence.OWNED_SOURCE_CLASSIFICATION_READY,
      longitudinal: citedSourceIntelligence.LONGITUDINAL,
      readership: citedSourceIntelligence.READERSHIP,
      byProvider: citedSourceIntelligence.BY_PROVIDER,
      SOURCE_OVERLAP_BETWEEN_PROVIDERS: sourceOverlap.SOURCE_OVERLAP_BETWEEN_PROVIDERS,
      COMPARABLE_RESPONSES: comparableResponses,
      READY: citedSourceIntelligence.READY,
    },
    sourceIntelligence: {
      CITED_DOMAINS: sources.map((s) => s.domain),
      ASSOCIATED_DOMAINS: sources.map((s) => s.domain),
      DOMAIN_FREQUENCY: (citedSourceIntelligence.TOP_CITED_SOURCES || []).map((s) => ({
        domain: s.domain,
        occurrenceCount: s.CITATION_OCCURRENCES ?? s.citationCount,
        responsesAppearingIn: s.RESPONSES_CITING_SOURCE ?? s.responsesAppearingIn,
        RESPONSES_CITING_SOURCE: s.RESPONSES_CITING_SOURCE,
        COMPARABLE_RESPONSES: s.COMPARABLE_RESPONSES,
        SOURCE_CITATION_FREQUENCY: s.SOURCE_CITATION_FREQUENCY,
        SOURCE_CITATION_FREQUENCY_DISPLAY: s.SOURCE_CITATION_FREQUENCY_DISPLAY,
        CITATION_OCCURRENCES: s.CITATION_OCCURRENCES,
        PROMPT_FAMILIES_CITING_SOURCE: s.PROMPT_FAMILIES_CITING_SOURCE || [],
      })),
      DOMAINS_BY_PROVIDER: domainsByProvider,
      DOMAINS_BY_PROMPT_FAMILY: citedSourceIntelligence.BY_OWNER_DECISION || {},
      SOURCE_OVERLAP_BETWEEN_PROVIDERS: sourceOverlap.SOURCE_OVERLAP_BETWEEN_PROVIDERS,
      INFLUENCING_SOURCES_LABEL: false,
      CAUSAL_LANGUAGE_USED: false,
    },
    evidenceAssociationLevel: providerEvidenceAssociationMap()[String(provider).toUpperCase()] || null,
    CAUSAL_LANGUAGE_USED: false,
    INFLUENCE_SCORE_CREATED: false,
    DUPLICATE_METRICS_ADDED: false,
    citationRateReadiness: "PARTIAL",
    brandReadServiceVersion: BRAND_READ_SERVICE_VERSION,
  };
}

/**
 * Evidence drawer payload with depth filtering.
 */
export async function getBrandEvidencePayload(args = {}) {
  const {
    dealalityUser,
    viewerContext,
    entitlementGraph,
    store,
    brandId,
    evidenceId,
    provider: providerArg = DEFAULT_AI_VISIBILITY_PROVIDER,
    geography,
    language: languageArg = null,
  } = args;

  const provider = resolveProviderId(providerArg);
  const allProvidersMode = isAllProvidersSelector(provider);

  const viewer = viewerContext || normalizeAiVisibilityViewerContext(dealalityUser);
  const access = resolveAiIntelligenceAccess({
    viewerContext: viewer,
    subject: { subjectType: "brand", subjectEntityId: brandId },
    entitlementGraph,
  });
  if (!access.allowed) {
    return { ok: false, allowed: false, reasonCode: access.reasonCode, evidence: [] };
  }

  const geo = parseGeographyQuery({ geography, ...args });
  const effectiveGeo = geo.geographyScope
    ? geo
    : { geographyScope: "Region", commercialRegion: "CALA", key: "CALA" };

  const { dataProvider } = await resolveAllProvidersReadScaffold({
    store,
    provider,
    effectiveGeo,
  });

  const langResolved = await resolveMonitoringLanguageForRead({
    store,
    provider: dataProvider,
    geographyFilter: effectiveGeo,
    language: languageArg,
  });
  if (!langResolved.ok) {
    return {
      ok: false,
      allowed: true,
      accessDepth: access.accessDepth,
      reasonCode: langResolved.reasonCode,
      message: langResolved.message,
      availableLanguages: langResolved.availableLanguages,
      languageFilterContract: langResolved.filterContract,
      SILENT_LANGUAGE_FALLBACK: false,
      evidence: [],
    };
  }
  const language = langResolved.language;
  if (langResolved.status === "not_monitored" && languageArg) {
    return {
      ok: true,
      allowed: true,
      accessDepth: access.accessDepth,
      reasonCode: access.reasonCode,
      brandId,
      provider,
      language,
      availableLanguages: langResolved.availableLanguages,
      languageFilterContract: langResolved.filterContract,
      SILENT_LANGUAGE_FALLBACK: false,
      availability: AVAILABILITY.NOT_MONITORED,
      availabilityReason: MONITORING_STATE.NO_BATCH,
      availabilityMessage: `No ${language === "es" ? "Spanish" : "English"} monitoring data is available for this brand in ${effectiveGeo.key} yet.`,
      evidence: [],
      brandReadServiceVersion: BRAND_READ_SERVICE_VERSION,
    };
  }

  let records = [];
  let emptyReason = null;
  let emptyMeta = null;
  if (evidenceId) {
    const one = await store.getEvidence(evidenceId);
    if (!one) {
      return {
        ok: true,
        allowed: true,
        accessDepth: access.accessDepth,
        reasonCode: access.reasonCode,
        emptyReason: "EVIDENCE_NOT_FOUND",
        evidence: [],
        brandId,
        provider,
        language: language || "en",
        availableLanguages: langResolved.availableLanguages,
        languageFilterContract: langResolved.filterContract,
        SILENT_LANGUAGE_FALLBACK: false,
        brandReadServiceVersion: BRAND_READ_SERVICE_VERSION,
      };
    }
    records = [one];
  } else if (typeof store.listEvidence === "function") {
    records = await store.listEvidence({
      entityId: brandId,
      provider: allProvidersMode ? undefined : dataProvider,
      language: language || "en",
    });
  }

  // Provider-pure evidence when a specific provider is selected.
  // All Providers may include all known provider rows (still not an All Providers run).
  const beforeProvider = records;
  records = (records || []).filter((ev) => {
    const evProvider = normalizeProviderId(ev.provider) || DEFAULT_AI_VISIBILITY_PROVIDER;
    if (allProvidersMode) {
      return KNOWN_AI_VISIBILITY_PROVIDER_IDS.includes(evProvider) || Boolean(evProvider);
    }
    return evProvider === dataProvider;
  });
  if (evidenceId && beforeProvider.length && !records.length) {
    emptyReason = "PROVIDER_MISMATCH";
    emptyMeta = {
      evidenceProvider: normalizeProviderId(beforeProvider[0]?.provider) || null,
      requestedProvider: dataProvider,
    };
  }

  // Language-pure — never leak cross-language evidence (incl. evidenceId lookup).
  const wantLanguage = language || "en";
  const beforeLanguage = records;
  records = records.filter((ev) =>
    recordMatchesLanguage(
      { language: ev.language ?? ev.payload?.language },
      wantLanguage,
      { treatMissingAsEn: true }
    )
  );
  if (evidenceId && beforeLanguage.length && !records.length && !emptyReason) {
    emptyReason = "LANGUAGE_MISMATCH";
    emptyMeta = {
      evidenceLanguage: normalizeLanguage(
        beforeLanguage[0]?.language ?? beforeLanguage[0]?.payload?.language
      ),
      requestedLanguage: wantLanguage,
    };
  }

  const safeRecords = records.map((ev) => sanitizeEvidenceForClient(ev, brandId));
  const filtered = filterEvidenceByAccessDepth(safeRecords, {
    accessDepth: access.accessDepth,
    subjectEntityId: brandId,
    entitledEntityIds: entitlementGraph?.entitledBrandIds || [],
  });

  if (
    evidenceId &&
    !filtered.evidence?.length &&
    !emptyReason &&
    safeRecords.length &&
    !filtered.ok
  ) {
    emptyReason = "ACCESS_DEPTH";
  } else if (evidenceId && !filtered.evidence?.length && !emptyReason && !safeRecords.length) {
    emptyReason = emptyReason || "EVIDENCE_NOT_FOUND";
  } else if (
    evidenceId &&
    !filtered.evidence?.length &&
    !emptyReason &&
    filtered.reason === "UNAUTHORIZED_EVIDENCE"
  ) {
    emptyReason = "ACCESS_DEPTH";
  }

  return {
    ok: filtered.ok,
    allowed: true,
    accessDepth: access.accessDepth,
    reasonCode: access.reasonCode,
    filterReason: filtered.reason,
    emptyReason: filtered.evidence?.length ? null : emptyReason,
    emptyMeta: filtered.evidence?.length ? null : emptyMeta,
    evidence: filtered.evidence,
    provider,
    language: wantLanguage,
    availableLanguages: langResolved.availableLanguages,
    languageFilterContract: langResolved.filterContract,
    SILENT_LANGUAGE_FALLBACK: false,
    brandReadServiceVersion: BRAND_READ_SERVICE_VERSION,
  };
}

function sanitizeEvidenceForClient(ev, brandId) {
  const mentions = ev.payload?.mentions || [];
  const subject = mentions.find((m) => m.entityId === brandId || m.resolvedEntityId === brandId);
  const text = ev.payload?.rawResponseText || "";
  const excerpt = text ? String(text).slice(0, 2800) : null;
  const citations = (ev.payload?.citations || []).map((c) => ({
    url: c.url || null,
    domain: c.domain || c.sourceDomain || null,
  }));
  const domains = citations.map((c) => c.domain).filter(Boolean);
  const presenceObserved = Boolean(subject);
  return {
    evidenceId: ev.evidenceId,
    entityId: brandId,
    entityName: subject?.entityName || null,
    promptId: ev.promptId,
    promptText: ev.promptText,
    mentionRole: subject?.role || null,
    brandStatus: presenceObserved ? "Present" : "Missing",
    provider: ev.provider,
    model: ev.model,
    geographyScope: ev.geographyScope,
    commercialRegion: ev.regionName,
    batchId: ev.batchId || null,
    timestamp: ev.timestamp,
    responseId: ev.responseId || ev.payload?.responseId || null,
    responseExcerpt: excerpt,
    citations,
    citationCount: citations.length,
    presenceObserved,
    /** Traceability: Metric → provider → prompt → response → entity → citations */
    drilldownTrace: {
      metric: "AI_SIGNAL_PRESENCE",
      provider: ev.provider || null,
      promptId: ev.promptId || null,
      promptText: ev.promptText || null,
      responseId: ev.responseId || ev.payload?.responseId || null,
      evidenceId: ev.evidenceId || null,
      canonicalEntityId: brandId,
      canonicalEntityOccurrence: Boolean(subject),
      entityMentionSpan: subject?.text || subject?.span || null,
      citationDomains: domains,
      CAUSAL_LANGUAGE_USED: false,
    },
    evidenceDescriptors: buildEvidenceDescriptors({
      promptCount: 1,
      periodCount: 1,
      citationCount: citations.length,
      observationCount: 1,
    }),
  };
}

function futureOpportunityQueue() {
  return {
    status: AVAILABILITY.FUTURE_READY,
    available: false,
    opportunities: [],
    note: "FUTURE-READY — NO GOVERNED DATA YET",
    COMPONENT_PATH_PRESERVED: true,
    FAKE_OPPORTUNITIES_CREATED: false,
  };
}

