/**
 * Brand AI Visibility — Executive Summary (portfolio / company briefing).
 * Factual deterministic aggregates only. No portfolio composite score.
 * Reuses Brand read monitoring-state + portfolio payloads.
 */

import { ACCESS_DEPTH } from "./access-depth.js";
import { AVAILABILITY } from "./availability-states.js";
import { resolveAiIntelligenceAccess } from "./authorization.js";
import {
  HEADLINE_GEOGRAPHIES,
  getBrandPortfolioPayload,
  getBrandCompetitorsPayload,
  getBrandSourcesPayload,
  getBrandTrendPayload,
  parseGeographyQuery,
  resolveBrandGeographyMonitoringState,
  resolveMonitoringLanguageForRead,
  findMatchingSummaries,
} from "./brand-read-service.js";
import {
  DEFAULT_AI_VISIBILITY_PROVIDER,
  isAllProvidersSelector,
  KNOWN_AI_VISIBILITY_PROVIDER_IDS,
  pickScaffoldDataProvider,
  resolveProviderId,
} from "./provider-dimension.js";
import { buildCrossProviderPresenceIntelligence } from "./cross-provider-presence.js";
import { buildExecutiveInsightBoxes } from "./brand-executive-insights.js";
import {
  buildExecutiveFindings,
  executiveFindingsToInsightBoxes,
} from "./executive-finding-engine.js";
import { peerSetBrandNamesById, PEER_SET_ID_V2 } from "./peer-sets.js";
import {
  computeComparablePresenceDelta,
  TREND_LANGUAGE_MATCH_REQUIRED,
} from "./trend-comparability.js";
import {
  buildDiscoverabilityExecutivePlaceholder,
  buildOpenAiDiscoverabilityExecutivePlaceholder,
} from "./future-discoverability.js";
import { normalizeAiVisibilityViewerContext } from "./viewer-context.js";
import { loadObservationsFromBatchSummary } from "./cohort-observations.js";
import { computePortfolioQuestionMetrics } from "./portfolio-question-metrics.js";
import {
  computePortfolioCrossProviderQuestionsMissing,
  loadObservationsByProviderForCohort,
} from "./cross-provider-questions.js";
import {
  PRIMARY_PORTFOLIO_KPI_LABEL,
  PRIMARY_PORTFOLIO_KPI_RECOMMENDATION,
  CLIENT_METRIC_DEFINITIONS,
  listClientMetricDefinitions,
} from "./client-metric-definitions.js";
import { buildMonitoringFreshness } from "./monitoring-freshness.js";
import {
  buildSourceExecutivePanel,
  describePresenceCitationRelationship,
} from "./citation-intelligence.js";
import {
  buildPromptFamilyMissingRollup,
  buildPeerPresentSubjectMissing,
  groupQuestionsMissingWatchlist,
} from "./questions-missing-intelligence.js";
import { buildCompetitiveGapView } from "./competitive-gap-intelligence.js";
import { buildDiscoverabilityPhase3c2Contract } from "./discoverability-phase3c2.js";
import { parseDomain } from "./extract-citations.js";
import {
  buildDiscoverabilityProductPayload,
  resolveOwnedDomainsForBrand,
  resolvePortfolioOwnedDomains,
} from "./brand-website-wiring.js";
import { listAvailableGovernedDomainFields } from "./owned-domain-resolution.js";
import { computeResponseCitationRates } from "./citation-intelligence.js";
import { loadLatestPhase3c2Report } from "./phase3c2-orchestrator.js";

/** Lazy import avoids executive-summary → HDV → brand-read cycle at load time. */
async function loadHdvPayload(args) {
  const { getHotelDecisionVisibilityPayload } = await import("./hotel-decision-visibility.js");
  return getHotelDecisionVisibilityPayload(args);
}

export const BRAND_EXECUTIVE_SUMMARY_VERSION =
  "ai_visibility_brand_executive_summary_v3_completion_wave2";

function formatPresenceRate(v) {
  if (v == null || !Number.isFinite(v)) return null;
  const n = v <= 1 ? v * 100 : v;
  return `${(Math.round(n * 10) / 10).toFixed(1)}%`;
}

function isMonitoredAvailability(avail) {
  return (
    avail === AVAILABILITY.OBSERVED ||
    avail === AVAILABILITY.ZERO ||
    avail === AVAILABILITY.PARTIAL
  );
}

function presenceValue(row) {
  const a = row?.aiPresence;
  if (!a || !isMonitoredAvailability(a.availability)) return null;
  if (typeof a.value === "number" && Number.isFinite(a.value)) return a.value;
  return null;
}

function rankValue(row) {
  const c = row?.competitivePosition;
  if (!c || c.availability !== AVAILABILITY.OBSERVED) return null;
  if (typeof c.rank === "number" && Number.isFinite(c.rank)) return c.rank;
  return null;
}

/**
 * @param {object} args
 */
export async function getBrandExecutiveSummaryPayload(args = {}) {
  const {
    dealalityUser,
    viewerContext,
    entitlementGraph,
    store,
    provider: providerArg = DEFAULT_AI_VISIBILITY_PROVIDER,
    geography,
    language: languageArg = null,
    brandNamesById = {},
    brandBasicsById = {},
  } = args;

  const provider = resolveProviderId(providerArg);
  const allProvidersMode = isAllProvidersSelector(provider);
  const viewer = viewerContext || normalizeAiVisibilityViewerContext(dealalityUser);
  const geo = parseGeographyQuery({ geography, ...args });
  const effectiveGeo = geo.geographyScope
    ? geo
    : { geographyScope: "Region", commercialRegion: "CALA", country: null, key: "CALA" };

  const portfolio = await getBrandPortfolioPayload({
    dealalityUser,
    viewerContext: viewer,
    entitlementGraph,
    store,
    provider,
    geography: effectiveGeo.key || geography || "CALA",
    language: languageArg,
    brandNamesById,
  });

  const dataProvider =
    allProvidersMode && portfolio.scaffoldProvider
      ? portfolio.scaffoldProvider
      : allProvidersMode
        ? pickScaffoldDataProvider(portfolio.availableProviders)
        : provider;

  const langResolved = await resolveMonitoringLanguageForRead({
    store,
    provider: dataProvider,
    geographyFilter: effectiveGeo,
    language: languageArg || portfolio.language || null,
  });
  const language =
    langResolved.ok && langResolved.status !== "not_monitored"
      ? langResolved.language || portfolio.language || "en"
      : portfolio.language || langResolved.language || "en";

  const brands = Array.isArray(portfolio.brands) ? portfolio.brands : [];
  const entitledCount = brands.length;
  const monitoredBrands = brands.filter((b) => {
    if (isMonitoredAvailability(b.aiPresence?.availability)) return true;
    // All Providers: brand counts as monitored when any provider completed, even if
    // cross-provider average is still resolving / soft-comparable.
    if (
      allProvidersMode &&
      Array.isArray(b.crossProviderPresence?.PROVIDERS_MONITORED) &&
      b.crossProviderPresence.PROVIDERS_MONITORED.length > 0
    ) {
      return true;
    }
    return false;
  });

  let topByPresence = null;
  let topByRank = null;
  let questionsMissingSum = 0;
  let missingValid = false;
  let portfolioQuestionMeta = null;
  /** @type {object[]} */
  let cohortObservations = [];

  // Unique-prompt portfolio question metrics.
  // Provider-specific: scaffold provider observations.
  // All Providers: cross-provider derived (never OpenAI-only proxy).
  /** @type {Array<{evidenceId?: string, responseId?: string, provider?: string, promptId?: string, label?: string, presenceObserved?: boolean, kind?: string}>} */
  let evidenceDeepLinks = [];
  /** @type {object|null} */
  let crossProviderQm = null;
  try {
    if (allProvidersMode) {
      const measuredIds = (portfolio.availableProviders || [])
        .map((p) => p.id || p)
        .filter(Boolean);
      const providerIds = measuredIds.length
        ? measuredIds
        : [...KNOWN_AI_VISIBILITY_PROVIDER_IDS];
      const byProvider = await loadObservationsByProviderForCohort({
        store,
        geoFilter: effectiveGeo,
        language: language || "en",
        providers: providerIds,
      });
      // Prefer any monitored provider's observations for citation panel / evidence samples.
      const preferredPack =
        byProvider[dataProvider] ||
        Object.values(byProvider).find((p) => p?.monitored) ||
        null;
      cohortObservations = preferredPack?.observations || [];
      const entitledIds = monitoredBrands.map((b) => b.brandId).filter(Boolean);
      crossProviderQm = computePortfolioCrossProviderQuestionsMissing({
        byProvider,
        entitledBrandIds: entitledIds,
      });
      if (crossProviderQm.denominator > 0) {
        questionsMissingSum = crossProviderQm.questionsMissingCount;
        missingValid = true;
        portfolioQuestionMeta = {
          eligiblePromptCount: crossProviderQm.denominator,
          questionsMissingCount: crossProviderQm.questionsMissingCount,
          questionsMissingRate: crossProviderQm.questionsMissingRate,
          aggregation: crossProviderQm.aggregation,
          OPENAI_SCAFFOLD: false,
        };
      }
    } else {
      const summaries = await findMatchingSummaries(store, effectiveGeo, dataProvider, {
        language: language || "en",
      });
      const latest = summaries[0];
      if (latest) {
        const { observations } = await loadObservationsFromBatchSummary(store, latest, {
          matchedSlotKeys: latest._matchedSlotKeys?.length
            ? latest._matchedSlotKeys
            : undefined,
          language: language || "en",
        });
        cohortObservations = observations || [];
        const entitledIds = monitoredBrands.map((b) => b.brandId).filter(Boolean);
        portfolioQuestionMeta = computePortfolioQuestionMetrics(
          cohortObservations,
          entitledIds
        );
        if (portfolioQuestionMeta.eligiblePromptCount > 0) {
          questionsMissingSum = portfolioQuestionMeta.questionsMissingCount;
          missingValid = true;
        }
      }
    }
    // Sample actionable evidence refs for insights (cap 5; no technical IDs on Exec copy).
    const entitledIds = monitoredBrands.map((b) => b.brandId).filter(Boolean);
    const entitledSet = new Set(entitledIds);
    for (const obs of cohortObservations) {
      if (evidenceDeepLinks.length >= 5) break;
      const mentions = obs.mentions || obs.payload?.mentions || [];
      const hit =
        (obs.presentEntityIds || []).some((id) => entitledSet.has(id)) ||
        mentions.some(
          (m) =>
            entitledSet.has(m.entityId) ||
            entitledSet.has(m.resolvedEntityId) ||
            entitledSet.has(m.canonicalEntityId)
        );
      const evidenceId = obs.evidenceId || obs.payload?.evidenceId;
      if (!evidenceId) continue;
      evidenceDeepLinks.push({
        evidenceId,
        responseId: obs.responseId || obs.payload?.responseId || null,
        provider: obs.provider || dataProvider,
        promptId: obs.promptId || null,
        label: hit ? "Open presence evidence" : "Open missing-presence evidence",
        presenceObserved: hit,
        kind: hit ? "present" : "missing",
      });
    }
  } catch (err) {
    if (typeof console !== "undefined" && console.warn) {
      console.warn("[executive-summary] portfolio question metrics skipped:", err.message);
    }
  }

  for (const b of monitoredBrands) {
    const p = presenceValue(b);
    if (p != null) {
      if (!topByPresence || p > topByPresence.presence) {
        topByPresence = {
          brandId: b.brandId,
          brandName: b.brandName,
          presence: p,
          display: b.aiPresence?.display || formatPresenceRate(p),
          geography: effectiveGeo.key,
        };
      }
    }
    const r = rankValue(b);
    if (r != null) {
      if (!topByRank || r < topByRank.rank) {
        topByRank = {
          brandId: b.brandId,
          brandName: b.brandName,
          rank: r,
          peerCount: b.competitivePosition?.peerCount ?? null,
          display: b.competitivePosition?.display || `#${r}`,
          geography: effectiveGeo.key,
        };
      }
    }
  }

  const portfolioPresenceRate =
    missingValid && portfolioQuestionMeta?.eligiblePromptCount > 0
      ? (portfolioQuestionMeta.eligiblePromptCount - questionsMissingSum) /
        portfolioQuestionMeta.eligiblePromptCount
      : null;

  const currentPosition = {
    /** Primary client KPI — How visible are our brands in AI? */
    portfolioAiPresence: missingValid
      ? {
          value: portfolioPresenceRate,
          availability: AVAILABILITY.OBSERVED,
          display: formatPresenceRate(portfolioPresenceRate) || "—",
          label: PRIMARY_PORTFOLIO_KPI_LABEL,
          helper:
            CLIENT_METRIC_DEFINITIONS.PORTFOLIO_AI_PRESENCE.oneLiner,
        }
      : {
          value: null,
          availability: AVAILABILITY.NOT_MONITORED,
          display: "Not Monitored",
          label: PRIMARY_PORTFOLIO_KPI_LABEL,
        },
    /** Supporting / legacy alias — not primary headline */
    decisionVisibilityCoverage: null,
    brandsMonitored: {
      monitored: monitoredBrands.length,
      entitled: entitledCount,
      display: `${monitoredBrands.length} of ${entitledCount}`,
    },
    topBrandByAiPresence: topByPresence,
    topBrandByCompetitivePosition: topByRank,
    questionsMissing: missingValid
      ? {
          value: questionsMissingSum,
          availability: AVAILABILITY.OBSERVED,
          display:
            portfolioQuestionMeta?.eligiblePromptCount > 0
              ? `${Math.round((questionsMissingSum / portfolioQuestionMeta.eligiblePromptCount) * 1000) / 10}% (${questionsMissingSum})`.replace(
                  /\.0%/,
                  "%"
                )
              : String(questionsMissingSum),
          denominator: portfolioQuestionMeta?.eligiblePromptCount ?? null,
          aggregation: allProvidersMode
            ? "portfolio_no_entitled_brand_on_any_comparable_provider"
            : "unique_prompts_no_entitled_brand_present",
          OPENAI_SCAFFOLD: allProvidersMode ? false : undefined,
          helper: allProvidersMode
            ? CLIENT_METRIC_DEFINITIONS.QUESTIONS_MISSING.allProvidersPortfolioOneLiner
            : CLIENT_METRIC_DEFINITIONS.QUESTIONS_MISSING.oneLiner,
        }
      : {
          value: null,
          availability: AVAILABILITY.NOT_MONITORED,
          display: "Not Monitored",
        },
    portfolioCompositeScore: null,
    QUESTIONS_WON_PRESENT: false,
    PRIMARY_KPI_LABEL: PRIMARY_PORTFOLIO_KPI_LABEL,
  };

  // All Providers: Portfolio AI Presence = mean of brand cross-provider averages
  // (Questions Missing uses cross-provider derived counts above — not OpenAI scaffold).
  if (allProvidersMode) {
    const presenceVals = monitoredBrands
      .map((b) => presenceValue(b))
      .filter((v) => typeof v === "number" && Number.isFinite(v));
    if (presenceVals.length) {
      const avg =
        presenceVals.reduce((sum, v) => sum + v, 0) / presenceVals.length;
      currentPosition.portfolioAiPresence = {
        value: avg,
        availability: AVAILABILITY.OBSERVED,
        display: formatPresenceRate(avg) || "—",
        label: PRIMARY_PORTFOLIO_KPI_LABEL,
        helper:
          CLIENT_METRIC_DEFINITIONS.PORTFOLIO_AI_PRESENCE.allProvidersOneLiner ||
          CLIENT_METRIC_DEFINITIONS.PORTFOLIO_AI_PRESENCE.oneLiner,
        ALL_PROVIDERS_DERIVED: true,
        aggregation: "mean_of_brand_cross_provider_averages",
      };
    }
  }

  // Proprietary HDV + regional table + brand trends — parallel (shared store cache).
  const [hdvPayload, geographySummary, trendResults] = await Promise.all([
    (async () => {
      try {
        return await loadHdvPayload({
          dealalityUser,
          viewerContext: viewer,
          entitlementGraph,
          store,
          provider: dataProvider,
          geography: effectiveGeo.key || geography || "CALA",
          language,
          brandId: null,
          brandNamesById,
          entitledBrands: brands.map((b) => ({
            brandId: b.brandId,
            brandName: b.brandName,
          })),
          includePortfolioOverview: true,
          includeTrendEnrichment: false,
        });
      } catch (err) {
        if (typeof console !== "undefined" && console.warn) {
          console.warn("[executive-summary] HDV merge skipped:", err.message);
        }
        return null;
      }
    })(),
    Promise.all(
      HEADLINE_GEOGRAPHIES.map(async (g) => {
        let monitored = 0;
        let leader = null;
        let bestRank = null;
        // Batch: one summary fetch per geography, then resolve each brand against it (avoid N+1).
        let geoSummaries = [];
        try {
          geoSummaries = await findMatchingSummaries(store, g, dataProvider, {
            language,
          });
        } catch (err) {
          if (typeof console !== "undefined" && console.warn) {
            console.warn(
              "[executive-summary] geography summary fetch failed:",
              g.key,
              err.message
            );
          }
        }
        const brandMons = await Promise.all(
          brands.map(async (b) => {
            const access = resolveAiIntelligenceAccess({
              viewerContext: viewer,
              subject: { subjectType: "brand", subjectEntityId: b.brandId },
              entitlementGraph,
            });
            if (!access.allowed || access.accessDepth !== ACCESS_DEPTH.DEEP) {
              return null;
            }
            const mon = await resolveBrandGeographyMonitoringState({
              store,
              brandId: b.brandId,
              geoFilter: g,
              provider: dataProvider,
              language,
              summaries: geoSummaries,
            });
            return { b, mon };
          })
        );
        for (const hit of brandMons) {
          if (!hit || !hit.mon?.monitored) continue;
          const { b, mon } = hit;
          monitored += 1;
          const p = typeof mon.presenceVal === "number" ? mon.presenceVal : null;
          if (p != null && (!leader || p > leader.presence)) {
            leader = {
              brandId: b.brandId,
              brandName: b.brandName || brandNamesById[b.brandId] || null,
              presence: p,
              display: formatPresenceRate(p),
            };
          }
          if (mon.rank?.rank != null && (!bestRank || mon.rank.rank < bestRank.rank)) {
            bestRank = {
              brandId: b.brandId,
              brandName: b.brandName || brandNamesById[b.brandId] || null,
              rank: mon.rank.rank,
              peerCount: mon.rank.peerCount,
              display: `#${mon.rank.rank}`,
            };
          }
        }

        return {
          geography: g.key,
          geographyScope: g.geographyScope,
          commercialRegion: g.commercialRegion,
          brandsMonitored: monitored,
          brandsEntitled: entitledCount,
          displayMonitored: `${monitored} of ${entitledCount}`,
          availability: monitored > 0 ? AVAILABILITY.OBSERVED : AVAILABILITY.NOT_MONITORED,
          topBrandByAiPresence: leader,
          // Peer rank is provider-specific — never scaffold-rank under All Providers.
          bestCompetitivePosition: allProvidersMode
            ? monitored > 0
              ? {
                  availability: AVAILABILITY.NOT_COMPARABLE,
                  rank: null,
                  peerCount: null,
                  display: "Select a provider for peer rank",
                  ALL_PROVIDERS_PEER_RANK: false,
                }
              : null
            : bestRank,
        };
      })
    ),
    Promise.all(
      monitoredBrands.map(async (b) => {
        const trend = await getBrandTrendPayload({
          dealalityUser,
          viewerContext: viewer,
          entitlementGraph,
          store,
          brandId: b.brandId,
          provider: dataProvider,
          geography: effectiveGeo.key,
          language,
        });
        return { b, trend };
      })
    ),
  ]);

  if (hdvPayload?.ok !== false && hdvPayload?.headline) {
    currentPosition.decisionVisibilityCoverage = hdvPayload.headline.decisionVisibilityCoverage;
    currentPosition.topDecisionTerritory = hdvPayload.headline.topDecisionTerritory;
    // Peer rank is provider-specific — never present a scaffold-provider rank as All Providers.
    if (allProvidersMode) {
      currentPosition.bestCompetitivePosition = {
        availability: AVAILABILITY.NOT_COMPARABLE,
        label: "Best Competitive Position",
        brandId: null,
        brandName: null,
        rank: null,
        peerCount: null,
        display: "Select a provider for peer rank",
        ALL_PROVIDERS_PEER_RANK: false,
      };
      currentPosition.topBrandByCompetitivePosition = null;
    } else {
      currentPosition.bestCompetitivePosition =
        hdvPayload.headline.competitivePositionInOwnerDecisions;
    }
    currentPosition.evidenceBackedReviewItemCount =
      hdvPayload.headline.evidenceBackedReviewItemCount;
  }

  // Enrich portfolio brands with Top Decision Territory from HDV when present
  const hdvBrandById = new Map(
    (hdvPayload?.portfolioOverview?.brands || []).map((b) => [b.brandId, b])
  );
  for (const b of brands) {
    const hb = hdvBrandById.get(b.brandId);
    if (hb?.topIntentTerritory) b.topDecisionTerritory = hb.topIntentTerritory;
    if (hb?.visibilityChange) b.visibilityChange = hb.visibilityChange;
  }

  // AI Presence change + market movement series — comparable history only
  const changeByBrandId = new Map();
  const changes = [];
  const brandTrendSeries = [];
  for (const { b, trend } of trendResults) {
    const points = (trend.points || []).filter(
      (p) => p && typeof p.value === "number" && Number.isFinite(p.value)
    );
    if (points.length) {
      brandTrendSeries.push({
        brandId: b.brandId,
        brandName: b.brandName || b.brandId,
        points,
        latestPresence: points[points.length - 1].value,
      });
    }
    if (points.length >= 2) {
      const latest = points[points.length - 1];
      const prior = points[points.length - 2];
      const deltaResult = computeComparablePresenceDelta(
        {
          ...prior,
          provider: dataProvider,
          geographyKey: effectiveGeo.key,
          language: language || "en",
          metric: "aiPresenceRate",
        },
        {
          ...latest,
          provider: dataProvider,
          geographyKey: effectiveGeo.key,
          language: language || "en",
          metric: "aiPresenceRate",
        }
      );
      if (!deltaResult.ok) {
        changeByBrandId.set(b.brandId, {
          brandId: b.brandId,
          brandName: b.brandName,
          availability: deltaResult.status,
          display: deltaResult.display,
          absoluteDelta: null,
          absoluteDeltaPp: null,
          INVALID_DELTA_BLOCKED: true,
        });
        continue;
      }
      const delta = deltaResult.delta;
      const pp = deltaResult.deltaPp;
      const absPp = Math.abs(pp);
      const direction = delta > 0 ? "up" : delta < 0 ? "down" : "flat";
      const compactDisplay = deltaResult.display;
      const verb =
        delta > 0 ? "increased" : delta < 0 ? "decreased" : "was unchanged";
      const deltaPhrase =
        delta === 0
          ? "unchanged vs the prior comparable monitoring period"
          : `${verb} by ${absPp} pp vs the prior comparable monitoring period`;
      const item = {
        brandId: b.brandId,
        brandName: b.brandName,
        metric: "aiPresenceRate",
        geography: effectiveGeo.key,
        provider: dataProvider,
        priorValue: prior.value,
        latestValue: latest.value,
        absoluteDelta: delta,
        absoluteDeltaPp: pp,
        direction,
        compactDisplay,
        display: `${b.brandName || b.brandId} AI Presence ${deltaPhrase}.`,
        availability: AVAILABILITY.OBSERVED,
      };
      changes.push(item);
      changeByBrandId.set(b.brandId, item);
    }
  }

  // Period keys must be monitoring runs (batchId), not calendar day —
  // same-day repeat batches (e.g. CALA) are distinct periods.
  const periodKeyForPoint = (p) => {
    if (p && p.batchId) return String(p.batchId);
    const s = String((p && p.date) || "");
    return s || "unknown";
  };
  const periodSortValue = (p) =>
    String((p && (p.date || p.batchDate || p.savedAt)) || "");
  const formatPeriodLabel = (p) => {
    const s = periodSortValue(p);
    if (/^\d{4}-\d{2}-\d{2}T/.test(s)) {
      return `${s.slice(0, 10)} · ${s.slice(11, 16)}`;
    }
    if (/^\d{4}-\d{2}-\d{2}/.test(s)) return s.slice(0, 10);
    if (p && p.batchId) {
      const m = String(p.batchId).match(/aiv_batch_(\d{8})/);
      if (m) {
        const d = m[1];
        return `${d.slice(0, 4)}-${d.slice(4, 6)}-${d.slice(6, 8)}`;
      }
      return String(p.batchId).replace(/^aiv_batch_/, "");
    }
    return s || "—";
  };

  const periodMeta = new Map();
  for (const series of brandTrendSeries) {
    for (const p of series.points) {
      const key = periodKeyForPoint(p);
      const existing = periodMeta.get(key);
      const sortDate = periodSortValue(p);
      if (!existing || sortDate > existing.sortDate) {
        periodMeta.set(key, {
          key,
          sortDate,
          label: formatPeriodLabel(p),
        });
      }
    }
  }
  const periods = [...periodMeta.values()].sort((a, b) =>
    String(a.sortDate).localeCompare(String(b.sortDate))
  );
  const labelKeys = periods.map((p) => p.key);
  const labels = periods.map((p) => p.label);
  const MAX_CHART_SERIES = 6;
  const rankedSeries = [...brandTrendSeries].sort(
    (a, b) => (b.latestPresence || 0) - (a.latestPresence || 0)
  );
  const chartSeriesSource = rankedSeries.slice(0, MAX_CHART_SERIES);
  const series = chartSeriesSource.map((s) => {
    const byPeriod = new Map(
      s.points.map((p) => [periodKeyForPoint(p), p.value])
    );
    return {
      brandId: s.brandId,
      brandName: s.brandName,
      pointCount: s.points.length,
      data: labelKeys.map((k) => (byPeriod.has(k) ? byPeriod.get(k) : null)),
    };
  });
  const periodCount = labelKeys.length;
  const chartReady = periodCount >= 2 && series.length >= 1;
  const firstLabel = labels[0] || null;
  const lastLabel = labels.length ? labels[labels.length - 1] : null;
  const marketMovement = {
    metric: "aiPresenceRate",
    geography: effectiveGeo,
    labels,
    labelKeys,
    series,
    periodCount,
    brandSeriesCount: series.length,
    brandsWithPoints: brandTrendSeries.length,
    brandsOmittedFromChart: Math.max(0, brandTrendSeries.length - series.length),
    chartReady,
    headlineValue: chartReady ? String(periodCount) : "—",
    headlineUnit: chartReady ? "periods" : null,
    dateRangeLabel:
      firstLabel && lastLabel
        ? firstLabel === lastLabel
          ? firstLabel
          : `${firstLabel} – ${lastLabel}`
        : null,
    emptyMessage: chartReady
      ? null
      : periodCount === 0
        ? monitoredBrands.length > 0
          ? "Current monitoring exists for this geography, but no AI Presence trend points were stored yet."
          : "Not Monitored for this geography yet — no AI Presence trend points."
        : "Current monitoring is available. Trend will develop as additional comparable monitoring periods are completed.",
    note: "Actual monitoring periods only. No interpolated midpoints. Values are AI Presence rates for entitled brands.",
    yAxisUnit: "percent",
    FAKE_INTERMEDIATE_POINTS: "NONE",
    portfolioCompositeScore: null,
  };

  const portfolioBrands = brands.map((b) => {
    const change = changeByBrandId.get(b.brandId);
    if (change?.INVALID_DELTA_BLOCKED) {
      return {
        ...stripBlockedClientMetrics(b),
        aiPresenceChange: {
          availability: change.availability || AVAILABILITY.NOT_COMPARABLE,
          value: null,
          absoluteDeltaPp: null,
          direction: null,
          display: change.display || "Not Comparable",
          code: change.availability,
          metric: "aiPresenceRate",
          INVALID_DELTA_BLOCKED: true,
        },
      };
    }
    if (change && change.absoluteDelta != null) {
      return {
        ...stripBlockedClientMetrics(b),
        aiPresenceChange: {
          availability: AVAILABILITY.OBSERVED,
          value: change.absoluteDelta,
          absoluteDeltaPp: change.absoluteDeltaPp,
          direction: change.direction,
          display: change.compactDisplay,
          metric: "aiPresenceRate",
        },
      };
    }
    return {
      ...stripBlockedClientMetrics(b),
      aiPresenceChange: {
        availability: AVAILABILITY.INSUFFICIENT_HISTORY,
        value: null,
        absoluteDeltaPp: null,
        direction: null,
        display: "Insufficient History",
        code: "INSUFFICIENT_HISTORY",
        metric: "aiPresenceRate",
        note: "INSUFFICIENT_HISTORY — not zero change.",
      },
    };
  });

  function stripBlockedClientMetrics(b) {
    const {
      recommendationRate,
      recommendationShare,
      top3RecommendationRate,
      firstRecommendationRate,
      questionsWon,
      ...rest
    } = b || {};
    return rest;
  }
  const notableItems = changes.filter((c) => c.absoluteDelta !== 0);
  const whatChanged = {
    items: changes,
    notableItems,
    emptyMessage:
      changes.length === 0 ? "INSUFFICIENT_COMPARABLE_HISTORY" : null,
    notableEmptyMessage:
      changes.length === 0
        ? "INSUFFICIENT_COMPARABLE_HISTORY"
        : notableItems.length === 0
          ? "No material AI Presence moves vs the prior comparable period."
          : null,
    primarySurface: "portfolio_overview_column",
  };

  // Top Strengths — factual only
  const strengths = [];
  if (topByPresence) {
    strengths.push({
      type: "highest_ai_presence",
      label: "Highest AI Presence",
      text: `${topByPresence.brandName || topByPresence.brandId} — ${topByPresence.display || `${Math.round(topByPresence.presence * 100)}%`} in ${effectiveGeo.key}.`,
      brandId: topByPresence.brandId,
    });
  }
  if (topByRank) {
    strengths.push({
      type: "best_competitive_position",
      label: "Best Competitive Position",
      text: `${topByRank.brandName || topByRank.brandId} — ${topByRank.display} in ${effectiveGeo.key}.`,
      brandId: topByRank.brandId,
    });
  }

  // Gaps / Risks — factual non-causal
  const gaps = [];
  const unmonitored = brands.filter((b) => !isMonitoredAvailability(b.aiPresence?.availability));
  for (const b of unmonitored) {
    gaps.push({
      type: "Monitoring Gap",
      label: "Monitoring Gap",
      text: `${b.brandName || b.brandId} has not yet been included in the ${effectiveGeo.key} monitoring cohort.`,
      brandId: b.brandId,
    });
  }
  for (const g of geographySummary) {
    if (g.availability === AVAILABILITY.NOT_MONITORED) {
      gaps.push({
        type: "Missing Coverage",
        label: "Missing Coverage",
        text: `${g.geography} has no completed monitoring batch for entitled brands yet.`,
        geography: g.geography,
      });
    }
  }
  if (missingValid && questionsMissingSum > 0) {
    gaps.push({
      type: "Visibility Gap",
      label: "Visibility Gap",
      text: `${questionsMissingSum} monitored owner questions in ${effectiveGeo.key} where entitled brands were absent.`,
    });
  }

  // Competitive context + sources — parallel once top brand is known
  let competitiveContext = {
    status: AVAILABILITY.NOT_MONITORED,
    message: "No competitive context for monitored entitled brands in this geography yet.",
    leadingPeer: null,
    subjectBrandId: null,
  };
  let evidenceSummary = {
    status: AVAILABILITY.NOT_MONITORED,
    evidenceBackedObservations: null,
    uniqueCitedSources: null,
    recurringDomains: [],
    message: "No cited sources available for monitored entitled brands in this geography yet.",
  };
  /** @type {object[]} */
  let competitorRows = [];

  if (topByPresence) {
    const [comps, sources] = await Promise.all([
      getBrandCompetitorsPayload({
        dealalityUser,
        viewerContext: viewer,
        entitlementGraph,
        store,
        brandId: topByPresence.brandId,
        provider: dataProvider,
        geography: effectiveGeo.key,
        language,
        brandNamesById,
      }),
      getBrandSourcesPayload({
        dealalityUser,
        viewerContext: viewer,
        entitlementGraph,
        store,
        brandId: topByPresence.brandId,
        provider: dataProvider,
        geography: effectiveGeo.key,
        language,
      }),
    ]);
    competitorRows = Array.isArray(comps.competitors) ? comps.competitors : [];
    const peers = competitorRows.filter((p) => !p.isSubject);
    const ranked = [...peers].sort((a, b) => {
      const ra = a.competitivePosition;
      const rb = b.competitivePosition;
      if (ra != null && rb != null) return ra - rb;
      const pa = a.aiPresenceRate;
      const pb = b.aiPresenceRate;
      if (typeof pa === "number" && typeof pb === "number") return pb - pa;
      return 0;
    });
    const leader = ranked[0] || null;
    const leaderPresence = leader?.aiPresenceRate;
    competitiveContext = {
      status: peers.length ? AVAILABILITY.OBSERVED : AVAILABILITY.NOT_MONITORED,
      subjectBrandId: topByPresence.brandId,
      subjectBrandName: topByPresence.brandName,
      leadingPeer: leader
        ? {
            entityId: leader.entityId || null,
            name: leader.entityName || null,
            rank: leader.competitivePosition ?? null,
            presence: leaderPresence ?? null,
            accessDepth: leader.accessDepth || ACCESS_DEPTH.COMPARATIVE,
            deepLinkAllowed: false,
          }
        : null,
      peerCount: peers.length,
      message: leader
        ? `In ${effectiveGeo.key}, ${leader.entityName || "a peer"} leads among observed peers` +
          (typeof leaderPresence === "number"
            ? ` (${formatPresenceRate(leaderPresence)} AI Presence).`
            : ".")
        : competitiveContext.message,
      FAKE_COMPETITOR_DEEP_DIAGNOSTICS: "NONE",
    };

    const list = Array.isArray(sources.sources) ? sources.sources : [];
    const domains = list
      .map((s) => s.domain || s)
      .filter(Boolean)
      .slice(0, 12);
    evidenceSummary = {
      status: list.length ? AVAILABILITY.OBSERVED : AVAILABILITY.NOT_MONITORED,
      evidenceBackedObservations: sources.evidenceBackedObservations ?? null,
      uniqueCitedSources: sources.uniqueCitedSources ?? list.length,
      recurringDomains: domains,
      message: list.length
        ? null
        : "No cited sources available for monitored entitled brands in this geography yet.",
      subjectBrandId: topByPresence.brandId,
    };
  }

  // Priority Review — deterministic watch items only (no priority score / LLM strategy)
  const priorityReviewItems = {
    status: AVAILABILITY.FUTURE_READY,
    items: [],
    message: "No factual watch items for this cohort yet.",
  };
  const factualReview = [];
  for (const g of geographySummary) {
    if (g.availability === AVAILABILITY.NOT_MONITORED && (g.geography === "Global" || g.geography === "North America")) {
      factualReview.push({
        type: "monitoring_coverage",
        text: `Expand monitoring coverage in ${g.geography}.`,
      });
    }
  }
  if (missingValid && questionsMissingSum > 0 && topByPresence) {
    factualReview.push({
      type: "questions_missing_review",
      text: `Review Questions Missing for ${topByPresence.brandName || topByPresence.brandId} in ${effectiveGeo.key}.`,
      brandId: topByPresence.brandId,
    });
  }
  // Final Priority Review assignment happens after peer/citation/discoverability enrichment (Wave 2).

  // Cross-provider Presence (derived) — when All Providers selected or ≥2 measured providers.
  let crossProviderPresence = null;
  const measuredProviders = portfolio.availableProviders || [];
  if (allProvidersMode || measuredProviders.length >= 2) {
    const subjectId = topByPresence?.brandId || monitoredBrands[0]?.brandId || null;
    if (subjectId) {
      const providerRows = [];
      for (const p of measuredProviders) {
        const pid = p.id || p;
        try {
          const mon = await resolveBrandGeographyMonitoringState({
            store,
            brandId: subjectId,
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
        } catch (err) {
          if (typeof console !== "undefined" && console.warn) {
            console.warn("[executive-summary] cross-provider presence skipped:", err.message);
          }
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
      crossProviderPresence = buildCrossProviderPresenceIntelligence({
        entityId: subjectId,
        geography: effectiveGeo.key,
        language: language || "en",
        providers: providerRows,
      });
    }
  }

  let weakestPresence = null;
  for (const b of monitoredBrands) {
    const p = presenceValue(b);
    if (p == null) continue;
    if (!weakestPresence || p < weakestPresence.presence) {
      weakestPresence = {
        brandId: b.brandId,
        brandName: b.brandName,
        presence: p,
        display: b.aiPresence?.display || formatPresenceRate(p),
        geography: effectiveGeo.key,
      };
    }
  }

  const topMove = (whatChanged.notableItems || [])[0] || (whatChanged.items || [])[0] || null;
  const presenceChange = topMove
    ? {
        brandName: topMove.display?.split(" ")[0] || topByPresence?.brandName,
        deltaPp:
          typeof topMove.absoluteDeltaPp === "number"
            ? topMove.absoluteDeltaPp
            : typeof topMove.absoluteDelta === "number"
              ? topMove.absoluteDelta * 100
              : null,
        geography: effectiveGeo.key,
        comparable: true,
      }
    : whatChanged.emptyMessage === "INSUFFICIENT_COMPARABLE_HISTORY"
      ? { comparable: false }
      : null;

  const competitiveGap =
    competitiveContext?.leadingPeer &&
    topByPresence &&
    typeof competitiveContext.leadingPeer.presence === "number" &&
    typeof topByPresence.presence === "number"
      ? {
          subjectName: topByPresence.brandName,
          peerName: competitiveContext.leadingPeer.name,
          subjectPresence: topByPresence.presence,
          peerPresence: competitiveContext.leadingPeer.presence,
          geography: effectiveGeo.key,
        }
      : null;

  // Wave 1 — Questions Missing + competitive peer-present/subject-missing
  const subjectId = topByPresence?.brandId || monitoredBrands[0]?.brandId || null;
  const peerNamesById = {};
  for (const c of competitorRows) {
    if (c.entityId) peerNamesById[c.entityId] = c.entityName || null;
  }
  const peerEntityIds = competitorRows
    .filter((c) => !c.isSubject)
    .map((c) => c.entityId)
    .filter(Boolean);

  const peerPresentSubjectMissing = subjectId
    ? buildPeerPresentSubjectMissing(cohortObservations, {
        subjectBrandId: subjectId,
        peerEntityIds,
        peerNamesById,
      })
    : { rows: [], PEER_PRESENT_SUBJECT_MISSING_N: 0, READY: false };

  competitiveContext.peerGaps = buildCompetitiveGapView({
    subject: {
      name: topByPresence?.brandName,
      presenceRate: topByPresence?.presence,
      competitivePosition: topByRank?.rank ?? null,
      peerCount: topByRank?.peerCount ?? peerEntityIds.length,
      rank: topByRank?.rank ?? null,
    },
    peers: competitorRows,
    PEER_PRESENT_SUBJECT_MISSING_N:
      peerPresentSubjectMissing.PEER_PRESENT_SUBJECT_MISSING_N,
    peerPresentRows: peerPresentSubjectMissing.rows,
    scopeLabel: effectiveGeo.key,
  });
  competitiveContext.PEER_PRESENT_SUBJECT_MISSING_N =
    peerPresentSubjectMissing.PEER_PRESENT_SUBJECT_MISSING_N;

  const promptFamilyMissing = subjectId
    ? buildPromptFamilyMissingRollup(cohortObservations, subjectId)
    : { families: [], TOTAL_MISSING: 0, TOTAL_MONITORED: 0 };

  const questionsMissingWatchlist = allProvidersMode && crossProviderQm
    ? {
        byPromptFamily: groupQuestionsMissingWatchlist(
          (crossProviderQm.rows || [])
            .filter((r) => r.MISSING_ACROSS_ALL_PROVIDERS)
            .map((r) => ({
              QUESTION: r.QUESTION,
              promptFamily: r.PROMPT_FAMILY,
              PROMPT_FAMILY: r.PROMPT_FAMILY,
              provider: "all",
              PROVIDERS_PRESENT: r.PROVIDERS_WITH_ENTITLED_PRESENCE || [],
              PROVIDERS_MISSING: (r.PROVIDERS_MONITORED || []).filter(
                (p) => !(r.PROVIDERS_WITH_ENTITLED_PRESENCE || []).includes(p)
              ),
              CROSS_PROVIDER_STATE: r.CROSS_PROVIDER_STATE,
              region: effectiveGeo.key,
              language: language || "en",
            })),
          "promptFamily"
        ),
        byProvider: [],
        byRegion: [],
        byLanguage: [],
        CLIENT_COPY:
          "Owner questions where none of your linked brands appeared on any comparable provider.",
        CROSS_PROVIDER: true,
        OPENAI_SCAFFOLD: false,
      }
    : {
        byPromptFamily: groupQuestionsMissingWatchlist(
          peerPresentSubjectMissing.rows,
          "promptFamily"
        ),
        byProvider: groupQuestionsMissingWatchlist(
          peerPresentSubjectMissing.rows,
          "provider"
        ),
        byRegion: groupQuestionsMissingWatchlist(peerPresentSubjectMissing.rows, "region"),
        byLanguage: groupQuestionsMissingWatchlist(
          peerPresentSubjectMissing.rows,
          "language"
        ),
        CLIENT_COPY:
          "Brand was not observed in these monitored owner questions.",
      };

  // Citation / Source executive panel — Brand Matrix + governed company portfolio domains
  const portfolioOwned = resolvePortfolioOwnedDomains({
    brandIds: brands.map((b) => b.brandId).filter(Boolean),
    brandNamesById,
    brandBasicsById,
  });
  const ownedDomains = portfolioOwned.ownedDomainList || [];
  const missingGovernedWebsite = [];
  for (const b of brands) {
    const { owned, brandRow } = resolveOwnedDomainsForBrand(b.brandId, {
      brandNamesById,
      brandBasicsById,
    });
    if (owned.OWNED_DOMAIN_STATUS !== "CONFIGURED" && !ownedDomains.length) {
      missingGovernedWebsite.push({
        brandId: b.brandId,
        brandName: b.brandName || brandNamesById[b.brandId] || null,
      });
    }
    if (brandRow.brandWebsite) b.brandWebsite = brandRow.brandWebsite;
  }
  // When portfolio owned domains are configured, brands without individual websites
  // are still covered by company-scope governance — do not treat as missing for KPI readiness.
  if (portfolioOwned.OWNED_DOMAIN_STATUS === "CONFIGURED") {
    missingGovernedWebsite.length = 0;
  }
  const sourceExecutivePanel = buildSourceExecutivePanel(cohortObservations, {
    ownedDomains: portfolioOwned.ownedDomainEntries?.length
      ? portfolioOwned.ownedDomainEntries
      : ownedDomains,
  });
  sourceExecutivePanel.MISSING_GOVERNED_SOURCE = missingGovernedWebsite.length;
  sourceExecutivePanel.missingGovernedWebsiteBrands = missingGovernedWebsite;
  sourceExecutivePanel.ELIGIBLE_BRANDS_WITH_OWNED_DOMAIN =
    brands.length - missingGovernedWebsite.length;
  sourceExecutivePanel.OWNED_DOMAIN_SCOPE = "PORTFOLIO";
  sourceExecutivePanel.PORTFOLIO_OWNED_DOMAINS = ownedDomains;
  sourceExecutivePanel.DOMAIN_SOURCE_MAPPING =
    portfolioOwned.DOMAIN_SOURCE_MAPPING || [];
  sourceExecutivePanel.AVAILABLE_GOVERNED_DOMAIN_FIELDS =
    listAvailableGovernedDomainFields();
  sourceExecutivePanel.USED_OWNED_DOMAIN_FIELDS = portfolioOwned.USED_FIELDS || [];
  sourceExecutivePanel.EXTERNAL_SOURCE_CITATION_RATE =
    sourceExecutivePanel.THIRD_PARTY_CITATION_RATE || null;
  sourceExecutivePanel.EXTERNAL_LABEL_ALIAS = true;

  // Recurring Sources: ranked citation-frequency rows (cited only; cohort-scoped).
  const rankedFrequency = Array.isArray(sourceExecutivePanel.DOMAIN_FREQUENCY)
    ? sourceExecutivePanel.DOMAIN_FREQUENCY
    : [];
  if (rankedFrequency.length) {
    evidenceSummary = {
      ...evidenceSummary,
      status: AVAILABILITY.OBSERVED,
      uniqueCitedSources: rankedFrequency.length,
      recurringDomains: rankedFrequency,
      message: null,
      SOURCE_CITATION_FREQUENCY_DEFINITION:
        sourceExecutivePanel.SOURCE_CITATION_FREQUENCY_DEFINITION || null,
      COMPARABLE_RESPONSES: sourceExecutivePanel.COMPARABLE_RESPONSES ?? null,
    };
  } else if (sourceExecutivePanel.CITATION_SUPPORT === "NOT_SUPPORTED") {
    evidenceSummary = {
      ...evidenceSummary,
      status: AVAILABILITY.UNAVAILABLE,
      recurringDomains: [],
      message:
        "Citation frequency is not supported for this provider cohort.",
    };
  }

  const presenceCitationRelationship = describePresenceCitationRelationship(
    portfolioPresenceRate,
    sourceExecutivePanel.OWNED_SOURCE_CITATION_RATE?.value
  );

  // All Providers executive panel (derived)
  const allProvidersPanel = crossProviderPresence
    ? {
        READY: true,
        DERIVED: true,
        ALL_PROVIDERS_RUN: false,
        PROVIDERS_MONITORED: crossProviderPresence.PROVIDERS_MONITORED,
        PROVIDERS_WHERE_BRAND_APPEARS:
          crossProviderPresence.PROVIDERS_WHERE_BRAND_APPEARS,
        CROSS_PROVIDER_AVERAGE_OBSERVED_PRESENCE:
          crossProviderPresence.CROSS_PROVIDER_AVERAGE_OBSERVED_PRESENCE,
        PRESENCE_BY_PROVIDER: crossProviderPresence.PROVIDER_PRESENCE_BREAKDOWN,
        STRONGEST_PROVIDER: crossProviderPresence.STRONGEST_PROVIDER_BY_PRESENCE,
        WEAKEST_PROVIDER: crossProviderPresence.WEAKEST_PROVIDER_BY_PRESENCE,
        PRESENCE_RANGE: crossProviderPresence.PRESENCE_RANGE,
        PROVIDER_DISAGREEMENT: crossProviderPresence.PROVIDER_DISAGREEMENT,
        QUESTIONS_WITH_ALL_PROVIDER_PRESENCE:
          crossProviderPresence.QUESTIONS_WITH_ALL_PROVIDER_PRESENCE,
        QUESTIONS_WITH_PARTIAL_PROVIDER_PRESENCE:
          crossProviderPresence.QUESTIONS_WITH_PARTIAL_PROVIDER_PRESENCE,
        QUESTIONS_WITH_NO_PROVIDER_PRESENCE:
          crossProviderPresence.QUESTIONS_WITH_NO_PROVIDER_PRESENCE,
        NOT_COMPARABLE: crossProviderPresence.NOT_COMPARABLE === true,
        message: crossProviderPresence.message || null,
        MISSING_PROVIDER_IS_NOT_ZERO: true,
      }
    : {
        READY: false,
        DERIVED: true,
        message: "Cross-provider panel available when All Providers or multi-provider data exists.",
      };

  // Strongest / weakest region from geography summary
  const monitoredGeos = (geographySummary || []).filter(
    (g) => g && g.availability !== "not_monitored" && g.brandsMonitored > 0
  );
  let strongestRegion = null;
  let weakestRegion = null;
  if (monitoredGeos.length) {
    const ranked = [...monitoredGeos].sort(
      (a, b) =>
        (b.topBrandByAiPresence?.presence ?? -1) -
        (a.topBrandByAiPresence?.presence ?? -1)
    );
    strongestRegion = ranked[0]
      ? {
          geography: ranked[0].geography,
          display: ranked[0].topBrandByAiPresence?.display || null,
        }
      : null;
    weakestRegion = ranked[ranked.length - 1]
      ? {
          geography: ranked[ranked.length - 1].geography,
          display: ranked[ranked.length - 1].topBrandByAiPresence?.display || null,
        }
      : null;
  }

  const comparableChange =
    Array.isArray(whatChanged?.items) && whatChanged.items.length
      ? whatChanged.items[0]
      : null;

  const portfolioSnapshot = {
    PORTFOLIO_AI_PRESENCE: currentPosition.portfolioAiPresence,
    BRANDS_MONITORED: currentPosition.brandsMonitored,
    PROVIDERS_MONITORED: allProvidersPanel.PROVIDERS_MONITORED || [
      dataProvider,
    ],
    PROVIDERS_WHERE_PORTFOLIO_APPEARS:
      allProvidersPanel.PROVIDERS_WHERE_BRAND_APPEARS || null,
    STRONGEST_BRAND_BY_PRESENCE: topByPresence,
    WEAKEST_BRAND_BY_PRESENCE: weakestPresence,
    STRONGEST_REGION: strongestRegion,
    WEAKEST_REGION: weakestRegion,
    QUESTIONS_MISSING_N: missingValid ? questionsMissingSum : null,
    COMPARABLE_CHANGE: comparableChange,
  };

  // EN vs ES comparison when both monitored (factual only — no preference language)
  let languageComparison = null;
  const availableLangs =
    langResolved.availableLanguages || portfolio.availableLanguages || [];
  if (
    availableLangs.includes("en") &&
    availableLangs.includes("es") &&
    topByPresence
  ) {
    try {
      const [enMon, esMon] = await Promise.all([
        resolveBrandGeographyMonitoringState({
          store,
          brandId: topByPresence.brandId,
          geoFilter: effectiveGeo,
          provider: dataProvider,
          language: "en",
        }),
        resolveBrandGeographyMonitoringState({
          store,
          brandId: topByPresence.brandId,
          geoFilter: effectiveGeo,
          provider: dataProvider,
          language: "es",
        }),
      ]);

      const { owned: subjectOwned } = resolveOwnedDomainsForBrand(
        topByPresence.brandId,
        { brandNamesById, brandBasicsById }
      );

      let enCitation = null;
      let esCitation = null;
      let enOwned = null;
      let esOwned = null;
      let enMissing = null;
      let esMissing = null;

      async function citationForLang(langMon, lang) {
        if (!langMon?.latestSummary) return null;
        const loaded = await loadObservationsFromBatchSummary(
          store,
          langMon.latestSummary,
          {}
        );
        return computeResponseCitationRates(loaded.observations || [], {
          ownedDomains: subjectOwned.ownedDomainList || [],
        });
      }

      if (enMon.monitored && esMon.monitored) {
        const [enRates, esRates] = await Promise.all([
          citationForLang(enMon, "en"),
          citationForLang(esMon, "es"),
        ]);
        enCitation = enRates?.CITATION_RATE?.value ?? null;
        esCitation = esRates?.CITATION_RATE?.value ?? null;
        enOwned =
          subjectOwned.OWNED_DOMAIN_STATUS === "CONFIGURED"
            ? enRates?.OWNED_SOURCE_CITATION_RATE?.value ?? null
            : null;
        esOwned =
          subjectOwned.OWNED_DOMAIN_STATUS === "CONFIGURED"
            ? esRates?.OWNED_SOURCE_CITATION_RATE?.value ?? null
            : null;
        enMissing =
          typeof enMon.questionsMissing === "number" ? enMon.questionsMissing : null;
        esMissing =
          typeof esMon.questionsMissing === "number" ? esMon.questionsMissing : null;
      }

      const enP =
        typeof enMon.presenceVal === "number" ? enMon.presenceVal : null;
      const esP =
        typeof esMon.presenceVal === "number" ? esMon.presenceVal : null;

      let presenceNote = null;
      if (enP != null && esP != null && enP !== esP) {
        presenceNote =
          enP > esP
            ? "Observed Presence was higher in English than Spanish for this comparable cohort."
            : "Observed Presence was higher in Spanish than English for this comparable cohort.";
      }
      let ownedNote = null;
      if (enOwned != null && esOwned != null) {
        ownedNote = `Official domains were cited in ${Math.round(enOwned * 100)}% of English responses and ${Math.round(esOwned * 100)}% of Spanish responses.`;
      }

      languageComparison = {
        brandId: topByPresence.brandId,
        EN_AI_PRESENCE: enP,
        ES_AI_PRESENCE: esP,
        EN_PRESENCE: enP,
        ES_PRESENCE: esP,
        EN_CITATION_RATE: enCitation,
        ES_CITATION_RATE: esCitation,
        EN_OWNED_SOURCE_CITATION_RATE: enOwned,
        ES_OWNED_SOURCE_CITATION_RATE: esOwned,
        EN_QUESTIONS_MISSING: enMissing,
        ES_QUESTIONS_MISSING: esMissing,
        OWNED_DOMAIN_STATUS: subjectOwned.OWNED_DOMAIN_STATUS,
        presenceNote,
        ownedCitationNote: ownedNote,
        LANGUAGE_PREFERENCE_INTERPRETATION: false,
        CAUSAL_CLAIMS: false,
      };
    } catch (err) {
      if (typeof console !== "undefined" && console.warn) {
        console.warn("[executive-summary] EN/ES comparison skipped:", err.message);
      }
    }
  }

  const subjectBrandId = topByPresence?.brandId || monitoredBrands[0]?.brandId || null;
  const publicDiscoverability = subjectBrandId
    ? buildDiscoverabilityProductPayload(subjectBrandId, {
        brandNamesById,
        brandBasicsById,
      })
    : {
        DISCOVERABILITY: "SOURCE_NOT_CONFIGURED",
        message: "No brand selected for Discoverability baseline.",
        ARBITRARY_DISCOVERABILITY_SCORE: false,
      };

  const discoverabilityPhase3c2 = buildDiscoverabilityPhase3c2Contract({
    brandRow: subjectBrandId
      ? resolveOwnedDomainsForBrand(subjectBrandId, {
          brandNamesById,
          brandBasicsById,
        }).brandRow
      : {},
    language: language || "en",
  });
  const phase3c2Latest = loadLatestPhase3c2Report();

  // Enrich Priority Review with Wave 2 factual watch items
  if (
    peerPresentSubjectMissing?.PEER_PRESENT_SUBJECT_MISSING_N > 0
  ) {
    factualReview.push({
      type: "peer_present_subject_missing",
      text: `${peerPresentSubjectMissing.PEER_PRESENT_SUBJECT_MISSING_N} monitored questions where peers appeared and the subject brand did not.`,
      brandId: subjectBrandId,
    });
  }
  if (crossProviderPresence?.PROVIDER_DISAGREEMENT?.status === "DISAGREE") {
    factualReview.push({
      type: "provider_disagreement",
      text: "Providers disagree on Observed Presence for the comparable cohort.",
      brandId: subjectBrandId,
    });
  }
  if (
    sourceExecutivePanel?.OWNED_SOURCE_CITATION_RATE?.value === 0 &&
    sourceExecutivePanel?.OWNED_SOURCE_CITATION_RATE?.denominator > 0
  ) {
    factualReview.push({
      type: "owned_citation_gap",
      text: "No owned-domain citations were observed in this monitored cohort.",
      brandId: subjectBrandId,
    });
  }
  if (
    publicDiscoverability?.DISCOVERABILITY === "SOURCE_NOT_CONFIGURED" ||
    publicDiscoverability?.OWNER_INTENT_CONTENT_GAPS?.length
  ) {
    factualReview.push({
      type: "discoverability_gap",
      text:
        publicDiscoverability.DISCOVERABILITY === "SOURCE_NOT_CONFIGURED"
          ? "No official brand website has been configured for Public Discoverability."
          : "Owner-intent public content gaps remain for configured official URLs.",
      brandId: subjectBrandId,
    });
  }
  if (presenceChange?.comparable && typeof presenceChange.deltaPp === "number") {
    factualReview.push({
      type: "comparable_presence_change",
      text: `Comparable Presence change of ${presenceChange.deltaPp > 0 ? "+" : ""}${presenceChange.deltaPp} pp observed.`,
      brandId: subjectBrandId,
    });
  }
  if (hdvPayload?.reviewItems?.length) {
    priorityReviewItems.status = AVAILABILITY.OBSERVED;
    priorityReviewItems.items = hdvPayload.reviewItems.slice(0, 5).map((item) => ({
      type: item.type,
      title: item.title,
      text: item.description || item.title,
      description: item.description,
      geography: item.geography,
      provider: item.provider,
      providerLabel: item.providerLabel,
      evidenceId: item.evidenceId,
      brandId: item.brandId,
    }));
    priorityReviewItems.message = null;
    priorityReviewItems.note =
      "Evidence-backed review items (deterministic rules) — not Airtable Opportunities.";
  } else if (factualReview.length) {
    priorityReviewItems.status = AVAILABILITY.OBSERVED;
    priorityReviewItems.items = factualReview.slice(0, 8);
    priorityReviewItems.message = null;
    priorityReviewItems.note =
      "Deterministic watch items from Questions Missing, peer gaps, citations, Discoverability, and comparable change — no priority score.";
  }
  priorityReviewItems.ARBITRARY_PRIORITY_SCORE = false;

  const portfolioBrandNamesById = {};
  for (const b of brands) {
    if (b.brandId) portfolioBrandNamesById[b.brandId] = b.brandName || null;
  }
  const gapBrandNamesById = {
    ...peerSetBrandNamesById(PEER_SET_ID_V2),
    ...portfolioBrandNamesById,
    ...peerNamesById,
  };
  const entitledBrandIds = monitoredBrands.map((b) => b.brandId).filter(Boolean);

  let executiveFindings = null;
  try {
    executiveFindings = await buildExecutiveFindings({
      store,
      brandIds: entitledBrandIds,
      brandNamesById: gapBrandNamesById,
      geographyKey: effectiveGeo.key || "CALA",
      language: language || "en",
      scope: "portfolio",
      crossProvider: crossProviderPresence,
      presenceChange,
      sourceExecutivePanel,
      peerSetId: PEER_SET_ID_V2,
    });
  } catch (err) {
    if (typeof console !== "undefined" && console.warn) {
      console.warn("[brand-executive-summary] executiveFindings skipped:", err.message);
    }
    executiveFindings = {
      IMPLEMENTED: false,
      findings: [],
      totalFindings: 0,
      emptyReason: "engine_error",
    };
  }

  const executiveInsights = buildExecutiveInsightBoxes({
    geographyKey: effectiveGeo.key,
    geographyScope: effectiveGeo.geographyScope || null,
    // Cross-region (Regional Gap) tiles only when the selected view is Global.
    allowCrossRegionTakeaways:
      effectiveGeo.geographyScope === "Global" ||
      String(effectiveGeo.key || "").toLowerCase() === "global",
    providerMode: allProvidersMode ? "DERIVED" : "PROVIDER_SPECIFIC",
    topByPresence,
    weakestPresence,
    // Always pass full summary for Markets & Movement; insight layer ignores it unless Global.
    geographySummary,
    competitiveGap,
    questionsMissing: missingValid
      ? {
          value: questionsMissingSum,
          denominator: portfolioQuestionMeta?.eligiblePromptCount ?? null,
          display: currentPosition.questionsMissing?.display,
        }
      : null,
    presenceChange,
    crossProvider: crossProviderPresence,
    evidenceDeepLinks,
    topMissingPromptFamily:
      promptFamilyMissing?.families?.[0] &&
      promptFamilyMissing.families[0].QUESTIONS_MISSING > 0
        ? promptFamilyMissing.families[0]
        : promptFamilyMissing?.families?.[0] || null,
    brandsMonitoredDisplay: currentPosition.brandsMonitored?.display || null,
    // Executive Summary tiles are the primary takeaway row (target 3–5).
    // Do not suppress material insights merely because lower spine sections also cover related facts.
    suppressStrongestBecauseKpi: false,
    suppressQuestionsMissingBecausePanel: false,
    suppressCompetitiveBecausePanel: false,
    suppressProviderBecausePanel: false,
  });

  const countryUxRecommendation = {
    primaryNavCountries: false,
    supportCountryWhenGovernedPromptsExist: true,
    note: "Do not add dozens of countries to primary geography navigation; expose country only where country-scoped prompts/observations exist.",
  };

  const monitoringFreshness = await buildMonitoringFreshness({
    store,
    geographyScope: effectiveGeo.geographyScope || null,
    commercialRegion: effectiveGeo.commercialRegion || effectiveGeo.key || null,
    country: effectiveGeo.country || null,
    language: language || "en",
    availableProviders: portfolio.availableProviders || [],
    provider,
  });

  return {
    ok: true,
    productSurface: "AI Visibility",
    view: "executive_summary",
    provider,
    providerLabel: portfolio.providerLabel || null,
    providerMode: allProvidersMode ? "DERIVED" : "PROVIDER_SPECIFIC",
    availableProviders: portfolio.availableProviders || [],
    providerSelectorOptions: portfolio.providerSelectorOptions || [],
    providerHasCompletedData: portfolio.providerHasCompletedData !== false,
    monitoringFreshness,
    ALL_PROVIDERS_DERIVED: allProvidersMode,
    ALL_PROVIDERS_RUN: false,
    ALL_PROVIDERS_PROVIDER_RECORD: false,
    NO_SILENT_PROVIDER_FALLBACK: true,
    geography: effectiveGeo,
    language: language || "en",
    availableLanguages: langResolved.availableLanguages || portfolio.availableLanguages || [],
    languageFilterContract: langResolved.filterContract || portfolio.languageFilterContract || null,
    SILENT_LANGUAGE_FALLBACK: false,
    currentPosition,
    portfolioSnapshot,
    allProvidersPanel,
    geographySummary,
    whatChanged,
    marketMovement,
    strengths,
    gaps,
    competitiveContext,
    competitiveIntelligence: competitiveContext.peerGaps || null,
    peerPresentSubjectMissing,
    promptFamilyMissing,
    questionsMissingWatchlist,
    crossProviderQuestions: crossProviderQm,
    OPENAI_SCAFFOLD_REMOVED_FOR_QM: allProvidersMode === true,
    priorityReviewItems,
    evidenceSummary,
    sourceExecutivePanel,
    presenceCitationRelationship,
    languageComparison,
    countryUxRecommendation,
    executiveInsights,
    executiveFindings,
    executiveIntelligenceInsights: executiveFindingsToInsightBoxes(executiveFindings),
    crossProviderPresence,
    clientMetricDefinitions: listClientMetricDefinitions(),
    primaryKpiRecommendation: PRIMARY_PORTFOLIO_KPI_RECOMMENDATION,
    brandV1: {
      CORE_SIGNAL: "AI_SIGNAL_PRESENCE",
      RECOMMENDED_REQUIRED_FOR_V1: false,
      STATUS: "PRESENCE_LED_PRODUCTION_BUILD",
      COMPLETION_WAVE: 2,
    },
    discoverabilityBusinessImpact: buildDiscoverabilityExecutivePlaceholder(),
    openAiDiscoverability: buildOpenAiDiscoverabilityExecutivePlaceholder(),
    discoverabilityPhase3c2,
    publicDiscoverability,
    discoverabilityBaseline: phase3c2Latest
      ? {
          LIVE_BASELINE_EXECUTED: phase3c2Latest.LIVE_BASELINE_EXECUTED === true,
          MODE: phase3c2Latest.MODE,
          BRANDS_CHECKED: phase3c2Latest.BRANDS_CHECKED,
          ACCESSIBLE: phase3c2Latest.ACCESSIBLE,
          CHECK_FAILED: phase3c2Latest.CHECK_FAILED,
          SOURCE_NOT_CONFIGURED: phase3c2Latest.SOURCE_NOT_CONFIGURED,
          ARBITRARY_SCORE: false,
        }
      : null,
    ownedDomainCoverage: {
      ELIGIBLE_BRANDS: sourceExecutivePanel.ELIGIBLE_BRANDS_WITH_OWNED_DOMAIN,
      MISSING_GOVERNED_SOURCE: sourceExecutivePanel.MISSING_GOVERNED_SOURCE,
      missingBrands: missingGovernedWebsite,
    },
    PRESENCE_TO_CITATIONS_TO_DISCOVERABILITY_NARRATIVE: true,
    portfolioOverview: {
      brands: portfolioBrands,
      emptyReason: portfolio.emptyReason || null,
    },
    portfolioCompositeScore: null,
    opportunityQueue: {
      status: AVAILABILITY.FUTURE_READY,
      items: [],
      message: "Not available yet. Opportunity detection has not been activated.",
      note: "Opportunity Engine not activated.",
    },
    brandExecutiveSummaryVersion: BRAND_EXECUTIVE_SUMMARY_VERSION,
    LIVE_PROVIDER_CALLS: 0,
    AIRTABLE_WRITES: 0,
    OPPORTUNITY_WRITES: 0,
  };
}
