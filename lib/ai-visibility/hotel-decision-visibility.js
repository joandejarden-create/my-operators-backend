/**
 * Brand AI Visibility — Hotel Decision Visibility (Phase 3A.3).
 * Proprietary decision-pattern read layer. No provider calls. No Airtable writes.
 * No composite HDV / GEO score.
 */

import { ACCESS_DEPTH } from "./access-depth.js";
import { AVAILABILITY } from "./availability-states.js";
import { resolveAiIntelligenceAccess } from "./authorization.js";
import {
  HEADLINE_GEOGRAPHIES,
  findMatchingSummaries,
  getBrandPortfolioPayload,
  getBrandTrendPayload,
  parseGeographyQuery,
} from "./brand-read-service.js";
import {
  computeAiPresenceRate,
  computeCompetitivePosition,
  computeQuestionsMissing,
  computeQuestionsWon,
  computeRecommendationShare,
} from "./metrics.js";
import { loadObservationsFromBatchSummary } from "./cohort-observations.js";
import { normalizeLanguage } from "./language-dimension.js";
import {
  HDV_REVIEW_RULES_VERSION,
  HDV_REVIEW_THRESHOLDS_V1,
  buildHotelDecisionVisibilityReviewItems,
} from "./hotel-decision-visibility-review-rules.js";
import {
  DEFAULT_AI_VISIBILITY_PROVIDER,
  formatProviderLabel,
  isAllProvidersSelector,
  pickScaffoldDataProvider,
  resolveProviderId,
  resolveProviderReadContext,
} from "./provider-dimension.js";
import { normalizeAiVisibilityViewerContext } from "./viewer-context.js";
import { buildCustomerSafeObservationContext } from "./customer-prompt-disclosure.js";

export const HOTEL_DECISION_VISIBILITY_VERSION = "ai_visibility_hotel_decision_visibility_v1";

export const HDV_DEFINITIONS = Object.freeze({
  DECISION_VISIBILITY_COVERAGE:
    "Portfolio: share of successful monitored questions where at least one linked brand appeared. Brand: AI Presence on the same question set.",
  OWNER_INTENT_COVERAGE:
    "For each Intent Territory: share of successful questions in that intent where at least one linked brand appeared.",
  TOP_DECISION_TERRITORY:
    "Intent Territory with the highest presence rate where brand Presence > 0 (portfolio coverage or brand presence within that intent). Ties broken by Questions Won, then Recommendation Share, then name.",
  REGIONAL_LEADER:
    "Linked brand with the highest AI Presence in that geography’s latest completed monitoring run.",
  REVIEW_ITEM_RULES_VERSION: HDV_REVIEW_RULES_VERSION,
  PRESENCE_GAP_PP: HDV_REVIEW_THRESHOLDS_V1.presenceGapPp,
  QUESTIONS_MISSING_SHARE: HDV_REVIEW_THRESHOLDS_V1.questionsMissingShare,
});

const HDV_GEO_KEYS = Object.freeze(["Global", "CALA", "Europe", "North America"]);

function formatPresenceRate(v) {
  if (v == null || !Number.isFinite(v)) return null;
  const rate = v > 1 ? v / 100 : v;
  return `${(Math.round(rate * 1000) / 10).toFixed(1)}%`;
}

function sortSummariesNewestFirst(summaries) {
  return [...(summaries || [])].sort((a, b) =>
    String(b.completedAt || b.startedAt || "").localeCompare(
      String(a.completedAt || a.startedAt || "")
    )
  );
}

function mentionEntityId(m) {
  return m?.entityId || m?.resolvedEntityId || m?.canonicalEntityId || null;
}

function isRecommendationRole(role) {
  return (
    role === "first_recommendation" ||
    role === "recommendation" ||
    role === "ranked_recommendation" ||
    role === "explicit_recommendation"
  );
}

function mentionDisplayName(m) {
  return (
    m?.canonicalEntityName ||
    m?.entityName ||
    m?.name ||
    m?.rawMention ||
    null
  );
}

/**
 * Build metrics Observations from batch evidence records.
 * Must honor language + multi-slot geography (CALA_EN vs CALA_ES) — same as
 * brand-read KPIs / questions — or AI vs Dealality mixes Spanish prompts under English.
 */
async function loadCohortFromSummary(store, summary, intentFilter, language = null) {
  const matchedSlotKeys = Array.isArray(summary?._matchedSlotKeys)
    ? summary._matchedSlotKeys
    : null;
  return loadObservationsFromBatchSummary(store, summary, {
    intentFilter: intentFilter || null,
    matchedSlotKeys: matchedSlotKeys?.length ? matchedSlotKeys : undefined,
    language: language || undefined,
  });
}

function portfolioPresenceCoverage(observations, entitledIds) {
  const relevant = (observations || []).filter((o) => o.success);
  const entitled = new Set(entitledIds || []);
  let hits = 0;
  for (const o of relevant) {
    if ((o.presentEntityIds || []).some((id) => entitled.has(id))) hits += 1;
  }
  return {
    numerator: hits,
    denominator: relevant.length,
    value: relevant.length ? hits / relevant.length : null,
  };
}

function ownerIntentCoverageRows(observations, entitledIds) {
  const entitled = new Set(entitledIds || []);
  const byIntent = new Map();
  for (const o of observations || []) {
    if (!o.success) continue;
    const intent = o.intentTerritory;
    if (!intent) continue;
    if (!byIntent.has(intent)) {
      byIntent.set(intent, { intentTerritory: intent, denominator: 0, numerator: 0 });
    }
    const row = byIntent.get(intent);
    row.denominator += 1;
    if ((o.presentEntityIds || []).some((id) => entitled.has(id))) row.numerator += 1;
  }
  return [...byIntent.values()]
    .map((r) => ({
      intentTerritory: r.intentTerritory,
      numerator: r.numerator,
      denominator: r.denominator,
      value: r.denominator ? r.numerator / r.denominator : null,
      display: formatPresenceRate(r.denominator ? r.numerator / r.denominator : null),
      availability: r.denominator
        ? r.numerator > 0
          ? AVAILABILITY.OBSERVED
          : AVAILABILITY.ZERO
        : AVAILABILITY.NOT_MONITORED,
      delta: null,
    }))
    .sort((a, b) => (b.value ?? -1) - (a.value ?? -1) || a.intentTerritory.localeCompare(b.intentTerritory));
}

function brandPresenceWithinIntent(observations, brandId, intent) {
  const subset = (observations || []).filter(
    (o) => o.success && o.intentTerritory === intent
  );
  return computeAiPresenceRate(subset, brandId);
}

function pickTopDecisionTerritory(args) {
  const { mode, intentRows, observations, brandId } = args;
  if (!intentRows?.length) return null;

  const scored = intentRows.map((row) => {
    const intent = row.intentTerritory;
    let coverage = row.value;
    let won = 0;
    let share = 0;
    if (mode === "brand" && brandId) {
      const presence = brandPresenceWithinIntent(observations, brandId, intent);
      coverage = presence.value;
      const subset = observations.filter((o) => o.success && o.intentTerritory === intent);
      won = computeQuestionsWon(subset, brandId).count ?? 0;
      share = computeRecommendationShare(subset, brandId).value ?? 0;
    } else {
      const subset = observations.filter((o) => o.success && o.intentTerritory === intent);
      // Portfolio "won" proxy: sum of entitled first-recs not needed; use coverage primarily.
      won = subset.filter((o) =>
        (o.recommendedEntityIds || [])[0] &&
        true
      ).length;
      share = coverage ?? 0;
    }
    return { intent, coverage: coverage ?? -1, won, share, row };
  });

  scored.sort((a, b) => {
    if (b.coverage !== a.coverage) return b.coverage - a.coverage;
    if (b.won !== a.won) return b.won - a.won;
    if (b.share !== a.share) return b.share - a.share;
    return a.intent.localeCompare(b.intent);
  });
  const top = scored[0];
  if (!top || !(top.coverage > 0)) return null;
  return {
    intentTerritory: top.intent,
    coverage: top.coverage,
    display: formatPresenceRate(top.coverage),
    availability: AVAILABILITY.OBSERVED,
  };
}

function applyIntentDeltas(currentRows, priorRows) {
  const priorMap = new Map((priorRows || []).map((r) => [r.intentTerritory, r.value]));
  return (currentRows || []).map((r) => {
    const prior = priorMap.get(r.intentTerritory);
    if (prior == null || r.value == null) return { ...r, delta: null };
    const absolute = r.value - prior;
    return {
      ...r,
      delta: {
        absolute,
        displayPp: Math.round(absolute * 1000) / 10,
        label: "vs prior comparable monitoring run",
      },
    };
  });
}

function topIntentForBrand(observations, brandId) {
  const intents = [
    ...new Set(
      (observations || []).filter((o) => o.success && o.intentTerritory).map((o) => o.intentTerritory)
    ),
  ];
  if (!intents.length) return null;
  const scored = intents.map((intent) => {
    const subset = observations.filter((o) => o.success && o.intentTerritory === intent);
    const presence = computeAiPresenceRate(subset, brandId);
    const won = computeQuestionsWon(subset, brandId).count ?? 0;
    const share = computeRecommendationShare(subset, brandId).value ?? 0;
    return { intent, presence: presence.value ?? -1, won, share };
  });
  scored.sort((a, b) => {
    if (b.presence !== a.presence) return b.presence - a.presence;
    if (b.won !== a.won) return b.won - a.won;
    if (b.share !== a.share) return b.share - a.share;
    return a.intent.localeCompare(b.intent);
  });
  const top = scored[0];
  if (!top || !(top.presence > 0)) return null;
  return top.intent;
}

export { topIntentForBrand };

function buildThinDealalityContext(brandFact) {
  if (!brandFact) return null;
  const parts = [];
  if (brandFact.chainScale) parts.push(brandFact.chainScale);
  if (brandFact.brandModel) parts.push(brandFact.brandModel);
  if (brandFact.parentCompany) parts.push(`parent ${brandFact.parentCompany}`);
  if (!parts.length) return null;
  return `${brandFact.name || "Brand"}: ${parts.join("; ")}.`;
}

function buildAiVsDealalityRows(evidenceRows, brandFactsById, entitledIds, limit = 6) {
  const entitled = new Set(entitledIds || []);
  const rows = [];
  for (const ev of evidenceRows || []) {
    if (rows.length >= limit) break;
    const aiNames = [];
    const seen = new Set();
    for (const m of ev.mentions || []) {
      const id = mentionEntityId(m);
      const name = mentionDisplayName(m);
      if (!name || seen.has(name)) continue;
      if (isRecommendationRole(m.role) || (id && entitled.has(id))) {
        seen.add(name);
        aiNames.push(name);
      }
      if (aiNames.length >= 4) break;
    }
    if (!aiNames.length) {
      for (const id of ev.presentEntityIds || []) {
        const fact = brandFactsById[id];
        const name = fact?.name || null;
        if (name && !seen.has(name)) {
          seen.add(name);
          aiNames.push(name);
        }
        if (aiNames.length >= 3) break;
      }
    }

    let dealalityContext = null;
    let contextStatus = AVAILABILITY.FUTURE_READY;
    for (const id of [...(ev.recommendedEntityIds || []), ...(ev.presentEntityIds || [])]) {
      if (!entitled.has(id)) continue;
      const ctx = buildThinDealalityContext(brandFactsById[id]);
      if (ctx) {
        dealalityContext = ctx;
        contextStatus = AVAILABILITY.OBSERVED;
        break;
      }
    }
    if (!dealalityContext) {
      dealalityContext = "Dealality context not yet available";
      contextStatus = AVAILABILITY.FUTURE_READY;
    }

    const safeContext = buildCustomerSafeObservationContext({
      promptId: ev.promptId,
      intentTerritory: ev.intentTerritory,
      PROMPT_FAMILY: ev.intentTerritory,
      REGION: ev.commercialRegion || ev.geographyScope,
      LANGUAGE: ev.language,
    });

    rows.push({
      ownerIntent: safeContext.ownerIntent,
      decisionContext: safeContext.decisionContext,
      geography: safeContext.geography,
      scenarioId: safeContext.scenarioId,
      promptId: ev.promptId,
      evidenceId: ev.evidenceId,
      intentTerritory: ev.intentTerritory,
      aiRepresentation: aiNames.length ? aiNames.join(", ") : "—",
      aiPattern: aiNames.length ? aiNames.join(", ") : "—",
      dealalityContext,
      contextStatus,
      reviewStatus: contextStatus === AVAILABILITY.OBSERVED ? "Observed" : "Review",
    });
  }
  return rows;
}

async function loadBrandFactsMap(entitledIds, brandNamesById) {
  const map = {};
  for (const id of entitledIds || []) {
    map[id] = {
      id,
      name: brandNamesById?.[id] || id,
      chainScale: null,
      brandModel: null,
      parentCompany: null,
    };
  }
  try {
    const { loadLiveBrandEntities } = await import("./load-brands-live.js");
    const loaded = await loadLiveBrandEntities({});
    const entities = loaded?.entities || [];
    for (const e of entities) {
      if (!map[e.id]) continue;
      map[e.id] = {
        id: e.id,
        name: e.name || map[e.id].name,
        chainScale: e.chainScale || null,
        brandModel: e.brandModel || null,
        parentCompany: e.parentCompany || null,
      };
    }
  } catch (err) {
    if (typeof console !== "undefined" && console.warn) {
      console.warn("[hdv] Brand Basics load skipped:", err.message);
    }
  }
  return map;
}

/**
 * @param {object} args
 */
export async function getHotelDecisionVisibilityPayload(args = {}) {
  const {
    dealalityUser,
    viewerContext,
    entitlementGraph,
    store,
    provider: providerArg = DEFAULT_AI_VISIBILITY_PROVIDER,
    geography,
    language: languageArg = null,
    intentTerritory,
    brandId = null,
    brandNamesById = {},
    /** Prefer injecting entitled brands from caller to avoid nested portfolio + cycle cost. */
    entitledBrands: entitledBrandsArg = null,
    entitledBrandIds: entitledBrandIdsArg = null,
    includePortfolioOverview = true,
    includeTrendEnrichment = true,
  } = args;

  const viewer = viewerContext || normalizeAiVisibilityViewerContext(dealalityUser);
  const provider = resolveProviderId(providerArg);
  const language = normalizeLanguage(languageArg) || "en";
  const geo = parseGeographyQuery({ geography, ...args });
  const effectiveGeo = geo.geographyScope
    ? geo
    : { geographyScope: "Region", commercialRegion: "CALA", country: null, key: "CALA" };
  const intentFilter = intentTerritory ? String(intentTerritory).trim() : null;
  const mode = brandId ? "brand" : "portfolio";

  const providerContext = await resolveProviderReadContext({
    store,
    requestedProvider: provider,
    geographyScope: effectiveGeo.geographyScope || null,
    commercialRegion: effectiveGeo.commercialRegion || null,
    country: effectiveGeo.country || null,
  });
  const allProvidersMode = isAllProvidersSelector(provider);
  const dataProvider = allProvidersMode
    ? pickScaffoldDataProvider(providerContext.availableProviders)
    : provider;

  if (mode === "brand") {
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
  }

  let entitledBrands = [];
  // Explicit array (including empty) means caller already resolved entitlement — do not re-fetch.
  if (Array.isArray(entitledBrandsArg)) {
    entitledBrands = entitledBrandsArg
      .map((b) => ({
        brandId: b.brandId,
        brandName: b.brandName || brandNamesById[b.brandId] || null,
      }))
      .filter((b) => b.brandId);
  } else if (Array.isArray(entitledBrandIdsArg) && entitledBrandIdsArg.length) {
    entitledBrands = entitledBrandIdsArg
      .filter(Boolean)
      .map((id) => ({
        brandId: id,
        brandName: brandNamesById[id] || null,
      }));
  } else {
    const portfolio = await getBrandPortfolioPayload({
      dealalityUser,
      viewerContext: viewer,
      entitlementGraph,
      store,
      provider,
      geography: effectiveGeo.key || geography || "CALA",
      language,
      brandNamesById,
    });
    entitledBrands = Array.isArray(portfolio.brands) ? portfolio.brands : [];
  }
  const entitledIds = entitledBrands.map((b) => b.brandId).filter(Boolean);
  // Brand-scoped reads must always include the subject in the review universe.
  if (mode === "brand" && brandId && !entitledIds.includes(brandId)) {
    entitledBrands = [
      ...entitledBrands,
      {
        brandId,
        brandName: brandNamesById[brandId] || null,
      },
    ];
    entitledIds.push(brandId);
  }
  const subjectIds = mode === "brand" ? [brandId] : entitledIds;

  const summaries = sortSummariesNewestFirst(
    await findMatchingSummaries(store, effectiveGeo, dataProvider, {
      language,
    })
  );
  const latest = summaries[0] || null;
  const prior = summaries[1] || null;

  const cohortPair = await Promise.all([
    latest
      ? loadCohortFromSummary(store, latest, intentFilter, language)
      : Promise.resolve({ observations: [], evidenceRows: [], intentMissing: false }),
    prior
      ? loadCohortFromSummary(store, prior, intentFilter, language)
      : Promise.resolve({ observations: [], evidenceRows: [] }),
  ]);
  const cohort = cohortPair[0];
  const priorCohort = cohortPair[1];

  const observations = cohort.observations;
  const coverage =
    mode === "brand" && brandId
      ? computeAiPresenceRate(observations, brandId)
      : portfolioPresenceCoverage(observations, entitledIds);

  let intentRows = ownerIntentCoverageRows(observations, entitledIds);
  if (mode === "brand" && brandId) {
    intentRows = intentRows.map((r) => {
      const presence = brandPresenceWithinIntent(observations, brandId, r.intentTerritory);
      return {
        ...r,
        numerator: presence.numerator,
        denominator: presence.denominator,
        value: presence.value,
        display: formatPresenceRate(presence.value),
        availability:
          presence.denominator == null || presence.denominator === 0
            ? AVAILABILITY.NOT_MONITORED
            : presence.value > 0
              ? AVAILABILITY.OBSERVED
              : AVAILABILITY.ZERO,
      };
    });
  }
  const priorIntentRows = ownerIntentCoverageRows(priorCohort.observations, entitledIds);
  intentRows = applyIntentDeltas(intentRows, priorIntentRows);

  const intentAnalyticsAvailable = !cohort.intentMissing && intentRows.length > 0;
  if (cohort.intentMissing && !intentRows.length) {
    intentRows = [];
  }

  const topTerritory = intentAnalyticsAvailable
    ? pickTopDecisionTerritory({
        mode,
        intentRows,
        observations,
        brandId,
      })
    : null;

  const peerIds =
    latest?.metrics?.competitivePosition?.peers?.map((p) => p.entityId).filter(Boolean) ||
    entitledIds;
  const competitive = computeCompetitivePosition(observations, peerIds);
  const entitledRanks = (competitive.peers || []).filter((p) => entitledIds.includes(p.entityId));
  entitledRanks.sort((a, b) => (a.rank ?? 999) - (b.rank ?? 999));

  let competitiveCard = {
    availability: AVAILABILITY.NOT_MONITORED,
    label: "Best Competitive Position",
    brandId: null,
    brandName: null,
    rank: null,
    peerCount: competitive.peers?.length || null,
    display: "—",
  };
  if (mode === "brand" && brandId) {
    const hit = (competitive.peers || []).find((p) => p.entityId === brandId);
    if (hit && typeof hit.rank === "number" && hit.rank > 0) {
      competitiveCard = {
        availability: AVAILABILITY.OBSERVED,
        label: "Competitive Position",
        brandId,
        brandName: brandNamesById[brandId] || entitledBrands.find((b) => b.brandId === brandId)?.brandName,
        rank: hit.rank,
        peerCount: competitive.peers.length,
        display: `#${hit.rank} of ${competitive.peers.length}`,
      };
    }
  } else if (
    entitledRanks[0] &&
    typeof entitledRanks[0].rank === "number" &&
    entitledRanks[0].rank > 0
  ) {
    const best = entitledRanks[0];
    competitiveCard = {
      availability: AVAILABILITY.OBSERVED,
      label: "Best Competitive Position",
      brandId: best.entityId,
      brandName:
        brandNamesById[best.entityId] ||
        entitledBrands.find((b) => b.brandId === best.entityId)?.brandName ||
        best.entityId,
      rank: best.rank,
      peerCount: competitive.peers.length,
      display: `#${best.rank} of ${competitive.peers.length}`,
    };
  }

  // Geography visibility + regional presence for review rules.
  // Parallel + resilient so one geo failure does not wipe Review Items.
  const geographyVisibility = [];
  const regionalPresenceByBrand = {};
  const geoJobs = HDV_GEO_KEYS.map((key) => {
    const gdef = HEADLINE_GEOGRAPHIES.find((g) => g.key === key);
    return gdef ? { key, gdef } : null;
  }).filter(Boolean);

  await Promise.all(
    geoJobs.map(async ({ key, gdef }) => {
      try {
        const gFilter = {
          geographyScope: gdef.geographyScope,
          commercialRegion: gdef.commercialRegion,
          country: gdef.country,
          key,
        };
        const gSummaries = sortSummariesNewestFirst(
          await findMatchingSummaries(store, gFilter, dataProvider, { language })
        );
        const gLatest = gSummaries[0];
        if (!gLatest) {
          geographyVisibility.push({
            geography: key,
            availability: AVAILABILITY.NOT_MONITORED,
            leadingBrandId: null,
            leadingBrandName: null,
            aiPresence: null,
            aiPresenceDisplay: "—",
            competitivePosition: null,
            coverageState: "not_monitored",
          });
          return;
        }
        // Prefer batch-summary metrics (fast) over reloading full evidence cohorts.
        const byEntity = gLatest?.metrics?.byEntity || {};
        const peerRows = gLatest?.metrics?.competitivePosition?.peers || [];
        let leading = null;
        for (const id of entitledIds) {
          const row =
            byEntity[id] ||
            Object.values(byEntity).find(
              (r) => r?.id === id || r?.entityId === id
            ) ||
            null;
          const peerHit = peerRows.find((p) => p.entityId === id);
          const presence =
            typeof row?.presence === "number"
              ? row.presence
              : typeof row?.aiPresenceRate === "number"
                ? row.aiPresenceRate
                : typeof peerHit?.presence === "number"
                  ? peerHit.presence
                  : typeof peerHit?.aiPresenceRate === "number"
                    ? peerHit.aiPresenceRate
                    : null;
          if (!regionalPresenceByBrand[id]) regionalPresenceByBrand[id] = {};
          regionalPresenceByBrand[id][key] = presence;
          if (presence == null) continue;
          if (!leading || presence > leading.presence) {
            leading = {
              brandId: id,
              brandName:
                brandNamesById[id] ||
                entitledBrands.find((b) => b.brandId === id)?.brandName,
              presence,
              rank: peerHit?.rank ?? peerHit?.position ?? null,
              peerCount: peerRows.length || null,
            };
          }
        }
        geographyVisibility.push({
          geography: key,
          availability: leading ? AVAILABILITY.OBSERVED : AVAILABILITY.ZERO,
          leadingBrandId: leading?.brandId || null,
          leadingBrandName: leading?.brandName || null,
          aiPresence: leading?.presence ?? null,
          aiPresenceDisplay: formatPresenceRate(leading?.presence) || "—",
          competitivePosition:
            leading?.rank != null ? `#${leading.rank} of ${leading.peerCount}` : "—",
          coverageState: leading ? "observed" : "observed_empty",
        });
      } catch (err) {
        if (typeof console !== "undefined" && console.warn) {
          console.warn(`[hdv] geography visibility skipped for ${key}:`, err.message);
        }
        geographyVisibility.push({
          geography: key,
          availability: AVAILABILITY.UNAVAILABLE,
          leadingBrandId: null,
          leadingBrandName: null,
          aiPresence: null,
          aiPresenceDisplay: "—",
          competitivePosition: null,
          coverageState: "unavailable",
        });
      }
    })
  );

  geographyVisibility.sort(
    (a, b) => HDV_GEO_KEYS.indexOf(a.geography) - HDV_GEO_KEYS.indexOf(b.geography)
  );

  const brandFactsById = await loadBrandFactsMap(entitledIds, {
    ...brandNamesById,
    ...Object.fromEntries(entitledBrands.map((b) => [b.brandId, b.brandName])),
  });

  const aiVsRows = buildAiVsDealalityRows(
    cohort.evidenceRows,
    brandFactsById,
    entitledIds,
    6
  );
  const aiVsDealalityContext = {
    status: !latest
      ? AVAILABILITY.NOT_MONITORED
      : aiVsRows.some((r) => r.contextStatus === AVAILABILITY.OBSERVED)
        ? AVAILABILITY.PARTIAL
        : aiVsRows.length
          ? AVAILABILITY.FUTURE_READY
          : AVAILABILITY.NOT_MONITORED,
    /** Provider scopes AI Pattern only — Dealality Context is provider-independent. */
    provider,
    providerLabel: formatProviderLabel(provider),
    aiPatternProviderScoped: true,
    dealalityContextProviderIndependent: true,
    governedDealalitySource: "Brand Setup - Brand Basics (Hotel Chain Scale, Brand Model, Parent Company)",
    rowsWithRealContext: aiVsRows.filter((r) => r.contextStatus === AVAILABILITY.OBSERVED).length,
    rowsFutureReady: aiVsRows.filter((r) => r.contextStatus === AVAILABILITY.FUTURE_READY).length,
    AI_AUTHORED_DEALALITY_CLAIMS: 0,
    rows: aiVsRows,
    message: !latest
      ? `No ${formatProviderLabel(provider)} monitoring data for this geography yet.`
      : aiVsRows.length
        ? null
        : "No question evidence available for AI vs Dealality comparison.",
  };

  // Review items
  const brandPresenceById = {};
  const brandMissingShareById = {};
  const brandEvidenceIdById = {};
  const monitoredBrandIdsInGeo = [];
  for (const id of entitledIds) {
    const presence = computeAiPresenceRate(observations, id);
    brandPresenceById[id] = presence.value;
    const missing = computeQuestionsMissing(observations, id);
    const denom = presence.denominator || 0;
    brandMissingShareById[id] = denom ? (missing.count ?? missing.value ?? 0) / denom : null;
    const ev = cohort.evidenceRows.find(
      (e) =>
        !(e.presentEntityIds || []).includes(id) || (e.presentEntityIds || []).includes(id)
    );
    // Prefer an evidence row where brand is present, else any cohort evidence for linking
    const presentEv = cohort.evidenceRows.find((e) =>
      (e.presentEntityIds || []).includes(id)
    );
    const missingEv = cohort.evidenceRows.find(
      (e) => !(e.presentEntityIds || []).includes(id)
    );
    brandEvidenceIdById[id] = presentEv?.evidenceId || missingEv?.evidenceId || ev?.evidenceId || null;
    if (latest && (presence.denominator || 0) > 0) monitoredBrandIdsInGeo.push(id);
  }

  const geoLeader = geographyVisibility.find((g) => g.geography === effectiveGeo.key);
  const reviewItems = buildHotelDecisionVisibilityReviewItems({
    geographyKey: effectiveGeo.key,
    provider,
    language,
    entitledBrands,
    subjectBrandIds: subjectIds,
    leaderPresence: geoLeader?.aiPresence ?? coverage.value,
    leaderBrandName: geoLeader?.leadingBrandName || competitiveCard.brandName,
    brandPresenceById,
    brandMissingShareById,
    brandEvidenceIdById,
    regionalPresenceByBrand,
    monitoredBrandIdsInGeo,
  }).slice(0, 8);

  // Portfolio overview enrichment (optional — skip on Detailed View merge path)
  const portfolioBrands = [];
  if (includePortfolioOverview) {
    for (const b of entitledBrands) {
      const presence = computeAiPresenceRate(observations, b.brandId);
      const rankHit = (competitive.peers || []).find((p) => p.entityId === b.brandId);
      const share = computeRecommendationShare(observations, b.brandId);
      const won = computeQuestionsWon(observations, b.brandId);
      const missing = computeQuestionsMissing(observations, b.brandId);
      const topIntent = intentAnalyticsAvailable
        ? topIntentForBrand(observations, b.brandId)
        : null;

      let visibilityChange = null;
      if (includeTrendEnrichment) {
        try {
          const trend = await getBrandTrendPayload({
            dealalityUser,
            viewerContext: viewer,
            entitlementGraph,
            store,
            brandId: b.brandId,
            provider,
            geography: effectiveGeo.key,
            language,
          });
          const points = trend.points || [];
          if (points.length >= 2) {
            const latestPt = points[points.length - 1];
            const priorPt = points[points.length - 2];
            if (typeof latestPt.value === "number" && typeof priorPt.value === "number") {
              const absolute = latestPt.value - priorPt.value;
              visibilityChange = {
                absolute,
                displayPp: Math.round(absolute * 1000) / 10,
                label: "vs prior comparable monitoring run",
                pointCount: points.length,
              };
            }
          } else if (points.length === 1) {
            visibilityChange = {
              absolute: null,
              displayPp: null,
              label: "Insufficient comparable history",
              pointCount: 1,
            };
          }
        } catch (_) {
          visibilityChange = null;
        }
      }

      portfolioBrands.push({
        brandId: b.brandId,
        brandName: b.brandName,
        aiPresence: {
          value: presence.value,
          display: formatPresenceRate(presence.value) || "—",
          availability:
            presence.denominator > 0
              ? presence.value > 0
                ? AVAILABILITY.OBSERVED
                : AVAILABILITY.ZERO
              : AVAILABILITY.NOT_MONITORED,
        },
        competitivePosition: {
          rank: rankHit?.rank ?? null,
          peerCount: competitive.peers?.length ?? null,
          display:
            rankHit?.rank != null
              ? `#${rankHit.rank} of ${competitive.peers.length}`
              : "—",
          availability: rankHit?.rank != null ? AVAILABILITY.OBSERVED : AVAILABILITY.NOT_MONITORED,
        },
        recommendationShare: {
          value: share.value,
          display: formatPresenceRate(share.value) || "—",
        },
        questionsWon: {
          value: won.count ?? won.value ?? null,
          display: String(won.count ?? won.value ?? "—"),
        },
        questionsMissing: {
          value: missing.count ?? missing.value ?? null,
          display: String(missing.count ?? missing.value ?? "—"),
        },
        topIntentTerritory: topIntent,
        visibilityChange,
      });
    }
  }

  const headlineCoverageAvail =
    coverage.denominator > 0
      ? coverage.value > 0
        ? AVAILABILITY.OBSERVED
        : AVAILABILITY.ZERO
      : AVAILABILITY.NOT_MONITORED;

  return {
    ok: true,
    allowed: true,
    hotelDecisionVisibilityVersion: HOTEL_DECISION_VISIBILITY_VERSION,
    scope: {
      mode,
      brandId: mode === "brand" ? brandId : null,
      geography: effectiveGeo.key,
      intent: intentFilter,
      provider,
      providerLabel: formatProviderLabel(provider),
    },
    provider,
    providerLabel: formatProviderLabel(provider),
    availableProviders: providerContext.availableProviders,
    providerHasCompletedData: providerContext.providerHasCompletedData,
    NO_SILENT_PROVIDER_FALLBACK: true,
    crossProvider: providerContext.crossProvider,
    definitions: HDV_DEFINITIONS,
    headline: {
      decisionVisibilityCoverage: {
        label:
          mode === "brand" ? "Decision Visibility Coverage" : "Decision Visibility Coverage",
        value: coverage.value,
        display: formatPresenceRate(coverage.value) || "—",
        numerator: coverage.numerator,
        denominator: coverage.denominator,
        availability: headlineCoverageAvail,
        definitionKey: "DECISION_VISIBILITY_COVERAGE",
      },
      topDecisionTerritory: topTerritory || {
        intentTerritory: null,
        display: "—",
        availability: intentAnalyticsAvailable
          ? AVAILABILITY.NOT_MONITORED
          : AVAILABILITY.FUTURE_READY,
        message: intentAnalyticsAvailable
          ? "No intent coverage yet."
          : "Intent Territory is missing on stored monitoring evidence. Coverage is not available yet.",
      },
      competitivePositionInOwnerDecisions: competitiveCard,
      evidenceBackedReviewItemCount: {
        value: reviewItems.length,
        display: String(reviewItems.length),
        availability: AVAILABILITY.OBSERVED,
      },
    },
    ownerIntentCoverage: {
      availability: intentAnalyticsAvailable
        ? intentRows.length
          ? AVAILABILITY.OBSERVED
          : AVAILABILITY.NOT_MONITORED
        : AVAILABILITY.FUTURE_READY,
      message: intentAnalyticsAvailable
        ? null
        : "Intent coverage needs Intent Territory on monitoring evidence. Groups are not invented.",
      rows: intentAnalyticsAvailable ? intentRows : [],
    },
    geographyVisibility,
    aiVsDealalityContext,
    reviewItems,
    portfolioOverview: {
      brands: portfolioBrands,
      FAKE_SPARKLINES: "NONE",
    },
    evidenceSummary: {
      observationCount: observations.length,
      evidenceCount: cohort.evidenceRows.length,
      batchId: latest?.batchId || null,
    },
    LIVE_PROVIDER_CALLS: 0,
    AIRTABLE_WRITES: 0,
    OPPORTUNITY_WRITES: 0,
    SCHEMA_CHANGES: 0,
    COMPOSITE_HDV_SCORE: "NONE",
    GEO_SCORE: "NONE",
  };
}
