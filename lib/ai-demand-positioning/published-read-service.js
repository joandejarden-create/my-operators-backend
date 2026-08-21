/**
 * AI Demand Positioning — Unified published report read service.
 * Priority: Airtable Live (optional) → local published → seed published → compute from raw period.
 */

import {
  loadPropertyProfile,
  loadLatestPeriod,
  loadLatestCustomerPeriod,
  loadAllPeriods,
} from "./data-model.js";
import { filterCustomerTrendPeriods, isCustomerTrendEligible } from "./period-eligibility-v1.js";
import { buildScenarioUniverse } from "./prompt-universe/scenario-registry.js";
import { buildOwnerPayload } from "./customer/owner-payload.js";
import { applyLeisureTerritoryCustomerLabelPatch } from "./metrics/intent-territory-labels.js";
import { buildGovernedIntentPresenceIndex } from "./metrics/governed-customer-presence-index.js";
import {
  loadPublishedReport,
  loadPublishedEvidenceIndex,
  loadPublishedManifest,
} from "./published-snapshot.js";
import {
  loadPublishedReportFromAirtable,
  loadPublishedEvidenceFromAirtable,
  loadPublishedManifestFromAirtable,
} from "./airtable-published-report.js";
import { queryEvidenceIndex } from "./customer/evidence-index.js";
import { attachOptionalExecutiveMetrics, buildOptionalExecutiveMetrics } from "./metrics/optional-executive-metrics.js";
import { buildAllTerritoryCompetitiveRankings } from "./customer/competitive-ranking-overall-view-v1.js";
import {
  attachDisplacementToCompetitiveRanking,
  resolveDisplacementEvidence,
  resolveCompetitorIdFromQuery,
  DISPLACEMENT_EVIDENCE_RESOLVER_VERSION,
} from "./customer/resolve-displacement-evidence-v1.js";
import {
  attachExecutiveReadUxLayer,
  buildExecutiveReadWithUx,
  executiveReadNeedsUxEnrichment,
} from "./customer/executive-read-v2.js";
import { computePropertyRealityCoverage } from "./customer/executive-read-v1.js";
import { isTargetedMeasurementPeriod } from "./data-model.js";
import { arePeriodsComparable } from "./metrics/longitudinal-comparability.js";
import { computeOwnedExternalSourceMix } from "./metrics/owned-source-classification-v1.js";

function overlayGovernedCustomerIndex(propertyId, payload) {
  if (!payload) return payload;
  const bakedIndex = payload.intentPresenceIndex;
  try {
    const profile = loadPropertyProfile(propertyId);
    if (!profile) return payload;
    const scenarios = buildScenarioUniverse(profile);
    const periods = loadAllPeriods(propertyId);
    const periodId = payload.period && payload.period.periodId;
    const period = periods.find((p) => p.periodId === periodId) || loadLatestPeriod(propertyId);
    const observations = (period?.observations || []).filter((o) => o.parsed);
    // Never wipe a published/baked index when runtime observations are unavailable
    // (lean deploys may ship published reports without full runtime period JSON).
    if (!period || observations.length === 0) {
      if (bakedIndex && typeof bakedIndex === "object" && Object.keys(bakedIndex).length > 0) {
        return payload;
      }
      payload.intentPresenceIndex = {};
      return payload;
    }
    payload.intentPresenceIndex = buildGovernedIntentPresenceIndex(observations, scenarios, profile);
    delete payload.aiConsiderationIndex;
    delete payload.aci;
    delete payload.intentPresenceIndexLegacyRollback;
  } catch (err) {
    console.error("[ADP read] governed index overlay failed:", err.message);
    // Preserve baked customer index on overlay failure.
    if (bakedIndex && typeof bakedIndex === "object") {
      payload.intentPresenceIndex = bakedIndex;
    }
  }
  return payload;
}

function preferTrendNumber(nextVal, priorVal) {
  if (nextVal != null && Number.isFinite(Number(nextVal))) return nextVal;
  if (priorVal != null && Number.isFinite(Number(priorVal))) return priorVal;
  return nextVal ?? priorVal ?? null;
}

function mergeTrendPoint(next, prior) {
  if (!prior) return next;
  if (!next) return prior;
  return {
    ...prior,
    ...next,
    // Non-destructive: never convert a valid baked rate to null via rebuild/enrich.
    considerationRate: preferTrendNumber(next.considerationRate, prior.considerationRate),
    scenarioPresenceRate: preferTrendNumber(next.scenarioPresenceRate, prior.scenarioPresenceRate),
    propertyRealityCoverage: preferTrendNumber(next.propertyRealityCoverage, prior.propertyRealityCoverage),
    demandCaptureRate: preferTrendNumber(next.demandCaptureRate, prior.demandCaptureRate),
    date: next.date || prior.date,
    periodId: next.periodId || prior.periodId,
    providerCount: next.providerCount ?? prior.providerCount ?? null,
    observationCount: next.observationCount ?? prior.observationCount ?? null,
  };
}

function attachTrendsIfMissing(propertyId, payload, allPeriods) {
  if (!payload) return payload;
  const hasTrends = Array.isArray(payload.trends) && payload.trends.length > 0;
  const ineligible =
    hasTrends &&
    payload.trends.some((t) => {
      const match = (allPeriods || []).find((p) => p.periodId === t.periodId);
      return match ? !isCustomerTrendEligible(match) : true;
    });
  const missingDerivedField =
    hasTrends &&
    !ineligible &&
    payload.trends.some(
      (t) =>
        t.propertyRealityCoverage == null ||
        t.considerationRate == null ||
        t.scenarioPresenceRate == null
    );

  // Prefer in-place derivation when any primary trend rates are already baked — full rebuild
  // must never wipe valid consideration/scenario/PRC values.
  if (missingDerivedField) {
    try {
      const profile = loadPropertyProfile(propertyId);
      const patched = payload.trends.map((t) => {
        const period = (allPeriods || []).find((p) => p.periodId === t.periodId);
        let next = { ...t };
        if (next.propertyRealityCoverage == null && period) {
          next.propertyRealityCoverage = computePropertyRealityCoverage(period, profile);
        }
        if ((next.considerationRate == null || next.scenarioPresenceRate == null) && period) {
          const scenarios = buildScenarioUniverse(profile);
          const em = buildOptionalExecutiveMetrics(period, scenarios, profile);
          if (next.considerationRate == null) {
            next.considerationRate = em?.considerationRate?.rate ?? null;
          }
          if (next.scenarioPresenceRate == null) {
            next.scenarioPresenceRate = em?.scenarioPresence?.rate ?? null;
          }
        }
        return mergeTrendPoint(next, t);
      });
      return { ...payload, trends: patched };
    } catch (err) {
      console.error("[ADP read] trends non-destructive patch failed:", err.message);
      return payload;
    }
  }

  const needsRebuild = !hasTrends || ineligible;
  if (!needsRebuild) return payload;
  try {
    const profile = loadPropertyProfile(propertyId);
    const scenarios = buildScenarioUniverse(profile);
    const trends = buildTrendsFromPeriods(propertyId, scenarios, allPeriods);
    // Empty official series is valid (Period 001 = first point). Never keep stale pre-baseline trends.
    // Preserve baked primary rates when rebuild returns null for the same periodId.
    const priorById = new Map((payload.trends || []).map((t) => [t.periodId, t]));
    const rebuilt = trends || [];
    if (!rebuilt.length && hasTrends) {
      // Rebuild produced nothing — keep baked trends rather than wiping the series.
      return payload;
    }
    const merged = rebuilt.map((t) => mergeTrendPoint(t, priorById.get(t.periodId)));
    // Keep any baked points the rebuild omitted (non-destructive union by periodId).
    for (const prior of payload.trends || []) {
      if (!merged.some((t) => t.periodId === prior.periodId)) merged.push(prior);
    }
    return { ...payload, trends: merged };
  } catch (err) {
    console.error("[ADP read] trends enrich failed:", err.message);
  }
  return payload;
}

export function enrichPayloadOptionalMetrics(propertyId, payload) {
  if (!payload) return payload;
  payload = overlayGovernedCustomerIndex(propertyId, payload);
  try {
    const profile = loadPropertyProfile(propertyId);
    const period = loadLatestCustomerPeriod(propertyId) || loadLatestPeriod(propertyId);
    if (!period) {
      return applyLeisureTerritoryCustomerLabelPatch(payload);
    }
    const scenarios = buildScenarioUniverse(profile);
    const allPeriods = loadAllPeriods(propertyId);
    let enriched = payload;
    if (!payload.executiveMetrics) {
      enriched = attachOptionalExecutiveMetrics(payload, period, scenarios, profile, { allPeriods });
    }
    enriched = attachExecutiveReadIfMissing(propertyId, enriched, period, scenarios, profile, allPeriods);
    enriched = attachCompetitiveRankingIfMissing(propertyId, enriched, period, scenarios, profile);
    enriched = attachOwnedSourceMixIfMissing(enriched, period, profile);
    enriched = attachTrendsIfMissing(propertyId, enriched, allPeriods);
    // CUSTOMER_TERMINOLOGY_PATCH: remap legacy "Resort Leisure" → "Leisure Travel" on every serve.
    return applyLeisureTerritoryCustomerLabelPatch(enriched);
  } catch (err) {
    console.error("[ADP read] optional executiveMetrics enrich failed:", err.message);
    return applyLeisureTerritoryCustomerLabelPatch(payload);
  }
}

function attachOwnedSourceMixIfMissing(payload, period, profile) {
  if (!payload || !profile) return payload;
  try {
    const observations = (period?.observations || []).filter((o) => o.parsed);
    const mix = computeOwnedExternalSourceMix(observations, profile);
    const evidence = { ...(payload.evidence || {}) };
    evidence.ownedSourceShare = mix.ownedShare;
    evidence.externalSourceShare = mix.externalShare;
    evidence.unknownSourceShare = mix.unknownShare;
    evidence.ownedSourceMix = mix;
    evidence.ownedDomainsConfigured = mix.domainsConfigured;
    const property = {
      ...(payload.property || {}),
      website: profile.website || payload.property?.website || null,
      ownedDomains: profile.ownedDomains || payload.property?.ownedDomains || [],
      officialBrandDomain: profile.officialBrandDomain || null,
      officialPropertyPageUrl: profile.officialPropertyPageUrl || null,
    };
    return { ...payload, evidence, property };
  } catch (err) {
    console.error("[ADP read] owned source mix enrich failed:", err.message);
    return payload;
  }
}

function airtableReadEnabled() {
  return process.env.ADP_PUBLISHED_READ_SOURCE === "airtable" || process.env.ADP_AIRTABLE_READ_LIVE === "1";
}

/**
 * Production read-source guardrail. Default active source is filesystem (published JSON).
 * Airtable is optional and must never silently become an ambiguous active source.
 */
export function getAdpPublishedReadSourceStatus() {
  const requested = airtableReadEnabled() ? "airtable" : "filesystem";
  const env = {
    ADP_PUBLISHED_READ_SOURCE: process.env.ADP_PUBLISHED_READ_SOURCE || null,
    ADP_AIRTABLE_READ_LIVE: process.env.ADP_AIRTABLE_READ_LIVE || null,
  };
  return {
    ok: true,
    requested,
    // Active default until a successful Airtable load proves otherwise.
    active: requested === "filesystem" ? "filesystem" : "airtable_requested",
    airtableRequested: requested === "airtable",
    airtableEnabled: false,
    fallback: null,
    env,
    note:
      requested === "filesystem"
        ? "Production SoT is local published JSON (filesystem). Airtable not requested."
        : "Airtable read requested — verify load success; filesystem remains the fallback SoT.",
  };
}

let _adpReadSourceLogged = false;
export function logAdpPublishedReadSourceAtStartup() {
  if (_adpReadSourceLogged) return getAdpPublishedReadSourceStatus();
  _adpReadSourceLogged = true;
  const status = getAdpPublishedReadSourceStatus();
  console.log(
    `[ADP read] production published source requested=${status.requested} ` +
      `(ADP_PUBLISHED_READ_SOURCE=${status.env.ADP_PUBLISHED_READ_SOURCE || "unset"} ` +
      `ADP_AIRTABLE_READ_LIVE=${status.env.ADP_AIRTABLE_READ_LIVE || "unset"}). ` +
      `Default SoT=filesystem. Airtable is not re-enabled.`
  );
  return status;
}

function withReadSourceMeta(result, meta) {
  return {
    ...result,
    readSource: {
      requested: meta.requested,
      active: meta.active,
      fallback: meta.fallback || null,
      fallbackReason: meta.fallbackReason || null,
    },
  };
}

function filterComparableFullPropertyPeriods(allPeriods, scenarios) {
  // Customer trends: official customer-eligible periods only (pre-baseline hidden).
  const customer = filterCustomerTrendPeriods(allPeriods);
  const full = customer.length
    ? customer
    : (allPeriods || []).filter((p) => !isTargetedMeasurementPeriod(p) && isCustomerTrendEligible(p));
  if (!full.length) return [];
  const sorted = [...full].sort((a, b) => String(a.executionDate || "").localeCompare(String(b.executionDate || "")));
  const anchor = sorted[sorted.length - 1];
  return sorted.filter((p) => arePeriodsComparable(anchor, p, scenarios).comparable);
}

function buildTrendsFromPeriods(propertyId, scenarios, periods) {
  const profile = loadPropertyProfile(propertyId);
  const comparable = filterComparableFullPropertyPeriods(periods, scenarios);
  if (!comparable.length) return undefined;

  return comparable.map((p) => {
    const em = buildOptionalExecutiveMetrics(p, scenarios, profile);
    return {
      periodId: p.periodId,
      date: p.executionDate,
      propertyRealityCoverage: computePropertyRealityCoverage(p, profile),
      scenarioPresenceRate: em?.scenarioPresence?.rate ?? null,
      considerationRate: em?.considerationRate?.rate ?? null,
      providerCount: p.providers ? p.providers.length : p.providerCount || null,
      observationCount: p.observations ? p.observations.length : 0,
    };
  });
}

function rankingNeedsDisplacementEnrichment(ranking) {
  if (!ranking?.byTerritory) return true;
  if (ranking.displacementEvidenceVersion !== DISPLACEMENT_EVIDENCE_RESOLVER_VERSION) return true;
  for (const block of Object.values(ranking.byTerritory)) {
    const sample = (block.displayRows || []).find((r) => !r.isSubject);
    if (sample && !sample.displacement) return true;
  }
  return false;
}

function attachCompetitiveRankingIfMissing(propertyId, payload, period, scenarios, profile) {
  if (!payload) return payload;
  try {
    const observations = (period?.observations || []).filter((o) => o.parsed);
    if (!observations.length) return payload;

    let ranking = payload.competitiveRankingByTerritory;
    if (!ranking?.byTerritory) {
      ranking = buildAllTerritoryCompetitiveRankings(observations, scenarios, profile);
    }
    if (rankingNeedsDisplacementEnrichment(ranking)) {
      ranking = attachDisplacementToCompetitiveRanking(ranking, observations, scenarios, profile);
    }
    return { ...payload, competitiveRankingByTerritory: ranking };
  } catch (err) {
    console.error("[ADP read] competitiveRanking enrich failed:", err.message);
    return payload;
  }
}

function attachExecutiveReadIfMissing(propertyId, payload, period, scenarios, profile, allPeriods) {
  if (!payload) return payload;
  try {
    const existing = payload.executiveRead;
    if (!existing) {
      const executiveRead = buildExecutiveReadWithUx(payload, period, scenarios, profile, { allPeriods });
      if (executiveRead) return { ...payload, executiveRead };
      return payload;
    }
    if (!executiveReadNeedsUxEnrichment(existing)) return payload;
    const executiveRead = attachExecutiveReadUxLayer(
      existing,
      payload,
      period,
      scenarios,
      profile
    );
    if (executiveRead) return { ...payload, executiveRead };
  } catch (err) {
    console.error("[ADP read] executiveRead enrich failed:", err.message);
  }
  return payload;
}

export async function getPublishedOwnerReport(propertyId) {
  const profile = loadPropertyProfile(propertyId);
  if (!profile) return { ok: false, error: "property_not_found" };

  const requested = airtableReadEnabled() ? "airtable" : "filesystem";

  if (airtableReadEnabled()) {
    try {
      const airtablePayload = await loadPublishedReportFromAirtable(propertyId);
      if (airtablePayload) {
        console.log(`[ADP read] property=${propertyId} active=airtable (requested=airtable)`);
        return withReadSourceMeta(
          { ok: true, source: "airtable", payload: enrichPayloadOptionalMetrics(propertyId, airtablePayload) },
          { requested, active: "airtable" }
        );
      }
      console.warn(
        `[ADP read] Airtable mode requested but report unavailable for ${propertyId}; falling back to filesystem published snapshot.`
      );
    } catch (err) {
      console.error(
        `[ADP read] Airtable mode requested but load failed for ${propertyId}; falling back to filesystem. error=${err.message}`
      );
    }
  }

  const publishedPayload = loadPublishedReport(propertyId);
  if (publishedPayload) {
    const active = "filesystem";
    const fallback = requested === "airtable" ? "filesystem" : null;
    if (fallback) {
      console.warn(
        `[ADP read] property=${propertyId} active=filesystem fallback_from=airtable (Airtable unavailable/auth failed or empty).`
      );
    } else {
      console.log(`[ADP read] property=${propertyId} active=filesystem (requested=filesystem)`);
    }
    return withReadSourceMeta(
      {
        ok: true,
        source: "published_snapshot",
        payload: enrichPayloadOptionalMetrics(propertyId, publishedPayload),
      },
      {
        requested,
        active,
        fallback,
        fallbackReason: fallback ? "airtable_unavailable_or_empty" : null,
      }
    );
  }

  const period = loadLatestCustomerPeriod(propertyId) || loadLatestPeriod(propertyId);
  if (!period) {
    return {
      ok: false,
      error: "no_monitoring_data",
      message: "No monitoring period has been executed for this property yet.",
      property: { name: profile.name, city: profile.city },
      readSource: { requested, active: null, fallback: null },
    };
  }

  const scenarios = buildScenarioUniverse(profile);
  const allPeriods = loadAllPeriods(propertyId);
  const fullPeriods = filterCustomerTrendPeriods(allPeriods);
  const payload = buildOwnerPayload(period, scenarios, profile, {
    allPeriods: fullPeriods.length ? fullPeriods : allPeriods.filter((p) => !isTargetedMeasurementPeriod(p)),
  });
  if (!payload.ok) {
    return { ok: false, error: payload.error, message: payload.message };
  }

  const trends = buildTrendsFromPeriods(propertyId, scenarios, allPeriods);
  if (trends) payload.trends = trends;

  console.warn(
    `[ADP read] property=${propertyId} active=computed_runtime (no published snapshot; requested=${requested})`
  );
  return withReadSourceMeta(
    { ok: true, source: "computed_runtime", payload },
    { requested, active: "computed_runtime", fallback: "computed_runtime", fallbackReason: "published_snapshot_missing" }
  );
}

export async function getPublishedEvidenceResponse(propertyId, query) {
  const profile = loadPropertyProfile(propertyId);
  if (!profile) return { ok: false, error: "property_not_found" };

  // Displacement: always resolve from immutable observations via shared governed resolver.
  // Do not use the truncated published evidence-index competitor cap.
  if (query.type === "displacement") {
    const period = loadLatestCustomerPeriod(propertyId) || loadLatestPeriod(propertyId);
    if (!period) return { ok: false, error: "no_data" };
    const scenarios = buildScenarioUniverse(profile);
    const observations = (period.observations || []).filter((o) => o.parsed);
    const competitorId = resolveCompetitorIdFromQuery(profile, query);
    const resolvedScope =
      query.scope === "overall" || query.scope === "OVERALL" || (!query.intent && query.scope !== "demand_territory")
        ? "overall"
        : query.intent || "overall";

    const result = resolveDisplacementEvidence({
      propertyProfile: profile,
      observations,
      scenarios,
      competitorId,
      competitorName: query.competitor,
      scope: resolvedScope,
      periodMeta: { executionDate: period.executionDate, periodId: period.periodId },
    });

    return {
      ok: true,
      source: "displacement_resolver_v1",
      propertyId,
      intent: result.scope?.intent || query.intent || null,
      scope: result.scope,
      type: "displacement",
      competitor: result.competitorName || query.competitor,
      competitorId: result.competitorId,
      evidence: result.evidence,
      total: result.total,
      count: result.count,
      grain: result.grain,
      version: DISPLACEMENT_EVIDENCE_RESOLVER_VERSION,
    };
  }

  let index = null;
  let source = null;

  if (airtableReadEnabled()) {
    try {
      index = await loadPublishedEvidenceFromAirtable(propertyId);
      if (index) source = "airtable";
    } catch (err) {
      console.error("[ADP read] Airtable evidence load failed:", err.message);
    }
  }

  if (!index) {
    index = loadPublishedEvidenceIndex(propertyId);
    if (index) source = "published_snapshot";
  }

  if (index?.ok) {
    const result = queryEvidenceIndex(index, query);
    return {
      ok: true,
      source,
      propertyId,
      intent: query.intent,
      type: query.type,
      competitor: query.competitor,
      evidence: result.evidence,
      total: result.total,
    };
  }

  // Fallback: compute from raw period (dev / legacy) — non-displacement types
  const period = loadLatestCustomerPeriod(propertyId) || loadLatestPeriod(propertyId);
  if (!period) return { ok: false, error: "no_data" };

  const scenarios = buildScenarioUniverse(profile);
  const scenarioMap = Object.fromEntries(scenarios.map((s) => [s.scenarioId, s]));
  let observations = period.observations || [];

  if (query.intent) {
    const intentScenarioIds = scenarios.filter((s) => s.intent === query.intent).map((s) => s.scenarioId);
    observations = observations.filter((o) => intentScenarioIds.includes(o.scenarioId));
  }
  if (query.type === "missing") observations = observations.filter((o) => !o.mentioned);
  else if (query.type === "present") observations = observations.filter((o) => o.mentioned);

  const evidence = observations.slice(0, 5).map((obs) => ({
    scenarioId: obs.scenarioId,
    scenarioLabel: scenarioMap[obs.scenarioId]?.label || obs.scenarioId,
    intent: scenarioMap[obs.scenarioId]?.intent || "",
    provider: obs.provider,
    mentioned: obs.mentioned,
    competitorsMentioned: obs.competitorsMentioned || [],
    responseExcerpt: obs.rawResponse ? obs.rawResponse.slice(0, 2000) : "",
    sourcesCited: obs.sourcesCited || [],
    providerCitations: obs.providerCitations || [],
    timestamp: period.executionDate,
  }));

  return {
    ok: true,
    source: "computed_runtime",
    propertyId,
    intent: query.intent,
    type: query.type,
    competitor: query.competitor,
    evidence,
    total: observations.length,
  };
}

export async function getPublishedManifestSummary(propertyId) {
  if (airtableReadEnabled()) {
    try {
      return await loadPublishedManifestFromAirtable(propertyId);
    } catch (err) {
      console.error("[ADP read] Airtable manifest load failed:", err.message);
    }
  }
  return loadPublishedManifest(propertyId);
}
