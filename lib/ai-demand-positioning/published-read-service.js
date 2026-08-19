/**
 * AI Demand Positioning — Unified published report read service.
 * Priority: Airtable Live (optional) → local published → seed published → compute from raw period.
 */

import { loadPropertyProfile, loadLatestPeriod, loadAllPeriods } from "./data-model.js";
import { buildScenarioUniverse } from "./prompt-universe/scenario-registry.js";
import { buildOwnerPayload } from "./customer/owner-payload.js";
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

function airtableReadEnabled() {
  return process.env.ADP_PUBLISHED_READ_SOURCE === "airtable" || process.env.ADP_AIRTABLE_READ_LIVE === "1";
}

function buildTrendsFromPeriods(propertyId, scenarios, periods) {
  if (!periods || periods.length <= 1) return undefined;
  const profile = loadPropertyProfile(propertyId);
  return periods.map((p) => {
    const pPayload = buildOwnerPayload(p, scenarios, profile);
    return {
      periodId: p.periodId,
      date: p.executionDate,
      demandCaptureRate: pPayload.demandCapture ? pPayload.demandCapture.overallRate : null,
      providerCount: p.providers ? p.providers.length : p.providerCount || null,
      observationCount: p.observations ? p.observations.length : 0,
    };
  });
}

export async function getPublishedOwnerReport(propertyId) {
  const profile = loadPropertyProfile(propertyId);
  if (!profile) return { ok: false, error: "property_not_found" };

  if (airtableReadEnabled()) {
    try {
      const airtablePayload = await loadPublishedReportFromAirtable(propertyId);
      if (airtablePayload) {
        return { ok: true, source: "airtable", payload: airtablePayload };
      }
    } catch (err) {
      console.error("[ADP read] Airtable report load failed:", err.message);
    }
  }

  const publishedPayload = loadPublishedReport(propertyId);
  if (publishedPayload) {
    return { ok: true, source: "published_snapshot", payload: publishedPayload };
  }

  const period = loadLatestPeriod(propertyId);
  if (!period) {
    return {
      ok: false,
      error: "no_monitoring_data",
      message: "No monitoring period has been executed for this property yet.",
      property: { name: profile.name, city: profile.city },
    };
  }

  const scenarios = buildScenarioUniverse(profile);
  const payload = buildOwnerPayload(period, scenarios, profile);
  if (!payload.ok) {
    return { ok: false, error: payload.error, message: payload.message };
  }

  const allPeriods = loadAllPeriods(propertyId);
  const trends = buildTrendsFromPeriods(propertyId, scenarios, allPeriods);
  if (trends) payload.trends = trends;

  return { ok: true, source: "computed_runtime", payload };
}

export async function getPublishedEvidenceResponse(propertyId, query) {
  const profile = loadPropertyProfile(propertyId);
  if (!profile) return { ok: false, error: "property_not_found" };

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

  // Fallback: compute from raw period (dev / legacy)
  const period = loadLatestPeriod(propertyId);
  if (!period) return { ok: false, error: "no_data" };

  const scenarios = buildScenarioUniverse(profile);
  const scenarioMap = Object.fromEntries(scenarios.map((s) => [s.scenarioId, s]));
  let observations = period.observations || [];

  if (query.type === "displacement" && query.competitor) {
    const compLow = query.competitor.toLowerCase();
    observations = observations.filter(
      (o) =>
        !o.mentioned &&
        (o.competitorsMentioned || []).some(
          (c) => c.toLowerCase().includes(compLow) || compLow.includes(c.toLowerCase())
        )
    );
    const byScenario = {};
    for (const obs of observations) {
      if (
        !byScenario[obs.scenarioId] ||
        (obs.rawResponse || "").length > (byScenario[obs.scenarioId].rawResponse || "").length
      ) {
        byScenario[obs.scenarioId] = obs;
      }
    }
    observations = Object.values(byScenario);
  } else {
    if (query.intent) {
      const intentScenarioIds = scenarios.filter((s) => s.intent === query.intent).map((s) => s.scenarioId);
      observations = observations.filter((o) => intentScenarioIds.includes(o.scenarioId));
    }
    if (query.type === "missing") observations = observations.filter((o) => !o.mentioned);
    else if (query.type === "present") observations = observations.filter((o) => o.mentioned);
  }

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
