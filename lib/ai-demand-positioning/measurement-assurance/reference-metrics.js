/**
 * Independent reference metric implementation for ADP Measurement Assurance V1.
 *
 * INTENTIONALLY does NOT import production metric modules
 * (consideration-rate.js, demand-capture-index.js, optional-executive-metrics.js, etc.).
 * Formulas follow adp-measurement-contract-v1 prose; shared grain rules are inlined.
 */

import { ASSURANCE_RESOLVER_VERSION, ROUNDING_TOLERANCE_PP } from "./version.js";
import { detectPropertyMention } from "../execution/response-parser.js";
import { extractPropertyRank } from "../metrics/position-extraction.js";

function roundAdpPercent(value) {
  if (value == null || !Number.isFinite(Number(value))) return null;
  return Math.round(Number(value) * 10) / 10;
}

/** Inlined comparable rule (contract) — not imported from grain-governance. */
export function refIsComparableObservation(obs) {
  if (!obs) return false;
  if (obs.dryRun) return false;
  if (obs.error || obs.status === "FAILED" || obs.status === "ERROR") return false;
  if (!obs.parsed && obs.mentioned === undefined && !obs.rawResponse) return false;
  if (obs.parsed && !obs.rawResponse && obs.mentioned === undefined) return false;
  return true;
}

/**
 * Assurance subject presence: governed Path A matching (detectPropertyMention).
 * Rank from extractPropertyRank for position fields only — mention does NOT use Path B variants.
 */
export function refInterpretObservation(obs, propertyProfile) {
  const raw = obs?.rawResponse || "";
  const mention = detectPropertyMention(raw, propertyProfile);
  const rank = extractPropertyRank(raw, propertyProfile);
  return {
    mentioned: Boolean(mention.mentioned),
    matchedVariant: mention.matchedVariant || null,
    context: mention.context || null,
    position: rank.position,
    rankEligible: Boolean(rank.rankEligible),
    rankSource: rank.rankSource || null,
    resolverVersion: ASSURANCE_RESOLVER_VERSION,
  };
}

export function attachReferenceInterpretation(observations, propertyProfile) {
  return (observations || []).map((obs) => {
    const interp = refInterpretObservation(obs, propertyProfile);
    return {
      ...obs,
      _ref: interp,
      mentioned: interp.mentioned,
      position: interp.position,
      rankEligible: interp.rankEligible,
      rankSource: interp.rankSource,
    };
  });
}

export function refConsiderationAndScenario(observations, scenarios) {
  const scenarioIds = new Set((scenarios || []).map((s) => s.scenarioId));
  const comparable = (observations || []).filter(
    (o) => refIsComparableObservation(o) && scenarioIds.has(o.scenarioId)
  );
  const present = comparable.filter((o) => o.mentioned);
  const considerationRate =
    comparable.length > 0 ? roundAdpPercent((present.length / comparable.length) * 100) : null;

  const byScenario = new Map();
  for (const s of scenarios || []) byScenario.set(s.scenarioId, false);
  for (const o of comparable) {
    if (o.mentioned) byScenario.set(o.scenarioId, true);
  }
  let captured = 0;
  for (const s of scenarios || []) {
    if (byScenario.get(s.scenarioId)) captured += 1;
  }
  const scenarioPresence =
    (scenarios || []).length > 0
      ? roundAdpPercent((captured / scenarios.length) * 100)
      : null;

  return {
    considerationRate,
    presentObservations: present.length,
    comparableObservations: comparable.length,
    scenarioPresence,
    capturedScenarios: captured,
    eligibleScenarios: (scenarios || []).length,
  };
}

/** Demand Capture (scenario grain): share of scenarios with ≥1 subject appearance. */
export function refDemandCapture(observations, scenarios) {
  const byScenario = new Map();
  for (const o of observations || []) {
    if (!refIsComparableObservation(o)) continue;
    if (!byScenario.has(o.scenarioId)) byScenario.set(o.scenarioId, false);
    if (o.mentioned) byScenario.set(o.scenarioId, true);
  }
  let total = 0;
  let captured = 0;
  const byIntent = {};
  for (const s of scenarios || []) {
    total += 1;
    const hit = byScenario.get(s.scenarioId) === true;
    if (hit) captured += 1;
    if (!byIntent[s.intent]) byIntent[s.intent] = { total: 0, captured: 0, rate: 0 };
    byIntent[s.intent].total += 1;
    if (hit) byIntent[s.intent].captured += 1;
  }
  for (const intent of Object.keys(byIntent)) {
    const row = byIntent[intent];
    row.rate = row.total > 0 ? roundAdpPercent((row.captured / row.total) * 100) : 0;
  }
  return {
    overallRate: total > 0 ? roundAdpPercent((captured / total) * 100) : 0,
    totalScenarios: total,
    capturedScenarios: captured,
    byIntent,
  };
}

export function refProviderPresence(observations) {
  const stats = {};
  for (const obs of observations || []) {
    const p = obs?.provider;
    if (!p) continue;
    if (!stats[p]) stats[p] = { scheduled: 0, comparable: 0, mentioned: 0, failed: 0 };
    stats[p].scheduled += 1;
    const failed =
      obs.error || obs.status === "FAILED" || obs.status === "ERROR" || obs.dryRun;
    if (failed && !refIsComparableObservation(obs)) stats[p].failed += 1;
    if (!refIsComparableObservation(obs)) continue;
    stats[p].comparable += 1;
    if (obs.mentioned) stats[p].mentioned += 1;
  }
  return Object.entries(stats).map(([provider, s]) => ({
    provider,
    scheduled: s.scheduled,
    comparable: s.comparable,
    failed: s.failed,
    missingAsZero: false,
    mentioned: s.mentioned,
    presence: s.comparable > 0 ? roundAdpPercent((s.mentioned / s.comparable) * 100) : null,
    denominator: "comparable_observations",
  }));
}

export function refTerritoryPresence(observations, scenarios) {
  const byIntent = {};
  for (const s of scenarios || []) {
    if (!byIntent[s.intent]) {
      byIntent[s.intent] = { scenarios: 0, captured: 0, observations: 0, present: 0 };
    }
    byIntent[s.intent].scenarios += 1;
  }
  const scenarioHit = new Map();
  for (const o of observations || []) {
    if (!refIsComparableObservation(o)) continue;
    const sc = (scenarios || []).find((s) => s.scenarioId === o.scenarioId);
    if (!sc) continue;
    byIntent[sc.intent].observations += 1;
    if (o.mentioned) {
      byIntent[sc.intent].present += 1;
      scenarioHit.set(o.scenarioId, true);
    }
  }
  for (const s of scenarios || []) {
    if (scenarioHit.get(s.scenarioId)) byIntent[s.intent].captured += 1;
  }
  for (const intent of Object.keys(byIntent)) {
    const r = byIntent[intent];
    r.scenarioPresenceRate =
      r.scenarios > 0 ? roundAdpPercent((r.captured / r.scenarios) * 100) : null;
    r.observationPresenceRate =
      r.observations > 0 ? roundAdpPercent((r.present / r.observations) * 100) : null;
  }
  return byIntent;
}

export function refRankAppearance(observations) {
  const comparable = (observations || []).filter(refIsComparableObservation);
  const rankEligible = comparable.filter(
    (o) => o.mentioned && o.rankEligible && o.position != null && Number.isFinite(o.position)
  );
  if (rankEligible.length < 20) {
    return { numberOneRate: null, top3Rate: null, rankEligible: rankEligible.length, sampleInsufficient: true };
  }
  const n1 = rankEligible.filter((o) => o.position === 1).length;
  const t3 = rankEligible.filter((o) => o.position <= 3).length;
  return {
    numberOneRate: roundAdpPercent((n1 / rankEligible.length) * 100),
    top3Rate: roundAdpPercent((t3 / rankEligible.length) * 100),
    rankEligible: rankEligible.length,
    sampleInsufficient: false,
  };
}

export function refCompetitorPresentGaps(observations, scenarios) {
  const scenarioIds = new Set((scenarios || []).map((s) => s.scenarioId));
  let gaps = 0;
  const comparable = (observations || []).filter(
    (o) => refIsComparableObservation(o) && scenarioIds.has(o.scenarioId)
  );
  for (const o of comparable) {
    if (o.mentioned) continue;
    const comps = o.competitorsMentioned || [];
    if (comps.length > 0) gaps += 1;
  }
  return { competitorPresentGapObservations: gaps };
}

export function refCitationCounts(observations) {
  let withCitations = 0;
  let comparable = 0;
  for (const o of observations || []) {
    if (!refIsComparableObservation(o)) continue;
    comparable += 1;
    if ((o.sourcesCited || []).length > 0) withCitations += 1;
  }
  return {
    comparable,
    withCitations,
    citationShare: comparable > 0 ? roundAdpPercent((withCitations / comparable) * 100) : null,
  };
}

export function nearlyEqual(a, b, tol = ROUNDING_TOLERANCE_PP) {
  if (a == null && b == null) return true;
  if (a == null || b == null) return false;
  return Math.abs(Number(a) - Number(b)) <= tol;
}

/**
 * Build full reference metric pack from frozen observations + profile.
 * Uses assurance reinterpretation of subject presence (not stored flags).
 */
export function buildReferenceMetricPack(period, scenarios, propertyProfile) {
  const interpreted = attachReferenceInterpretation(period?.observations || [], propertyProfile);
  const cons = refConsiderationAndScenario(interpreted, scenarios);
  const demand = refDemandCapture(interpreted, scenarios);
  const providers = refProviderPresence(interpreted);
  const territories = refTerritoryPresence(interpreted, scenarios);
  const rank = refRankAppearance(interpreted);
  const gaps = refCompetitorPresentGaps(interpreted, scenarios);
  const citations = refCitationCounts(interpreted);

  return {
    version: "adp_reference_metrics_v1",
    resolverVersion: ASSURANCE_RESOLVER_VERSION,
    considerationRate: cons.considerationRate,
    scenarioPresence: cons.scenarioPresence,
    demandCapture: demand.overallRate,
    demandCaptureByIntent: demand.byIntent,
    providers,
    territories,
    numberOneAppearance: rank.numberOneRate,
    top3Appearance: rank.top3Rate,
    rankEligible: rank.rankEligible,
    competitorPresentGaps: gaps.competitorPresentGapObservations,
    citations,
    interpretedObservationCount: interpreted.length,
  };
}

/**
 * Compare production published/payload values to reference pack.
 */
export function reconcileProductionVsReference(production, reference) {
  const rows = [
    {
      metric: "AI Consideration Rate",
      production: production?.considerationRate ?? null,
      reference: reference.considerationRate,
    },
    {
      metric: "Scenario Presence",
      production: production?.scenarioPresence ?? null,
      reference: reference.scenarioPresence,
    },
    {
      metric: "Demand Capture",
      production: production?.demandCapture ?? null,
      reference: reference.demandCapture,
    },
    {
      metric: "#1 Appearance",
      production: production?.numberOneAppearance ?? null,
      reference: reference.numberOneAppearance,
    },
    {
      metric: "Top-3 Appearance",
      production: production?.top3Appearance ?? null,
      reference: reference.top3Appearance,
    },
  ];
  return rows.map((r) => {
    let status = "PASS";
    if (r.production == null && r.reference == null) status = "PASS";
    else if (r.production == null && r.reference != null) status = "DISCLOSURE";
    else if (!nearlyEqual(r.production, r.reference)) status = "FAIL";
    return {
      ...r,
      delta:
        r.production == null || r.reference == null
          ? null
          : Math.round((Number(r.production) - Number(r.reference)) * 10) / 10,
      status,
    };
  });
}
