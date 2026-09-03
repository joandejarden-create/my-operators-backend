/**
 * INDEPENDENT Brand & Portfolio metric reference calculator.
 * Must NOT import computeBrandPortfolioMetricsV1 — Measurement Assurance reference only.
 */

import { detectPropertyMention, normalizeSubjectHaystack, findTokenBoundaryIndex } from "../execution/response-parser.js";

export const PORTFOLIO_METRIC_GRAIN_INTEGRITY = "PORTFOLIO_METRIC_GRAIN_INTEGRITY";
export const PORTFOLIO_MEASUREMENT_CONTRACT_CONFORMANCE = "PORTFOLIO_MEASUREMENT_CONTRACT_CONFORMANCE";
export const PORTFOLIO_INDEPENDENT_REFERENCE_METRIC_INTEGRITY = "PORTFOLIO_INDEPENDENT_REFERENCE_METRIC_INTEGRITY";
export const PROVIDER_DISAGREEMENT_METRIC_GRAIN_GOLD = "PROVIDER_DISAGREEMENT_METRIC_GRAIN_GOLD";
export const PORTFOLIO_BENCHMARK_GRAIN_ALIGNMENT = "PORTFOLIO_BENCHMARK_GRAIN_ALIGNMENT";
export const PORTFOLIO_RANKING_GRAIN_ALIGNMENT = "PORTFOLIO_RANKING_GRAIN_ALIGNMENT";
export const PORTFOLIO_METRIC_GRAIN_MISMATCH = "PORTFOLIO_METRIC_GRAIN_MISMATCH";

export const PROVIDERS = Object.freeze(["openai", "gemini", "perplexity", "claude"]);

function peerMentioned(response, peerHotel) {
  if (!response || !peerHotel) return false;
  const text = normalizeSubjectHaystack(response);
  const variants = [
    peerHotel,
    peerHotel.replace(/,.*$/, "").trim(),
    peerHotel.replace(/\s+(Curio Collection|A Tapestry Collection).*$/i, "").trim(),
    peerHotel.replace(/\s+by Hilton.*$/i, "").trim(),
    peerHotel.replace(/\s+Hotel$/i, "").trim(),
  ].filter((v) => v && v.length >= 5);
  for (const v of variants) {
    const needle = normalizeSubjectHaystack(v);
    if (needle.length < 5) continue;
    if (findTokenBoundaryIndex(text, needle) !== -1) return true;
  }
  return false;
}

function comparable(obs) {
  return obs && !obs.error && obs.rawResponse && obs.metricInclusion !== false;
}

/**
 * Independent multi-grain presence calculation for subject + peers.
 */
export function computeIndependentPortfolioGrainAudit({ profile, peerSet, scenarios, observations }) {
  const peers = peerSet.included || [];
  const byScenario = new Map();
  for (const s of scenarios) byScenario.set(s.scenarioId, {});
  for (const obs of observations || []) {
    if (!byScenario.has(obs.scenarioId)) continue;
    if (!comparable(obs)) {
      byScenario.get(obs.scenarioId)[obs.provider] = { comparable: false, subject: false, peers: [] };
      continue;
    }
    const subject = !!detectPropertyMention(obs.rawResponse, profile).mentioned;
    const peerHits = peers
      .filter((p) => peerMentioned(obs.rawResponse, p.peerHotel))
      .map((p) => p.canonicalEntityId);
    byScenario.get(obs.scenarioId)[obs.provider] = {
      comparable: true,
      subject,
      peers: peerHits,
      position: detectPropertyMention(obs.rawResponse, profile).position ?? null,
    };
  }

  const matrix = scenarios.map((s) => {
    const row = { scenarioId: s.scenarioId, territory: s.territory };
    let anySubject = false;
    let allSubject = true;
    let comparableCount = 0;
    for (const p of PROVIDERS) {
      const cell = byScenario.get(s.scenarioId)?.[p];
      const present = !!(cell && cell.comparable && cell.subject);
      row[p] = cell?.comparable ? (present ? "PRESENT" : "ABSENT") : "MISSING";
      if (cell?.comparable) {
        comparableCount += 1;
        if (present) anySubject = true;
        else allSubject = false;
      }
    }
    if (comparableCount === 0) allSubject = false;
    row.anyProviderScenarioPresence = anySubject;
    row.allProviderScenarioPresence = comparableCount === PROVIDERS.length && allSubject;
    return row;
  });

  // A — observation grain
  let obsDenom = 0;
  let obsNum = 0;
  const providerHits = Object.fromEntries(PROVIDERS.map((p) => [p, { hits: 0, denom: 0 }]));
  for (const s of scenarios) {
    for (const p of PROVIDERS) {
      const cell = byScenario.get(s.scenarioId)?.[p];
      if (!cell?.comparable) continue;
      obsDenom += 1;
      providerHits[p].denom += 1;
      if (cell.subject) {
        obsNum += 1;
        providerHits[p].hits += 1;
      }
    }
  }
  const observationPresence = obsDenom ? obsNum / obsDenom : null;
  const providerRates = Object.fromEntries(
    PROVIDERS.map((p) => [
      p,
      providerHits[p].denom ? providerHits[p].hits / providerHits[p].denom : null,
    ])
  );
  const equalProviderMean =
    PROVIDERS.every((p) => providerRates[p] != null)
      ? PROVIDERS.reduce((a, p) => a + providerRates[p], 0) / PROVIDERS.length
      : null;

  const anyProviderScenarioPresence =
    scenarios.length > 0
      ? matrix.filter((r) => r.anyProviderScenarioPresence).length / scenarios.length
      : null;
  const allProviderScenarioPresence =
    scenarios.length > 0
      ? matrix.filter((r) => r.allProviderScenarioPresence).length / scenarios.length
      : null;

  // Peer rates — observation grain
  const peerObs = peers.map((peer) => {
    let hits = 0;
    let denom = 0;
    for (const s of scenarios) {
      for (const p of PROVIDERS) {
        const cell = byScenario.get(s.scenarioId)?.[p];
        if (!cell?.comparable) continue;
        denom += 1;
        if (cell.peers.includes(peer.canonicalEntityId)) hits += 1;
      }
    }
    return {
      canonicalEntityId: peer.canonicalEntityId,
      name: peer.peerHotel,
      brand: peer.brand,
      observationPresence: denom ? hits / denom : 0,
      hits,
      denom,
    };
  });

  // Peer rates — any-provider scenario grain (INTERNAL_DIAGNOSTIC only)
  const peerScenario = peers.map((peer) => {
    let hits = 0;
    for (const s of scenarios) {
      let any = false;
      for (const p of PROVIDERS) {
        const cell = byScenario.get(s.scenarioId)?.[p];
        if (cell?.comparable && cell.peers.includes(peer.canonicalEntityId)) any = true;
      }
      if (any) hits += 1;
    }
    return {
      canonicalEntityId: peer.canonicalEntityId,
      name: peer.peerHotel,
      brand: peer.brand,
      anyProviderScenarioPresence: scenarios.length ? hits / scenarios.length : 0,
      hits,
      denom: scenarios.length,
    };
  });

  function rankBy(rateKey, entities) {
    return [...entities]
      .sort((a, b) => (b[rateKey] || 0) - (a[rateKey] || 0) || a.name.localeCompare(b.name))
      .map((e, i) => ({ ...e, rank: i + 1, presenceRate: e[rateKey] }));
  }

  const subjectObsEntity = {
    canonicalEntityId: profile.propertyId,
    name: profile.name,
    brand: profile.brand,
    observationPresence,
    isSubject: true,
  };
  const subjectScenarioEntity = {
    canonicalEntityId: profile.propertyId,
    name: profile.name,
    brand: profile.brand,
    anyProviderScenarioPresence,
    isSubject: true,
  };

  const rankingObservation = rankBy("observationPresence", [
    ...peerObs.map((p) => ({ ...p, isSubject: false })),
    subjectObsEntity,
  ]);
  const rankingScenarioAny = rankBy("anyProviderScenarioPresence", [
    ...peerScenario.map((p) => ({ ...p, isSubject: false })),
    subjectScenarioEntity,
  ]);

  const peerObsRates = peerObs.map((p) => p.observationPresence);
  const benchmarkObservation =
    peerObsRates.length >= 5 ? peerObsRates.reduce((a, b) => a + b, 0) / peerObsRates.length : null;
  const indexObservation =
    benchmarkObservation > 0 && observationPresence != null
      ? Math.round((observationPresence / benchmarkObservation) * 100)
      : null;

  const peerScenRates = peerScenario.map((p) => p.anyProviderScenarioPresence);
  const benchmarkScenarioAny =
    peerScenRates.length >= 5 ? peerScenRates.reduce((a, b) => a + b, 0) / peerScenRates.length : null;
  const indexScenarioAny =
    benchmarkScenarioAny > 0 && anyProviderScenarioPresence != null
      ? Math.round((anyProviderScenarioPresence / benchmarkScenarioAny) * 100)
      : null;

  // #1 / Top-3 — observation grain (provider responses with ordinal)
  let n1Obs = 0;
  let top3Obs = 0;
  let ordinalDenom = 0;
  for (const s of scenarios) {
    for (const p of PROVIDERS) {
      const cell = byScenario.get(s.scenarioId)?.[p];
      if (!cell?.comparable || !cell.subject) continue;
      ordinalDenom += 1;
      if (cell.position === 1) n1Obs += 1;
      if (cell.position != null && cell.position <= 3) top3Obs += 1;
    }
  }
  // Scenario grain #1: best ordinal among providers that mentioned subject
  let n1Scen = 0;
  let top3Scen = 0;
  for (const s of scenarios) {
    let best = null;
    let mentioned = false;
    for (const p of PROVIDERS) {
      const cell = byScenario.get(s.scenarioId)?.[p];
      if (!cell?.comparable || !cell.subject) continue;
      mentioned = true;
      if (cell.position != null) best = best == null ? cell.position : Math.min(best, cell.position);
    }
    if (mentioned && best === 1) n1Scen += 1;
    if (mentioned && (best == null || best <= 3)) top3Scen += 1;
  }

  return {
    matrix,
    grains: {
      A_observationPresence: observationPresence,
      B_equalProviderMean: equalProviderMean,
      C_anyProviderScenarioPresence: anyProviderScenarioPresence,
      D_allProviderScenarioPresence: allProviderScenarioPresence,
    },
    providerRates,
    providerHits,
    obsNum,
    obsDenom,
    rankingObservation,
    rankingScenarioAny,
    benchmarkObservation,
    indexObservation,
    benchmarkScenarioAny,
    indexScenarioAny,
    numberOne: {
      observationGrain: obsDenom ? n1Obs / obsDenom : null,
      // among subject-present observations with any ordinal intent: use all subject-present obs
      observationAmongPresent: ordinalDenom ? n1Obs / Math.max(ordinalDenom, 1) : null,
      scenarioAnyGrain: scenarios.length ? n1Scen / scenarios.length : null,
    },
    top3: {
      observationGrain: obsDenom ? top3Obs / obsDenom : null,
      scenarioAnyGrain: scenarios.length ? top3Scen / scenarios.length : null,
    },
    subjectRankObservation: rankingObservation.find((r) => r.isSubject)?.rank ?? null,
    subjectRankScenarioAny: rankingScenarioAny.find((r) => r.isSubject)?.rank ?? null,
  };
}

/**
 * Gold case: one provider present in a scenario must not equal 100% observation presence.
 */
export function runProviderDisagreementMetricGrainGold() {
  const scenarios = [{ scenarioId: "gold_s1", territory: "business" }];
  const profile = { propertyId: "gold_subject", name: "Gold Subject Hotel", brand: "Independent" };
  const peerSet = { included: [] };
  const observations = [
    {
      scenarioId: "gold_s1",
      provider: "openai",
      rawResponse: "I recommend Gold Subject Hotel for business travelers.",
      metricInclusion: true,
    },
    { scenarioId: "gold_s1", provider: "gemini", rawResponse: "Try Peer Alpha instead.", metricInclusion: true },
    { scenarioId: "gold_s1", provider: "perplexity", rawResponse: "Consider Peer Beta.", metricInclusion: true },
    { scenarioId: "gold_s1", provider: "claude", rawResponse: "Look at Peer Gamma.", metricInclusion: true },
  ];
  const audit = computeIndependentPortfolioGrainAudit({ profile, peerSet, scenarios, observations });
  const observationOk = Math.abs((audit.grains.A_observationPresence || 0) - 0.25) < 0.001;
  const scenarioAnyOk = Math.abs((audit.grains.C_anyProviderScenarioPresence || 0) - 1.0) < 0.001;
  const distinguished = observationOk && scenarioAnyOk;
  return {
    gate: PROVIDER_DISAGREEMENT_METRIC_GRAIN_GOLD,
    pass: distinguished,
    observationPresence: audit.grains.A_observationPresence,
    anyProviderScenarioPresence: audit.grains.C_anyProviderScenarioPresence,
    expectedObservation: 0.25,
    expectedScenarioAny: 1.0,
  };
}

/**
 * Compare production metrics (V1.1) to independent reference — do not import internals beyond inputs.
 * @param {object} production - computeBrandPortfolioMetricsV1 output
 * @param {object} independent - computeIndependentPortfolioGrainAudit output
 */
export function assertIndependentReferenceMatch(production, independent, { tol = 0.0015 } = {}) {
  const defects = [];
  const near = (a, b, label) => {
    if (a == null && b == null) return;
    if (a == null || b == null || Math.abs(a - b) > tol) defects.push(label);
  };
  near(production.portfolioAiPresence, independent.grains.A_observationPresence, "AI_PRESENCE");
  near(production.equalProviderMean, independent.grains.B_equalProviderMean, "EQUAL_PROVIDER_MEAN");
  near(production.portfolioScenarioPresence, independent.grains.C_anyProviderScenarioPresence, "SCENARIO_PRESENCE_INTERNAL");
  near(production.portfolioBenchmark, independent.benchmarkObservation, "BENCHMARK");
  if (production.portfolioPresenceIndex != null || independent.indexObservation != null) {
    if (production.portfolioPresenceIndex !== independent.indexObservation) defects.push("INDEX");
  }
  if (production.portfolioRank !== independent.subjectRankObservation) defects.push("RANK");
  near(production.numberOneAppearance, independent.numberOne.scenarioAnyGrain, "NUMBER_ONE_SCENARIO");
  near(production.top3Appearance, independent.top3.scenarioAnyGrain, "TOP3_SCENARIO");
  for (const p of PROVIDERS) {
    near(production.byProvider?.[p]?.presenceRate, independent.providerRates?.[p], `PROVIDER_${p}`);
  }
  return {
    gate: PORTFOLIO_INDEPENDENT_REFERENCE_METRIC_INTEGRITY,
    pass: defects.length === 0,
    defects,
  };
}
