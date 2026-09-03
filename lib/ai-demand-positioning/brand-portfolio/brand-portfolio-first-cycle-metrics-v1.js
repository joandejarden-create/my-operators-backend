/**
 * Brand & Portfolio metrics — PORTFOLIO_KPI_CONTRACT_V1_1 grains.
 * Presence / Benchmark / Index / Rank = PROVIDER_OBSERVATION
 * #1 / Top-3 / Displacement / Shared = UNIQUE_SCENARIO
 * portfolioScenarioPresence = INTERNAL_DIAGNOSTIC (scenario-any; not customer KPI)
 */

import { detectPropertyMention, normalizeSubjectHaystack, findTokenBoundaryIndex } from "../execution/response-parser.js";
import { hashResponse } from "../measurement-assurance/prompt-persistence-v1.js";
import { PORTFOLIO_KPI_CONTRACT_V1_1 } from "./brand-portfolio-first-cycle-contract-v1.js";

export const PORTFOLIO_METRICS_VERSION = "ADP_BRAND_PORTFOLIO_METRICS_V1_1";
export const PORTFOLIO_METRICS_VERSION_LEGACY_SCENARIO_ANY = "ADP_BRAND_PORTFOLIO_METRICS_V1_SCENARIO_ANY";
export const PORTFOLIO_FROZEN_PEER_UNIVERSE_ENFORCEMENT = "PORTFOLIO_FROZEN_PEER_UNIVERSE_ENFORCEMENT";
export const PORTFOLIO_KPI_PEER_ADEQUACY_INTEGRITY = "PORTFOLIO_KPI_PEER_ADEQUACY_INTEGRITY";
export const PORTFOLIO_METRIC_GRAIN_INTEGRITY = "PORTFOLIO_METRIC_GRAIN_INTEGRITY";
export const PORTFOLIO_BENCHMARK_GRAIN_ALIGNMENT = "PORTFOLIO_BENCHMARK_GRAIN_ALIGNMENT";
export const PORTFOLIO_RANKING_GRAIN_ALIGNMENT = "PORTFOLIO_RANKING_GRAIN_ALIGNMENT";
export const PORTFOLIO_RANK_POSITION_SCENARIO_GRAIN_INTEGRITY = "PORTFOLIO_RANK_POSITION_SCENARIO_GRAIN_INTEGRITY";
export const PORTFOLIO_MEASUREMENT_CONTRACT_CONFORMANCE = "PORTFOLIO_MEASUREMENT_CONTRACT_CONFORMANCE";

const PROVIDERS = ["openai", "gemini", "perplexity", "claude"];

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

function comparableObs(obs) {
  return obs && !obs.error && obs.rawResponse && obs.metricInclusion !== false;
}

function pct1(rate) {
  if (rate == null || Number.isNaN(rate)) return null;
  return Math.round(rate * 1000) / 10;
}

function formatPct(rate) {
  if (rate == null || Number.isNaN(rate)) return null;
  return `${(rate * 100).toFixed(1)}%`;
}

function rankEntities(entities) {
  return [...entities]
    .sort((a, b) => b.presenceRate - a.presenceRate || a.name.localeCompare(b.name))
    .map((row, idx) => ({
      ...row,
      rank: idx + 1,
      rankLabel: `#${idx + 1}`,
      presencePct: pct1(row.presenceRate),
      numerator: row.obsHits,
      denominator: row.obsDenom,
      tieState: null,
    }));
}

/**
 * @param {object} args
 * @param {object} args.profile
 * @param {object} args.peerSet
 * @param {object[]} args.scenarios
 * @param {object[]} args.observations
 */
export function computeBrandPortfolioMetricsV1({ profile, peerSet, scenarios, observations, lens }) {
  const peerList = peerSet.included || [];
  const peerIds = new Set(peerList.map((p) => p.canonicalEntityId));
  const adequacy = peerSet.adequacy;
  const scenarioIds = scenarios.map((s) => s.scenarioId);

  const byScenario = new Map();
  for (const sid of scenarioIds) byScenario.set(sid, []);
  for (const obs of observations || []) {
    if (!byScenario.has(obs.scenarioId)) continue;
    byScenario.get(obs.scenarioId).push(obs);
  }

  const scenarioRows = [];
  for (const sc of scenarios) {
    const obsList = (byScenario.get(sc.scenarioId) || []).filter(comparableObs);
    let subjectPresent = false;
    let subjectRankBest = null;
    const peersPresent = new Set();
    const byProvider = {};

    for (const obs of obsList) {
      const subject = detectPropertyMention(obs.rawResponse, profile);
      const mentioned = !!subject.mentioned;
      if (mentioned) {
        subjectPresent = true;
        if (subject.position != null) {
          subjectRankBest =
            subjectRankBest == null ? subject.position : Math.min(subjectRankBest, subject.position);
        }
      }
      const peerHits = [];
      for (const peer of peerList) {
        if (peerMentioned(obs.rawResponse, peer.peerHotel)) {
          peersPresent.add(peer.canonicalEntityId);
          peerHits.push(peer.canonicalEntityId);
        }
      }
      byProvider[obs.provider] = {
        subjectMentioned: mentioned,
        subjectPosition: subject.position ?? null,
        peerHits,
        observationId: obs.observationId,
      };
    }

    scenarioRows.push({
      scenarioId: sc.scenarioId,
      territory: sc.territory,
      subjectPresent,
      subjectRankBest,
      peersPresent: [...peersPresent],
      eligiblePeerPresent: peersPresent.size > 0,
      displacement: !subjectPresent && peersPresent.size > 0,
      shared: subjectPresent && peersPresent.size > 0,
      byProvider,
    });
  }

  const eligibleScenarios = scenarioRows.length;
  const subjectScenarioHits = scenarioRows.filter((r) => r.subjectPresent).length;

  /** INTERNAL_DIAGNOSTIC — former headline (scenario-any). Not customer Portfolio AI Presence. */
  const portfolioScenarioPresence = eligibleScenarios ? subjectScenarioHits / eligibleScenarios : null;

  const comparable = (observations || []).filter(comparableObs);
  const subjectObsHits = comparable.filter((o) => detectPropertyMention(o.rawResponse, profile).mentioned).length;
  const portfolioAiPresence = comparable.length ? subjectObsHits / comparable.length : null;

  // Peer observation-grain presence (same grain as subject)
  const peerPresence = peerList.map((peer) => {
    let hits = 0;
    let denom = 0;
    for (const obs of comparable) {
      denom += 1;
      if (peerMentioned(obs.rawResponse, peer.peerHotel)) hits += 1;
    }
    return {
      canonicalEntityId: peer.canonicalEntityId,
      name: peer.peerHotel,
      brand: peer.brand,
      presenceRate: denom ? hits / denom : 0,
      obsHits: hits,
      obsDenom: denom,
      scenarioHits: scenarioRows.filter((r) => r.peersPresent.includes(peer.canonicalEntityId)).length,
      isSubject: false,
    };
  });

  const subjectRow = {
    canonicalEntityId: profile.propertyId || profile.id,
    name: profile.name,
    brand: profile.brand || null,
    presenceRate: portfolioAiPresence ?? 0,
    obsHits: subjectObsHits,
    obsDenom: comparable.length,
    scenarioHits: subjectScenarioHits,
    isSubject: true,
  };

  const rankingUniverse = rankEntities([...peerPresence, subjectRow]).map((row) => ({
    ...row,
    peerSetVersion: peerSet.peerSetVersion,
    deltaDisplay: "—",
  }));

  for (const r of rankingUniverse) {
    const same = rankingUniverse.filter((x) => x.presenceRate === r.presenceRate);
    if (same.length > 1) r.tieState = "TIE";
  }

  const subjectRankRow = rankingUniverse.find((r) => r.isSubject);

  // #1 / Top-3 — UNIQUE_SCENARIO grain (best ordinal across providers in scenario)
  const numberOne = scenarioRows.filter((r) => r.subjectPresent && r.subjectRankBest === 1).length;
  const numberOneRate = eligibleScenarios ? numberOne / eligibleScenarios : null;
  const top3AppearanceRate = eligibleScenarios
    ? scenarioRows.filter((r) => {
        if (!r.subjectPresent) return false;
        if (r.subjectRankBest == null) return true;
        return r.subjectRankBest <= 3;
      }).length / eligibleScenarios
    : null;

  let portfolioBenchmark = null;
  let portfolioPresenceIndex = null;
  if (adequacy.canBenchmark && peerPresence.length >= 5) {
    portfolioBenchmark = peerPresence.reduce((s, p) => s + p.presenceRate, 0) / peerPresence.length;
    if (portfolioBenchmark > 0 && portfolioAiPresence != null) {
      portfolioPresenceIndex = Math.round((portfolioAiPresence / portfolioBenchmark) * 100);
    }
  }

  const displacementScenarios = scenarioRows.filter((r) => r.displacement).map((r) => r.scenarioId);
  const sharedScenarios = scenarioRows.filter((r) => r.shared).map((r) => r.scenarioId);

  const peerDisplacement = peerList.map((peer) => {
    const count = scenarioRows.filter(
      (r) => !r.subjectPresent && r.peersPresent.includes(peer.canonicalEntityId)
    ).length;
    const shared = scenarioRows.filter(
      (r) => r.subjectPresent && r.peersPresent.includes(peer.canonicalEntityId)
    ).length;
    return {
      canonicalEntityId: peer.canonicalEntityId,
      name: peer.peerHotel,
      brand: peer.brand,
      displacementScenarios: count,
      scenariosShared: shared,
    };
  });

  // Territory breakdown — observation grain for presence/rank/bench/index
  const territories = [...new Set(scenarios.map((s) => s.territory))];
  const byTerritory = {};
  for (const t of territories) {
    const tScenarios = scenarios.filter((s) => s.territory === t);
    const tScenarioIds = new Set(tScenarios.map((s) => s.scenarioId));
    const tObs = comparable.filter((o) => tScenarioIds.has(o.scenarioId));
    const tSubjectHits = tObs.filter((o) => detectPropertyMention(o.rawResponse, profile).mentioned).length;
    const tPresence = tObs.length ? tSubjectHits / tObs.length : null;

    const tPeerPresence = peerList.map((peer) => {
      let hits = 0;
      for (const obs of tObs) {
        if (peerMentioned(obs.rawResponse, peer.peerHotel)) hits += 1;
      }
      return {
        canonicalEntityId: peer.canonicalEntityId,
        name: peer.peerHotel,
        brand: peer.brand,
        presenceRate: tObs.length ? hits / tObs.length : 0,
        obsHits: hits,
        obsDenom: tObs.length,
        isSubject: false,
      };
    });
    const tSubjectEntity = {
      canonicalEntityId: profile.propertyId || profile.id,
      name: profile.name,
      brand: profile.brand || null,
      presenceRate: tPresence ?? 0,
      obsHits: tSubjectHits,
      obsDenom: tObs.length,
      isSubject: true,
    };
    const tRanking = rankEntities([...tPeerPresence, tSubjectEntity]);
    const tSubjectRank = tRanking.find((r) => r.isSubject);
    let tBenchmark = null;
    let tIndex = null;
    if (adequacy.canBenchmark && tPeerPresence.length >= 5 && tObs.length > 0) {
      tBenchmark = tPeerPresence.reduce((s, p) => s + p.presenceRate, 0) / tPeerPresence.length;
      if (tBenchmark > 0 && tPresence != null) {
        tIndex = Math.round((tPresence / tBenchmark) * 100);
      }
    }

    // Legacy scenario-any for territory delta reporting
    const tRows = scenarioRows.filter((r) => r.territory === t);
    const tScenarioAny = tRows.length ? tRows.filter((r) => r.subjectPresent).length / tRows.length : null;

    byTerritory[t] = {
      territory: t,
      scenarios: tScenarios.length,
      observations: tObs.length,
      subjectHits: tSubjectHits,
      presenceRate: tPresence,
      presenceDisplay: formatPct(tPresence),
      portfolioRank: tSubjectRank?.rank ?? null,
      portfolioRankOf: tRanking.length,
      portfolioBenchmark: tBenchmark,
      portfolioPresenceIndex: tIndex,
      rankingUniverse: tRanking,
      legacyScenarioAnyPresence: tScenarioAny,
    };
  }

  const byProvider = {};
  for (const p of PROVIDERS) {
    const obs = comparable.filter((o) => o.provider === p);
    const hits = obs.filter((o) => detectPropertyMention(o.rawResponse, profile).mentioned).length;
    byProvider[p] = {
      provider: p,
      observations: obs.length,
      subjectHits: hits,
      presenceRate: obs.length ? hits / obs.length : null,
      errors: (observations || []).filter((o) => o.provider === p && o.error).length,
    };
  }

  const equalProviderMean =
    PROVIDERS.every((p) => byProvider[p].presenceRate != null)
      ? PROVIDERS.reduce((a, p) => a + byProvider[p].presenceRate, 0) / PROVIDERS.length
      : null;

  const kpis = [];
  kpis.push({
    id: "portfolioAiPresence",
    label: "Portfolio AI Presence",
    value: formatPct(portfolioAiPresence),
    valueRaw: portfolioAiPresence,
    meta: `${subjectObsHits} of ${comparable.length} AI observations`,
    grain: "PROVIDER_OBSERVATION",
    available: portfolioAiPresence != null,
  });
  if (adequacy.canRank) {
    kpis.push({
      id: "portfolioRank",
      label: "Portfolio Rank",
      value: subjectRankRow ? `${subjectRankRow.rankLabel} of ${rankingUniverse.length}` : null,
      valueRaw: subjectRankRow?.rank ?? null,
      meta: `Among ${rankingUniverse.length} relevant ${(lens && lens.label) || peerSet.lensLabel || "peer"} hotels`,
      grain: "PROVIDER_OBSERVATION",
      available: true,
    });
  }
  if (adequacy.canBenchmark && portfolioBenchmark != null) {
    kpis.push({
      id: "portfolioBenchmark",
      label: "Portfolio Benchmark",
      value: formatPct(portfolioBenchmark),
      valueRaw: portfolioBenchmark,
      meta: "Average AI Presence of the peer set",
      grain: "PROVIDER_OBSERVATION",
      available: true,
    });
  }
  if (adequacy.canIndex && portfolioPresenceIndex != null) {
    kpis.push({
      id: "portfolioPresenceIndex",
      label: "Portfolio Presence Index",
      value: String(portfolioPresenceIndex),
      valueRaw: portfolioPresenceIndex,
      meta: "Your presence vs. peer benchmark",
      grain: "PROVIDER_OBSERVATION",
      available: true,
    });
  }
  kpis.push({
    id: "numberOneAppearance",
    label: "#1 Appearance",
    value: formatPct(numberOneRate),
    valueRaw: numberOneRate,
    meta: `${numberOne} of ${eligibleScenarios} scenarios`,
    grain: "UNIQUE_SCENARIO",
    available: numberOneRate != null,
  });
  kpis.push({
    id: "top3Appearance",
    label: "Top-3 Appearance",
    value: formatPct(top3AppearanceRate),
    valueRaw: top3AppearanceRate,
    meta: `${Math.round((top3AppearanceRate || 0) * eligibleScenarios)} of ${eligibleScenarios} scenarios`,
    grain: "UNIQUE_SCENARIO",
    available: top3AppearanceRate != null,
  });

  const tableRows = rankingUniverse.map((r) => {
    const disp = peerDisplacement.find((d) => d.canonicalEntityId === r.canonicalEntityId);
    return {
      rank: r.rank,
      rankLabel: r.rankLabel,
      name: r.name,
      brand: r.brand,
      isSubject: r.isSubject,
      presenceDisplay: `${r.presencePct}%`,
      deltaDisplay: "—",
      displacementDisplay: r.isSubject ? "—" : String(disp?.displacementScenarios ?? 0),
      sharedDisplay: r.isSubject ? "—" : String(disp?.scenariosShared ?? 0),
      canonicalEntityId: r.canonicalEntityId,
    };
  });

  const grainIntegrity = {
    gate: PORTFOLIO_METRIC_GRAIN_INTEGRITY,
    pass:
      Math.abs((portfolioAiPresence ?? -1) - (equalProviderMean ?? -2)) < 0.001 &&
      peerPresence.every((p) => p.obsDenom === comparable.length) &&
      (subjectRankRow?.presenceRate ?? null) === portfolioAiPresence,
    subjectGrain: "PROVIDER_OBSERVATION",
    peerGrain: "PROVIDER_OBSERVATION",
    benchmarkGrain: "PROVIDER_OBSERVATION",
    rankingGrain: "PROVIDER_OBSERVATION",
  };

  const benchmarkAlignment = {
    gate: PORTFOLIO_BENCHMARK_GRAIN_ALIGNMENT,
    pass:
      !adequacy.canBenchmark ||
      (portfolioBenchmark != null &&
        Math.abs(
          portfolioBenchmark - peerPresence.reduce((s, p) => s + p.presenceRate, 0) / peerPresence.length
        ) < 0.0001),
  };

  const rankingAlignment = {
    gate: PORTFOLIO_RANKING_GRAIN_ALIGNMENT,
    pass: rankingUniverse.every((r) => r.isSubject || peerIds.has(r.canonicalEntityId)),
  };

  const rankPositionScenarioGrain = {
    gate: PORTFOLIO_RANK_POSITION_SCENARIO_GRAIN_INTEGRITY,
    pass: true,
    numberOneGrain: "UNIQUE_SCENARIO",
    top3Grain: "UNIQUE_SCENARIO",
    aggregation: PORTFOLIO_KPI_CONTRACT_V1_1.kpis.numberOneAppearance.providerToScenarioAggregation,
  };

  const contractConformance = {
    gate: PORTFOLIO_MEASUREMENT_CONTRACT_CONFORMANCE,
    pass:
      grainIntegrity.pass &&
      benchmarkAlignment.pass &&
      rankingAlignment.pass &&
      rankPositionScenarioGrain.pass,
    contractVersion: PORTFOLIO_KPI_CONTRACT_V1_1.version,
  };

  const enforcement = {
    gate: PORTFOLIO_FROZEN_PEER_UNIVERSE_ENFORCEMENT,
    pass: rankingUniverse.every((r) => r.isSubject || peerIds.has(r.canonicalEntityId)),
    peerCountInRank: rankingUniverse.filter((r) => !r.isSubject).length,
    frozenPeerCount: peerList.length,
  };

  const adequacyGate = {
    gate: PORTFOLIO_KPI_PEER_ADEQUACY_INTEGRITY,
    pass:
      (adequacy.canBenchmark
        ? kpis.some((k) => k.id === "portfolioBenchmark")
        : !kpis.some((k) => k.id === "portfolioBenchmark")) &&
      (adequacy.canIndex
        ? kpis.some((k) => k.id === "portfolioPresenceIndex")
        : !kpis.some((k) => k.id === "portfolioPresenceIndex")),
    adequacyStatus: adequacy.status,
  };

  return {
    metricsVersion: PORTFOLIO_METRICS_VERSION,
    kpiContractVersion: PORTFOLIO_KPI_CONTRACT_V1_1.version,
    lens: lens || { lensId: peerSet.lensId, label: peerSet.lensLabel },
    peerSetId: peerSet.peerSetId,
    peerSetVersion: peerSet.peerSetVersion,
    portfolioAiPresence,
    portfolioScenarioPresence,
    observationPresence: portfolioAiPresence,
    equalProviderMean,
    portfolioRank: subjectRankRow?.rank ?? null,
    portfolioRankOf: rankingUniverse.length,
    numberOneAppearance: numberOneRate,
    top3Appearance: top3AppearanceRate,
    portfolioBenchmark,
    portfolioPresenceIndex,
    kpis: kpis.filter((k) => k.available !== false),
    suppressedKpis: (adequacy.suppressKpis || []).slice(),
    rankingUniverse,
    tableRows,
    displacement: {
      scenarioCount: displacementScenarios.length,
      scenarioIds: displacementScenarios,
      byPeer: peerDisplacement.sort((a, b) => b.displacementScenarios - a.displacementScenarios),
    },
    scenariosShared: {
      scenarioCount: sharedScenarios.length,
      scenarioIds: sharedScenarios,
      byPeer: [...peerDisplacement].sort((a, b) => b.scenariosShared - a.scenariosShared),
    },
    byTerritory,
    byProvider,
    scenarioRows,
    firstPeriodDelta: "—",
    hasPriorPeriod: false,
    gates: {
      enforcement,
      adequacyGate,
      grainIntegrity,
      benchmarkAlignment,
      rankingAlignment,
      rankPositionScenarioGrain,
      contractConformance,
    },
  };
}

export function buildPositiveEvidencePack({ observations, profile, limit = 12 }) {
  const hits = [];
  for (const obs of observations || []) {
    if (!comparableObs(obs)) continue;
    const m = detectPropertyMention(obs.rawResponse, profile);
    if (!m.mentioned) continue;
    hits.push({
      observationId: obs.observationId,
      scenarioId: obs.scenarioId,
      provider: obs.provider,
      territory: obs.territory,
      matchedVariant: m.matchedVariant,
      context: m.context,
      position: m.position,
      promptHash: obs.promptHash,
      responseHash: obs.responseHash || hashResponse(obs.rawResponse),
      exactResponse: obs.rawResponse,
    });
    if (hits.length >= limit) break;
  }
  return hits;
}

export function buildMissingEvidencePack({ observations, profile, limit = 12 }) {
  const misses = [];
  for (const obs of observations || []) {
    if (!comparableObs(obs)) continue;
    const m = detectPropertyMention(obs.rawResponse, profile);
    if (m.mentioned) continue;
    misses.push({
      observationId: obs.observationId,
      scenarioId: obs.scenarioId,
      provider: obs.provider,
      territory: obs.territory,
      promptHash: obs.promptHash,
      responseHash: obs.responseHash || hashResponse(obs.rawResponse),
      exactResponse: obs.rawResponse,
    });
    if (misses.length >= limit) break;
  }
  return misses;
}
