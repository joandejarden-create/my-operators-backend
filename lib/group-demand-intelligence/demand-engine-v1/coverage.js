/**
 * Demand-engine research coverage matrix (qualitative states — no fake precision).
 */

import {
  allDemandEngines,
  COVERAGE_STATE,
  DEMAND_ENGINE_TAXONOMY,
  defaultEnginesForHotel,
} from "./taxonomy.js";
import { classifyDemandEngine } from "./classify-engine.js";

function coverageStateFrom({
  researched,
  candidateCount,
  readyCount,
  watchCount,
  qualifiedCount,
  sourceFamiliesAttempted,
}) {
  if (!researched && candidateCount === 0) return COVERAGE_STATE.NOT_RESEARCHED;
  const useful = readyCount + watchCount;
  const families = (sourceFamiliesAttempted || []).length;
  if (useful >= 3 && families >= 3 && candidateCount >= 8) return COVERAGE_STATE.SATURATED;
  if (useful >= 1 && families >= 2 && candidateCount >= 4) return COVERAGE_STATE.DEEP;
  if (qualifiedCount >= 1 || (candidateCount >= 3 && families >= 1)) return COVERAGE_STATE.ADEQUATE;
  if (researched || candidateCount > 0 || families > 0) return COVERAGE_STATE.LIGHT;
  return COVERAGE_STATE.NOT_RESEARCHED;
}

/**
 * @param {{ hotelId, marketId, hotelKey, opportunities[], researchLedger? }} input
 */
export function buildGdiDemandEngineCoverage(input = {}) {
  const hotelId = input.hotelId || null;
  const marketId = input.marketId || input.market || null;
  const hotelKey = input.hotelKey || "UNKNOWN";
  const ops = input.opportunities || [];
  const ledger = input.researchLedger || {};
  const engines = allDemandEngines();
  const priority = defaultEnginesForHotel(hotelKey);

  const byEngine = {};
  for (const e of engines) {
    byEngine[e] = {
      hotelId,
      marketId,
      demandEngine: e,
      label: DEMAND_ENGINE_TAXONOMY[e]?.label || e,
      candidateCount: 0,
      qualifiedCount: 0,
      readyCount: 0,
      watchCount: 0,
      sourceFamiliesAttempted: new Set(ledger[e]?.sourceFamilies || []),
      languagesAttempted: new Set(ledger[e]?.languages || []),
      lastResearchDate: ledger[e]?.lastResearchDate || null,
      gaps: [],
    };
  }

  for (const opp of ops) {
    const { demandEngine } = classifyDemandEngine(opp);
    const row = byEngine[demandEngine] || byEngine[engines[0]];
    row.candidateCount += 1;
    const pri = String(opp.priority || "");
    if (pri && pri !== "DISQUALIFIED") row.qualifiedCount += 1;
    if (opp.customerVisible === true || opp.customerReadiness?.ok === true) row.readyCount += 1;
    if (opp.watchValidation?.ok === true || opp.customerFacingState === "FUTURE_WATCH") {
      row.watchCount += 1;
    }
    const scout = opp.discoveryMeta?.scoutFamily || opp.demandFamily;
    if (scout) row.sourceFamiliesAttempted.add(String(scout));
    const lang = opp.queryLanguage || opp.discoveryMeta?.queryLanguage;
    if (lang) row.languagesAttempted.add(String(lang));
  }

  const rows = engines.map((e) => {
    const r = byEngine[e];
    const researched = Boolean(ledger[e]?.researched) || r.sourceFamiliesAttempted.size > 0;
    const coverageState = coverageStateFrom({
      researched,
      candidateCount: r.candidateCount,
      readyCount: r.readyCount,
      watchCount: r.watchCount,
      qualifiedCount: r.qualifiedCount,
      sourceFamiliesAttempted: [...r.sourceFamiliesAttempted],
    });
    const remainingKnownGaps = [];
    if (coverageState === COVERAGE_STATE.NOT_RESEARCHED) {
      remainingKnownGaps.push("no_research_pass");
    }
    if (r.candidateCount > 0 && r.readyCount === 0 && r.watchCount === 0) {
      remainingKnownGaps.push("candidates_without_useful_yield");
    }
    if (r.sourceFamiliesAttempted.size < 2 && coverageState !== COVERAGE_STATE.SATURATED) {
      remainingKnownGaps.push("narrow_source_family_coverage");
    }
    const yieldScore =
      r.candidateCount > 0 ? (r.readyCount + r.watchCount) / r.candidateCount : 0;

    return {
      hotelId,
      marketId,
      hotelKey,
      demandEngine: e,
      label: r.label,
      coverageState,
      sourceFamiliesAttempted: [...r.sourceFamiliesAttempted],
      languagesAttempted: [...r.languagesAttempted],
      lastResearchDate: r.lastResearchDate,
      candidateCount: r.candidateCount,
      qualifiedCount: r.qualifiedCount,
      readyCount: r.readyCount,
      watchCount: r.watchCount,
      yield: Math.round(yieldScore * 1000) / 1000,
      remainingKnownGaps,
      priorityRank: priority.indexOf(e) >= 0 ? priority.indexOf(e) + 1 : 99,
      underResearched:
        coverageState === COVERAGE_STATE.NOT_RESEARCHED ||
        coverageState === COVERAGE_STATE.LIGHT,
    };
  });

  return {
    hotelId,
    marketId,
    hotelKey,
    builtAt: new Date().toISOString(),
    rows,
    underResearchedEngines: rows.filter((r) => r.underResearched).map((r) => r.demandEngine),
    summary: {
      notResearched: rows.filter((r) => r.coverageState === COVERAGE_STATE.NOT_RESEARCHED).length,
      light: rows.filter((r) => r.coverageState === COVERAGE_STATE.LIGHT).length,
      adequate: rows.filter((r) => r.coverageState === COVERAGE_STATE.ADEQUATE).length,
      deep: rows.filter((r) => r.coverageState === COVERAGE_STATE.DEEP).length,
      saturated: rows.filter((r) => r.coverageState === COVERAGE_STATE.SATURATED).length,
    },
  };
}
