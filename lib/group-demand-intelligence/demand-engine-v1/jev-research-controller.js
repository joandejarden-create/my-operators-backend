/**
 * Jev Research Controller V1 — WHERE to look next, never WHAT is true.
 *
 * Decisions:
 * CONTINUE_ENGINE | SWITCH_ENGINE | SWITCH_SOURCE | SWITCH_LANGUAGE |
 * SWITCH_FEEDER_MARKET | DEEPEN_EXISTING_CANDIDATE | STOP_LOW_YIELD
 */

import { isJevEnabled } from "../jev/jev-config.js";
import { decide } from "../jev/jev-decision-service.js";
import { JEV_DECISION_TYPE } from "../jev/jev-types.js";
import { COVERAGE_STATE } from "./taxonomy.js";

export const CONTROLLER_ACTION = Object.freeze({
  CONTINUE_ENGINE: "CONTINUE_ENGINE",
  SWITCH_ENGINE: "SWITCH_ENGINE",
  SWITCH_SOURCE: "SWITCH_SOURCE",
  SWITCH_LANGUAGE: "SWITCH_LANGUAGE",
  SWITCH_FEEDER_MARKET: "SWITCH_FEEDER_MARKET",
  DEEPEN_EXISTING_CANDIDATE: "DEEPEN_EXISTING_CANDIDATE",
  STOP_LOW_YIELD: "STOP_LOW_YIELD",
});

/**
 * Deterministic controller from yield economics.
 */
export function decideResearchControllerDeterministic(input = {}) {
  const {
    coverage,
    engineYield = {},
    languageYield = {},
    sourceYield = {},
    feederYield = {},
    costUsd = 0,
    useful = 0,
    promisingDeepen = null,
    currentEngine = null,
  } = input;

  const decisions = [];
  const rows = coverage?.rows || [];

  // Stop saturated / zero-yield heavy engines
  for (const r of rows) {
    if (r.candidateCount >= 6 && (r.readyCount + r.watchCount) === 0) {
      decisions.push({
        action: CONTROLLER_ACTION.STOP_LOW_YIELD,
        demandEngine: r.demandEngine,
        rationale: `Zero useful yield on ${r.candidateCount} candidates`,
        source: "DETERMINISTIC",
        acceptedByPolicy: true,
      });
    }
  }

  if (promisingDeepen) {
    decisions.push({
      action: CONTROLLER_ACTION.DEEPEN_EXISTING_CANDIDATE,
      opportunityId: promisingDeepen.id || promisingDeepen.opportunityId,
      rationale: "Single-blocker near-miss deserves one targeted research action",
      source: "DETERMINISTIC",
      acceptedByPolicy: true,
    });
  }

  const under = rows.filter((r) => r.underResearched).sort((a, b) => a.priorityRank - b.priorityRank);
  if (under[0]) {
    if (currentEngine && currentEngine === under[0].demandEngine) {
      decisions.push({
        action: CONTROLLER_ACTION.CONTINUE_ENGINE,
        demandEngine: under[0].demandEngine,
        rationale: `Continue under-researched priority engine ${under[0].demandEngine}`,
        source: "DETERMINISTIC",
        acceptedByPolicy: true,
      });
    } else {
      decisions.push({
        action: CONTROLLER_ACTION.SWITCH_ENGINE,
        demandEngine: under[0].demandEngine,
        fromEngine: currentEngine,
        rationale: `Switch to under-researched engine ${under[0].demandEngine}`,
        source: "DETERMINISTIC",
        acceptedByPolicy: true,
      });
    }
  }

  // Language switch if primary yield poor and secondary available
  const langEntries = Object.entries(languageYield);
  if (langEntries.length >= 2) {
    const sorted = langEntries.sort((a, b) => (b[1].useful || 0) - (a[1].useful || 0));
    const [bestLang, best] = sorted[0];
    const [worstLang, worst] = sorted[sorted.length - 1];
    if ((worst.candidates || 0) >= 3 && (worst.useful || 0) === 0 && (best.useful || 0) > 0) {
      decisions.push({
        action: CONTROLLER_ACTION.SWITCH_LANGUAGE,
        recommendedLanguage: bestLang,
        stopLanguage: worstLang,
        rationale: `Stop ${worstLang} (0 useful); prefer ${bestLang}`,
        source: "DETERMINISTIC",
        acceptedByPolicy: true,
      });
    }
  }

  const feederEntries = Object.entries(feederYield);
  if (feederEntries.length) {
    const top = feederEntries.sort((a, b) => (b[1].useful || 0) - (a[1].useful || 0))[0];
    if (top && (top[1].candidates || 0) > 0) {
      decisions.push({
        action: CONTROLLER_ACTION.SWITCH_FEEDER_MARKET,
        recommendedFeederMarket: top[0],
        rationale: `Feeder ${top[0]} shows relative signal activity`,
        source: "DETERMINISTIC",
        acceptedByPolicy: true,
      });
    }
  }

  // Source switch when engine has candidates but no lodging-bearing sources
  for (const r of rows.filter((x) => x.coverageState === COVERAGE_STATE.LIGHT && x.candidateCount > 0)) {
    if ((r.sourceFamiliesAttempted || []).length <= 1) {
      decisions.push({
        action: CONTROLLER_ACTION.SWITCH_SOURCE,
        demandEngine: r.demandEngine,
        rationale: "Narrow source family — try procurement/official calendar next",
        source: "DETERMINISTIC",
        acceptedByPolicy: true,
      });
      break;
    }
  }

  // Cost guard
  if (costUsd >= 5 && useful === 0) {
    decisions.unshift({
      action: CONTROLLER_ACTION.STOP_LOW_YIELD,
      rationale: `Cost $${costUsd.toFixed(2)} with 0 useful candidates — halt tranche`,
      source: "DETERMINISTIC",
      acceptedByPolicy: true,
    });
  }

  return decisions.map((d) => ({
    ...d,
    writesVerifiedFacts: false,
    promotesOpportunities: false,
    changesThresholds: false,
    advisoryOnly: true,
  }));
}

/**
 * Controller entry — deterministic first; Jev may annotate CONTINUE/STOP only.
 */
export async function runJevGdiResearchController(input = {}) {
  const deterministic = decideResearchControllerDeterministic(input);
  const result = {
    hotelId: input.coverage?.hotelId,
    hotelKey: input.coverage?.hotelKey,
    decisions: deterministic,
    jevCalled: false,
    jevAccepted: 0,
    jevRejectedByPolicy: 0,
    jevError: null,
  };

  if (input.skipJev === true || !isJevEnabled()) {
    result.note = "Jev skipped — deterministic controller only";
    return result;
  }

  try {
    const decision = await decide({
      decisionType: JEV_DECISION_TYPE.STOP_CONTINUE,
      context: {
        targetType: "RESEARCH_CONTROLLER",
        entityType: "HOTEL_MARKET",
        queriesAlreadyRun: input.queriesRun || 0,
        costSoFarUsd: input.costUsd || 0,
        signalsFound: input.signalsFound || 0,
        opportunitiesCreated: input.useful || 0,
        evidenceSummary: JSON.stringify({
          engineYield: input.engineYield || {},
          under: input.coverage?.underResearchedEngines || [],
        }),
        decisionHint: "ROUTING_ONLY_NO_TRUTH",
      },
      existingDecision: {
        choice: deterministic.some((d) => d.action === CONTROLLER_ACTION.STOP_LOW_YIELD)
          ? "STOP"
          : "CONTINUE",
      },
      shadow: true,
    });
    result.jevCalled = true;
    const choice = decision?.choice || decision?.jevChoice;
    if (choice && /STOP/i.test(String(choice))) {
      const hasStop = deterministic.some((d) => d.action === CONTROLLER_ACTION.STOP_LOW_YIELD);
      if (hasStop) {
        result.jevAccepted += 1;
        result.decisions = deterministic.map((d) =>
          d.action === CONTROLLER_ACTION.STOP_LOW_YIELD
            ? { ...d, source: "JEV_ADVISORY+DETERMINISTIC", jevChoice: choice }
            : d
        );
      } else {
        result.jevRejectedByPolicy += 1;
        result.decisions.push({
          action: "JEV_STOP_REJECTED_BY_POLICY",
          rationale: "Jev STOP without deterministic low-yield basis — ignored",
          source: "JEV_ADVISORY",
          acceptedByPolicy: false,
          writesVerifiedFacts: false,
          promotesOpportunities: false,
          advisoryOnly: true,
        });
      }
    } else if (choice) {
      result.jevAccepted += 1;
      result.decisions = deterministic.map((d, i) =>
        i === 0 ? { ...d, source: "JEV_ADVISORY+DETERMINISTIC", jevChoice: choice } : d
      );
    }
  } catch (err) {
    result.jevError = String(err?.message || err).slice(0, 160);
  }

  return result;
}
