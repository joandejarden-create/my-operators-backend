/**
 * Exact reconstruction of the live ADP AI Presence Index.
 * RESEARCH / AUDIT ONLY — do not import from owner UI.
 * Formula copied from customer/owner-payload.js computeIntentPresenceIndex.
 * Do not change owner-payload.js from this module.
 */

import { roundAdpPercent } from "../format-percent.js";
import { computeDemandCaptureIndex } from "../intelligence/demand-capture-index.js";
import { PROVIDERS } from "../data-model.js";
import { territoryLabelForIntent } from "./intent-territory-labels.js";

export const ADP_INDEX_MIN_AVG_COMP_RATE = 30;
export const ADP_INDEX_MAX = 200;
export const ADP_INDEX_MIN_PARTICIPATING_COMPS = 3;

export const PRESENCE_INDEX_SOURCE_FILE = "lib/ai-demand-positioning/customer/owner-payload.js";
export const PRESENCE_INDEX_FUNCTION = "computeIntentPresenceIndex";

export const PRESENCE_INDEX_CURRENT_FORMULA =
  "index = min(round((subjectScenarioPresenceRate / participatingDeclaredCompAveragePresenceRate) × 100), 200) " +
  "when participatingDeclaredComps >= 3 AND avgCompRate >= 30; else null";

/**
 * Same declared-comp matcher used by the live Presence Index (not matchesDeclaredComp).
 */
export function isDeclaredCompPresenceIndex(name, declaredLower) {
  const nLow = String(name || "").toLowerCase();
  for (const d of declaredLower) {
    if (d === nLow || d.includes(nLow) || nLow.includes(d)) return true;
    const nWords = nLow.replace(/[^a-z0-9\s]/g, "").split(/\s+/).filter((w) => w.length > 2);
    const dWords = d.replace(/[^a-z0-9\s]/g, "").split(/\s+/).filter((w) => w.length > 2);
    const overlap = nWords.filter((w) => dWords.includes(w));
    if (overlap.length >= 2 && overlap.length >= nWords.length * 0.6) return true;
  }
  return false;
}

function creditDeclaredEntry(comp, declaredLower, compScenarioCounts, scenarioId) {
  const cLow = String(comp || "").toLowerCase();
  for (const d of declaredLower) {
    if (d === cLow || d.includes(cLow) || cLow.includes(d)) {
      compScenarioCounts[d].add(scenarioId);
      return d;
    }
  }
  return null;
}

/**
 * Reconstruct live Presence Index for one observation set (typically parsed-only).
 */
export function reconstructIntentPresenceIndex(observations, scenarios, propertyProfile, demandCapture) {
  const declared = (propertyProfile.declaredCompSet || []).map((d) => d.toLowerCase());
  if (!declared.length) return {};

  const dc = demandCapture || computeDemandCaptureIndex(observations, scenarios);
  const scenariosByIntent = {};
  for (const s of scenarios) {
    if (!scenariosByIntent[s.intent]) scenariosByIntent[s.intent] = [];
    scenariosByIntent[s.intent].push(s.scenarioId);
  }

  const result = {};
  for (const [intent, scenarioIds] of Object.entries(scenariosByIntent)) {
    const intentObs = (observations || []).filter((o) => scenarioIds.includes(o.scenarioId));
    if (!intentObs.length) continue;

    const totalScenarios = scenarioIds.length;
    const compScenarioCounts = {};
    for (const d of declared) compScenarioCounts[d] = new Set();

    for (const obs of intentObs) {
      for (const comp of obs.competitorsMentioned || []) {
        if (isDeclaredCompPresenceIndex(comp, declared)) {
          creditDeclaredEntry(comp, declared, compScenarioCounts, obs.scenarioId);
        }
      }
    }

    const peerRows = declared.map((d) => ({
      declaredName: d,
      scenarioCount: compScenarioCounts[d].size,
      presenceRate: totalScenarios > 0 ? (compScenarioCounts[d].size / totalScenarios) * 100 : 0,
    }));
    const compRates = peerRows.map((r) => r.presenceRate);
    const participating = peerRows.filter((r) => r.presenceRate > 0);
    const participatingComps = participating.length;
    const avgCompRate =
      participatingComps >= ADP_INDEX_MIN_PARTICIPATING_COMPS
        ? participating.reduce((a, b) => a + b.presenceRate, 0) / participatingComps
        : 0;

    const myRate = dc.byIntent[intent]?.rate || 0;
    let index = null;
    let suppressionState = "SHOWN";
    if (participatingComps < ADP_INDEX_MIN_PARTICIPATING_COMPS) {
      suppressionState = "SUPPRESSED_THIN_PEER_SET";
    } else if (avgCompRate < ADP_INDEX_MIN_AVG_COMP_RATE) {
      suppressionState = "SUPPRESSED_THIN_AVG_COMP_RATE";
    } else {
      const raw = Math.round((myRate / avgCompRate) * 100);
      index = Math.min(raw, ADP_INDEX_MAX);
      if (raw > ADP_INDEX_MAX) suppressionState = "SHOWN_CAPPED_AT_200";
    }

    const providersPresent = [...new Set(intentObs.map((o) => o.provider).filter(Boolean))];

    result[intent] = {
      index,
      myRate,
      avgCompRate: roundAdpPercent(avgCompRate),
      intent,
      territoryLabel: territoryLabelForIntent(intent),
      subjectPresenceRate: myRate,
      currentPeerAvgPresenceRate: roundAdpPercent(avgCompRate),
      currentAiPresenceIndex: index,
      currentPeerSet: participating.map((r) => r.declaredName),
      currentPeerCount: participatingComps,
      declaredPeerRates: peerRows,
      currentDenominator: "mean scenario-level presence rate of declared comps with rate > 0",
      currentProviderScope: "ALL_OBSERVATIONS_UNSCOPED",
      currentAllProvidersLogic:
        "Scenario presence uses OR across whatever providers exist in the observation set; no independent provider indexes; missing provider is not zero-filled",
      currentGrain: "SCENARIO_GRAIN (property × scenario; any-provider mention)",
      suppressionState,
      providersPresent,
      providerCountObserved: providersPresent.length,
    };
  }

  return result;
}

export function presenceIndexCustomerMeaning() {
  return {
    customerQuestion:
      "How does this hotel's scenario-level AI appearance frequency compare with the average scenario-level appearance frequency of declared competitors that appeared at least once in this demand territory?",
    interpretation100:
      "Subject scenario presence rate equals the average presence rate of participating declared competitors (zeros excluded). This is relative frequency vs a peer average — not fair share of a consideration set.",
    interpretation120:
      "Subject scenario presence rate is 20% higher than that participating declared-comp average (before the 200 cap).",
    interpretation80:
      "Subject scenario presence rate is 20% below that participating declared-comp average.",
    mathematicallyValidForStatedQuestion: true,
    parityAsFairShareValid: false,
    caveats: [
      "Declared comp set only — observed alternatives never enter the denominator",
      "Declared comps with 0% presence are dropped from the average (raises peer average, lowers index)",
      "Index is capped at 200",
      "No provider-scoped indexes; All Providers is not a derived certified scope",
      "Thin sets (<3 participating comps or avg < 30%) are suppressed to null",
    ],
  };
}

export function auditPresenceIndexQuality(intentRow, propertyProfile) {
  const blockers = [];
  if (!intentRow) {
    return { productionSafe: "NO", blockers: ["missing_intent_row"] };
  }
  if (intentRow.suppressionState !== "SHOWN" && intentRow.suppressionState !== "SHOWN_CAPPED_AT_200") {
    blockers.push(intentRow.suppressionState);
  }
  if (intentRow.currentPeerCount < 3) blockers.push("THIN_PEER_SETS");
  if ((propertyProfile.declaredCompSet || []).length && intentRow.currentPeerCount < (propertyProfile.declaredCompSet || []).length) {
    blockers.push("DECLARED_COMP_ONLY_BIAS");
    blockers.push("ZERO_RATE_PEERS_EXCLUDED");
  }
  blockers.push("NO_TERRITORY_SPECIFIC_CORE_UNIVERSE");
  blockers.push("NO_ENTITY_QUALITY_FILTER_ON_DECLARED_MATCHING");
  blockers.push("NO_PROVIDER_SCOPED_INDEX");
  if (intentRow.suppressionState === "SHOWN_CAPPED_AT_200") blockers.push("EXTREME_SCORE_CAPPED");
  if ((intentRow.providersPresent || []).length < PROVIDERS.length) {
    blockers.push("INCOMPLETE_PROVIDER_COVERAGE");
  }

  let productionSafe = "PARTIAL";
  if (intentRow.index == null) productionSafe = "NO";
  else if (blockers.includes("EXTREME_SCORE_CAPPED") || intentRow.currentPeerCount < 4) {
    productionSafe = "PARTIAL";
  }

  return { productionSafe, blockers: [...new Set(blockers)] };
}
