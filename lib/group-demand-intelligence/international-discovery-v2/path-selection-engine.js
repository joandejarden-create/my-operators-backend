/**
 * Deterministic next-best discovery path selection.
 * Strategy only — does not change Ready/Watch truth standards.
 */

import { DISCOVERY_SPINE } from "./constants.js";
import { resolveMarketDiscoveryProfile } from "./market-discovery-profile.js";

function hasFlag(state, key) {
  return Boolean(state?.[key]);
}

/**
 * @param {object} input
 * @returns {{ nextBestDiscoveryPath, nextBestEvidenceTarget, stopContinue, reason, rankedPaths }}
 */
export function selectNextDiscoveryPath(input = {}) {
  const {
    hotelId,
    market,
    country,
    availableEvidence = {},
    priorResearchState = {},
    hotelSuppliedEvidenceCount = 0,
    marketProfile = null,
  } = input;

  const profile =
    marketProfile || resolveMarketDiscoveryProfile({ hotelId, market, country });
  const weights = { ...(profile.pathWeights || {}) };

  const ev = {
    participantListPublished: hasFlag(availableEvidence, "participantListPublished"),
    participantListAbsent: hasFlag(availableEvidence, "participantListAbsent"),
    controllerResolved: hasFlag(availableEvidence, "controllerResolved"),
    lodgingPagePublished: hasFlag(availableEvidence, "lodgingPagePublished"),
    accountsKnown: hasFlag(availableEvidence, "accountsKnown"),
    travelingEntityKnown: hasFlag(availableEvidence, "travelingEntityKnown"),
    buyerPathKnown: hasFlag(availableEvidence, "buyerPathKnown"),
    futureDecisionKnown: hasFlag(availableEvidence, "futureDecisionKnown"),
    recurringCongress: hasFlag(availableEvidence, "recurringCongress"),
    priorCycleKnown: hasFlag(availableEvidence, "priorCycleKnown"),
    hotelHistoryAvailable: hotelSuppliedEvidenceCount > 0 || hasFlag(availableEvidence, "hotelHistoryAvailable"),
    publicDataCeiling: hasFlag(availableEvidence, "publicDataCeiling"),
  };

  // Hard routing rules (deterministic overrides)
  if (ev.hotelHistoryAvailable && !priorResearchState.hotelHistorySpineDone) {
    return finish(DISCOVERY_SPINE.HOTEL_HISTORY_FIRST, "hotel_supplied_or_history_available", {
      evidenceTarget: "HOTEL_SUPPLIED_LOOKALIKE_GRAPH",
      stopContinue: "CONTINUE",
      weights,
      profile,
      overrides: ["hotel_history"],
    });
  }

  if (ev.recurringCongress && ev.priorCycleKnown && !ev.controllerResolved) {
    return finish(DISCOVERY_SPINE.HISTORICAL_PROCESS_FIRST, "recurring_congress_prior_cycle", {
      evidenceTarget: "PRIOR_CYCLE_PCO_HOUSING_MODEL",
      stopContinue: "CONTINUE",
      weights,
      profile,
      overrides: ["historical_process"],
    });
  }

  if ((ev.participantListAbsent || ev.publicDataCeiling) && !ev.controllerResolved) {
    return finish(DISCOVERY_SPINE.DEMAND_CONTROLLER_FIRST, "list_unpublished_or_ceiling_no_controller", {
      evidenceTarget: "PCO_DMC_SECRETARIAT_HOUSING_PAGE",
      stopContinue: "CONTINUE",
      weights,
      profile,
      overrides: ["controller_first"],
    });
  }

  if (ev.controllerResolved && !ev.accountsKnown && !ev.travelingEntityKnown) {
    // Prefer account-first now; keep participant-first as later option
    weights[DISCOVERY_SPINE.ACCOUNT_FIRST] =
      (weights[DISCOVERY_SPINE.ACCOUNT_FIRST] || 0.5) + 0.25;
    weights[DISCOVERY_SPINE.PARTICIPANT_FIRST] =
      (weights[DISCOVERY_SPINE.PARTICIPANT_FIRST] || 0.5) + 0.1;
    return finish(DISCOVERY_SPINE.ACCOUNT_FIRST, "controller_resolved_accounts_unknown", {
      evidenceTarget: "NAMED_TRAVELING_ACCOUNTS",
      stopContinue: "CONTINUE",
      weights,
      profile,
      overrides: ["account_after_controller"],
      note: "PARTICIPANT_FIRST remains available when lists publish",
    });
  }

  if (ev.participantListPublished && !ev.accountsKnown) {
    return finish(DISCOVERY_SPINE.PARTICIPANT_FIRST, "public_list_available", {
      evidenceTarget: "OFFICIAL_PARTICIPANT_LIST",
      stopContinue: "CONTINUE",
      weights,
      profile,
      overrides: ["participant_list"],
    });
  }

  // Weighted fallback by market profile
  const ranked = Object.entries(weights)
    .map(([path, w]) => ({ path, weight: Number(w) || 0 }))
    .sort((a, b) => b.weight - a.weight);

  const top = ranked[0]?.path || DISCOVERY_SPINE.DEMAND_CONTROLLER_FIRST;
  return finish(top, "market_profile_weighted_default", {
    evidenceTarget: defaultEvidenceTarget(top),
    stopContinue: ev.publicDataCeiling && ev.controllerResolved && !ev.lodgingPagePublished
      ? "CONTINUE_BOUNDED"
      : "CONTINUE",
    weights,
    profile,
    overrides: [],
    rankedPaths: ranked,
  });
}

function defaultEvidenceTarget(spine) {
  return (
    {
      [DISCOVERY_SPINE.PARTICIPANT_FIRST]: "OFFICIAL_PARTICIPANT_OR_EXHIBITOR_LIST",
      [DISCOVERY_SPINE.DEMAND_CONTROLLER_FIRST]: "CONTROLLER_HOUSING_AUTHORITY_PAGE",
      [DISCOVERY_SPINE.ACCOUNT_FIRST]: "CORPORATE_OR_INSTITUTIONAL_TRIGGER",
      [DISCOVERY_SPINE.HISTORICAL_PROCESS_FIRST]: "PRIOR_CYCLE_PROCESS_PATTERN",
      [DISCOVERY_SPINE.HOTEL_HISTORY_FIRST]: "HOTEL_SUPPLIED_ACCOUNT_GRAPH",
    }[spine] || "NEXT_MISSING_PILLAR"
  );
}

function finish(path, reason, extra) {
  return {
    nextBestDiscoveryPath: path,
    nextBestEvidenceTarget: extra.evidenceTarget,
    stopContinue: extra.stopContinue || "CONTINUE",
    reason,
    note: extra.note || null,
    marketProfileVersion: extra.profile?.marketProfileVersion || null,
    rankedPaths:
      extra.rankedPaths ||
      Object.entries(extra.weights || {})
        .map(([p, w]) => ({ path: p, weight: w }))
        .sort((a, b) => b.weight - a.weight),
    overridesApplied: extra.overrides || [],
  };
}
