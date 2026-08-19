/**
 * Operator Competitive Gap DEV + HOLDOUT gold labels.
 * Constructed cases. Do not leak into Presence classifier training.
 * Hard negatives included. Binary force-labeling is not required.
 */

import { GAP_GOLD_LABEL } from "./gaps.js";

export const OPERATOR_GAP_GOLD_VERSION = "operator_competitive_gap_gold_v1";

const IDS = Object.freeze({
  marriott: "recGmiPhRt6hiayd9",
  ihg: "rec7IXYQYpKMYsrDl",
  hilton: "rec3Uwxe6ovpiokuN",
  aimbridge: "recGWxIJqnYHkJZFD",
  hotelEquities: "recWPKu5laVZxsvpn",
  arbor: "recF5Z87OAqFgndoq",
  ghl: "reciI2tYQBfMoMK9G",
  brittain: "receHCdI6CEsJqdG4",
  remington: "rec6UB6RpMKSs2tAo",
});

function c(partial) {
  return Object.freeze({
    operatorPresent: false,
    observationCount: 2,
    comparableObservation: true,
    ...partial,
  });
}

export const OPERATOR_COMPETITIVE_GAP_GOLD_CASES = Object.freeze([
  c({
    caseId: "gap_dev_aimbridge_absent_he_ghl_cala",
    split: "DEV",
    goldLabel: GAP_GOLD_LABEL.TRUE_COMPETITIVE_GAP,
    operatorId: IDS.aimbridge,
    scenarioId: "op_scenario_cala_latam_regional_capability_v1",
    presentPeerOperatorIds: [IDS.hotelEquities, IDS.ghl],
    note: "Aimbridge absent; CALA/LATAM alternatives present and commercially relevant.",
  }),
  c({
    caseId: "gap_dev_he_absent_aimbridge_third_party",
    split: "DEV",
    goldLabel: GAP_GOLD_LABEL.TRUE_COMPETITIVE_GAP,
    operatorId: IDS.hotelEquities,
    scenarioId: "op_scenario_third_party_management_v1",
    presentPeerOperatorIds: [IDS.aimbridge, IDS.remington],
    note: "Third-party CORE substitutes present.",
  }),
  c({
    caseId: "gap_dev_marriott_absent_aimbridge_brand_agnostic",
    split: "DEV",
    goldLabel: GAP_GOLD_LABEL.OUT_OF_SCOPE,
    operatorId: IDS.marriott,
    scenarioId: "op_scenario_brand_agnostic_operation_v1",
    presentPeerOperatorIds: [IDS.aimbridge, IDS.hotelEquities],
    note: "Brand-managed operator is out of scope for brand-agnostic third-party selection.",
  }),
  c({
    caseId: "gap_dev_marriott_absent_local_tpm_full_service",
    split: "DEV",
    goldLabel: GAP_GOLD_LABEL.EXPECTED_POSITIONING_DIFFERENCE,
    operatorId: IDS.marriott,
    scenarioId: "op_scenario_full_service_uu_operator_selection_v1",
    presentPeerOperatorIds: [IDS.hotelEquities],
    note: "Local third-party present is SECONDARY_CONTEXT, not a direct brand-managed gap.",
  }),
  c({
    caseId: "gap_dev_subject_absent_no_alternative",
    split: "DEV",
    goldLabel: GAP_GOLD_LABEL.NOT_A_GAP,
    operatorId: IDS.aimbridge,
    scenarioId: "op_scenario_third_party_management_v1",
    presentPeerOperatorIds: [],
    note: "Absence without a relevant alternative is Questions Missing, not a gap.",
  }),
  c({
    caseId: "gap_dev_brittain_cala_regional",
    split: "DEV",
    goldLabel: GAP_GOLD_LABEL.OUT_OF_SCOPE,
    operatorId: IDS.brittain,
    scenarioId: "op_scenario_cala_latam_regional_capability_v1",
    presentPeerOperatorIds: [IDS.aimbridge, IDS.ghl],
    note: "US Southeast operator is out of scope for CALA regional capability.",
  }),
  c({
    caseId: "gap_dev_arbor_limitation",
    split: "DEV",
    goldLabel: GAP_GOLD_LABEL.INSUFFICIENT_CONTEXT,
    operatorId: IDS.arbor,
    scenarioId: "op_scenario_third_party_management_v1",
    presentPeerOperatorIds: [IDS.hotelEquities, IDS.aimbridge],
    note: "Arbor has no live positive gold mentions; do not make Arbor-specific executive gap claims.",
  }),
  c({
    caseId: "gap_dev_brand_as_operator_negative",
    split: "DEV",
    goldLabel: GAP_GOLD_LABEL.NOT_A_GAP,
    trapClass: "brand_as_operator",
    operatorId: IDS.marriott,
    scenarioId: "op_scenario_full_service_uu_operator_selection_v1",
    operatorPresent: false,
    presentPeerOperatorIds: [],
    note: "Marriott Bonvoy / hotel-brand wording is not operator Presence and not a gap.",
  }),
  c({
    caseId: "gap_dev_source_citation_negative",
    split: "DEV",
    goldLabel: GAP_GOLD_LABEL.NOT_A_GAP,
    trapClass: "source_only",
    operatorId: IDS.remington,
    scenarioId: "op_scenario_third_party_management_v1",
    presentPeerOperatorIds: [],
    note: "Source-domain citation without operating mention is not Presence.",
  }),
  c({
    caseId: "gap_dev_generic_management_company",
    split: "DEV",
    goldLabel: GAP_GOLD_LABEL.NOT_A_GAP,
    trapClass: "generic_management_company",
    operatorId: IDS.hotelEquities,
    scenarioId: "op_scenario_third_party_management_v1",
    presentPeerOperatorIds: [],
    note: "Generic 'management company' language without named operators is not a gap.",
  }),
  c({
    caseId: "gap_dev_hilton_marriott_core_full_service",
    split: "DEV",
    goldLabel: GAP_GOLD_LABEL.TRUE_COMPETITIVE_GAP,
    operatorId: IDS.hilton,
    scenarioId: "op_scenario_full_service_uu_operator_selection_v1",
    presentPeerOperatorIds: [IDS.marriott, IDS.ihg],
    note: "Brand-managed CORE substitutes on full-service UU.",
  }),
  c({
    caseId: "gap_holdout_remington_absent_he_third_party",
    split: "HOLDOUT",
    goldLabel: GAP_GOLD_LABEL.TRUE_COMPETITIVE_GAP,
    operatorId: IDS.remington,
    scenarioId: "op_scenario_third_party_management_v1",
    presentPeerOperatorIds: [IDS.hotelEquities, IDS.aimbridge],
  }),
  c({
    caseId: "gap_holdout_aimbridge_absent_only_marriott",
    split: "HOLDOUT",
    goldLabel: GAP_GOLD_LABEL.NOT_A_GAP,
    operatorId: IDS.aimbridge,
    scenarioId: "op_scenario_third_party_management_v1",
    presentPeerOperatorIds: [IDS.marriott],
    note: "Brand-managed name on a third-party scenario is not a CORE alternative.",
  }),
  c({
    caseId: "gap_holdout_brittain_vs_he_third_party_geo",
    split: "HOLDOUT",
    goldLabel: GAP_GOLD_LABEL.NOT_A_GAP,
    trapClass: "regional_mismatch",
    operatorId: IDS.brittain,
    scenarioId: "op_scenario_third_party_management_v1",
    presentPeerOperatorIds: [IDS.hotelEquities],
    note: "US Southeast vs CALA third-party managers are not geographically overlapping.",
  }),
  c({
    caseId: "gap_holdout_remington_bare_trap",
    split: "HOLDOUT",
    goldLabel: GAP_GOLD_LABEL.NOT_A_GAP,
    trapClass: "remington_bare",
    operatorId: IDS.remington,
    scenarioId: "op_scenario_third_party_management_v1",
    presentPeerOperatorIds: [],
    note: "Bare Remington remains blocked; not operator Presence.",
  }),
  c({
    caseId: "gap_holdout_institutional_not_auto_gap",
    split: "HOLDOUT",
    goldLabel: GAP_GOLD_LABEL.REQUIRES_REVIEW,
    operatorId: IDS.marriott,
    scenarioId: "op_scenario_institutional_platform_alignment_v1",
    presentPeerOperatorIds: [IDS.hilton, IDS.ihg],
    note: "Institutional scenario is DETAIL_ONLY. Do not auto-promote a competitive gap.",
  }),
  c({
    caseId: "gap_holdout_ghl_absent_aimbridge_cala",
    split: "HOLDOUT",
    goldLabel: GAP_GOLD_LABEL.EXPECTED_POSITIONING_DIFFERENCE,
    operatorId: IDS.ghl,
    scenarioId: "op_scenario_cala_latam_regional_capability_v1",
    presentPeerOperatorIds: [IDS.aimbridge],
    note: "GHL mixed regional platform vs Aimbridge TPM is SECONDARY, not CORE.",
  }),
  c({
    caseId: "gap_holdout_historical_not_current",
    split: "HOLDOUT",
    goldLabel: GAP_GOLD_LABEL.NOT_A_GAP,
    trapClass: "historical_mention",
    operatorId: IDS.aimbridge,
    scenarioId: "op_scenario_third_party_management_v1",
    presentPeerOperatorIds: [],
    note: "Historical mention without a current alternative is not a gap. No temporal classifier in V1.",
  }),
  c({
    caseId: "gap_holdout_luxury_conditional",
    split: "HOLDOUT",
    goldLabel: GAP_GOLD_LABEL.REQUIRES_REVIEW,
    operatorId: IDS.aimbridge,
    scenarioId: "op_scenario_luxury_operator_selection_v1",
    presentPeerOperatorIds: [IDS.hotelEquities],
    note: "Luxury eligibility is conditional for these third-party operators.",
  }),
  c({
    caseId: "gap_holdout_failed_provider",
    split: "HOLDOUT",
    goldLabel: GAP_GOLD_LABEL.INSUFFICIENT_CONTEXT,
    operatorId: IDS.aimbridge,
    scenarioId: "op_scenario_third_party_management_v1",
    comparableObservation: false,
    presentPeerOperatorIds: [IDS.hotelEquities],
    note: "Failed or unavailable provider is not operator absence.",
  }),
]);

export function listGapGoldCases(split = null) {
  if (!split) return [...OPERATOR_COMPETITIVE_GAP_GOLD_CASES];
  return OPERATOR_COMPETITIVE_GAP_GOLD_CASES.filter((c) => c.split === split);
}

function labelsMatch(predicted, gold) {
  if (predicted === gold) return true;
  if (gold === GAP_GOLD_LABEL.OUT_OF_SCOPE && predicted === GAP_GOLD_LABEL.EXPECTED_POSITIONING_DIFFERENCE) {
    return true;
  }
  if (gold === GAP_GOLD_LABEL.NOT_A_GAP && predicted === GAP_GOLD_LABEL.INSUFFICIENT_CONTEXT) {
    return true;
  }
  return false;
}

export function scoreOperatorGapGold(interpretFn, cases = OPERATOR_COMPETITIVE_GAP_GOLD_CASES) {
  let tp = 0;
  let fp = 0;
  let fn = 0;
  let tn = 0;
  let criticalIdentityErrors = 0;
  let brandAsOperatorErrors = 0;
  let regionalScopeErrors = 0;
  let secondaryAsCoreErrors = 0;
  let conditionalAsCoreErrors = 0;
  let nonComparableAsCoreErrors = 0;
  const mismatches = [];

  for (const gold of cases) {
    const predicted = interpretFn({
      operatorId: gold.operatorId,
      scenarioId: gold.scenarioId,
      operatorPresent: gold.operatorPresent,
      presentPeerOperatorIds: gold.presentPeerOperatorIds,
      observationCount: gold.observationCount,
      comparableObservation: gold.comparableObservation,
    });
    const predLabel = predicted.goldLabel || predicted.interpretation;
    const isTrueGold = gold.goldLabel === GAP_GOLD_LABEL.TRUE_COMPETITIVE_GAP;
    const isTruePred = predLabel === GAP_GOLD_LABEL.TRUE_COMPETITIVE_GAP;
    if (isTrueGold && isTruePred) tp += 1;
    else if (!isTrueGold && isTruePred) {
      fp += 1;
      mismatches.push({ caseId: gold.caseId, gold: gold.goldLabel, predicted: predLabel });
    } else if (isTrueGold && !isTruePred) {
      fn += 1;
      mismatches.push({ caseId: gold.caseId, gold: gold.goldLabel, predicted: predLabel });
    } else {
      tn += 1;
      if (!labelsMatch(predLabel, gold.goldLabel) && gold.goldLabel !== GAP_GOLD_LABEL.TRUE_COMPETITIVE_GAP) {
        mismatches.push({ caseId: gold.caseId, gold: gold.goldLabel, predicted: predLabel, class: "non_positive_mismatch" });
      }
    }
    if (isTruePred && gold.trapClass) {
      criticalIdentityErrors += 1;
      if (gold.trapClass === "brand_as_operator") brandAsOperatorErrors += 1;
      if (gold.trapClass === "regional_mismatch") regionalScopeErrors += 1;
    }
    if (isTruePred && gold.goldLabel === GAP_GOLD_LABEL.EXPECTED_POSITIONING_DIFFERENCE) {
      secondaryAsCoreErrors += 1;
    }
    if (isTruePred && gold.goldLabel === GAP_GOLD_LABEL.REQUIRES_REVIEW) {
      conditionalAsCoreErrors += 1;
    }
    if (
      isTruePred &&
      gold.goldLabel === GAP_GOLD_LABEL.NOT_A_GAP &&
      (gold.trapClass === "regional_mismatch" ||
        gold.note?.includes("not a CORE") ||
        gold.presentPeerOperatorIds?.length)
    ) {
      nonComparableAsCoreErrors += 1;
    }
  }

  const precision = tp + fp ? tp / (tp + fp) : 1;
  const recall = tp + fn ? tp / (tp + fn) : 1;
  const f1 = precision + recall ? (2 * precision * recall) / (precision + recall) : 0;
  return {
    version: OPERATOR_GAP_GOLD_VERSION,
    cases: cases.length,
    tp,
    fp,
    tn,
    fn,
    precision,
    recall,
    f1,
    criticalIdentityErrors,
    brandAsOperatorErrors,
    regionalScopeErrors,
    secondaryAsCoreErrors,
    conditionalAsCoreErrors,
    nonComparableAsCoreErrors,
    mismatches,
  };
}
