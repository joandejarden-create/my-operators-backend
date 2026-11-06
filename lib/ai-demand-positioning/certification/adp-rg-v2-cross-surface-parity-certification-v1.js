/**
 * ADP_RG_V2_CROSS_SURFACE_PARITY_CERTIFICATION_V1
 *
 * Permanent gate for Reality Gap weighted V2 cross-surface parity.
 * Wire into future universal ADP Full E2E — do not launch Full E2E from this module.
 *
 * Independent recalc · V1 preservation · lineage · staged report/share/PDF/browser parity ·
 * trend-comparability protection · published immutability · synthetic exclusion.
 */

import { listCanonicalAdp31PropertyIds } from "../governance/adp-operational-registry-hygiene-v1.js";
import { isAdpSyntheticOrTestPropertyId } from "../governance/adp-operational-registry-hygiene-v1.js";
import {
  loadPropertyProfile,
  loadLatestCoreCustomerPeriod,
  isLoyaltyMeasurementPeriod,
} from "../data-model.js";
import { getCensusRecordIdForAdpProperty } from "../census-link-registry.js";
import { computeRealityGap } from "../intelligence/reality-gap.js";
import {
  CONDITIONAL_APPROVED_METHODOLOGY_ID,
  APPROVED_CUSTOMER_METHODOLOGY_ID,
  computeRealityGapMethodologyComparisonV2,
  assertApprovedFormulaUnchanged,
} from "../intelligence/reality-gap-methodology-v2.js";
import { loadStagedOwnerPayload } from "../review/adp-staged-report-store-v1.js";
import { loadPublishedManifest } from "../published-snapshot.js";
import { enrichObservationsWithRank } from "../metrics/executive-metrics-foundation.js";

export const ADP_RG_V2_CROSS_SURFACE_PARITY_CERTIFICATION_V1 =
  "ADP_RG_V2_CROSS_SURFACE_PARITY_CERTIFICATION_V1";

export const RG_V2_GATE_NEGATIVE_TESTS = Object.freeze([
  "V1_SCORE_SHOWN_AS_CURRENT_V2",
  "INVALID_TESTED_ELIGIBLE_DENOMINATOR",
  "WRONG_ATTRIBUTE_SET_VERSION",
  "INCORRECT_PARTIAL_WEIGHTING",
  "PROMPT_LEAKAGE_AS_RECOGNITION",
  "UNTESTED_ATTRIBUTE_AS_MISS",
  "STALE_EXECUTIVE_SUMMARY",
  "STALE_STAGED_SHARE",
  "STALE_STAGED_PDF",
  "PDF_ATTACHED_TO_WRONG_PROPERTY",
  "WRONG_REPORT_PERIOD",
  "SYNTHETIC_OPERATIONAL_HOTEL",
  "METHODOLOGY_SHOWN_AS_PERFORMANCE_TREND",
  "FAILED_EVIDENCE_DRAWER",
  "FALSE_PASS_HTML_STRUCTURE_ONLY",
]);

/**
 * Independently recalculate V1 + V2 from Core observations + live profile.
 * @param {string} propertyId
 */
export function independentlyRecalculateRealityGapV1V2(propertyId) {
  const profile = loadPropertyProfile(propertyId);
  const core = loadLatestCoreCustomerPeriod(propertyId);
  if (!profile || !core?.observations?.length) {
    return {
      ok: false,
      propertyId,
      error: !profile ? "profile_missing" : "core_observations_missing",
    };
  }
  if (isLoyaltyMeasurementPeriod(core)) {
    return { ok: false, propertyId, error: "loyalty_period_refused_for_core_rg" };
  }
  // Match buildOwnerPayload observation grain: parsed-only + enrichObservationsWithRank.
  const parsed = (core.observations || []).filter((o) => o.parsed);
  if (!parsed.length) {
    return { ok: false, propertyId, error: "no_parsed_observations" };
  }
  const observations = enrichObservationsWithRank(parsed, profile);
  const cmp = computeRealityGapMethodologyComparisonV2(observations, profile);
  assertApprovedFormulaUnchanged(cmp);
  const v1 = computeRealityGap(observations, profile, {
    methodologyId: APPROVED_CUSTOMER_METHODOLOGY_ID,
  });
  const v2 = computeRealityGap(observations, profile, {
    methodologyId: CONDITIONAL_APPROVED_METHODOLOGY_ID,
  });
  const denom = v2.testedEligibleCount || 0;
  const sumStates =
    (v2.recognizedCountScored || 0) +
    (v2.partialCount || 0) +
    (v2.missedCount || 0);
  const denomOk = denom === 0 ? true : sumStates === denom;
  const expectedV1 =
    denom > 0
      ? Math.round(
          (((v2.missedCount || 0) + (v2.partialCount || 0)) / denom) * 1000
        ) / 10
      : v1.gapScore;
  const expectedV2 =
    denom > 0
      ? Math.round(
          (((v2.missedCount || 0) + 0.5 * (v2.partialCount || 0)) / denom) *
            1000
        ) / 10
      : v2.gapScore;
  return {
    ok: true,
    propertyId,
    hotelName: profile.name || propertyId,
    hpcId: getCensusRecordIdForAdpProperty(propertyId) || null,
    corePeriodId: core.periodId,
    observationCount: observations.length,
    rawObservationCount: core.observations.length,
    attributeSetSize: Array.isArray(profile.attributes)
      ? profile.attributes.length
      : Object.keys(profile.attributes || {}).length,
    testedEligible: denom,
    recognized: v2.recognizedCountScored,
    partial: v2.partialCount,
    missed: v2.missedCount,
    untested: cmp.counts?.untested ?? null,
    denomOk,
    gapScoreV1: v1.gapScore,
    gapScoreV2: v2.gapScore,
    gapScoreBinaryV1OnV2Payload: v2.gapScoreBinaryV1,
    expectedV1FromCounts: expectedV1,
    expectedV2FromCounts: expectedV2,
    formulaV1Ok: v1.gapScore === expectedV1 || v1.gapScore == null,
    formulaV2Ok: v2.gapScore === expectedV2 || v2.gapScore == null,
    methodologyIdV2: CONDITIONAL_APPROVED_METHODOLOGY_ID,
    methodologyIdV1: APPROVED_CUSTOMER_METHODOLOGY_ID,
    optionA: cmp.optionA_binaryGap,
    optionB: cmp.optionB_weightedGap,
  };
}

/**
 * Compare staged Reality Gap pack to independent V2 recalc.
 * @param {string} propertyId
 * @param {ReturnType<typeof independentlyRecalculateRealityGapV1V2>} [live]
 */
export function certifyStagedRealityGapV2Parity(propertyId, live = null) {
  const recalc = live || independentlyRecalculateRealityGapV1V2(propertyId);
  const staged = loadStagedOwnerPayload(propertyId);
  const published = loadPublishedManifest(propertyId);
  const rg = staged?.payload?.realityGap || {};
  const issues = [];
  if (!recalc.ok) issues.push(`recalc_${recalc.error}`);
  if (!staged?.payload) issues.push("staged_missing");
  if (rg.methodologyId !== CONDITIONAL_APPROVED_METHODOLOGY_ID) {
    issues.push("staged_methodology_not_v2");
  }
  if (recalc.ok && rg.gapScore !== recalc.gapScoreV2) {
    issues.push(
      `staged_gap_mismatch_staged_${rg.gapScore}_live_${recalc.gapScoreV2}`
    );
  }
  if (
    recalc.ok &&
    rg.gapScoreBinaryV1 != null &&
    rg.gapScoreBinaryV1 !== recalc.gapScoreV1
  ) {
    issues.push(
      `v1_preserve_mismatch_staged_${rg.gapScoreBinaryV1}_live_${recalc.gapScoreV1}`
    );
  }
  if (recalc.ok && !recalc.denomOk) issues.push("invalid_tested_eligible_denominator");
  if (recalc.ok && !recalc.formulaV2Ok) issues.push("incorrect_partial_weighting");
  if (isAdpSyntheticOrTestPropertyId(propertyId)) {
    issues.push("synthetic_operational_hotel");
  }

  const stagedPeriod =
    staged?.payload?.period?.periodId || staged?.periodId || null;
  if (
    recalc.ok &&
    stagedPeriod &&
    recalc.corePeriodId &&
    stagedPeriod !== recalc.corePeriodId
  ) {
    issues.push("wrong_report_period");
  }

  return {
    gate: ADP_RG_V2_CROSS_SURFACE_PARITY_CERTIFICATION_V1,
    propertyId,
    ok: issues.length === 0 && recalc.ok === true,
    issues,
    recalc,
    staged: {
      versionId: staged?.versionId || null,
      periodId: stagedPeriod,
      methodologyId: rg.methodologyId || null,
      gapScore: rg.gapScore ?? null,
      gapScoreBinaryV1: rg.gapScoreBinaryV1 ?? null,
      display: rg.display || null,
      testedEligible: rg.testedEligibleCount ?? null,
      recognized: rg.recognizedCountScored ?? rg.recognizedCount ?? null,
      partial: rg.partialCount ?? null,
      missed: rg.missedCount ?? null,
    },
    published: {
      latestPeriodId: published?.latestPeriodId || null,
      latestVersionId: published?.latestVersionId || null,
      untouchedByThisGate: true,
    },
  };
}

/**
 * Certify all 31 hotels for staged V2 parity (JSON surfaces only — not browser/PDF).
 */
export function certifyCanonical31RgV2StagedParity() {
  const rows = [];
  for (const propertyId of listCanonicalAdp31PropertyIds()) {
    rows.push(certifyStagedRealityGapV2Parity(propertyId));
  }
  const pass = rows.filter((r) => r.ok).length;
  const fail = rows.filter((r) => !r.ok).length;
  const syntheticInUniverse = listCanonicalAdp31PropertyIds().some((id) =>
    isAdpSyntheticOrTestPropertyId(id)
  );
  return {
    gate: ADP_RG_V2_CROSS_SURFACE_PARITY_CERTIFICATION_V1,
    universeSize: rows.length,
    pass,
    fail,
    syntheticInUniverse,
    ok: fail === 0 && rows.length === 31 && !syntheticInUniverse,
    rows,
  };
}

/**
 * Detect Reality Gap % in narrative that equals preserved V1 while current is V2
 * (false “current” display). Scenario Presence coincidences must be excluded by caller context.
 */
export function narrativeMentionsStaleV1AsCurrentRealityGap(payload, liveV2, liveV1) {
  if (liveV2 == null || liveV1 == null || liveV2 === liveV1) return false;
  const rg = payload?.realityGap || {};
  if (rg.methodologyId !== CONDITIONAL_APPROVED_METHODOLOGY_ID) return true;
  if (rg.gapScore === liveV1 && liveV1 !== liveV2) return true;
  if (String(rg.display || "").includes(String(liveV1)) && rg.gapScore !== liveV2) {
    return true;
  }
  return false;
}
