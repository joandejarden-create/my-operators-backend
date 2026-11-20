/**
 * ADP 2.0 Phase 1B — Verified Context Bridge + Reality Gap dual-run (diagnostic only).
 * Does not alter customer-facing Reality Gap or historical periods.
 */

import { createHash } from "crypto";
import { ATTRIBUTE_DEFINITIONS, computeRealityGap } from "../intelligence/reality-gap.js";
import { loadPropertyProfile, loadLatestCustomerPeriod } from "../data-model.js";
import { getCensusRecordIdForAdpProperty } from "../census-link-registry.js";
import { loadPublishedReport } from "../published-snapshot.js";
import { classifySelectedAttributeEligibility } from "./adp-attribute-eligibility-classifier-v1.js";
import { assessMeasurementCoveragePolicy } from "./adp-measurement-coverage-policy-v1.js";
import { collapseToUniqueMeasurableConcepts } from "./adp-semantic-equivalence-v1.js";
import { suggestAttributeCandidates } from "./adp-attribute-suggestion-candidates-v1.js";

export const ADP_VERIFIED_CONTEXT_BRIDGE_V1 = "ADP_VERIFIED_CONTEXT_BRIDGE_V1";
export const DIAGNOSTIC_MODE = "RG_BRIDGED_ELIGIBLE_DIAGNOSTIC_V1";
export const FIXTURE_BASELINE_MODE = "RG_FIXTURE_BASELINE";

export const MEASUREMENT_COMPARISON_STATUS = Object.freeze({
  OBSERVATION_BACKED: "OBSERVATION_BACKED",
  DATA_INSUFFICIENT: "DATA_INSUFFICIENT",
  ATTRIBUTE_COVERAGE_ONLY: "ATTRIBUTE_COVERAGE_ONLY",
});

export const PILOT_HOTELS = Object.freeze([
  {
    propertyId: "adp_bethesda_marriott",
    hpcHotelId: "recLuxvwwxID7U2B8",
    role: "PILOT_1_CORPORATE_MEDICAL_MEETINGS",
  },
  {
    propertyId: "adp_spice_island_beach_resort",
    hpcHotelId: "recKRJjcPnb4tVDDS",
    role: "PILOT_2_LUXURY_LEISURE_WEDDINGS",
  },
]);

export const REGRESSION_HOTELS = Object.freeze([
  {
    propertyId: "adp_yotel_geneva_lake",
    hpcHotelId: "recrPQcZg7SFARRb2",
    role: "DEFINITION_REGRESSION",
  },
]);

function summarizeRg(rg) {
  if (!rg) return null;
  return {
    gapScore: rg.gapScore,
    display: rg.display,
    totalAttributes: rg.totalAttributes,
    recognizedCount: rg.recognizedCount,
    gapCount: rg.gapCount,
    totalPropertyMentions: rg.totalPropertyMentions,
  };
}

/**
 * Subject-mentioned observation corpus required for validated RG score comparison.
 */
export function assessObservationEvidence(observations = []) {
  const list = Array.isArray(observations) ? observations : [];
  const mentioned = list.filter((o) => o && o.mentioned === true);
  const withAttrs = mentioned.filter(
    (o) => Array.isArray(o.attributesRecognized) && o.attributesRecognized.length > 0
  );
  const sufficient = mentioned.length > 0;
  return {
    observationCount: list.length,
    subjectMentionedCount: mentioned.length,
    subjectMentionedWithAttributesCount: withAttrs.length,
    sufficientForScoreComparison: sufficient,
    status: sufficient
      ? MEASUREMENT_COMPARISON_STATUS.OBSERVATION_BACKED
      : MEASUREMENT_COMPARISON_STATUS.DATA_INSUFFICIENT,
    reason: sufficient
      ? null
      : list.length === 0
        ? "No research-period observations available for dual-run score comparison"
        : "No subject-mentioned observations available — empty corpus cannot validate Reality Gap scores",
  };
}

/**
 * Read-only citation of published RG when raw observations are unavailable.
 * Never treated as dual-run success.
 */
export function loadPublishedRealityGapReference(propertyId) {
  try {
    const report = loadPublishedReport(propertyId);
    const rg = report?.realityGap || null;
    if (!rg || typeof rg !== "object") return null;
    return {
      available: true,
      propertyId,
      periodId: report?.period?.periodId || report?.periodId || null,
      measurementContractVersion: report?.measurementContractVersion || null,
      realityGap: summarizeRg(rg),
      note:
        "Published certified/archived Reality Gap citation only. Not a dual-run recomputation and not observation-backed comparison success.",
      dualRunValid: false,
    };
  } catch {
    return null;
  }
}

/**
 * Detect score "improvement" caused only by shrinking the denominator.
 */
export function explainDenominatorDifference(fixtureRg, bridgedRg, eligibility, { comparisonStatus } = {}) {
  const fixtureDenom = fixtureRg?.totalAttributes ?? 0;
  const bridgedDenom = bridgedRg?.totalAttributes ?? 0;
  const fixtureScore = fixtureRg?.gapScore ?? null;
  const bridgedScore = bridgedRg?.gapScore ?? null;

  const excludedFromEligible = (eligibility?.rows || [])
    .filter((r) => r.inSelectionInventory && !r.inEligibleDenominator)
    .map((r) => ({
      attribute: r.attribute,
      eligibilityStatus: r.eligibilityStatus,
      note: r.note,
    }));

  if (comparisonStatus === MEASUREMENT_COMPARISON_STATUS.DATA_INSUFFICIENT) {
    return {
      fixtureDenominator: fixtureDenom,
      bridgedEligibleDenominator: bridgedDenom,
      denominatorDelta: bridgedDenom - fixtureDenom,
      fixtureGapScore: null,
      bridgedEligibleGapScore: null,
      gapScoreDelta: null,
      excludedFromEligible,
      artifactWarning: null,
      customerFacingUnchanged: true,
      comparisonStatus: MEASUREMENT_COMPARISON_STATUS.DATA_INSUFFICIENT,
      interpretation:
        "Attribute denominators compared; Reality Gap scores withheld — DATA_INSUFFICIENT observation evidence.",
    };
  }

  let artifactWarning = null;
  if (
    fixtureScore != null &&
    bridgedScore != null &&
    bridgedDenom < fixtureDenom &&
    bridgedScore < fixtureScore
  ) {
    artifactWarning =
      "DENOMINATOR_ARTIFACT — bridged gapScore is lower (looks 'better') primarily because ineligible selected attributes were excluded from the diagnostic denominator. This is NOT improved AI recognition.";
  }

  return {
    fixtureDenominator: fixtureDenom,
    bridgedEligibleDenominator: bridgedDenom,
    denominatorDelta: bridgedDenom - fixtureDenom,
    fixtureGapScore: fixtureScore,
    bridgedEligibleGapScore: bridgedScore,
    gapScoreDelta:
      fixtureScore != null && bridgedScore != null ? bridgedScore - fixtureScore : null,
    excludedFromEligible,
    artifactWarning,
    customerFacingUnchanged: true,
    comparisonStatus: MEASUREMENT_COMPARISON_STATUS.OBSERVATION_BACKED,
    interpretation:
      "Compare inventories and per-attribute recognition. Never treat denominator exclusion as AI positioning improvement.",
  };
}

/**
 * Build dual-run diagnostic for one property.
 */
export function buildVerifiedContextBridgeDiagnostic(
  propertyId,
  {
    profile = null,
    observations = null,
    period = null,
    fixturePath = null,
    airtableFactIndex = null,
    minimumEligible = undefined,
  } = {}
) {
  const loadedProfile = profile || loadPropertyProfile(propertyId);
  if (!loadedProfile) {
    return {
      ok: false,
      error: "property_profile_missing",
      propertyId,
      contractVersion: ADP_VERIFIED_CONTEXT_BRIDGE_V1,
    };
  }

  const selectedAttributes = Array.isArray(loadedProfile.attributes)
    ? [...loadedProfile.attributes]
    : [];

  const censusFromRegistry = getCensusRecordIdForAdpProperty(propertyId);
  const hpcHotelId =
    loadedProfile.censusRecordId ||
    loadedProfile.hpcHotelId ||
    censusFromRegistry ||
    PILOT_HOTELS.concat(REGRESSION_HOTELS).find((h) => h.propertyId === propertyId)?.hpcHotelId ||
    null;

  const eligibility = classifySelectedAttributeEligibility(selectedAttributes, loadedProfile, {
    fixturePath,
    airtableFactIndex,
  });

  // Minimum/max uses unique commercial concepts — true duplicates cannot pad the threshold.
  const uniqueConcepts = collapseToUniqueMeasurableConcepts(eligibility.eligibleKeys);
  const coverage = assessMeasurementCoveragePolicy(uniqueConcepts.uniqueConceptCount, {
    minimum: minimumEligible,
  });
  // Preserve Phase 1B shape fields used by closeout summarizers
  coverage.meetsMinimum = coverage.meetsMinimum;
  coverage.eligibleCount = coverage.uniqueEligibleConceptCount;
  coverage.flag =
    coverage.status === "BELOW_MINIMUM_ATTRIBUTE_COVERAGE"
      ? "BELOW_MINIMUM_ATTRIBUTE_COVERAGE"
      : coverage.status === "ABOVE_MAXIMUM_ATTRIBUTE_COVERAGE"
        ? "ABOVE_MAXIMUM_ATTRIBUTE_COVERAGE"
        : null;

  const suggestions = suggestAttributeCandidates(loadedProfile, {
    selectedKeys: selectedAttributes,
  });

  let resolvedPeriod = period;
  if (!resolvedPeriod && observations == null) {
    resolvedPeriod = loadLatestCustomerPeriod(propertyId);
  }
  const obs =
    Array.isArray(observations)
      ? observations
      : Array.isArray(resolvedPeriod?.observations)
        ? resolvedPeriod.observations
        : [];

  const observationEvidence = assessObservationEvidence(obs);
  const publishedReference =
    observationEvidence.status === MEASUREMENT_COMPARISON_STATUS.DATA_INSUFFICIENT
      ? loadPublishedRealityGapReference(propertyId)
      : null;

  const fixtureDenomKeys = selectedAttributes.filter((a) => ATTRIBUTE_DEFINITIONS[a]);
  const bridgedDenomKeys = eligibility.eligibleKeys;

  let fixtureRg = null;
  let bridgedRg = null;
  let dualRunScoresValid = false;

  if (observationEvidence.sufficientForScoreComparison) {
    fixtureRg = computeRealityGap(obs, loadedProfile);
    bridgedRg = computeRealityGap(obs, {
      ...loadedProfile,
      attributes: eligibility.eligibleKeys,
    });
    dualRunScoresValid = true;
  } else {
    // Denominator inventory only — do not treat computeRealityGap([]) as a validated score.
    fixtureRg = {
      gapScore: null,
      display: null,
      totalAttributes: fixtureDenomKeys.length,
      recognizedCount: null,
      gapCount: null,
      totalPropertyMentions: 0,
    };
    bridgedRg = {
      gapScore: null,
      display: null,
      totalAttributes: bridgedDenomKeys.length,
      recognizedCount: null,
      gapCount: null,
      totalPropertyMentions: 0,
    };
  }

  const difference = explainDenominatorDifference(fixtureRg, bridgedRg, eligibility, {
    comparisonStatus: observationEvidence.status,
  });

  const attributeCoverageValidation = {
    status: "ATTRIBUTE_COVERAGE_VALIDATED",
    selectedCount: selectedAttributes.length,
    eligibleCount: eligibility.eligibleCount,
    eligibleUniqueConceptCount: eligibility.eligibleUniqueConceptCount,
    minimumCoverage: coverage,
  };

  const measurementComparison = {
    status: observationEvidence.status,
    dualRunScoresValid,
    observationEvidence,
    reason: observationEvidence.reason,
    publishedReference,
  };

  const transparency = {
    totalVerifiedHotelAttributesEstimate: eligibility.rows.filter((r) =>
      String(r.verificationStatus || "").startsWith("VERIFIED_")
    ).length,
    suggestedPositioningAttributes: suggestions.suggestionCount,
    approvedSelectedAttributes: selectedAttributes.length,
    eligibleMeasurableAttributes: eligibility.eligibleCount,
    eligibleUniqueConcepts: uniqueConcepts.uniqueConceptCount,
    actuallyMeasuredFixture: dualRunScoresValid ? fixtureRg.totalAttributes : null,
    actuallyMeasuredBridgedEligible: dualRunScoresValid ? bridgedRg.totalAttributes : null,
    belowMinimumShortfall: coverage.shortfall,
    awaitingVerification: eligibility.needsVerificationKeys.length,
    missingRecognitionDefinitions: eligibility.undefinedKeys.length,
    measurementComparisonStatus: measurementComparison.status,
  };

  const inventoryPin = {
    selectedAttributeKeys: selectedAttributes,
    selectionInventoryHash: eligibility.selectionInventoryHash,
    attributeDefinitionsVersion: eligibility.attributeDefinitionsVersion,
    eligibilityPolicyVersion: eligibility.eligibilityPolicyVersion,
    methodologyNote: "Customer RG remains fixture baseline; bridged path is diagnostic-only",
    researchPeriodId: resolvedPeriod?.periodId || resolvedPeriod?.id || null,
    observationCount: obs.length,
    subjectMentionedCount: observationEvidence.subjectMentionedCount,
  };

  return {
    ok: true,
    contractVersion: ADP_VERIFIED_CONTEXT_BRIDGE_V1,
    diagnosticMode: DIAGNOSTIC_MODE,
    customerFacingMode: FIXTURE_BASELINE_MODE,
    propertyId,
    propertyName: loadedProfile.name || loadedProfile.displayName || null,
    hpcHotelId,
    identity: {
      propertyId,
      hpcHotelId,
      censusRecordId: censusFromRegistry || loadedProfile.censusRecordId || null,
      isolationKey: `${propertyId}::${hpcHotelId || "NO_HPC"}`,
    },
    selectedAttributes,
    eligibility,
    minimumCoverage: coverage,
    attributeCoverageValidation,
    measurementComparison,
    suggestions: {
      ...suggestions,
      topSuggestions: suggestions.suggestions.slice(0, 12),
    },
    dualRun: {
      fixtureBaseline: {
        mode: FIXTURE_BASELINE_MODE,
        scoresValid: dualRunScoresValid,
        realityGap: dualRunScoresValid
          ? summarizeRg(fixtureRg)
          : {
              gapScore: null,
              display: null,
              totalAttributes: fixtureDenomKeys.length,
              recognizedCount: null,
              gapCount: null,
              totalPropertyMentions: 0,
              status: MEASUREMENT_COMPARISON_STATUS.DATA_INSUFFICIENT,
            },
        denominatorKeys: fixtureDenomKeys,
      },
      bridgedEligible: {
        mode: DIAGNOSTIC_MODE,
        scoresValid: dualRunScoresValid,
        realityGap: dualRunScoresValid
          ? summarizeRg(bridgedRg)
          : {
              gapScore: null,
              display: null,
              totalAttributes: bridgedDenomKeys.length,
              recognizedCount: null,
              gapCount: null,
              totalPropertyMentions: 0,
              status: MEASUREMENT_COMPARISON_STATUS.DATA_INSUFFICIENT,
            },
        denominatorKeys: bridgedDenomKeys,
      },
      difference,
    },
    transparency,
    inventoryPin,
    historicalCompatibility: {
      customerFacingRealityGapUnchanged: true,
      historicalPeriodsImmutable: true,
      methodologyWeightsUnchanged: true,
      automaticPublication: false,
      cutoverAuthorized: false,
    },
    generatedAt: new Date().toISOString(),
  };
}

export function buildPilotSuiteDiagnostics(opts = {}) {
  const pilots = PILOT_HOTELS.map((p) => buildVerifiedContextBridgeDiagnostic(p.propertyId, opts));
  const regression = REGRESSION_HOTELS.map((p) =>
    buildVerifiedContextBridgeDiagnostic(p.propertyId, opts)
  );

  const isolation = [];
  for (let i = 0; i < pilots.length; i++) {
    for (let j = i + 1; j < pilots.length; j++) {
      const a = pilots[i];
      const b = pilots[j];
      if (!a.ok || !b.ok) continue;
      isolation.push({
        a: a.propertyId,
        b: b.propertyId,
        selectionHashesEqual:
          a.eligibility.selectionInventoryHash === b.eligibility.selectionInventoryHash,
        ok: a.identity.isolationKey !== b.identity.isolationKey,
      });
    }
  }

  return {
    contractVersion: ADP_VERIFIED_CONTEXT_BRIDGE_V1,
    suite: "PHASE_1B_PR1_VERIFIED_CONTEXT_DUAL_RUN",
    pilots,
    regression,
    isolation,
    observationBackedHotels: [...pilots, ...regression]
      .filter((p) => p.ok && p.measurementComparison?.dualRunScoresValid)
      .map((p) => p.propertyId),
    attributeCoverageOnlyHotels: [...pilots, ...regression]
      .filter((p) => p.ok && !p.measurementComparison?.dualRunScoresValid)
      .map((p) => p.propertyId),
    generatedAt: new Date().toISOString(),
    contentHash: createHash("sha256")
      .update(
        JSON.stringify({
          pilots: pilots.map((p) => p.propertyId),
          hashes: pilots.map((p) => p.eligibility?.selectionInventoryHash),
        })
      )
      .digest("hex")
      .slice(0, 16),
  };
}
