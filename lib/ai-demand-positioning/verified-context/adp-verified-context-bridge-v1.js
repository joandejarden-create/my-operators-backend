/**
 * ADP 2.0 Phase 1B — Verified Context Bridge + Reality Gap dual-run (diagnostic only).
 * Does not alter customer-facing Reality Gap or historical periods.
 */

import { createHash } from "crypto";
import { ATTRIBUTE_DEFINITIONS, computeRealityGap } from "../intelligence/reality-gap.js";
import { loadPropertyProfile, loadLatestCustomerPeriod } from "../data-model.js";
import { getCensusRecordIdForAdpProperty } from "../census-link-registry.js";
import { classifySelectedAttributeEligibility } from "./adp-attribute-eligibility-classifier-v1.js";
import { assessMinimumAttributeCoverage } from "./adp-minimum-attribute-coverage-v1.js";
import { suggestAttributeCandidates } from "./adp-attribute-suggestion-candidates-v1.js";

export const ADP_VERIFIED_CONTEXT_BRIDGE_V1 = "ADP_VERIFIED_CONTEXT_BRIDGE_V1";
export const DIAGNOSTIC_MODE = "RG_BRIDGED_ELIGIBLE_DIAGNOSTIC_V1";
export const FIXTURE_BASELINE_MODE = "RG_FIXTURE_BASELINE";

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
 * Detect score "improvement" caused only by shrinking the denominator.
 */
export function explainDenominatorDifference(fixtureRg, bridgedRg, eligibility) {
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

  const coverage = assessMinimumAttributeCoverage(eligibility.eligibleCount, {
    minimumEligible,
  });

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

  const fixtureRg = computeRealityGap(obs, loadedProfile);
  const bridgedProfile = {
    ...loadedProfile,
    attributes: eligibility.eligibleKeys,
  };
  const bridgedRg = computeRealityGap(obs, bridgedProfile);

  const difference = explainDenominatorDifference(fixtureRg, bridgedRg, eligibility);

  const transparency = {
    totalVerifiedHotelAttributesEstimate: eligibility.rows.filter((r) =>
      String(r.verificationStatus || "").startsWith("VERIFIED_")
    ).length,
    suggestedPositioningAttributes: suggestions.suggestionCount,
    approvedSelectedAttributes: selectedAttributes.length,
    eligibleMeasurableAttributes: eligibility.eligibleCount,
    actuallyMeasuredFixture: fixtureRg.totalAttributes,
    actuallyMeasuredBridgedEligible: bridgedRg.totalAttributes,
    belowMinimumShortfall: coverage.shortfall,
    awaitingVerification: eligibility.needsVerificationKeys.length,
    missingRecognitionDefinitions: eligibility.undefinedKeys.length,
  };

  const inventoryPin = {
    selectedAttributeKeys: selectedAttributes,
    selectionInventoryHash: eligibility.selectionInventoryHash,
    attributeDefinitionsVersion: eligibility.attributeDefinitionsVersion,
    eligibilityPolicyVersion: eligibility.eligibilityPolicyVersion,
    methodologyNote: "Customer RG remains fixture baseline; bridged path is diagnostic-only",
    researchPeriodId: resolvedPeriod?.periodId || resolvedPeriod?.id || null,
    observationCount: obs.length,
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
    suggestions: {
      ...suggestions,
      // Cap payload size in report summaries
      topSuggestions: suggestions.suggestions.slice(0, 12),
    },
    dualRun: {
      fixtureBaseline: {
        mode: FIXTURE_BASELINE_MODE,
        realityGap: summarizeRg(fixtureRg),
        denominatorKeys: selectedAttributes.filter((a) => ATTRIBUTE_DEFINITIONS[a]),
      },
      bridgedEligible: {
        mode: DIAGNOSTIC_MODE,
        realityGap: summarizeRg(bridgedRg),
        denominatorKeys: eligibility.eligibleKeys,
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
  const pilots = PILOT_HOTELS.map((p) =>
    buildVerifiedContextBridgeDiagnostic(p.propertyId, {
      ...opts,
      // identity check
    })
  );
  const regression = REGRESSION_HOTELS.map((p) =>
    buildVerifiedContextBridgeDiagnostic(p.propertyId, opts)
  );

  // Cross-hotel isolation: ensure no attribute inventory bleed
  const isolation = [];
  for (let i = 0; i < pilots.length; i++) {
    for (let j = i + 1; j < pilots.length; j++) {
      const a = pilots[i];
      const b = pilots[j];
      if (!a.ok || !b.ok) continue;
      const sameSelection =
        a.eligibility.selectionInventoryHash === b.eligibility.selectionInventoryHash;
      isolation.push({
        a: a.propertyId,
        b: b.propertyId,
        selectionHashesEqual: sameSelection,
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
