/**
 * Deal-level Operator Fit v2 evaluation entry (pure domain + adapters).
 */

import { adaptProjectFromDealContext } from "./adapters/project-from-deal.js";
import {
  adaptOperatorFromPrefill,
  adaptBrandManagedCandidate,
} from "./adapters/operator-from-prefill.js";
import { selectTop5OperatorAlignment } from "./top5-selector.js";
import {
  getOperatorFitEngineFlagState,
  OPERATOR_FIT_ENGINE_VERSION,
  resolveOperatorFitMethodology,
  getOperatorFitEngineVersionForMethodology,
} from "./feature-flag.js";

/**
 * @param {{
 *   dealId: string,
 *   dealFields: object,
 *   locationData: object,
 *   mpData: object,
 *   siData: object,
 *   operatorPrefills: Array<{ operatorId: string, companyName?: string, prefill: object }>,
 *   brandManagedCandidates?: Array<object>,
 * }} input
 */
export function evaluateOperatorFitForDeal(input) {
  const flag = getOperatorFitEngineFlagState();
  const project = adaptProjectFromDealContext({
    dealId: input.dealId,
    dealFields: input.dealFields,
    locationData: input.locationData,
    mpData: input.mpData,
    siData: input.siData,
  });
  if (input.allowResearchStage) {
    project.allowResearchStageLifecycle = true;
  }

  const operators = [];
  for (const row of input.operatorPrefills || []) {
    const op = adaptOperatorFromPrefill(row.prefill || {}, {
      operatorId: row.operatorId,
      companyName: row.companyName,
    });
    if (input.allowResearchStage || row.researchStage || row.lifecycle === "Research Stage") {
      op.researchStageAllowed = true;
      op.lifecycle = "Research Stage";
    }
    operators.push(op);
  }

  // Brand-managed candidates when provided and not excluded
  const excl = project.knownExclusions?.excludesBrandManaged?.value === true;
  if (!excl) {
    for (const bm of input.brandManagedCandidates || []) {
      if (!bm || !bm.brandName) continue;
      // Do not treat strategic preference alone as confirmed availability
      const confirmed = Boolean(bm.offersBrandManagementConfirmed);
      const fallbackMarkets =
        project.geography?.country?.value != null
          ? [project.geography.country.value]
          : [];
      operators.push(
        adaptBrandManagedCandidate({
          brandId: bm.brandId,
          brandName: bm.brandName,
          offersBrandManagement: Boolean(bm.offersBrandManagement || confirmed),
          offersBrandManagementConfirmed: confirmed,
          markets: bm.markets && bm.markets.length ? bm.markets : fallbackMarkets,
          scales:
            bm.scales && bm.scales.length
              ? bm.scales
              : project.hotelSegment?.value
                ? [project.hotelSegment.value]
                : [],
          evidenceClasses: bm.evidenceClasses,
          sources: bm.sources,
        })
      );
    }
  }

  const methodology = resolveOperatorFitMethodology(input || {});
  const { top5, diagnostics, allEvaluated } = selectTop5OperatorAlignment(
    project,
    operators,
    {
      methodology,
      criFormulation: input.criFormulation,
      useV21: input.useV21,
    }
  );

  return {
    featureVersion: getOperatorFitEngineVersionForMethodology(methodology) || OPERATOR_FIT_ENGINE_VERSION,
    methodology,
    flag,
    dealId: input.dealId,
    project,
    projectSummary: {
      country: project.geography?.country?.value || null,
      scale: project.hotelSegment?.value || null,
      developmentType: project.developmentType?.value || null,
      preferredBrands: project.selectedOrEvaluatedBrands?.value || [],
      structures: project.operatingStructurePreferences?.value || [],
    },
    top5,
    diagnostics,
    allEvaluated,
    shortlistNote:
      "Target List is brand-only; operator save-to-Target-List is disabled in Phase 1–2.",
  };
}
