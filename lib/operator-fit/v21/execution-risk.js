/**
 * Operator Fit v2.1 — execution risk.
 * Numerical penalties only for confirmed_risk.
 * unknown_validation / potential_concern remain as items (Validate Next) with 0 applied points.
 */

import { EXECUTION_RISK, CANDIDATE_TYPE } from "../config.js";
import { listValue, scalarValue, isKnownPositive } from "../adapters/field-state.js";

/**
 * Build the same item catalog as v2, then apply v2.1 numerical policy.
 */
export function evaluateExecutionRiskV21(
  project,
  operator,
  { eligibility, coverage, structureAlign, brandCompat }
) {
  const items = [];
  const P = EXECUTION_RISK.penalties;

  const country = scalarValue(project.geography?.country);
  const opCountries = listValue(operator.geography?.countries);
  if (country && opCountries.length) {
    const hit = opCountries.some(
      (c) => String(c).toLowerCase() === country.toLowerCase()
    );
    if (!hit) {
      items.push({
        key: "geographicMobilization",
        kind: "confirmed_risk",
        points: P.geographicMobilization,
        message: `Geographic mobilization risk: no active country match for ${country}.`,
      });
    }
  } else if (country && !opCountries.length) {
    items.push({
      key: "geographicMobilization",
      kind: "unknown_validation",
      points: Math.round(P.geographicMobilization / 2),
      message: "Geographic mobilization requires validation (operator countries unknown).",
    });
  }

  if (brandCompat.state === "unknown") {
    items.push({
      key: "brandApprovalUncertainty",
      kind: "unknown_validation",
      points: P.brandApprovalUncertainty,
      message: "Brand-approval / relationship status is unconfirmed.",
    });
  } else if (brandCompat.category === "Unsupported") {
    items.push({
      key: "brandApprovalUncertainty",
      kind: "confirmed_risk",
      points: P.brandApprovalUncertainty,
      message: "Brand relationship appears unsupported for preferred brands.",
    });
  }

  if (structureAlign.state === "unknown") {
    items.push({
      key: "missingStructureSupport",
      kind: "unknown_validation",
      points: Math.round(P.missingStructureSupport / 2),
      message: "Operating-structure support is unconfirmed.",
    });
  } else if (structureAlign.score != null && structureAlign.score < 40) {
    items.push({
      key: "missingStructureSupport",
      kind: "confirmed_risk",
      points: P.missingStructureSupport,
      message: "Weak operating-structure alignment.",
    });
  }

  const comps = isKnownPositive(operator.comparables) ? operator.comparables.value : [];
  const assets = listValue(operator.assetExperience);
  const sits = listValue(operator.developmentExperience);
  if (!comps.length && !assets.length && !sits.length) {
    items.push({
      key: "limitedComparableExperience",
      kind: "unknown_validation",
      points: P.limitedComparableExperience,
      message: "Directly comparable experience is not documented.",
    });
  } else if (!comps.length && assets.length >= 6) {
    items.push({
      key: "limitedComparableExperience",
      kind: "potential_concern",
      points: Math.round(P.limitedComparableExperience / 2),
      message: "Portfolio breadth present without project-similar comparables.",
    });
  }

  const resources = listValue(operator.regionalResources);
  if (!resources.length) {
    items.push({
      key: "unconfirmedRegionalResources",
      kind: "unknown_validation",
      points: P.unconfirmedRegionalResources,
      message: "Regional resources are unconfirmed.",
    });
  }

  const pre = scalarValue(project.preOpeningNeeds);
  if (pre && /yes|full|required/i.test(pre)) {
    const opPre = scalarValue(operator.specialistExperience?.preOpening);
    if (!opPre) {
      items.push({
        key: "unconfirmedPreOpeningCapacity",
        kind: "unknown_validation",
        points: P.unconfirmedPreOpeningCapacity,
        message: "Pre-opening capacity is unconfirmed while the project indicates need.",
      });
    }
  }

  if ((coverage.coveragePct || 0) < 40) {
    items.push({
      key: "materialDataGaps",
      kind: "potential_concern",
      points: P.materialDataGaps,
      message: `Material data gaps (coverage ${coverage.coveragePct}%).`,
    });
  }

  const lessIdeal = scalarValue(operator.risksAndConcerns);
  const breakers = listValue(project.knownExclusions?.dealBreakers);
  if (lessIdeal && breakers.some((b) => lessIdeal.toLowerCase().includes(String(b).toLowerCase()))) {
    items.push({
      key: "competitiveConflict",
      kind: "confirmed_risk",
      points: P.competitiveConflict,
      message: "Deal-breaker overlap with operator less-ideal situations.",
    });
  }

  if (operator.candidateType === CANDIDATE_TYPE.BRAND_MANAGED) {
    // no extra invention
  }

  const annotated = items.map((it) => {
    const appliesNumerically = it.kind === "confirmed_risk";
    return {
      ...it,
      catalogPoints: it.points,
      appliedPoints: appliesNumerically ? it.points : 0,
      v21Numerical: appliesNumerically,
    };
  });

  const totalCatalog = annotated.reduce((s, i) => s + (i.catalogPoints || 0), 0);
  const totalApplied = annotated.reduce((s, i) => s + (i.appliedPoints || 0), 0);
  const capped = Math.min(totalApplied, EXECUTION_RISK.maxTotalPenaltyPoints);

  return {
    totalPenaltyPoints: totalApplied,
    cappedPenaltyPoints: capped,
    catalogPenaltyPoints: totalCatalog,
    items: annotated,
    policy: "v21_confirmed_risk_only",
  };
}
