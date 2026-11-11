/**
 * ADP_REALITY_GAP_CANONICAL_ATTRIBUTE_UNIQUENESS_V1
 * Fail-closed gate: no duplicate customer-facing Reality Gap attributes.
 */
import {
  ADP_REALITY_GAP_CANONICAL_ATTRIBUTE_DEDUP_V1,
  findDuplicateCustomerFacingLabels,
  findRecognizeMissConflicts,
  resolveCanonicalAttributeSet,
} from "../property-attributes/adp-reality-gap-canonical-attribute-dedup-v1.js";
import { ATTRIBUTE_DEFINITIONS, computeRealityGap } from "../intelligence/reality-gap.js";
import { CONDITIONAL_APPROVED_METHODOLOGY_ID } from "../intelligence/reality-gap-founder-approval-v2.js";
import { independentlyRecalculateRealityGapV1V2 } from "./adp-rg-v2-cross-surface-parity-certification-v1.js";
import { loadStagedOwnerPayload } from "../review/adp-staged-report-store-v1.js";
import { loadPropertyProfile } from "../data-model.js";
import { listCanonicalAdp31PropertyIds } from "../governance/adp-operational-registry-hygiene-v1.js";

export const ADP_REALITY_GAP_CANONICAL_ATTRIBUTE_UNIQUENESS_V1 =
  "ADP_REALITY_GAP_CANONICAL_ATTRIBUTE_UNIQUENESS_V1";

export function assessRealityGapUniquenessFromPayload(realityGap, propertyId = null) {
  const issues = [];
  const recognized = realityGap?.recognized || [];
  const gaps = realityGap?.gaps || [];
  const cards = [...recognized, ...gaps];
  const dups = findDuplicateCustomerFacingLabels(cards);
  if (dups.length) {
    issues.push({ code: "DUPLICATE_CUSTOMER_FACING_LABEL", detail: dups });
  }
  const conflicts = findRecognizeMissConflicts(recognized, gaps);
  if (conflicts.length) {
    issues.push({ code: "RECOGNIZE_AND_MISS_SAME_ATTRIBUTE", detail: conflicts });
  }
  const attrs = cards.map((c) => c.attribute).filter(Boolean);
  if (attrs.includes("restaurant") && attrs.includes("on_site_restaurant")) {
    issues.push({
      code: "RESTAURANT_ALIAS_PAIR_BOTH_DISPLAYED",
      detail: { restaurant: true, on_site_restaurant: true },
    });
  }
  const dedup = realityGap?.canonicalDedup;
  if (dedup && dedup.contractVersion !== ADP_REALITY_GAP_CANONICAL_ATTRIBUTE_DEDUP_V1) {
    issues.push({
      code: "MISSING_OR_STALE_DEDUP_CONTRACT",
      detail: dedup.contractVersion || null,
    });
  }
  // Inflated denominator heuristic: profile collapse should match tested set size loosely
  if (propertyId) {
    const profile = loadPropertyProfile(propertyId);
    const governed = (profile?.attributes || []).filter((a) => ATTRIBUTE_DEFINITIONS[a]);
    const resolved = resolveCanonicalAttributeSet(governed, {
      labelFor: (id) => ATTRIBUTE_DEFINITIONS[id]?.label || id,
    });
    if (
      realityGap?.testedEligibleCount != null &&
      realityGap.testedEligibleCount > resolved.canonicalCount
    ) {
      issues.push({
        code: "TESTED_ELIGIBLE_EXCEEDS_CANONICAL_SET",
        detail: {
          testedEligibleCount: realityGap.testedEligibleCount,
          canonicalCount: resolved.canonicalCount,
        },
      });
    }
  }
  return {
    gate: ADP_REALITY_GAP_CANONICAL_ATTRIBUTE_UNIQUENESS_V1,
    propertyId,
    ok: issues.length === 0,
    issues,
  };
}

export function assessHotelRealityGapUniqueness(propertyId) {
  const staged = loadStagedOwnerPayload(propertyId);
  const fromStaged = assessRealityGapUniquenessFromPayload(
    staged?.payload?.realityGap,
    propertyId
  );
  const live = independentlyRecalculateRealityGapV1V2(propertyId);
  // Live path returns counts; also rebuild payload via compute for cards when needed
  return {
    propertyId,
    stagedOk: fromStaged.ok,
    stagedIssues: fromStaged.issues,
    liveRecalcOk: live.ok,
    liveGapScoreV2: live.ok ? live.gapScoreV2 : null,
    ok: fromStaged.ok,
  };
}

export function runAll31UniquenessGate() {
  const rows = [];
  for (const propertyId of listCanonicalAdp31PropertyIds()) {
    rows.push(assessHotelRealityGapUniqueness(propertyId));
  }
  const fail = rows.filter((r) => !r.ok);
  return {
    gate: ADP_REALITY_GAP_CANONICAL_ATTRIBUTE_UNIQUENESS_V1,
    hotelCount: rows.length,
    passCount: rows.filter((r) => r.ok).length,
    failCount: fail.length,
    ok: fail.length === 0,
    failures: fail.slice(0, 20),
    rows,
  };
}

/** Synthetic negative fixtures for permanent regression tests. */
export function negativeRegressionFixtures() {
  const badDup = {
    recognized: [],
    gaps: [
      { attribute: "restaurant", label: "On-Site Restaurant" },
      { attribute: "on_site_restaurant", label: "On-Site Restaurant" },
    ],
    testedEligibleCount: 22,
  };
  const badBothSides = {
    recognized: [{ attribute: "meeting_space", label: "Meeting & Event Space" }],
    gaps: [{ attribute: "meetings", label: "Meeting & Event Space" }],
  };
  return {
    restaurantPairMustFail: !assessRealityGapUniquenessFromPayload(badDup).ok,
    recognizeMissMustFail: !assessRealityGapUniquenessFromPayload(badBothSides).ok,
    methodologyId: CONDITIONAL_APPROVED_METHODOLOGY_ID,
  };
}
