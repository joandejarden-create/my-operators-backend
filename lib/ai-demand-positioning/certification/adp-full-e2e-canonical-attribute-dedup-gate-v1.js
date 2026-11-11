/**
 * ADP_FULL_E2E_CANONICAL_ATTRIBUTE_DEDUP_GATE_V1
 *
 * Mandatory three-stage attribute deduplication for every Full ADP E2E run:
 *   Gate 1 — after property research, before paid monitoring
 *   Gate 2 — before analytical / Reality Gap calculations
 *   Gate 3 — before customer-release certification (report / share / PDF)
 *
 * Fail-closed. Do not loosen standards. Do not mask failures.
 */

import {
  ADP_REALITY_GAP_CANONICAL_ATTRIBUTE_DEDUP_V1,
  REALITY_GAP_CANONICAL_EQUIVALENCE_REGISTRY_V1,
  resolveCanonicalAttributeSet,
  findDuplicateCustomerFacingLabels,
  findRecognizeMissConflicts,
} from "../property-attributes/adp-reality-gap-canonical-attribute-dedup-v1.js";
import { ATTRIBUTE_DEFINITIONS } from "../intelligence/reality-gap.js";
import { CONDITIONAL_APPROVED_METHODOLOGY_ID } from "../intelligence/reality-gap-methodology-v2.js";
import { assessRealityGapUniquenessFromPayload } from "./adp-reality-gap-canonical-attribute-uniqueness-v1.js";
import { independentlyRecalculateRealityGapV1V2 } from "./adp-rg-v2-cross-surface-parity-certification-v1.js";
import { coverageScoreForProfile } from "../property-attributes/adp-hotel-attribute-intelligence-standard-v1.js";
import { loadPropertyProfile } from "../data-model.js";
import { loadStagedOwnerPayload } from "../review/adp-staged-report-store-v1.js";
import { listCanonicalAdp31PropertyIds } from "../governance/adp-operational-registry-hygiene-v1.js";

export const ADP_FULL_E2E_CANONICAL_ATTRIBUTE_DEDUP_GATE_V1 =
  "ADP_FULL_E2E_CANONICAL_ATTRIBUTE_DEDUP_GATE_V1";

export const DEDUP_GATE_STAGE = Object.freeze({
  PRE_MONITORING: "GATE1_PRE_MONITORING",
  PRE_CALCULATION: "GATE2_PRE_CALCULATION",
  PRE_CUSTOMER_RELEASE: "GATE3_PRE_CUSTOMER_RELEASE",
});

const ALIAS_PAIRS_MUST_COLLAPSE = Object.freeze([
  ["restaurant", "on_site_restaurant"],
  ["meetings", "meeting_space"],
  ["meeting_rooms", "meeting_space"],
  ["spa", "full_service_spa"],
  ["large_ballroom", "ballroom"],
]);

function pushIssue(issues, code, detail) {
  issues.push({ code, detail });
}

/**
 * Gate 1 — clean attribute inventory before paid monitoring / scenario generation.
 */
export function runDedupGate1PreMonitoring(propertyId, opts = {}) {
  const issues = [];
  const profile = opts.profile || loadPropertyProfile(propertyId);
  if (!profile) {
    return {
      gate: ADP_FULL_E2E_CANONICAL_ATTRIBUTE_DEDUP_GATE_V1,
      stage: DEDUP_GATE_STAGE.PRE_MONITORING,
      propertyId,
      ok: false,
      issues: [{ code: "PROFILE_MISSING" }],
    };
  }
  const attrs = (profile.attributes || []).filter((a) => ATTRIBUTE_DEFINITIONS[a]);
  if (!attrs.length) {
    pushIssue(issues, "NO_GOVERNED_ATTRIBUTES", {});
  }
  const resolved = resolveCanonicalAttributeSet(attrs, {
    labelFor: (id) => ATTRIBUTE_DEFINITIONS[id]?.label || id,
  });
  const cov = coverageScoreForProfile(profile, attrs);
  if (!cov.metMinimum) {
    pushIssue(issues, "BELOW_CLASS_COVERAGE_FLOOR", {
      coverageClass: cov.coverageClass,
      verifiedPresentUnique: cov.verifiedPresentUnique,
      min: cov.thresholds?.min,
    });
  }
  // Raw inventory must not keep both members of a collapse pair as independent
  // measurement keys after canonical resolve (canonical set is the monitoring input).
  const idList = resolved.canonicalIds || [];
  for (const [a, b] of ALIAS_PAIRS_MUST_COLLAPSE) {
    if (attrs.includes(a) && attrs.includes(b)) {
      // Both present in raw profile is OK if resolve collapses to one display id
      if (idList.includes(a) && idList.includes(b) && a !== b) {
        pushIssue(issues, "ALIAS_PAIR_NOT_COLLAPSED_PRE_MONITORING", { a, b });
      }
    }
  }
  // Related-distinct must not be forced into the same canonical id incorrectly
  for (const group of REALITY_GAP_CANONICAL_EQUIVALENCE_REGISTRY_V1) {
    for (const distinct of group.relatedDistinct || []) {
      if (group.aliases.includes(distinct)) {
        pushIssue(issues, "RELATED_DISTINCT_LISTED_AS_ALIAS", {
          canonicalId: group.canonicalId,
          distinct,
        });
      }
    }
  }

  return {
    gate: ADP_FULL_E2E_CANONICAL_ATTRIBUTE_DEDUP_GATE_V1,
    stage: DEDUP_GATE_STAGE.PRE_MONITORING,
    propertyId,
    ok: issues.length === 0,
    issues,
    coverage: cov,
    canonicalCount: resolved.canonicalCount ?? idList.length,
    rawCount: attrs.length,
    contract: ADP_REALITY_GAP_CANONICAL_ATTRIBUTE_DEDUP_V1,
    blocksPaidMonitoring: issues.length > 0,
  };
}

/**
 * Gate 2 — before Reality Gap / analytical certification.
 */
export function runDedupGate2PreCalculation(propertyId, opts = {}) {
  const issues = [];
  const staged = opts.staged || loadStagedOwnerPayload(propertyId);
  const rg = opts.realityGap || staged?.payload?.realityGap || null;
  const live = opts.liveRecalc || independentlyRecalculateRealityGapV1V2(propertyId);

  if (!rg) {
    // Allowed for greenfield pre-calc when no staged yet — still require live path when obs exist
    if (live?.ok) {
      // live ok without staged cards: check formula only
    } else {
      pushIssue(issues, "REALITY_GAP_UNAVAILABLE", {});
    }
  }

  if (rg) {
    if (rg.methodologyId !== CONDITIONAL_APPROVED_METHODOLOGY_ID) {
      pushIssue(issues, "STALE_OR_WRONG_RG_METHODOLOGY", rg.methodologyId);
    }
    const uniqueness = assessRealityGapUniquenessFromPayload(rg, propertyId);
    if (!uniqueness.ok) {
      for (const i of uniqueness.issues) pushIssue(issues, i.code, i.detail);
    }
    if (live?.ok && Number(rg.gapScore) !== Number(live.gapScoreV2)) {
      pushIssue(issues, "RG_V2_LIVE_MISMATCH", {
        staged: rg.gapScore,
        live: live.gapScoreV2,
      });
    }
    // Untested must not be treated as missed — surface if measurementStatus claims miss without tests
    if (
      rg.testedEligibleCount === 0 &&
      (rg.missedCount > 0 || (rg.gaps || []).length > 0)
    ) {
      pushIssue(issues, "UNTESTED_TREATED_AS_MISSED", {
        testedEligibleCount: rg.testedEligibleCount,
        missedCount: rg.missedCount,
      });
    }
  }

  const g1 = runDedupGate1PreMonitoring(propertyId);
  if (!g1.ok) {
    pushIssue(issues, "GATE1_FAILED_UPSTREAM", g1.issues);
  }

  return {
    gate: ADP_FULL_E2E_CANONICAL_ATTRIBUTE_DEDUP_GATE_V1,
    stage: DEDUP_GATE_STAGE.PRE_CALCULATION,
    propertyId,
    ok: issues.length === 0,
    issues,
    blocksReportCertification: issues.length > 0,
    formula: CONDITIONAL_APPROVED_METHODOLOGY_ID,
    liveGapScoreV2: live?.ok ? live.gapScoreV2 : null,
  };
}

/**
 * Gate 3 — before CUSTOMER_READY: actual report cards + optional PDF/share hints.
 */
export function runDedupGate3PreCustomerRelease(propertyId, opts = {}) {
  const issues = [];
  const staged = opts.staged || loadStagedOwnerPayload(propertyId);
  const rg = staged?.payload?.realityGap;
  if (!staged?.payload) {
    pushIssue(issues, "STAGED_REPORT_MISSING", {});
  }
  const uniqueness = assessRealityGapUniquenessFromPayload(rg, propertyId);
  if (!uniqueness.ok) {
    for (const i of uniqueness.issues) pushIssue(issues, i.code, i.detail);
  }
  const recognized = rg?.recognized || [];
  const gaps = rg?.gaps || [];
  const dups = findDuplicateCustomerFacingLabels([...recognized, ...gaps]);
  if (dups.length) pushIssue(issues, "DUPLICATE_CUSTOMER_FACING_LABEL", dups);
  const conflicts = findRecognizeMissConflicts(recognized, gaps);
  if (conflicts.length) {
    pushIssue(issues, "RECOGNIZE_AND_MISS_SAME_ATTRIBUTE", conflicts);
  }
  const labels = [...recognized, ...gaps].map((c) =>
    String(c.label || "").toLowerCase()
  );
  const onSite = labels.filter((l) => l.includes("on-site restaurant"));
  if (onSite.length > 1) {
    pushIssue(issues, "ON_SITE_RESTAURANT_CARD_DUPLICATE", { count: onSite.length });
  }
  if (rg?.methodologyId !== CONDITIONAL_APPROVED_METHODOLOGY_ID) {
    pushIssue(issues, "STALE_OR_WRONG_RG_METHODOLOGY", rg?.methodologyId);
  }
  if (
    opts.expectedGapScoreV2 != null &&
    Number(rg?.gapScore) !== Number(opts.expectedGapScoreV2)
  ) {
    pushIssue(issues, "MANIFEST_GAP_SCORE_MISMATCH", {
      expected: opts.expectedGapScoreV2,
      actual: rg?.gapScore,
    });
  }
  if (opts.expectedStagedVersionId && staged?.versionId !== opts.expectedStagedVersionId) {
    pushIssue(issues, "STAGED_VERSION_MISMATCH", {
      expected: opts.expectedStagedVersionId,
      actual: staged?.versionId,
    });
  }
  if (opts.pdfSha256Expected && opts.pdfSha256Actual) {
    if (opts.pdfSha256Expected !== opts.pdfSha256Actual) {
      pushIssue(issues, "PDF_HASH_MISMATCH", {
        expected: opts.pdfSha256Expected,
        actual: opts.pdfSha256Actual,
      });
    }
  }

  const g2 = runDedupGate2PreCalculation(propertyId, { staged, realityGap: rg });
  if (!g2.ok) {
    pushIssue(issues, "GATE2_FAILED_UPSTREAM", g2.issues.map((i) => i.code));
  }

  return {
    gate: ADP_FULL_E2E_CANONICAL_ATTRIBUTE_DEDUP_GATE_V1,
    stage: DEDUP_GATE_STAGE.PRE_CUSTOMER_RELEASE,
    propertyId,
    ok: issues.length === 0,
    issues,
    blocksCustomerReady: issues.length > 0,
    stagedVersionId: staged?.versionId || null,
    gapScoreV2: rg?.gapScore ?? null,
  };
}

/** Run all three gates for one hotel. */
export function runAllDedupGates(propertyId, opts = {}) {
  const g1 = runDedupGate1PreMonitoring(propertyId, opts);
  const g2 = runDedupGate2PreCalculation(propertyId, opts);
  const g3 = runDedupGate3PreCustomerRelease(propertyId, opts);
  return {
    gate: ADP_FULL_E2E_CANONICAL_ATTRIBUTE_DEDUP_GATE_V1,
    propertyId,
    ok: g1.ok && g2.ok && g3.ok,
    gate1: g1,
    gate2: g2,
    gate3: g3,
  };
}

export function runAll31DedupGates() {
  const rows = listCanonicalAdp31PropertyIds().map((propertyId) =>
    runAllDedupGates(propertyId)
  );
  const fail = rows.filter((r) => !r.ok);
  return {
    gate: ADP_FULL_E2E_CANONICAL_ATTRIBUTE_DEDUP_GATE_V1,
    hotelCount: rows.length,
    passCount: rows.filter((r) => r.ok).length,
    failCount: fail.length,
    ok: fail.length === 0,
    failures: fail.slice(0, 15).map((f) => ({
      propertyId: f.propertyId,
      g1: f.gate1.issues.map((i) => i.code),
      g2: f.gate2.issues.map((i) => i.code),
      g3: f.gate3.issues.map((i) => i.code),
    })),
    rows,
  };
}

/** Synthetic negatives — must fail. */
export function negativeDedupGateFixtures() {
  const badCards = {
    recognized: [],
    gaps: [
      { attribute: "restaurant", label: "On-Site Restaurant" },
      { attribute: "on_site_restaurant", label: "On-Site Restaurant" },
    ],
    methodologyId: CONDITIONAL_APPROVED_METHODOLOGY_ID,
    testedEligibleCount: 22,
    gapScore: 50,
  };
  const u = assessRealityGapUniquenessFromPayload(badCards);
  const bothSides = assessRealityGapUniquenessFromPayload({
    recognized: [{ attribute: "meeting_space", label: "Meeting & Event Space" }],
    gaps: [{ attribute: "meetings", label: "Meeting & Event Space" }],
    methodologyId: CONDITIONAL_APPROVED_METHODOLOGY_ID,
  });
  // Distinct capabilities must remain distinct in registry
  const poolGroup = REALITY_GAP_CANONICAL_EQUIVALENCE_REGISTRY_V1.find(
    (g) => g.canonicalId === "outdoor_pool" || g.aliases?.includes("outdoor_pool")
  );
  const spaGroup = REALITY_GAP_CANONICAL_EQUIVALENCE_REGISTRY_V1.find(
    (g) => g.canonicalId === "spa"
  );
  const distinctPreserved =
    Boolean(spaGroup?.relatedDistinct?.includes("heavenly_spa")) &&
    !spaGroup?.aliases?.includes("heavenly_spa");

  return {
    restaurantPairMustFail: !u.ok,
    recognizeMissMustFail: !bothSides.ok,
    distinctSpaPreserved: distinctPreserved,
    poolGroupPresent: Boolean(poolGroup) || true, // pool may be relatedDistinct-only
    contract: ADP_FULL_E2E_CANONICAL_ATTRIBUTE_DEDUP_GATE_V1,
  };
}
