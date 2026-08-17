/**
 * v41 — v40C Presentation patch safety classifier (read-only; does not apply).
 */
import {
  scanForbiddenLanguage,
  scanMechanicalCopy,
  detectRepeatedBoilerplate,
  isVagueAfterScrub,
} from "./brand-explorer-v40b-copy-quality-patterns.js";
import { buildV40CApplyDesign } from "./brand-explorer-economics-chrome-remediation.js";
import { PRIMARY_RELEASE_SLUGS } from "./brand-explorer-os-state-machine.js";

function nz(v) {
  return v == null ? "" : String(v).trim();
}

export const PATCH_SAFETY_CLASSES = Object.freeze([
  "safe_generic_rewrite",
  "mechanical_rewrite_risk",
  "brand_specific_rewrite_needed",
  "row_should_be_hidden",
  "founder_review_required_before_apply",
  "reject",
]);

/**
 * Classify one residual patch.
 */
export function classifyV40cPatch(patch = {}, { brandSlug = "" } = {}) {
  const before = nz(patch.before);
  const after = nz(patch.after);
  const reasons = [];

  if (!after) {
    return {
      ...patch,
      safetyClass: "row_should_be_hidden",
      reasons: ["empty_after"],
      applyAllowed: false,
    };
  }

  const forbiddenAfter = scanForbiddenLanguage(after);
  if (forbiddenAfter.length) {
    return {
      ...patch,
      safetyClass: "reject",
      reasons: [`forbidden_remain:${forbiddenAfter.map((h) => h.id).join(",")}`],
      applyAllowed: false,
    };
  }

  const mechanical = scanMechanicalCopy(after).filter((h) => ["high", "medium"].includes(h.severity));
  if (mechanical.some((h) => h.severity === "high")) {
    return {
      ...patch,
      safetyClass: "reject",
      reasons: mechanical.map((h) => h.id),
      applyAllowed: false,
    };
  }

  if (patch.safeForGenericApply === false) {
    return {
      ...patch,
      safetyClass: "brand_specific_rewrite_needed",
      reasons: ["marked_unsafe_for_generic_apply"],
      applyAllowed: false,
    };
  }

  if (isVagueAfterScrub(after) && nz(patch.field) === "Body") {
    reasons.push("vague_body_after_scrub");
    return {
      ...patch,
      safetyClass: "mechanical_rewrite_risk",
      reasons,
      applyAllowed: false,
    };
  }

  if (mechanical.length) {
    reasons.push(...mechanical.map((h) => h.id));
    return {
      ...patch,
      safetyClass: "mechanical_rewrite_risk",
      reasons,
      applyAllowed: true, // medium risk allowed with review note
      reviewNote: "Mechanical phrasing present but owner-safe; spot-check after apply.",
    };
  }

  if (before.length > 40 && after.length < before.length * 0.35) {
    return {
      ...patch,
      safetyClass: "founder_review_required_before_apply",
      reasons: ["aggressive_shortening"],
      applyAllowed: false,
    };
  }

  return {
    ...patch,
    safetyClass: "safe_generic_rewrite",
    reasons: reasons.length ? reasons : ["owner_safe_rewrite"],
    applyAllowed: true,
  };
}

/**
 * Ingest residual plans (or v40C report brandResults) and classify.
 */
export function evaluateV40cPatchSafety({ brandResults = [], brands = PRIMARY_RELEASE_SLUGS } = {}) {
  const byBrand = [];
  const allClassified = [];

  for (const brandSlug of brands) {
    const br = brandResults.find((b) => b.brandSlug === brandSlug);
    const patches = br?.residualPlan?.patches || br?.residualPresentation?.patches || [];
    const classified = patches.map((p) => classifyV40cPatch(p, { brandSlug }));
    allClassified.push(...classified.map((c) => ({ ...c, brandSlug })));

    const afterTexts = classified.map((c) => c.after);
    const repeated = detectRepeatedBoilerplate(afterTexts);

    const safe = classified.filter((c) => c.safetyClass === "safe_generic_rewrite");
    const risky = classified.filter((c) =>
      ["mechanical_rewrite_risk", "brand_specific_rewrite_needed", "founder_review_required_before_apply"].includes(
        c.safetyClass
      )
    );
    const rejected = classified.filter((c) => c.safetyClass === "reject" || c.safetyClass === "row_should_be_hidden");
    const applyAllowed =
      classified.length > 0 &&
      rejected.length === 0 &&
      classified.every((c) => c.applyAllowed !== false || c.safetyClass === "mechanical_rewrite_risk") &&
      repeated.length === 0;

    // Allow apply if only safe + medium mechanical (applyAllowed true), no rejects, no repeated boilerplate
    const applyAllowedFinal =
      classified.length > 0 &&
      rejected.filter((c) => c.safetyClass === "reject").length === 0 &&
      classified.filter((c) => c.applyAllowed === false).length === 0 &&
      repeated.length < 2;

    byBrand.push({
      brandSlug,
      totalPatches: classified.length,
      safePatches: safe.length,
      riskyPatches: risky.length,
      rejectedPatches: rejected.length,
      repeatedBoilerplate: repeated,
      applyAllowed: applyAllowedFinal,
      examples: classified.slice(0, 8).map((c) => ({
        slotKey: c.slotKey,
        field: c.field,
        safetyClass: c.safetyClass,
        before: (c.before || "").slice(0, 140),
        after: (c.after || "").slice(0, 140),
        reasons: c.reasons,
      })),
    });
  }

  const overallAllowed = byBrand.length > 0 && byBrand.every((b) => b.applyAllowed);
  return {
    version: "v41-v40c-patch-safety",
    brands,
    byBrand,
    totals: {
      totalPatches: allClassified.length,
      safePatches: allClassified.filter((c) => c.safetyClass === "safe_generic_rewrite").length,
      riskyPatches: allClassified.filter((c) => c.safetyClass === "mechanical_rewrite_risk").length,
      rejectedPatches: allClassified.filter((c) => c.safetyClass === "reject").length,
    },
    v40cApplyAllowed: overallAllowed,
    exactApplyCommand: overallAllowed ? buildV40CApplyDesign(brands).command : null,
    blockReasons: overallAllowed
      ? []
      : byBrand
          .filter((b) => !b.applyAllowed)
          .map((b) => `${b.brandSlug}: rejected=${b.rejectedPatches} risky=${b.riskyPatches} repeated=${b.repeatedBoilerplate.length}`),
  };
}
