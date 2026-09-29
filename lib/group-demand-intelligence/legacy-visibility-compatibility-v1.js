/**
 * Temporary legacy customer-visibility compatibility for GDI readiness convergence V1.
 *
 * ONLY for opportunities that were already customer-visible before convergence
 * and still fail a non-DQ readiness gate after enrichment.
 *
 * New opportunities MUST NOT receive this stamp.
 * Sunset when legacyVisibilityPreserved count reaches 0 across control hotels.
 */

export const LEGACY_VISIBILITY_COHORT_V1 = "gdi_readiness_convergence_v1";

export function stampLegacyVisibilityPreserved(opp = {}, meta = {}) {
  return {
    ...opp,
    legacyVisibilityPreserved: true,
    legacyVisibilityCohort: LEGACY_VISIBILITY_COHORT_V1,
    legacyVisibilityReason: meta.reason || "post_enrich_not_ready",
    legacyVisibilityBlockers: meta.blockers || [],
    legacyVisibilityStampedAt: new Date().toISOString(),
  };
}

export function clearLegacyVisibilityPreserved(opp = {}) {
  if (!opp || typeof opp !== "object") return opp;
  if (opp.legacyVisibilityPreserved !== true) return opp;
  const next = { ...opp };
  delete next.legacyVisibilityPreserved;
  delete next.legacyVisibilityCohort;
  delete next.legacyVisibilityReason;
  delete next.legacyVisibilityBlockers;
  delete next.legacyVisibilityStampedAt;
  return next;
}

/**
 * True only for explicit temporary cohort stamp (never for new opportunities).
 */
export function isLegacyVisibilityCompatible(opp = {}) {
  return (
    opp?.legacyVisibilityPreserved === true &&
    opp?.legacyVisibilityCohort === LEGACY_VISIBILITY_COHORT_V1
  );
}

export function legacyCompatibilitySunsetCondition() {
  return {
    cohort: LEGACY_VISIBILITY_COHORT_V1,
    removeWhen:
      "legacyVisibilityPreserved count is 0 for Bethesda+Renaissance+Waterstone after enrichment/revalidation",
    neverApplyTo: "opportunities created after convergence (no stamp on create/enrich paths)",
  };
}
