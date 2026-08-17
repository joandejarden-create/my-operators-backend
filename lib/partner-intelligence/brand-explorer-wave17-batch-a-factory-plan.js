/**
 * Brand Explorer Wave 17 Batch A — Hyatt LOW-risk controlled tab build cohort.
 *
 * Targets: Hyatt Regency · Hyatt Centric · Thompson Hotels
 * Out of scope: Caption, Destination, Unbound, Dream Hotels, Active 65.
 */
export const WAVE17_BATCH_A_PLAN_VERSION = "wave17-batch-a-factory-plan-v1";
export const WAVE17_BATCH_A_VERSION = "brand-explorer-wave17-batch-a-v1";

export const WAVE17_PROTECTED_ACTIVE_COUNT = 65;

export const WAVE17_PARENT_PLATFORM = "Hyatt Hotels Corporation";
export const WAVE17_LOYALTY = "World of Hyatt";

export const WAVE17_BATCH_A_IDENTITIES = Object.freeze({
  "hyatt-regency": Object.freeze({
    slug: "hyatt-regency",
    suppliedName: "Hyatt Regency",
    exactBrandBasicsName: "Hyatt Regency",
    recordId: "recP9SqDootMrzaU1",
    parentCompany: WAVE17_PARENT_PLATFORM,
    readinessScore: 91,
    buildRisk: "LOW",
  }),
  "hyatt-centric": Object.freeze({
    slug: "hyatt-centric",
    suppliedName: "Hyatt Centric",
    exactBrandBasicsName: "Hyatt Centric",
    recordId: "recNy2efMm4N1JtgC",
    parentCompany: WAVE17_PARENT_PLATFORM,
    readinessScore: 90,
    buildRisk: "LOW",
  }),
  "thompson-hotels": Object.freeze({
    slug: "thompson-hotels",
    suppliedName: "Thompson Hotels",
    exactBrandBasicsName: "Thompson Hotels",
    recordId: "rec4Mga6ejz3L1M3P",
    parentCompany: WAVE17_PARENT_PLATFORM,
    readinessScore: 90,
    buildRisk: "LOW",
  }),
});

/** Sequential build order (required). */
export const WAVE17_BATCH_A_APPROVED_SLUGS = Object.freeze([
  "hyatt-regency",
  "hyatt-centric",
  "thompson-hotels",
]);

export const WAVE17_BATCH_A_OUT_OF_SCOPE = Object.freeze({
  "caption-by-hyatt": Object.freeze({
    slug: "caption-by-hyatt",
    recordId: "recSgIQ0bhRpPXZbU",
    name: "Caption by Hyatt",
  }),
  "destination-by-hyatt": Object.freeze({
    slug: "destination-by-hyatt",
    recordId: "recUWTcF6fEFcWhMQ",
    name: "Destination by Hyatt",
  }),
  "unbound-collection-by-hyatt": Object.freeze({
    slug: "unbound-collection-by-hyatt",
    recordId: "recDQwcMFontK2CSP",
    name: "Unbound Collection by Hyatt",
  }),
  "dream-hotels": Object.freeze({
    slug: "dream-hotels",
    recordId: "recdOAwTjgtuQ13WP",
    name: "Dream Hotels",
  }),
});

export const WAVE17_BATCH_A_APPLY_FLAGS = Object.freeze([
  "--approve-wave17-batch-a-controlled-tab-build",
  "--confirm-three-brand-scope",
  "--confirm-all-three-under-review",
  "--confirm-active-65-protected",
  "--confirm-no-brand-status-writes",
  "--confirm-no-release-writes",
  "--confirm-no-company-validation-writes",
  "--confirm-no-brand-verified-writes",
  "--confirm-no-census-writes",
  "--confirm-no-recent-momentum-writes",
  "--confirm-no-image-writes",
  "--confirm-no-source-library-writes",
  "--confirm-no-registry-writes",
  "--confirm-no-batch-b-writes",
  "--confirm-no-dream-hotels-writes",
  "--confirm-no-non-target-writes",
  "--confirm-presentation-only-controlled-build",
]);

export function getWave17BatchAIdentity(slug) {
  const id = WAVE17_BATCH_A_IDENTITIES[slug];
  if (!id) throw new Error(`Unknown Wave 17 Batch A slug: ${slug}`);
  return id;
}
