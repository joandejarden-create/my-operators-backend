/**
 * Brand Explorer Wave 16A — Marriott portfolio controlled-build cohort (Stage 1+).
 *
 * Source of identities: reports/brand-explorer-wave16-marriott-readiness-audit.json
 * Protected baseline remains frozen_62 until intentional freeze revision after promotion.
 *
 * Four Points Flex by Sheraton is OUTSIDE Wave 16A (PROTECTED_HOLD).
 * Marriott Conference Center + Wave 16B remain outside this cohort.
 */
export const WAVE16A_PLAN_VERSION = "wave16a-factory-plan-v1";
export const WAVE16A_VERSION = "brand-explorer-wave16a-factory-v1";

export const WAVE16A_PROTECTED_BASELINE_COUNT = 62;
export const WAVE16A_PROTECTED_BASELINE_ID =
  "frozen_62_active_public_full_baseline_quality_clean_flex_held";

export const WAVE16A_STATUS_FROM = "Under Review";

export const WAVE16A_PARENT_PLATFORM = "Marriott International, Inc.";

/** Wave 16A Stage 1 approved build-preparation cohort (11). */
export const WAVE16A_SLUGS = Object.freeze([
  "jw-marriott",
  "w-hotels",
  "st-regis",
  "luxury-collection",
  "ritz-carlton",
  "delta-hotels-by-marriott",
  "edition",
  "fairfield-by-marriott",
  "le-meridien",
  "renaissance",
  "four-points-by-sheraton",
]);

export const WAVE16A_IDENTITIES = Object.freeze({
  "jw-marriott": Object.freeze({
    slug: "jw-marriott",
    suppliedName: "JW Marriott",
    exactBrandBasicsName: "JW Marriott",
    recordId: "recj5BCSpWqMEEBDg",
    parentCompany: WAVE16A_PARENT_PLATFORM,
    readinessScore: 92,
  }),
  "w-hotels": Object.freeze({
    slug: "w-hotels",
    suppliedName: "W Hotels",
    exactBrandBasicsName: "W Hotels",
    recordId: "recqBiokz5ePxbFTE",
    parentCompany: WAVE16A_PARENT_PLATFORM,
    readinessScore: 91,
  }),
  "st-regis": Object.freeze({
    slug: "st-regis",
    suppliedName: "St. Regis",
    exactBrandBasicsName: "St. Regis",
    recordId: "recYsUrz4Po7Tg48T",
    parentCompany: WAVE16A_PARENT_PLATFORM,
    readinessScore: 90,
  }),
  "luxury-collection": Object.freeze({
    slug: "luxury-collection",
    suppliedName: "The Luxury Collection",
    exactBrandBasicsName: "Luxury Collection",
    recordId: "recpKmYbyFr8ScGTi",
    parentCompany: WAVE16A_PARENT_PLATFORM,
    readinessScore: 90,
  }),
  "ritz-carlton": Object.freeze({
    slug: "ritz-carlton",
    suppliedName: "The Ritz-Carlton",
    exactBrandBasicsName: "Ritz-Carlton",
    recordId: "recX9RJcmafZxWdXx",
    parentCompany: WAVE16A_PARENT_PLATFORM,
    readinessScore: 90,
  }),
  "delta-hotels-by-marriott": Object.freeze({
    slug: "delta-hotels-by-marriott",
    suppliedName: "Delta Hotels by Marriott",
    exactBrandBasicsName: "Delta Hotels by Marriott",
    recordId: "rec50qkCj6yt9fMPg",
    parentCompany: WAVE16A_PARENT_PLATFORM,
    readinessScore: 89,
  }),
  edition: Object.freeze({
    slug: "edition",
    suppliedName: "EDITION",
    exactBrandBasicsName: "Edition",
    recordId: "rec66ikTdLlT49Vkh",
    parentCompany: WAVE16A_PARENT_PLATFORM,
    readinessScore: 89,
  }),
  "fairfield-by-marriott": Object.freeze({
    slug: "fairfield-by-marriott",
    suppliedName: "Fairfield by Marriott",
    exactBrandBasicsName: "Fairfield by Marriott",
    recordId: "recpUTDtwt1wPMDPj",
    parentCompany: WAVE16A_PARENT_PLATFORM,
    readinessScore: 89,
  }),
  "le-meridien": Object.freeze({
    slug: "le-meridien",
    suppliedName: "Le Méridien",
    exactBrandBasicsName: "Le Meridien",
    recordId: "recN6sas0Km17O2Yn",
    parentCompany: WAVE16A_PARENT_PLATFORM,
    readinessScore: 88,
  }),
  renaissance: Object.freeze({
    slug: "renaissance",
    suppliedName: "Renaissance Hotels",
    exactBrandBasicsName: "Renaissance",
    recordId: "recmj7cNIUbYPKVJl",
    parentCompany: WAVE16A_PARENT_PLATFORM,
    readinessScore: 88,
  }),
  "four-points-by-sheraton": Object.freeze({
    slug: "four-points-by-sheraton",
    suppliedName: "Four Points by Sheraton",
    exactBrandBasicsName: "Four Points by Sheraton",
    recordId: "recH5ZF9V6ivz9p5h",
    parentCompany: WAVE16A_PARENT_PLATFORM,
    readinessScore: 86,
  }),
});

/** Protected hold — never in Wave 16A cohort. */
export const WAVE16A_FLEX_HOLD = Object.freeze({
  slug: "four-points-flex-by-sheraton",
  exactBrandBasicsName: "Four Points Flex by Sheraton",
  recordId: "recgaMzDn2GKkpUsi",
  classification: "PROTECTED_HOLD",
});

export const WAVE16A_OUTSIDE_COHORT = Object.freeze([
  "four-points-flex-by-sheraton",
  "marriott-conference-center",
  "gaylord-hotels",
  "element-by-westin",
  "protea-hotels-by-marriott",
  "bulgari",
  "citizenm",
  "the-ritz-carlton-reserve",
  "the-house-of-originals",
  "morgans-originals",
  "radisson-collection",
]);

export const WAVE16A_STAGES = Object.freeze([
  "stage1-source-foundation",
  "factory-preview-cohort",
  "tab-factory-build",
  "image-materialization",
  "post-image-content-cleanup",
  "founder-review",
  "status-promotion",
  "public-release",
]);

export function getWave16aIdentity(slug) {
  const id = WAVE16A_IDENTITIES[slug];
  if (!id) throw new Error(`Unknown Wave 16A slug: ${slug}`);
  return id;
}

/** Wave 16A Stage 2A — LOW-risk controlled tab build cohort only. */
export const WAVE16A_STAGE2A_APPROVED_SLUGS = Object.freeze([
  "fairfield-by-marriott",
  "four-points-by-sheraton",
  "delta-hotels-by-marriott",
]);

export const WAVE16A_STAGE2A_APPLY_FLAGS = Object.freeze([
  "--approve-wave16a-stage2a-controlled-tab-build",
  "--confirm-three-brand-scope",
  "--confirm-all-three-under-review",
  "--confirm-active-62-protected",
  "--confirm-no-brand-status-writes",
  "--confirm-no-release-writes",
  "--confirm-no-company-validation-writes",
  "--confirm-no-brand-verified-writes",
  "--confirm-no-census-writes",
  "--confirm-no-recent-momentum-writes",
  "--confirm-no-image-writes",
  "--confirm-no-source-library-writes",
  "--confirm-no-registry-writes",
  "--confirm-no-four-points-flex-writes",
  "--confirm-no-wave16b-writes",
  "--confirm-no-non-target-writes",
  "--confirm-presentation-only-controlled-build",
]);

/** Wave 16A Stage 2B — LOW-risk image materialization cohort (same three brands). */
export const WAVE16A_STAGE2B_APPROVED_SLUGS = Object.freeze([...WAVE16A_STAGE2A_APPROVED_SLUGS]);

/** Wave 16A LOW-risk publication cohort (Stage 2C complete → Active/Live public). */
export const WAVE16A_LOW_RISK_RELEASE_SLUGS = Object.freeze([...WAVE16A_STAGE2A_APPROVED_SLUGS]);

export const WAVE16A_STATUS_TO_PREFERRED = "Active";
export const WAVE16A_STATUS_TO_ALLOWED = Object.freeze(["Active", "Live"]);
export const WAVE16A_EXPECTED_FINAL_ACTIVE_COUNT = 65; // 62 + 3 LOW-risk

/**
 * Public-release fields for Wave 16A LOW-risk.
 * Founder Visual Review Pass is intentionally OMITTED — display-state
 * `active_profile_ready` enables full profile render without it; founder
 * will review visually in-app after publication.
 */
export const WAVE16A_RELEASE_FIELDS = Object.freeze([
  "Active Profile Approved",
  "Ready for Active Profile",
  "Active Profile Approved Date",
]);

export const WAVE16A_NEVER_WRITE_FIELDS = Object.freeze([
  "Company Validated",
  "Company Validation Date",
  "Brand Verified",
  "Source Library Status",
  "Registry Approval",
  "Registry Status",
  "Founder Visual Review Pass",
]);

export const WAVE16A_STATUS_PROMOTION_APPLY_FLAGS = Object.freeze([
  "--approve-wave16a-low-risk-status-promotion",
  "--confirm-founder-publication-approval-for-in-app-visual-review",
  "--confirm-three-brand-scope",
  "--confirm-status-to-active",
  "--confirm-active-62-before",
  "--confirm-no-four-points-flex-writes",
  "--confirm-no-remaining-wave16a-writes",
  "--confirm-no-wave16b-writes",
  "--confirm-no-company-validation-writes",
  "--confirm-no-brand-verified-writes",
  "--confirm-no-census-writes",
  "--confirm-no-recent-momentum-writes",
  "--confirm-no-content-writes",
  "--confirm-no-image-writes",
  "--confirm-no-release-field-writes",
]);

export const WAVE16A_PUBLIC_RELEASE_APPLY_FLAGS = Object.freeze([
  "--approve-wave16a-low-risk-public-release",
  "--confirm-founder-publication-approval-for-in-app-visual-review",
  "--confirm-brand-status-active",
  "--confirm-three-brand-scope",
  "--confirm-no-founder-visual-review-pass-writes",
  "--confirm-no-four-points-flex-writes",
  "--confirm-no-remaining-wave16a-writes",
  "--confirm-no-wave16b-writes",
  "--confirm-no-company-validation-writes",
  "--confirm-no-brand-verified-writes",
  "--confirm-no-census-writes",
  "--confirm-no-recent-momentum-writes",
  "--confirm-no-content-rewrites",
  "--confirm-no-image-writes",
  "--confirm-active-profile-approved-fields-only",
]);

export const WAVE16A_POST_RELEASE_READY =
  "wave16a_low_risk_3_released_active_65_quality_clean_flex_held";

export const WAVE16A_FREEZE_DECISION_65 =
  "frozen_65_active_public_full_baseline_quality_clean_flex_held";
