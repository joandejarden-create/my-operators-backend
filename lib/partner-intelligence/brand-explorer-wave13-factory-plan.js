/**
 * Brand Explorer Wave 13 — factory cohort constants + stage contracts.
 *
 * 8 Accor / Accor-adjacent brands destined for factory preview → Active/Live
 * public-full after gates + founder approval. Must NOT join Active/Live until
 * status promotion.
 *
 * Protected baseline remains 39 (`39-active-public-full-baseline-v1`) until an
 * intentional freeze revision after Wave 13 promotion.
 */
export const WAVE13_PLAN_VERSION = "wave13-factory-plan-v1";
export const WAVE13_VERSION = "brand-explorer-wave13-factory-v1";

export const WAVE13_PROTECTED_BASELINE_COUNT = 39;
export const WAVE13_EXPECTED_FINAL_ACTIVE_COUNT = 47; // 39 + 8 if all promote

export const WAVE13_STAGES = Object.freeze([
  "preflight",
  "manifest",
  "factory-preview-cohort",
  "source-packs",
  "open-items-resolution",
  "tab-factory-build",
  "stage4-content-cleanup",
  "image-materialization",
  "post-image-content-cleanup",
  "founder-review",
  "status-promotion",
  "public-release",
  "value-scenario-pattern-cleanup",
  "public-six-geo-momentum-cleanup",
  "so-hold-remediation",
  "so-status-promotion",
  "so-public-release",
  "so-section-pattern-cleanup",
]);

/** Six founder-approved brands for partial Stage 9–10 (SO/ held). */
export const WAVE13_PARTIAL_PROMOTION_SLUGS = Object.freeze([
  "mama-shelter",
  "mercure",
  "ibis",
  "novotel",
  "pullman",
  "fairmont-hotels-and-resorts",
]);

export const WAVE13_HELD_PROMOTION_SLUG = "so-hotels-and-resorts";

/** 39 protected + 6 partial Wave 13 promotions. */
export const WAVE13_EXPECTED_PARTIAL_ACTIVE_COUNT = 45;
/** After SO/ Stage 9 status promotion (Under Review → Active). */
export const WAVE13_EXPECTED_SO_ACTIVE_COUNT = 46;

export const WAVE13_STATUS_FROM = "Under Review";
export const WAVE13_STATUS_TO_PREFERRED = "Active";
export const WAVE13_STATUS_TO_ALLOWED = Object.freeze(["Active", "Live"]);

export const WAVE13_FOUNDER_APPROVE_RECOMMENDATION =
  "approve_for_status_promotion_and_public_release";
export const WAVE13_FOUNDER_HOLD_RECOMMENDATION = "approve_after_minor_cleanup";

export const WAVE13_RELEASE_FIELDS = Object.freeze([
  "Active Profile Approved",
  "Ready for Active Profile",
  "Active Profile Approved Date",
  "Founder Visual Review Pass",
]);

export const WAVE13_NEVER_WRITE_FIELDS = Object.freeze([
  "Company Validated",
  "Company Validation Date",
  "Source Library status",
  "Registry approval/status",
  "Source Library Status",
  "Registry Approval",
  "Registry Status",
]);

export const WAVE13_STATUS_PROMOTION_APPLY_FLAGS = Object.freeze([
  "--approve-wave13-partial-brand-status-promotion",
  "--confirm-founder-approval-for-six-only",
  "--confirm-so-held",
  "--confirm-target-brands-only",
  "--confirm-status-to-active",
  "--confirm-no-so-status-change",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-content-writes",
  "--confirm-no-image-writes",
  "--confirm-no-release-field-writes",
  "--confirm-no-protected-39-brand-changes",
  "--confirm-no-house-of-originals-writes",
  "--confirm-no-morgans-originals-writes",
  "--confirm-no-radisson-collection-changes",
]);

export const WAVE13_SO_STATUS_PROMOTION_APPLY_FLAGS = Object.freeze([
  "--approve-so-brand-status-promotion",
  "--confirm-founder-accepts-cleanly-unavailable-steward-posture",
  "--confirm-so-only",
  "--confirm-status-to-active",
  "--confirm-no-content-writes",
  "--confirm-no-image-writes",
  "--confirm-no-release-field-writes",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-active-45-writes",
  "--confirm-no-house-of-originals-writes",
  "--confirm-no-morgans-originals-writes",
  "--confirm-no-radisson-collection-changes",
]);

export const WAVE13_SO_PUBLIC_RELEASE_APPLY_FLAGS = Object.freeze([
  "--approve-so-public-release",
  "--confirm-founder-visual-review-passed",
  "--confirm-founder-accepts-cleanly-unavailable-steward-posture",
  "--confirm-so-brand-status-active",
  "--confirm-so-only",
  "--confirm-fully-ready",
  "--confirm-public-visibility-quality-lock-passed",
  "--confirm-no-content-rewrites",
  "--confirm-no-image-writes",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-active-45-writes",
  "--confirm-no-house-of-originals-writes",
  "--confirm-no-morgans-originals-writes",
  "--confirm-no-radisson-collection-changes",
]);

export const WAVE13_SO_SECTION_PATTERN_CLEANUP_APPLY_FLAGS = Object.freeze([
  "--approve-so-section-pattern-cleanup",
  "--confirm-so-only",
  "--confirm-targeted-section-pattern-fixes-only",
  "--confirm-no-brand-status-changes",
  "--confirm-no-release-field-writes",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-public-restore-registry-changes",
  "--confirm-no-image-writes",
  "--confirm-no-active-45-writes",
  "--confirm-no-house-of-originals-writes",
  "--confirm-no-morgans-originals-writes",
  "--confirm-no-radisson-collection-changes",
  "--confirm-no-wave14-work",
  "--confirm-no-broad-rewrites",
  "--confirm-no-adr",
  "--confirm-no-revpar",
  "--confirm-no-fee-stack",
  "--confirm-no-raw-urls",
  "--confirm-recent-momentum-structured",
  "--confirm-geo-footprint-source-supported-or-cleanly-suppressed",
  "--confirm-growth-priorities-brand-specific",
]);

export const WAVE13_PUBLIC_RELEASE_APPLY_FLAGS = Object.freeze([
  "--approve-wave13-partial-public-release",
  "--confirm-founder-visual-review-passed-for-six-only",
  "--confirm-so-held",
  "--confirm-brand-status-active",
  "--confirm-fully-ready",
  "--confirm-public-visibility-quality-lock-passed",
  "--confirm-target-brands-only",
  "--confirm-no-so-release-field-writes",
  "--confirm-no-so-status-change",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-content-rewrites",
  "--confirm-no-image-writes",
  "--confirm-no-protected-39-brand-changes",
  "--confirm-no-house-of-originals-writes",
  "--confirm-no-morgans-originals-writes",
  "--confirm-no-radisson-collection-changes",
]);

export const WAVE13_VALUE_SCENARIO_PATTERN_CLEANUP_APPLY_FLAGS = Object.freeze([
  "--approve-wave13-value-scenario-pattern-cleanup",
  "--confirm-wave13-target-scope-only",
  "--confirm-so-held-status-unchanged",
  "--confirm-targeted-value-scenario-fixes-only",
  "--confirm-image-writes-only-if-scenario-flagged",
  "--confirm-no-brand-status-changes",
  "--confirm-no-release-field-writes",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-protected-39-writes",
  "--confirm-no-house-of-originals-writes",
  "--confirm-no-morgans-originals-writes",
  "--confirm-no-radisson-collection-changes",
  "--confirm-no-broad-rewrites",
  "--confirm-no-internal-process-language",
  "--confirm-no-adr",
  "--confirm-no-revpar",
  "--confirm-no-fee-stack",
  "--confirm-no-raw-urls",
]);

export const WAVE13_PUBLIC_SIX_GEO_MOMENTUM_CLEANUP_APPLY_FLAGS = Object.freeze([
  "--approve-wave13-public-six-geo-momentum-cleanup",
  "--confirm-six-public-brand-scope",
  "--confirm-target-brands-only",
  "--confirm-so-held-and-untouched",
  "--confirm-house-of-originals-excluded",
  "--confirm-no-morgans-originals-writes",
  "--confirm-no-radisson-collection-changes",
  "--confirm-no-protected-39-content-writes",
  "--confirm-targeted-geo-and-momentum-fixes-only",
  "--confirm-no-brand-status-changes",
  "--confirm-no-release-field-writes",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-image-writes",
  "--confirm-no-broad-rewrites",
  "--confirm-no-adr",
  "--confirm-no-revpar",
  "--confirm-no-fee-stack",
  "--confirm-no-raw-urls",
  "--confirm-recent-momentum-structured",
  "--confirm-geo-footprint-source-supported",
]);

export const WAVE13_SO_HOLD_REMEDIATION_APPLY_FLAGS = Object.freeze([
  "--approve-wave13-so-hold-remediation",
  "--confirm-so-only",
  "--confirm-so-remains-under-review",
  "--confirm-no-brand-status-changes",
  "--confirm-no-release-field-writes",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-public-restore-registry-changes",
  "--confirm-no-active-45-writes",
  "--confirm-no-house-of-originals-writes",
  "--confirm-no-morgans-originals-writes",
  "--confirm-no-radisson-collection-changes",
  "--confirm-no-broad-rewrites",
  "--confirm-no-adr",
  "--confirm-no-revpar",
  "--confirm-no-fee-stack",
  "--confirm-no-raw-urls",
  "--confirm-steward-fields-source-supported-or-left-cleanly-unavailable",
]);

export const WAVE13_POST_IMAGE_CONTENT_CLEANUP_APPLY_FLAGS = Object.freeze([
  "--approve-wave13-post-image-content-cleanup",
  "--confirm-seven-brand-stage6-scope",
  "--confirm-target-brands-only",
  "--confirm-targeted-field-fixes-only",
  "--confirm-so-live-basics-record-recTJdPlr4mDs9app",
  "--confirm-so-steward-data-not-invented",
  "--confirm-fairmont-san-francisco-do-not-display-only",
  "--confirm-protected-39-live-pvql-regreen",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-brand-status-changes",
  "--confirm-no-release-field-writes",
  "--confirm-no-image-writes",
  "--confirm-no-house-of-originals-writes",
  "--confirm-no-morgans-originals-writes",
  "--confirm-no-radisson-collection-changes",
  "--confirm-no-broad-rewrites",
  "--confirm-no-adr",
  "--confirm-no-revpar",
  "--confirm-no-fee-stack",
  "--confirm-no-raw-urls",
]);

export const WAVE13_IMAGE_MATERIALIZATION_APPLY_FLAGS = Object.freeze([
  "--approve-wave13-image-materialization",
  "--confirm-seven-brand-stage5-scope",
  "--confirm-target-brands-only",
  "--confirm-house-of-originals-excluded",
  "--confirm-no-morgans-originals-writes",
  "--confirm-no-radisson-collection-changes",
  "--confirm-no-protected-39-brand-changes",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-brand-status-changes",
  "--confirm-no-release-field-writes",
  "--confirm-image-uniqueness",
  "--confirm-image-role-match",
  "--confirm-scene7-filename-aware-distinct-images",
  "--confirm-cala-first-openings-priority",
  "--confirm-international-reference-labels-where-needed",
  "--confirm-no-logo-only-filler",
  "--confirm-no-wrong-brand-images",
  "--confirm-no-sibling-brand-images",
  "--confirm-no-content-rewrites",
  "--confirm-no-so-steward-data-fills",
]);

export const WAVE13_STAGE4_APPROVED_SLUGS = Object.freeze([
  "mama-shelter",
  "mercure",
  "ibis",
  "novotel",
  "pullman",
  "so-hotels-and-resorts",
  "fairmont-hotels-and-resorts",
]);

export const WAVE13_TAB_FACTORY_BUILD_APPLY_FLAGS = Object.freeze([
  "--approve-wave13-tab-factory-build",
  "--confirm-seven-brand-stage4-scope",
  "--confirm-target-brands-only",
  "--confirm-source-pack-grounded",
  "--confirm-so-basics-record-present",
  "--confirm-house-of-originals-excluded",
  "--confirm-no-morgans-originals-writes",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-brand-status-changes",
  "--confirm-no-release-field-writes",
  "--confirm-no-protected-39-brand-changes",
  "--confirm-no-radisson-collection-changes",
  "--confirm-no-image-writes",
  "--confirm-no-broad-rewrites",
  "--confirm-no-adr",
  "--confirm-no-revpar",
  "--confirm-no-fee-stack",
  "--confirm-target-guest-segments-validated",
]);

export const WAVE13_STAGE4_CONTENT_CLEANUP_APPLY_FLAGS = Object.freeze([
  "--approve-wave13-stage4-content-cleanup",
  "--confirm-seven-brand-stage4-scope",
  "--confirm-target-brands-only",
  "--confirm-targeted-field-fixes-only",
  "--confirm-no-image-writes",
  "--confirm-no-brand-status-changes",
  "--confirm-no-release-field-writes",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-protected-39-brand-changes",
  "--confirm-house-of-originals-excluded",
  "--confirm-no-morgans-originals-writes",
  "--confirm-no-radisson-collection-changes",
  "--confirm-no-broad-rewrites",
  "--confirm-no-adr",
  "--confirm-no-revpar",
  "--confirm-no-fee-stack",
  "--confirm-no-raw-urls",
]);

export const WAVE13_OPEN_ITEMS_APPLY_FLAGS = Object.freeze([
  "--approve-so-brand-basics-creation",
  "--confirm-so-under-review-only",
  "--confirm-no-active-live-status",
  "--confirm-no-release-field-writes",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-presentation-writes",
  "--confirm-no-image-writes",
  "--confirm-no-protected-39-brand-changes",
  "--confirm-no-morgans-originals-record-changes",
]);

/** Central Brand Basics field map for Wave 13 open-items writes. */
export const WAVE13_BASICS_FIELD_MAP = Object.freeze({
  brandName: "Brand Name",
  brandStatus: "Brand Status",
  parentCompany: "Parent Company",
  internalNotes: "Internal Notes",
  // Forbidden — never write in open-items-resolution:
  companyValidated: "Company Validated",
  companyValidationDate: "Company Validation Date",
  validationStatus: "Validation Status",
  activeProfileApproved: "Active Profile Approved",
  readyForActiveProfile: "Ready for Active Profile",
  activeProfileApprovedDate: "Active Profile Approved Date",
  founderVisualReviewPass: "Founder Visual Review Pass",
  sourceLibrary: "Partner Intelligence - Source Library",
  assetRegistry: "Partner Intelligence - Brand Asset Registry",
});

export const WAVE13_SO_BASICS_CREATE_PLAN = Object.freeze({
  slug: "so-hotels-and-resorts",
  brandName: "SO/",
  displayAlias: "SO/ Hotels & Resorts",
  brandStatus: "Under Review",
  parentCompany: "AccorHotels",
  allowedFields: Object.freeze([
    "Brand Name",
    "Brand Status",
    "Parent Company",
    "Internal Notes",
  ]),
  internalNotes:
    "Created for Wave 13 factory preview; not public; not Active/Live; not company validated. Display alias (no dedicated Airtable field): SO/ Hotels & Resorts. Slug is code-side: so-hotels-and-resorts (Brand Basics has no Slug field).",
});

/** Canonical Wave 13 target slugs. */
export const WAVE13_SLUGS = Object.freeze([
  "mama-shelter",
  "mercure",
  "ibis",
  "novotel",
  "pullman",
  "so-hotels-and-resorts",
  "fairmont-hotels-and-resorts",
  "the-house-of-originals",
]);

/**
 * Planned identities. recordId filled by manifest discovery.
 * Names / aliases must be checked against Brand Basics — do not invent Airtable options.
 */
export const WAVE13_BRAND_PLAN = Object.freeze({
  "mama-shelter": Object.freeze({
    slug: "mama-shelter",
    name: "Mama Shelter",
    parentPlatform: "Accor",
    family: "accor",
    recommendedStatusWhileInFactory: "Under Review",
    nameAliases: ["MamaShelter", "Mama Shelter Hotels"],
    aliasSearchTokens: ["mama shelter", "mamashelter"],
    lens: "Accor lifestyle / urban boutique; distinguish from SO/ and Handwritten.",
  }),
  mercure: Object.freeze({
    slug: "mercure",
    name: "Mercure",
    parentPlatform: "Accor",
    family: "accor",
    recommendedStatusWhileInFactory: "Under Review",
    nameAliases: ["Mercure Hotels"],
    aliasSearchTokens: ["mercure"],
    siblingAliasNotes: ["Grand Mercure is a sibling Accor line — report only, not a Mercure duplicate"],
    lens: "Accor midscale/upper-midscale; distinguish from Novotel, ibis, Pullman.",
  }),
  ibis: Object.freeze({
    slug: "ibis",
    name: "ibis",
    parentPlatform: "Accor",
    family: "accor",
    recommendedStatusWhileInFactory: "Under Review",
    nameAliases: ["Ibis", "IBIS"],
    aliasSearchTokens: ["ibis"],
    lens: "Accor economy / midscale core; distinguish from ibis Styles / ibis budget siblings.",
  }),
  novotel: Object.freeze({
    slug: "novotel",
    name: "Novotel",
    parentPlatform: "Accor",
    family: "accor",
    recommendedStatusWhileInFactory: "Under Review",
    nameAliases: ["Novotel Hotels"],
    aliasSearchTokens: ["novotel"],
    lens: "Accor midscale/upper-midscale business + leisure; distinguish from Mercure and Pullman.",
  }),
  pullman: Object.freeze({
    slug: "pullman",
    name: "Pullman",
    parentPlatform: "Accor",
    family: "accor",
    recommendedStatusWhileInFactory: "Under Review",
    nameAliases: ["Pullman Hotels", "Pullman Hotels & Resorts"],
    aliasSearchTokens: ["pullman"],
    lens: "Accor upscale lifestyle / meetings; distinguish from Novotel, Fairmont, SO/.",
  }),
  "so-hotels-and-resorts": Object.freeze({
    slug: "so-hotels-and-resorts",
    name: "SO/",
    parentPlatform: "Accor",
    family: "accor",
    recommendedStatusWhileInFactory: "Under Review",
    nameAliases: [
      "SO/",
      "SO Hotels",
      "SO/ Hotels",
      "SO/ Hotels & Resorts",
      "SO Hotels & Resorts",
      "Sofitel SO",
    ],
    aliasSearchTokens: ["so/ hotels", "so hotels", "so/"],
    lens: "Accor design lifestyle soft brand; distinguish from Mama Shelter and Sofitel.",
  }),
  "fairmont-hotels-and-resorts": Object.freeze({
    slug: "fairmont-hotels-and-resorts",
    name: "Fairmont Hotels & Resorts",
    parentPlatform: "Accor",
    family: "accor",
    recommendedStatusWhileInFactory: "Under Review",
    nameAliases: ["Fairmont", "Fairmont Hotels and Resorts", "Fairmont Hotels"],
    aliasSearchTokens: ["fairmont"],
    lens: "Accor luxury / icon brand; distinguish from Pullman and Raffles.",
  }),
  "the-house-of-originals": Object.freeze({
    slug: "the-house-of-originals",
    name: "The House of Originals",
    parentPlatform: "Accor",
    family: "accor",
    recommendedStatusWhileInFactory: "Under Review",
    nameAliases: ["House of Originals", "The House Of Originals"],
    aliasSearchTokens: ["house of originals"],
    lens: "Accor soft / originals collection; distinguish from Mama Shelter and SO/.",
  }),
});

export const WAVE13_MANIFEST_CLASSIFICATIONS = Object.freeze([
  "existing_ready_for_audit",
  "existing_needs_factory_build",
  "existing_needs_status_correction",
  "missing_brand_basics_record",
  "duplicate_or_slug_conflict",
  "blocked_requires_manual_review",
]);

export const WAVE13_FACTORY_PREVIEW_APPLY_FLAGS = Object.freeze([
  "--approve-factory-preview-cohort",
  "--confirm-no-airtable-writes",
  "--confirm-no-brand-status-changes",
  "--confirm-no-release-field-changes",
  "--confirm-no-protected-baseline-changes",
]);

export function getWave13Plan(slug) {
  const plan = WAVE13_BRAND_PLAN[slug];
  if (!plan) throw new Error(`Unknown Wave 13 slug: ${slug}`);
  return plan;
}

