/**
 * Brand Explorer Wave 14 — Marriott brand factory cohort + stage contracts.
 *
 * 9 Marriott International brands for factory preview → later Active/Live
 * public-full after gates + founder approval. Must NOT join Active/Live until
 * status promotion.
 *
 * Protected baseline remains 46 (`46-active-public-full-baseline-v1`) until an
 * intentional freeze revision after Wave 14 promotion.
 */
export const WAVE14_PLAN_VERSION = "wave14-factory-plan-v1";
export const WAVE14_VERSION = "brand-explorer-wave14-factory-v1";

export const WAVE14_PROTECTED_BASELINE_COUNT = 46;
export const WAVE14_EXPECTED_FINAL_ACTIVE_COUNT = 55; // 46 + 9 if all promote

/** Eight founder-approved brands for partial Stage 9–10 (Flex held). */
export const WAVE14_PARTIAL_PROMOTION_SLUGS = Object.freeze([
  "marriott-hotels",
  "sheraton",
  "westin",
  "residence-inn-by-marriott",
  "springhill-suites-by-marriott",
  "towneplace-suites-by-marriott",
  "aloft-hotels",
  "studiores",
]);

export const WAVE14_HELD_PROMOTION_SLUG = "four-points-flex-by-sheraton";

/** 46 protected + 8 Wave 14 partial (Flex held). */
export const WAVE14_EXPECTED_PARTIAL_ACTIVE_COUNT = 54;

export const WAVE14_STATUS_FROM = "Under Review";
export const WAVE14_STATUS_TO_PREFERRED = "Active";
export const WAVE14_STATUS_TO_ALLOWED = Object.freeze(["Active", "Live"]);

export const WAVE14_FOUNDER_APPROVE_RECOMMENDATION =
  "approve_for_status_promotion_and_public_release";
export const WAVE14_FOUNDER_HOLD_RECOMMENDATION = "approve_after_minor_cleanup";

export const WAVE14_STATUS_PROMOTION_APPLY_FLAGS = Object.freeze([
  "--approve-wave14-partial-brand-status-promotion",
  "--confirm-founder-approval-for-eight-only",
  "--confirm-four-points-flex-held",
  "--confirm-target-brands-only",
  "--confirm-status-to-active",
  "--confirm-no-flex-status-change",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-content-writes",
  "--confirm-no-image-writes",
  "--confirm-no-release-field-writes",
  "--confirm-no-protected-46-brand-changes",
  "--confirm-no-house-of-originals-writes",
  "--confirm-no-morgans-originals-writes",
  "--confirm-no-radisson-collection-changes",
]);

export const WAVE14_PUBLIC_RELEASE_APPLY_FLAGS = Object.freeze([
  "--approve-wave14-partial-public-release",
  "--confirm-founder-visual-review-passed-for-eight-only",
  "--confirm-four-points-flex-held",
  "--confirm-brand-status-active",
  "--confirm-fully-ready",
  "--confirm-public-visibility-quality-lock-passed",
  "--confirm-target-brands-only",
  "--confirm-no-flex-release-field-writes",
  "--confirm-no-flex-status-change",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-content-rewrites",
  "--confirm-no-image-writes",
  "--confirm-no-protected-46-brand-changes",
  "--confirm-no-house-of-originals-writes",
  "--confirm-no-morgans-originals-writes",
  "--confirm-no-radisson-collection-changes",
]);

export const WAVE14_STAGES = Object.freeze([
  "preflight",
  "manifest",
  "factory-preview-cohort",
  "source-packs",
  "tab-factory-build",
  "image-materialization",
  "post-image-content-cleanup",
  "founder-review",
  "status-promotion",
  "public-release",
  "dated-momentum-cleanup",
  "value-scenario-visual-remediation",
  "founder-visual-semantic-remediation",
  "public-active-semantic-blocker-cleanup",
]);

export const WAVE14_PUBLIC_ACTIVE_SEMANTIC_BLOCKER_CLEANUP_APPLY_FLAGS = Object.freeze([
  "--approve-wave14-public-active-semantic-blocker-cleanup",
  "--confirm-eight-active-wave14-scope",
  "--confirm-targeted-visible-copy-only",
  "--confirm-pvql-failures-extracted",
  "--confirm-24tab-failures-extracted",
  "--confirm-no-four-points-flex-writes",
  "--confirm-no-non-wave14-active-brand-writes",
  "--confirm-no-brand-status-changes",
  "--confirm-no-release-field-writes",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-image-writes",
  "--confirm-no-broad-rewrites",
  "--confirm-no-internal-source-language",
  "--confirm-no-placeholder-property-titles",
  "--confirm-recent-momentum-semantics-preserved",
  "--confirm-portfolio-mix-structured",
  "--confirm-no-gate-weakening",
]);

export const WAVE14_FOUNDER_VISUAL_SEMANTIC_REMEDIATION_APPLY_FLAGS = Object.freeze([
  "--approve-wave14-founder-visual-semantic-remediation",
  "--confirm-eight-active-wave14-scope",
  "--confirm-targeted-sections-only",
  "--confirm-recent-momentum-opening-announcement_semantics",
  "--confirm-portfolio-mix-structured",
  "--confirm-property-cards-use-actual-hotel-names",
  "--confirm-no-four-points-flex-writes",
  "--confirm-no-brand-status-changes",
  "--confirm-no-release-field-writes",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-broad-rewrites",
  "--confirm-no-internal-source-language",
  "--confirm-no-raw-urls",
  "--confirm-no-placeholder-property-titles",
  "--confirm-no-directory-only-momentum",
  "--confirm-no-gate-weakening",
]);

export const WAVE14_VALUE_SCENARIO_VISUAL_REMEDIATION_APPLY_FLAGS = Object.freeze([
  "--approve-wave14-value-scenario-visual-remediation",
  "--confirm-eight-active-wave14-scope",
  "--confirm-targeted-value-scenario-only",
  "--confirm-no-four-points-flex-writes",
  "--confirm-no-brand-status-changes",
  "--confirm-no-release-field-writes",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-recent-momentum-writes",
  "--confirm-no-geo-footprint-writes",
  "--confirm-no-broad-rewrites",
  "--confirm-no-sibling-brand-carryover",
  "--confirm-no-duplicate-scenario-images",
  "--confirm-no-near-duplicate-scenario-images",
  "--confirm-no-generic-bonvoy-third-card",
  "--confirm-no-internal-diligence-language",
  "--confirm-owner-value-specificity",
]);

export const WAVE14_DATED_MOMENTUM_CLEANUP_APPLY_FLAGS = Object.freeze([
  "--approve-wave14-dated-momentum-cleanup",
  "--confirm-eight-public-brand-scope",
  "--confirm-four-points-flex-held",
  "--confirm-momentum-only",
  "--confirm-no-status-writes",
  "--confirm-no-release-field-writes",
  "--confirm-no-image-writes",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-geo-rewrites",
  "--confirm-no-protected-46-brand-changes",
  "--confirm-no-house-of-originals-writes",
  "--confirm-no-morgans-originals-writes",
  "--confirm-no-radisson-collection-changes",
]);

export const WAVE14_PARENT_PLATFORM = "Marriott International";

export const WAVE14_SLUGS = Object.freeze([
  "marriott-hotels",
  "sheraton",
  "westin",
  "residence-inn-by-marriott",
  "springhill-suites-by-marriott",
  "towneplace-suites-by-marriott",
  "aloft-hotels",
  "four-points-flex-by-sheraton",
  "studiores",
]);

export const WAVE14_BRAND_PLAN = Object.freeze({
  "marriott-hotels": Object.freeze({
    slug: "marriott-hotels",
    name: "Marriott Hotels",
    displayName: "Marriott Hotels",
    parentPlatform: WAVE14_PARENT_PLATFORM,
    recommendedStatusWhileInFactory: "Under Review",
    segmentHint: "Full-service flagship",
    nameAliases: ["Marriott Hotels", "Marriott Hotels & Resorts"],
    aliasSearchTokens: ["marriott hotels"],
    siblingDistinctions: [
      "Marriott International (corporate / parent — not this brand)",
      "JW Marriott",
      "Sheraton",
      "Westin",
      "Renaissance",
      "Autograph Collection",
      "Tribute Portfolio",
    ],
    aliasRisks: ["Marriott International", "Marriott", "Marriott Hotel"],
  }),
  sheraton: Object.freeze({
    slug: "sheraton",
    name: "Sheraton",
    displayName: "Sheraton",
    parentPlatform: WAVE14_PARENT_PLATFORM,
    recommendedStatusWhileInFactory: "Under Review",
    segmentHint: "Full-service / legacy repositioning",
    nameAliases: ["Sheraton", "Sheraton Hotels & Resorts", "Sheraton Hotels"],
    aliasSearchTokens: ["sheraton"],
    siblingDistinctions: [
      "Marriott Hotels",
      "Westin",
      "Four Points by Sheraton",
      "Four Points Flex by Sheraton",
    ],
    aliasRisks: ["Sheraton Hotels & Resorts", "Sheraton Hotels"],
  }),
  westin: Object.freeze({
    slug: "westin",
    name: "Westin",
    displayName: "Westin",
    parentPlatform: WAVE14_PARENT_PLATFORM,
    recommendedStatusWhileInFactory: "Under Review",
    segmentHint: "Wellness / premium full-service",
    nameAliases: ["Westin", "Westin Hotels & Resorts", "The Westin"],
    aliasSearchTokens: ["westin"],
    siblingDistinctions: [
      "Sheraton",
      "Marriott Hotels",
      "W Hotels",
      "JW Marriott",
      "Renaissance",
    ],
    aliasRisks: ["Westin Hotels & Resorts", "The Westin"],
  }),
  "residence-inn-by-marriott": Object.freeze({
    slug: "residence-inn-by-marriott",
    name: "Residence Inn by Marriott",
    displayName: "Residence Inn by Marriott",
    parentPlatform: WAVE14_PARENT_PLATFORM,
    recommendedStatusWhileInFactory: "Under Review",
    segmentHint: "Extended-stay",
    nameAliases: ["Residence Inn by Marriott", "Residence Inn"],
    aliasSearchTokens: ["residence inn"],
    siblingDistinctions: [
      "TownePlace Suites by Marriott",
      "StudioRes",
      "Element",
      "Apartments by Marriott Bonvoy",
    ],
    aliasRisks: ["Residence Inn", "Residence Inn Marriott"],
  }),
  "springhill-suites-by-marriott": Object.freeze({
    slug: "springhill-suites-by-marriott",
    name: "SpringHill Suites by Marriott",
    displayName: "SpringHill Suites by Marriott",
    parentPlatform: WAVE14_PARENT_PLATFORM,
    recommendedStatusWhileInFactory: "Under Review",
    segmentHint: "All-suite select-service",
    nameAliases: ["SpringHill Suites by Marriott", "SpringHill Suites", "Spring Hill Suites"],
    aliasSearchTokens: ["springhill", "spring hill suites"],
    siblingDistinctions: [
      "Residence Inn by Marriott",
      "Fairfield by Marriott",
      "Courtyard by Marriott",
      "TownePlace Suites by Marriott",
    ],
    aliasRisks: ["SpringHill Suites", "Spring Hill Suites"],
  }),
  "towneplace-suites-by-marriott": Object.freeze({
    slug: "towneplace-suites-by-marriott",
    name: "TownePlace Suites by Marriott",
    displayName: "TownePlace Suites by Marriott",
    parentPlatform: WAVE14_PARENT_PLATFORM,
    recommendedStatusWhileInFactory: "Under Review",
    segmentHint: "Longer-stay / extended-stay select-service",
    nameAliases: ["TownePlace Suites by Marriott", "TownePlace Suites"],
    aliasSearchTokens: ["towneplace", "townplace"],
    siblingDistinctions: [
      "Residence Inn by Marriott",
      "StudioRes",
      "SpringHill Suites by Marriott",
      "Element",
    ],
    aliasRisks: ["TownePlace Suites", "TownPlace Suites"],
  }),
  "aloft-hotels": Object.freeze({
    slug: "aloft-hotels",
    name: "Aloft Hotels",
    displayName: "Aloft Hotels",
    parentPlatform: WAVE14_PARENT_PLATFORM,
    recommendedStatusWhileInFactory: "Under Review",
    segmentHint: "Select-service lifestyle",
    nameAliases: ["Aloft Hotels", "Aloft"],
    aliasSearchTokens: ["aloft"],
    siblingDistinctions: [
      "Moxy Hotels",
      "AC Hotels by Marriott",
      "Four Points by Sheraton",
      "Element",
      "W Hotels",
    ],
    aliasRisks: ["Aloft", "aloft hotels"],
  }),
  "four-points-flex-by-sheraton": Object.freeze({
    slug: "four-points-flex-by-sheraton",
    name: "Four Points Flex by Sheraton",
    displayName: "Four Points Flex by Sheraton",
    parentPlatform: WAVE14_PARENT_PLATFORM,
    recommendedStatusWhileInFactory: "Under Review",
    segmentHint: "Flex / conversion-oriented Sheraton family line",
    nameAliases: ["Four Points Flex by Sheraton", "Four Points Flex"],
    aliasSearchTokens: ["four points flex", "fourpoints flex"],
    siblingDistinctions: [
      "Four Points by Sheraton (distinct brand — do not conflate)",
      "Sheraton",
    ],
    aliasRisks: [
      "Four Points by Sheraton",
      "Four Points Flex",
      "FourPoints Flex",
    ],
    criticalAliasWarning:
      "Do not confuse Four Points Flex by Sheraton with Four Points by Sheraton.",
  }),
  studiores: Object.freeze({
    slug: "studiores",
    name: "StudioRes",
    displayName: "StudioRes",
    parentPlatform: WAVE14_PARENT_PLATFORM,
    recommendedStatusWhileInFactory: "Under Review",
    segmentHint: "Extended-stay / studio product (verify official naming)",
    nameAliases: ["StudioRes", "StudioRes by Marriott", "StudioRes Studios"],
    aliasSearchTokens: ["studiores", "studio res"],
    siblingDistinctions: [
      "Residence Inn by Marriott",
      "TownePlace Suites by Marriott",
      "Element",
      "Apartments by Marriott Bonvoy",
    ],
    aliasRisks: [
      "StudioRes by Marriott",
      "StudioRes Studios",
      "Studio Res",
    ],
    criticalAliasWarning:
      "Do not confuse StudioRes with Residence Inn, TownePlace Suites, Element, or Apartments by Marriott Bonvoy.",
  }),
});

export const WAVE14_FACTORY_PREVIEW_APPLY_FLAGS = Object.freeze([
  "--approve-factory-preview-cohort",
  "--confirm-no-airtable-writes",
  "--confirm-no-brand-status-changes",
  "--confirm-no-release-field-changes",
  "--confirm-no-protected-baseline-changes",
]);

export const WAVE14_STAGE4_APPROVED_SLUGS = Object.freeze([...WAVE14_SLUGS]);
export const WAVE14_STAGE5_APPROVED_SLUGS = Object.freeze([...WAVE14_SLUGS]);

export function isWave14Stage5Slug(slug) {
  return WAVE14_STAGE5_APPROVED_SLUGS.includes(String(slug || "").trim().toLowerCase());
}

export const WAVE14_FORBIDDEN_WRITE_FIELDS = Object.freeze([
  "Company Validated",
  "Company Validation Date",
  "Source Library Status",
  "Registry Approval",
  "Registry Status",
]);

export const WAVE14_IMAGE_MATERIALIZATION_APPLY_FLAGS = Object.freeze([
  "--approve-wave14-image-materialization",
  "--confirm-nine-brand-stage5-scope",
  "--confirm-target-brands-only",
  "--confirm-no-protected-46-brand-changes",
  "--confirm-no-accor-wave13-active-brand-writes",
  "--confirm-no-house-of-originals-writes",
  "--confirm-no-morgans-originals-writes",
  "--confirm-no-radisson-collection-changes",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-brand-status-changes",
  "--confirm-no-release-field-writes",
  "--confirm-no-content-rewrites",
  "--confirm-image-uniqueness",
  "--confirm-image-role-match",
  "--confirm-no-wrong-brand-images",
  "--confirm-no-sibling-brand-images",
  "--confirm-marriott-brand-family-separated",
  "--confirm-four-points-flex-not-four-points",
  "--confirm-studiores-not-residence-inn-or-towneplace",
  "--confirm-cala-first-openings-priority",
  "--confirm-international-reference-labels-where-needed",
  "--confirm-property-url-matches-required-for-named-gallery",
  "--confirm-cleanly-unavailable-for-unsupported-property-images",
]);

export const WAVE14_TAB_FACTORY_BUILD_APPLY_FLAGS = Object.freeze([
  "--approve-wave14-tab-factory-build",
  "--confirm-nine-brand-stage4-scope",
  "--confirm-target-brands-only",
  "--confirm-source-pack-grounded",
  "--confirm-value-scenario-pattern-enforced",
  "--confirm-recent-momentum-structured",
  "--confirm-geo-footprint-source-supported",
  "--confirm-ai-assisted-footnote-preview-supported",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-brand-status-changes",
  "--confirm-no-release-field-writes",
  "--confirm-no-protected-46-brand-changes",
  "--confirm-no-radisson-collection-changes",
  "--confirm-no-image-writes",
  "--confirm-no-broad-rewrites",
  "--confirm-no-adr",
  "--confirm-no-revpar",
  "--confirm-no-fee-stack",
  "--confirm-no-raw-urls",
  "--confirm-no-owner-fit-diligence",
  "--confirm-no-section-labels-as-scenario-titles",
  "--confirm-target-guest-segments-validated",
]);

/** Wave 14 Stage 6 — post-image content cleanup apply flags. */
export const WAVE14_POST_IMAGE_CONTENT_CLEANUP_APPLY_FLAGS = Object.freeze([
  "--approve-wave14-post-image-content-cleanup",
  "--confirm-nine-brand-stage6-scope",
  "--confirm-target-brands-only",
  "--confirm-all-nine-remain-under-review",
  "--confirm-no-brand-status-changes",
  "--confirm-no-release-field-writes",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-public-restore-registry-changes",
  "--confirm-no-protected-46-brand-changes",
  "--confirm-no-accor-wave13-active-brand-writes",
  "--confirm-no-house-of-originals-writes",
  "--confirm-no-morgans-originals-writes",
  "--confirm-no-radisson-collection-changes",
  "--confirm-no-broad-rewrites",
  "--confirm-no-wrong-brand-images",
  "--confirm-no-sibling-brand-images",
  "--confirm-four-points-flex-not-four-points",
  "--confirm-studiores-not-residence-inn-or-towneplace",
  "--confirm-flex-source-limitations-cleanly-held",
  "--confirm-no-adr",
  "--confirm-no-revpar",
  "--confirm-no-fee-stack",
  "--confirm-no-raw-urls",
  "--confirm-recent-momentum-structured",
  "--confirm-geo-footprint-source-supported-or-cleanly-unavailable",
]);

/** Placeholder if a Stage 4.5 cleanup stage is added later. */
export const WAVE14_STAGE4_CONTENT_CLEANUP_APPLY_FLAGS = Object.freeze([
  "--approve-wave14-stage4-content-cleanup",
  "--confirm-nine-brand-stage4-scope",
  "--confirm-target-brands-only",
  "--confirm-no-image-writes",
  "--confirm-no-brand-status-changes",
  "--confirm-no-release-field-writes",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-protected-46-brand-changes",
  "--confirm-no-radisson-collection-changes",
]);

export const WAVE14_MANIFEST_CLASSIFICATIONS = Object.freeze([
  "existing_ready_for_audit",
  "existing_needs_factory_build",
  "existing_needs_status_correction",
  "missing_brand_basics_record",
  "duplicate_or_slug_conflict",
  "blocked_requires_manual_review",
]);

export const WAVE14_RELEASE_FIELDS = Object.freeze([
  "Active Profile Approved",
  "Ready for Active Profile",
  "Active Profile Approved Date",
  "Founder Visual Review Pass",
]);

export const WAVE14_NEVER_WRITE_FIELDS = Object.freeze([
  "Company Validated",
  "Company Validation Date",
  "Source Library status",
  "Registry approval/status",
  "Source Library Status",
  "Registry Approval",
  "Registry Status",
  "Brand Status",
]);

export function getWave14Plan(slug) {
  const plan = WAVE14_BRAND_PLAN[slug];
  if (!plan) throw new Error(`Unknown Wave 14 slug: ${slug}`);
  return plan;
}
