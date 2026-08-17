/**
 * Brand Explorer Wave 15 — Hilton Brand Family factory cohort + stage contracts.
 *
 * 8 Hilton Worldwide brands for factory preview → later Active/Live public-full
 * after gates + founder approval. Must NOT join Active/Live until status
 * promotion. Unlike Wave 14 (Four Points Flex by Sheraton held), no slug in
 * the Wave 15 eight is pre-designated as held.
 *
 * Protected baseline remains 54
 * (`frozen_54_active_public_full_baseline_semantic_clean_flex_held`) until an
 * intentional freeze revision after Wave 15 promotion.
 */
export const WAVE15_PLAN_VERSION = "wave15-factory-plan-v1";
export const WAVE15_VERSION = "brand-explorer-wave15-factory-v1";

export const WAVE15_PROTECTED_BASELINE_COUNT = 54;
export const WAVE15_EXPECTED_FINAL_ACTIVE_COUNT = 62; // 54 + 8 if all promote

/** No slug in the Wave 15 eight is held (unlike Wave 14 Four Points Flex). */
export const WAVE15_NO_HELD_SLUGS = true;
export const WAVE15_HELD_PROMOTION_SLUG = null;

export const WAVE15_STATUS_FROM = "Under Review";
export const WAVE15_STATUS_TO_PREFERRED = "Active";
export const WAVE15_STATUS_TO_ALLOWED = Object.freeze(["Active", "Live"]);

export const WAVE15_STAGES = Object.freeze([
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
]);

export const WAVE15_PARENT_PLATFORM = "Hilton Worldwide";

export const WAVE15_SLUGS = Object.freeze([
  "hilton-hotels-and-resorts",
  "homewood-suites-by-hilton",
  "home2-suites-by-hilton",
  "tru-by-hilton",
  "doubletree-by-hilton",
  "hampton-by-hilton",
  "hilton-garden-inn",
  "spark-by-hilton",
]);

export const WAVE15_BRAND_PLAN = Object.freeze({
  "hilton-hotels-and-resorts": Object.freeze({
    slug: "hilton-hotels-and-resorts",
    name: "Hilton Hotels & Resorts",
    displayName: "Hilton Hotels & Resorts",
    parentPlatform: WAVE15_PARENT_PLATFORM,
    recommendedStatusWhileInFactory: "Under Review",
    segmentHint: "Full-service flagship",
    nameAliases: [
      "Hilton Hotels & Resorts",
      "Hilton Hotels and Resorts",
      "Hilton Hotels",
    ],
    aliasSearchTokens: ["hilton hotels"],
    siblingDistinctions: [
      "Hilton Worldwide (corporate / parent — not this brand)",
      "Waldorf Astoria Hotels & Resorts",
      "Conrad Hotels & Resorts",
      "Signia by Hilton",
      "Curio Collection by Hilton",
      "DoubleTree by Hilton",
    ],
    aliasRisks: ["Hilton", "Hilton Worldwide", "Hilton International", "Hilton Hotels"],
    criticalAliasWarning:
      "Do not confuse Hilton Hotels & Resorts (the flagship full-service brand) with Hilton Worldwide (the parent corporate entity) or generic 'Hilton' as a company name.",
  }),
  "homewood-suites-by-hilton": Object.freeze({
    slug: "homewood-suites-by-hilton",
    name: "Homewood Suites by Hilton",
    displayName: "Homewood Suites by Hilton",
    parentPlatform: WAVE15_PARENT_PLATFORM,
    recommendedStatusWhileInFactory: "Under Review",
    segmentHint: "Upscale extended-stay",
    nameAliases: ["Homewood Suites by Hilton", "Homewood Suites"],
    aliasSearchTokens: ["homewood suites", "homewood"],
    siblingDistinctions: [
      "Home2 Suites by Hilton (midscale / cost-conscious extended-stay — do not conflate)",
      "Hilton Garden Inn",
      "Embassy Suites by Hilton",
    ],
    aliasRisks: ["Homewood", "Homewood Suites"],
    criticalAliasWarning:
      "Do not confuse Homewood Suites by Hilton (upscale extended-stay, full hot breakfast + evening reception) with Home2 Suites by Hilton (midscale, cost-conscious extended-stay) — same category, different segment and price point.",
  }),
  "home2-suites-by-hilton": Object.freeze({
    slug: "home2-suites-by-hilton",
    name: "Home2 Suites by Hilton",
    displayName: "Home2 Suites by Hilton",
    parentPlatform: WAVE15_PARENT_PLATFORM,
    recommendedStatusWhileInFactory: "Under Review",
    segmentHint: "Midscale / cost-conscious extended-stay",
    nameAliases: ["Home2 Suites by Hilton", "Home2 Suites", "Home 2 Suites by Hilton"],
    aliasSearchTokens: ["home2 suites", "home2", "home 2 suites"],
    siblingDistinctions: [
      "Homewood Suites by Hilton (upscale extended-stay — do not conflate)",
      "Hampton by Hilton",
    ],
    aliasRisks: ["Home2", "Home 2 Suites"],
    criticalAliasWarning:
      "Do not confuse Home2 Suites by Hilton (midscale, flexible-configuration extended-stay) with Homewood Suites by Hilton (upscale extended-stay) — same category, different segment and price point.",
  }),
  "tru-by-hilton": Object.freeze({
    slug: "tru-by-hilton",
    name: "Tru by Hilton",
    displayName: "Tru by Hilton",
    parentPlatform: WAVE15_PARENT_PLATFORM,
    recommendedStatusWhileInFactory: "Under Review",
    segmentHint: "New-build focused-service, spirited / value, cross-generational",
    nameAliases: ["Tru by Hilton", "Tru"],
    aliasSearchTokens: ["tru by hilton"],
    siblingDistinctions: [
      "Spark by Hilton (conversion-only premium economy — do not conflate)",
      "Hampton by Hilton (classic focused-service, free hot breakfast + 100% Hampton Guarantee)",
      "Home2 Suites by Hilton (dual-brand pairing partner, distinct extended-stay category)",
    ],
    aliasRisks: ["Tru Hotel", "Tru Hotels"],
    criticalAliasWarning:
      "Tru by Hilton (new-build focused-service, spirited/value design) ≠ Spark by Hilton (conversion-only premium economy) ≠ Hampton by Hilton (classic focused-service with free hot breakfast and 100% Hampton Guarantee). Three distinct, easily-conflated focused-service/value brands.",
  }),
  "doubletree-by-hilton": Object.freeze({
    slug: "doubletree-by-hilton",
    name: "DoubleTree by Hilton",
    displayName: "DoubleTree by Hilton",
    parentPlatform: WAVE15_PARENT_PLATFORM,
    recommendedStatusWhileInFactory: "Under Review",
    segmentHint: "Upscale full-service",
    nameAliases: ["DoubleTree by Hilton", "DoubleTree", "Double Tree by Hilton"],
    aliasSearchTokens: ["doubletree", "double tree"],
    siblingDistinctions: [
      "Hilton Hotels & Resorts (flagship full-service — distinct brand)",
      "Curio Collection by Hilton",
      "Tapestry Collection by Hilton",
      "Embassy Suites by Hilton",
    ],
    aliasRisks: ["Double Tree", "DoubleTree Hotel", "DoubleTree Suites"],
  }),
  "hampton-by-hilton": Object.freeze({
    slug: "hampton-by-hilton",
    name: "Hampton by Hilton",
    displayName: "Hampton by Hilton",
    parentPlatform: WAVE15_PARENT_PLATFORM,
    recommendedStatusWhileInFactory: "Under Review",
    segmentHint: "Focused-service ('Hamptonality', 100% Hampton Guarantee)",
    nameAliases: [
      "Hampton by Hilton",
      "Hampton Inn",
      "Hampton Inn & Suites",
      "Hampton Hotels",
    ],
    aliasSearchTokens: ["hampton by hilton", "hampton inn", "hampton hotels"],
    siblingDistinctions: [
      "Tru by Hilton (do not conflate — different focused-service value tier)",
      "Spark by Hilton (do not conflate — conversion-only premium economy)",
      "Hilton Garden Inn (upscale, higher tier)",
    ],
    aliasRisks: ["Hampton", "Hampton Suites"],
    criticalAliasWarning:
      "Hampton by Hilton (classic focused-service with free hot breakfast and 100% Hampton Guarantee) ≠ Tru by Hilton (new-build spirited/value) ≠ Spark by Hilton (conversion-only premium economy). Legacy Brand Basics records may still say 'Hampton Inn' or 'Hampton Inn & Suites' — same brand, current name is Hampton by Hilton.",
  }),
  "hilton-garden-inn": Object.freeze({
    slug: "hilton-garden-inn",
    name: "Hilton Garden Inn",
    displayName: "Hilton Garden Inn",
    parentPlatform: WAVE15_PARENT_PLATFORM,
    recommendedStatusWhileInFactory: "Under Review",
    segmentHint: "Upscale focused/select-service",
    nameAliases: ["Hilton Garden Inn"],
    aliasSearchTokens: ["hilton garden inn", "garden inn"],
    siblingDistinctions: [
      "Hampton by Hilton (lower tier — do not conflate)",
      "DoubleTree by Hilton (upscale full-service — distinct segment)",
      "Homewood Suites by Hilton / Home2 Suites by Hilton (extended-stay — distinct category)",
    ],
    aliasRisks: ["Hilton Garden Inn & Suites", "Garden Inn"],
  }),
  "spark-by-hilton": Object.freeze({
    slug: "spark-by-hilton",
    name: "Spark by Hilton",
    displayName: "Spark by Hilton",
    parentPlatform: WAVE15_PARENT_PLATFORM,
    recommendedStatusWhileInFactory: "Under Review",
    segmentHint: "Premium economy, conversion-only (newest Hilton brand, launched 2023)",
    nameAliases: ["Spark by Hilton", "Spark"],
    aliasSearchTokens: ["spark by hilton"],
    siblingDistinctions: [
      "Tru by Hilton (new-build, not conversion-only — do not conflate)",
      "Hampton by Hilton (higher tier, full hot breakfast guarantee)",
    ],
    aliasRisks: ["Spark Hotel", "Spark Hotels"],
    criticalAliasWarning:
      "Spark by Hilton (conversion-only premium economy, launched 2023) ≠ Tru by Hilton (new-build, spirited/value) ≠ Hampton by Hilton (classic focused-service). Verify Brand Basics record is not a placeholder given Spark's recency — confirm before Stage 4.",
  }),
});

/** General duplicate/alias risk notes across the Wave 15 Hilton family (manifest + source-pack level). */
export const WAVE15_DUPLICATE_ALIAS_RISK_NOTES = Object.freeze([
  "Hilton Worldwide (corporate/parent) vs Hilton Hotels & Resorts (the flagship brand) — never select the corporate record as the brand target.",
  "Homewood Suites by Hilton (upscale extended-stay) vs Home2 Suites by Hilton (midscale extended-stay) — distinct brands, easily confused by name similarity.",
  "Tru by Hilton vs Spark by Hilton vs Hampton by Hilton — three distinct focused-service/value brands; do not merge sibling distinctions or share imagery/momentum across them.",
  "Hampton by Hilton legacy naming: Brand Basics may still carry 'Hampton Inn' or 'Hampton Inn & Suites' — treat as the same brand, current canonical name is Hampton by Hilton.",
  "DoubleTree by Hilton vs Hilton Hotels & Resorts — both full-service, but distinct brands; do not use Hilton flagship imagery/momentum for DoubleTree or vice versa.",
  "Spark by Hilton is the newest brand (launched 2023) — higher risk of missing/placeholder Brand Basics records; verify before Stage 4.",
]);

export const WAVE15_FACTORY_PREVIEW_APPLY_FLAGS = Object.freeze([
  "--approve-wave15-factory-preview-cohort",
  "--confirm-no-airtable-writes",
  "--confirm-no-brand-status-changes",
  "--confirm-no-release-field-changes",
  "--confirm-no-protected-54-baseline-changes",
]);

/** Wave 15 Stage 4 — tab-factory-build apply flags (exact task contract). */
export const WAVE15_TAB_FACTORY_BUILD_APPLY_FLAGS = Object.freeze([
  "--approve-wave15-tab-factory-build",
  "--confirm-eight-brand-stage4-scope",
  "--confirm-target-brands-only",
  "--confirm-source-pack-grounded",
  "--confirm-semantic-product-standard-enforced",
  "--confirm-recent-momentum-real-announcement-or-property-proof",
  "--confirm-portfolio-mix-structured",
  "--confirm-openings-use-actual-property-names",
  "--confirm-value-scenarios-brand-specific",
  "--confirm-ai-assisted-footnote-preview-supported",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-brand-status-changes",
  "--confirm-no-release-field-writes",
  "--confirm-no-protected-54-brand-changes",
  "--confirm-no-four-points-flex-writes",
  "--confirm-no-house-of-originals-writes",
  "--confirm-no-morgans-originals-writes",
  "--confirm-no-radisson-collection-changes",
  "--confirm-no-image-writes",
  "--confirm-no-broad-rewrites",
  "--confirm-no-adr",
  "--confirm-no-revpar",
  "--confirm-no-fee-stack",
  "--confirm-no-raw-urls",
  "--confirm-no-internal-source-language",
  "--confirm-no-placeholder-property-titles",
  "--confirm-target-guest-segments-validated",
]);

/** Optional Stage 4.5 cleanup flags (not required for Stage 4 acceptance). */
export const WAVE15_STAGE4_CONTENT_CLEANUP_APPLY_FLAGS = Object.freeze([
  "--approve-wave15-stage4-content-cleanup",
  "--confirm-eight-brand-stage4-scope",
  "--confirm-target-brands-only",
  "--confirm-no-image-writes",
  "--confirm-no-brand-status-changes",
  "--confirm-no-release-field-writes",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-protected-54-brand-changes",
  "--confirm-no-radisson-collection-changes",
  "--confirm-no-four-points-flex-writes",
]);

/** Wave 15 Stage 5 — image materialization apply flags (exact task contract). */
export const WAVE15_IMAGE_MATERIALIZATION_APPLY_FLAGS = Object.freeze([
  "--approve-wave15-image-materialization",
  "--confirm-eight-brand-stage5-scope",
  "--confirm-target-brands-only",
  "--confirm-protected-54-identity-preflight-passed",
  "--confirm-no-protected-54-brand-changes",
  "--confirm-no-marriott-hotels-writes",
  "--confirm-no-four-points-flex-writes",
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
  "--confirm-hilton-brand-family-separated",
  "--confirm-hilton-hotels-not-hilton-corporate",
  "--confirm-homewood-not-home2",
  "--confirm-home2-not-homewood-or-tru",
  "--confirm-tru-not-spark-or-hampton",
  "--confirm-spark-not-tru-or-hampton",
  "--confirm-cala-first-openings-priority",
  "--confirm-americas-reference-before-international-reference",
  "--confirm-property-url-matches-required-for-named-gallery",
  "--confirm-cleanly-unavailable-for-unsupported-property-images",
]);

/** Wave 15 Stage 6 — post-image content cleanup apply flags (exact task contract). */
export const WAVE15_POST_IMAGE_CONTENT_CLEANUP_APPLY_FLAGS = Object.freeze([
  "--approve-wave15-post-image-content-cleanup",
  "--confirm-eight-brand-stage6-scope",
  "--confirm-target-brands-only",
  "--confirm-all-eight-remain-under-review",
  "--confirm-snapshot-typical-keys-handled",
  "--confirm-no-brand-status-changes",
  "--confirm-no-release-field-writes",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-public-restore-registry-changes",
  "--confirm-no-protected-54-brand-changes",
  "--confirm-no-marriott-hotels-writes",
  "--confirm-no-four-points-flex-writes",
  "--confirm-no-house-of-originals-writes",
  "--confirm-no-morgans-originals-writes",
  "--confirm-no-radisson-collection-changes",
  "--confirm-no-broad-rewrites",
  "--confirm-no-wrong-brand-images",
  "--confirm-no-sibling-brand-images",
  "--confirm-hilton-brand-family-separated",
  "--confirm-no-internal-source-language",
  "--confirm-no-raw-urls",
  "--confirm-recent-momentum-semantics-preserved",
  "--confirm-portfolio-mix-structured",
  "--confirm-openings-use-actual-property-names",
  "--confirm-geo-footprint-source-supported-or-cleanly-unavailable",
]);

export const WAVE15_STAGE4_APPROVED_SLUGS = Object.freeze([...WAVE15_SLUGS]);
export const WAVE15_STAGE5_APPROVED_SLUGS = Object.freeze([...WAVE15_SLUGS]);
export const WAVE15_STAGE6_APPROVED_SLUGS = Object.freeze([...WAVE15_SLUGS]);

export const WAVE15_PROMOTION_SLUGS = Object.freeze([...WAVE15_SLUGS]);
export const WAVE15_FOUNDER_APPROVE_RECOMMENDATION =
  "approve_for_status_promotion_and_public_release";

/** Four Points Flex is not in Wave 15 — verify held separately. */
export const WAVE15_FLEX_HELD_SLUG = "four-points-flex-by-sheraton";
export const WAVE15_FLEX_HELD_RECORD_ID = "recgaMzDn2GKkpUsi";

/** Wave 15 Stage 9 — Brand Status promotion apply flags (exact task contract). */
export const WAVE15_STATUS_PROMOTION_APPLY_FLAGS = Object.freeze([
  "--approve-wave15-brand-status-promotion",
  "--confirm-founder-signoff-for-eight",
  "--confirm-target-brands-only",
  "--confirm-status-to-active",
  "--confirm-no-four-points-flex-writes",
  "--confirm-no-protected-54-brand-changes",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-content-writes",
  "--confirm-no-image-writes",
  "--confirm-no-release-field-writes",
]);

/** Wave 15 Stage 10 — public release apply flags (exact task contract). */
export const WAVE15_PUBLIC_RELEASE_APPLY_FLAGS = Object.freeze([
  "--approve-wave15-public-release",
  "--confirm-founder-visual-review-passed-for-eight",
  "--confirm-brand-status-active",
  "--confirm-fully-ready",
  "--confirm-public-visibility-quality-lock-passed",
  "--confirm-target-brands-only",
  "--confirm-no-four-points-flex-writes",
  "--confirm-no-protected-54-brand-changes",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-content-rewrites",
  "--confirm-no-image-writes",
]);

export function isWave15Stage5Slug(slug) {
  return WAVE15_STAGE5_APPROVED_SLUGS.includes(String(slug || "").trim().toLowerCase());
}

export const WAVE15_FORBIDDEN_WRITE_FIELDS = Object.freeze([
  "Company Validated",
  "Company Validation Date",
  "Source Library Status",
  "Registry Approval",
  "Registry Status",
]);

export const WAVE15_RELEASE_FIELDS = Object.freeze([
  "Active Profile Approved",
  "Ready for Active Profile",
  "Active Profile Approved Date",
  "Founder Visual Review Pass",
]);

export const WAVE15_NEVER_WRITE_FIELDS = Object.freeze([
  "Company Validated",
  "Company Validation Date",
  "Source Library status",
  "Registry approval/status",
  "Source Library Status",
  "Registry Approval",
  "Registry Status",
  "Brand Status",
]);

/** Same taxonomy as Wave 14 — every manifest row must resolve to exactly one of these. */
export const WAVE15_MANIFEST_CLASSIFICATIONS = Object.freeze([
  "existing_ready_for_audit",
  "existing_needs_factory_build",
  "existing_needs_status_correction",
  "missing_brand_basics_record",
  "duplicate_or_slug_conflict",
  "blocked_requires_manual_review",
]);

export function getWave15Plan(slug) {
  const plan = WAVE15_BRAND_PLAN[slug];
  if (!plan) throw new Error(`Unknown Wave 15 slug: ${slug}`);
  return plan;
}
