/**
 * Brand Explorer Wave 12 — factory cohort constants + stage contracts.
 *
 * 12 new brands destined for Active/Live public-full after factory gates +
 * founder approval. Must NOT join Active/Live universe until status promotion.
 *
 * Protected baseline was 27 (`27-active-public-full-baseline-v2`) until the
 * intentional 39 freeze after all 12 Wave 12 brands were public-full.
 */
export const WAVE12_VERSION = "wave12-factory-v1";

export const WAVE12_EXPECTED_FINAL_ACTIVE_COUNT = 39;
export const WAVE12_PROTECTED_BASELINE_COUNT = 27;

export const WAVE12_STAGES = Object.freeze([
  "preflight",
  "manifest",
  "factory-preview-cohort",
  "source-packs",
  "tab-factory-build",
  "image-materialization",
  "post-image-content-cleanup",
  "evidence-quality-fixes",
  "gate-suite",
  "founder-review",
  "status-promotion",
  "public-release",
  "post-release-freeze-cleanup",
  "baseline-39",
]);

/** Target slugs for the wave (canonical). */
export const WAVE12_SLUGS = Object.freeze([
  "even-hotels",
  "voco-hotels",
  "avid-hotels",
  "holiday-inn-express",
  "courtyard-by-marriott",
  "ac-hotels-by-marriott",
  "city-express-by-marriott",
  "moxy-hotels",
  "canopy-by-hilton",
  "motto-by-hilton",
  "tempo-by-hilton",
  "bunkhouse-hotels",
]);

/**
 * Planned identities (name + parent). recordId filled by manifest discovery.
 * Names must match Brand Basics when records exist.
 */
export const WAVE12_BRAND_PLAN = Object.freeze({
  "even-hotels": Object.freeze({
    slug: "even-hotels",
    name: "EVEN Hotels",
    parentPlatform: "IHG",
    family: "ihg",
    recommendedStatusWhileInFactory: "Under Review",
    nameAliases: ["Even Hotels", "EVEN Hotel", "Even Hotel"],
    lens:
      "Wellness-oriented IHG hotel experience; distinguish from Holiday Inn Express, avid, voco, Hotel Indigo.",
  }),
  "voco-hotels": Object.freeze({
    slug: "voco-hotels",
    name: "voco",
    parentPlatform: "IHG",
    family: "ihg",
    recommendedStatusWhileInFactory: "Under Review",
    nameAliases: ["voco hotels", "Voco Hotels", "voco by IHG", "VOCO", "voco Hotels"],
    lens:
      "Upscale conversion-oriented IHG soft brand; distinguish from Hotel Indigo, Kimpton, Vignette, Holiday Inn.",
  }),
  "avid-hotels": Object.freeze({
    slug: "avid-hotels",
    name: "avid hotels",
    parentPlatform: "IHG",
    family: "ihg",
    recommendedStatusWhileInFactory: "Under Review",
    nameAliases: ["avid Hotels", "Avid Hotels"],
    lens:
      "Essentials-focused midscale/select-service IHG; distinguish from Holiday Inn Express and other midscale brands.",
  }),
  "holiday-inn-express": Object.freeze({
    slug: "holiday-inn-express",
    name: "Holiday Inn Express",
    parentPlatform: "IHG",
    family: "ihg",
    recommendedStatusWhileInFactory: "Under Review",
    lens:
      "Scaled select-service / limited-service IHG; distinguish from Holiday Inn and avid.",
  }),
  "courtyard-by-marriott": Object.freeze({
    slug: "courtyard-by-marriott",
    name: "Courtyard by Marriott",
    parentPlatform: "Marriott",
    family: "marriott",
    recommendedStatusWhileInFactory: "Under Review",
    lens:
      "Major Marriott select-service / business-transient brand; distinguish from AC, Moxy, City Express, Fairfield.",
  }),
  "ac-hotels-by-marriott": Object.freeze({
    slug: "ac-hotels-by-marriott",
    name: "AC Hotels by Marriott",
    parentPlatform: "Marriott",
    family: "marriott",
    recommendedStatusWhileInFactory: "Under Review",
    nameAliases: ["AC Hotel", "AC Hotels"],
    lens:
      "Design-led lifestyle-select Marriott; distinguish from Moxy, Courtyard, Autograph, Tribute.",
  }),
  "city-express-by-marriott": Object.freeze({
    slug: "city-express-by-marriott",
    name: "City Express by Marriott",
    parentPlatform: "Marriott",
    family: "marriott",
    recommendedStatusWhileInFactory: "Under Review",
    nameAliases: ["City Express", "City Express Hotels"],
    lens:
      "Latin America / CALA-relevant select-service Marriott; distinguish from Courtyard, Fairfield, Four Points.",
  }),
  "moxy-hotels": Object.freeze({
    slug: "moxy-hotels",
    name: "Moxy Hotels",
    parentPlatform: "Marriott",
    family: "marriott",
    recommendedStatusWhileInFactory: "Under Review",
    nameAliases: ["Moxy", "MOXY Hotels"],
    lens:
      "Playful/social lifestyle-select Marriott; distinguish from AC, Aloft, Autograph.",
  }),
  "canopy-by-hilton": Object.freeze({
    slug: "canopy-by-hilton",
    name: "Canopy by Hilton",
    parentPlatform: "Hilton",
    family: "hilton",
    recommendedStatusWhileInFactory: "Under Review",
    lens:
      "Hilton lifestyle / neighborhood-oriented brand; distinguish from Curio, Tapestry, Tempo, Motto.",
  }),
  "motto-by-hilton": Object.freeze({
    slug: "motto-by-hilton",
    name: "Motto by Hilton",
    parentPlatform: "Hilton",
    family: "hilton",
    recommendedStatusWhileInFactory: "Under Review",
    lens:
      "Compact urban lifestyle Hilton; distinguish from Canopy and Tempo.",
  }),
  "tempo-by-hilton": Object.freeze({
    slug: "tempo-by-hilton",
    name: "Tempo by Hilton",
    parentPlatform: "Hilton",
    family: "hilton",
    recommendedStatusWhileInFactory: "Under Review",
    lens:
      "Modern lifestyle / wellness-productivity Hilton; distinguish from Canopy, Motto, Hilton Garden Inn.",
  }),
  "bunkhouse-hotels": Object.freeze({
    slug: "bunkhouse-hotels",
    name: "Bunkhouse Hotels",
    parentPlatform: "Hyatt",
    family: "lifestyle",
    recommendedStatusWhileInFactory: "Under Review",
    nameAliases: ["Bunkhouse", "Bunkhouse Group"],
    lens:
      "Lifestyle/boutique hotel platform now under Hyatt lifestyle / World of Hyatt — label Hyatt materials as parent context; do not invent commercial model.",
  }),
});

export const WAVE12_PROTECTED_BASELINE_SLUGS_NOTE =
  "Protected 27 Active/Live public-full baseline — never write Presentation/Basics for those brands in wave12 stages.";

export const WAVE12_FORBIDDEN_WRITE_FIELDS = Object.freeze([
  "Company Validated",
  "Company Validation Date",
  "Source Library Status",
  "Registry Approval",
  "Registry Status",
]);

export const WAVE12_FACTORY_PREVIEW_APPLY_FLAGS = Object.freeze([
  "--approve-factory-preview-cohort",
  "--confirm-no-airtable-writes",
  "--confirm-no-brand-status-changes",
  "--confirm-no-release-field-changes",
  "--confirm-no-protected-baseline-changes",
]);

export const WAVE12_TAB_FACTORY_BUILD_APPLY_FLAGS = Object.freeze([
  "--approve-wave12-tab-factory-build",
  "--confirm-target-brands-only",
  "--confirm-source-pack-grounded",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-brand-status-changes",
  "--confirm-no-release-field-writes",
  "--confirm-no-protected-27-brand-changes",
  "--confirm-no-radisson-collection-changes",
  "--confirm-no-image-writes",
  "--confirm-no-broad-rewrites",
  "--confirm-target-guest-segments-validated",
]);

export const WAVE12_IMAGE_MATERIALIZATION_APPLY_FLAGS = Object.freeze([
  "--approve-wave12-image-materialization",
  "--confirm-target-brands-only",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-brand-status-changes",
  "--confirm-no-release-field-writes",
  "--confirm-no-protected-27-brand-changes",
  "--confirm-image-uniqueness",
  "--confirm-image-role-match",
  "--confirm-cala-first-openings-priority",
  "--confirm-international-reference-labels-where-needed",
  "--confirm-no-logo-only-filler",
  "--confirm-no-wrong-brand-images",
]);

export const WAVE12_POST_IMAGE_CONTENT_CLEANUP_APPLY_FLAGS = Object.freeze([
  "--approve-wave12-post-image-content-cleanup",
  "--confirm-target-brands-only",
  "--confirm-targeted-field-fixes-only",
  "--confirm-recent-momentum-and-openings-quality",
  "--confirm-cala-first-priority",
  "--confirm-international-reference-labels-where-needed",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-brand-status-changes",
  "--confirm-no-release-field-writes",
  "--confirm-no-protected-27-brand-changes",
  "--confirm-no-radisson-collection-changes",
  "--confirm-no-image-writes-except-caption-only-if-flagged",
  "--confirm-no-broad-rewrites",
  "--confirm-no-raw-urls",
]);

/** Stage 9 — Brand Status only (Under Review → Active; matches protected-27 freeze). */
export const WAVE12_STATUS_PROMOTION_APPLY_FLAGS = Object.freeze([
  "--approve-wave12-brand-status-promotion",
  "--confirm-founder-approval",
  "--confirm-target-brands-only",
  "--confirm-status-to-active-or-live",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-content-writes",
  "--confirm-no-image-writes",
  "--confirm-no-protected-27-brand-changes",
  "--confirm-no-radisson-collection-changes",
]);

/** Stage 10 — release/restore fields + intentional public restore registry only. */
export const WAVE12_PUBLIC_RELEASE_APPLY_FLAGS = Object.freeze([
  "--approve-wave12-public-release",
  "--confirm-founder-visual-review-passed",
  "--confirm-brand-status-active-or-live",
  "--confirm-fully-ready",
  "--confirm-public-visibility-quality-lock-passed",
  "--confirm-target-brands-only",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-content-rewrites",
  "--confirm-no-image-writes",
  "--confirm-no-protected-27-brand-changes",
  "--confirm-no-radisson-collection-changes",
]);

/** Post-release freeze cleanup — bunkhouse / moxy / voco only. */
export const WAVE12_POST_RELEASE_FREEZE_CLEANUP_APPLY_FLAGS = Object.freeze([
  "--approve-wave12-post-release-freeze-cleanup",
  "--confirm-target-brands-only",
  "--confirm-targeted-field-fixes-only",
  "--confirm-no-company-validation-changes",
  "--confirm-no-source-library-status-changes",
  "--confirm-no-registry-approval-changes",
  "--confirm-no-brand-status-changes",
  "--confirm-no-release-field-changes",
  "--confirm-no-public-restore-registry-changes",
  "--confirm-no-image-writes-except-caption-only-if-flagged",
  "--confirm-no-other-active-brand-changes",
  "--confirm-no-radisson-collection-changes",
  "--confirm-no-broad-rewrites",
]);

/** Preferred Brand Status target — frozen 27 universe uses Active (not Live). */
export const WAVE12_STATUS_FROM = "Under Review";
export const WAVE12_STATUS_TO_PREFERRED = "Active";
export const WAVE12_STATUS_TO_ALLOWED = Object.freeze(["Active", "Live"]);

export const WAVE12_FOUNDER_APPROVE_RECOMMENDATION =
  "approve_for_status_promotion_and_public_release";

export const WAVE12_RELEASE_FIELDS = Object.freeze([
  "Active Profile Approved",
  "Ready for Active Profile",
  "Active Profile Approved Date",
  "Founder Visual Review Pass",
]);

export const WAVE12_NEVER_WRITE_FIELDS = Object.freeze([
  "Company Validated",
  "Company Validation Date",
  "Source Library status",
  "Registry approval/status",
]);

export function isWave12Slug(slug) {
  return WAVE12_SLUGS.includes(String(slug || "").trim().toLowerCase());
}

export function getWave12Plan(slug) {
  return WAVE12_BRAND_PLAN[String(slug || "").trim().toLowerCase()] || null;
}
