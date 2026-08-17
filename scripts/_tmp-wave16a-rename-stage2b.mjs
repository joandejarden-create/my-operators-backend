import fs from "node:fs";

const p = "lib/partner-intelligence/brand-explorer-wave16a-stage2b-image-materialization.js";
let s = fs.readFileSync(p, "utf8");

const pairs = [
  ["Wave 14 Stage 5", "Wave 16A Stage 2B"],
  ["Wave 14", "Wave 16A Stage 2B"],
  ["wave14-image-materialization-v1", "wave16a-stage2b-image-materialization-v1"],
  ["WAVE14_IMAGE_MATERIALIZATION_VERSION", "WAVE16A_STAGE2B_IMAGE_VERSION"],
  ["WAVE14_IMAGE_MATERIALIZATION_APPLY_FLAGS", "WAVE16A_STAGE2B_APPLY_FLAGS"],
  ["parseWave14ImageMaterializationFlags", "parseWave16aStage2bImageFlags"],
  ["resolveWave14ImageIdentity", "resolveWave16aStage2bImageIdentity"],
  ["loadWave14GalleryPool", "loadWave16aStage2bGalleryPool"],
  ["isWave14RejectedImageUrl", "isWave16aStage2bRejectedImageUrl"],
  ["normalizeWave14Pool", "normalizeWave16aStage2bPool"],
  ["buildWave14ImageAssetPackForBrand", "buildWave16aStage2bImageAssetPackForBrand"],
  ["planWave14BrandImageMaterialization", "planWave16aStage2bBrandImageMaterialization"],
  ["applyWave14ImageMaterializationPlans", "applyWave16aStage2bImageMaterializationPlans"],
  ["runWave14ImageMaterialization", "runWave16aStage2bImageMaterialization"],
  ["isWave14PropertyHoldSlug", "isWave16aStage2bPropertyHoldSlug"],
  ["isWave14Stage5Slug", "isWave16aStage2bSlug"],
  ["getWave14CuratedPoolSeed", "getWave16aStage2bCuratedPoolSeed"],
  ["getWave14SupplementalOpenings", "getWave16aStage2bSupplementalOpenings"],
  ["getWave14SourcePack", "getWave16aStage2bSourcePack"],
  ["WAVE14_STAGE4_APPROVED_SLUGS", "WAVE16A_STAGE2B_APPROVED_SLUGS"],
  ["WAVE14_FORBIDDEN_WRITE_FIELDS", "WAVE16A_STAGE2B_FORBIDDEN_WRITE_FIELDS"],
  ["WAVE14_VERSION", "WAVE16A_VERSION"],
  ["brand-explorer-wave14-image-materialization", "brand-explorer-wave16a-stage2b-image-materialization"],
  [
    "wave14_image_materialization_ready_for_post_image_cleanup",
    "wave16a_stage2b_low_risk_images_complete_ready_for_post_image_review",
  ],
  [
    "wave14_stage5_image_materialization_dry_run_ready",
    "wave16a_stage2b_image_materialization_dry_run_ready",
  ],
  ["wave14-${s}-gallery-pool", "wave16a-${s}-gallery-pool"],
  ["[wave14-image]", "[wave16a-stage2b-image]"],
  ["EXPECTED_ACTIVE_COUNT_46", "EXPECTED_ACTIVE_COUNT_62"],
  ["protected 46", "protected 62"],
  ["noProtected46", "noProtected62"],
  ["--confirm-no-protected-46-brand-changes", "--confirm-active-62-protected"],
  ["--confirm-nine-brand-stage5-scope", "--confirm-three-brand-scope"],
  ["--approve-wave14-image-materialization", "--approve-wave16a-stage2b-image-materialization"],
  ["--confirm-no-accor-wave13-active-brand-writes", "--confirm-no-wave16b-writes"],
  ["--confirm-studiores-not-residence-inn-or-towneplace", "--confirm-no-recent-momentum-writes"],
  [
    "--confirm-cleanly-unavailable-for-unsupported-property-images",
    "--confirm-no-four-points-flex-writes",
  ],
];

for (const [a, b] of pairs) s = s.split(a).join(b);

fs.writeFileSync(p, s);
console.log("renamed ok", s.length);
