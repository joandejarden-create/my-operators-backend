#!/usr/bin/env node
import "dotenv/config";
import {
  buildLane2ImageAssetPackForBrand,
} from "../lib/partner-intelligence/brand-explorer-lane2-image-asset-pack.js";
import {
  planLane2BrandImageMaterialization,
  applyLane2ImageMaterializationPlans,
  REQUIRED_APPLY_FLAGS,
} from "../lib/partner-intelligence/brand-explorer-lane2-image-materialization.js";

const brands = [
  "bw-premier-collection",
  "bw-signature-collection",
  "preferred-hotels-and-resorts",
];

const argv = [
  "--apply",
  ...REQUIRED_APPLY_FLAGS,
];

const brandResults = [];
for (const slug of brands) {
  const pack = buildLane2ImageAssetPackForBrand(slug);
  const plan = await planLane2BrandImageMaterialization(slug, { assetPackBrand: pack });
  console.log(
    `${slug}: pack=${pack.pass} planBlocked=${plan.blocked} patches=${plan.presentationPatches?.length} blockers=${(plan.blockers || []).join(",") || "—"}`
  );
  brandResults.push(plan);
}

const applyResult = await applyLane2ImageMaterializationPlans({
  brandResults,
  apply: true,
  argv,
});
console.log(JSON.stringify({
  applied: applyResult.applied,
  reason: applyResult.reason,
  summary: applyResult.summary,
  byBrand: Object.fromEntries(
    Object.entries(applyResult.resultsByBrand || applyResult.byBrand || {}).map(([k, v]) => [
      k,
      { applied: v.applied, created: v.created?.length, updated: v.updated?.length, errors: v.errors?.length },
    ])
  ),
}, null, 2));
