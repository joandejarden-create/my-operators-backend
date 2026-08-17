/**
 * Image-only rematerialize for Lane 2 — PATCH Image field only, verify each write.
 */
import "dotenv/config";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  buildLane2ImageAssetPackForBrand,
} from "../lib/partner-intelligence/brand-explorer-lane2-image-asset-pack.js";
import { planLane2BrandImageMaterialization } from "../lib/partner-intelligence/brand-explorer-lane2-image-materialization.js";
import { resolveFullBuildSlug } from "../lib/partner-intelligence/brand-explorer-full-build-content.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const baseId = process.env.AIRTABLE_BASE_ID;
const apiKey = process.env.AIRTABLE_API_KEY;
const table = "Brand Setup - Brand Explorer Presentation";

const brands = [
  "autograph-collection",
  "handwritten-collection",
  "radisson-collection",
  "tapestry-collection-by-hilton",
  "vignette-collection",
];

async function patchImage(recordId, imageUrl) {
  const res = await fetch(
    `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${recordId}`,
    {
      method: "PATCH",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ fields: { Image: [{ url: imageUrl }] } }),
    }
  );
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || `HTTP ${res.status}`);
  return Boolean(json.fields?.Image?.[0]?.url);
}

async function readHasImage(recordId) {
  const res = await fetch(
    `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${recordId}`,
    { headers: { Authorization: `Bearer ${apiKey}` } }
  );
  const json = await res.json();
  return Boolean(json.fields?.Image?.[0]?.url);
}

const summary = [];
for (const brand of brands) {
  const pack = buildLane2ImageAssetPackForBrand(brand);
  const plan = await planLane2BrandImageMaterialization(brand, { assetPackBrand: pack });
  console.log(`\n${brand} patches=${plan.presentationPatches.length} blocked=${plan.blocked}`);
  let ok = 0;
  let fail = 0;
  for (const patch of plan.presentationPatches) {
    if (!patch.recordId) {
      console.log(`  SKIP create-needed ${patch.planSlotKey}`);
      fail += 1;
      continue;
    }
    try {
      const wrote = await patchImage(patch.recordId, patch.imageUrl);
      await new Promise((r) => setTimeout(r, 350));
      const verified = await readHasImage(patch.recordId);
      console.log(
        `  ${verified ? "OK" : "FAIL"} ${patch.planSlotKey || patch.slotKey} wrote=${wrote} verified=${verified}`
      );
      if (verified) ok += 1;
      else fail += 1;
    } catch (err) {
      console.log(`  ERR ${patch.planSlotKey}: ${err.message}`);
      fail += 1;
    }
  }
  summary.push({ brand, ok, fail });
}

console.log("\nSUMMARY", summary);
fs.writeFileSync(
  path.join(ROOT, "reports", "brand-explorer-lane2-image-rematerialize-verify.json"),
  `${JSON.stringify({ generatedAt: new Date().toISOString(), summary }, null, 2)}\n`
);
