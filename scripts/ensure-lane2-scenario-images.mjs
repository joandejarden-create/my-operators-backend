/**
 * Ensure active scenario rows have distinct images (after thinner-dupe hide).
 */
import "dotenv/config";
import {
  listPresentationRowsLight,
  resolveLane2BrandIdentity,
  loadLane2GalleryPool,
  normalizePoolAssets,
} from "../lib/partner-intelligence/brand-explorer-lane2-common.js";
import { pickDistinctImageAssets } from "../lib/partner-intelligence/brand-explorer-image-uniqueness.js";
import { toAirtableFetchableImageUrl } from "../lib/partner-intelligence/brand-explorer-lane2-image-materialization.js";
import { buildLane2ImageAssetPackForBrand } from "../lib/partner-intelligence/brand-explorer-lane2-image-asset-pack.js";

const baseId = process.env.AIRTABLE_BASE_ID;
const apiKey = process.env.AIRTABLE_API_KEY;
const table = "Brand Setup - Brand Explorer Presentation";

async function patch(recordId, fields) {
  const res = await fetch(
    `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${recordId}`,
    {
      method: "PATCH",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ fields }),
    }
  );
  const j = await res.json();
  if (!res.ok) throw new Error(j.error?.message || `HTTP ${res.status}`);
  await new Promise((r) => setTimeout(r, 500));
  return j;
}

const brands = [
  "autograph-collection",
  "handwritten-collection",
  "radisson-collection",
  "tapestry-collection-by-hilton",
  "vignette-collection",
];

for (const slug of brands) {
  const id = resolveLane2BrandIdentity(slug);
  const { rows } = await listPresentationRowsLight(id.recordId, id.name);
  const pack = buildLane2ImageAssetPackForBrand(slug);
  const scenarioAssets = pack.visualAssetPack?.scenarioCandidates || [];
  const { accepted } = normalizePoolAssets(loadLane2GalleryPool(slug), slug);
  const galleryUrls = new Set(
    rows
      .filter((r) => String(r.slotKey).startsWith("materials.gallery") && r.active !== false)
      .map((r) => r.imageUrl)
      .filter(Boolean)
  );

  console.log(`\n${slug}`);
  for (let i = 1; i <= 3; i++) {
    const slot = `overview.scenario.${i}`;
    const active = rows.filter(
      (r) =>
        r.slotKey === slot &&
        r.active !== false &&
        !/do not display/i.test(r.externalDisplayStatus || "")
    );
    // Prefer row with longest body among active
    active.sort((a, b) => (b.body?.length || 0) - (a.body?.length || 0));
    const target = active[0];
    if (!target) {
      console.log(`  missing active ${slot}`);
      continue;
    }
    // Hide other active duplicates if any remain
    for (const extra of active.slice(1)) {
      await patch(extra.recordId, { Active: false, "External Display Status": "Do Not Display" });
      console.log(`  hide extra ${slot}`);
    }

    let asset = scenarioAssets[i - 1];
    if (!asset?.imageUrl) {
      const picks = pickDistinctImageAssets(accepted, 1, { excludeGroupIds: [] });
      asset = picks[0];
    }
    const url = toAirtableFetchableImageUrl(asset.imageUrl);
    await patch(target.recordId, { Image: [{ url }] });
    // verify
    const check = await fetch(
      `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${target.recordId}`,
      { headers: { Authorization: `Bearer ${apiKey}` } }
    );
    const cj = await check.json();
    const ok = Boolean(cj.fields?.Image?.[0]?.url);
    console.log(`  ${ok ? "OK" : "FAIL"} ${slot} ${target.title.slice(0, 40)}`);
  }
}
console.log("\nDone scenario image ensure.");
