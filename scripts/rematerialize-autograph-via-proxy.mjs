/**
 * Rematerialize Autograph images via wsrv.nl proxy (Marriott CDN blocks Airtable fetch).
 */
import "dotenv/config";
import {
  buildLane2ImageAssetPackForBrand,
} from "../lib/partner-intelligence/brand-explorer-lane2-image-asset-pack.js";
import {
  planLane2BrandImageMaterialization,
  toAirtableFetchableImageUrl,
} from "../lib/partner-intelligence/brand-explorer-lane2-image-materialization.js";

const baseId = process.env.AIRTABLE_BASE_ID;
const apiKey = process.env.AIRTABLE_API_KEY;
const table = "Brand Setup - Brand Explorer Presentation";

async function patchImage(recordId, imageUrl) {
  const fetchUrl = toAirtableFetchableImageUrl(imageUrl);
  const res = await fetch(
    `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${recordId}`,
    {
      method: "PATCH",
      headers: {
        Authorization: `Bearer ${apiKey}`,
        "Content-Type": "application/json",
      },
      body: JSON.stringify({ fields: { Image: [{ url: fetchUrl }] } }),
    }
  );
  const json = await res.json();
  if (!res.ok) throw new Error(json.error?.message || `HTTP ${res.status}`);
  await new Promise((r) => setTimeout(r, 900));
  const g = await fetch(
    `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${recordId}`,
    { headers: { Authorization: `Bearer ${apiKey}` } }
  );
  const gj = await g.json();
  return Boolean(gj.fields?.Image?.[0]?.url);
}

async function patchMeta(recordId, fields) {
  if (!Object.keys(fields || {}).length) return;
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
  if (!res.ok) {
    const j = await res.json().catch(() => ({}));
    throw new Error(j.error?.message || `meta HTTP ${res.status}`);
  }
}

const pack = buildLane2ImageAssetPackForBrand("autograph-collection");
const plan = await planLane2BrandImageMaterialization("autograph-collection", {
  assetPackBrand: pack,
});

let ok = 0;
let fail = 0;
for (const patch of plan.presentationPatches) {
  if (!patch.recordId) {
    console.log("SKIP", patch.planSlotKey);
    fail += 1;
    continue;
  }
  try {
    const meta = {};
    if (patch.fields?.Title) meta.Title = patch.fields.Title;
    if (patch.fields?.Body) meta.Body = patch.fields.Body;
    await patchMeta(patch.recordId, meta);
    const verified = await patchImage(patch.recordId, patch.imageUrl);
    console.log(`${verified ? "OK" : "FAIL"} ${patch.planSlotKey || patch.slotKey}`);
    if (verified) ok += 1;
    else fail += 1;
  } catch (err) {
    console.log(`ERR ${patch.planSlotKey}: ${err.message}`);
    fail += 1;
  }
}
console.log({ ok, fail });
