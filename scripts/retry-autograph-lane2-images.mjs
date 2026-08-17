/**
 * Retry Autograph (+ any failed) image writes with longer delays and preferred Marriott URLs.
 */
import "dotenv/config";
import fs from "fs";
import {
  buildLane2ImageAssetPackForBrand,
} from "../lib/partner-intelligence/brand-explorer-lane2-image-asset-pack.js";
import { planLane2BrandImageMaterialization } from "../lib/partner-intelligence/brand-explorer-lane2-image-materialization.js";
import { loadLane2GalleryPool, normalizePoolAssets } from "../lib/partner-intelligence/brand-explorer-lane2-common.js";
import { pickDistinctImageAssets, buildImageIdentity } from "../lib/partner-intelligence/brand-explorer-image-uniqueness.js";

const baseId = process.env.AIRTABLE_BASE_ID;
const apiKey = process.env.AIRTABLE_API_KEY;
const table = "Brand Setup - Brand Explorer Presentation";

function preferMarriottUrl(url) {
  // Prefer content/dam renditions over Scene7 is/image (more stable for Airtable fetch).
  if (/cache\.marriott\.com\/content\/dam\/marriott-renditions\//i.test(url)) return 2;
  if (/cache\.marriott\.com\/is\/image\/marriotts7prod\//i.test(url)) return 1;
  return 0;
}

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
  await new Promise((r) => setTimeout(r, 800));
  const res = await fetch(
    `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(table)}/${recordId}`,
    { headers: { Authorization: `Bearer ${apiKey}` } }
  );
  const json = await res.json();
  return Boolean(json.fields?.Image?.[0]?.url);
}

const { accepted } = normalizePoolAssets(loadLane2GalleryPool("autograph-collection"), "autograph-collection");
accepted.sort((a, b) => preferMarriottUrl(b.imageUrl) - preferMarriottUrl(a.imageUrl));

const pack = buildLane2ImageAssetPackForBrand("autograph-collection");
// Rebuild gallery candidates preferring content/dam
const gallery = pickDistinctImageAssets(
  accepted.filter((a) => preferMarriottUrl(a.imageUrl) >= 2),
  6
);
const used = gallery.map((g) => g._imageIdentity.duplicateGroupId);
const propertyKeys = ["mspak-emery", "chidx-emc2", "mciak-raphael"];
const properties = [];
for (const key of propertyKeys) {
  const pool = accepted.filter(
    (a) => a.propertyKey === key && preferMarriottUrl(a.imageUrl) >= 1
  );
  const pick = pickDistinctImageAssets(pool, 1, { excludeGroupIds: used });
  if (pick[0]) {
    used.push(pick[0]._imageIdentity.duplicateGroupId);
    properties.push(pick[0]);
  }
}
const scenarios = pickDistinctImageAssets(accepted, 3, { excludeGroupIds: used });

pack.visualAssetPack = {
  galleryCandidates: gallery.map((a, i) => ({
    ...a,
    slotKey: `materials.gallery.${i + 1}`,
    title: a.title || `Gallery ${i + 1} — ${a.propertyName}`,
    role: "gallery",
  })),
  scenarioCandidates: scenarios.map((a, i) => ({
    ...a,
    slotKey: `overview.scenario.${i + 1}`,
    role: "scenario",
  })),
  propertyExampleCandidates: properties.map((a, i) => ({
    ...a,
    slotKey: "footprint.openings",
    planSlotKey: `footprint.openings.${i + 1}`,
    title: `${a.propertyName} — Property Example`,
    body: `${a.propertyName} — directional Autograph Collection property example. Confirm design narrative strength, public-space capital, and Marriott systems fit before treating this as a conversion path.`,
    role: "property_example",
  })),
};
pack.pass = true;
pack.status = "asset_pack_ready";
pack.blockers = [];

const plan = await planLane2BrandImageMaterialization("autograph-collection", {
  assetPackBrand: pack,
});

// Also patch openings body/title
async function patchFields(recordId, fields) {
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
  const json = await res.json();
  if (!res.ok) throw new Error(json.error?.message || `HTTP ${res.status}`);
  return json;
}

let ok = 0;
let fail = 0;
const attempts = [];
for (const patch of plan.presentationPatches) {
  if (!patch.recordId) {
    fail += 1;
    continue;
  }
  // Try primary URL then up to 2 alternates from same property
  const candidates = [patch.imageUrl];
  const propKey = pack.visualAssetPack.galleryCandidates
    .concat(pack.visualAssetPack.propertyExampleCandidates)
    .concat(pack.visualAssetPack.scenarioCandidates)
    .find((c) => c.imageUrl === patch.imageUrl)?.propertyKey;
  if (propKey) {
    for (const a of accepted) {
      if (a.propertyKey === propKey && a.imageUrl !== patch.imageUrl && preferMarriottUrl(a.imageUrl) >= 2) {
        candidates.push(a.imageUrl);
      }
      if (candidates.length >= 4) break;
    }
  }

  let verified = false;
  let usedUrl = null;
  for (const url of candidates) {
    try {
      await patchImage(patch.recordId, url);
      verified = await readHasImage(patch.recordId);
      if (verified) {
        usedUrl = url;
        break;
      }
    } catch (err) {
      attempts.push({ slot: patch.planSlotKey, url, error: err.message });
    }
    await new Promise((r) => setTimeout(r, 500));
  }

  if (verified && patch.fields?.Title) {
    const meta = {};
    if (patch.fields.Title) meta.Title = patch.fields.Title;
    if (patch.fields.Body) meta.Body = patch.fields.Body;
    if (Object.keys(meta).length) await patchFields(patch.recordId, meta);
  }

  console.log(
    `${verified ? "OK" : "FAIL"} ${patch.planSlotKey || patch.slotKey} ${usedUrl ? usedUrl.slice(0, 80) : ""}`
  );
  if (verified) ok += 1;
  else fail += 1;
  await new Promise((r) => setTimeout(r, 600));
}

// Fix radisson gallery.5 too
const radPlan = await planLane2BrandImageMaterialization("radisson-collection");
const g5 = radPlan.presentationPatches.find((p) => p.planSlotKey === "materials.gallery.5");
if (g5?.recordId) {
  const pool = normalizePoolAssets(
    loadLane2GalleryPool("radisson-collection"),
    "radisson-collection"
  ).accepted;
  let fixed = false;
  for (const a of pool) {
    await patchImage(g5.recordId, a.imageUrl);
    if (await readHasImage(g5.recordId)) {
      console.log("OK radisson gallery.5", a.imageUrl.slice(0, 80));
      fixed = true;
      break;
    }
  }
  if (!fixed) console.log("FAIL radisson gallery.5");
}

console.log({ autograph: { ok, fail } });
fs.writeFileSync(
  "reports/brand-explorer-lane2-autograph-image-retry.json",
  `${JSON.stringify({ ok, fail, attempts: attempts.slice(0, 20) }, null, 2)}\n`
);
