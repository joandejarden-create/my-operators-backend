/**
 * Clean Lane 2 duplicate openings/gallery and thinner scenario duplicates.
 * Presentation-only; no release fields.
 */
import "dotenv/config";
import { listPresentationRowsLight, resolveLane2BrandIdentity } from "../lib/partner-intelligence/brand-explorer-lane2-common.js";
import { loadLane2GalleryPool, normalizePoolAssets } from "../lib/partner-intelligence/brand-explorer-lane2-common.js";
import { pickDistinctImageAssets } from "../lib/partner-intelligence/brand-explorer-image-uniqueness.js";
import { toAirtableFetchableImageUrl } from "../lib/partner-intelligence/brand-explorer-lane2-image-materialization.js";

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
  return j;
}

async function hide(recordId, reason) {
  await patch(recordId, {
    Active: false,
    "External Display Status": "Do Not Display",
  });
  console.log(`  hide ${recordId} (${reason})`);
}

// --- Handwritten: hide openings with empty body or duplicate title+image ---
{
  const id = resolveLane2BrandIdentity("handwritten-collection");
  const { rows } = await listPresentationRowsLight(id.recordId, id.name);
  const openings = rows.filter((r) => r.slotKey === "footprint.openings");
  const seen = new Set();
  for (const r of openings) {
    const key = `${r.title}|${r.imageUrl}`;
    if (!r.body || !r.body.trim()) {
      await hide(r.recordId, "empty body openings");
      continue;
    }
    if (seen.has(key)) {
      await hide(r.recordId, "duplicate openings");
      continue;
    }
    seen.add(key);
  }
  // Hide thinner scenario duplicates (shorter body when two share slot)
  for (const slot of ["overview.scenario.1", "overview.scenario.2", "overview.scenario.3"]) {
    const list = rows
      .filter((r) => r.slotKey === slot)
      .sort((a, b) => (b.body?.length || 0) - (a.body?.length || 0));
    for (const extra of list.slice(1)) {
      await hide(extra.recordId, `thinner duplicate ${slot}`);
    }
  }
}

// --- Radisson: replace gallery.5 with distinct image; hide empty openings ---
{
  const id = resolveLane2BrandIdentity("radisson-collection");
  const { rows } = await listPresentationRowsLight(id.recordId, id.name);
  for (const r of rows.filter((r) => r.slotKey === "footprint.openings" && !(r.body || "").trim())) {
    await hide(r.recordId, "empty body openings");
  }
  for (const slot of ["overview.scenario.1", "overview.scenario.2", "overview.scenario.3"]) {
    const list = rows
      .filter((r) => r.slotKey === slot)
      .sort((a, b) => (b.body?.length || 0) - (a.body?.length || 0));
    for (const extra of list.slice(1)) await hide(extra.recordId, `thinner duplicate ${slot}`);
  }
  const g4 = rows.find((r) => r.slotKey === "materials.gallery.4");
  const g5 = rows.find((r) => r.slotKey === "materials.gallery.5");
  const { accepted } = normalizePoolAssets(
    loadLane2GalleryPool("radisson-collection"),
    "radisson-collection"
  );
  const used = new Set(
    rows
      .filter((r) => String(r.slotKey).startsWith("materials.gallery") && r.slotKey !== "materials.gallery.5")
      .map((r) => r.imageUrl)
      .filter(Boolean)
  );
  // pick a pool URL not already used conceptually — use distinct from pool
  const picks = pickDistinctImageAssets(accepted, 12);
  let replacement = null;
  for (const p of picks) {
    // avoid same property photo as g4 if possible
    if (g4?.title && p.imageUrl && !String(g4.imageUrl || "").includes("airtable")) {
      replacement = p;
      break;
    }
    replacement = p;
  }
  // Prefer later pool entries for variety
  replacement = picks[Math.min(8, picks.length - 1)] || picks[0];
  if (g5?.recordId && replacement?.imageUrl) {
    await patch(g5.recordId, {
      Image: [{ url: toAirtableFetchableImageUrl(replacement.imageUrl) }],
      Title: `Design Detail — ${replacement.propertyName}`,
    });
    console.log("  radisson gallery.5 replaced", replacement.imageUrl.slice(0, 80));
  }
}

// --- Autograph / Tapestry / Vignette: hide thinner scenario dupes + empty openings ---
for (const slug of [
  "autograph-collection",
  "tapestry-collection-by-hilton",
  "vignette-collection",
]) {
  const id = resolveLane2BrandIdentity(slug);
  const { rows } = await listPresentationRowsLight(id.recordId, id.name);
  console.log(`\n${slug}`);
  for (const r of rows.filter((r) => r.slotKey === "footprint.openings" && !(r.body || "").trim())) {
    await hide(r.recordId, "empty body openings");
  }
  for (const slot of ["overview.scenario.1", "overview.scenario.2", "overview.scenario.3"]) {
    const list = rows
      .filter((r) => r.slotKey === slot)
      .sort((a, b) => (b.body?.length || 0) - (a.body?.length || 0));
    for (const extra of list.slice(1)) await hide(extra.recordId, `thinner duplicate ${slot}`);
  }
  // Vignette: if all openings same property, leave for founder judgment but ensure distinct images
  if (slug === "vignette-collection") {
    const openings = rows.filter(
      (r) => r.slotKey === "footprint.openings" && r.active !== false && (r.body || "").trim()
    );
    const { accepted } = normalizePoolAssets(
      loadLane2GalleryPool("vignette-collection"),
      "vignette-collection"
    );
    const byProp = new Map();
    for (const a of accepted) {
      if (!byProp.has(a.propertyKey)) byProp.set(a.propertyKey, a);
    }
    const props = [...byProp.values()].slice(0, 3);
    for (let i = 0; i < Math.min(openings.length, props.length); i++) {
      const a = props[i];
      await patch(openings[i].recordId, {
        Title: `${a.propertyName} — Property Example`,
        Body: `${a.propertyName} — directional Vignette Collection property example. Confirm independent character, design narrative, and IHG systems fit before treating this as a conversion path.`,
        Image: [{ url: toAirtableFetchableImageUrl(a.imageUrl) }],
      });
      console.log(`  vignette openings ${i + 1} -> ${a.propertyName}`);
      await new Promise((r) => setTimeout(r, 400));
    }
  }
}

console.log("\nDone cleanup.");
