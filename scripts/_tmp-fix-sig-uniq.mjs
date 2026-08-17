#!/usr/bin/env node
import "dotenv/config";
import fs from "fs";
import { getBrandLibraryBrandById } from "../api/brand-library.js";
import {
  buildImageIdentity,
  evaluateImageUniqueness,
} from "../lib/partner-intelligence/brand-explorer-image-uniqueness.js";
import { toAirtableFetchableImageUrl } from "../lib/partner-intelligence/brand-explorer-lane2-image-materialization.js";

const baseId = process.env.AIRTABLE_BASE_ID;
const apiKey = process.env.AIRTABLE_API_KEY;
const table = "Brand Setup - Brand Explorer Presentation";

function cleanUrl(u) {
  const s = String(u || "");
  const m = s.match(/^https?:\/\/[^\s"'<>]+\.(?:jpg|jpeg|png|webp)(?:\?[^\s"'<>]*)?/i);
  return m ? m[0] : "";
}

function mockRes() {
  return {
    headers: {},
    setHeader() {},
    status(c) {
      this.statusCode = c;
      return this;
    },
    json(p) {
      this.payload = p;
    },
  };
}

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
  const json = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(json.error?.message || res.status);
}

for (const brand of [
  "bw-premier-collection",
  "bw-signature-collection",
  "preferred-hotels-and-resorts",
]) {
  const p = `fixtures/lane2-${brand}-gallery-pool.json`;
  const rows = JSON.parse(fs.readFileSync(p, "utf8"))
    .map((r) => ({ ...r, imageUrl: cleanUrl(r.imageUrl) }))
    .filter((r) => r.imageUrl && !/hugedomains|parking/i.test(r.imageUrl));
  const dedup = [];
  const seen = new Set();
  for (const r of rows) {
    if (seen.has(r.imageUrl)) continue;
    seen.add(r.imageUrl);
    dedup.push(r);
  }
  fs.writeFileSync(p, `${JSON.stringify(dedup, null, 2)}\n`);
  console.log("cleaned", brand, dedup.length);
}

const res = mockRes();
await getBrandLibraryBrandById({ query: { brandId: "recdeh1NsP4gjrv80" }, headers: {} }, res);
const brand = res.payload.brand;
const blocks = brand.brandExplorer.blocks || [];
const uniq = evaluateImageUniqueness({
  brand,
  presentationRows: blocks,
  brandSlug: "bw-signature-collection",
});
const used = new Set();
for (const g of uniq.gallery || []) {
  if (g.sourceImageId) used.add(g.sourceImageId);
  if (g.normalizedFilename) used.add(String(g.normalizedFilename).toLowerCase());
  if (g.duplicateGroupId) used.add(g.duplicateGroupId);
}
const pool = JSON.parse(
  fs.readFileSync("fixtures/lane2-bw-signature-collection-gallery-pool.json", "utf8")
);
const candidate = pool.find((p) => {
  const id = buildImageIdentity(p.imageUrl, {});
  return (
    !used.has(id.sourceImageId) &&
    !used.has(String(id.normalizedFilename || "").toLowerCase()) &&
    !used.has(id.duplicateGroupId)
  );
});
console.log("candidate", candidate?.imageUrl);
if (!candidate) {
  console.error("no candidate");
  process.exit(1);
}
const g3 = blocks.find((b) => b.slotKey === "materials.gallery.3");
await patch(g3.recordId, {
  Image: [{ url: toAirtableFetchableImageUrl(candidate.imageUrl) }],
});
const res2 = mockRes();
await getBrandLibraryBrandById({ query: { brandId: "recdeh1NsP4gjrv80" }, headers: {} }, res2);
const uniq2 = evaluateImageUniqueness({
  brand: res2.payload.brand,
  presentationRows: res2.payload.brand.brandExplorer.blocks,
  brandSlug: "bw-signature-collection",
});
console.log({
  pass: uniq2.pass,
  gallery: uniq2.galleryDistinctCount,
  prop: uniq2.propertyExampleDistinctCount,
  dups: (uniq2.duplicates || []).map((d) => d.slots),
});
