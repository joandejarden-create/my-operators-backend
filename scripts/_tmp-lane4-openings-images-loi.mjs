#!/usr/bin/env node
import "dotenv/config";
import fs from "fs";
import { getBrandLibraryBrandById } from "../api/brand-library.js";
import { toAirtableFetchableImageUrl } from "../lib/partner-intelligence/brand-explorer-lane2-image-materialization.js";

const baseId = process.env.AIRTABLE_BASE_ID;
const apiKey = process.env.AIRTABLE_API_KEY;
const table = "Brand Setup - Brand Explorer Presentation";

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

function nz(v) {
  return v == null ? "" : String(v).trim();
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
  if (!res.ok) throw new Error(`${recordId}: ${json.error?.message || res.status}`);
  return json;
}

const brands = [
  {
    slug: "bw-premier-collection",
    id: "recwXZ5gVZ8ZH8ekA",
    pool: "fixtures/lane2-bw-premier-collection-gallery-pool.json",
  },
  {
    slug: "bw-signature-collection",
    id: "recdeh1NsP4gjrv80",
    pool: "fixtures/lane2-bw-signature-collection-gallery-pool.json",
  },
  {
    slug: "preferred-hotels-and-resorts",
    id: "recwl5JOYxlChuCAr",
    pool: "fixtures/lane2-preferred-hotels-and-resorts-gallery-pool.json",
  },
];

for (const b of brands) {
  const res = mockRes();
  await getBrandLibraryBrandById({ query: { brandId: b.id }, headers: {} }, res);
  const blocks = res.payload.brand.brandExplorer.blocks || [];
  const pool = JSON.parse(fs.readFileSync(b.pool, "utf8"));
  console.log(`\n=== ${b.slug} ===`);

  // Scrub LOI anywhere on openings
  for (const row of blocks.filter((r) => r.slotKey === "footprint.openings")) {
    const fields = {};
    for (const [api, airtable] of [
      ["body", "Body"],
      ["caseSummaryOverview", "Case Summary Overview"],
      ["caseSummaryInterpretation", "Case Summary Interpretation"],
      ["caseSummaryBrandRelevance", "Case Summary Brand Relevance"],
      ["caseSummaryOwnerObjective", "Case Summary Owner Objective"],
      ["caseSummaryTags", "Case Summary Tags"],
    ]) {
      const val = nz(row[api]);
      if (/\bLOI\b/i.test(val)) {
        fields[airtable] = val.replace(/\bLOI\b/gi, "affiliation agreement");
      }
    }
    if (Object.keys(fields).length) {
      await patch(row.recordId, fields);
      console.log("LOI scrub", row.recordId, Object.keys(fields));
    }
  }

  // Attach images to openings missing Image
  const openings = blocks.filter((r) => r.slotKey === "footprint.openings");
  const usedUrls = new Set(
    blocks.filter((r) => r.imageUrl).map((r) => String(r.imageUrl).split("?")[0])
  );
  let poolIdx = 0;
  for (const row of openings) {
    if (row.imageUrl) continue;
    let candidate = null;
    // Prefer matching property name
    const title = nz(row.title).toLowerCase();
    candidate =
      pool.find(
        (p) =>
          p.imageUrl &&
          !usedUrls.has(p.imageUrl.split("?")[0]) &&
          title.includes(String(p.propertyKey || "").toLowerCase())
      ) ||
      pool.find(
        (p) =>
          p.imageUrl &&
          !usedUrls.has(p.imageUrl.split("?")[0]) &&
          title.includes(String(p.propertyName || "").toLowerCase().split(" ")[0])
      );
    while (!candidate && poolIdx < pool.length) {
      const p = pool[poolIdx++];
      if (p.imageUrl && !usedUrls.has(p.imageUrl.split("?")[0])) candidate = p;
    }
    if (!candidate?.imageUrl) {
      console.log("no image for openings", row.recordId, row.title);
      continue;
    }
    const imageUrl = toAirtableFetchableImageUrl(candidate.imageUrl);
    usedUrls.add(candidate.imageUrl.split("?")[0]);
    try {
      await patch(row.recordId, { Image: [{ url: imageUrl }] });
      console.log("openings image", row.recordId, candidate.propertyKey || candidate.imageUrl.slice(0, 60));
    } catch (err) {
      console.log("openings image FAIL", row.recordId, err.message);
    }
  }

  // Restore missing gallery images
  for (let i = 1; i <= 6; i++) {
    const slot = `materials.gallery.${i}`;
    const row = blocks.find((r) => r.slotKey === slot);
    if (!row?.recordId || row.imageUrl) continue;
    const candidate = pool.find((p) => p.imageUrl && !usedUrls.has(p.imageUrl.split("?")[0]));
    if (!candidate) {
      console.log("no pool image for", slot);
      continue;
    }
    const imageUrl = toAirtableFetchableImageUrl(candidate.imageUrl);
    usedUrls.add(candidate.imageUrl.split("?")[0]);
    try {
      await patch(row.recordId, { Image: [{ url: imageUrl }] });
      console.log("gallery image restored", slot, candidate.imageUrl.slice(0, 70));
    } catch (err) {
      console.log("gallery FAIL", slot, err.message);
    }
  }
}

console.log("\nDone.");
