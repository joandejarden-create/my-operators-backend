#!/usr/bin/env node
/**
 * Targeted Lane 4 post-image remediation:
 * - fill openings modal placeholders
 * - scrub LOI on Preferred openings
 * - restore missing Signature gallery.2 image
 * - align gallery titles to role captions for role-match
 */
import "dotenv/config";
import { getBrandLibraryBrandById } from "../api/brand-library.js";
import { GALLERY_ROLE_CAPTIONS, DEFAULT_GALLERY_ROLE_SEQUENCE } from "../lib/partner-intelligence/brand-explorer-image-role-match.js";
import { buildLane2ImageAssetPackForBrand } from "../lib/partner-intelligence/brand-explorer-lane2-image-asset-pack.js";
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

function isEmpty(v) {
  const t = nz(v);
  return !t || t === "—" || t === "-";
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

const targets = [
  { slug: "bw-premier-collection", id: "recwXZ5gVZ8ZH8ekA" },
  { slug: "bw-signature-collection", id: "recdeh1NsP4gjrv80" },
  { slug: "preferred-hotels-and-resorts", id: "recwl5JOYxlChuCAr" },
];

for (const t of targets) {
  const res = mockRes();
  await getBrandLibraryBrandById({ query: { brandId: t.id }, headers: {} }, res);
  const brand = res.payload.brand;
  const blocks = brand.brandExplorer?.blocks || [];
  console.log(`\n=== ${t.slug} ===`);

  // 1) openings modal fill + LOI scrub
  const openings = blocks.filter((b) => b.slotKey === "footprint.openings" && b.recordId);
  for (const row of openings) {
    const fields = {};
    const overview =
      nz(row.caseSummaryOverview) ||
      nz(row.body)
        .split("\n")
        .map((l) => l.trim())
        .filter((l) => l && !/^https?:\/\//i.test(l) && !/^[A-Z /]+$/.test(l))
        .slice(-1)[0] ||
      `${nz(row.title)} is a published property reference for owners comparing conversion fit and commercial systems under this brand.`;
    if (isEmpty(row.caseSummaryOverview)) fields["Case Summary Overview"] = overview.slice(0, 500);
    if (isEmpty(row.caseSummaryTags)) {
      fields["Case Summary Tags"] = [row.marketCity, "Soft Brand", "Owner reference"]
        .filter(Boolean)
        .join(", ");
    }
    if (isEmpty(row.caseSummaryBrandRelevance)) {
      fields["Case Summary Brand Relevance"] =
        "Published member/collection listing used as positioning context for owner diligence—not a performance proxy.";
    }
    if (isEmpty(row.caseSummaryOwnerObjective)) {
      fields["Case Summary Owner Objective"] =
        "Benchmark conversion intensity, guest-experience capital, and platform participation against this property type.";
    }
    if (isEmpty(row.caseSummaryInterpretation) || /\\bLOI\\b/i.test(nz(row.caseSummaryInterpretation))) {
      fields["Case Summary Interpretation"] =
        "Use as a conversion and affiliation reference only; confirm agreement-specific terms and capital scope directly with the brand.";
    }
    // Body LOI scrub
    if (/\\bLOI\\b/i.test(nz(row.body))) {
      fields.Body = nz(row.body).replace(/\\bLOI\\b/gi, "affiliation agreement");
    }
    if (Object.keys(fields).length) {
      await patch(row.recordId, fields);
      console.log("patched openings", row.recordId, Object.keys(fields));
    }
  }

  // 2) gallery title role alignment
  for (let i = 0; i < DEFAULT_GALLERY_ROLE_SEQUENCE.length; i++) {
    const slot = `materials.gallery.${i + 1}`;
    const role = DEFAULT_GALLERY_ROLE_SEQUENCE[i];
    const caption = GALLERY_ROLE_CAPTIONS[role] || `Gallery ${i + 1}`;
    const row = blocks.find((b) => b.slotKey === slot);
    if (!row?.recordId) continue;
    const title = nz(row.title);
    if (!title || !title.toLowerCase().includes(caption.split(" ")[0].toLowerCase())) {
      const propertyHint = title.includes("—") ? title.split("—").slice(1).join("—").trim() : "";
      const nextTitle = propertyHint ? `${caption} — ${propertyHint}` : caption;
      await patch(row.recordId, { Title: nextTitle });
      console.log("gallery title", slot, nextTitle);
    }
  }

  // 3) Signature missing gallery.2
  if (t.slug === "bw-signature-collection") {
    const g2 = blocks.find((b) => b.slotKey === "materials.gallery.2");
    if (g2?.recordId && !g2.imageUrl) {
      const pack = buildLane2ImageAssetPackForBrand(t.slug);
      const candidate =
        pack.visualAssetPack?.galleryCandidates?.[1] ||
        pack.visualAssetPack?.galleryCandidates?.[0];
      if (candidate?.imageUrl) {
        const imageUrl = toAirtableFetchableImageUrl(candidate.imageUrl);
        await patch(g2.recordId, {
          Title: GALLERY_ROLE_CAPTIONS[DEFAULT_GALLERY_ROLE_SEQUENCE[1]] || "Guest Room / Suite",
          Image: [{ url: imageUrl }],
        });
        console.log("restored gallery.2", imageUrl.slice(0, 80));
      }
    }
  }
}

console.log("\nDone.");
