#!/usr/bin/env node
/**
 * Patch Recent Momentum cards for BW Premier, BW Signature, Preferred + thicken Preferred thin fields.
 * Presentation Body/Title only. No CV/Source/Registry/Brand Status/release/image writes.
 */
import "dotenv/config";
import { buildRecentMomentumCard } from "../lib/partner-intelligence/brand-explorer-recent-momentum-contract.js";
import { evaluateBrandPublicVisibility } from "../lib/partner-intelligence/brand-explorer-public-visibility-quality-lock.js";
import { getBrandLibraryBrandById } from "../api/brand-library.js";
import { isOwnerFacingPresentationRow } from "../lib/partner-intelligence/brand-explorer-public-visibility-quality-lock.js";

const APPLY = process.argv.includes("--apply");
const PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";

const MOMENTUM = {
  "bw-premier-collection": [
    {
      recordId: "recS02hxbMuHn3MxO",
      ...buildRecentMomentumCard({
        title: "Hotel Eastlund Joins BW Premier Collection In Portland",
        dateLine: "2024",
        summary:
          "Hotel Eastlund joined BW Premier Collection in Portland, Oregon—BWH's first Premier location in the city and a conversion signal for owners evaluating upscale independent urban hotels on the BWH platform.",
        url: "https://hotelbusiness.com/best-western-opens-eight-properties/",
        sort: 1,
      }),
    },
    {
      recordId: "recbP2oNAO2fcP2My",
      ...buildRecentMomentumCard({
        title: "BWH Upscale Sales Push Highlights Premier Collection Assets",
        dateLine: "2025",
        summary:
          "BWH Hotels launched a dedicated upscale and luxury worldwide sales division featuring Premier Collection hotels such as The Whitehall Hotel in Chicago and Orakai Songdo Park—useful context for owners weighing commercial support behind Premier positioning.",
        url: "https://hotelbusiness.com/bwh-hotels-launches-global-upscale-luxury-sales-division/",
        sort: 2,
      }),
    },
    {
      recordId: "recXLGpm9Kof7OPQf",
      ...buildRecentMomentumCard({
        title: "Zion Wildflower Explorer Series Debuts Under Premier Collection",
        dateLine: "Mar 2025",
        summary:
          "BWH introduced an Explorer Series glamping path within the Premier Collection at Zion Wildflower—illustrating how distinctive destination assets can seek BWH distribution while keeping a non-prototype guest experience.",
        url: "https://hotelbusiness.com/hb-on-the-scene-bwh-hotels-enters-glamping-market/",
        sort: 3,
      }),
    },
  ],
  "bw-signature-collection": [
    {
      recordId: "recLfag75nGEPYEX8",
      ...buildRecentMomentumCard({
        title: "Hotel Gio Stockholm Featured As BW Signature Collection Example",
        dateLine: "2025",
        summary:
          "Hotel Gio in Stockholm was highlighted among BWH upscale and luxury sales assets as a BW Signature Collection hotel—useful for owners comparing Signature's independent-identity path with Premier and core Best Western.",
        url: "https://hotelbusiness.com/bwh-hotels-launches-global-upscale-luxury-sales-division/",
        sort: 1,
      }),
    },
    {
      recordId: "rec51tKgG3k8vs30U",
      ...buildRecentMomentumCard({
        title: "BWH Upscale Platform Continues To Feature Signature Collection Hotels",
        dateLine: "2025",
        summary:
          "BWH's upscale sales organization continues to feature Signature Collection assets alongside Premier and WorldHotels—directional evidence that Signature remains an active soft-brand path for independently identified hotels seeking BWH reach.",
        url: "https://hotelbusiness.com/bwh-hotels-launches-global-upscale-luxury-sales-division/",
        sort: 2,
      }),
    },
    {
      recordId: "recQuc9PS615YxZdQ",
      ...buildRecentMomentumCard({
        title: "Best Western Portfolio Growth Keeps Soft-Brand Paths Visible",
        dateLine: "2024",
        summary:
          "Best Western portfolio opening coverage continues to surface soft-brand and collection additions across North America—context for owners evaluating whether Signature's flexible independent path fits a conversion or affiliation thesis better than a core flag.",
        url: "https://hotelbusiness.com/best-western-opens-eight-properties/",
        sort: 3,
      }),
    },
  ],
  "preferred-hotels-and-resorts": [
    {
      recordId: "receUx1yDzzuI2j0U",
      ...buildRecentMomentumCard({
        title: "Preferred Adds 20 New Members In Q1 2026 Global Expansion",
        dateLine: "Apr 2026",
        summary:
          "Preferred Hotels & Resorts announced 20 new member properties for Q1 2026 spanning destinations from Indonesia to France, Austria, and Brazil—useful for owners tracking independent luxury affiliation momentum and membership category breadth.",
        url: "https://preferredhotels.com/bulletin/preferred-hotels-resorts-expands-global-portfolio-20-new-members",
        sort: 1,
      }),
    },
    {
      recordId: "recrj5LVxY6rYsKKU",
      ...buildRecentMomentumCard({
        title: "Preferred Accelerates African Portfolio To 37 Independent Hotels",
        dateLine: "Apr 2026",
        summary:
          "Preferred reported African portfolio growth to 37 independent hotels across nine countries, including new openings in Zanzibar and South Africa—directional evidence for owners evaluating Preferred representation outside traditional franchise conversion models.",
        url: "https://preferredhotels.com/bulletin/preferred-hotels-resorts-accelerates-african-expansion-now-spanning-37-independent-hotels",
        sort: 2,
      }),
    },
    {
      recordId: "recrgagmbKLc2Fnaz",
      ...buildRecentMomentumCard({
        title: "Preferred Reveals New Independent Hotel Openings For 2026",
        dateLine: "Dec 2025",
        summary:
          "Preferred unveiled 2026 independent hotel openings and major refurbishments across Spain, Morocco, Mexico City, Malta, London, and the U.S.—a pipeline signal for owners comparing soft-affiliation timing and destination fit.",
        url: "https://preferredhotels.com/bulletin/preferred-hotels-resorts-reveals-its-new-independent-hotel-openings-2026",
        sort: 3,
      }),
    },
  ],
};

async function fetchBrand(brandId) {
  const res = {
    statusCode: 200,
    payload: null,
    setHeader() {},
    status(c) {
      this.statusCode = c;
      return this;
    },
    json(p) {
      this.payload = p;
    },
  };
  await getBrandLibraryBrandById({ query: { brandId }, headers: {} }, res);
  return res.payload?.brand;
}

async function airtablePatch({ baseId, apiKey, recordId, fields }) {
  const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(PRESENTATION_TABLE)}/${recordId}`;
  const res = await fetch(url, {
    method: "PATCH",
    headers: {
      Authorization: `Bearer ${apiKey}`,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ fields }),
  });
  const json = await res.json();
  if (!res.ok) throw new Error(`PATCH ${recordId}: ${res.status} ${JSON.stringify(json)}`);
  return json;
}

async function main() {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) throw new Error("AIRTABLE credentials required");

  const patches = [];
  for (const [slug, cards] of Object.entries(MOMENTUM)) {
    for (const c of cards) {
      patches.push({
        slug,
        recordId: c.recordId,
        fields: { Title: c.title, Body: c.body },
        kind: "momentum",
      });
    }
  }

  // Preferred thin fields — add one more clause each
  const preferred = await fetchBrand("recwl5JOYxlChuCAr");
  const owner = (preferred.brandExplorer?.blocks || []).filter(isOwnerFacingPresentationRow);
  const thicken = {
    "valueOwners.lifecycle.5":
      "Review guest feedback, channel mix, and commercial activity against the hotel's own target segment and competitive set each quarter. Adjust programming, sales focus, and service execution without assuming that collection participation alone determines ramp-up performance outcomes for the asset.",
    "economics.opening.step.4":
      "Coordinate commercial activation with an operating launch that delivers the promised independent experience from day one. Resolve content, channel, guest-program, and service-readiness issues before relying on broader representation to carry opening demand.",
  };
  for (const [slot, body] of Object.entries(thicken)) {
    const row = owner.find((r) => r.slotKey === slot);
    if (row) {
      patches.push({
        slug: "preferred-hotels-and-resorts",
        recordId: row.recordId,
        fields: { Body: body },
        kind: "thicken",
      });
    }
  }

  console.log(`[momentum-residual] dryRun=${!APPLY} patches=${patches.length}`);
  for (const p of patches) console.log(`  ${p.kind} ${p.slug} ${p.recordId}`);

  if (!APPLY) return;

  for (const p of patches) {
    await airtablePatch({ baseId, apiKey, recordId: p.recordId, fields: p.fields });
    console.log(`PATCHED ${p.kind} ${p.slug} ${p.recordId}`);
  }

  for (const slug of Object.keys(MOMENTUM)) {
    const row = await evaluateBrandPublicVisibility(slug);
    console.log(
      `${slug}: pf=${row.publicFullProfile} fails=${(row.failures || []).join(",") || "-"}`
    );
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
