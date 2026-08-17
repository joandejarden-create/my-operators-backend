#!/usr/bin/env node
/**
 * test:brand-explorer-section-pattern-parity
 */
import "dotenv/config";
import { verifySectionPatternParityBrand } from "../lib/partner-intelligence/brand-explorer-section-pattern-parity-remediation.js";
import { resolveSectionPatternBrandList } from "../lib/partner-intelligence/brand-explorer-section-pattern-parity-audit.js";
import {
  evaluateRecentMomentumPattern,
  evaluateGeographicFootprintPattern,
} from "../lib/partner-intelligence/brand-explorer-section-pattern-parity.js";
import {
  buildRecentMomentumCard,
  withRecentMomentumSortOrder,
  looksLikeDiligenceFillerMomentum,
  RECENT_MOMENTUM_MIN_LINKED_URLS,
} from "../lib/partner-intelligence/brand-explorer-recent-momentum-contract.js";

function parseBrands(argv) {
  const idx = argv.indexOf("--brands");
  if (idx >= 0 && argv[idx + 1]) {
    return resolveSectionPatternBrandList(
      argv[idx + 1]
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean)
    );
  }
  return null;
}

function unitChecks() {
  const card = buildRecentMomentumCard({
    title: "Named Opening In Market City",
    dateLine: "Jul 2024",
    summary: "Owner-useful opening interpretation with geography.",
    url: "https://www.example.com/news/opening",
  });
  if (!card.body.includes("https://www.example.com/news/opening")) {
    throw new Error("buildRecentMomentumCard must embed trailing announcement URL");
  }
  if (!looksLikeDiligenceFillerMomentum("Owner diligence: where named opening press is thin")) {
    throw new Error("expected diligence filler detection");
  }
  const sorted = withRecentMomentumSortOrder([
    { title: "Older", dateLine: "2023", summary: "a", url: "https://a.example/1", body: "x" },
    { title: "Newer", dateLine: "2025", summary: "b", url: "https://a.example/2", body: "y" },
  ]);
  if (sorted[0].title !== "Newer" || sorted[0].sort !== 1) {
    throw new Error("expected newest-first sort order");
  }
  if (RECENT_MOMENTUM_MIN_LINKED_URLS < 2) {
    throw new Error("contract min linked URLs should be >= 2");
  }

  const failBlob = evaluateRecentMomentumPattern({
    brandSlug: "mgallery-collection",
    brandName: "MGallery Collection",
    presentationRows: [
      {
        slotKey: "footprint.momentum",
        title: "",
        body: "2024–2025 · Accor collection positioning\n\nUse as directional context for collection growth themes.",
        active: true,
      },
    ],
    html: '<p class="oe-section-hint">Illustrative activity</p>',
  });
  if (failBlob.pass) throw new Error("expected MGallery-style blob to fail");
  if (!["source_note_style", "wrong_pattern", "too_generic", "needs_patch"].includes(failBlob.status)) {
    throw new Error(`unexpected blob status: ${failBlob.status}`);
  }

  const passMomentum = evaluateRecentMomentumPattern({
    brandSlug: "kimpton",
    brandName: "Kimpton Hotels",
    presentationRows: [
      {
        slotKey: "footprint.momentum",
        title: "First Kimpton in the Dominican Republic",
        body: "Jul 2024\n\nKimpton Las Mercedes opened in Santo Domingo for owners comparing lifestyle boutique conversion fit.\n\nhttps://www.ihgplc.com/en/news-and-media/news-releases/2024/example",
        active: true,
      },
      {
        slotKey: "footprint.momentum",
        title: "Baja Sur resort debut in Todos Santos",
        body: "Apr 2024\n\nKimpton Mas Olas opened on Mexico’s Baja Sur coast—owner-relevant resort lifestyle signal.\n\nhttps://www.ihgplc.com/en/news-and-media/news-releases/2024/example2",
        active: true,
      },
    ],
  });
  if (!passMomentum.pass) throw new Error(`expected Kimpton-style momentum to pass: ${passMomentum.failureReason}`);

  const failGeo = evaluateGeographicFootprintPattern({
    brandSlug: "mgallery-collection",
    brandName: "MGallery Collection",
    presentationRows: [
      {
        slotKey: "footprint.geo_intro",
        title: "Footprint Perspective",
        body: "MGallery references can be useful across city settings.",
        active: true,
      },
    ],
  });
  if (failGeo.pass) throw new Error("expected thin geo without regions to fail");

  console.log("[PASS] unit section pattern contracts + Recent Momentum template");
}

async function main() {
  unitChecks();
  const brands = parseBrands(process.argv.slice(2));
  if (!brands?.length) {
    console.log("No --brands provided; unit checks only.");
    return;
  }
  let failed = 0;
  for (const slug of brands) {
    const result = await verifySectionPatternParityBrand(slug);
    if (result.pass) {
      console.log(`[PASS] ${slug} section pattern parity`);
    } else {
      failed += 1;
      console.log(`[FAIL] ${slug}`);
      for (const [k, v] of Object.entries(result.sections || {})) {
        if (!v.pass) console.log(`  - ${k}: ${v.status}`);
      }
    }
  }
  if (failed) {
    console.error(`\n${failed} brand(s) failed section pattern parity.`);
    process.exit(1);
  }
  console.log(`\nAll ${brands.length} brand(s) passed section pattern parity.`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
