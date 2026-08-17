#!/usr/bin/env node
/**
 * test:brand-explorer-golden-content-quality
 */
import "dotenv/config";
import { loadBrandFactoryContext } from "../lib/partner-intelligence/brand-explorer-active-profile-factory.js";
import { renderBrandExplorerHtmlForTest } from "../lib/partner-intelligence/brand-explorer-atelier-render-test-loader.js";
import { evaluateGoldenContentQuality } from "../lib/partner-intelligence/brand-explorer-golden-content-quality.js";

async function fetchBrand(slug) {
  const { getBrandLibraryBrandById } = await import("../api/brand-library.js");
  const { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } = await import(
    "../lib/partner-intelligence/brand-explorer-factory-preview-candidates.js"
  );
  const identity = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[slug];
  const brandId = identity?.recordId || slug;
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
  if (res.statusCode !== 200 || !res.payload?.brand) {
    throw new Error(`Brand fetch failed for ${slug}`);
  }
  return res.payload.brand;
}

function parseBrands(argv) {
  const idx = argv.indexOf("--brands");
  if (idx >= 0 && argv[idx + 1]) {
    return argv[idx + 1]
      .split(",")
      .map((s) => s.trim())
      .filter(Boolean);
  }
  return ["hotel-indigo", "mgallery-collection", "small-luxury-hotels-of-the-world"];
}

async function main() {
  const brands = parseBrands(process.argv.slice(2));
  let failed = 0;
  for (const slug of brands) {
    const brand = await fetchBrand(slug);
    const ctx = await loadBrandFactoryContext(slug);
    const html = renderBrandExplorerHtmlForTest(brand, {
      allPanels: true,
      internalPreview: false,
    });
    const result = evaluateGoldenContentQuality(brand, ctx.presentationRows || [], html, {
      brandSlug: slug,
    });
    if (result.pass) {
      console.log(`[PASS] ${slug} golden content quality`);
    } else {
      failed += 1;
      console.log(`[FAIL] ${slug}`);
      for (const f of result.failures) console.log(`  - ${f}`);
    }
  }
  if (failed) {
    console.error(`\n${failed} brand(s) failed golden content quality.`);
    process.exit(1);
  }
  console.log(`\nAll ${brands.length} brand(s) passed golden content quality.`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
