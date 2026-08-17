#!/usr/bin/env node
import "dotenv/config";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { loadBrandFactoryContext } from "../lib/partner-intelligence/brand-explorer-active-profile-factory.js";
import { renderBrandExplorerHtmlForTest } from "../lib/partner-intelligence/brand-explorer-atelier-render-test-loader.js";
import { scanNoEmptyRenderedComponents } from "../lib/partner-intelligence/brand-explorer-no-empty-rendered-components.js";
import { TAB_FACTORY_TARGET_BRANDS } from "../lib/partner-intelligence/brand-explorer-tab-contracts.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

function parseBrands(argv) {
  const idx = argv.indexOf("--brands");
  if (idx >= 0 && argv[idx + 1]) {
    return argv[idx + 1].split(",").map((s) => s.trim()).filter(Boolean);
  }
  return [...TAB_FACTORY_TARGET_BRANDS];
}

async function fetchBrandApi(slug) {
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
  if (res.statusCode !== 200 || !res.payload?.brand) throw new Error(`Brand API failed ${slug}`);
  return res.payload.brand;
}

async function main() {
  const brands = parseBrands(process.argv.slice(2));
  const brandResults = [];
  let failed = 0;
  for (const brandSlug of brands) {
    const brand = await fetchBrandApi(brandSlug);
    await loadBrandFactoryContext(brandSlug);
    const html = renderBrandExplorerHtmlForTest(brand, { allPanels: true, internalPreview: false });
    const scan = scanNoEmptyRenderedComponents(html, { brandSlug });
    brandResults.push(scan);
    if (scan.pass) console.log(`[PASS] ${brandSlug} no empty rendered components`);
    else {
      failed += 1;
      console.log(`[FAIL] ${brandSlug} emptyFails=${scan.failFindings}`);
      for (const f of scan.findings.slice(0, 20)) console.log(`  - ${f.id}: ${f.detail}`);
    }
  }
  const report = {
    generatedAt: new Date().toISOString(),
    brands,
    brandResults,
    auditPass: failed === 0,
  };
  const reportsDir = path.join(ROOT, "reports");
  fs.mkdirSync(reportsDir, { recursive: true });
  fs.writeFileSync(
    path.join(reportsDir, "brand-explorer-no-empty-rendered-components.json"),
    JSON.stringify(report, null, 2)
  );
  fs.writeFileSync(
    path.join(reportsDir, "brand-explorer-no-empty-rendered-components.md"),
    `# No Empty Rendered Components\n\nPass: **${report.auditPass}**\n\n` +
      brandResults.map((b) => `- ${b.brandSlug}: pass=${b.pass} fails=${b.failFindings}`).join("\n") +
      "\n"
  );
  if (failed) process.exit(1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
