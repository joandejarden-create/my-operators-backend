#!/usr/bin/env node
import "dotenv/config";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { loadBrandFactoryContext } from "../lib/partner-intelligence/brand-explorer-active-profile-factory.js";
import { evaluateSourceProvenanceByTab, formatSourceProvenanceMarkdown } from "../lib/partner-intelligence/brand-explorer-source-provenance-by-tab.js";
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
  await getBrandLibraryBrandById({ query: { brandId: slug }, headers: {} }, res);
  if (res.statusCode !== 200 || !res.payload?.brand) {
    throw new Error(`Brand API failed for ${slug}`);
  }
  return res.payload.brand;
}

async function main() {
  const brands = parseBrands(process.argv.slice(2));
  const brandResults = [];
  for (const brandSlug of brands) {
    const brand = await fetchBrandApi(brandSlug);
    const ctx = await loadBrandFactoryContext(brandSlug);
    brandResults.push(
      evaluateSourceProvenanceByTab({
        brandSlug,
        brandConfig: ctx.brandConfig || ctx.activeProfileConfig,
        registryAssets: ctx.registryAssets || [],
        presentationRows: ctx.presentationRows || [],
        brandApi: brand,
      })
    );
  }
  const report = {
    generatedAt: new Date().toISOString(),
    brands,
    brandResults,
    auditPass: brandResults.every((b) => b.pass === true),
  };
  const reportsDir = path.join(ROOT, "reports");
  fs.mkdirSync(reportsDir, { recursive: true });
  const jsonPath = path.join(reportsDir, "brand-explorer-source-provenance-by-tab.json");
  const mdPath = path.join(reportsDir, "brand-explorer-source-provenance-by-tab.md");
  fs.writeFileSync(jsonPath, JSON.stringify(report, null, 2));
  fs.writeFileSync(
    mdPath,
    [`# Source Provenance by Tab`, "", `Pass: **${report.auditPass}**`, "", ...brandResults.map(formatSourceProvenanceMarkdown)].join(
      "\n"
    )
  );
  console.log(`Wrote ${jsonPath}`);
  console.log(`Wrote ${mdPath}`);
  for (const b of brandResults) {
    console.log(`  ${b.brandSlug}: pass=${b.pass} failures=${(b.failures || []).join(",") || "—"}`);
  }
  if (!report.auditPass) process.exit(3);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
