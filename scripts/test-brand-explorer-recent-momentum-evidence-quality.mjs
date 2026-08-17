#!/usr/bin/env node
/**
 * test:brand-explorer-recent-momentum-evidence-quality
 */
import "../load-env.js";
import { resolveLane2BrandIdentity } from "../lib/partner-intelligence/brand-explorer-lane2-common.js";
import { LANE2_PROPERTY_CATALOG_BY_SLUG } from "../lib/partner-intelligence/brand-explorer-lane2-property-catalog.js";
import { renderBrandExplorerHtmlForTest } from "../lib/partner-intelligence/brand-explorer-atelier-render-test-loader.js";
import {
  evaluateRecentMomentumEvidenceQuality,
  MOMENTUM_EVIDENCE_TARGET_SLUGS,
} from "../lib/partner-intelligence/brand-explorer-recent-momentum-evidence-quality.js";
import { CALA_AVAILABLE_BY_SLUG } from "../lib/partner-intelligence/brand-explorer-27-recent-momentum-evidence-fix-content.js";

async function listRows(brandName) {
  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  const formula = `{Brand Name}='${String(brandName).replace(/'/g, "\\'")}'`;
  const rows = [];
  let offset = "";
  do {
    const params = new URLSearchParams({ pageSize: "100", filterByFormula: formula });
    if (offset) params.set("offset", offset);
    const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(
      "Brand Setup - Brand Explorer Presentation"
    )}?${params}`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
    const json = await res.json().catch(() => ({}));
    if (!res.ok) throw new Error(json.error?.message || `list failed ${res.status}`);
    for (const rec of json.records || []) {
      const f = rec.fields || {};
      rows.push({
        recordId: rec.id,
        slotKey: String(f["Slot Key"] || "").trim(),
        title: String(f.Title || "").trim(),
        body: String(f.Body || "").trim(),
        caseSummaryOverview: String(f["Case Summary Overview"] || "").trim(),
        caseSummaryTags: String(f["Case Summary Tags"] || "").trim(),
        caseSummaryBrandRelevance: String(f["Case Summary Brand Relevance"] || "").trim(),
        externalDisplayStatus: String(f["External Display Status"] || "").trim(),
        active: f.Active !== false,
        sortOrder: f["Sort Order"] ?? 0,
      });
    }
    offset = json.offset || "";
  } while (offset);
  return rows;
}

async function fetchBrand(slug) {
  const { getBrandLibraryBrandById } = await import("../api/brand-library.js");
  const identity = resolveLane2BrandIdentity(slug);
  const brandId = identity.recordId || slug;
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
    throw new Error(`Brand API failed: ${slug}`);
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
  return [...MOMENTUM_EVIDENCE_TARGET_SLUGS];
}

async function main() {
  const brands = parseBrands(process.argv.slice(2));
  let failed = 0;
  for (const slug of brands) {
    const identity = resolveLane2BrandIdentity(slug);
    const rows = await listRows(identity.name);
    const brand = await fetchBrand(slug);
    const html = renderBrandExplorerHtmlForTest(brand, { allPanels: true, internalPreview: false });
    const result = evaluateRecentMomentumEvidenceQuality({
      brandSlug: slug,
      brandName: identity.name,
      presentationRows: rows,
      html,
      propertyCatalog: LANE2_PROPERTY_CATALOG_BY_SLUG[slug] || [],
      calaAvailableOverride: CALA_AVAILABLE_BY_SLUG[slug],
    });
    if (result.pass) {
      console.log(`[PASS] ${slug} recent-momentum-evidence-quality`);
    } else {
      failed += 1;
      console.log(`[FAIL] ${slug} fails=${result.failures.length}`);
      for (const f of result.failures.slice(0, 20)) {
        console.log(`  - ${f.id}${f.title ? ` · ${f.title}` : ""}${f.detail ? ` · ${f.detail}` : ""}`);
      }
    }
  }
  if (failed) {
    console.error(`${failed} brand(s) failed recent-momentum-evidence-quality.`);
    process.exit(1);
  }
  console.log(`All ${brands.length} brand(s) passed recent-momentum-evidence-quality.`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
