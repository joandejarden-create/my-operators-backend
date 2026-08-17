#!/usr/bin/env node
/**
 * Internal preview owner-copy gate — founder review path must be owner-safe.
 * External quality lock PASS alone is not sufficient.
 */
import "dotenv/config";
import { renderBrandExplorerHtmlForTest } from "../lib/partner-intelligence/brand-explorer-atelier-render-test-loader.js";
import { scanInternalPreviewOwnerCopy } from "../lib/partner-intelligence/brand-explorer-economics-chrome-remediation.js";
import { buildResidualOwnerCopyPatchPlan } from "../lib/partner-intelligence/brand-explorer-residual-owner-copy-remediation.js";
import { loadBrandFactoryContext } from "../lib/partner-intelligence/brand-explorer-active-profile-factory.js";

const DEFAULT_BRANDS = [
  "everhome-suites",
  "kimpton",
  "radisson-individuals-by-choice",
];

const FIELD_TO_API = {
  Title: "title",
  Body: "body",
  "Case Summary Overview": "caseSummaryOverview",
  "Case Summary Brand Relevance": "caseSummaryBrandRelevance",
  "Case Summary Owner Objective": "caseSummaryOwnerObjective",
  "Case Summary Interpretation": "caseSummaryInterpretation",
  "Case Summary Tags": "caseSummaryTags",
};

function parseArgs(argv) {
  const brandsIdx = argv.indexOf("--brands");
  const brands =
    brandsIdx >= 0 && argv[brandsIdx + 1]
      ? argv[brandsIdx + 1]
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean)
      : [...DEFAULT_BRANDS];
  const projectResidual = !argv.includes("--no-project-residual");
  return { brands, projectResidual };
}

function stripHtmlForCopyScan(html) {
  return String(html || "")
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/\s(?:href|src|srcset|data-src)=["'][^"']*["']/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/&nbsp;/g, " ")
    .replace(/&amp;/g, "&")
    .replace(/\s+/g, " ")
    .trim();
}

function applyPatchesToBlocks(blocks = [], patches = []) {
  const byRecord = new Map();
  for (const p of patches) {
    if (!p.recordId) continue;
    if (!byRecord.has(p.recordId)) byRecord.set(p.recordId, {});
    const apiKey = FIELD_TO_API[p.field];
    if (apiKey) byRecord.get(p.recordId)[apiKey] = p.after;
  }
  return (blocks || []).map((b) => {
    const overlay = byRecord.get(b.recordId);
    if (!overlay) return b;
    return { ...b, ...overlay };
  });
}

async function fetchBrand(slug) {
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
      return this;
    },
  };
  await getBrandLibraryBrandById({ query: { brandId: slug }, headers: {} }, res);
  if (res.statusCode !== 200 || !res.payload?.brand) {
    throw new Error(`brand fetch failed for ${slug}: HTTP ${res.statusCode}`);
  }
  return res.payload.brand;
}

async function main() {
  const { brands, projectResidual } = parseArgs(process.argv.slice(2));
  let failed = 0;

  for (const slug of brands) {
    const brand = await fetchBrand(slug);
    let brandForRender = brand;
    let residualNote = "live Presentation";

    if (projectResidual) {
      const ctx = await loadBrandFactoryContext(slug).catch(() => null);
      const rows = ctx?.presentationRows || brand?.brandExplorer?.blocks || [];
      const plan = buildResidualOwnerCopyPatchPlan({ brandSlug: slug, presentationRows: rows });
      const blocks = applyPatchesToBlocks(brand?.brandExplorer?.blocks || [], plan.patches);
      brandForRender = {
        ...brand,
        brandExplorer: { ...(brand.brandExplorer || {}), blocks },
      };
      residualNote = `projected residual patches=${plan.summary.patchCount}`;
    }

    const html = renderBrandExplorerHtmlForTest(brandForRender, {
      allPanels: true,
      internalPreview: true,
    });
    const text = stripHtmlForCopyScan(html);
    const hits = scanInternalPreviewOwnerCopy(text);

    // Banner may include "Not owner-ready" — allowed. Fail only on forbidden owner-copy list.
    if (hits.length) {
      failed += 1;
      console.log(`[FAIL] ${slug} (${residualNote}) forbidden=${hits.length}`);
      for (const h of hits) console.log(`  - ${h.label}: ${h.snippet}`);
    } else {
      console.log(`[PASS] ${slug} (${residualNote}) internal preview owner-copy clean`);
    }
  }

  if (failed) {
    console.error(`\n${failed}/${brands.length} brand(s) failed internal preview owner-copy gate.`);
    process.exit(1);
  }
  console.log(`\nAll ${brands.length} brand(s) passed internal preview owner-copy gate.`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
