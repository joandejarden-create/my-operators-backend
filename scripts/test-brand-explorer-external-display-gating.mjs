#!/usr/bin/env node
/**
 * v37C-R2 UI proof — fails if incomplete external Brand Explorer UI contains forbidden strings.
 *
 * Usage:
 *   npm run test:brand-explorer-external-display-gating -- --brands hotel-indigo,mgallery-collection
 */
import "../load-env.js";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import { getActiveProfileBrandConfig } from "../lib/partner-intelligence/brand-explorer-active-profile-brand-config.js";
import { scanRenderedHtmlForForbiddenStrings } from "../lib/partner-intelligence/brand-explorer-external-display-forbidden-patterns.js";
import { renderBrandExplorerHtmlForTest } from "../lib/partner-intelligence/brand-explorer-atelier-render-test-loader.js";

const __dirname = dirname(fileURLToPath(import.meta.url));

function parseArgs(argv) {
  const brandsIdx = argv.indexOf("--brands");
  const brands =
    brandsIdx >= 0 && argv[brandsIdx + 1]
      ? argv[brandsIdx + 1].split(",").map((s) => s.trim()).filter(Boolean)
      : ["hotel-indigo", "mgallery-collection"];
  return { brands, allPanels: argv.includes("--all-panels") };
}

function mockRes() {
  const out = { statusCode: 200, body: null };
  return {
    setHeader() {},
    status(code) {
      out.statusCode = code;
      return this;
    },
    json(payload) {
      out.body = payload;
      return this;
    },
    getOut() {
      return out;
    },
  };
}

async function fetchBrandBySlug(slug) {
  const config = getActiveProfileBrandConfig(slug);
  if (!config?.recordId) throw new Error(`No active profile config for slug: ${slug}`);
  const { getBrandLibraryBrandById } = await import("../api/brand-library.js");
  const res = mockRes();
  await getBrandLibraryBrandById({ query: { brandId: slug }, headers: {} }, res);
  const out = res.getOut();
  if (out.statusCode !== 200 || !out.body?.success) {
    throw new Error(`${slug}: HTTP ${out.statusCode} ${JSON.stringify(out.body)}`);
  }
  return out.body.brand;
}

async function runBrandTest(slug, options) {
  const brand = await fetchBrandBySlug(slug);
  const html = renderBrandExplorerHtmlForTest(brand, { allPanels: options.allPanels });
  const companyValidated = brand?.governance?.companyValidated === true;
  const scan = scanRenderedHtmlForForbiddenStrings(html, { companyValidated });
  const suppressExpected = brand.shouldSuppressIncompleteExternalSections === true;

  return {
    brandSlug: slug,
    brandRecordId: brand.id,
    brandExplorerDisplayState: brand.brandExplorerDisplayState,
    shouldSuppressIncompleteExternalSections: brand.shouldSuppressIncompleteExternalSections,
    should_hide_external_profile: brand.shouldHideExternalProfile,
    suppressExpected,
    rendered_sections_suppressed: suppressExpected && html.includes('data-be-display-gate="profile-in-preparation"'),
    actual_helper_text_visible: html.includes("Scenario cards will appear"),
    forbidden_strings_found: scan.forbiddenStringsFound.length,
    forbidden_matches: scan.matches,
    ui_proof_test_pass: scan.forbiddenStringsFound.length === 0,
    htmlLength: html.length,
  };
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  const results = [];
  let failed = 0;

  for (const slug of opts.brands) {
    try {
      const result = await runBrandTest(slug, opts);
      results.push(result);
      if (!result.ui_proof_test_pass) failed += 1;
      const status = result.ui_proof_test_pass ? "PASS" : "FAIL";
      console.log(
        `[${status}] ${slug} displayState=${result.brandExplorerDisplayState} forbidden=${result.forbidden_strings_found}`
      );
      if (!result.ui_proof_test_pass) {
        for (const m of result.forbidden_matches) {
          console.log(`  - ${m.pattern}: ${m.snippet}`);
        }
      }
    } catch (err) {
      failed += 1;
      results.push({ brandSlug: slug, ui_proof_test_pass: false, error: err.message });
      console.error(`[FAIL] ${slug}: ${err.message}`);
    }
  }

  if (failed > 0) {
    console.error(`\n${failed} brand(s) failed external display gating UI proof.`);
    process.exit(1);
  }
  console.log(`\nAll ${results.length} brand(s) passed external display gating UI proof.`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
