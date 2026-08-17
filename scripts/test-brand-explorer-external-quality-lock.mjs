#!/usr/bin/env node
/**
 * v38 — External DOM quality lock test across all Brand Explorer tabs.
 */
import "../load-env.js";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  DEFAULT_BRANDS,
  DEFAULT_COMPLETE_BRANDS,
  DEFAULT_INCOMPLETE_BRANDS,
} from "../lib/partner-intelligence/brand-explorer-v38-display-quality-lock-audit.js";
import { evaluateBrandExternalQualityLock } from "../lib/partner-intelligence/brand-explorer-display-quality-lock.js";
import { renderBrandExplorerHtmlForTest } from "../lib/partner-intelligence/brand-explorer-atelier-render-test-loader.js";
import { shouldRenderFullBrandExplorerProfile } from "../lib/partner-intelligence/brand-explorer-display-state.js";

const __dirname = dirname(fileURLToPath(import.meta.url));

function parseArgs(argv) {
  const brandsIdx = argv.indexOf("--brands");
  const brands =
    brandsIdx >= 0 && argv[brandsIdx + 1]
      ? argv[brandsIdx + 1].split(",").map((s) => s.trim()).filter(Boolean)
      : [...DEFAULT_BRANDS];
  return { brands };
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
  const { getBrandLibraryBrandById } = await import("../api/brand-library.js");
  const res = mockRes();
  await getBrandLibraryBrandById({ query: { brandId: slug }, headers: {} }, res);
  const out = res.getOut();
  if (out.statusCode !== 200 || !out.body?.success) {
    throw new Error(`${slug}: HTTP ${out.statusCode} ${JSON.stringify(out.body)}`);
  }
  return out.body.brand;
}

async function runBrandTest(slug) {
  const brand = await fetchBrandBySlug(slug);
  const expectFull = shouldRenderFullBrandExplorerProfile(brand);
  const cohort = DEFAULT_INCOMPLETE_BRANDS.includes(slug)
    ? "incomplete"
    : DEFAULT_COMPLETE_BRANDS.includes(slug)
      ? "complete"
      : "other";

  const html = renderBrandExplorerHtmlForTest(brand, { allPanels: true });
  const result = evaluateBrandExternalQualityLock(brand, html, { brandSlug: slug });

  const cohortExpectationMet =
    cohort === "incomplete"
      ? !result.actualRenderedFullProfile && result.profileInPreparationRendered
      : cohort === "complete"
        ? result.shouldRenderFullProfile
          ? result.actualRenderedFullProfile && !result.profileInPreparationRendered
          : result.profileInPreparationRendered && !result.actualRenderedFullProfile
        : true;

  const testPass = result.externalQualityLockPass && cohortExpectationMet;

  return {
    brandSlug: slug,
    cohort,
    brandRecordId: brand.id,
    brandExplorerDisplayState: brand.brandExplorerDisplayState,
    shouldRenderFullProfile: brand.shouldRenderFullProfile,
    ...result,
    cohortExpectationMet,
    testPass,
  };
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  const results = [];
  let failed = 0;

  for (const slug of opts.brands) {
    try {
      const result = await runBrandTest(slug);
      results.push(result);
      if (!result.testPass) failed += 1;
      const status = result.testPass ? "PASS" : "FAIL";
      console.log(
        `[${status}] ${slug} (${result.cohort}) state=${result.displayState} forbidden=${result.forbiddenStringsFound} tabs=${result.tabsRenderedExternally.length} prep=${result.profileInPreparationRendered}`
      );
      if (!result.testPass) {
        for (const m of result.forbiddenMatches || []) {
          console.log(`  - forbidden: ${m.pattern}: ${m.snippet}`);
        }
        if (!result.cohortExpectationMet) {
          console.log(`  - cohort expectation failed (expectFull=${result.shouldRenderFullProfile})`);
        }
      }
    } catch (err) {
      failed += 1;
      results.push({ brandSlug: slug, testPass: false, error: err.message });
      console.error(`[FAIL] ${slug}: ${err.message}`);
    }
  }

  if (failed > 0) {
    console.error(`\n${failed} brand(s) failed external quality lock.`);
    process.exit(1);
  }
  console.log(`\nAll ${results.length} brand(s) passed external quality lock.`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
