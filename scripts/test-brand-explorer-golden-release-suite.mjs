#!/usr/bin/env node
/**
 * Golden release suite — Everhome / Kimpton / Radisson Individuals.
 * Fails on forbidden copy, chrome regressions, imageUrl drops, lock leaks, unlock without founder.
 */
import "dotenv/config";
import { evaluateBrandExplorerOsBrand } from "../lib/partner-intelligence/brand-explorer-os-run.js";
import { PRIMARY_RELEASE_SLUGS } from "../lib/partner-intelligence/brand-explorer-os-state-machine.js";
import { scanInternalPreviewOwnerCopy } from "../lib/partner-intelligence/brand-explorer-economics-chrome-remediation.js";
import { renderBrandExplorerHtmlForTest } from "../lib/partner-intelligence/brand-explorer-atelier-render-test-loader.js";
import { evaluateBrandExternalQualityLock } from "../lib/partner-intelligence/brand-explorer-display-quality-lock.js";

function parseArgs(argv) {
  const brandsIdx = argv.indexOf("--brands");
  const brands =
    brandsIdx >= 0 && argv[brandsIdx + 1]
      ? argv[brandsIdx + 1]
          .split(",")
          .map((s) => s.trim())
          .filter(Boolean)
      : [...PRIMARY_RELEASE_SLUGS];
  return { brands };
}

function stripHtml(html) {
  return String(html || "")
    .replace(/<script[\s\S]*?<\/script>/gi, " ")
    .replace(/<style[\s\S]*?<\/style>/gi, " ")
    .replace(/\s(?:href|src|srcset|data-src)=["'][^"']*["']/gi, " ")
    .replace(/<[^>]+>/g, " ")
    .replace(/\s+/g, " ")
    .trim();
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
    throw new Error(`fetch failed ${slug}`);
  }
  return res.payload.brand;
}

async function main() {
  const { brands } = parseArgs(process.argv.slice(2));
  let failed = 0;

  for (const slug of brands) {
    const os = await evaluateBrandExplorerOsBrand(slug);
    const brand = await fetchBrand(slug);
    const issues = [];

    if ((os.metrics.galleryCount || 0) < 6) {
      issues.push(`gallery_imageurl_below_6:${os.metrics.galleryCount}`);
    }
    if ((os.metrics.openingsCount || 0) < 3) {
      issues.push(`property_imageurl_below_3:${os.metrics.openingsCount}`);
    }

    // Live internal preview (no projection) — chrome must stay clean
    const internalHtml = renderBrandExplorerHtmlForTest(brand, {
      allPanels: true,
      internalPreview: true,
    });
    const chromeHits = scanInternalPreviewOwnerCopy(stripHtml(internalHtml)).filter((h) =>
      ["fdd", "loi", "item_7", "item_19", "fee_stack", "net_contribution", "franchise_disclosure", "disclosure_document"].includes(
        h.id
      )
    );
    if (chromeHits.length) {
      issues.push(`renderer_chrome:${chromeHits.map((h) => h.id).join(",")}`);
    }

    const externalHtml = renderBrandExplorerHtmlForTest(brand, {
      allPanels: true,
      internalPreview: false,
    });
    const ql = evaluateBrandExternalQualityLock(brand, externalHtml, { brandSlug: slug });
    if (!ql.profileInPreparationRendered && os.metrics.activeReleaseApproved !== true) {
      issues.push("external_lock_leak_or_missing_prep");
    }
    if ((ql.tabsRenderedExternally || []).length > 1 && os.metrics.activeReleaseApproved !== true) {
      issues.push(`external_partial_tabs:${ql.tabsRenderedExternally.length}`);
    }

    if (os.routing.activeReleaseAllowed && !os.metrics.founderVisualReviewPassed) {
      issues.push("active_release_without_founder");
    }
    if (os.canonicalState === "active_profile_ready" && !os.metrics.founderVisualReviewPassed) {
      issues.push("active_profile_ready_without_founder");
    }

    // Forbidden copy in live internal preview for non-residual chrome tokens
    const liveHits = scanInternalPreviewOwnerCopy(stripHtml(internalHtml));
    const hardForbidden = liveHits.filter((h) =>
      ["fdd", "loi", "item_19", "fee_stack", "net_contribution"].includes(h.id)
    );
    if (hardForbidden.length) {
      issues.push(`forbidden_copy:${hardForbidden.map((h) => h.id).join(",")}`);
    }

    if (issues.length) {
      failed += 1;
      console.log(`[FAIL] ${slug}`);
      for (const i of issues) console.log(`  - ${i}`);
    } else {
      console.log(
        `[PASS] ${slug} state=${os.canonicalState} action=${os.routing.allowedNextAction} gallery=${os.metrics.galleryCount} openings=${os.metrics.openingsCount}`
      );
    }
  }

  if (failed) {
    console.error(`\n${failed}/${brands.length} golden brand(s) failed.`);
    process.exit(1);
  }
  console.log(`\nAll ${brands.length} golden brand(s) passed release suite.`);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
