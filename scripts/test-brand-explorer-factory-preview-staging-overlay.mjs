#!/usr/bin/env node
/**
 * Factory Preview staging overlay — Fairfield Brand Website.
 *
 * Proves:
 * - Factory preview resolves Fairfield to https://fairfield.marriott.com/
 * - Parent root https://marriott.com/ is not rendered in factory preview HTML
 * - Non-factory / production path is unaffected (live website preserved)
 * - Preview uses staged fixture data before production promotion
 * - No Airtable writes
 */
import "dotenv/config";
import assert from "node:assert/strict";
import {
  applyFactoryPreviewStagingOverlay,
  loadFactoryPreviewStagingOverlay,
  resolveFactoryPreviewStagedBrandWebsite,
} from "../lib/partner-intelligence/brand-explorer-factory-preview-staging-overlay.js";
import { FAIRFIELD_OFFICIAL_BRAND_WEBSITE } from "../lib/partner-intelligence/brand-explorer-brand-website-preference.js";
import { renderBrandExplorerHtmlForTest } from "../lib/partner-intelligence/brand-explorer-atelier-render-test-loader.js";
import { getBrandLibraryBrandById } from "../api/brand-library.js";
import { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } from "../lib/partner-intelligence/brand-explorer-factory-preview-candidates.js";

const SLUG = "fairfield-by-marriott";
const RECORD_ID = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[SLUG].recordId;
const PARENT_ROOT = "https://marriott.com/";

function mockRes() {
  return {
    statusCode: 200,
    payload: null,
    headers: {},
    setHeader(k, v) {
      this.headers[k] = v;
    },
    status(c) {
      this.statusCode = c;
      return this;
    },
    json(p) {
      this.payload = p;
      return this;
    },
  };
}

async function fetchBrand(query) {
  const res = mockRes();
  await getBrandLibraryBrandById({ query, headers: {} }, res);
  assert.equal(res.statusCode, 200, `API status ${res.statusCode}`);
  assert.equal(res.payload?.success, true, "API success");
  return res.payload.brand;
}

function mainSync() {
  const staging = loadFactoryPreviewStagingOverlay(SLUG);
  assert.ok(staging?.fixturePath, "Fairfield staging fixture must exist");
  assert.equal(
    staging.brandWebsite,
    FAIRFIELD_OFFICIAL_BRAND_WEBSITE,
    `fixture brandWebsite must be ${FAIRFIELD_OFFICIAL_BRAND_WEBSITE}`
  );

  const liveParent = {
    id: RECORD_ID,
    slug: SLUG,
    name: "Fairfield by Marriott",
    brandWebsite: PARENT_ROOT,
    brandExplorer: { version: 1, blocks: [{ slotKey: "overview.hero", body: "x", sort: 0 }] },
    shouldRenderFullProfile: false,
    brandExplorerDisplayState: "profile_in_preparation",
  };

  const production = applyFactoryPreviewStagingOverlay(liveParent, { factoryPreview: false });
  assert.equal(production.brandWebsite, PARENT_ROOT, "production path must keep live Brand Website");
  assert.equal(production.factoryPreviewStaging, undefined, "no staging meta on production path");

  const preview = applyFactoryPreviewStagingOverlay(liveParent, {
    factoryPreview: true,
    search: "?beInternalPreview=1&factoryPreview=1",
  });
  assert.equal(
    preview.brandWebsite,
    FAIRFIELD_OFFICIAL_BRAND_WEBSITE,
    "factory preview must use fixture brandWebsite"
  );
  assert.equal(preview.factoryPreviewStaging?.applied, true, "staging applied");
  assert.equal(preview.factoryPreviewStaging?.airtableWrites, false, "no Airtable writes");
  assert.equal(
    preview.factoryPreviewStaging?.priorBrandWebsite,
    PARENT_ROOT,
    "prior website recorded"
  );

  // Non-factory path must not mutate brandWebsite even when object is reused for preview later.
  assert.equal(
    liveParent.brandWebsite,
    PARENT_ROOT,
    "overlay must not mutate the original brand object in place"
  );

  const resolved = resolveFactoryPreviewStagedBrandWebsite({
    slug: SLUG,
    liveBrandWebsite: PARENT_ROOT,
  });
  assert.equal(resolved.brandWebsite, FAIRFIELD_OFFICIAL_BRAND_WEBSITE);
  assert.equal(resolved.applied, true);
  assert.equal(resolved.reason, "fixture_brandWebsite");

  // Use a brand that would render overview snapshot (not quality-locked).
  const unlockedLiveParent = {
    ...liveParent,
    shouldRenderFullProfile: true,
    brandExplorerDisplayState: "active_profile_ready",
  };
  const previewHtml = renderBrandExplorerHtmlForTest(unlockedLiveParent, {
    factoryPreview: true,
    allPanels: true,
  });
  assert.match(
    previewHtml,
    /fairfield\.marriott\.com/i,
    "factory preview HTML must include fairfield.marriott.com"
  );
  assert.ok(
    !/>\s*https?:\/\/(www\.)?marriott\.com\/?\s*</i.test(previewHtml),
    "factory preview HTML must not render bare marriott.com parent root as a value"
  );

  const publicHtml = renderBrandExplorerHtmlForTest(unlockedLiveParent, { allPanels: true });
  assert.match(
    publicHtml,
    /https?:\/\/(www\.)?marriott\.com\/?/i,
    "non-factory path still receives live brandWebsite value in HTML when provided"
  );
  assert.ok(
    !/fairfield\.marriott\.com/i.test(publicHtml),
    "non-factory path must not inject staged fairfield.marriott.com"
  );

  console.log("[PASS] sync unit: fixture overlay + HTML factory vs production");
}

async function mainLive() {
  const prodBrand = await fetchBrand({ brandId: RECORD_ID, refresh: "1" });
  const previewBrand = await fetchBrand({
    brandId: RECORD_ID,
    refresh: "1",
    beInternalPreview: "1",
    factoryPreview: "1",
  });

  assert.equal(
    previewBrand.brandWebsite,
    FAIRFIELD_OFFICIAL_BRAND_WEBSITE,
    `API factory preview brandWebsite must be ${FAIRFIELD_OFFICIAL_BRAND_WEBSITE}, got ${previewBrand.brandWebsite}`
  );
  assert.notEqual(
    previewBrand.brandWebsite.replace(/\/$/, ""),
    "https://marriott.com",
    "API factory preview must not return parent root"
  );
  assert.equal(previewBrand.factoryPreviewStaging?.applied, true, "API staging applied");
  assert.equal(previewBrand.factoryPreviewStaging?.airtableWrites, false);

  // Production path: either live Airtable value OR at least not forced to fixture when not previewing
  assert.ok(prodBrand.brandWebsite != null, "production brandWebsite present");
  if (String(prodBrand.brandWebsite).includes("fairfield.marriott.com")) {
    console.log(
      "[INFO] live Airtable Brand Website already brand-specific; production still returns it without staging meta"
    );
  } else {
    assert.notEqual(
      prodBrand.factoryPreviewStaging?.applied,
      true,
      "production API must not apply staging overlay"
    );
  }

  const liveParentStub = {
    ...prodBrand,
    brandWebsite: PARENT_ROOT,
    slug: SLUG,
  };
  const html = renderBrandExplorerHtmlForTest(liveParentStub, {
    factoryPreview: true,
    allPanels: true,
  });
  assert.match(html, /https:\/\/fairfield\.marriott\.com\/?/i);
  assert.ok(!html.includes(">" + PARENT_ROOT + "<") && !html.includes(">" + PARENT_ROOT.replace(/\/$/, "") + "<"));

  console.log("[PASS] live API: factory preview stages Fairfield brandWebsite; production unaffected");
  console.log(
    JSON.stringify(
      {
        productionWebsite: prodBrand.brandWebsite,
        previewWebsite: previewBrand.brandWebsite,
        staging: previewBrand.factoryPreviewStaging,
        airtableWrites: false,
      },
      null,
      2
    )
  );
}

mainSync();
mainLive()
  .then(() => {
    console.log("READY FOR CHATGPT QA — factory preview Brand Website overlay");
  })
  .catch((err) => {
    console.error("[FAIL]", err?.stack || err);
    process.exit(1);
  });
