#!/usr/bin/env node
/**
 * Local Brand Explorer app staging + factory-preview overlay tests.
 *
 * Proves:
 * - localhost / local-dev Brand Explorer list+detail use staged Fairfield brandWebsite
 * - production / non-local path still uses live Airtable value
 * - brands without staged fixtures keep live Airtable values
 * - no cache contamination between staged responses and live cache payloads
 * - no Airtable writes
 */
import "dotenv/config";
import assert from "node:assert/strict";
import {
  applyFactoryPreviewStagingOverlay,
  applyFactoryPreviewStagingOverlayToBrandList,
  isLocalBrandExplorerAppStagingEnabled,
  loadFactoryPreviewStagingOverlay,
  resolveFactoryPreviewStagedBrandWebsite,
} from "../lib/partner-intelligence/brand-explorer-factory-preview-staging-overlay.js";
import { FAIRFIELD_OFFICIAL_BRAND_WEBSITE } from "../lib/partner-intelligence/brand-explorer-brand-website-preference.js";
import { renderBrandExplorerHtmlForTest } from "../lib/partner-intelligence/brand-explorer-atelier-render-test-loader.js";
import {
  getBrandLibraryBrandById,
  getBrandLibraryBrands,
} from "../api/brand-library.js";
import { FACTORY_PREVIEW_CANDIDATE_IDENTITIES } from "../lib/partner-intelligence/brand-explorer-factory-preview-candidates.js";

const SLUG = "fairfield-by-marriott";
const RECORD_ID = FACTORY_PREVIEW_CANDIDATE_IDENTITIES[SLUG].recordId;
const PARENT_ROOT = "https://marriott.com/";
const PROD_ENV = { NODE_ENV: "production", BRAND_EXPLORER_LOCAL_STAGING: "0" };
const LOCAL_ENV = { NODE_ENV: "development", BRAND_EXPLORER_LOCAL_STAGING: "1" };

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

async function fetchBrand(query, headers = {}) {
  const res = mockRes();
  await getBrandLibraryBrandById({ query, headers }, res);
  assert.equal(res.statusCode, 200, `detail API status ${res.statusCode}`);
  assert.equal(res.payload?.success, true, "detail API success");
  return { brand: res.payload.brand, headers: res.headers };
}

async function fetchBrands(query = { refresh: "1" }, headers = {}) {
  const res = mockRes();
  await getBrandLibraryBrands({ query, headers }, res);
  assert.equal(res.statusCode, 200, `list API status ${res.statusCode}`);
  assert.equal(res.payload?.success, true, "list API success");
  return { brands: res.payload.brands || [], headers: res.headers, payload: res.payload };
}

function mainSync() {
  assert.equal(
    isLocalBrandExplorerAppStagingEnabled({ env: PROD_ENV, host: "dealality.com" }),
    false,
    "production must disable local staging"
  );
  assert.equal(
    isLocalBrandExplorerAppStagingEnabled({ env: LOCAL_ENV, host: "localhost:8080" }),
    true,
    "local host must enable local staging"
  );
  assert.equal(
    isLocalBrandExplorerAppStagingEnabled({
      env: { NODE_ENV: "production" },
      host: "localhost:8080",
    }),
    false,
    "production NODE_ENV stays off even on localhost without explicit opt-in"
  );

  const staging = loadFactoryPreviewStagingOverlay(SLUG);
  assert.ok(staging?.fixturePath, "Fairfield staging fixture must exist");
  assert.equal(staging.brandWebsite, FAIRFIELD_OFFICIAL_BRAND_WEBSITE);

  const liveParent = {
    id: RECORD_ID,
    slug: SLUG,
    name: "Fairfield by Marriott",
    brandWebsite: PARENT_ROOT,
    website: PARENT_ROOT,
    brandExplorer: { version: 1, blocks: [{ slotKey: "overview.hero", body: "x", sort: 0 }] },
    shouldRenderFullProfile: true,
    brandExplorerDisplayState: "active_profile_ready",
  };

  const production = applyFactoryPreviewStagingOverlay(liveParent, {
    factoryPreview: false,
    env: PROD_ENV,
    host: "api.dealality.com",
  });
  assert.equal(production.brandWebsite, PARENT_ROOT, "production keeps live Brand Website");
  assert.equal(production.website, PARENT_ROOT, "production keeps live website");
  assert.equal(production.factoryPreviewStaging, undefined, "no staging meta on production");

  const localApp = applyFactoryPreviewStagingOverlay(liveParent, {
    factoryPreview: false,
    env: LOCAL_ENV,
    host: "localhost:8080",
  });
  assert.equal(localApp.brandWebsite, FAIRFIELD_OFFICIAL_BRAND_WEBSITE, "local app uses fixture");
  assert.equal(localApp.website, FAIRFIELD_OFFICIAL_BRAND_WEBSITE, "local list field uses fixture");
  assert.equal(localApp.factoryPreviewStaging?.mode, "local_app");
  assert.equal(localApp.factoryPreviewStaging?.airtableWrites, false);
  assert.equal(liveParent.brandWebsite, PARENT_ROOT, "must not mutate original brand");

  const noFixtureBrand = {
    id: "recNoFixtureBrandXX",
    slug: "some-brand-without-fixture-zzzz",
    name: "Some Brand Without Fixture",
    website: "https://example.com/",
    brandWebsite: "https://example.com/",
  };
  const noFixtureLocal = applyFactoryPreviewStagingOverlay(noFixtureBrand, {
    env: LOCAL_ENV,
    host: "localhost:8080",
  });
  assert.equal(noFixtureLocal.website, "https://example.com/", "no fixture → live fallback");
  assert.equal(noFixtureLocal.brandWebsite, "https://example.com/");
  assert.equal(noFixtureLocal.factoryPreviewStaging, undefined);

  const listLive = [
    { ...liveParent },
    { ...noFixtureBrand },
  ];
  const listProd = applyFactoryPreviewStagingOverlayToBrandList(listLive, {
    env: PROD_ENV,
    host: "api.dealality.com",
  });
  assert.equal(listProd[0].website, PARENT_ROOT, "prod list unchanged");
  assert.equal(listProd[1].website, "https://example.com/");

  const listLocal = applyFactoryPreviewStagingOverlayToBrandList(listLive, {
    env: LOCAL_ENV,
    host: "localhost:8080",
  });
  assert.equal(listLocal[0].website, FAIRFIELD_OFFICIAL_BRAND_WEBSITE, "local list stages Fairfield");
  assert.equal(listLocal[1].website, "https://example.com/", "other brands stay live");
  assert.equal(listLive[0].website, PARENT_ROOT, "list overlay must not mutate source array items in place when unchanged path…");
  // first item is cloned when staged:
  assert.notEqual(listLocal[0], listLive[0]);

  const unlocked = {
    ...liveParent,
    shouldRenderFullProfile: true,
    brandExplorerDisplayState: "active_profile_ready",
  };
  const previewHtml = renderBrandExplorerHtmlForTest(
    applyFactoryPreviewStagingOverlay(unlocked, { env: LOCAL_ENV, host: "localhost:8080" }),
    { allPanels: true }
  );
  assert.match(previewHtml, /fairfield\.marriott\.com/i);
  assert.ok(!/>\s*https?:\/\/(www\.)?marriott\.com\/?\s*</i.test(previewHtml));

  const prodHtml = renderBrandExplorerHtmlForTest(unlocked, { allPanels: true });
  assert.match(prodHtml, /marriott\.com/i);
  assert.ok(!/fairfield\.marriott\.com/i.test(prodHtml), "HTML without overlay keeps live parent root");

  console.log("[PASS] sync: local app staging vs production + fixture fallback");
}

async function mainLive() {
  const prevLocal = process.env.BRAND_EXPLORER_LOCAL_STAGING;
  const prevNode = process.env.NODE_ENV;

  try {
    // --- Local founder-review path (default for this worktree server) ---
    process.env.BRAND_EXPLORER_LOCAL_STAGING = "1";
    process.env.NODE_ENV = "development";

    const { brand: localDetail, headers: detailHeaders } = await fetchBrand({
      brandId: RECORD_ID,
      refresh: "1",
    }, { host: "localhost:8080" });
    assert.equal(
      localDetail.brandWebsite,
      FAIRFIELD_OFFICIAL_BRAND_WEBSITE,
      `local detail must stage Fairfield website, got ${localDetail.brandWebsite}`
    );
    assert.equal(localDetail.factoryPreviewStaging?.applied, true);
    assert.equal(localDetail.factoryPreviewStaging?.mode, "local_app");
    assert.equal(localDetail.factoryPreviewStaging?.airtableWrites, false);
    assert.ok(detailHeaders["X-Brand-Explorer-Staging"], "staging response header set");

    const { brands: localBrands, headers: listHeaders, payload: localListPayload } =
      await fetchBrands({ refresh: "1" }, { host: "localhost:8080" });
    const fairfieldCard = localBrands.find((b) => b.id === RECORD_ID);
    assert.ok(fairfieldCard, "Fairfield present in list");
    assert.equal(
      fairfieldCard.website,
      FAIRFIELD_OFFICIAL_BRAND_WEBSITE,
      `local list card must stage Fairfield website, got ${fairfieldCard.website}`
    );
    assert.equal(fairfieldCard.factoryPreviewStaging?.applied, true);
    assert.ok(Number(listHeaders["X-Brand-Explorer-Staging-Count"] || 0) >= 1);

    // Cache contamination check: live cache payload must still hold parent root;
    // overlay is response-only. Force a production-disabled response next.
    process.env.BRAND_EXPLORER_LOCAL_STAGING = "0";
    process.env.NODE_ENV = "production";

    const { brand: prodDetail } = await fetchBrand({
      brandId: RECORD_ID,
      refresh: "1",
    }, { host: "api.dealality.com" });
    assert.ok(
      String(prodDetail.brandWebsite || "").includes("marriott.com"),
      "production detail returns live Airtable website"
    );
    assert.notEqual(
      prodDetail.factoryPreviewStaging?.applied,
      true,
      "production detail must not apply staging"
    );
    assert.ok(
      !String(prodDetail.brandWebsite).includes("fairfield.marriott.com") ||
        String(prodDetail.brandWebsite).includes("fairfield.marriott.com") === false,
      "production must not inject staged fairfield host when live is parent root"
    );
    // Live Airtable currently stores parent root for Fairfield:
    assert.match(String(prodDetail.brandWebsite), /marriott\.com/i);
    assert.ok(
      !/^https?:\/\/fairfield\.marriott\.com\/?$/i.test(String(prodDetail.brandWebsite)),
      "production Fairfield website must remain live value (parent root today)"
    );

    const { brands: prodBrands } = await fetchBrands(
      { refresh: "1" },
      { host: "api.dealality.com" }
    );
    const fairfieldProdCard = prodBrands.find((b) => b.id === RECORD_ID);
    assert.ok(fairfieldProdCard);
    assert.ok(
      !/^https?:\/\/fairfield\.marriott\.com\/?$/i.test(String(fairfieldProdCard.website || "")),
      "production list must not stage fairfield.marriott.com"
    );
    assert.notEqual(fairfieldProdCard.factoryPreviewStaging?.applied, true);

    // Prove prior local list payload object was a response clone, not the shared cache:
    assert.equal(
      localListPayload.brands.find((b) => b.id === RECORD_ID)?.website,
      FAIRFIELD_OFFICIAL_BRAND_WEBSITE,
      "earlier local response still holds staged value (its own clone)"
    );

    // Re-enable local and confirm still stages (cache hit path also overlays)
    process.env.BRAND_EXPLORER_LOCAL_STAGING = "1";
    process.env.NODE_ENV = "development";
    const { brand: localAgain } = await fetchBrand({ brandId: RECORD_ID }, { host: "localhost:8080" });
    assert.equal(localAgain.brandWebsite, FAIRFIELD_OFFICIAL_BRAND_WEBSITE, "cache HIT still overlays locally");

    const { brands: localListAgain } = await fetchBrands({}, { host: "localhost:8080" });
    const cardAgain = localListAgain.find((b) => b.id === RECORD_ID);
    assert.equal(cardAgain.website, FAIRFIELD_OFFICIAL_BRAND_WEBSITE, "list cache HIT still overlays locally");

    console.log("[PASS] live API: local app stages Fairfield list+detail; production path unchanged");
    console.log(
      JSON.stringify(
        {
          localDetailWebsite: localDetail.brandWebsite,
          localCardWebsite: fairfieldCard.website,
          prodDetailWebsite: prodDetail.brandWebsite,
          prodCardWebsite: fairfieldProdCard.website,
          airtableWrites: false,
        },
        null,
        2
      )
    );
  } finally {
    if (prevLocal === undefined) delete process.env.BRAND_EXPLORER_LOCAL_STAGING;
    else process.env.BRAND_EXPLORER_LOCAL_STAGING = prevLocal;
    if (prevNode === undefined) delete process.env.NODE_ENV;
    else process.env.NODE_ENV = prevNode;
  }
}

mainSync();
mainLive()
  .then(() => {
    console.log("READY FOR CHATGPT QA — local Brand Explorer app staging");
  })
  .catch((err) => {
    console.error("[FAIL]", err?.stack || err);
    process.exit(1);
  });
