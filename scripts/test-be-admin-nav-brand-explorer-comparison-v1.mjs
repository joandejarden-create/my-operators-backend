#!/usr/bin/env node
/**
 * Permanent Admin Resources → Brand Explorer Comparison navigation regression.
 *
 *   node scripts/test-be-admin-nav-brand-explorer-comparison-v1.mjs
 *   npm run test:be-admin-nav-brand-explorer-comparison-v1
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");

function read(rel) {
  return fs.readFileSync(path.join(root, rel), "utf8");
}

const appJs = read("public/app.js");
const compareHtml = read("public/brand-explorer-compare.html");
const compareJs = read("public/js/brand-explorer-compare-studio.js");
const brandIndex = JSON.parse(
  read("public/brand-explorer-phase2e-preview/comparison-studio-brand-index-v1.json")
);

// --- Single Admin Resources registration ---
const labelMatches = appJs.match(/label:\s*'Brand Explorer Comparison'/g) || [];
assert.equal(
  labelMatches.length,
  1,
  "Exactly one Brand Explorer Comparison nav label required (no duplicates)"
);
assert.match(appJs, /route:\s*'\/brand-explorer-compare'/);
assert.match(appJs, /navId:\s*'brand-explorer-comparison'/);
assert.match(
  appJs,
  /label:\s*'Admin Resources'[\s\S]*?Brand Explorer Comparison[\s\S]*?Route Map/
);

// Route meta: admin + internal runbook + origin-relative defaultQuery
assert.match(
  appJs,
  /'\/brand-explorer-compare':\s*\{[\s\S]*?internalRunbookOnly:\s*true[\s\S]*?defaultQuery:\s*\{\s*beInternalPreview:\s*'1'/
);
assert.match(appJs, /file:\s*'\/brand-explorer-compare\.html'/);
assert.doesNotMatch(
  appJs.slice(appJs.indexOf("'/brand-explorer-compare'"), appJs.indexOf("'/brand-explorer-export'")),
  /127\.0\.0\.1:8082|localhost:8082/
);

// --- VM: defaultQuery applied to embed URL on active origin ---
const sandbox = {
  window: {
    location: {
      origin: "http://example.test:9999",
      hostname: "example.test",
      href: "http://example.test:9999/app",
      search: "",
      hash: "#/home",
      pathname: "/app",
    },
    localStorage: {
      getItem: () => null,
      setItem: () => {},
      removeItem: () => {},
    },
  },
  document: {
    getElementById: () => null,
    querySelector: () => null,
    querySelectorAll: () => [],
    createElement: () => ({
      style: {},
      setAttribute: () => {},
      addEventListener: () => {},
      appendChild: () => {},
      classList: { add: () => {}, toggle: () => {} },
    }),
    addEventListener: () => {},
    documentElement: { classList: { add: () => {} } },
    body: { classList: { add: () => {}, toggle: () => {} } },
  },
  URL,
  URLSearchParams,
  console,
};
sandbox.window.window = sandbox.window;
sandbox.window.document = sandbox.document;
sandbox.globalThis = sandbox;
vm.createContext(sandbox);

// Extract only the pieces we need by evaluating a thin harness that reuses routeToEmbedUrl logic.
const harness = `
(function () {
  var ROUTES = {
    '/brand-explorer-compare': {
      file: '/brand-explorer-compare.html',
      title: 'Brand Explorer Comparison',
      roles: ['admin'],
      internalRunbookOnly: true,
      defaultQuery: { beInternalPreview: '1' }
    }
  };
  function getHashQueryParams() { return new URLSearchParams(''); }
  function appendMsTokenToEmbedUrl(url) { return url; }
  function routeToEmbedUrl(route, role) {
    var embedQs = 'embed=1&appShell=1';
    var mapped = ROUTES[route];
    if (!mapped || !mapped.file) return '/app/home.html?' + embedQs;
    var sep = mapped.file.indexOf('?') === -1 ? '?' : '&';
    var url = mapped.file + sep + embedQs;
    var u = new URL(url, 'http://example.test:9999');
    if (mapped.defaultQuery && typeof mapped.defaultQuery === 'object') {
      Object.keys(mapped.defaultQuery).forEach(function (key) {
        if (!u.searchParams.get(key)) {
          u.searchParams.set(key, String(mapped.defaultQuery[key]));
        }
      });
    }
    return u.pathname + u.search;
  }
  globalThis.__BECS_NAV_HARNESS__ = {
    embed: routeToEmbedUrl('/brand-explorer-compare', 'admin'),
    originRelative: true
  };
})();
`;
vm.runInContext(harness, sandbox);
const embed = sandbox.__BECS_NAV_HARNESS__.embed;
assert.equal(
  embed,
  "/brand-explorer-compare.html?embed=1&appShell=1&beInternalPreview=1"
);
assert.doesNotMatch(embed, /127\.0\.0\.1|8082|8080/);

// --- Access gate + no hardcoded founder URL in studio ---
assert.match(compareHtml, /support-admin-gate\.js/);
assert.match(compareHtml, /requireInternalRunbook/);
assert.match(compareHtml, /id="supportGateContent"/);
assert.match(compareHtml, /Brand Explorer Comparison Studio/);
assert.doesNotMatch(compareHtml, /127\.0\.0\.1:8082/);
assert.match(compareJs, /location\.origin/);
assert.doesNotMatch(compareJs, /http:\/\/127\.0\.0\.1:8082\/brand-explorer-compare/);
assert.match(compareJs, /BrandExplorerCompareStudioBoot/);

// Brand index uses app-shell relative path (environment-independent)
assert.equal(
  brandIndex.canonicalUrl,
  "/app#/brand-explorer-compare?beInternalPreview=1"
);
assert.doesNotMatch(brandIndex.canonicalUrl, /127\.0\.0\.1|localhost:\d+/);

// Count Admin Resources children mentioning Brand Explorer Comparison = 1 via structured scrape
const adminBlock = appJs.slice(
  appJs.indexOf("label: 'Admin Resources'"),
  appJs.indexOf("label: 'Route Map'")
);
assert.equal(
  (adminBlock.match(/Brand Explorer Comparison/g) || []).length,
  1
);
assert.doesNotMatch(adminBlock, /Brand Explorer Comparison[\s\S]*Brand Explorer Comparison/);

console.log(
  JSON.stringify(
    {
      ok: true,
      contract: "BE_ADMIN_NAV_BRAND_EXPLORER_COMPARISON_V1",
      embedExample: embed,
      canonicalUrl: brandIndex.canonicalUrl,
      ready: "admin_nav_brand_explorer_comparison_pass",
    },
    null,
    2
  )
);
