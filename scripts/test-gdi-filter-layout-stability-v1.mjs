#!/usr/bin/env node
/**
 * GDI property-bar filter layout stability (no content-driven width shift).
 *   npm run test:gdi-filter-layout-stability-v1
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const css = fs.readFileSync(
  path.join(ROOT, "public/css/group-demand-intelligence.css"),
  "utf8",
);
const html = fs.readFileSync(
  path.join(ROOT, "public/group-demand-intelligence.html"),
  "utf8",
);

let failed = 0;
function check(name, fn) {
  try {
    fn();
    console.log(`PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`FAIL ${name}: ${err && err.message ? err.message : err}`);
  }
}

check("cache_bust", () => {
  assert.match(html, /group-demand-intelligence\.css\?v=gdi-/);
});

check("opportunity_type_fixed_width", () => {
  assert.match(
    css,
    /\.gdi-page \.gdi-opportunity-type-filter \{[\s\S]*?flex:\s*0 0 220px;[\s\S]*?width:\s*220px;/,
  );
  assert.match(
    css,
    /\.gdi-page \.gdi-opportunity-type-filter \.filter-select \{[\s\S]*?text-overflow:\s*ellipsis;/,
  );
});

check("weekly_and_market_scope_fixed", () => {
  assert.match(
    css,
    /\.gdi-page \.gdi-weekly-filter \{[\s\S]*?flex:\s*0 0 168px;[\s\S]*?width:\s*168px;/,
  );
  // Market Scope is content-sized (compact padding + fit-to-label), not a fixed wide box.
  assert.match(
    css,
    /\.gdi-page \.gdi-market-scope-filter \{[\s\S]*?flex:\s*0 0 auto;[\s\S]*?width:\s*auto;/,
  );
  assert.match(
    css,
    /\.gdi-page \.gdi-market-scope \{[\s\S]*?display:\s*inline-flex;/,
  );
  assert.match(
    css,
    /\.gdi-page \.gdi-market-scope__btn \{[\s\S]*?padding:\s*0 10px;/,
  );
});

check("property_select_ellipsis", () => {
  assert.match(
    css,
    /\.gdi-page \.gdi-property-bar \.filter-group--grow [\s\S]*?text-overflow:\s*ellipsis;/,
  );
});

if (failed) {
  console.error(`\n${failed} failure(s)`);
  process.exit(1);
}
console.log("\nOK test:gdi-filter-layout-stability-v1");
