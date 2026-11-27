#!/usr/bin/env node
/**
 * Market Scope segmented control — compact content-fit width.
 *   npm run test:gdi-market-scope-compact-width-v1
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
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

const css = fs.readFileSync(
  path.join(ROOT, "public/css/group-demand-intelligence.css"),
  "utf8",
);
const html = fs.readFileSync(
  path.join(ROOT, "public/group-demand-intelligence.html"),
  "utf8",
);
const ui = fs.readFileSync(
  path.join(ROOT, "public/js/group-demand-intelligence/dealality-gdi-ui.js"),
  "utf8",
);

check("market_scope_is_content_sized_with_10px_padding", () => {
  assert.match(css, /\.gdi-page \.gdi-market-scope-filter \{[\s\S]*?flex:\s*0 0 auto;/);
  assert.match(css, /\.gdi-page \.gdi-market-scope \{[\s\S]*?display:\s*inline-flex;/);
  assert.match(css, /\.gdi-page \.gdi-market-scope__btn \{[\s\S]*?padding:\s*0 10px;/);
  assert.doesNotMatch(css, /flex:\s*0 0 420px/);
  assert.doesNotMatch(
    css,
    /grid-template-columns:\s*minmax\(72px,\s*0\.85fr\)/,
  );
});

check("labels_and_behavior_unchanged", () => {
  assert.match(ui, /data-gdi-market-scope="/);
  assert.match(ui, /MARKET_SCOPE\.ALL|value:\s*MARKET_SCOPE\.ALL|"ALL"/);
  assert.match(ui, /MARKET_SCOPE\.HOTEL|"HOTEL"/);
  assert.match(ui, /MARKET_SCOPE\.COMPETITIVE|"COMPETITIVE"/);
  assert.match(css, /height:\s*var\(--gdi-toolbar-control-height\)/);
  assert.match(html, /group-demand-intelligence\.css\?v=gdi-market-scope-compact-20261127/);
});

if (failed) {
  console.error(`\n${failed} failure(s)`);
  process.exit(1);
}
console.log("\nOK test:gdi-market-scope-compact-width-v1");
