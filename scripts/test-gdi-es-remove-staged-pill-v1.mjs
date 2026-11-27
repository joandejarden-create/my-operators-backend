#!/usr/bin/env node
/**
 * Executive Summary hero must not render a STAGED/CURRENT status pill.
 * Publication dataState / STAGED inventory behavior remains unchanged.
 *   npm run test:gdi-es-remove-staged-pill-v1
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

check("hero_title_has_no_status_chip_markup", () => {
  const js = fs.readFileSync(
    path.join(ROOT, "public/js/group-demand-intelligence/executive-summary-tab.js"),
    "utf8",
  );
  assert.match(js, /<h1>Executive Summary<\/h1>/);
  assert.doesNotMatch(js, /function stagedChip/);
  assert.doesNotMatch(js, /gdi-es-status-chip--staged/);
  assert.doesNotMatch(js, /gdi-es-status-chip--current/);
  assert.doesNotMatch(js, />Staged<\/span>/);
  assert.match(js, /gdi-es-hero__title/);
  assert.match(js, /gdi-es-hero__meta/);
});

check("cache_bust_updated_and_publication_logic_untouched", () => {
  const html = fs.readFileSync(
    path.join(ROOT, "public/group-demand-intelligence.html"),
    "utf8",
  );
  const app = fs.readFileSync(
    path.join(ROOT, "public/js/group-demand-intelligence/app.js"),
    "utf8",
  );
  assert.match(html, /executive-summary-tab\.js\?v=gdi-es-remove-staged-pill-20261127/);
  assert.match(app, /FOUNDER_STAGED_PREVIEW/);
  assert.match(app, /preview=staged|STAGED/);
});

if (failed) {
  console.error(`\n${failed} failure(s)`);
  process.exit(1);
}
console.log("\nOK test:gdi-es-remove-staged-pill-v1");
