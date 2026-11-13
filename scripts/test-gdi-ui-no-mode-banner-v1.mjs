#!/usr/bin/env node
/**
 * Permanent gate: Mode A/B/C presentation banners must not appear in normal GDI UI.
 *
 *   npm run test:gdi-ui-no-mode-banner-v1
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
    console.error(`FAIL ${name}: ${err.message}`);
  }
}

const appJs = fs.readFileSync(
  path.join(ROOT, "public/js/group-demand-intelligence/app.js"),
  "utf8",
);
const shareJs = fs.readFileSync(
  path.join(ROOT, "public/js/group-demand-intelligence/share-app.js"),
  "utf8",
);
const authHtml = fs.readFileSync(
  path.join(ROOT, "public/group-demand-intelligence.html"),
  "utf8",
);

check("auth_banner_gated_by_gdiDiag", () => {
  assert.match(appJs, /function wantsPresentationModeDiagnostics\s*\(/);
  assert.match(appJs, /params\.get\("gdiDiag"\)/);
  assert.match(appJs, /if\s*\(\s*wantsPresentationModeDiagnostics\s*\(\s*\)\s*\)/);
  // Must not unconditionally inject Mode A/B banner into root HTML.
  assert.doesNotMatch(
    appJs,
    /root\.innerHTML\s*=\s*[\s\S]{0,400}Mode B — Founder Publication Preview/,
  );
});

check("live_and_package_modes_preserved", () => {
  assert.match(appJs, /function wantsLiveResearchBrowse\s*\(/);
  assert.match(appJs, /function wantsPublicationPackagePreview\s*\(/);
  assert.match(appJs, /params\.get\("live"\)/);
  assert.match(appJs, /publicationPackage=1/);
});

check("share_app_has_no_mode_banner", () => {
  assert.doesNotMatch(shareJs, /Mode A — Live Research/);
  assert.doesNotMatch(shareJs, /Mode B — Founder Publication Preview/);
  assert.doesNotMatch(shareJs, /gdiPresentationModeBanner/);
  assert.doesNotMatch(shareJs, /gdi-presentation-mode-banner/);
});

check("auth_html_cache_bust_updated", () => {
  assert.match(authHtml, /app\.js\?v=gdi-ui-no-mode-banner-v1/);
});

if (failed) {
  console.error(`\n${failed} failure(s)`);
  process.exit(1);
}
console.log("\nOK test:gdi-ui-no-mode-banner-v1");
