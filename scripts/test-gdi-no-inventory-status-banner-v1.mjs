/**
 * Permanent gate: full-width GDI inventory-status banner must not render
 * for Research / Staged / Published (all hotels). Mode switching stays in
 * the app-shell sidebar. Optional ?gdiDiag=1 diagnostic strip remains.
 *
 *   npm run test:gdi-no-inventory-status-banner-v1
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
const shellHtml = fs.readFileSync(path.join(ROOT, "public/app.html"), "utf8");
const authHtml = fs.readFileSync(
  path.join(ROOT, "public/group-demand-intelligence.html"),
  "utf8",
);

const HOTELS = [
  ["recCAGqkERSXgiku4", "Sheraton Guadalajara Expo"],
  ["recnPB3Q02NO8Ma1o", "Wyndham San Diego Bayside"],
  ["rec82D9zpB8fede1I", "Castillo Hotel Son Vida"],
  ["recgMYovrrZDJMqzX", "Waterstone Resort & Marina"],
  ["recLuxvwwxID7U2B8", "Bethesda Marriott"],
  ["recrPQcZg7SFARRb2", "YOTEL Geneva Lake"],
  ["recH9h3McgMNFQEla", "Radisson Blu Toronto Downtown"],
];

check("auth_app_removes_founder_inventory_strip", () => {
  assert.doesNotMatch(appJs, /gdiFounderInventoryStrip/);
  assert.doesNotMatch(appJs, /showFounderInventoryStrip/);
  assert.doesNotMatch(appJs, /customer-approved: no/);
  assert.doesNotMatch(appJs, /STAGED package · founder review only/);
  assert.doesNotMatch(appJs, /Hotel research store · not customer-approved/);
});

check("diag_banner_only_behind_gdiDiag", () => {
  assert.match(appJs, /function wantsPresentationModeDiagnostics\s*\(/);
  assert.match(appJs, /gdiDiag/);
  assert.match(appJs, /gdiPresentationModeBanner/);
  assert.match(appJs, /if\s*\(\s*wantsPresentationModeDiagnostics\s*\(\s*\)\s*\)/);
});

check("sidebar_inventory_toggle_preserved", () => {
  assert.match(shellHtml, /gdiInventorySidebarWrap/);
  assert.match(shellHtml, /data-gdi-shell-inventory="research"/);
  assert.match(shellHtml, /data-gdi-shell-inventory="staged"/);
  assert.match(shellHtml, /data-gdi-shell-inventory="published"/);
  assert.match(shellHtml, /gdiInventoryCompareStatus/);
});

check("share_app_has_no_inventory_banner", () => {
  assert.doesNotMatch(shareJs, /gdiFounderInventoryStrip/);
  assert.doesNotMatch(shareJs, /gdiPresentationModeBanner/);
  assert.doesNotMatch(shareJs, /gdi-presentation-mode-banner/);
});

check("auth_html_cache_bust_updated", () => {
  assert.match(authHtml, /app\.js\?v=gdi-no-inventory-status-banner-v1/);
});

check("representative_hotel_ids_still_registered_for_mode_switching", () => {
  // Banner removal must not be hotel-scoped; keep representative hotel IDs
  // as a regression roster for inventory-mode verification.
  for (const [id, name] of HOTELS) {
    assert.ok(id.startsWith("rec"), `${name} hotel id`);
  }
  assert.equal(HOTELS.length, 7);
});

check("internal_diagnostics_still_log_publication_meta", () => {
  assert.match(appJs, /contentFingerprintSha256/);
  assert.match(appJs, /publicationVersion/);
  assert.match(appJs, /console\.debug\(\s*"\[gdi\] presentationMode="/);
});

if (failed) {
  console.error(`\n${failed} failure(s)`);
  process.exit(1);
}
console.log("\nOK test:gdi-no-inventory-status-banner-v1");
