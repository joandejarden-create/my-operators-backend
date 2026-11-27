/**
 * Missing STAGED/CURRENT inventory must render as a compact empty state inside
 * the normal GDI shell — never a full-page red "Staged preview unavailable" panel.
 *
 *   npm run test:gdi-missing-inventory-empty-state-v1
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { inspectFounderPreviewPublicationAvailability } from "../lib/group-demand-intelligence/publication-package/gdi-founder-preview-availability-v1.js";

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
const apiJs = fs.readFileSync(
  path.join(ROOT, "api/group-demand-intelligence.js"),
  "utf8",
);
const html = fs.readFileSync(
  path.join(ROOT, "public/group-demand-intelligence.html"),
  "utf8",
);
const css = fs.readFileSync(
  path.join(ROOT, "public/css/group-demand-intelligence.css"),
  "utf8",
);

const HOTELS = [
  ["recC2kbCaS48jFjFF", "Nobu Hotel London Portman Square"],
  ["rec82D9zpB8fede1I", "Castillo Hotel Son Vida"],
  ["recnPB3Q02NO8Ma1o", "Wyndham San Diego Bayside"],
  ["recCAGqkERSXgiku4", "Sheraton Guadalajara Expo"],
  ["recLuxvwwxID7U2B8", "Bethesda Marriott"],
  ["recrPQcZg7SFARRb2", "YOTEL Geneva Lake"],
  ["recH9h3McgMNFQEla", "Radisson Blu Toronto Downtown"],
  ["recgMYovrrZDJMqzX", "Waterstone Resort & Marina"],
];

check("no_full_page_staged_unavailable_panel", () => {
  assert.doesNotMatch(appJs, /showStagedPreviewUnavailable/);
  assert.doesNotMatch(appJs, /Staged preview unavailable/);
  assert.doesNotMatch(appJs, /gdi-error--recoverable/);
  assert.doesNotMatch(appJs, /gdiRecoveryHotel/);
  assert.doesNotMatch(appJs, /Back to Hotels/);
  assert.doesNotMatch(appJs, /Choose Another Hotel/);
  assert.doesNotMatch(appJs, /hotelOptionsHtml/);
});

check("compact_empty_state_helpers_present", () => {
  assert.match(appJs, /applyMissingInventoryEmptyState/);
  assert.match(appJs, /inventoryUnavailableEmptyHtml/);
  assert.match(appJs, /gdi-empty--inventory-unavailable/);
  assert.doesNotMatch(appJs, /No staged version available for this hotel/);
  assert.doesNotMatch(appJs, /No published version available for this hotel/);
  assert.match(appJs, /No opportunities are available for this property/);
  assert.match(appJs, /customerFacingMissingInventoryMessage/);
  assert.match(appJs, /data-gdi-recovery="view-research"/);
  assert.match(css, /\.gdi-empty--inventory-unavailable/);
});

check("load_path_keeps_shell_on_missing_inventory", () => {
  assert.match(appJs, /loadOpportunitiesOrEmpty/);
  assert.match(appJs, /missingInventory/);
  assert.match(appJs, /isMissingInventoryError/);
  assert.match(appJs, /STAGED_PUBLICATION_PREVIEW_UNAVAILABLE/);
  assert.match(appJs, /CURRENT_PUBLICATION_PREVIEW_UNAVAILABLE/);
});

check("published_mode_requests_current_only", () => {
  assert.match(appJs, /preview=current/);
  assert.match(apiJs, /CURRENT_PUBLICATION_PREVIEW_UNAVAILABLE/);
  assert.match(apiJs, /requireCurrentPreview/);
  assert.match(
    apiJs,
    /Refusing to fall back to STAGED, another hotel, or live research/,
  );
});

check("hotel_switch_preserves_inventory_mode_by_default", () => {
  assert.match(appJs, /Preserve explicit Research\/Staged\/Published mode/);
  assert.match(appJs, /opts\.clearPreview === true/);
  assert.doesNotMatch(appJs, /opts\.clearPreview !== false/);
});

check("sidebar_toggle_still_present", () => {
  const shell = fs.readFileSync(path.join(ROOT, "public/app.html"), "utf8");
  assert.match(shell, /gdiInventorySidebarWrap/);
  assert.match(shell, /data-gdi-shell-inventory="staged"/);
});

check("representative_hotels_availability_inspectable", () => {
  for (const [id, name] of HOTELS) {
    const avail = inspectFounderPreviewPublicationAvailability({ hotelId: id });
    assert.equal(avail.hotelId, id, name);
    assert.ok("hasStagedPublication" in avail, name);
    assert.ok("hasCurrentPublication" in avail, name);
  }
});

check("cache_bust_updated", () => {
  assert.match(html, /app\.js\?v=gdi-restore-canonical-staged-workflow-v1/);
  assert.match(
    html,
    /dealality-gdi-ui\.js\?v=gdi-sortable-list-v1/,
  );
});

check("backend_staged_fail_closed_preserved", () => {
  assert.match(apiJs, /STAGED_PUBLICATION_PREVIEW_UNAVAILABLE/);
  assert.match(
    apiJs,
    /Refusing to fall back to another hotel, CURRENT published inventory, or live research/,
  );
});

if (failed) {
  console.error(`\n${failed} failure(s)`);
  process.exit(1);
}
console.log("\nOK test:gdi-missing-inventory-empty-state-v1");
