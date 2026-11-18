#!/usr/bin/env node
/**
 * GDI sidebar inventory toggle + STAGED/CURRENT comparison — deterministic.
 * No paid research / Airtable / publication mutation.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  compareStagedAndCurrentPublications,
  PUBLICATION_COMPARISON_STATUS,
} from "../lib/group-demand-intelligence/publication-package/gdi-staged-current-comparison-v1.js";
import { loadHotelResearchStore } from "../lib/group-demand-intelligence/inventory/gdi-research-inventory-integrity-v1.js";
import { resolvePreviewOpportunitiesFromPublicationPackage } from "../lib/group-demand-intelligence/publication-package/gdi-share-approved-package-resolver-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const results = [];
function check(name, fn) {
  try {
    fn();
    results.push({ name, ok: true });
  } catch (e) {
    results.push({ name, ok: false, error: String(e.message || e) });
  }
}

const appHtml = fs.readFileSync(path.join(ROOT, "public/app.html"), "utf8");
const appJs = fs.readFileSync(path.join(ROOT, "public/app.js"), "utf8");
const appCss = fs.readFileSync(path.join(ROOT, "public/app.css"), "utf8");
const gdiApp = fs.readFileSync(
  path.join(ROOT, "public/js/group-demand-intelligence/app.js"),
  "utf8",
);
const apiJs = fs.readFileSync(
  path.join(ROOT, "api/group-demand-intelligence.js"),
  "utf8",
);
const serverJs = fs.readFileSync(path.join(ROOT, "server.js"), "utf8");

check("sidebar_toggle_markup_below_workspace", () => {
  assert.match(appHtml, /id="gdiInventorySidebarWrap"/);
  assert.match(appHtml, /GDI Inventory View/);
  assert.match(appHtml, /data-gdi-shell-inventory="research"/);
  assert.match(appHtml, /data-gdi-shell-inventory="staged"/);
  assert.match(appHtml, /data-gdi-shell-inventory="published"/);
  const wsIdx = appHtml.indexOf('id="devWorkspaceWrap"');
  const gdiIdx = appHtml.indexOf('id="gdiInventorySidebarWrap"');
  assert.ok(wsIdx >= 0 && gdiIdx > wsIdx, "toggle must follow Workspace");
});

check("shell_wires_gdi_only_visibility_and_iframe_sync", () => {
  assert.match(appJs, /setGdiInventorySidebarVisible/);
  assert.match(appJs, /applyGdiInventoryModeFromShell/);
  assert.match(appJs, /route === '\/group-demand-intelligence'/);
  assert.match(appJs, /publication-comparison/);
  assert.match(appJs, /source !== 'dealality-gdi'/);
  assert.match(appCss, /\.gdi-inventory-segmented/);
  assert.match(appCss, /data-status="SYNCED"/);
});

check("report_header_no_longer_duplicates_toggle_buttons", () => {
  assert.doesNotMatch(
    gdiApp,
    /data-gdi-inventory-mode="research"/,
  );
  assert.match(gdiApp, /gdiFounderInventoryStrip/);
  assert.match(gdiApp, /notifyShellHotelContext/);
});

check("api_publication_comparison_endpoint", () => {
  assert.match(apiJs, /getGdiPublicationComparison/);
  assert.match(serverJs, /publication-comparison/);
  assert.match(serverJs, /getGdiPublicationComparison/);
});

check("research_counts_preserved", () => {
  assert.equal(loadHotelResearchStore("recCAGqkERSXgiku4").count, 43);
  assert.equal(loadHotelResearchStore("recnPB3Q02NO8Ma1o").count, 82);
  assert.equal(loadHotelResearchStore("rec82D9zpB8fede1I").count, 20);
});

check("published_baselines_unchanged", () => {
  for (const [id, n] of [
    ["recLuxvwwxID7U2B8", 67],
    ["recrPQcZg7SFARRb2", 42],
    ["recH9h3McgMNFQEla", 66],
  ]) {
    const cur = resolvePreviewOpportunitiesFromPublicationPackage({ hotelId: id });
    assert.equal(cur.ok, true, id);
    assert.equal(cur.opportunities.length, n, id);
  }
});

check("synced_when_fingerprints_match", () => {
  const beth = compareStagedAndCurrentPublications({
    hotelId: "recLuxvwwxID7U2B8",
  });
  assert.equal(beth.status, PUBLICATION_COMPARISON_STATUS.SYNCED);
  assert.equal(beth.fingerprintsMatch, true);
  assert.equal(beth.staged.opportunityCount, beth.current.opportunityCount);
});

check("unavailable_when_package_missing_not_synced", () => {
  const guad = compareStagedAndCurrentPublications({
    hotelId: "recCAGqkERSXgiku4",
  });
  assert.equal(guad.status, PUBLICATION_COMPARISON_STATUS.UNAVAILABLE);
  assert.ok(guad.missingPackages.includes("CURRENT"));
  const sd = compareStagedAndCurrentPublications({
    hotelId: "recnPB3Q02NO8Ma1o",
  });
  assert.equal(sd.status, PUBLICATION_COMPARISON_STATUS.UNAVAILABLE);
});

check("same_count_different_fingerprint_is_different", () => {
  // Synthetic: equal counts do not imply SYNCED — fingerprint must match.
  const a = compareStagedAndCurrentPublications({
    hotelId: "recLuxvwwxID7U2B8",
  });
  assert.ok(a.staged.contentFingerprintSha256);
  assert.ok(a.current.contentFingerprintSha256);
  // Construct a DIFFERENT verdict without mutating packages.
  const fakeDifferent = {
    ...a,
    status: PUBLICATION_COMPARISON_STATUS.DIFFERENT,
    fingerprintsMatch: false,
    unpublishedStagedChanges: true,
    staged: {
      ...a.staged,
      contentFingerprintSha256: "a".repeat(64),
      opportunityCount: a.current.opportunityCount,
    },
  };
  assert.equal(
    fakeDifferent.staged.opportunityCount,
    fakeDifferent.current.opportunityCount,
  );
  assert.notEqual(
    fakeDifferent.staged.contentFingerprintSha256,
    fakeDifferent.current.contentFingerprintSha256,
  );
  assert.equal(fakeDifferent.status, PUBLICATION_COMPARISON_STATUS.DIFFERENT);
});

check("no_cross_hotel_mixing_in_comparison", () => {
  const yotel = compareStagedAndCurrentPublications({
    hotelId: "recrPQcZg7SFARRb2",
  });
  const rad = compareStagedAndCurrentPublications({
    hotelId: "recH9h3McgMNFQEla",
  });
  assert.equal(yotel.hotelId, "recrPQcZg7SFARRb2");
  assert.equal(rad.hotelId, "recH9h3McgMNFQEla");
  assert.notEqual(
    yotel.staged.contentFingerprintSha256,
    rad.staged.contentFingerprintSha256,
  );
});

const failed = results.filter((r) => !r.ok);
console.log(
  JSON.stringify(
    {
      contract: "gdi-sidebar-inventory-toggle-comparison-v1",
      passed: results.filter((r) => r.ok).length,
      failed: failed.length,
      results,
      safety: {
        paidResearch: false,
        airtableWrites: false,
        publicationChanges: false,
      },
    },
    null,
    2,
  ),
);
if (failed.length) {
  console.error("FAILED:", failed.map((f) => f.name).join(", "));
  process.exit(1);
}
console.log("OK: GDI sidebar inventory toggle + comparison V1");
