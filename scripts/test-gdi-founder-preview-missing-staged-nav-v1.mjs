#!/usr/bin/env node
/**
 * Founder preview missing-STAGED navigation recovery — deterministic tests.
 * No paid research, no Airtable writes, no publication mutation.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "path";
import { fileURLToPath } from "node:url";
import { inspectFounderPreviewPublicationAvailability } from "../lib/group-demand-intelligence/publication-package/gdi-founder-preview-availability-v1.js";
import { resolvePreviewOpportunitiesFromPublicationPackage } from "../lib/group-demand-intelligence/publication-package/gdi-share-approved-package-resolver-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const SAN_DIEGO = "recnPB3Q02NO8Ma1o";
const YOTEL = "recrPQcZg7SFARRb2";
const BETHESDA = "recLuxvwwxID7U2B8";
const RADISSON = "recH9h3McgMNFQEla";

const results = [];
function check(name, fn) {
  try {
    fn();
    results.push({ name, ok: true });
  } catch (e) {
    results.push({ name, ok: false, error: String(e.message || e) });
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

check("san_diego_id_is_wyndham_bayside_and_has_no_staged", () => {
  const avail = inspectFounderPreviewPublicationAvailability({
    hotelId: SAN_DIEGO,
  });
  assert.equal(avail.hasStagedPublication, false);
  assert.equal(avail.hasCurrentPublication, false);
  assert.ok(avail.recoveryActions.includes("BACK_TO_HOTELS"));
  assert.ok(avail.recoveryActions.includes("CHOOSE_ANOTHER_HOTEL"));
  assert.ok(!avail.recoveryActions.includes("VIEW_PUBLISHED_VERSION"));
  const stagedOnly = resolvePreviewOpportunitiesFromPublicationPackage({
    hotelId: SAN_DIEGO,
    preferStaged: true,
  });
  assert.equal(stagedOnly.ok, false);
  assert.equal(stagedOnly.mode, "FAIL_CLOSED");
});

check("yotel_has_staged_and_opens_via_prefer_staged", () => {
  const avail = inspectFounderPreviewPublicationAvailability({ hotelId: YOTEL });
  assert.equal(avail.hasStagedPublication, true);
  const staged = resolvePreviewOpportunitiesFromPublicationPackage({
    hotelId: YOTEL,
    preferStaged: true,
  });
  assert.equal(staged.ok, true);
  assert.equal(staged.meta?.previewSource, "STAGED");
});

check("bethesda_has_current_and_staged", () => {
  const avail = inspectFounderPreviewPublicationAvailability({
    hotelId: BETHESDA,
  });
  assert.equal(avail.hasStagedPublication, true);
  assert.equal(avail.hasCurrentPublication, true);
  assert.ok(avail.recoveryActions.includes("VIEW_PUBLISHED_VERSION"));
});

check("radisson_inventory_present", () => {
  const avail = inspectFounderPreviewPublicationAvailability({
    hotelId: RADISSON,
  });
  assert.equal(avail.hasStagedPublication, true);
});

check("ui_has_recoverable_staged_unavailable_state", () => {
  assert.match(appJs, /showStagedPreviewUnavailable/);
  assert.match(appJs, /handleLoadFailure/);
  assert.match(appJs, /Back to Hotels/);
  assert.match(appJs, /Choose Another Hotel/);
  assert.match(appJs, /View Published Version/);
  assert.match(appJs, /data-gdi-recovery="back-hotels"/);
  assert.match(appJs, /data-gdi-recovery="choose-hotel"/);
  assert.match(appJs, /data-gdi-recovery="view-published"/);
  assert.match(appJs, /clearPreview:\s*true/);
  assert.match(appJs, /STAGED_PUBLICATION_PREVIEW_UNAVAILABLE/);
});

check("select_hotel_clears_preview_params", () => {
  assert.match(appJs, /url\.searchParams\.delete\("preview"\)/);
  assert.match(appJs, /Switching properties clears incompatible/);
});

check("api_returns_recovery_without_silent_fallback", () => {
  assert.match(apiJs, /inspectFounderPreviewPublicationAvailability/);
  assert.match(apiJs, /founderMessage/);
  assert.match(apiJs, /recovery:\s*availability/);
  assert.match(
    apiJs,
    /Refusing to fall back to another hotel, CURRENT published inventory, or live research/,
  );
});

check("cache_bust_updated", () => {
  assert.match(html, /gdi-founder-local-research-preview-v1|gdi-founder-preview-missing-staged-nav-v1/);
});

check("no_cross_hotel_fallback_in_recovery_copy", () => {
  assert.match(
    appJs,
    /will not switch to another hotel, another publication version, or live research automatically/,
  );
});

check("inventories_unchanged_pointers", () => {
  for (const id of [YOTEL, BETHESDA, RADISSON]) {
    const staged = path.join(
      ROOT,
      "lib/group-demand-intelligence/bundled-publication-packages",
      id,
      "STAGED.json",
    );
    assert.ok(fs.existsSync(staged), id);
  }
  assert.equal(
    fs.existsSync(
      path.join(
        ROOT,
        "lib/group-demand-intelligence/bundled-publication-packages",
        SAN_DIEGO,
        "STAGED.json",
      ),
    ),
    false,
  );
});

const failed = results.filter((r) => !r.ok);
console.log(
  JSON.stringify(
    {
      contract: "gdi-founder-preview-missing-staged-nav-v1",
      sanDiegoHotelId: SAN_DIEGO,
      sanDiegoHotelName: "Wyndham San Diego Bayside",
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
console.log("OK: founder preview missing-STAGED navigation recovery");
