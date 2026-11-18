#!/usr/bin/env node
/**
 * Restore founder local Research Preview — deterministic.
 * No paid research / Airtable / publication mutation.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { inspectFounderPreviewPublicationAvailability } from "../lib/group-demand-intelligence/publication-package/gdi-founder-preview-availability-v1.js";
import { resolvePreviewOpportunitiesFromPublicationPackage } from "../lib/group-demand-intelligence/publication-package/gdi-share-approved-package-resolver-v1.js";
import { resolveShareOpportunitiesFromApprovedPackage } from "../lib/group-demand-intelligence/publication-package/gdi-share-approved-package-resolver-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

const HOTELS = Object.freeze({
  castillo: "rec82D9zpB8fede1I",
  sanDiego: "recnPB3Q02NO8Ma1o",
  bethesda: "recLuxvwwxID7U2B8",
  yotel: "recrPQcZg7SFARRb2",
  radisson: "recH9h3McgMNFQEla",
});

const results = [];
function check(name, fn) {
  try {
    fn();
    results.push({ name, ok: true });
  } catch (e) {
    results.push({ name, ok: false, error: String(e.message || e) });
  }
}

function researchOppCount(hotelId) {
  const p = path.join(
    ROOT,
    "data/group-demand-intelligence/hotels",
    hotelId,
    "opportunities.json",
  );
  if (!fs.existsSync(p)) return 0;
  const doc = JSON.parse(fs.readFileSync(p, "utf8"));
  return Array.isArray(doc.opportunities) ? doc.opportunities.length : 0;
}

const appJs = fs.readFileSync(
  path.join(ROOT, "public/js/group-demand-intelligence/app.js"),
  "utf8",
);
const shareJs = fs.readFileSync(
  path.join(ROOT, "public/js/group-demand-intelligence/share-app.js"),
  "utf8",
);
const html = fs.readFileSync(
  path.join(ROOT, "public/group-demand-intelligence.html"),
  "utf8",
);

check("localhost_default_is_research_not_staged", () => {
  assert.match(appJs, /resolveFounderInventoryMode/);
  assert.match(appJs, /if \(isLocalFounderHost\(\)\) return "research"/);
  assert.doesNotMatch(
    appJs,
    /Local founder review: when STAGED ≠ CURRENT, localhost must load/,
  );
  // Auto-staged localhost default removed.
  assert.doesNotMatch(
    appJs,
    /if \(isLocalFounderHost\(\)\) return true;\s*return false;/,
  );
});

check("explicit_staged_still_supported", () => {
  assert.match(appJs, /if \(preview === "staged"\) return "staged"/);
  assert.match(appJs, /preview=staged/);
  assert.match(appJs, /STAGED_PUBLICATION_PREVIEW_UNAVAILABLE/);
});

check("inventory_mode_controls_present", () => {
  assert.match(appJs, /data-gdi-inventory-mode="research"/);
  assert.match(appJs, /data-gdi-inventory-mode="staged"/);
  assert.match(appJs, /data-gdi-inventory-mode="published"/);
  assert.match(appJs, /Open Research Preview/);
  assert.match(appJs, /applyFounderInventoryMode/);
});

check("castillo_and_san_diego_have_research_without_staged", () => {
  for (const [label, id] of [
    ["castillo", HOTELS.castillo],
    ["sanDiego", HOTELS.sanDiego],
  ]) {
    const avail = inspectFounderPreviewPublicationAvailability({ hotelId: id });
    assert.equal(avail.hasStagedPublication, false, label);
    assert.equal(avail.hasCurrentPublication, false, label);
    assert.ok(researchOppCount(id) > 0, `${label} research missing`);
    const staged = resolvePreviewOpportunitiesFromPublicationPackage({
      hotelId: id,
      preferStaged: true,
    });
    assert.equal(staged.ok, false, label);
  }
});

check("published_hotels_still_resolve_current", () => {
  for (const id of [HOTELS.bethesda, HOTELS.yotel, HOTELS.radisson]) {
    const avail = inspectFounderPreviewPublicationAvailability({ hotelId: id });
    assert.equal(avail.hasCurrentPublication, true, id);
    const cur = resolvePreviewOpportunitiesFromPublicationPackage({ hotelId: id });
    assert.equal(cur.ok, true, id);
    assert.equal(cur.mode, "PACKAGE", id);
  }
});

check("share_path_cannot_use_live_research_default", () => {
  assert.doesNotMatch(shareJs, /resolveFounderInventoryMode/);
  assert.doesNotMatch(shareJs, /isLocalFounderHost/);
  // Share keeps package / token gates — no localhost research bypass.
  const shareCastillo = resolveShareOpportunitiesFromApprovedPackage({
    hotelId: HOTELS.castillo,
  });
  assert.equal(shareCastillo.ok === true && shareCastillo.mode === "PACKAGE", false);
});

check("hotel_switch_clears_preview_params", () => {
  assert.match(appJs, /Switching properties clears incompatible/);
  assert.match(appJs, /url\.searchParams\.delete\("preview"\)/);
});

check("cache_bust", () => {
  assert.match(html, /gdi-founder-local-research-preview-v1/);
});

check("no_hardcoded_castillo_or_sandiego_exception", () => {
  assert.doesNotMatch(appJs, /rec82D9zpB8fede1I/);
  assert.doesNotMatch(appJs, /recnPB3Q02NO8Ma1o/);
});

const failed = results.filter((r) => !r.ok);
console.log(
  JSON.stringify(
    {
      contract: "gdi-founder-local-research-preview-v1",
      hotels: {
        castillo: {
          id: HOTELS.castillo,
          researchOpp: researchOppCount(HOTELS.castillo),
          staged: false,
        },
        sanDiego: {
          id: HOTELS.sanDiego,
          researchOpp: researchOppCount(HOTELS.sanDiego),
          staged: false,
        },
      },
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
console.log("OK: founder local research preview restored");
