#!/usr/bin/env node
/**
 * Offline + API tests for authenticated STAGED founder preview (no publish).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { resolvePreviewOpportunitiesFromPublicationPackage } from "../lib/group-demand-intelligence/publication-package/gdi-share-approved-package-resolver-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL = "recCAGqkERSXgiku4";
const FP =
  "60f2d7ef732ac9578e33e159988092b90f49ec8372b8b174c7630296240e6211";

const appJs = fs.readFileSync(
  path.join(ROOT, "public/js/group-demand-intelligence/app.js"),
  "utf8",
);
assert.match(
  appJs,
  /HOTEL_NOT_IN_SELECTABLE_CATALOG/,
  "UI must refuse silent hotel remapping when hotelId is explicit",
);
assert.match(appJs, /preview=staged/, "UI must pass preview=staged to API");
assert.doesNotMatch(
  appJs,
  /if \(ids\.length && ids\.indexOf\(state\.hotelId\) === -1\) \{\s*state\.hotelId = ids\[0\];/,
  "Must not unconditionally remap unknown hotelId to catalog[0]",
);

const apiJs = fs.readFileSync(
  path.join(ROOT, "api/group-demand-intelligence.js"),
  "utf8",
);
assert.match(apiJs, /STAGED_PUBLICATION_PREVIEW_UNAVAILABLE/);
assert.match(apiJs, /requireStagedPreview/);

const resolved = resolvePreviewOpportunitiesFromPublicationPackage({
  hotelId: HOTEL,
});
assert.equal(resolved.ok, true);
assert.equal(resolved.mode, "PACKAGE");
assert.equal(resolved.meta?.previewSource, "STAGED");
assert.equal(resolved.meta?.contentFingerprintSha256, FP);
assert.equal(resolved.opportunities?.length, 62);

// Bethesda CURRENT must remain loadable and distinct.
const bethesda = resolvePreviewOpportunitiesFromPublicationPackage({
  hotelId: "recLuxvwwxID7U2B8",
});
assert.equal(bethesda.ok, true);
assert.equal(bethesda.mode, "PACKAGE");
assert.notEqual(bethesda.meta?.contentFingerprintSha256, FP);

console.log(
  JSON.stringify(
    {
      ok: true,
      sheratonPreviewSource: resolved.meta.previewSource,
      sheratonCount: resolved.opportunities.length,
      bethesdaPreviewSource: bethesda.meta?.previewSource || null,
      bethesdaCount: bethesda.opportunities?.length || 0,
    },
    null,
    2,
  ),
);
