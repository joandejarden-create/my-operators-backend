#!/usr/bin/env node
/**
 * Hotel GDI toolbar: Reset View only — no Run Research / Publish buttons.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const appJs = fs.readFileSync(
  path.join(ROOT, "public/js/group-demand-intelligence/app.js"),
  "utf8"
);
const shareJs = fs.readFileSync(
  path.join(ROOT, "public/js/group-demand-intelligence/share-app.js"),
  "utf8"
);

const resetMarkup =
  '<button type="button" class="btn-clear" id="gdiResetViewBtn">Reset View</button>';

assert.ok(appJs.includes(resetMarkup), "auth Reset View markup unchanged");
assert.ok(shareJs.includes(resetMarkup), "share Reset View markup unchanged");
assert.equal(appJs.includes('id="gdiRunBtn"'), false);
assert.equal(appJs.includes('id="gdiPublishBtn"'), false);
assert.equal(/Run Research/i.test(shareJs), false);
assert.equal(shareJs.includes('id="gdiPublishBtn"'), false);

// propertyBarActionsHtml returns Reset View only
const fnMatch = appJs.match(
  /function propertyBarActionsHtml\(\)\s*\{[\s\S]*?\n  \}/
);
assert.ok(fnMatch, "propertyBarActionsHtml present");
assert.ok(fnMatch[0].includes(resetMarkup));
assert.equal(/gdiRunBtn|gdiPublishBtn|Run Research/.test(fnMatch[0]), false);

// Backend capabilities preserved
assert.ok(appJs.includes("function runResearch("));
assert.ok(appJs.includes("function publishHotelInventory("));
assert.ok(appJs.includes("/research/run"));
assert.ok(appJs.includes("/publish"));
assert.ok(
  fs.existsSync(
    path.join(
      ROOT,
      "lib/group-demand-intelligence/durable-publication/gdi-one-click-hotel-publish-v1.js"
    )
  )
);

console.log(
  JSON.stringify(
    {
      ok: true,
      authResetView: true,
      shareResetView: true,
      runResearchButton: false,
      publishButton: false,
      researchHelperPreserved: true,
      publishHelperPreserved: true,
      oneClickPublishModule: true,
    },
    null,
    2
  )
);
console.log("PASS gdi hotel toolbar reset-view-only v1");
