#!/usr/bin/env node
/**
 * Hotel GDI toolbar: Reset View only — no Run Research / Publish buttons.
 * Active-filter badge on Reset View matches Radar / Brand Explorer pattern.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import vm from "node:vm";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const appJs = fs.readFileSync(
  path.join(ROOT, "public/js/group-demand-intelligence/app.js"),
  "utf8"
);
const shareJs = fs.readFileSync(
  path.join(ROOT, "public/js/group-demand-intelligence/share-app.js"),
  "utf8"
);
const uiSrc = fs.readFileSync(
  path.join(ROOT, "public/js/group-demand-intelligence/dealality-gdi-ui.js"),
  "utf8"
);
const css = fs.readFileSync(
  path.join(ROOT, "public/css/group-demand-intelligence.css"),
  "utf8"
);

assert.ok(appJs.includes("gdiResetViewBtn"), "auth Reset View id present");
assert.ok(shareJs.includes("gdiResetViewBtn"), "share Reset View id present");
assert.ok(
  appJs.includes("resetViewButtonHtml") || appJs.includes("gdiResetViewBtn"),
  "auth wires Reset View"
);
assert.ok(
  shareJs.includes("resetViewButtonHtml") || shareJs.includes("gdiResetViewBtn"),
  "share wires Reset View"
);
assert.equal(appJs.includes('id="gdiRunBtn"'), false);
assert.equal(appJs.includes('id="gdiPublishBtn"'), false);
assert.equal(/Run Research/i.test(shareJs), false);
assert.equal(shareJs.includes('id="gdiPublishBtn"'), false);

const fnMatch = appJs.match(
  /function propertyBarActionsHtml\(\)\s*\{[\s\S]*?\n  \}/
);
assert.ok(fnMatch, "propertyBarActionsHtml present");
assert.ok(fnMatch[0].includes("gdiResetViewBtn") || fnMatch[0].includes("resetViewButtonHtml"));
assert.equal(/gdiRunBtn|gdiPublishBtn|Run Research/.test(fnMatch[0]), false);

assert.match(uiSrc, /function countActiveBrowseFilters/);
assert.match(uiSrc, /function resetViewButtonHtml/);
assert.match(css, /\.filter-count-badge/);
assert.match(css, /accent--primary-1/);

const sandbox = { window: {}, console };
vm.runInNewContext(uiSrc, sandbox);
const UI = sandbox.window.DealalityGdiUi;
assert.ok(UI, "DealalityGdiUi loads");

assert.equal(UI.countActiveBrowseFilters(UI.resetBrowseFilters()), 0);
assert.equal(UI.countActiveBrowseFilters({}), 0);
assert.equal(
  UI.countActiveBrowseFilters({ weekly: "NEW" }),
  1,
  "one active weekly filter"
);
assert.equal(
  UI.countActiveBrowseFilters({
    weekly: "NEW",
    opportunityType: "association_events",
  }),
  2,
  "two active filters"
);
assert.equal(
  UI.countActiveBrowseFilters({
    weekly: "UPDATED",
    opportunityType: "scaling_companies",
    marketScope: "HOTEL",
    priority: "HIGH_PRIORITY",
  }),
  4,
  "multiple active filters"
);
assert.equal(
  UI.countActiveBrowseFilters({
    marketScope: "ALL",
    competitiveStatus: "ALL_COMPETITIVE",
    sourcingSubgroup: "BOTH",
    weekly: "",
  }),
  0,
  "All defaults do not count"
);

const emptyBtn = UI.resetViewButtonHtml({ filters: UI.resetBrowseFilters() });
assert.match(emptyBtn, /id="gdiResetViewBtn"/);
assert.match(emptyBtn, /Reset View/);
assert.match(emptyBtn, /gdiFilterCountBadge/);
assert.match(emptyBtn, /display:\s*none/);

const activeBtn = UI.resetViewButtonHtml({
  filters: { weekly: "NEW", marketScope: "COMPETITIVE" },
});
assert.match(activeBtn, />2</);
assert.doesNotMatch(activeBtn, /display:\s*none/);

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
      activeFilterBadge: true,
      runResearchButton: false,
      publishButton: false,
    },
    null,
    2
  )
);
console.log("PASS gdi hotel toolbar reset-view-only v1");
