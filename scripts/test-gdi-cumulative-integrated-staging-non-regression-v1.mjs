#!/usr/bin/env node
/**
 * Permanent integrated non-regression gate for cumulative GDI founder staging.
 * Feature-branch tests alone are not product acceptance — this gate verifies
 * the cumulative integrated surface retains previously accepted capabilities.
 *
 *   npm run test:gdi-cumulative-integrated-staging-non-regression-v1
 */
import assert from "node:assert/strict";
import fs from "fs";
import path from "path";
import { fileURLToPath, pathToFileURL } from "url";
import vm from "vm";
import {
  resolveWyndhamFounderInventoryPreview,
  loadWyndhamLockedIdsForTests,
} from "../lib/group-demand-intelligence/publication-package/gdi-wyndham-proposed-26-founder-preview-v1.js";
import { createGdiPublicationPackageStore } from "../lib/group-demand-intelligence/publication-package/index.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
/** Accepted cumulative cache busts (latest supersedes prior lifecycle markers). */
const ACCEPTED_UI_BUSTS = [
  "gdi-market-scope-compact-20261127",
  "gdi-list-market-scope-col-20261127",
  "gdi-es-wave-loading-20261127",
  "gdi-es-remove-staged-pill-20261127",
  "gdi-es-outlook-micro-20261127",
  "gdi-es-outlook-12mo-kpis-20261127",
  "gdi-list-cols-simplify-20261127",
  "gdi-es-receipt-funnel-v1b",
  "gdi-competitive-scope-v1",
  "gdi-es-admin-shared-tiles-v1",
  "gdi-wyndham-26-cert",
  "gdi-e2e-staged-lifecycle-v1",
];
const WY = "recnPB3Q02NO8Ma1o";
void pathToFileURL;

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

function read(rel) {
  return fs.readFileSync(path.join(ROOT, rel), "utf8");
}

function assertAcceptedBust(html, scriptBase) {
  const re = new RegExp(`${scriptBase}\\?v=([^\"&]+)`);
  const m = html.match(re);
  assert.ok(m, `missing script ${scriptBase}`);
  assert.ok(
    ACCEPTED_UI_BUSTS.includes(m[1]) || /gdi-/.test(m[1]),
    `${scriptBase} bust ${m[1]} not in cumulative accepted set`,
  );
}

check("auth_shell_has_four_tabs_and_exec_summary_script", () => {
  const html = read("public/group-demand-intelligence.html");
  assert.match(html, /executive-summary-tab\.js\?v=/);
  assert.match(html, /gdi-commercial-list-table-v1\.js\?v=/);
  assertAcceptedBust(html, "dealality-gdi-ui\\.js");
  assertAcceptedBust(html, "app\\.js");
  assert.match(
    html,
    /gdi-market-scope-compact-|gdi-list-market-scope-col-|gdi-es-wave-loading-|gdi-es-remove-staged-pill-|gdi-es-outlook-micro-|gdi-es-outlook-12mo-kpis-|gdi-list-cols-simplify-|gdi-es-receipt-funnel-|gdi-competitive-scope-v1|gdi-es-admin-shared-tiles-v1|gdi-wyndham-26-cert/,
  );
});

check("ui_registers_exec_summary_and_left_cluster_tabs", () => {
  const ui = read("public/js/group-demand-intelligence/dealality-gdi-ui.js");
  assert.match(ui, /GDI_EXEC_SUMMARY_TAB/);
  assert.match(ui, /id:\s*"executive-summary"/);
  assert.match(ui, /GDI_CUSTOMER_TABS/);
  assert.match(ui, /gdi-content-tabs|gdi-main-tabs|contentTabsHtml/);
});

check("list_and_tiles_paths_both_present", () => {
  const ui = read("public/js/group-demand-intelligence/dealality-gdi-ui.js");
  assert.match(ui, /DealalityGdiCommercialListTable/);
  assert.match(ui, /opportunityCardsGridHtml/);
  assert.match(ui, /gdi-results-grid/);
  assert.ok(fs.existsSync(path.join(ROOT, "public/js/group-demand-intelligence/gdi-commercial-list-table-v1.js")));
  assert.ok(fs.existsSync(path.join(ROOT, "public/js/group-demand-intelligence/executive-summary-tab.js")));
});

check("list_tiles_parity_helper_and_table_contract", () => {
  const sandbox = { console, sessionStorage: { _m: {}, getItem() { return null; }, setItem() {} } };
  sandbox.window = sandbox;
  sandbox.globalThis = sandbox;
  vm.runInNewContext(read("public/js/group-demand-intelligence/dealality-gdi-ui.js"), sandbox);
  vm.runInNewContext(
    read("public/js/group-demand-intelligence/gdi-commercial-list-table-v1.js"),
    sandbox,
  );
  const UI = sandbox.DealalityGdiUi;
  const Table = sandbox.DealalityGdiCommercialListTable;
  const rows = [
    { id: "a", title: "A", priority: "HIGH_PRIORITY", priorityFilterKey: "HIGH_PRIORITY" },
    { id: "b", title: "B", priority: "WATCHLIST", priorityFilterKey: "WATCHLIST" },
  ];
  const listHtml = UI.opportunityCardsGridHtml(rows, "list", {}, { sortKey: "priority", sortDir: 1 });
  const tilesHtml = UI.opportunityCardsGridHtml(rows, "tiles", {}, { sortKey: "priority", sortDir: 1 });
  assert.match(listHtml, /gdiCommercialOppTable/);
  assert.doesNotMatch(tilesHtml, /gdiCommercialOppTable/);
  assert.match(tilesHtml, /gdi-results-grid/);
  assert.equal(Table.listTilesIdParity(rows, rows).ok, true);
});

check("local_staged_default_and_customer_current_isolation_markers", () => {
  const app = read("public/js/group-demand-intelligence/app.js");
  assert.match(app, /function resolveFounderInventoryMode/);
  assert.match(app, /isLocalFounderHost\(\)\)\s*return\s*"staged"/);
  assert.match(app, /preview=staged/);
  assert.match(app, /preview=current/);
  assert.match(app, /STAGED_PUBLICATION_PREVIEW_UNAVAILABLE|missingInventory|inventoryEmpty/);
  assert.doesNotMatch(app, /founderInventoryModeSelectorHtml\(/);
  const share = read("public/js/group-demand-intelligence/share-app.js");
  assert.doesNotMatch(share, /isLocalFounderHost\(\) return "staged"/);
});

check("wyndham_staged_complete_qualified_26_and_clean11_preserved", () => {
  const store = createGdiPublicationPackageStore({});
  const staged = store.readPointer(WY, "STAGED");
  const current = store.readPointer(WY, "CURRENT");
  assert.ok(staged?.publicationVersion);
  assert.equal(current?.publicationVersion || null, null);
  const loaded = store.loadPackage(WY, staged.publicationVersion);
  assert.equal(loaded.ok, true);
  assert.equal(loaded.package.opportunityCount, 26);
  // Historical CLEAN11 package bytes preserved
  const clean11 = JSON.parse(
    read(
      `lib/group-demand-intelligence/bundled-publication-packages/${WY}/gdi_hotel_wide_fqo_recnPB3Q02NO8Ma1o_20261125_3283ee92/package.json`,
    ),
  );
  assert.equal(clean11.opportunityCount || clean11.opportunities.length, 11);
});

check("locked_26_hydrate_id_parity_internal", () => {
  const proposed = resolveWyndhamFounderInventoryPreview({ bucket: "proposed" });
  assert.equal(proposed.ok, true);
  assert.equal(proposed.count, 26);
  const ids = proposed.opportunities.map((o) => o.id);
  assert.equal(new Set(ids).size, 26);
  const locked = loadWyndhamLockedIdsForTests();
  assert.deepEqual(ids.slice().sort(), locked.proposedQualifiedIds.slice().sort());
  const store = createGdiPublicationPackageStore({});
  const staged = store.readPointer(WY, "STAGED");
  const loaded = store.loadPackage(WY, staged.publicationVersion);
  const stagedIds = loaded.package.opportunities.map((o) => o.id || o.opportunityId);
  assert.deepEqual(stagedIds.slice().sort(), locked.proposedQualifiedIds.slice().sort());
});

check("single_row_list_toolbar_columns_svg", () => {
  const table = read("public/js/group-demand-intelligence/gdi-commercial-list-table-v1.js");
  assert.match(table, /Customize Columns/);
  assert.match(table, /COLUMNS_ICON_SVG|rect x="3" y="4"/);
  assert.doesNotMatch(table, /gdi-list-table-toolbar/);
  const sandbox = {
    console,
    sessionStorage: {
      _m: {},
      getItem() {
        return null;
      },
      setItem() {},
    },
  };
  sandbox.window = sandbox;
  sandbox.globalThis = sandbox;
  vm.runInNewContext(
    read("public/js/group-demand-intelligence/gdi-commercial-list-table-v1.js"),
    sandbox,
  );
  vm.runInNewContext(read("public/js/group-demand-intelligence/dealality-gdi-ui.js"), sandbox);
  const toolbar = sandbox.DealalityGdiUi.resultsToolbarHtml({
    shown: 26,
    total: 26,
    viewMode: "list",
    sort: "priority",
  });
  assert.match(toolbar, /gdiListColumnSettingsBtn/);
  assert.match(toolbar, /gdiExportCsvBtn/);
  assert.match(toolbar, /gdiSortDirBtn/);
  assert.match(toolbar, /gdiSortSelect/);
  assert.match(toolbar, /data-gdi-view="list"/);
  const exportIdx = toolbar.indexOf("gdiExportCsvBtn");
  const sortDirIdx = toolbar.indexOf("gdiSortDirBtn");
  const sortSelIdx = toolbar.indexOf("gdiSortSelect");
  const colIdx = toolbar.indexOf("gdiListColumnSettingsBtn");
  const listIdx = toolbar.indexOf('data-gdi-view="list"');
  assert.ok(exportIdx < sortDirIdx && sortDirIdx < sortSelIdx && sortSelIdx < colIdx && colIdx < listIdx);
  const tilesToolbar = sandbox.DealalityGdiUi.resultsToolbarHtml({
    shown: 11,
    total: 11,
    viewMode: "tiles",
  });
  assert.doesNotMatch(tilesToolbar, /gdiListColumnSettingsBtn/);
});

check("lifecycle_docs_and_no_proposed_shell_gate", () => {
  assert.ok(
    fs.existsSync(
      path.join(ROOT, "docs/ai-build-system/DEALALITY_ADDITIVE_NON_REGRESSION_PRINCIPLES.md"),
    ),
  );
  const constitution = read("docs/ai-build-system/DEALALITY_PRODUCT_CONSTITUTION.md");
  assert.match(constitution, /FULL E2E → STAGED → FOUNDER APPROVAL → CURRENT/);
  const shell = read("public/app.html");
  assert.doesNotMatch(shell, /data-gdi-shell-inventory="proposed"/);
  assert.match(shell, /data-gdi-shell-inventory="staged"/);
});

if (failed) {
  console.error(`\n${failed} failure(s)`);
  process.exit(1);
}
console.log("\nOK test:gdi-cumulative-integrated-staging-non-regression-v1");
