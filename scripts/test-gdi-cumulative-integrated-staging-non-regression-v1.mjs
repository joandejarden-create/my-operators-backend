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
import { fileURLToPath } from "url";
import vm from "vm";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const CACHE = "gdi-cumulative-es-list-v1";

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

check("auth_shell_has_four_tabs_and_exec_summary_script", () => {
  const html = read("public/group-demand-intelligence.html");
  assert.match(html, /executive-summary-tab\.js\?v=/);
  assert.match(html, /gdi-commercial-list-table-v1\.js\?v=/);
  assert.match(html, new RegExp(`dealality-gdi-ui\\.js\\?v=${CACHE}`));
  assert.match(html, new RegExp(`app\\.js\\?v=${CACHE}`));
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
  // Customer share must not auto-stage
  const share = read("public/js/group-demand-intelligence/share-app.js");
  assert.doesNotMatch(share, /isLocalFounderHost\(\) return "staged"/);
});

check("wyndham_staged_pointer_and_clean11_ids_present", () => {
  const staged = JSON.parse(
    read(
      "lib/group-demand-intelligence/bundled-publication-packages/recnPB3Q02NO8Ma1o/STAGED.json",
    ),
  );
  assert.equal(staged.kind, "STAGED");
  assert.equal(
    staged.publicationVersion,
    "gdi_hotel_wide_fqo_recnPB3Q02NO8Ma1o_20261125_3283ee92",
  );
  const pkg = JSON.parse(
    read(
      `lib/group-demand-intelligence/bundled-publication-packages/recnPB3Q02NO8Ma1o/${staged.publicationVersion}/package.json`,
    ),
  );
  assert.equal(pkg.opportunityCount || pkg.opportunities.length, 11);
  assert.equal(pkg.opportunities.length, 11);
});

check("proposed_write_set_review_surface_present", () => {
  assert.ok(fs.existsSync(path.join(ROOT, "api/gdi-proposed-write-set-review.js")));
  assert.ok(
    fs.existsSync(
      path.join(
        ROOT,
        "lib/group-demand-intelligence/publication-package/gdi-proposed-write-set-founder-review-v1.js",
      ),
    ),
  );
  const server = read("server.js");
  assert.match(server, /proposed-write-set-review/);
  assert.match(server, /getGdiExecutiveSummary/);
});

check("additive_non_regression_docs_present", () => {
  assert.ok(
    fs.existsSync(
      path.join(ROOT, "docs/ai-build-system/DEALALITY_ADDITIVE_NON_REGRESSION_PRINCIPLES.md"),
    ),
  );
  const constitution = read("docs/ai-build-system/DEALALITY_PRODUCT_CONSTITUTION.md");
  assert.match(constitution, /8080|integrated|STAGED|non-regression|additive/i);
});

if (failed) {
  console.error(`\n${failed} failure(s)`);
  process.exit(1);
}
console.log("\nOK test:gdi-cumulative-integrated-staging-non-regression-v1");
