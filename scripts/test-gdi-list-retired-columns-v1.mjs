#!/usr/bin/env node
/**
 * Contact Readiness + Opportunity Status permanently removed from List View.
 *   npm run test:gdi-list-retired-columns-v1
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
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

function loadModules() {
  const sandbox = {
    console,
    sessionStorage: {
      _m: Object.create(null),
      getItem(k) {
        return Object.prototype.hasOwnProperty.call(this._m, k) ? this._m[k] : null;
      },
      setItem(k, v) {
        this._m[k] = String(v);
      },
      removeItem(k) {
        delete this._m[k];
      },
    },
  };
  sandbox.window = sandbox;
  sandbox.globalThis = sandbox;
  sandbox.document = {
    createElement() {
      return {
        style: {},
        setAttribute() {},
        addEventListener() {},
        querySelector() {
          return null;
        },
      };
    },
    body: { appendChild() {} },
    addEventListener() {},
  };
  vm.runInNewContext(read("public/js/group-demand-intelligence/dealality-gdi-ui.js"), sandbox);
  vm.runInNewContext(
    read("public/js/group-demand-intelligence/gdi-commercial-list-table-v1.js"),
    sandbox,
  );
  return {
    Table: sandbox.DealalityGdiCommercialListTable,
    UI: sandbox.DealalityGdiUi,
  };
}

check("optional_and_sort_omit_retired_columns", () => {
  const { Table, UI } = loadModules();
  assert.ok(!Table.OPTIONAL_COLUMNS.some((c) => c.id === "contactReadiness"));
  assert.ok(!Table.OPTIONAL_COLUMNS.some((c) => c.id === "opportunityStatus"));
  assert.ok(Table.RETIRED_OPTIONAL_COLUMN_IDS.includes("contactReadiness"));
  assert.ok(Table.RETIRED_OPTIONAL_COLUMN_IDS.includes("opportunityStatus"));
  assert.ok(!UI.SORT_OPTIONS.some((o) => o.value === "contactReadiness"));
  assert.ok(!UI.SORT_OPTIONS.some((o) => o.value === "opportunityStatus"));
  assert.equal(UI.sanitizeListSortKey("contactReadiness"), "priority");
  assert.equal(UI.sanitizeListSortKey("opportunityStatus"), "priority");
  assert.equal(UI.sanitizeListSortKey("title"), "title");
});

check("prefs_and_menu_auto_drop_retired", () => {
  const { Table, UI } = loadModules();
  Table.saveOptionalColumns({
    contactReadiness: true,
    opportunityStatus: true,
    marketScope: true,
  });
  const prefs = Table.loadOptionalColumns();
  assert.equal(prefs.marketScope, true);
  assert.equal(prefs.contactReadiness, undefined);
  assert.equal(prefs.opportunityStatus, undefined);
  assert.equal(Object.keys(prefs).join(","), "marketScope");
  const menu = Table.columnSettingsHtml(prefs, {
    inventoryRows: [
      {
        id: "1",
        title: "A",
        hotelMarketScopeEligible: true,
        hotelSourcingStatusLabel: "Hotel Sourcing Unconfirmed",
        contactActionabilityTier: "SALES_READY",
      },
    ],
    UI,
  });
  assert.doesNotMatch(menu, /Contact Readiness/);
  assert.doesNotMatch(menu, /Opportunity Status/);
});

check("default_table_omits_retired_headers", () => {
  const { Table } = loadModules();
  const html = Table.tableHtml(
    [{ id: "1", title: "Alpha", priority: "MEDIUM_PRIORITY" }],
    { sortKey: "priority", sortDir: 1 },
  );
  assert.doesNotMatch(html, />Contact Readiness</);
  assert.doesNotMatch(html, />Opportunity Status</);
  assert.match(html, /data-col="opportunity"/);
  assert.match(html, /data-col="whoToContact"/);
  assert.match(html, /data-col="contactDetails"/);
  assert.match(html, /action-icon/);
});

check("css_keeps_unboxed_actions", () => {
  const css = read("public/css/group-demand-intelligence.css");
  const m = css.match(/\.gdi-page \.gdi-list-actions \.action-icon \{([^}]+)\}/);
  assert.ok(m);
  assert.match(m[1], /border:\s*none/);
});

if (failed) {
  console.error(`\n${failed} failure(s)`);
  process.exit(1);
}
console.log("\nOK test:gdi-list-retired-columns-v1");
