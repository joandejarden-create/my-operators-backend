#!/usr/bin/env node
/**
 * GDI List optional columns — hydration + inventory-driven menu.
 *   npm run test:gdi-list-optional-columns-hydration-v1
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const CACHE = "gdi-competitive-filter-layout-v1";

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
      return { style: {}, setAttribute() {}, addEventListener() {}, querySelector() { return null; } };
    },
    body: { appendChild() {} },
    addEventListener() {},
  };
  vm.runInNewContext(read("public/js/group-demand-intelligence/dealality-gdi-ui.js"), sandbox);
  vm.runInNewContext(
    read("public/js/group-demand-intelligence/gdi-commercial-list-table-v1.js"),
    sandbox,
  );
  return sandbox;
}

const hotelOnly = [
  {
    id: "h1",
    title: "Hotel Opp",
    hotelMarketScopeEligible: true,
    competitiveMarketScopeEligible: false,
    contactActionabilityTier: "SALES_READY",
    contactActionabilityLabel: "Sales-ready contact",
    hotelSourcingStatusLabel: "Hotel Sourcing Unconfirmed",
  },
  {
    id: "h2",
    title: "Hotel Opp 2",
    hotelMarketScopeEligible: true,
    contactActionabilityTier: "REACHABLE",
  },
];

const mixedScope = [
  ...hotelOnly,
  {
    id: "c1",
    title: "Competitive Opp",
    hotelMarketScopeEligible: false,
    competitiveMarketScopeEligible: true,
    marketScope: "COMPETITIVE",
    inventoryPopulationPrimary: "COMPETITIVE_MARKET_INTELLIGENCE",
    contactActionabilityTier: "CONTACT_RESEARCH_REQUIRED",
    needDateIndicator: { displayLabel: "No Current Match" },
    hotelSourcingStatusLabel: "Hotel Sourcing Open",
  },
];

check("cache_and_no_sales_opportunity_type_column", () => {
  assert.match(
    read("public/group-demand-intelligence.html"),
    new RegExp(`gdi-commercial-list-table-v1\\.js\\?v=${CACHE}`),
  );
  const { DealalityGdiCommercialListTable: Table } = loadModules();
  assert.ok(!Table.OPTIONAL_COLUMNS.some((c) => c.id === "salesOpportunityType"));
  assert.ok(Table.OPTIONAL_COLUMNS.some((c) => c.id === "contactReadiness"));
  assert.ok(Table.OPTIONAL_COLUMNS.some((c) => c.id === "marketScope"));
});

check("market_scope_hydration_from_eligibility", () => {
  const { DealalityGdiCommercialListTable: Table, DealalityGdiUi: UI } =
    loadModules();
  assert.equal(Table.resolveMarketScopeDisplay(hotelOnly[0], UI), "Hotel");
  assert.equal(
    Table.resolveMarketScopeDisplay(mixedScope[2], UI),
    "Competitive",
  );
  assert.match(
    Table.optionalCellValue(mixedScope[2], "marketScope", UI).display,
    /Competitive/,
  );
});

check("need_period_and_opportunity_status_hydration", () => {
  const { DealalityGdiCommercialListTable: Table, DealalityGdiUi: UI } =
    loadModules();
  const need = Table.optionalCellValue(mixedScope[2], "needPeriodAlignment", UI);
  assert.equal(need.display, "No Current Match");
  assert.equal(need.populated, true);
  const status = Table.optionalCellValue(hotelOnly[0], "opportunityStatus", UI);
  assert.match(status.display, /Hotel Sourcing Unconfirmed/i);
  assert.equal(
    Table.optionalCellValue(hotelOnly[0], "estimatedRoomNights", UI).populated,
    false,
  );
});

check("menu_hides_empty_and_redundant_market_scope_when_uniform", () => {
  const { DealalityGdiCommercialListTable: Table, DealalityGdiUi: UI } =
    loadModules();
  const hotelAssess = Table.assessAllOptionalColumns(hotelOnly, UI);
  const byId = Object.fromEntries(hotelAssess.map((a) => [a.id, a]));
  assert.equal(byId.contactReadiness.offer, true);
  assert.equal(byId.opportunityStatus.offer, true);
  // Hotel-only inventory still offers Market Scope so column matches filter taxonomy.
  assert.equal(byId.marketScope.offer, true);
  assert.equal(byId.marketScope.decision, "RETAIN_HYDRATED_HOTEL_ONLY");
  assert.equal(byId.estimatedRoomNights.offer, false);
  assert.equal(byId.weeklyChange.offer, false);
  assert.equal(byId.lastVerified.offer, false);
  assert.equal(byId.needPeriodAlignment.offer, false);

  const mixedAssess = Table.assessAllOptionalColumns(mixedScope, UI);
  const m = Object.fromEntries(mixedAssess.map((a) => [a.id, a]));
  assert.equal(m.marketScope.offer, true);
  assert.equal(m.needPeriodAlignment.offer, true);
  const ids = Table.availableOptionalColumnIds(mixedScope, UI);
  assert.ok(ids.includes("marketScope"));
  assert.ok(ids.includes("needPeriodAlignment"));
  assert.ok(!ids.includes("salesOpportunityType"));
});

check("column_menu_html_data_driven_and_prefs_preserved", () => {
  const { DealalityGdiCommercialListTable: Table, DealalityGdiUi: UI } =
    loadModules();
  Table.saveOptionalColumns({
    contactReadiness: true,
    marketScope: true,
    estimatedRoomNights: true,
  });
  const htmlHotel = Table.columnSettingsHtml(Table.loadOptionalColumns(), {
    inventoryRows: hotelOnly,
    UI,
  });
  assert.match(htmlHotel, /Contact Readiness/);
  assert.doesNotMatch(htmlHotel, /Sales Opportunity Type/);
  assert.match(htmlHotel, /Market Scope/);
  assert.doesNotMatch(htmlHotel, /Estimated Room Nights/);
  assert.match(htmlHotel, /checked/);

  const htmlMixed = Table.columnSettingsHtml(Table.loadOptionalColumns(), {
    inventoryRows: mixedScope,
    UI,
  });
  assert.match(htmlMixed, /Market Scope/);
  assert.match(htmlMixed, /Need Period Alignment/);
});

check("table_uses_inventory_not_filtered_rows_for_availability", () => {
  const { DealalityGdiCommercialListTable: Table, DealalityGdiUi: UI } =
    loadModules();
  Table.saveOptionalColumns({ marketScope: true, contactReadiness: true });
  const filtered = [mixedScope[0]]; // hotel-only slice
  const html = Table.tableHtml(filtered, {
    UI,
    optionalColumns: Table.loadOptionalColumns(),
    inventoryRows: mixedScope,
  });
  assert.match(html, /Market Scope/);
  assert.match(html, /Contact Readiness/);
  assert.match(html, />Hotel</);
});

check("controller_passes_inventory_rows", () => {
  const ctrl = read(
    "public/js/group-demand-intelligence/gdi-universal-presentation-controller.js",
  );
  assert.match(ctrl, /inventoryRows:\s*state\.opportunities/);
  assert.match(
    read("public/js/group-demand-intelligence/app.js"),
    /inventoryRows:\s*state\.opportunities/,
  );
});

if (failed) {
  console.error(`\n${failed} failure(s)`);
  process.exit(1);
}
console.log("\nOK test:gdi-list-optional-columns-hydration-v1");
