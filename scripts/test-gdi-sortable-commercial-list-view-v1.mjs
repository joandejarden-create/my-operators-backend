#!/usr/bin/env node
/**
 * Permanent regression: GDI sortable commercial List View (Deal Opportunities parity).
 *
 * Guarantees:
 * - List View renders structured sortable table (not tile-card list layout)
 * - Tiles View HTML path remains opportunityTile / results-grid cards
 * - List/Tiles opportunity-ID parity under identical filtered inventory
 * - Column sort + optional columns + no fabricated revenue
 * - Auth + share shells wire the table module
 *
 *   npm run test:gdi-sortable-commercial-list-view-v1
 *   node scripts/test-gdi-sortable-commercial-list-view-v1.mjs
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const CACHE = "gdi-sortable-list-v1";

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
        return Object.prototype.hasOwnProperty.call(this._m, k)
          ? this._m[k]
          : null;
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
  vm.runInNewContext(
    read("public/js/group-demand-intelligence/dealality-gdi-ui.js"),
    sandbox,
  );
  vm.runInNewContext(
    read("public/js/group-demand-intelligence/gdi-commercial-list-table-v1.js"),
    sandbox,
  );
  assert.ok(sandbox.DealalityGdiUi, "DealalityGdiUi missing");
  assert.ok(
    sandbox.DealalityGdiCommercialListTable,
    "DealalityGdiCommercialListTable missing",
  );
  return sandbox;
}

const sampleRows = [
  {
    id: "opp-a",
    title: "Alpha Conference",
    displayTitle: "Alpha Conference",
    organizationName: "Alpha Org",
    priority: "HIGH_PRIORITY",
    priorityFilterKey: "HIGH_PRIORITY",
    segment: "associations_government",
    segmentLabel: "Association / Conference",
    eventStartDate: "2026-09-10",
    eventEndDate: "2026-09-12",
    salesActionPillLabel: "Pursue Now",
    salesActionKey: "PURSUE_NOW",
    primaryContactName: "Jordan Lee",
    primaryContactRole: "Director of Meetings",
    contactActionabilityTier: "SALES_READY",
    contactActionabilityLabel: "Sales-ready",
    estimatedRoomNights: 420,
    weeklyDeltaLabel: "New",
    marketScope: "LOCAL",
  },
  {
    id: "opp-b",
    title: "Beta Scaling",
    displayTitle: "Beta Scaling",
    organizationName: "Beta Co",
    priority: "WATCHLIST",
    priorityFilterKey: "WATCHLIST",
    segment: "scaling_companies",
    segmentLabel: "Scaling Company",
    eventTiming: "Ongoing",
    salesActionPillLabel: "Build Account",
    salesActionKey: "BUILD_ACCOUNT",
    contactActionabilityTier: "CONTACT_IDENTIFICATION_REQUIRED",
    contactActionabilityLabel: "Not identified",
    estimatedRoomNights: null,
    weeklyDeltaLabel: "Unchanged",
    marketScope: "COMPETITIVE",
  },
  {
    id: "opp-c",
    title: "Charlie Event",
    displayTitle: "Charlie Event",
    organizationName: "Charlie Assoc",
    priority: "MEDIUM_PRIORITY",
    priorityFilterKey: "MEDIUM_PRIORITY",
    segment: "local_corporate",
    segmentLabel: "Local Corporate",
    eventStartDate: "2026-03-01",
    salesActionPillLabel: "Monitor",
    salesActionKey: "MONITOR",
    primaryContact: {
      name: "Sam Rivera",
      role: "Procurement Lead",
      buyerVsEntry: "RELEVANT_COMMERCIAL_ENTRY_POINT",
    },
    contactActionabilityTier: "REACHABLE",
    contactActionabilityLabel: "Reachable",
    estimatedRoomNights: 90,
    weeklyDeltaLabel: "Updated",
    marketScope: "LOCAL",
  },
];

check("shells_wire_table_module_and_cache_bust", () => {
  const auth = read("public/group-demand-intelligence.html");
  const share = read("public/group-demand-intelligence-share.html");
  assert.match(
    auth,
    new RegExp(`gdi-commercial-list-table-v1\\.js\\?v=${CACHE}`),
  );
  assert.match(
    share,
    new RegExp(`gdi-commercial-list-table-v1\\.js\\?v=${CACHE}`),
  );
  assert.match(auth, new RegExp(`dealality-gdi-ui\\.js\\?v=${CACHE}`));
  assert.match(share, new RegExp(`dealality-gdi-ui\\.js\\?v=${CACHE}`));
  assert.match(auth, new RegExp(`app\\.js\\?v=${CACHE}`));
  assert.match(share, new RegExp(`share-app\\.js\\?v=${CACHE}`));
  assert.match(auth, new RegExp(`group-demand-intelligence\\.css\\?v=${CACHE}`));
});

check("list_view_renders_commercial_table_not_tile_grid", () => {
  const { DealalityGdiUi: UI } = loadModules();
  const html = UI.opportunityCardsGridHtml(sampleRows, "list", {}, {
    sortKey: "priority",
    sortDir: 1,
  });
  assert.match(html, /gdi-commercial-table/);
  assert.match(html, /id="gdiCommercialOppTable"/);
  assert.match(html, /data-gdi-contract="gdi-commercial-list-table-v1"/);
  assert.match(html, /data-sort="title"/);
  assert.match(html, /data-sort="priority"/);
  assert.match(html, /data-sort="eventStartDate"/);
  assert.match(html, /Who to Contact/);
  assert.match(html, /Contact Readiness/);
  assert.match(html, /gdiListColumnSettingsBtn/);
  assert.doesNotMatch(html, /gdi-results-grid--list/);
  assert.doesNotMatch(html, /brand-card__/);
  assert.ok(html.includes("opp-a"));
  assert.ok(html.includes("opp-b"));
  assert.ok(html.includes("opp-c"));
});

check("tiles_view_unchanged_card_grid_path", () => {
  const { DealalityGdiUi: UI } = loadModules();
  const html = UI.opportunityCardsGridHtml(sampleRows, "tiles", {}, {
    sortKey: "priority",
    sortDir: 1,
  });
  assert.match(html, /gdi-results-grid/);
  assert.match(html, /data-gdi-view-mode="tiles"/);
  assert.doesNotMatch(html, /gdi-commercial-table/);
  assert.doesNotMatch(html, /gdiCommercialOppTable/);
  assert.doesNotMatch(html, /gdi-results-grid--list/);
});

check("list_tiles_id_parity_helper", () => {
  const { DealalityGdiCommercialListTable: Table } = loadModules();
  const ok = Table.listTilesIdParity(sampleRows, sampleRows.slice());
  assert.equal(ok.ok, true);
  const bad = Table.listTilesIdParity(sampleRows, sampleRows.slice(0, 2));
  assert.equal(bad.ok, false);
  assert.deepEqual(bad.missingFromTiles, ["opp-c"]);
});

check("filtered_inventory_id_parity_via_sortOpportunities", () => {
  const { DealalityGdiUi: UI, DealalityGdiCommercialListTable: Table } =
    loadModules();
  const listSorted = UI.sortOpportunities(sampleRows, "title", 1);
  const tilesSorted = UI.sortOpportunities(sampleRows, "title", 1);
  const parity = Table.listTilesIdParity(listSorted, tilesSorted);
  assert.equal(parity.ok, true, JSON.stringify(parity));
  assert.deepEqual(
    listSorted.map((o) => o.id),
    tilesSorted.map((o) => o.id),
  );
});

check("column_sort_semantic_priority_and_chrono_window", () => {
  const { DealalityGdiUi: UI } = loadModules();
  const byPri = UI.sortOpportunities(sampleRows, "priority", 1);
  assert.deepEqual(
    byPri.map((o) => o.id),
    ["opp-a", "opp-c", "opp-b"],
  );
  const byDate = UI.sortOpportunities(sampleRows, "eventStartDate", 1);
  // dated first chronologically; ongoing/empty after
  assert.equal(byDate[0].id, "opp-c");
  assert.equal(byDate[1].id, "opp-a");
  assert.equal(byDate[2].id, "opp-b");
  const byRooms = UI.sortOpportunities(sampleRows, "estimatedRoomNights", -1);
  assert.equal(byRooms[0].id, "opp-a");
  assert.equal(byRooms[1].id, "opp-c");
});

check("optional_columns_toggle_without_fabricated_revenue", () => {
  const { DealalityGdiCommercialListTable: Table } = loadModules();
  Table.saveOptionalColumns({ estimatedRoomNights: true, lastVerified: true });
  const html = Table.tableHtml(sampleRows, {
    sortKey: "priority",
    sortDir: 1,
    optionalColumns: Table.loadOptionalColumns(),
  });
  assert.match(html, /Estimated Room Nights/);
  assert.match(html, /Last Verified/);
  assert.match(html, />420</);
  assert.doesNotMatch(html, /ADR|synthetic|estimated revenue|\$\d/i);
  // Missing lastVerified shows em dash, not invented date
  assert.match(html, /—/);
});

check("who_to_contact_does_not_invent_names", () => {
  const { DealalityGdiCommercialListTable: Table } = loadModules();
  const html = Table.tableHtml(
    [{ id: "x", title: "No Contact Opp", priority: "WATCHLIST" }],
    { sortKey: "title", sortDir: 1 },
  );
  assert.match(html, /Contact not yet identified/);
  assert.doesNotMatch(html, /Jane Doe|John Smith|example@/i);
});

check("app_and_share_wire_list_interactions", () => {
  const app = read("public/js/group-demand-intelligence/app.js");
  const share = read("public/js/group-demand-intelligence/share-app.js");
  assert.match(app, /wireListTableInteractions/);
  assert.match(share, /wireListTableInteractions/);
  assert.match(app, /sortKey:\s*state\.sortKey/);
  assert.match(
    read("public/js/group-demand-intelligence/gdi-universal-presentation-controller.js"),
    /sortKey:\s*state\.sortKey/,
  );
});

check("css_includes_commercial_table_parity_styles", () => {
  const css = read("public/css/group-demand-intelligence.css");
  assert.match(css, /\.gdi-commercial-table/);
  assert.match(css, /sort-indicator/);
  assert.match(css, /position:\s*sticky/);
  assert.match(css, /\.gdi-list-table-scroll/);
});

check("deal_opportunities_page_untouched", () => {
  // Ensure this enhancement did not edit DOP page (parity reference only).
  // Presence check: DOP still owns dop-table; GDI owns gdi-commercial-table.
  const dop = read("public/deal-opportunities.html");
  assert.match(dop, /dop-table/);
  assert.doesNotMatch(dop, /gdi-commercial-table|gdiCommercialOppTable/);
});

if (failed) {
  console.error(`\n${failed} failure(s)`);
  process.exit(1);
}
console.log("\nOK test:gdi-sortable-commercial-list-view-v1");
