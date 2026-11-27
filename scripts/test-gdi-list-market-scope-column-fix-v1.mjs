#!/usr/bin/env node
/**
 * Market Scope must not collapse into Actions under fixed list table layout.
 *   npm run test:gdi-list-market-scope-column-fix-v1
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

function loadTable() {
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
  return sandbox.DealalityGdiCommercialListTable;
}

check("market_scope_inserts_before_actions_in_header_and_cells", () => {
  const Table = loadTable();
  const cols = Table.visibleColumns({ marketScope: true }, ["marketScope"]);
  const ids = cols.map((c) => c.id);
  assert.ok(ids.includes("marketScope"));
  assert.ok(ids.includes("actions"));
  assert.ok(ids.indexOf("marketScope") < ids.indexOf("actions"));
  assert.equal(ids[ids.length - 1], "actions");

  const html = Table.tableHtml(
    [
      {
        id: "opp1",
        title: "Sample Opportunity",
        inventoryPopulations: ["HOTEL"],
        hotelMarketScopeEligible: true,
      },
    ],
    { optionalColumns: { marketScope: true }, inventoryRows: [{ id: "opp1", inventoryPopulations: ["HOTEL"], hotelMarketScopeEligible: true }] },
  );
  assert.match(html, /data-col="marketScope"/);
  assert.match(html, /data-col="actions"/);
  const headerOrder = [...html.matchAll(/<th[^>]*data-col="([^"]+)"/g)].map((m) => m[1]);
  const cellOrder = [...html.matchAll(/<td data-col="([^"]+)"/g)].map((m) => m[1]);
  assert.deepEqual(headerOrder, cellOrder.slice(0, headerOrder.length));
  assert.ok(headerOrder.indexOf("marketScope") < headerOrder.indexOf("actions"));
});

check("css_reserves_market_scope_and_actions_widths", () => {
  const css = read("public/css/group-demand-intelligence.css");
  const html = read("public/group-demand-intelligence.html");
  assert.match(css, /table-layout:\s*fixed/);
  assert.match(css, /th\[data-col="marketScope"\]/);
  assert.match(css, /td\[data-col="marketScope"\]/);
  assert.match(css, /min-width:\s*5\.5rem/);
  assert.match(css, /th\[data-col="actions"\][\s\S]*?min-width:\s*4\.75rem/);
  assert.match(css, /min-width:\s*1240px/);
  assert.match(html, /group-demand-intelligence\.css\?v=gdi-list-market-scope-col-20261127/);
});

if (failed) {
  console.error(`\n${failed} failure(s)`);
  process.exit(1);
}
console.log("\nOK test:gdi-list-market-scope-column-fix-v1");
