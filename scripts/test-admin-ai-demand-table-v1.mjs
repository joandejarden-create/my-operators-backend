#!/usr/bin/env node
/**
 * Unit checks for shared AI Admin table helpers (loaded via vm).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const src = fs.readFileSync(
  path.join(root, "public/js/admin-ai-demand-table.js"),
  "utf8"
);
const sandbox = { window: {}, console };
vm.createContext(sandbox);
vm.runInContext(src, sandbox);
const T = sandbox.window.DealalityAiAdminTable;
assert.ok(T);

const rows = [
  { hotelId: "a", displayName: "Alpha", readyCount: 2, lastGeneratedAt: "2026-10-01T12:00:00Z" },
  { hotelId: "b", displayName: "Beta", readyCount: null, lastGeneratedAt: null },
  { hotelId: "c", displayName: "Charlie", readyCount: 10, lastGeneratedAt: "2026-09-01T12:00:00Z" },
];

const byNum = T.sortRows(rows, { key: "readyCount", dir: -1, type: "number", tieBreak: "displayName" });
assert.equal(byNum[0].hotelId, "c");
assert.equal(byNum[1].hotelId, "a");
assert.equal(byNum[2].hotelId, "b"); // null last

const byDate = T.sortRows(rows, { key: "lastGeneratedAt", dir: 1, type: "date", tieBreak: "displayName" });
assert.equal(byDate[0].hotelId, "c");
assert.equal(byDate[2].hotelId, "b");

const sel = T.createSelectionStore();
sel.add("a");
sel.add("b");
sel.retain(["a", "c"]);
assert.equal(sel.count(), 1);
assert.equal(sel.has("a"), true);
assert.equal(sel.has("b"), false);

const ctrl = T.createTableController({
  defaultSortKey: "displayName",
  idKey: "hotelId",
  sortTypes: { displayName: "string", readyCount: "number" },
  onResetFilters: function () {},
});
ctrl.selection.add("a");
ctrl.selection.add("z");
const view = ctrl.apply(rows, function (r) {
  return r.hotelId !== "b";
});
assert.equal(view.total, 2);
assert.equal(ctrl.selection.has("z"), false); // pruned after filter
assert.ok(T.actionsHtml({ primary: [{ label: "Open", act: "open" }], more: [{ label: "More", act: "x" }] }, { hotel: "a" }).includes("ada-row-actions"));

console.log("test-admin-ai-demand-table-v1: PASS");
