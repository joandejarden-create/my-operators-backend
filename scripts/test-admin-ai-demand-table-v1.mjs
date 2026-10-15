#!/usr/bin/env node
/**
 * Unit checks for shared AI Admin table helpers (loaded via vm).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const src = fs.readFileSync(
  path.join(root, "public/js/admin-ai-demand-table.js"),
  "utf8"
);

function loadTable(doc) {
  const sandbox = {
    window: {},
    console,
    document: doc,
    setTimeout: global.setTimeout,
    clearTimeout: global.clearTimeout,
  };
  sandbox.window.document = doc;
  sandbox.globalThis = sandbox;
  vm.createContext(sandbox);
  vm.runInContext(src, sandbox);
  return sandbox.window.DealalityAiAdminTable;
}

const css = fs.readFileSync(
  path.join(root, "public/css/admin-ai-demand-table.css"),
  "utf8"
);
assert.match(css, /\.ada-more-menu\s*\{/);
assert.match(css, /z-index:\s*10050/);
assert.doesNotMatch(
  css,
  /\.ada-more__menu\s*\{[^}]*z-index:\s*40/s,
  "legacy in-cell more menu stacking must be removed"
);

// Minimal DOM for portal menu (happy-dom or linkedom if present; else lightweight shim)
let document;
try {
  const require = createRequire(import.meta.url);
  const { Window } = require("happy-dom");
  const win = new Window({ url: "https://example.test/admin" });
  document = win.document;
} catch {
  try {
    const require = createRequire(import.meta.url);
    const { parseHTML } = require("linkedom");
    document = parseHTML("<!doctype html><html><body></body></html>").document;
  } catch {
    document = null;
  }
}

const rows = [
  { hotelId: "a", displayName: "Alpha", readyCount: 2, lastGeneratedAt: "2026-10-01T12:00:00Z" },
  { hotelId: "b", displayName: "Beta", readyCount: null, lastGeneratedAt: null },
  { hotelId: "c", displayName: "Charlie", readyCount: 10, lastGeneratedAt: "2026-09-01T12:00:00Z" },
];

if (!document) {
  // Fallback: pure string checks without portal open
  const sandbox = { window: {}, console, document: undefined };
  vm.createContext(sandbox);
  vm.runInContext(src.replace(/installMoreMenuOnce\(\);/g, "/* skip */"), sandbox);
  const T = sandbox.window.DealalityAiAdminTable;
  assert.ok(T);
  const byNum = T.sortRows(rows, {
    key: "readyCount",
    dir: -1,
    type: "number",
    tieBreak: "displayName",
  });
  assert.equal(byNum[0].hotelId, "c");
  const html = T.actionsHtml(
    {
      primary: [{ label: "View PDF", act: "view-pdf" }],
      more: [{ label: "Archive", act: "archive" }],
    },
    { hotel: "a" }
  );
  assert.match(html, /ada-row-actions/);
  assert.match(html, /ada-action-icon/);
  assert.match(html, /ada-more-trigger/);
  assert.match(html, /ada-more-template/);
  assert.doesNotMatch(html, /<details class="ada-more"/);
  assert.match(T.selectionBarHtml(2, true), /Select this page/);
  assert.doesNotMatch(T.selectionBarHtml(2, true), /page select/);
  console.log("test-admin-ai-demand-table-v1: PASS (string-only; no DOM lib)");
  process.exit(0);
}

const T = loadTable(document);
assert.ok(T);

const byNum = T.sortRows(rows, {
  key: "readyCount",
  dir: -1,
  type: "number",
  tieBreak: "displayName",
});
assert.equal(byNum[0].hotelId, "c");
assert.equal(byNum[1].hotelId, "a");
assert.equal(byNum[2].hotelId, "b");

const byDate = T.sortRows(rows, {
  key: "lastGeneratedAt",
  dir: 1,
  type: "date",
  tieBreak: "displayName",
});
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
assert.equal(ctrl.selection.has("z"), false);

const shell = document.createElement("div");
shell.className = "ada-table-shell";
shell.innerHTML =
  '<table class="adr-table"><tbody><tr>' +
  '<td class="ada-col-actions" style="position:sticky;right:0;z-index:3">' +
  T.actionsHtml(
    {
      primary: [{ label: "View PDF", act: "view-pdf" }],
      more: [
        { label: "Copy GDI URL", act: "copy-gdi" },
        { label: "Report Archive", act: "archive" },
      ],
    },
    { hotel: "recTEST", name: "Test Hotel" }
  ) +
  '</td><td class="ada-col-actions" style="position:sticky;right:0;z-index:3">neighbor</td>' +
  "</tr></tbody></table>";
document.body.appendChild(shell);

const html = shell.innerHTML;
assert.match(html, /ada-more-trigger/);
assert.match(html, /ada-more-template/);
assert.doesNotMatch(html, /<details class="ada-more"/);
assert.match(T.selectionBarHtml(1, true), /Select this page/);
assert.doesNotMatch(T.selectionBarHtml(1, true), /≠/);

let firedAct = null;
shell.addEventListener("click", function (ev) {
  const btn = ev.target.closest("button[data-act]");
  if (!btn) return;
  firedAct = btn.getAttribute("data-act");
  const wrap = btn.closest("[data-hotel]");
  assert.equal(wrap && wrap.getAttribute("data-hotel"), "recTEST");
});

const trigger = shell.querySelector(".ada-more-trigger");
assert.ok(trigger);
T.installMoreMenuOnce();
T.openMoreMenu(trigger);

const portal = document.querySelector(".ada-more-menu[data-ada-portal-menu]");
assert.ok(portal, "menu must portal to document.body");
assert.equal(portal.parentNode, document.body);
assert.ok(!shell.contains(portal), "menu must escape table/sticky stacking");
const style = portal.getAttribute("style") || "";
assert.match(style, /position:\s*fixed/i);
assert.equal(trigger.getAttribute("aria-expanded"), "true");

const archiveItem = portal.querySelector('[data-act="archive"]');
assert.ok(archiveItem);
archiveItem.dispatchEvent(
  new document.defaultView.MouseEvent("click", { bubbles: true, cancelable: true })
);
assert.equal(firedAct, "archive", "menu item click must reach row handler via proxy");
assert.equal(document.querySelectorAll(".ada-more-menu").length, 0, "menu closes after act");

T.openMoreMenu(trigger);
assert.ok(document.querySelector(".ada-more-menu"));
T.closeMoreMenu();
assert.equal(document.querySelectorAll(".ada-more-menu").length, 0);
assert.equal(trigger.getAttribute("aria-expanded"), "false");

console.log("test-admin-ai-demand-table-v1: PASS");
