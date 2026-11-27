#!/usr/bin/env node
/**
 * GDI Group & Demand Opportunities — Tiles is the permanent reload default.
 * List may be selected during a session; it must not persist across reload.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import vm from "node:vm";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const APP = path.join(ROOT, "public/js/group-demand-intelligence/app.js");
const src = fs.readFileSync(APP, "utf8");

assert.match(src, /viewMode:\s*"tiles"/);
assert.doesNotMatch(
  src,
  /sessionStorage\.getItem\(STORAGE_PREFIX \+ "viewMode"\)/,
  "viewMode must not be restored from sessionStorage on load",
);
assert.match(
  src,
  /sessionStorage\.removeItem\(STORAGE_PREFIX \+ "viewMode"\)/,
  "stale viewMode persistence must be cleared",
);

// Extract + execute the browse persistence helpers in isolation.
const storage = new Map();
const fakeSession = {
  getItem(k) {
    return storage.has(k) ? storage.get(k) : null;
  },
  setItem(k, v) {
    storage.set(String(k), String(v));
  },
  removeItem(k) {
    storage.delete(k);
  },
};
const state = { viewMode: "tiles", sortKey: "priority", sortDir: 1 };
const STORAGE_PREFIX = "gdi-auth-";
storage.set(STORAGE_PREFIX + "viewMode", "list");
storage.set(STORAGE_PREFIX + "sortKey", "priority");
storage.set(STORAGE_PREFIX + "sortDir", "1");

const helpersSrc = `
${src.match(/function readPersistedBrowse\(\) \{[\s\S]*?\n  \}/)[0]}
${src.match(/function persistBrowse\(\) \{[\s\S]*?\n  \}/)[0]}
`;
const sandbox = {
  state,
  STORAGE_PREFIX,
  sessionStorage: fakeSession,
  UI: { sanitizeListSortKey: (s) => s },
};
vm.runInNewContext(
  `${helpersSrc}\nreadPersistedBrowse();\nstate.viewMode = "list";\npersistBrowse();\n`,
  sandbox,
);

assert.equal(sandbox.state.viewMode, "list", "manual List switch remains allowed in-session");
assert.equal(
  fakeSession.getItem(STORAGE_PREFIX + "viewMode"),
  null,
  "List must not be written to sessionStorage",
);

// Simulate reload: re-read persistence with a stale list value planted.
storage.set(STORAGE_PREFIX + "viewMode", "list");
sandbox.state.viewMode = "list";
vm.runInNewContext(`readPersistedBrowse();`, sandbox);
assert.equal(
  sandbox.state.viewMode,
  "tiles",
  "reload must force Tiles even if sessionStorage previously held list",
);
assert.equal(fakeSession.getItem(STORAGE_PREFIX + "viewMode"), null);

console.log("OK test:gdi-tiles-default-view-v1");
