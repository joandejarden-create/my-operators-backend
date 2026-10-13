#!/usr/bin/env node
/**
 * Brand Explorer route-state regression tests (direct + /app#/ hash).
 *
 *   node scripts/test-brand-explorer-route-state.mjs
 *   npm run test:brand-explorer-route-state
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import vm from "node:vm";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const src = fs.readFileSync(
  path.join(__dirname, "../public/js/brand-explorer-route-state.js"),
  "utf8"
);
const sandbox = { URLSearchParams, console };
vm.createContext(sandbox);
vm.runInContext(src, sandbox);
const RS = sandbox.BrandExplorerRouteState;
assert.ok(RS && typeof RS.getBrandExplorerRouteState === "function");

function loc({ pathname = "/", search = "", hash = "" } = {}) {
  return { pathname, search, hash };
}

function testDirectPreview() {
  const st = RS.getBrandExplorerRouteState({
    location: loc({
      pathname: "/brand-explorer-combined.html",
      search: "?id=abc&beInternalPreview=1",
    }),
    parentLocation: null,
  });
  assert.equal(st.brandId, "abc");
  assert.equal(st.beInternalPreview, true);
  assert.equal(st.isInternalPreviewRequest, true);
}

function testAppShellPreview() {
  const st = RS.getBrandExplorerRouteState({
    location: loc({
      pathname: "/brand-explorer-combined.html",
      search: "?embed=1&appShell=1&id=abc",
    }),
    parentLocation: loc({
      pathname: "/app",
      search: "",
      hash: "#/brand-explorer-combined.html?id=abc&beInternalPreview=1",
    }),
  });
  assert.equal(st.brandId, "abc");
  assert.equal(st.beInternalPreview, true);
  assert.equal(st.isInternalPreviewRequest, true);
}

function testAppShellNoPreview() {
  const st = RS.getBrandExplorerRouteState({
    location: loc({
      pathname: "/brand-explorer-combined.html",
      search: "?embed=1&appShell=1&id=abc",
    }),
    parentLocation: loc({
      pathname: "/app",
      hash: "#/brand-explorer-combined.html?id=abc",
    }),
  });
  assert.equal(st.brandId, "abc");
  assert.equal(st.beInternalPreview, false);
  assert.equal(st.isInternalPreviewRequest, false);
}

function testHashParse() {
  const parsed = RS.parseHashRoute(
    "#/brand-explorer-combined.html?id=recSgIQ0bhRpPXZbU&beInternalPreview=1"
  );
  assert.equal(parsed.route, "/brand-explorer-combined.html");
  assert.equal(parsed.params.get("id"), "recSgIQ0bhRpPXZbU");
  assert.equal(parsed.params.get("beInternalPreview"), "1");
}

function testExportPdfCountsAsPreview() {
  const st = RS.getBrandExplorerRouteState({
    location: loc({ search: "?id=abc&exportPdf=1" }),
    parentLocation: null,
  });
  assert.equal(st.exportPdf, true);
  assert.equal(st.isInternalPreviewRequest, true);
}

function testBuildQueryPreservesPreview() {
  const st = RS.getBrandExplorerRouteState({
    location: loc({ search: "?embed=1&appShell=1&id=old&beInternalPreview=1" }),
    parentLocation: loc({
      pathname: "/app",
      hash: "#/brand-explorer-combined?id=old&beInternalPreview=1",
    }),
  });
  const q = RS.buildBrandExplorerQuery("newId", st);
  assert.equal(q.get("id"), "newId");
  assert.equal(q.get("beInternalPreview"), "1");
  assert.equal(q.get("embed"), null);
  assert.equal(q.get("appShell"), null);
}

function testForwardKeysIncludePreview() {
  assert.ok(RS.FORWARD_QUERY_KEYS.includes("beInternalPreview"));
  assert.ok(RS.FORWARD_QUERY_KEYS.includes("factoryPreview"));
  assert.ok(RS.FORWARD_QUERY_KEYS.includes("id"));
}

function testParentPreviewOverridesStaleIframe() {
  const st = RS.getBrandExplorerRouteState({
    location: loc({
      pathname: "/brand-explorer-combined.html",
      search: "?embed=1&appShell=1&id=recSgIQ0bhRpPXZbU",
    }),
    parentLocation: loc({
      pathname: "/app",
      hash: "#/brand-explorer-combined.html?id=recSgIQ0bhRpPXZbU&beInternalPreview=1",
    }),
  });
  assert.equal(st.beInternalPreview, true);
  assert.equal(st.brandId, "recSgIQ0bhRpPXZbU");
}

/** Simulate app.js hash→embed forward (pre vs post fix). */
function testHashToEmbedForward() {
  const hashQs = RS.parseHashRoute(
    "#/brand-explorer-combined.html?id=recSgIQ0bhRpPXZbU&beInternalPreview=1"
  ).params;
  const u = new URL("http://localhost:8080/brand-explorer-combined.html?embed=1&appShell=1");
  RS.FORWARD_QUERY_KEYS.forEach((key) => {
    const v = hashQs.get(key);
    if (v && !u.searchParams.get(key)) u.searchParams.set(key, v);
  });
  assert.equal(u.searchParams.get("id"), "recSgIQ0bhRpPXZbU");
  assert.equal(u.searchParams.get("beInternalPreview"), "1");
  assert.equal(u.searchParams.get("embed"), "1");
}

testDirectPreview();
testAppShellPreview();
testAppShellNoPreview();
testHashParse();
testExportPdfCountsAsPreview();
testBuildQueryPreservesPreview();
testForwardKeysIncludePreview();
testParentPreviewOverridesStaleIframe();
testHashToEmbedForward();

console.log(
  JSON.stringify(
    {
      ok: true,
      cases: 9,
      ready: "brand_explorer_route_state_direct_and_hash_preview_pass",
    },
    null,
    2
  )
);
