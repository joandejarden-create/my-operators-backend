#!/usr/bin/env node
/**
 * GDI performance + security unit gates (no live Airtable required).
 */
import assert from "node:assert/strict";
import {
  getCachedOpportunityDoc,
  setCachedOpportunityDoc,
  invalidateGdiHotelReadCache,
  getCachedProgressionMap,
  setCachedProgressionMap,
} from "../lib/group-demand-intelligence/read-cache.js";
import { rowsToCsv } from "../lib/group-demand-intelligence/live-commercial-quality-v1.js";

let pass = 0;
let fail = 0;
function check(name, fn) {
  try {
    fn();
    pass += 1;
    console.log("PASS", name);
  } catch (err) {
    fail += 1;
    console.error("FAIL", name, err.message || err);
  }
}

check("read_cache_hotel_scoped_no_bleed", () => {
  invalidateGdiHotelReadCache();
  setCachedOpportunityDoc("hotelA", { hotelId: "hotelA", opportunities: [{ id: "1" }] });
  setCachedOpportunityDoc("hotelB", { hotelId: "hotelB", opportunities: [{ id: "2" }] });
  assert.equal(getCachedOpportunityDoc("hotelA").opportunities[0].id, "1");
  assert.equal(getCachedOpportunityDoc("hotelB").opportunities[0].id, "2");
  invalidateGdiHotelReadCache("hotelA");
  assert.equal(getCachedOpportunityDoc("hotelA"), null);
  assert.ok(getCachedOpportunityDoc("hotelB"));
});

check("progression_cache_invalidate", () => {
  invalidateGdiHotelReadCache();
  const m = new Map([["opp1", { currentStatusLabel: "Open" }]]);
  setCachedProgressionMap("hotelA", m);
  assert.equal(getCachedProgressionMap("hotelA").get("opp1").currentStatusLabel, "Open");
  invalidateGdiHotelReadCache("hotelA");
  assert.equal(getCachedProgressionMap("hotelA"), null);
});

check("csv_formula_injection_escaped", () => {
  const csv = rowsToCsv([
    { A: "=CMD()", B: "+1-555", C: "-1", D: "@sum", E: "normal" },
  ]);
  assert.match(csv, /'=CMD\(\)/);
  assert.match(csv, /'\+1-555/);
  assert.match(csv, /'-1/);
  assert.match(csv, /'@sum/);
  assert.match(csv, /,normal/);
});

check("safe_http_url_helper_in_ui_module", async () => {
  const fs = await import("node:fs");
  const path = await import("node:path");
  const ui = fs.readFileSync(
    path.join(process.cwd(), "public/js/group-demand-intelligence/dealality-gdi-ui.js"),
    "utf8"
  );
  assert.match(ui, /function isSafeHttpUrl/);
  assert.match(ui, /isSafeHttpUrl\(s\.url\)/);
  assert.doesNotMatch(ui, /href="' \+ esc\(s\.url\) \+ '" target="_blank" rel="noopener">'/);
});

check("share_openDetail_shows_immediate_shell", async () => {
  const fs = await import("node:fs");
  const path = await import("node:path");
  const share = fs.readFileSync(
    path.join(process.cwd(), "public/js/group-demand-intelligence/share-app.js"),
    "utf8"
  );
  assert.match(share, /gdi-detail-loading/);
  assert.match(share, /peekShareHotelId/);
  assert.match(share, /Promise\.all\(\[resolvePromise, oppsPromise\]\)/);
});

check("auth_openDetail_shows_immediate_shell", async () => {
  const fs = await import("node:fs");
  const path = await import("node:path");
  const app = fs.readFileSync(
    path.join(process.cwd(), "public/js/group-demand-intelligence/app.js"),
    "utf8"
  );
  assert.match(app, /gdi-detail-loading/);
});

console.log(JSON.stringify({ suite: "gdi-performance-security-v1", pass, fail }));
process.exit(fail ? 1 : 0);
