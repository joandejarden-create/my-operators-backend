#!/usr/bin/env node
/**
 * GDI property-bar research status — compact Last Updated + details ⓘ
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import vm from "node:vm";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

function loadUi() {
  const src = fs.readFileSync(
    path.join(ROOT, "public/js/group-demand-intelligence/dealality-gdi-ui.js"),
    "utf8"
  );
  const sandbox = { window: {}, globalThis: {}, console, URL, URLSearchParams };
  sandbox.window = sandbox;
  sandbox.globalThis = sandbox;
  vm.runInNewContext(src, sandbox, { filename: "dealality-gdi-ui.js" });
  return sandbox.DealalityGdiUi;
}

const UI = loadUi();
let failed = 0;
function check(name, fn) {
  try {
    fn();
    console.log("PASS", name);
  } catch (err) {
    failed += 1;
    console.error("FAIL", name, err.message);
  }
}

check("compact_date_strips_time_and_timezone", () => {
  assert.equal(
    UI.formatCompactResearchDate("2026-10-26T18:30:00.000Z"),
    "Oct 26, 2026"
  );
  assert.equal(
    UI.formatCompactResearchDate("Oct 26, 2026, 2:30 PM EDT"),
    "Oct 26, 2026"
  );
  assert.equal(UI.formatCompactResearchDate(null), null);
  assert.equal(UI.formatCompactResearchDate("History unavailable"), null);
});

check("last_updated_uses_research_not_publication", () => {
  const date = UI.resolveLastUpdatedCompactDate({
    lastResearchAt: "2026-10-26T12:00:00.000Z",
    lastPublishedUpdateAt: "2026-11-01T12:00:00.000Z",
    lastSourceVerificationAt: "2026-11-02T12:00:00.000Z",
  });
  assert.equal(date, "Oct 26, 2026");
  assert.equal(
    UI.resolveLastUpdatedCompactDate({
      lastPublishedUpdateAt: "2026-11-01T12:00:00.000Z",
      lastSourceVerificationAt: "2026-11-02T12:00:00.000Z",
    }),
    null
  );
});

check("status_states", () => {
  assert.equal(
    UI.compactResearchStatusLabel({ runStatus: "COMPLETED" }),
    "Research Complete"
  );
  assert.equal(
    UI.compactResearchStatusLabel({ runStatus: "NOT_YET_RUN" }),
    "Not run yet"
  );
  assert.equal(
    UI.compactResearchStatusLabel({ runStatus: "FAILED" }),
    "Research unsuccessful"
  );
  assert.equal(
    UI.compactResearchStatusLabel({ runStatus: "RUNNING" }),
    "Research in progress"
  );
  assert.equal(
    UI.compactResearchStatusLabel({ runStatus: "HISTORY_UNAVAILABLE" }),
    "History unavailable"
  );
});

check("property_bar_compact_main_line", () => {
  const html = UI.propertyBarHtml({
    mode: "auth",
    hotelId: "recLuxvwwxID7U2B8",
    hotel: { hotelName: "Bethesda Marriott", city: "Bethesda", state: "MD" },
    lastResearchAt: "2026-10-26T15:00:00.000Z",
    lastSourceVerificationAt: "2026-10-27T09:00:00.000Z",
    lastPublishedUpdateAt: "2026-10-28T11:00:00.000Z",
    runStatus: "COMPLETED",
    runStatusLabel: "Research Completed",
  });
  assert.match(html, /Last Updated:/);
  assert.match(html, /data-gdi-last-updated="Oct 26, 2026"/);
  assert.match(html, /data-gdi-research-status="Research Complete"/);
  assert.match(html, /gdi-research-activity-details-wrap/);
  assert.match(html, /Last research/);
  assert.match(html, /Last source verification/);
  assert.match(html, /Last published/);
  const compactValue = html.match(
    /data-gdi-last-updated="([^"]+)"/
  );
  assert.ok(compactValue);
  assert.doesNotMatch(compactValue[1], /\d{1,2}:\d{2}/);
  assert.doesNotMatch(compactValue[1], /EDT|UTC|GMT/);
  assert.doesNotMatch(html, />Run:</);
  assert.doesNotMatch(
    html,
    /gdi-property-bar__history-label">Last research:</
  );
});

check("never_run_state", () => {
  const html = UI.propertyBarHtml({
    mode: "auth",
    hotelId: "recX",
    hotel: { hotelName: "Demo Hotel" },
    runStatus: "NOT_YET_RUN",
  });
  assert.match(html, /Last Updated:/);
  assert.match(html, /—|–|-/);
  assert.match(html, /Not run yet/);
});

check("app_and_share_pass_iso_fields", () => {
  const app = fs.readFileSync(
    path.join(ROOT, "public/js/group-demand-intelligence/app.js"),
    "utf8"
  );
  const share = fs.readFileSync(
    path.join(ROOT, "public/js/group-demand-intelligence/share-app.js"),
    "utf8"
  );
  assert.match(app, /lastResearchAt:\s*s\.lastResearchAt/);
  assert.match(share, /lastResearchAt:\s*s\.lastResearchAt/);
  assert.match(app, /lastSourceVerificationAt:\s*s\.lastSourceVerificationAt/);
  assert.match(share, /lastPublishedUpdateAt:/);
});

if (failed) {
  console.error(`\n${failed} failed`);
  process.exit(1);
}
console.log("\nAll research-status-header-compact checks passed");
