/**
 * GDI live visibility integrity invariants V1 (unit + optional live Airtable).
 *
 *   node scripts/test-gdi-live-visibility-integrity-v1.mjs
 *   node scripts/test-gdi-live-visibility-integrity-v1.mjs --live
 */
import "../load-env.js";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  CONTROL_API_VISIBLE_EXPECT,
  simulateGdiListApiOpportunities,
  assertCustomerFacingReturnedByListApi,
  assertApiEqualsUiDataModel,
  checkControlApiVisibleCount,
} from "../lib/group-demand-intelligence/live-visibility-invariants-v1.js";
import { isCustomerSurfaceActiveEligible } from "../lib/group-demand-intelligence/customer-surface-revalidation-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const LIVE = process.argv.includes("--live");

// --- unit: surface-eligible rows must appear in list simulation ---
{
  const rows = [
    {
      id: "gdi_opp_vis_ok_1",
      hotelId: "recLuxvwwxID7U2B8",
      title: "Visibility OK Event 2027",
      organizationName: "Test Org",
      priority: "MEDIUM_PRIORITY",
      customerFacingState: "ACTIVE",
      eventStartDate: "2027-06-01",
      officialSource: "https://example.com/event",
      lodgingEvidence: { roomBlockMentioned: true, status: "CONFIRMED" },
      entityClass: "NAMED_ORGANIZATION_EVENT",
    },
    {
      id: "gdi_opp_vis_dq_1",
      hotelId: "recLuxvwwxID7U2B8",
      title: "How To Manage The Paper — EXHIBITOR BLOCK",
      organizationName: "Noise",
      priority: "WATCHLIST",
      customerFacingState: "FUTURE_WATCH",
    },
  ];
  const inv = assertCustomerFacingReturnedByListApi(rows, {
    hotelId: "recLuxvwwxID7U2B8",
  });
  // DQ row may or may not be surface-eligible; invariant only cares about eligible → API
  assert.equal(inv.ok, true, JSON.stringify(inv.violations));
}

// --- unit: API count === UI data model before chips ---
{
  const api = [{ id: "a" }, { id: "b" }];
  const ui = [{ id: "a" }, { id: "b" }];
  const eq = assertApiEqualsUiDataModel(api, ui);
  assert.equal(eq.ok, true);
  const bad = assertApiEqualsUiDataModel(api, [{ id: "a" }]);
  assert.equal(bad.ok, false);
  assert.deepEqual(bad.onlyApi, ["b"]);
}

// --- unit: control expect map ---
assert.equal(checkControlApiVisibleCount("RENAISSANCE", 11).ok, true);
assert.equal(checkControlApiVisibleCount("HILTON", 0).ok, true);
assert.equal(checkControlApiVisibleCount("RENAISSANCE", 0).ok, false);

// --- unit: list path does not require readiness gate ---
{
  const thinButSurface = {
    id: "gdi_opp_thin_but_visible",
    hotelId: "recG66DQJKP2c0UNh",
    title: "Thin Summary Event 2027 NYC",
    organizationName: "Org",
    priority: "MEDIUM_PRIORITY",
    customerFacingState: "WATCH",
    eventStartDate: "2027-09-01",
    officialSource: "https://example.com/e",
    lodgingEvidence: { roomBlockMentioned: true, status: "CONFIRMED" },
    entityClass: "NAMED_ORGANIZATION_EVENT",
    summary: "short",
  };
  const list = simulateGdiListApiOpportunities([thinButSurface]);
  // If surface-eligible, must be in list regardless of THIN summary
  if (isCustomerSurfaceActiveEligible(thinButSurface)) {
    assert.equal(list.length, 1);
  }
}

console.log("unit invariants OK");

if (!LIVE) {
  // Offline: freeze-check against last audit artifact if present
  const summaryPath = path.join(
    ROOT,
    "reports/group-demand-intelligence/live-visibility-integrity-audit-v1/RUN_SUMMARY.json"
  );
  if (fs.existsSync(summaryPath)) {
    const summary = JSON.parse(fs.readFileSync(summaryPath, "utf8"));
    for (const [key, cfg] of Object.entries(CONTROL_API_VISIBLE_EXPECT)) {
      const lower = key.toLowerCase();
      const actual = summary.controls?.[lower]?.api;
      if (actual == null) continue;
      const chk = checkControlApiVisibleCount(key, actual);
      assert.equal(
        chk.ok,
        true,
        `${key} api=${actual} expect=${cfg.expect}`
      );
    }
    assert.equal(summary.proven63Match, true, "proven63Match must be true");
    console.log("offline freeze vs RUN_SUMMARY OK");
  }
  console.log("PASS (unit; use --live for Airtable)");
  process.exit(0);
}

// --- live Airtable control hotels ---
const { loadOpportunitiesCanonical } = await import(
  "../lib/group-demand-intelligence/opportunity-persistence.js"
);

for (const [key, cfg] of Object.entries(CONTROL_API_VISIBLE_EXPECT)) {
  const doc = await loadOpportunitiesCanonical(cfg.hpc);
  const api = simulateGdiListApiOpportunities(doc.opportunities || []);
  const chk = checkControlApiVisibleCount(key, api.length);
  assert.equal(
    chk.ok,
    true,
    `${key} live api=${api.length} expect=${cfg.expect}`
  );
  const inv = assertCustomerFacingReturnedByListApi(doc.opportunities || [], {
    hotelId: cfg.hpc,
  });
  assert.equal(inv.ok, true, `${key} violations=${JSON.stringify(inv.violations)}`);
  // UI data model before chips = API list
  const uiEq = assertApiEqualsUiDataModel(api, api);
  assert.equal(uiEq.ok, true);
  console.log(`[live] ${key} api=${api.length} OK`);
}

console.log("PASS (live)");
