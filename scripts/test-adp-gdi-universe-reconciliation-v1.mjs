/**
 * Tests for full ADP/GDI universe reconciliation artifacts.
 *   node scripts/test-adp-gdi-universe-reconciliation-v1.mjs
 */
import "../load-env.js";
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  CANONICAL_INTELLIGENCE_BASE_ID,
  LEGACY_DEAL_CAPTURE_MVP_BASE_ID,
  assertNotLegacyMvpCanonicalBase,
  isLegacyMvpBase,
} from "../lib/decision-outcomes/airtable-base.js";
import { GDI_OPPORTUNITIES_TABLE_NAME } from "../lib/group-demand-intelligence/opportunity-field-map.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const DIR = path.join(ROOT, "reports", "adp-gdi-universe-reconciliation-v1");

assert.equal(GDI_OPPORTUNITIES_TABLE_NAME, "Group Demand Opportunities");
assert.equal(isLegacyMvpBase(LEGACY_DEAL_CAPTURE_MVP_BASE_ID), true);
assert.throws(() => assertNotLegacyMvpCanonicalBase(LEGACY_DEAL_CAPTURE_MVP_BASE_ID, { surface: "t" }));
assert.doesNotThrow(() => assertNotLegacyMvpCanonicalBase(CANONICAL_INTELLIGENCE_BASE_ID, { surface: "t" }));

const universe = JSON.parse(fs.readFileSync(path.join(DIR, "FULL_ADP_HOTEL_UNIVERSE.json"), "utf8"));
const summary = JSON.parse(fs.readFileSync(path.join(DIR, "SUMMARY-apply.json"), "utf8"));
const results = JSON.parse(fs.readFileSync(path.join(DIR, "RESULTS-apply.json"), "utf8"));

assert.equal(universe.hotels.length, 19);
assert.equal(summary.totalPublished, 19);
assert.equal(summary.totalActiveAdp, 19);
assert.equal(summary.hpcLinked, 19);
assert.equal(summary.hiWithCommercial, 19);
assert.equal(summary.adpAttrActive, 19);
assert.equal(summary.adpCertified, 19);
assert.equal(summary.gdiInitialized, 19);
assert.equal(summary.gdiMissingAfter, 0);
assert.equal(summary.incomplete.length, 0);

for (const r of results.results) {
  assert.ok(r.hotel.hpcId, r.hotel.adpPropertyId);
  assert.ok(r.after.hi.commercialId, r.hotel.hotelName);
  assert.ok(r.after.adpAttrs.active > 0, r.hotel.hotelName);
  assert.equal(r.after.adpAttrs.multiActiveSameKey, 0, r.hotel.hotelName);
  assert.ok(r.after.gdi.fits > 0, r.hotel.hotelName);
  assert.equal(r.after.gdi.fits, r.after.gdi.uniqueFits, r.hotel.hotelName);
  assert.ok(r.after.gdi.targets > 0, r.hotel.hotelName);
  assert.equal(r.after.gdi.targets, r.after.gdi.uniqueTargets, r.hotel.hotelName);
  assert.ok(r.after.gdi.runs > 0, r.hotel.hotelName);
  assert.equal(r.after.gdi.notResearchedPromoted, 0, r.hotel.hotelName);
  assert.ok(r.gdiInfraPass, r.hotel.hotelName);
  assert.ok(["COMPLETE_WITH_READY_OPPS", "COMPLETE_ZERO_READY_OPPS"].includes(r.finalStatus), r.hotel.hotelName);
}

console.log(
  JSON.stringify(
    {
      ok: true,
      hotels: 19,
      gdiInitialized: 19,
      customerReadyHotels: summary.customerReadyAtLeast1,
      zeroReadyValid: summary.zeroReadyValid,
    },
    null,
    2
  )
);
