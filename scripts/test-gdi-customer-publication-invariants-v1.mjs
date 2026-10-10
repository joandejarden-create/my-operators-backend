/**
 * GDI customer publication invariants V1 — gate counts must match API surface.
 *
 *   node scripts/test-gdi-customer-publication-invariants-v1.mjs
 *   npm run test:gdi-customer-publication-invariants-v1
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  countCanonicalWatchPublication,
  countCustomerFacingReadyWatch,
  assertCustomerPublicationCountsMatch,
  assertReportCountsMatchGates,
  CUSTOMER_PUBLICATION_INVARIANT_VERSION,
} from "../lib/group-demand-intelligence/customer-publication-invariants-v1.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import { isValidFutureWatch } from "../lib/group-demand-intelligence/future-watch/is-valid-future-watch-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const NOW = "2026-10-08";

function loadOpps(hotelId) {
  const p = path.join(
    ROOT,
    "data/group-demand-intelligence/hotels",
    hotelId,
    "opportunities.json"
  );
  if (!fs.existsSync(p)) return null;
  const doc = JSON.parse(fs.readFileSync(p, "utf8"));
  return Array.isArray(doc) ? doc : doc.opportunities || [];
}

assert.equal(CUSTOMER_PUBLICATION_INVARIANT_VERSION, "gdi_customer_publication_invariants_v1");

// Empty bag
{
  const r = assertCustomerPublicationCountsMatch([], { nowDate: NOW });
  assert.equal(r.ok, true);
  assert.equal(r.canonical.canonicalValidWatch, 0);
}

// Heuristic report claiming Valid Watch without gates must fail
{
  const r = assertReportCountsMatchGates([], {
    claimedReady: 0,
    claimedValidWatch: 2,
    nowDate: NOW,
  });
  assert.equal(r.ok, false);
  assert.equal(r.failures[0].code, "REPORT_VALID_WATCH_MISMATCH");
}

// Sheraton Mallorca — false report Valid Watch=2; gates=0; facing=0
{
  const opps = loadOpps("recjhQdAUiSyxfCqE");
  assert.ok(opps, "Sheraton opportunities missing");
  const counts = countCanonicalWatchPublication(opps, { nowDate: NOW });
  assert.equal(counts.canonicalValidWatch, 0);
  assert.equal(counts.canonicalReady, 0);
  assert.equal(filterCustomerFacingOpportunities(opps).length, 0);
  assert.equal(
    assertReportCountsMatchGates(opps, {
      claimedReady: 0,
      claimedValidWatch: 2,
      nowDate: NOW,
    }).ok,
    false
  );
  assert.equal(
    assertReportCountsMatchGates(opps, {
      claimedReady: 0,
      claimedValidWatch: 0,
      nowDate: NOW,
    }).ok,
    true
  );
  assert.equal(assertCustomerPublicationCountsMatch(opps, { nowDate: NOW }).ok, true);
  // Seed "watches" are NOT valid
  for (const id of [
    "camp_sheraton_golf_tournament_2026_sheraton",
    "camp_gph_sheraton_golf_break_2027_sheraton",
  ]) {
    const o = opps.find((x) => x.id === id);
    assert.ok(o, id);
    assert.equal(o.customerVisible, false);
    assert.equal(isValidFutureWatch(o, { nowDate: NOW }).ok, false);
  }
}

// Castillo — empty customer surface correct
{
  const opps = loadOpps("rec82D9zpB8fede1I");
  assert.ok(opps, "Castillo opportunities missing");
  const api = countCustomerFacingReadyWatch(opps, { nowDate: NOW });
  assert.equal(api.customerFacingTotal, 0);
  assert.equal(api.customerFacingReady, 0);
  assert.equal(api.customerFacingWatch, 0);
  assert.equal(assertCustomerPublicationCountsMatch(opps, { nowDate: NOW }).ok, true);
}

// YOTEL control — customer-facing Valid Watches must remain in facing set
{
  const opps = loadOpps("recrPQcZg7SFARRb2");
  assert.ok(opps, "YOTEL opportunities missing");
  const facing = filterCustomerFacingOpportunities(opps);
  assert.ok(facing.length >= 1, "YOTEL should have customer-facing rows");
  const facingIds = new Set(facing.map((o) => o.id));
  for (const o of facing) {
    if (o.customerVisible === true && isValidFutureWatch(o, { nowDate: NOW }).ok) {
      assert.ok(facingIds.has(o.id), `YOTEL Valid Watch missing from facing: ${o.id}`);
    }
  }
  const honest = assertReportCountsMatchGates(opps, {
    claimedReady: countCanonicalWatchPublication(opps, { nowDate: NOW }).canonicalReady,
    claimedValidWatch: countCanonicalWatchPublication(opps, { nowDate: NOW })
      .canonicalValidWatch,
    nowDate: NOW,
  });
  assert.equal(honest.ok, true);
}

console.log("test-gdi-customer-publication-invariants-v1: PASS");
