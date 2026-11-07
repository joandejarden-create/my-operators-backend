/**
 * GDI customer publication invariants V1 — gate counts must match API surface.
 *
 * Distinguishes:
 *   - Approved durable publication present (Spice Island control)
 *   - No approved publication (YOTEL control) — research FS ≠ published inventory
 *   - Report claims must equal gate counts (no inflated Valid Watch)
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
import { isGdiCustomerOpportunityReady } from "../lib/group-demand-intelligence/customer-readiness-gate-v1.js";
import { isValidFutureWatch } from "../lib/group-demand-intelligence/future-watch/is-valid-future-watch-v1.js";
import { createGdiPublishedInventoryRegistry } from "../lib/group-demand-intelligence/durable-publication/gdi-published-inventory-registry-v1.js";
import { SPICE_ISLAND_HOTEL_ID } from "../lib/group-demand-intelligence/durable-publication/gdi-approved-publication-opportunity-source-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const NOW = "2026-10-08";
const YOTEL_HPC = "recrPQcZg7SFARRb2";
const SPICE_HPC = SPICE_ISLAND_HOTEL_ID;

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
assert.equal(SPICE_HPC, "recKRJjcPnb4tVDDS");

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
  assert.equal(filterCustomerFacingOpportunities(opps, { nowDate: NOW }).length, 0);
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

// Spice Island — approved durable publication present (inventory unchanged by this gate)
{
  const registry = createGdiPublishedInventoryRegistry();
  const published = registry.listPublished(SPICE_HPC);
  assert.ok(
    published.length >= 1,
    "Spice Island must retain approved durable publication rows for release gates"
  );
  const blocked = registry.assertCannotPromoteFromResearchAlone({
    approval: null,
  });
  assert.equal(blocked.blocked, true, "research-alone promotion must stay blocked");
}

// YOTEL Geneva Lake — NO approved customer-facing publication.
// Research-side opportunities.json (if present) is not published inventory.
// Absence of approved publication must be represented accurately; research must
// not leak into customer views via silent publication claims.
{
  const registry = createGdiPublishedInventoryRegistry();
  const published = registry.listPublished(YOTEL_HPC);
  assert.equal(
    published.length,
    0,
    "YOTEL must have zero approved durable GDI publications"
  );

  const promoteBlocked = registry.assertCannotPromoteFromResearchAlone({
    hotelId: YOTEL_HPC,
    approval: null,
  });
  assert.equal(
    promoteBlocked.blocked,
    true,
    "YOTEL research cannot promote without approval"
  );

  const opps = loadOpps(YOTEL_HPC);
  // Research artifact may exist in the repo tree; it is not authority for publication.
  // Do NOT apply assertCustomerPublicationCountsMatch to research bags:
  // customerVisible stamps on research ≠ approved customer publication.
  if (opps) {
    const facing = filterCustomerFacingOpportunities(opps, { nowDate: NOW });
    const counts = countCanonicalWatchPublication(opps, { nowDate: NOW });

    // Honest research report claims must equal gate-derived counts (no inflated Valid Watch).
    assert.equal(
      assertReportCountsMatchGates(opps, {
        claimedReady: counts.canonicalReady,
        claimedValidWatch: counts.canonicalValidWatch,
        nowDate: NOW,
      }).ok,
      true
    );
    assert.equal(
      assertReportCountsMatchGates(opps, {
        claimedReady: counts.canonicalReady,
        claimedValidWatch: counts.canonicalValidWatch + 1,
        nowDate: NOW,
      }).ok,
      false,
      "must not inflate Valid Watch above research gate counts"
    );

    // Non-ready research rows must not leak into the customer-facing filter.
    const asOfToday = "2026-11-07";
    const facingToday = filterCustomerFacingOpportunities(opps, {
      nowDate: asOfToday,
    });
    const facingTodayIds = new Set(facingToday.map((o) => o.id));
    for (const o of opps) {
      const ready = isGdiCustomerOpportunityReady(o, { nowDate: asOfToday });
      if (!ready.ok) {
        assert.equal(
          facingTodayIds.has(o.id),
          false,
          `non-ready research must not leak to customer facing: ${o.id}`
        );
      }
    }

    // Research-side facing (even when non-empty under a frozen NOW) is not publication.
    // Absence of approved publication remains authoritative.
    assert.equal(
      published.length,
      0,
      `research facing=${facing.length} must not invent approved publication`
    );
  } else {
    // Missing research file is also a valid representation of no customer inventory.
    assert.equal(published.length, 0);
  }
}

// Registry unavailable / missing pointer must not silently invent customer inventory
{
  const emptyRoot = path.join(ROOT, "tmp-gdi-empty-published-registry-test");
  fs.rmSync(emptyRoot, { recursive: true, force: true });
  fs.mkdirSync(path.join(emptyRoot, "registry"), { recursive: true });
  fs.writeFileSync(
    path.join(emptyRoot, "registry", "current-registry-v1.json"),
    JSON.stringify({ version: 1, hotels: {} }, null, 2)
  );
  const emptyReg = createGdiPublishedInventoryRegistry({ root: emptyRoot });
  assert.equal(emptyReg.listPublished(YOTEL_HPC).length, 0);
  assert.equal(emptyReg.listPublished(SPICE_HPC).length, 0);
  fs.rmSync(emptyRoot, { recursive: true, force: true });
}

console.log("test-gdi-customer-publication-invariants-v1: PASS");
