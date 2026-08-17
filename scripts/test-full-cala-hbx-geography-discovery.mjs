/**
 * Unit tests — HBX geography discovery wave gates (no live HBX/Airtable writes).
 */
import test from "node:test";
import assert from "node:assert/strict";
import {
  listDealalityCalaGeographies,
  resolveDealalityCalaGeography,
} from "../lib/research-engine-v2/dealality-cala-geography-registry-v1.js";
import {
  HBX_ALTERNATE_QUERY_CODES,
  HBX_DISCOVERY_STATUS,
  classifyHbxPullOutcome,
  initOrLoadLedger,
  repairFalseZeroResultLedgerEntries,
} from "../lib/research-engine-v2/full-cala-hbx-geography-discovery-wave-v1.js";

test("Bermuda is IN_SCOPE after founder decision", () => {
  const bermuda = listDealalityCalaGeographies({ includeScopeReview: true }).find(
    (g) => g.geography_id === "bermuda"
  );
  assert.ok(bermuda);
  assert.equal(bermuda.scope, "in_scope");
  assert.equal(bermuda.region, "Caribbean");
});

test("Wave1 geographies recognized COMPLETE in ledger seed", () => {
  const ledger = initOrLoadLedger();
  for (const name of [
    "Mexico",
    "Dominican Republic",
    "Colombia",
    "Costa Rica",
    "Panama",
  ]) {
    const g = resolveDealalityCalaGeography(name);
    assert.ok(g, name);
    const e = ledger.geographies[g.geography_id];
    assert.equal(e.hbx_status, HBX_DISCOVERY_STATUS.COMPLETE, name);
    assert.equal(e.wave1_prior_complete, true, name);
  }
});

test("403/429 never classify as COMPLETE_ZERO_RESULTS", () => {
  assert.equal(
    classifyHbxPullOutcome({ hotels: [], error: { status: 403 } }).status,
    HBX_DISCOVERY_STATUS.FAILED_REQUIRES_REVIEW
  );
  assert.equal(
    classifyHbxPullOutcome({
      hotels: [],
      error: { status: 403, message: "Quota exceeded" },
    }).status,
    HBX_DISCOVERY_STATUS.FAILED_RETRYABLE
  );
  assert.equal(
    classifyHbxPullOutcome({ hotels: [], error: { status: 429 } }).status,
    HBX_DISCOVERY_STATUS.FAILED_RETRYABLE
  );
  assert.equal(
    classifyHbxPullOutcome({ hotels: [], error: null }).status,
    HBX_DISCOVERY_STATUS.COMPLETE_ZERO_RESULTS
  );
  assert.equal(
    classifyHbxPullOutcome({ hotels: [{ x: 1 }], error: { status: 403 } }).status,
    HBX_DISCOVERY_STATUS.COMPLETE
  );
});

test("repairFalseZeroResultLedgerEntries reclassifies auth failures", () => {
  const ledger = initOrLoadLedger();
  repairFalseZeroResultLedgerEntries(ledger);
  const belize = ledger.geographies.belize;
  if (belize?.errors?.[0]?.status === 403) {
    assert.equal(
      belize.hbx_status,
      HBX_DISCOVERY_STATUS.FAILED_REQUIRES_REVIEW
    );
  }
  const jamaica = resolveDealalityCalaGeography("Jamaica");
  assert.ok(jamaica);
  assert.notEqual(
    ledger.geographies[jamaica.geography_id].hbx_status,
    HBX_DISCOVERY_STATUS.COMPLETE
  );
  assert.ok(HBX_ALTERNATE_QUERY_CODES.puerto_rico.includes("PR"));
  assert.ok(HBX_ALTERNATE_QUERY_CODES.puerto_rico.includes("US"));
  assert.ok(HBX_ALTERNATE_QUERY_CODES.bonaire.includes("BQ"));
});

test("canonical in-scope count includes Bermuda and excludes scope_review-only", () => {
  const inScope = listDealalityCalaGeographies({ includeScopeReview: false });
  assert.ok(inScope.some((g) => g.geography_id === "bermuda"));
  assert.ok(inScope.length >= 52);
});
