#!/usr/bin/env node
/**
 * Bethesda share inventory — must not inflate past published-19 without
 * founder approval, and must annotate Market Scope populations.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { selectBethesdaExternalShareOpportunities } from "../lib/group-demand-intelligence/bethesda-share-inventory-v1.js";
import {
  BETHESDA_HOTEL_ID,
  BETHESDA_PUBLISHED_CUSTOMER_LIST_SCOPE,
  BETHESDA_SALES_TEAM_LIST_SCOPE,
  loadBethesda67SalesBaselineManifest,
} from "../lib/group-demand-intelligence/bethesda-67-sales-baseline-cmi-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
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

const manifest = loadBethesda67SalesBaselineManifest({ forceReload: true });
const doc = await loadOpportunitiesCanonical(BETHESDA_HOTEL_ID, {
  bypassCache: true,
});

check("unapproved_share_selects_exact_published_19", () => {
  const sel = selectBethesdaExternalShareOpportunities(doc.opportunities || [], {
    hotelId: BETHESDA_HOTEL_ID,
    forceApproved: false,
  });
  assert.equal(sel.approved, false);
  assert.equal(sel.listScope, BETHESDA_PUBLISHED_CUSTOMER_LIST_SCOPE);
  assert.equal(sel.opportunities.length, 19);
  assert.equal(sel.selection.missingIds.length, 0);
  assert.deepEqual(
    [...sel.opportunities.map((o) => o.id)].sort(),
    [...manifest.publishedIds].sort()
  );
});

check("approved_share_selects_exact_baseline_67", () => {
  const sel = selectBethesdaExternalShareOpportunities(doc.opportunities || [], {
    hotelId: BETHESDA_HOTEL_ID,
    forceApproved: true,
  });
  assert.equal(sel.approved, true);
  assert.equal(sel.listScope, BETHESDA_SALES_TEAM_LIST_SCOPE);
  assert.equal(sel.opportunities.length, 67);
  assert.equal(sel.selection.missingIds.length, 0);
  assert.ok(sel.inventoryPopulationCounts.allCommercialIntelligence === 67);
  assert.ok(sel.inventoryPopulationCounts.salesOpportunities >= 50);
  assert.ok(
    sel.inventoryPopulationCounts.competitiveMarketIntelligence >= 1,
    "approved share must expose competitive population for Market Scope"
  );
});

check("share_rows_carry_inventory_populations", () => {
  const sel = selectBethesdaExternalShareOpportunities(doc.opportunities || [], {
    hotelId: BETHESDA_HOTEL_ID,
    forceApproved: false,
  });
  const withPop = sel.opportunities.filter(
    (o) =>
      Array.isArray(o.inventoryPopulations) ||
      o.inventoryPopulation ||
      o.competitiveStatus
  );
  assert.ok(
    withPop.length >= 10,
    `expected annotated populations, got ${withPop.length}`
  );
});

check("api_share_route_wires_helper", () => {
  const api = fs.readFileSync(
    path.join(ROOT, "api/group-demand-intelligence.js"),
    "utf8"
  );
  assert.match(api, /selectBethesdaExternalShareOpportunities/);
  assert.match(api, /inventoryPopulationCounts/);
});

check("does_not_use_live_customerFacing_alone_for_bethesda_share", () => {
  const api = fs.readFileSync(
    path.join(ROOT, "api/group-demand-intelligence.js"),
    "utf8"
  );
  // Within getGdiShareOpportunities Bethesda branch must prefer helper.
  const idx = api.indexOf("export async function getGdiShareOpportunities");
  const slice = api.slice(idx, idx + 4500);
  assert.match(slice, /selectBethesdaExternalShareOpportunities/);
  assert.match(slice, /hotelId === BETHESDA_HOTEL_ID/);
});

if (failed) {
  console.error(`\n${failed} check(s) failed`);
  process.exit(1);
}
console.log(
  JSON.stringify({
    suite: "gdi-bethesda-share-inventory-v1",
    passed: true,
    published19: 19,
    approved67: 67,
  })
);
