#!/usr/bin/env node
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  BETHESDA_HOTEL_ID,
  loadBethesda67SalesBaselineManifest,
  isBethesda67CustomerPublicationApproved,
  annotateInventoryPopulations,
  selectBethesdaSalesOpportunityInventory,
  classifyCompetitiveMarketIntelligence,
  CMI_CLASS,
  INVENTORY_POPULATION,
} from "../lib/group-demand-intelligence/bethesda-67-sales-baseline-cmi-v1.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import { filterSalespersonView } from "../lib/group-demand-intelligence/opportunity-factory.js";
import { mergeBethesdaInternalResearchOpportunities } from "../lib/group-demand-intelligence/research/bethesda-internal-research-union-v1.js";
import {
  opportunityMatchesTypeFilter,
  OPPORTUNITY_TYPE_FILTER,
} from "../lib/group-demand-intelligence/opportunity-type-filter-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

let passed = 0;
let failed = 0;
function check(name, fn) {
  try {
    fn();
    passed += 1;
    console.log("PASS", name);
  } catch (err) {
    failed += 1;
    console.error("FAIL", name, err.message);
  }
}

const manifest = loadBethesda67SalesBaselineManifest({ forceReload: true });
const doc = JSON.parse(
  fs.readFileSync(
    path.join(
      ROOT,
      `data/group-demand-intelligence/hotels/${BETHESDA_HOTEL_ID}/opportunities.json`
    ),
    "utf8"
  )
);
const union = mergeBethesdaInternalResearchOpportunities(
  BETHESDA_HOTEL_ID,
  doc.opportunities || []
);
const unionOpps = union.opportunities || [];

check("baseline_67_exact", () => {
  assert.equal(manifest.baselineIds.length, 67);
  assert.equal(manifest.publishedIds.length, 19);
  assert.equal(manifest.stagedIds.length, 48);
  assert.equal(new Set(manifest.baselineIds).size, 67);
});

check("customer_stays_at_19_without_approval", () => {
  assert.equal(isBethesda67CustomerPublicationApproved(), false);
  const facing = filterCustomerFacingOpportunities(
    filterSalespersonView(doc.opportunities || []),
    { hotelId: BETHESDA_HOTEL_ID }
  );
  assert.equal(facing.length, 19);
  for (const id of manifest.publishedIds) {
    assert.ok(facing.some((o) => o.id === id), `missing published ${id}`);
  }
  for (const id of manifest.stagedIds) {
    assert.ok(!facing.some((o) => o.id === id), `staged leaked ${id}`);
  }
});

check("founder_review_selects_67", () => {
  const sel = selectBethesdaSalesOpportunityInventory(unionOpps, {
    founderReview: true,
  });
  assert.equal(sel.selected.length, 67);
  assert.equal(sel.missingIds.length, 0);
});

check("approval_expands_customer_facing_to_67", () => {
  const facing = filterCustomerFacingOpportunities(unionOpps, {
    hotelId: BETHESDA_HOTEL_ID,
    forceApproved: true,
  });
  assert.equal(facing.length, 67);
});

check("exa_verified_cmi_seeds", () => {
  const nado = classifyCompetitiveMarketIntelligence({
    id: "gdi_opp_nado_ddaa_washcon_2027_overflow",
  });
  assert.equal(nado.class, CMI_CLASS.COMPETITIVE_MARKET_INTELLIGENCE);
  assert.equal(nado.verified, true);
  const weak = classifyCompetitiveMarketIntelligence({
    id: "gdi_opp_ahima_advocacy_2027",
    competitorHotel: "Hyatt Regency Bethesda",
  });
  assert.equal(weak.class, CMI_CLASS.NEEDS_FURTHER_VERIFICATION);
});

check("population_filter_sales_vs_cmi", () => {
  const annotated = annotateInventoryPopulations(unionOpps, {
    hotelId: BETHESDA_HOTEL_ID,
  });
  const sales = annotated.filter((o) =>
    opportunityMatchesTypeFilter(
      o,
      OPPORTUNITY_TYPE_FILTER.SALES_OPPORTUNITIES
    )
  );
  const cmi = annotated.filter((o) =>
    opportunityMatchesTypeFilter(
      o,
      OPPORTUNITY_TYPE_FILTER.COMPETITIVE_MARKET_INTELLIGENCE
    )
  );
  assert.ok(sales.length >= 19);
  assert.ok(cmi.length >= 3);
  const verified = cmi.filter((o) => o.cmiVerified === true);
  assert.ok(verified.length >= 3, "at least 3 verified CMI");
  assert.ok(
    cmi.every(
      (o) =>
        o.competitiveStatus === "VERIFIED" ||
        o.competitiveStatus === "NEEDS_VERIFICATION" ||
        o.competitiveStatus === "HISTORICAL" ||
        o.cmiVerified === true
    ),
    "Competitive population is status-labeled"
  );
  // CMI does not invent Sales Ready
  for (const o of cmi) {
    if (!manifest.baselineIds.includes(o.id)) {
      assert.ok(
        !(o.inventoryPopulations || []).includes(
          INVENTORY_POPULATION.SALES_OPPORTUNITIES
        ) || o.salesBaselineMembership === "OUTSIDE_67_BASELINE"
      );
    }
  }
});

check("no_cross_hotel_baseline_replication", () => {
  const other = annotateInventoryPopulations(
    [{ id: "gdi_opp_other_hotel_1", title: "Other" }],
    { hotelId: "recOTHERHOTEL0001" }
  );
  assert.equal(other[0].salesBaselineMembership, "OUTSIDE_67_BASELINE");
});

console.log(
  JSON.stringify({ suite: "gdi-bethesda-67-sales-baseline-cmi-v1", passed, failed }, null, 2)
);
process.exit(failed ? 1 : 0);
