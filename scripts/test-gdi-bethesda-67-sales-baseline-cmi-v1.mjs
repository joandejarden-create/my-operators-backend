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
import { isCustomerSurfaceActiveEligible } from "../lib/group-demand-intelligence/customer-surface-revalidation-v1.js";
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
  // Explicitly deny approval for this gate — a real founder receipt may exist after Decision A.
  assert.equal(
    isBethesda67CustomerPublicationApproved({
      forceApproved: false,
      receiptPath: path.join(
        ROOT,
        "data/group-demand-intelligence/hotels/recLuxvwwxID7U2B8/__no_such_approval__.json"
      ),
    }),
    false
  );
  // Allowlist size remains 19 (publication authorization). Customer list count is
  // the surface-active subset of that allowlist — past-closed published events
  // (e.g. Bethesda Premier Cup after eventEndDate) correctly drop from facing.
  assert.equal(manifest.publishedIds.length, 19, "published allowlist stays 19");
  const salesperson = filterSalespersonView(doc.opportunities || []);
  const byId = new Map(salesperson.map((o) => [o.id, o]));
  const publishedSurfaceActive = manifest.publishedIds.filter((id) => {
    const row = byId.get(id);
    return row && isCustomerSurfaceActiveEligible(row, { hotelId: BETHESDA_HOTEL_ID });
  });
  const facing = filterCustomerFacingOpportunities(salesperson, {
    hotelId: BETHESDA_HOTEL_ID,
    forceApproved: false,
  });
  assert.equal(
    facing.length,
    publishedSurfaceActive.length,
    `customer facing must equal surface-active published subset (allowlist=19, active=${publishedSurfaceActive.length}, facing=${facing.length})`
  );
  for (const id of publishedSurfaceActive) {
    assert.ok(facing.some((o) => o.id === id), `missing surface-active published ${id}`);
  }
  for (const id of manifest.stagedIds) {
    assert.ok(!facing.some((o) => o.id === id), `staged leaked ${id}`);
  }
  // Document known temporal exclusion when present (do not weaken surface gates).
  const premier = byId.get("gdi_opp_bethesda_premier_cup_2026");
  if (premier && !isCustomerSurfaceActiveEligible(premier, { hotelId: BETHESDA_HOTEL_ID })) {
    assert.ok(
      !facing.some((o) => o.id === "gdi_opp_bethesda_premier_cup_2026"),
      "past-closed Premier Cup must not appear on customer facing"
    );
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

check("hotel_block_synonym_verifies_edition_specific_cmi", () => {
  const [row] = annotateInventoryPopulations(
    [
      {
        id: "probe_aha_hotel_block_synonym",
        title: "AHA Annual Membership Meeting 2027",
        eventStartDate: "2027-04-18",
        competitorHotel: "Marriott Marquis Washington, DC",
        competitorVenueStatus: "CONFIRMED_EVENT_VENUE",
        competitorVenueConfirmed: true,
        competitorBookingEvidenceClass: "VERIFIED_BOOKING",
        competitorBookingClaim:
          "AHA hotel block expires March 17, 2027 at Marriott Marquis Washington, D.C.",
        evidence: [
          {
            url: "https://annualmeeting.aha.org/hotel-travel-0",
            statement: "hotel block 2027",
            accepted: true,
          },
        ],
      },
    ],
    { hotelId: BETHESDA_HOTEL_ID }
  );
  assert.equal(row.competitiveStatus, "VERIFIED");
  assert.equal(row.competitiveMarketScopeEligible, true);
  assert.ok(
    (row.inventoryPopulations || []).includes(
      INVENTORY_POPULATION.COMPETITIVE_MARKET_INTELLIGENCE
    )
  );
  assert.equal(row.hotelMarketScopeEligible, false);
});

console.log(
  JSON.stringify({ suite: "gdi-bethesda-67-sales-baseline-cmi-v1", passed, failed }, null, 2)
);
process.exit(failed ? 1 : 0);
