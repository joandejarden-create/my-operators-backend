#!/usr/bin/env node
/**
 * Regression: authorized Bethesda sales-team Opportunities must return 67,
 * while published/customer audience stays at 19. Proves the founder-visible
 * local 19-card bug (customer-facing filter applied before sales inventory).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  BETHESDA_HOTEL_ID,
  BETHESDA_SALES_TEAM_LIST_SCOPE,
  BETHESDA_PUBLISHED_CUSTOMER_LIST_SCOPE,
  GDI_INVENTORY_AUDIENCE,
  loadBethesda67SalesBaselineManifest,
  selectBethesdaSalesOpportunityInventory,
  resolveGdiInventoryAudience,
  annotateInventoryPopulations,
  buildInventoryPopulationCounts,
} from "../lib/group-demand-intelligence/bethesda-67-sales-baseline-cmi-v1.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import { mergeBethesdaInternalResearchOpportunities } from "../lib/group-demand-intelligence/research/bethesda-internal-research-union-v1.js";
import {
  filterOpportunitiesByCommercialSurface,
  GDI_LIST_SURFACE_VIEW,
} from "../lib/group-demand-intelligence/gdi-commercial-surface-disposition-v1.js";
import { applyBethesdaCommercialEventFamilyPresentation } from "../lib/group-demand-intelligence/research/bethesda-88-commercial-event-family-v1.js";
import { opportunityMatchesTypeFilter, OPPORTUNITY_TYPE_FILTER } from "../lib/group-demand-intelligence/opportunity-type-filter-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
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
const family = applyBethesdaCommercialEventFamilyPresentation(
  union.opportunities,
  { hideLinkedFromList: true }
);
const surface = filterOpportunitiesByCommercialSurface(
  family.opportunities,
  GDI_LIST_SURFACE_VIEW.COMMERCIAL
);

check("root_cause_customer_facing_collapses_67_to_19", () => {
  assert.equal(surface.length >= 67, true);
  const baselineOnSurface = manifest.baselineIds.filter((id) =>
    surface.some((o) => o.id === id)
  );
  assert.equal(baselineOnSurface.length, 67);
  const facing = filterCustomerFacingOpportunities(surface, {
    hotelId: BETHESDA_HOTEL_ID,
  });
  assert.equal(facing.length, 19, "customer filter is the 67→19 collapse point");
});

check("sales_team_select_returns_exact_67", () => {
  const sel = selectBethesdaSalesOpportunityInventory(surface, {
    salesTeamInventory: true,
    founderReview: true,
  });
  assert.equal(sel.selected.length, 67);
  assert.equal(sel.missingIds.length, 0);
  assert.deepEqual(
    [...sel.selected.map((o) => o.id)].sort(),
    [...manifest.baselineIds].sort()
  );
});

check("audience_resolver_local_capability_pilot_is_sales_team", () => {
  const a = resolveGdiInventoryAudience(
    { gdiPilotUnauthRead: true, query: {} },
    {
      hotelId: BETHESDA_HOTEL_ID,
      env: { GDI_LOCAL_SALES_TEAM_INVENTORY_CAPABILITY: "1", NODE_ENV: "test" },
    }
  );
  assert.equal(a, GDI_INVENTORY_AUDIENCE.SALES_TEAM_INVENTORY);
});

check("audience_resolver_admin_dealality_user_is_sales_team", () => {
  const a = resolveGdiInventoryAudience(
    {
      dealalityUser: { email: "sales@dealality.test", isAdmin: true },
      query: {},
    },
    { hotelId: BETHESDA_HOTEL_ID }
  );
  assert.equal(a, GDI_INVENTORY_AUDIENCE.SALES_TEAM_INVENTORY);
});

check("audience_resolver_generic_user_fail_closed_published", () => {
  const a = resolveGdiInventoryAudience(
    { dealalityUser: { email: "sales@dealality.test" }, query: {} },
    { hotelId: BETHESDA_HOTEL_ID }
  );
  assert.equal(a, GDI_INVENTORY_AUDIENCE.PUBLISHED_CUSTOMER_FEED);
});

check("audience_resolver_explicit_published_stays_19", () => {
  const a = resolveGdiInventoryAudience(
    {
      gdiPilotUnauthRead: true,
      query: { inventoryAudience: "published" },
    },
    { hotelId: BETHESDA_HOTEL_ID }
  );
  assert.equal(a, GDI_INVENTORY_AUDIENCE.PUBLISHED_CUSTOMER_FEED);
});

check("audience_resolver_share_surface_forced_published", () => {
  const a = resolveGdiInventoryAudience(
    { gdiPilotUnauthRead: true, query: {} },
    { hotelId: BETHESDA_HOTEL_ID, shareSurface: true }
  );
  assert.equal(a, GDI_INVENTORY_AUDIENCE.PUBLISHED_CUSTOMER_FEED);
});

check("population_counts_67_plus_verified_cmi_unique_67", () => {
  const annotated = annotateInventoryPopulations(
    selectBethesdaSalesOpportunityInventory(surface, {
      salesTeamInventory: true,
    }).selected,
    { hotelId: BETHESDA_HOTEL_ID }
  );
  const counts = buildInventoryPopulationCounts(annotated);
  assert.equal(counts.salesOpportunities, 67);
  // Evidence-backed CMI ledger: 4 verified (not legacy seed-as-proof 7).
  assert.equal(counts.competitiveMarketIntelligence, 4);
  assert.equal(counts.allCommercialIntelligence, 67);
  const sales = annotated.filter((o) =>
    opportunityMatchesTypeFilter(o, OPPORTUNITY_TYPE_FILTER.SALES_OPPORTUNITIES)
  );
  const cmi = annotated.filter((o) =>
    opportunityMatchesTypeFilter(
      o,
      OPPORTUNITY_TYPE_FILTER.COMPETITIVE_MARKET_INTELLIGENCE
    )
  );
  assert.equal(sales.length, 67);
  assert.equal(cmi.length, 4);
  assert.ok(cmi.every((o) => o.cmiVerified === true));
});

check("api_source_wires_sales_team_scope", () => {
  const api = fs.readFileSync(
    path.join(ROOT, "api/group-demand-intelligence.js"),
    "utf8"
  );
  assert.match(api, /selectBethesdaSalesOpportunityInventory/);
  assert.match(api, /resolveGdiInventoryAudience/);
  assert.match(api, /BETHESDA_SALES_TEAM_LIST_SCOPE/);
  assert.match(api, /BETHESDA_PUBLISHED_CUSTOMER_LIST_SCOPE/);
  assert.match(
    api,
    /Do NOT apply public customer-publication eligibility/
  );
  assert.equal(BETHESDA_SALES_TEAM_LIST_SCOPE, "BETHESDA_SALES_TEAM_INVENTORY_67");
  assert.equal(
    BETHESDA_PUBLISHED_CUSTOMER_LIST_SCOPE,
    "BETHESDA_PUBLISHED_CUSTOMER_FEED_19"
  );
});

check("frontend_accepts_sales_team_scope", () => {
  const app = fs.readFileSync(
    path.join(ROOT, "public/js/group-demand-intelligence/app.js"),
    "utf8"
  );
  assert.match(app, /BETHESDA_SALES_TEAM_INVENTORY_67/);
  assert.match(app, /inventoryAudience/);
});

console.log(
  JSON.stringify(
    {
      suite: "gdi-bethesda-67-sales-team-inventory-display-v1",
      passed,
      failed,
      scopes: {
        salesTeam: BETHESDA_SALES_TEAM_LIST_SCOPE,
        published: BETHESDA_PUBLISHED_CUSTOMER_LIST_SCOPE,
      },
    },
    null,
    2
  )
);
process.exit(failed ? 1 : 0);
