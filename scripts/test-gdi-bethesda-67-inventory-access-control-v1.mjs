#!/usr/bin/env node
/**
 * Negative access-control certification for Bethesda sales-team inventory (PR #220).
 * List / detail / CSV audience parity — no opportunity data mutation.
 */

import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  BETHESDA_HOTEL_ID,
  GDI_INVENTORY_AUDIENCE,
  resolveGdiInventoryAudience,
  assertHotelScopedSalesTeamInventoryAccess,
  assertOpportunityReadableForInventoryAudience,
  resolveSalesTeamInventoryHotelAllowlist,
  loadBethesda67SalesBaselineManifest,
  selectBethesdaSalesOpportunityInventory,
  annotateInventoryPopulations,
  buildInventoryPopulationCounts,
} from "../lib/group-demand-intelligence/bethesda-67-sales-baseline-cmi-v1.js";
import { mergeBethesdaInternalResearchOpportunities } from "../lib/group-demand-intelligence/research/bethesda-internal-research-union-v1.js";
import { applyBethesdaCommercialEventFamilyPresentation } from "../lib/group-demand-intelligence/research/bethesda-88-commercial-event-family-v1.js";
import {
  filterOpportunitiesByCommercialSurface,
  GDI_LIST_SURFACE_VIEW,
} from "../lib/group-demand-intelligence/gdi-commercial-surface-disposition-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const WYNDHAM_HOTEL_ID = "recnPB3Q02NO8Ma1o";

let failed = 0;
function check(name, fn) {
  try {
    fn();
    console.log(`PASS ${name}`);
  } catch (err) {
    failed += 1;
    console.error(`FAIL ${name}: ${err.message}`);
  }
}

const manifest = loadBethesda67SalesBaselineManifest({ forceReload: true });
const unpublishedId = (manifest.stagedIds || manifest.baselineIds || []).find(
  (id) => !(manifest.publishedIds || []).includes(id)
);
const publishedId = (manifest.publishedIds || [])[0];
assert.ok(unpublishedId, "staged/unpublished id required");
assert.ok(publishedId, "published id required");

const fixturePath = path.join(
  ROOT,
  "data/group-demand-intelligence/hotels",
  BETHESDA_HOTEL_ID,
  "opportunities.json"
);
let surface = [];
if (fs.existsSync(fixturePath)) {
  const doc = JSON.parse(fs.readFileSync(fixturePath, "utf8"));
  const union = mergeBethesdaInternalResearchOpportunities(
    BETHESDA_HOTEL_ID,
    doc.opportunities || []
  );
  const family = applyBethesdaCommercialEventFamilyPresentation(
    union.opportunities,
    { hideLinkedFromList: true }
  );
  surface = filterOpportunitiesByCommercialSurface(
    family.opportunities,
    GDI_LIST_SURFACE_VIEW.COMMERCIAL
  );
}

check("allowlist_defaults_to_bethesda_only", () => {
  const set = resolveSalesTeamInventoryHotelAllowlist({});
  assert.equal(set.size, 1);
  assert.ok(set.has(BETHESDA_HOTEL_ID));
  assert.equal(set.has(WYNDHAM_HOTEL_ID), false);
});

check("pilot_unauth_without_hotel_allowlist_denied", () => {
  const access = assertHotelScopedSalesTeamInventoryAccess(
    { gdiPilotUnauthRead: true },
    {
      hotelId: WYNDHAM_HOTEL_ID,
      env: { GDI_SALES_TEAM_INVENTORY_HOTEL_IDS: BETHESDA_HOTEL_ID },
    }
  );
  assert.equal(access.ok, false);
  assert.match(access.reason, /allowlist/i);
});

check("pilot_unauth_bethesda_allowlisted_grants_sales_team", () => {
  const a = resolveGdiInventoryAudience(
    { gdiPilotUnauthRead: true, query: {} },
    { hotelId: BETHESDA_HOTEL_ID }
  );
  assert.equal(a, GDI_INVENTORY_AUDIENCE.SALES_TEAM_INVENTORY);
  const access = assertHotelScopedSalesTeamInventoryAccess(
    { gdiPilotUnauthRead: true },
    { hotelId: BETHESDA_HOTEL_ID }
  );
  assert.equal(access.ok, true);
  assert.equal(access.via, "gdiPilotUnauthRead");
});

check("anonymous_no_pilot_stays_published", () => {
  const a = resolveGdiInventoryAudience(
    { query: {} },
    { hotelId: BETHESDA_HOTEL_ID }
  );
  assert.equal(a, GDI_INVENTORY_AUDIENCE.PUBLISHED_CUSTOMER_FEED);
});

check("share_surface_forces_published_even_with_pilot", () => {
  const a = resolveGdiInventoryAudience(
    { gdiPilotUnauthRead: true, gdiExternalShare: true, query: {} },
    { hotelId: BETHESDA_HOTEL_ID }
  );
  assert.equal(a, GDI_INVENTORY_AUDIENCE.PUBLISHED_CUSTOMER_FEED);
});

check("cross_hotel_scoped_user_cannot_expand_bethesda", () => {
  const req = {
    dealalityUser: {
      email: "wyndham-only@dealality.test",
      allowedHotelIds: [WYNDHAM_HOTEL_ID],
    },
    query: {},
  };
  const access = assertHotelScopedSalesTeamInventoryAccess(req, {
    hotelId: BETHESDA_HOTEL_ID,
  });
  assert.equal(access.ok, false);
  assert.match(access.reason, /scope_excludes/i);
  const a = resolveGdiInventoryAudience(req, { hotelId: BETHESDA_HOTEL_ID });
  assert.equal(a, GDI_INVENTORY_AUDIENCE.PUBLISHED_CUSTOMER_FEED);
});

check("bethesda_scoped_user_can_expand_bethesda", () => {
  const req = {
    dealalityUser: {
      email: "bethesda@dealality.test",
      gdiHotelIds: [BETHESDA_HOTEL_ID],
    },
    query: {},
  };
  const a = resolveGdiInventoryAudience(req, { hotelId: BETHESDA_HOTEL_ID });
  assert.equal(a, GDI_INVENTORY_AUDIENCE.SALES_TEAM_INVENTORY);
});

check("detail_unpublished_denied_under_published_audience", () => {
  const req = { query: { inventoryAudience: "published" }, gdiPilotUnauthRead: true };
  const gate = assertOpportunityReadableForInventoryAudience(
    req,
    { id: unpublishedId, hotelId: BETHESDA_HOTEL_ID },
    { hotelId: BETHESDA_HOTEL_ID, opportunityId: unpublishedId }
  );
  assert.equal(gate.ok, false);
  assert.match(gate.reason, /unpublished_baseline/i);
});

check("detail_unpublished_denied_for_cross_hotel_user", () => {
  const req = {
    dealalityUser: { allowedHotelIds: [WYNDHAM_HOTEL_ID] },
    query: {},
  };
  const gate = assertOpportunityReadableForInventoryAudience(
    req,
    { id: unpublishedId, hotelId: BETHESDA_HOTEL_ID },
    { hotelId: BETHESDA_HOTEL_ID, opportunityId: unpublishedId }
  );
  assert.equal(gate.ok, false);
});

check("detail_published_allowed_under_published_audience", () => {
  const req = { query: { inventoryAudience: "published" } };
  const gate = assertOpportunityReadableForInventoryAudience(
    req,
    { id: publishedId, hotelId: BETHESDA_HOTEL_ID },
    { hotelId: BETHESDA_HOTEL_ID, opportunityId: publishedId }
  );
  assert.equal(gate.ok, true);
});

check("detail_unpublished_allowed_for_pilot_bethesda", () => {
  const req = { gdiPilotUnauthRead: true, query: {} };
  const gate = assertOpportunityReadableForInventoryAudience(
    req,
    { id: unpublishedId, hotelId: BETHESDA_HOTEL_ID },
    { hotelId: BETHESDA_HOTEL_ID, opportunityId: unpublishedId }
  );
  assert.equal(gate.ok, true);
});

check("list_select_published_is_19_sales_is_67", () => {
  if (!surface.length) {
    console.log("SKIP list_select (no local opportunities store)");
    return;
  }
  const pub = selectBethesdaSalesOpportunityInventory(surface, {
    inventoryAudience: GDI_INVENTORY_AUDIENCE.PUBLISHED_CUSTOMER_FEED,
  });
  const sales = selectBethesdaSalesOpportunityInventory(surface, {
    salesTeamInventory: true,
  });
  assert.equal(pub.selected.length, 19);
  assert.equal(sales.selected.length, 67);
});

check("preserved_67_plus_7_cmi_under_sales_audience", () => {
  if (!surface.length) {
    console.log("SKIP preserved_67_plus_7 (no local opportunities store)");
    return;
  }
  const selected = selectBethesdaSalesOpportunityInventory(surface, {
    salesTeamInventory: true,
  }).selected;
  const annotated = annotateInventoryPopulations(selected, {
    hotelId: BETHESDA_HOTEL_ID,
  });
  const counts = buildInventoryPopulationCounts(annotated);
  assert.equal(counts.salesOpportunities, 67);
  assert.equal(counts.competitiveMarketIntelligence, 7);
});

check("api_detail_wires_inventory_audience_guard", () => {
  const api = fs.readFileSync(
    path.join(ROOT, "api/group-demand-intelligence.js"),
    "utf8"
  );
  assert.match(api, /assertOpportunityReadableForInventoryAudience/);
  assert.match(api, /assertHotelScopedSalesTeamInventoryAccess/);
  assert.match(api, /resolveGdiInventoryAudience/);
});

check("middleware_trace_gdiPilotReadAuth_sets_flag", () => {
  const mw = fs.readFileSync(
    path.join(ROOT, "middleware/gdiPilotReadAuth.js"),
    "utf8"
  );
  assert.match(mw, /gdiPilotUnauthRead\s*=\s*true/);
  assert.match(mw, /memberstackAuth/);
  assert.match(mw, /requireDealalityUser/);
});

check("middleware_trace_requireDealalityUser_sets_user", () => {
  const mw = fs.readFileSync(
    path.join(ROOT, "middleware/requireDealalityUser.js"),
    "utf8"
  );
  assert.match(mw, /req\.dealalityUser\s*=/);
});

console.log(
  JSON.stringify(
    {
      suite: "gdi-bethesda-67-inventory-access-control-v1",
      failed,
      unpublishedId,
      publishedId,
      bethesdaHotelId: BETHESDA_HOTEL_ID,
    },
    null,
    2
  )
);
process.exit(failed ? 1 : 0);
