#!/usr/bin/env node
/**
 * Regression: frozen Bethesda commercial 67 must be fully accounted for;
 * previously approved customer-facing rows must not vanish; research/watchlist
 * must not re-enter the customer feed without approval.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { loadOpportunitiesCanonical } from "../lib/group-demand-intelligence/opportunity-persistence.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";
import { applyApprovedBethesdaCustomerProjection } from "../lib/group-demand-intelligence/workflow/gdi-bethesda-publication-v1.js";
import {
  filterOpportunitiesByCommercialSurface,
  GDI_LIST_SURFACE_VIEW,
} from "../lib/group-demand-intelligence/gdi-commercial-surface-disposition-v1.js";
import { mergeBethesdaInternalResearchOpportunities } from "../lib/group-demand-intelligence/research/bethesda-internal-research-union-v1.js";
import {
  isBlockedFromQualifiedCustomerOpportunityFeed,
  hasApprovedCustomerCommercialClass,
} from "../lib/group-demand-intelligence/gdi-qualified-customer-opportunity-feed-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const HOTEL_ID = "recLuxvwwxID7U2B8";
const FIXTURE = path.join(
  ROOT,
  "fixtures/gdi-bethesda-frozen-commercial-67-customer-feed-v1.json"
);
const SCORECARD = path.join(
  ROOT,
  "reports/gdi-bethesda-68-hotel-sales-commercial-acceptance-v1/01-commercial-sales-scorecard.json"
);

const fixture = JSON.parse(fs.readFileSync(FIXTURE, "utf8"));
const score = JSON.parse(fs.readFileSync(SCORECARD, "utf8"));

assert.equal(fixture.frozenOpportunityIds.length, 67);
assert.equal(score.rows.length, 67);
assert.equal(fixture.expectedMissingCount, 48);

const scoreIds = score.rows.map((r) => r.id).sort();
assert.deepEqual(
  [...fixture.frozenOpportunityIds].sort(),
  scoreIds,
  "fixture must match frozen scorecard IDs"
);

const doc = await loadOpportunitiesCanonical(HOTEL_ID);
const union = mergeBethesdaInternalResearchOpportunities(
  HOTEL_ID,
  doc.opportunities || []
);
const commercial = filterOpportunitiesByCommercialSurface(
  union.opportunities,
  GDI_LIST_SURFACE_VIEW.COMMERCIAL
);
const facing = applyApprovedBethesdaCustomerProjection(
  filterCustomerFacingOpportunities(commercial, { hotelId: HOTEL_ID })
);
const facingIds = facing.map((o) => o.id).sort();
const frozenSet = new Set(fixture.frozenOpportunityIds);

// Every facing row from this hotel feed path that is in the frozen cohort is accounted
const facingFromFrozen = facingIds.filter((id) => frozenSet.has(id));
assert.equal(
  facingFromFrozen.length,
  fixture.expectedCustomerFacingFromFrozen.length,
  "customer-facing count from frozen 67 drifted"
);
assert.deepEqual(
  facingFromFrozen,
  [...fixture.expectedCustomerFacingFromFrozen].sort()
);

// No WATCHLIST on customer feed
assert.equal(
  facing.filter((o) => String(o.priority).toUpperCase() === "WATCHLIST").length,
  0
);

// Missing count from frozen
const missing = fixture.frozenOpportunityIds.filter((id) => !facingIds.includes(id));
assert.equal(missing.length, 48);

// Approved commercial class rows in frozen that are facing must stay facing
for (const id of fixture.expectedCustomerFacingFromFrozen) {
  const opp = union.opportunities.find((o) => o.id === id);
  assert.ok(opp, `missing union row ${id}`);
  assert.equal(
    isBlockedFromQualifiedCustomerOpportunityFeed(opp),
    false,
    `${id} unexpectedly blocked from qualified feed`
  );
}

// Synthetic: inferred demand without RFP must not alone equal EXCLUDE via approved-class helper
assert.equal(
  hasApprovedCustomerCommercialClass({
    demandConfidence: "RELIABLE_INFERRED_DEMAND",
    priority: "MEDIUM_PRIORITY",
    // no maturity / registry
  }),
  false,
  "inferred demand alone is not customer approval"
);
assert.equal(
  hasApprovedCustomerCommercialClass({
    commercialMaturity: "ACCOUNT_DEVELOPMENT",
    customerVisible: true,
  }),
  true
);

// Publication review ready without approval stays blocked
assert.equal(
  isBlockedFromQualifiedCustomerOpportunityFeed({
    publicationReviewReady: true,
    customerPublicationReady: false,
    priority: "MEDIUM_PRIORITY",
  }),
  true
);

console.log(
  JSON.stringify(
    {
      ok: true,
      frozen: 67,
      facingFromFrozen: facingFromFrozen.length,
      missing: missing.length,
      facingTotal: facingIds.length,
    },
    null,
    2
  )
);
console.log("PASS gdi bethesda 67 customer-feed reconciliation v1");
