#!/usr/bin/env node
import assert from "node:assert/strict";
import {
  isBlockedFromQualifiedCustomerOpportunityFeed,
  filterQualifiedCustomerOpportunityFeed,
  hasApprovedCustomerCommercialClass,
} from "../lib/group-demand-intelligence/gdi-qualified-customer-opportunity-feed-v1.js";
import { filterCustomerFacingOpportunities } from "../lib/group-demand-intelligence/customer-visibility.js";

assert.equal(
  isBlockedFromQualifiedCustomerOpportunityFeed({
    id: "w1",
    priority: "WATCHLIST",
    customerVisible: true,
    title: "Early watch",
  }),
  true
);

assert.equal(
  isBlockedFromQualifiedCustomerOpportunityFeed({
    id: "rc1",
    priority: "MEDIUM_PRIORITY",
    commercialMaturity: "RESEARCH_CONTINUATION",
    customerVisible: true,
  }),
  true
);

assert.equal(
  isBlockedFromQualifiedCustomerOpportunityFeed({
    id: "shadow1",
    liveCampaignV1Shadow: true,
    localShadowOnly: true,
    customerVisible: true,
  }),
  true
);

assert.equal(
  isBlockedFromQualifiedCustomerOpportunityFeed({
    id: "stage1",
    publicationReviewReady: true,
    customerVisible: false,
  }),
  true
);

assert.equal(
  hasApprovedCustomerCommercialClass({
    commercialInventoryClass: "ACCOUNT_DEVELOPMENT",
  }),
  true
);

assert.equal(
  isBlockedFromQualifiedCustomerOpportunityFeed({
    id: "ad1",
    priority: "WATCHLIST",
    commercialInventoryClass: "ACCOUNT_DEVELOPMENT",
    customerVisible: true,
  }),
  false
);

const facing = filterCustomerFacingOpportunities([
  {
    id: "pub1",
    title: "AHIMA Advocacy Summit 2027",
    organizationName: "AHIMA",
    priority: "MEDIUM_PRIORITY",
    customerVisible: true,
    officialSource: "https://example.com",
    summaryWhat: "Named association advocacy conference with lodging demand",
    whoResearchAttempted: true,
    primaryContactName: "Jane Doe",
    primaryContactRole: "Meetings Manager",
    hotelFitScore: 70,
    summaryWhyHotel: "Bethesda Marriott is practical for this DC program",
  },
  {
    id: "watch1",
    title: "ASAE Annual Meeting 2029 — early watch",
    organizationName: "ASAE",
    priority: "WATCHLIST",
    customerVisible: true,
  },
  {
    id: "cvfalse",
    title: "Hidden",
    priority: "HIGH_PRIORITY",
    customerVisible: false,
  },
]);

assert.ok(!facing.some((o) => o.id === "watch1"));
assert.ok(!facing.some((o) => o.id === "cvfalse"));

const filtered = filterQualifiedCustomerOpportunityFeed([
  { id: "a", priority: "WATCHLIST" },
  { id: "b", priority: "HIGH_PRIORITY", customerVisible: true },
]);
assert.deepEqual(
  filtered.map((o) => o.id),
  ["b"]
);

console.log("PASS gdi qualified customer opportunity feed v1");
