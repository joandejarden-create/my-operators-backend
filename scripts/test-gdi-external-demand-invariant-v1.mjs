/**
 * External-demand invariant — hotel-hosted product cannot be customer opportunity
 * without independent external named demand entity.
 *
 *   npm run test:gdi-external-demand-invariant-v1
 */
import assert from "node:assert/strict";
import {
  isHotelHostedProduct,
  assertExternalDemandForCustomerOpportunity,
  classifyHotelHostedCampaignAdmission,
} from "../lib/group-demand-intelligence/external-demand-invariant-v1.js";
import { classifyCampaignAdmission } from "../lib/group-demand-intelligence/demand-campaigns/campaign-from-opportunity-v1.js";
import { isValidFutureWatch } from "../lib/group-demand-intelligence/future-watch/is-valid-future-watch-v1.js";

const tournament = {
  id: "camp_sheraton_golf_tournament_2026_sheraton",
  title: "VI Sheraton Mallorca Golf Tournament 2026",
  organizationName: "Arabella Golf Mallorca / Sheraton Mallorca",
  selectionStatus: "HOTEL_HOSTED_PACKAGE",
  eventStartDate: "2026-11-29",
  officialSource:
    "https://arabellagolfmallorca.com/en/vi-sheraton-mallorca-golf-tournament-2026-the-golf-tournament-at-golf-son-vida/",
  hotelName: "Sheraton Mallorca Arabella Golf Hotel",
  whyMonitor: "Recurring / future-cycle pattern — monitor for next published cycle",
  summaryQuality: "STRONG",
  hotelOpportunityThesis: "Stay and play package at hotel",
  sources: [{ url: "https://arabellagolfmallorca.com/en/vi-sheraton-mallorca-golf-tournament-2026/" }],
};

assert.equal(isHotelHostedProduct(tournament), true);
assert.equal(assertExternalDemandForCustomerOpportunity(tournament).ok, false);
assert.equal(classifyHotelHostedCampaignAdmission(tournament).class, "REJECT");

const admit = classifyCampaignAdmission(tournament);
assert.equal(admit.class, "REJECT");
assert.ok(
  admit.reasons.some((r) => /hotel_hosted|external/i.test(r)),
  JSON.stringify(admit.reasons)
);

const watch = isValidFutureWatch(tournament, { nowDate: "2026-10-08" });
assert.equal(watch.ok, false);
assert.ok(
  (watch.reasons || []).some((r) => /hotel_hosted|external/i.test(r)) ||
    watch.class === "ENTITY_INVALID" ||
    watch.class === "RESEARCH_BACKLOG_NOT_WATCH",
  JSON.stringify(watch)
);

// External tour operator package is NOT hotel-hosted by brand alone
const gph = {
  id: "camp_gph",
  title: "Golf Planet Holidays Sheraton Mallorca Son Vida break 2027",
  organizationName: "Golf Planet Holidays",
  selectionStatus: "ORGANIZER_PACKAGE",
  eventStartDate: "2027-02-28",
  officialSource: "https://golfplanetholidays.com/product/sheraton-mallorca-arabella-golf-hotel/",
};
assert.equal(isHotelHostedProduct(gph), false);
assert.equal(assertExternalDemandForCustomerOpportunity(gph).ok, true);

console.log("test-gdi-external-demand-invariant-v1: PASS");
