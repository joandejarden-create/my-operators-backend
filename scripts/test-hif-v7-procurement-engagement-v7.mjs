#!/usr/bin/env node
/**
 * Gate: HIF V7 procurement status + engagement route helpers.
 */
import assert from "node:assert/strict";
import {
  COMMERCIAL_CATEGORY,
  PROCUREMENT_STATUS,
  ENGAGEMENT_ROUTE,
  isPrincipalBctOrMct,
  isAccessibleToNewParticipants,
  qualifiesVerifiedPriorityAForMilestone,
  HIF_V7_PROCUREMENT_ENGAGEMENT,
} from "../lib/hotel-intelligence/sec-edgar/hif-v7-procurement-engagement-v7.js";

assert.equal(HIF_V7_PROCUREMENT_ENGAGEMENT, "hif-v7-procurement-engagement-v7");
assert.equal(isPrincipalBctOrMct(COMMERCIAL_CATEGORY.MANAGEMENT_CONTRACT_TARGET), true);
assert.equal(isPrincipalBctOrMct(COMMERCIAL_CATEGORY.HOTEL_CONCESSION_OPERATING), false);
assert.equal(isPrincipalBctOrMct(COMMERCIAL_CATEGORY.HOTEL_DEVELOPMENT), false);
assert.equal(isAccessibleToNewParticipants(PROCUREMENT_STATUS.OPEN_FOR_SUBMISSIONS), true);
assert.equal(isAccessibleToNewParticipants(PROCUREMENT_STATUS.AWARD_PENDING), false);

assert.equal(
  qualifiesVerifiedPriorityAForMilestone({
    commercialCategory: COMMERCIAL_CATEGORY.MANAGEMENT_CONTRACT_TARGET,
    verifiedPriorityA: true,
    accessibleToNewParticipants: true,
    procurementStatus: PROCUREMENT_STATUS.OPEN_FOR_SUBMISSIONS,
  }),
  true
);

assert.equal(
  qualifiesVerifiedPriorityAForMilestone({
    commercialCategory: COMMERCIAL_CATEGORY.MANAGEMENT_CONTRACT_TARGET,
    verifiedPriorityA: true,
    accessibleToNewParticipants: false,
    procurementStatus: PROCUREMENT_STATUS.AWARD_PENDING,
  }),
  false,
  "Chicago-style closed MCT must not count toward first-100"
);

assert.equal(
  qualifiesVerifiedPriorityAForMilestone({
    commercialCategory: COMMERCIAL_CATEGORY.HOTEL_CONCESSION_OPERATING,
    verifiedPriorityA: true,
    accessibleToNewParticipants: true,
    procurementStatus: PROCUREMENT_STATUS.OPEN_FOR_SUBMISSIONS,
  }),
  false,
  "Gideon-style concession is adjacent — not principal first-100"
);

assert.equal(
  qualifiesVerifiedPriorityAForMilestone({
    commercialCategory: COMMERCIAL_CATEGORY.HOTEL_DEVELOPMENT,
    verifiedPriorityA: true,
    accessibleToNewParticipants: true,
    procurementStatus: PROCUREMENT_STATUS.OPEN_FOR_SUBMISSIONS,
  }),
  false,
  "Pueblo-style development is adjacent — not BCT first-100"
);

assert.ok(ENGAGEMENT_ROUTE.PUBLIC_TENDER_SUBMISSION);
assert.ok(ENGAGEMENT_ROUTE.NO_LONGER_COMMERCIALLY_ACCESSIBLE);

console.log("PASS test-hif-v7-procurement-engagement-v7");
