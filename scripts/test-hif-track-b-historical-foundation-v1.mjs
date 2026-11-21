#!/usr/bin/env node
/**
 * Unit tests — HIF Track B historical foundation helpers.
 */
import assert from "node:assert/strict";
import {
  CURRENT_STATUS_CLASS,
  TRACK_A_RESERVED_DISCOVERY_IDS,
  classifyCurrentStatus,
  lackOfNewerFilingImpliesCurrent,
  isTrackAReservedId,
  buildCountryTimingMatrixRow,
} from "../lib/hotel-intelligence/sec-edgar/hif-track-b-historical-foundation-v1.js";
import {
  classifyWriteCategory,
  WRITE_CATEGORY,
  planFoundationPersistBatch,
} from "../lib/hotel-intelligence/sec-edgar/hif-foundation-persistence-v1.js";

assert.equal(lackOfNewerFilingImpliesCurrent(), false);
assert.equal(lackOfNewerFilingImpliesCurrent(true), false);

assert.ok(TRACK_A_RESERVED_DISCOVERY_IDS.includes("disc_cb1_lakeville_inn_mn_2026"));
assert.equal(isTrackAReservedId("disc_cb1_lakeville_inn_mn_2026"), true);
assert.equal(isTrackAReservedId("disc_tb_yorkville_royal_sonesta_toronto_2025"), false);

assert.equal(
  classifyCurrentStatus({ historicallyVerified: true }).currentStatusClass,
  CURRENT_STATUS_CLASS.HISTORICALLY_VERIFIED_CURRENT_UNKNOWN,
);
assert.equal(
  classifyCurrentStatus({ historicallyVerified: true, agreementExtendedOrAmended: true }).currentStatusClass,
  CURRENT_STATUS_CLASS.AGREEMENT_EXTENDED_OR_AMENDED,
);
assert.equal(
  classifyCurrentStatus({ brandChanged: true }).currentStatusClass,
  CURRENT_STATUS_CLASS.BRAND_CHANGED,
);

const row = buildCountryTimingMatrixRow({ country: "CA", hpcHotels: 1, contractualEvents: 2 });
assert.equal(row.country, "CA");
assert.equal(row.worldwideEligible, true);
assert.match(row.hotelUniverseCoverage.note, /≠/);

const cls = classifyWriteCategory({
  destination: "HI Hotel Discovery Inventory",
  verified: true,
  evidenceSupported: true,
  sourceUrl: "https://example.com",
  identityClass: "VERIFIED_PROPERTY",
});
assert.equal(cls.category, WRITE_CATEGORY.AUTO_PERSIST_ELIGIBLE);

const hpcForbid = classifyWriteCategory({
  destination: "Hotel Property Census",
  createHpc: true,
  verified: true,
  evidenceSupported: true,
  sourceUrl: "https://example.com",
});
assert.equal(hpcForbid.category, WRITE_CATEGORY.RESEARCH_ONLY);

const plan = planFoundationPersistBatch([
  {
    destination: "HI Hotel Discovery Inventory",
    verified: true,
    evidenceSupported: true,
    sourceUrl: "https://example.com",
    identityClass: "VERIFIED_PROPERTY",
  },
  {
    destination: "HI Hotel Discovery Inventory",
    stagedForReview: true,
    evidenceSupported: true,
    sourceUrl: "https://example.com",
  },
]);
assert.equal(plan.counts.creates, 1);
assert.equal(plan.counts.staged, 1);

console.log("test-hif-track-b-historical-foundation-v1: PASS");
