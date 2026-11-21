#!/usr/bin/env node
import assert from "node:assert/strict";
import { extractCmbsPropertySchedule } from "../lib/hotel-intelligence/sec-edgar/cmbs-property-schedule-extractor-v1.js";
import {
  scoreCmbsScheduleYield,
  listNameFreeQueries,
  DISCOVERY_STRATEGIES,
} from "../lib/hotel-intelligence/sec-edgar/hif-v6-source-intelligence-audit-v6.js";

const fixture = `
Expiration/Termination of Related License/ Franchise Agreement, Operating Agreement or Management Agreement
Maturity Date of the related Mortgage Loan
AHIP Northeast Portfolio I––Hilton Garden Inn - Baltimore White Marsh $ 15,745,967 1.4 % 6/30/2032 7/6/2027
AHIP Northeast Portfolio I––Courtyard by Marriott - Wall at Monmouth Shores $ 10,000,000 1.0 % 5/15/2032 7/6/2027
Franchise Expiration Reserve (During the continuance of a Cash Management Period caused solely by a Franchise Expiration Trigger Event)
`;

const ex = extractCmbsPropertySchedule({ text: fixture, url: "n-test.htm" }, { sourceUrl: "n-test.htm" });
assert.ok(ex.counts.hotelContractDates >= 2, "should extract plaintext related-license rows");
assert.ok(
  ex.hotelContractFindings.some((f) => /Hilton Garden Inn/i.test(f.hotelName) && f.primaryDate === "2032-06-30"),
);
assert.equal(ex.counts.loanMaturities, ex.counts.relatedLicensePlainRows);

const reserveOnly = scoreCmbsScheduleYield(
  "Franchise Expiration Reserve Trigger Event cash management",
  "n999_424b2-x1.htm",
);
assert.equal(reserveOnly.likelyExtractableSchedule, false, "reserve/trigger must not look extractable");

const gold = scoreCmbsScheduleYield(
  "Expiration/Termination of Related License/ Franchise Agreement Hotel",
  "n1063_424b2-x11.htm",
);
assert.ok(gold.likelyExtractableSchedule, "gold header should score extractable");

const qs = listNameFreeQueries();
assert.ok(qs.some((q) => /Expiration\/Termination of Related License/.test(q.q)));
assert.ok(Object.values(DISCOVERY_STRATEGIES).includes("B_CONTRACTUAL_PHRASE_FIRST"));

console.log(
  JSON.stringify(
    {
      ok: true,
      hotelContractDates: ex.counts.hotelContractDates,
      sample: ex.hotelContractFindings.slice(0, 2).map((f) => ({ n: f.hotelName, d: f.primaryDate })),
    },
    null,
    2,
  ),
);
