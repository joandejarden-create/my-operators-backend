#!/usr/bin/env node
import assert from "node:assert/strict";
import {
  classifyFindingQuality,
  buildQualityGatedMigration,
  QUALITY_CLASS,
} from "../lib/hotel-intelligence/sec-edgar/contract-timing-v4-migration-quality-v1.js";

let passed = 0;
function test(name, fn) {
  try {
    fn();
    passed += 1;
    console.log(`PASS ${name}`);
  } catch (err) {
    console.error(`FAIL ${name}`);
    console.error(err);
    process.exitCode = 1;
  }
}

test("rejects_rv_resort", () => {
  const q = classifyFindingQuality({
    hotelName: "Maple Grove RV Resort",
    city: "Everett",
    stateRegion: "WA",
    isHotelContractDate: true,
    isLoanMaturity: false,
    primaryDate: "2002-12-31",
    hotelAttributed: true,
    agreementType: "FRANCHISE_AGREEMENT",
    dateMeaning: "FRANCHISE_EXPIRATION",
    snippet: "Maple Grove RV Resort | Everett | WA | Manufactured Housing",
    source: { documentUrl: "https://www.sec.gov/x", accessionNumber: "0001" },
  });
  assert.equal(q.class, QUALITY_CLASS.REJECTED);
});

test("bellagio_county_city_conflicted", () => {
  const q = classifyFindingQuality({
    hotelName: "Bellagio Hotel and Casino",
    city: "Clark",
    stateRegion: "NV",
    isHotelContractDate: true,
    isLoanMaturity: false,
    primaryDate: "2019-01-01",
    hotelAttributed: true,
    agreementType: "FRANCHISE_AGREEMENT",
    dateMeaning: "FRANCHISE_EXPIRATION",
    snippet: "Bellagio Hotel and Casino | Clark | NV | 89109",
    source: { documentUrl: "https://www.sec.gov/x", accessionNumber: "0001" },
  });
  assert.equal(q.class, QUALITY_CLASS.CONFLICTED);
});

test("weak_wording_is_research_only", () => {
  const q = classifyFindingQuality({
    hotelName: "Some Boutique Hotel",
    city: "Austin",
    stateRegion: "TX",
    isHotelContractDate: true,
    isLoanMaturity: false,
    primaryDate: "2030-01-01",
    hotelAttributed: true,
    agreementType: "FRANCHISE_AGREEMENT",
    dateMeaning: "OTHER_DATE",
    snippet: "Some Boutique Hotel Austin TX — property listed without franchise column",
    source: { documentUrl: "https://www.sec.gov/x", accessionNumber: "0001" },
  });
  assert.equal(q.class, QUALITY_CLASS.RESEARCH_ONLY);
});

test("cmbs_franchise_column_can_verify", () => {
  const q = classifyFindingQuality({
    hotelName: "Hampton Inn Austin",
    city: "Austin",
    stateRegion: "TX",
    isHotelContractDate: true,
    isLoanMaturity: false,
    primaryDate: "2030-01-01",
    hotelAttributed: true,
    agreementType: "FRANCHISE_AGREEMENT",
    dateMeaning: "FRANCHISE_EXPIRATION",
    snippet: "Hampton Inn Austin | 100 Main St | Austin | TX | 78701 | Hotel | Full Service",
    source: { documentUrl: "https://www.sec.gov/x", accessionNumber: "0001" },
  });
  assert.equal(q.class, QUALITY_CLASS.VERIFIED);
});

test("build_counts_shape", () => {
  const m = buildQualityGatedMigration([
    {
      findingId: "1",
      hotelName: "Maple Grove RV Resort",
      city: "Everett",
      stateRegion: "WA",
      isHotelContractDate: true,
      isLoanMaturity: false,
      primaryDate: "2002-12-31",
      hotelAttributed: true,
      snippet: "Manufactured Housing RV Resort",
      source: { documentUrl: "https://www.sec.gov/x", accessionNumber: "0001" },
      dateMeaning: "FRANCHISE_EXPIRATION",
      agreementType: "FRANCHISE_AGREEMENT",
      wave: "V3",
    },
  ]);
  assert.equal(m.findingQualityCounts.REJECTED, 1);
});

console.log(`\n${passed} migration-quality tests passed`);
