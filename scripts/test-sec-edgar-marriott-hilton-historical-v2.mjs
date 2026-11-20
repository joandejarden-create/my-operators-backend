#!/usr/bin/env node
/**
 * Tests for SEC EDGAR Marriott/Hilton Historical Intelligence V2.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  HISTORICAL_APPLICABILITY,
  SEC_EDGAR_HISTORICAL_V2,
} from "../lib/hotel-intelligence/sec-edgar/contract.js";
import {
  listHistoricalFilings,
  scoreFilingYield,
  flattenSubmissionFilings,
} from "../lib/hotel-intelligence/sec-edgar/filing-discovery-v2.js";
import { detectBrandFamily, brandFamilySummary } from "../lib/hotel-intelligence/sec-edgar/brand-families-v2.js";
import { extractFranchiseExpirationSchedule } from "../lib/hotel-intelligence/sec-edgar/franchise-schedule-extractor-v2.js";
import {
  classifyHistoricalApplicability,
  applyHistoricalApplicability,
  assertNotPromotedWithoutEvidence,
} from "../lib/hotel-intelligence/sec-edgar/historical-verification-v2.js";
import { listHistoricalResearchCompanies } from "../lib/hotel-intelligence/sec-edgar/cik-registry-v2.js";
import {
  buildMarriottHiltonInventory,
  marriottHiltonInventoryToCsv,
} from "../lib/hotel-intelligence/sec-edgar/marriott-hilton-inventory-v2.js";
import { DISCOVERY_STATUS } from "../lib/hotel-intelligence/sec-edgar/contract.js";
import { VERIFICATION_CLASS } from "../lib/hotel-intelligence/sec-edgar/contract.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

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

test("historical_v2_contract_id", () => {
  assert.equal(SEC_EDGAR_HISTORICAL_V2, "SEC_EDGAR_HISTORICAL_INTELLIGENCE_V2");
});

test("expanded_company_registry_includes_diamondrock_hilton", () => {
  const cos = listHistoricalResearchCompanies({ maxPriority: 2 });
  assert.ok(cos.some((c) => c.slug === "diamondrock-hospitality"));
  assert.ok(cos.some((c) => c.slug === "hilton-worldwide"));
  assert.ok(cos.some((c) => c.slug === "marriott-international"));
});

test("brand_family_detection_marriott_hilton", () => {
  assert.equal(detectBrandFamily("Courtyard Denver Downtown").family, "MARRIOTT");
  assert.equal(detectBrandFamily("Hilton Garden Inn Boston Downtown").family, "HILTON");
  assert.equal(detectBrandFamily("Embassy Suites Chicago Downtown").family, "HILTON");
  assert.equal(detectBrandFamily("Westin San Diego").family, "MARRIOTT");
  const sum = brandFamilySummary([
    { hotelName: "Courtyard X" },
    { hotelName: "Hampton Inn Y" },
  ]);
  assert.equal(sum.marriott, 1);
  assert.equal(sum.hilton, 1);
});

test("diamondrock_fixture_schedule_extraction", () => {
  const fixturePath = path.join(
    ROOT,
    "fixtures",
    "sec-edgar",
    "diamondrock-franchise-expiration-schedule-v2.txt",
  );
  const text = fs.readFileSync(fixturePath, "utf8");
  const out = extractFranchiseExpirationSchedule(
    { text },
    {
      cik: "0001298946",
      companyName: "DiamondRock Hospitality Company",
      form: "10-K",
      filingDate: "2023-02-28",
      documentUrl: "https://www.sec.gov/Archives/edgar/data/fixture/drh.htm",
      accessionNumber: "0001298946-23-000012",
    },
  );
  assert.ok(out.hotelCount >= 8, `expected >=8 hotels, got ${out.hotelCount}`);
  assert.ok(out.findings.every((f) => f.hotelAttributed));
  assert.ok(
    out.findings.every(
      (f) => f.historicalApplicability === HISTORICAL_APPLICABILITY.HISTORICALLY_VERIFIED,
    ),
  );
  const courtyard = out.hotels.find((h) => /Courtyard Denver/i.test(h.hotelName));
  assert.ok(courtyard, "Courtyard Denver row");
  assert.equal(courtyard.city, "Denver");
  assert.equal(courtyard.stateRegion, "CO");
  assert.equal(courtyard.brandFamily, "MARRIOTT");
  const hilton = out.hotels.find((h) => /Hilton Garden Inn Boston/i.test(h.hotelName));
  assert.ok(hilton);
  assert.equal(hilton.brandFamily, "HILTON");
});

test("historical_applicability_never_auto_promotes", () => {
  const f = {
    verificationClass: VERIFICATION_CLASS.VERIFIED_EXPIRATION,
    primaryDate: "2028",
    source: { filingDate: "2006-03-01" },
  };
  const c = classifyHistoricalApplicability(f);
  assert.equal(c.historicalApplicability, HISTORICAL_APPLICABILITY.HISTORICALLY_VERIFIED);
  assert.equal(c.currentlyVerifiedExpiration, null);

  const expired = classifyHistoricalApplicability({
    ...f,
    primaryDate: "2010",
  });
  assert.equal(
    expired.historicalApplicability,
    HISTORICAL_APPLICABILITY.CURRENT_STATUS_UNCONFIRMED,
  );

  const promoted = assertNotPromotedWithoutEvidence({
    historicalApplicability: HISTORICAL_APPLICABILITY.CURRENTLY_VERIFIED,
  });
  assert.equal(promoted.ok, false);
});

test("amendment_and_termination_lifecycle", () => {
  const applied = applyHistoricalApplicability([
    {
      attributedHotelName: "Courtyard Denver Downtown",
      verificationClass: VERIFICATION_CLASS.VERIFIED_EXPIRATION,
      primaryDate: "2025",
      source: { filingDate: "2018-01-01" },
    },
    {
      attributedHotelName: "Courtyard Denver Downtown",
      verificationClass: VERIFICATION_CLASS.VERIFIED_AMENDMENT,
      primaryDate: "2030",
      source: { filingDate: "2022-01-01" },
    },
  ]);
  const first = applied.findings.find((f) => f.primaryDate === "2025");
  assert.equal(first.historicalApplicability, HISTORICAL_APPLICABILITY.AMENDED);
});

test("duplicate_schedule_and_wrong_hotel_dates", () => {
  const text = `
    Courtyard Denver Downtown Denver CO Franchise 2028
    Courtyard Denver Downtown Denver CO Franchise 2028
    Westin San Diego San Diego, CA Franchise 2030
  `;
  const out = extractFranchiseExpirationSchedule(
    { text },
    { cik: "0001298946", companyName: "DRH", form: "10-K", filingDate: "2023-02-28" },
  );
  const denver = out.hotels.filter((h) => /Courtyard Denver/i.test(h.hotelName));
  assert.equal(denver.length, 1);
  // Dates stay on attributed hotel — Westin does not inherit Denver expiration
  const westin = out.hotels.find((h) => /Westin San Diego/i.test(h.hotelName));
  assert.ok(westin.contractFindings.every((f) => /2030/.test(f.primaryDate)));
  assert.ok(!westin.contractFindings.some((f) => /2028/.test(f.primaryDate)));
});

test("hotels_missing_from_hpc_retained_in_inventory_export", () => {
  const inv = [
    {
      discoveryId: "disc_1",
      hotelName: "Courtyard Denver Downtown",
      city: "Denver",
      stateRegion: "CO",
      country: "United States",
      brandFamily: "MARRIOTT",
      historicalBrand: "Courtyard",
      registrant: "DiamondRock",
      cik: "0001298946",
      hpcMatchStatus: DISCOVERY_STATUS.DISCOVERED_NOT_IN_HPC,
      contractFindings: [
        {
          verificationClass: VERIFICATION_CLASS.VERIFIED_EXPIRATION,
          historicalApplicability: HISTORICAL_APPLICABILITY.HISTORICALLY_VERIFIED,
          agreementType: "FRANCHISE_AGREEMENT",
          primaryDate: "2028",
          initialExpiration: "2028",
          source: { filingDate: "2023-02-28", documentUrl: "https://www.sec.gov/x" },
        },
      ],
    },
  ];
  const report = buildMarriottHiltonInventory(inv);
  assert.equal(report.rowCount, 1);
  assert.equal(report.rows[0].hpcMatchStatus, DISCOVERY_STATUS.DISCOVERED_NOT_IN_HPC);
  assert.equal(report.rows[0].currentlyVerifiedExpiration, "");
  assert.equal(report.rows[0].initialExpiration, "2028");
  const csv = marriottHiltonInventoryToCsv(report);
  assert.ok(csv.includes("Courtyard Denver Downtown"));
  assert.ok(csv.includes("HISTORICALLY_VERIFIED"));
});

test("historical_filing_discovery_uses_additional_shards_and_yield", () => {
  const body = {
    cik: "1298946",
    name: "DiamondRock",
    filings: {
      recent: {
        form: ["10-K", "4", "S-11"],
        filingDate: ["2023-02-28", "2023-01-01", "2005-05-01"],
        accessionNumber: ["a", "b", "c"],
        primaryDocument: ["d1.htm", "d2.htm", "d3.htm"],
        primaryDocDescription: ["Annual report", "", "IPO"],
        reportDate: ["", "", ""],
        size: [2000000, 10, 900000],
      },
      files: [{ name: "CIK0001298946-submissions-001.json" }],
    },
  };
  const listed = listHistoricalFilings(body, { sinceYear: 1994, limit: 10 });
  assert.equal(listed.ok, true);
  assert.ok(listed.filings.some((f) => f.form === "S-11"));
  assert.ok(!listed.filings.some((f) => f.form === "4"));
  assert.ok(scoreFilingYield({ form: "EX-10.1", size: 1000 }) > scoreFilingYield({ form: "10-Q", size: 1000 }));
  assert.ok(listed.coverage.eligibleFound >= 2);
  const flat = flattenSubmissionFilings(body, [
    {
      form: ["10-K"],
      filingDate: ["1999-03-01"],
      accessionNumber: ["old"],
      primaryDocument: ["old.htm"],
      primaryDocDescription: [""],
      reportDate: [""],
      size: [100],
    },
  ]);
  assert.ok(flat.length >= 4);
});

test("conversion_and_alias_brand_change_detection", () => {
  const before = detectBrandFamily("Sheraton Boston Hotel");
  const after = detectBrandFamily("Autograph Collection Boston Harbor");
  assert.equal(before.family, "MARRIOTT");
  assert.equal(after.family, "MARRIOTT");
  assert.notEqual(before.brandKey, after.brandKey);
  const hiltonToOther = detectBrandFamily("Independent Downtown Hotel");
  assert.equal(hiltonToOther.family, null);
});

console.log(`\n${passed} tests passed`);
