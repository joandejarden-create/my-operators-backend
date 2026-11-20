#!/usr/bin/env node
/**
 * Unit tests — Hotel Contract Timing Intelligence (CMBS / REIT pilot).
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  DATE_MEANING,
  SOURCE_FAMILY,
  classifyDateHorizon,
  isHotelContractDateMeaning,
  isLoanMaturityMeaning,
  CONTRACT_TIMING_GUARANTEES,
} from "../lib/hotel-intelligence/sec-edgar/contract-timing-types-v1.js";
import {
  extractCmbsPropertySchedule,
  inferDateMeaning,
} from "../lib/hotel-intelligence/sec-edgar/cmbs-property-schedule-extractor-v1.js";
import { verifyContractTimingFindings } from "../lib/hotel-intelligence/sec-edgar/contract-timing-verifier-v1.js";
import {
  listSeedDocuments,
  CONTRACT_TIMING_PILOT_DOC_CAP,
  CMBS_ISSUER_REGISTRY,
} from "../lib/hotel-intelligence/sec-edgar/contract-timing-pilot-registry-v1.js";
import { extractFranchiseExpirationSchedule } from "../lib/hotel-intelligence/sec-edgar/franchise-schedule-extractor-v2.js";
import { bridgeFranchiseScheduleToContractTiming } from "../lib/hotel-intelligence/sec-edgar/contract-timing-franchise-bridge-v1.js";

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

test("guarantees_loan_vs_franchise_isolation", () => {
  assert.ok(CONTRACT_TIMING_GUARANTEES.some((g) => /loan|mortgage/i.test(g)));
  assert.equal(isLoanMaturityMeaning(DATE_MEANING.LOAN_MATURITY_DATE), true);
  assert.equal(isHotelContractDateMeaning(DATE_MEANING.LOAN_MATURITY_DATE), false);
  assert.equal(isHotelContractDateMeaning(DATE_MEANING.FRANCHISE_EXPIRATION), true);
});

test("infer_date_meaning_separates_loan_and_termination", () => {
  assert.equal(
    inferDateMeaning("Loan Maturity Date", "11/01/2026"),
    DATE_MEANING.LOAN_MATURITY_DATE,
  );
  assert.equal(
    inferDateMeaning("Franchise Expiration", "12/31/2028"),
    DATE_MEANING.FRANCHISE_EXPIRATION,
  );
  assert.equal(
    inferDateMeaning("Termination Right", "01/15/2027"),
    DATE_MEANING.TERMINATION_RIGHT_DATE,
  );
});

test("cmbs_fixture_extracts_hotel_dates_excludes_loan_and_portfolio", () => {
  const fixture = fs.readFileSync(
    path.join(ROOT, "fixtures/sec-edgar/contract-timing-cmbs-schedule-sample.txt"),
    "utf8",
  );
  const out = extractCmbsPropertySchedule(
    { text: fixture },
    {
      cik: "0001013611",
      accessionNumber: "0001013611-99-000001",
      form: "424B5",
      companyName: "Sample CMBS Depositor",
      sourceFamily: SOURCE_FAMILY.CMBS_424B,
      filingDate: "2024-06-01",
      documentUrl: "https://www.sec.gov/Archives/edgar/data/1013611/fixture.htm",
    },
  );
  assert.ok(out.hotelContractFindings.length >= 4, `got ${out.hotelContractFindings.length}`);
  assert.ok(out.loanMaturityFindings.length >= 1, "loan maturities isolated");
  assert.ok(
    out.hotelContractFindings.every((f) => !f.isLoanMaturity),
    "no loan maturity in hotel set",
  );
  assert.ok(
    out.rejected.some((r) => /portfolio|multi_hotel/i.test(r.reason)),
    "portfolio/multi-hotel rejected",
  );
  const boston = out.hotelContractFindings.find((f) => /Courtyard Boston/i.test(f.hotelName));
  assert.ok(boston);
  assert.equal(boston.dateMeaning, DATE_MEANING.FRANCHISE_EXPIRATION);
  assert.equal(boston.primaryDate, "2028-12-31");
  const larkspur = out.hotelContractFindings.find((f) => /Larkspur/i.test(f.hotelName));
  assert.ok(larkspur, "HTML table Franchise Expiration Date column");
  assert.equal(larkspur.primaryDate, "2028-12-31");
  assert.ok(!out.hotelContractFindings.some((f) => /Office Portfolio/i.test(f.hotelName)));
});

test("horizon_buckets_and_historical", () => {
  const now = new Date("2026-11-20T00:00:00Z");
  const h6 = classifyDateHorizon("2027-01-15", now);
  assert.equal(h6.historical, false);
  assert.ok(h6.buckets.includes(6));
  assert.ok(h6.buckets.includes(12));
  const hist = classifyDateHorizon("2020-01-01", now);
  assert.equal(hist.historical, true);
  assert.deepEqual(hist.buckets, []);
});

test("verifier_excludes_loan_promotes_verified_schedule_rows", () => {
  const fixture = fs.readFileSync(
    path.join(ROOT, "fixtures/sec-edgar/contract-timing-cmbs-schedule-sample.txt"),
    "utf8",
  );
  const extracted = extractCmbsPropertySchedule(
    { text: fixture },
    { cik: "0001013611", accessionNumber: "x", form: "424B5", sourceFamily: SOURCE_FAMILY.CMBS_424B },
  );
  const verified = verifyContractTimingFindings(extracted.hotelContractFindings, {
    now: "2026-11-20",
    hpcCandidates: [
      { id: "recBostonCY", name: "Courtyard Boston Downtown", city: "Boston", brand: "Courtyard" },
    ],
  });
  assert.equal(verified.counts.loanMaturityExcluded, 0); // already filtered at extract
  assert.ok(verified.counts.verifiedHotelContractDates + verified.counts.probable >= 3);
  assert.ok(verified.counts.hpcMatched >= 1);
});

test("franchise_bridge_maps_reit_schedule", () => {
  const text = `
Franchise Expiration Schedule
Courtyard Denver Downtown Denver CO Franchise 2031
Westin San Diego San Diego CA Management December 31, 2030
`;
  const sched = extractFranchiseExpirationSchedule(
    { text },
    { cik: "0001298946", companyName: "DiamondRock", form: "10-K", accessionNumber: "x" },
  );
  const bridged = bridgeFranchiseScheduleToContractTiming(sched, {
    sourceFamily: SOURCE_FAMILY.HOTEL_REIT_SCHEDULE,
  });
  assert.ok(bridged.hotelContractFindings.length >= 2);
  assert.ok(
    bridged.hotelContractFindings.every((f) => isHotelContractDateMeaning(f.dateMeaning)),
  );
});

test("pilot_registry_includes_cmbs_issuers_and_cap", () => {
  const seeds = listSeedDocuments();
  assert.ok(seeds.some((s) => s.sourceFamily === SOURCE_FAMILY.CMBS_424B));
  assert.ok(seeds.some((s) => /chatham/i.test(s.slug)));
  assert.ok(seeds.some((s) => /sotherly/i.test(s.slug)));
  assert.ok(seeds.some((s) => /nexpoint/i.test(s.slug)));
  assert.ok(CMBS_ISSUER_REGISTRY.length >= 3);
  assert.equal(CONTRACT_TIMING_PILOT_DOC_CAP, 100);
});

console.log(`\n${passed} tests passed`);
if (process.exitCode) process.exit(process.exitCode);
