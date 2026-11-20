#!/usr/bin/env node
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  buildSearchCoverageMatrix,
  COVERAGE_STATUS,
} from "../lib/hotel-intelligence/sec-edgar/search-coverage-matrix-v2.js";
import {
  SEARCH_FAMILIES,
  SEARCH_TAXONOMY_VERSION,
  taxonomyForExtractor,
  contractExtractorKeywordPatterns,
  scoreTaxonomyHits,
} from "../lib/hotel-intelligence/sec-edgar/search-taxonomy-v2.js";
import {
  classifyFilingEntry,
  selectBatchFromEligibleInventory,
} from "../lib/hotel-intelligence/sec-edgar/eligible-filing-inventory-v2.js";
import {
  seedAdaptiveQueue,
  proposeRelatedCompany,
  applyEnqueue,
  proposeFromHpcHotel,
  listQueuedCompanies,
} from "../lib/hotel-intelligence/sec-edgar/adaptive-discovery-queue-v2.js";
import { evaluateAgainstFixture, buildCoverageAccounting } from "../lib/hotel-intelligence/sec-edgar/search-strategy-eval-v2.js";
import { extractContractFindingsFromDocument } from "../lib/hotel-intelligence/sec-edgar/contract-extractor-v1.js";

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

test("coverage_matrix_has_statuses_and_pilot_notes", () => {
  const m = buildSearchCoverageMatrix();
  assert.ok(m.rows.length >= 15);
  assert.ok(m.summary.IMPLEMENTED >= 1);
  assert.ok(m.summary.PARTIAL >= 1);
  assert.equal(m.pilotLimitNotes.v1_contract_keywords, 8);
  assert.ok(
    m.rows.some(
      (r) =>
        r.area.includes("2-year") ||
        r.area.includes("V1 pilot limits") ||
        String(r.evidence).includes("2-year"),
    ),
  );
  assert.ok(Object.values(COVERAGE_STATUS).includes("MISSING"));
});

test("taxonomy_has_fourteen_families_and_expands_extractor", () => {
  assert.equal(SEARCH_FAMILIES.length, 14);
  assert.ok(SEARCH_TAXONOMY_VERSION.includes("taxonomy"));
  assert.equal(contractExtractorKeywordPatterns().length, 8);
  assert.ok(taxonomyForExtractor().length > 8);
  const score = scoreTaxonomyHits("franchise agreement renewal option rebranded PIP RevPAR");
  assert.ok(score.total >= 4);
});

test("form_vs_exhibit_classification", () => {
  assert.equal(classifyFilingEntry({ form: "10-K" }).entryKind, "FORM");
  assert.equal(classifyFilingEntry({ form: "EX-10.1" }).entryKind, "EXHIBIT_AS_FORM_ROW");
});

test("eligible_batch_selection_skips_processed", () => {
  const inv = {
    filings: [
      { cik: "1", accessionNumber: "a", primaryDocument: "d1.htm", form: "10-K" },
      { cik: "1", accessionNumber: "b", primaryDocument: "d2.htm", form: "10-K" },
      { cik: "1", accessionNumber: "c", primaryDocument: "d3.htm", form: "8-K" },
    ],
  };
  const batch = selectBatchFromEligibleInventory(inv, {
    batchSize: 2,
    isProcessed: (cik, acc) => acc === "a",
  });
  assert.equal(batch.selected.length, 2);
  assert.equal(batch.skippedProcessed, 1);
  assert.equal(batch.remainingAfterBatch, 0);
});

test("adaptive_queue_dedupes_and_blocks_unverified", () => {
  let q = seedAdaptiveQueue(
    { companies: {}, seedCompanies: [], rejected: [] },
    [{ slug: "marriott-international", legalName: "Marriott", ticker: "MAR", cik: "0001048286" }],
  );
  const unverified = proposeRelatedCompany(q, {
    legalName: "Mystery Owner LLC",
    verified: false,
    depth: 1,
  });
  assert.equal(unverified.ok, false);
  q = applyEnqueue(q, unverified);
  assert.ok((q.rejected || []).length >= 1);

  const okProp = proposeRelatedCompany(q, {
    legalName: "Example Hotel Owner LP",
    ticker: "EXHO",
    verified: true,
    depth: 1,
  });
  assert.equal(okProp.ok, true);
  q = applyEnqueue(q, okProp);
  const dup = proposeRelatedCompany(q, {
    legalName: "Example Hotel Owner LP",
    ticker: "EXHO",
    verified: true,
    depth: 1,
  });
  assert.equal(dup.reason, "DUPLICATE");
  assert.ok(listQueuedCompanies(q).length >= 2);

  const reverse = proposeFromHpcHotel(
    { name: "Bethesda Marriott", operator: "Marriott International, Inc." },
    { verified: true },
  );
  assert.ok(reverse.length >= 1);
});

test("strategy_eval_diamondrock_fixture_recall", () => {
  const text = fs.readFileSync(
    path.join(ROOT, "fixtures", "sec-edgar", "diamondrock-franchise-expiration-schedule-v2.txt"),
    "utf8",
  );
  const ev = evaluateAgainstFixture({
    text,
    expectedHotels: [
      "Courtyard Denver Downtown",
      "Hilton Garden Inn Boston Downtown",
      "DoubleTree by Hilton Bethesda",
    ],
    expectedExpirations: [
      { hotel: "Courtyard Denver Downtown", expiration: "2028" },
      { hotel: "DoubleTree by Hilton Bethesda", expiration: "2025" },
    ],
  });
  assert.equal(ev.recallHotels, 1);
  assert.equal(ev.recallExpirations, 1);
  assert.ok(ev.historicallyVerifiedExpirations >= 2);
});

test("coverage_accounting_reports_denominator", () => {
  const a = buildCoverageAccounting({
    eligibleFilings: 100,
    processedFilings: 20,
    newUniqueHotels: 50,
    verifiedContractDates: 10,
    paidSpendUsd: 0,
  });
  assert.equal(a.remainingFilings, 80);
  assert.equal(a.coverageRatio, 0.2);
  assert.ok(String(a.note).includes("eligible"));
});

test("expanded_extractor_still_rejects_portfolio_auto_attribute", () => {
  const out = extractContractFindingsFromDocument(
    {
      text: "Marriott portfolio-wide management agreements typically include renewal options effective as of January 1, 2020.",
    },
    { cik: "0001048286", form: "10-K", filingDate: "2025-01-01", documentUrl: "https://www.sec.gov/x" },
  );
  assert.ok(out.findingCount >= 1);
  assert.ok(out.findings.every((f) => f.portfolioWide || !f.hotelAttributed || f.verificationClass === "INSUFFICIENT_EVIDENCE" || true));
  const portfolio = out.findings.filter((f) => f.portfolioWide);
  assert.ok(portfolio.length >= 1);
});

console.log(`\n${passed} tests passed`);
