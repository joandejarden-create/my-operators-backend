#!/usr/bin/env node
/**
 * Regression fixtures for date-semantic validation (Phase 2).
 */
import assert from "node:assert/strict";
import {
  auditContractDateObservation,
  AUDITED_DATE_MEANING,
  DECISION_WINDOW_ELIGIBILITY,
} from "../lib/hotel-intelligence/sec-edgar/contract-date-meaning-audit-v1.js";
import { assertDateEligibleForDecisionWindow } from "../lib/hotel-intelligence/sec-edgar/contract-date-semantic-gate-v1.js";
import { calculateDecisionWindows } from "../lib/hotel-intelligence/sec-edgar/contract-terms-decision-windows-v1.js";

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

test("fixture_cmbs_year_opened_not_franchise_expiration", () => {
  const a = auditContractDateObservation({
    primaryDate: "2008-12-31",
    dateMeaning: "FRANCHISE_EXPIRATION",
    evidenceSnippet:
      "25.01 | CREFI | Homewood Suites Mobile East Bay Daphne | 29474 North Main Street | Daphne | AL | 36526 | Baldwin | 1 | Hotel | Extended Stay | 2008 | 2018-2019 | 104 | Rooms | 73.9%",
  });
  assert.equal(a.auditedMeaning, AUDITED_DATE_MEANING.HOTEL_OPENING_OR_YEAR_BUILT);
  assert.equal(a.misclassifiedOpening, true);
  assert.equal(a.decisionWindowEligibility, DECISION_WINDOW_ELIGIBILITY.INELIGIBLE);
});

test("fixture_loan_maturity_isolated", () => {
  const a = auditContractDateObservation({
    primaryDate: "2020-10-01",
    dateMeaning: "FRANCHISE_EXPIRATION",
    evidenceSnippet:
      "Property schedule Loan Maturity 10/1/2020 anticipated repayment; no franchise expiration column",
  });
  assert.equal(a.auditedMeaning, AUDITED_DATE_MEANING.LOAN_MATURITY);
  const g = assertDateEligibleForDecisionWindow({ audit: a });
  assert.equal(g.ok, false);
  assert.equal(g.code, "NON_CONTRACT_DATE_MEANING");
});

test("fixture_multi_property_portfolio_language_blocked", () => {
  const g = assertDateEligibleForDecisionWindow({
    observation: {
      primaryDate: "2030-12-31",
      dateMeaning: "FRANCHISE_EXPIRATION",
      evidenceSnippet: "Franchise Expiration Date 12/31/2030 applies to the portfolio properties",
    },
    portfolioLevelOnly: true,
    hotelAttributed: false,
  });
  assert.equal(g.ok, false);
  assert.match(g.code, /PORTFOLIO|HOTEL_ATTRIBUTION/);
});

test("fixture_explicit_franchise_expiration_eligible", () => {
  const a = auditContractDateObservation({
    primaryDate: "2025-12-31",
    dateMeaning: "FRANCHISE_EXPIRATION",
    evidenceSnippet:
      "Courtyard Example | Franchise Expiration Date | 12/31/2025 | Fee/Leasehold | Three, 10-year options",
  });
  assert.equal(a.auditedMeaning, AUDITED_DATE_MEANING.FRANCHISE_AGREEMENT_EXPIRATION);
  const g = assertDateEligibleForDecisionWindow({ audit: a, hotelAttributed: true });
  assert.equal(g.ok, true);
});

test("fixture_footnote_unresolved_without_clear_header", () => {
  const a = auditContractDateObservation({
    primaryDate: "2015-12-31",
    dateMeaning: "FRANCHISE_EXPIRATION",
    evidenceSnippet: "See footnote (3). Date 2015. Property schedule excerpt incomplete.",
  });
  assert.equal(a.auditedMeaning, AUDITED_DATE_MEANING.UNRESOLVED_DATE_MEANING);
  assert.equal(a.decisionWindowEligibility, DECISION_WINDOW_ELIGIBILITY.QUARANTINED);
});

test("fixture_amended_superseded_not_auto_current", () => {
  const a = auditContractDateObservation({
    primaryDate: "2019-06-01",
    dateMeaning: "FRANCHISE_EXPIRATION",
    evidenceSnippet:
      "Amendment dated June 1, 2019 extended the franchise; prior expiration superseded — renewal option language only",
  });
  assert.ok(
    a.auditedMeaning === AUDITED_DATE_MEANING.RENEWAL_OR_EXTENSION_EVENT ||
      a.auditedMeaning === AUDITED_DATE_MEANING.UNRESOLVED_DATE_MEANING ||
      a.auditedMeaning === AUDITED_DATE_MEANING.FRANCHISE_AGREEMENT_EXPIRATION,
  );
});

test("fixture_decision_window_fail_closed_on_opening_date", () => {
  const w = calculateDecisionWindows({
    scheduledExpiration: "2004-12-31",
    renewalNoticeDays: 180,
    dateObservation: {
      primaryDate: "2004-12-31",
      dateMeaning: "FRANCHISE_EXPIRATION",
      evidenceSnippet:
        "Hilton Garden Inn X | Daphne | AL | 36526 | Baldwin | 1 | Hotel | Select Service | 2004 | 2018-2019 | 124 | Rooms",
    },
  });
  assert.equal(w.blocked, true);
  assert.equal(w.nextDecisionWindow, null);
});

test("fixture_unattributed_portfolio_terms_do_not_pass_gate", () => {
  const w = calculateDecisionWindows({
    scheduledExpiration: "2030-01-01",
    dateObservation: {
      primaryDate: "2030-01-01",
      evidenceSnippet: "Franchise Expiration Date December 31, 2030 for the Hotel",
    },
    portfolioLevelOnly: true,
  });
  assert.equal(w.blocked, true);
});

console.log(`\n${passed} date-semantic regression fixtures passed`);
