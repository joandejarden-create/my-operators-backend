#!/usr/bin/env node
/**
 * Unit tests for Contract Terms / Renewals / Decision Windows V4 modules.
 */
import assert from "node:assert/strict";
import {
  extractContractTermsFromText,
  enrichAgreementProfile,
} from "../lib/hotel-intelligence/sec-edgar/contract-terms-extractor-v1.js";
import { classifyRenewalOutcome } from "../lib/hotel-intelligence/sec-edgar/contract-terms-renewal-classifier-v1.js";
import { calculateDecisionWindows } from "../lib/hotel-intelligence/sec-edgar/contract-terms-decision-windows-v1.js";
import { selectContractTermsCohort } from "../lib/hotel-intelligence/sec-edgar/contract-terms-cohort-v1.js";
import {
  buildAdditiveSchemaDiffPacket,
  RENEWAL_STATUS_CLASS,
  DISCLOSURE_CLASS,
} from "../lib/hotel-intelligence/sec-edgar/contract-terms-schema-proposal-v4.js";
import { assertModePermissions } from "../lib/hotel-intelligence/hotel-intelligence-factory/modes-v1.js";
import { HIF_STAGE } from "../lib/hotel-intelligence/hotel-intelligence-factory/stages-v1.js";

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

test("extract_renewal_options_and_notice", () => {
  const text =
    "The franchise agreement has an initial term of 20 years with two (2) 10-year renewal options at the option of the franchisee, requiring not less than 180 days prior notice.";
  const r = extractContractTermsFromText(text, { sourceUrl: "https://www.sec.gov/x", accession: "0001" });
  assert.equal(r.terms.originalTermYears, 20);
  assert.equal(r.terms.renewalOptionsCount, 2);
  assert.equal(r.terms.renewalOptionYearsEach, 10);
  assert.equal(r.terms.renewalNoticeDays, 180);
  assert.equal(r.terms.renewalMode, "ELECTIVE");
  assert.ok(r.disclosedFieldCount >= 4);
  assert.ok(r.evidence.every((e) => e.isInference === false));
});

test("never_invent_missing_values", () => {
  const r = extractContractTermsFromText("Hotel ABC | Rooms | 120");
  assert.equal(r.terms.originalTermYears, null);
  assert.equal(r.terms.renewalOptionsCount, null);
  assert.ok(r.evidence.some((e) => e.disclosureClass === DISCLOSURE_CLASS.NOT_DISCLOSED));
});

test("unexercised_option_not_executed_renewal", () => {
  const c = classifyRenewalOutcome({
    originalText: "two renewal options available to the franchisee",
    laterText: "",
    sameBrandObserved: true,
    scheduledExpiration: "2030-12-31",
  });
  assert.equal(c.renewalStatusClass, RENEWAL_STATUS_CLASS.RENEWAL_OPTION_AVAILABLE);
});

test("same_brand_after_expiration_not_proof_of_renewal", () => {
  const c = classifyRenewalOutcome({
    originalText: "franchise expiration 2018",
    laterText: "",
    sameBrandObserved: true,
    scheduledExpiration: "2018-12-31",
    asOf: "2026-01-01",
  });
  assert.equal(c.renewalStatusClass, RENEWAL_STATUS_CLASS.STATUS_UNCONFIRMED);
  assert.ok(c.rules.sameBrandNotProofOfRenewal);
});

test("executed_renewal_requires_explicit_later_language", () => {
  const c = classifyRenewalOutcome({
    originalText: "initial franchise term",
    laterText: "The parties renewed the franchise agreement effective January 1, 2024",
    scheduledExpiration: "2023-12-31",
  });
  assert.equal(c.renewalStatusClass, RENEWAL_STATUS_CLASS.RENEWAL_EXECUTED_VERIFIED);
});

test("decision_window_calculated_from_notice_days", () => {
  const w = calculateDecisionWindows({
    scheduledExpiration: "2028-06-30",
    renewalNoticeDays: 90,
    renewalOptionsCount: 2,
    skipDateSemanticGate: true,
  });
  const notice = w.windows.find((x) => x.kind === "RENEWAL_NOTICE_DEADLINE");
  assert.equal(notice.date, "2028-04-01"); // 2028-06-30 minus 90 days (UTC)
  assert.equal(notice.disclosureClass, DISCLOSURE_CLASS.CALCULATED_FROM_VERIFIED_TERMS);
  assert.ok(w.nextDecisionWindow);
});

test("vague_terms_not_precisely_dated", () => {
  const w = calculateDecisionWindows({
    scheduledExpiration: null,
    renewalNoticeDays: null,
    terminationWithoutCause: true,
    skipDateSemanticGate: true,
  });
  const term = w.windows.find((x) => x.kind === "EARLIEST_TERMINATION_WINDOW");
  assert.equal(term.date, null);
  assert.equal(term.disclosureClass, DISCLOSURE_CLASS.CONDITIONAL);
});

test("opening_date_blocks_decision_windows", () => {
  const w = calculateDecisionWindows({
    scheduledExpiration: "2008-12-31",
    renewalNoticeDays: 90,
    dateObservation: {
      primaryDate: "2008-12-31",
      dateMeaning: "FRANCHISE_EXPIRATION",
      evidenceSnippet:
        "25.01 | CREFI | Homewood Suites X | Daphne | AL | 36526 | Baldwin | 1 | Hotel | Extended Stay | 2008 | 2018-2019 | 104 | Rooms | 73.9%",
    },
  });
  assert.equal(w.blocked, true);
  assert.match(w.dateSemanticGate.code, /NON_CONTRACT_DATE|UNRESOLVED/);
  assert.equal(w.nextDecisionWindow, null);
});

test("cohort_franchise_only_no_relabel", () => {
  const input = [
    {
      contractId: "a",
      hotelName: "Hampton Inn A",
      city: "Austin",
      stateRegion: "TX",
      agreementType: "FRANCHISE",
      brandFamily: "HILTON",
      quality: "VERIFIED",
      sourceUrls: ["https://www.sec.gov/a"],
      milestones: [{ primaryDate: "2027-12-31" }],
    },
    {
      contractId: "b",
      hotelName: "Courtyard B",
      city: "Dallas",
      stateRegion: "TX",
      agreementType: "FRANCHISE",
      brandFamily: "MARRIOTT",
      quality: "VERIFIED",
      sourceUrls: ["https://www.sec.gov/b"],
      milestones: [{ primaryDate: "2019-12-31" }],
    },
    {
      contractId: "c",
      hotelName: "Should Skip",
      city: "X",
      stateRegion: "Y",
      agreementType: "FRANCHISE",
      brandFamily: "IHG",
      quality: "RESEARCH_ONLY",
      sourceUrls: ["https://www.sec.gov/c"],
      milestones: [{ primaryDate: "2026-01-01" }],
    },
  ];
  const sel = selectContractTermsCohort(input, { targetSize: 30 });
  assert.equal(sel.franchiseOnly, true);
  assert.ok(sel.cohort.every((r) => r.agreementType === "FRANCHISE"));
  assert.ok(!sel.cohort.some((r) => r.contractId === "c"));
});

test("schema_proposal_pending_not_applied", () => {
  const p = buildAdditiveSchemaDiffPacket();
  assert.match(p.status, /PROPOSED/);
  assert.ok(p.addFields.includes("Renewal Status Class"));
  assert.ok(p.addFields.includes("Decision Window Next Date"));
});

test("enrich_stage_allowed_in_research_mode", () => {
  assert.equal(HIF_STAGE.ENRICH_CONTRACT_TERMS, "ENRICH_CONTRACT_TERMS");
  const r = assertModePermissions({
    mode: "Research",
    stages: ["CONFIG", "ENRICH_CONTRACT_TERMS", "REPORT"],
  });
  assert.equal(r.ok, true);
});

test("enrich_stage_not_in_promotion_mode", () => {
  const r = assertModePermissions({
    mode: "CANONICAL_PROMOTION",
    stages: ["ENRICH_CONTRACT_TERMS"],
  });
  assert.equal(r.ok, false);
});

test("profile_marks_incremental_beyond_expiration", () => {
  const extraction = extractContractTermsFromText(
    "20-year franchise with three five-year renewal options and termination for cause",
  );
  const profile = enrichAgreementProfile(
    { contractId: "x", hotelName: "Test", agreementType: "FRANCHISE" },
    extraction,
    { primaryDate: "2030-01-01" },
  );
  assert.equal(profile.incrementalBeyondExpiration, true);
});

console.log(`\n${passed} tests passed`);
