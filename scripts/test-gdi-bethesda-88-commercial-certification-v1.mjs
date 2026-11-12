#!/usr/bin/env node
import assert from "node:assert/strict";
import {
  runBethesda88CommercialCertification,
  BETHESDA_88_COMMERCIAL_CERTIFICATION_V1,
  CERTIFICATION_OUTCOME,
} from "../lib/group-demand-intelligence/research/bethesda-88-commercial-certification-v1.js";
import {
  loadHotelOpportunityDoc,
  BETHESDA_PROTECTED_SCALING_IDS,
  TITLE_REPAIRED_59_ROW_SHA256,
  CANONICAL_12_CONTACT_PATCHED_59_ROW_SHA256,
  ACCEPTABLE_59_ROW_SHA256S,
} from "../lib/group-demand-intelligence/research/gdi-hotel-store-integrity-guard-v1.js";
import { PROPOSED_PAID_CONTACT_BATCH_V1 } from "../lib/group-demand-intelligence/research/bethesda-88-contact-quality-completion-v1.js";

assert.equal(typeof BETHESDA_88_COMMERCIAL_CERTIFICATION_V1, "string");
assert.equal(PROPOSED_PAID_CONTACT_BATCH_V1.authorized, false);

const storeBefore = loadHotelOpportunityDoc("recLuxvwwxID7U2B8");
assert.equal(storeBefore.count, 59);
assert.ok(ACCEPTABLE_59_ROW_SHA256S.includes(storeBefore.sha256));
assert.ok(
  [
    TITLE_REPAIRED_59_ROW_SHA256,
    CANONICAL_12_CONTACT_PATCHED_59_ROW_SHA256,
  ].includes(storeBefore.sha256)
);

const report = runBethesda88CommercialCertification({
  applyHistoricalContacts: true,
});

assert.equal(report.sourceOfTruth.union.union, 88);
assert.equal(report.scorecard.length, 88);
assert.equal(report.sourceOfTruth.storeShaUnchanged, true);
assert.equal(report.sourceOfTruth.scalingCompaniesIntact, true);
assert.equal(report.safety.paidResearch, false);
assert.equal(report.safety.opportunitiesJsonWrites, false);
assert.equal(report.safety.customerPublication, false);
assert.equal(report.proposedPaidBatch.authorized, false);

// Woman's Club Diane preserved
const wc = report.scorecard.find((r) => r.id === "gdi_pe_781f12393f8117e7");
assert.ok(wc);
assert.equal(wc.contactAfter.name, "Diane Robertson");
assert.equal(wc.contactAfter.named, true);

// Scaling Companies five present
for (const id of BETHESDA_PROTECTED_SCALING_IDS) {
  assert.ok(
    report.scorecard.some((r) => r.id === id),
    `missing SC ${id}`
  );
}

// Outcomes only from allowed set
const allowed = new Set(Object.values(CERTIFICATION_OUTCOME));
for (const row of report.scorecard) {
  assert.ok(allowed.has(row.certificationOutcome), row.id);
}

// Store untouched
const storeAfter = loadHotelOpportunityDoc("recLuxvwwxID7U2B8");
assert.equal(storeAfter.sha256, storeBefore.sha256);

// Material named recovery expected from this zero-paid batch (projected)
assert.ok(
  report.contactCoverage.afterIncludingCanonicalDryRunProjection.namedRelevantPrimary >=
    report.contactCoverage.before.namedRelevantPrimary,
  "named coverage must not regress"
);
assert.ok(
  report.contactCoverage.afterIncludingCanonicalDryRunProjection.stillMissingNamed <= 43,
  "must not worsen the 43-gap baseline when dry-run projection included"
);
// COMMERCIALLY_CERTIFIED must align with SR or AD engines (no weaker audit label).
for (const row of report.scorecard) {
  if (row.certificationOutcome === "COMMERCIALLY_CERTIFIED") {
    assert.ok(
      row.eligibilityEngines?.salesReadyEligible === true ||
        row.eligibilityEngines?.accountDevelopmentEligible === true,
      `${row.id} certified without engine pass`
    );
  }
}

console.log(
  JSON.stringify(
    {
      ok: true,
      test: "test-gdi-bethesda-88-commercial-certification-v1",
      union: 88,
      namedProjected:
        report.contactCoverage.afterIncludingCanonicalDryRunProjection.namedRelevantPrimary,
      stillMissingNamed:
        report.contactCoverage.afterIncludingCanonicalDryRunProjection.stillMissingNamed,
      commerciallyCertified: report.meetingCommercialCertification,
      storeShaUnchanged: true,
    },
    null,
    2
  )
);
