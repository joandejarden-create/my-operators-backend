#!/usr/bin/env node
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { spawnSync } from "node:child_process";
import {
  ZERO_PAID_CONTACT_EVIDENCE_BY_ID,
  mergeContactAntiClobber,
  extractContactSnapshot,
  buildCanonicalContactDryRunPatches,
  COVERAGE_BUCKET,
} from "../lib/group-demand-intelligence/research/bethesda-88-contact-quality-completion-v1.js";
import {
  loadHotelOpportunityDoc,
  BETHESDA_PROTECTED_SCALING_IDS,
  TITLE_REPAIRED_59_ROW_SHA256,
  CANONICAL_12_CONTACT_PATCHED_59_ROW_SHA256,
  ACCEPTABLE_59_ROW_SHA256S,
} from "../lib/group-demand-intelligence/research/gdi-hotel-store-integrity-guard-v1.js";
import { mergeBethesdaInternalResearchOpportunities } from "../lib/group-demand-intelligence/research/bethesda-internal-research-union-v1.js";
import { evaluateSharedCommercialEligibility } from "../lib/group-demand-intelligence/commercial-eligibility-contract-v1.js";
import { evaluateAccountDevelopmentEligibility } from "../lib/group-demand-intelligence/account-development-eligibility-v1.js";
import {
  runBethesda88CommercialCertification,
  CERTIFICATION_OUTCOME,
} from "../lib/group-demand-intelligence/research/bethesda-88-commercial-certification-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

const storeBefore = loadHotelOpportunityDoc("recLuxvwwxID7U2B8");
assert.ok(ACCEPTABLE_59_ROW_SHA256S.includes(storeBefore.sha256));
assert.ok(
  [
    TITLE_REPAIRED_59_ROW_SHA256,
    CANONICAL_12_CONTACT_PATCHED_59_ROW_SHA256,
  ].includes(storeBefore.sha256)
);

// Rejected Carrie family must not enter dry-run patches.
for (const id of [
  "gdi_opp_series_request_a_conference_registration_website_2026",
  "gdi_opp_2027_nci_rna_biology_symposium_7",
  "gdi_opp_series_nci_rna_biology_symposium_ncif_conference_2027",
]) {
  assert.equal(ZERO_PAID_CONTACT_EVIDENCE_BY_ID[id].rejected, true);
  assert.equal(ZERO_PAID_CONTACT_EVIDENCE_BY_ID[id].founderValidation, "FAIL");
}

const patches = buildCanonicalContactDryRunPatches();
assert.equal(
  patches.patches.every(
    (p) =>
      ![
        "gdi_opp_series_request_a_conference_registration_website_2026",
        "gdi_opp_2027_nci_rna_biology_symposium_7",
        "gdi_opp_series_nci_rna_biology_symposium_ncif_conference_2027",
      ].includes(p.id)
  ),
  true
);
// After founder apply of the 12 PASS patches, remaining dry-run may be empty.
assert.ok(Array.isArray(patches.patches));
assert.equal(
  patches.patches.every((p) => p.opportunitiesJsonWriteAuthorized === false),
  true
);

// Org-route vs direct-email classification: AMWA associatedirector is org route.
const amwaLive = storeBefore.opportunities.find((o) => o.id === "gdi_opp_amwa_2028_annual");
const amwaMerged = mergeContactAntiClobber(
  amwaLive,
  ZERO_PAID_CONTACT_EVIDENCE_BY_ID.gdi_opp_amwa_2028_annual
);
const amwaSnap = extractContactSnapshot(amwaMerged);
assert.equal(amwaSnap.named, true);
assert.equal(amwaSnap.name, "Kelly McMahon");
assert.equal(amwaSnap.hasDirectEmail, false);
assert.equal(amwaSnap.hasOrgRoute, true);
assert.equal(amwaSnap.coverage, COVERAGE_BUCKET.B);

// Anti-clobber still holds.
const preserved = mergeContactAntiClobber(
  {
    primaryContact: {
      name: "Diane Robertson",
      organizationContactRoute: "rentals@womansclubofbethesda.org",
      emailOwnership: "organization_or_department",
      directPersonEmail: false,
      contactClass: "RELEVANT_COMMERCIAL_ENTRY_POINT",
    },
  },
  {
    primaryContact: {
      name: "Buyer Not Yet Identified",
      email: "info@example.org",
      contactClass: "FUNCTIONAL_INBOX",
    },
  }
);
assert.equal(preserved.primaryContact.name, "Diane Robertson");

// Shared contacts across AMWA IDs remain opportunity-specific evidence entries.
for (const id of [
  "gdi_opp_amwa_2028_annual",
  "gdi_opp_amwa_2027_annual",
  "gdi_opp_amwa_2027_interim",
]) {
  assert.equal(
    ZERO_PAID_CONTACT_EVIDENCE_BY_ID[id].primaryContact.name,
    "Kelly McMahon"
  );
  assert.ok(
    /AMWA|interim|Annual/i.test(
      ZERO_PAID_CONTACT_EVIDENCE_BY_ID[id].primaryContact.whyRelevant
    )
  );
}

// Run validation script.
const val = spawnSync(
  process.execPath,
  ["scripts/gdi-bethesda-88-canonical-contact-patch-validation-v1.mjs"],
  { cwd: ROOT, encoding: "utf8" }
);
assert.equal(val.status, 0, val.stderr || val.stdout);
const candidate = JSON.parse(
  fs.readFileSync(
    path.join(
      ROOT,
      "reports/gdi-bethesda-88-commercial-certification-v1/07-founder-approved-candidate-manifest.json"
    ),
    "utf8"
  )
);
assert.equal(candidate.authorized, false);
assert.equal(candidate.opportunitiesJsonWriteAuthorized, false);
assert.ok(candidate.passedPatches.length >= 10);
assert.ok(candidate.failedPatches.length >= 3);

// Certification vs maturity agreement after engine alignment.
const report = runBethesda88CommercialCertification({
  applyHistoricalContacts: false,
});
assert.equal(report.sourceOfTruth.union.union, 88);
assert.equal(report.sourceOfTruth.storeShaUnchanged, true);
assert.equal(report.safety.opportunitiesJsonWrites, false);
assert.equal(report.safety.customerPublication, false);

for (const row of report.scorecard) {
  if (row.certificationOutcome === CERTIFICATION_OUTCOME.COMMERCIALLY_CERTIFIED) {
    assert.ok(
      row.eligibilityEngines?.salesReadyEligible === true ||
        row.eligibilityEngines?.accountDevelopmentEligible === true,
      `${row.id} certified without SR/AD engine pass`
    );
  }
}

// Product maturity not auto-promoted by certification.
assert.equal(report.productMaturityCounts.SALES_READY, 0);

// 88 integrity + SC + publication isolation
const storeAfter = loadHotelOpportunityDoc("recLuxvwwxID7U2B8");
assert.equal(storeAfter.sha256, storeBefore.sha256);
assert.ok(
  BETHESDA_PROTECTED_SCALING_IDS.every((id) => storeAfter.ids.includes(id))
);
const union = mergeBethesdaInternalResearchOpportunities(
  "recLuxvwwxID7U2B8",
  storeAfter.opportunities
);
assert.equal(union.counts.union, 88);

// Spot-check engines exist
const sample = storeAfter.opportunities.find((o) => o.id === "gdi_sc_b36b66547864e8ce");
assert.equal(typeof evaluateSharedCommercialEligibility(sample).salesReady, "boolean");
assert.equal(typeof evaluateAccountDevelopmentEligibility(sample).accountDevelopment, "boolean");

console.log(
  JSON.stringify(
    {
      ok: true,
      test: "test-gdi-bethesda-88-canonical-contact-patch-validation-v1",
      passPatches: candidate.passedPatches.length,
      failPatches: candidate.failedPatches.length,
      commerciallyCertified: report.meetingCommercialCertification,
      storeShaUnchanged: true,
      union: 88,
    },
    null,
    2
  )
);
