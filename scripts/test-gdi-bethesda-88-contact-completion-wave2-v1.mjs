#!/usr/bin/env node
import assert from "node:assert/strict";
import {
  runBethesda88ContactCompletionWave2,
  WAVE2_CANONICAL_IDS,
  WAVE2_HISTORICAL_IDS,
  WAVE2_FOUNDER_APPROVED_CANONICAL_IDS,
  WAVE2_ACVNU_DEFERRED_ID,
  WAVE2_CANONICAL_APPROVAL_TOKEN,
  buildWave2CanonicalDryRunPatches,
  applyWave2CanonicalContactPatches,
} from "../lib/group-demand-intelligence/research/bethesda-88-contact-completion-wave2-v1.js";
import {
  ZERO_PAID_CONTACT_EVIDENCE_BY_ID,
  extractContactSnapshot,
  mergeContactAntiClobber,
} from "../lib/group-demand-intelligence/research/bethesda-88-contact-quality-completion-v1.js";
import {
  loadHotelOpportunityDoc,
  CANONICAL_12_CONTACT_PATCHED_59_ROW_SHA256,
  WAVE2_FOUR_CONTACT_PATCHED_59_ROW_SHA256,
  FINAL_FULL_E2E_REMEDIATED_59_ROW_SHA256,
  COMMERCIAL_DEMAND_TYPE_CLASSIFIED_59_ROW_SHA256,
  COMMERCIAL_SURFACE_RECONCILED_59_ROW_SHA256,
  ACCEPTABLE_59_ROW_SHA256S,
  BETHESDA_PROTECTED_SCALING_IDS,
} from "../lib/group-demand-intelligence/research/gdi-hotel-store-integrity-guard-v1.js";

const storeBefore = loadHotelOpportunityDoc("recLuxvwwxID7U2B8");
assert.ok(ACCEPTABLE_59_ROW_SHA256S.includes(storeBefore.sha256));
assert.ok(
  [
    CANONICAL_12_CONTACT_PATCHED_59_ROW_SHA256,
    WAVE2_FOUR_CONTACT_PATCHED_59_ROW_SHA256,
    FINAL_FULL_E2E_REMEDIATED_59_ROW_SHA256,
    COMMERCIAL_DEMAND_TYPE_CLASSIFIED_59_ROW_SHA256,
  COMMERCIAL_SURFACE_RECONCILED_59_ROW_SHA256,
  ].includes(storeBefore.sha256)
);

for (const id of [...WAVE2_CANONICAL_IDS, ...WAVE2_HISTORICAL_IDS]) {
  const ev = ZERO_PAID_CONTACT_EVIDENCE_BY_ID[id];
  assert.ok(ev, `missing evidence ${id}`);
  assert.ok(ev.primaryContact?.name, `missing named primary ${id}`);
}

assert.equal(WAVE2_FOUNDER_APPROVED_CANONICAL_IDS.length, 4);
assert.ok(!WAVE2_FOUNDER_APPROVED_CANONICAL_IDS.includes(WAVE2_ACVNU_DEFERRED_ID));

// ACVNU: named role OK, Gmail must NOT count as verified direct business email
const acvnuEv = ZERO_PAID_CONTACT_EVIDENCE_BY_ID[WAVE2_ACVNU_DEFERRED_ID];
assert.equal(acvnuEv.primaryContact.directPersonEmail, false);
assert.equal(acvnuEv.primaryContact.email, null);
assert.equal(
  acvnuEv.primaryContact.personalMailboxObserved?.classification,
  "PERSONAL_MAILBOX_NOT_VERIFIED_BUSINESS_DIRECT"
);
const acvnuRow = storeBefore.opportunities.find((o) => o.id === WAVE2_ACVNU_DEFERRED_ID);
const acvnuMerged = mergeContactAntiClobber(acvnuRow, acvnuEv);
const acvnuSnap = extractContactSnapshot(acvnuMerged);
assert.equal(acvnuSnap.name, "JD Foster");
assert.equal(acvnuSnap.hasDirectEmail, false);
assert.equal(acvnuSnap.coverage, "B_NAMED_PLUS_ORG_ROUTE");
assert.match(String(acvnuSnap.orgEmail || acvnuSnap.orgRoute), /info@acvnu\.org/i);

// Live four-ID apply already on WAVE2_FOUR or later FINAL remediated store
if (
  storeBefore.sha256 === WAVE2_FOUR_CONTACT_PATCHED_59_ROW_SHA256 ||
  storeBefore.sha256 === FINAL_FULL_E2E_REMEDIATED_59_ROW_SHA256 ||
  storeBefore.sha256 === COMMERCIAL_DEMAND_TYPE_CLASSIFIED_59_ROW_SHA256 ||
  storeBefore.sha256 === COMMERCIAL_SURFACE_RECONCILED_59_ROW_SHA256
) {
  for (const id of WAVE2_FOUNDER_APPROVED_CANONICAL_IDS) {
    const row = storeBefore.opportunities.find((o) => o.id === id);
    assert.ok(extractContactSnapshot(row).named, `expected live named ${id}`);
    assert.equal(row.salesReady, false);
    assert.notEqual(row.customerFacing, true);
    assert.equal(row.publicationApproved, false);
    assert.equal(row.approvedContactUpdate?.wave, "wave2_20261112");
  }
  if (storeBefore.sha256 === WAVE2_FOUR_CONTACT_PATCHED_59_ROW_SHA256) {
    assert.equal(
      storeBefore.opportunities.find((o) => o.id === WAVE2_ACVNU_DEFERRED_ID)
        ?.approvedContactUpdate?.wave,
      undefined
    );
  } else {
    // Final remediation applied ACVNU corrected contact.
    assert.equal(
      extractContactSnapshot(
        storeBefore.opportunities.find((o) => o.id === WAVE2_ACVNU_DEFERRED_ID)
      ).name,
      "JD Foster"
    );
  }
}

// Subset apply without auth must fail
const denied = applyWave2CanonicalContactPatches({
  founderAuthorization: false,
  approvalToken: WAVE2_CANONICAL_APPROVAL_TOKEN,
  dryRun: false,
  onlyIds: [...WAVE2_FOUNDER_APPROVED_CANONICAL_IDS],
});
assert.equal(denied.ok, false);
assert.equal(denied.writeApplied, false);

const report = runBethesda88ContactCompletionWave2({
  applyHistorical: true,
  applyCanonical: false,
  founderAuthorization: false,
});

assert.equal(report.ok, true);
assert.equal(report.safety.paidResearch, false);
assert.equal(report.safety.opportunitiesJsonWrites, false);
assert.equal(report.proposedPaidBatch.authorized, false);
assert.ok(report.afterProjectedIncludingCanonicalDryRun.raw.named >= 70);

const manifest = buildWave2CanonicalDryRunPatches();
assert.equal(manifest.patchCount, WAVE2_CANONICAL_IDS.length);
assert.equal(manifest.approvalToken, WAVE2_CANONICAL_APPROVAL_TOKEN);

const storeAfter = loadHotelOpportunityDoc("recLuxvwwxID7U2B8");
assert.equal(storeAfter.sha256, storeBefore.sha256);
for (const id of BETHESDA_PROTECTED_SCALING_IDS) {
  assert.ok(storeAfter.ids.includes(id));
}

// Anti-clobber: Woman's Club Diane preserved
const wc = storeAfter.opportunities.find((o) => o.id === "gdi_pe_781f12393f8117e7");
assert.equal(extractContactSnapshot(wc).name, "Diane Robertson");

// Placeholder names rejected
assert.equal(
  extractContactSnapshot({
    primaryContact: { name: "NADO conference staff", email: "info@nado.org" },
  }).named,
  false
);
assert.equal(
  extractContactSnapshot({
    primaryContact: { name: "NRC RIC Hotel Information" },
  }).named,
  false
);

console.log(
  JSON.stringify(
    {
      ok: true,
      test: "test-gdi-bethesda-88-contact-completion-wave2-v1",
      storeSha: storeAfter.sha256,
      liveFourApplied: [
        WAVE2_FOUR_CONTACT_PATCHED_59_ROW_SHA256,
        FINAL_FULL_E2E_REMEDIATED_59_ROW_SHA256,
        COMMERCIAL_DEMAND_TYPE_CLASSIFIED_59_ROW_SHA256,
  COMMERCIAL_SURFACE_RECONCILED_59_ROW_SHA256,
      ].includes(storeAfter.sha256),
      beforeNamed: report.before.raw.named,
      afterProjectedNamed: report.afterProjectedIncludingCanonicalDryRun.raw.named,
      uniqueMissingBefore: report.before.uniqueMissingNamed,
      uniqueMissingAfterProjected:
        report.afterProjectedIncludingCanonicalDryRun.uniqueMissingNamed,
      storeShaUnchanged: true,
      paidAuthorized: false,
      acvnuGmailNotDirect: true,
    },
    null,
    2
  )
);
