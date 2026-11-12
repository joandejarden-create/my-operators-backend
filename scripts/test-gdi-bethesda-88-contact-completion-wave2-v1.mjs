#!/usr/bin/env node
import assert from "node:assert/strict";
import {
  runBethesda88ContactCompletionWave2,
  WAVE2_CANONICAL_IDS,
  WAVE2_HISTORICAL_IDS,
  WAVE2_CANONICAL_APPROVAL_TOKEN,
  buildWave2CanonicalDryRunPatches,
} from "../lib/group-demand-intelligence/research/bethesda-88-contact-completion-wave2-v1.js";
import {
  ZERO_PAID_CONTACT_EVIDENCE_BY_ID,
  extractContactSnapshot,
} from "../lib/group-demand-intelligence/research/bethesda-88-contact-quality-completion-v1.js";
import {
  loadHotelOpportunityDoc,
  CANONICAL_12_CONTACT_PATCHED_59_ROW_SHA256,
  BETHESDA_PROTECTED_SCALING_IDS,
} from "../lib/group-demand-intelligence/research/gdi-hotel-store-integrity-guard-v1.js";

const storeBefore = loadHotelOpportunityDoc("recLuxvwwxID7U2B8");
assert.equal(storeBefore.sha256, CANONICAL_12_CONTACT_PATCHED_59_ROW_SHA256);

for (const id of [...WAVE2_CANONICAL_IDS, ...WAVE2_HISTORICAL_IDS]) {
  const ev = ZERO_PAID_CONTACT_EVIDENCE_BY_ID[id];
  assert.ok(ev, `missing evidence ${id}`);
  assert.ok(ev.primaryContact?.name, `missing named primary ${id}`);
}

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
assert.ok(
  report.afterProjectedIncludingCanonicalDryRun.uniqueMissingNamed <
    report.before.uniqueMissingNamed
);

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
      beforeNamed: report.before.raw.named,
      afterProjectedNamed: report.afterProjectedIncludingCanonicalDryRun.raw.named,
      uniqueMissingBefore: report.before.uniqueMissingNamed,
      uniqueMissingAfterProjected:
        report.afterProjectedIncludingCanonicalDryRun.uniqueMissingNamed,
      storeShaUnchanged: true,
      paidAuthorized: false,
    },
    null,
    2
  )
);
