#!/usr/bin/env node
import assert from "node:assert/strict";
import {
  BETHESDA_88_CONTACT_QUALITY_COMPLETION_V1,
  COVERAGE_BUCKET,
  extractContactSnapshot,
  mergeContactAntiClobber,
  auditBethesda88Contacts,
  applyHistoricalContactEnrichment,
  buildCanonicalContactDryRunPatches,
  ZERO_PAID_CONTACT_EVIDENCE_BY_ID,
  PROPOSED_PAID_CONTACT_BATCH_V1,
} from "../lib/group-demand-intelligence/research/bethesda-88-contact-quality-completion-v1.js";
import {
  loadHotelOpportunityDoc,
  BETHESDA_PROTECTED_SCALING_IDS,
  ACCEPTABLE_59_ROW_SHA256S,
  WOMANS_CLUB_CONTACT_PATCHED_59_ROW_SHA256,
} from "../lib/group-demand-intelligence/research/gdi-hotel-store-integrity-guard-v1.js";
import { mergeBethesdaInternalResearchOpportunities } from "../lib/group-demand-intelligence/research/bethesda-internal-research-union-v1.js";
import { BETHESDA_29_ATTESTED_IDS } from "../lib/group-demand-intelligence/research/bethesda-historical-attestation-bag-v1.js";

assert.equal(typeof BETHESDA_88_CONTACT_QUALITY_COMPLETION_V1, "string");
assert.equal(PROPOSED_PAID_CONTACT_BATCH_V1.authorized, false);

// Anti-clobber: named person cannot be wiped by placeholder.
const preserved = mergeContactAntiClobber(
  {
    id: "t1",
    primaryContact: {
      name: "Clarissa Harris",
      email: "charris@greaterbethesdachamber.org",
      contactClass: "RELEVANT_COMMERCIAL_ENTRY_POINT",
    },
    contactRecovery: { namedPerson: true, usableRoute: true },
  },
  {
    primaryContact: {
      name: "Buyer Not Yet Identified",
      email: "info@example.org",
      contactClass: "FUNCTIONAL_INBOX",
    },
  }
);
assert.equal(preserved.primaryContact.name, "Clarissa Harris");
assert.equal(preserved.primaryContact.email, "charris@greaterbethesdachamber.org");
assert.equal(
  preserved.primaryContact.organizationContactRoute || null,
  "info@example.org"
);

const storeBefore = loadHotelOpportunityDoc("recLuxvwwxID7U2B8");
assert.equal(storeBefore.count, 59);
assert.ok(
  ACCEPTABLE_59_ROW_SHA256S.includes(storeBefore.sha256),
  `store sha must be allowlisted (got ${storeBefore.sha256})`
);
assert.equal(
  storeBefore.sha256,
  WOMANS_CLUB_CONTACT_PATCHED_59_ROW_SHA256,
  "expected Woman's Club contact-patched 59-row store"
);
const wc = storeBefore.opportunities.find(
  (o) => o.id === "gdi_pe_781f12393f8117e7"
);
assert.equal(wc?.primaryContact?.name, "Diane Robertson");
assert.equal(wc?.primaryContact?.emailOwnership, "organization_or_department");
assert.equal(wc?.primaryContactEmail, null);
const wcSnap = extractContactSnapshot(wc);
assert.equal(wcSnap.coverage, COVERAGE_BUCKET.B);
assert.equal(wcSnap.hasDirectEmail, false);
assert.equal(wcSnap.hasOrgRoute, true);

const dry = applyHistoricalContactEnrichment({ dryRun: true });
assert.equal(dry.ok, true);
assert.equal(dry.writeApplied, false);

const applied = applyHistoricalContactEnrichment({ dryRun: false });
assert.equal(applied.ok, true);

const audit = auditBethesda88Contacts();
assert.equal(audit.union.union, 88);
assert.equal(audit.rows.length, 88);
assert.equal(audit.store.scalingCompaniesIntact, true);

// Exa three remain named.
for (const id of [
  "disp_recLuxvwwxID7U2B8_parenteral_drug_association_pda_",
  "disp_recLuxvwwxID7U2B8_greater_bethesda_chamber_of_commerce",
  "gdi_pep_live_c165c897d233",
]) {
  const row = audit.rows.find((r) => r.id === id);
  assert.ok(row, id);
  assert.equal(row.before.named, true, `${id} must keep named contact`);
}

// Visit Montgomery enrichment present after apply.
const vm = audit.rows.find((r) => r.id === "gdi_e2d_bc0a94d7c6007f36");
assert.ok(vm);
assert.equal(vm.before.name, "Katherine Crosse");
assert.equal(vm.before.hasDirectEmail, true);

const patches = buildCanonicalContactDryRunPatches();
assert.ok(patches.patches.length >= 1);
assert.equal(
  patches.patches.every((p) => p.opportunitiesJsonWriteAuthorized === false),
  true
);

const storeAfter = loadHotelOpportunityDoc("recLuxvwwxID7U2B8");
assert.equal(storeAfter.sha256, storeBefore.sha256);
assert.equal(storeAfter.count, 59);
assert.ok(
  BETHESDA_PROTECTED_SCALING_IDS.every((id) => storeAfter.ids.includes(id))
);

const union = mergeBethesdaInternalResearchOpportunities(
  "recLuxvwwxID7U2B8",
  storeAfter.opportunities
);
assert.equal(union.counts.union, 88);
assert.equal(BETHESDA_29_ATTESTED_IDS.length, 29);

// Snapshot helper rejects staff placeholders.
const fake = extractContactSnapshot({
  primaryContact: { name: "NADO conference staff", email: "info@nado.org" },
});
assert.equal(fake.named, false);
assert.ok(
  fake.coverage === COVERAGE_BUCKET.D || fake.coverage === COVERAGE_BUCKET.E
);

console.log(
  JSON.stringify(
    {
      ok: true,
      test: "test-gdi-bethesda-88-contact-quality-completion-v1",
      union: 88,
      named: audit.namedRelevantPrimary,
      distribution: audit.distribution,
      zeroPaidMapped: Object.keys(ZERO_PAID_CONTACT_EVIDENCE_BY_ID).length,
      storeShaUnchanged: true,
    },
    null,
    2
  )
);
