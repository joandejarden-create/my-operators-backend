#!/usr/bin/env node
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  BETHESDA_88_APPLY_APPROVED_CANONICAL_CONTACT_PATCHES_V1,
  applyApprovedCanonicalContactPatchesV1,
} from "../lib/group-demand-intelligence/research/bethesda-88-apply-approved-canonical-contact-patches-v1.js";
import {
  loadHotelOpportunityDoc,
  BETHESDA_PROTECTED_SCALING_IDS,
  CANONICAL_12_CONTACT_PATCHED_59_ROW_SHA256,
  ACCEPTABLE_59_ROW_SHA256S,
} from "../lib/group-demand-intelligence/research/gdi-hotel-store-integrity-guard-v1.js";
import { auditBethesda88Contacts } from "../lib/group-demand-intelligence/research/bethesda-88-contact-quality-completion-v1.js";

assert.equal(typeof BETHESDA_88_APPLY_APPROVED_CANONICAL_CONTACT_PATCHES_V1, "string");

const store = loadHotelOpportunityDoc("recLuxvwwxID7U2B8");
assert.equal(store.count, 59);
assert.ok(ACCEPTABLE_59_ROW_SHA256S.includes(store.sha256));
assert.equal(store.sha256, CANONICAL_12_CONTACT_PATCHED_59_ROW_SHA256);
assert.ok(
  BETHESDA_PROTECTED_SCALING_IDS.every((id) => store.ids.includes(id))
);

const PASS_IDS = [
  "gdi_opp_amwa_2028_annual",
  "gdi_opp_arlington_spring_tournament_2027",
  "gdi_opp_georgetown_homecoming_2027",
  "gdi_opp_series_new_in_the_library_august_2026",
  "gdi_opp_accp_annual_meeting_20261001",
  "gdi_opp_umd_alumni_weekend_watch",
  "gdi_opp_amwa_2027_annual",
  "gdi_opp_nih_sbpo_vos_pattern",
  "gdi_opp_amwa_2027_interim",
  "gdi_opp_series_ctn_annual_conference_2027",
  "gdi_opp_ecs_251_2027_overflow",
  "gdi_opp_series_march_15_17_bethesda_md_ctn_dissemination_2027",
];

for (const id of PASS_IDS) {
  const row = store.opportunities.find((o) => o.id === id);
  assert.ok(row, id);
  assert.ok(row.primaryContact?.name, `${id} named`);
  assert.equal(row.salesReady, false);
  assert.notEqual(row.customerFacing, true);
  assert.equal(
    row.approvedContactUpdate?.version,
    BETHESDA_88_APPLY_APPROVED_CANONICAL_CONTACT_PATCHES_V1
  );
}

// Direct vs org separation samples
const lizzy = store.opportunities.find(
  (o) => o.id === "gdi_opp_arlington_spring_tournament_2027"
);
assert.equal(lizzy.primaryContact.email, "lizzy@arlingtonsoccer.com");
assert.equal(
  lizzy.primaryContact.organizationContactRoute,
  "tournaments@arlingtonsoccer.com"
);

const annette = store.opportunities.find(
  (o) => o.id === "gdi_opp_nih_sbpo_vos_pattern"
);
assert.equal(annette.primaryContact.email, "scarbora@od.nih.gov");
assert.equal(
  annette.primaryContact.organizationContactRoute,
  "NIHSmallBusiness@mail.nih.gov"
);

const kelly = store.opportunities.find((o) => o.id === "gdi_opp_amwa_2028_annual");
assert.equal(kelly.primaryContact.name, "Kelly McMahon");
assert.equal(kelly.primaryContact.email, null);
assert.equal(
  kelly.primaryContact.organizationContactRoute,
  "associatedirector@amwa-doc.org"
);

// Woman's Club preserved
const wc = store.opportunities.find((o) => o.id === "gdi_pe_781f12393f8117e7");
assert.equal(wc.primaryContact?.name, "Diane Robertson");

// Dry-run without authorization must not write
const beforeSha = store.sha256;
const dry = applyApprovedCanonicalContactPatchesV1({
  dryRun: true,
  founderAuthorization: false,
});
assert.equal(dry.writeApplied, false);
assert.equal(loadHotelOpportunityDoc("recLuxvwwxID7U2B8").sha256, beforeSha);

const audit = auditBethesda88Contacts();
assert.equal(audit.union.union, 88);
assert.ok(audit.namedRelevantPrimary >= 58);

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const summaryPath = path.join(
  ROOT,
  "reports/gdi-bethesda-88-apply-approved-canonical-contact-patches-v1/02-apply-summary.json"
);
assert.ok(fs.existsSync(summaryPath));
const summary = JSON.parse(fs.readFileSync(summaryPath, "utf8"));
assert.equal(summary.appliedCount, 12);
assert.equal(summary.paidResearch, false);

console.log(
  JSON.stringify(
    {
      ok: true,
      test: "test-gdi-bethesda-88-apply-approved-canonical-contact-patches-v1",
      storeSha: store.sha256,
      applied: 12,
      namedUnion: audit.namedRelevantPrimary,
      union: 88,
    },
    null,
    2
  )
);
