#!/usr/bin/env node
import assert from "node:assert/strict";
import {
  runBethesda88FinalFullE2eRemediation,
  buildFinalRemediationDryRun,
  FINAL_REMEDIATION_APPROVAL_TOKEN,
  FINAL_FACTUAL_FIELD_PATCHES,
  BETHESDA_88_FINAL_FULL_E2E_REMEDIATION_V1,
} from "../lib/group-demand-intelligence/research/bethesda-88-final-full-e2e-remediation-v1.js";
import {
  loadHotelOpportunityDoc,
  FINAL_FULL_E2E_REMEDIATED_59_ROW_SHA256,
  COMMERCIAL_DEMAND_TYPE_CLASSIFIED_59_ROW_SHA256,
  COMMERCIAL_SURFACE_RECONCILED_59_ROW_SHA256,
  ACCEPTABLE_59_ROW_SHA256S,
  BETHESDA_PROTECTED_SCALING_IDS,
} from "../lib/group-demand-intelligence/research/gdi-hotel-store-integrity-guard-v1.js";
import {
  extractContactSnapshot,
  ZERO_PAID_CONTACT_EVIDENCE_BY_ID,
} from "../lib/group-demand-intelligence/research/bethesda-88-contact-quality-completion-v1.js";
import { evaluateOpportunityQualityContract } from "../lib/group-demand-intelligence/gdi-opportunity-quality-contract-v1.js";

assert.equal(typeof BETHESDA_88_FINAL_FULL_E2E_REMEDIATION_V1, "string");
assert.ok(FINAL_FACTUAL_FIELD_PATCHES.length >= 4);

const store = loadHotelOpportunityDoc("recLuxvwwxID7U2B8");
assert.ok(ACCEPTABLE_59_ROW_SHA256S.includes(store.sha256));
assert.ok(
  [
    FINAL_FULL_E2E_REMEDIATED_59_ROW_SHA256,
    COMMERCIAL_DEMAND_TYPE_CLASSIFIED_59_ROW_SHA256,
  COMMERCIAL_SURFACE_RECONCILED_59_ROW_SHA256,
  ].includes(store.sha256),
  `unexpected store sha ${store.sha256}`,
);
assert.equal(store.count, 59);
for (const id of BETHESDA_PROTECTED_SCALING_IDS) {
  assert.ok(store.ids.includes(id));
}

// ACVNU live: named + org route, no Gmail direct
const acvnu = store.opportunities.find(
  (o) => o.id === "gdi_opp_acvnu_renal_week_20270412"
);
const acvnuSnap = extractContactSnapshot(acvnu);
assert.equal(acvnuSnap.name, "JD Foster");
assert.equal(acvnuSnap.hasDirectEmail, false);
assert.match(String(acvnuSnap.orgEmail || acvnuSnap.orgRoute), /info@acvnu\.org/i);
assert.equal(acvnu.hotelFitHonesty, "OVERFLOW_WATCH_COMPETITOR_HOST");
assert.equal(acvnu.salesReady, false);
assert.notEqual(acvnu.customerFacing, true);

// Evidence still forbids Gmail as direct
const ev = ZERO_PAID_CONTACT_EVIDENCE_BY_ID.gdi_opp_acvnu_renal_week_20270412;
assert.equal(ev.primaryContact.email, null);
assert.equal(ev.primaryContact.directPersonEmail, false);

// Destination-outside honesty
const aao = store.opportunities.find((o) => o.id === "gdi_opp_aao_2027_annual_session_9");
assert.equal(aao.priority, "DISQUALIFIED");
assert.match(String(aao.hotelFitHonesty), /DESTINATION_OUTSIDE/);
assert.equal(aao.eventLocation, "Las Vegas, NV");

const iscb = store.opportunities.find(
  (o) => o.id === "gdi_opp_48th_iscb_annual_conference_2027_12"
);
assert.equal(iscb.priority, "DISQUALIFIED");
assert.equal(iscb.eventLocation, "Rotterdam, The Netherlands");
assert.equal(extractContactSnapshot(iscb).name, "Dimitris Rizopoulos");

// Quality contract: DQ destination-outside does not require named contact
const aaoQc = evaluateOpportunityQualityContract(aao, {
  familyRole: "STANDALONE",
});
const contactAxis = (aaoQc.axes || []).find((a) => a.axis === "CONTACT");
assert.ok(contactAxis);
assert.equal(contactAxis.ok, true);
assert.equal(contactAxis.detail?.contactExempt, true);

// Woman's Club anti-clobber
const wc = store.opportunities.find((o) => o.id === "gdi_pe_781f12393f8117e7");
assert.equal(extractContactSnapshot(wc).name, "Diane Robertson");

// Dry-run does not rewrite
const beforeSha = store.sha256;
const dry = buildFinalRemediationDryRun();
assert.equal(dry.approvalToken, FINAL_REMEDIATION_APPROVAL_TOKEN);
assert.ok(dry.patchCount >= 4);
const report = runBethesda88FinalFullE2eRemediation({
  applyHistorical: true,
  applyAcvnuContact: false,
  applyFactualPatches: false,
  founderAuthorization: false,
});
assert.equal(report.safety.paidResearch, false);
assert.equal(report.safety.customerPublication, false);
assert.equal(report.safety.maturityPromotion, false);
const after = loadHotelOpportunityDoc("recLuxvwwxID7U2B8");
assert.equal(after.sha256, beforeSha);

console.log(
  JSON.stringify(
    {
      ok: true,
      test: "test-gdi-bethesda-88-final-full-e2e-remediation-v1",
      storeSha: after.sha256,
      acvnuNamed: acvnuSnap.name,
      acvnuDirect: acvnuSnap.hasDirectEmail,
      iscbNamed: extractContactSnapshot(iscb).name,
      paidAuthorized: false,
    },
    null,
    2
  )
);
