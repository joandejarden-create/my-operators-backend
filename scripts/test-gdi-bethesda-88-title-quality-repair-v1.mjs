#!/usr/bin/env node
import assert from "node:assert/strict";
import {
  hasMeetingCredentialContamination,
  redactSensitiveTitle,
  sanitizeOpportunityTitle,
  titleBlocksCustomerPublication,
  classifyOpportunityTitleDefects,
  TITLE_DEFECT,
  customerSafeOpportunityTitle,
} from "../lib/group-demand-intelligence/opportunity-title-quality-v1.js";
import {
  auditBethesda88Titles,
  applyHistoricalTitleRepairs,
  buildCanonicalTitleDryRunPatches,
  applyApprovedCanonicalTitlePatches,
  resolveRepairedTitle,
  BETHESDA_88_TITLE_QUALITY_REPAIR_V1,
  BETHESDA_CANONICAL_TITLE_APPLY_APPROVAL_TOKEN,
} from "../lib/group-demand-intelligence/research/bethesda-88-title-quality-repair-v1.js";
import {
  loadHotelOpportunityDoc,
  BETHESDA_PROTECTED_SCALING_IDS,
  ACCEPTABLE_59_ROW_SHA256S,
  TITLE_REPAIRED_59_ROW_SHA256,
  WOMANS_CLUB_CONTACT_PATCHED_59_ROW_SHA256,
  CANONICAL_12_CONTACT_PATCHED_59_ROW_SHA256,
  WAVE2_FOUR_CONTACT_PATCHED_59_ROW_SHA256,
  FINAL_FULL_E2E_REMEDIATED_59_ROW_SHA256,
  COMMERCIAL_DEMAND_TYPE_CLASSIFIED_59_ROW_SHA256,
} from "../lib/group-demand-intelligence/research/gdi-hotel-store-integrity-guard-v1.js";
import { mergeBethesdaInternalResearchOpportunities } from "../lib/group-demand-intelligence/research/bethesda-internal-research-union-v1.js";
import { BETHESDA_29_ATTESTED_IDS } from "../lib/group-demand-intelligence/research/bethesda-historical-attestation-bag-v1.js";
import { classifyCustomerSurfaceOpportunity } from "../lib/group-demand-intelligence/customer-surface-revalidation-v1.js";

assert.equal(typeof BETHESDA_88_TITLE_QUALITY_REPAIR_V1, "string");

// Credential detection + redaction (do not assert on secret contents).
const dirty =
  "If prompted, enter Meeting ID REDACTED_TEST and Passcode REDACTED 2027";
assert.equal(hasMeetingCredentialContamination(dirty), true);
assert.equal(redactSensitiveTitle(dirty), "[REDACTED_JOINING_INSTRUCTION_TITLE]");
assert.equal(titleBlocksCustomerPublication(dirty), true);
assert.equal(sanitizeOpportunityTitle(dirty), null);

const nav =
  "NCI RNA Biology Symposium | NCIF Conferences Skip to main content NCI at Frederick St 2027";
assert.ok(
  classifyOpportunityTitleDefects(nav).includes(TITLE_DEFECT.SOURCE_NAV_CONTAMINATION)
);
assert.match(sanitizeOpportunityTitle(nav), /NCI RNA Biology Symposium/i);

assert.equal(
  resolveRepairedTitle({
    id: "gdi_opp_amwa_2028_annual",
    title: "AMWA 113th Annual Meeting 2028 — destination TBD",
  }),
  "AMWA 113th Annual Meeting 2028"
);
assert.equal(
  resolveRepairedTitle({
    id: "gdi_opp_nih_research_festival_2026",
    title:
      "2026 NIH Research Festival — campus event with vendor/attendee overflow lodging potential",
  }),
  "NIH Research Festival 2026"
);

const blocked = classifyCustomerSurfaceOpportunity({
  title: dirty,
  organizationName: "NIDA Clinical Trials Network",
  eventEndDate: "2027-12-01",
});
assert.equal(blocked.keepActive, false);
assert.ok(blocked.reasons.includes("opportunity_title_quality_block"));

const storeBefore = loadHotelOpportunityDoc("recLuxvwwxID7U2B8");
assert.equal(storeBefore.count, 59);
assert.ok(ACCEPTABLE_59_ROW_SHA256S.includes(storeBefore.sha256));

const dry = applyHistoricalTitleRepairs({ dryRun: true });
assert.equal(dry.ok, true);
assert.equal(dry.writeApplied, false);

const applied = applyHistoricalTitleRepairs({ dryRun: false });
assert.equal(applied.ok, true);

const audit = auditBethesda88Titles();
assert.equal(audit.union.union, 88);
assert.equal(audit.rows.length, 88);
assert.equal(audit.defectiveCount, 0);
assert.equal(audit.credentialContaminationCount, 0);
assert.equal(
  audit.rows.filter(
    (r) =>
      r.inventory === "historical" &&
      r.defects.includes(TITLE_DEFECT.MEETING_CREDENTIALS)
  ).length,
  0
);

const meeting = storeBefore.opportunities.find(
  (o) =>
    o.id === "gdi_opp_series_if_prompted_enter_meeting_id_250_658_877__2027"
);
assert.equal(meeting?.titleSourceStatus, "TITLE_SOURCE_INVALID");
assert.match(String(meeting?.title || ""), /TITLE_SOURCE_INVALID/);
assert.equal(hasMeetingCredentialContamination(meeting?.title || ""), false);

const patches = buildCanonicalTitleDryRunPatches();
// After founder-approved apply, remaining dry-run patch count is zero.
assert.equal(patches.patches.length, 0);

const refuse = applyApprovedCanonicalTitlePatches({
  approvalToken: "WRONG",
  dryRun: true,
});
assert.equal(refuse.ok, false);
assert.equal(refuse.code, "APPROVAL_TOKEN_REQUIRED");

const idempotent = applyApprovedCanonicalTitlePatches({
  approvalToken: BETHESDA_CANONICAL_TITLE_APPLY_APPROVAL_TOKEN,
  dryRun: false,
});
assert.equal(idempotent.ok, true);
assert.ok(
  idempotent.code === "ALREADY_APPLIED" || idempotent.writeApplied === false
);

const storeAfter = loadHotelOpportunityDoc("recLuxvwwxID7U2B8");
assert.equal(storeAfter.sha256, storeBefore.sha256);
assert.equal(storeAfter.count, 59);
assert.ok(
  [
    TITLE_REPAIRED_59_ROW_SHA256,
    CANONICAL_12_CONTACT_PATCHED_59_ROW_SHA256,
    WAVE2_FOUR_CONTACT_PATCHED_59_ROW_SHA256,
    FINAL_FULL_E2E_REMEDIATED_59_ROW_SHA256,
    COMMERCIAL_DEMAND_TYPE_CLASSIFIED_59_ROW_SHA256,
  ].includes(storeAfter.sha256),
  "store must remain title-repaired baseline or later founder-approved contact/demand-type patched SHA"
);
assert.ok(
  BETHESDA_PROTECTED_SCALING_IDS.every((id) => storeAfter.ids.includes(id))
);
const wc = storeAfter.opportunities.find((o) => o.id === "gdi_pe_781f12393f8117e7");
assert.equal(wc?.primaryContact?.name, "Diane Robertson");
assert.equal(wc?.secondaryContact?.name, "Mary Sherman");
assert.ok(storeAfter.sha256 !== WOMANS_CLUB_CONTACT_PATCHED_59_ROW_SHA256);

const union = mergeBethesdaInternalResearchOpportunities(
  "recLuxvwwxID7U2B8",
  storeAfter.opportunities
);
assert.equal(union.counts.union, 88);
assert.equal(BETHESDA_29_ATTESTED_IDS.length, 29);

const safe = customerSafeOpportunityTitle({
  title: dirty,
  organizationName: "NIDA Clinical Trials Network",
});
assert.ok(!/Meeting ID|Passcode/i.test(safe));

for (const o of storeAfter.opportunities) {
  const t = customerSafeOpportunityTitle(o);
  assert.ok(!/Meeting ID|Passcode|If prompted|Skip to main/i.test(t || ""));
  assert.ok(
    !/overflow lodging potential|recommended-hotel outreach|destination TBD|housing pending/i.test(
      o.title || ""
    )
  );
}

console.log(
  JSON.stringify(
    {
      ok: true,
      test: "test-gdi-bethesda-88-title-quality-repair-v1",
      union: 88,
      defectiveRemaining: audit.defectiveCount,
      changeCount: audit.changeCount,
      canonicalPatchesRemaining: patches.patches.length,
      storeSha: storeAfter.sha256.slice(0, 12),
      titleRepairedSha: true,
      womansClubContactPreserved: true,
      meetingQuarantined: true,
    },
    null,
    2
  )
);
