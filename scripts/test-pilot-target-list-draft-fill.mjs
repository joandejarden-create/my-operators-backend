/**
 * Unit tests for Pilot Target List draft fill logic.
 */
import assert from "node:assert/strict";
import { MAP_PILOT_TARGET_LIST } from "../lib/gtm-owner-target/pilot-target-list-field-map.js";
import {
  buildDraftFillPlan,
  inferOutreachSegment,
  hasText,
} from "../lib/gtm-owner-target/pilot-target-list-draft-fill.js";
import {
  SEGMENT_DRAFT_TEMPLATES,
  buildEmailDraft,
  buildFollowUpDraft,
  buildLinkedInDm,
  assertNaturalDraftCopy,
  BANNED_DRAFT_PHRASES,
} from "../lib/gtm-owner-target/pilot-outreach-draft-templates.js";

const F = MAP_PILOT_TARGET_LIST;

function baseOwnerFields(overrides = {}) {
  return {
    [F.name]: "Ryan Forde",
    [F.category]: "Owner",
    [F.company]: ["recCompany1"],
    [F.relationshipStrength]: "Strong",
    ...overrides,
  };
}

function testDoesNotOverwriteWithoutFlag() {
  const plan = buildDraftFillPlan({
    recordId: "rec1",
    fields: baseOwnerFields({ [F.outreachSegment]: "Owner / Investor" }),
    companyNameById: new Map([["recCompany1", "Example Hotels"]]),
    overwrite: false,
  });
  assert.equal(plan.patch[F.outreachSegment], undefined);
  assert.ok(plan.patch[F.emailDraft]);
}

function testOverwriteReplacesExisting() {
  const plan = buildDraftFillPlan({
    recordId: "rec1",
    fields: baseOwnerFields({ [F.emailDraft]: "Old draft" }),
    overwrite: true,
  });
  assert.ok(plan.patch[F.emailDraft]);
  assert.notEqual(plan.patch[F.emailDraft], "Old draft");
}

function testSkipsDoNotContact() {
  const plan = buildDraftFillPlan({
    recordId: "rec1",
    fields: baseOwnerFields({ [F.doNotContact]: true }),
  });
  assert.equal(plan.skipped, true);
  assert.equal(plan.skipReason, "do_not_contact");
}

function testDoesNotSetApproved() {
  const plan = buildDraftFillPlan({
    recordId: "rec1",
    fields: baseOwnerFields(),
  });
  assert.notEqual(plan.patch[F.outreachStatus], "Approved");
  assert.notEqual(plan.patch[F.outreachStatus], "Sent");
}

function testDoesNotCheckReadyForMailMerge() {
  const plan = buildDraftFillPlan({
    recordId: "rec1",
    fields: baseOwnerFields(),
  });
  assert.equal(plan.patch[F.readyForMailMerge], undefined);
}

function testLeavesFinalApprovedEmailBlank() {
  const plan = buildDraftFillPlan({
    recordId: "rec1",
    fields: baseOwnerFields(),
  });
  assert.equal(plan.patch[F.finalApprovedEmail], undefined);
}

function testFillsDraftsBySegment() {
  const brandPlan = buildDraftFillPlan({
    recordId: "recBrand",
    fields: {
      [F.name]: "Paul Adan",
      [F.category]: "Brand Referral Source",
    },
  });
  assert.equal(brandPlan.patch[F.outreachSegment], "Brand / Referral Source");
  assert.ok(String(brandPlan.patch[F.emailDraft]).includes("Dealality"));
  assertNaturalDraftCopy(brandPlan.patch[F.emailDraft]);
  assertNaturalDraftCopy(brandPlan.patch[F.linkedInDmDraft]);
  assertNaturalDraftCopy(brandPlan.patch[F.followUpDraft]);

  const ownerPlan = buildDraftFillPlan({
    recordId: "recOwner",
    fields: baseOwnerFields(),
  });
  assert.equal(ownerPlan.patch[F.outreachSegment], "Owner / Investor");
  assert.ok(String(ownerPlan.patch[F.emailDraft]).includes("keeping the first group small"));
  assertNaturalDraftCopy(ownerPlan.patch[F.emailDraft]);
}

function testDraftsExcludeBannedPhrases() {
  for (const segment of Object.keys(SEGMENT_DRAFT_TEMPLATES)) {
    const email = buildEmailDraft("Alex", segment, null);
    const dm = buildLinkedInDm("Alex", segment);
    const followUp = buildFollowUpDraft("Alex");
    assertNaturalDraftCopy(email);
    assertNaturalDraftCopy(dm);
    assertNaturalDraftCopy(followUp);
  }
  for (const phrase of BANNED_DRAFT_PHRASES) {
    assert.ok(phrase.length > 0);
  }
}

function testMissingEmailSetsReviewReason() {
  const plan = buildDraftFillPlan({
    recordId: "rec1",
    fields: baseOwnerFields(),
  });
  assert.ok(plan.reviewReasons.includes("missing_email"));
  assert.ok(["Drafted", "Needs Review", "Draft Needed"].includes(plan.patch[F.outreachStatus]));
}

function testSkipsApprovedRows() {
  const plan = buildDraftFillPlan({
    recordId: "rec1",
    fields: baseOwnerFields({ [F.outreachStatus]: "Approved" }),
  });
  assert.equal(plan.skipped, true);
  assert.equal(plan.skipReason, "already_approved_or_sent");
}

function testInferSegmentFromCategory() {
  assert.equal(inferOutreachSegment("Brand Referral Source"), "Brand / Referral Source");
  assert.equal(inferOutreachSegment("Operator Referral Source"), "Brand / Referral Source");
  assert.equal(inferOutreachSegment("Owner"), "Owner / Investor");
}

function testHandlesMissingEmailSafely() {
  const plan = buildDraftFillPlan({
    recordId: "rec1",
    fields: baseOwnerFields({ [F.email]: "" }),
  });
  assert.equal(plan.patch[F.email], undefined);
  assert.ok(hasText(plan.patch[F.emailDraft]));
}

function run() {
  testDoesNotOverwriteWithoutFlag();
  testOverwriteReplacesExisting();
  testSkipsDoNotContact();
  testDoesNotSetApproved();
  testDoesNotCheckReadyForMailMerge();
  testLeavesFinalApprovedEmailBlank();
  testFillsDraftsBySegment();
  testDraftsExcludeBannedPhrases();
  testMissingEmailSetsReviewReason();
  testSkipsApprovedRows();
  testInferSegmentFromCategory();
  testHandlesMissingEmailSafely();
  console.log("test-pilot-target-list-draft-fill: all passed");
}

run();
