#!/usr/bin/env node
/**
 * Unit checks for v34D staged apply workflow.
 */
import assert from "node:assert/strict";
import { FACTORY_GUARD_FLAGS } from "../lib/partner-intelligence/brand-explorer-active-profile-factory-rules.js";
import {
  buildDraftApplyCommand,
  buildActiveApprovalCommand,
  validateDraftApplyRequest,
  validateActiveApprovalRequest,
  evaluateFounderVisualReview,
  STAGED_APPLY_VERSION,
} from "../lib/partner-intelligence/brand-explorer-active-profile-staged-apply.js";

assert.equal(STAGED_APPLY_VERSION, "v34D");
assert.ok(buildDraftApplyCommand("suburban-studios").includes("apply-draft"));
assert.ok(buildDraftApplyCommand("suburban-studios").includes(FACTORY_GUARD_FLAGS.approveDraft));
assert.ok(!buildDraftApplyCommand("suburban-studios").includes(`${FACTORY_GUARD_FLAGS.approveActiveProfile} `));
assert.ok(!buildDraftApplyCommand("suburban-studios").endsWith(FACTORY_GUARD_FLAGS.approveActiveProfile));
assert.ok(buildActiveApprovalCommand("suburban-studios").includes("apply-approved"));
assert.ok(buildActiveApprovalCommand("suburban-studios").includes(FACTORY_GUARD_FLAGS.approveActiveProfile));
assert.ok(
  buildActiveApprovalCommand("suburban-studios").includes(FACTORY_GUARD_FLAGS.confirmFounderVisualReviewPassed)
);

{
  const blocked = validateDraftApplyRequest({
    apply: true,
    guardFlags: {
      approveBrandExplorerActiveProfile: true,
      approveBrandExplorerActiveProfileDraft: true,
      founderVisualReview: true,
      confirmNoCompanyValidationClaim: true,
      confirmNoSummaryUrlField: true,
      confirmBrandOnly: true,
      confirmOfficialSourceImagesOnly: true,
      confirmMinimumSixVisibleGalleryImages: true,
      confirmPropertyExamplesHaveHotelImages: true,
      confirmNoLogoLifestylePropertyImages: true,
      confirmStandardDetailGovernanceReviewed: true,
      approveCopyGovernance: true,
    },
    draftPlan: { presentationPatches: [{ recordId: "rec1" }] },
  });
  assert.equal(blocked.allowed, false);
  assert.ok(blocked.blockers.includes("draft_apply_cannot_use_approve_brand_explorer_active_profile"));
}

{
  const ok = validateDraftApplyRequest({
    apply: true,
    guardFlags: {
      approveBrandExplorerActiveProfileDraft: true,
      founderVisualReview: true,
      confirmNoCompanyValidationClaim: true,
      confirmNoSummaryUrlField: true,
      confirmBrandOnly: true,
      confirmOfficialSourceImagesOnly: true,
      confirmMinimumSixVisibleGalleryImages: true,
      confirmPropertyExamplesHaveHotelImages: true,
      confirmNoLogoLifestylePropertyImages: true,
      confirmStandardDetailGovernanceReviewed: true,
      approveCopyGovernance: true,
    },
    draftPlan: { presentationPatches: [{ recordId: "rec1" }] },
    copyGovernancePlan: { repairs: [], founderReviewQueue: [] },
  });
  assert.equal(ok.allowed, true);
  assert.equal(ok.writesActiveProfileApproval, false);
}

{
  const blocked = validateActiveApprovalRequest({
    apply: true,
    guardFlags: {
      approveBrandExplorerActiveProfile: true,
      confirmNoCompanyValidationClaim: true,
      confirmBrandOnly: true,
    },
    founderVisualReview: { pass: true, blockers: [] },
  });
  assert.equal(blocked.allowed, false);
  assert.ok(blocked.blockers.includes("missing_confirm_founder_visual_review_passed"));
}

{
  const blocked = validateActiveApprovalRequest({
    apply: true,
    guardFlags: {
      approveBrandExplorerActiveProfile: true,
      confirmFounderVisualReviewPassed: true,
      confirmNoCompanyValidationClaim: true,
      confirmBrandOnly: true,
    },
    founderVisualReview: {
      pass: false,
      blockers: ["founder_visual_check_failed:gallery_six_visible"],
    },
  });
  assert.equal(blocked.allowed, false);
  assert.ok(blocked.blockers.includes("founder_visual_check_failed:gallery_six_visible"));
}

{
  const review = evaluateFounderVisualReview({
    factoryRules: {
      pass: false,
      blockers: ["risky_copy:fdd:loyalty.proof"],
      rules: {
        gallery: { pass: false, withImageUrl: 2 },
        propertyExamples: { pass: false, visibleOpeningCards: 1, defects: [] },
        scenarioImages: { pass: true },
        copySafety: { pass: false, highCount: 1 },
        standardDetail: { pass: true, sectionStatus: "ok" },
        registryTraceability: { pass: true, gaps: [] },
        uiFallback: { pass: true, risks: [] },
      },
    },
    brandBasics: { "Company Validated": false },
    companyValidatedBefore: false,
  });
  assert.equal(review.pass, false);
  assert.ok(review.failedChecks.some((c) => c.id === "gallery_six_visible"));
  assert.ok(review.checks.find((c) => c.id === "company_validated_untouched")?.pass);
}

console.log("test-brand-explorer-active-profile-staged-apply: ok");
