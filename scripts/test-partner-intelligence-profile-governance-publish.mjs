#!/usr/bin/env node
/**
 * Unit checks for PI → profile governance publish script helpers.
 */
import assert from "node:assert/strict";
import {
  assessPublishProtection,
  diffGovernancePublish,
  governanceProposedToAirtable,
  isSourceTypeSafeToWrite,
  NEVER_PUBLISH_API_KEYS,
  proposedToPublishValues,
} from "../lib/partner-intelligence/profile-governance-publish.js";
import {
  GOVERNANCE_VALIDATION_STATUS,
  GOVERNANCE_EXTERNAL_DISPLAY,
  GOVERNANCE_USAGE_PERMISSION,
} from "../lib/profile-governance/profile-governance-fields.js";
import { buildPublishPlanEntry } from "../lib/partner-intelligence/profile-governance-publish.js";
import { proposeProfileGovernance } from "../lib/partner-intelligence/profile-governance-publish-readiness.js";

// operator confidence writes Data Confidence Level
{
  const { fields, columnMap } = governanceProposedToAirtable(
    {
      validationStatus: GOVERNANCE_VALIDATION_STATUS.sourceInformed,
      usagePermission: GOVERNANCE_USAGE_PERMISSION.platformDisplayAllowed,
      confidenceLevel: "Medium",
      externalDisplayStatus: GOVERNANCE_EXTERNAL_DISPLAY.showTrustLabel,
      lastReviewedDate: "2026-06-10",
    },
    {
      entityType: "operator",
      recordFields: { company_name: "Arbor Lodging", "Data Confidence Level": "Low" },
    }
  );
  assert.equal(fields["Data Confidence Level"], "Medium");
  assert.equal(columnMap["Confidence Level"], "Data Confidence Level");
  assert.equal(fields["Confidence Level"], undefined);
}

// Company Validated target blocks publish
{
  const protection = assessPublishProtection(
    {
      "Company Validated": true,
      "Validation Status": GOVERNANCE_VALIDATION_STATUS.companyValidated,
      "Usage Permission": GOVERNANCE_USAGE_PERMISSION.platformDisplayAllowed,
    },
    "operator",
    {
      validationStatus: GOVERNANCE_VALIDATION_STATUS.sourceInformed,
      usagePermission: GOVERNANCE_USAGE_PERMISSION.platformDisplayAllowed,
      confidenceLevel: "Medium",
      externalDisplayStatus: GOVERNANCE_EXTERNAL_DISPLAY.showTrustLabel,
      lastReviewedDate: "2026-06-10",
    },
    "2026-06-10"
  );
  assert.equal(protection.blocked, true);
  assert.ok(protection.reasons.includes("company_validated_checkbox"));
}

// newer Last Reviewed Date blocks publish
{
  const protection = assessPublishProtection(
    {
      "Last Reviewed Date": "2026-07-01",
      "Usage Permission": GOVERNANCE_USAGE_PERMISSION.platformDisplayAllowed,
    },
    "operator",
    {
      validationStatus: GOVERNANCE_VALIDATION_STATUS.sourceInformed,
      usagePermission: GOVERNANCE_USAGE_PERMISSION.platformDisplayAllowed,
      confidenceLevel: "Medium",
      externalDisplayStatus: GOVERNANCE_EXTERNAL_DISPLAY.showTrustLabel,
      lastReviewedDate: "2026-06-10",
    },
    "2026-06-10"
  );
  assert.equal(protection.blocked, true);
  assert.ok(protection.reasons.some((r) => r.startsWith("target_last_reviewed_newer")));
}

// never publish company validated fields
{
  const values = proposedToPublishValues({
    validationStatus: GOVERNANCE_VALIDATION_STATUS.sourceInformed,
    companyValidated: true,
    companyValidationDate: "2026-01-01",
    confidenceLevel: "Medium",
  });
  assert.equal(values.companyValidated, undefined);
  assert.equal(values.companyValidationDate, undefined);
  assert.ok(!NEVER_PUBLISH_API_KEYS.some((k) => values[k] != null));
}

// dry-run plan does not include apply patch on protected target
{
  const plan = buildPublishPlanEntry({
    packageEntry: {
      entityKey: "operator:recTEST",
      entityType: "operator",
      recordId: "recTEST",
      entityName: "Protected Op",
      blockReasons: [],
      changeClass: "new",
      proposed: {
        proposed: {
          validationStatus: GOVERNANCE_VALIDATION_STATUS.sourceInformed,
          usagePermission: GOVERNANCE_USAGE_PERMISSION.platformDisplayAllowed,
          sourceRegion: "CALA-Specific",
          confidenceLevel: "Medium",
          lastReviewedDate: "2026-06-10",
          externalDisplayStatus: GOVERNANCE_EXTERNAL_DISPLAY.showTrustLabel,
          internalNotes: "test",
        },
        expectedGovernance: {
          displayLabel: "Source-Informed Profile",
          displaySubtitle:
            "Last Reviewed: Jun 10, 2026 · Source Basis: Reviewed Sources · Region: CALA-specific",
        },
      },
    },
    targetProfile: {
      id: "recTEST",
      entityType: "operator",
      name: "Protected Op",
      fields: { "Company Validated": true },
    },
    mode: "dry-run",
  });
  assert.equal(plan.write.status, "skipped");
  assert.equal(plan.write.reason, "protected_fields");
}

// Arbor-like eligible package maps to Source-Informed / Platform Display Allowed / Show Trust Label
{
  const proposed = {
    validationStatus: GOVERNANCE_VALIDATION_STATUS.sourceInformed,
    usagePermission: GOVERNANCE_USAGE_PERMISSION.platformDisplayAllowed,
    sourceRegion: "CALA-Specific",
    confidenceLevel: "Medium",
    lastReviewedDate: "2026-06-10",
    externalDisplayStatus: GOVERNANCE_EXTERNAL_DISPLAY.showTrustLabel,
    evidenceNotes: "PI sources (7): Arbor…",
    internalNotes: "PI publish readiness audit proposal — not written.",
  };
  const diff = diffGovernancePublish({}, proposed, "operator");
  assert.ok(diff.wouldUpdate.some((r) => r.apiKey === "validationStatus" && r.to === "Source-Informed"));
  assert.ok(diff.wouldUpdate.some((r) => r.apiKey === "usagePermission"));
  assert.ok(diff.wouldUpdate.some((r) => r.apiKey === "externalDisplayStatus" && r.to === "Show Trust Label"));
  const { expectedGovernance } = proposeProfileGovernance({
    entityType: "operator",
    sources: [
      {
        id: "recS1",
        status: "Approved",
        sourceQuality: "Medium",
        approvedForExplorerUse: "Yes",
        sourceOrigin: "Public Web",
        sourceType: "Press Release",
        region: "CALA",
        sourceTitle: "Arbor brochure",
        lastReviewed: "2026-06-10",
      },
    ],
    facts: [
      {
        id: "recF1",
        humanReviewStatus: "Approved",
        approvedValue: "Arbor Lodging",
        extractionType: "Directly Stated",
        reviewedAt: "2026-06-10",
        publicVisibility: "Public",
      },
    ],
    publishedRows: [],
    piReviewDate: "2026-06-10",
  });
  assert.equal(expectedGovernance.displayLabel, "Source-Informed Profile");
  assert.ok(expectedGovernance.displaySubtitle.includes("CALA"));
  assert.ok(expectedGovernance.displaySubtitle.includes("Source Basis: Reviewed Sources"));
  assert.ok(!expectedGovernance.displaySubtitle.includes("Confidence:"));
}

// Curio-like sparse identity-only package proposes Medium, not High
{
  const { proposed, expectedGovernance } = proposeProfileGovernance({
    entityType: "brand",
    sources: [
      {
        id: "recy2pyEahF9UUsEk",
        status: "Extracted",
        sourceQuality: "High",
        approvedForExplorerUse: "Yes",
        sourceOrigin: "Brand Provided",
        sourceType: "FDD",
        region: "Global",
        sourceTitle: "2025 US Curio FDD",
        lastReviewed: "2026-07-06",
      },
    ],
    facts: [
      {
        fieldName: "be.identity.brandName",
        humanReviewStatus: "Approved",
        approvedValue: "Curio Collection by Hilton",
        extractionType: "Directly Stated",
        publicVisibility: "Public",
      },
      {
        fieldName: "be.identity.parentCompany",
        humanReviewStatus: "Approved",
        approvedValue: "Hilton Worldwide",
        extractionType: "Directly Stated",
        publicVisibility: "Public",
      },
    ],
    publishedRows: [],
    piReviewDate: "2026-07-06",
  });
  assert.equal(proposed.confidenceLevel, "Medium");
  assert.ok(proposed.evidenceNotes.includes("Sparse publish scope"));
  assert.ok(proposed.evidenceNotes.includes("identity-only coverage"));
  assert.equal(expectedGovernance.displayLabel, "AI-Assisted Profile");
  assert.ok(!expectedGovernance.displaySubtitle.includes("Confidence:"));
}

function buildApplyPlanEntry({ entityType, recordId }) {
  return buildPublishPlanEntry({
    packageEntry: {
      entityKey: `${entityType}:${recordId}`,
      entityType,
      recordId,
      entityName: "Test Entity",
      blockReasons: [],
      changeClass: "new",
      proposed: {
        proposed: {
          validationStatus: GOVERNANCE_VALIDATION_STATUS.sourceInformed,
          usagePermission: GOVERNANCE_USAGE_PERMISSION.platformDisplayAllowed,
          confidenceLevel: "Medium",
          lastReviewedDate: "2026-07-06",
          externalDisplayStatus: GOVERNANCE_EXTERNAL_DISPLAY.showTrustLabel,
          internalNotes: "PI publish readiness audit proposal — not written.",
        },
      },
    },
    targetProfile: { id: recordId, entityType, fields: {} },
    mode: "apply",
    applyTimestamp: "2026-07-06",
  });
}

// apply internal notes use brand entity type label
{
  const plan = buildApplyPlanEntry({
    entityType: "brand",
    recordId: "receQkxgjlezsc1xg",
  });
  const internalNotes = plan.fieldDiff.wouldUpdate.find((row) => row.apiKey === "internalNotes");
  assert.equal(
    internalNotes?.to,
    "PI profile-governance publish 2026-07-06 (brand:receQkxgjlezsc1xg)."
  );
}

// apply internal notes use operator entity type label
{
  const plan = buildApplyPlanEntry({
    entityType: "operator",
    recordId: "recF5Z87OAqFgndoq",
  });
  const internalNotes = plan.fieldDiff.wouldUpdate.find((row) => row.apiKey === "internalNotes");
  assert.equal(
    internalNotes?.to,
    "PI profile-governance publish 2026-07-06 (operator:recF5Z87OAqFgndoq)."
  );
}

// Kimpton-like richer facts can still propose High
{
  const { proposed } = proposeProfileGovernance({
    entityType: "brand",
    sources: [
      {
        id: "recKimptonFdd",
        status: "Approved",
        sourceQuality: "High",
        approvedForExplorerUse: "Yes",
        sourceOrigin: "Brand Provided",
        sourceType: "FDD",
        lastReviewed: "2026-06-01",
      },
    ],
    facts: [
      { fieldName: "be.identity.brandName", humanReviewStatus: "Approved", publicVisibility: "Public" },
      { fieldName: "be.identity.parentCompany", humanReviewStatus: "Approved", publicVisibility: "Public" },
      { fieldName: "be.positioning.summary", humanReviewStatus: "Approved", publicVisibility: "Public" },
      { fieldName: "be.overview.typicalUseCase", humanReviewStatus: "Approved", publicVisibility: "Public" },
    ],
    publishedRows: [],
    piReviewDate: "2026-06-01",
  });
  assert.equal(proposed.confidenceLevel, "High");
  assert.ok(!proposed.evidenceNotes?.includes("identity-only coverage"));
}

// skip Unknown source type
assert.equal(isSourceTypeSafeToWrite("Unknown", "Company Website"), false);
assert.equal(isSourceTypeSafeToWrite("Company Website", ""), true);

console.log("test-partner-intelligence-profile-governance-publish: ok");
