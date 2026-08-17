#!/usr/bin/env node
/**
 * Unit checks for lib/partner-intelligence/stewardship-package.js
 */
import assert from "node:assert/strict";
import {
  buildSafeSourcePatch,
  buildSafeFactPatch,
  recommendGovernanceFacts,
  scoreGovernanceFact,
  summarizeFactStatuses,
  isFactExcludedFromRecommendation,
  NEVER_UPDATE,
  BRAND_GOVERNANCE_FIELD_KEYS,
  OPERATOR_GOVERNANCE_FIELD_KEYS,
} from "../lib/partner-intelligence/stewardship-package.js";

const KIMPTON = "recCKuXCmGvxHPfb3";
const ARBOR = "recF5Z87OAqFgndoq";
const CURIO = "receQkxgjlezsc1xg";

// dry-run patch requires allowWrites false
{
  const patch = buildSafeSourcePatch(
    { id: "recSRC1", brandId: KIMPTON, approvedForExplorerUse: "No", status: "Approved", sourceQuality: "High" },
    "brand",
    KIMPTON,
    { approvedSourceIds: new Set(["recSRC1"]), allowWrites: false, allowQualityBump: false, allowStatusAdvance: false }
  );
  assert.equal(patch.patch, null);
  assert.ok(patch.skipped.includes("dry_run_no_writes"));
}

// source updates require explicit source IDs
{
  const patch = buildSafeSourcePatch(
    { id: "recSRC1", brandId: KIMPTON, approvedForExplorerUse: "No", status: "Approved", sourceQuality: "High" },
    "brand",
    KIMPTON,
    { approvedSourceIds: new Set(), allowWrites: true, allowQualityBump: true, allowStatusAdvance: true }
  );
  assert.equal(patch.patch, null);
  assert.ok(patch.skipped.includes("source_id_not_in_approve_list"));
}

// wrong entity linkage blocks writes
{
  const patch = buildSafeSourcePatch(
    { id: "recSRC1", brandId: "recOTHER", approvedForExplorerUse: "No", status: "Approved", sourceQuality: "High" },
    "brand",
    KIMPTON,
    { approvedSourceIds: new Set(["recSRC1"]), allowWrites: true, allowQualityBump: true, allowStatusAdvance: true }
  );
  assert.equal(patch.patch, null);
  assert.ok(patch.skipped.includes("not_linked_to_target"));
}

// fact updates require explicit fact IDs
{
  const patch = buildSafeFactPatch(
    {
      id: "recF1",
      brandId: KIMPTON,
      humanReviewStatus: "Pending",
      extractedValue: "Kimpton Hotels",
      evidenceText: "quote",
    },
    "brand",
    KIMPTON,
    { approvedFactIds: new Set(), allowWrites: true }
  );
  assert.equal(patch.patch, null);
  assert.ok(patch.skipped.includes("fact_id_not_in_approve_list"));
}

// no Company Validated in never-update list
assert.ok(NEVER_UPDATE.some((n) => n.includes("Company Validated")));
assert.ok(NEVER_UPDATE.some((n) => n.includes("Company Validation Date")));

// Kimpton-like recommendations prioritize identity/positioning
{
  const facts = [
    {
      id: "recA",
      fieldName: "be.identity.brandName",
      humanReviewStatus: "Pending",
      extractionType: "Directly Stated",
      confidenceLevel: "High",
      evidenceText: "Kimpton Hotels",
      extractedValue: "Kimpton Hotels",
      brandId: KIMPTON,
      sourceRecordId: "recSRC1",
    },
    {
      id: "recB",
      fieldName: "be.misc.obscureField",
      humanReviewStatus: "Pending",
      extractionType: "Inferred",
      confidenceLevel: "Low",
      extractedValue: "maybe",
      brandId: KIMPTON,
    },
    {
      id: "recC",
      fieldName: "be.positioning.summary",
      humanReviewStatus: "Pending",
      extractionType: "Directly Stated",
      confidenceLevel: "High",
      evidenceText: "Boutique lifestyle",
      extractedValue: "Boutique lifestyle brand",
      brandId: KIMPTON,
      sourceRecordId: "recSRC1",
    },
  ];
  const rec = recommendGovernanceFacts(facts, "brand", { stewardSourceIds: ["recSRC1"] });
  assert.ok(rec.length >= 2);
  const ids = rec.map((r) => r.id);
  assert.ok(ids.includes("recA"));
  assert.ok(ids.includes("recC"));
  assert.ok(BRAND_GOVERNANCE_FIELD_KEYS.includes(rec[0].fieldName) || rec[0].score >= rec[rec.length - 1].score);
}

// Arbor-like operator recommendations
{
  const facts = [
    {
      id: "recO1",
      fieldName: "op.snapshot.companyName",
      humanReviewStatus: "Pending",
      extractionType: "Directly Stated",
      confidenceLevel: "Medium",
      evidenceText: "Arbor Lodging",
      extractedValue: "Arbor Lodging",
      operatorId: ARBOR,
    },
    {
      id: "recO2",
      fieldName: "op.capabilities.managementServices",
      humanReviewStatus: "Pending",
      extractionType: "Directly Stated",
      confidenceLevel: "Medium",
      evidenceText: "Third-party management",
      extractedValue: "Third-party hotel management",
      operatorId: ARBOR,
    },
    {
      id: "recO3",
      fieldName: "op.internal.notes",
      humanReviewStatus: "Pending",
      extractionType: "Inferred",
      confidenceLevel: "Low",
      extractedValue: "vague",
      operatorId: ARBOR,
    },
  ];
  const rec = recommendGovernanceFacts(facts, "operator");
  const topIds = rec.slice(0, 2).map((r) => r.id);
  assert.ok(topIds.includes("recO1"));
  assert.ok(topIds.includes("recO2"));
  assert.ok(OPERATOR_GOVERNANCE_FIELD_KEYS.includes("op.snapshot.companyName"));
  const low = scoreGovernanceFact(facts[2], "operator");
  assert.ok(low.score < scoreGovernanceFact(facts[0], "operator").score);
}

// 1. Rejected facts are not recommended
{
  const facts = [
    {
      id: "recRejected",
      fieldName: "be.identity.brandName",
      humanReviewStatus: "Rejected",
      extractionType: "Directly Stated",
      confidenceLevel: "High",
      evidenceText: "Kimpton Hotels",
      extractedValue: "Kimpton Hotels",
      brandId: CURIO,
    },
    {
      id: "recPending",
      fieldName: "be.identity.parentCompany",
      humanReviewStatus: "Pending",
      extractionType: "Directly Stated",
      confidenceLevel: "High",
      evidenceText: "Hilton",
      extractedValue: "Hilton",
      brandId: CURIO,
    },
  ];
  const rec = recommendGovernanceFacts(facts, "brand");
  const ids = rec.map((r) => r.id);
  assert.ok(!ids.includes("recRejected"));
  assert.ok(ids.includes("recPending"));
}

// 2. Quarantined facts with quarantine notes are not recommended
{
  const facts = [
    {
      id: "recQuarantine",
      fieldName: "be.identity.brandName",
      humanReviewStatus: "Pending",
      reviewerNotes: "Quarantined — wrong-brand contamination from Mexico FDD",
      extractionType: "Directly Stated",
      confidenceLevel: "High",
      evidenceText: "Kimpton",
      extractedValue: "Kimpton",
      brandId: CURIO,
    },
    {
      id: "recClean",
      fieldName: "be.positioning.summary",
      humanReviewStatus: "Pending",
      extractionType: "Directly Stated",
      confidenceLevel: "Medium",
      evidenceText: "Curio Collection",
      extractedValue: "Curio Collection by Hilton",
      brandId: CURIO,
    },
  ];
  const rec = recommendGovernanceFacts(facts, "brand");
  const ids = rec.map((r) => r.id);
  assert.ok(!ids.includes("recQuarantine"));
  assert.ok(ids.includes("recClean"));
  assert.ok(isFactExcludedFromRecommendation(facts[0]));
}

// 3. Pending clean facts can still be recommended (covered by Kimpton test above)

// 4. Approved/Edited facts count toward eligibility but are not recommended again
{
  const facts = [
    {
      id: "recApproved",
      fieldName: "be.identity.brandName",
      humanReviewStatus: "Approved",
      extractedValue: "Kimpton Hotels",
      brandId: KIMPTON,
    },
    {
      id: "recPending2",
      fieldName: "be.positioning.summary",
      humanReviewStatus: "Pending",
      extractionType: "Directly Stated",
      confidenceLevel: "High",
      evidenceText: "Boutique",
      extractedValue: "Boutique lifestyle",
      brandId: KIMPTON,
    },
  ];
  const summary = summarizeFactStatuses(facts);
  assert.equal(summary.approved, 1);
  assert.equal(summary.pendingCandidates, 1);
  const rec = recommendGovernanceFacts(facts, "brand");
  const ids = rec.map((r) => r.id);
  assert.ok(!ids.includes("recApproved"));
  assert.ok(ids.includes("recPending2"));
}

// 5. Curio-like wrong-brand rejected identity facts are excluded
{
  const facts = [
    {
      id: "recHXBcC5nZD4yx6b",
      fieldName: "be.identity.brandName",
      humanReviewStatus: "Rejected",
      reviewerNotes: "Quarantined — Kimpton/IHG wrong-brand contamination",
      extractionValue: "Kimpton Hotels & Restaurants",
      extractedValue: "Kimpton Hotels & Restaurants",
      evidenceText: "Kimpton Hotels",
      brandId: CURIO,
      sourceRecordId: "recIH5lyY8MASnfrp",
    },
    {
      id: "recFC00O4aDSA2l8e",
      fieldName: "be.identity.parentCompany",
      humanReviewStatus: "Rejected",
      reviewerNotes: "Do not approve — contamination",
      extractedValue: "InterContinental Hotels Group",
      brandId: CURIO,
    },
    {
      id: "recCurioClean",
      fieldName: "be.identity.brandName",
      humanReviewStatus: "Pending",
      extractionType: "Directly Stated",
      confidenceLevel: "High",
      evidenceText: "Curio Collection",
      extractedValue: "Curio Collection by Hilton",
      brandId: CURIO,
    },
  ];
  const rec = recommendGovernanceFacts(facts, "brand", { stewardSourceIds: ["recIH5lyY8MASnfrp"] });
  const ids = rec.map((r) => r.id);
  assert.ok(!ids.includes("recHXBcC5nZD4yx6b"));
  assert.ok(!ids.includes("recFC00O4aDSA2l8e"));
  assert.ok(ids.includes("recCurioClean"));
  const patchRejected = buildSafeFactPatch(facts[0], "brand", CURIO, {
    approvedFactIds: new Set(["recHXBcC5nZD4yx6b"]),
    allowWrites: true,
  });
  assert.equal(patchRejected.patch, null);
  assert.ok(patchRejected.skipped.includes("fact_excluded_from_recommendation"));
}

// rejected facts still appear in diagnostic excluded counts
{
  const facts = [
    { id: "recR1", humanReviewStatus: "Rejected", fieldName: "be.identity.brandName" },
    { id: "recP1", humanReviewStatus: "Pending", fieldName: "be.positioning.summary" },
  ];
  const summary = summarizeFactStatuses(facts);
  assert.equal(summary.excluded, 1);
  assert.equal(summary.pendingCandidates, 1);
  assert.equal(summary.excludedFacts[0].id, "recR1");
}

console.log("test-partner-intelligence-stewardship-package: ok");
