#!/usr/bin/env node
/**
 * Unit checks for approved-fact-correction v1.
 */
import assert from "node:assert/strict";
import {
  APPROVAL_CLI_FLAG,
  CORRECTION_PATCH_ALLOWLIST,
  buildFactCorrectionPatch,
  buildReviewerNotesEntry,
  getFactDisplayValue,
  validateApprovedFactCorrectionGates,
} from "../lib/partner-intelligence/approved-fact-correction.js";
import { MAP_PARTNER_FACT } from "../api/lib/partner-intelligence-field-map.js";

const ghlFact = {
  id: "reccszsLnWjA5fPnp",
  fieldName: "op.markets.regionsSupported",
  operatorId: "reciI2tYQBfMoMK9G",
  sourceRecordId: "recoOcRjSD3VZb3qt",
  extractedValue: "Destinations in Latin America",
  approvedValue: "Latin America, Colombia, Peru, Chile, Guatemala",
  humanReviewStatus: "Approved",
  reviewerNotes: "",
};

const correctValue = "Colombia, Chile, Guatemala, Peru";
const reason =
  "Destinations page lists specific markets as Colombia, Chile, Guatemala, and Peru; Latin America is regional context.";

function baseInput(overrides = {}) {
  return {
    fact: ghlFact,
    correctValue,
    reason,
    applyRequested: false,
    approvalPresent: false,
    evidenceSourceId: "reckrUB2WmnSm02g3",
    ...overrides,
  };
}

// Dry-run eligible
{
  const v = validateApprovedFactCorrectionGates(baseInput());
  assert.equal(v.ok, true);
  assert.equal(v.plan.mode, "dry-run");
  assert.equal(v.recommendedHumanReviewStatus, "Edited");
  assert.ok(v.patchPreview);
  assert.equal(v.patchPreview[MAP_PARTNER_FACT.approvedValue], correctValue);
  assert.ok(!v.patchPreview[MAP_PARTNER_FACT.extractedValue]);
}

// Apply without approval token rejected
{
  const v = validateApprovedFactCorrectionGates(
    baseInput({ applyRequested: true, approvalPresent: false })
  );
  assert.equal(v.ok, false);
  assert.ok(v.failures.includes("apply_without_correction_approval_token"));
}

// Pending fact blocked
{
  const v = validateApprovedFactCorrectionGates(
    baseInput({ fact: { ...ghlFact, humanReviewStatus: "Pending" } })
  );
  assert.equal(v.ok, false);
  assert.ok(v.failures.includes("fact_review_status_not_correctable"));
}

// Rejected fact blocked
{
  const v = validateApprovedFactCorrectionGates(
    baseInput({ fact: { ...ghlFact, humanReviewStatus: "Rejected" } })
  );
  assert.equal(v.ok, false);
}

// Empty corrected value blocked
{
  const v = validateApprovedFactCorrectionGates(baseInput({ correctValue: "" }));
  assert.equal(v.ok, false);
  assert.ok(v.failures.includes("missing_correct_value"));
}

// Same value blocked
{
  const v = validateApprovedFactCorrectionGates(
    baseInput({ correctValue: ghlFact.approvedValue })
  );
  assert.equal(v.ok, false);
  assert.ok(v.failures.includes("correct_value_matches_current_display_value"));
}

// Missing reason blocked
{
  const v = validateApprovedFactCorrectionGates(baseInput({ reason: "" }));
  assert.equal(v.ok, false);
  assert.ok(v.failures.includes("missing_correction_reason"));
}

// Extracted value preserved in patch
{
  const { fields } = buildFactCorrectionPatch(ghlFact, {
    correctValue,
    reason,
    evidenceSourceId: "reckrUB2WmnSm02g3",
  });
  assert.ok(fields[MAP_PARTNER_FACT.approvedValue]);
  assert.equal(fields[MAP_PARTNER_FACT.humanReviewStatus], "Edited");
  assert.ok(fields[MAP_PARTNER_FACT.reviewerNotes].includes("Approved fact correction"));
  assert.ok(fields[MAP_PARTNER_FACT.reviewerNotes].includes("reckrUB2WmnSm02g3"));
  assert.equal(fields[MAP_PARTNER_FACT.extractedValue], undefined);
}

// Reviewer notes append planned
{
  const factWithNotes = { ...ghlFact, reviewerNotes: "Prior steward note." };
  const { fields } = buildFactCorrectionPatch(factWithNotes, { correctValue, reason });
  assert.ok(fields[MAP_PARTNER_FACT.reviewerNotes].startsWith("Prior steward note."));
  assert.ok(fields[MAP_PARTNER_FACT.reviewerNotes].includes("Approved fact correction"));
}

// Patch allowlist only — no platform/governance fields
{
  const { fields } = buildFactCorrectionPatch(ghlFact, { correctValue, reason });
  for (const key of Object.keys(fields)) {
    assert.ok(CORRECTION_PATCH_ALLOWLIST.has(key), `unexpected patch field: ${key}`);
  }
  assert.equal(
    getFactDisplayValue(ghlFact),
    "Latin America, Colombia, Peru, Chile, Guatemala"
  );
}

// Identity field blocked
{
  const v = validateApprovedFactCorrectionGates(
    baseInput({
      fact: {
        ...ghlFact,
        fieldName: "op.snapshot.companyName",
        approvedValue: "GHL",
      },
      correctValue: "GHL Hoteles",
    })
  );
  assert.equal(v.ok, false);
  assert.ok(v.failures.includes("identity_field_correction_blocked"));
}

assert.equal(APPROVAL_CLI_FLAG, "--approve-approved-fact-correction");

console.log("test-approved-fact-correction: ok");
