#!/usr/bin/env node
/**
 * Unit checks for controlled-platform-field-publishing v2.
 */
import assert from "node:assert/strict";
import { PUBLISH_MODES } from "../lib/partner-intelligence/approved-intelligence-field-publishing.js";
import { RISK_LEVELS } from "../lib/partner-intelligence/approved-intelligence-field-suggestions.js";
import {
  APPROVAL_CLI_FLAG,
  CORRECTION_APPROVAL_CLI_FLAG,
  V2_ALLOWED_OPERATOR_DESTINATIONS,
  validateControlledPublishGates,
  validateControlledPublishCorrectionGates,
  buildSuggestionKey,
} from "../lib/partner-intelligence/controlled-platform-field-publishing.js";

const targetRecId = "reciI2tYQBfMoMK9G";
const factId = "reccszsLnWjA5fPnp";
const factKey = "op.markets.regionsSupported";

const ghlSuggestion = {
  suggestionId: buildSuggestionKey(targetRecId, factId, factKey),
  entityType: "operator",
  targetRecId,
  entityName: "GHL Hoteles",
  sourceFactId: factId,
  sourceId: "recoOcRjSD3VZb3qt",
  factKey,
  classification: PUBLISH_MODES.controlledPublishCandidate,
  publishMode: PUBLISH_MODES.controlledPublishCandidate,
  riskLevel: RISK_LEVELS.low,
  destinationTable: V2_ALLOWED_OPERATOR_DESTINATIONS.specificMarkets.destinationTable,
  destinationField: "specificMarkets",
  proposedValue: "Latin America, Colombia, Peru, Chile, Guatemala",
  approvedValue: "Latin America, Colombia, Peru, Chile, Guatemala",
};

const approvedFact = {
  id: factId,
  humanReviewStatus: "Approved",
  fieldName: factKey,
  sourceRecordId: "recoOcRjSD3VZb3qt",
};

const approvedSource = {
  id: "recoOcRjSD3VZb3qt",
  approvedForExplorerUse: "Yes",
};

function baseInput(overrides = {}) {
  return {
    entityType: "operator",
    targetRecId,
    destinationFieldKey: "specificMarkets",
    suggestion: ghlSuggestion,
    fact: approvedFact,
    source: approvedSource,
    governance: { displayAllowed: true },
    liveValue: "",
    applyRequested: false,
    approvalPresent: false,
    factId,
    suggestionKey: null,
    ...overrides,
  };
}

// GHL-style low-risk suggestion produces apply plan on dry-run
{
  const v = validateControlledPublishGates(baseInput());
  assert.equal(v.ok, true);
  assert.equal(v.plan.mode, "dry-run");
  assert.equal(v.plan.destinationField, "specificMarkets");
}

// Apply without approval token rejected
{
  const v = validateControlledPublishGates(
    baseInput({ applyRequested: true, approvalPresent: false })
  );
  assert.equal(v.ok, false);
  assert.ok(v.failures.includes("apply_without_approval_token"));
}

// Non-low-risk blocked
{
  const v = validateControlledPublishGates(
    baseInput({
      suggestion: { ...ghlSuggestion, riskLevel: RISK_LEVELS.medium },
    })
  );
  assert.equal(v.ok, false);
  assert.ok(v.failures.includes("risk_not_low"));
}

// Populated destination blocked
{
  const v = validateControlledPublishGates(baseInput({ liveValue: "CALA" }));
  assert.equal(v.ok, false);
  assert.ok(v.failures.includes("destination_not_blank"));
}

// Unsupported destination field blocked
{
  const v = validateControlledPublishGates(
    baseInput({ destinationFieldKey: "companyDescription" })
  );
  assert.equal(v.ok, false);
  assert.ok(v.failures.includes("destination_not_allowlisted"));
}

// Company Validated blocked
{
  const v = validateControlledPublishGates(
    baseInput({
      destinationFieldKey: "specificMarkets",
      suggestion: {
        ...ghlSuggestion,
        destinationField: "Company Validated",
        factKey: "op.markets.regionsSupported",
      },
    })
  );
  assert.equal(v.ok, false);
}

// Identity field blocked
{
  const v = validateControlledPublishGates(
    baseInput({
      suggestion: {
        ...ghlSuggestion,
        factKey: "op.snapshot.companyName",
        classification: PUBLISH_MODES.controlledPublishCandidate,
        destinationField: "company_name",
      },
      fact: { ...approvedFact, fieldName: "op.snapshot.companyName" },
    })
  );
  assert.equal(v.ok, false);
  assert.ok(v.failures.includes("identity_field_blocked"));
}

// Pending fact blocked
{
  const v = validateControlledPublishGates(
    baseInput({ fact: { ...approvedFact, humanReviewStatus: "Pending" } })
  );
  assert.equal(v.ok, false);
  assert.ok(v.failures.includes("fact_not_approved"));
}

// Unsupported entity type blocked
{
  const v = validateControlledPublishGates(baseInput({ entityType: "brand" }));
  assert.equal(v.ok, false);
  assert.ok(v.failures.includes("unsupported_entity_type"));
}

// Default dry-run: apply not requested
{
  const v = validateControlledPublishGates(baseInput());
  assert.equal(v.plan.mode, "dry-run");
}

// Apply plan when both flags set
{
  const v = validateControlledPublishGates(
    baseInput({ applyRequested: true, approvalPresent: true })
  );
  assert.equal(v.ok, true);
  assert.equal(v.plan.mode, "apply");
}

assert.equal(APPROVAL_CLI_FLAG, "--approve-controlled-field-publish");
assert.equal(CORRECTION_APPROVAL_CLI_FLAG, "--approve-controlled-field-correction");

// Correction: populated destination + reason + correct value → dry-run eligible
{
  const v = validateControlledPublishCorrectionGates({
    entityType: "operator",
    targetRecId,
    destinationFieldKey: "specificMarkets",
    correctValue: "Colombia, Chile, Guatemala, Peru",
    reason: "Destinations page lists specific countries only.",
    liveValue: "Latin America, Colombia, Peru, Chile, Guatemala",
    applyRequested: false,
    approvalPresent: false,
  });
  assert.equal(v.ok, true);
  assert.equal(v.plan.mode, "correction-dry-run");
  assert.equal(v.plan.previousValue, "Latin America, Colombia, Peru, Chile, Guatemala");
}

// Correction: blank destination blocked
{
  const v = validateControlledPublishCorrectionGates({
    entityType: "operator",
    targetRecId,
    destinationFieldKey: "specificMarkets",
    correctValue: "Colombia",
    reason: "test",
    liveValue: "",
    applyRequested: false,
    approvalPresent: false,
  });
  assert.equal(v.ok, false);
  assert.ok(v.failures.includes("destination_not_populated"));
}

// Correction: apply without approval token blocked
{
  const v = validateControlledPublishCorrectionGates({
    entityType: "operator",
    targetRecId,
    destinationFieldKey: "specificMarkets",
    correctValue: "Colombia, Chile, Guatemala, Peru",
    reason: "test",
    liveValue: "Latin America, Colombia, Peru, Chile, Guatemala",
    applyRequested: true,
    approvalPresent: false,
  });
  assert.equal(v.ok, false);
  assert.ok(v.failures.includes("apply_without_correction_approval_token"));
}

// Correction: missing reason blocked
{
  const v = validateControlledPublishCorrectionGates({
    entityType: "operator",
    targetRecId,
    destinationFieldKey: "specificMarkets",
    correctValue: "Colombia",
    reason: "",
    liveValue: "existing",
    applyRequested: false,
    approvalPresent: false,
  });
  assert.equal(v.ok, false);
  assert.ok(v.failures.includes("missing_correction_reason"));
}

console.log("test-controlled-platform-field-publishing: ok");
