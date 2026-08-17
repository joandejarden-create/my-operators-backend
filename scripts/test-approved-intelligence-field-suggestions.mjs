#!/usr/bin/env node
/**
 * Unit checks for approved-intelligence-field-suggestions v1.
 */
import assert from "node:assert/strict";
import { PUBLISH_MODES } from "../lib/partner-intelligence/approved-intelligence-field-publishing.js";
import {
  RISK_LEVELS,
  assessSuggestionRisk,
  buildFieldSuggestionsFromAudit,
  mappingToSuggestion,
  rejectSuggestionsApplyFlags,
} from "../lib/partner-intelligence/approved-intelligence-field-suggestions.js";

const baseAudit = {
  auditVersion: "v1",
  generatedAt: new Date().toISOString(),
  entityType: "operator",
  targetRecId: "recTEST0000000001",
  entityName: "Test Operator",
  governance: { displayAllowed: true },
  mappings: [],
  excludedFacts: [{ factId: "recPEND", fieldKey: "op.snapshot.companyDescription", humanReviewStatus: "Pending" }],
};

const source = {
  id: "recSRC1",
  approvedForExplorerUse: "Yes",
  sourceTitle: "Official site",
  sourceType: "Website Capture",
};

function mapping(overrides = {}) {
  return {
    factId: "recFACT1",
    fieldKey: "op.markets.regionsSupported",
    displayLabel: "Regions Supported",
    approvedValue: "CALA, Mexico",
    proposedValue: "CALA, Mexico",
    sourceId: "recSRC1",
    destinationTable: "Operator Setup - Platform & Markets",
    destinationField: "specificMarkets",
    destinationFieldType: "longText",
    liveValue: null,
    liveValuePopulated: false,
    publishMode: PUBLISH_MODES.controlledPublishCandidate,
    blockers: ["staging_preferred_blank_destination"],
    policy: null,
    ...overrides,
  };
}

// Controlled publish candidates become suggestions
{
  const audit = {
    ...baseAudit,
    mappings: [
      mapping(),
      mapping({
        factId: "recFACT2",
        fieldKey: "op.snapshot.companyDescription",
        publishMode: PUBLISH_MODES.suggestedUpdate,
        liveValue: "Existing text",
        liveValuePopulated: true,
        blockers: ["destination_field_populated"],
      }),
      mapping({
        factId: "recFACT3",
        fieldKey: "op.custom.unknown",
        publishMode: PUBLISH_MODES.evidenceOnly,
        blockers: ["v1_evidence_only_registry"],
      }),
      mapping({
        factId: "recFACT4",
        fieldKey: "op.dealFit.bestFitOwnerTypes",
        publishMode: PUBLISH_MODES.blocked,
        blockers: ["deal_fit_or_scoring_inference_risk"],
      }),
    ],
  };
  const report = buildFieldSuggestionsFromAudit(audit, [source], []);
  assert.equal(report.suggestions.length, 2);
  assert.equal(report.controlledPublishCandidates.length, 1);
  assert.equal(report.suggestedOnlyUpdates.length, 1);
  assert.equal(report.excludedMappings.length, 2);
  assert.equal(report.excludedFacts.length, 1);
}

// Non-empty destinations → Medium or High, not Low
{
  const risk = assessSuggestionRisk(
    mapping({
      liveValue: "CALA",
      liveValuePopulated: true,
      publishMode: PUBLISH_MODES.suggestedUpdate,
      blockers: ["destination_field_populated"],
    }),
    source,
    { displayAllowed: true }
  );
  assert.notEqual(risk.level, RISK_LEVELS.low);
}

{
  const risk = assessSuggestionRisk(mapping(), source, { displayAllowed: true });
  assert.equal(risk.level, RISK_LEVELS.low);
}

// Identity field → High
{
  const risk = assessSuggestionRisk(
    mapping({
      fieldKey: "op.snapshot.companyName",
      liveValue: "GHL Hoteles (GHL Holding)",
      liveValuePopulated: true,
      publishMode: PUBLISH_MODES.suggestedUpdate,
      blockers: ["identity_field_populated_no_overwrite"],
      policy: { identityField: true },
    }),
    source,
    {}
  );
  assert.equal(risk.level, RISK_LEVELS.high);
}

// No apply mode
{
  assert.equal(rejectSuggestionsApplyFlags(["node", "--apply"]).rejected, true);
  assert.equal(rejectSuggestionsApplyFlags(["node", "--plan"]).rejected, false);
}

// mappingToSuggestion required fields
{
  const s = mappingToSuggestion(mapping(), {
    entityType: "operator",
    targetRecId: "reciI2tYQBfMoMK9G",
    entityName: "GHL",
    sourceById: new Map([["recSRC1", source]]),
    factById: new Map([["recFACT1", { evidenceText: "From homepage" }]]),
    governance: { displayAllowed: true },
  });
  assert.equal(s.entityType, "operator");
  assert.equal(s.sourceFactId, "recFACT1");
  assert.ok(s.proposedValue);
  assert.ok(s.recommendation);
}

console.log("test-approved-intelligence-field-suggestions: ok");
