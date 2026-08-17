#!/usr/bin/env node
/**
 * Unit checks for approved-intelligence-field-publishing v1.
 */
import assert from "node:assert/strict";
import {
  PUBLISH_MODES,
  classifyFactMapping,
  rejectFieldPublishingApplyFlags,
  resolveDestination,
} from "../lib/partner-intelligence/approved-intelligence-field-publishing.js";
import { getRegistryField } from "../api/lib/partner-intelligence-explorer-field-registry.js";

const approvedSource = { id: "recSRC1", approvedForExplorerUse: "Yes", status: "Extracted" };
const approvedFact = {
  id: "recFACT1",
  fieldName: "op.snapshot.companyDescription",
  humanReviewStatus: "Approved",
  approvedValue: "Hotel operator with 35 hotels.",
  sourceRecordId: "recSRC1",
};

// Approved facts map to suggestions when destination populated
{
  const reg = getRegistryField("op.snapshot.companyDescription", "Operator Explorer");
  const dest = resolveDestination(reg, "operator");
  const result = classifyFactMapping({
    fact: approvedFact,
    source: approvedSource,
    entityType: "operator",
    registry: reg,
    destination: dest,
    liveValue: "Existing curated description.",
    governance: { live: { usagePermission: "Platform Display Allowed" } },
  });
  assert.equal(result.mode, PUBLISH_MODES.suggestedUpdate);
  assert.ok(result.blockers.includes("destination_field_populated"));
}

// Pending facts excluded via classify (blocked)
{
  const reg = getRegistryField("op.snapshot.companyDescription", "Operator Explorer");
  const dest = resolveDestination(reg, "operator");
  const result = classifyFactMapping({
    fact: { ...approvedFact, humanReviewStatus: "Pending" },
    source: approvedSource,
    entityType: "operator",
    registry: reg,
    destination: dest,
    liveValue: "",
    governance: {},
  });
  assert.equal(result.mode, PUBLISH_MODES.blocked);
  assert.ok(result.blockers.some((b) => b.startsWith("fact_status_")));
}

// Company Validated destinations blocked
{
  const result = classifyFactMapping({
    fact: approvedFact,
    source: approvedSource,
    entityType: "operator",
    registry: getRegistryField("op.snapshot.companyDescription", "Operator Explorer"),
    destination: {
      destinationTable: "Operator Setup - Master",
      destinationField: "Company Validated",
    },
    liveValue: "",
    governance: {},
  });
  assert.equal(result.mode, PUBLISH_MODES.blocked);
  assert.ok(result.blockers.includes("destination_governance_field"));
}

// Non-empty destination → suggested update, not controlled publish
{
  const reg = getRegistryField("op.markets.regionsSupported", "Operator Explorer");
  const dest = resolveDestination(reg, "operator");
  const result = classifyFactMapping({
    fact: {
      ...approvedFact,
      fieldName: "op.markets.regionsSupported",
      approvedValue: "CALA, Mexico",
    },
    source: approvedSource,
    entityType: "operator",
    registry: reg,
    destination: dest,
    liveValue: "CALA",
    governance: { live: { usagePermission: "Platform Display Allowed" } },
  });
  assert.equal(result.mode, PUBLISH_MODES.suggestedUpdate);
}

// Blank destination + approved → controlled publish candidate
{
  const reg = getRegistryField("op.markets.regionsSupported", "Operator Explorer");
  const dest = resolveDestination(reg, "operator");
  const result = classifyFactMapping({
    fact: {
      ...approvedFact,
      fieldName: "op.markets.regionsSupported",
      approvedValue: "CALA, Mexico",
    },
    source: approvedSource,
    entityType: "operator",
    registry: reg,
    destination: dest,
    liveValue: "",
    governance: { live: { usagePermission: "Platform Display Allowed" } },
  });
  assert.equal(result.mode, PUBLISH_MODES.controlledPublishCandidate);
}

// Unsupported registry keys → evidence only when in v1 list, else blocked
{
  const result = classifyFactMapping({
    fact: {
      ...approvedFact,
      fieldName: "op.capabilities.managementServices",
      approvedValue: "Full service",
    },
    source: approvedSource,
    entityType: "operator",
    registry: null,
    destination: null,
    liveValue: "",
    governance: { live: { usagePermission: "Platform Display Allowed" } },
  });
  assert.equal(result.mode, PUBLISH_MODES.evidenceOnly);
}

{
  const result = classifyFactMapping({
    fact: {
      ...approvedFact,
      fieldName: "op.custom.unknownKey",
      approvedValue: "x",
    },
    source: approvedSource,
    entityType: "operator",
    registry: null,
    destination: null,
    liveValue: "",
    governance: {},
  });
  assert.equal(result.mode, PUBLISH_MODES.blocked);
}

{
  const result = classifyFactMapping({
    fact: {
      ...approvedFact,
      fieldName: "op.ownerValueProposition",
      approvedValue: "Value prop",
    },
    source: approvedSource,
    entityType: "operator",
    registry: null,
    destination: null,
    liveValue: "",
    governance: { live: { usagePermission: "Platform Display Allowed" } },
  });
  assert.equal(result.mode, PUBLISH_MODES.evidenceOnly);
}

// No apply flags
{
  assert.equal(rejectFieldPublishingApplyFlags(["node", "--apply"]).rejected, true);
  assert.equal(rejectFieldPublishingApplyFlags(["node", "--plan"]).rejected, false);
}

console.log("test-approved-intelligence-field-publishing: ok");
