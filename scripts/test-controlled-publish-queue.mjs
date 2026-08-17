#!/usr/bin/env node
/**
 * Unit checks for controlled-publish-queue v2.1.
 */
import assert from "node:assert/strict";
import { PUBLISH_MODES } from "../lib/partner-intelligence/approved-intelligence-field-publishing.js";
import { RISK_LEVELS } from "../lib/partner-intelligence/approved-intelligence-field-suggestions.js";
import {
  QUEUE_STATUSES,
  classifyPublishQueueItem,
  buildEntityControlledPublishQueueEntry,
  buildUnresolvedEntityEntry,
  filterQueueEntries,
  rejectControlledPublishQueueApplyFlags,
  summarizeControlledPublishQueue,
} from "../lib/partner-intelligence/controlled-publish-queue.js";

const ghlPopulatedSuggestion = {
  sourceFactId: "reccszsLnWjA5fPnp",
  factKey: "op.markets.regionsSupported",
  destinationField: "specificMarkets",
  publishMode: PUBLISH_MODES.suggestedUpdate,
  classification: PUBLISH_MODES.suggestedUpdate,
  riskLevel: RISK_LEVELS.medium,
  liveValuePopulated: true,
  currentLiveValue: "Latin America, Colombia, Peru, Chile, Guatemala",
  blockers: ["destination_field_populated"],
};

// GHL post-publish: specificMarkets is already published, not ready
{
  const c = classifyPublishQueueItem(ghlPopulatedSuggestion, null, "operator");
  assert.equal(c.status, QUEUE_STATUSES.alreadyPublished);
}

// Ready only when Low + blank + allowlisted
{
  const readySuggestion = {
    sourceFactId: "recFACT1",
    factKey: "op.markets.regionsSupported",
    destinationField: "specificMarkets",
    publishMode: PUBLISH_MODES.controlledPublishCandidate,
    riskLevel: RISK_LEVELS.low,
    liveValuePopulated: false,
    blockers: [],
  };
  const c = classifyPublishQueueItem(readySuggestion, null, "operator");
  assert.equal(c.status, QUEUE_STATUSES.readyForControlledPublish);
}

// Medium not ready
{
  const c = classifyPublishQueueItem(
    {
      ...ghlPopulatedSuggestion,
      liveValuePopulated: false,
      publishMode: PUBLISH_MODES.suggestedUpdate,
      riskLevel: RISK_LEVELS.medium,
      blockers: ["select_option_validation_required"],
    },
    null,
    "operator"
  );
  assert.equal(c.status, QUEUE_STATUSES.needsStewardReview);
}

// High risk identity → needs steward review (blank destination path)
{
  const c = classifyPublishQueueItem(
    {
      factKey: "op.snapshot.companyName",
      destinationField: "company_name",
      publishMode: PUBLISH_MODES.suggestedUpdate,
      riskLevel: RISK_LEVELS.high,
      liveValuePopulated: false,
      blockers: ["identity_field_populated_no_overwrite"],
    },
    null,
    "operator"
  );
  assert.equal(c.status, QUEUE_STATUSES.needsStewardReview);
}

// Entity entry: GHL has 0 ready, 1 already published
{
  const audit = {
    mappings: [
      {
        factId: "reccszsLnWjA5fPnp",
        fieldKey: "op.markets.regionsSupported",
        publishMode: PUBLISH_MODES.suggestedUpdate,
        liveValuePopulated: true,
      },
    ],
    summary: {
      approvedFacts: 5,
      suggestedUpdate: 5,
      controlledPublishCandidate: 0,
      blocked: 0,
      evidenceOnly: 0,
    },
  };
  const suggestionsReport = {
    governance: { displayAllowed: true },
    suggestions: [ghlPopulatedSuggestion],
    excludedFacts: [],
  };
  const entry = buildEntityControlledPublishQueueEntry({
    entityType: "operator",
    targetRecId: "reciI2tYQBfMoMK9G",
    entityName: "GHL Hoteles",
    audit,
    suggestionsReport,
    sources: [],
    facts: [{ id: "reccszsLnWjA5fPnp", humanReviewStatus: "Approved" }],
  });
  assert.equal(entry.summary.readyForControlledPublish, 0);
  assert.equal(entry.summary.alreadyPublished, 1);
}

// Unresolved does not crash
{
  const u = buildUnresolvedEntityEntry({
    entityName: "Aimbridge",
    entityType: "operator",
    targetRecId: null,
  });
  assert.equal(u.resolved, false);
  const summary = summarizeControlledPublishQueue([u]);
  assert.equal(summary.unresolved, 1);
}

// Apply rejected
{
  assert.equal(rejectControlledPublishQueueApplyFlags(["node", "--apply"]).rejected, true);
}

// ready-only filter
{
  const entries = [
    {
      resolved: true,
      entityType: "operator",
      readyItems: [{ riskLevel: "Low" }],
      items: [{ riskLevel: "Low" }],
      summary: { readyForControlledPublish: 1 },
    },
    {
      resolved: true,
      entityType: "operator",
      readyItems: [],
      items: [],
      summary: { readyForControlledPublish: 0 },
    },
  ];
  assert.equal(filterQueueEntries(entries, { readyOnly: true }).length, 1);
}

console.log("test-controlled-publish-queue: ok");
