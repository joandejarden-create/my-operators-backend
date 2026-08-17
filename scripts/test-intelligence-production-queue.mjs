#!/usr/bin/env node
/**
 * Unit checks for intelligence-production-queue v1.1 helpers.
 */
import assert from "node:assert/strict";
import {
  PRIORITY_QUEUE_ENTRIES,
  buildProductionQueue,
  buildQueueEntryFromPlan,
  buildUnresolvedQueueEntry,
  categorizeQueueEntry,
  filterQueueEntries,
  isPlatformReady,
  rejectQueueApplyFlags,
  summarizeQueue,
} from "../lib/partner-intelligence/intelligence-production-queue.js";

// Apply flags rejected
{
  const r = rejectQueueApplyFlags(["node", "script", "--apply"]);
  assert.equal(r.rejected, true);
  const ok = rejectQueueApplyFlags(["node", "script", "--plan"]);
  assert.equal(ok.rejected, false);
}

// Stage 8 entities in batch report
{
  const stage8Plan = {
    entityName: "GHL Hoteles",
    entityType: "operator",
    targetRecId: "reciI2tYQBfMoMK9G",
    currentStage: { stageId: 8, stageKey: "platform_usage", stageLabel: "Platform usage" },
    sourceCounts: { total: 6, approvedExplorer: 5 },
    factCounts: { total: 7, approved: 5 },
    governance: {
      live: {
        validationStatus: "Company Published",
        externalDisplayStatus: "Show Trust Label",
        companyValidated: false,
      },
      changeClass: "no_op",
      eligible: true,
      expectedChip: { displayLabel: "AI-Assisted Profile" },
    },
    blockers: { workflow: [], publishScope: [], labels: [] },
    nextCommands: ["npm run audit-partner-intelligence-publish-readiness"],
    completeness: { tier: "useful" },
  };
  const entry = buildQueueEntryFromPlan(stage8Plan);
  assert.equal(entry.currentStage.stageId, 8);
  assert.equal(entry.readyForPlatformUsage, true);
  assert.equal(isPlatformReady(entry), true);

  const queue = buildProductionQueue({
    entries: [entry, buildUnresolvedQueueEntry(PRIORITY_QUEUE_ENTRIES[5])],
  });
  assert.equal(queue.platformReady.length, 1);
  assert.ok(summarizeQueue(queue.entries).completeStage8 >= 1);
}

// Kimpton-style downgrade protection → platform ready at stage 8
{
  const kimptonPlan = {
    entityName: "Kimpton Hotels",
    entityType: "brand",
    targetRecId: "recCKuXCmGvxHPfb3",
    currentStage: {
      stageId: 8,
      stageKey: "platform_usage",
      stageLabel: "Platform usage",
      blockers: ["stronger_live_governance_preserved"],
    },
    sourceCounts: { total: 4, approvedExplorer: 4 },
    factCounts: { total: 48, approved: 4 },
    governance: {
      live: {
        validationStatus: "Company Published",
        externalDisplayStatus: "Show Trust Label",
        companyValidated: false,
      },
      changeClass: "downgrade",
      eligible: false,
      normalized: { displayLabel: "Source-Informed Profile" },
    },
    blockers: {
      workflow: ["stronger_live_governance_preserved"],
      publishScope: ["would_downgrade_existing_validation"],
      labels: [],
    },
    nextCommands: ["npm run audit-partner-intelligence-publish-readiness"],
    completeness: { tier: "sparse" },
  };
  const entry = buildQueueEntryFromPlan(kimptonPlan);
  assert.equal(entry.status, "platform_ready");
  assert.equal(entry.readyForPlatformUsage, true);
  assert.equal(isPlatformReady(entry), true);
  assert.equal(categorizeQueueEntry(entry), "complete");
}

// Blocked entities show blockers
{
  const blockedPlan = {
    entityName: "Test Brand",
    entityType: "brand",
    targetRecId: "recTESTblocked01",
    currentStage: { stageId: 1, stageKey: "source_discovery", blockers: ["no_linked_sources"] },
    sourceCounts: { total: 0, approvedExplorer: 0 },
    factCounts: { total: 0, approved: 0 },
    governance: { live: {}, changeClass: null, eligible: false },
    blockers: { workflow: ["no_linked_sources"], publishScope: [], labels: [] },
    nextCommands: ["npm run partner-reference:init-folder -- --dry-run"],
    completeness: { tier: "empty" },
  };
  const entry = buildQueueEntryFromPlan(blockedPlan);
  assert.ok(entry.allBlockers.includes("no_linked_sources"));
  assert.equal(categorizeQueueEntry(entry), "needs_sources");
  assert.equal(isPlatformReady(entry), false);
}

// Missing record does not crash — unresolved entry
{
  const unresolved = buildUnresolvedQueueEntry({
    entityName: "Hilton Garden Inn",
    entityType: "brand",
    targetRecId: null,
    trackerPriority: "next",
    unresolvedReason: "Record ID TBD in tracker",
  });
  assert.equal(unresolved.resolved, false);
  assert.equal(unresolved.status, "unresolved");
  const queue = buildProductionQueue({ entries: [unresolved] });
  assert.equal(queue.summary.unresolvedCount, 1);
}

// Filters
{
  const entries = [
    buildQueueEntryFromPlan({
      entityName: "Op A",
      entityType: "operator",
      targetRecId: "recA",
      currentStage: { stageId: 8, stageKey: "platform_usage" },
      sourceCounts: { total: 1, approvedExplorer: 1 },
      factCounts: { total: 2, approved: 2 },
      governance: { live: {}, changeClass: "no_op", eligible: true },
      blockers: { workflow: [], publishScope: [], labels: [] },
      nextCommands: [],
      completeness: { tier: "sparse" },
    }),
    buildQueueEntryFromPlan({
      entityName: "Brand B",
      entityType: "brand",
      targetRecId: "recB",
      currentStage: { stageId: 4, stageKey: "extraction_dry_run" },
      sourceCounts: { total: 2, approvedExplorer: 1 },
      factCounts: { total: 0, approved: 0 },
      governance: { live: {}, changeClass: null, eligible: false },
      blockers: { workflow: ["no_extracted_facts"], publishScope: [], labels: [] },
      nextCommands: ["npm run hotel-equities-extract -- --dry-run"],
      completeness: { tier: "sources_only" },
    }),
  ];
  const ops = filterQueueEntries(entries, { entityType: "operator" });
  assert.equal(ops.length, 1);
  const ready = filterQueueEntries(entries, { readyOnly: true });
  assert.equal(ready.length, 1);
  const blocked = filterQueueEntries(entries, { blockedOnly: true });
  assert.ok(blocked.length >= 1);
}

assert.ok(PRIORITY_QUEUE_ENTRIES.length >= 10);

console.log("test-intelligence-production-queue: ok");
