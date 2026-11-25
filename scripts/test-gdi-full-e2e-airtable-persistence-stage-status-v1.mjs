#!/usr/bin/env node
/**
 * Permanent gates for research-persistence stage status + operation classes.
 */
import assert from "node:assert/strict";
import {
  classifyResearchPersistenceOperation,
  classifyResearchPersistenceStageStatus,
  isResearchPersistenceCertified,
  RESEARCH_PERSISTENCE_OPERATION_CLASS,
  RESEARCH_PERSISTENCE_STAGE_STATUS,
  persistFullE2eResearchInventory,
  evaluateResearchPersistenceAuthorization,
} from "../lib/group-demand-intelligence/workflow/gdi-full-e2e-research-persistence-bridge-v1.js";

assert.equal(
  classifyResearchPersistenceStageStatus(null, { requested: false }),
  RESEARCH_PERSISTENCE_STAGE_STATUS.NOT_REQUESTED,
);

assert.equal(
  classifyResearchPersistenceStageStatus(
    { dryRun: true, ok: true, counts: { FAILED: 0 }, rows: [] },
    { requested: true },
  ),
  RESEARCH_PERSISTENCE_STAGE_STATUS.DRY_RUN,
);

assert.equal(
  classifyResearchPersistenceStageStatus(
    {
      dryRun: false,
      ok: true,
      counts: { CREATED: 1, UPDATED: 0, FAILED: 0 },
      readBack: [{ matched: true }],
    },
    { requested: true, requireReadBack: true },
  ),
  RESEARCH_PERSISTENCE_STAGE_STATUS.PERSISTED_AND_VERIFIED,
);

assert.equal(
  isResearchPersistenceCertified(
    { dryRun: true, ok: true, counts: {}, rows: [] },
    { requireReadBack: true },
  ),
  false,
);

assert.equal(
  classifyResearchPersistenceOperation({
    disposition: "DISQUALIFIED_WITH_EVIDENCE",
  }),
  RESEARCH_PERSISTENCE_OPERATION_CLASS.DISQUALIFIED_OR_HISTORICAL,
);

assert.equal(
  classifyResearchPersistenceOperation({
    disposition: "CONTACT_RESEARCH_REQUIRED",
    researchContinuation: true,
  }),
  RESEARCH_PERSISTENCE_OPERATION_CLASS.RESEARCH_CONTINUATION,
);

assert.equal(
  classifyResearchPersistenceOperation({
    reason: "DUPLICATE_COMPOSITE_KEY_IN_RUN",
    action: "DUPLICATE",
  }),
  RESEARCH_PERSISTENCE_OPERATION_CLASS.DUPLICATE_OR_MERGED,
);

const locked = evaluateResearchPersistenceAuthorization({
  apply: true,
  authorizeResearchPersistence: true,
  exactWriteSetAuthorization: "x",
});
assert.equal(locked.productionUnlocked, false);

const dry = await persistFullE2eResearchInventory({
  hotelId: "recLuxvwwxID7U2B8",
  runId: "stage-status-unit",
  apply: false,
  opportunities: [],
});
assert.equal(dry.stageStatus, RESEARCH_PERSISTENCE_STAGE_STATUS.DRY_RUN);
assert.equal(dry.persistenceCertified, false);
assert.equal(dry.gdoWrites, 0);
assert.equal(dry.publicationWrites, 0);

console.log("OK test-gdi-full-e2e-airtable-persistence-stage-status-v1");
