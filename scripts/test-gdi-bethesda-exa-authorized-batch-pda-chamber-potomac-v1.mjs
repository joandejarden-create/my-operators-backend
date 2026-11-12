#!/usr/bin/env node
/**
 * Zero-paid gate: allowlist, spend guard, dry-run path.
 */
import assert from "node:assert/strict";
import {
  AUTHORIZED_TARGETS,
  AUTHORIZATION_ID,
  SPEND_CEILING,
  validateAuthorizedTargets,
  executeBethesdaExaAuthorizedBatch,
} from "../lib/group-demand-intelligence/research/bethesda-exa-authorized-batch-pda-chamber-potomac-v1.js";
import { BETHESDA_29_ATTESTED_IDS } from "../lib/group-demand-intelligence/research/bethesda-historical-attestation-bag-v1.js";
import { BETHESDA_CANONICAL_HOTEL_ID } from "../lib/group-demand-intelligence/research/bethesda-full-demand-database-reconciliation-v1.js";

assert.equal(AUTHORIZED_TARGETS.length, 3);
assert.equal(SPEND_CEILING.maxPaidCalls, 3);
assert.equal(SPEND_CEILING.maxUsd, 12);
assert.ok(AUTHORIZATION_ID.includes("PDA_CHAMBER_POTOMAC"));

for (const t of AUTHORIZED_TARGETS) {
  assert.ok(BETHESDA_29_ATTESTED_IDS.includes(t.id), `missing from 29: ${t.id}`);
}

const pre = validateAuthorizedTargets();
assert.equal(pre.ok, true, JSON.stringify(pre.issues));
assert.equal(pre.resolved.length, 3);
assert.ok(
  pre.resolved.every((r) => r.row.hotelId === BETHESDA_CANONICAL_HOTEL_ID)
);

const dry = await executeBethesdaExaAuthorizedBatch({
  dryRun: true,
  authorizeExaAgentLive: false,
});
assert.equal(dry.ok, true);
assert.equal(dry.ledger.paidCallsIssued, 0);
assert.equal(dry.ledger.spentUsd, 0);
assert.equal(dry.ledger.status, "DRY_RUN_COMPLETE");
assert.equal(dry.ledger.entries.length, 3);
assert.ok(dry.ledger.entries.every((e) => e.status === "DRY_RUN_NOT_EXECUTED"));

console.log(
  JSON.stringify(
    {
      ok: true,
      test: "test-gdi-bethesda-exa-authorized-batch-pda-chamber-potomac-v1",
      allowlistIds: AUTHORIZED_TARGETS.map((t) => t.id),
    },
    null,
    2
  )
);
