#!/usr/bin/env node
/**
 * Offline regression: fourteen-segment orchestrator must forward live Exa deep
 * seed flags when --authorize-paid-research is set, and must NOT enable them
 * when unpaid.
 *
 * Does not call Exa. Does not spend.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const ORCH = path.join(
  ROOT,
  "scripts/gdi-full-e2e-fourteen-segment-orchestrator-v1.mjs"
);
const CHAIN = path.join(
  ROOT,
  "lib/group-demand-intelligence/workflow/original-full-e2e-research-chain-v1.js"
);
const RESTORE = path.join(
  ROOT,
  "lib/group-demand-intelligence/workflow/gdi-original-full-e2e-execution-restore-v1.js"
);

const orchSrc = fs.readFileSync(ORCH, "utf8");
const chainSrc = fs.readFileSync(CHAIN, "utf8");
const restoreSrc = fs.readFileSync(RESTORE, "utf8");

assert.match(
  orchSrc,
  /authorizeExaAgentLive:\s*authorizePaidResearch\s*===\s*true/,
  "orchestrator must forward authorizeExaAgentLive when paid research authorized"
);
assert.match(
  orchSrc,
  /allowLiveExaDeepSeed:\s*authorizePaidResearch\s*===\s*true/,
  "orchestrator must forward allowLiveExaDeepSeed when paid research authorized"
);

assert.match(
  chainSrc,
  /allowLiveExaDeepSeed:\s*allowLiveExaDeepSeedForOpp/,
  "chain must pass per-opportunity allowLiveExaDeepSeed (org-dedup aware) into processOpportunity"
);
assert.match(
  chainSrc,
  /authorizeExaAgentLive:\s*opts\.authorizeExaAgentLive\s*===\s*true/,
  "chain must pass authorizeExaAgentLive into processOpportunityThroughOriginalChain"
);
assert.match(
  chainSrc,
  /paidDeepOrgKeys/,
  "chain must track org keys to prevent repeated paid deep across segments"
);

assert.match(
  restoreSrc,
  /if \(opts\.authorizeExaAgentLive !== true \|\| opts\.allowLiveExaDeepSeed !== true\)/,
  "maybeSeedExaDeepResearch must refuse live seed unless both flags are true"
);

// Behavioral unit: import maybeSeed and prove unpaid path is $0 / not seeded.
const { maybeSeedExaDeepResearch } =
  await import(
    "../lib/group-demand-intelligence/workflow/gdi-original-full-e2e-execution-restore-v1.js"
  );

const unpaid = await maybeSeedExaDeepResearch(
  {
    id: "disc_test_unpaid",
    hotelId: "recLuxvwwxID7U2B8",
    researchSegment: "associations_conferences_events",
    organizationName: "Test Org",
  },
  {
    authorizeExaAgentLive: false,
    allowLiveExaDeepSeed: false,
  }
);
assert.equal(unpaid.seeded, false);
assert.equal(unpaid.reason, "LIVE_EXA_SEED_NOT_AUTHORIZED");
assert.equal(Number(unpaid.costUsd || 0), 0);

const halfAuth = await maybeSeedExaDeepResearch(
  {
    id: "disc_test_half",
    hotelId: "recLuxvwwxID7U2B8",
    researchSegment: "associations_conferences_events",
    organizationName: "Test Org",
  },
  {
    authorizeExaAgentLive: true,
    allowLiveExaDeepSeed: false,
  }
);
assert.equal(halfAuth.seeded, false);
assert.equal(halfAuth.reason, "LIVE_EXA_SEED_NOT_AUTHORIZED");

console.log(
  JSON.stringify(
    {
      ok: true,
      version: "test-gdi-fourteen-segment-deep-auth-flag-propagation-v1",
      checks: [
        "orchestrator_forwards_both_flags_when_paid_research",
        "chain_forwards_flags_to_processOpportunity",
        "maybeSeed_refuses_without_dual_auth",
        "unpaid_seed_cost_zero",
      ],
    },
    null,
    2
  )
);
