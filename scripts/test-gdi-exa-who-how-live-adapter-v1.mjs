#!/usr/bin/env node
/**
 * $0 regression + authorization-control tests for WHO/HOW live adapter.
 * Never hits api.exa.ai. Never spends.
 */
import assert from "assert";
import fs from "fs";
import os from "os";
import path from "path";
import { fileURLToPath } from "url";
import {
  assertWhoHowLiveAuthorization,
  executeWhoHowLiveOrDry,
  executeWhoHowTranche,
  reconcileWhoHowJobSpend,
  WHO_HOW_RESEARCH_PURPOSE,
  GDI_EXA_WHO_HOW_LIVE_ADAPTER_V1,
} from "../lib/group-demand-intelligence/research/exa-agent/gdi-exa-who-how-live-adapter-v1.js";
import {
  buildExaAgentDeepJobKey,
  markExaAgentDeepJobSubmitted,
  markExaAgentDeepJobCompleted,
  beginOrResumeExaAgentDeepJob,
} from "../lib/group-demand-intelligence/research/exa-agent/gdi-exa-agent-deep-job-store-v1.js";
import {
  WHO_HOW_STATUS,
  WHO_HOW_PROMPT_CONTRACT_ID,
} from "../lib/group-demand-intelligence/workflow/gdi-who-how-agent-contract-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const root = path.join(__dirname, "..");
const fixturePath = path.join(
  root,
  "fixtures/gdi-full-e2e-v3/who-how-scenarios-v1.json",
);
const fixtures = JSON.parse(fs.readFileSync(fixturePath, "utf8"));
const scenarioA = fixtures.scenarios.find((s) => s.id === "A_verified_direct_buyer");

let passed = 0;
function pass(name) {
  passed += 1;
  console.log(`PASS ${name}`);
}

const prevLive = process.env.GDI_EXA_AGENT_LIVE;
delete process.env.GDI_EXA_AGENT_LIVE;

// 1 — DryRun default refuses spend
{
  const auth = assertWhoHowLiveAuthorization({ mode: "DryRun" });
  assert.equal(auth.ok, true);
  assert.equal(auth.live, false);
  const r = await executeWhoHowLiveOrDry(scenarioA.dossier, { mode: "DryRun" });
  assert.equal(r.paid, false);
  assert.equal(r.costUsd, 0);
  assert.equal(r.reason, "DRY_RUN_NO_PROVIDER_CALL");
  pass("dryrun_default_no_spend");
}

// 2 — Live without founder auth refused
{
  const auth = assertWhoHowLiveAuthorization({
    mode: "Live",
    hardCeilingUsd: 5,
    authorizationId: "FOUNDER_GO_TEST",
    skipEnvGate: true,
  });
  assert.equal(auth.ok, false);
  assert.ok(auth.errors.includes("MISSING_FOUNDER_PAID_AUTHORIZATION"));
  const r = await executeWhoHowLiveOrDry(scenarioA.dossier, {
    mode: "Live",
    hardCeilingUsd: 5,
    authorizationId: "FOUNDER_GO_TEST",
  });
  assert.equal(r.paid, false);
  assert.equal(r.reason, "LIVE_WHO_HOW_NOT_AUTHORIZED");
  pass("live_without_auth_refused");
}

// 3 — Live without env gate refused
{
  const auth = assertWhoHowLiveAuthorization({
    mode: "Live",
    authorizeExaAgentLive: true,
    authorizationId: "FOUNDER_GO_TEST",
    hardCeilingUsd: 5,
  });
  assert.equal(auth.ok, false);
  assert.ok(auth.errors.includes("GDI_EXA_AGENT_LIVE_ENV_NOT_SET"));
  pass("live_without_env_refused");
}

// 4 — Hard ceiling blocks before submit
{
  const auth = assertWhoHowLiveAuthorization({
    mode: "Live",
    authorizeExaAgentLive: true,
    authorizationId: "FOUNDER_GO_TEST",
    hardCeilingUsd: 5,
    spentUsd: 4.95,
    skipEnvGate: true,
  });
  assert.equal(auth.ok, false);
  assert.ok(auth.errors.includes("BUDGET_CEILING_WOULD_BE_EXCEEDED"));
  pass("hard_ceiling_blocks");
}

// 5 — Fixture path normalizes WHO/HOW without spend
{
  const r = await executeWhoHowLiveOrDry(scenarioA.dossier, {
    mode: "DryRun",
    fixture: true,
    fixtureOutput: scenarioA.output,
  });
  assert.equal(r.paid, false);
  assert.equal(r.costUsd, 0);
  assert.equal(r.whoHow.whoHowStatus, WHO_HOW_STATUS.VERIFIED_DIRECT_BUYER);
  assert.equal(r.opportunity.buyerVsEntry, "DIRECT_BUYER");
  pass("fixture_who_how_normalize");
}

// 6 — Settled-job reuse prevents duplicate billing
{
  const storeDir = fs.mkdtempSync(path.join(os.tmpdir(), "gdi-who-how-jobs-"));
  process.env.GDI_EXA_AGENT_DEEP_JOB_STORE_PATH = storeDir;
  const authId = "FOUNDER_GO_REUSE_TEST";
  const oppId = "fx_v3_a_buyer";
  const jobKey = buildExaAgentDeepJobKey({
    authorizationId: authId,
    hotelId: "recnPB3Q02NO8Ma1o",
    opportunityId: oppId,
    researchPurpose: WHO_HOW_RESEARCH_PURPOSE,
    contractId: WHO_HOW_PROMPT_CONTRACT_ID,
  });
  beginOrResumeExaAgentDeepJob({
    jobKey,
    hotelId: "recnPB3Q02NO8Ma1o",
    opportunityId: oppId,
    authorizationId: authId,
    contractId: WHO_HOW_PROMPT_CONTRACT_ID,
  });
  markExaAgentDeepJobSubmitted(jobKey, {
    providerRunId: "agent_run_settled_reuse_1",
    costUsd: 0.1,
  });
  markExaAgentDeepJobCompleted(jobKey, {
    providerRunId: "agent_run_settled_reuse_1",
    costUsd: 0.1,
    resultSummary: { whoHow: scenarioA.output },
  });

  process.env.GDI_EXA_AGENT_LIVE = "1";
  const r = await executeWhoHowLiveOrDry(scenarioA.dossier, {
    mode: "Live",
    authorizeExaAgentLive: true,
    authorizePaidResearch: true,
    authorizationId: authId,
    hardCeilingUsd: 5,
    spentUsd: 0,
    hotelId: "recnPB3Q02NO8Ma1o",
  });
  assert.equal(r.reused, true);
  assert.equal(r.paid, false);
  assert.equal(r.costUsd, 0);
  assert.ok(
    /reuse|do_not_resubmit/i.test(r.reason),
    `expected reuse reason, got ${r.reason}`,
  );
  assert.equal(r.whoHow.whoHowStatus, WHO_HOW_STATUS.VERIFIED_DIRECT_BUYER);

  const recon = reconcileWhoHowJobSpend({
    authorizationId: authId,
    storeDir,
  });
  assert.ok(recon.settledUsd >= 0.1 - 1e-9);
  pass("settled_job_reuse_no_rebill");

  delete process.env.GDI_EXA_AGENT_LIVE;
  delete process.env.GDI_EXA_AGENT_DEEP_JOB_STORE_PATH;
  fs.rmSync(storeDir, { recursive: true, force: true });
}

// 7 — Tranche DryRun processes 25 without spend; Live blocked by auth
{
  const items = fixtures.scenarios.slice(0, 3).map((s) => ({
    opportunityId: s.output.opportunityId,
    dossier: s.dossier,
    fixtureOutput: s.output,
  }));
  const dry = await executeWhoHowTranche(items, {
    mode: "DryRun",
    hardCeilingUsd: 5,
    authorizationId: "FOUNDER_GO_WYNDHAM_WHO_HOW_V3_TRANCHE_20261127",
  });
  assert.equal(dry.spentUsd, 0);
  assert.equal(dry.processed, items.length);

  const blocked = await executeWhoHowTranche(items, {
    mode: "Live",
    hardCeilingUsd: 5,
    authorizationId: "FOUNDER_GO_WYNDHAM_WHO_HOW_V3_TRANCHE_20261127",
    authorizePaidResearch: true,
    authorizeExaAgentLive: true,
    // env not set → blocked
  });
  assert.ok(blocked.blocked);
  assert.equal(blocked.spentUsd, 0);
  pass("tranche_dryrun_and_live_auth_block");
}

// 8 — Adapter version stamp
{
  assert.equal(
    GDI_EXA_WHO_HOW_LIVE_ADAPTER_V1,
    "gdi-exa-who-how-live-adapter-v1",
  );
  pass("adapter_version");
}

if (prevLive != null) process.env.GDI_EXA_AGENT_LIVE = prevLive;
else delete process.env.GDI_EXA_AGENT_LIVE;

console.log(`\n${passed} PASS — gdi-exa-who-how-live-adapter-v1 ($0)`);
