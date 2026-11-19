#!/usr/bin/env node
/**
 * HE-INTEL-001 P1 — Hotel Intelligence Research Engine V1 foundation tests.
 * Offline only: no paid providers, no Airtable writes, no real payable adapters.
 */
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

import {
  ENGINE_CONTRACT_ID,
  ENGINE_CONTRACT_VERSION,
  ENGINE_STAGE_GRAPH,
  ENGINE_STAGE,
  loadPilotManifest,
  listCanonicalPilotHpcIds,
  resolvePilotHotel,
  runHotelIntelligenceResearchEngine,
  runPilotCohortOffline,
  createProviderGateway,
  createMockProviderAdapter,
  createBudgetLedger,
  createExecutionGateway,
  buildOperationId,
  PROVIDER_IDS,
  resolveApplyMode,
  APPLY_MODE,
  assertNoAirtableMutations,
  assertNotGoldenDemoAsProduction,
  assertResearchCompleteClaim,
  projectHotelExplorerContactViewModel,
  emptyStagedPackage,
  normalizeOrganization,
  normalizeRelationship,
  assertNoDuplicateKeys,
  orgDedupeKey,
  KGPV_HPC_ID,
  writeGuaranteesP1,
  EXECUTION_MODE,
} from "../lib/hotel-intelligence/research-engine-v1/index.js";
import { createResearchCaseStore } from "../lib/hotel-intelligence/contact-intelligence/research-case-store.js";
import {
  assertHeIntelEngineAuth,
  allowHeIntelUnauthLocal,
  isHeIntelProductionEnv,
} from "../api/hotel-intelligence-research-engine-v1.js";
import { WORKSPACE_ADMIN } from "../lib/company-workspace-access.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");

function tmpStore() {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "he-intel-p1-"));
  return path.join(dir, "research-cases");
}

let passed = 0;
function ok(name) {
  passed += 1;
  console.log(`PASS ${name}`);
}

// 1. Manifest
{
  const m = loadPilotManifest();
  assert.equal(m.status, "FROZEN");
  assert.equal(m.founderPropertyCount, 16);
  assert.equal(m.hpcDistinctRecordCount, 15);
  assert.equal(listCanonicalPilotHpcIds(m).length, 15);
  const hyatt = resolvePilotHotel("recBJZwMvgbdG7y4F", m);
  assert.equal(hyatt.ok, true);
  assert.equal(hyatt.dualHouse, true);
  assert.equal(hyatt.founderExperienceCount, 2);
  const kgpv = resolvePilotHotel(KGPV_HPC_ID, m);
  assert.equal(kgpv.ok, false);
  assert.equal(kgpv.reason, "GOLDEN_DEMO_ISOLATION");
  ok("pilot_manifest_16_to_15_and_kgpv_excluded");
}

// 2. Golden demo isolation
{
  const g = assertNotGoldenDemoAsProduction(KGPV_HPC_ID);
  assert.equal(g.ok, false);
  ok("golden_demo_isolation");
}

// 3. Apply disabled
{
  const a = resolveApplyMode(APPLY_MODE.APPLY);
  assert.equal(a.ok, false);
  assert.equal(a.error, "APPLY_DISABLED");
  const w = assertNoAirtableMutations([{ type: "airtable_write", table: "x" }]);
  assert.equal(w.ok, false);
  ok("apply_disabled_p1");
}

// 4. Provider refusals + unknown cost (via reservation gateway)
{
  const gw = createProviderGateway({ paidProvidersEnabled: false });
  const r = await gw.call({
    provider: PROVIDER_IDS.EXA,
    operation: "search",
    hotelId: "recLuxvwwxID7U2B8",
  });
  assert.equal(r.ok, false);
  assert.equal(r.error, "PROVIDER_UNAUTHORIZED");
  const usage = gw.getUsage();
  assert.equal(usage[0].status, "REFUSED");
  assert.equal(usage[0].cost_usd, null);
  assert.notEqual(usage[0].cost_usd_status, "REPORTED_AS_ZERO");
  assert.equal(usage[0].cost_usd_status, "UNKNOWN");
  assert.ok(usage[0].operation_id);
  ok("paid_providers_refused_unknown_cost_not_zero");
}

// 5. Budget enforcement (credit cap)
{
  const ledger = createBudgetLedger({ perHotelCreditCap: 1 });
  const first = ledger.recordUsage({
    hotel_id: "recLuxvwwxID7U2B8",
    provider: "MOCK",
    status: "OK",
    credits: 1,
    cost_usd: null,
    cost_usd_status: "UNKNOWN",
  });
  assert.equal(first.ok, true);
  const second = ledger.canSpend({
    hotelId: "recLuxvwwxID7U2B8",
    credits: 1,
  });
  assert.equal(second.ok, false);
  assert.equal(second.error, "BUDGET_EXHAUSTED");
  ok("budget_credit_enforcement");
}

// 5b. Reservation → settle → resume no duplicate spend
{
  const ledger = createBudgetLedger({ perHotelCreditCap: 2 });
  const mock = createMockProviderAdapter({ costUsd: 0, credits: 1 });
  const gw = createProviderGateway({
    ledger,
    adapters: { [PROVIDER_IDS.MOCK]: mock },
    paidProvidersEnabled: false,
  });
  const opId = buildOperationId({
    caseId: "case_resume_test",
    hotelId: "recLuxvwwxID7U2B8",
    provider: PROVIDER_IDS.MOCK,
    operation: "lookup",
    params: { q: "owner" },
  });
  const first = await gw.call({
    provider: PROVIDER_IDS.MOCK,
    operation: "lookup",
    hotelId: "recLuxvwwxID7U2B8",
    caseId: "case_resume_test",
    params: { q: "owner" },
    operationId: opId,
    estimated_credits: 1,
  });
  assert.equal(first.ok, true);
  assert.equal(first.operation_id, opId);
  assert.equal(ledger.getOperation(opId).status, "SETTLED");
  assert.equal(ledger.snapshot().hotels.find((h) => h.hotel_id === "recLuxvwwxID7U2B8").credits, 1);

  const replay = await gw.call({
    provider: PROVIDER_IDS.MOCK,
    operation: "lookup",
    hotelId: "recLuxvwwxID7U2B8",
    caseId: "case_resume_test",
    params: { q: "owner" },
    operationId: opId,
    estimated_credits: 1,
  });
  assert.equal(replay.ok, true);
  assert.equal(replay.replayed, true);
  assert.equal(ledger.snapshot().hotels.find((h) => h.hotel_id === "recLuxvwwxID7U2B8").credits, 1);
  ok("reservation_settle_resume_no_duplicate_spend");
}

// 5c. Fail-closed: sealed adapter cannot be called outside gateway
{
  const ledger = createBudgetLedger({ perHotelCreditCap: 10 });
  let rawInvoked = false;
  const sneaky = {
    call: async () => {
      rawInvoked = true;
      return { ok: true, cost_usd: 0, cost_usd_status: "REPORTED", credits: 0 };
    },
  };
  const gw = createProviderGateway({
    ledger,
    adapters: { [PROVIDER_IDS.MOCK]: sneaky },
  });
  await assert.rejects(
    () => gw.attemptDirectAdapterCall(PROVIDER_IDS.MOCK, { operation: "x" }),
    (err) => err?.code === "PROVIDER_ADAPTER_BYPASS_FORBIDDEN" || /BYPASS_FORBIDDEN/.test(String(err))
  );
  assert.equal(rawInvoked, false);

  // Gateway path still works
  const viaGw = await gw.call({
    provider: PROVIDER_IDS.MOCK,
    operation: "x",
    hotelId: "recLuxvwwxID7U2B8",
    estimated_credits: 0,
  });
  assert.equal(viaGw.ok, true);
  assert.equal(rawInvoked, true);
  ok("provider_adapter_bypass_forbidden");
}

// 5d. Budget refuse before execution (reserve fail-closed)
{
  const ledger = createBudgetLedger({ perHotelCreditCap: 0 });
  let executed = false;
  const mock = {
    estimateCost: () => ({ cost_usd: 0, cost_usd_status: "ESTIMATED", credits: 1 }),
    call: async () => {
      executed = true;
      return { ok: true, cost_usd: 0, cost_usd_status: "REPORTED", credits: 1 };
    },
  };
  const gw = createProviderGateway({
    ledger,
    adapters: { [PROVIDER_IDS.MOCK]: mock },
  });
  const r = await gw.call({
    provider: PROVIDER_IDS.MOCK,
    operation: "blocked",
    hotelId: "recLuxvwwxID7U2B8",
    estimated_credits: 1,
  });
  assert.equal(r.ok, false);
  assert.equal(r.error, "BUDGET_EXHAUSTED");
  assert.equal(executed, false);
  ok("budget_reserve_blocks_execution");
}

// 5e. createExecutionGateway requires ledger with reserve
{
  assert.throws(() => createExecutionGateway({ adapters: {} }), /budget_ledger/);
  ok("execution_gateway_requires_ledger");
}

// 6. Dedupe keys
{
  const org = normalizeOrganization({ legal_name: "HEI Hotel & Resorts", jurisdiction: "US" });
  const org2 = normalizeOrganization({ legal_name: "HEI Hotel & Resorts", jurisdiction: "US" });
  assert.equal(org.dedupe_key, org2.dedupe_key);
  const rels = [
    normalizeRelationship({
      hpc_id: "recnPB3Q02NO8Ma1o",
      relationship_type: "OPERATOR",
      org_dedupe_key: org.dedupe_key,
    }),
    normalizeRelationship({
      hpc_id: "recnPB3Q02NO8Ma1o",
      relationship_type: "OPERATOR",
      org_dedupe_key: org.dedupe_key,
    }),
  ];
  const d = assertNoDuplicateKeys(rels, "relationships");
  assert.equal(d.ok, false);
  assert.ok(orgDedupeKey({ legalName: "Test Org" }));
  ok("entity_dedupe_keys");
}

// 7. Contact VM ownershipSummary
{
  const staged = emptyStagedPackage({ hpcId: "recLuxvwwxID7U2B8" });
  staged.hotel_name = "Bethesda Marriott";
  staged.ownership.owner_entity = normalizeOrganization({
    legal_name: "Example Owner LLC",
    jurisdiction: "US",
  });
  const vm = projectHotelExplorerContactViewModel(staged);
  assert.ok(vm.ownershipSummary);
  assert.equal(vm.ownershipSummary.hotel.hpcId, "recLuxvwwxID7U2B8");
  assert.equal(vm.research_complete, false);
  assert.equal(vm.write_guarantees.airtable_apply, false);
  ok("he_contact_view_model_projection");
}

// 8. Research-complete claim forbidden from empty package
{
  const staged = emptyStagedPackage({ hpcId: "recLuxvwwxID7U2B8" });
  staged.research_complete = true;
  const c = assertResearchCompleteClaim(staged);
  assert.equal(c.ok, false);
  ok("research_complete_claim_forbidden");
}

// 9. Single hotel offline run + resume + COMPLETE checkpoint in stored case
{
  const storeRoot = tmpStore();
  const r1 = await runHotelIntelligenceResearchEngine(
    {
      hpcId: "recLuxvwwxID7U2B8",
      apply_mode: APPLY_MODE.DRY_RUN,
      use_live_workflow: false,
      probe_paid_provider: PROVIDER_IDS.PARALLEL_TASK,
    },
    { storeRoot }
  );
  assert.equal(r1.ok, true);
  assert.equal(r1.contract, ENGINE_CONTRACT_ID);
  assert.equal(r1.contract_version, ENGINE_CONTRACT_VERSION);
  assert.deepEqual(r1.stage_graph, ENGINE_STAGE_GRAPH);
  assert.equal(r1.research_complete, false);
  assert.equal(r1.execution_mode, EXECUTION_MODE.OFFLINE_DETERMINISTIC_STAGING);
  assert.equal(r1.live_research_verified, false);
  assert.equal(r1.write_guarantees.canonical_writes, false);
  assert.ok(r1.contact_view_model.ownershipSummary);
  assert.ok(r1.provider_usage.some((u) => u.status === "REFUSED"));
  assert.equal(r1.airtable_writes.length, 0);

  const completeCp = r1.checkpoints.find((c) => c.stage === ENGINE_STAGE.COMPLETE);
  assert.ok(completeCp, "COMPLETE checkpoint present in returned result");
  assert.equal(completeCp.execution_mode, EXECUTION_MODE.OFFLINE_DETERMINISTIC_STAGING);

  const store = createResearchCaseStore({ root: storeRoot });
  const stored = store.getCase(r1.case_id);
  assert.ok(stored);
  assert.equal(stored.current_stage, ENGINE_STAGE.COMPLETE);
  assert.equal(stored.execution_mode, EXECUTION_MODE.OFFLINE_DETERMINISTIC_STAGING);
  assert.equal(stored.live_research_verified, false);
  const storedComplete = stored.checkpoints.find((c) => c.stage === ENGINE_STAGE.COMPLETE);
  assert.ok(storedComplete, "COMPLETE checkpoint persisted BEFORE putCase");
  assert.equal(stored.checkpoints.length, r1.checkpoints.length);

  const r2 = await runHotelIntelligenceResearchEngine(
    {
      hpcId: "recLuxvwwxID7U2B8",
      resume_case_id: r1.case_id,
      apply_mode: APPLY_MODE.DRY_RUN,
      use_live_workflow: false,
    },
    { storeRoot }
  );
  assert.equal(r2.ok, true);
  assert.equal(r2.case_id, r1.case_id);
  assert.equal(r2.input_fingerprint, r1.input_fingerprint);
  assert.equal(r2.execution_mode, EXECUTION_MODE.OFFLINE_DETERMINISTIC_STAGING);
  ok("offline_run_resume_complete_checkpoint_persisted");
}

// 10. Cross-hotel mismatch on resume
{
  const storeRoot = tmpStore();
  const r1 = await runHotelIntelligenceResearchEngine(
    { hpcId: "recLuxvwwxID7U2B8", use_live_workflow: false },
    { storeRoot }
  );
  const bad = await runHotelIntelligenceResearchEngine(
    {
      hpcId: "rec0eTEnCqlsGJJt9",
      resume_case_id: r1.case_id,
      use_live_workflow: false,
    },
    { storeRoot }
  );
  assert.equal(bad.ok, false);
  assert.equal(bad.error, "CASE_IDENTITY_MISMATCH");
  ok("no_cross_hotel_case_contamination");
}

// 11. Full cohort same stage graph
{
  const storeRoot = tmpStore();
  const cohort = await runPilotCohortOffline({ storeRoot });
  assert.equal(cohort.ok, true);
  assert.equal(cohort.distinct_hpc_count, 15);
  assert.equal(cohort.same_stage_graph, true);
  assert.equal(cohort.execution_mode, EXECUTION_MODE.OFFLINE_DETERMINISTIC_STAGING);
  assert.equal(cohort.live_research_verified, false);
  assert.equal(cohort.results.every((r) => r.research_complete === false), true);
  assert.equal(
    cohort.results.every((r) => r.write_guarantees.airtable_apply === false),
    true
  );
  assert.equal(
    cohort.results.every((r) => r.execution_mode === EXECUTION_MODE.OFFLINE_DETERMINISTIC_STAGING),
    true
  );
  ok("pilot_cohort_same_stage_graph_15");
}

// 12. Write guarantees shape
{
  const g = writeGuaranteesP1();
  assert.equal(g.contract, ENGINE_CONTRACT_ID);
  assert.equal(g.paid_providers, false);
  assert.equal(g.surfe, false);
  assert.equal(g.customer_publication, "BLOCKED");
  ok("write_guarantees_p1");
}

// 13. Fixture file committed
{
  const p = path.join(
    ROOT,
    "fixtures/hotel-explorer/HE-INTEL-001-pilot-16-manifest-frozen-v1.json"
  );
  assert.equal(fs.existsSync(p), true);
  ok("frozen_manifest_fixture_present");
}

// 14. Auth: unauthorized without identity
{
  const gate = assertHeIntelEngineAuth({}, { NODE_ENV: "development" });
  assert.equal(gate.ok, false);
  assert.equal(gate.status, 401);
  ok("auth_unauthorized_without_identity");
}

// 15. Auth: production never honors HE_INTEL_ENGINE_ALLOW_UNAUTH_LOCAL
{
  assert.equal(
    allowHeIntelUnauthLocal({
      NODE_ENV: "production",
      HE_INTEL_ENGINE_ALLOW_UNAUTH_LOCAL: "1",
    }),
    false
  );
  assert.equal(isHeIntelProductionEnv({ NODE_ENV: "production" }), true);
  assert.equal(isHeIntelProductionEnv({ VERCEL_ENV: "production" }), true);
  const prodUnauth = assertHeIntelEngineAuth(
    {},
    { NODE_ENV: "production", HE_INTEL_ENGINE_ALLOW_UNAUTH_LOCAL: "1" }
  );
  assert.equal(prodUnauth.ok, false);
  assert.equal(prodUnauth.status, 401);
  ok("auth_production_never_allows_unauth_local");
}

// 16. Auth: local unauth only when non-prod + flag
{
  const local = assertHeIntelEngineAuth(
    {},
    { NODE_ENV: "development", HE_INTEL_ENGINE_ALLOW_UNAUTH_LOCAL: "1" }
  );
  assert.equal(local.ok, true);
  assert.equal(local.auth_mode, "UNAUTH_LOCAL_DEV_ONLY");
  ok("auth_local_unauth_flag_non_prod_only");
}

// 17. Auth: admin allowed
{
  const gate = assertHeIntelEngineAuth(
    { dealalityUser: { isAdmin: true, email: "admin@dealality.com" } },
    { NODE_ENV: "production" }
  );
  assert.equal(gate.ok, true);
  assert.equal(gate.auth_mode, "ADMIN");
  ok("auth_admin_allowed");
}

// 18. Auth: customer / share isolation
{
  const share = assertHeIntelEngineAuth(
    {
      dealalityUser: {
        isAdmin: true,
        isShareSession: true,
        email: "share@example.com",
      },
    },
    { NODE_ENV: "production" }
  );
  assert.equal(share.ok, false);
  assert.equal(share.status, 403);
  assert.equal(share.body.error, "FORBIDDEN_SHARE_ISOLATION");

  const customer = assertHeIntelEngineAuth(
    {
      dealalityUser: {
        isCustomerOnly: true,
        email: "customer@example.com",
      },
    },
    { NODE_ENV: "production" }
  );
  assert.equal(customer.ok, false);
  assert.equal(customer.body.error, "FORBIDDEN_CUSTOMER_ISOLATION");

  const nonAdmin = assertHeIntelEngineAuth(
    {
      dealalityUser: {
        isAdmin: false,
        workspaceAccess: [],
        email: "owner@example.com",
      },
    },
    { NODE_ENV: "production" }
  );
  assert.equal(nonAdmin.ok, false);
  assert.equal(nonAdmin.body.error, "FORBIDDEN_ADMIN");
  ok("auth_customer_share_isolation");
}

// 19. Auth: workspace admin allowed
{
  const gate = assertHeIntelEngineAuth(
    {
      dealalityUser: {
        isAdmin: false,
        workspaceAccess: [WORKSPACE_ADMIN],
        email: "ws-admin@dealality.com",
      },
    },
    { NODE_ENV: "production" }
  );
  assert.equal(gate.ok, true);
  assert.equal(gate.auth_mode, "WORKSPACE_ADMIN");
  ok("auth_workspace_admin_allowed");
}

// 20. Server mounts requireAdminAccess
{
  const serverSrc = fs.readFileSync(path.join(ROOT, "server.js"), "utf8");
  const metaIdx = serverSrc.indexOf("/api/hotel-intelligence/research-engine/v1/meta");
  const dryIdx = serverSrc.indexOf("/api/hotel-intelligence/research-engine/v1/dry-run");
  assert.ok(metaIdx > 0 && dryIdx > 0);
  const metaBlock = serverSrc.slice(metaIdx, metaIdx + 280);
  const dryBlock = serverSrc.slice(dryIdx, dryIdx + 280);
  assert.match(metaBlock, /requireAdminAccess/);
  assert.match(dryBlock, /requireAdminAccess/);
  assert.match(metaBlock, /requireDealalityUser/);
  assert.match(dryBlock, /requireDealalityUser/);
  ok("server_routes_require_admin_access");
}

console.log(`\nOK he-intel-001-research-engine-v1 (${passed} assertions)`);
