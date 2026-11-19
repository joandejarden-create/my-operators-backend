#!/usr/bin/env node
/**
 * HE-INTEL-001 P1 — Hotel Intelligence Research Engine V1 foundation tests.
 * Offline only: no paid providers, no Airtable writes.
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
  loadPilotManifest,
  listCanonicalPilotHpcIds,
  resolvePilotHotel,
  runHotelIntelligenceResearchEngine,
  runPilotCohortOffline,
  createProviderGateway,
  PROVIDER_IDS,
  createBudgetLedger,
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
} from "../lib/hotel-intelligence/research-engine-v1/index.js";

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

// 4. Provider refusals + unknown cost
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

// 9. Single hotel offline run + resume
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
  assert.equal(r1.write_guarantees.canonical_writes, false);
  assert.ok(r1.contact_view_model.ownershipSummary);
  assert.ok(r1.provider_usage.some((u) => u.status === "REFUSED"));
  assert.equal(r1.airtable_writes.length, 0);

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
  ok("offline_run_resume_deterministic_fingerprint");
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
  assert.equal(cohort.results.every((r) => r.research_complete === false), true);
  assert.equal(
    cohort.results.every((r) => r.write_guarantees.airtable_apply === false),
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

console.log(`\nOK he-intel-001-research-engine-v1 (${passed} assertions)`);
