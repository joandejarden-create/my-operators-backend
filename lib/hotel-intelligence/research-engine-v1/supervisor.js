/**
 * Deterministic Hotel Intelligence Research Engine V1 supervisor.
 * Promotes full-research-workflow-v2 into a versioned engine contract.
 *
 * P1 default path is offline/deterministic staging (no paid providers, no Airtable).
 * Optional live workflow delegation via deps.runWorkflow (still apply-disabled).
 */

import crypto from "node:crypto";
import {
  ENGINE_CONTRACT_ID,
  ENGINE_CONTRACT_VERSION,
  PUBLICATION_STATE,
  FAILURE_CLASS,
  writeGuaranteesP1,
} from "./contract.js";
import { ENGINE_STAGE, ENGINE_STAGE_GRAPH } from "./stages.js";
import { loadPilotManifest, resolvePilotHotel } from "./pilot-manifest.js";
import {
  assertNotGoldenDemoAsProduction,
  assertResearchCompleteClaim,
} from "./golden-demo-isolation.js";
import { resolveApplyMode, APPLY_MODE, assertNoAirtableMutations } from "./apply-policy.js";
import { createProviderGateway, PROVIDER_IDS } from "./provider-interface.js";
import { createBudgetLedger } from "./budget-ledger.js";
import { emptyStagedPackage } from "./output-schemas.js";
import { projectHotelExplorerContactViewModel } from "./he-contact-projection.js";
import { assertNoDuplicateKeys } from "./entity-keys.js";
import {
  createResearchCaseStore,
  newCaseId,
} from "../contact-intelligence/research-case-store.js";
import { RESEARCH_OBJECTIVE } from "../contact-intelligence/full-research-workflow.js";

function stableHash(obj) {
  return crypto.createHash("sha256").update(JSON.stringify(obj)).digest("hex").slice(0, 16);
}

function nowIso() {
  return new Date().toISOString();
}

/**
 * Build a deterministic offline staged package from hotel seed (not research-complete).
 */
function buildOfflineStagedPackage({ hpcId, hotelName, caseId, runId }) {
  const pkg = emptyStagedPackage({ hpcId, caseId });
  pkg.hotel_name = hotelName || null;
  pkg.run_id = runId;
  pkg.publication_state = PUBLICATION_STATE.RESEARCH_IN_PROGRESS;
  pkg.stage_graph = [...ENGINE_STAGE_GRAPH];
  pkg.research_complete = false;
  pkg.note =
    "P1 offline staging shell — identical inputs yield identical structure; not research-complete";
  return pkg;
}

/**
 * Run one hotel through the engine supervisor.
 */
export async function runHotelIntelligenceResearchEngine(input = {}, deps = {}) {
  const writeGuarantees = writeGuaranteesP1();
  const apply = resolveApplyMode(input.apply_mode || APPLY_MODE.DRY_RUN, {
    founderAuth: input.founder_auth === true,
  });
  if (apply.mode === APPLY_MODE.APPLY && !apply.ok) {
    return {
      ok: false,
      contract: ENGINE_CONTRACT_ID,
      contract_version: ENGINE_CONTRACT_VERSION,
      error: apply.error,
      write_guarantees: writeGuarantees,
    };
  }

  const manifest = deps.manifest || loadPilotManifest({ path: input.manifest_path });
  const hpcId = String(input.hpcId || input.hotel_id || input.hpc_id || "").trim();
  const golden = assertNotGoldenDemoAsProduction(hpcId, {
    allowGoldenDemo: input.allow_golden_demo === true,
  });
  if (!golden.ok) {
    return {
      ok: false,
      contract: ENGINE_CONTRACT_ID,
      contract_version: ENGINE_CONTRACT_VERSION,
      error: FAILURE_CLASS.GOLDEN_DEMO_ISOLATION,
      hpc_id: hpcId,
      write_guarantees: writeGuarantees,
    };
  }

  const pilot = resolvePilotHotel(hpcId, manifest);
  if (!pilot.ok) {
    return {
      ok: false,
      contract: ENGINE_CONTRACT_ID,
      contract_version: ENGINE_CONTRACT_VERSION,
      error:
        pilot.reason === "NOT_IN_PILOT_MANIFEST"
          ? FAILURE_CLASS.NOT_IN_PILOT_MANIFEST
          : FAILURE_CLASS.INPUT_INVALID,
      hpc_id: hpcId,
      write_guarantees: writeGuarantees,
    };
  }

  const providers = deps.providers || createProviderGateway({
    paidProvidersEnabled: false,
    surfeEnabled: false,
    adapters: deps.providerAdapters || {},
  });
  const ledger = deps.ledger || createBudgetLedger({
    perHotelUsdCap: input.per_hotel_usd_cap ?? null,
    perHotelCreditCap: input.per_hotel_credit_cap ?? 0,
  });

  // Refuse any attempted paid call up-front probe (records refusal)
  if (input.probe_paid_provider) {
    await providers.call({
      provider: input.probe_paid_provider,
      operation: "probe",
      hotelId: hpcId,
    });
  }

  const store =
    deps.store ||
    createResearchCaseStore({
      root: deps.storeRoot,
      env: deps.env || process.env,
    });

  const objective = input.objective || RESEARCH_OBJECTIVE.HOTEL_OWNERSHIP_CONTACT;
  const runId =
    input.run_id ||
    `heire_${stableHash({ hpcId, objective, v: ENGINE_CONTRACT_VERSION })}`;

  let caseId = input.resume_case_id || input.case_id || null;
  let existing = null;
  if (caseId) {
    existing = store.getCase(caseId);
    if (!existing && input.resume_case_id) {
      return {
        ok: false,
        error: "CASE_NOT_FOUND",
        case_id: caseId,
        contract: ENGINE_CONTRACT_ID,
        contract_version: ENGINE_CONTRACT_VERSION,
        write_guarantees: writeGuarantees,
      };
    }
    if (
      existing &&
      existing.hotel_id &&
      String(existing.hotel_id) !== String(hpcId)
    ) {
      return {
        ok: false,
        error: FAILURE_CLASS.CASE_IDENTITY_MISMATCH,
        case_id: caseId,
        stored: existing.hotel_id,
        incoming: hpcId,
        contract: ENGINE_CONTRACT_ID,
        contract_version: ENGINE_CONTRACT_VERSION,
        write_guarantees: writeGuarantees,
      };
    }
  } else {
    caseId = newCaseId(hpcId);
  }

  const lockAcq = store.tryAcquireLock(caseId, 120_000);
  if (!lockAcq?.ok) {
    return {
      ok: false,
      error: "CASE_LOCKED",
      case_id: caseId,
      reason: lockAcq?.reason || null,
      contract: ENGINE_CONTRACT_ID,
      contract_version: ENGINE_CONTRACT_VERSION,
      write_guarantees: writeGuarantees,
    };
  }
  const lockToken = lockAcq.lock?.lock_token;
  const fenceToken = lockAcq.lock?.fence_token;

  const checkpoints = [];
  const pushCp = (stage, note, extra = {}) => {
    checkpoints.push({ stage, note, at: nowIso(), ...extra });
  };

  pushCp(ENGINE_STAGE.INIT, "engine_opened", { run_id: runId });
  pushCp(ENGINE_STAGE.CANONICAL_HPC_LOOKUP, "pilot_manifest_resolved", {
    canonical_hpc_id: pilot.canonicalHpcId,
    dual_house: pilot.dualHouse,
  });
  pushCp(ENGINE_STAGE.IDENTITY_RESOLUTION, "identity_seed_from_manifest", {
    name: pilot.primary.name,
  });

  let staged;
  let workflowResult = null;

  if (typeof deps.runWorkflow === "function" && input.use_live_workflow === true) {
    workflowResult = await deps.runWorkflow(
      {
        hotel: {
          hotel_id: hpcId,
          name: pilot.primary.name,
        },
        objective,
        case_id: caseId,
        allow_allocate_case_id: true,
        resume_case_id: input.resume_case_id || null,
      },
      input.budgets || {},
      { store, env: deps.env, networkBlocked: true }
    );
    staged = {
      ...emptyStagedPackage({ hpcId, caseId }),
      hotel_name: pilot.primary.name,
      workflow: workflowResult,
      research_complete: false,
      publication_state: PUBLICATION_STATE.VALIDATION_REQUIRED,
    };
    pushCp(ENGINE_STAGE.SYNTHESIS, "live_workflow_delegated_network_blocked");
  } else {
    // Deterministic offline path — same stage graph, structured empty/staged shell
    for (const stage of [
      ENGINE_STAGE.OWNERSHIP,
      ENGINE_STAGE.ORGANIZATION,
      ENGINE_STAGE.RELATIONSHIP,
      ENGINE_STAGE.PERSON,
      ENGINE_STAGE.CONTACT,
      ENGINE_STAGE.EVIDENCE_VALIDATION,
    ]) {
      pushCp(stage, "offline_stage_entered");
    }
    staged = buildOfflineStagedPackage({
      hpcId,
      hotelName: pilot.primary.name,
      caseId,
      runId,
    });
    pushCp(ENGINE_STAGE.SYNTHESIS, "offline_staged_package");
  }

  const completeClaim = assertResearchCompleteClaim(staged);
  if (!completeClaim.ok) {
    staged.research_complete = false;
  }

  const dedupeCheck = {
    organizations: assertNoDuplicateKeys(staged.organizations, "organizations"),
    relationships: assertNoDuplicateKeys(staged.relationships, "relationships"),
    persons: assertNoDuplicateKeys(staged.persons, "persons"),
    channels: assertNoDuplicateKeys(staged.channels, "channels"),
  };

  const sideEffects = input.side_effects || [];
  const noWrites = assertNoAirtableMutations(sideEffects);

  const contactVm = projectHotelExplorerContactViewModel(staged);

  const caseRecord = {
    case_id: caseId,
    hotel_id: hpcId,
    hotel_name: pilot.primary.name,
    objective,
    engine_contract: ENGINE_CONTRACT_ID,
    engine_contract_version: ENGINE_CONTRACT_VERSION,
    run_id: runId,
    current_stage: ENGINE_STAGE.COMPLETE,
    status: "STAGED",
    final_status: "ENGINE_STAGED_P1",
    publication_state: staged.publication_state || PUBLICATION_STATE.RESEARCH_IN_PROGRESS,
    stage_graph: [...ENGINE_STAGE_GRAPH],
    checkpoints,
    staged_package: staged,
    contact_view_model: contactVm,
    provider_usage: providers.getUsage(),
    budget: ledger.snapshot(),
    dedupe_check: dedupeCheck,
    write_guarantees: writeGuarantees,
    apply_mode: apply.mode,
    research_complete: false,
    input_fingerprint: stableHash({
      hpcId,
      objective,
      contract: ENGINE_CONTRACT_VERSION,
      offline: input.use_live_workflow !== true,
    }),
    updated_at: nowIso(),
    created_at: existing?.created_at || nowIso(),
  };

  const put = store.putCase(caseRecord, {
    lock_token: lockToken,
    fence_token: fenceToken,
  });
  if (put && put.ok === false) {
    try {
      store.releaseLock(caseId, lockToken);
    } catch {
      /* ignore */
    }
    return {
      ok: false,
      error: "CASE_PERSIST_FAILED",
      reason: put.reason || null,
      contract: ENGINE_CONTRACT_ID,
      contract_version: ENGINE_CONTRACT_VERSION,
      write_guarantees: writeGuarantees,
    };
  }
  try {
    store.releaseLock(caseId, lockToken);
  } catch {
    /* ignore */
  }

  pushCp(ENGINE_STAGE.COMPLETE, "engine_staged_p1");

  return {
    ok: true,
    contract: ENGINE_CONTRACT_ID,
    contract_version: ENGINE_CONTRACT_VERSION,
    hpc_id: hpcId,
    canonical_hpc_id: pilot.canonicalHpcId,
    dual_house: pilot.dualHouse,
    case_id: caseId,
    run_id: runId,
    stage_graph: [...ENGINE_STAGE_GRAPH],
    checkpoints,
    staged_package: staged,
    contact_view_model: contactVm,
    provider_usage: providers.getUsage(),
    budget: ledger.snapshot(),
    dedupe_check: dedupeCheck,
    airtable_writes: noWrites.ok ? [] : noWrites.writes,
    apply_mode: apply.mode,
    publication_state: caseRecord.publication_state,
    research_complete: false,
    write_guarantees: writeGuarantees,
    input_fingerprint: caseRecord.input_fingerprint,
    workflow_result: workflowResult,
  };
}

/**
 * Batch-run all distinct pilot HPC IDs with the same stage graph.
 */
export async function runPilotCohortOffline(opts = {}, deps = {}) {
  const manifest = deps.manifest || loadPilotManifest({ path: opts.manifest_path });
  const ids = manifest.distinctHpcIds;
  const results = [];
  for (const hpcId of ids) {
    const r = await runHotelIntelligenceResearchEngine(
      {
        hpcId,
        apply_mode: APPLY_MODE.DRY_RUN,
        use_live_workflow: false,
        per_hotel_credit_cap: 0,
      },
      { ...deps, manifest, storeRoot: opts.storeRoot || deps.storeRoot }
    );
    results.push(r);
  }
  const graphsOk = results.every(
    (r) => r.ok && JSON.stringify(r.stage_graph) === JSON.stringify(ENGINE_STAGE_GRAPH)
  );
  const fingerprints = results.map((r) => r.input_fingerprint);
  return {
    ok: graphsOk && results.every((r) => r.ok),
    contract: ENGINE_CONTRACT_ID,
    contract_version: ENGINE_CONTRACT_VERSION,
    hotel_count: results.length,
    distinct_hpc_count: ids.length,
    same_stage_graph: graphsOk,
    stage_graph: [...ENGINE_STAGE_GRAPH],
    results,
    fingerprints,
    write_guarantees: writeGuaranteesP1(),
  };
}

export { PROVIDER_IDS, ENGINE_STAGE_GRAPH, PUBLICATION_STATE };
