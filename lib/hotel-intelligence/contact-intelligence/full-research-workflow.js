/**
 * Full Dealality research workflow orchestration (staging).
 *
 * Thin case coordinator: hotel/objective → durable case → ownership handoff
 * → person/contact staging → evidence-backed staged result + next action.
 *
 * Repairs (Astra F1–F9, Patch A–C core):
 * - Trusted policy deny precedence
 * - Lock → load → identity bind before mutation
 * - Truthful completion (evidence + attributable contact)
 * - Handoff result adapter (sources/budgets/conflicts/resume)
 * - Interruption checkpoint persistence
 */

import { validateOwnershipHandoffInput, OWNERSHIP_HANDOFF_ALLOWED_BUDGET_KEYS } from "./ownership-handoff-contract.js";
import { runHotelOwnershipContactWorkflow } from "./hotel-ownership-contact-workflow.js";
import {
  createResearchCaseStore,
  newCaseId,
  assertValidCaseId,
  RESEARCH_CASE_STORE_VERSION,
} from "./research-case-store.js";
import {
  buildTrustedExecutionPolicy,
  validateResearchObjective,
  remainingAllowance,
} from "./research-execution-policy.js";
import {
  adaptHandoffResearchResult,
  deriveEvidenceContactStatuses,
  buildSupportedRelationships,
  deriveObjectiveCompletion,
  deriveNextActionFromAdapted,
  filterBlockedSources,
} from "./handoff-result-adapter.js";
import {
  assessHotelPhysicalIdentity,
  IDENTITY_STATUS,
  AIRTABLE_ID_RE,
} from "./hotel-physical-identity.js";
import {
  pendingUnknownFromJournal,
  mergeCumulativeBudgetUsage,
  reconcileContextDevCaseSpend,
} from "./research-operation-journal.js";

export const FULL_RESEARCH_WORKFLOW_VERSION = "full-research-workflow-v2";

export const RESEARCH_STAGE = Object.freeze({
  INIT: "INIT",
  IDENTITY: "IDENTITY",
  OWNERSHIP: "OWNERSHIP",
  PERSON: "PERSON",
  CONTACT: "CONTACT",
  SYNTHESIS: "SYNTHESIS",
  COMPLETE: "COMPLETE",
  STOPPED: "STOPPED",
});

export const RESEARCH_OBJECTIVE = Object.freeze({
  HOTEL_OWNERSHIP_CONTACT: "HOTEL_OWNERSHIP_CONTACT",
  HOTEL_OWNERSHIP_ONLY: "HOTEL_OWNERSHIP_ONLY",
  OWNER_PORTFOLIO: "OWNER_PORTFOLIO",
  CONTACT_INTELLIGENCE: "CONTACT_INTELLIGENCE",
  GDI_DEVELOPMENT: "GDI_DEVELOPMENT",
  ADP_RESEARCH: "ADP_RESEARCH",
  OPEN_RESEARCH: "OPEN_RESEARCH",
});

function emptyContactStatuses() {
  return {
    person_found: false,
    affiliation_evidenced: false,
    relevant_person: false,
    email_found: false,
    email_attributable: false,
    email_verified: false,
    phone_found: false,
    phone_attributable: false,
    phone_verified: false,
    per_person: [],
  };
}

function checkpoint(stage, note, extra = {}) {
  return { stage, note, ...extra };
}

function writeGuarantees() {
  return { canonical_writes: false, customer_publication: "BLOCKED" };
}

function identitiesMatch(stored, incomingHotelId, incomingObjective, incomingScope) {
  if (incomingHotelId != null && stored.hotel_id != null && String(incomingHotelId) !== String(stored.hotel_id)) {
    return { ok: false, reason: "HOTEL_ID_MISMATCH", stored: stored.hotel_id, incoming: incomingHotelId };
  }
  if (incomingObjective != null && stored.objective != null && String(incomingObjective) !== String(stored.objective)) {
    return { ok: false, reason: "OBJECTIVE_MISMATCH", stored: stored.objective, incoming: incomingObjective };
  }
  if (incomingScope != null && stored.scope != null) {
    const a = JSON.stringify(stored.scope);
    const b = JSON.stringify(incomingScope);
    if (a !== b) {
      return { ok: false, reason: "SCOPE_MISMATCH" };
    }
  }
  return { ok: true };
}

function isTerminalComplete(existing) {
  return (
    existing?.final_status === "STAGED_COMPLETE" &&
    existing?.staged_result &&
    (existing?.completed_work_keys || []).includes("workflow:research")
  );
}

/**
 * Run or resume the full research workflow with durable case persistence.
 */
export async function runFullResearchWorkflow(input = {}, budgets = {}, deps = {}) {
  const store = deps.store || createResearchCaseStore({ root: deps.storeRoot, env: deps.env });

  const objectiveCheck = validateResearchObjective(input.objective || RESEARCH_OBJECTIVE.HOTEL_OWNERSHIP_CONTACT);
  if (!objectiveCheck.ok) {
    return {
      version: FULL_RESEARCH_WORKFLOW_VERSION,
      ok: false,
      error: "UNKNOWN_OBJECTIVE",
      objective: objectiveCheck.objective,
      write_guarantees: writeGuarantees(),
    };
  }
  const requestedObjective = objectiveCheck.objective;

  const policy = buildTrustedExecutionPolicy(budgets, {
    networkBlocked: deps.networkBlocked === true,
    enableContactEnrichment: deps.enableContactEnrichment,
    env: deps.env || process.env,
  });
  if (!policy.ok) {
    return {
      version: FULL_RESEARCH_WORKFLOW_VERSION,
      ok: false,
      error: "INVALID_BUDGETS",
      invalid_keys: policy.invalid_keys,
      write_guarantees: writeGuarantees(),
    };
  }
  const trustedBudgets = policy.budgets;

  const incomingHotel = input.hotel || input.caseInput?.hotel || {};
  const incomingHotelId = incomingHotel.hotel_id || incomingHotel.id || input.hotel_id || null;
  // Explicit resume must fail closed if missing.
  // Preallocated creation requires allow_allocate_case_id (pilot bind-before-dispatch only).
  // Plain case_id without allocate flag = resume semantics (M4 / admin fail-closed).
  const explicitResume = Boolean(input.resume_case_id);
  const allowAllocate = input.allow_allocate_case_id === true;
  const preboundCaseId = input.case_id || input.resume_case_id || null;
  const requireExistingCase = explicitResume || (Boolean(input.case_id) && !allowAllocate);

  let caseId = preboundCaseId;

  if (caseId) {
    try {
      assertValidCaseId(caseId);
    } catch {
      return {
        version: FULL_RESEARCH_WORKFLOW_VERSION,
        ok: false,
        error: "INVALID_CASE_ID",
        case_id: caseId,
        write_guarantees: writeGuarantees(),
      };
    }
  }

  // --- Lock BEFORE load/mutate ---
  if (!caseId) {
    caseId = newCaseId(incomingHotelId);
  }

  const lock = store.tryAcquireLock(caseId, deps.lockTtlMs || 120_000);
  if (!lock.ok) {
    return {
      version: FULL_RESEARCH_WORKFLOW_VERSION,
      ok: false,
      case_id: caseId,
      error: "CASE_LOCKED",
      lock: lock.lock,
      write_guarantees: writeGuarantees(),
    };
  }

  const lockOpts = {
    lock_token: lock.lock.lock_token,
    fence_token: lock.lock.fence_token,
  };

  let persistenceBlocked = false;
  let persistenceBlockReason = null;

  const put = (record) => {
    const result = store.putCase(record, lockOpts);
    if (!result?.ok) {
      persistenceBlocked = true;
      persistenceBlockReason = result?.reason || result?.error || "PUT_CASE_FAILED";
    }
    return result;
  };

  const renewOrBlock = () => {
    const renewed = store.renewLease(caseId, lock.lock?.lock_token);
    if (!renewed?.ok) {
      persistenceBlocked = true;
      persistenceBlockReason = renewed?.reason || "LEASE_RENEW_FAILED";
      return renewed;
    }
    return renewed;
  };

  try {
    // Load after lock
    let existing = null;
    let resumeRequested = false;
    if (preboundCaseId || explicitResume) {
      const loaded = store.getCaseResult(caseId);
      if (loaded.corrupt) {
        return {
          version: FULL_RESEARCH_WORKFLOW_VERSION,
          ok: false,
          case_id: caseId,
          error: "CASE_CORRUPT",
          reason: loaded.reason,
          write_guarantees: writeGuarantees(),
        };
      }
      if (loaded.missing || !loaded.case) {
        if (requireExistingCase) {
          return {
            version: FULL_RESEARCH_WORKFLOW_VERSION,
            ok: false,
            case_id: caseId,
            error: "CASE_NOT_FOUND",
            write_guarantees: writeGuarantees(),
          };
        }
        // Trusted preallocated create (pilot allow_allocate_case_id only)
        existing = null;
        resumeRequested = false;
      } else {
        existing = loaded.case;
        resumeRequested = true;

        const match = identitiesMatch(
          existing,
          incomingHotelId,
          input.objective ? requestedObjective : null,
          input.scope || null
        );
        if (!match.ok) {
          return {
            version: FULL_RESEARCH_WORKFLOW_VERSION,
            ok: false,
            case_id: caseId,
            error: "CASE_IDENTITY_MISMATCH",
            mismatch: match,
            write_guarantees: writeGuarantees(),
          };
        }
      }
    } else {
      try {
        existing = store.getCase(caseId);
      } catch (err) {
        if (err?.code === "CASE_CORRUPT") {
          return {
            version: FULL_RESEARCH_WORKFLOW_VERSION,
            ok: false,
            case_id: caseId,
            error: "CASE_CORRUPT",
            reason: err.reason,
            write_guarantees: writeGuarantees(),
          };
        }
        throw err;
      }
    }

    // Recover identity/objective from stored case on resume
    const objective = existing?.objective || requestedObjective;
    const hotelId = existing?.hotel_id || incomingHotelId;
    const hotel =
      existing?.hotel_seed && resumeRequested
        ? { ...existing.hotel_seed, hotel_id: hotelId }
        : { ...incomingHotel, hotel_id: hotelId || incomingHotel.hotel_id };

    // Completed replay: read-only (audit event only — do not alter outcome state)
    if (isTerminalComplete(existing) && input.force_refresh !== true) {
      const replayPut = put({
        case_id: caseId,
        journal_entry: {
          kind: "audit",
          event: "completed_replay",
          note: "read_only_resume_no_state_mutation",
        },
        // Preserve terminal fields explicitly
        status: existing.status,
        final_status: existing.final_status,
        current_stage: existing.current_stage,
        stop_reason: existing.stop_reason,
        staged_result: existing.staged_result,
        next_action: existing.next_action,
        contact_statuses: existing.contact_statuses,
        relationships: existing.relationships,
        hotel_id: existing.hotel_id,
        objective: existing.objective,
        hotel_seed: existing.hotel_seed,
      });
      if (!replayPut?.ok) {
        return {
          version: FULL_RESEARCH_WORKFLOW_VERSION,
          ok: false,
          case_id: caseId,
          error: "PERSISTENCE_FAILED",
          reason: replayPut?.reason || "REPLAY_AUDIT_WRITE_FAILED",
          final_status: "INTERRUPTED",
          staged_result: existing.staged_result || null,
          case: existing,
          write_guarantees: writeGuarantees(),
        };
      }
      const refreshed = store.getCase(caseId);
      return {
        version: FULL_RESEARCH_WORKFLOW_VERSION,
        ok: true,
        case_id: caseId,
        stage: refreshed?.current_stage || RESEARCH_STAGE.COMPLETE,
        resumed: true,
        skipped_completed_work: true,
        final_status: refreshed?.final_status,
        staged_result: refreshed?.staged_result,
        next_action: refreshed?.next_action,
        contact_statuses: refreshed?.contact_statuses || emptyContactStatuses(),
        relationships: refreshed?.relationships || [],
        case: refreshed,
        write_guarantees: writeGuarantees(),
      };
    }

    // Unsupported objectives (except ADP skip)
    if (objective === RESEARCH_OBJECTIVE.ADP_RESEARCH && !input.force_ownership) {
      const staged = {
        finding: "OBJECTIVE_DOES_NOT_REQUIRE_OWNERSHIP_CONTACT",
        confidence: "N/A",
        limitations: ["ADP research uses separate methodology; ownership/contact not forced"],
      };
      const next_action = {
        action: "USE_ADP_PIPELINE",
        reason: "Shared research capabilities available; contact enrichment not applicable",
        pending_question: null,
        pending_questions: [],
      };
      const done = put({
        case_id: caseId,
        hotel_id: hotelId,
        objective,
        hotel_seed: hotel,
        current_stage: RESEARCH_STAGE.COMPLETE,
        status: "COMPLETE",
        final_status: "OBJECTIVE_SCOPED_SKIP",
        stop_reason: "OBJECTIVE_SCOPED_SKIP",
        staged_result: staged,
        next_action,
        checkpoint: checkpoint(RESEARCH_STAGE.COMPLETE, "adp_skip_contact"),
      });
      return {
        version: FULL_RESEARCH_WORKFLOW_VERSION,
        ok: true,
        case_id: caseId,
        stage: RESEARCH_STAGE.COMPLETE,
        stop_reason: "OBJECTIVE_SCOPED_SKIP",
        final_status: "OBJECTIVE_SCOPED_SKIP",
        staged_result: staged,
        next_action,
        contact_statuses: emptyContactStatuses(),
        relationships: [],
        case: done.case,
        write_guarantees: writeGuarantees(),
      };
    }

    if (
      objectiveCheck.unsupported_until_adapter &&
      objective !== RESEARCH_OBJECTIVE.ADP_RESEARCH &&
      !CONNECTED_VIA_OWNERSHIP(objective)
    ) {
      const blocked = put({
        case_id: caseId,
        hotel_id: hotelId,
        objective,
        hotel_seed: hotel,
        current_stage: RESEARCH_STAGE.STOPPED,
        status: "STOPPED",
        final_status: "OBJECTIVE_UNSUPPORTED",
        stop_reason: "OBJECTIVE_UNSUPPORTED",
        staged_result: {
          finding: "OBJECTIVE_UNSUPPORTED",
          limitations: [`Objective ${objective} is not connected to ownership/contact path yet`],
        },
        next_action: {
          action: "USE_PRODUCT_ADAPTER",
          reason: `No connected adapter for ${objective}`,
          pending_question: null,
          pending_questions: [],
        },
        checkpoint: checkpoint(RESEARCH_STAGE.STOPPED, "unsupported_objective"),
      });
      return {
        version: FULL_RESEARCH_WORKFLOW_VERSION,
        ok: false,
        case_id: caseId,
        error: "OBJECTIVE_UNSUPPORTED",
        final_status: "OBJECTIVE_UNSUPPORTED",
        case: blocked.case,
        write_guarantees: writeGuarantees(),
      };
    }

    const priorBudget = existing?.budget_usage || {};
    const completedWork = new Set([
      ...(existing?.completed_work_keys || []),
      ...(input.completed_work_keys || []),
    ]);

    // Context.dev allowance contract (credits, not call counts):
    // - remaining: trustedBudgets.context_dev_max is the incremental window; prior already
    //   subtracted once upstream (pilot ledger). Do not subtract again.
    // - cumulative: trustedBudgets.context_dev_max is the case cap; handoff seeds alreadySpent
    //   from journal. Do not pre-shrink max here or spend is subtracted twice.
    const priorSpentCredits = Number(priorBudget.context_dev_spent || 0);
    const allowanceMode = String(trustedBudgets.context_dev_allowance_mode || "cumulative");
    const contextDevMaxEffective = Math.max(0, Number(trustedBudgets.context_dev_max) || 0);

    const effectiveBudgets = {
      ...trustedBudgets,
      serpapi_max: remainingAllowance(trustedBudgets.serpapi_max, priorBudget.serpapi_calls),
      context_dev_max: contextDevMaxEffective,
      context_dev_allowance_mode: allowanceMode,
      context_dev_prior_spent: priorSpentCredits,
      enrichment_max: remainingAllowance(trustedBudgets.enrichment_max, priorBudget.enrichment_calls),
      disable_network: true === trustedBudgets.disable_network,
      enable_contact_enrichment: trustedBudgets.enable_contact_enrichment,
    };

    const opened = put({
      case_id: caseId,
      hotel_id: hotelId,
      objective,
      scope: existing?.scope || input.scope || { stages: Object.values(RESEARCH_STAGE) },
      current_stage: RESEARCH_STAGE.INIT,
      status: "IN_PROGRESS",
      hotel_seed: hotel,
      pending_questions: existing?.pending_questions || [],
      search_history: existing?.search_history || [],
      sources: existing?.sources || [],
      evidence: existing?.evidence || [],
      claims: existing?.claims || [],
      contradictions: existing?.contradictions || [],
      entities: existing?.entities || [],
      people: existing?.people || [],
      contacts: existing?.contacts || [],
      provider_limitations: existing?.provider_limitations || [],
      usage_restrictions: existing?.usage_restrictions || ["STAGING_ONLY", "NO_CANONICAL_PROMOTION"],
      budget_usage: priorBudget,
      completed_work_keys: [...completedWork],
      resume_checkpoint: existing?.resume_checkpoint || { stage: RESEARCH_STAGE.INIT },
      human_review_reason: existing?.human_review_reason || null,
      staged_result: existing?.staged_result || null,
      next_action: existing?.next_action || null,
      contact_statuses: existing?.contact_statuses || null,
      relationships: existing?.relationships || null,
      iterative_resume_state: existing?.iterative_resume_state || null,
      operation_journal: existing?.operation_journal,
      checkpoint: checkpoint(RESEARCH_STAGE.INIT, "case_opened"),
      journal_entry: { kind: "checkpoint", stage: RESEARCH_STAGE.INIT, event: "case_opened" },
    });
    if (!opened?.ok) {
      return {
        version: FULL_RESEARCH_WORKFLOW_VERSION,
        ok: false,
        case_id: caseId,
        error: "PERSISTENCE_FAILED",
        reason: opened?.reason || "PUT_CASE_FAILED",
        write_guarantees: writeGuarantees(),
      };
    }

    const lease0 = renewOrBlock();
    if (!lease0?.ok) {
      return {
        version: FULL_RESEARCH_WORKFLOW_VERSION,
        ok: false,
        case_id: caseId,
        error: "PERSISTENCE_FAILED",
        reason: persistenceBlockReason || "LEASE_RENEW_FAILED",
        write_guarantees: writeGuarantees(),
      };
    }

    const handoffBudgets = {};
    for (const k of OWNERSHIP_HANDOFF_ALLOWED_BUDGET_KEYS) {
      if (effectiveBudgets[k] != null) handoffBudgets[k] = effectiveBudgets[k];
    }
    handoffBudgets.disable_network = effectiveBudgets.disable_network;
    handoffBudgets.enrichment_max = effectiveBudgets.enrichment_max;
    // enable_contact_enrichment is coordinator-only — not an ownership-handoff budget key

    // Physical identity: HPC load by stable ID when available; body fields are hints only
    let physicalIdentity = null;
    const stableId =
      (input.hotel_id && AIRTABLE_ID_RE.test(String(input.hotel_id)) && String(input.hotel_id)) ||
      (hotelId && AIRTABLE_ID_RE.test(String(hotelId)) ? String(hotelId) : null);
    const hintFields = { ...incomingHotel };
    // Never allow hint hotel_id to replace the stable route/case id
    if (stableId) hintFields.hotel_id = incomingHotel.hotel_id;

    if (stableId && deps.skipPhysicalIdentity !== true) {
      physicalIdentity = await assessHotelPhysicalIdentity({
        hotel_id: stableId,
        hints: hintFields,
        deps: {
          loadRecord: deps.loadHpcRecord,
          fixtureRecord: deps.hpcFixtureRecord,
          base: deps.airtableBase,
        },
      });
    }

    let identityHotel = hotel;
    let identityOk = false;
    let identityValidation = null;

    if (physicalIdentity) {
      put({
        case_id: caseId,
        current_stage: RESEARCH_STAGE.IDENTITY,
        identity: {
          input_valid: physicalIdentity.input_valid,
          hpc_record_loaded: physicalIdentity.hpc_record_loaded,
          physical_identity_sufficiently_supported:
            physicalIdentity.physical_identity_sufficiently_supported,
          identity_status: physicalIdentity.identity_status,
          match_status: physicalIdentity.match_status,
          conflicts: physicalIdentity.conflicts || [],
          matching_reasons: physicalIdentity.matching_reasons || [],
          note: physicalIdentity.note,
          detail: physicalIdentity.detail,
        },
        hpc_record: physicalIdentity.hpc_record,
        checkpoint: checkpoint(
          RESEARCH_STAGE.IDENTITY,
          physicalIdentity.physical_identity_sufficiently_supported
            ? "physical_identity_supported"
            : "physical_identity_blocked"
        ),
      });

      if (physicalIdentity.blocks_ownership_contact || !physicalIdentity.physical_identity_sufficiently_supported) {
        const stopCode =
          physicalIdentity.identity_status === IDENTITY_STATUS.AMBIGUOUS
            ? "IDENTITY_AMBIGUOUS"
            : physicalIdentity.identity_status === IDENTITY_STATUS.HPC_NOT_FOUND
              ? "HPC_NOT_FOUND"
              : "IDENTITY_UNRESOLVED";
        const next_action = deriveNextActionFromAdapted({
          stage: RESEARCH_STAGE.IDENTITY,
          adapted: { ok: false },
          contactStatuses: emptyContactStatuses(),
          objective,
          stopReason: "IDENTITY_UNRESOLVED",
        });
        next_action.reason = physicalIdentity.detail || next_action.reason;
        const staged = {
          finding: stopCode,
          confidence: "UNRESOLVED",
          unknown_code: "AMBIGUOUS_ENTITY",
          identity_status: physicalIdentity.identity_status,
          conflicts: physicalIdentity.conflicts || [],
          limitations: [
            "Physical hotel identity not sufficiently supported — ownership/contact blocked",
            physicalIdentity.note,
          ].filter(Boolean),
        };
        const stopped = put({
          case_id: caseId,
          hotel_id: hotelId,
          hotel_seed: physicalIdentity.hotel || hotel,
          current_stage: RESEARCH_STAGE.STOPPED,
          status: "STOPPED",
          final_status: stopCode,
          stop_reason: stopCode,
          human_review_reason: next_action.reason,
          pending_questions: next_action.pending_questions || [],
          staged_result: staged,
          next_action,
          checkpoint: checkpoint(RESEARCH_STAGE.STOPPED, "identity_block"),
        });
        return {
          version: FULL_RESEARCH_WORKFLOW_VERSION,
          ok: false,
          case_id: caseId,
          stage: RESEARCH_STAGE.STOPPED,
          stop_reason: stopCode,
          identity: {
            input_valid: physicalIdentity.input_valid,
            hpc_record_loaded: physicalIdentity.hpc_record_loaded,
            physical_identity_sufficiently_supported: false,
            identity_status: physicalIdentity.identity_status,
          },
          staged_result: staged,
          next_action,
          contact_statuses: emptyContactStatuses(),
          relationships: [],
          case: stopped.case,
          write_guarantees: writeGuarantees(),
        };
      }

      identityHotel = physicalIdentity.hotel;
      identityOk = true;
      identityValidation = { ok: true, hotel: identityHotel, physical: physicalIdentity };
      put({
        case_id: caseId,
        hotel_id: stableId,
        hotel_seed: identityHotel,
        identity: {
          input_valid: true,
          hpc_record_loaded: true,
          physical_identity_sufficiently_supported: true,
          identity_status: physicalIdentity.identity_status,
          match_status: physicalIdentity.match_status,
          conflicts: physicalIdentity.conflicts || [],
          matching_reasons: physicalIdentity.matching_reasons || [],
          note: physicalIdentity.note,
        },
      });
    } else {
      // Fallback: seed field validation only (non-Airtable synthetic IDs / offline fixtures)
      identityValidation = validateOwnershipHandoffInput({ hotel }, handoffBudgets);
      identityOk =
        identityValidation?.ok !== false &&
        Boolean(String(hotel.hotel_name || hotel.name || "").trim()) &&
        Boolean(String(hotel.country || "").trim());
      identityHotel = identityValidation.hotel || hotel;

      put({
        case_id: caseId,
        current_stage: RESEARCH_STAGE.IDENTITY,
        identity: {
          input_valid: identityOk,
          hpc_record_loaded: false,
          physical_identity_sufficiently_supported: false,
          identity_status: identityOk ? "SEED_VALIDATED_NO_HPC" : IDENTITY_STATUS.UNRESOLVED,
          validation: identityValidation,
          unresolved_issues: identityOk ? [] : ["IDENTITY_INCOMPLETE"],
          note: "Seed field validation only — no HPC record ID for physical confirmation",
        },
        checkpoint: checkpoint(RESEARCH_STAGE.IDENTITY, identityOk ? "identity_seed_ok" : "identity_unresolved"),
      });

      if (!identityOk) {
        const next_action = deriveNextActionFromAdapted({
          stage: RESEARCH_STAGE.IDENTITY,
          adapted: { ok: false },
          contactStatuses: emptyContactStatuses(),
          objective,
          stopReason: "IDENTITY_UNRESOLVED",
        });
        const staged = {
          finding: "IDENTITY_UNRESOLVED",
          confidence: "UNRESOLVED",
          unknown_code: "AMBIGUOUS_ENTITY",
          limitations: ["Physical identity incomplete — dependent ownership/contact blocked"],
        };
        const stopped = put({
          case_id: caseId,
          current_stage: RESEARCH_STAGE.STOPPED,
          status: "STOPPED",
          final_status: "IDENTITY_UNRESOLVED",
          stop_reason: "IDENTITY_UNRESOLVED",
          human_review_reason: next_action.reason,
          pending_questions: next_action.pending_questions || [],
          staged_result: staged,
          next_action,
          checkpoint: checkpoint(RESEARCH_STAGE.STOPPED, "identity_block"),
        });
        return {
          version: FULL_RESEARCH_WORKFLOW_VERSION,
          ok: false,
          case_id: caseId,
          stage: RESEARCH_STAGE.STOPPED,
          stop_reason: "IDENTITY_UNRESOLVED",
          identity: {
            input_valid: false,
            hpc_record_loaded: false,
            physical_identity_sufficiently_supported: false,
            identity_status: IDENTITY_STATUS.UNRESOLVED,
          },
          staged_result: staged,
          next_action,
          contact_statuses: emptyContactStatuses(),
          relationships: [],
          case: stopped.case,
          write_guarantees: writeGuarantees(),
        };
      }
    }

    // Re-validate budgets against authoritative hotel seed
    if (!physicalIdentity) {
      /* seed path already validated */
    } else {
      const reval = validateOwnershipHandoffInput({ hotel: identityHotel }, handoffBudgets);
      if (reval?.ok === false && reval.error === "UNSUPPORTED_BUDGET_KEYS") {
        return {
          version: FULL_RESEARCH_WORKFLOW_VERSION,
          ok: false,
          case_id: caseId,
          error: "INVALID_BUDGETS",
          write_guarantees: writeGuarantees(),
        };
      }
      identityValidation = reval?.ok !== false ? reval : { ok: true, hotel: identityHotel, budgets: handoffBudgets };
    }

    // Owner reuse — pass package into research; never copy hotel edges
    let ownerReusePackage = null;
    let ownerReuseNote = null;
    const reuseOwnerId =
      input.owner_entity_id ||
      existing?.research_snapshot?.ownership?.owner_entity_id ||
      existing?.staged_result?.ownership?.owner_entity_id ||
      null;
    if (reuseOwnerId) {
      const reused = store.getOwnerReuse(reuseOwnerId);
      if (reused && !reused.usage_rights_blocked && reused.usage_rights !== "BLOCKED") {
        ownerReusePackage = reused;
        ownerReuseNote = {
          owner_entity_id: reuseOwnerId,
          reused_fields: ["organization_domain", "people_candidates"].filter(
            (f) => reused[f] != null || (f === "people_candidates" && reused.people)
          ),
          hotel_relationships_copied: false,
          evidence_passed_to_research: true,
          note: "Org/person research reusable; hotel→owner edges require hotel-specific evidence",
        };
      }
    }

    put({
      case_id: caseId,
      current_stage: RESEARCH_STAGE.OWNERSHIP,
      checkpoint: checkpoint(RESEARCH_STAGE.OWNERSHIP, "ownership_research_start"),
      journal_entry: {
        kind: "reservation",
        stage: RESEARCH_STAGE.OWNERSHIP,
        event: "research_dispatch",
        budgets: {
          serpapi_max: effectiveBudgets.serpapi_max,
          context_dev_max: effectiveBudgets.context_dev_max,
          enrichment_max: effectiveBudgets.enrichment_max,
          disable_network: effectiveBudgets.disable_network,
        },
      },
    });

    const enableContact = Boolean(effectiveBudgets.enable_contact_enrichment);

    const workflowDeps = {
      research: deps.research,
      enableContactEnrichment: enableContact,
      surfeEnrichFn: effectiveBudgets.disable_network ? undefined : deps.surfeEnrichFn,
      pdlEnrichFn: effectiveBudgets.disable_network ? undefined : deps.pdlEnrichFn,
      search: deps.search,
      scrape: deps.scrape,
      isConfigured: deps.isConfigured,
      serpGoogle: deps.serpGoogle,
      contextDevExtract: deps.contextDevExtract,
      modelOwnershipDocumentArm: deps.modelOwnershipDocumentArm,
      proposeModelFollowUpSearches: deps.proposeModelFollowUpSearches,
      callModelJson: deps.callModelJson,
      operation_journal: existing?.operation_journal || [],
      forceRetryUnknown: input.force_retry_unknown === true,
      preDispatchGate: deps.preDispatchGate || null,
      nowFn: deps.nowFn || null,
      onOperationCheckpoint: async (entry) => {
        if (persistenceBlocked) {
          return { ok: false, reason: persistenceBlockReason || "PERSISTENCE_BLOCKED" };
        }
        // Renew before put so short lock TTLs (interrupt tests) cannot expire mid-write.
        const leaseBefore = renewOrBlock();
        if (!leaseBefore?.ok) {
          return { ok: false, reason: persistenceBlockReason || "LEASE_RENEW_FAILED" };
        }
        const prior = store.getCase(caseId)?.budget_usage || priorBudget || {};
        const fromEntry = entry.budget_usage || {};
        const putResult = put({
          case_id: caseId,
          journal_entry: { kind: "operation", ...entry },
          resume_checkpoint: entry,
          completed_work_keys:
            (entry.status === "COMPLETED" ||
              entry.status === "FAILED" ||
              entry.event === "durable_result_written") &&
            entry.work_key
              ? [entry.work_key]
              : undefined,
          budget_usage: mergeCumulativeBudgetUsage(prior, {
            ...fromEntry,
            last_reservation_credits: entry.reservation_credits ?? prior.last_reservation_credits ?? null,
          }),
        });
        if (!putResult?.ok) {
          persistenceBlocked = true;
          persistenceBlockReason = putResult?.reason || "PUT_CASE_FAILED";
          return { ok: false, reason: persistenceBlockReason };
        }
        // After durable put succeeds, notify observers (interrupt kill hooks) even if
        // a follow-on lease renew fails — the write is already on disk.
        if (typeof deps.onOperationCheckpoint === "function") {
          await deps.onOperationCheckpoint(entry);
        }
        const leaseAfter = renewOrBlock();
        if (!leaseAfter?.ok) {
          return { ok: false, reason: persistenceBlockReason || "LEASE_RENEW_FAILED" };
        }
        return { ok: true };
      },
      preDispatchGate: async (gateArgs) => {
        if (persistenceBlocked) {
          return { ok: false, reason: persistenceBlockReason || "PERSISTENCE_BLOCKED" };
        }
        if (typeof deps.preDispatchGate === "function") {
          return deps.preDispatchGate(gateArgs);
        }
        return { ok: true };
      },
      budget_usage: priorBudget,
    };

    const pendingUnknown = pendingUnknownFromJournal(existing?.operation_journal || []);
    const caseInput = {
      hotel: identityHotel,
      enable_contact_enrichment: enableContact,
      iterative_resume_state: (() => {
        const prior = existing?.iterative_resume_state || input.iterative_resume_state;
        const journalResults = {};
        for (const e of existing?.operation_journal || []) {
          if (e?.work_key && e.durable_result) journalResults[e.work_key] = e.durable_result;
        }
        if (!prior && pendingUnknown.size === 0 && !Object.keys(journalResults).length) return prior;
        return {
          ...(prior || {}),
          pending_unknown_keys: [
            ...new Set([...(prior?.pending_unknown_keys || []), ...pendingUnknown]),
          ],
          result_by_work_key: {
            ...(prior?.result_by_work_key || {}),
            ...journalResults,
          },
          completed_work_keys: [
            ...new Set([
              ...(prior?.completed_work_keys || []),
              ...(existing?.completed_work_keys || []),
            ]),
          ],
        };
      })(),
      inspect_urls: existing?.inspect_urls || input.inspect_urls,
      ownership_seed: input.ownership_seed,
      owner_entity_id: reuseOwnerId || input.owner_entity_id,
      owner_reuse_package: ownerReusePackage,
      person_hypotheses: ownerReusePackage?.people || input.person_hypotheses,
      operation_journal: existing?.operation_journal || [],
      iterative_ownership_loop: input.iterative_ownership_loop !== false,
      adaptive_ownership_controller:
        input.adaptive_ownership_controller === true ||
        handoffBudgets?.adaptive_ownership_controller === true ||
        String(process.env.ADAPTIVE_OWNERSHIP_CONTROLLER || "") === "1",
    };

    let workflow;
    try {
      const workflowBudgets = {
        ...(identityValidation?.budgets || handoffBudgets),
        enrichment_max: effectiveBudgets.enrichment_max,
      };
      // Coordinator-only flags must not enter ownership handoff budget contract
      delete workflowBudgets.enable_contact_enrichment;
      workflow = await runHotelOwnershipContactWorkflow(
        caseInput,
        workflowBudgets,
        workflowDeps
      );
    } catch (err) {
      const errCode = String(err?.code || err?.name || "EXECUTION_INTERRUPTED").slice(0, 80);
      const errDetail = String(err?.message || err)
        .replace(/(sk-|key|token|secret|password|Bearer)\S+/gi, "[redacted]")
        .slice(0, 240);
      const interrupted = put({
        case_id: caseId,
        current_stage: existing?.current_stage || RESEARCH_STAGE.OWNERSHIP,
        status: "INTERRUPTED",
        final_status: "INTERRUPTED",
        stop_reason: "EXECUTION_INTERRUPTED",
        human_review_reason: "Research interrupted — resume from checkpoint",
        pending_questions: ["Resume case after interruption"],
        checkpoint: checkpoint(RESEARCH_STAGE.STOPPED, "interrupted", {
          error_code: "EXECUTION_INTERRUPTED",
        }),
        journal_entry: {
          kind: "failure",
          event: "execution_interrupted",
          error_code: "EXECUTION_INTERRUPTED",
          error_class: errCode,
          error_detail_sanitized: errDetail,
        },
      });
      return {
        version: FULL_RESEARCH_WORKFLOW_VERSION,
        ok: false,
        case_id: caseId,
        error: "EXECUTION_INTERRUPTED",
        error_class: errCode,
        error_detail_sanitized: errDetail,
        resumable: true,
        case: interrupted.case,
        write_guarantees: writeGuarantees(),
      };
    }

    completedWork.add("workflow:research");
    const adapted = adaptHandoffResearchResult(workflow);
    const contactStatuses = deriveEvidenceContactStatuses(adapted, workflow);
    const relationships = buildSupportedRelationships(adapted);

    // Authoritative Context.dev spend: unique journal ops / ledger incremental.
    // Do NOT Math.max absolute journal totals with orchestration call-log deltas.
    const caseAfterResearch = store.getCase(caseId);
    const spendReconcile = reconcileContextDevCaseSpend({
      priorSpent: Number(priorBudget.context_dev_spent || 0),
      priorCalls: Number(priorBudget.context_dev_calls || 0),
      allowanceMode,
      ledgerSnapshot: adapted.research?.budgets?.context_dev || workflow?.budgets?.context_dev || null,
      operationJournal: caseAfterResearch?.operation_journal || existing?.operation_journal || [],
      orchestrationCallCount: Number(adapted.budget_delta?.context_dev_calls || 0),
    });

    const budget_usage = {
      context_dev_spent: spendReconcile.context_dev_spent,
      context_dev_calls: spendReconcile.context_dev_calls,
      context_dev_outstanding: spendReconcile.outstanding_credits,
      context_dev_reconciliation_source: spendReconcile.reconciliation_source,
      serpapi_calls: (priorBudget.serpapi_calls || 0) + adapted.budget_delta.serpapi_calls,
      serpapi_usd: (priorBudget.serpapi_usd || 0) + adapted.budget_delta.serpapi_usd,
      enrichment_calls: (priorBudget.enrichment_calls || 0) + adapted.budget_delta.enrichment_calls,
      model_calls: (priorBudget.model_calls || 0) + adapted.budget_delta.model_calls,
      model_usd: (priorBudget.model_usd || 0) + adapted.budget_delta.model_usd,
      external_usd: (priorBudget.external_usd || 0) + adapted.budget_delta.external_usd,
    };

    if (ownerReuseNote && ownerReusePackage) {
      store.putOwnerReuse(reuseOwnerId, {
        increment_reuse: true,
        last_reused_by_case: caseId,
      });
    }

    put({
      case_id: caseId,
      current_stage: RESEARCH_STAGE.PERSON,
      research_snapshot: {
        ok: adapted.ok,
        ownership: adapted.ownership || null,
        people_count: adapted.people.length,
        qualifies_for_fullenrich: adapted.qualifies_for_fullenrich,
      },
      people: adapted.people,
      sources: filterBlockedSources([...(existing?.sources || []), ...adapted.sources]),
      domain_evidence: filterBlockedSources(adapted.domain_evidence),
      evidence: filterBlockedSources([
        ...(adapted.ownership?.evidence_refs || []),
        ...adapted.people.flatMap((p) => p.evidence || []),
      ]),
      claims: adapted.research.claims || relationships,
      contradictions: adapted.contradictions,
      unresolved_reasons: adapted.unresolved_reasons,
      pending_questions: adapted.pending_questions,
      relationships,
      contact_statuses: contactStatuses,
      owner_reuse: ownerReuseNote,
      budget_usage,
      completed_work_keys: [...completedWork],
      iterative_resume_state: adapted.iterative_resume_state,
      search_history: [
        ...(existing?.search_history || []),
        ...(adapted.research.queries_attempted || adapted.research.calls || []).slice(0, 50),
      ],
      checkpoint: checkpoint(RESEARCH_STAGE.PERSON, "person_stage"),
      journal_entry: {
        kind: "settlement",
        stage: RESEARCH_STAGE.PERSON,
        event: "research_returned",
        budget_delta: adapted.budget_delta,
      },
    });

    if (enableContact) {
      put({
        case_id: caseId,
        current_stage: RESEARCH_STAGE.CONTACT,
        contact_enrichment: workflow.contact_enrichment,
        checkpoint: checkpoint(RESEARCH_STAGE.CONTACT, "contact_stage"),
      });
    }

    const ownerId = adapted.ownership?.owner_entity_id;
    if (ownerId && adapted.ok) {
      store.putOwnerReuse(ownerId, {
        owner_display_name: adapted.ownership?.owner_display_name,
        organization_domain: adapted.confirmed_company_domain_host || null,
        people: adapted.people.map((p) => ({
          display_name: p.display_name,
          title: p.title,
          publication_label: p.publication_label,
          evidence: p.evidence || [],
          channels: (p.channels || []).map((c) => ({
            kind: c.kind,
            attribution: c.attribution,
            // values omitted from reuse package when restricted
          })),
        })),
        domain_evidence: adapted.domain_evidence,
        usage_rights: "INTERNAL_ONLY",
        hotel_relationships_copied: false,
        provenance_case_id: caseId,
      });
    }

    const completion = deriveObjectiveCompletion({
      objective,
      adapted,
      contactStatuses,
      relationships,
    });

    const stopReason =
      completion.stop_reason ||
      (!adapted.ok ? adapted.research?.error || "RESEARCH_FAILED" : null);

    const next_action = deriveNextActionFromAdapted({
      stage: RESEARCH_STAGE.SYNTHESIS,
      adapted,
      contactStatuses,
      objective,
      stopReason,
      completion,
    });

    const staged_result = {
      finding: adapted.ok
        ? adapted.ownership_supported
          ? `${adapted.research.hotel_name || hotelId} → ${adapted.ownership.owner_display_name}`
          : "INSUFFICIENT_OWNERSHIP_EVIDENCE"
        : "RESEARCH_FAILED",
      confidence: adapted.ownership?.confidence || (adapted.ok ? "LOW" : "UNRESOLVED"),
      unknown_code:
        adapted.research.unknown_code ||
        (!adapted.ownership_supported ? "INSUFFICIENT_EVIDENCE" : null),
      ownership: adapted.ownership || null,
      people: adapted.people.map((p) => ({
        name: p.display_name,
        title: p.title,
        publication_label: p.publication_label,
      })),
      contact_statuses: contactStatuses,
      relationships,
      unresolved_reasons: adapted.unresolved_reasons,
      contradictions: adapted.contradictions,
      limitations: [
        ...(adapted.research.limitations || []),
        "STAGED_ONLY",
        "NO_CANONICAL_PROMOTION",
        "CUSTOMER_PUBLICATION_BLOCKED",
        effectiveBudgets.disable_network ? "NETWORK_DISABLED_OFFLINE" : null,
      ].filter(Boolean),
      provider_limitations: adapted.research.provider_limitations || [],
      usage_restrictions: {
        surfe_customer_publication: "BLOCKED",
        storage: "INTERNAL_STAGING",
      },
    };

    let saved;
    try {
      saved = put({
        case_id: caseId,
        current_stage: RESEARCH_STAGE.COMPLETE,
        status: completion.final_status,
        final_status: completion.final_status,
        stop_reason: stopReason,
        human_review_reason: next_action.action === "HUMAN_REVIEW" ? next_action.reason : null,
        pending_questions: next_action.pending_questions || [],
        staged_result,
        next_action,
        contact_statuses: contactStatuses,
        relationships,
        sources: filterBlockedSources([...(existing?.sources || []), ...adapted.sources]),
        contradictions: adapted.contradictions,
        unresolved_reasons: adapted.unresolved_reasons,
        budget_usage,
        iterative_resume_state: adapted.iterative_resume_state,
        workflow_version: workflow.version,
        full_workflow_version: FULL_RESEARCH_WORKFLOW_VERSION,
        case_store_version: RESEARCH_CASE_STORE_VERSION,
        write_guarantees: writeGuarantees(),
        checkpoint: checkpoint(RESEARCH_STAGE.COMPLETE, completion.final_status),
        journal_entry: {
          kind: "completion",
          event: completion.final_status,
          complete: completion.complete,
        },
      });
    } catch (persistErr) {
      persistenceBlocked = true;
      persistenceBlockReason = persistErr?.message || "FINAL_STAGED_SAVE_THREW";
      return {
        version: FULL_RESEARCH_WORKFLOW_VERSION,
        ok: false,
        case_id: caseId,
        error: "PERSISTENCE_FAILED",
        reason: persistenceBlockReason,
        final_status: "INTERRUPTED",
        stop_reason: "PERSISTENCE_FAILED",
        resumable: true,
        staged_result: existing?.staged_result || null,
        next_action: {
          action: "RESUME_AFTER_PERSISTENCE_FAILURE",
          reason: "Final staged-result save threw — last durable state preserved",
          pending_questions: ["Retry case after storage recovery"],
        },
        contact_statuses: existing?.contact_statuses || contactStatuses,
        relationships: existing?.relationships || relationships,
        budget_usage: existing?.budget_usage || priorBudget,
        case: store.getCase(caseId),
        write_guarantees: writeGuarantees(),
      };
    }

    if (!saved?.ok) {
      return {
        version: FULL_RESEARCH_WORKFLOW_VERSION,
        ok: false,
        case_id: caseId,
        error: "PERSISTENCE_FAILED",
        reason: saved?.reason || "FINAL_STAGED_SAVE_FAILED",
        final_status: "INTERRUPTED",
        stop_reason: "PERSISTENCE_FAILED",
        resumable: true,
        staged_result: existing?.staged_result || null,
        next_action: {
          action: "RESUME_AFTER_PERSISTENCE_FAILURE",
          reason: "Final staged-result save failed — last durable state preserved",
          pending_questions: ["Retry case after storage recovery"],
        },
        contact_statuses: existing?.contact_statuses || contactStatuses,
        relationships: existing?.relationships || relationships,
        budget_usage: existing?.budget_usage || budget_usage,
        case: store.getCase(caseId),
        write_guarantees: writeGuarantees(),
      };
    }

    return {
      version: FULL_RESEARCH_WORKFLOW_VERSION,
      ok: Boolean(adapted.ok),
      case_id: caseId,
      stage: RESEARCH_STAGE.COMPLETE,
      final_status: completion.final_status,
      stop_reason: saved.case?.stop_reason,
      staged_result: saved.case?.staged_result || staged_result,
      next_action,
      contact_statuses: contactStatuses,
      relationships,
      enrichment_subjects: workflow.enrichment_subjects || [],
      rejected_pre_submission: workflow.rejected_pre_submission || [],
      contact_enrichment: workflow.contact_enrichment,
      research: adapted.research,
      owner_reuse: ownerReuseNote,
      budget_usage,
      policy: {
        disable_network: effectiveBudgets.disable_network,
        enable_contact_enrichment: enableContact,
        unknown_budget_keys: policy.unknown_keys,
      },
      case: saved.case,
      write_guarantees: writeGuarantees(),
    };
  } finally {
    store.releaseLock(caseId, lock.lock?.lock_token);
  }
}

function CONNECTED_VIA_OWNERSHIP(objective) {
  return (
    objective === RESEARCH_OBJECTIVE.HOTEL_OWNERSHIP_CONTACT ||
    objective === RESEARCH_OBJECTIVE.HOTEL_OWNERSHIP_ONLY ||
    objective === RESEARCH_OBJECTIVE.CONTACT_INTELLIGENCE
  );
}

export function loadResearchCase(caseId, deps = {}) {
  const store = deps.store || createResearchCaseStore({ root: deps.storeRoot, env: deps.env });
  try {
    assertValidCaseId(caseId);
  } catch {
    return { ok: false, error: "INVALID_CASE_ID" };
  }
  const result = store.getCaseResult(caseId);
  if (result.corrupt) return { ok: false, error: "CASE_CORRUPT", reason: result.reason };
  if (result.missing || !result.case) return { ok: false, error: "CASE_NOT_FOUND" };
  return { ok: true, case: result.case };
}

export default runFullResearchWorkflow;
