/**
 * State-based next-action controller for Dealality Full E2E OS.
 *
 * On every iteration the controller:
 * 1. Reads authoritative run state
 * 2. Finds next incomplete required stage
 * 3. Inspects blockers / auth / budget / provider reuse
 * 4. Selects the next justified action (not LLM-invented)
 * 5. Returns an executable plan (caller may invoke underlying CLI)
 *
 * AI research agents may recommend research strategies but MUST NOT redefine
 * quality gates, budgets, schemas, or publication rules.
 */

import fs from "node:fs";
import path from "node:path";
import {
  EXECUTION_STATE,
  PRODUCT,
  RUN_MODE,
  OVERALL_RUN_STATUS,
  isTerminalExecutionState,
  isWaitingState,
} from "./execution-states.js";
import { getGdiWorkflowDefinition } from "./gdi-full-e2e-workflow-v1.js";
import { getAdpWorkflowDefinition } from "./adp-full-e2e-workflow-v1.js";
import { buildStageResult } from "./stage-result.js";

export const NEXT_ACTION_EXECUTOR_VERSION = "dealality-next-action-executor-v1";

const ZERO_PROGRESS_LIMIT = 3;

/**
 * @param {object} opts
 * @param {'GDI'|'ADP'} opts.product
 * @param {string} opts.hotelId
 * @param {string} [opts.mode]
 * @param {object} [opts.runState] durable run journal
 * @param {object} [opts.authorizations]
 * @param {object} [opts.budget]
 */
export function selectNextAction({
  product,
  hotelId,
  mode = RUN_MODE.STATUS,
  runState = null,
  authorizations = {},
  budget = {},
  zeroProgressStreak = 0,
} = {}) {
  const workflow =
    product === PRODUCT.ADP
      ? getAdpWorkflowDefinition()
      : getGdiWorkflowDefinition();

  if (!hotelId) {
    return buildPlan({
      product,
      hotelId: null,
      mode,
      workflow,
      overallStatus: OVERALL_RUN_STATUS.FAILED,
      nextAction: {
        type: "BLOCKED",
        code: "MISSING_HOTEL_ID",
        message: "Required: hotel HPC id (or ADP property id for ADP)",
      },
      blockers: [{ code: "MISSING_HOTEL_ID" }],
    });
  }

  const stages = workflow.requiredStages || [];
  const completed = new Set(
    (runState?.completedStageIds || [])
      .concat(
        Object.entries(runState?.stageResults || {})
          .filter(([, r]) => r?.state === EXECUTION_STATE.COMPLETE)
          .map(([id]) => id)
      )
      .filter(Boolean)
  );

  const stageResults = runState?.stageResults || {};
  let nextStage = null;
  for (const stage of stages) {
    if (!stage.required) continue;
    const prior = stageResults[stage.id];
    if (prior && isWaitingState(prior.state)) {
      return buildPlan({
        product,
        hotelId,
        mode,
        workflow,
        overallStatus: OVERALL_RUN_STATUS.RUNNING,
        nextAction: {
          type: "WAIT",
          code: prior.state,
          stageId: stage.id,
          message: `Stage ${stage.id} is ${prior.state}`,
          providerJobIds: prior.providerJobIds || [],
        },
        currentStage: stage,
        blockers: prior.blockers || [],
      });
    }
    if (prior && prior.state === EXECUTION_STATE.FAILED) {
      return buildPlan({
        product,
        hotelId,
        mode,
        workflow,
        overallStatus: OVERALL_RUN_STATUS.FAILED,
        nextAction: {
          type: "FAILED",
          code: "STAGE_FAILED",
          stageId: stage.id,
          message: `Stage ${stage.id} failed — inspect blockers before retry`,
        },
        currentStage: stage,
        blockers: prior.blockers || [],
      });
    }
    if (!completed.has(stage.id)) {
      nextStage = stage;
      break;
    }
  }

  if (!nextStage) {
    const evaluatorComplete =
      runState?.evaluatorStatus === "COMPLETE" ||
      runState?.overallStatus === OVERALL_RUN_STATUS.FULL_E2E_COMPLETE;
    return buildPlan({
      product,
      hotelId,
      mode,
      workflow,
      overallStatus: evaluatorComplete
        ? OVERALL_RUN_STATUS.FULL_E2E_COMPLETE
        : OVERALL_RUN_STATUS.WAITING_PUBLICATION_AUTHORIZATION,
      nextAction: evaluatorComplete
        ? {
            type: "DONE",
            code: "ALL_REQUIRED_STAGES_COMPLETE",
            message:
              "All OS required stages complete — confirm independent evaluator FULL_E2E_COMPLETE before claiming delivery",
          }
        : {
            type: "EVALUATE",
            code: "RUN_INDEPENDENT_COMPLETION_EVALUATOR",
            message:
              "Stages complete at OS layer — run independent completion evaluator / delivery verify",
            command:
              product === PRODUCT.GDI
                ? `node scripts/gdi-full-e2e-status-v1.mjs --hotel ${hotelId}`
                : `node scripts/dealality-adp-full-e2e.mjs --hotel ${hotelId} --mode status`,
          },
      blockers: [],
    });
  }

  // Mode / auth gates
  const modeCfg = workflow.modes?.[mode] || workflow.modes?.[RUN_MODE.STATUS];
  const authNeed = nextStage.authorization || null;

  if (
    authNeed === "authorize-paid-discovery" ||
    authNeed === "adp-authorization-ledger"
  ) {
    const paidAllowed =
      modeCfg?.allowPaidDiscovery === true ||
      modeCfg?.allowPaidMeasurement === true;
    const authOk =
      authorizations.paid === true ||
      authorizations.authorizePaidDiscovery === true ||
      authorizations.measurement === true;
    if (!paidAllowed || mode === RUN_MODE.DRY_RUN || mode === RUN_MODE.VALIDATE) {
      return buildPlan({
        product,
        hotelId,
        mode,
        workflow,
        overallStatus: OVERALL_RUN_STATUS.PARTIAL,
        nextAction: {
          type: "SKIP_OR_PLAN",
          code: "PAID_NOT_PERMITTED_IN_MODE",
          stageId: nextStage.id,
          message: `Mode ${mode} does not permit paid stage ${nextStage.id}; continue with reuse/validate path`,
          command: buildUnderlyingCommand({
            product,
            hotelId,
            mode: RUN_MODE.VALIDATE,
            authorizations,
            budget,
          }),
        },
        currentStage: nextStage,
        blockers: [],
      });
    }
    if (!authOk) {
      return buildPlan({
        product,
        hotelId,
        mode,
        workflow,
        overallStatus: OVERALL_RUN_STATUS.BLOCKED,
        nextAction: {
          type: "WAIT_AUTHORIZATION",
          code: EXECUTION_STATE.WAITING_AUTHORIZATION,
          stageId: nextStage.id,
          message: `Stage ${nextStage.id} requires paid authorization for hotel ${hotelId}`,
        },
        currentStage: nextStage,
        blockers: [
          {
            code: "WAITING_AUTHORIZATION",
            authNeed,
            hotelId,
          },
        ],
      });
    }
    if (budget?.remainingUsd != null && Number(budget.remainingUsd) <= 0) {
      return buildPlan({
        product,
        hotelId,
        mode,
        workflow,
        overallStatus: OVERALL_RUN_STATUS.BLOCKED,
        nextAction: {
          type: "WAIT_AUTHORIZATION",
          code: "BUDGET_EXHAUSTED",
          stageId: nextStage.id,
          message: `No remaining budget for hotel ${hotelId} / product ${product}`,
        },
        currentStage: nextStage,
        blockers: [{ code: "BUDGET_EXHAUSTED", remainingUsd: 0 }],
      });
    }
  }

  if (authNeed === "authorize-publish") {
    if (
      !authorizations.publish &&
      !authorizations.authorizePublish &&
      mode !== RUN_MODE.NEW &&
      mode !== RUN_MODE.REFRESH
    ) {
      // Still allow planning publication without applying.
    }
    if (!authorizations.publish && !authorizations.authorizePublish) {
      return buildPlan({
        product,
        hotelId,
        mode,
        workflow,
        overallStatus: OVERALL_RUN_STATUS.WAITING_PUBLICATION_AUTHORIZATION,
        nextAction: {
          type: "WAIT_AUTHORIZATION",
          code: "PUBLICATION_AUTH_REQUIRED",
          stageId: nextStage.id,
          message:
            "Research may be staged; customer publication requires explicit --authorize-publish",
        },
        currentStage: nextStage,
        blockers: [{ code: "PUBLICATION_AUTH_REQUIRED" }],
      });
    }
  }

  if (zeroProgressStreak >= ZERO_PROGRESS_LIMIT) {
    return buildPlan({
      product,
      hotelId,
      mode,
      workflow,
      overallStatus: OVERALL_RUN_STATUS.BLOCKED,
      nextAction: {
        type: "STOP_ZERO_PROGRESS",
        code: "ZERO_PROGRESS_CYCLE",
        stageId: nextStage.id,
        message: `Detected ${zeroProgressStreak} zero-progress iterations — refuse indefinite retry`,
      },
      currentStage: nextStage,
      blockers: [{ code: "ZERO_PROGRESS_CYCLE", streak: zeroProgressStreak }],
    });
  }

  // Section completeness — independent product certification (no paid remonitor).
  if (nextStage.id === "section_completeness_certification") {
    return buildPlan({
      product,
      hotelId,
      mode,
      workflow,
      overallStatus: OVERALL_RUN_STATUS.RUNNING,
      nextAction: {
        type: "EXECUTE",
        code: "ASSESS_SECTION_COMPLETENESS",
        stageId: nextStage.id,
        message:
          "Assess every ADP report section independently; repair DATA_EXISTS_NOT_CONNECTED where authorized; block publication readiness on material gaps",
        command: `node scripts/dealality-adp-product-completeness-audit-v1.mjs`,
        module: "lib/ai-demand-positioning/certification/adp-section-completeness-v1.js",
        reuseProviderJobs: true,
      },
      currentStage: nextStage,
      blockers: [],
    });
  }

  const command = buildUnderlyingCommand({
    product,
    hotelId,
    mode,
    authorizations,
    budget,
    stage: nextStage,
    resumeRun: runState?.runId || null,
  });

  return buildPlan({
    product,
    hotelId,
    mode,
    workflow,
    overallStatus: OVERALL_RUN_STATUS.RUNNING,
    nextAction: {
      type: "EXECUTE",
      code: "EXECUTE_NEXT_STAGE",
      stageId: nextStage.id,
      message: `Execute stage ${nextStage.id}`,
      command,
      reuseProviderJobs: true,
    },
    currentStage: nextStage,
    blockers: [],
  });
}

function buildUnderlyingCommand({
  product,
  hotelId,
  mode,
  authorizations = {},
  budget = {},
  stage = null,
  resumeRun = null,
}) {
  if (product === PRODUCT.ADP) {
    const parts = [
      "node scripts/dealality-adp-full-e2e.mjs",
      `--hotel ${hotelId}`,
      `--mode ${mode}`,
    ];
    if (authorizations.authorizePublish || authorizations.publish) {
      parts.push("--authorize-publish");
    }
    if (authorizations.authorizePaidDiscovery || authorizations.measurement) {
      parts.push("--authorize-measurement");
    }
    if (budget.budgetUsd != null) parts.push(`--budget-usd ${budget.budgetUsd}`);
    if (authorizations.authorizationRef) {
      parts.push(`--authorization-ref ${authorizations.authorizationRef}`);
    }
    if (mode === RUN_MODE.DRY_RUN || mode === RUN_MODE.VALIDATE) {
      parts.push("--dry-run");
    }
    return parts.join(" ");
  }

  const parts = [
    "node scripts/gdi-research-to-page-workflow-v1.mjs",
    `--hotel ${hotelId}`,
    "--mode full-e2e",
  ];
  if (resumeRun) parts.push(`--resume-run ${resumeRun}`);
  if (mode === RUN_MODE.DRY_RUN || mode === RUN_MODE.VALIDATE) {
    parts.push("--dry-run");
  } else {
    parts.push("--run-discovery");
  }
  if (authorizations.authorizePaidDiscovery || authorizations.paid) {
    parts.push("--authorize-paid-discovery");
  }
  if (authorizations.authorizePublish || authorizations.publish) {
    parts.push("--authorize-publish");
  }
  if (authorizations.authorizeSurfe) parts.push("--authorize-surfe");
  if (budget.budgetUsd != null) parts.push(`--budget-usd ${budget.budgetUsd}`);
  if (authorizations.authorizationRef) {
    parts.push(`--authorization-ref ${authorizations.authorizationRef}`);
  }
  if (stage?.id === "multisegment_research") {
    // paid_multisegment is inside full-e2e path
  }
  return parts.join(" ");
}

function buildPlan({
  product,
  hotelId,
  mode,
  workflow,
  overallStatus,
  nextAction,
  currentStage = null,
  blockers = [],
}) {
  return Object.freeze({
    executorVersion: NEXT_ACTION_EXECUTOR_VERSION,
    product,
    hotelId,
    mode,
    workflowId: workflow.workflowId,
    workflowVersion: workflow.workflowVersion,
    overallStatus,
    currentStageId: currentStage?.id || null,
    nextAction: Object.freeze({ ...nextAction }),
    blockers: Object.freeze([...(blockers || [])]),
    generatedAt: new Date().toISOString(),
  });
}

/**
 * Load a durable run journal if present (GDI stage ledger or OS journal).
 */
export function loadRunStateFromDisk({
  product,
  hotelId,
  runDir = null,
  repoRoot = process.cwd(),
} = {}) {
  const candidates = [];
  if (runDir) {
    candidates.push(path.join(runDir, "os-run-state.json"));
    candidates.push(path.join(runDir, "stage-completion-ledger.json"));
    candidates.push(path.join(runDir, "full_e2e", "full-e2e-completion.json"));
  }
  if (product === PRODUCT.GDI && hotelId) {
    candidates.push(
      path.join(
        repoRoot,
        "reports/gdi-adp-five-hotel-portfolio-e2e-v1/gdi-runs",
        slugHotel(hotelId),
        "stage-completion-ledger.json"
      )
    );
  }

  for (const p of candidates) {
    if (!p || !fs.existsSync(p)) continue;
    try {
      const doc = JSON.parse(fs.readFileSync(p, "utf8"));
      return normalizeRunState(doc, p);
    } catch {
      // try next
    }
  }
  return null;
}

function slugHotel(hotelId) {
  const map = {
    recbRIFhro6tLmSrO: "charlotte-marriott-city-center",
    recSM9ARs8U9UZ3AZ: "tampa-airport-marriott",
    recRkGGNKslJptFIL: "falls-church-marriott-fairview-park",
    recyZciNlXq5KulnD: "renaissance-arlington-capital-view",
    recLuxvwwxID7U2B8: "bethesda-marriott",
  };
  return map[hotelId] || hotelId;
}

function normalizeRunState(doc, sourcePath) {
  if (doc?.completedStageIds || doc?.stageResults) {
    return { ...doc, _sourcePath: sourcePath };
  }
  // GDI stage-completion-ledger shape
  if (doc?.stages || doc?.stageLedger) {
    const stages = doc.stages || doc.stageLedger || {};
    const completedStageIds = [];
    const stageResults = {};
    for (const [id, row] of Object.entries(stages)) {
      const done =
        row?.ok === true ||
        row?.status === "COMPLETE" ||
        row?.state === "COMPLETE";
      if (done) completedStageIds.push(mapCoordinatorToOsStage(id));
      stageResults[mapCoordinatorToOsStage(id)] = buildStageResult({
        stageId: mapCoordinatorToOsStage(id),
        state: done ? EXECUTION_STATE.COMPLETE : EXECUTION_STATE.PENDING,
        extras: { raw: row },
      });
    }
    return {
      runId: doc.runId || doc.workflowRunId || null,
      completedStageIds,
      stageResults,
      evaluatorStatus: doc.computedOverallStatus || doc.overallStatus || null,
      _sourcePath: sourcePath,
    };
  }
  // Completion evaluator artifact
  if (doc?.overallStatus || doc?.status) {
    return {
      runId: doc.runId || null,
      completedStageIds: [],
      stageResults: {},
      evaluatorStatus: doc.overallStatus || doc.status,
      overallStatus: doc.overallStatus || doc.status,
      _sourcePath: sourcePath,
    };
  }
  return { ...doc, _sourcePath: sourcePath };
}

function mapCoordinatorToOsStage(coordinatorId) {
  const map = {
    validate_config: "hotel_identity",
    reuse_evidence: "inventory_reuse",
    discover_gaps: "segment_applicability",
    paid_multisegment: "multisegment_research",
    opportunity_research_maturation: "opportunity_maturation",
    assess_fit: "fit_eligibility_cards",
    evaluate_eligibility: "fit_eligibility_cards",
    generate_cards: "fit_eligibility_cards",
    prepare_manifest: "research_persist_staged",
    preview_verify: "research_persist_staged",
    apply_corporate_packages: "research_persist_staged",
    apply_pe_canonical: "research_persist_staged",
    apply_publish: "controlled_publication",
    local_delivery_verify: "delivery_verification",
    production_delivery_verify: "delivery_verification",
    reconcile_run: "reconcile_lessons_evaluate",
    capture_lessons: "reconcile_lessons_evaluate",
    evaluate_completion: "reconcile_lessons_evaluate",
  };
  return map[coordinatorId] || coordinatorId;
}

export function recordZeroProgress(prevStreak, plan, previousPlan) {
  if (!previousPlan) return 0;
  if (
    plan?.nextAction?.stageId &&
    plan.nextAction.stageId === previousPlan?.nextAction?.stageId &&
    plan.nextAction.type === previousPlan?.nextAction?.type &&
    plan.nextAction.code === previousPlan?.nextAction?.code
  ) {
    return (prevStreak || 0) + 1;
  }
  return 0;
}

export { isTerminalExecutionState };
