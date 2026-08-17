/**
 * Validation for FPP agent writes — Joan must approve Completed.
 */
import { MAP_MASTER_TODO } from "./master-todo-field-map.js";
import {
  FPP_AGENT_ALLOWED_STATUSES,
  FPP_COMPLETION_STATUS,
  isHumanOnlyTask,
  resolveTaskPlaybook,
} from "./fpp-agent-workflow-config.js";

const F = MAP_MASTER_TODO;

/**
 * @param {object} params
 * @param {object} params.existingFields
 * @param {object} params.patch - status, progress, nextAction, completedDate, blocker
 * @param {string} [params.worker]
 * @param {string} [params.approvedBy] - required when status = Completed
 */
export function validateAgentTaskWrite({ existingFields, patch, worker, approvedBy }) {
  const failures = [];
  const warnings = [];

  const playbook = resolveTaskPlaybook(existingFields || {});
  if (playbook.agentEligible === false && patch[F.status] === FPP_COMPLETION_STATUS) {
    failures.push("Task is human-only; agents cannot mark Completed.");
  }
  if (isHumanOnlyTask(existingFields) && patch[F.status] === FPP_COMPLETION_STATUS && !approvedBy) {
    failures.push("Human-only task requires Joan approval to complete.");
  }

  if (patch[F.status] !== undefined) {
    const next = String(patch[F.status]);
    if (next === FPP_COMPLETION_STATUS) {
      if (!approvedBy || !String(approvedBy).trim()) {
        failures.push(
          `Status "${FPP_COMPLETION_STATUS}" requires --approved-by "Joan D." (founder sign-off).`
        );
      }
    } else if (!FPP_AGENT_ALLOWED_STATUSES.has(next)) {
      failures.push(`Agents cannot set Status to "${next}".`);
    }
  }

  if (patch[F.progress] !== undefined) {
    const p = String(patch[F.progress]);
    if (!/^\d{1,3}%$/.test(p)) {
      failures.push(`Progress must be a percent string like "75%" (got "${p}").`);
    }
  }

  if (patch[F.completedDate] && patch[F.status] !== FPP_COMPLETION_STATUS) {
    warnings.push("Completed Date set without Completed status — will only apply if status is Completed.");
  }

  if (worker && playbook.worker !== "human-only" && worker !== playbook.worker) {
    warnings.push(
      `Worker "${worker}" differs from recommended "${playbook.worker}" for this playbook.`
    );
  }

  const pass = failures.length === 0;
  return {
    pass,
    failures,
    warnings,
    playbookId: playbook.playbookId,
    recommendedWorker: playbook.worker,
  };
}

/**
 * Build sanitized Airtable patch (only allowed fields).
 * @param {object} input
 */
export function buildAgentTaskPatch(input) {
  const patch = {};
  if (input.status !== undefined) patch[F.status] = input.status;
  if (input.progress !== undefined) patch[F.progress] = input.progress;
  if (input.nextAction !== undefined) patch[F.nextAction] = input.nextAction;
  if (input.blocker !== undefined) patch[F.blocker] = input.blocker;
  if (input.completedDate !== undefined) patch[F.completedDate] = input.completedDate;
  return patch;
}

export function formatValidationResult(validation) {
  const lines = [];
  lines.push(validation.pass ? "VALIDATION: PASS" : "VALIDATION: FAIL");
  if (validation.failures.length) {
    lines.push("Failures:");
    validation.failures.forEach((f) => lines.push(`  - ${f}`));
  }
  if (validation.warnings.length) {
    lines.push("Warnings:");
    validation.warnings.forEach((w) => lines.push(`  - ${w}`));
  }
  return lines.join("\n");
}
