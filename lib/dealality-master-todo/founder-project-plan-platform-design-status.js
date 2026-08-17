/**
 * Platform Design phase — status updates aligned to shipped Dealality platform work.
 * Match rows by Phase = "Platform Design" + Step Number (excludes [Phase rollup]).
 */
import { MAP_MASTER_TODO } from "./master-todo-field-map.js";

const F = MAP_MASTER_TODO;

/** @type {Record<number, { status: string, progress: string, completedDate?: string, nextAction: string }>} */
export const PLATFORM_DESIGN_STATUS_BY_STEP = {
  /** PMO rollup — reflects ~13/37 substantive tasks completed after this sync */
  1: {
    status: "In Progress",
    progress: "35%",
    nextAction:
      "Review remaining Platform Design tasks (referral program, vendor process, formal journey map); update rollup when milestones close.",
  },
  5: {
    status: "Completed",
    progress: "100%",
    completedDate: "2026-07-03",
    nextAction: "No action required — task completed.",
  },
  6: {
    status: "Completed",
    progress: "100%",
    completedDate: "2026-07-03",
    nextAction: "No action required — task completed.",
  },
  7: {
    status: "Completed",
    progress: "100%",
    completedDate: "2026-07-03",
    nextAction: "No action required — task completed.",
  },
  8: {
    status: "In Progress",
    progress: "60%",
    nextAction:
      "Unify conditional filtering rules across Brand Explorer, Operator Strategy, and match-results views; document filter matrix.",
  },
  11: {
    status: "Completed",
    progress: "100%",
    completedDate: "2026-07-03",
    nextAction: "No action required — task completed.",
  },
  12: {
    status: "In Progress",
    progress: "50%",
    nextAction:
      "Wire deal-room NDA stages through My Deals pipeline to final decision; close gaps in brand-side workflow.",
  },
  14: {
    status: "In Progress",
    progress: "50%",
    nextAction:
      "Finalize brand data-quality SOP from existing audit scripts; assign review owner and cadence.",
  },
  15: {
    status: "Completed",
    progress: "100%",
    completedDate: "2026-07-03",
    nextAction: "No action required — task completed.",
  },
  18: {
    status: "In Progress",
    progress: "40%",
    nextAction:
      "Extract reusable deal-progress component from My Deals pipeline; apply to deal-room and intake flows.",
  },
  19: {
    status: "Completed",
    progress: "100%",
    completedDate: "2026-07-03",
    nextAction: "No action required — task completed.",
  },
  20: {
    status: "Completed",
    progress: "100%",
    completedDate: "2026-07-03",
    nextAction: "No action required — task completed.",
  },
  22: {
    status: "Completed",
    progress: "100%",
    completedDate: "2026-07-03",
    nextAction: "No action required — task completed.",
  },
  34: {
    status: "Completed",
    progress: "100%",
    completedDate: "2026-07-03",
    nextAction: "No action required — task completed.",
  },
  35: {
    status: "Completed",
    progress: "100%",
    completedDate: "2026-07-03",
    nextAction: "No action required — task completed.",
  },
  36: {
    status: "In Progress",
    progress: "30%",
    nextAction:
      "Document third-party integration plan (Webflow, Memberstack, Airtable, outreach tools); prioritize pilot-critical connectors.",
  },
  37: {
    status: "In Progress",
    progress: "40%",
    nextAction:
      "Consolidate explorer/alignment docs into a single functional spec index; link each user journey to live pages.",
  },
};

export function isPlatformDesignTaskRow(fields) {
  const task = String(fields?.[F.task] || "");
  const phase = fields?.[F.phase];
  if (phase !== "Platform Design") return false;
  if (task.includes("[Phase rollup]")) return false;
  return true;
}

export function buildPlatformDesignStatusPatch(fields) {
  const step = fields?.[F.stepNumber];
  if (step == null || step === "") return null;
  const target = PLATFORM_DESIGN_STATUS_BY_STEP[Number(step)];
  if (!target) return null;

  const patch = {};
  const existing = fields || {};

  if (existing[F.status] !== target.status) patch[F.status] = target.status;
  if (existing[F.progress] !== target.progress) patch[F.progress] = target.progress;
  if (existing[F.nextAction] !== target.nextAction) patch[F.nextAction] = target.nextAction;

  if (target.status === "Completed" && target.completedDate) {
    if (!existing[F.completedDate]) patch[F.completedDate] = target.completedDate;
  }

  if (Object.keys(patch).length === 0) return null;
  return { patch, target, step: Number(step) };
}
