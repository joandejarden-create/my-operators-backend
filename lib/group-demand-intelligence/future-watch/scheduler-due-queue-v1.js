/**
 * Enhanced due-queue for scheduled execution — priority + PDC exclusion.
 */

import {
  WATCH_STATUS,
  DATE_PROVENANCE,
  DUE_PRIORITY,
} from "./constants.js";
import { isGdiFutureWatchReadyForResearch } from "./watch-readiness-v1.js";

const TERMINAL = new Set([
  WATCH_STATUS.STOPPED,
  WATCH_STATUS.PROMOTED,
  WATCH_STATUS.CLOSED,
  WATCH_STATUS.REJECTED,
]);

/**
 * Build prioritized due queue for scheduler.
 * PUBLIC_DATA_CEILING excluded from normal queue unless low-freq / source-change / override.
 */
export function queryScheduledDueWatches(watches = [], opts = {}) {
  const now = opts.now || new Date();
  const due = [];

  for (const w of watches) {
    if (!w || TERMINAL.has(w.watchStatus)) continue;
    if (w.watchStatus === WATCH_STATUS.RESEARCH_BLOCKED_PROVIDER) continue;

    const isCeiling = w.watchStatus === WATCH_STATUS.PUBLIC_DATA_CEILING;
    const sourceChanged = opts.sourceChanges?.[w.watchId] === true;
    const triggerDetected = opts.triggersDetected?.[w.watchId] === true;
    const manualOverride = opts.manualOverrides?.[w.watchId] === true;
    const forceDue = opts.forceDueIds?.has?.(w.watchId) || opts.forceDueIds?.has?.(w.candidateId);

    if (isCeiling && !sourceChanged && !manualOverride && !forceDue) {
      // Only low-frequency quarterly window for ceiling
      const check = isGdiFutureWatchReadyForResearch(w, { now });
      if (!check.ready || !check.reasons.includes("QUARTERLY_CEILING_WINDOW")) {
        continue;
      }
    }

    if (forceDue) {
      due.push({
        watch: w,
        readiness: { ready: true, reasons: ["FORCE_DUE_DEV"], due: true },
        duePriority: DUE_PRIORITY.MANUAL_OVERRIDE,
        dueReason: "FORCE_DUE_DEV",
      });
      continue;
    }

    const check = isGdiFutureWatchReadyForResearch(w, {
      now,
      sourceMateriallyChanged: sourceChanged,
      triggerDetected,
      manualOverride,
    });
    if (!check.ready) continue;

    let duePriority = DUE_PRIORITY.HEURISTIC;
    let dueReason = check.reasons[0] || "DUE";
    if (manualOverride) {
      duePriority = DUE_PRIORITY.MANUAL_OVERRIDE;
      dueReason = "MANUAL_OVERRIDE";
    } else if (triggerDetected || check.reasons.includes("TRIGGER_DETECTED")) {
      duePriority = DUE_PRIORITY.EXPLICIT_TRIGGER;
      dueReason = "EXPLICIT_TRIGGER";
    } else if (
      sourceChanged ||
      w.dateProvenance === DATE_PROVENANCE.EVIDENCE_BASED
    ) {
      duePriority =
        sourceChanged || check.reasons.includes("SOURCE_MATERIAL_CHANGE")
          ? DUE_PRIORITY.EXPLICIT_TRIGGER
          : DUE_PRIORITY.EVIDENCE_BASED;
      dueReason = sourceChanged ? "SOURCE_MATERIAL_CHANGE" : "EVIDENCE_BASED_DUE";
    } else if (w.dateProvenance === DATE_PROVENANCE.HEURISTIC) {
      duePriority = DUE_PRIORITY.HEURISTIC;
      dueReason = "HEURISTIC_DUE";
    }

    due.push({ watch: w, readiness: check, duePriority, dueReason });
  }

  due.sort((a, b) => {
    if (a.duePriority !== b.duePriority) return a.duePriority - b.duePriority;
    const da = String(a.watch.nextResearchDate || "");
    const db = String(b.watch.nextResearchDate || "");
    if (da !== db) return da < db ? -1 : 1;
    return String(a.watch.watchId).localeCompare(String(b.watch.watchId));
  });

  return due;
}
