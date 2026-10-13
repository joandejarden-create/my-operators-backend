/**
 * isGdiFutureWatchReadyForResearch — due / source-change / trigger / override.
 */

import { WATCH_STATUS } from "./constants.js";

/**
 * @param {object} watch
 * @param {{ now?: Date|string, sourceMateriallyChanged?: boolean, triggerDetected?: boolean, manualOverride?: boolean }} [opts]
 */
export function isGdiFutureWatchReadyForResearch(watch = {}, opts = {}) {
  const reasons = [];
  if (!watch || !watch.watchId) {
    return { ready: false, reasons: ["MISSING_WATCH"], due: false };
  }
  if (watch.watchStatus === WATCH_STATUS.STOPPED || watch.watchStatus === WATCH_STATUS.PROMOTED) {
    return { ready: false, reasons: ["TERMINAL_STATUS"], due: false };
  }
  if (watch.watchStatus === WATCH_STATUS.PUBLIC_DATA_CEILING) {
    // Low-frequency: only source-change or manual
    if (opts.manualOverride) {
      return { ready: true, reasons: ["MANUAL_OVERRIDE"], due: true };
    }
    if (opts.sourceMateriallyChanged) {
      return { ready: true, reasons: ["SOURCE_MATERIAL_CHANGE"], due: true };
    }
    const now = new Date(opts.now || Date.now());
    const next = watch.nextResearchDate ? new Date(watch.nextResearchDate) : null;
    if (next && now >= next) {
      return { ready: true, reasons: ["QUARTERLY_CEILING_WINDOW"], due: true };
    }
    return { ready: false, reasons: ["PUBLIC_DATA_CEILING_NOT_DUE"], due: false };
  }

  if (opts.manualOverride) {
    reasons.push("MANUAL_OVERRIDE");
    return { ready: true, reasons, due: true };
  }
  if (opts.triggerDetected) {
    reasons.push("TRIGGER_DETECTED");
    return { ready: true, reasons, due: true };
  }
  if (opts.sourceMateriallyChanged) {
    reasons.push("SOURCE_MATERIAL_CHANGE");
    return { ready: true, reasons, due: true };
  }

  const now = new Date(opts.now || Date.now());
  const next = watch.nextResearchDate ? new Date(watch.nextResearchDate) : null;
  if (next && !Number.isNaN(next.getTime()) && now >= next) {
    reasons.push("NEXT_RESEARCH_DATE_REACHED");
    return { ready: true, reasons, due: true };
  }

  return { ready: false, reasons: ["NOT_YET_DUE"], due: false };
}
