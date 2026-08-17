/**
 * Robust shadow monitoring state for recurring ops (dedupe only — NOT SoT).
 */

import { createHash } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname } from "node:path";

export function claimFingerprint(item) {
  const parts = [
    item.hotel_id || item.hotelId || item.entity_id || "",
    item.field || item.claim_type || "",
    String(item.current_value ?? item.currentDealalityValue ?? ""),
    String(item.observed_value ?? item.observedValue ?? ""),
    item.recommended_action || "",
    item.evidence?.[0]?.url || item.evidenceUrl || item.last_evidence_url || "",
  ];
  return createHash("sha256").update(parts.join("|").toLowerCase()).digest("hex").slice(0, 24);
}

export function loadShadowState(statePath) {
  if (!existsSync(statePath)) {
    return {
      version: "shadow-state-v2",
      updatedAt: null,
      claims: {},
    };
  }
  return JSON.parse(readFileSync(statePath, "utf8"));
}

export function saveShadowState(statePath, state) {
  mkdirSync(dirname(statePath), { recursive: true });
  state.updatedAt = new Date().toISOString();
  state.version = state.version || "shadow-state-v2";
  writeFileSync(statePath, JSON.stringify(state, null, 2), "utf8");
}

function confidenceRank(c) {
  const m = { high: 3, medium: 2, low: 1 };
  return m[String(c || "").toLowerCase()] || 0;
}

/**
 * Surface alerts only when material change, evidence change, confidence up,
 * steward recheck, or reminder threshold reached.
 *
 * @param {object} state
 * @param {object[]} items
 * @param {{ suppressDays?: number, reminderDays?: number }} [opts]
 */
export function applyAlertDedup(state, items, opts = {}) {
  const suppressDays = opts.suppressDays ?? 30;
  const reminderDays = opts.reminderDays ?? 90;
  const now = Date.now();
  const surface = [];
  const suppressed = [];

  for (const item of items) {
    const fp = item.fingerprint || claimFingerprint(item);
    const evidenceDate = item.evidence?.[0]?.sourceDate || item.evidenceDate || item.evidence_date || null;
    const evidenceUrl = item.evidence?.[0]?.url || item.evidenceUrl || item.last_evidence_url || "";
    const observedValue = String(item.observed_value ?? item.observedValue ?? "");
    const currentValue = String(item.current_value ?? item.currentDealalityValue ?? "");
    const confidence = item.confidence || item.confidenceBand || item.current_confidence || null;
    const stewardRecheck = item.steward_status === "Needs More Research" || item.forceRecheck === true;

    const prev = state.claims[fp];

    if (prev) {
      const last = new Date(prev.last_detected || prev.lastDetected || prev.first_detected || prev.firstDetected).getTime();
      const ageDays = (now - last) / (1000 * 60 * 60 * 24);
      const observedChanged = String(prev.observed_value ?? prev.observedValue ?? "") !== observedValue;
      const evidenceChanged =
        String(prev.last_evidence_url || prev.evidenceUrl || "") !== String(evidenceUrl) ||
        String(prev.last_evidence_date || prev.evidenceDate || "") !== String(evidenceDate || "");
      const confidenceUp =
        confidenceRank(confidence) > confidenceRank(prev.current_confidence || prev.confidence);
      const reminderDue = ageDays >= reminderDays && prev.previously_surfaced !== false;

      prev.last_detected = new Date().toISOString();
      prev.detection_count = (prev.detection_count || prev.detectionCount || 1) + 1;
      prev.last_evidence_url = evidenceUrl;
      prev.last_evidence_date = evidenceDate;
      prev.current_confidence = confidence;
      prev.steward_status = item.steward_status || prev.steward_status || "open";

      const shouldSurface =
        observedChanged || evidenceChanged || confidenceUp || stewardRecheck || reminderDue;

      if (!shouldSurface && ageDays < suppressDays && (prev.previously_surfaced || prev.previouslySurfaced)) {
        suppressed.push({
          ...item,
          fingerprint: fp,
          suppressReason: "duplicate_within_window",
        });
        continue;
      }
    } else {
      state.claims[fp] = {
        fingerprint: fp,
        entity_id: item.hotel_id || item.hotelId || item.entity_id || null,
        claim_type: item.field || item.claim_type || null,
        current_dealality_value: currentValue,
        observed_value: observedValue,
        first_detected: new Date().toISOString(),
        last_detected: new Date().toISOString(),
        last_evidence_url: evidenceUrl,
        last_evidence_date: evidenceDate,
        detection_count: 1,
        current_confidence: confidence,
        steward_status: item.steward_status || "New",
        previously_surfaced: false,
        resolved_at: null,
      };
    }

    state.claims[fp].previously_surfaced = true;
    state.claims[fp].previouslySurfaced = true;
    surface.push({
      ...item,
      fingerprint: fp,
      first_detected: state.claims[fp].first_detected || state.claims[fp].firstDetected,
    });
  }

  return { surface, suppressed };
}

// Re-export aliases for older callers
export { applyAlertDedup as applyOpsAlertDedup };
