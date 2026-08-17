/**
 * Internal pilot analytics events — local JSONL, no external telemetry.
 */

import { appendFileSync, mkdirSync, existsSync, readFileSync } from "fs";
import { join, dirname } from "path";
import { fileURLToPath } from "url";
import { OPERATOR_FIT_ENGINE_VERSION } from "./feature-flag.js";

const ROOT = join(dirname(fileURLToPath(import.meta.url)), "..", "..");
const DEFAULT_PATH = join(ROOT, "data", "operator-fit", "internal-pilot-events.jsonl");

export const PILOT_EVENT_TYPES = Object.freeze([
  "results_viewed",
  "operator_detail_expanded",
  "evidence_viewed",
  "comparable_viewed",
  "operator_shortlisted",
  "operator_removed",
  "comparison_opened",
  "validation_question_viewed",
  "research_stage_candidate_viewed",
]);

export function getPilotEventsPath() {
  return process.env.OPERATOR_FIT_PILOT_EVENTS_PATH || DEFAULT_PATH;
}

/**
 * @param {{ event: string, dealId?: string, operatorId?: string, userEmailHash?: string, meta?: object }} payload
 */
export function recordPilotEvent(payload = {}) {
  const event = String(payload.event || "").trim();
  if (!PILOT_EVENT_TYPES.includes(event)) {
    console.error("[operator-fit-pilot-events] unknown event", event);
    return { ok: false, error: "unknown_event" };
  }
  const row = {
    event,
    dealId: payload.dealId || null,
    operatorId: payload.operatorId || null,
    /** Prefer hashed / role label — avoid storing raw PII when possible */
    internalUser: payload.internalUser || payload.userRole || "internal_admin",
    engineVersion: payload.engineVersion || OPERATOR_FIT_ENGINE_VERSION,
    timestamp: new Date().toISOString(),
    meta: payload.meta || undefined,
  };
  const path = getPilotEventsPath();
  mkdirSync(dirname(path), { recursive: true });
  appendFileSync(path, `${JSON.stringify(row)}\n`);
  return { ok: true, row };
}

export function readPilotEvents({ limit = 500 } = {}) {
  const path = getPilotEventsPath();
  if (!existsSync(path)) return [];
  const lines = readFileSync(path, "utf8").split(/\n/).filter(Boolean);
  return lines
    .slice(-Math.max(1, limit))
    .map((line) => {
      try {
        return JSON.parse(line);
      } catch {
        return null;
      }
    })
    .filter(Boolean);
}
