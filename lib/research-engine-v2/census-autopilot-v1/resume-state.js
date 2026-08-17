/**
 * Autopilot V1 resume state — extends checkpoint conventions.
 */

import fs from "node:fs";
import path from "node:path";
import { AUTOPILOT_V1_VERSION } from "./constants.js";

/**
 * @param {object} partial
 */
export function buildResumeState(partial = {}) {
  const runId = partial.run_id || partial.runId;
  return {
    version: "census-autopilot-v1-resume-state",
    autopilot_version: AUTOPILOT_V1_VERSION,
    updated_at: new Date().toISOString(),
    run_id: runId,
    mode: partial.mode || null,
    group: partial.group || null,
    brand: partial.brand || null,
    country: partial.country || null,
    region: partial.region || null,
    priority: partial.priority || null,
    max_records: partial.max_records ?? null,
    dry_run: partial.dry_run !== false,
    progress: {
      total_in_scope: partial.total_in_scope ?? 0,
      completed: partial.completed ?? 0,
      failed: partial.failed ?? 0,
      remaining: partial.remaining ?? 0,
      pct: partial.total_in_scope
        ? Math.round((100 * (partial.completed || 0)) / partial.total_in_scope)
        : 0,
    },
    completed_entity_ids: partial.completed_entity_ids || [],
    failed_entities: partial.failed_entities || [],
    retry_state: partial.retry_state || {},
    source_failures: partial.source_failures || [],
    research_checkpoints: partial.research_checkpoints || [],
    observability_snapshot: partial.observability_snapshot || null,
    resume_command: runId
      ? `npm run census:autopilot-v1 -- --resume ${runId}`
      : null,
    constraints: {
      no_webhound: true,
      no_airtable_writes: true,
      no_credit_spend: true,
    },
  };
}

export function saveResumeState(runDir, state) {
  fs.mkdirSync(runDir, { recursive: true });
  const payload = buildResumeState(state);
  fs.writeFileSync(path.join(runDir, "resume-state.json"), JSON.stringify(payload, null, 2), "utf8");
  return payload;
}

export function loadResumeState(runDir) {
  const fp = path.join(runDir, "resume-state.json");
  if (!fs.existsSync(fp)) return null;
  return JSON.parse(fs.readFileSync(fp, "utf8"));
}

export function resolveAutopilotV1RunDir(artifactRoot, runId) {
  const direct = path.join(artifactRoot, "runs", runId);
  if (fs.existsSync(direct) || fs.existsSync(path.join(direct, "resume-state.json"))) {
    return direct;
  }
  const runsRoot = path.join(artifactRoot, "runs");
  if (!fs.existsSync(runsRoot)) return direct;
  const hit = fs.readdirSync(runsRoot).find((e) => e === runId || e.includes(runId));
  return hit ? path.join(runsRoot, hit) : direct;
}
