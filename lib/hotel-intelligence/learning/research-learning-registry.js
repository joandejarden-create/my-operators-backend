/**
 * Packet 2.8 — ResearchLearningRegistry (immutable candidates; no auto-promote).
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

export const LEARNING_REGISTRY_VERSION = "research-learning-registry-v1";

export const LEARNING_STATUSES = Object.freeze([
  "CANDIDATE",
  "VALIDATED",
  "PROMOTED_TO_PLAYBOOK",
  "REJECTED",
]);

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const DEFAULT_REGISTRY_PATH = path.resolve(
  __dirname,
  "../../../reports/hotel-intelligence/packet-2.8/RESEARCH_LEARNING_REGISTRY.json"
);

/**
 * Promotion rule — learnings never auto-mutate production prompts.
 */
export function canPromoteLearning(record = {}) {
  return (
    record.status === "VALIDATED" &&
    Boolean(record.case_evidence) &&
    Boolean(record.generic_enough) &&
    Boolean(record.negative_cases_understood) &&
    Boolean(record.eval_id) &&
    Boolean(record.regression_suite_passed)
  );
}

export function createLearningRecord(partial = {}) {
  const status = LEARNING_STATUSES.includes(partial.status) ? partial.status : "CANDIDATE";
  return {
    learning_id: partial.learning_id || null,
    run_id: partial.run_id || null,
    hotel_id: partial.hotel_id || null,
    hotel_archetype: partial.hotel_archetype || null,
    jurisdiction: partial.jurisdiction || null,
    template_id: partial.template_id || null,
    template_version: partial.template_version || null,
    problem: partial.problem || null,
    successful_method: partial.successful_method || null,
    failed_methods: partial.failed_methods || [],
    source_hierarchy: partial.source_hierarchy || [],
    query_patterns: partial.query_patterns || [],
    document_patterns: partial.document_patterns || [],
    entity_patterns: partial.entity_patterns || [],
    temporal_patterns: partial.temporal_patterns || [],
    negative_screens: partial.negative_screens || [],
    validation_rules: partial.validation_rules || [],
    stop_condition: partial.stop_condition || null,
    native_reproduction: partial.native_reproduction || "UNKNOWN", // YES | PARTIAL | NO | UNKNOWN
    native_reproduction_method: partial.native_reproduction_method || null,
    recommended_playbook_change: partial.recommended_playbook_change || null,
    new_eval: partial.new_eval || null,
    status,
    auto_promote_forbidden: true,
    created_at: partial.created_at || new Date().toISOString(),
    ...partial,
    status,
  };
}

export function loadLearningRegistry(filePath = DEFAULT_REGISTRY_PATH) {
  if (!fs.existsSync(filePath)) {
    return {
      registry_version: LEARNING_REGISTRY_VERSION,
      auto_promote: false,
      records: [],
    };
  }
  return JSON.parse(fs.readFileSync(filePath, "utf8"));
}

export function saveLearningRegistry(registry, filePath = DEFAULT_REGISTRY_PATH) {
  const payload = {
    registry_version: LEARNING_REGISTRY_VERSION,
    auto_promote: false,
    updated_at: new Date().toISOString(),
    records: registry.records || [],
    note: "Learnings are CANDIDATE by default. Promotion requires eval + regression. Never auto-mutate production prompts.",
  };
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  fs.writeFileSync(filePath, JSON.stringify(payload, null, 2) + "\n");
  return payload;
}
