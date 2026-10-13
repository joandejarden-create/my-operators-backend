/**
 * Research Method Library registry (versioned playbooks live under playbooks/).
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { RESEARCH_METHOD_LIBRARY_VERSION, validatePlaybook } from "./playbook-schema.js";
import { RESEARCH_ESCALATION_POLICY_VERSION } from "./escalation-policy.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));

export const PLAYBOOK_READINESS = Object.freeze([
  "EXPERIMENTAL",
  "PROMISING",
  "READY_FOR_LIMITED_SCALE",
  "PRODUCTION_READY",
]);

/**
 * @param {string} playbookId
 * @param {{ triggered?: boolean, useful_evidence?: boolean, precision?: number|null, known_failures?: string[], mexico_ready?: boolean }} metrics
 */
export function classifyPlaybookReadiness(playbookId, metrics = {}) {
  // Never PRODUCTION_READY from KGPV alone
  if (!metrics.triggered) return "EXPERIMENTAL";
  if (metrics.useful_evidence && (metrics.precision == null || metrics.precision >= 0.8)) {
    return "PROMISING";
  }
  if (metrics.mexico_ready === true && metrics.precision != null && metrics.precision >= 0.9) {
    return "READY_FOR_LIMITED_SCALE";
  }
  return "EXPERIMENTAL";
}

export function loadMethodLibrarySnapshot() {
  const packs = [
    path.join(__dirname, "playbooks", "mexico-pubco-v1.json"),
    path.join(__dirname, "playbooks", "mexico-private-propco-v1.json"),
  ];
  const playbooks = [];
  const packVersions = [];
  for (const playbookPath of packs) {
    if (!fs.existsSync(playbookPath)) continue;
    const raw = JSON.parse(fs.readFileSync(playbookPath, "utf8"));
    packVersions.push(raw.version);
    for (const p of raw.playbooks || []) {
      const v = validatePlaybook(p);
      playbooks.push({
        ...p,
        schema_ok: v.ok,
        schema_errors: v.errors,
        readiness: p.readiness || "EXPERIMENTAL",
        pack_version: raw.version,
      });
    }
  }
  return {
    library_version: RESEARCH_METHOD_LIBRARY_VERSION,
    escalation_policy_version: RESEARCH_ESCALATION_POLICY_VERSION,
    playbook_pack: packVersions.join("+"),
    playbooks,
    readiness_rule: "Conservative graduation; PRODUCTION_READY requires multi-packet independent evidence. Packet 2.4D may set READY_FOR_LIMITED_SCALE only. Private PropCo pack remains CANDIDATE_TRANSFER_TEST_REQUIRED until out-of-sample (e.g. voco) passes.",
  };
}
