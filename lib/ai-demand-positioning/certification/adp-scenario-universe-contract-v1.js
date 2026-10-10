/**
 * Global ADP scenario universe contract + versioning (Parts 7–9, 29).
 * Comparison requires exact or governed-equivalent scenario IDs — never count alone.
 */

import crypto from "crypto";
import { buildScenarioUniverse } from "../prompt-universe/scenario-registry.js";

export const ADP_SCENARIO_UNIVERSE_CONTRACT_VERSION = "adp_scenario_universe_contract_v1";

export const CAPABILITY_EXCLUSION_REASONS = Object.freeze({
  TRUE_CAPABILITY_EXCLUSION: "TRUE_CAPABILITY_EXCLUSION",
  DATA_GAP_EXCLUSION: "DATA_GAP_EXCLUSION",
  MODEL_RULE_EXCLUSION: "MODEL_RULE_EXCLUSION",
  UNKNOWN: "UNKNOWN",
});

function sha16(value) {
  return crypto.createHash("sha256").update(String(value || "")).digest("hex").slice(0, 16);
}

export function buildScenarioUniverseManifest(propertyProfile, options = {}) {
  const scenarios = options.scenarios || buildScenarioUniverse(propertyProfile);
  const scenarioIds = scenarios.map((s) => s.scenarioId).filter(Boolean);
  const sortedIds = [...scenarioIds].sort();
  const territoryAssignments = {};
  for (const s of scenarios) {
    const intent = s.intent || s.travelerIntent || s.territory || "UNKNOWN";
    territoryAssignments[s.scenarioId] = intent;
  }

  const capabilityExclusions = (options.capabilityExclusions || []).map((row) => ({
    scenarioId: row.scenarioId || null,
    capability: row.capability || null,
    reasonClass: CAPABILITY_EXCLUSION_REASONS[row.reasonClass] || CAPABILITY_EXCLUSION_REASONS.UNKNOWN,
    reason: row.reason || null,
  }));

  const scenarioUniverseId =
    options.scenarioUniverseId ||
    propertyProfile?.scenarioUniverseId ||
    `su_${sha16(sortedIds.join("|"))}`;

  return {
    scenarioUniverseContractVersion: ADP_SCENARIO_UNIVERSE_CONTRACT_VERSION,
    scenarioUniverseId,
    scenarioUniverseVersion:
      options.scenarioUniverseVersion ||
      propertyProfile?.scenarioUniverseVersion ||
      scenarioUniverseId,
    scenarioIds: sortedIds,
    scenarioCount: sortedIds.length,
    territoryAssignments,
    promptTemplateVersion:
      options.promptTemplateVersion || propertyProfile?.promptTemplateVersion || null,
    capabilityEligibility: options.capabilityEligibility || null,
    rankEligibility: options.rankEligibility || null,
    providerEligibility: options.providerEligibility || null,
    capabilityExclusions,
    scenarioBuilderVersion:
      options.scenarioBuilderVersion ||
      propertyProfile?.scenarioBuilderVersion ||
      "scenario-registry/buildScenarioUniverse",
    scenarioIdsHash: sha16(sortedIds.join("|")),
  };
}

/**
 * Diff two scenario universes for change control (Part 8 / 29).
 */
export function diffScenarioUniverses(priorManifest, nextManifest, meta = {}) {
  const priorIds = new Set(priorManifest?.scenarioIds || []);
  const nextIds = new Set(nextManifest?.scenarioIds || []);
  const added = [...nextIds].filter((id) => !priorIds.has(id)).sort();
  const removed = [...priorIds].filter((id) => !nextIds.has(id)).sort();

  const territoryChanges = [];
  for (const id of [...priorIds].filter((x) => nextIds.has(x))) {
    const a = priorManifest?.territoryAssignments?.[id];
    const b = nextManifest?.territoryAssignments?.[id];
    if (a && b && a !== b) {
      territoryChanges.push({ scenarioId: id, from: a, to: b });
    }
  }

  return {
    priorUniverseId: priorManifest?.scenarioUniverseId || null,
    nextUniverseId: nextManifest?.scenarioUniverseId || null,
    priorVersion: priorManifest?.scenarioUniverseVersion || null,
    nextVersion: nextManifest?.scenarioUniverseVersion || null,
    addedScenarioIds: added,
    removedScenarioIds: removed,
    territoryChanges,
    eligibilityLogicChanged: Boolean(meta.eligibilityLogicChanged),
    reason: meta.reason || null,
    codeVersion: meta.codeVersion || null,
    affectedHotels: meta.affectedHotels || [],
    silentDrift: added.length === 0 && removed.length === 0 && territoryChanges.length === 0
      ? false
      : !meta.reason,
  };
}

/**
 * Extract scenario IDs from a persisted period (immutable snapshot preferred).
 */
export function scenarioIdsFromPeriod(period) {
  if (Array.isArray(period?.scenarioUniverse?.scenarioIds)) {
    return [...period.scenarioUniverse.scenarioIds].sort();
  }
  if (Array.isArray(period?.scenarioIds)) {
    return [...period.scenarioIds].sort();
  }
  const fromObs = [
    ...new Set((period?.observations || []).map((o) => o.scenarioId).filter(Boolean)),
  ].sort();
  return fromObs;
}

export function persistableScenarioUniverseOnPeriod(period, propertyProfile) {
  if (period?.scenarioUniverse?.scenarioIds?.length) return period.scenarioUniverse;
  return buildScenarioUniverseManifest(propertyProfile, {
    scenarioUniverseId: period?.scenarioUniverseId || null,
    scenarioUniverseVersion: period?.scenarioUniverseVersion || null,
  });
}
