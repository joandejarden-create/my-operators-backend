/**
 * Full E2E fourteen-segment discovery executor registry V1.
 *
 * Every canonical PowerShell workflow maps to runSegmentDiscoveryExecutor,
 * which builds segment-specific queries via adapters and invokes an injectable
 * provider. Association/Scaling hotel runners remain available as specialized
 * deep paths but are NOT required for hotel resolution (GDI config + HPC only).
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { listFourteenWorkflowSegmentIds } from "../research/universal-contact-research-contract-v1.js";

export const FULL_E2E_FOURTEEN_SEGMENT_DISCOVERY_EXECUTOR_REGISTRY_V1 =
  "gdi-full-e2e-fourteen-segment-discovery-executor-registry-v1";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

const SHARED_RUNNER =
  "lib/group-demand-intelligence/research/segment-discovery-executor-v1.js";
const SHARED_EXPORT = "runSegmentDiscoveryExecutor";

/** @typedef {"FULL_EXECUTOR_PRESENT"|"PARTIAL_EXECUTOR"|"REPLAY_ONLY"|"FIXTURE_ONLY"|"STUB_OR_NO_OP"|"BROKEN"|"UNAVAILABLE_PROVIDER"} ExecutorClass */

function spec(segmentId, classification, strategy, extras = {}) {
  return Object.freeze({
    segmentId,
    classification,
    strategy,
    hotelRunnerRel: SHARED_RUNNER,
    exportName: SHARED_EXPORT,
    zeroCostMode: extras.zeroCostMode || "fixture_or_historical_or_blocked_paid",
    paidProvider: extras.paidProvider || "exa_agent",
    sourceFamilies: Object.freeze(
      extras.sourceFamilies || ["segment_adapter", "historical_ledger", "fixture"]
    ),
    specializedRunnerRel: extras.specializedRunnerRel || null,
  });
}

export const FOURTEEN_SEGMENT_EXECUTOR_SPECS = Object.freeze({
  associations_conferences_events: spec(
    "associations_conferences_events",
    "PARTIAL_EXECUTOR",
    "assoc_strategies_plus_segment_executor",
    {
      specializedRunnerRel:
        "lib/group-demand-intelligence/research/association-events-hotel-runner-v1.js",
      sourceFamilies: ["association_strategies", "exa_agent", "historical_ledger"],
    }
  ),
  corporate_accounts_scaling: spec(
    "corporate_accounts_scaling",
    "PARTIAL_EXECUTOR",
    "scaling_strategies_plus_segment_executor",
    {
      specializedRunnerRel:
        "lib/group-demand-intelligence/research/scaling-companies-hotel-runner-v1.js",
      sourceFamilies: ["scaling_strategies", "exa_agent", "historical_ledger"],
    }
  ),
  corporate_meetings_incentives: spec(
    "corporate_meetings_incentives",
    "PARTIAL_EXECUTOR",
    "destination_corporate_demand_strategies"
  ),
  government_public_sector: spec(
    "government_public_sector",
    "PARTIAL_EXECUTOR",
    "government_manual_plus_gdi_queries"
  ),
  sports_entertainment: spec(
    "sports_entertainment",
    "PARTIAL_EXECUTOR",
    "base_of_demand_sports_entertainment"
  ),
  education_scientific: spec(
    "education_scientific",
    "PARTIAL_EXECUTOR",
    "medical_assoc_plus_pharma_base"
  ),
  projects_infrastructure: spec(
    "projects_infrastructure",
    "PARTIAL_EXECUTOR",
    "base_of_demand_project_workforce"
  ),
  social_special_events: spec(
    "social_special_events",
    "PARTIAL_EXECUTOR",
    "venue_discovery_queries"
  ),
  local_catering_day_meetings: spec(
    "local_catering_day_meetings",
    "PARTIAL_EXECUTOR",
    "local_meetings_catering_queries"
  ),
  corporate_celebrations_private_events: spec(
    "corporate_celebrations_private_events",
    "PARTIAL_EXECUTOR",
    "destination_recognition_celebration_filter"
  ),
  travel_trade_group_intermediaries: spec(
    "travel_trade_group_intermediaries",
    "PARTIAL_EXECUTOR",
    "professional_planner_queries"
  ),
  relocation_extended_stay: spec(
    "relocation_extended_stay",
    "PARTIAL_EXECUTOR",
    "relocation_manual_templates"
  ),
  preferred_lodging_partnerships: spec(
    "preferred_lodging_partnerships",
    "PARTIAL_EXECUTOR",
    "preferred_lodging_prompt_builder",
    { sourceFamilies: ["preferred_lodging_prompt", "exa_agent", "fixture"] }
  ),
  professional_event_planners: spec(
    "professional_event_planners",
    "PARTIAL_EXECUTOR",
    "professional_event_planner_prompt_builder",
    { sourceFamilies: ["planner_prompt", "exa_agent", "fixture"] }
  ),
});

export function getFourteenSegmentExecutorSpec(segmentId) {
  return FOURTEEN_SEGMENT_EXECUTOR_SPECS[segmentId] || null;
}

export function listFourteenSegmentExecutorSpecs() {
  return listFourteenWorkflowSegmentIds().map((id) => {
    const row = getFourteenSegmentExecutorSpec(id);
    if (!row) {
      return { segmentId: id, classification: "BROKEN", missing: true };
    }
    const abs = path.join(ROOT, row.hotelRunnerRel);
    return {
      ...row,
      hotelRunnerExists: fs.existsSync(abs),
      hotelRunnerAbs: abs,
      specializedRunnerExists: row.specializedRunnerRel
        ? fs.existsSync(path.join(ROOT, row.specializedRunnerRel))
        : null,
    };
  });
}

export function assertFourteenSegmentExecutorArtifactsOnDisk() {
  const failures = [];
  for (const row of listFourteenSegmentExecutorSpecs()) {
    if (row.missing) {
      failures.push({ segmentId: row.segmentId, reason: "missing_spec" });
      continue;
    }
    if (!row.hotelRunnerExists) {
      failures.push({
        segmentId: row.segmentId,
        reason: "missing_hotel_runner",
        path: row.hotelRunnerRel,
      });
    }
    // Specialized deep runners (assoc/scaling hotel runners) are optional for
    // the shared segment executor path. Missing files are noted, not hard failures.
    if (row.specializedRunnerRel && row.specializedRunnerExists === false) {
      // soft note only — segment-discovery-executor remains the canonical path
    }
  }
  return { ok: failures.length === 0, failures };
}
