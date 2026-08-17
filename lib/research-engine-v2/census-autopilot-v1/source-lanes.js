/**
 * Autopilot V1 source lane registry.
 */

import { SOURCE_LANE } from "./constants.js";

export const SOURCE_LANE_REGISTRY = Object.freeze({
  version: "census-autopilot-v1-source-lane-registry",
  lanes: [
    {
      id: "A",
      name: SOURCE_LANE.A_STRUCTURED_OFFICIAL,
      cost: "lowest",
      preferred: true,
      families: [
        "IHG",
        "Hilton",
        "Choice",
        "Marriott",
        "Hyatt",
        "Accor",
        "Wyndham",
        "Minor",
      ],
      adapters_reuse: [
        "census-autopilot-ihg-cala-discovery-adapter.js",
        "census-autopilot-hilton-cala-discovery-adapter.js",
        "census-autopilot-choice-cala-discovery-adapter.js",
        "census-autopilot-marriott-discovery-adapter.js",
        "census-autopilot-accor-cala-discovery-adapter.js",
        "census-autopilot-wyndham-cala-discovery-adapter.js",
        "census-autopilot-family-directory-adapters.js",
        "clean-census/*-mexico-discovery.js",
      ],
      production_eligible_when: "source-rights allow + Exact/High identity + steward gates",
    },
    {
      id: "B",
      name: SOURCE_LANE.B_INDEPENDENT_PROPERTY,
      cost: "medium",
      when: "Lane A does not resolve the field",
      sources: [
        "official hotel website",
        "official operator",
        "official owner/developer",
        "official development announcement",
        "official tourism/public source",
        "permitted geospatial source",
        "approved industry source",
      ],
      adapters_reuse: [
        "clean-census/field-research.js",
        "production-census-geocoding-providers.js",
        "census-autopilot-level-2-source-extraction-v1.js",
      ],
      never: ["legacy_census_as_evidence", "cvent_as_evidence"],
    },
    {
      id: "C",
      name: SOURCE_LANE.C_DEEP_ESCALATION,
      cost: "highest",
      auto_call_webhound: false,
      destinations: [
        "Webhound Candidate (queued only)",
        "Human Research",
        "Specialist Registry",
        "First-Party Validation",
      ],
      when: [
        "search branches fail",
        "sources blocked",
        "ownership opaque",
        "evidence conflicts",
        "marginal effort exceeds expected value",
      ],
    },
  ],
  quarantined_coverage_only: [
    {
      id: "cvent",
      role: "COVERAGE CHALLENGE SOURCE ONLY",
      production_claims: false,
      flag: "cvent_used_as_source=false",
    },
    {
      id: "legacy_str_client",
      role: "COVERAGE CHALLENGE SOURCE ONLY",
      production_claims: false,
      flag: "legacy_used_as_source=false",
    },
  ],
});
