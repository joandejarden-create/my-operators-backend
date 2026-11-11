/**
 * Segment-aware Exa agent-family routing V1.
 *
 * Restores original GDI routing: two Exa families (Scaling Companies,
 * Association/Events) + fourteen segment query adapters.
 * Does NOT invent fourteen new agents.
 *
 * Paid calls remain gated by the provider adapter / Full E2E ledger.
 */

import { listFourteenWorkflowSegmentIds } from "../universal-contact-research-contract-v1.js";
import {
  EXA_AGENT_DISCOVERY_CONTRACT_ACTIVE,
  EXA_AGENT_DEEP_RESEARCH_CONTRACT_V1,
  EXA_AGENT_DISCOVERY_CONTRACT_V2,
} from "./exa-agent-contracts-v1.js";
import {
  EXA_EVENTS_DISCOVERY_CONTRACT_V1,
  EXA_EVENTS_DEEP_RESEARCH_CONTRACT_V1,
} from "./exa-agent-events-contracts-v1.js";
import { EXA_AGENT_RESEARCH_STRATEGIES } from "./exa-agent-ors-strategy-v1.js";
import { EXA_EVENTS_RESEARCH_STRATEGIES } from "./exa-agent-events-ors-strategy-v1.js";
import {
  EXA_PREFERRED_LODGING_DISCOVERY_V1,
  EXA_PREFERRED_LODGING_DEEP_V1,
} from "../../preferred-lodging-partnership/exa-preferred-lodging-contracts-v1.js";

export const GDI_SEGMENT_AWARE_EXA_AGENT_ROUTING_V1 =
  "gdi-segment-aware-exa-agent-routing-v1";

export const EXA_AGENT_FAMILY = Object.freeze({
  SCALING_COMPANIES: "SCALING_COMPANIES",
  ASSOCIATION_EVENTS: "ASSOCIATION_EVENTS",
  PREFERRED_LODGING: "PREFERRED_LODGING",
  NONE: "NONE",
});

/** Strategy IDs for Preferred Lodging — dedicated contracts, not Scaling/Events. */
export const EXA_PREFERRED_LODGING_RESEARCH_STRATEGIES = Object.freeze({
  EXA_AGENT_PREFERRED_LODGING_DISCOVERY: "EXA_AGENT_PREFERRED_LODGING_DISCOVERY",
  EXA_AGENT_PREFERRED_LODGING_DEEP_RESEARCH:
    "EXA_AGENT_PREFERRED_LODGING_DEEP_RESEARCH",
});

export const ROUTING_SUPPORT = Object.freeze({
  FULL: "FULL",
  PARTIAL: "PARTIAL",
  UNSUPPORTED: "UNSUPPORTED",
});

const SEGMENT_LABELS = Object.freeze({
  associations_conferences_events: "Associations, Conferences & Events",
  corporate_accounts_scaling: "Corporate Accounts / Scaling Companies",
  corporate_meetings_incentives: "Corporate Meetings & Incentives",
  government_public_sector: "Government & Public Sector",
  sports_entertainment: "Sports & Entertainment",
  education_scientific: "Education & Scientific",
  projects_infrastructure: "Projects & Infrastructure",
  social_special_events: "Social & Special Events",
  local_catering_day_meetings: "Local Catering & Day Meetings",
  corporate_celebrations_private_events: "Corporate Celebrations & Private Events",
  travel_trade_group_intermediaries: "Travel Trade & Group Intermediaries",
  relocation_extended_stay: "Relocation & Extended Stay",
  preferred_lodging_partnerships: "Preferred Lodging Partnerships",
  professional_event_planners: "Professional Event Planners",
});

function familySpec(family, support, rationale) {
  if (family === EXA_AGENT_FAMILY.SCALING_COMPANIES) {
    return {
      agentFamily: family,
      agentName: "Scaling Companies Exa Agent",
      support,
      rationale,
      discovery: {
        strategyId: EXA_AGENT_RESEARCH_STRATEGIES.EXA_AGENT_SCALING_DISCOVERY,
        contractId: EXA_AGENT_DISCOVERY_CONTRACT_ACTIVE,
        promptVersion: "v2",
        promptPath:
          "lib/group-demand-intelligence/research/exa-agent/contracts/discovery-agent-prompt-v2.md",
        researchPurpose: "DISCOVERY",
      },
      deepResearch: {
        strategyId: EXA_AGENT_RESEARCH_STRATEGIES.EXA_AGENT_SCALING_DEEP_RESEARCH,
        contractId: EXA_AGENT_DEEP_RESEARCH_CONTRACT_V1,
        promptVersion: "v1",
        promptPath:
          "lib/group-demand-intelligence/research/exa-agent/contracts/deep-research-agent-prompt-v1.md",
        researchPurpose: "DEEP_RESEARCH",
      },
      contactInvestigation: {
        mode: "UNIVERSAL_CONTACT_BLOCK_IN_DEEP_CONTEXT",
        usesSegmentBuyerRoles: true,
        note: "Contact investigation rides inside the selected deep-research agent context via universal-contact-research-contract-v1.",
      },
    };
  }
  if (family === EXA_AGENT_FAMILY.ASSOCIATION_EVENTS) {
    return {
      agentFamily: family,
      agentName: "Association / Events Exa Agent",
      support,
      rationale,
      discovery: {
        strategyId: EXA_EVENTS_RESEARCH_STRATEGIES.EXA_AGENT_EVENTS_DISCOVERY,
        contractId: EXA_EVENTS_DISCOVERY_CONTRACT_V1,
        promptVersion: "v1",
        promptPath:
          "lib/group-demand-intelligence/research/exa-agent/contracts/event-discovery-agent-prompt-v1.md",
        researchPurpose: "DISCOVERY",
      },
      deepResearch: {
        strategyId: EXA_EVENTS_RESEARCH_STRATEGIES.EXA_AGENT_EVENTS_DEEP_RESEARCH,
        contractId: EXA_EVENTS_DEEP_RESEARCH_CONTRACT_V1,
        promptVersion: "v1",
        promptPath:
          "lib/group-demand-intelligence/research/exa-agent/contracts/event-deep-research-agent-prompt-v1.md",
        researchPurpose: "DEEP_RESEARCH",
      },
      contactInvestigation: {
        mode: "UNIVERSAL_CONTACT_BLOCK_IN_DEEP_CONTEXT",
        usesSegmentBuyerRoles: true,
        note: "Contact investigation rides inside Events deep context with segment-specific buyer role targets.",
      },
    };
  }
  if (family === EXA_AGENT_FAMILY.PREFERRED_LODGING) {
    return {
      agentFamily: family,
      agentName: "Preferred Lodging Partnerships Exa Agent",
      support,
      rationale,
      discovery: {
        strategyId:
          EXA_PREFERRED_LODGING_RESEARCH_STRATEGIES.EXA_AGENT_PREFERRED_LODGING_DISCOVERY,
        contractId: EXA_PREFERRED_LODGING_DISCOVERY_V1,
        promptVersion: "v1",
        promptPath:
          "lib/group-demand-intelligence/preferred-lodging-partnership/exa-preferred-lodging-contracts-v1.js",
        researchPurpose: "DISCOVERY",
      },
      deepResearch: {
        strategyId:
          EXA_PREFERRED_LODGING_RESEARCH_STRATEGIES.EXA_AGENT_PREFERRED_LODGING_DEEP_RESEARCH,
        contractId: EXA_PREFERRED_LODGING_DEEP_V1,
        promptVersion: "v1",
        promptPath:
          "lib/group-demand-intelligence/preferred-lodging-partnership/exa-preferred-lodging-contracts-v1.js",
        researchPurpose: "DEEP_RESEARCH",
      },
      contactInvestigation: {
        mode: "PREFERRED_LODGING_VENUE_CONTACT_BLOCK",
        usesSegmentBuyerRoles: true,
        note: "Venue owner/GM/events director contacts per preferred-lodging deep contract; not Scaling growth buyers.",
      },
    };
  }
  return {
    agentFamily: EXA_AGENT_FAMILY.NONE,
    agentName: null,
    support: ROUTING_SUPPORT.UNSUPPORTED,
    rationale,
    discovery: null,
    deepResearch: null,
    contactInvestigation: {
      mode: "UNIVERSAL_CONTACT_BLOCK_ONLY_WHEN_DEEP_ALLOWED",
      usesSegmentBuyerRoles: true,
      note: "No Exa agent family authorized for this segment until a genuine contract exists.",
    },
  };
}

/**
 * Canonical mapping: original two-family model + adapter queries.
 * PARTIAL = existing prompt genuinely covers the category with segment overlay.
 * UNSUPPORTED = do not force Scaling/Events silently.
 */
const SEGMENT_FAMILY_MAP = Object.freeze({
  associations_conferences_events: familySpec(
    EXA_AGENT_FAMILY.ASSOCIATION_EVENTS,
    ROUTING_SUPPORT.FULL,
    "Events discovery/deep contracts are the original association/conference/event pathway."
  ),
  corporate_accounts_scaling: familySpec(
    EXA_AGENT_FAMILY.SCALING_COMPANIES,
    ROUTING_SUPPORT.FULL,
    "Scaling discovery/deep contracts are the original corporate growth/expansion pathway."
  ),
  corporate_meetings_incentives: familySpec(
    EXA_AGENT_FAMILY.ASSOCIATION_EVENTS,
    ROUTING_SUPPORT.FULL,
    "Events discovery demand families B/C explicitly cover corporate meetings, training, and incentive/recognition travel."
  ),
  government_public_sector: familySpec(
    EXA_AGENT_FAMILY.ASSOCIATION_EVENTS,
    ROUTING_SUPPORT.PARTIAL,
    "Events agent covers institutional conferences/programmes and lodging/meetings mechanisms; not a dedicated government RFP agent."
  ),
  sports_entertainment: familySpec(
    EXA_AGENT_FAMILY.ASSOCIATION_EVENTS,
    ROUTING_SUPPORT.PARTIAL,
    "Event-led sports/entertainment demand fits Events commercial tracks better than Scaling growth framing."
  ),
  education_scientific: familySpec(
    EXA_AGENT_FAMILY.ASSOCIATION_EVENTS,
    ROUTING_SUPPORT.FULL,
    "Scientific/medical/academic programmes are core Events association/conference demand."
  ),
  projects_infrastructure: familySpec(
    EXA_AGENT_FAMILY.SCALING_COMPANIES,
    ROUTING_SUPPORT.PARTIAL,
    "Scaling deep explicitly investigates project teams, visiting staff, and operating expansion lodging — not pure construction bidding."
  ),
  social_special_events: familySpec(
    EXA_AGENT_FAMILY.ASSOCIATION_EVENTS,
    ROUTING_SUPPORT.FULL,
    "Social/special events and destination celebrations are event-led demand under Events contracts."
  ),
  local_catering_day_meetings: familySpec(
    EXA_AGENT_FAMILY.ASSOCIATION_EVENTS,
    ROUTING_SUPPORT.PARTIAL,
    "Events mission includes meetings space and catering/F&B demand; day-meeting volume is overlay via segment adapters."
  ),
  corporate_celebrations_private_events: familySpec(
    EXA_AGENT_FAMILY.ASSOCIATION_EVENTS,
    ROUTING_SUPPORT.FULL,
    "Events demand family B includes corporate celebrations and company group experiences."
  ),
  travel_trade_group_intermediaries: familySpec(
    EXA_AGENT_FAMILY.ASSOCIATION_EVENTS,
    ROUTING_SUPPORT.PARTIAL,
    "Events pathway can investigate planners/DMCs as commercial intermediaries for group demand; not a dedicated wholesaler contract."
  ),
  relocation_extended_stay: familySpec(
    EXA_AGENT_FAMILY.SCALING_COMPANIES,
    ROUTING_SUPPORT.PARTIAL,
    "Scaling deep covers corporate travel, mobility, and extended operating presence better than Events calendaring."
  ),
  preferred_lodging_partnerships: familySpec(
    EXA_AGENT_FAMILY.PREFERRED_LODGING,
    ROUTING_SUPPORT.FULL,
    "Dedicated Preferred Lodging discovery/deep contracts (exa-preferred-lodging-contracts-v1) — not Scaling Companies and not Association/Events."
  ),
  professional_event_planners: familySpec(
    EXA_AGENT_FAMILY.ASSOCIATION_EVENTS,
    ROUTING_SUPPORT.FULL,
    "Professional planners are the Events commercial entry ecosystem; Events deep + planner buyer roles apply."
  ),
});

export function getSegmentLabel(segmentId) {
  return SEGMENT_LABELS[segmentId] || String(segmentId || "");
}

/**
 * Resolve discovery + deep + contact routing for a canonical segment.
 */
export function resolveExaAgentRoutingForSegment(segmentId, opts = {}) {
  const sid = String(segmentId || "").trim();
  const base = SEGMENT_FAMILY_MAP[sid] || null;
  const stage = String(opts.stage || "").toUpperCase();

  if (!base) {
    return {
      version: GDI_SEGMENT_AWARE_EXA_AGENT_ROUTING_V1,
      segmentId: sid || null,
      segmentLabel: getSegmentLabel(sid),
      knownSegment: false,
      agentFamily: EXA_AGENT_FAMILY.NONE,
      agentName: null,
      support: ROUTING_SUPPORT.UNSUPPORTED,
      rationale: "Unknown segment — no Exa agent family authorized.",
      discovery: null,
      deepResearch: null,
      contactInvestigation: {
        mode: "BLOCKED",
        usesSegmentBuyerRoles: false,
      },
      paidExaAllowed: false,
      blockReason: "UNKNOWN_SEGMENT_NO_AGENT_FAMILY",
      stage: stage || null,
    };
  }

  const unsupported = base.support === ROUTING_SUPPORT.UNSUPPORTED;
  const discoveryAllowed = !unsupported && base.discovery;
  const deepAllowed = !unsupported && base.deepResearch;

  let paidExaAllowed = true;
  let blockReason = null;
  if (unsupported) {
    paidExaAllowed = false;
    blockReason = "AGENT_FAMILY_UNSUPPORTED_FOR_SEGMENT";
  } else if (stage === "DISCOVERY" && !discoveryAllowed) {
    paidExaAllowed = false;
    blockReason = "DISCOVERY_AGENT_UNSUPPORTED";
  } else if (stage === "DEEP_RESEARCH" && !deepAllowed) {
    paidExaAllowed = false;
    blockReason = "DEEP_RESEARCH_AGENT_UNSUPPORTED";
  }

  return {
    version: GDI_SEGMENT_AWARE_EXA_AGENT_ROUTING_V1,
    segmentId: sid,
    segmentLabel: getSegmentLabel(sid),
    knownSegment: true,
    agentFamily: base.agentFamily,
    agentName: base.agentName,
    support: base.support,
    rationale: base.rationale,
    discovery: base.discovery,
    deepResearch: base.deepResearch,
    contactInvestigation: base.contactInvestigation,
    paidExaAllowed,
    blockReason,
    stage: stage || null,
    discoveryContractId: base.discovery?.contractId || null,
    deepResearchContractId: base.deepResearch?.contractId || null,
    // Convenience aliases for metadata stamping
    promptVersion:
      stage === "DISCOVERY"
        ? base.discovery?.promptVersion || null
        : stage === "DEEP_RESEARCH"
          ? base.deepResearch?.promptVersion || null
          : null,
    researchPurpose:
      stage === "DISCOVERY"
        ? "DISCOVERY"
        : stage === "DEEP_RESEARCH"
          ? "DEEP_RESEARCH"
          : stage === "CONTACT"
            ? "CONTACT_INVESTIGATION"
            : null,
  };
}

export function listSegmentExaAgentRoutingTable() {
  return listFourteenWorkflowSegmentIds().map((segmentId) => {
    const r = resolveExaAgentRoutingForSegment(segmentId);
    return {
      segmentId,
      segmentLabel: r.segmentLabel,
      agentFamily: r.agentFamily,
      agentName: r.agentName,
      support: r.support,
      discoveryContractId: r.discovery?.contractId || null,
      discoveryPromptVersion: r.discovery?.promptVersion || null,
      deepResearchContractId: r.deepResearch?.contractId || null,
      deepResearchPromptVersion: r.deepResearch?.promptVersion || null,
      contactMode: r.contactInvestigation?.mode || null,
      rationale: r.rationale,
      paidExaAllowed: r.paidExaAllowed,
    };
  });
}

export function buildExaOperationRoutingMetadata({
  hotel = {},
  segmentId = null,
  stage = "DEEP_RESEARCH",
  extra = {},
} = {}) {
  const routing = resolveExaAgentRoutingForSegment(segmentId, { stage });
  const stagePack =
    stage === "DISCOVERY"
      ? routing.discovery
      : stage === "DEEP_RESEARCH"
        ? routing.deepResearch
        : null;
  return {
    version: GDI_SEGMENT_AWARE_EXA_AGENT_ROUTING_V1,
    hotelId: hotel.hotelId || hotel.id || null,
    hotelName: hotel.displayName || hotel.name || hotel.hotelName || null,
    demandSegment: routing.segmentId,
    segmentLabel: routing.segmentLabel,
    agentFamily: routing.agentFamily,
    agentName: routing.agentName,
    support: routing.support,
    promptContractId: stagePack?.contractId || null,
    promptVersion: stagePack?.promptVersion || null,
    promptPath: stagePack?.promptPath || null,
    strategyId: stagePack?.strategyId || null,
    researchPurpose: stagePack?.researchPurpose || routing.researchPurpose || stage,
    paidExaAllowed: routing.paidExaAllowed,
    blockReason: routing.blockReason,
    ...extra,
  };
}

/**
 * Assert every canonical segment has an explicit routing decision (including UNSUPPORTED).
 */
export function assertFourteenSegmentRoutingCoverage() {
  const rows = listSegmentExaAgentRoutingTable();
  const missing = listFourteenWorkflowSegmentIds().filter(
    (id) => !rows.find((r) => r.segmentId === id)
  );
  const unsupported = rows.filter((r) => r.support === ROUTING_SUPPORT.UNSUPPORTED);
  const forcedScaling = rows.filter(
    (r) =>
      r.segmentId !== "corporate_accounts_scaling" &&
      r.segmentId !== "projects_infrastructure" &&
      r.segmentId !== "relocation_extended_stay" &&
      r.agentFamily === EXA_AGENT_FAMILY.SCALING_COMPANIES
  );
  return {
    version: GDI_SEGMENT_AWARE_EXA_AGENT_ROUTING_V1,
    ok: missing.length === 0 && forcedScaling.length === 0,
    rowCount: rows.length,
    missing,
    unsupportedSegmentIds: unsupported.map((r) => r.segmentId),
    nonScalingForcedIntoScaling: forcedScaling.map((r) => r.segmentId),
    scalingDiscoveryContract: EXA_AGENT_DISCOVERY_CONTRACT_V2,
    eventsDiscoveryContract: EXA_EVENTS_DISCOVERY_CONTRACT_V1,
    rows,
  };
}

export default {
  resolveExaAgentRoutingForSegment,
  listSegmentExaAgentRoutingTable,
  buildExaOperationRoutingMetadata,
  assertFourteenSegmentRoutingCoverage,
  EXA_AGENT_FAMILY,
  ROUTING_SUPPORT,
  EXA_PREFERRED_LODGING_RESEARCH_STRATEGIES,
};
