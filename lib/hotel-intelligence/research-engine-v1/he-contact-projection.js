/**
 * Hotel Explorer Contact Intelligence view-model projection.
 * Produces ownershipSummary required by public/js/hotel-contact-intelligence.js
 * plus package-shaped fields from contact-intelligence/view-model.js.
 */

import { buildContactIntelligenceViewModel } from "../contact-intelligence/view-model.js";
import { ENGINE_CONTRACT_ID, ENGINE_CONTRACT_VERSION, writeGuaranteesP1 } from "./contract.js";
import {
  OWNERSHIP_DRAWER_QUESTIONS,
  OWNERSHIP_RESEARCH_OBJECTIVE_ID,
  OWNERSHIP_SUCCESS_TIER,
  writeOwnershipObjectiveGuarantees,
} from "./ownership-research-objective.js";

function orgNode(org, label) {
  if (!org) {
    return { label, name: null, shortName: null };
  }
  return {
    label,
    name: org.legal_name || org.name || null,
    shortName: org.short_name || org.legal_name || org.name || null,
    org_id: org.org_id || null,
    dedupe_key: org.dedupe_key || null,
  };
}

/**
 * Project a staged engine package into HE Contact UI view-model.
 * Does not claim research-complete unless explicitly staged verified.
 */
export function projectHotelExplorerContactViewModel(staged = {}, opts = {}) {
  const pkg = {
    hotel_id: staged.hpc_id || staged.hotel_id || null,
    hotel_name: staged.hotel_name || null,
    owner_entity_id: staged.ownership?.owner_entity?.org_id || null,
    owner_display_name: staged.ownership?.owner_entity?.legal_name || null,
    owner_resolved: Boolean(staged.ownership?.owner_entity?.legal_name),
    hotel_contact: staged.hotel_contact || { channels: [] },
    organization_contact_route: staged.organization_contact_route || { channels: [] },
    people: (staged.persons || []).map((p) => ({
      ...p,
      channels: (staged.channels || []).filter((c) => c.subject_id === p.person_id),
    })),
    refresh_status: staged.refresh_status || "STAGED_ONLY",
    last_refreshed_at: staged.last_refreshed_at || null,
    unresolved_reasons: staged.unresolved_reasons || [],
    write_guarantees: writeGuaranteesP1(),
  };

  const base = buildContactIntelligenceViewModel(pkg, {
    audience: opts.audience || "internal",
  });

  const owner = staged.ownership?.owner_entity || null;
  const ultimate =
    staged.ownership?.ultimate_owner ||
    staged.ownership?.controlling_investor ||
    staged.ownership?.economic_sponsor ||
    null;
  const sponsor = ultimate || staged.ownership?.economic_sponsor || owner;
  const operator = staged.ownership?.operator_entity || null;
  const brand = staged.brand || null;
  const legalTitleholder = staged.ownership?.legal_titleholder || owner;
  const ownershipTier = staged.ownership?.success_tier || null;
  const ultimateUnresolved =
    staged.ownership?.ultimate_owner_status === "UNRESOLVED" ||
    (!ultimate && Boolean(legalTitleholder));
  const decisionMakers = base.decision_makers || [];
  const whoToSpeakWith =
    staged.ownership?.primary_ownership_contact ||
    decisionMakers[0] ||
    staged.ownership?.ownership_representative ||
    null;
  const howToReach =
    staged.ownership?.primary_contact_channels ||
    whoToSpeakWith?.channels ||
    [];
  const howWeKnow = staged.ownership?.evidence_summary || staged.evidence_summary || null;

  let successTier = ownershipTier;
  if (!successTier) {
    if (ultimate && decisionMakers.length && howToReach?.length) {
      successTier = OWNERSHIP_SUCCESS_TIER.FULL_ACTIONABLE;
    } else if (legalTitleholder && !ultimate) {
      successTier = OWNERSHIP_SUCCESS_TIER.PARTIAL_LEGAL_ENTITY_ONLY;
    } else if (ultimateUnresolved && (whoToSpeakWith || howToReach?.length)) {
      successTier = OWNERSHIP_SUCCESS_TIER.PARTIAL_REPRESENTATIVE_PATHWAY;
    } else {
      successTier = OWNERSHIP_SUCCESS_TIER.UNRESOLVED;
    }
  }

  return {
    ...base,
    version: base.version,
    engine_contract: ENGINE_CONTRACT_ID,
    engine_contract_version: ENGINE_CONTRACT_VERSION,
    ownership_research_objective: OWNERSHIP_RESEARCH_OBJECTIVE_ID,
    ownership_objective_guarantees: writeOwnershipObjectiveGuarantees(),
    hotelName: pkg.hotel_name,
    research_complete: false,
    ownershipSummary: {
      hotel: { name: pkg.hotel_name, hpcId: pkg.hotel_id },
      /** Legal titleholder / PropCo — intermediary evidence, not final success alone */
      propco: orgNode(legalTitleholder, "Legal Titleholder / PropCo"),
      legalTitleholder: orgNode(legalTitleholder, "Legal Titleholder"),
      /** Ultimate / economic / controlling ownership — primary goal */
      ultimateOwner: orgNode(ultimate, "Ultimate / Controlling Owner"),
      sponsor: orgNode(sponsor, "Economic / Beneficial Owner"),
      operator: {
        ...orgNode(operator, "Hotel Management / Operator"),
        sameAsSponsor: Boolean(
          operator?.dedupe_key &&
            sponsor?.dedupe_key &&
            operator.dedupe_key === sponsor.dedupe_key
        ),
        notAnOwner: true,
      },
      brand: {
        ...orgNode(brand, "Brand / Franchise"),
        notAnOwner: true,
      },
      brandHistory: staged.brand_history || [],
      formerOwner: staged.former_owner || null,
      successTier,
      ultimateOwnerStatus: ultimate
        ? "STAGED"
        : ultimateUnresolved
          ? "UNRESOLVED"
          : "UNRESOLVED",
      drawerQuestions: OWNERSHIP_DRAWER_QUESTIONS,
      drawerAnswers: {
        whoOwnsThisHotel: ultimate?.legal_name || ultimate?.name || null,
        whoShouldISpeakWith:
          whoToSpeakWith?.full_name ||
          whoToSpeakWith?.name ||
          whoToSpeakWith?.legal_name ||
          null,
        howCanIReachThem: howToReach,
        howDoWeKnow: howWeKnow,
      },
    },
    qualitySummary: {
      ownerOrgStatus: ultimate
        ? "STAGED"
        : legalTitleholder
          ? "PARTIAL_LEGAL_ENTITY_ONLY"
          : "UNRESOLVED",
      ultimateOwnerStatus: ultimate ? "STAGED" : "UNRESOLVED",
      operatorStatus: operator ? "STAGED" : "UNRESOLVED",
      contactStatus: decisionMakers.length ? "STAGED" : "NONE",
      evidenceStatus: base.evidence_available ? "PRESENT" : "NONE",
      ownershipSuccessTier: successTier,
      fullResearchSuccess: successTier === OWNERSHIP_SUCCESS_TIER.FULL_ACTIONABLE,
      publication: "BLOCKED",
    },
  };
}
