/**
 * Hotel Explorer Contact Intelligence view-model projection.
 * Ownership success uses the shared evidence-grounded classifier only —
 * populated staged arrays never auto-promote to FULL_ACTIONABLE.
 */

import { buildContactIntelligenceViewModel } from "../contact-intelligence/view-model.js";
import { ENGINE_CONTRACT_ID, ENGINE_CONTRACT_VERSION, writeGuaranteesP1 } from "./contract.js";
import {
  OWNERSHIP_DRAWER_QUESTIONS,
  OWNERSHIP_RESEARCH_OBJECTIVE_ID,
  OWNERSHIP_SUCCESS_TIER,
  buildClassifierInputFromStaged,
  classifyOwnershipResearchSuccess,
  writeOwnershipObjectiveGuarantees,
} from "./ownership-research-objective.js";

function orgNode(org, label, extra = {}) {
  if (!org) {
    return { label, name: null, shortName: null, ...extra };
  }
  return {
    label,
    name: org.legal_name || org.name || null,
    shortName: org.short_name || org.legal_name || org.name || null,
    org_id: org.org_id || null,
    dedupe_key: org.dedupe_key || null,
    verified: org.verified === true,
    ...extra,
  };
}

/**
 * Project a staged engine package into HE Contact UI view-model.
 * research_complete, evidence verification, and customer publication remain separate gates.
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

  const classifierInput = buildClassifierInputFromStaged(staged);
  const ownershipOutcome = classifyOwnershipResearchSuccess(classifierInput);
  const successTier = ownershipOutcome.tier;

  const whoToSpeakWith =
    staged.ownership?.primary_ownership_contact ||
    staged.ownership?.ownership_representative ||
    null;
  const howToReach = staged.ownership?.primary_contact_channels || [];
  const howWeKnow = staged.ownership?.evidence_summary || staged.evidence_summary || null;

  const sameOrgDual =
    Boolean(operator?.dedupe_key && sponsor?.dedupe_key && operator.dedupe_key === sponsor.dedupe_key) ||
    Boolean(operator?.org_id && sponsor?.org_id && operator.org_id === sponsor.org_id);
  const dualVerified =
    sameOrgDual &&
    operator?.operation_verified_independently === true &&
    (ultimate?.verified === true || sponsor?.ownership_verified_independently === true) &&
    staged.ownership?.ownership_inferred_from_management_or_brand_alone !== true;

  return {
    ...base,
    version: base.version,
    engine_contract: ENGINE_CONTRACT_ID,
    engine_contract_version: ENGINE_CONTRACT_VERSION,
    ownership_research_objective: OWNERSHIP_RESEARCH_OBJECTIVE_ID,
    ownership_objective_guarantees: writeOwnershipObjectiveGuarantees(),
    hotelName: pkg.hotel_name,
    /** Separate from ownership evidence verification and publication */
    research_complete: false,
    ownershipOutcome,
    ownershipSummary: {
      hotel: { name: pkg.hotel_name, hpcId: pkg.hotel_id },
      propco: orgNode(legalTitleholder, "Legal Titleholder / PropCo"),
      legalTitleholder: orgNode(legalTitleholder, "Legal Titleholder"),
      ultimateOwner: orgNode(ultimate, "Ultimate / Controlling Owner"),
      sponsor: orgNode(sponsor, "Economic / Beneficial Owner"),
      operator: {
        ...orgNode(operator, "Hotel Management / Operator"),
        sameAsSponsor: sameOrgDual,
        /** Not an owner by default; dual role only when independently verified */
        notAnOwner: !dualVerified,
        dualRoleIndependentlyVerified: dualVerified,
      },
      brand: {
        ...orgNode(brand, "Brand / Franchise"),
        notAnOwner: true,
      },
      brandHistory: staged.brand_history || [],
      formerOwner: staged.former_owner || null,
      successTier,
      ultimateOwnerStatus: ownershipOutcome.unresolvedUltimateOwnership
        ? "UNRESOLVED"
        : ultimate?.verified
          ? "VERIFIED"
          : ultimate
            ? "PROVISIONAL"
            : "UNRESOLVED",
      contactChannelClass: ownershipOutcome.contactChannelClass,
      verifiedDirectContact:
        ownershipOutcome.contactChannelClass === "VERIFIED_DIRECT",
      indirectInstitutionalPathway:
        ownershipOutcome.contactChannelClass === "INDIRECT_INSTITUTIONAL",
      drawerQuestions: OWNERSHIP_DRAWER_QUESTIONS,
      drawerAnswers: {
        whoOwnsThisHotel:
          ultimate?.verified === true
            ? ultimate.legal_name || ultimate.name || null
            : null,
        whoShouldISpeakWith:
          whoToSpeakWith?.verified === true
            ? whoToSpeakWith.full_name || whoToSpeakWith.name || null
            : null,
        howCanIReachThem: {
          verifiedDirect: howToReach.filter((c) => c?.verified === true && c?.inferred !== true),
          indirectInstitutional: howToReach.filter(
            (c) => c?.pathway === "institutional" && c?.verified === true
          ),
        },
        howDoWeKnow: howWeKnow,
      },
    },
    qualitySummary: {
      ownerOrgStatus: ownershipOutcome.tier,
      ultimateOwnerStatus: ownershipOutcome.unresolvedUltimateOwnership
        ? "UNRESOLVED"
        : "CLASSIFIED",
      operatorStatus: operator ? (dualVerified ? "DUAL_ROLE_VERIFIED" : "STAGED") : "UNRESOLVED",
      contactStatus: ownershipOutcome.contactChannelClass,
      evidenceStatus: ownershipOutcome.evidenceVerification,
      ownershipSuccessTier: successTier,
      /** Mirrors shared classifier — never inferred from array population alone */
      fullResearchSuccess: ownershipOutcome.fullResearchSuccess === true,
      researchSuccess: ownershipOutcome.researchSuccess === true,
      evidenceVerification: ownershipOutcome.evidenceVerification,
      customerPublication: ownershipOutcome.customerPublication || "BLOCKED",
      provisionalStaged: ownershipOutcome.provisionalStaged === true,
      publication: "BLOCKED",
    },
  };
}

export { OWNERSHIP_SUCCESS_TIER };
