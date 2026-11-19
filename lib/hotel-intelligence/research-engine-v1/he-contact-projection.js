/**
 * Hotel Explorer Contact Intelligence view-model projection.
 * Produces ownershipSummary required by public/js/hotel-contact-intelligence.js
 * plus package-shaped fields from contact-intelligence/view-model.js.
 */

import { buildContactIntelligenceViewModel } from "../contact-intelligence/view-model.js";
import { ENGINE_CONTRACT_ID, ENGINE_CONTRACT_VERSION, writeGuaranteesP1 } from "./contract.js";

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
  const sponsor = staged.ownership?.economic_sponsor || owner;
  const operator = staged.ownership?.operator_entity || null;
  const brand = staged.brand || null;

  return {
    ...base,
    version: base.version,
    engine_contract: ENGINE_CONTRACT_ID,
    engine_contract_version: ENGINE_CONTRACT_VERSION,
    hotelName: pkg.hotel_name,
    research_complete: false,
    ownershipSummary: {
      hotel: { name: pkg.hotel_name, hpcId: pkg.hotel_id },
      propco: orgNode(owner, "Property Owner / PropCo"),
      sponsor: orgNode(sponsor, "Economic Owner"),
      operator: {
        ...orgNode(operator, "Current Operator"),
        sameAsSponsor: Boolean(
          operator?.dedupe_key &&
            sponsor?.dedupe_key &&
            operator.dedupe_key === sponsor.dedupe_key
        ),
      },
      brand: orgNode(brand, "Current Brand"),
      brandHistory: staged.brand_history || [],
      formerOwner: staged.former_owner || null,
    },
    qualitySummary: {
      ownerOrgStatus: owner ? "STAGED" : "UNRESOLVED",
      operatorStatus: operator ? "STAGED" : "UNRESOLVED",
      contactStatus: (base.decision_makers || []).length ? "STAGED" : "NONE",
      evidenceStatus: base.evidence_available ? "PRESENT" : "NONE",
      publication: "BLOCKED",
    },
  };
}
