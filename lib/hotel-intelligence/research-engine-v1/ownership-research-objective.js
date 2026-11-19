/**
 * HE-INTEL-001 — Ownership research objective (founder correction).
 *
 * FULL_ACTIONABLE requires evidence-grounded ultimate ownership intelligence —
 * not claim-type labels alone, and not populated staged arrays alone.
 *
 * Shared by P3A/P3B benchmarking and Hotel Explorer ownership presentation.
 */

export const OWNERSHIP_RESEARCH_OBJECTIVE_ID =
  "HE_INTEL_001_OWNERSHIP_RESEARCH_OBJECTIVE_V1";
export const OWNERSHIP_RESEARCH_OBJECTIVE_VERSION = "1.1.0";

export const OWNERSHIP_DRAWER_QUESTIONS = Object.freeze([
  "Who owns this hotel?",
  "Who should I speak with about it?",
  "How can I reach them?",
  "How do we know?",
]);

export const OWNERSHIP_ROLE = Object.freeze({
  LEGAL_TITLEHOLDER: "LEGAL_TITLEHOLDER",
  ECONOMIC_OR_BENEFICIAL_OWNER: "ECONOMIC_OR_BENEFICIAL_OWNER",
  CONTROLLING_INVESTOR_OR_GROUP: "CONTROLLING_INVESTOR_OR_GROUP",
  ASSET_MANAGER_OR_OWNERSHIP_REPRESENTATIVE: "ASSET_MANAGER_OR_OWNERSHIP_REPRESENTATIVE",
  HOTEL_MANAGEMENT_OPERATOR: "HOTEL_MANAGEMENT_OPERATOR",
  BRAND_OR_FRANCHISE: "BRAND_OR_FRANCHISE",
});

export const OWNERSHIP_SUCCESS_TIER = Object.freeze({
  FULL_ACTIONABLE: "FULL_ACTIONABLE",
  PARTIAL_LEGAL_ENTITY_ONLY: "PARTIAL_LEGAL_ENTITY_ONLY",
  PARTIAL_OWNER_WITHOUT_CONTACT: "PARTIAL_OWNER_WITHOUT_CONTACT",
  PARTIAL_REPRESENTATIVE_PATHWAY: "PARTIAL_REPRESENTATIVE_PATHWAY",
  UNRESOLVED: "UNRESOLVED",
});

export const PRIMARY_OWNERSHIP_GOALS = Object.freeze([
  "ULTIMATE_OWNER_INVESTOR_FAMILY_FUND_REIT_OR_CONTROLLING_ORG",
  "NAMED_OWNERSHIP_DECISION_MAKERS",
  "VERIFIED_CONTACT_AND_CREDIBLE_PATHWAYS",
  "OWNERSHIP_STRUCTURE_AND_LEGAL_ENTITY_CHAIN",
]);

export const OWNERSHIP_DISTINCTION_RULES = Object.freeze({
  legal_entities_are_intermediary_evidence: true,
  legal_entity_only_is_partial_not_full_success: true,
  do_not_infer_ownership_from_management_or_brand_alone: true,
  dual_ownership_and_operation_allowed_when_independently_verified: true,
  never_invent_beneficial_owners_percentages_names_emails_phones: true,
  full_actionable_requires_evidence_not_labels: true,
  when_ultimate_ownership_not_public:
    "Mark UNRESOLVED and provide strongest verified ownership representative or institutional contact pathway",
});

export const OWNERSHIP_CLAIM_TYPES = Object.freeze({
  LEGAL_TITLEHOLDER: "LEGAL_TITLEHOLDER",
  ULTIMATE_OWNER: "ULTIMATE_OWNER",
  CONTROLLING_INVESTOR: "CONTROLLING_INVESTOR",
  OWNERSHIP_REPRESENTATIVE: "OWNERSHIP_REPRESENTATIVE",
  OWNERSHIP_STRUCTURE_EDGE: "OWNERSHIP_STRUCTURE_EDGE",
  OWNERSHIP_DECISION_MAKER: "OWNERSHIP_DECISION_MAKER",
  ALTERNATIVE_CONTACT_PATHWAY: "ALTERNATIVE_CONTACT_PATHWAY",
  OWNER_ENTITY: "OWNER_ENTITY",
  ECONOMIC_SPONSOR: "ECONOMIC_SPONSOR",
});

function hasUrl(v) {
  return Boolean(v && String(v).trim());
}

function evidenceOk(node) {
  return Boolean(
    node &&
      node.verified === true &&
      hasUrl(node.evidenceUrl) &&
      node.inferred !== true &&
      node.unsupported !== true
  );
}

/**
 * True when ownership is asserted only from operator/brand relationship
 * without independent ownership evidence.
 */
export function ownershipInferredFromOperatorOrBrandAlone(input = {}) {
  if (input.ownershipInferredFromManagementOrBrandAlone === true) return true;
  const roles = Array.isArray(input.orgRoles) ? input.orgRoles : [];
  for (const r of roles) {
    if (r?.ownershipInferredFromManagementOnly === true) return true;
    if (
      r?.claimsOwnership === true &&
      r?.ownershipVerifiedIndependently !== true &&
      (r?.roles || []).some((x) =>
        ["OPERATOR", "BRAND", "MANAGEMENT"].includes(String(x).toUpperCase())
      )
    ) {
      return true;
    }
  }
  return false;
}

/**
 * Dual owner+operator for same org is allowed only when each role is
 * independently verified (never infer ownership from management alone).
 */
export function evaluateDualOrgRoles(orgRoles = []) {
  const supported = [];
  const rejected = [];
  for (const r of orgRoles || []) {
    const roles = new Set((r.roles || []).map((x) => String(x).toUpperCase()));
    const owns = roles.has("OWNER") || roles.has("ULTIMATE_OWNER") || roles.has("INVESTOR");
    const operates = roles.has("OPERATOR") || roles.has("MANAGEMENT");
    if (owns && operates) {
      if (
        r.ownershipVerifiedIndependently === true &&
        r.operationVerifiedIndependently === true &&
        r.ownershipInferredFromManagementOnly !== true
      ) {
        supported.push({
          orgId: r.orgId || null,
          dualRole: true,
          ok: true,
        });
      } else {
        rejected.push({
          orgId: r.orgId || null,
          dualRole: true,
          ok: false,
          reason: "OWNERSHIP_NOT_INDEPENDENTLY_VERIFIED",
        });
      }
    }
  }
  return { supported, rejected };
}

/**
 * Evidence-grounded ownership success classifier (single authority).
 *
 * @param {object} input
 * @param {object} [input.ultimateOwner] — { verified, name, evidenceUrl, linkedToHpc, inferred, unsupported }
 * @param {object} [input.legalTitleholder]
 * @param {object} [input.ownershipPerson] — named ownership/asset-mgmt person
 * @param {object} [input.contact] — { directVerified, indirectInstitutionalPathway, inferred, evidenceUrl, connectedToPersonOrOwner }
 * @param {object} [input.structureChain] — { verified, evidenceUrl, relevant }
 * @param {object[]} [input.orgRoles]
 * @param {boolean} [input.stagedOnly] — staged arrays without verification stay provisional
 * @param {boolean} [input.ultimateOwnerUnresolved]
 */
export function classifyOwnershipResearchSuccess(input = {}) {
  const dual = evaluateDualOrgRoles(input.orgRoles || []);
  const inferredFromMgmt = ownershipInferredFromOperatorOrBrandAlone(input);

  const ultimate = input.ultimateOwner || null;
  const legal = input.legalTitleholder || null;
  const person = input.ownershipPerson || null;
  const contact = input.contact || null;
  const structure = input.structureChain || null;

  const ultimateVerified =
    evidenceOk(ultimate) &&
    ultimate.linkedToHpc === true &&
    !inferredFromMgmt;

  const legalVerified = evidenceOk(legal) && legal.linkedToHpc === true;

  const personOk =
    evidenceOk(person) &&
    person.roleRelevant === true &&
    person.connectedToOwnerOrRepresentative === true &&
    person.unrelatedHotelSales !== true &&
    person.genericOperatorContact !== true;

  const directContact =
    contact?.directVerified === true &&
    contact?.inferred !== true &&
    contact?.connectedToPersonOrOwner === true &&
    hasUrl(contact?.evidenceUrl);

  const indirectPathway =
    contact?.indirectInstitutionalPathway === true &&
    contact?.inferred !== true &&
    contact?.connectedToPersonOrOwner === true &&
    hasUrl(contact?.evidenceUrl);

  const contactOk = directContact || indirectPathway;

  const structureRelevant = structure?.relevant !== false;
  const structureOk =
    !structureRelevant ||
    (evidenceOk(structure) && structure.verified === true);

  // Staged-only populated fields without verification → never FULL
  if (input.stagedOnly === true && !ultimateVerified) {
    return {
      tier: legalVerified
        ? OWNERSHIP_SUCCESS_TIER.PARTIAL_LEGAL_ENTITY_ONLY
        : OWNERSHIP_SUCCESS_TIER.UNRESOLVED,
      fullResearchSuccess: false,
      partial: Boolean(legalVerified || personOk || contactOk),
      unresolvedUltimateOwnership: true,
      provisionalStaged: true,
      researchSuccess: false,
      evidenceVerification: "PROVISIONAL",
      customerPublication: "BLOCKED",
      contactChannelClass: directContact
        ? "VERIFIED_DIRECT"
        : indirectPathway
          ? "INDIRECT_INSTITUTIONAL"
          : "NONE",
      dualOrgRoles: dual,
      note: "Staged but unsupported/unverified owner claims remain provisional",
    };
  }

  if (ultimateVerified && personOk && contactOk && structureOk) {
    return {
      tier: OWNERSHIP_SUCCESS_TIER.FULL_ACTIONABLE,
      fullResearchSuccess: true,
      partial: false,
      unresolvedUltimateOwnership: false,
      provisionalStaged: false,
      researchSuccess: true,
      evidenceVerification: "VERIFIED",
      customerPublication: "BLOCKED",
      contactChannelClass: directContact
        ? "VERIFIED_DIRECT"
        : "INDIRECT_INSTITUTIONAL",
      dualOrgRoles: dual,
      note: "Evidence-grounded ultimate owner + relevant ownership person + contact pathway + hotel link",
    };
  }

  if (ultimateVerified && !contactOk) {
    return {
      tier: OWNERSHIP_SUCCESS_TIER.PARTIAL_OWNER_WITHOUT_CONTACT,
      fullResearchSuccess: false,
      partial: true,
      unresolvedUltimateOwnership: false,
      provisionalStaged: false,
      researchSuccess: false,
      evidenceVerification: personOk ? "PARTIAL" : "PARTIAL",
      customerPublication: "BLOCKED",
      contactChannelClass: "NONE",
      dualOrgRoles: dual,
      note: "Verified owner without relevant verified contact pathway",
    };
  }

  if (
    ultimateVerified &&
    contactOk &&
    (!personOk || !structureOk)
  ) {
    return {
      tier: OWNERSHIP_SUCCESS_TIER.PARTIAL_REPRESENTATIVE_PATHWAY,
      fullResearchSuccess: false,
      partial: true,
      unresolvedUltimateOwnership: false,
      provisionalStaged: false,
      researchSuccess: false,
      evidenceVerification: "PARTIAL",
      customerPublication: "BLOCKED",
      contactChannelClass: directContact
        ? "VERIFIED_DIRECT"
        : "INDIRECT_INSTITUTIONAL",
      dualOrgRoles: dual,
      note: !personOk
        ? "Owner present but person not ownership-relevant or not connected"
        : "Owner present but ownership structure evidence incomplete",
    };
  }

  if (input.ultimateOwnerUnresolved === true || (!ultimateVerified && !legalVerified)) {
    if (personOk || indirectPathway || directContact) {
      return {
        tier: OWNERSHIP_SUCCESS_TIER.PARTIAL_REPRESENTATIVE_PATHWAY,
        fullResearchSuccess: false,
        partial: true,
        unresolvedUltimateOwnership: true,
        provisionalStaged: false,
        researchSuccess: false,
        evidenceVerification: "PARTIAL",
        customerPublication: "BLOCKED",
        contactChannelClass: directContact
          ? "VERIFIED_DIRECT"
          : indirectPathway
            ? "INDIRECT_INSTITUTIONAL"
            : "NONE",
        dualOrgRoles: dual,
        note: "Ultimate ownership UNRESOLVED; strongest verified representative/pathway retained",
      };
    }
    return {
      tier: OWNERSHIP_SUCCESS_TIER.UNRESOLVED,
      fullResearchSuccess: false,
      partial: false,
      unresolvedUltimateOwnership: true,
      provisionalStaged: Boolean(input.stagedOnly),
      researchSuccess: false,
      evidenceVerification: "UNRESOLVED",
      customerPublication: "BLOCKED",
      contactChannelClass: "NONE",
      dualOrgRoles: dual,
      note: "Ultimate ownership not publicly ascertainable",
    };
  }

  if (legalVerified && !ultimateVerified) {
    return {
      tier: OWNERSHIP_SUCCESS_TIER.PARTIAL_LEGAL_ENTITY_ONLY,
      fullResearchSuccess: false,
      partial: true,
      unresolvedUltimateOwnership: true,
      provisionalStaged: false,
      researchSuccess: false,
      evidenceVerification: "PARTIAL",
      customerPublication: "BLOCKED",
      contactChannelClass: directContact
        ? "VERIFIED_DIRECT"
        : indirectPathway
          ? "INDIRECT_INSTITUTIONAL"
          : "NONE",
      dualOrgRoles: dual,
      note: "Legal-entity-only result is partial — not full research success",
    };
  }

  return {
    tier: OWNERSHIP_SUCCESS_TIER.PARTIAL_REPRESENTATIVE_PATHWAY,
    fullResearchSuccess: false,
    partial: true,
    unresolvedUltimateOwnership: !ultimateVerified,
    provisionalStaged: false,
    researchSuccess: false,
    evidenceVerification: "PARTIAL",
    customerPublication: "BLOCKED",
    contactChannelClass: directContact
      ? "VERIFIED_DIRECT"
      : indirectPathway
        ? "INDIRECT_INSTITUTIONAL"
        : "NONE",
    dualOrgRoles: dual,
    note: "Partial ownership intelligence; actionable evidence bar not met",
  };
}

/**
 * Build classifier input from verified claim envelopes (benchmark path).
 * Labels alone are insufficient — each fact needs URL + verification + linkage flags.
 */
export function buildClassifierInputFromClaims(claims = [], opts = {}) {
  const EVIDENCE_LAYER = {
    DISCOVERED_CLAIM: "DISCOVERED_CLAIM",
    SUPPORTING_SOURCE: "SUPPORTING_SOURCE",
    INDEPENDENT_VERIFICATION: "INDEPENDENT_VERIFICATION",
    ACCEPTANCE_DECISION: "ACCEPTANCE_DECISION",
  };

  const accepted = (claims || []).filter(
    (c) => c?.[EVIDENCE_LAYER.ACCEPTANCE_DECISION] === "ACCEPTED"
  );

  function pick(types) {
    const set = new Set(types);
    return accepted.find((c) =>
      set.has(c?.[EVIDENCE_LAYER.DISCOVERED_CLAIM]?.claimType)
    );
  }

  function nodeFromClaim(c, extra = {}) {
    if (!c) return null;
    const url = c[EVIDENCE_LAYER.SUPPORTING_SOURCE]?.url || null;
    const verified = Boolean(c[EVIDENCE_LAYER.INDEPENDENT_VERIFICATION]?.ok && url);
    return {
      verified,
      name: c[EVIDENCE_LAYER.DISCOVERED_CLAIM]?.value || null,
      evidenceUrl: url,
      inferred: Boolean(c.inferredContact),
      unsupported: c[EVIDENCE_LAYER.ACCEPTANCE_DECISION] !== "ACCEPTED",
      linkedToHpc: c.linkedToHpc === true || opts.linkedToHpc === true,
      ...extra,
    };
  }

  const ultimateClaim = pick([
    "ULTIMATE_OWNER",
    "CONTROLLING_INVESTOR",
    "ECONOMIC_SPONSOR",
  ]);
  const legalClaim = pick(["LEGAL_TITLEHOLDER", "OWNER_ENTITY"]);
  const personClaim = pick(["OWNERSHIP_DECISION_MAKER", "OWNERSHIP_REPRESENTATIVE", "DECISION_MAKER"]);
  const emailClaim = pick(["PUBLIC_EMAIL", "PUBLIC_PHONE"]);
  const altClaim = pick(["ALTERNATIVE_CONTACT_PATHWAY"]);
  const structureClaim = pick(["OWNERSHIP_STRUCTURE_EDGE", "ORG_RELATIONSHIP"]);

  const personMeta = personClaim || null;
  const roleRelevant =
    personMeta?.roleRelevant === true ||
    personMeta?.[EVIDENCE_LAYER.DISCOVERED_CLAIM]?.claimType === "OWNERSHIP_DECISION_MAKER" ||
    personMeta?.[EVIDENCE_LAYER.DISCOVERED_CLAIM]?.claimType === "OWNERSHIP_REPRESENTATIVE";

  return {
    ultimateOwner: nodeFromClaim(ultimateClaim),
    legalTitleholder: nodeFromClaim(legalClaim),
    ownershipPerson: nodeFromClaim(personClaim, {
      roleRelevant,
      connectedToOwnerOrRepresentative:
        personMeta?.connectedToOwnerOrRepresentative === true,
      unrelatedHotelSales: personMeta?.unrelatedHotelSales === true,
      genericOperatorContact: personMeta?.genericOperatorContact === true,
    }),
    contact: {
      directVerified: Boolean(
        emailClaim &&
          emailClaim.inferredContact !== true &&
          emailClaim[EVIDENCE_LAYER.INDEPENDENT_VERIFICATION]?.ok &&
          emailClaim[EVIDENCE_LAYER.SUPPORTING_SOURCE]?.url
      ),
      indirectInstitutionalPathway: Boolean(
        altClaim &&
          altClaim[EVIDENCE_LAYER.INDEPENDENT_VERIFICATION]?.ok &&
          altClaim[EVIDENCE_LAYER.SUPPORTING_SOURCE]?.url
      ),
      inferred: Boolean(emailClaim?.inferredContact),
      evidenceUrl:
        emailClaim?.[EVIDENCE_LAYER.SUPPORTING_SOURCE]?.url ||
        altClaim?.[EVIDENCE_LAYER.SUPPORTING_SOURCE]?.url ||
        null,
      connectedToPersonOrOwner:
        emailClaim?.connectedToPersonOrOwner === true ||
        altClaim?.connectedToPersonOrOwner === true ||
        false,
    },
    structureChain: structureClaim
      ? {
          ...nodeFromClaim(structureClaim),
          relevant: opts.structureRelevant !== false,
        }
      : { verified: false, evidenceUrl: null, relevant: opts.structureRelevant === true },
    orgRoles: opts.orgRoles || [],
    stagedOnly: opts.stagedOnly === true,
    ultimateOwnerUnresolved: opts.ultimateOwnerUnresolved === true,
    ownershipInferredFromManagementOrBrandAlone:
      opts.ownershipInferredFromManagementOrBrandAlone === true,
  };
}

/** @deprecated Use ownershipInferredFromOperatorOrBrandAlone */
export function isBrandOrOperatorMistakenForOwner(claimType, roleHint) {
  const t = String(claimType || "");
  const r = String(roleHint || "");
  if (t === "OPERATOR_ENTITY" || t === "BRAND") return true;
  if (r === OWNERSHIP_ROLE.HOTEL_MANAGEMENT_OPERATOR) return true;
  if (r === OWNERSHIP_ROLE.BRAND_OR_FRANCHISE) return true;
  return false;
}

export function scoreUltimateOwnershipDimension(inputOrTypes = new Set()) {
  // Back-compat: Set of types → coarse score; prefer evidence input when object
  if (inputOrTypes && typeof inputOrTypes === "object" && !Array.isArray(inputOrTypes) && !(inputOrTypes instanceof Set)) {
    const outcome = classifyOwnershipResearchSuccess(inputOrTypes);
    if (outcome.tier === OWNERSHIP_SUCCESS_TIER.FULL_ACTIONABLE) return 3;
    if (outcome.tier === OWNERSHIP_SUCCESS_TIER.PARTIAL_OWNER_WITHOUT_CONTACT) return 2;
    if (outcome.tier === OWNERSHIP_SUCCESS_TIER.PARTIAL_LEGAL_ENTITY_ONLY) return 1;
    if (evidenceOk(inputOrTypes.ultimateOwner)) return 2;
    return 0;
  }
  const types = inputOrTypes instanceof Set ? inputOrTypes : new Set(inputOrTypes || []);
  if (types.has(OWNERSHIP_CLAIM_TYPES.ULTIMATE_OWNER) || types.has(OWNERSHIP_CLAIM_TYPES.CONTROLLING_INVESTOR)) {
    return 3; // label hint only — full success still requires evidence classifier
  }
  if (types.has(OWNERSHIP_CLAIM_TYPES.ECONOMIC_SPONSOR)) return 2;
  if (
    types.has(OWNERSHIP_CLAIM_TYPES.LEGAL_TITLEHOLDER) ||
    types.has(OWNERSHIP_CLAIM_TYPES.OWNER_ENTITY)
  ) {
    return 1;
  }
  return 0;
}

/**
 * Build classifier input from HE staged package (projection path).
 * Populated arrays without verification → stagedOnly provisional.
 */
export function buildClassifierInputFromStaged(staged = {}) {
  const own = staged.ownership || {};
  const ultimate = own.ultimate_owner || own.controlling_investor || own.economic_sponsor || null;
  const legal = own.legal_titleholder || own.owner_entity || null;
  const person =
    own.primary_ownership_contact ||
    own.ownership_representative ||
    (Array.isArray(staged.persons) ? staged.persons[0] : null);
  const channels =
    own.primary_contact_channels ||
    person?.channels ||
    staged.organization_contact_route?.channels ||
    [];
  const channel0 = Array.isArray(channels) ? channels[0] : null;

  const ultimateVerified = Boolean(
    ultimate?.verified === true &&
      (ultimate?.evidence_url || ultimate?.evidenceUrl) &&
      ultimate?.linked_to_hpc !== false
  );
  const legalVerified = Boolean(
    legal?.verified === true && (legal?.evidence_url || legal?.evidenceUrl)
  );
  const personVerified = Boolean(
    person?.verified === true &&
      (person?.evidence_url || person?.evidenceUrl) &&
      person?.role_relevant === true &&
      person?.connected_to_owner_or_representative === true
  );

  const anyStagedName = Boolean(
    ultimate?.legal_name || ultimate?.name || legal?.legal_name || person?.name || person?.full_name
  );
  const stagedOnly =
    own.staged_only === true ||
    (anyStagedName && !ultimateVerified && !personVerified && own.force_verified !== true);

  return {
    ultimateOwner: ultimate
      ? {
          verified: ultimateVerified,
          name: ultimate.legal_name || ultimate.name || null,
          evidenceUrl: ultimate.evidence_url || ultimate.evidenceUrl || null,
          linkedToHpc: ultimate.linked_to_hpc !== false,
          inferred: ultimate.inferred === true,
          unsupported: ultimate.verified !== true,
        }
      : null,
    legalTitleholder: legal
      ? {
          verified: legalVerified,
          name: legal.legal_name || legal.name || null,
          evidenceUrl: legal.evidence_url || legal.evidenceUrl || null,
          linkedToHpc: legal.linked_to_hpc !== false,
          inferred: false,
          unsupported: legal.verified !== true,
        }
      : null,
    ownershipPerson: person
      ? {
          verified: personVerified,
          name: person.full_name || person.name || null,
          evidenceUrl: person.evidence_url || person.evidenceUrl || null,
          roleRelevant: person.role_relevant === true,
          connectedToOwnerOrRepresentative:
            person.connected_to_owner_or_representative === true,
          unrelatedHotelSales: person.unrelated_hotel_sales === true,
          genericOperatorContact: person.generic_operator_contact === true,
          inferred: person.inferred === true,
        }
      : null,
    contact: {
      directVerified: Boolean(
        channel0?.verified === true &&
          channel0?.inferred !== true &&
          (channel0?.kind === "email" ||
            channel0?.kind === "phone" ||
            channel0?.type === "email" ||
            channel0?.type === "phone") &&
          (channel0?.evidence_url || channel0?.evidenceUrl || channel0?.source_url)
      ),
      indirectInstitutionalPathway: Boolean(
        channel0?.pathway === "institutional" &&
          channel0?.verified === true &&
          channel0?.inferred !== true
      ),
      inferred: channel0?.inferred === true,
      evidenceUrl:
        channel0?.evidence_url || channel0?.evidenceUrl || channel0?.source_url || null,
      connectedToPersonOrOwner: channel0?.connected_to_person_or_owner === true,
    },
    structureChain: {
      verified: own.structure_chain?.verified === true,
      evidenceUrl: own.structure_chain?.evidence_url || null,
      relevant: own.structure_chain?.relevant !== false,
    },
    orgRoles: own.org_roles || [],
    stagedOnly,
    ultimateOwnerUnresolved:
      own.ultimate_owner_status === "UNRESOLVED" || (!ultimateVerified && !ultimate),
    ownershipInferredFromManagementOrBrandAlone:
      own.ownership_inferred_from_management_or_brand_alone === true,
  };
}

export function writeOwnershipObjectiveGuarantees() {
  return {
    objective_id: OWNERSHIP_RESEARCH_OBJECTIVE_ID,
    objective_version: OWNERSHIP_RESEARCH_OBJECTIVE_VERSION,
    drawer_questions: OWNERSHIP_DRAWER_QUESTIONS,
    primary_goals: PRIMARY_OWNERSHIP_GOALS,
    legal_entity_only_is_partial: true,
    full_actionable_requires_evidence: true,
    never_invent: true,
    brand_or_operator_not_inferred_as_owner: true,
    dual_roles_require_independent_verification: true,
    research_success_evidence_publication_are_separate_gates: true,
  };
}
