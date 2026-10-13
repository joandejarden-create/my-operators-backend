/**
 * Legal-entity staging — promote beyond UNRESOLVED without claiming PROPERTY_OWNER.
 * Research/staging states only; never writes canonical HPC ownership.
 */
import {
  OWNERSHIP_CONCLUSION_STATE,
  ENTITY_CANDIDATE_ROLE,
  PROPERTY_ENTITY_MATCH,
  NON_OWNER_WITHOUT_INDEPENDENT_EVIDENCE,
} from "./constants.js";
import { extractLegalEntityBundle } from "./legal-entity-extract.js";
import {
  classifyPropertyEntityMatch,
  buildExactAddressContinuationQueries,
} from "./property-entity-identity-match.js";

/**
 * Stage a legal-entity research conclusion from extracted evidence + identity match.
 *
 * @param {{
 *   hotel: object,
 *   text?: string,
 *   source_url?: string,
 *   bundle?: object,
 *   independent_ownership_evidence?: boolean,
 * }} args
 */
export function stageLegalEntityFromEvidence(args = {}) {
  const hotel = args.hotel || {};
  const text = String(args.text || "");
  const bundle = args.bundle || extractLegalEntityBundle(text, { source_url: args.source_url });
  const entityForMatch = {
    legal_name: bundle.legal_name,
    trade_name: bundle.trade_name,
    text: `${text} ${bundle.legal_name || ""}`,
    address: null,
  };
  const identity = classifyPropertyEntityMatch(hotel, entityForMatch);

  const principals = (bundle.principals || []).map((p) => ({
    ...p,
    entity_name: bundle.legal_name || null,
    cnpj: bundle.primary_cnpj?.cnpj_digits || null,
    source_url: args.source_url || bundle.source_url || null,
    relationship: "PERSON_TO_LEGAL_ENTITY",
    not_automatic_property_owner: true,
  }));

  const base = {
    version: "brazil-v1.1-legal-entity-staging",
    entity_name: bundle.legal_name || bundle.trade_name || null,
    cnpj: bundle.primary_cnpj?.cnpj_digits || null,
    cnpj_formatted: bundle.primary_cnpj?.cnpj_formatted || null,
    entity_status: bundle.company_status || (bundle.inactive ? "BAIXADA" : null),
    inactive: Boolean(bundle.inactive),
    roles: bundle.roles || [],
    evidence_urls: [args.source_url || bundle.source_url].filter(Boolean),
    identity_match: identity.match,
    identity_match_evidence: identity,
    confidence: null,
    principals,
    titled_property_ownership_claimed: false,
    property_owner_resolved: false,
    unresolved_ownership_question:
      "Who holds titled / economic property ownership for this exact hotel?",
    address_continuation_queries: [],
  };

  // Wrong-city / conflicting location — ambiguous, do not claim resolved legal entity for target
  if (
    identity.match === PROPERTY_ENTITY_MATCH.CONFLICTING_LOCATION ||
    identity.match === PROPERTY_ENTITY_MATCH.WRONG_PROPERTY
  ) {
    return {
      ...base,
      staging_conclusion: OWNERSHIP_CONCLUSION_STATE.LEGAL_ENTITY_AMBIGUOUS,
      legal_entity_resolved_for_target: false,
      address_continuation_queries: buildExactAddressContinuationQueries(hotel),
      confidence: "LOW",
      note: "Same-name entity with conflicting location — exact-address continuation required",
    };
  }

  // Name-only without address/city corroboration — keep ambiguous unless strong registry+city
  if (
    identity.match === PROPERTY_ENTITY_MATCH.NAME_ONLY_MATCH &&
    !bundle.hotel_cnae_signal
  ) {
    return {
      ...base,
      staging_conclusion: OWNERSHIP_CONCLUSION_STATE.LEGAL_ENTITY_AMBIGUOUS,
      legal_entity_resolved_for_target: false,
      address_continuation_queries: buildExactAddressContinuationQueries(hotel),
      confidence: "LOW",
      note: "Name-only match — require city/address corroboration",
    };
  }

  if (!bundle.legal_name && !bundle.primary_cnpj) {
    // FII lead without full legal entity
    if ((bundle.roles || []).includes(ENTITY_CANDIDATE_ROLE.REAL_ESTATE_FUND) || /\bFII\b/.test(text)) {
      return {
        ...base,
        staging_conclusion: OWNERSHIP_CONCLUSION_STATE.SUPPORTED_REAL_ESTATE_FUND_LEAD,
        legal_entity_resolved_for_target: false,
        confidence: "PROBABLE",
        note: "FII/investment lead from evidence — not titled property owner",
        fii_lead: true,
      };
    }
    return {
      ...base,
      staging_conclusion: OWNERSHIP_CONCLUSION_STATE.UNRESOLVED,
      legal_entity_resolved_for_target: false,
      confidence: null,
    };
  }

  const roles = bundle.roles || [];
  const isOp = roles.some((r) => NON_OWNER_WITHOUT_INDEPENDENT_EVIDENCE.includes(r));
  const isFund = roles.includes(ENTITY_CANDIDATE_ROLE.REAL_ESTATE_FUND);
  const strongIdentity =
    identity.match === PROPERTY_ENTITY_MATCH.EXACT_PROPERTY_MATCH ||
    identity.match === PROPERTY_ENTITY_MATCH.STRONG_PROPERTY_MATCH ||
    (identity.match === PROPERTY_ENTITY_MATCH.PARTIAL_MATCH &&
      Boolean(bundle.primary_cnpj) &&
      Boolean(bundle.legal_name));

  if (!strongIdentity && identity.match === PROPERTY_ENTITY_MATCH.PARTIAL_MATCH) {
    return {
      ...base,
      staging_conclusion: OWNERSHIP_CONCLUSION_STATE.LEGAL_ENTITY_AMBIGUOUS,
      legal_entity_resolved_for_target: false,
      address_continuation_queries: buildExactAddressContinuationQueries(hotel),
      confidence: "LOW",
    };
  }

  if (args.independent_ownership_evidence === true && !isOp) {
    return {
      ...base,
      staging_conclusion:
        OWNERSHIP_CONCLUSION_STATE.STRONG_PROPERTY_OWNER_CANDIDATE_NEEDS_CONFIRMATION,
      legal_entity_resolved_for_target: true,
      roles: [...new Set([...roles, ENTITY_CANDIDATE_ROLE.PROPERTY_OWNER])],
      confidence: "PROBABLE",
      property_owner_resolved: false,
      note: "Strong owner candidate — still requires confirmation before canonical write",
    };
  }

  if (isFund) {
    return {
      ...base,
      staging_conclusion: OWNERSHIP_CONCLUSION_STATE.SUPPORTED_REAL_ESTATE_FUND_LEAD,
      legal_entity_resolved_for_target: true,
      confidence: "PROBABLE",
      note: "Real-estate fund / FII lead — not automatic titled owner",
      fii_lead: true,
    };
  }

  if (bundle.inactive) {
    return {
      ...base,
      staging_conclusion: isOp
        ? OWNERSHIP_CONCLUSION_STATE.SUPPORTED_HISTORICAL_OPERATING_ENTITY
        : OWNERSHIP_CONCLUSION_STATE.SUPPORTED_HISTORICAL_OWNER,
      legal_entity_resolved_for_target: true,
      confidence: "PROBABLE",
      note: "Inactive/BAIXADA entity — lineage / successor research required",
      successor_research_required: true,
    };
  }

  if (isOp) {
    const admin = roles.includes(ENTITY_CANDIDATE_ROLE.MANAGEMENT_COMPANY);
    return {
      ...base,
      staging_conclusion: admin
        ? OWNERSHIP_CONCLUSION_STATE.SUPPORTED_CURRENT_OPERATING_ENTITY
        : OWNERSHIP_CONCLUSION_STATE.SUPPORTED_CURRENT_OPERATING_ENTITY,
      legal_entity_resolved_for_target: true,
      confidence: "HIGH",
      note: "Operating/management entity staged — PROPERTY_OWNER remains unconfirmed",
    };
  }

  if (bundle.legal_name && bundle.primary_cnpj && strongIdentity) {
    return {
      ...base,
      staging_conclusion: OWNERSHIP_CONCLUSION_STATE.LEGAL_ENTITY_IDENTIFIED_OWNER_UNCONFIRMED,
      legal_entity_resolved_for_target: true,
      confidence: "HIGH",
      note: "Legal entity identified for property — titled ownership unconfirmed",
    };
  }

  if (bundle.legal_name || bundle.primary_cnpj) {
    return {
      ...base,
      staging_conclusion: OWNERSHIP_CONCLUSION_STATE.LEGAL_ENTITY_IDENTIFIED_OWNER_UNCONFIRMED,
      legal_entity_resolved_for_target: strongIdentity,
      confidence: "PROBABLE",
    };
  }

  return {
    ...base,
    staging_conclusion: OWNERSHIP_CONCLUSION_STATE.UNRESOLVED,
    legal_entity_resolved_for_target: false,
  };
}

/**
 * Apply staged conclusion onto an ownership object (research staging only).
 * Never sets classification=PROPERTY_OWNER without independent evidence.
 */
export function applyStagedConclusionToOwnership(ownership = {}, staged = {}) {
  const out = { ...ownership };
  const conclusion = staged.staging_conclusion || OWNERSHIP_CONCLUSION_STATE.UNRESOLVED;

  out.research_staging_conclusion = conclusion;
  out.classification =
    conclusion === OWNERSHIP_CONCLUSION_STATE.UNRESOLVED
      ? out.classification || "UNRESOLVED"
      : conclusion;
  out.legal_entity_name = staged.entity_name || out.legal_entity_name || null;
  out.legal_entity_cnpj = staged.cnpj || out.legal_entity_cnpj || null;
  out.entity_role = (staged.roles || [])[0] || out.entity_role || null;
  out.entity_roles = staged.roles || out.entity_roles || [];
  out.entity_status = staged.entity_status || out.entity_status || null;
  out.identity_match = staged.identity_match || null;
  out.principals = staged.principals?.length ? staged.principals : out.principals || [];
  out.property_owner_resolved = false;
  out.titled_property_ownership_claimed = false;
  out.evidence_note =
    staged.note ||
    out.evidence_note ||
    (conclusion !== OWNERSHIP_CONCLUSION_STATE.UNRESOLVED
      ? `Staged research conclusion: ${conclusion}`
      : out.evidence_note);
  out.evidence_refs = [
    ...new Set([...(out.evidence_refs || []), ...(staged.evidence_urls || [])]),
  ];
  out.unresolved_ownership_question = staged.unresolved_ownership_question;
  out.address_continuation_queries = staged.address_continuation_queries || [];
  out.fii_lead = staged.fii_lead === true;
  out.confidence = staged.confidence || out.confidence;

  // Display name for operating/legal entity — not property owner claim
  if (
    staged.entity_name &&
    [
      OWNERSHIP_CONCLUSION_STATE.LEGAL_ENTITY_IDENTIFIED_OWNER_UNCONFIRMED,
      OWNERSHIP_CONCLUSION_STATE.SUPPORTED_CURRENT_OPERATING_ENTITY,
      OWNERSHIP_CONCLUSION_STATE.SUPPORTED_CURRENT_OPERATOR,
      OWNERSHIP_CONCLUSION_STATE.SUPPORTED_HISTORICAL_OPERATING_ENTITY,
      OWNERSHIP_CONCLUSION_STATE.SUPPORTED_REAL_ESTATE_FUND_LEAD,
    ].includes(conclusion)
  ) {
    out.owner_display_name = null; // do not misuse owner field
    out.operating_entity_name = staged.entity_name;
    out.legal_entity_display_name = staged.entity_name;
  }

  if (conclusion === OWNERSHIP_CONCLUSION_STATE.LEGAL_ENTITY_AMBIGUOUS) {
    out.owner_display_name = null;
    out.operating_entity_name = null;
    out.ambiguous_entity_candidate = {
      name: staged.entity_name,
      cnpj: staged.cnpj,
      identity_match: staged.identity_match,
    };
  }

  return out;
}
