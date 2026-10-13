/**
 * Packet 2.5A — Role lexicon: source language → allowed / forbidden claim types.
 * Ambiguous terms must not auto-map to OWNED_BY.
 */

export const ROLE_LEXICON_VERSION = "claim-role-lexicon-v1";

/** @type {Record<string, { allowed: string[], forbidden: string[], ambiguity?: string }>} */
export const ROLE_LEXICON = Object.freeze({
  owns: {
    allowed: ["ENTITY_IS_OWNER_OF_PROPERTY", "ENTITY_OWNS_PERCENT_OF_ENTITY"],
    forbidden: [],
  },
  "owned by": {
    allowed: ["ENTITY_IS_OWNER_OF_PROPERTY"],
    forbidden: [],
  },
  holds: {
    allowed: ["ENTITY_HOLDS_TITLE", "ENTITY_OWNS_PERCENT_OF_ENTITY"],
    forbidden: [],
    ambiguity: "holds_may_be_title_or_stake",
  },
  "controlled by": {
    allowed: ["ENTITY_CONTROLS_ENTITY"],
    forbidden: ["ENTITY_IS_OWNER_OF_PROPERTY"],
  },
  subsidiary: {
    allowed: ["ENTITY_IS_SUBSIDIARY_OF_ENTITY"],
    forbidden: ["ENTITY_IS_OWNER_OF_PROPERTY"],
  },
  affiliate: {
    allowed: [],
    forbidden: ["ENTITY_IS_OWNER_OF_PROPERTY", "ENTITY_CONTROLS_ENTITY"],
    ambiguity: "affiliate_observation_only",
  },
  participation: {
    allowed: ["ENTITY_PARTICIPATES_IN_JV", "ENTITY_OWNS_PERCENT_OF_ENTITY"],
    forbidden: ["ENTITY_IS_OWNER_OF_PROPERTY"],
    ambiguity: "participation_not_sole_owner",
  },
  interest: {
    allowed: ["ENTITY_OWNS_PERCENT_OF_ENTITY", "ENTITY_PARTICIPATES_IN_JV"],
    forbidden: ["ENTITY_IS_OWNER_OF_PROPERTY"],
  },
  invested: {
    allowed: ["ENTITY_SPONSORED_PROJECT"],
    forbidden: ["ENTITY_IS_OWNER_OF_PROPERTY"],
    ambiguity: "invested_not_owned",
  },
  "invested in": {
    allowed: ["ENTITY_SPONSORED_PROJECT"],
    forbidden: ["ENTITY_IS_OWNER_OF_PROPERTY"],
  },
  acquired: {
    allowed: ["ENTITY_ACQUIRED_ENTITY_OR_ASSET", "ENTITY_OWNS_PERCENT_OF_ENTITY"],
    forbidden: [],
  },
  sold: {
    allowed: ["ENTITY_SOLD_ENTITY_OR_ASSET"],
    forbidden: [],
  },
  developed: {
    allowed: ["ENTITY_DEVELOPED_PROPERTY"],
    forbidden: ["ENTITY_IS_OWNER_OF_PROPERTY"],
  },
  sponsored: {
    allowed: ["ENTITY_SPONSORED_PROJECT"],
    forbidden: ["ENTITY_IS_OWNER_OF_PROPERTY"],
  },
  operates: {
    allowed: ["ENTITY_OPERATES_HOTEL"],
    forbidden: ["ENTITY_IS_OWNER_OF_PROPERTY"],
  },
  "operated by": {
    allowed: ["ENTITY_OPERATES_HOTEL"],
    forbidden: ["ENTITY_IS_OWNER_OF_PROPERTY"],
  },
  "managed by": {
    allowed: ["ENTITY_OPERATES_HOTEL", "ENTITY_MANAGES_HOTEL"],
    forbidden: ["ENTITY_IS_OWNER_OF_PROPERTY"],
  },
  brand: {
    allowed: ["HOTEL_CURRENT_BRAND", "ENTITY_BRANDS_HOTEL"],
    forbidden: ["ENTITY_IS_OWNER_OF_PROPERTY"],
  },
  franchise: {
    allowed: ["ENTITY_BRANDS_HOTEL", "HOTEL_CURRENT_BRAND"],
    forbidden: ["ENTITY_IS_OWNER_OF_PROPERTY"],
  },
  leased: {
    allowed: ["ENTITY_LEASES_PROPERTY"],
    forbidden: ["ENTITY_IS_OWNER_OF_PROPERTY"],
  },
  borrower: {
    allowed: ["ENTITY_IS_BORROWER"],
    forbidden: ["ENTITY_IS_OWNER_OF_PROPERTY"],
  },
  lender: {
    allowed: ["ENTITY_FINANCED_PROPERTY"],
    forbidden: ["ENTITY_IS_OWNER_OF_PROPERTY"],
  },
  "joint venture": {
    allowed: ["ENTITY_PARTICIPATES_IN_JV"],
    forbidden: ["ENTITY_IS_OWNER_OF_PROPERTY"],
  },
  partner: {
    allowed: ["ENTITY_PARTICIPATES_IN_JV"],
    forbidden: ["ENTITY_IS_OWNER_OF_PROPERTY"],
    ambiguity: "partner_ambiguous",
  },
  propietario: {
    allowed: ["ENTITY_IS_OWNER_OF_PROPERTY", "ENTITY_HOLDS_TITLE"],
    forbidden: [],
  },
  propiedad: {
    allowed: ["ENTITY_IS_OWNER_OF_PROPERTY", "ENTITY_HOLDS_TITLE"],
    forbidden: [],
  },
});

/** Explicit ownership / title language required for OWNER claims. */
export const OWNERSHIP_PREDICATE_RE =
  /\b(own(?:s|ed|ership)|owned\s+by|propiedad(?:\s+de)?|propietari[oa]|dueño|title\s+holder|entidad\s+propietaria|holds?\s+title|inmueble)\b/i;

export const OPERATOR_PREDICATE_RE =
  /\b(operat(?:e|es|ed|ing|or)|managed\s+by|manages|operado|administrado|gesti[oó]n|management)\b/i;

export const DEVELOPER_PREDICATE_RE =
  /\b(develop(?:ed|er|ment)|desarroll(?:o|ado|ador)|promotor)\b/i;

export const SPONSOR_PREDICATE_RE =
  /\b(sponsor(?:ed)?|backed\s+by|invest(?:ed|ment)|filiales\s+de|affiliates?\s+of)\b/i;

export const JV_PREDICATE_RE =
  /\b(joint\s+venture|\bjv\b|fideicomiso|co-invest|participaci[oó]n|partnership)\b/i;

export const BRAND_PREDICATE_RE =
  /\b(brand|branded|franchise|current\s+brand|trading\s+name|formerly\s+a|is\s+now\s+the)\b/i;

export const TITLE_PREDICATE_RE =
  /\b(director(?:a)?\s+general|ceo|cfo|directora?\s+de\s+ventas|chief\s+executive|title)\b/i;

export const SIGNING_AUTHORITY_PREDICATE_RE =
  /\b(signed|executed|authorized\s+to\s+sign|signing\s+authority|apoderado|poder\s+notarial)\b/i;

export const MODALITY_RE =
  /\b(plans?\s+to|expects?\s+to|intends?\s+to|proposed|may|could|potential|under\s+consideration|scheduled|announced|subject\s+to|set\s+to\s+open|abrirá|apertura)\b/i;

export const NEGATION_RE =
  /\b(not\s+owned\s+by|no\s+longer\s+(?:owns|operates|managed)|was\s+not\s+involved|did\s+not\s+acquire|formerly\s+managed|agreement\s+terminated|do\s+not\s+(?:assert|infer|attach)|operator\s+is\s+not\s+evidence\s+of\s+ownership|not\s+sole\s+ownership)\b/i;

export const TEMPORAL_FORMER_RE =
  /\b(former(?:ly)?|previously|until|antes|ex-|historical|sold|divested|ya\s+no)\b/i;

export const TEMPORAL_CURRENT_RE =
  /\b(current(?:ly)?|as\s+of|since|actualmente|hoy|is\s+owned|is\s+operated)\b/i;

export const TEMPORAL_ANNOUNCED_RE =
  /\b(announced|will\s+become|expected\s+to\s+open|set\s+to\s+open|abrirá|planned|upcoming)\b/i;

/**
 * Claim-type semantic preconditions (local span must satisfy).
 */
export const CLAIM_TYPE_PRECONDITIONS = Object.freeze({
  ENTITY_IS_OWNER_OF_PROPERTY: {
    must: OWNERSHIP_PREDICATE_RE,
    must_not: /\b(operated|managed|operado|administrado|develop|sponsor|brand|borrower|invested\s+in)\b/i,
  },
  ENTITY_HOLDS_TITLE: {
    must: /\b(title|propietaria|inmueble|holds?\s+title|entidad\s+propietaria|propiedad)\b/i,
  },
  ENTITY_OPERATES_HOTEL: {
    must: OPERATOR_PREDICATE_RE,
  },
  ENTITY_MANAGES_HOTEL: {
    must: OPERATOR_PREDICATE_RE,
  },
  ENTITY_DEVELOPED_PROPERTY: {
    must: DEVELOPER_PREDICATE_RE,
  },
  ENTITY_SPONSORED_PROJECT: {
    must: SPONSOR_PREDICATE_RE,
  },
  ENTITY_PARTICIPATES_IN_JV: {
    must: JV_PREDICATE_RE,
  },
  ENTITY_OWNS_PERCENT_OF_ENTITY: {
    must: /\d{1,3}\s*%/,
  },
  PERSON_HAS_SIGNING_AUTHORITY: {
    must: SIGNING_AUTHORITY_PREDICATE_RE,
  },
  PERSON_HAS_TITLE: {
    must: TITLE_PREDICATE_RE,
  },
  HOTEL_CURRENT_BRAND: {
    must: BRAND_PREDICATE_RE,
  },
  HOTEL_ANNOUNCED_BRAND: {
    must: TEMPORAL_ANNOUNCED_RE,
  },
});
