/**
 * Claim-typed ownership retrieval — claim IDs and open-claim inference.
 * Does not change verification ontology; feeds evidence into existing controllers.
 */

export const CLAIM_SOURCE_ROUTER_VERSION = "claim-source-router-v1";

/** Claims the NOW-stage router must route. */
export const OWNERSHIP_CLAIM = Object.freeze({
  LEGAL_ENTITY: "LEGAL_ENTITY",
  QSA_PRINCIPALS: "QSA_PRINCIPALS",
  OPERATING_ENTITY: "OPERATING_ENTITY",
  PROPERTY_OWNER: "PROPERTY_OWNER",
  ECONOMIC_SPONSOR: "ECONOMIC_SPONSOR",
  FII_PROPERTY_RELATIONSHIP: "FII_PROPERTY_RELATIONSHIP",
  CURRENT_OWNER_AFTER_TRANSACTION: "CURRENT_OWNER_AFTER_TRANSACTION",
  PROPERTY_TITLE: "PROPERTY_TITLE",
  ENTITY_RELATIONSHIP: "ENTITY_RELATIONSHIP",
});

export const CLAIM_STATUS = Object.freeze({
  OPEN: "OPEN",
  PARTIAL: "PARTIAL",
  RESOLVED: "RESOLVED",
  BLOCKED: "BLOCKED",
});

/** Source strategy roles — discovery finds leads; claim-resolution supports verification. */
export const SOURCE_STRATEGY_ROLE = Object.freeze({
  DISCOVERY: "DISCOVERY",
  CLAIM_RESOLUTION: "CLAIM_RESOLUTION",
  FALLBACK: "FALLBACK",
});

export const SOURCE_FAMILY = Object.freeze({
  CADASTUR: "CADASTUR",
  CNPJ_QSA_DIRECT: "CNPJ_QSA_DIRECT",
  CVM_FII_DIRECT: "CVM_FII_DIRECT",
  CORPORATE_PRIMARY: "CORPORATE_PRIMARY",
  REGISTRY_DERIVED: "REGISTRY_DERIVED",
  TRANSACTION_NEWS: "TRANSACTION_NEWS",
  TITLE_REGISTRY: "TITLE_REGISTRY",
  CLAIM_SCOPED_AGENT: "CLAIM_SCOPED_AGENT",
  CONTEXT_DEV_SEARCH: "CONTEXT_DEV_SEARCH",
  CONTEXT_DEV_SCRAPE: "CONTEXT_DEV_SCRAPE",
  SERP_DISCOVERY: "SERP_DISCOVERY",
});

/**
 * Infer open claims from hotel + existing evidence (blind — no expected answers).
 * @param {object} hotel
 * @param {object} [evidence]
 */
export function inferOpenClaims(hotel = {}, evidence = {}) {
  const country = String(hotel.country || "").toLowerCase();
  const isBr = country === "brazil" || country === "brasil" || country === "br";
  const open = [];

  const hasLegal = Boolean(
    evidence.primary_cnpj || evidence.legal_entity_name || evidence.legal_entity_resolved
  );
  const hasQsa = Array.isArray(evidence.qsa) && evidence.qsa.length > 0;
  const hasOwner = Boolean(evidence.supported_property_owner || evidence.supported_owner_spv);
  const hasSponsor = Boolean(evidence.supported_economic_sponsor);
  const hasFundRel = Boolean(evidence.supported_fii_property_relationship);
  const brandHint = `${hotel.hotel_name || ""} ${hotel.brand || ""} ${hotel.parent_company || ""}`;
  const likelyFund =
    isBr &&
    /\bibis\b|novotel|mercure|tryp|wyndham|meli[aá]|caesar|marriott|hilton|accor/i.test(brandHint);

  if (!hasLegal) {
    open.push({
      claim: OWNERSHIP_CLAIM.LEGAL_ENTITY,
      status: CLAIM_STATUS.OPEN,
      priority: 10,
      reason: "No resolved target-property legal entity",
    });
  }
  if (hasLegal && !hasQsa && isBr) {
    open.push({
      claim: OWNERSHIP_CLAIM.QSA_PRINCIPALS,
      status: CLAIM_STATUS.OPEN,
      priority: 20,
      reason: "Legal entity known; QSA/principals missing",
    });
  }
  if (!evidence.operating_entity_supported) {
    open.push({
      claim: OWNERSHIP_CLAIM.OPERATING_ENTITY,
      status: CLAIM_STATUS.OPEN,
      priority: 25,
      reason: "Operating entity not yet supported",
    });
  }
  if (likelyFund && !hasFundRel) {
    open.push({
      claim: OWNERSHIP_CLAIM.FII_PROPERTY_RELATIONSHIP,
      status: CLAIM_STATUS.OPEN,
      priority: 30,
      reason: "Branded/institutional pattern — FII/fund relationship claim open",
    });
  }
  if (!hasOwner) {
    open.push({
      claim: OWNERSHIP_CLAIM.PROPERTY_OWNER,
      status: CLAIM_STATUS.OPEN,
      priority: 40,
      reason: "Property owner unsupported",
    });
  }
  if (!hasSponsor && !hasOwner) {
    open.push({
      claim: OWNERSHIP_CLAIM.ECONOMIC_SPONSOR,
      status: CLAIM_STATUS.OPEN,
      priority: 45,
      reason: "Economic sponsor unsupported",
    });
  }
  if (!evidence.property_title_supported) {
    open.push({
      claim: OWNERSHIP_CLAIM.PROPERTY_TITLE,
      status: CLAIM_STATUS.OPEN,
      priority: 50,
      reason: "Property title claim open",
    });
  }
  if (evidence.entity_relationship_unresolved) {
    open.push({
      claim: OWNERSHIP_CLAIM.ENTITY_RELATIONSHIP,
      status: CLAIM_STATUS.OPEN,
      priority: 35,
      reason: "Entity relationship unresolved",
    });
  }
  if (evidence.historical_transaction_without_current) {
    open.push({
      claim: OWNERSHIP_CLAIM.CURRENT_OWNER_AFTER_TRANSACTION,
      status: CLAIM_STATUS.OPEN,
      priority: 38,
      reason: "Historical transaction without current owner lock",
    });
  }

  return open.sort((a, b) => a.priority - b.priority);
}
