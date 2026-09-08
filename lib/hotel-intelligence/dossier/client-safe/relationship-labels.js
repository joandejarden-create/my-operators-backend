/**
 * Map internal relationship ontology codes → customer-safe labels.
 */

const LABEL_MAP = Object.freeze({
  SPONSORED_BY: "Sponsored / controlled by",
  OWNED_BY: "Owned by",
  OWNED_VIA_CBHL_PROBABLE: "Ownership via Cambridge Beaches Holdings Limited (probable)",
  OPERATED_BY: "Operated by",
  OPERATED_BY_DOVETAIL_PROBABLE: "Operated by Dovetail (probable)",
  OPERATED_BY_PYRAMID_2023: "Operated by Pyramid (announced 2023)",
  OPERATED_STATUS_CONTESTED: "Current operating arrangement unresolved",
  DEVELOPED_BY: "Developed by",
  COMMERCIAL_PARTNER_CHARLESTOWNE: "Commercial partner (Charlestowne Hotels)",
  HISTORICAL_OPERATOR: "Former / historical operator",
  CURRENT_STATUS_CONTESTED: "Current operating arrangement unresolved",
  FINANCED_BY: "Financed by",
  CONTROLLED_BY: "Controlled by",
  ASSET_MANAGED_BY: "Asset-managed by",
});

/** Ownership-chain role codes → external diligence labels (never raw snake_case). */
const OWNERSHIP_ROLE_LABELS = Object.freeze({
  hotel: "Hotel",
  propco: "Property company",
  property_company: "Property company",
  economic_owner: "Economic owner",
  sponsor: "Sponsor",
  sponsor_principals: "Sponsor / buyer principals",
  former_owner: "Former owner",
  ubo: "Ultimate beneficial owner",
  operator: "Operator",
  brand: "Brand",
  franchisor: "Franchisor",
  asset_manager: "Asset manager",
  lender: "Lender",
});

const STATUS_LABELS = Object.freeze({
  HIGH: "High",
  VERIFIED: "Verified",
  PROBABLE: "Probable",
  MEDIUM: "Medium",
  LOW: "Low",
  UNKNOWN: "Unverified",
  UNRESOLVED: "Unresolved",
  FORMER: "Former",
  CURRENT: "Current",
  ANNOUNCED: "Announced",
  CONFLICTED: "Conflicted",
});

export function customerRelationshipLabel(codeOrText) {
  const raw = String(codeOrText || "").trim();
  if (!raw) return "—";
  if (LABEL_MAP[raw]) return LABEL_MAP[raw];
  // Multi-code join
  if (/[_A-Z]{3,}/.test(raw) && /_/.test(raw)) {
    const parts = raw.split(/\s*[·|,;/]\s*/);
    return parts.map((p) => LABEL_MAP[p.trim()] || humanizeToken(p.trim())).join(" · ");
  }
  return raw;
}

function humanizeToken(tok) {
  if (LABEL_MAP[tok]) return LABEL_MAP[tok];
  return String(tok)
    .replace(/_/g, " ")
    .toLowerCase()
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

export function customerRelationshipLabels(listOrText) {
  if (Array.isArray(listOrText)) {
    return listOrText.map(customerRelationshipLabel).join(" · ");
  }
  return customerRelationshipLabel(listOrText);
}

export function customerOwnershipRoleLabel(roleOrText) {
  const raw = String(roleOrText || "").trim();
  if (!raw) return "—";
  const key = raw.toLowerCase().replace(/\s+/g, "_");
  if (OWNERSHIP_ROLE_LABELS[key]) return OWNERSHIP_ROLE_LABELS[key];
  if (OWNERSHIP_ROLE_LABELS[raw]) return OWNERSHIP_ROLE_LABELS[raw];
  return humanizeToken(raw);
}

export function customerConfidenceLabel(statusOrText) {
  const raw = String(statusOrText || "").trim();
  if (!raw) return "";
  const key = raw.toUpperCase();
  if (STATUS_LABELS[key]) return STATUS_LABELS[key];
  return humanizeToken(raw);
}

export { LABEL_MAP as RELATIONSHIP_CUSTOMER_LABELS, OWNERSHIP_ROLE_LABELS };
