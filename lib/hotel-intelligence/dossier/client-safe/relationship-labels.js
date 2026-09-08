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

export { LABEL_MAP as RELATIONSHIP_CUSTOMER_LABELS };
