/**
 * Classify corporate / registry contact routes by purpose — not all are owner development contacts.
 */

export const CONTACT_PURPOSE = Object.freeze({
  FINANCE: "FINANCE",
  PURCHASING: "PURCHASING",
  HOTEL_ADMIN: "HOTEL_ADMIN",
  SUSTAINABILITY: "SUSTAINABILITY",
  PRIVACY_ARCO: "PRIVACY_ARCO",
  GENERAL_CORPORATE: "GENERAL_CORPORATE",
  PORTFOLIO_BRAND_GENERAL: "PORTFOLIO_BRAND_GENERAL",
  UNATTRIBUTED_PERSON_LEAD: "UNATTRIBUTED_PERSON_LEAD",
  PERSON_NAMED: "PERSON_NAMED",
  DENUE_REGISTRY: "DENUE_REGISTRY",
  UNKNOWN: "UNKNOWN",
});

export function classifyContactPurpose(email, context = {}) {
  const local = String(email || "").split("@")[0].toLowerCase();
  const domain = String(email || "").split("@")[1]?.toLowerCase() || "";
  const src = String(context.source_class || "").toUpperCase();

  if (context.is_person_named === true) return CONTACT_PURPOSE.PERSON_NAMED;
  if (src === "DENUE_REGISTRY") return CONTACT_PURPOSE.DENUE_REGISTRY;

  if (/^(sostenibilidad|sustainability|csr)/.test(local)) return CONTACT_PURPOSE.SUSTAINABILITY;
  if (/^(derechosarco|privacidad|arco|privacy|datos)/.test(local)) return CONTACT_PURPOSE.PRIVACY_ARCO;
  if (/^(compras|purchasing|procurement)/.test(local)) return CONTACT_PURPOSE.PURCHASING;
  if (/^(finanzas|finance|contabilidad|administracion|adm|reforma\.adm)/.test(local)) return CONTACT_PURPOSE.FINANCE;
  if (/^(info|contact|contacto|reserv)/.test(local)) return CONTACT_PURPOSE.GENERAL_CORPORATE;
  if (/^[a-z]\.[a-z]+@/.test(String(email)) || /^[a-z]{1,12}@/.test(String(email))) {
    return CONTACT_PURPOSE.UNATTRIBUTED_PERSON_LEAD;
  }
  if (context.portfolio_brand_domain && domain !== context.owner_domain) {
    return CONTACT_PURPOSE.PORTFOLIO_BRAND_GENERAL;
  }
  if (/hotel|property|reforma|mexico/.test(local)) return CONTACT_PURPOSE.HOTEL_ADMIN;
  return CONTACT_PURPOSE.UNKNOWN;
}

export function isUsableOwnerDevelopmentRoute(purpose) {
  return purpose === CONTACT_PURPOSE.PERSON_NAMED;
}

export function isUsableOwnerBusinessRoute(purpose) {
  return [
    CONTACT_PURPOSE.GENERAL_CORPORATE,
    CONTACT_PURPOSE.FINANCE,
    CONTACT_PURPOSE.HOTEL_ADMIN,
    CONTACT_PURPOSE.DENUE_REGISTRY,
  ].includes(purpose);
}
