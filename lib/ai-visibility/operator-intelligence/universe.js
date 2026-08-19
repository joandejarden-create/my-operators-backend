/**
 * Operator AI Intelligence — controlled 9-operator universe (V1).
 * Operator Master is identity SSOT. Regional scope is stored separately.
 * Do not expand without founder approval.
 */

export const OPERATOR_AI_UNIVERSE_VERSION = "operator_ai_intelligence_universe_v1";
export const PRIMARY_OPERATOR_COUNT = 9;
export const NEW_APPROVED_OPERATOR = "Remington Hospitality CALA";
export const UNAPPROVED_OPERATOR_EXPANSION = 0;

/** Founder shorthand → governed Operator Master. */
export const OPERATOR_AI_UNIVERSE = Object.freeze([
  Object.freeze({
    founderName: "Marriott International",
    slug: "marriott-international-managed",
    canonicalName: "Marriott International (Managed)",
    canonicalId: "recGmiPhRt6hiayd9",
    parentPlatform: "Marriott International",
    operatorLens: "BRAND_MANAGED_OPERATING_CAPABILITY",
    monitoredScope: "GLOBAL",
    regionalEmphasis: "CALA_WHERE_GOVERNED",
    identityStatus: "HIGH",
    measurementEligible: true,
    truthCoverage: "PARTIAL",
    domain: "marriott.com",
    managedProgramAlias: "Managed by Marriott (MxM)",
    notes:
      "Brand AI parent company is not the Operator AI subject. Operator Master uses the (Managed) lens.",
  }),
  Object.freeze({
    founderName: "IHG",
    slug: "ihg-managed",
    canonicalName: "IHG Hotels & Resorts (Managed)",
    canonicalId: "rec7IXYQYpKMYsrDl",
    parentPlatform: "IHG Hotels & Resorts",
    operatorLens: "BRAND_MANAGED_OPERATING_CAPABILITY",
    monitoredScope: "GLOBAL",
    regionalEmphasis: "CALA_WHERE_GOVERNED",
    identityStatus: "HIGH",
    measurementEligible: true,
    truthCoverage: "PARTIAL",
    domain: "ihg.com",
    managedProgramAlias: null,
    notes: "Treat as hotel management / operating capability, not franchise brand portfolio.",
  }),
  Object.freeze({
    founderName: "Hilton",
    slug: "hilton-managed",
    canonicalName: "Hilton (Managed)",
    canonicalId: "rec3Uwxe6ovpiokuN",
    parentPlatform: "Hilton",
    operatorLens: "BRAND_MANAGED_OPERATING_CAPABILITY",
    monitoredScope: "GLOBAL",
    regionalEmphasis: "CALA_WHERE_GOVERNED",
    identityStatus: "HIGH",
    measurementEligible: true,
    truthCoverage: "PARTIAL",
    domain: "hilton.com",
    managedProgramAlias: "Hilton Management Services",
    notes: "Do not infer operator capability solely because Hilton owns brands.",
  }),
  Object.freeze({
    founderName: "Aimbridge LATAM",
    slug: "aimbridge-latam",
    canonicalName: "Aimbridge Hospitality (LATAM)",
    canonicalId: "recGWxIJqnYHkJZFD",
    parentPlatform: "Aimbridge Hospitality",
    operatorLens: "THIRD_PARTY_MANAGER",
    monitoredScope: "LATAM",
    regionalEmphasis: "LATAM",
    identityStatus: "HIGH",
    measurementEligible: true,
    truthCoverage: "PARTIAL",
    domain: "aimbridgelatam.com",
    parentDomain: "aimbridgehospitality.com",
    notes:
      "Not a fake standalone company. Canonical entity is Aimbridge Hospitality; monitored scope is LATAM / CALA.",
  }),
  Object.freeze({
    founderName: "Hotel Equities CALA",
    slug: "hotel-equities-cala",
    canonicalName: "Hotel Equities (CALA)",
    canonicalId: "recWPKu5laVZxsvpn",
    parentPlatform: "Hotel Equities",
    operatorLens: "THIRD_PARTY_MANAGER",
    monitoredScope: "CALA",
    regionalEmphasis: "CALA",
    identityStatus: "HIGH",
    measurementEligible: true,
    truthCoverage: "HIGH",
    domain: "hotelequities.com",
    notes: "Operator Explorer quality baseline. Regional CALA lens of Hotel Equities.",
  }),
  Object.freeze({
    founderName: "Arbor Lodging",
    slug: "arbor-lodging-cala",
    canonicalName: "Arbor Lodging (CALA)",
    canonicalId: "recF5Z87OAqFgndoq",
    parentPlatform: "Arbor Lodging",
    operatorLens: "THIRD_PARTY_MANAGER",
    monitoredScope: "CALA",
    regionalEmphasis: "CALA",
    identityStatus: "HIGH",
    measurementEligible: true,
    truthCoverage: "HIGH",
    domain: "arborlodging.com",
    notes: "Operator Explorer quality baseline. Honest zero CALA managed footprint remains labeled, not invented.",
  }),
  Object.freeze({
    founderName: "GHL",
    slug: "ghl-hoteles",
    canonicalName: "GHL Hoteles (GHL Holding)",
    canonicalId: "reciI2tYQBfMoMK9G",
    parentPlatform: "GHL Holding",
    operatorLens: "LATAM_HOTEL_PLATFORM",
    monitoredScope: "CALA",
    regionalEmphasis: "CALA",
    identityStatus: "HIGH",
    measurementEligible: true,
    truthCoverage: "PARTIAL",
    domain: "ghlhoteles.com",
    notes: "Canonical display GHL Hoteles (GHL Holding). Founder shorthand GHL.",
  }),
  Object.freeze({
    founderName: "Brittain Resorts",
    slug: "brittain-resorts-hotels",
    canonicalName: "Brittain Resorts & Hotels (BRH)",
    canonicalId: "receHCdI6CEsJqdG4",
    parentPlatform: "Brittain Resorts & Hotels",
    operatorLens: "THIRD_PARTY_MANAGER",
    monitoredScope: "US_SOUTHEAST",
    regionalEmphasis: "US",
    identityStatus: "HIGH",
    measurementEligible: true,
    truthCoverage: "LOW",
    domain: "brittainresorts.com",
    notes:
      "Kept in the founder 8. CALA relevance is not established on brittainresorts.com — CALA scenarios are OUT_OF_SCOPE, not a dropped identity.",
  }),
  Object.freeze({
    founderName: "Remington CALA",
    slug: "remington-hospitality-cala",
    canonicalName: "Remington Hospitality (CALA)",
    canonicalId: "rec6UB6RpMKSs2tAo",
    parentPlatform: "Remington Hospitality",
    operatorLens: "THIRD_PARTY_MANAGER",
    monitoredScope: "CALA",
    regionalEmphasis: "CALA",
    identityStatus: "HIGH",
    measurementEligible: true,
    truthCoverage: "PARTIAL",
    domain: "remingtonhospitality.com",
    notes:
      "Founder-approved 9th operator. Not a fake standalone company. Canonical entity is Remington Hospitality; monitored scope is CALA. Promoted from observed-competitor evidence path.",
  }),
]);

const BLOCKED_EXPANSION_NAMES = Object.freeze([
  "Highgate",
  "Pyramid",
  "Davidson",
  "HEI",
  "Crescent",
  "CoralTree",
  "MCR",
]);

export function listPrimaryOperatorIds() {
  return OPERATOR_AI_UNIVERSE.map((o) => o.canonicalId);
}

export function getOperatorByFounderName(name) {
  const key = String(name || "").trim().toLowerCase();
  return (
    OPERATOR_AI_UNIVERSE.find((o) => o.founderName.toLowerCase() === key) ||
    OPERATOR_AI_UNIVERSE.find((o) => o.canonicalName.toLowerCase() === key) ||
    null
  );
}

export function getOperatorById(id) {
  return OPERATOR_AI_UNIVERSE.find((o) => o.canonicalId === id) || null;
}

export function isPrimaryMonitoredOperator(id) {
  return OPERATOR_AI_UNIVERSE.some((o) => o.canonicalId === id);
}

export function isBlockedUniverseExpansionName(name) {
  const key = String(name || "").trim().toLowerCase();
  return BLOCKED_EXPANSION_NAMES.some((n) => n.toLowerCase() === key);
}

export function assertUniverseLock() {
  if (OPERATOR_AI_UNIVERSE.length !== PRIMARY_OPERATOR_COUNT) {
    throw new Error(`PRIMARY_OPERATOR_COUNT expected ${PRIMARY_OPERATOR_COUNT}`);
  }
  const ids = new Set(listPrimaryOperatorIds());
  if (ids.size !== PRIMARY_OPERATOR_COUNT) {
    throw new Error("duplicate operator ids in universe");
  }
  return true;
}
