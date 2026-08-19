/**
 * Operator commercial-comparability truth V1.
 * Minimum structured facts required to decide CORE vs SECONDARY vs CONDITIONAL.
 * Sources: Operator Master / governed universe / Operator Setup Operating Model /
 * Operator Explorer baselines / factory packs. No Census. No AI-response inference.
 */

import { OPERATOR_AI_UNIVERSE } from "./universe.js";

export const OPERATOR_COMPARABILITY_TRUTH_VERSION = "operator_comparability_truth_v1";

export const OPERATOR_MODEL = Object.freeze({
  BRAND_MANAGED_PLATFORM: "BRAND_MANAGED_PLATFORM",
  THIRD_PARTY_MANAGER: "THIRD_PARTY_MANAGER",
  REGIONAL_PLATFORM_MIXED: "REGIONAL_PLATFORM_MIXED",
  OTHER_UNCERTAIN: "OTHER_UNCERTAIN",
});

export const TRUTH_FIELD_CLASS = Object.freeze({
  PRODUCTION_REQUIRED: "PRODUCTION_REQUIRED",
  DETAIL_ONLY: "DETAIL_ONLY",
  NOT_REQUIRED: "NOT_REQUIRED",
});

export const COMPARABILITY_TRUTH_FIELD_POLICY = Object.freeze({
  OPERATOR_MODEL: TRUTH_FIELD_CLASS.PRODUCTION_REQUIRED,
  MANAGED_BRAND_AFFILIATED: TRUTH_FIELD_CLASS.PRODUCTION_REQUIRED,
  THIRD_PARTY_MANAGEMENT: TRUTH_FIELD_CLASS.PRODUCTION_REQUIRED,
  GEOGRAPHIC_OPERATING_SCOPE: TRUTH_FIELD_CLASS.PRODUCTION_REQUIRED,
  BRAND_AGNOSTIC_CAPABILITY: TRUTH_FIELD_CLASS.PRODUCTION_REQUIRED,
  FULL_SERVICE_CAPABILITY: TRUTH_FIELD_CLASS.DETAIL_ONLY,
  LUXURY_CAPABILITY: TRUTH_FIELD_CLASS.DETAIL_ONLY,
  LIFESTYLE_BOUTIQUE_CAPABILITY: TRUTH_FIELD_CLASS.DETAIL_ONLY,
  RESORT_CAPABILITY: TRUTH_FIELD_CLASS.DETAIL_ONLY,
  INDEPENDENT_HOTEL_CAPABILITY: TRUTH_FIELD_CLASS.DETAIL_ONLY,
  CONVERSION_REPOSITIONING_CAPABILITY: TRUTH_FIELD_CLASS.DETAIL_ONLY,
  CHAIN_SCALE_HOTEL_SEGMENT: TRUTH_FIELD_CLASS.NOT_REQUIRED,
});

const SOURCE = Object.freeze({
  OPERATOR_MASTER: "OPERATOR_MASTER_COMPANY_VALIDATED",
  UNIVERSE: "OPERATOR_AI_UNIVERSE_V1",
  QUALITY_BASELINE: "OPERATOR_EXPLORER_QUALITY_BASELINE",
  FACTORY_PACK: "OPERATOR_EXPLORER_FACTORY_PACK",
  FIRST_PARTY: "FIRST_PARTY_DOMAIN_ALREADY_STORED",
});

/**
 * Governed per-operator comparability truth. Not Explorer enrichment.
 * Capability fields that are not production-certified stay DETAIL_ONLY / unresolved.
 */
export const OPERATOR_COMPARABILITY_TRUTH_PACKS = Object.freeze({
  recGmiPhRt6hiayd9: Object.freeze({
    operator: "Marriott International — Managed",
    canonicalId: "recGmiPhRt6hiayd9",
    model: OPERATOR_MODEL.BRAND_MANAGED_PLATFORM,
    modelValidationState: "PRODUCTION_VALIDATED",
    managedBrandAffiliated: true,
    thirdPartyManagement: false,
    brandAgnosticCapability: false,
    geographicOperatingScope: "GLOBAL",
    geographicFamily: "GLOBAL",
    currentProductionTruth: [
      "OPERATOR_IDENTITY",
      "OPERATOR_LENS",
      "MONITORED_SCOPE",
      "CANONICAL_NAME",
      "CANONICAL_ID",
      "FIRST_PARTY_DOMAIN",
      "OPERATOR_MODEL",
      "MANAGED_BRAND_AFFILIATED",
      "THIRD_PARTY_MANAGEMENT",
      "GEOGRAPHIC_OPERATING_SCOPE",
      "BRAND_AGNOSTIC_CAPABILITY",
    ],
    fieldsResolved: [
      "OPERATOR_MODEL",
      "MANAGED_BRAND_AFFILIATED",
      "THIRD_PARTY_MANAGEMENT",
      "GEOGRAPHIC_OPERATING_SCOPE",
      "BRAND_AGNOSTIC_CAPABILITY",
    ],
    missingComparabilityFields: [
      "LUXURY_CAPABILITY_DEPTH",
      "CONVERSION_REPOSITIONING_CAPABILITY",
    ],
    sourceType: SOURCE.OPERATOR_MASTER,
    sourceNote:
      "Operator Master (Managed) lens + Operating Model Hybrid (company-validated Operator Setup). Brand-agnostic / third-party = false by model.",
    confidence: "HIGH",
    validationState: "PRODUCTION_VALIDATED",
    competitiveEvidenceState: "VALIDATED",
  }),
  rec7IXYQYpKMYsrDl: Object.freeze({
    operator: "IHG Hotels & Resorts — Managed",
    canonicalId: "rec7IXYQYpKMYsrDl",
    model: OPERATOR_MODEL.BRAND_MANAGED_PLATFORM,
    modelValidationState: "PRODUCTION_VALIDATED",
    managedBrandAffiliated: true,
    thirdPartyManagement: false,
    brandAgnosticCapability: false,
    geographicOperatingScope: "GLOBAL",
    geographicFamily: "GLOBAL",
    currentProductionTruth: [
      "OPERATOR_IDENTITY",
      "OPERATOR_LENS",
      "MONITORED_SCOPE",
      "CANONICAL_NAME",
      "CANONICAL_ID",
      "FIRST_PARTY_DOMAIN",
      "OPERATOR_MODEL",
      "MANAGED_BRAND_AFFILIATED",
      "THIRD_PARTY_MANAGEMENT",
      "GEOGRAPHIC_OPERATING_SCOPE",
      "BRAND_AGNOSTIC_CAPABILITY",
    ],
    fieldsResolved: [
      "OPERATOR_MODEL",
      "MANAGED_BRAND_AFFILIATED",
      "THIRD_PARTY_MANAGEMENT",
      "GEOGRAPHIC_OPERATING_SCOPE",
      "BRAND_AGNOSTIC_CAPABILITY",
    ],
    missingComparabilityFields: [
      "LUXURY_CAPABILITY_DEPTH",
      "CONVERSION_REPOSITIONING_CAPABILITY",
    ],
    sourceType: SOURCE.OPERATOR_MASTER,
    sourceNote: "Operator Master (Managed) lens + Operating Model Hybrid.",
    confidence: "HIGH",
    validationState: "PRODUCTION_VALIDATED",
    competitiveEvidenceState: "VALIDATED",
  }),
  rec3Uwxe6ovpiokuN: Object.freeze({
    operator: "Hilton — Managed",
    canonicalId: "rec3Uwxe6ovpiokuN",
    model: OPERATOR_MODEL.BRAND_MANAGED_PLATFORM,
    modelValidationState: "PRODUCTION_VALIDATED",
    managedBrandAffiliated: true,
    thirdPartyManagement: false,
    brandAgnosticCapability: false,
    geographicOperatingScope: "GLOBAL",
    geographicFamily: "GLOBAL",
    currentProductionTruth: [
      "OPERATOR_IDENTITY",
      "OPERATOR_LENS",
      "MONITORED_SCOPE",
      "CANONICAL_NAME",
      "CANONICAL_ID",
      "FIRST_PARTY_DOMAIN",
      "OPERATOR_MODEL",
      "MANAGED_BRAND_AFFILIATED",
      "THIRD_PARTY_MANAGEMENT",
      "GEOGRAPHIC_OPERATING_SCOPE",
      "BRAND_AGNOSTIC_CAPABILITY",
    ],
    fieldsResolved: [
      "OPERATOR_MODEL",
      "MANAGED_BRAND_AFFILIATED",
      "THIRD_PARTY_MANAGEMENT",
      "GEOGRAPHIC_OPERATING_SCOPE",
      "BRAND_AGNOSTIC_CAPABILITY",
    ],
    missingComparabilityFields: [
      "LUXURY_CAPABILITY_DEPTH",
      "CONVERSION_REPOSITIONING_CAPABILITY",
    ],
    sourceType: SOURCE.OPERATOR_MASTER,
    sourceNote: "Operator Master (Managed) lens + Operating Model Hybrid.",
    confidence: "HIGH",
    validationState: "PRODUCTION_VALIDATED",
    competitiveEvidenceState: "VALIDATED",
  }),
  recGWxIJqnYHkJZFD: Object.freeze({
    operator: "Aimbridge Hospitality LATAM",
    canonicalId: "recGWxIJqnYHkJZFD",
    model: OPERATOR_MODEL.THIRD_PARTY_MANAGER,
    modelValidationState: "PRODUCTION_VALIDATED",
    managedBrandAffiliated: false,
    thirdPartyManagement: true,
    brandAgnosticCapability: true,
    geographicOperatingScope: "LATAM",
    geographicFamily: "LATAM_CALA",
    currentProductionTruth: [
      "OPERATOR_IDENTITY",
      "OPERATOR_LENS",
      "MONITORED_SCOPE",
      "CANONICAL_NAME",
      "CANONICAL_ID",
      "FIRST_PARTY_DOMAIN",
      "OPERATOR_MODEL",
      "MANAGED_BRAND_AFFILIATED",
      "THIRD_PARTY_MANAGEMENT",
      "GEOGRAPHIC_OPERATING_SCOPE",
      "BRAND_AGNOSTIC_CAPABILITY",
    ],
    fieldsResolved: [
      "OPERATOR_MODEL",
      "MANAGED_BRAND_AFFILIATED",
      "THIRD_PARTY_MANAGEMENT",
      "GEOGRAPHIC_OPERATING_SCOPE",
      "BRAND_AGNOSTIC_CAPABILITY",
    ],
    missingComparabilityFields: ["LUXURY_CAPABILITY_DEPTH", "CONVERSION_REPOSITIONING_CAPABILITY"],
    sourceType: SOURCE.FACTORY_PACK,
    sourceNote:
      "Operator Master LATAM lens + Operating Model Third-Party + aimbridgelatam.com factory pack (brand-agnostic multi-flag TPM).",
    confidence: "HIGH",
    validationState: "PRODUCTION_VALIDATED",
    competitiveEvidenceState: "VALIDATED",
  }),
  recWPKu5laVZxsvpn: Object.freeze({
    operator: "Hotel Equities CALA",
    canonicalId: "recWPKu5laVZxsvpn",
    model: OPERATOR_MODEL.THIRD_PARTY_MANAGER,
    modelValidationState: "PRODUCTION_VALIDATED",
    managedBrandAffiliated: false,
    thirdPartyManagement: true,
    brandAgnosticCapability: true,
    geographicOperatingScope: "CALA",
    geographicFamily: "LATAM_CALA",
    currentProductionTruth: [
      "OPERATOR_IDENTITY",
      "OPERATOR_LENS",
      "MONITORED_SCOPE",
      "CANONICAL_NAME",
      "CANONICAL_ID",
      "FIRST_PARTY_DOMAIN",
      "OPERATOR_MODEL",
      "MANAGED_BRAND_AFFILIATED",
      "THIRD_PARTY_MANAGEMENT",
      "GEOGRAPHIC_OPERATING_SCOPE",
      "BRAND_AGNOSTIC_CAPABILITY",
    ],
    fieldsResolved: [
      "OPERATOR_MODEL",
      "MANAGED_BRAND_AFFILIATED",
      "THIRD_PARTY_MANAGEMENT",
      "GEOGRAPHIC_OPERATING_SCOPE",
      "BRAND_AGNOSTIC_CAPABILITY",
    ],
    missingComparabilityFields: ["LUXURY_CAPABILITY_DEPTH", "RESORT_CAPABILITY"],
    sourceType: SOURCE.QUALITY_BASELINE,
    sourceNote: "Operator Explorer quality baseline + Operating Model Third-Party.",
    confidence: "HIGH",
    validationState: "PRODUCTION_VALIDATED",
    competitiveEvidenceState: "VALIDATED",
  }),
  recF5Z87OAqFgndoq: Object.freeze({
    operator: "Arbor Lodging CALA",
    canonicalId: "recF5Z87OAqFgndoq",
    model: OPERATOR_MODEL.THIRD_PARTY_MANAGER,
    modelValidationState: "PRODUCTION_VALIDATED",
    managedBrandAffiliated: false,
    thirdPartyManagement: true,
    brandAgnosticCapability: true,
    geographicOperatingScope: "CALA",
    geographicFamily: "LATAM_CALA",
    currentProductionTruth: [
      "OPERATOR_IDENTITY",
      "OPERATOR_LENS",
      "MONITORED_SCOPE",
      "CANONICAL_NAME",
      "CANONICAL_ID",
      "FIRST_PARTY_DOMAIN",
      "OPERATOR_MODEL",
      "MANAGED_BRAND_AFFILIATED",
      "THIRD_PARTY_MANAGEMENT",
      "GEOGRAPHIC_OPERATING_SCOPE",
      "BRAND_AGNOSTIC_CAPABILITY",
    ],
    fieldsResolved: [
      "OPERATOR_MODEL",
      "MANAGED_BRAND_AFFILIATED",
      "THIRD_PARTY_MANAGEMENT",
      "GEOGRAPHIC_OPERATING_SCOPE",
      "BRAND_AGNOSTIC_CAPABILITY",
    ],
    missingComparabilityFields: ["CALA_MANAGED_FOOTPRINT", "LIVE_POSITIVE_PRESENCE_EVIDENCE"],
    sourceType: SOURCE.QUALITY_BASELINE,
    sourceNote:
      "Model is company-validated third-party. Competitive Presence evidence remains insufficient (0 live positive mentions).",
    confidence: "HIGH",
    validationState: "PRODUCTION_VALIDATED",
    competitiveEvidenceState: "INSUFFICIENT_OPERATOR_SPECIFIC_EVIDENCE",
  }),
  reciI2tYQBfMoMK9G: Object.freeze({
    operator: "GHL Hoteles / GHL Holding CALA",
    canonicalId: "reciI2tYQBfMoMK9G",
    model: OPERATOR_MODEL.REGIONAL_PLATFORM_MIXED,
    modelValidationState: "PRODUCTION_VALIDATED",
    managedBrandAffiliated: true,
    thirdPartyManagement: true,
    brandAgnosticCapability: true,
    geographicOperatingScope: "CALA",
    geographicFamily: "LATAM_CALA",
    currentProductionTruth: [
      "OPERATOR_IDENTITY",
      "OPERATOR_LENS",
      "MONITORED_SCOPE",
      "CANONICAL_NAME",
      "CANONICAL_ID",
      "FIRST_PARTY_DOMAIN",
      "OPERATOR_MODEL",
      "GEOGRAPHIC_OPERATING_SCOPE",
    ],
    fieldsResolved: ["OPERATOR_MODEL", "GEOGRAPHIC_OPERATING_SCOPE", "THIRD_PARTY_MANAGEMENT"],
    missingComparabilityFields: [
      "PURE_THIRD_PARTY_VS_PROPRIETARY_BRAND_SPLIT",
      "LUXURY_CAPABILITY_DEPTH",
    ],
    sourceType: SOURCE.OPERATOR_MASTER,
    sourceNote:
      "Operator Master Operating Model = Third-Party, but first-party/Explorer materials document proprietary GHL brands plus international affiliations. Classified MIXED regional platform. Not a pure TPM CORE substitute.",
    confidence: "HIGH",
    validationState: "PRODUCTION_VALIDATED",
    competitiveEvidenceState: "VALIDATED",
  }),
  receHCdI6CEsJqdG4: Object.freeze({
    operator: "Brittain Resorts & Hotels — US Southeast",
    canonicalId: "receHCdI6CEsJqdG4",
    model: OPERATOR_MODEL.THIRD_PARTY_MANAGER,
    modelValidationState: "PRODUCTION_VALIDATED",
    managedBrandAffiliated: false,
    thirdPartyManagement: true,
    brandAgnosticCapability: null,
    geographicOperatingScope: "US_SOUTHEAST",
    geographicFamily: "US_SOUTHEAST",
    currentProductionTruth: [
      "OPERATOR_IDENTITY",
      "OPERATOR_LENS",
      "MONITORED_SCOPE",
      "CANONICAL_NAME",
      "CANONICAL_ID",
      "FIRST_PARTY_DOMAIN",
      "OPERATOR_MODEL",
      "THIRD_PARTY_MANAGEMENT",
      "GEOGRAPHIC_OPERATING_SCOPE",
    ],
    fieldsResolved: ["OPERATOR_MODEL", "THIRD_PARTY_MANAGEMENT", "GEOGRAPHIC_OPERATING_SCOPE"],
    missingComparabilityFields: ["BRAND_AGNOSTIC_CAPABILITY", "LUXURY_CAPABILITY"],
    sourceType: SOURCE.OPERATOR_MASTER,
    sourceNote:
      "US Southeast third-party manager. CALA geography not established. Luxury capability unresolved (INSUFFICIENT_TRUTH).",
    confidence: "MEDIUM",
    validationState: "PRODUCTION_VALIDATED",
    competitiveEvidenceState: "VALIDATED",
  }),
  rec6UB6RpMKSs2tAo: Object.freeze({
    operator: "Remington Hospitality (CALA)",
    canonicalId: "rec6UB6RpMKSs2tAo",
    model: OPERATOR_MODEL.THIRD_PARTY_MANAGER,
    modelValidationState: "PRODUCTION_VALIDATED",
    managedBrandAffiliated: false,
    thirdPartyManagement: true,
    brandAgnosticCapability: true,
    geographicOperatingScope: "CALA",
    geographicFamily: "LATAM_CALA",
    currentProductionTruth: [
      "OPERATOR_IDENTITY",
      "OPERATOR_LENS",
      "MONITORED_SCOPE",
      "CANONICAL_NAME",
      "CANONICAL_ID",
      "FIRST_PARTY_DOMAIN",
      "OPERATOR_MODEL",
      "MANAGED_BRAND_AFFILIATED",
      "THIRD_PARTY_MANAGEMENT",
      "GEOGRAPHIC_OPERATING_SCOPE",
      "BRAND_AGNOSTIC_CAPABILITY",
    ],
    fieldsResolved: [
      "OPERATOR_MODEL",
      "MANAGED_BRAND_AFFILIATED",
      "THIRD_PARTY_MANAGEMENT",
      "GEOGRAPHIC_OPERATING_SCOPE",
      "BRAND_AGNOSTIC_CAPABILITY",
    ],
    missingComparabilityFields: ["LUXURY_CAPABILITY_DEPTH", "CONVERSION_REPOSITIONING_CAPABILITY"],
    sourceType: SOURCE.OPERATOR_MASTER,
    sourceNote:
      "Operator Master rec6UB6RpMKSs2tAo — Remington Hospitality parent; monitored scope CALA. Bare Remington is not a truth alias.",
    confidence: "HIGH",
    validationState: "PRODUCTION_VALIDATED",
    competitiveEvidenceState: "VALIDATED",
  }),
});

export function getComparabilityTruth(operatorId) {
  return OPERATOR_COMPARABILITY_TRUTH_PACKS[operatorId] || null;
}

export function listOperatorModels() {
  return OPERATOR_AI_UNIVERSE.map((o) => {
    const pack = getComparabilityTruth(o.canonicalId);
    return {
      operator: pack.operator,
      canonicalId: o.canonicalId,
      model: pack.model,
      validationState: pack.modelValidationState,
      competitiveEvidenceState: pack.competitiveEvidenceState,
    };
  });
}

export function productionRequiredComparabilityFields() {
  return Object.entries(COMPARABILITY_TRUTH_FIELD_POLICY)
    .filter(([, v]) => v === TRUTH_FIELD_CLASS.PRODUCTION_REQUIRED)
    .map(([k]) => k);
}

export function detailOnlyComparabilityFields() {
  return Object.entries(COMPARABILITY_TRUTH_FIELD_POLICY)
    .filter(([, v]) => v === TRUTH_FIELD_CLASS.DETAIL_ONLY)
    .map(([k]) => k);
}

export function modelsCommerciallySubstitutable(subjectModel, otherModel, scenarioId) {
  if (!subjectModel || !otherModel) return false;
  if (subjectModel === OPERATOR_MODEL.OTHER_UNCERTAIN || otherModel === OPERATOR_MODEL.OTHER_UNCERTAIN) {
    return false;
  }
  const tpmScenarios = new Set([
    "op_scenario_third_party_management_v1",
    "op_scenario_brand_agnostic_operation_v1",
    "op_scenario_owner_control_flexibility_v1",
    "op_scenario_independent_hotel_operation_v1",
  ]);
  if (tpmScenarios.has(scenarioId)) {
    return (
      subjectModel === OPERATOR_MODEL.THIRD_PARTY_MANAGER &&
      otherModel === OPERATOR_MODEL.THIRD_PARTY_MANAGER
    );
  }
  if (scenarioId === "op_scenario_cala_latam_regional_capability_v1") {
    return (
      subjectModel === OPERATOR_MODEL.THIRD_PARTY_MANAGER &&
      otherModel === OPERATOR_MODEL.THIRD_PARTY_MANAGER
    );
  }
  if (scenarioId === "op_scenario_full_service_uu_operator_selection_v1") {
    return subjectModel === otherModel && subjectModel !== OPERATOR_MODEL.REGIONAL_PLATFORM_MIXED;
  }
  return false;
}
