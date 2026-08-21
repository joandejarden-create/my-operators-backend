/**
 * ADP property certification statuses + issue taxonomy.
 * Audit/certification only — does not change measurement methodology.
 */

export const CERTIFICATION_STATUSES = Object.freeze({
  CERTIFIED: "CERTIFIED",
  CERTIFIED_WITH_DISCLOSURES: "CERTIFIED_WITH_DISCLOSURES",
  NOT_CERTIFIED: "NOT_CERTIFIED",
});

export const ISSUE_CLASSES = Object.freeze({
  DATA_MISSING: "DATA_MISSING",
  DATA_QUALITY: "DATA_QUALITY",
  ENTITY_RESOLUTION: "ENTITY_RESOLUTION",
  PARSING: "PARSING",
  METHODOLOGY: "METHODOLOGY",
  IMPLEMENTATION: "IMPLEMENTATION",
  CALCULATION: "CALCULATION",
  PUBLISHING: "PUBLISHING",
  UI_DISPLAY: "UI_DISPLAY",
  EVIDENCE: "EVIDENCE",
  COMPARABILITY: "COMPARABILITY",
  GOVERNANCE: "GOVERNANCE",
});

export const GATE_IDS = Object.freeze({
  CONTRACT_VERSION: "contract_version_consistency",
  SCENARIO_UNIVERSE: "scenario_completeness",
  PROVIDER_COMPLETENESS: "provider_completeness_rules",
  DEMAND_TERRITORY: "demand_territory_mapping",
  ATTRIBUTE_DICTIONARY: "attribute_dictionary_consistency",
  SUBJECT_ENTITY: "subject_entity_resolution",
  COMPETITOR_ENTITY: "competitor_entity_quality",
  METRIC_RECONCILIATION: "metric_reconciliation",
  CORE_BENCHMARK: "core_benchmark",
  EVIDENCE_COMPLETENESS: "evidence_completeness",
  TREND_BASELINE: "trend_baseline",
  PUBLISHED_PAYLOAD: "published_payload",
  OPENAI_FALSE_NEGATIVE: "openai_false_negative_audit",
  ANOMALY_DETECTION: "anomaly_detection",
  AIRTABLE_READ_PATH: "airtable_read_path",
  CUSTOMER_CLAIMS: "customer_claims",
  UI_STRUCTURAL: "ui_structural",
});

export const ADP_CERTIFICATION_VERSION = "adp_property_certification_v1";

export const LIVE_EXISTING_HOTEL_PROPERTY_IDS = Object.freeze([
  "adp_waterstone_boca_raton",
  "adp_renaissance_times_square",
  "adp_cambridge_beaches_bermuda",
  "adp_now_now_noho",
  "adp_hotel_phillips_kansas_city",
]);

export function gateResult({
  gateId,
  status,
  material = false,
  issueClass = null,
  summary = "",
  details = null,
  disclosures = [],
}) {
  return {
    gateId,
    status, // PASS | FAIL | PASS_WITH_DISCLOSURE | SKIP | BLOCKED
    material: Boolean(material),
    issueClass,
    summary,
    details,
    disclosures: disclosures || [],
  };
}

/**
 * Aggregate gate results into a certification status.
 * Material FAIL → NOT_CERTIFIED.
 * Only non-material disclosures → CERTIFIED_WITH_DISCLOSURES.
 * All PASS → CERTIFIED.
 */
export function aggregateCertificationStatus(gates = []) {
  const materialFails = gates.filter((g) => g.status === "FAIL" && g.material);
  if (materialFails.length) {
    return {
      status: CERTIFICATION_STATUSES.NOT_CERTIFIED,
      materialFailCount: materialFails.length,
      disclosureCount: gates.filter((g) => g.disclosures?.length).length,
    };
  }
  const disclosures = gates.flatMap((g) => g.disclosures || []);
  const softFails = gates.filter((g) => g.status === "FAIL" && !g.material);
  const passWithDisc = gates.filter((g) => g.status === "PASS_WITH_DISCLOSURE");
  if (disclosures.length || softFails.length || passWithDisc.length) {
    return {
      status: CERTIFICATION_STATUSES.CERTIFIED_WITH_DISCLOSURES,
      materialFailCount: 0,
      disclosureCount: disclosures.length + softFails.length + passWithDisc.length,
    };
  }
  return {
    status: CERTIFICATION_STATUSES.CERTIFIED,
    materialFailCount: 0,
    disclosureCount: 0,
  };
}
