/**
 * Sample narrative metadata for CALA demo operators (Phase 5C profiles).
 * Used for narrative polish only — not scoring weights or factor math.
 */

/** @type {Record<string, { label: string, theme: string }>} */
export const OPERATOR_SAMPLE_NARRATIVE_META = {
  recq3NiRxOerg4kZU: {
    label: "Mexico select-service specialist",
    theme: "select_service_mexico",
  },
  recQ6Cf8O2z0tiqBz: {
    label: "Yucatán / Cancún select-service",
    theme: "yucatan_select_upper_mid",
  },
  recZPHT2zqc8K6itx: {
    label: "Andean commercial platform",
    theme: "andean_commercial_platform",
  },
  recZgNR85WZKDItLF: {
    label: "Full-service / lifestyle F&B",
    theme: "full_service_lifestyle_fb",
  },
  recbT3q8ApRIBu4j5: {
    label: "Institutional / luxury platform",
    theme: "institutional_luxury",
  },
  recTUjuDxL96yWcQA: {
    label: "Caribbean upscale operator",
    theme: "caribbean_upscale",
  },
  recWPKu5laVZxsvpn: {
    label: "CALA resort / all-inclusive",
    theme: "cala_resort_all_inclusive",
  },
  reckO98E46sKTn3F3: {
    label: "Southern Cone full-service",
    theme: "southern_cone_full_service",
  },
  recwbyY4qfNP1bV3r: {
    label: "Brazil regional operator",
    theme: "brazil_regional_mixed",
  },
  recxAa86Qoc0nFRSt: {
    label: "Central America resort operator",
    theme: "central_america_resort",
  },
};

export function getOperatorSampleNarrativeMeta(operatorId) {
  return OPERATOR_SAMPLE_NARRATIVE_META[operatorId] || null;
}
