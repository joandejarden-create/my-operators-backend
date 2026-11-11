/**
 * HPC field importance relative to ADP Full E2E dependencies.
 * Importance is dependency-driven — not a single universal required list for every hotel type.
 */

import {
  HPC_MINIMUM_IDENTITY_FIELDS,
  HPC_ADP_ESSENTIAL_FIELDS,
  HPC_ENRICHMENT_TARGET_FIELDS,
} from "./hpc-full-onboarding-contract-v1.js";

export const HPC_FIELD_IMPORTANCE_V1 = "HPC_FIELD_IMPORTANCE_V1";

export const HPC_FIELD_IMPORTANCE = Object.freeze({
  CRITICAL: "CRITICAL",
  IMPORTANT: "IMPORTANT",
  ENRICHMENT: "ENRICHMENT",
  INTERNAL_WORKFLOW: "INTERNAL_WORKFLOW",
  PROVIDER_SPECIFIC: "PROVIDER_SPECIFIC",
});

/** Fields whose absence/contradiction must fail-closed dependent ADP stages. */
export const HPC_CRITICAL_FIELDS = Object.freeze([
  ...HPC_MINIMUM_IDENTITY_FIELDS,
  "Latitude",
  "Longitude",
  "Country",
  "City",
  "Market",
]);

/** Continue Core monitoring when incomplete, but retain research obligations. */
export const HPC_IMPORTANT_FIELDS = Object.freeze([
  ...HPC_ADP_ESSENTIAL_FIELDS.filter((f) => !HPC_CRITICAL_FIELDS.includes(f)),
  "Submarket",
  "Sub-Continent",
  "Continent",
  "Official Property URL",
  "Rooms / Keys",
  "Operator / Management Company",
  "Property Type",
  "State / Region",
]);

const INTERNAL_PATTERNS = [
  /^Candidate Brand/i,
  /^Brand Validation/i,
  /^Address Confidence$/i,
  /^Building \/ Asset Notes$/i,
  /Reviewed Date$/i,
  /Review Status$/i,
  /Slug if mapped/i,
];

const PROVIDER_PATTERNS = [/^HBX /i];

/**
 * @param {string} fieldName
 * @returns {string}
 */
export function classifyHpcFieldImportance(fieldName) {
  const name = String(fieldName || "").trim();
  if (!name) return HPC_FIELD_IMPORTANCE.ENRICHMENT;
  if (PROVIDER_PATTERNS.some((re) => re.test(name))) {
    return HPC_FIELD_IMPORTANCE.PROVIDER_SPECIFIC;
  }
  if (INTERNAL_PATTERNS.some((re) => re.test(name))) {
    return HPC_FIELD_IMPORTANCE.INTERNAL_WORKFLOW;
  }
  if (HPC_CRITICAL_FIELDS.includes(name)) return HPC_FIELD_IMPORTANCE.CRITICAL;
  if (HPC_IMPORTANT_FIELDS.includes(name)) return HPC_FIELD_IMPORTANCE.IMPORTANT;
  if (HPC_ENRICHMENT_TARGET_FIELDS.includes(name)) {
    return HPC_FIELD_IMPORTANCE.ENRICHMENT;
  }
  if (/Flag$/i.test(name) || /Source Text$/i.test(name) || /Source URL$/i.test(name)) {
    return HPC_FIELD_IMPORTANCE.ENRICHMENT;
  }
  if (/Confidence$|Evidence Tier$|Source Type$/i.test(name)) {
    return HPC_FIELD_IMPORTANCE.ENRICHMENT;
  }
  return HPC_FIELD_IMPORTANCE.ENRICHMENT;
}

/**
 * Whether missing this field must block the HPC enrichment stage quality gate.
 */
export function isCriticalHpcBlocker(fieldName) {
  return classifyHpcFieldImportance(fieldName) === HPC_FIELD_IMPORTANCE.CRITICAL;
}

/**
 * Whether Core AI monitoring may proceed despite this field remaining open.
 */
export function allowsCoreMonitoringDespiteGap(fieldName) {
  const imp = classifyHpcFieldImportance(fieldName);
  return (
    imp === HPC_FIELD_IMPORTANCE.ENRICHMENT ||
    imp === HPC_FIELD_IMPORTANCE.IMPORTANT ||
    imp === HPC_FIELD_IMPORTANCE.INTERNAL_WORKFLOW ||
    imp === HPC_FIELD_IMPORTANCE.PROVIDER_SPECIFIC
  );
}

export function buildImportanceCsvRows() {
  const names = new Set([
    ...HPC_CRITICAL_FIELDS,
    ...HPC_IMPORTANT_FIELDS,
    ...HPC_ENRICHMENT_TARGET_FIELDS,
  ]);
  return [...names].sort().map((field) => {
    const importance = classifyHpcFieldImportance(field);
    return {
      field,
      importance,
      blocksDependentAdpStage: importance === HPC_FIELD_IMPORTANCE.CRITICAL ? "YES" : "NO",
      allowsCoreMonitoring:
        importance === HPC_FIELD_IMPORTANCE.CRITICAL ? "NO" : "YES",
      dependencyNotes:
        importance === HPC_FIELD_IMPORTANCE.CRITICAL
          ? "Identity/geo integrity — fail-closed hpc_onboarding_enrichment"
          : importance === HPC_FIELD_IMPORTANCE.IMPORTANT
            ? "Mark analyses; keep research obligation; Core may proceed if min identity ready"
            : "Enrichment backlog — never treat hotel as fully enriched while open",
    };
  });
}
