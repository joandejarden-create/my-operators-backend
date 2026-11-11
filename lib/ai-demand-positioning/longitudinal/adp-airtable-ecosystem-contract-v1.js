/**
 * ADP_AIRTABLE_ECOSYSTEM_CONTRACT_V1
 *
 * Complete governed map of every Airtable base/table that ADP may read or write.
 * Full E2E S9B must consult this contract — not a hard-coded 9-table list.
 *
 * SoT: filesystem remains PRIMARY for exact report reproduction.
 * Airtable = structured index / identity / optional Live overlay / attribute trail.
 */

import { HISTORY_TABLES, EXISTING_LIVE_PUBLISH } from "./airtable-history-schema-final-v1.js";
import { ADP_PUBLISHED_REPORTS_TABLE } from "../airtable-field-map.js";
import { HOTEL_ADP_ATTRIBUTES_TABLE } from "../../hotel-intelligence/adp-attributes/field-map.js";
import { ADP_GOVERNED_BASE_ID } from "./resolve-adp-history-destination-v1.js";

export const ADP_AIRTABLE_ECOSYSTEM_CONTRACT_V1 = "ADP_AIRTABLE_ECOSYSTEM_CONTRACT_V1";

export const TABLE_CLASS = Object.freeze({
  HOTEL_IDENTITY: "HOTEL_IDENTITY",
  MONITORING_EXECUTION: "MONITORING_EXECUTION",
  PROVIDER_OBSERVATIONS: "PROVIDER_OBSERVATIONS",
  CORE_METRICS: "CORE_METRICS",
  TERRITORY_METRICS: "TERRITORY_METRICS",
  COMPETITIVE_INTELLIGENCE: "COMPETITIVE_INTELLIGENCE",
  REALITY_GAP: "REALITY_GAP",
  BPP_LOYALTY: "BPP_LOYALTY",
  EVIDENCE: "EVIDENCE",
  REPORT_ARTIFACTS: "REPORT_ARTIFACTS",
  PUBLICATION: "PUBLICATION",
  HISTORICAL_AUDIT: "HISTORICAL_AUDIT",
  OTHER_ADP_RELATED: "OTHER_ADP_RELATED",
});

/** Authorization scopes — none implies another. */
export const PERSISTENCE_AUTH_SCOPE = Object.freeze({
  MEASUREMENT_HISTORY: "MEASUREMENT_HISTORY",
  /** Live Published Reports overlay / publication lifecycle Airtable sync */
  PUBLICATION_HISTORY: "PUBLICATION_HISTORY",
  /** Alias kept for older call sites */
  CUSTOMER_PUBLICATION: "PUBLICATION_HISTORY",
  ATTRIBUTE_INTELLIGENCE: "ATTRIBUTE_INTELLIGENCE",
  /** Hotel Intelligence Evidence (research trail) — distinct from ADP Evidence Index */
  EVIDENCE_INTELLIGENCE: "EVIDENCE_INTELLIGENCE",
  PROPERTY_IDENTITY: "PROPERTY_IDENTITY",
  DECISION_HISTORY: "DECISION_HISTORY",
  DECISION_OUTCOMES: "DECISION_HISTORY",
  PRODUCTION_DEPLOYMENT: "PRODUCTION_DEPLOYMENT",
  EXTERNAL_SHARE: "EXTERNAL_SHARE",
  LEAK_AUDIT_ISOLATED: "LEAK_AUDIT_ISOLATED",
  OTHER_GOVERNED_SCOPE: "OTHER_GOVERNED_SCOPE",
});

/**
 * Canonical ADP-related table registry (code + live discovery expected).
 * fullE2ePopulate: whether S9B must prepare/sync under MEASUREMENT_HISTORY (or listed scope).
 */
export const ADP_ECOSYSTEM_TABLES = Object.freeze([
  {
    tableName: "Hotel Property Census",
    baseRole: "PLATFORM_ALT",
    expectedBaseEnv: "AIRTABLE_BASE_ID_ALT",
    class: TABLE_CLASS.HOTEL_IDENTITY,
    purpose: "Canonical hotel identity (HPC rec id) for ADP census link",
    sot: "AIRTABLE_PRODUCTION",
    fullE2ePopulate: false,
    fullE2eRequireLink: true,
    authScope: PERSISTENCE_AUTH_SCOPE.PROPERTY_IDENTITY,
    writer: "lib/hotel-census/hpc-authorized-create-reuse-v1.js",
    reader: "lib/hotel-census/hpc-live-identity-collision-v1.js",
    cardinality: "1_PER_HOTEL",
    publicationOnly: false,
    deprecated: false,
  },
  {
    tableName: ADP_PUBLISHED_REPORTS_TABLE,
    baseRole: "INTELLIGENCE_ADP",
    expectedBaseId: ADP_GOVERNED_BASE_ID,
    class: TABLE_CLASS.PUBLICATION,
    purpose: "Current Live published report overlay (NOT longitudinal SoT)",
    sot: "FS_PRIMARY_OPTIONAL_OVERLAY",
    fullE2ePopulate: false,
    fullE2eRequireLink: false,
    authScope: PERSISTENCE_AUTH_SCOPE.PUBLICATION_HISTORY,
    writer: "lib/ai-demand-positioning/airtable-published-report.js#upsertPublishedReportToAirtable",
    reader: "lib/ai-demand-positioning/published-read-service.js",
    cardinality: "0_OR_1_LIVE_PER_PROPERTY",
    publicationOnly: true,
    deprecated: false,
    longitudinalSafe: EXISTING_LIVE_PUBLISH.longitudinalSafe,
  },
  {
    tableName: HISTORY_TABLES.MONITORING_PERIODS,
    baseRole: "INTELLIGENCE_ADP",
    expectedBaseId: ADP_GOVERNED_BASE_ID,
    class: TABLE_CLASS.MONITORING_EXECUTION,
    purpose: "Append-only Core/Loyalty/BPP monitoring period index",
    sot: "AIRTABLE_INDEX_FS_ARCHIVE_PRIMARY",
    fullE2ePopulate: true,
    authScope: PERSISTENCE_AUTH_SCOPE.MEASUREMENT_HISTORY,
    writer: "lib/ai-demand-positioning/longitudinal/persist-adp-historical-period-v1.js",
    reader: "longitudinal assess gate",
    cardinality: "1_PLUS_PER_PROPERTY_PERIOD_FAMILY",
    publicationOnly: false,
    deprecated: false,
  },
  {
    tableName: HISTORY_TABLES.REPORT_SNAPSHOTS,
    baseRole: "INTELLIGENCE_ADP",
    expectedBaseId: ADP_GOVERNED_BASE_ID,
    class: TABLE_CLASS.REPORT_ARTIFACTS,
    purpose: "Snapshot metadata + store ref (full JSON on disk)",
    sot: "AIRTABLE_INDEX_FS_BLOB_PRIMARY",
    fullE2ePopulate: true,
    authScope: PERSISTENCE_AUTH_SCOPE.MEASUREMENT_HISTORY,
    writer: "persist-adp-historical-period-v1.js",
    cardinality: "1_PLUS_PER_SNAPSHOT",
    publicationOnly: false,
    deprecated: false,
  },
  {
    tableName: HISTORY_TABLES.PERIOD_METRICS,
    baseRole: "INTELLIGENCE_ADP",
    expectedBaseId: ADP_GOVERNED_BASE_ID,
    class: TABLE_CLASS.CORE_METRICS,
    purpose: "Period KPI index (consideration, scenario presence, RG, etc.)",
    sot: "AIRTABLE_INDEX_FS_CERTIFIED_PRIMARY",
    fullE2ePopulate: true,
    authScope: PERSISTENCE_AUTH_SCOPE.MEASUREMENT_HISTORY,
    writer: "persist-adp-historical-period-v1.js (+ BPP persist-bpp-historical-period-v1)",
    cardinality: "1_PLUS_PER_PROPERTY_PERIOD_FAMILY_VERSION",
    publicationOnly: false,
    deprecated: false,
    methodologyField: "Disclosures JSON / Certification Status (Methodology ID via payload)",
  },
  {
    tableName: HISTORY_TABLES.TERRITORY_METRICS,
    baseRole: "INTELLIGENCE_ADP",
    expectedBaseId: ADP_GOVERNED_BASE_ID,
    class: TABLE_CLASS.TERRITORY_METRICS,
    purpose: "Traveler-intent / territory KPI rows",
    sot: "AIRTABLE_INDEX",
    fullE2ePopulate: true,
    authScope: PERSISTENCE_AUTH_SCOPE.MEASUREMENT_HISTORY,
    writer: "persist-adp-historical-period-v1.js",
    cardinality: "N_PER_TERRITORY_PER_PERIOD",
    publicationOnly: false,
    deprecated: false,
  },
  {
    tableName: HISTORY_TABLES.PROVIDER_METRICS,
    baseRole: "INTELLIGENCE_ADP",
    expectedBaseId: ADP_GOVERNED_BASE_ID,
    class: TABLE_CLASS.CORE_METRICS,
    purpose: "Provider-level presence (ChatGPT/Gemini/Claude/Perplexity)",
    sot: "AIRTABLE_INDEX",
    fullE2ePopulate: true,
    authScope: PERSISTENCE_AUTH_SCOPE.MEASUREMENT_HISTORY,
    writer: "persist-adp-historical-period-v1.js",
    cardinality: "4_TYPICAL_PER_PERIOD",
    publicationOnly: false,
    deprecated: false,
  },
  {
    tableName: HISTORY_TABLES.COMPETITIVE_RANKINGS,
    baseRole: "INTELLIGENCE_ADP",
    expectedBaseId: ADP_GOVERNED_BASE_ID,
    class: TABLE_CLASS.COMPETITIVE_INTELLIGENCE,
    purpose: "Full competitive universe ranks (corrected unprompted denominators)",
    sot: "AIRTABLE_INDEX_FS_CERTIFIED_PRIMARY",
    fullE2ePopulate: true,
    authScope: PERSISTENCE_AUTH_SCOPE.MEASUREMENT_HISTORY,
    writer: "persist-adp-historical-period-v1.js",
    cardinality: "N_PER_ENTITY_PER_SCOPE_PER_PERIOD",
    publicationOnly: false,
    deprecated: false,
  },
  {
    tableName: HISTORY_TABLES.EVIDENCE_INDEX,
    baseRole: "INTELLIGENCE_ADP",
    expectedBaseId: ADP_GOVERNED_BASE_ID,
    class: TABLE_CLASS.EVIDENCE,
    purpose: "Evidence refs to FS observation blobs (not verbatim in Airtable)",
    sot: "AIRTABLE_INDEX_FS_BLOB_PRIMARY",
    fullE2ePopulate: true,
    authScope: PERSISTENCE_AUTH_SCOPE.MEASUREMENT_HISTORY,
    writer: "persist-adp-historical-period-v1.js",
    cardinality: "N_PER_EVIDENCE_CARD",
    publicationOnly: false,
    deprecated: false,
  },
  {
    tableName: HISTORY_TABLES.REPORT_CORRECTIONS,
    baseRole: "INTELLIGENCE_ADP",
    expectedBaseId: ADP_GOVERNED_BASE_ID,
    class: TABLE_CLASS.HISTORICAL_AUDIT,
    purpose: "Append-only correction provenance",
    sot: "AIRTABLE_AUDIT",
    fullE2ePopulate: true,
    authScope: PERSISTENCE_AUTH_SCOPE.MEASUREMENT_HISTORY,
    writer: "persist-adp-historical-period-v1.js (correctionMode)",
    cardinality: "SPARSE_PER_CORRECTION",
    publicationOnly: false,
    deprecated: false,
    neverOverwrite: true,
  },
  {
    tableName: HISTORY_TABLES.PROMPT_LEDGER,
    baseRole: "INTELLIGENCE_ADP",
    expectedBaseId: ADP_GOVERNED_BASE_ID,
    liveNameAliases: ["ADP Observation Prompt Ledger"],
    class: TABLE_CLASS.PROVIDER_OBSERVATIONS,
    purpose: "Prompt integrity index (exact prompt on disk)",
    sot: "AIRTABLE_INDEX_FS_PROMPT_PRIMARY",
    fullE2ePopulate: true,
    authScope: PERSISTENCE_AUTH_SCOPE.MEASUREMENT_HISTORY,
    writer:
      "persist-adp-historical-period-v1.js (Core when `period` passed) + persist-bpp-historical-period-v1.js (BPP)",
    cardinality: "N_PER_OBSERVATION",
    publicationOnly: false,
    deprecated: false,
    note: "Live table name is ADP Observation Prompt Ledger; apply map uses Prompt Ledger Key",
  },
  {
    tableName: HOTEL_ADP_ATTRIBUTES_TABLE,
    baseRole: "INTELLIGENCE_ADP",
    expectedBaseId: ADP_GOVERNED_BASE_ID,
    class: TABLE_CLASS.REALITY_GAP,
    purpose: "Governed attribute trail feeding Reality Gap (not period metrics)",
    sot: "AIRTABLE_PLUS_FS_PROFILE",
    fullE2ePopulate: false,
    fullE2eRequireReadiness: true,
    authScope: PERSISTENCE_AUTH_SCOPE.ATTRIBUTE_INTELLIGENCE,
    writer: "lib/hotel-intelligence/adp-attributes/airtable-store.js + sync:hotel-adp-attributes",
    cardinality: "N_PER_ATTRIBUTE_PER_HOTEL",
    publicationOnly: false,
    deprecated: false,
  },
  {
    tableName: "Hotel Intelligence Evidence",
    baseRole: "INTELLIGENCE_ADP",
    expectedBaseId: ADP_GOVERNED_BASE_ID,
    class: TABLE_CLASS.EVIDENCE,
    purpose: "Hotel-intelligence source evidence (attribute/commercial research)",
    sot: "AIRTABLE_PLUS_FS",
    fullE2ePopulate: false,
    authScope: PERSISTENCE_AUTH_SCOPE.EVIDENCE_INTELLIGENCE,
    writer: "lib/hotel-intelligence/evidence-store.js / research pipelines",
    cardinality: "N_PER_EVIDENCE_ITEM",
    publicationOnly: false,
    deprecated: false,
  },
  {
    tableName: "Hotel Commercial Profiles",
    baseRole: "INTELLIGENCE_ADP",
    expectedBaseId: ADP_GOVERNED_BASE_ID,
    class: TABLE_CLASS.OTHER_ADP_RELATED,
    purpose: "Commercial/physical profile facts (GDI+ADP shared intelligence)",
    sot: "AIRTABLE",
    fullE2ePopulate: false,
    authScope: PERSISTENCE_AUTH_SCOPE.ATTRIBUTE_INTELLIGENCE,
    writer: "hotel-intelligence research",
    cardinality: "0_OR_1_PER_HOTEL",
    publicationOnly: false,
    deprecated: false,
  },
  {
    tableName: "Decisions",
    baseRole: "INTELLIGENCE_ADP",
    expectedBaseId: ADP_GOVERNED_BASE_ID,
    class: TABLE_CLASS.OTHER_ADP_RELATED,
    purpose: "Shared decision ledger (GDI+ADP module-neutral)",
    sot: "AIRTABLE",
    fullE2ePopulate: false,
    authScope: PERSISTENCE_AUTH_SCOPE.DECISION_HISTORY,
    writer: "lib/decision-outcomes/*",
    cardinality: "EVENT_DRIVEN",
    publicationOnly: false,
    deprecated: false,
  },
  {
    tableName: "Decision Events",
    baseRole: "INTELLIGENCE_ADP",
    expectedBaseId: ADP_GOVERNED_BASE_ID,
    class: TABLE_CLASS.OTHER_ADP_RELATED,
    purpose: "Decision event stream",
    sot: "AIRTABLE",
    fullE2ePopulate: false,
    authScope: PERSISTENCE_AUTH_SCOPE.DECISION_HISTORY,
    writer: "lib/decision-outcomes/*",
    cardinality: "EVENT_DRIVEN",
    publicationOnly: false,
    deprecated: false,
  },
  {
    tableName: "Group Demand Opportunities",
    baseRole: "INTELLIGENCE_ADP",
    expectedBaseId: ADP_GOVERNED_BASE_ID,
    class: TABLE_CLASS.OTHER_ADP_RELATED,
    purpose: "GDI opportunities (same base; not Full ADP Core metrics)",
    sot: "AIRTABLE",
    fullE2ePopulate: false,
    authScope: null,
    writer: "lib/group-demand-intelligence/opportunity-persistence.js",
    cardinality: "N_PER_OPPORTUNITY",
    publicationOnly: false,
    deprecated: false,
    note: "Do not treat as ADP measurement store",
  },
  // Proposed / abandoned — must NOT be populated
  {
    tableName: "ADP Evidence Observations",
    baseRole: "PROPOSED_NEVER_CREATED",
    class: TABLE_CLASS.EVIDENCE,
    purpose: "Legacy proposal (verbatim in AT) — superseded by Evidence Index + FS blobs",
    sot: "DEPRECATED_PROPOSAL",
    fullE2ePopulate: false,
    deprecated: true,
    writer: null,
  },
  {
    tableName: "ADP Trend History",
    baseRole: "PROPOSED_NEVER_CREATED",
    class: TABLE_CLASS.HISTORICAL_AUDIT,
    purpose: "Legacy proposal — trends derive from Monitoring Periods + Period Metrics",
    sot: "DEPRECATED_PROPOSAL",
    fullE2ePopulate: false,
    deprecated: true,
    writer: null,
  },
]);

/** Tables S9B must sync under MEASUREMENT_HISTORY authorization */
export function listFullE2eMeasurementHistoryTables() {
  return ADP_ECOSYSTEM_TABLES.filter((t) => t.fullE2ePopulate === true && !t.deprecated);
}

/** Tables that must never receive staged-as-published writes */
export function listPublicationOnlyTables() {
  return ADP_ECOSYSTEM_TABLES.filter((t) => t.publicationOnly === true);
}

export function resolveLiveTableName(contractRow) {
  if (contractRow.liveNameAliases?.length) return contractRow.liveNameAliases[0];
  return contractRow.tableName;
}

/**
 * Negative fixtures for ecosystem completeness.
 */
export function runEcosystemContractNegativeFixtures() {
  const results = [];
  const hist = listFullE2eMeasurementHistoryTables();
  results.push({
    id: "S9B_INCLUDES_PROMPT_LEDGER",
    pass: hist.some((t) => /Prompt Ledger/i.test(t.tableName) || t.liveNameAliases?.some((a) => /Prompt Ledger/i.test(a))),
  });
  results.push({
    id: "PUBLICATION_NOT_IN_HISTORY_POPULATE",
    pass: !hist.some((t) => t.publicationOnly),
  });
  results.push({
    id: "DEPRECATED_NOT_POPULATED",
    pass: !hist.some((t) => t.deprecated),
  });
  results.push({
    id: "ATTRIBUTES_NOT_MEASUREMENT_HISTORY_SCOPE",
    pass: ADP_ECOSYSTEM_TABLES.find((t) => t.tableName === HOTEL_ADP_ATTRIBUTES_TABLE)?.fullE2ePopulate === false,
  });
  results.push({
    id: "NINE_TABLE_LIST_INSUFFICIENT",
    pass: hist.length > 8,
  });
  return { ok: results.every((r) => r.pass), results };
}
