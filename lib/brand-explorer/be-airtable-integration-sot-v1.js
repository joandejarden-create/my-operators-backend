/**
 * Brand Explorer — Airtable integration & field-level source-of-truth contract V1.
 *
 * Read-only contract for Phase 1. Does not write Airtable, does not populate brands,
 * and does not modify ADP/GDI/HE product code paths.
 *
 * Contract: BE_AIRTABLE_INTEGRATION_SOT_V1
 */

export const BE_AIRTABLE_INTEGRATION_SOT_V1 = "BE_AIRTABLE_INTEGRATION_SOT_V1";

/** Product base (Brand Setup). */
export const BE_PRODUCT_BASE_ENV = "AIRTABLE_BASE_ID";

/** Platform / census / HPC base. */
export const BE_PLATFORM_BASE_ENV = "AIRTABLE_BASE_ID_ALT";

export const BE_BRAND_BASICS_TABLE = "Brand Setup - Brand Basics";
export const BE_PRESENTATION_TABLE = "Brand Setup - Brand Explorer Presentation";
export const BE_FOOTPRINT_SETUP_TABLE = "Brand Setup - Brand Footprint";

export const BE_BRAND_SETUP_CHILD_TABLES = Object.freeze([
  "Brand Setup - Brand Footprint",
  "Brand Setup - Project Fit",
  "Brand Setup - Portfolio & Performance",
  "Brand Setup - Brand Standards",
  "Brand Setup - Fee Structure",
  "Brand Setup - Deal Terms",
  "Brand Setup - Operational Support",
  "Brand Setup - Legal Terms",
  "Brand Setup - Loyalty & Commercial",
  "Brand Setup - Sustainability & ESG",
]);

export const BE_HOTEL_CENSUS_TABLE = "Hotel Census";
export const BE_BRAND_ALIAS_TABLE = "Brand Alias Mapping";
export const BE_HPC_TABLE = "Hotel Property Census";

/** Canonical atelier tab order (must match ATELIER_TAB_DEFS). */
export const BE_CANONICAL_TAB_IDS = Object.freeze([
  "atelier-overview",
  "atelier-value-owners",
  "atelier-ops",
  "atelier-standards-owner",
  "atelier-commercial",
  "atelier-economics",
  "atelier-loyalty",
  "atelier-footprint",
  "atelier-materials",
  "atelier-insight",
]);

/**
 * Ownership roles for a profile surface / metric.
 * FACTORY may research and stage Presentation copy only where owner = factory_or_presentation.
 */
export const BE_FIELD_OWNERS = Object.freeze({
  BRAND_BASICS: "brand_basics",
  BRAND_SETUP_CHILD: "brand_setup_child",
  PRESENTATION: "presentation",
  HOTEL_CENSUS: "hotel_census",
  HPC: "hpc",
  COMPANY_REPORTED: "company_reported",
  FACTORY_RESEARCH: "factory_research",
  ADP_READ_CONTRACT: "adp_read_contract",
  GDI_READ_CONTRACT: "gdi_read_contract",
  DERIVED: "derived",
});

export const BE_WRITE_CLASSES = Object.freeze({
  READ_ONLY: "read_only",
  FACTORY_STAGE_ONLY: "factory_stage_only",
  FOUNDER_PUBLISH: "founder_publish",
  SYSTEM_OWNED_NO_FACTORY_WRITE: "system_owned_no_factory_write",
});

/**
 * Durable brand identity: Brand Basics record id is primary.
 * Aliases and Affiliation strings are matchers, never competing primary keys.
 */
export const BE_IDENTITY_CONTRACT = Object.freeze({
  primaryKey: {
    system: "airtable",
    baseEnv: BE_PRODUCT_BASE_ENV,
    table: BE_BRAND_BASICS_TABLE,
    field: "id",
    description: "Airtable record id (rec…) on Brand Setup - Brand Basics",
  },
  displayNameField: "Brand Name",
  universeFilter: "OR({Brand Status}='Active', {Brand Status}='Live')",
  parentCompanyField: "Parent Company",
  aliasTable: {
    baseEnv: BE_PLATFORM_BASE_ENV,
    table: BE_BRAND_ALIAS_TABLE,
    canonicalField: "Canonical Brand Name",
    aliasField: "Alias / Source Brand Name",
    parentCompanyField: "Parent Company",
    activeField: "Active",
  },
  censusAffiliationJoin: {
    transitionalTable: BE_HOTEL_CENSUS_TABLE,
    affiliationField: "Affiliation",
    parentCompanyField: "Parent Company",
    statusField: "status",
    roomsField: "rooms",
  },
  hpcBrandJoin: {
    designatedTable: BE_HPC_TABLE,
    brandField: "Current Brand",
    affiliationStatusField: "Affiliation Status",
    brandFamilyField: "Brand Family",
    radarStatusField: "Radar Status",
    roomsKeysField: "Rooms / Keys",
    note: "HPC is the designated footprint authority for Brand Explorer when join coverage is certified. Hotel Census rollups remain transitional until HPC brand rollup gates pass.",
  },
  preserveRules: Object.freeze([
    "Never mint a second Brand Basics row for an existing brand identity",
    "Never rewrite Brand Basics record ids",
    "Deduplicate via alias table + exact Affiliation / Current Brand matchers",
    "Regional licensing / operating-rights differences must not collapse distinct brands",
  ]),
});

/**
 * Footprint metric authority. Company-reported Brand Setup numbers are attributed
 * separately and must never replace HPC (or certified transitional census) totals.
 */
export const BE_FOOTPRINT_METRIC_CONTRACT = Object.freeze({
  designatedAuthority: "hpc",
  transitionalAuthority: "hotel_census",
  companyReportedAuthority: "brand_setup_footprint",
  qualifyingHpcStatuses: Object.freeze(["Open", "Pipeline"]),
  statusBuckets: Object.freeze({
    open: Object.freeze(["Open"]),
    pipeline: Object.freeze(["Pipeline"]),
    other: Object.freeze(["Closed", "Inactive", "Unknown", ""]),
  }),
  requiredMetricKeys: Object.freeze([
    "totalOpenHotels",
    "totalOpenKeys",
    "totalPipelineHotels",
    "totalPipelineKeys",
  ]),
  requiredMetadataKeys: Object.freeze([
    "source.table",
    "source.base",
    "source.aggregatedAt",
    "match.canonicalBrandName",
    "match.affiliationMatchers",
    "census.recordsMatched",
    "warnings",
    "fallbackRecommended",
    "coverageLimitations",
  ]),
  displayRules: Object.freeze([
    "When HPC (or transitional Hotel Census with fallbackRecommended=false) is available, show those metrics as Dealality inventory",
    "When Brand Setup Footprint has company-reported figures, show them as separately attributed company-reported metrics",
    "Never present Brand Setup / AI-generated counts as HPC inventory",
    "Always surface coverage limitations and timestamps with derived totals",
  ]),
  factoryWriteRules: Object.freeze([
    "Factory must not overwrite Brand Setup - Brand Footprint numeric inventory fields that are system- or company-owned",
    "Factory may stage qualitative footprint.copy (Presentation slots) only",
    "Factory must not invent open/pipeline hotel or key totals",
  ]),
});

/**
 * Ten-tab field ownership (high level). Presentation slots hold owner-facing copy;
 * Brand Setup child tables hold structured intake; HPC/census own inventory counts.
 */
export const BE_TAB_SOURCE_OF_TRUTH = Object.freeze([
  {
    tabId: "atelier-overview",
    label: "Overview",
    primaryOwner: BE_FIELD_OWNERS.PRESENTATION,
    supportingSources: [BE_FIELD_OWNERS.BRAND_BASICS],
    writeClass: BE_WRITE_CLASSES.FACTORY_STAGE_ONLY,
    notes: "Snapshot KVs may read Basics; owner-value scenarios are Presentation-owned.",
  },
  {
    tabId: "atelier-value-owners",
    label: "Value to Owners",
    primaryOwner: BE_FIELD_OWNERS.PRESENTATION,
    supportingSources: [],
    writeClass: BE_WRITE_CLASSES.FACTORY_STAGE_ONLY,
    notes: "Four valueOwners.scenario cards — Presentation only.",
  },
  {
    tabId: "atelier-ops",
    label: "Operating Model",
    primaryOwner: BE_FIELD_OWNERS.PRESENTATION,
    supportingSources: [BE_FIELD_OWNERS.BRAND_SETUP_CHILD],
    writeClass: BE_WRITE_CLASSES.FACTORY_STAGE_ONLY,
    notes: "Flexibility + operating model copy from Presentation; Brand Standards child is supporting evidence, not silent overwrite target.",
  },
  {
    tabId: "atelier-standards-owner",
    label: "Owner Considerations",
    primaryOwner: BE_FIELD_OWNERS.PRESENTATION,
    supportingSources: [BE_FIELD_OWNERS.BRAND_SETUP_CHILD],
    writeClass: BE_WRITE_CLASSES.FACTORY_STAGE_ONLY,
    notes: "Owner Considerations presentation slots; Project Fit / Standards remain system-owned structured fields.",
  },
  {
    tabId: "atelier-commercial",
    label: "Commercial Engine",
    primaryOwner: BE_FIELD_OWNERS.PRESENTATION,
    supportingSources: [BE_FIELD_OWNERS.BRAND_SETUP_CHILD],
    writeClass: BE_WRITE_CLASSES.FACTORY_STAGE_ONLY,
    notes: "Demand themes / commercial copy. Loyalty & Commercial child table is structured SoT when present.",
  },
  {
    tabId: "atelier-economics",
    label: "Economics & Obligations",
    primaryOwner: BE_FIELD_OWNERS.PRESENTATION,
    supportingSources: [BE_FIELD_OWNERS.BRAND_SETUP_CHILD],
    writeClass: BE_WRITE_CLASSES.FACTORY_STAGE_ONLY,
    notes: "Never fabricate fees. Fee Structure / Deal Terms / Legal Terms are system-owned; factory may only stage clearly attributed public estimates in Presentation.",
  },
  {
    tabId: "atelier-loyalty",
    label: "Loyalty Program",
    primaryOwner: BE_FIELD_OWNERS.PRESENTATION,
    supportingSources: [BE_FIELD_OWNERS.BRAND_SETUP_CHILD],
    writeClass: BE_WRITE_CLASSES.FACTORY_STAGE_ONLY,
    notes: "Loyalty architecture/tiers/benefits — Presentation for explorer; Loyalty & Commercial for structured facts.",
  },
  {
    tabId: "atelier-footprint",
    label: "Footprint & Growth",
    primaryOwner: BE_FIELD_OWNERS.HPC,
    supportingSources: [
      BE_FIELD_OWNERS.HOTEL_CENSUS,
      BE_FIELD_OWNERS.COMPANY_REPORTED,
      BE_FIELD_OWNERS.PRESENTATION,
    ],
    writeClass: BE_WRITE_CLASSES.SYSTEM_OWNED_NO_FACTORY_WRITE,
    notes: "Open/Pipeline hotel+keys from HPC (designated) or transitional Hotel Census. Presentation owns qualitative openings/momentum/region narrative only.",
  },
  {
    tabId: "atelier-materials",
    label: "Brand Materials",
    primaryOwner: BE_FIELD_OWNERS.PRESENTATION,
    supportingSources: [],
    writeClass: BE_WRITE_CLASSES.FACTORY_STAGE_ONLY,
    notes: "Gallery + brand files via Presentation / asset governance — no HPC numeric overwrite.",
  },
  {
    tabId: "atelier-insight",
    label: "Dealality Insight",
    primaryOwner: BE_FIELD_OWNERS.PRESENTATION,
    supportingSources: [BE_FIELD_OWNERS.ADP_READ_CONTRACT, BE_FIELD_OWNERS.GDI_READ_CONTRACT],
    writeClass: BE_WRITE_CLASSES.FACTORY_STAGE_ONLY,
    notes: "Similar brands + Dealality insight copy. Future ADP/GDI brand-level panels attach via read contracts only.",
  },
]);

/**
 * Extensible read-only integration stubs — do not invent metrics.
 */
export const BE_EXTERNAL_READ_CONTRACTS = Object.freeze({
  adpBrandLevel: {
    status: "RESERVED_READ_ONLY",
    consumer: "Brand Explorer",
    producer: "ADP",
    allowed: false,
    joinKey: "brandBasicsRecordId",
    note: "Future brand-level ADP summaries may attach by Brand Basics id or certified brand slug. No speculative metrics in V1.",
  },
  gdiBrandLevel: {
    status: "RESERVED_READ_ONLY",
    consumer: "Brand Explorer",
    producer: "GDI",
    allowed: false,
    joinKey: "brandBasicsRecordId",
    note: "Future GDI brand-level opportunity aggregates may attach by Brand Basics id. No speculative metrics in V1. Do not modify GDI workstream.",
  },
});

export const BE_PUBLICATION_LIFECYCLE = Object.freeze([
  "DISCOVERED",
  "RESEARCHING",
  "RESEARCH_COMPLETE",
  "DRAFT_GENERATED",
  "VALIDATION_REQUIRED",
  "READY_FOR_FOUNDER_REVIEW",
  "FOUNDER_APPROVED",
  "PUBLISHED",
]);

export const BE_BLOCKING_STATUSES = Object.freeze([
  "BLOCKED",
  "STALE",
  "REFRESH_REQUIRED",
  "RESEARCH_INCOMPLETE",
]);

export const BE_FACTORY_SAFETY = Object.freeze({
  noBulkPopulationUntil: Object.freeze([
    "canonical_identity_verified",
    "airtable_joins_verified",
    "hpc_footprint_rules_verified",
    "field_ownership_contract_merged",
  ]),
  preserveApprovedProfiles: true,
  dryRunDefault: true,
  noAutoPublish: true,
  noCompetingDatabase: true,
});

export function listTabIds() {
  return [...BE_CANONICAL_TAB_IDS];
}

export function getTabSoT(tabId) {
  return BE_TAB_SOURCE_OF_TRUTH.find((t) => t.tabId === tabId) || null;
}

export function assertCanonicalTabOrder(tabIds) {
  const expected = BE_CANONICAL_TAB_IDS;
  if (!Array.isArray(tabIds) || tabIds.length !== expected.length) {
    return {
      ok: false,
      error: `expected ${expected.length} tabs, got ${Array.isArray(tabIds) ? tabIds.length : "non-array"}`,
    };
  }
  for (let i = 0; i < expected.length; i += 1) {
    if (tabIds[i] !== expected[i]) {
      return { ok: false, error: `tab order drift at ${i}: expected ${expected[i]}, got ${tabIds[i]}` };
    }
  }
  return { ok: true };
}

/**
 * Pure helpers for join / dedupe / status filter regression tests.
 */
export function normalizeExactKey(value) {
  return String(value == null ? "" : value)
    .trim()
    .replace(/\s+/g, " ")
    .toLowerCase();
}

export function dedupeByRecordId(rows) {
  const seen = new Set();
  const out = [];
  for (const row of rows || []) {
    const id = row?.id || row?.recordId;
    if (!id || seen.has(id)) continue;
    seen.add(id);
    out.push(row);
  }
  return out;
}

export function filterHpcRowsForBrandFootprint(rows, { brandMatchers = [], statuses = BE_FOOTPRINT_METRIC_CONTRACT.qualifyingHpcStatuses } = {}) {
  const matchers = new Set((brandMatchers || []).map(normalizeExactKey).filter(Boolean));
  const statusSet = new Set((statuses || []).map(normalizeExactKey));
  const matched = [];
  const excluded = { noBrandMatch: 0, nonQualifyingStatus: 0, missingId: 0 };

  for (const row of rows || []) {
    const id = row?.id || row?.recordId;
    if (!id) {
      excluded.missingId += 1;
      continue;
    }
    const brand = normalizeExactKey(row.currentBrand || row["Current Brand"] || row.brand);
    if (matchers.size && !matchers.has(brand)) {
      excluded.noBrandMatch += 1;
      continue;
    }
    const status = normalizeExactKey(row.radarStatus || row["Radar Status"] || row.status);
    if (!statusSet.has(status)) {
      excluded.nonQualifyingStatus += 1;
      continue;
    }
    matched.push(row);
  }

  return { matched: dedupeByRecordId(matched), excluded };
}

export function aggregateOpenPipelineCounts(rows) {
  let totalOpenHotels = 0;
  let totalOpenKeys = 0;
  let totalPipelineHotels = 0;
  let totalPipelineKeys = 0;
  for (const row of rows || []) {
    const status = normalizeExactKey(row.radarStatus || row["Radar Status"] || row.status);
    const keys = Number(row.roomsKeys ?? row["Rooms / Keys"] ?? row.rooms ?? 0);
    const keysSafe = Number.isFinite(keys) && keys > 0 ? keys : 0;
    if (status === "open") {
      totalOpenHotels += 1;
      totalOpenKeys += keysSafe;
    } else if (status === "pipeline") {
      totalPipelineHotels += 1;
      totalPipelineKeys += keysSafe;
    }
  }
  return { totalOpenHotels, totalOpenKeys, totalPipelineHotels, totalPipelineKeys };
}

export function companyReportedMustNotReplaceHpc({ hpcMetrics, companyReported, preferCompanyReported }) {
  if (preferCompanyReported === true && hpcMetrics && companyReported) {
    return {
      ok: false,
      error: "company-reported footprint must not replace HPC/designated inventory metrics",
    };
  }
  return { ok: true };
}

export function buildCoverageLimitations({
  designatedAuthority = BE_FOOTPRINT_METRIC_CONTRACT.designatedAuthority,
  usingTransitional = false,
  unmatchedAliases = [],
  excludedStatuses = [],
  recordsMatched = 0,
}) {
  const limitations = [];
  if (usingTransitional) {
    limitations.push(
      `Using transitional authority '${BE_FOOTPRINT_METRIC_CONTRACT.transitionalAuthority}' until HPC brand rollup is certified (designated='${designatedAuthority}').`,
    );
  }
  if (unmatchedAliases.length) {
    limitations.push(`Unmatched alias/affiliation matchers: ${unmatchedAliases.join(", ")}`);
  }
  if (excludedStatuses.length) {
    limitations.push(`Non-qualifying statuses excluded from totals: ${excludedStatuses.join(", ")}`);
  }
  if (!recordsMatched) {
    limitations.push("No qualifying inventory records matched — do not display fabricated counts.");
  }
  return limitations;
}

export function summarizeContract() {
  return {
    contract: BE_AIRTABLE_INTEGRATION_SOT_V1,
    tabCount: BE_CANONICAL_TAB_IDS.length,
    brandSetupChildTables: BE_BRAND_SETUP_CHILD_TABLES.length,
    footprintDesignatedAuthority: BE_FOOTPRINT_METRIC_CONTRACT.designatedAuthority,
    footprintTransitionalAuthority: BE_FOOTPRINT_METRIC_CONTRACT.transitionalAuthority,
    externalReadContracts: Object.keys(BE_EXTERNAL_READ_CONTRACTS),
    factorySafety: BE_FACTORY_SAFETY,
  };
}
