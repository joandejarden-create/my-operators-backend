/**
 * Search coverage matrix — IMPLEMENTED | PARTIAL | MISSING | NOT_APPLICABLE
 */

import { SEARCH_TAXONOMY_VERSION, SEARCH_FAMILIES } from "./search-taxonomy-v2.js";
import { HISTORICAL_FORM_TYPES } from "./filing-discovery-v2.js";
import { DEFAULT_FORM_TYPES } from "./filing-discovery-v1.js";

export const SEARCH_COVERAGE_MATRIX_VERSION = "sec-edgar-search-coverage-matrix-v2";

export const COVERAGE_STATUS = Object.freeze({
  IMPLEMENTED: "IMPLEMENTED",
  PARTIAL: "PARTIAL",
  MISSING: "MISSING",
  NOT_APPLICABLE: "NOT_APPLICABLE",
});

export function buildSearchCoverageMatrix() {
  const rows = [
    {
      area: "SEC registrants and historical CIKs",
      status: COVERAGE_STATUS.PARTIAL,
      evidence:
        "V1 pilot 4 cos + V2 registry (Hilton, DiamondRock, Park, RLJ, Apple, PEB, CLDT, XHR, INN). Live ticker→CIK resolve. Predecessor CIK graph incomplete.",
      gap: "Need systematic predecessor/successor CIK mapping and dissolved-entity harvest.",
    },
    {
      area: "Company aliases and predecessor names",
      status: COVERAGE_STATUS.PARTIAL,
      evidence: "Brand-family aliases + f/k/a taxonomy patterns. No dedicated registrant alias registry beyond CIK map.",
      gap: "Registrant legal-name alias table (e.g. Hilton Hotels Corp → Hilton Worldwide).",
    },
    {
      area: "Filing form types (10-K, 8-K, S-11, 424B, …)",
      status: COVERAGE_STATUS.IMPLEMENTED,
      evidence: `V1 forms=${DEFAULT_FORM_TYPES.length}; V2 historical forms=${HISTORICAL_FORM_TYPES.length} incl S-11/424B/DEF 14A.`,
      gap: null,
    },
    {
      area: "Exhibit document types (EX-10, schedules)",
      status: COVERAGE_STATUS.PARTIAL,
      evidence:
        "EX-10 treated as form match when present in submissions.form; primaryDocument download. No exhibit-index crawl of all EX-10.* within a filing package.",
      gap: "Parse filing index.json / FilingSummary to enumerate exhibits separately from form types.",
    },
    {
      area: "Historical filing years",
      status: COVERAGE_STATUS.PARTIAL,
      evidence:
        "V1 pilot used ~2-year window (sinceYear = current-2). V2 default sinceYear=1994 + additional submissions shards. Pilot runner still can pass narrow windows.",
      gap: "Make complete eligible inventory the default path; keep short windows only for smoke tests.",
    },
    {
      area: "Additional historical submission files",
      status: COVERAGE_STATUS.IMPLEMENTED,
      evidence: "loadAllSubmissionBodies + getSubmissionsAdditionalFile.",
      gap: null,
    },
    {
      area: "Complete eligible filing inventory before download",
      status: COVERAGE_STATUS.PARTIAL,
      evidence: "listHistoricalFilings returns eligibleCount + remainingInQueue; batch limit still truncates returned set.",
      gap: "Persist full eligible inventory artifact per CIK before any document fetch.",
    },
    {
      area: "Search-term families (14 taxonomy families)",
      status: COVERAGE_STATUS.IMPLEMENTED,
      evidence: `${SEARCH_FAMILIES.length} versioned families in search-taxonomy-v2 (${SEARCH_TAXONOMY_VERSION}).`,
      gap: "Continuous learning loop to promote high-yield phrases.",
    },
    {
      area: "Legacy 8 contract keyword patterns",
      status: COVERAGE_STATUS.IMPLEMENTED,
      evidence: "Still in contract-extractor-v1; taxonomy now expands extractor patterns.",
      gap: "Do not rely on these 8 alone — already mitigated by taxonomy + schedule extractors.",
    },
    {
      area: "Hotel name variants",
      status: COVERAGE_STATUS.PARTIAL,
      evidence: "hotel-discovery-v1 + brand families + schedule extractor. Historical aliases field present but sparsely populated.",
      gap: "Systematic alias capture from f/k/a / conversion language.",
    },
    {
      area: "Legal entity and subsidiary names",
      status: COVERAGE_STATUS.PARTIAL,
      evidence: "Taxonomy patterns; HI Organizations persist path when matched. No subsidiary graph extractor.",
      gap: "Subsidiary-name extractor from Exhibit 21 / org charts.",
    },
    {
      area: "Contract terminology",
      status: COVERAGE_STATUS.IMPLEMENTED,
      evidence: "Taxonomy families signing/effectiveness/expiration/termination/renewal + verification classes.",
      gap: null,
    },
    {
      area: "Ownership and management terminology",
      status: COVERAGE_STATUS.PARTIAL,
      evidence: "Taxonomy + opportunity/relationship planner. Limited owner/operator entity resolution from prose.",
      gap: "Named-entity owner/operator linker with evidence spans.",
    },
    {
      area: "Brand conversion terminology",
      status: COVERAGE_STATUS.PARTIAL,
      evidence: "Taxonomy brand_conversions + brand-family detector. Conversion events rarely persisted as structured milestones.",
      gap: "Structured conversion event findings with before/after brand.",
    },
    {
      area: "Geographic variations",
      status: COVERAGE_STATUS.PARTIAL,
      evidence: "US city/state parsers; country field default United States. Intl readiness adapter stub only.",
      gap: "Non-US geography normalization for 20-F/6-K property lists.",
    },
    {
      area: "Structured property-table extraction",
      status: COVERAGE_STATUS.IMPLEMENTED,
      evidence: "franchise-schedule-extractor-v2 (DiamondRock-style) + hotel-discovery schedule lines.",
      gap: "More table layouts (HTML <table>, XBRL).",
    },
    {
      area: "Structured agreement-exhibit extraction",
      status: COVERAGE_STATUS.PARTIAL,
      evidence: "Deterministic contract extractor on primary docs; EX-10 exhibit package crawl incomplete.",
      gap: "Exhibit-index driven EX-10.* download + extract.",
    },
    {
      area: "Adaptive registrant↔hotel↔owner expansion",
      status: COVERAGE_STATUS.PARTIAL,
      evidence: "adaptive-discovery-queue-v2 module; deduped queue with priority caps.",
      gap: "Auto-enqueue only after verified entity extraction quality gates.",
    },
    {
      area: "HPC hotel → SEC company reverse path",
      status: COVERAGE_STATUS.PARTIAL,
      evidence: "Seed HPC candidates + name match; reverse identity→CIK search not fully automated.",
      gap: "HPC Affiliation/Operator → ticker/CIK resolve → enqueue.",
    },
    {
      area: "V1 pilot limits (2-year window, 6/12 filings)",
      status: COVERAGE_STATUS.PARTIAL,
      evidence: "Still present on V1 pilot script for smoke/cost control. V2 historical runner uses batch/checkpoint instead of universe caps.",
      gap: "Document V1 limits as smoke-only; V2 inventory-first is the production research path.",
    },
    {
      area: "Fair-access / cache / checkpoint",
      status: COVERAGE_STATUS.IMPLEMENTED,
      evidence: "edgar-client rate limit + cache; research-checkpoint-v2.",
      gap: null,
    },
    {
      area: "Paid AI ceiling $25",
      status: COVERAGE_STATUS.IMPLEMENTED,
      evidence: "budget-guard-v1; paid AI off by default.",
      gap: null,
    },
    {
      area: "Durable unmatched hotel inventory",
      status: COVERAGE_STATUS.IMPLEMENTED,
      evidence: "data/sec-edgar/discovery-inventory/hotels.json + MH inventory export.",
      gap: "Airtable unmatched-hotel table requires founder schema approval.",
    },
    {
      area: "Customer publication",
      status: COVERAGE_STATUS.NOT_APPLICABLE,
      evidence: "Blocked by PUBLICATION_GATE; not in scope.",
      gap: null,
    },
  ];

  const counts = { IMPLEMENTED: 0, PARTIAL: 0, MISSING: 0, NOT_APPLICABLE: 0 };
  for (const r of rows) counts[r.status] = (counts[r.status] || 0) + 1;

  return {
    version: SEARCH_COVERAGE_MATRIX_VERSION,
    taxonomyVersion: SEARCH_TAXONOMY_VERSION,
    generatedAt: new Date().toISOString(),
    summary: counts,
    rows,
    pilotLimitNotes: {
      v1_sinceYear_default: "currentYear - 3 (filing-discovery-v1)",
      v1_pilot_sinceYear: "currentYear - 2",
      v1_pilot_filing_limits: "REIT 10–12, brand/franchisor 6 (V1 pilot); V2 historical uses batchSize against full eligible inventory",
      v1_contract_keywords: 8,
      v2_taxonomy_families: SEARCH_FAMILIES.length,
      v2_historical_sinceYear_default: 1994,
    },
  };
}

export function coverageMatrixToMarkdown(matrix = buildSearchCoverageMatrix()) {
  const lines = [
    `# SEC EDGAR Search Coverage Matrix`,
    ``,
    `**Version:** \`${matrix.version}\`  `,
    `**Taxonomy:** \`${matrix.taxonomyVersion}\`  `,
    `**Generated:** ${matrix.generatedAt}`,
    ``,
    `| Status | Count |`,
    `|---|---|`,
    ...Object.entries(matrix.summary).map(([k, v]) => `| ${k} | ${v} |`),
    ``,
    `## Pilot limit notes`,
    ``,
    ...Object.entries(matrix.pilotLimitNotes || {}).map(([k, v]) => `- **${k}:** ${v}`),
    ``,
    `## Coverage rows`,
    ``,
    `| Area | Status | Evidence | Gap |`,
    `|---|---|---|---|`,
    ...matrix.rows.map(
      (r) =>
        `| ${r.area} | ${r.status} | ${String(r.evidence).replace(/\|/g, "/")} | ${String(r.gap || "—").replace(/\|/g, "/")} |`,
    ),
    ``,
  ];
  return lines.join("\n");
}
