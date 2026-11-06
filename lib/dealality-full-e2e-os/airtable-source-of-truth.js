/**
 * Canonical Airtable / artifact ownership matrix.
 * Field names and table names MUST come from existing schema docs / field maps —
 * never invent Airtable identifiers here.
 */

export const AIRTABLE_SOT_MATRIX_VERSION = "dealality-airtable-sot-matrix-v1";

/**
 * Ownership matrix derived from existing repo schemas (docs + field maps).
 * `status`: PRODUCTION | OPTIONAL_OVERLAY | PROPOSED | FS_PRIMARY
 */
export const AIRTABLE_SOURCE_OF_TRUTH_MATRIX = Object.freeze([
  {
    domain: "Hotel Property Census",
    owner: "Hotel Property Census table (main product base)",
    tableHint: "Hotel Property Census",
    tableIdHint: "tbl9aY5ijiuIzzWam",
    fieldAuthority: "docs/* census / hotel-census modules",
    gdiUse: "Canonical hotel identity (HPC rec id)",
    adpUse: "Census Record ID / Linked Census Property on published report",
    status: "PRODUCTION",
    notes: "Dealality geography authoritative; do not invent fields",
  },
  {
    domain: "GDI opportunities",
    owner: "Group Demand Opportunities",
    tableHint: "Group Demand Opportunities",
    fieldAuthority: "lib/group-demand-intelligence/opportunity-field-map.js",
    schemaVersion: "gdi_opportunity_airtable_v1",
    persistFacade: "lib/group-demand-intelligence/opportunity-persistence.js",
    status: "PRODUCTION",
    notes:
      "FS mirror optional via GDI_OPPORTUNITIES_FS_*; Airtable is canonical when configured",
  },
  {
    domain: "GDI candidate research / segment research status",
    owner: "FS segment-research-state + workflow artifacts; research-coverage Airtable adjacent",
    fieldAuthority:
      "lib/group-demand-intelligence/research/* + research-coverage/airtable-*",
    status: "PRODUCTION",
    notes: "Segment sufficiency is durable under data/group-demand-intelligence/hotels/<id>/",
  },
  {
    domain: "GDI evidence references",
    owner: "Opportunity payload JSON + evidence artifacts (FS) + optional Airtable fields",
    fieldAuthority: "opportunityPayloadJson + sourceSummary fields",
    status: "PRODUCTION",
  },
  {
    domain: "GDI contact/buyer intelligence",
    owner: "Opportunity contact fields + Surfe ledger (FS)",
    fieldAuthority: "MAP_GDI_OPPORTUNITY primaryContact*",
    status: "PRODUCTION",
  },
  {
    domain: "GDI Sales Intelligence Packets",
    owner: "Corporate package persist + PE canonical persist modules",
    fieldAuthority:
      "workflow/corporate-package-persist-v1.js + private-events airtable",
    status: "PRODUCTION",
  },
  {
    domain: "GDI qualification results",
    owner: "Eligibility + ready.ok on opportunity records / payloads",
    fieldAuthority:
      "prospect-publication-eligibility-v1 + customer-readiness-gate-v1",
    status: "PRODUCTION",
    notes: "customerVisible ≠ research completion",
  },
  {
    domain: "ADP observation metadata",
    owner: "Filesystem runtime corpora",
    pathHint: "data/ai-demand-positioning/runtime/",
    status: "FS_PRIMARY",
    notes: "Raw monitoring stays outside Airtable by policy",
  },
  {
    domain: "ADP run and metric summaries / published report",
    owner: "FS published snapshots + optional Airtable live overlay",
    tableHint: "AI Demand Positioning - Published Reports",
    fieldAuthority:
      "docs/ai-demand-positioning-airtable-fields.md + airtable-field-map.js",
    schemaVersion: "adp published report v1 (proposed ensure script)",
    status: "FS_PRIMARY",
    overlay: "OPTIONAL_OVERLAY",
    notes: "PRIMARY_SOT=filesystem per adp-persistence-policy.js",
  },
  {
    domain: "ADP competitor and report intelligence",
    owner: "Entity registries in code + competitive-history FS",
    pathHint: "data/ai-demand-positioning/competitive-history/",
    status: "FS_PRIMARY",
  },
  {
    domain: "Published report versions",
    owner: "manifest.json pointers under published/<propertyId>/",
    status: "FS_PRIMARY",
    notes: "Do not overwrite without publication authorization",
  },
  {
    domain: "Workflow run registry",
    owner: "FS run journals under reports/gdi-* and reports/adp-* + OS os-run-state.json",
    status: "FS_PRIMARY",
    gap: "No dedicated Airtable Workflow Runs table required for v1 OS — propose only if ops demand",
  },
  {
    domain: "Publication status",
    owner: "GDI actionStatus/customer flags; ADP Publish Status select on overlay",
    status: "PRODUCTION",
  },
  {
    domain: "Customer feedback",
    owner: "GDI feedback / customer-validation APIs + Airtable where configured",
    status: "PRODUCTION",
    notes: "Preserve stable IDs across refresh",
  },
]);

export const SCHEMA_GAP_AND_MIGRATION_PLAN = Object.freeze({
  version: AIRTABLE_SOT_MATRIX_VERSION,
  principles: Object.freeze([
    "Do not invent base IDs, table names, or field mappings",
    "Reuse existing tables where suitable",
    "Propose additional tables only when contracts cannot be met",
    "Mutate schemas only after dependency inventory + dry-run ensure scripts",
    "Every write: hotel-scoped, product-scoped, schema-validated, version-aware, idempotent, auditable",
    "After write: read back IDs, hashes, counts, FKs, hotel identity, publication state",
  ]),
  gaps: Object.freeze([
    {
      id: "workflow_run_registry_airtable",
      severity: "optional",
      recommendation:
        "Keep FS run journals as SoT for OS v1; add Airtable Workflow Runs only if multi-operator ops require it",
      action: "DEFER",
    },
    {
      id: "gdi_segment_status_airtable_columns",
      severity: "low",
      recommendation:
        "Segment research state remains FS-primary; ensure research-coverage tables stay advisory unless productized",
      action: "DOCUMENT",
    },
    {
      id: "adp_airtable_overlay_not_primary",
      severity: "info",
      recommendation:
        "Keep FS published snapshots primary; enable Airtable overlay only with ADP_AIRTABLE_* flags + read-back",
      action: "KEEP_POLICY",
    },
    {
      id: "publication_lifecycle_explicit_field",
      severity: "medium",
      recommendation:
        "Encode RESEARCH_STAGED → DELIVERY_VERIFIED in OS journals; map onto existing GDI/ADP visibility fields without inventing new Airtable enums until ensure-* script adds them",
      action: "OS_JOURNAL_FIRST",
    },
  ]),
  migrationSteps: Object.freeze([
    "Inventory live base via ensure:* --dry-run scripts",
    "Diff proposed fields against docs/*-airtable-fields.md",
    "Shadow-write OS journal fields to FS",
    "Optional: ensure schema apply with founder approval",
    "Enable read-back gate in publication stage",
    "Canary on Charlotte GDI staged + Charlotte ADP staged (no live overwrite)",
  ]),
});

export function getAirtableSotDocument() {
  return Object.freeze({
    version: AIRTABLE_SOT_MATRIX_VERSION,
    matrix: AIRTABLE_SOURCE_OF_TRUTH_MATRIX,
    migration: SCHEMA_GAP_AND_MIGRATION_PLAN,
  });
}
