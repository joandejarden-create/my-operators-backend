/**
 * Option B Airtable strategic selection persistence contract (implementation-ready).
 * PR-2: PERSISTENCE_DESIGNED — no table creation or production writes.
 */

export const ADP_STRATEGIC_SELECTION_AIRTABLE_CONTRACT_V1 =
  "ADP_STRATEGIC_SELECTION_AIRTABLE_CONTRACT_V1";

export const CONFIG_APPROVAL_STATUS = Object.freeze({
  DRAFT: "DRAFT",
  SUGGESTED: "SUGGESTED",
  REVIEW_REQUIRED: "REVIEW_REQUIRED",
  APPROVED: "APPROVED",
  SUPERSEDED: "SUPERSEDED",
});

export const LINE_STATUS = Object.freeze({
  SUGGESTED: "SUGGESTED",
  SELECTED: "SELECTED",
  DISABLED: "DISABLED",
  REJECTED: "REJECTED",
  REVIEW_REQUIRED: "REVIEW_REQUIRED",
});

export const STRATEGIC_SELECTION_PERSISTENCE_PROPOSAL = Object.freeze({
  contractVersion: ADP_STRATEGIC_SELECTION_AIRTABLE_CONTRACT_V1,
  status: "OPTION_B_DESIGN_APPROVED_IMPLEMENTATION_READY",
  phase: "PR-2",
  productionWritesAuthorized: false,
  schemaCreationAuthorized: false,
  persistenceClassification: "PERSISTENCE_DESIGNED",
  baseId: "appa2cE7FTRmIbB32",
  layers: {
    verifiedFacts: {
      owner: "Hotel ADP Attributes",
      tableId: "tblMA6v0HAmsY9ImW",
      baseId: "appa2cE7FTRmIbB32",
      role: "Layer A facts only — do not treat Used in ADP? as strategic selection SoT",
    },
    strategicSelection: {
      recommendedApproach: "OPTION_B_CONFIG_PLUS_LINES",
      tables: {
        configurations: {
          name: "ADP Priority Attribute Configurations",
          schemaVersion: "adp_priority_attr_cfg_v1",
          fields: [
            { name: "Configuration ID", type: "singleLineText", required: true, role: "primary" },
            { name: "HPC Hotel ID", type: "singleLineText", required: true },
            { name: "ADP Property ID", type: "singleLineText", required: true },
            { name: "Hotel Name", type: "singleLineText" },
            { name: "Configuration Version", type: "number", required: true },
            {
              name: "Approval Status",
              type: "singleSelect",
              options: Object.values(CONFIG_APPROVAL_STATUS),
            },
            { name: "Approver / Steward", type: "singleLineText" },
            { name: "Effective Date", type: "date" },
            { name: "Minimum Policy", type: "number", default: 12 },
            { name: "Preferred Min", type: "number", default: 15 },
            { name: "Preferred Max", type: "number", default: 18 },
            { name: "Maximum Policy", type: "number", default: 20 },
            { name: "Policy Version", type: "singleLineText" },
            { name: "Selected Unique Concept Count", type: "number" },
            { name: "Eligible Unique Concept Count", type: "number" },
            { name: "Inventory Hash", type: "singleLineText" },
            { name: "Attribute Definitions Version", type: "singleLineText" },
            { name: "Supersedes Configuration ID", type: "singleLineText" },
            { name: "Active?", type: "checkbox" },
            { name: "Idempotency Key", type: "singleLineText" },
            { name: "Schema Version", type: "singleLineText" },
            { name: "Notes", type: "multilineText" },
          ],
          uniqueConstraints: [
            "Configuration ID",
            "Idempotency Key",
            "one Active? APPROVED per HPC Hotel ID (app-enforced)",
          ],
        },
        lines: {
          name: "ADP Priority Attribute Lines",
          schemaVersion: "adp_priority_attr_line_v1",
          fields: [
            { name: "Line ID", type: "singleLineText", required: true, role: "primary" },
            { name: "Configuration ID", type: "singleLineText", required: true },
            { name: "HPC Hotel ID", type: "singleLineText", required: true },
            { name: "ADP Property ID", type: "singleLineText", required: true },
            { name: "Governed Attribute Key", type: "singleLineText", required: true },
            { name: "Label", type: "singleLineText" },
            { name: "Semantic Concept ID", type: "singleLineText" },
            {
              name: "Line Status",
              type: "singleSelect",
              options: Object.values(LINE_STATUS),
            },
            { name: "Measurement Enabled?", type: "checkbox" },
            {
              name: "Strategic Priority",
              type: "singleSelect",
              options: ["high", "medium", "low"],
              note: "Management only — not RG weights",
            },
            { name: "Verification Status", type: "singleLineText" },
            {
              name: "Measurement Eligibility",
              type: "singleSelect",
              options: [
                "ELIGIBLE",
                "NEEDS_VERIFICATION",
                "UNDEFINED",
                "CONFLICTING",
                "N_A",
              ],
            },
            { name: "Source Fact Refs", type: "multilineText", note: "JSON pointers to Hotel ADP Attributes / HI Evidence — not duplicated fact bodies" },
            { name: "Evidence URLs", type: "multilineText" },
            { name: "Demand Segment Relevance", type: "multilineText" },
            { name: "Selection Rationale", type: "multilineText" },
            { name: "Recommended Action", type: "singleLineText" },
            { name: "Approved At", type: "dateTime" },
            { name: "Active?", type: "checkbox" },
            { name: "Dedupe Key", type: "singleLineText" },
            { name: "Schema Version", type: "singleLineText" },
          ],
          uniqueConstraints: ["Line ID", "Dedupe Key = HPC::configVersion::attributeKey"],
        },
      },
    },
    researchPeriodSnapshot: {
      owner: "Immutable period / report artifacts (FS) + optional AT index refs",
      role: "Pin approved selection version + inventory hash used for measurement",
    },
  },
  editingLifecycle: {
    states: ["DRAFT", "SUGGESTED", "REVIEW_REQUIRED", "APPROVED", "SUPERSEDED"],
    rules: [
      "Draft edits never mutate the active APPROVED configuration used by live research pins",
      "Approve creates new Configuration Version; prior APPROVED → SUPERSEDED",
      "Fixture migration imports lines as REVIEW_REQUIRED / SELECTED — never auto-APPROVED",
      "Suggestions appear as SUGGESTED lines until steward promotion",
      "Below-min / above-max warnings stored on configuration header (diagnostic)",
      "Approved selection applies to NEW research periods only after explicit cutover authorization",
    ],
  },
  upsertStrategy: {
    idempotencyKey: "adp_selcfg:{hpcHotelId}:v{version}",
    lineDedupeKey: "{hpcHotelId}::{configurationVersion}::{governedAttributeKey}",
    concurrentEditProtection: "version check + Active? uniqueness app-enforced before write",
    rollback: "supersession chain; never delete APPROVED history rows",
  },
  apiUsageEstimate: {
    perHotelApprove: "1 config upsert + N line upserts (typically ≤20) + deactivate stale lines",
    retrieval: "filter by HPC Hotel ID + Active?",
  },
  migrationPolicy: {
    fixtureBaselineImportStatus: "REVIEW_REQUIRED",
    autoApproveFixtureSelections: false,
    note: "Existing fixture attributes[] migrate as review-required, not newly APPROVED configurations",
  },
  persistenceGate: {
    fullE2eRequiresAirtableApprovedSelection: true,
    fixtureOnlyBlocksClassification: "FULL_E2E_VALIDATED",
    pr2Classification: "PERSISTENCE_DESIGNED",
    pr3RequiresFounderAuth: true,
  },
});
