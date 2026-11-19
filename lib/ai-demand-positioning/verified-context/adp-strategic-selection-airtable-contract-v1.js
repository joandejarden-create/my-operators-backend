/**
 * Option B Airtable strategic selection persistence contract (implementation-ready).
 * PR-2: PERSISTENCE_DESIGNED — no table creation or production writes.
 */

import {
  ADP_MEASUREMENT_COVERAGE_POLICY_V1,
  DEFAULT_COVERAGE_LIMITS,
} from "./adp-measurement-coverage-policy-v1.js";
import { ADP_SEMANTIC_EQUIVALENCE_RULES_VERSION } from "./adp-semantic-equivalence-v1.js";

export const ADP_STRATEGIC_SELECTION_AIRTABLE_CONTRACT_V1 =
  "ADP_STRATEGIC_SELECTION_AIRTABLE_CONTRACT_V1";

/** Configuration header lifecycle — single source of truth. */
export const CONFIG_APPROVAL_STATUS = Object.freeze({
  DRAFT: "DRAFT",
  SUGGESTED: "SUGGESTED",
  REVIEW_REQUIRED: "REVIEW_REQUIRED",
  APPROVED: "APPROVED",
  SUPERSEDED: "SUPERSEDED",
});

/**
 * Line lifecycle aligns to configuration states where applicable.
 * REVIEW_REQUIRED is used for fixture migration and steward gates.
 */
export const LINE_STATUS = Object.freeze({
  DRAFT: "DRAFT",
  SUGGESTED: "SUGGESTED",
  REVIEW_REQUIRED: "REVIEW_REQUIRED",
  SELECTED: "SELECTED",
  DISABLED: "DISABLED",
  REJECTED: "REJECTED",
});

export const STRATEGIC_SELECTION_PERSISTENCE_PROPOSAL = Object.freeze({
  contractVersion: ADP_STRATEGIC_SELECTION_AIRTABLE_CONTRACT_V1,
  status: "OPTION_B_DESIGN_APPROVED_IMPLEMENTATION_READY",
  phase: "PR-2",
  productionWritesAuthorized: false,
  schemaCreationAuthorized: false,
  persistenceClassification: "PERSISTENCE_DESIGNED",
  baseId: "appa2cE7FTRmIbB32",
  sharedCoveragePolicy: {
    version: ADP_MEASUREMENT_COVERAGE_POLICY_V1,
    semanticRulesVersion: ADP_SEMANTIC_EQUIVALENCE_RULES_VERSION,
    limits: DEFAULT_COVERAGE_LIMITS,
    note: "Configuration fields must reference this shared policy — no conflicting hard-coded limits",
  },
  layers: {
    verifiedFacts: {
      owner: "Hotel ADP Attributes",
      tableId: "tblMA6v0HAmsY9ImW",
      baseId: "appa2cE7FTRmIbB32",
      role: "Layer A facts only — do not treat Used in ADP? as strategic selection SoT",
    },
    strategicSelection: {
      recommendedApproach: "OPTION_B_CONFIG_PLUS_LINES",
      inventoryScopes: {
        fullCandidateInventory:
          "All attribute lines (suggested + selected + research-needed). NOT limited by maximum 20.",
        approvedMeasurementInventory:
          "Lines with Measurement Enabled?=true on APPROVED config. Unique eligible concepts must satisfy min/max policy.",
      },
      tables: {
        configurations: {
          name: "ADP Priority Attribute Configurations",
          schemaVersion: "adp_priority_attr_cfg_v1_1",
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
              required: true,
            },
            { name: "Approver / Steward", type: "singleLineText" },
            { name: "Effective Date", type: "date" },
            {
              name: "Minimum Eligible Required",
              type: "number",
              default: DEFAULT_COVERAGE_LIMITS.minimum,
              source: ADP_MEASUREMENT_COVERAGE_POLICY_V1,
            },
            {
              name: "Maximum Eligible Allowed",
              type: "number",
              default: DEFAULT_COVERAGE_LIMITS.maximum,
              source: ADP_MEASUREMENT_COVERAGE_POLICY_V1,
            },
            {
              name: "Preferred Range Minimum",
              type: "number",
              default: DEFAULT_COVERAGE_LIMITS.preferredMin,
              source: ADP_MEASUREMENT_COVERAGE_POLICY_V1,
            },
            {
              name: "Preferred Range Maximum",
              type: "number",
              default: DEFAULT_COVERAGE_LIMITS.preferredMax,
              source: ADP_MEASUREMENT_COVERAGE_POLICY_V1,
            },
            {
              name: "Measurement Coverage Policy Version",
              type: "singleLineText",
              default: ADP_MEASUREMENT_COVERAGE_POLICY_V1,
            },
            {
              name: "Semantic Equivalence Rules Version",
              type: "singleLineText",
              default: ADP_SEMANTIC_EQUIVALENCE_RULES_VERSION,
            },
            { name: "Unique Eligible Concept Count", type: "number" },
            {
              name: "Measurement Coverage Compliance Result",
              type: "singleSelect",
              options: [
                "BELOW_MINIMUM_ATTRIBUTE_COVERAGE",
                "MEASUREMENT_COVERAGE_COMPLIANT",
                "ABOVE_MAXIMUM_ATTRIBUTE_COVERAGE",
              ],
            },
            { name: "Below-Minimum Flag", type: "checkbox" },
            { name: "Above-Maximum Flag", type: "checkbox" },
            {
              name: "Approved Measurement Inventory Hash",
              type: "singleLineText",
              note: "Hash of measurement-enabled unique concept keys on APPROVED config",
            },
            { name: "Full Candidate Inventory Count", type: "number" },
            { name: "Attribute Definitions Version", type: "singleLineText" },
            { name: "Supersedes Configuration ID", type: "singleLineText" },
            {
              name: "Active?",
              type: "checkbox",
              note: "At most one Active APPROVED config per HPC Hotel ID",
            },
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
          schemaVersion: "adp_priority_attr_line_v1_1",
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
            {
              name: "Measurement Enabled?",
              type: "checkbox",
              note: "Only measurement-enabled lines enter the approved measurement inventory (≤ max unique concepts)",
            },
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
            {
              name: "Source Fact Refs",
              type: "multilineText",
              note: "JSON pointers to Hotel ADP Attributes / HI Evidence — not duplicated fact bodies",
            },
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
      role: "Pin approved selection version + Approved Measurement Inventory Hash used for measurement",
    },
  },
  editingLifecycle: {
    states: Object.values(CONFIG_APPROVAL_STATUS),
    lineStates: Object.values(LINE_STATUS),
    rules: [
      "DRAFT / SUGGESTED / REVIEW_REQUIRED edits never mutate the Active APPROVED configuration",
      "Approve creates a new Configuration Version; prior Active APPROVED → SUPERSEDED (history retained)",
      "SUPERSEDED configurations remain historically retrievable by Configuration ID / version",
      "Fixture migration imports lines as REVIEW_REQUIRED — never auto-APPROVED",
      "Suggestions appear as SUGGESTED lines until steward promotion",
      "Full candidate inventory may exceed Maximum Eligible Allowed; only Measurement Enabled? lines form the approved measurement inventory",
      "Below-Minimum / Above-Maximum compliance result is diagnostic on the configuration header",
      "Approved measurement inventory applies to NEW research periods only after explicit cutover authorization",
    ],
  },
  upsertStrategy: {
    idempotencyKey: "adp_selcfg:{hpcHotelId}:v{version}",
    lineDedupeKey: "{hpcHotelId}::{configurationVersion}::{governedAttributeKey}",
    concurrentEditProtection: "version check + Active? uniqueness app-enforced before write",
    rollback: "supersession chain; never delete APPROVED/SUPERSEDED history rows",
  },
  apiUsageEstimate: {
    perHotelApprove: "1 config upsert + N line upserts (full candidate inventory) + deactivate stale lines",
    retrieval: "filter by HPC Hotel ID + Active? / Approval Status",
  },
  migrationPolicy: {
    fixtureBaselineImportStatus: CONFIG_APPROVAL_STATUS.REVIEW_REQUIRED,
    lineImportStatus: LINE_STATUS.REVIEW_REQUIRED,
    autoApproveFixtureSelections: false,
    note: "Existing fixture attributes[] migrate as REVIEW_REQUIRED, not newly APPROVED configurations",
  },
  persistenceGate: {
    fullE2eRequiresAirtableApprovedSelection: true,
    fixtureOnlyBlocksClassification: "FULL_E2E_VALIDATED",
    pr2Classification: "PERSISTENCE_DESIGNED",
    pr3RequiresFounderAuth: true,
  },
});
