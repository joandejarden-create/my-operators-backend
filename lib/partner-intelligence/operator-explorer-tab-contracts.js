/**
 * Operator Explorer Tab Contracts (v1 scaffold).
 *
 * Tab → section inventory aligned to Partner Intelligence operator registry.
 * Field-level pass/fail rules mirror Brand Explorer Tab Factory semantics.
 *
 * Benchmarks: Arbor Lodging + Hotel Equities
 * (lib/partner-intelligence/operator-explorer-quality-baseline.js)
 */
import {
  OPERATOR_QUALITY_BASELINE_OPERATORS,
  OPERATOR_QUALITY_BASELINE_SLUGS,
} from "./operator-explorer-quality-baseline.js";
import { buildFullOperatorExplorerRegistry } from "./operator-explorer-registry-catalog.js";

export const OPERATOR_TAB_FACTORY_VERSION = "operator-tab-factory-v1";

export const OPERATOR_TAB_FACTORY_BENCHMARK_OPERATORS = OPERATOR_QUALITY_BASELINE_OPERATORS;
export const OPERATOR_TAB_FACTORY_PROTECTED_SLUGS = OPERATOR_QUALITY_BASELINE_SLUGS;

/**
 * Publishable Operator Explorer tabs (keep aligned with
 * api/lib/partner-intelligence-explorer-field-registry.js → OPERATOR_EXPLORER_TABS).
 */
export const OPERATOR_PUBLISHABLE_TABS = Object.freeze([
  Object.freeze({ tab: "Profile & Positioning", tabIndex: 1, publishScope: true }),
  Object.freeze({ tab: "Operating Platform", tabIndex: 2, publishScope: true }),
  Object.freeze({ tab: "Brand & Relationships", tabIndex: 3, publishScope: true }),
  Object.freeze({ tab: "Markets & Footprint", tabIndex: 4, publishScope: true }),
  Object.freeze({ tab: "Owner Engagement & Reporting", tabIndex: 5, publishScope: true }),
  Object.freeze({ tab: "Infrastructure & Data", tabIndex: 6, publishScope: true }),
  Object.freeze({ tab: "Leadership", tabIndex: 7, publishScope: true }),
  Object.freeze({ tab: "Project Fit & Deal Profile", tabIndex: 8, publishScope: true }),
  Object.freeze({ tab: "Proof & Track Record", tabIndex: 9, publishScope: true }),
  Object.freeze({ tab: "Operator Materials", tabIndex: 10, publishScope: true }),
]);

/** Field resolution outcomes (every audited field must land in one). */
export const OPERATOR_FIELD_RESOLUTION_STATES = Object.freeze([
  "complete",
  "source_supported_directional",
  "intentionally_suppressed",
  "clean_unavailable_state",
  "needs_patch",
  "blocked_missing_source",
  "blocked_wrong_source",
  "blocked_generic_copy",
  "blocked_empty_render",
]);

export const OPERATOR_TAB_CONTRACT_FAIL_RULES = Object.freeze({
  visible_empty_field: "Visible label/value pair with blank value is a hard fail.",
  visible_empty_card: "Visible card with title-only or empty body is a hard fail.",
  unsupported_zero: "Unsupported 0 counts presented as facts without labeling is a hard fail.",
  parent_source_overuse:
    "CALA-specific sections mostly on unlabeled parent/enterprise URLs is a hard fail.",
  generic_interchangeable_copy:
    "Copy that could apply to another operator by name-swap is a hard fail.",
  below_benchmark_depth:
    "Section thinner than Arbor/Hotel Equities pattern without intentional suppress is a hard fail.",
});

/**
 * Publishable Explorer tabs (excludes Dealality Insights / Alignment Context).
 * @returns {ReadonlyArray<{ tab: string, tabIndex: number, publishScope: boolean }>}
 */
export function listOperatorPublishableTabs() {
  return OPERATOR_PUBLISHABLE_TABS;
}

/**
 * Build a tab → fields contract matrix from the PI registry catalog.
 * @returns {ReadonlyArray<{
 *   tabName: string,
 *   tabIndex: number,
 *   fields: ReadonlyArray<{
 *     fieldKey: string,
 *     explorerSection: string,
 *     displayLabel: string,
 *     prefillKey?: string,
 *     required: boolean,
 *     suppressible: boolean,
 *     passRule: string,
 *     failRule: string
 *   }>
 * }>}
 */
export function buildOperatorTabContractsFromRegistry() {
  const registry = buildFullOperatorExplorerRegistry();
  const byTab = new Map();

  for (const tab of listOperatorPublishableTabs()) {
    byTab.set(tab.tab, {
      tabName: tab.tab,
      tabIndex: tab.tabIndex,
      fields: [],
    });
  }

  for (const field of registry) {
    if (field.publishScope === false) continue;
    const bucket = byTab.get(field.explorerTab);
    if (!bucket) continue;
    bucket.fields.push(
      Object.freeze({
        fieldKey: field.fieldKey,
        explorerSection: field.explorerSection || "",
        displayLabel: field.displayLabel || field.fieldKey,
        prefillKey: field.prefillKey,
        valueType: field.valueType || "text",
        allowGapCopy: field.allowGapCopy === true,
        // Materials overview may be thin until materials table is filled; optional for v1 factory.
        tabFactoryOptional: field.fieldKey === "op.materials.galleryOverview",
        required: field.fieldKey !== "op.materials.galleryOverview",
        suppressible: field.fieldKey === "op.materials.galleryOverview",
        passRule:
          "Must be complete, intentionally suppressed, cleanly unavailable, or patched before founder_review_ready.",
        failRule: OPERATOR_TAB_CONTRACT_FAIL_RULES.visible_empty_field,
      })
    );
  }

  return Object.freeze(
    [...byTab.values()].map((t) =>
      Object.freeze({
        ...t,
        fields: Object.freeze(t.fields),
      })
    )
  );
}

export const OPERATOR_TAB_CONTRACTS = buildOperatorTabContractsFromRegistry();

/**
 * @returns {{
 *   version: string,
 *   publishableTabCount: number,
 *   fieldCount: number,
 *   benchmarkSlugs: string[]
 * }}
 */
export function getOperatorTabFactoryContractSummary() {
  const fieldCount = OPERATOR_TAB_CONTRACTS.reduce((n, t) => n + t.fields.length, 0);
  return {
    version: OPERATOR_TAB_FACTORY_VERSION,
    publishableTabCount: OPERATOR_TAB_CONTRACTS.length,
    fieldCount,
    benchmarkSlugs: [...OPERATOR_TAB_FACTORY_PROTECTED_SLUGS],
  };
}
