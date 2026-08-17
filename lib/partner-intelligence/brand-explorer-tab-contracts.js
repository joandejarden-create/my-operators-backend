/**
 * Permanent Brand Explorer Tab Contracts.
 * Tab → Section → Component → Field with required/optional/suppressible + pass/fail rules.
 * Source of field inventory: brand-explorer-rendered-field-completeness-inventory.js
 */
import {
  ALL_INVENTORY_FIELDS,
  BENCHMARK_BRANDS,
  COMPLIANCE_FIELDS,
  FLEXIBILITY_FIELDS,
  FOOTPRINT_FIELDS,
  LIFECYCLE_FIELDS,
  MOMENTUM_AND_MIX_FIELDS,
  OPENING_PATH_FIELDS,
  OPERATIONS_MODEL_FIELDS,
  OVERVIEW_CONTENT_SLOTS,
  OWNER_CONSIDERATIONS_FIELDS,
  POSITIONING_FIELDS,
  PROTECTED_BRANDS,
  SIMILAR_BRAND_FIELDS,
  SNAPSHOT_KV_FIELDS,
  TARGET_BRANDS,
} from "./brand-explorer-rendered-field-completeness-inventory.js";

export const TAB_FACTORY_VERSION = "tab-factory-v1";

export {
  TARGET_BRANDS as TAB_FACTORY_TARGET_BRANDS,
  PROTECTED_BRANDS as TAB_FACTORY_PROTECTED_BRANDS,
  BENCHMARK_BRANDS as TAB_FACTORY_BENCHMARK_BRANDS,
};

/** Field resolution outcomes (every audited field must land in one). */
export const FIELD_RESOLUTION_STATES = Object.freeze([
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

export const TAB_CONTRACT_FAIL_RULES = Object.freeze({
  visible_empty_field: "Visible label/value pair with blank value is a hard fail.",
  visible_empty_card: "Visible card with title-only or empty body is a hard fail.",
  visible_empty_phase: "Visible lifecycle/opening phase box with empty body is a hard fail.",
  visible_empty_bar: "Visible flexibility indicator with empty fill is a hard fail.",
  unsupported_zero: "Unsupported 0 hotels/rooms/pipeline is a hard fail.",
  parent_source_overuse: "Brand-specific sections mostly on parent umbrella URLs is a hard fail.",
  generic_interchangeable_copy: "Copy that could apply to another brand by name-swap is a hard fail.",
  duplicate_scenario_image: "Duplicate scenario images without justification is a hard fail.",
});

/**
 * Canonical tab contract matrix grouped by atelier tab.
 * Each field inherits inventory metadata + requirement flags.
 */
function contractField(inv, extras = {}) {
  return Object.freeze({
    ...inv,
    required: extras.required !== false,
    optional: extras.optional === true,
    suppressible: extras.suppressible === true || inv.suppressible === true,
    brandSpecificSourceRequired: extras.brandSpecificSourceRequired === true,
    passRule:
      extras.passRule ||
      "Must be complete, intentionally suppressed, cleanly unavailable, or patched before founder_review_ready.",
    failRule: extras.failRule || TAB_CONTRACT_FAIL_RULES.visible_empty_field,
  });
}

export const TAB_CONTRACTS = Object.freeze([
  {
    tabId: "atelier-overview",
    tabName: "Overview",
    sections: [
      {
        sectionName: "Brand Snapshot",
        fields: SNAPSHOT_KV_FIELDS.map((f) =>
          contractField(f, {
            brandSpecificSourceRequired: ["snapshot.brand_website", "snapshot.relative_positioning"].includes(
              f.fieldId
            ),
          })
        ),
      },
      {
        sectionName: "Brand Positioning",
        fields: POSITIONING_FIELDS.map((f) => contractField(f, { brandSpecificSourceRequired: true })),
      },
      {
        sectionName: "Value Creation & Proof",
        fields: OVERVIEW_CONTENT_SLOTS.map((f) =>
          contractField(f, {
            brandSpecificSourceRequired: /^overview\.(scenario|proof|why|differentiator)/.test(f.fieldId),
            failRule: TAB_CONTRACT_FAIL_RULES.visible_empty_card,
          })
        ),
      },
    ],
  },
  {
    tabId: "atelier-value-owners",
    tabName: "Value to Owners",
    sections: [
      {
        sectionName: "Support Across the Lifecycle",
        fields: LIFECYCLE_FIELDS.map((f) =>
          contractField(f, {
            brandSpecificSourceRequired: true,
            failRule: TAB_CONTRACT_FAIL_RULES.visible_empty_phase,
          })
        ),
      },
    ],
  },
  {
    tabId: "atelier-ops",
    tabName: "Operations & Standards",
    sections: [
      {
        sectionName: "Operating Model",
        fields: OPERATIONS_MODEL_FIELDS.filter((f) => f.fieldId.startsWith("operations.model.")).map((f) =>
          contractField(f)
        ),
      },
      {
        sectionName: "Standards Philosophy",
        fields: OPERATIONS_MODEL_FIELDS.filter((f) => f.fieldId === "operations.standards_philosophy").map((f) =>
          contractField(f, { brandSpecificSourceRequired: true })
        ),
      },
      {
        sectionName: "Flexibility Indicators",
        fields: FLEXIBILITY_FIELDS.map((f) =>
          contractField(f, {
            suppressible: true,
            failRule: TAB_CONTRACT_FAIL_RULES.visible_empty_bar,
          })
        ),
      },
      {
        sectionName: "Third-Party Operator Compatibility",
        fields: OPERATIONS_MODEL_FIELDS.filter((f) => f.fieldId.startsWith("operations.operator_compat.")).map(
          (f) => contractField(f, { brandSpecificSourceRequired: true })
        ),
      },
      {
        sectionName: "Compliance & Oversight",
        fields: COMPLIANCE_FIELDS.map((f) => contractField(f)),
      },
    ],
  },
  {
    tabId: "atelier-standards-owner",
    tabName: "Owner Considerations",
    sections: [
      {
        sectionName: "Owner Considerations",
        fields: OWNER_CONSIDERATIONS_FIELDS.map((f) => contractField(f, { brandSpecificSourceRequired: true })),
      },
    ],
  },
  {
    tabId: "atelier-economics",
    tabName: "Economics & Obligations",
    sections: [
      {
        sectionName: "Opening & Conversion Path",
        fields: OPENING_PATH_FIELDS.map((f) =>
          contractField(f, {
            brandSpecificSourceRequired: true,
            failRule: TAB_CONTRACT_FAIL_RULES.visible_empty_phase,
          })
        ),
      },
    ],
  },
  {
    tabId: "atelier-footprint",
    tabName: "Footprint & Growth",
    sections: [
      {
        sectionName: "Geographic Footprint & Growth",
        fields: FOOTPRINT_FIELDS.map((f) => contractField(f, { brandSpecificSourceRequired: true })),
      },
      {
        sectionName: "Recent Momentum & Portfolio Mix",
        fields: MOMENTUM_AND_MIX_FIELDS.map((f) => contractField(f, { suppressible: true, optional: true })),
      },
    ],
  },
  {
    tabId: "atelier-insight",
    tabName: "Dealality Insight",
    sections: [
      {
        sectionName: "Similar Brands",
        fields: SIMILAR_BRAND_FIELDS.map((f) => contractField(f, { failRule: TAB_CONTRACT_FAIL_RULES.visible_empty_card })),
      },
    ],
  },
]);

export const ALL_TAB_CONTRACT_FIELDS = Object.freeze(
  TAB_CONTRACTS.flatMap((t) => t.sections.flatMap((s) => s.fields))
);

/** Ensure inventory and contracts stay aligned. */
export function assertTabContractsCoverInventory() {
  const contractIds = new Set(ALL_TAB_CONTRACT_FIELDS.map((f) => f.fieldId));
  const missing = ALL_INVENTORY_FIELDS.filter((f) => !contractIds.has(f.fieldId)).map((f) => f.fieldId);
  return { ok: missing.length === 0, missing };
}

export function listTabsForContract() {
  return TAB_CONTRACTS.map((t) => ({ tabId: t.tabId, tabName: t.tabName, fieldCount: t.sections.reduce((n, s) => n + s.fields.length, 0) }));
}
