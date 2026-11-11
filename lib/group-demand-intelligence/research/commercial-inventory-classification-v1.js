/**
 * Commercial inventory classification (presentation / reporting layer).
 *
 * Distinct from researchStatus lifecycle. ACCOUNT_DEVELOPMENT can overlay
 * WATCH or RESEARCH_CONTINUATION without weakening Sales Ready gates.
 *
 * Backward compatible: missing commercialInventoryClass → derive from status.
 */
import { RESEARCH_STATUS } from "./discovery-to-decision/research-status-transitions-v1.js";

export const COMMERCIAL_INVENTORY_CLASSIFICATION_V1 =
  "gdi-commercial-inventory-classification-v1";

export const COMMERCIAL_INVENTORY_CLASS = Object.freeze({
  SALES_READY: "SALES_READY",
  ACCOUNT_DEVELOPMENT: "ACCOUNT_DEVELOPMENT",
  RESEARCH_CONTINUATION: "RESEARCH_CONTINUATION",
  WATCH: "WATCH",
  REJECTED: "REJECTED",
  AWAITING_REVIEW: "AWAITING_REVIEW",
  QUEUED: "QUEUED",
  DUPLICATE: "DUPLICATE",
  UNKNOWN: "UNKNOWN",
});

/**
 * Migration plan (documentation + helper — no Airtable writes).
 */
export const ACCOUNT_DEVELOPMENT_MIGRATION_PLAN = Object.freeze({
  version: COMMERCIAL_INVENTORY_CLASSIFICATION_V1,
  steps: [
    "1. Keep researchStatus as lifecycle SoT (WATCH / RESEARCH_CONTINUATION / AWAITING_FOUNDER_PROMOTION / …).",
    "2. Add optional commercialInventoryClass on ledger/opportunity records.",
    "3. Derive class for legacy rows via resolveCommercialInventoryClass().",
    "4. Optionally promote recurring WATCH/RC rows to researchStatus=ACCOUNT_DEVELOPMENT when explicitly classified (backward-compatible transitions).",
    "5. Customer-facing publication still requires CUSTOMER_FACING + founder promotion — Account Development is not auto-published.",
  ],
  backwardCompatible: true,
  doesNotWeakenSalesReady: true,
});

function text(v) {
  return String(v == null ? "" : v).replace(/\s+/g, " ").trim();
}

export function looksLikeAccountDevelopment(row = {}) {
  if (row.commercialInventoryClass === COMMERCIAL_INVENTORY_CLASS.ACCOUNT_DEVELOPMENT) {
    return true;
  }
  if (row.recurringAccountPotential === true) return true;
  const blob = `${row.organizationName || ""} ${row.programmeName || ""} ${row.workflowId || ""} ${row.segmentId || ""} ${row.disposition?.code || ""}`;
  return /planner|dmc|incentive house|intermediar|preferred.?lodging|account.?development|recurring (?:account|programme|program|meeting)|annual (?:congress|meeting|incentive)/i.test(
    blob,
  );
}

/**
 * Resolve presentation class without mutating the record.
 */
export function resolveCommercialInventoryClass(row = {}) {
  if (
    row.commercialInventoryClass &&
    Object.values(COMMERCIAL_INVENTORY_CLASS).includes(row.commercialInventoryClass)
  ) {
    return {
      class: row.commercialInventoryClass,
      source: "explicit_field",
      version: COMMERCIAL_INVENTORY_CLASSIFICATION_V1,
    };
  }

  const status = String(row.researchStatus || "");
  const imm = String(
    row.immediateVsRecurring || row.pursuitMode || row.salesPursuitClass || "",
  ).toUpperCase();
  const recurringFlag =
    imm.includes("RECURRING") ||
    imm.includes("ACCOUNT_DEVELOPMENT") ||
    looksLikeAccountDevelopment(row);

  if (status === RESEARCH_STATUS.ACCOUNT_DEVELOPMENT) {
    return {
      class: COMMERCIAL_INVENTORY_CLASS.ACCOUNT_DEVELOPMENT,
      source: "derived_from_researchStatus",
      version: COMMERCIAL_INVENTORY_CLASSIFICATION_V1,
    };
  }
  if (
    status === RESEARCH_STATUS.AWAITING_FOUNDER_PROMOTION ||
    status === RESEARCH_STATUS.SALES_READY_INTERNAL ||
    status === RESEARCH_STATUS.CUSTOMER_FACING
  ) {
    // Recurring / intermediary pursuits are Account Development — not weaker
    // Sales Ready, but distinct presentation class (does not auto-publish).
    if (recurringFlag && status !== RESEARCH_STATUS.CUSTOMER_FACING) {
      return {
        class: COMMERCIAL_INVENTORY_CLASS.ACCOUNT_DEVELOPMENT,
        source: "derived_recurring_overlay_on_staged_sales_ready",
        version: COMMERCIAL_INVENTORY_CLASSIFICATION_V1,
        note: "Staged Sales Ready + recurring pursuit → Account Development presentation class.",
      };
    }
    return {
      class: COMMERCIAL_INVENTORY_CLASS.SALES_READY,
      source: "derived_from_researchStatus",
      version: COMMERCIAL_INVENTORY_CLASSIFICATION_V1,
    };
  }
  if (status === RESEARCH_STATUS.REJECTED) {
    return {
      class: COMMERCIAL_INVENTORY_CLASS.REJECTED,
      source: "derived_from_researchStatus",
      version: COMMERCIAL_INVENTORY_CLASSIFICATION_V1,
    };
  }
  if (status === RESEARCH_STATUS.DUPLICATE) {
    return {
      class: COMMERCIAL_INVENTORY_CLASS.DUPLICATE,
      source: "derived_from_researchStatus",
      version: COMMERCIAL_INVENTORY_CLASSIFICATION_V1,
    };
  }
  if (status === RESEARCH_STATUS.QUEUED_FOR_DEEP_RESEARCH) {
    return {
      class: COMMERCIAL_INVENTORY_CLASS.QUEUED,
      source: "derived_from_researchStatus",
      version: COMMERCIAL_INVENTORY_CLASSIFICATION_V1,
    };
  }
  if (
    status === RESEARCH_STATUS.WATCH ||
    status === RESEARCH_STATUS.RESEARCH_CONTINUATION
  ) {
    if (looksLikeAccountDevelopment(row)) {
      return {
        class: COMMERCIAL_INVENTORY_CLASS.ACCOUNT_DEVELOPMENT,
        source: "derived_heuristic_overlay",
        version: COMMERCIAL_INVENTORY_CLASSIFICATION_V1,
        note: "Overlay only — researchStatus unchanged until explicit migration.",
      };
    }
    return {
      class:
        status === RESEARCH_STATUS.WATCH
          ? COMMERCIAL_INVENTORY_CLASS.WATCH
          : COMMERCIAL_INVENTORY_CLASS.RESEARCH_CONTINUATION,
      source: "derived_from_researchStatus",
      version: COMMERCIAL_INVENTORY_CLASSIFICATION_V1,
    };
  }
  if (
    status === RESEARCH_STATUS.DEEP_RESEARCH_INCOMPLETE ||
    status === RESEARCH_STATUS.DEEP_RESEARCH_IN_PROGRESS
  ) {
    return {
      class: COMMERCIAL_INVENTORY_CLASS.AWAITING_REVIEW,
      source: "derived_from_researchStatus",
      version: COMMERCIAL_INVENTORY_CLASSIFICATION_V1,
    };
  }
  return {
    class: COMMERCIAL_INVENTORY_CLASS.UNKNOWN,
    source: "fallback",
    version: COMMERCIAL_INVENTORY_CLASSIFICATION_V1,
    researchStatus: status || null,
  };
}

export function withCommercialInventoryClass(row = {}) {
  const resolved = resolveCommercialInventoryClass(row);
  return {
    ...row,
    commercialInventoryClass: resolved.class,
    commercialInventoryClassMeta: {
      source: resolved.source,
      note: resolved.note || null,
      version: resolved.version,
    },
  };
}
