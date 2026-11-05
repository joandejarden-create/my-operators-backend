/**
 * GDI_UNIFIED_COMMERCIAL_INVENTORY_CONTRACT_V1
 *
 * Shared commercial eligibility + customer vs research inventory split.
 * Hotel-agnostic. No hard-coded opportunity-count ceilings.
 *
 * Retain customer-facing when evidence-grounded:
 *  - credible future hotel demand (SALES_READY), OR
 *  - meaningful Account Development with realistic future pathway
 *
 * Exclude historical-only / PAST_CLOSED / competitor-exclusive / speculative
 * from customer-facing; preserve in research ledger (never delete).
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  createGdiPublishedInventoryRegistry,
  COMMERCIAL_PUBLICATION_CLASS,
} from "../durable-publication/gdi-published-inventory-registry-v1.js";
import { resolveGdiPublishedInventoryRoot } from "../durable-publication/resolve-gdi-published-registry-root-v1.js";

export const GDI_UNIFIED_COMMERCIAL_INVENTORY_CONTRACT_V1 =
  "gdi-unified-commercial-inventory-contract-v1";

export const COMMERCIAL_INVENTORY_CLASS = Object.freeze({
  SALES_READY: "SALES_READY",
  ACCOUNT_DEVELOPMENT: "ACCOUNT_DEVELOPMENT",
  WATCH: "WATCH",
  RESEARCH_CONTINUATION: "RESEARCH_CONTINUATION",
  REJECTED: "REJECTED",
});

export const GDI_INVENTORY_VIEW = Object.freeze({
  CUSTOMER_OPPORTUNITIES: "customer",
  RESEARCH_INVENTORY: "research",
});

export const CUSTOMER_FACING_COMMERCIAL_CLASSES = Object.freeze([
  COMMERCIAL_INVENTORY_CLASS.SALES_READY,
  COMMERCIAL_INVENTORY_CLASS.ACCOUNT_DEVELOPMENT,
]);

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const EXCLUSIONS_DIR = path.join(
  ROOT,
  "data/group-demand-intelligence/commercial-inventory-exclusions",
);

const PAST_CLOSED_RE =
  /^(PAST_CLOSED|HISTORICAL_ONLY|HISTORICAL|CLOSED_NO_FUTURE|NO_FUTURE_PATHWAY)$/i;

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function upper(v) {
  return text(v).toUpperCase();
}

/**
 * Load durable founder commercial exclusions for a hotel (research preserved).
 * Shape: { hotelId, exclusions: [{ opportunityId, reason, disposition, excludedAt }] }
 */
export function loadCommercialInventoryExclusions(hotelId, opts = {}) {
  const id = text(hotelId);
  if (!id) return { hotelId: id, exclusions: [], byId: new Map() };
  const dir = opts.exclusionsDir || EXCLUSIONS_DIR;
  const file = path.join(dir, `${id}.json`);
  if (!fs.existsSync(file)) {
    return { hotelId: id, exclusions: [], byId: new Map(), path: file };
  }
  try {
    const doc = JSON.parse(fs.readFileSync(file, "utf8"));
    const exclusions = Array.isArray(doc.exclusions) ? doc.exclusions : [];
    const byId = new Map(
      exclusions
        .filter((e) => e && e.opportunityId)
        .map((e) => [String(e.opportunityId), e]),
    );
    return { hotelId: id, exclusions, byId, path: file, updatedAt: doc.updatedAt || null };
  } catch {
    return { hotelId: id, exclusions: [], byId: new Map(), path: file, error: "parse_failed" };
  }
}

/**
 * True when opportunity is historical-only / past-closed with no credible future pathway.
 * Does not require fixed event dates to remain customer-facing for AD / SR.
 */
export function isHistoricalOnlyCustomerExclusion(opp = {}, opts = {}) {
  if (!opp || typeof opp !== "object") return false;

  const hotelId = text(opts.hotelId || opp.hotelId);
  if (hotelId) {
    const ex = loadCommercialInventoryExclusions(hotelId, opts);
    if (ex.byId.has(String(opp.id || opp.opportunityId || ""))) return true;
  }

  // Explicit durable stamps
  if (opp.customerFacingCommercialExcluded === true) return true;
  if (opp.historicalOnlyCustomerExclusion === true) return true;
  if (opp.pastClosed === true && opp.futureDemandPathway !== true) return true;

  const dispositionCandidates = [
    opp.commercialDisposition,
    opp.disposition,
    opp.customerReadinessDisposition,
    opp.acceptanceDisposition,
    opp.commercialAcceptanceDisposition,
    opp.surfaceDisposition,
    opp.exclusionDisposition,
    opp.rejectedReason,
    opp.exclusionReason,
    opp.dispositionReasonCode,
  ]
    .map(upper)
    .filter(Boolean);

  if (dispositionCandidates.some((d) => PAST_CLOSED_RE.test(d))) {
    // Allow explicit override when new verified future pathway is stamped.
    if (opp.futureDemandPathway === true || opp.accountDevelopmentReactivated === true) {
      return false;
    }
    return true;
  }

  // Published registry rows are never historical exclusions.
  if (opp.gdiPublishedInventoryRegistry === true) return false;

  return false;
}

export function isCustomerFacingCommercialClass(classification) {
  const c = upper(classification);
  return CUSTOMER_FACING_COMMERCIAL_CLASSES.includes(c);
}

/**
 * When a hotel has published registry rows, customer-facing inventory is the
 * published set only (strict). Research inventory remains the full ledger.
 * Hotel-agnostic — no count ceilings.
 */
export function hotelHasPublishedCustomerInventory(hotelId, opts = {}) {
  const id = text(hotelId);
  if (!id) return false;
  try {
    const registry =
      opts.registry ||
      createGdiPublishedInventoryRegistry({
        env: opts.env || process.env,
        root: opts.root || resolveGdiPublishedInventoryRoot(opts.env || process.env),
      });
    return registry.listPublished(id).length > 0;
  } catch {
    return false;
  }
}

export function getPublishedCustomerOpportunityIds(hotelId, opts = {}) {
  const id = text(hotelId);
  if (!id) return [];
  try {
    const registry =
      opts.registry ||
      createGdiPublishedInventoryRegistry({
        env: opts.env || process.env,
        root: opts.root || resolveGdiPublishedInventoryRoot(opts.env || process.env),
      });
    return registry.getPublishedOpportunityIds(id);
  } catch {
    return [];
  }
}

/**
 * Apply commercial inventory view selection.
 * - customer: published-strict when registry present; else visibility filter minus historical exclusions
 * - research: full research ledger (caller supplies pre-filtered or raw list)
 */
export function selectCommercialInventoryView(
  hotelId,
  opportunities = [],
  opts = {},
) {
  const view =
    String(opts.inventoryView || opts.inventoryScope || GDI_INVENTORY_VIEW.CUSTOMER_OPPORTUNITIES)
      .trim()
      .toLowerCase() === GDI_INVENTORY_VIEW.RESEARCH_INVENTORY
      ? GDI_INVENTORY_VIEW.RESEARCH_INVENTORY
      : GDI_INVENTORY_VIEW.CUSTOMER_OPPORTUNITIES;

  const rows = Array.isArray(opportunities) ? opportunities : [];
  const exclusions = loadCommercialInventoryExclusions(hotelId, opts);

  if (view === GDI_INVENTORY_VIEW.RESEARCH_INVENTORY) {
    return {
      contractVersion: GDI_UNIFIED_COMMERCIAL_INVENTORY_CONTRACT_V1,
      hotelId,
      inventoryView: view,
      inventoryViewLabel: "Research Inventory",
      opportunities: rows,
      count: rows.length,
      excludedFromCustomer: exclusions.exclusions,
      publishedStrict: false,
    };
  }

  const publishedIds = getPublishedCustomerOpportunityIds(hotelId, opts);
  const publishedStrict = publishedIds.length > 0;
  let facing;
  if (publishedStrict) {
    const allow = new Set(publishedIds);
    facing = rows.filter((o) => allow.has(String(o.id || "")));
  } else {
    facing = rows.filter(
      (o) => !isHistoricalOnlyCustomerExclusion(o, { ...opts, hotelId }),
    );
  }

  // Always drop durable exclusions even if a stale stamp slipped into published merge.
  facing = facing.filter(
    (o) => !exclusions.byId.has(String(o.id || "")) &&
      !isHistoricalOnlyCustomerExclusion(o, { ...opts, hotelId }),
  );

  const classCounts = facing.reduce(
    (acc, o) => {
      const c = upper(o.commercialInventoryClass || o.commercialClassification);
      if (c === COMMERCIAL_PUBLICATION_CLASS.SALES_READY) acc.SALES_READY += 1;
      else if (c === COMMERCIAL_PUBLICATION_CLASS.ACCOUNT_DEVELOPMENT) {
        acc.ACCOUNT_DEVELOPMENT += 1;
      } else acc.OTHER += 1;
      return acc;
    },
    { SALES_READY: 0, ACCOUNT_DEVELOPMENT: 0, OTHER: 0 },
  );

  return {
    contractVersion: GDI_UNIFIED_COMMERCIAL_INVENTORY_CONTRACT_V1,
    hotelId,
    inventoryView: view,
    inventoryViewLabel: "Customer Opportunities",
    opportunities: facing,
    count: facing.length,
    publishedStrict,
    publishedIds,
    classCounts,
    excludedFromCustomer: exclusions.exclusions,
  };
}

/**
 * Guard: historical-only exclusions must not auto-resurrect into customer view.
 */
export function assertHistoricalCannotAutoResurrect(opp = {}, opts = {}) {
  const excluded = isHistoricalOnlyCustomerExclusion(opp, opts);
  if (!excluded) return { ok: true, blocked: false };
  const resurrected =
    opp.customerVisible === true &&
    opp.gdiPublishedInventoryRegistry !== true &&
    opp.futureDemandPathway !== true &&
    opp.accountDevelopmentReactivated !== true;
  return {
    ok: !resurrected,
    blocked: true,
    reason: "HISTORICAL_ONLY_CUSTOMER_EXCLUSION",
    requires: "new_verified_future_demand_or_account_development_pathway",
  };
}
