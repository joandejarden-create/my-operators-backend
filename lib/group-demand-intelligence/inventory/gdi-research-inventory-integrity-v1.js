/**
 * Research inventory integrity — detect unexplained Research Preview reductions.
 * Read-only. Does not write stores, packages, Airtable, or publications.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { filterCustomerFacingOpportunities } from "../customer-visibility.js";
import { filterSalespersonView } from "../opportunity-factory.js";

export const GDI_RESEARCH_INVENTORY_INTEGRITY_V1 =
  "gdi-research-inventory-integrity-v1";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");

export const PRIORITY_REGRESSION_HOTELS = Object.freeze([
  Object.freeze({
    hotelId: "recCAGqkERSXgiku4",
    name: "Sheraton Guadalajara Expo",
    historicalResearchEvidence: "~40 founder-visible (store now 43)",
  }),
  Object.freeze({
    hotelId: "recnPB3Q02NO8Ma1o",
    name: "Wyndham San Diego Bayside",
    historicalResearchEvidence: "store 82",
  }),
  Object.freeze({
    hotelId: "rec82D9zpB8fede1I",
    name: "Castillo Hotel Son Vida",
    historicalResearchEvidence: "store 20",
  }),
  Object.freeze({
    hotelId: "recLuxvwwxID7U2B8",
    name: "Bethesda Marriott",
    publishedExpected: 67,
  }),
  Object.freeze({
    hotelId: "recrPQcZg7SFARRb2",
    name: "YOTEL Geneva Lake",
    publishedExpected: 42,
  }),
  Object.freeze({
    hotelId: "recH9h3McgMNFQEla",
    name: "Radisson Blu Toronto Downtown",
    publishedExpected: 66,
  }),
]);

function text(v) {
  return String(v || "").replace(/\s+/g, " ").trim();
}

function readJson(rel) {
  const p = path.join(ROOT, rel);
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function packageCount(hotelId, kind) {
  const ptr = readJson(
    `lib/group-demand-intelligence/bundled-publication-packages/${hotelId}/${kind}.json`,
  );
  if (!ptr) return { exists: false, count: null, version: null, lifecycle: null };
  const version = ptr.publicationVersion || null;
  const pkg = version
    ? readJson(
        `lib/group-demand-intelligence/bundled-publication-packages/${hotelId}/${version}/package.json`,
      )
    : null;
  const count = Array.isArray(pkg?.opportunities)
    ? pkg.opportunities.length
    : pkg?.opportunityCount ?? null;
  return {
    exists: true,
    count,
    version,
    lifecycle: ptr.lifecycleState || null,
  };
}

export function loadHotelResearchStore(hotelId, opts = {}) {
  const root = opts.root || ROOT;
  const p = path.join(
    root,
    "data/group-demand-intelligence/hotels",
    hotelId,
    "opportunities.json",
  );
  if (!fs.existsSync(p)) {
    return { ok: false, hotelId, opportunities: [], count: 0 };
  }
  const doc = JSON.parse(fs.readFileSync(p, "utf8"));
  const opportunities = Array.isArray(doc.opportunities) ? doc.opportunities : [];
  return {
    ok: true,
    hotelId,
    updatedAt: doc.updatedAt || null,
    source: doc.source || null,
    opportunities,
    count: opportunities.length,
    ids: opportunities.map((o) => text(o.id || o.opportunityId)).filter(Boolean),
  };
}

/**
 * Simulate broken Research Preview (customer-facing filter on live store)
 * vs corrected Research Preview (salesperson filter only).
 */
export function diagnoseResearchPreviewFilterTrap(hotelId, opts = {}) {
  const store = loadHotelResearchStore(hotelId, opts);
  const raw = store.opportunities || [];
  const sales = filterSalespersonView(raw);
  const facing = filterCustomerFacingOpportunities(raw, { hotelId });
  const rawIds = new Set(raw.map((o) => text(o.id || o.opportunityId)).filter(Boolean));
  const facingIds = new Set(
    facing.map((o) => text(o.id || o.opportunityId)).filter(Boolean),
  );
  const suppressedIds = [...rawIds].filter((id) => !facingIds.has(id));
  const reduction =
    raw.length > 0 ? Number((((raw.length - facing.length) / raw.length) * 100).toFixed(1)) : 0;
  const largeDrop = raw.length >= 10 && facing.length / Math.max(raw.length, 1) <= 0.5;

  return {
    version: GDI_RESEARCH_INVENTORY_INTEGRITY_V1,
    hotelId,
    storeCount: raw.length,
    researchPreviewCorrectCount: sales.length,
    brokenCustomerFacingCount: facing.length,
    suppressedByCustomerFacingCount: suppressedIds.length,
    suppressedIds,
    reductionPctVsStore: reduction,
    largeUnexplainedDropIfCustomerFacingApplied: largeDrop,
    rootCauseCode: largeDrop
      ? "RESEARCH_PREVIEW_APPLIED_CUSTOMER_FACING_FILTER"
      : facing.length < raw.length
        ? "CUSTOMER_FACING_FILTER_NARROWS_RESEARCH"
        : "NO_CUSTOMER_FACING_REDUCTION",
    staged: packageCount(hotelId, "STAGED"),
    current: packageCount(hotelId, "CURRENT"),
  };
}

export function reconcilePriorityHotelsResearchInventory(opts = {}) {
  const hotels = opts.hotels || PRIORITY_REGRESSION_HOTELS;
  const rows = hotels.map((h) => {
    const diag = diagnoseResearchPreviewFilterTrap(h.hotelId, opts);
    return {
      ...h,
      ...diag,
      recordsPreservedInStore: diag.storeCount > 0,
      opportunitiesActuallyDeleted: false,
      wrongInventoryLoadedIfShowingFacingCount: diag.brokenCustomerFacingCount,
    };
  });
  return {
    version: GDI_RESEARCH_INVENTORY_INTEGRITY_V1,
    generatedAt: new Date().toISOString(),
    safety: {
      paidResearch: false,
      airtableWrites: false,
      publicationPointerChanges: false,
      storeWrites: false,
    },
    rows,
    systemicRootCause:
      "getGdiOpportunities(?live=1) applied filterCustomerFacingOpportunities to founder Research Preview, silently dropping non-customer-facing research rows.",
  };
}

/**
 * Build integrity payload for an API research-preview response.
 */
export function buildResearchPreviewIntegrityMeta({
  hotelId,
  storeCount,
  returnedCount,
  customerFacingWouldHaveReturned,
  suppressedIds = [],
} = {}) {
  const drop =
    storeCount > 0 &&
    typeof customerFacingWouldHaveReturned === "number" &&
    customerFacingWouldHaveReturned < storeCount;
  return {
    version: GDI_RESEARCH_INVENTORY_INTEGRITY_V1,
    mode: "FOUNDER_RESEARCH_PREVIEW",
    hotelId: hotelId || null,
    storeCount: storeCount ?? null,
    returnedCount: returnedCount ?? null,
    customerFacingWouldHaveReturned:
      customerFacingWouldHaveReturned ?? null,
    suppressedByCustomerFacingFilterCount: Array.isArray(suppressedIds)
      ? suppressedIds.length
      : null,
    warning:
      drop &&
      storeCount >= 10 &&
      customerFacingWouldHaveReturned / storeCount <= 0.5
        ? {
            code: "RESEARCH_INVENTORY_WOULD_HAVE_COLLAPSED_UNDER_CUSTOMER_FACING_FILTER",
            message:
              "Customer-facing publication filters would have hidden most research rows. Research Preview correctly bypasses them.",
          }
        : null,
  };
}

export default {
  GDI_RESEARCH_INVENTORY_INTEGRITY_V1,
  PRIORITY_REGRESSION_HOTELS,
  loadHotelResearchStore,
  diagnoseResearchPreviewFilterTrap,
  reconcilePriorityHotelsResearchInventory,
  buildResearchPreviewIntegrityMeta,
};
