/**
 * GDI Unlimited Opportunity Inventory V1
 *
 * Inventory is unbounded subject to commercial eligibility, research budgets,
 * and technical capacity. Numeric constants below are BATCH / SAFETY controls
 * — never total-inventory ceilings.
 *
 * Does not authorize paid research or customer-facing publication.
 */
export const UNLIMITED_OPPORTUNITY_INVENTORY_V1 =
  "gdi-unlimited-opportunity-inventory-v1";

export const MINIMUM_TARGET_PER_HOTEL = Object.freeze({
  initial: 15,
  stretch: 25,
  isMaximum: false,
});

/** Limit taxonomy for audits and operator docs. */
export const LIMIT_CLASS = Object.freeze({
  DISCOVERY_LIMIT: "DISCOVERY_LIMIT",
  STORAGE_LIMIT: "STORAGE_LIMIT",
  RESEARCH_BATCH_LIMIT: "RESEARCH_BATCH_LIMIT",
  PUBLICATION_LIMIT: "PUBLICATION_LIMIT",
  DISPLAY_LIMIT: "DISPLAY_LIMIT",
  SAFETY_LIMIT: "SAFETY_LIMIT",
});

/**
 * @param {object[]} items
 * @param {{ offset?: number, limit?: number, cursor?: string|null }} opts
 * @returns {{ items: object[], offset: number, limit: number, total: number, nextOffset: number|null, hasMore: boolean }}
 */
export function paginateInventory(items = [], opts = {}) {
  const total = Array.isArray(items) ? items.length : 0;
  const limitRaw = opts.limit == null ? 50 : Number(opts.limit);
  const limit =
    !Number.isFinite(limitRaw) || limitRaw <= 0
      ? total
      : Math.min(Math.floor(limitRaw), Math.max(total, 1));
  let offset = Number(opts.offset || 0);
  if (opts.cursor != null && String(opts.cursor).trim() !== "") {
    const n = Number(opts.cursor);
    if (Number.isFinite(n) && n >= 0) offset = Math.floor(n);
  }
  if (!Number.isFinite(offset) || offset < 0) offset = 0;
  const slice = (items || []).slice(offset, offset + (limit || total));
  const nextOffset = offset + slice.length;
  const hasMore = nextOffset < total;
  return {
    version: UNLIMITED_OPPORTUNITY_INVENTORY_V1,
    items: slice,
    offset,
    limit: limit || total,
    total,
    nextOffset: hasMore ? nextOffset : null,
    nextCursor: hasMore ? String(nextOffset) : null,
    hasMore,
    note: "Pagination batches retrieval only — total inventory is not capped.",
  };
}

/**
 * Merge supporting evidence when deduplicating discovery rows.
 * Never drops unique URLs or excerpts from the loser.
 */
export function mergeDiscoveryEvidence(primary = {}, duplicate = {}) {
  const urls = [
    ...(primary.originalSourceUrls || primary.evidenceUrls || []),
    ...(duplicate.originalSourceUrls || duplicate.evidenceUrls || []),
  ]
    .map((u) => String(u || "").trim())
    .filter(Boolean);
  const uniqueUrls = [...new Set(urls)];
  const excerpts = [
    primary.originalResultExcerpt,
    duplicate.originalResultExcerpt,
    primary.evidenceSummary,
    duplicate.evidenceSummary,
  ]
    .map((e) => String(e || "").trim())
    .filter(Boolean);
  const supportingSources = [
    ...(primary.supportingSources || []),
    ...(duplicate.supportingSources || []),
  ];
  return {
    ...primary,
    originalSourceUrls: uniqueUrls,
    evidenceUrls: uniqueUrls,
    supportingSources,
    evidenceMergeNote:
      "Deduped without discarding additional supporting URLs/excerpts from duplicate row.",
    mergedFromDiscoveryIds: [
      ...new Set(
        [
          ...(primary.mergedFromDiscoveryIds || []),
          primary.discoveryId,
          duplicate.discoveryId,
        ].filter(Boolean),
      ),
    ],
    preservedExcerpts: [...new Set(excerpts)].slice(0, 8),
  };
}

/**
 * Resumable batch over complete eligible inventory.
 * batchSize is a SAFETY/RESEARCH_BATCH control — not an inventory max.
 */
export function takeResumableBatch(items = [], opts = {}) {
  const batchSize = Math.max(1, Number(opts.batchSize || 25));
  const cursor = Math.max(0, Number(opts.cursor || 0));
  const page = paginateInventory(items, { offset: cursor, limit: batchSize });
  return {
    version: UNLIMITED_OPPORTUNITY_INVENTORY_V1,
    batchSize,
    cursor,
    ...page,
    resumeCursor: page.nextOffset,
    completeInventoryProcessed: !page.hasMore && cursor === 0 && page.total <= batchSize
      ? true
      : !page.hasMore,
  };
}

/**
 * Sales Ready / qualified count ≥ 25 must NOT stop research by itself.
 */
export function shouldContinueBeyondMinimum({
  salesReadyOrQualifiedCount = 0,
  remainingAuthorizedBudgetUsd = null,
  unfinishedHighPriorityCount = 0,
  paidResearchAuthorized = false,
} = {}) {
  const atOrAboveStretch =
    Number(salesReadyOrQualifiedCount) >= MINIMUM_TARGET_PER_HOTEL.stretch;
  const budgetOk =
    remainingAuthorizedBudgetUsd == null
      ? false // unknown budget → do not auto-continue paid work
      : Number(remainingAuthorizedBudgetUsd) > 0;
  const hasQueue = Number(unfinishedHighPriorityCount) > 0;
  return {
    version: UNLIMITED_OPPORTUNITY_INVENTORY_V1,
    atOrAboveStretchMinimum: atOrAboveStretch,
    continueResearch:
      paidResearchAuthorized === true && budgetOk && hasQueue,
    reason: !paidResearchAuthorized
      ? "PAID_RESEARCH_NOT_AUTHORIZED"
      : !budgetOk
        ? "NO_REMAINING_AUTHORIZED_BUDGET"
        : !hasQueue
          ? "NO_UNFINISHED_HIGH_PRIORITY_QUEUE"
          : atOrAboveStretch
            ? "CONTINUE_BEYOND_MINIMUM_TARGET_WHILE_VALUE_AND_BUDGET_REMAIN"
            : "CONTINUE_TOWARD_MINIMUM_TARGET",
    minimumTarget: MINIMUM_TARGET_PER_HOTEL,
  };
}

/**
 * Sort / filter helpers for large inventories (stable, no hard caps).
 */
export function sortInventory(items = [], { key = "score", direction = "desc" } = {}) {
  const dir = direction === "asc" ? 1 : -1;
  return [...(items || [])].sort((a, b) => {
    const av = a?.[key];
    const bv = b?.[key];
    if (av == null && bv == null) return 0;
    if (av == null) return 1;
    if (bv == null) return -1;
    if (typeof av === "number" && typeof bv === "number") return (av - bv) * dir;
    return String(av).localeCompare(String(bv)) * dir;
  });
}

export function filterInventory(items = [], predicate) {
  if (typeof predicate !== "function") return [...(items || [])];
  return (items || []).filter(predicate);
}
