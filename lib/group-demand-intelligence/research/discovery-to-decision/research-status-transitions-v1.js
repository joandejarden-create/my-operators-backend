/**
 * GDI V37 — research lifecycle statuses.
 * Distinguishes incomplete research from commercial rejection.
 */

export const RESEARCH_STATUS = Object.freeze({
  DISCOVERED_UNSCREENED: "DISCOVERED_UNSCREENED",
  SCREENED_ELIGIBLE: "SCREENED_ELIGIBLE",
  SCREENED_EXCLUDED: "SCREENED_EXCLUDED",
  QUEUED_FOR_DEEP_RESEARCH: "QUEUED_FOR_DEEP_RESEARCH",
  DEEP_RESEARCH_IN_PROGRESS: "DEEP_RESEARCH_IN_PROGRESS",
  DEEP_RESEARCH_INCOMPLETE: "DEEP_RESEARCH_INCOMPLETE",
  RESEARCH_CONTINUATION: "RESEARCH_CONTINUATION",
  WATCH: "WATCH",
  /** Recurring / intermediary account path — not Sales Ready; not discarded. */
  ACCOUNT_DEVELOPMENT: "ACCOUNT_DEVELOPMENT",
  SALES_READY_INTERNAL: "SALES_READY_INTERNAL",
  AWAITING_FOUNDER_PROMOTION: "AWAITING_FOUNDER_PROMOTION",
  CUSTOMER_FACING: "CUSTOMER_FACING",
  REJECTED: "REJECTED",
  DUPLICATE: "DUPLICATE",
});

/** Mutually exclusive terminal-ish statuses for funnel status totals. */
export const MUTUALLY_EXCLUSIVE_STATUS_BUCKETS = Object.freeze([
  RESEARCH_STATUS.DISCOVERED_UNSCREENED,
  RESEARCH_STATUS.SCREENED_ELIGIBLE,
  RESEARCH_STATUS.SCREENED_EXCLUDED,
  RESEARCH_STATUS.QUEUED_FOR_DEEP_RESEARCH,
  RESEARCH_STATUS.DEEP_RESEARCH_IN_PROGRESS,
  RESEARCH_STATUS.DEEP_RESEARCH_INCOMPLETE,
  RESEARCH_STATUS.RESEARCH_CONTINUATION,
  RESEARCH_STATUS.WATCH,
  RESEARCH_STATUS.ACCOUNT_DEVELOPMENT,
  RESEARCH_STATUS.SALES_READY_INTERNAL,
  RESEARCH_STATUS.AWAITING_FOUNDER_PROMOTION,
  RESEARCH_STATUS.CUSTOMER_FACING,
  RESEARCH_STATUS.REJECTED,
  RESEARCH_STATUS.DUPLICATE,
]);

/**
 * Cumulative funnel stages (denominators) — NOT mutually exclusive.
 * A CUSTOMER_FACING candidate also counts in Exa discoveries, parsed, etc.
 */
export const FUNNEL_STAGE = Object.freeze({
  EXA_DISCOVERIES: "exaDiscoveries",
  SUCCESSFULLY_PARSED: "successfullyParsed",
  UNIQUE_CANDIDATES: "uniqueCandidates",
  DUPLICATES: "duplicates",
  SCREENED_OUT: "screenedOut",
  ELIGIBLE_FOR_DEEP: "eligibleForDeepResearch",
  DEEP_RESEARCHED: "deepResearched",
  AWAITING_DEEP: "awaitingDeepResearch",
  RESEARCH_CONTINUATION: "researchContinuation",
  WATCH: "watch",
  SALES_READY_INDEPENDENT: "independentlyAcceptedSalesReady",
  AWAITING_PROMOTION: "awaitingPromotion",
  CUSTOMER_FACING: "customerFacing",
  REJECTED: "rejected",
});

const ALLOWED = Object.freeze({
  [RESEARCH_STATUS.DISCOVERED_UNSCREENED]: [
    RESEARCH_STATUS.SCREENED_ELIGIBLE,
    RESEARCH_STATUS.SCREENED_EXCLUDED,
    RESEARCH_STATUS.DUPLICATE,
    RESEARCH_STATUS.REJECTED,
  ],
  [RESEARCH_STATUS.SCREENED_ELIGIBLE]: [
    RESEARCH_STATUS.QUEUED_FOR_DEEP_RESEARCH,
    RESEARCH_STATUS.DEEP_RESEARCH_INCOMPLETE,
    RESEARCH_STATUS.DUPLICATE,
    RESEARCH_STATUS.SCREENED_EXCLUDED,
  ],
  [RESEARCH_STATUS.SCREENED_EXCLUDED]: [RESEARCH_STATUS.REJECTED],
  [RESEARCH_STATUS.QUEUED_FOR_DEEP_RESEARCH]: [
    RESEARCH_STATUS.DEEP_RESEARCH_IN_PROGRESS,
    RESEARCH_STATUS.DEEP_RESEARCH_INCOMPLETE,
    RESEARCH_STATUS.DUPLICATE,
  ],
  [RESEARCH_STATUS.DEEP_RESEARCH_IN_PROGRESS]: [
    RESEARCH_STATUS.DEEP_RESEARCH_INCOMPLETE,
    RESEARCH_STATUS.RESEARCH_CONTINUATION,
    RESEARCH_STATUS.WATCH,
    RESEARCH_STATUS.ACCOUNT_DEVELOPMENT,
    RESEARCH_STATUS.SALES_READY_INTERNAL,
    RESEARCH_STATUS.REJECTED,
  ],
  [RESEARCH_STATUS.DEEP_RESEARCH_INCOMPLETE]: [
    RESEARCH_STATUS.QUEUED_FOR_DEEP_RESEARCH,
    RESEARCH_STATUS.DEEP_RESEARCH_IN_PROGRESS,
    RESEARCH_STATUS.RESEARCH_CONTINUATION,
    RESEARCH_STATUS.WATCH,
    RESEARCH_STATUS.ACCOUNT_DEVELOPMENT,
    RESEARCH_STATUS.REJECTED,
  ],
  [RESEARCH_STATUS.RESEARCH_CONTINUATION]: [
    RESEARCH_STATUS.WATCH,
    RESEARCH_STATUS.ACCOUNT_DEVELOPMENT,
    RESEARCH_STATUS.SALES_READY_INTERNAL,
    RESEARCH_STATUS.REJECTED,
  ],
  [RESEARCH_STATUS.WATCH]: [
    RESEARCH_STATUS.RESEARCH_CONTINUATION,
    RESEARCH_STATUS.ACCOUNT_DEVELOPMENT,
    RESEARCH_STATUS.SALES_READY_INTERNAL,
    RESEARCH_STATUS.REJECTED,
  ],
  [RESEARCH_STATUS.ACCOUNT_DEVELOPMENT]: [
    RESEARCH_STATUS.WATCH,
    RESEARCH_STATUS.RESEARCH_CONTINUATION,
    RESEARCH_STATUS.SALES_READY_INTERNAL,
    RESEARCH_STATUS.REJECTED,
  ],
  [RESEARCH_STATUS.SALES_READY_INTERNAL]: [
    RESEARCH_STATUS.AWAITING_FOUNDER_PROMOTION,
    RESEARCH_STATUS.CUSTOMER_FACING,
  ],
  [RESEARCH_STATUS.AWAITING_FOUNDER_PROMOTION]: [
    RESEARCH_STATUS.CUSTOMER_FACING,
    RESEARCH_STATUS.SALES_READY_INTERNAL,
  ],
  [RESEARCH_STATUS.CUSTOMER_FACING]: [],
  [RESEARCH_STATUS.REJECTED]: [],
  [RESEARCH_STATUS.DUPLICATE]: [],
});

export function isValidResearchStatus(status) {
  return Object.prototype.hasOwnProperty.call(RESEARCH_STATUS, status) ||
    Object.values(RESEARCH_STATUS).includes(status);
}

export function canTransition(from, to) {
  if (!from) return isValidResearchStatus(to);
  if (from === to) return true;
  const next = ALLOWED[from] || [];
  return next.includes(to);
}

export function appendStatusTransition(record, { status, stage, reasonCode, at, note } = {}) {
  const next = String(status || "").trim();
  if (!isValidResearchStatus(next)) {
    throw new Error(`invalid_research_status:${next}`);
  }
  const prev = record.researchStatus || null;
  if (prev && !canTransition(prev, next)) {
    // Soft-allow historical backfills that jump stages; record the jump.
  }
  const history = Array.isArray(record.statusHistory) ? [...record.statusHistory] : [];
  history.push({
    from: prev,
    to: next,
    stage: stage || null,
    reasonCode: reasonCode || null,
    note: note || null,
    at: at || new Date().toISOString(),
  });
  return {
    ...record,
    researchStatus: next,
    statusHistory: history,
    lastEvaluatedAt: at || new Date().toISOString(),
  };
}

/** Map acceptance disposition strings → research status. */
export function researchStatusFromAcceptanceDisposition(disposition) {
  const d = String(disposition || "")
    .toUpperCase()
    .replace(/\s+/g, "_");
  if (d.includes("SALES_READY")) return RESEARCH_STATUS.SALES_READY_INTERNAL;
  if (d.includes("ACCOUNT_DEVELOPMENT")) return RESEARCH_STATUS.ACCOUNT_DEVELOPMENT;
  if (d.includes("RESEARCH_CONTINUATION")) return RESEARCH_STATUS.RESEARCH_CONTINUATION;
  if (d.includes("WATCH")) return RESEARCH_STATUS.WATCH;
  if (d.includes("REJECT") || d.includes("NOT_CUSTOMER") || d.includes("EXHAUSTED")) {
    return RESEARCH_STATUS.REJECTED;
  }
  return null;
}
