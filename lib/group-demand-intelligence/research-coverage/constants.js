/**
 * GDI Research Coverage & Target Registry V1 — constants & enums.
 * Persistent weekly research audit layer. Does not redefine opportunity lifecycle.
 */

export const RESEARCH_COVERAGE_VERSION = "gdi_research_coverage_v1";
export const TARGET_ID_PREFIX = "gdirt_";
export const RUN_ID_PREFIX = "gdir_";
export const TARGET_RUN_ID_PREFIX = "gditr_";

export const RESEARCH_TARGETS_TABLE_NAME = "GDI Research Targets";
export const RESEARCH_RUNS_TABLE_NAME = "GDI Research Runs";
export const RESEARCH_TARGET_RUNS_TABLE_NAME = "GDI Research Target Runs";

/** Durable monitoring unit types (hotel-scoped). */
export const TARGET_TYPE = Object.freeze({
  DEMAND_GENERATOR: "DEMAND_GENERATOR",
  PROGRAM: "PROGRAM",
  EVENT_SERIES: "EVENT_SERIES",
  PRIVATE_EVENT_VENUE: "PRIVATE_EVENT_VENUE",
  ASSOCIATION: "ASSOCIATION",
  GOVERNMENT_PROGRAM: "GOVERNMENT_PROGRAM",
  CORPORATE_PROGRAM: "CORPORATE_PROGRAM",
  SPORTS_SERIES: "SPORTS_SERIES",
  TRAINING_PROGRAM: "TRAINING_PROGRAM",
  OFFICIAL_CALENDAR: "OFFICIAL_CALENDAR",
  HOUSING_PAGE: "HOUSING_PAGE",
  VENUE_PAGE: "VENUE_PAGE",
  PROJECT_CONTRACT: "PROJECT_CONTRACT",
  OTHER_MONITORED_SOURCE: "OTHER_MONITORED_SOURCE",
});

export const TARGET_STATUS = Object.freeze({
  ACTIVE: "ACTIVE",
  WATCH: "WATCH",
  LOW_PRIORITY: "LOW_PRIORITY",
  PAUSED: "PAUSED",
  RETIRED: "RETIRED",
});

export const TARGET_PRIORITY = Object.freeze({
  HIGH: "HIGH",
  MEDIUM: "MEDIUM",
  LOW: "LOW",
});

export const RESEARCH_CADENCE = Object.freeze({
  WEEKLY: "WEEKLY",
  BIWEEKLY: "BIWEEKLY",
  MONTHLY: "MONTHLY",
  QUARTERLY: "QUARTERLY",
  EVENT_TIMED: "EVENT_TIMED",
});

export const RUN_TYPE = Object.freeze({
  WEEKLY: "WEEKLY",
  MONTHLY_EXPANSION: "MONTHLY_EXPANSION",
  MANUAL: "MANUAL",
  CANARY: "CANARY",
  BACKFILL: "BACKFILL",
});

export const RUN_STATUS = Object.freeze({
  PLANNED: "PLANNED",
  RUNNING: "RUNNING",
  COMPLETED: "COMPLETED",
  PARTIAL: "PARTIAL",
  FAILED: "FAILED",
});

export const EXECUTION_STATUS = Object.freeze({
  DUE: "DUE",
  RESEARCHED: "RESEARCHED",
  /** Evidence ledger classification without web research — not RESEARCHED. */
  LEDGER_CHECK: "LEDGER_CHECK",
  SKIPPED: "SKIPPED",
  FAILED: "FAILED",
  NOT_DUE: "NOT_DUE",
});

export const RESULT_TYPE = Object.freeze({
  NO_MATERIAL_CHANGE: "NO_MATERIAL_CHANGE",
  /** First-pass baseline: existing inventory verified, not weekly NEW. */
  BASELINE_EXISTING: "BASELINE_EXISTING",
  NEW_SIGNAL: "NEW_SIGNAL",
  SIGNAL_UPDATED: "SIGNAL_UPDATED",
  OPPORTUNITY_CREATED: "OPPORTUNITY_CREATED",
  OPPORTUNITY_UPDATED: "OPPORTUNITY_UPDATED",
  TARGET_RETIRED: "TARGET_RETIRED",
  INSUFFICIENT_EVIDENCE: "INSUFFICIENT_EVIDENCE",
  ERROR: "ERROR",
});

/** Default cadence days by priority (ACTIVE). */
export const CADENCE_DAYS_BY_PRIORITY = Object.freeze({
  HIGH: 7,
  MEDIUM: 14,
  LOW: 30,
});

/** After this many consecutive no-change runs, relax cadence one step. */
export const NO_CHANGE_RELAX_THRESHOLD = 3;

export const VAL_TARGET_TYPE = Object.freeze(Object.values(TARGET_TYPE));
export const VAL_TARGET_STATUS = Object.freeze(Object.values(TARGET_STATUS));
export const VAL_TARGET_PRIORITY = Object.freeze(Object.values(TARGET_PRIORITY));
export const VAL_RESEARCH_CADENCE = Object.freeze(Object.values(RESEARCH_CADENCE));
export const VAL_RUN_TYPE = Object.freeze(Object.values(RUN_TYPE));
export const VAL_RUN_STATUS = Object.freeze(Object.values(RUN_STATUS));
/** Airtable-writable only — LEDGER_CHECK is in-memory semantic, maps to SKIPPED on write. */
export const VAL_EXECUTION_STATUS = Object.freeze(
  Object.values(EXECUTION_STATUS).filter((v) => v !== EXECUTION_STATUS.LEDGER_CHECK)
);
/** Airtable-writable only — BASELINE_EXISTING is summary semantic, maps to NO_MATERIAL_CHANGE. */
export const VAL_RESULT_TYPE = Object.freeze(
  Object.values(RESULT_TYPE).filter((v) => v !== RESULT_TYPE.BASELINE_EXISTING)
);
