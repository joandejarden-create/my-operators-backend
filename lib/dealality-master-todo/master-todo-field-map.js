/**
 * Dealality Master To-Do — Airtable field mapping (Founder Project Plan table).
 * Table: Founder Project Plan (tblpCg0QZ0kIPXihE) on GTM base appKZuK006BWIVjNW.
 */

export const MASTER_TODO_DEFAULT_TABLE_ID = "tblpCg0QZ0kIPXihE";
export const MASTER_TODO_DEFAULT_TABLE_NAME = "Founder Project Plan";
export const MASTER_TODO_GTM_BASE_ID = "appKZuK006BWIVjNW";

/** Canonical source value stamped on master to-do rows. */
export const MASTER_TODO_SOURCE_VALUE = "ChatGPT Master To-Do";

export const MAP_MASTER_TODO = {
  task: "Task",
  workstream: "Workstream",
  status: "Status",
  priority: "Priority",
  phase: "Phase",
  owner: "Assigned To",
  dueDate: "End",
  startDate: "Start",
  description: "Task Objective/Description",
  nextAction: "Next Action",
  progress: "Progress",
  blocker: "Blocker",
  dependency: "Dependency",
  relatedTable: "Related Table",
  source: "Source",
  relatedArea: "Related Area",
  gtmResourceType: "GTM Resource Type",
  pilotStage: "Pilot Stage",
  completedDate: "Completed Date",
  sortOrder: "Order / Sort",
  phaseNumber: "Phase Number",
  stepNumber: "Step Number",
  roadmapSort: "Roadmap Sort",
  successMetric: "Success Metric",
};

/** Recommended select options for master to-do workflow. */
export const VAL_MASTER_TODO_STATUS = [
  "Not Started",
  "In Progress",
  "Waiting",
  "Drafted",
  "Needs Review",
  "Completed",
  "Deferred",
  "Blocked",
];

export const VAL_MASTER_TODO_PRIORITY = ["P0", "P1", "P2", "P3"];

/** Founder Project Plan Priority labels in Airtable (tblpCg0QZ0kIPXihE). */
export const VAL_FPP_PRIORITY = [
  "P0 = Urgent / Launch-Critical",
  "P1 = Important Near-Term",
  "P2 = Useful but Not Urgent",
  "P3 = Backlog / Nice-to-Have",
];

/**
 * Map short priority codes (seed/ChatGPT) → Founder Project Plan Priority select.
 */
export const PRIORITY_WRITE_MAP = {
  P0: "P0 = Urgent / Launch-Critical",
  P1: "P1 = Important Near-Term",
  P2: "P2 = Useful but Not Urgent",
  P3: "P3 = Backlog / Nice-to-Have",
};

export function mapPriorityForWrite(value) {
  const v = String(value ?? "").trim();
  if (!v) return v;
  if (VAL_FPP_PRIORITY.includes(v)) return v;
  if (PRIORITY_WRITE_MAP[v]) return PRIORITY_WRITE_MAP[v];
  if (/^P0\b/.test(v)) return PRIORITY_WRITE_MAP.P0;
  if (/^P1\b/.test(v)) return PRIORITY_WRITE_MAP.P1;
  if (/^P2\b/.test(v)) return PRIORITY_WRITE_MAP.P2;
  if (/^P3\b/.test(v)) return PRIORITY_WRITE_MAP.P3;
  return v;
}

export const VAL_MASTER_TODO_PHASE = [
  "GTM / Outreach",
  "Pilot Conversion",
  "Pilot Delivery",
  "Product / Access",
  "Airtable / Data",
  "Resources / Collateral",
  "Later",
];

export const VAL_MASTER_TODO_WORKSTREAM = [
  "Pilot Target List",
  "Outreach Execution",
  "Reply Handling",
  "GTM Resources",
  "Pilot Offer",
  "Pilot Conversion",
  "Access Hygiene",
  "Pilot Delivery",
  "Product QA",
  "Content / LinkedIn",
  "Data / Reporting",
  "Later",
];

export const VAL_MASTER_TODO_SOURCE = [
  "ChatGPT Master To-Do",
  "Cursor",
  "Airtable",
  "Manual",
  "Pilot Outreach",
  "GTM Resource",
];

export const VAL_MASTER_TODO_RELATED_AREA = [
  "Owner Pilot",
  "Lawyer Referral",
  "Advisor / Consultant",
  "Pilot Target List",
  "Webflow / Memberstack",
  "Dealality Platform",
  "GTM",
  "Later",
];

/** Fields recommended for master to-do (high priority). */
export const RECOMMENDED_MASTER_TODO_FIELDS = [
  { key: "task", name: MAP_MASTER_TODO.task, type: "multilineText", priority: "required" },
  { key: "workstream", name: MAP_MASTER_TODO.workstream, type: "singleLineText|multilineText|singleSelect", priority: "required" },
  { key: "status", name: MAP_MASTER_TODO.status, type: "singleSelect", priority: "required" },
  { key: "priority", name: MAP_MASTER_TODO.priority, type: "singleSelect", priority: "required" },
  { key: "phase", name: MAP_MASTER_TODO.phase, type: "singleSelect", priority: "required" },
  { key: "owner", name: MAP_MASTER_TODO.owner, type: "singleSelect", priority: "required" },
  { key: "dueDate", name: MAP_MASTER_TODO.dueDate, type: "date", priority: "required" },
  { key: "description", name: MAP_MASTER_TODO.description, type: "multilineText", priority: "required" },
  { key: "nextAction", name: MAP_MASTER_TODO.nextAction, type: "multilineText", priority: "required" },
  { key: "source", name: MAP_MASTER_TODO.source, type: "singleSelect", priority: "required" },
  { key: "relatedArea", name: MAP_MASTER_TODO.relatedArea, type: "singleSelect", priority: "required" },
  { key: "progress", name: MAP_MASTER_TODO.progress, type: "singleSelect|percent", priority: "required" },
  { key: "completedDate", name: MAP_MASTER_TODO.completedDate, type: "date", priority: "required" },
  { key: "blocker", name: MAP_MASTER_TODO.blocker, type: "multilineText", priority: "optional" },
  { key: "dependency", name: MAP_MASTER_TODO.dependency, type: "multilineText", priority: "optional" },
  { key: "relatedTable", name: MAP_MASTER_TODO.relatedTable, type: "singleSelect", priority: "optional" },
  { key: "gtmResourceType", name: MAP_MASTER_TODO.gtmResourceType, type: "singleSelect", priority: "optional" },
  { key: "pilotStage", name: MAP_MASTER_TODO.pilotStage, type: "singleSelect", priority: "optional" },
  { key: "sortOrder", name: MAP_MASTER_TODO.sortOrder, type: "number", priority: "optional" },
];

/**
 * Map external/legacy status labels → Founder Project Plan Status.
 * ChatGPT or seed may say "Done"; Airtable uses "Completed" only.
 */
export const STATUS_WRITE_MAP = {
  Done: "Completed",
  Deferred: "Deferred",
};

export const COMPLETED_STATUS_VALUES = new Set(["done", "completed"]);
