/**
 * Phase Progress Summary table — dashboard rows synced from Founder Project Plan.
 * Base: appKZuK006BWIVjNW (GTM)
 */

export const PHASE_PROGRESS_SUMMARY_TABLE_NAME = "Phase Completion Dashboard";

/** Alternate name from initial create (still matched on sync). */
export const PHASE_PROGRESS_SUMMARY_TABLE_ALIASES = [
  "Phase Completion Dashboard",
  "Phase Progress Summary",
];

/** Table ID assigned on 2026-07-03 sync. Override via PHASE_PROGRESS_SUMMARY_TABLE_ID env. */
export const PHASE_PROGRESS_SUMMARY_TABLE_ID =
  process.env.PHASE_PROGRESS_SUMMARY_TABLE_ID || "tblilet6ncUmmJQDv";

export const MAP_PHASE_PROGRESS_SUMMARY = {
  phase: "Phase",
  totalTasks: "Total Tasks",
  completed: "Completed",
  inProgress: "In Progress",
  notStarted: "Not Started",
  otherStatus: "Other Status",
  percentDone: "Percent Done",
  lastSynced: "Last Synced",
};

export const PHASE_PROGRESS_SUMMARY_FIELDS = [
  { name: MAP_PHASE_PROGRESS_SUMMARY.phase, type: "singleLineText" },
  {
    name: MAP_PHASE_PROGRESS_SUMMARY.totalTasks,
    type: "number",
    options: { precision: 0 },
  },
  {
    name: MAP_PHASE_PROGRESS_SUMMARY.completed,
    type: "number",
    options: { precision: 0 },
  },
  {
    name: MAP_PHASE_PROGRESS_SUMMARY.inProgress,
    type: "number",
    options: { precision: 0 },
  },
  {
    name: MAP_PHASE_PROGRESS_SUMMARY.notStarted,
    type: "number",
    options: { precision: 0 },
  },
  {
    name: MAP_PHASE_PROGRESS_SUMMARY.otherStatus,
    type: "number",
    options: { precision: 0 },
  },
  {
    name: MAP_PHASE_PROGRESS_SUMMARY.percentDone,
    type: "percent",
    options: { precision: 1 },
  },
  {
    name: MAP_PHASE_PROGRESS_SUMMARY.lastSynced,
    type: "dateTime",
    options: { dateFormat: { name: "iso" }, timeFormat: { name: "24hour" }, timeZone: "utc" },
  },
];

/**
 * Airtable percent fields expect decimal (0.314 = 31.4%).
 * @param {number} percentDisplay e.g. 31.4
 */
export function toAirtablePercent(percentDisplay) {
  return percentDisplay / 100;
}
