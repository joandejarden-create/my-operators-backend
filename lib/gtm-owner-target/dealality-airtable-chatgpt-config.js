/**
 * Dealality ChatGPT → GTM Airtable wrapper — allowed tables and limits.
 * Base: Owner Targets / GTM (appKZuK006BWIVjNW).
 */

export const DEFAULT_GTM_BASE_ID = "appKZuK006BWIVjNW";

/** @type {Record<string, { id: string, name: string }>} */
export const MAP_DEALALITY_CHATGPT_TABLES = {
  properties: { id: "tbl5m3z72YehxLMr0", name: "Properties" },
  founderProjectPlan: { id: "tblpCg0QZ0kIPXihE", name: "Founder Project Plan" },
  pilotTargetList: { id: "tblgsKWuI25MWohAP", name: "Pilot Target List" },
  companies: { id: "tblhGEiTZxed0RNoS", name: "Companies" },
  contacts: { id: "tblVXzjorXmTrcDWN", name: "Contacts" },
  ownerTargets: { id: "tblV6XDSnhw2TETXC", name: "Owner Targets" },
};

export const ALLOWED_TABLE_IDS = new Set(
  Object.values(MAP_DEALALITY_CHATGPT_TABLES).map((t) => t.id)
);

export const LIST_MAX_RECORDS_DEFAULT = 20;
export const LIST_MAX_RECORDS_CAP = 100;
export const SUMMARIZE_MAX_RECORDS_DEFAULT = 100;
export const SUMMARIZE_MAX_RECORDS_CAP = 500;
export const WRITE_BATCH_MIN = 1;
export const WRITE_BATCH_MAX = 10;

export const INCOMPLETE_STATUS_VALUES = new Set([
  "completed",
  "not needed",
  "deferred",
]);
