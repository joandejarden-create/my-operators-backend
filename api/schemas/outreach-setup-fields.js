/**
 * Outreach Setup — UI logical keys ↔ Airtable column names.
 * Table: Outreach Setup (tblpVQAVCY5i1L4jz). Verified against base meta API.
 *
 * UI uses short keys in my-deals.html form `name` attributes; Airtable uses full question labels.
 */

/** @type {Record<string, string>} map_uiKey → Airtable field name */
export const MAP_OUTREACH_UI_TO_AIRTABLE = {
  Confidentiality: "Do you want this opportunity to remain fully confidential?",
  "Identity Disclosure": "Should your identity be disclosed during outreach?",
  "Exclude Brands":
    "Are there specific brand or management companies you'd like to exclude from outreach?",
  "Exclude Brands List": "Are there specific brand list",
  "Prioritize Companies": "Are there specific companies you'd like prioritized in outreach?",
  "Prioritize Companies List": "Are there specific companies list",
  "Outreach From": "Who should outreach come from?",
  "Messaging Involvement": "How involved would you like to be in messaging?",
  "Approve Each Message": "Do you want to approve each outreach message before it is sent?",
  "Preferred Tone": "Preferred tone for outreach?",
  "When to Begin Outreach": "When should outreach begin?",
  "Outreach Start Date": "Outreach begin date",
  "Follow-up Frequency":
    "How often would you like follow-up messages sent to operators who haven\u2019t responded?",
  "Notify on Open or Respond":
    "Should Deal Capture notify you when a recipient opens or responds to an outreach message?",
  "Attachments to Include": "Which attachments should be included in outreach messages?",
  "Attachments Gated": "Should attachments be gated behind NDA or click-to-accept terms?",
  "Allow Forward or Share": "Do you want recipients to be able to forward/share your deal summary?",
  "Custom First Message": "Would you like to provide a custom message for the first outreach?",
};

/** UI keys accepted on API read/write (stable contract for my-deals.html). */
export const OUTREACH_UI_FIELD_KEYS = Object.freeze(Object.keys(MAP_OUTREACH_UI_TO_AIRTABLE));

export const OUTREACH_TABLE = process.env.AIRTABLE_TABLE_OUTREACH_SETUP || "Outreach Setup";

/** Link to Users — default outreach record is scoped per user. */
export const OUTREACH_USER_FIELD =
  process.env.AIRTABLE_OUTREACH_SETUP_USER_FIELD || "User_ID";

export const OUTREACH_USER_WEBFLOW_FIELD =
  process.env.AIRTABLE_OUTREACH_SETUP_USER_WEBFLOW_FIELD || "User_Webflow_ID";

/**
 * Optional link to Deals for per-deal overrides. Not present in current base schema;
 * set AIRTABLE_OUTREACH_SETUP_DEAL_FIELD=Deal after the column is added in Airtable.
 */
export const OUTREACH_DEAL_FIELD = (
  process.env.AIRTABLE_OUTREACH_SETUP_DEAL_FIELD || ""
).trim();

export function map_outreachUiFieldsToAirtable(uiFields) {
  const out = {};
  if (!uiFields || typeof uiFields !== "object") return out;
  for (const uiKey of OUTREACH_UI_FIELD_KEYS) {
    if (uiFields[uiKey] === undefined || uiFields[uiKey] === null) continue;
    const raw = uiFields[uiKey];
    const airtableName = MAP_OUTREACH_UI_TO_AIRTABLE[uiKey];
    if (uiKey === "Outreach Start Date") {
      const s = typeof raw === "string" ? raw.trim() : String(raw ?? "").trim();
      out[airtableName] = s || null;
      continue;
    }
    out[airtableName] = typeof raw === "string" ? raw.trim() : raw;
  }
  return out;
}

export function map_outreachAirtableToUiFields(airtableFields) {
  const out = {};
  if (!airtableFields || typeof airtableFields !== "object") return out;
  const reverse = new Map(
    Object.entries(MAP_OUTREACH_UI_TO_AIRTABLE).map(([ui, at]) => [at, ui])
  );
  for (const [atName, uiKey] of reverse) {
    const v = airtableFields[atName];
    if (v == null || v === "") continue;
    if (uiKey === "Outreach Start Date" && typeof v === "string") {
      out[uiKey] = v.slice(0, 10);
      continue;
    }
    out[uiKey] = valueToStr(v);
  }
  return out;
}

function valueToStr(v) {
  if (v == null) return "";
  if (typeof v === "string") return v.trim();
  if (typeof v === "object" && v !== null && typeof v.name === "string") return v.name.trim();
  if (Array.isArray(v) && v[0]) return valueToStr(v[0]);
  return "";
}
