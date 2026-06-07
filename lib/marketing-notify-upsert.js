import { createHash } from "crypto";
import Airtable from "airtable";
import {
  isEmptyAirtableValue,
  writeUsersRecordWithFieldFallback,
} from "./airtable-users-protected-patch.js";
import { upsertSignupUserRecord, USERS_SIGNUP } from "./signup-airtable-upsert.js";

export const MARKETING_NOTIFY_USER_TYPE = (
  process.env.MARKETING_NOTIFY_USER_TYPE || "Notify only"
).trim();

const DEFAULT_SOURCE = "Landing page — Stay informed";
const DEFAULT_REASON = "Beta launch notification";

function getBase() {
  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) {
    const err = new Error("Set AIRTABLE_API_KEY and AIRTABLE_BASE_ID");
    err.statusCode = 500;
    throw err;
  }
  return new Airtable({ apiKey }).base(baseId);
}

/** Stable placeholder Unique Webflow ID for notify-only users (no Memberstack account). */
export function notifyOnlyWebflowId(normalizedEmail) {
  const hex = createHash("sha256").update(normalizedEmail).digest("hex").slice(0, 20);
  return `notify-${hex}`;
}

/**
 * Create or touch a Users row for marketing "Stay informed" capture.
 * New users get User Type = Notify only; existing users keep their current type.
 *
 * @param {string} email
 * @param {{ source?: string, reason?: string }} [meta]
 */
export async function upsertMarketingNotifyUser(email, meta = {}) {
  const normalizedEmail = typeof email === "string" ? email.trim().toLowerCase() : "";
  if (!normalizedEmail) {
    const err = new Error("Email is required");
    err.statusCode = 400;
    throw err;
  }

  const body = {
    email: normalizedEmail,
    role: MARKETING_NOTIFY_USER_TYPE,
    companyType: MARKETING_NOTIFY_USER_TYPE,
    howDidYouHear: meta.source || DEFAULT_SOURCE,
    reasonToJoin: meta.reason || DEFAULT_REASON,
  };

  const statusOnWrite = (process.env.MARKETING_NOTIFY_STATUS || "").trim();
  const { record, isCreate } = await upsertSignupUserRecord(body, notifyOnlyWebflowId(normalizedEmail), {
    statusOnWrite: statusOnWrite || undefined,
  });

  if (isCreate && isEmptyAirtableValue((record.fields || {})["User Type"])) {
    const base = getBase();
    const updated = await writeUsersRecordWithFieldFallback(base, USERS_SIGNUP.table, {
      isCreate: false,
      recordId: record.id,
      fields: { "User Type": MARKETING_NOTIFY_USER_TYPE },
    });
    return { record: updated, created: true };
  }

  return { record, created: isCreate };
}
