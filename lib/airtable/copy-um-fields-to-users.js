/**
 * Map User Management record fields → Users table write payload.
 */
import { PUF } from "./platform-users-table.js";

export const UM_FIELD_RENAMES = {
  Company: PUF.companyProfile,
  "Memberstack ID": "Unique_Webflow_ID",
};

/** After mapping Company → Company Profile, also write the legacy column name if it exists on Users */
export const UM_MIRROR_DEST_FIELDS = {
  [PUF.companyProfile]: [PUF.company],
};

/** UM fields never copied to Users */
export const UM_SKIP_FIELDS = new Set([
  "Emp_ID",
  "Record_ID",
  "Last Modified",
  "Created",
  "Brand Name (from Brands Supported)",
]);

export function sanitizeAttachmentValue(value) {
  if (!Array.isArray(value) || value.length === 0) return undefined;
  const out = [];
  for (const item of value) {
    if (!item || typeof item !== "object") continue;
    const url = typeof item.url === "string" ? item.url.trim() : "";
    if (!url.startsWith("http")) continue;
    const entry = { url };
    if (item.filename) entry.filename = item.filename;
    out.push(entry);
  }
  return out.length ? out : undefined;
}

export function sanitizeFieldValue(value) {
  if (value === undefined || value === null) return undefined;
  if (Array.isArray(value)) {
    if (value.length === 0) return undefined;
    if (value[0] && typeof value[0] === "object" && value[0].url) {
      return sanitizeAttachmentValue(value);
    }
    if (typeof value[0] === "string" && value[0].startsWith("rec")) {
      return value.filter((id) => typeof id === "string" && id.startsWith("rec"));
    }
    return value;
  }
  return value;
}

/**
 * @param {object} umFields
 * @param {Set<string>} usersFieldNames - field names that exist on Users table
 * @param {{ overwrite?: boolean, existingUserFields?: object }} [opts]
 */
export function buildUsersPayloadFromUm(umFields, usersFieldNames, opts = {}) {
  const { overwrite = false, existingUserFields = {} } = opts;
  const out = {};
  const safe = umFields && typeof umFields === "object" ? umFields : {};

  const setDest = (dest, value) => {
    const sanitized = sanitizeFieldValue(value);
    if (sanitized === undefined) return;
    if (!usersFieldNames.has(dest)) return;
    if (!overwrite) {
      const cur = existingUserFields[dest];
      if (cur !== undefined && cur !== null && cur !== "" && !(Array.isArray(cur) && cur.length === 0)) {
        return;
      }
    }
    out[dest] = sanitized;
  };

  for (const [src, raw] of Object.entries(safe)) {
    if (UM_SKIP_FIELDS.has(src)) continue;
    const dest = UM_FIELD_RENAMES[src] || src;
    setDest(dest, raw);
    const sanitized = sanitizeFieldValue(raw);
    const mirrors = UM_MIRROR_DEST_FIELDS[dest];
    if (sanitized !== undefined && mirrors) {
      for (const mirror of mirrors) setDest(mirror, sanitized);
    }
  }

  const email = normEmailFromUm(safe);
  if (email) {
    setDest(PUF.email, email);
    setDest(PUF.companyEmail, email);
  }

  const msId = safe["Memberstack ID"];
  if (msId && String(msId).trim()) {
    const id = String(msId).trim();
    setDest("Unique_Webflow_ID", id);
    if (usersFieldNames.has("Slug")) setDest("Slug", id);
    if (usersFieldNames.has("Memberstack ID")) setDest("Memberstack ID", id);
  }

  const platformRole = safe[PUF.platformRole];
  if (platformRole && usersFieldNames.has("User Type")) {
    setDest("User Type", platformRole);
  }

  const companyTitle = safe[PUF.companyTitle];
  if (companyTitle && usersFieldNames.has("Title")) {
    setDest("Title", companyTitle);
  }

  return out;
}

export function normEmailFromUm(fields) {
  const e = fields?.[PUF.companyEmail] || fields?.Email || fields?.[PUF.email] || "";
  return String(Array.isArray(e) ? e[0] : e)
    .trim()
    .toLowerCase();
}
