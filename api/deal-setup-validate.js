/**
 * Deal Setup (PATCH) payload validation and sanitization.
 * - Validation: optional soft checks (e.g. Deal Status allowlist); no hard fail on unknown selects.
 * - Sanitization: trim strings, coerce dates/numbers for known fields.
 * - Returns validation result, sanitized payload, and (dev-only) field mapping used.
 */

import { classifyDealSetupFormField } from "./schemas/deal-setup-fields.js";

const DEV = process.env.NODE_ENV !== "production" || process.env.DEBUG_DEAL_SETUP === "true";

/**
 * Coerce value to YYYY-MM-DD if it looks like a date string.
 */
function toAirtableDateString(val) {
  if (val == null || typeof val !== "string") return null;
  const trimmed = val.trim();
  if (!trimmed) return null;
  const d = new Date(trimmed);
  if (Number.isNaN(d.getTime())) return null;
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, "0");
  const day = String(d.getDate()).padStart(2, "0");
  return `${y}-${m}-${day}`;
}

/**
 * Validate and sanitize Deal Setup PATCH body.fields.
 * - valid: true unless payload is empty or recordId missing (recordId checked by caller).
 * - errors: list of { field, message } for soft warnings only; no hard fail on unknown selects.
 * - payload: sanitized copy (trimmed strings, coerced dates/numbers for known fields).
 * - fieldMappingUsed: (dev only) which table each key routes to.
 *
 * @param {Record<string, unknown>} fields - body.fields from PATCH request
 * @returns {{ valid: boolean, errors: Array<{ field: string, message: string }>, payload: Record<string, unknown>, fieldMappingUsed?: Record<string, string> }}
 */
export function validateDealSetupPayload(fields) {
  const errors = [];
  const payload = {};
  const fieldMappingUsed = DEV ? {} : undefined;

  if (!fields || typeof fields !== "object") {
    return { valid: false, errors: [{ field: "_", message: "fields must be an object" }], payload: {} };
  }

  for (const [key, value] of Object.entries(fields)) {
    let val = value;
    if (typeof val === "string") val = val.trim();
    if (val === "" || val == null) continue;

    if (DEV && fieldMappingUsed) {
      fieldMappingUsed[key] = classifyDealSetupFormField(key);
    }

    if (key === "Current Franchise/Management Contract End Date" && typeof val === "string") {
      const coerced = toAirtableDateString(val);
      payload[key] = coerced && /^\d{4}-\d{2}-\d{2}$/.test(coerced) ? coerced : val;
      continue;
    }
    if (key === "Proposal Deadline" && typeof val === "string") {
      const coerced = toAirtableDateString(val);
      payload[key] = coerced && /^\d{4}-\d{2}-\d{2}$/.test(coerced) ? coerced : val;
      continue;
    }
    if (key === "Expected Opening or Rebranding Date" && typeof val === "string") {
      const coerced = toAirtableDateString(val);
      payload[key] = coerced && /^\d{4}-\d{2}-\d{2}$/.test(coerced) ? coerced : val;
      continue;
    }
    if (key === "Property STR Number (if applicable)") {
      const num = typeof val === "number" ? val : parseInt(String(val).replace(/,/g, ""), 10);
      if (!Number.isNaN(num) && num >= 0) payload[key] = num;
      continue;
    }

    payload[key] = val;
  }

  if (Object.keys(payload).length === 0) {
    errors.push({ field: "_", message: "No updatable fields after sanitization" });
    return { valid: false, errors, payload: {}, fieldMappingUsed };
  }

  return { valid: true, errors, payload, fieldMappingUsed };
}
