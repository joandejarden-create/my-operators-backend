/**
 * Owner deal ID allow-list helpers (Batch 2A).
 */

import { dealRecordAllowedForUser } from "./deal-record-access.js";
import { userCanAccessOwnerWorkspace } from "./user-workspace-gates.js";
import { DEALS_TABLE } from "../../api/schemas/deal-setup-fields.js";

function normalizeDealIds(raw) {
  if (raw == null) return [];
  const arr = Array.isArray(raw) ? raw : String(raw).split(",");
  return arr.map((s) => String(s).trim()).filter((id) => id.startsWith("rec"));
}

export function ownerMustEnforceDealAccess(user) {
  if (!user) return false;
  if (user.isAdmin) return true;
  return userCanAccessOwnerWorkspace(user);
}

export function filterDealIdsForOwner(user, rawDealIds) {
  const ids = normalizeDealIds(rawDealIds);
  if (!ids.length) return [];
  if (!user) return [];
  if (user.isAdmin) return ids;
  if (!userCanAccessOwnerWorkspace(user)) return ids;
  return ids.filter((id) => userMayAccessDealIdSync(user, id) !== false);
}

/**
 * Sync check when deal fields are not loaded — uses user link / company only via deal fetch elsewhere.
 * Returns true | false | null (null = need Airtable fetch).
 */
export function userMayAccessDealIdSync(user, dealId) {
  if (!user || !dealId) return false;
  if (user.isAdmin) return true;
  if (!userCanAccessOwnerWorkspace(user)) return null;
  return null;
}

export async function assertOwnerDealAccess(user, dealId, { baseId, apiKey } = {}) {
  if (!dealId || !dealId.startsWith("rec")) {
    return { ok: false, status: 400, error: "Valid deal record ID is required" };
  }
  if (!user) {
    return { ok: false, status: 401, error: "authentication_required" };
  }
  if (user.isAdmin) return { ok: true };
  if (!userCanAccessOwnerWorkspace(user)) {
    return { ok: true, skipped: true };
  }

  const bid = baseId || process.env.AIRTABLE_BASE_ID;
  const key = apiKey || process.env.AIRTABLE_API_KEY;
  if (!bid || !key) {
    return { ok: false, status: 500, error: "Airtable credentials not configured" };
  }

  const url = `https://api.airtable.com/v0/${bid}/${encodeURIComponent(DEALS_TABLE)}/${encodeURIComponent(dealId)}`;
  const res = await fetch(url, { headers: { Authorization: `Bearer ${key}` } });
  if (res.status === 404) {
    return { ok: false, status: 404, error: "Deal not found" };
  }
  if (!res.ok) {
    return { ok: false, status: 502, error: "Failed to load deal for access check" };
  }
  const data = await res.json();
  if (!dealRecordAllowedForUser(data.fields, user)) {
    return { ok: false, status: 403, error: "forbidden", message: "You do not have access to this deal." };
  }
  return { ok: true, dealFields: data.fields };
}

export async function filterDealIdsWithAirtable(user, rawDealIds) {
  const ids = normalizeDealIds(rawDealIds);
  if (!ids.length) return [];
  if (!ownerMustEnforceDealAccess(user)) return ids;

  const allowed = [];
  for (const id of ids) {
    const check = await assertOwnerDealAccess(user, id);
    if (check.ok) allowed.push(id);
  }
  return allowed;
}

export { normalizeDealIds };
