/**
 * Target list batch access validation (Batch 2A).
 */

import { TARGET_LIST_TABLE } from "../../api/schemas/target-list-fields.js";
import { BDR_TABLE } from "../../api/schemas/brand-deal-request-fields.js";
import { assertOwnerDealAccess, ownerMustEnforceDealAccess } from "./owner-deal-id-access.js";

function dealIdFromTargetFields(fields) {
  const raw = fields?.Deal_ID;
  if (Array.isArray(raw) && raw[0]) return String(raw[0]).trim();
  return null;
}

export async function assertOwnerTargetIdsAccess(user, targetIds) {
  if (!ownerMustEnforceDealAccess(user)) {
    return { ok: true };
  }
  const ids = Array.isArray(targetIds) ? targetIds.filter((id) => String(id).startsWith("rec")) : [];
  if (!ids.length) {
    return { ok: false, status: 400, error: "targetIds array required" };
  }

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) {
    return { ok: false, status: 500, error: "Airtable credentials not configured" };
  }

  for (const targetId of ids) {
    const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(TARGET_LIST_TABLE)}/${encodeURIComponent(targetId)}`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
    if (res.status === 404) {
      return { ok: false, status: 404, error: "Target not found: " + targetId };
    }
    if (!res.ok) {
      return { ok: false, status: 502, error: "Failed to load target for access check" };
    }
    const data = await res.json();
    const dealId = dealIdFromTargetFields(data.fields);
    if (!dealId) {
      return { ok: false, status: 403, error: "forbidden", message: "Target has no linked deal." };
    }
    const check = await assertOwnerDealAccess(user, dealId, { baseId, apiKey });
    if (!check.ok) {
      return check;
    }
  }

  return { ok: true };
}

export async function assertOwnerBdrRequestIdsAccess(user, requestIds) {
  if (!ownerMustEnforceDealAccess(user)) {
    return { ok: true };
  }
  const ids = Array.isArray(requestIds) ? requestIds.filter((id) => String(id).startsWith("rec")) : [];
  if (!ids.length) {
    return { ok: false, status: 400, error: "requestId required" };
  }

  const baseId = process.env.AIRTABLE_BASE_ID;
  const apiKey = process.env.AIRTABLE_API_KEY;
  if (!baseId || !apiKey) {
    return { ok: false, status: 500, error: "Airtable credentials not configured" };
  }

  for (const requestId of ids) {
    const url = `https://api.airtable.com/v0/${baseId}/${encodeURIComponent(BDR_TABLE)}/${encodeURIComponent(requestId)}`;
    const res = await fetch(url, { headers: { Authorization: `Bearer ${apiKey}` } });
    if (res.status === 404) {
      return { ok: false, status: 404, error: "Brand Deal Request not found" };
    }
    if (!res.ok) {
      return { ok: false, status: 502, error: "Failed to load request for access check" };
    }
    const data = await res.json();
    const dealRaw = data.fields?.Deal;
    const first = Array.isArray(dealRaw) ? dealRaw[0] : dealRaw;
    const dealId =
      typeof first === "string"
        ? first.trim()
        : first && first.id
          ? String(first.id).trim()
          : "";
    if (!dealId.startsWith("rec")) {
      return { ok: false, status: 403, error: "forbidden", message: "Request has no linked deal." };
    }
    const check = await assertOwnerDealAccess(user, dealId, { baseId, apiKey });
    if (!check.ok) {
      return check;
    }
  }

  return { ok: true };
}
