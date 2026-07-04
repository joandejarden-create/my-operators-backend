import axios from "axios";
import { randomBytes } from "crypto";
import { buildMemberstackCustomFields } from "./memberstack-custom-fields.js";

const BASE = (process.env.MEMBERSTACK_BASE_URL || "https://admin.memberstack.com").replace(/\/$/, "");

function memberstackHeaders() {
  const key = (process.env.MEMBERSTACK_SECRET_KEY || "").trim();
  if (!key) return null;
  return { "X-API-KEY": key, "Content-Type": "application/json" };
}

/** @deprecated Use buildMemberstackCustomFields — kept for imports from api/signup.js */
export function buildSignupCustomFields(body, extras) {
  return buildMemberstackCustomFields(body, extras);
}

/**
 * Create or link a Memberstack member for public signup (replaces Zap-triggered New Member).
 *
 * Env:
 * - MEMBERSTACK_SECRET_KEY — required
 * - SIGNUP_MEMBERSTACK_MODE — `create` (default) | `lookup` | `off`
 * - MEMBERSTACK_SIGNUP_PENDING_PLAN_ID — optional pln_… assigned on create (stagegate / no full access)
 * - MEMBERSTACK_SIGNUP_FREE_PLAN_ID — not used on signup; assign manually in Memberstack when approving
 *
 * @returns {Promise<{ memberstackId: string | null, memberstackNote: string, created: boolean }>}
 */
export async function provisionMemberstackForSignup({
  email,
  firstName,
  lastName,
  customFields,
  password,
  mode: modeOverride,
}) {
  const headers = memberstackHeaders();
  const mode = (
    modeOverride ||
    process.env.SIGNUP_MEMBERSTACK_MODE ||
    "create"
  )
    .trim()
    .toLowerCase();
  if (!headers || mode === "off") {
    return { memberstackId: null, memberstackNote: "memberstack_disabled", created: false };
  }

  const normalized = typeof email === "string" ? email.trim().toLowerCase() : "";
  if (!normalized) {
    return { memberstackId: null, memberstackNote: "missing_email", created: false };
  }

  const enc = encodeURIComponent(normalized);
  const pendingPlanId = (process.env.MEMBERSTACK_SIGNUP_PENDING_PLAN_ID || "").trim();
  const cf = customFields && typeof customFields === "object" ? customFields : {};

  let getRes;
  try {
    getRes = await axios.get(`${BASE}/members/${enc}`, {
      headers,
      validateStatus: (s) => s === 200 || s === 404,
    });
  } catch (e) {
    return { memberstackId: null, memberstackNote: `lookup_error:${e.message || "unknown"}`, created: false };
  }

  if (getRes.status === 200 && getRes.data?.data?.id) {
    const id = getRes.data.data.id;
    const prev = getRes.data.data.customFields;
    const merged = {
      ...(prev && typeof prev === "object" ? prev : {}),
      ...cf,
    };
    try {
      await axios.patch(`${BASE}/members/${id}`, { customFields: merged }, { headers });
    } catch (e) {
      console.warn("Memberstack customFields patch (existing member):", e.response?.data || e.message);
    }
    return { memberstackId: id, memberstackNote: "existing_member", created: false };
  }

  if (mode !== "create") {
    return { memberstackId: null, memberstackNote: "member_not_found_lookup_only", created: false };
  }

  const pwd =
    typeof password === "string" && password.length >= 8
      ? password
      : randomBytes(18).toString("base64url").slice(0, 22);

  const payload = {
    email: normalized,
    password: pwd,
    customFields: cf,
  };
  if (pendingPlanId) {
    payload.plans = [{ planId: pendingPlanId }];
  }

  const postRes = await axios.post(`${BASE}/members`, payload, {
    headers,
    validateStatus: () => true,
  });

  if (postRes.status >= 200 && postRes.status < 300 && postRes.data?.data?.id) {
    const note = pendingPlanId
      ? "created_member_pending_plan"
      : typeof password === "string" && password.length >= 8
        ? "created_member"
        : "created_member_password_reset_required";
    return {
      memberstackId: postRes.data.data.id,
      memberstackNote: note,
      created: true,
    };
  }

  const hint =
    typeof postRes.data === "object" ? JSON.stringify(postRes.data).slice(0, 240) : String(postRes.status);
  return { memberstackId: null, memberstackNote: `create_failed:${postRes.status}:${hint}`, created: false };
}

/**
 * Patch Memberstack member after Airtable write (replaces Zap write-back steps).
 */
export async function patchMemberstackAfterAirtable(
  memberstackId,
  { airtableRecordId, body, companyProfileId } = {}
) {
  const headers = memberstackHeaders();
  if (!headers || !memberstackId) return { ok: false };

  const customFields = buildMemberstackCustomFields(body || {}, {
    airtableRecordId,
    companyProfileId,
  });
  if (!Object.keys(customFields).length) return { ok: true };

  try {
    const res = await axios.patch(
      `${BASE}/members/${memberstackId}`,
      { customFields },
      { headers, validateStatus: () => true }
    );
    if (res.status >= 200 && res.status < 300) return { ok: true };
    console.warn(
      "Memberstack customFields patch failed:",
      res.status,
      typeof res.data === "object" ? JSON.stringify(res.data).slice(0, 400) : res.data
    );
    return { ok: false, status: res.status };
  } catch (e) {
    console.warn("Memberstack customFields patch error:", e.response?.data || e.message);
    return { ok: false };
  }
}

/** Ensure pending plan is on member (e.g. after client DOM signup). */
export async function ensureMemberstackPendingPlan(memberstackId) {
  const headers = memberstackHeaders();
  const planId = (process.env.MEMBERSTACK_SIGNUP_PENDING_PLAN_ID || "").trim();
  if (!headers || !planId || !memberstackId) return { ok: false };

  try {
    await axios.post(`${BASE}/members/${memberstackId}/add-plan`, { planId }, { headers });
    return { ok: true };
  } catch {
    return { ok: false };
  }
}

/** @deprecated Use provisionMemberstackForSignup — pilot alias */
export async function resolveMemberstackIdForPilotSignup(args) {
  const r = await provisionMemberstackForSignup(args);
  return { memberstackId: r.memberstackId, memberstackNote: r.memberstackNote };
}
