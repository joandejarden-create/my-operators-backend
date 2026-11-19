/**
 * Verify Memberstack member JWT server-side (identity only).
 *
 * Primary: Admin REST API POST /members/verify-token (X-API-KEY).
 * Fallback: @memberstack/admin JWKS verifyToken (no secret required for signature).
 *
 * Permission model: this module returns member id + email only. Workspace Access,
 * Owner-Operator, and Demo flags are resolved in lib/dealality/resolve-user.js from Airtable
 * (Company Profile + Users), not from Memberstack plan names or custom fields.
 */

import memberstackAdmin from "@memberstack/admin";
import { memberstackAdminSecretForMemberId } from "./environment.js";

const DEFAULT_BASE_URL = "https://admin.memberstack.com";

function pickMemberEmail(data) {
  if (!data || typeof data !== "object") return null;
  const candidates = [
    data.email,
    data.auth?.email,
    data.member?.email,
    data.data?.email,
    data.data?.auth?.email,
  ];
  for (const c of candidates) {
    const s = c != null ? String(c).trim() : "";
    if (s && s.includes("@")) return s;
  }
  return null;
}

function pickMemberId(data) {
  if (!data || typeof data !== "object") return null;
  const candidates = [data.id, data.sub, data.memberId, data.member?.id, data.data?.id, data.data?.member?.id];
  for (const c of candidates) {
    const s = c != null ? String(c).trim() : "";
    if (s) return s;
  }
  return null;
}

async function verifyViaRestApi(token) {
  const secret = process.env.MEMBERSTACK_SECRET_KEY;
  if (!secret || secret === "unused_verify_only") {
    return null;
  }
  const baseUrl = (process.env.MEMBERSTACK_BASE_URL || DEFAULT_BASE_URL).replace(/\/$/, "");
  const url = `${baseUrl}/members/verify-token`;

  const res = await fetch(url, {
    method: "POST",
    headers: {
      "X-API-KEY": secret,
      "Content-Type": "application/json",
    },
    body: JSON.stringify({ token }),
  });

  const body = await res.json().catch(() => ({}));
  if (!res.ok) {
    const err = new Error(body?.message || body?.error || `Memberstack verify-token HTTP ${res.status}`);
    err.statusCode = res.status;
    throw err;
  }

  const payload = body.data != null ? body.data : body;
  const id = pickMemberId(payload);
  if (!id) {
    throw new Error("Memberstack verify-token response missing member id");
  }
  return {
    id,
    email: pickMemberEmail(payload),
    raw: payload,
    verifiedVia: "rest",
  };
}

/**
 * @returns {Promise<{ email: string|null, lookup: string }>}
 * lookup: ok | no_secret | secret_invalid | http_N | empty_member | error
 */
async function fetchMemberEmailById(memberstackId) {
  const secret = memberstackAdminSecretForMemberId(memberstackId);
  if (!secret || secret === "unused_verify_only" || !memberstackId) {
    return { email: null, lookup: "no_secret" };
  }
  const baseUrl = (process.env.MEMBERSTACK_BASE_URL || DEFAULT_BASE_URL).replace(/\/$/, "");
  try {
    const res = await fetch(`${baseUrl}/members/${encodeURIComponent(memberstackId)}`, {
      method: "GET",
      headers: { "X-API-KEY": secret, Accept: "application/json" },
    });
    const body = await res.json().catch(() => ({}));
    if (!res.ok) {
      const code = String(body?.code || "");
      if (res.status === 401 || code.includes("invalid-secret-key")) {
        return { email: null, lookup: "secret_invalid" };
      }
      return { email: null, lookup: `http_${res.status}` };
    }
    const data = body && body.data != null ? body.data : body;
    const email = pickMemberEmail(data);
    if (!email) return { email: null, lookup: "empty_member" };
    return { email: email.toLowerCase(), lookup: "ok" };
  } catch (_) {
    return { email: null, lookup: "error" };
  }
}

async function enrichVerifiedEmail(verified) {
  if (!verified || verified.email || !verified.id) {
    return verified?.email
      ? { ...verified, emailLookup: "jwt_claim" }
      : verified;
  }
  const { email, lookup } = await fetchMemberEmailById(verified.id);
  if (!email) return { ...verified, emailLookup: lookup };
  return {
    ...verified,
    email,
    emailLookup: lookup,
    verifiedVia: verified.verifiedVia + "+member_lookup",
  };
}

async function verifyViaAdminPackage(token) {
  const secret = process.env.MEMBERSTACK_SECRET_KEY || "unused_verify_only";
  const client = memberstackAdmin.init(secret);
  const audience = process.env.MEMBERSTACK_APP_ID || undefined;
  const payload = await client.verifyToken({ token, ...(audience ? { audience } : {}) });
  const id = pickMemberId(payload);
  if (!id) {
    throw new Error("JWT payload missing member id");
  }
  return {
    id,
    email: pickMemberEmail(payload),
    raw: payload,
    verifiedVia: "jwks",
  };
}

/**
 * @param {string} token
 * @returns {Promise<{ id: string, email: string|null, raw: object, verifiedVia: string }>}
 */
export async function verifyMemberstackToken(token) {
  if (!token || typeof token !== "string") {
    throw new Error("Missing token");
  }

  try {
    const rest = await verifyViaRestApi(token);
    if (rest) return enrichVerifiedEmail(rest);
  } catch (restErr) {
    if (!process.env.MEMBERSTACK_APP_ID && !process.env.MEMBERSTACK_SECRET_KEY) {
      throw restErr;
    }
    try {
      return enrichVerifiedEmail(await verifyViaAdminPackage(token));
    } catch (jwksErr) {
      const err = new Error(restErr.message || jwksErr.message || "Invalid token");
      err.cause = { rest: restErr.message, jwks: jwksErr.message };
      throw err;
    }
  }

  return enrichVerifiedEmail(await verifyViaAdminPackage(token));
}
