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
    if (rest) return rest;
  } catch (restErr) {
    if (!process.env.MEMBERSTACK_APP_ID && !process.env.MEMBERSTACK_SECRET_KEY) {
      throw restErr;
    }
    try {
      return await verifyViaAdminPackage(token);
    } catch (jwksErr) {
      const err = new Error(restErr.message || jwksErr.message || "Invalid token");
      err.cause = { rest: restErr.message, jwks: jwksErr.message };
      throw err;
    }
  }

  return verifyViaAdminPackage(token);
}
