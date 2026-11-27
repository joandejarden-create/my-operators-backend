/**
 * Verify Memberstack member JWT server-side (identity only).
 *
 * Primary: Admin REST API POST /members/verify-token (X-API-KEY).
 * Fallback: @memberstack/admin JWKS verifyToken (no secret required for signature).
 *
 * Sandbox (Test Mode) vs live: peeks unsigned JWT claims only to choose which Admin
 * secret to try first. Signature verification still required — peek never authorizes.
 *
 * Permission model: this module returns member id + email only. Workspace Access,
 * Owner-Operator, and Demo flags are resolved in lib/dealality/resolve-user.js from Airtable
 * (Company Profile + Users), not from Memberstack plan names or custom fields.
 */

import memberstackAdmin from "@memberstack/admin";
import {
  isLiveMemberstackId,
  isSandboxMemberstackId,
  memberstackAdminSecretForMemberId,
  memberstackSecretEnvironment,
} from "./environment.js";

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

/** Decode JWT payload without verifying signature — for secret selection only. */
function peekJwtPayloadUnsafe(token) {
  try {
    const parts = String(token || "").split(".");
    if (parts.length < 2) return null;
    const b64 = parts[1].replace(/-/g, "+").replace(/_/g, "/");
    const pad = b64.length % 4 === 0 ? "" : "=".repeat(4 - (b64.length % 4));
    const json = Buffer.from(b64 + pad, "base64").toString("utf8");
    return JSON.parse(json);
  } catch {
    return null;
  }
}

/**
 * Choose Admin API secrets to attempt.
 * Production: live/primary secret only — never accept sandbox Test Mode tokens.
 * Local/dev: prefer secret matching peeked member id class; never authorize from peek alone.
 */
function candidateAdminSecrets(token) {
  const primary = (process.env.MEMBERSTACK_SECRET_KEY || "").trim();
  const sandbox = (
    process.env.MEMBERSTACK_TEST_SECRET_KEY ||
    process.env.MEMBERSTACK_SANDBOX_SECRET_KEY ||
    ""
  ).trim();
  const ordered = [];
  const push = (s) => {
    if (s && s !== "unused_verify_only" && !ordered.includes(s)) ordered.push(s);
  };

  if (process.env.NODE_ENV === "production") {
    push(primary);
    return ordered;
  }

  const peekedId = pickMemberId(peekJwtPayloadUnsafe(token)) || "";
  if (isSandboxMemberstackId(peekedId)) {
    push(sandbox);
    // Do not fall through to live for sandbox-shaped ids — wrong-environment reject.
  } else if (isLiveMemberstackId(peekedId)) {
    push(primary);
  } else {
    // Ambiguous peek: try primary then sandbox, but assertEnvMatch rejects mismatches.
    push(primary);
    push(sandbox);
  }
  return ordered;
}

/** Fail closed if a verified member id class does not match the Admin secret environment. */
function assertSecretMatchesMemberId(secret, memberId) {
  const env = memberstackSecretEnvironment(secret);
  if (env === "unset" || env === "unknown") return;
  if (isSandboxMemberstackId(memberId) && env !== "sandbox") {
    const err = new Error("Memberstack sandbox member cannot be verified with a live Admin secret");
    err.code = "MEMBERSTACK_ENV_MISMATCH";
    throw err;
  }
  if (isLiveMemberstackId(memberId) && env !== "live") {
    const err = new Error("Memberstack live member cannot be verified with a sandbox Admin secret");
    err.code = "MEMBERSTACK_ENV_MISMATCH";
    throw err;
  }
}

async function verifyViaRestApiWithSecret(token, secret) {
  if (!secret || secret === "unused_verify_only") return null;
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
  assertSecretMatchesMemberId(secret, id);
  return {
    id,
    email: pickMemberEmail(payload),
    raw: payload,
    verifiedVia: "rest",
  };
}

async function verifyViaRestApi(token) {
  const secrets = candidateAdminSecrets(token);
  if (!secrets.length) return null;
  let lastErr = null;
  for (const secret of secrets) {
    try {
      const hit = await verifyViaRestApiWithSecret(token, secret);
      if (hit) return hit;
    } catch (err) {
      lastErr = err;
    }
  }
  if (lastErr) throw lastErr;
  return null;
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

async function verifyViaAdminPackageWithSecret(token, secret) {
  const client = memberstackAdmin.init(secret || "unused_verify_only");
  const env = memberstackSecretEnvironment(secret);
  const liveAudience = (process.env.MEMBERSTACK_APP_ID || "").trim() || undefined;
  const sandboxAudience =
    (process.env.MEMBERSTACK_TEST_APP_ID || process.env.MEMBERSTACK_SANDBOX_APP_ID || "").trim() ||
    undefined;
  const audience = env === "sandbox" ? sandboxAudience || liveAudience : liveAudience;
  const payload = await client.verifyToken({ token, ...(audience ? { audience } : {}) });
  const id = pickMemberId(payload);
  if (!id) {
    throw new Error("JWT payload missing member id");
  }
  assertSecretMatchesMemberId(secret, id);
  return {
    id,
    email: pickMemberEmail(payload),
    raw: payload,
    verifiedVia: "jwks",
  };
}

async function verifyViaAdminPackage(token) {
  const secrets = candidateAdminSecrets(token);
  const trySecrets = secrets.length ? secrets : ["unused_verify_only"];
  let lastErr = null;
  for (const secret of trySecrets) {
    try {
      return await verifyViaAdminPackageWithSecret(token, secret);
    } catch (err) {
      lastErr = err;
    }
  }
  throw lastErr || new Error("Invalid token");
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
