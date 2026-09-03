/**
 * SIGNED_SHARE_CAPABILITY_AUTHORIZATION — Existing Hotel ADP owner share links.
 * HMAC-signed capability tokens bound to property + surfaces. Secret never in client.
 */
import { createHmac, randomBytes, timingSafeEqual } from "crypto";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "fs";
import { join } from "path";

export const ADP_SHARE_TOKEN_VERSION = 1;
export const ADP_SHARE_TOKEN_PREFIX = "adpshare.v1.";
export const SIGNED_SHARE_CAPABILITY_AUTHORIZATION = "SIGNED_SHARE_CAPABILITY_AUTHORIZATION";
export const REPORT_API_OBJECT_LEVEL_AUTHORIZATION = "REPORT_API_OBJECT_LEVEL_AUTHORIZATION";
export const SHARE_TOKEN_PROPERTY_SCOPE_INTEGRITY = "SHARE_TOKEN_PROPERTY_SCOPE_INTEGRITY";
export const UNPUBLISHED_REPORT_NOT_ACCESSIBLE_BY_SHARE_TOKEN =
  "UNPUBLISHED_REPORT_NOT_ACCESSIBLE_BY_SHARE_TOKEN";

export const DEFAULT_SHARE_SURFACES = Object.freeze([
  "report",
  "evidence",
  "properties",
  "publication_meta",
]);

const REGISTRY_DIR =
  (process.env.ADP_SHARE_REGISTRY_DIR && String(process.env.ADP_SHARE_REGISTRY_DIR).trim()) ||
  join(process.cwd(), "config/client-share/adp-share-registry");
const REGISTRY_PATH = join(REGISTRY_DIR, "active-tokens.json");
const LEGACY_REGISTRY_PATH = join(
  process.cwd(),
  "data/ai-demand-positioning/share-registry/active-tokens.json"
);

function resolveRegistryPath() {
  if (existsSync(REGISTRY_PATH)) return REGISTRY_PATH;
  if (existsSync(LEGACY_REGISTRY_PATH)) return LEGACY_REGISTRY_PATH;
  return REGISTRY_PATH;
}

function b64url(buf) {
  return Buffer.from(buf)
    .toString("base64")
    .replace(/\+/g, "-")
    .replace(/\//g, "_")
    .replace(/=+$/g, "");
}

function b64urlJson(obj) {
  return b64url(Buffer.from(JSON.stringify(obj), "utf8"));
}

function fromB64url(str) {
  const pad = str.length % 4 === 0 ? "" : "=".repeat(4 - (str.length % 4));
  const b64 = str.replace(/-/g, "+").replace(/_/g, "/") + pad;
  return Buffer.from(b64, "base64");
}

export function getShareCapabilitySecret() {
  const secret = process.env.ADP_SHARE_CAPABILITY_SECRET || "";
  if (secret && secret.length >= 32) return secret;
  // Deterministic local/dev fallback ONLY when explicitly allowed — never production default.
  if (
    process.env.ADP_SHARE_CAPABILITY_ALLOW_DEV_SECRET === "1" ||
    process.env.ADP_SHARE_CAPABILITY_ALLOW_DEV_SECRET === "true"
  ) {
    return "adp-share-dev-secret-do-not-use-in-production-32b";
  }
  return null;
}

export function shareEnforcementEnabled() {
  if (process.env.ADP_SHARE_CAPABILITY_ENFORCE === "0") return false;
  if (process.env.ADP_SHARE_CAPABILITY_ENFORCE === "false") return false;
  return true; // fail closed by default
}

function loadRegistry() {
  const path = resolveRegistryPath();
  if (!existsSync(path)) {
    return { version: "adp_share_registry_v1", tokens: {} };
  }
  return JSON.parse(readFileSync(path, "utf8"));
}

function saveRegistry(reg) {
  mkdirSync(REGISTRY_DIR, { recursive: true });
  writeFileSync(REGISTRY_PATH, JSON.stringify(reg, null, 2) + "\n");
}

export function readShareRegistry() {
  return loadRegistry();
}

export function issueShareCapability({
  propertyId,
  label = null,
  surfaces = DEFAULT_SHARE_SURFACES,
  expiresAt = null,
  reportScope = "current_published",
  tokenId = null,
} = {}) {
  const secret = getShareCapabilitySecret();
  if (!secret) {
    throw new Error(
      "ADP_SHARE_CAPABILITY_SECRET missing (min 32 chars). Set env or ADP_SHARE_CAPABILITY_ALLOW_DEV_SECRET=1 for local only."
    );
  }
  if (!propertyId) throw new Error("propertyId required");
  const tid = tokenId || `sht_${randomBytes(12).toString("hex")}`;
  const iat = Math.floor(Date.now() / 1000);
  const payload = {
    v: ADP_SHARE_TOKEN_VERSION,
    tid,
    propertyId,
    surfaces: [...surfaces],
    reportScope,
    iat,
    exp: expiresAt ? Math.floor(new Date(expiresAt).getTime() / 1000) : null,
  };
  const body = b64urlJson(payload);
  const sig = b64url(createHmac("sha256", secret).update(body).digest());
  const token = `${ADP_SHARE_TOKEN_PREFIX}${body}.${sig}`;

  const reg = loadRegistry();
  reg.tokens[tid] = {
    tokenId: tid,
    propertyId,
    label: label || propertyId,
    surfaces: payload.surfaces,
    reportScope,
    status: "ACTIVE",
    issuedAt: new Date(iat * 1000).toISOString(),
    expiresAt: expiresAt || null,
    revokedAt: null,
  };
  saveRegistry(reg);

  return {
    token,
    tokenId: tid,
    propertyId,
    sharePath: `/owner-ai-demand-share.html?share=${encodeURIComponent(token)}`,
    meta: reg.tokens[tid],
  };
}

export function revokeShareCapability(tokenId, reason = "revoked") {
  const reg = loadRegistry();
  const row = reg.tokens[tokenId];
  if (!row) return { ok: false, error: "token_not_found" };
  row.status = "REVOKED";
  row.revokedAt = new Date().toISOString();
  row.revokeReason = reason;
  saveRegistry(reg);
  return { ok: true, tokenId, meta: row };
}

export function rotateShareCapability(tokenId, opts = {}) {
  const reg = loadRegistry();
  const row = reg.tokens[tokenId];
  if (!row) throw new Error("token_not_found");
  revokeShareCapability(tokenId, "rotated");
  return issueShareCapability({
    propertyId: row.propertyId,
    label: opts.label || row.label,
    surfaces: opts.surfaces || row.surfaces,
    expiresAt: opts.expiresAt || row.expiresAt,
    reportScope: opts.reportScope || row.reportScope,
  });
}

function safeEqualStr(a, b) {
  const ba = Buffer.from(String(a));
  const bb = Buffer.from(String(b));
  if (ba.length !== bb.length) return false;
  return timingSafeEqual(ba, bb);
}

/**
 * Verify signed share capability. Fail closed.
 */
export function verifyShareCapability(token, { expectedPropertyId = null, requiredSurface = null } = {}) {
  const secret = getShareCapabilitySecret();
  if (!secret) {
    return { ok: false, error: "share_secret_not_configured", code: "SHARE_SECRET_MISSING" };
  }
  if (!token || typeof token !== "string") {
    return { ok: false, error: "share_capability_required", code: "SHARE_MISSING" };
  }
  let raw = token.trim();
  if (raw.startsWith(ADP_SHARE_TOKEN_PREFIX)) {
    raw = raw.slice(ADP_SHARE_TOKEN_PREFIX.length);
  }
  const parts = raw.split(".");
  if (parts.length !== 2) {
    return { ok: false, error: "malformed_share_capability", code: "SHARE_MALFORMED" };
  }
  const [body, sig] = parts;
  const expectedSig = b64url(createHmac("sha256", secret).update(body).digest());
  if (!safeEqualStr(sig, expectedSig)) {
    return { ok: false, error: "invalid_share_signature", code: "SHARE_BAD_SIGNATURE" };
  }
  let payload;
  try {
    payload = JSON.parse(fromB64url(body).toString("utf8"));
  } catch {
    return { ok: false, error: "malformed_share_payload", code: "SHARE_MALFORMED" };
  }
  if (payload.v !== ADP_SHARE_TOKEN_VERSION) {
    return { ok: false, error: "unsupported_share_version", code: "SHARE_VERSION" };
  }
  if (!payload.tid || !payload.propertyId) {
    return { ok: false, error: "incomplete_share_payload", code: "SHARE_MALFORMED" };
  }
  if (payload.exp != null && Number(payload.exp) < Math.floor(Date.now() / 1000)) {
    return { ok: false, error: "share_capability_expired", code: "SHARE_EXPIRED" };
  }
  const reg = loadRegistry();
  const row = reg.tokens[payload.tid];
  if (!row) {
    return { ok: false, error: "share_capability_unknown", code: "SHARE_UNKNOWN" };
  }
  if (row.status !== "ACTIVE") {
    return { ok: false, error: "share_capability_revoked", code: "SHARE_REVOKED" };
  }
  if (row.propertyId !== payload.propertyId) {
    return { ok: false, error: "share_registry_mismatch", code: "SHARE_REGISTRY_MISMATCH" };
  }
  if (expectedPropertyId && expectedPropertyId !== payload.propertyId) {
    return {
      ok: false,
      error: "share_property_scope_mismatch",
      code: "SHARE_PROPERTY_SCOPE",
      tokenPropertyId: payload.propertyId,
      requestedPropertyId: expectedPropertyId,
    };
  }
  if (requiredSurface && !(payload.surfaces || []).includes(requiredSurface)) {
    return { ok: false, error: "share_surface_not_allowed", code: "SHARE_SURFACE" };
  }
  if (payload.reportScope && payload.reportScope !== "current_published") {
    // Controlled release: only current published customer reports
    return { ok: false, error: "share_scope_not_allowed", code: "SHARE_SCOPE" };
  }
  return {
    ok: true,
    claims: payload,
    meta: row,
    gate: SIGNED_SHARE_CAPABILITY_AUTHORIZATION,
  };
}

export function extractShareCapabilityFromRequest(req) {
  const q = req?.query || {};
  if (typeof q.share === "string" && q.share) return q.share;
  const h = req?.headers || {};
  if (typeof h["x-adp-share-capability"] === "string" && h["x-adp-share-capability"]) {
    return h["x-adp-share-capability"];
  }
  const auth = h.authorization || h.Authorization;
  if (typeof auth === "string" && auth.toLowerCase().startsWith("bearer ")) {
    const t = auth.slice(7).trim();
    // Memberstack JWTs also contain dots — only treat explicit ADP share prefix as capability.
    if (t.startsWith(ADP_SHARE_TOKEN_PREFIX)) return t;
  }
  return null;
}
