/**
 * GDI signed share capabilities — mirrors ADP/BAI share pattern.
 *
 * SHARE LINK DURABILITY LAW
 * A valid client-facing Dealality share URL must survive application commits,
 * application restarts, Railway redeployments, and horizontal instance changes.
 * A URL becomes invalid only because of explicit expiration, explicit revocation,
 * or an intentional security migration (with previous-key verification).
 *
 * Validity model: HMAC (stateless) + registry/revoke list (server-side).
 * Missing ACTIVE registry rows self-heal from a valid signature unless the
 * tokenId is on the durable revoked list.
 */
import { createHmac, randomBytes, timingSafeEqual } from "crypto";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "fs";
import { join } from "path";

export const GDI_SHARE_TOKEN_VERSION = 1;
export const GDI_SHARE_TOKEN_PREFIX = "gdishare.v1.";
export const GDI_SIGNED_SHARE_CAPABILITY = "GDI_SIGNED_SHARE_CAPABILITY";

/** Explicit permission capabilities (not inferred from page name). */
export const GDI_SHARE_CAPABILITY = Object.freeze({
  CAN_READ_BRIEF: "CAN_READ_BRIEF",
  CAN_READ_OPPORTUNITIES: "CAN_READ_OPPORTUNITIES",
  CAN_READ_OPPORTUNITY_DETAIL: "CAN_READ_OPPORTUNITY_DETAIL",
  CAN_READ_SUMMARY: "CAN_READ_SUMMARY",
  CAN_VALIDATE: "CAN_VALIDATE",
  CAN_RECORD_ACTION: "CAN_RECORD_ACTION",
  CAN_RECORD_OUTCOME: "CAN_RECORD_OUTCOME",
});

export const DEFAULT_GDI_SHARE_SURFACES = Object.freeze([
  "brief",
  "opportunities",
  "opportunity_detail",
  "summary",
]);

/** Customer-safe messages — never expose crypto/registry internals. */
export const GDI_SHARE_CUSTOMER_MESSAGES = Object.freeze({
  SHARE_EXPIRED:
    "This Dealality access link is no longer active. Please request an updated link from your Dealality contact.",
  SHARE_REVOKED:
    "This Dealality access link is no longer active. Please request an updated link from your Dealality contact.",
  SHARE_BAD_SIGNATURE:
    "This Dealality access link is no longer active. Please request an updated link from your Dealality contact.",
  SHARE_UNKNOWN:
    "This Dealality access link is no longer active. Please request an updated link from your Dealality contact.",
  SHARE_MALFORMED:
    "This Dealality access link is no longer active. Please request an updated link from your Dealality contact.",
  SHARE_SECRET_MISSING:
    "This Dealality access link is temporarily unavailable. Please try again later or contact Dealality.",
  SHARE_HOTEL_SCOPE:
    "This Dealality access link is no longer active. Please request an updated link from your Dealality contact.",
  SHARE_SURFACE:
    "This Dealality access link is no longer active. Please request an updated link from your Dealality contact.",
  SHARE_MISSING:
    "This Dealality access link is missing or incomplete. Please use the full link from your Dealality contact.",
  SHARE_VERSION:
    "This Dealality access link is no longer active. Please request an updated link from your Dealality contact.",
  SHARE_REGISTRY_MISMATCH:
    "This Dealality access link is no longer active. Please request an updated link from your Dealality contact.",
  INTERNAL_ERROR:
    "This Dealality access link is temporarily unavailable. Please try again later or contact Dealality.",
  HOTEL_NOT_FOUND:
    "This Dealality access link is no longer active. Please request an updated link from your Dealality contact.",
});

function resolveRegistryDir() {
  return (
    (process.env.GDI_SHARE_REGISTRY_DIR && String(process.env.GDI_SHARE_REGISTRY_DIR).trim()) ||
    join(process.cwd(), "config/client-share/gdi-share-registry")
  );
}

function resolveRegistryPath() {
  return join(resolveRegistryDir(), "active-tokens.json");
}

function resolveRevokedPath() {
  return join(resolveRegistryDir(), "revoked-token-ids.json");
}

function loadRegistry() {
  const registryPath = resolveRegistryPath();
  if (!existsSync(registryPath)) {
    return { version: "gdi_share_registry_v1", tokens: {} };
  }
  return JSON.parse(readFileSync(registryPath, "utf8"));
}

function saveRegistry(reg) {
  const registryDir = resolveRegistryDir();
  mkdirSync(registryDir, { recursive: true });
  writeFileSync(resolveRegistryPath(), JSON.stringify(reg, null, 2) + "\n");
}

/**
 * Durable revoke list — survives ephemeral registry loss on Railway.
 * Must be committed / shipped in deploy artifact.
 */
export function loadDurableRevokedTokenIds() {
  const p = resolveRevokedPath();
  if (!existsSync(p)) {
    return { version: "gdi_share_revoked_v1", tokenIds: [] };
  }
  try {
    const doc = JSON.parse(readFileSync(p, "utf8"));
    const ids = Array.isArray(doc.tokenIds) ? doc.tokenIds.map(String) : [];
    return { version: doc.version || "gdi_share_revoked_v1", tokenIds: ids };
  } catch {
    return { version: "gdi_share_revoked_v1", tokenIds: [] };
  }
}

function saveDurableRevokedTokenIds(doc) {
  const registryDir = resolveRegistryDir();
  mkdirSync(registryDir, { recursive: true });
  writeFileSync(resolveRevokedPath(), JSON.stringify(doc, null, 2) + "\n");
}

export function isTokenIdDurablyRevoked(tokenId) {
  const tid = String(tokenId || "").trim();
  if (!tid) return false;
  return loadDurableRevokedTokenIds().tokenIds.includes(tid);
}

export function markTokenIdDurablyRevoked(tokenId, reason = "revoked") {
  const tid = String(tokenId || "").trim();
  if (!tid) return { ok: false, error: "token_id_required" };
  const doc = loadDurableRevokedTokenIds();
  if (!doc.tokenIds.includes(tid)) {
    doc.tokenIds.push(tid);
    doc.updatedAt = new Date().toISOString();
    doc.lastReason = reason;
    saveDurableRevokedTokenIds(doc);
  }
  return { ok: true, tokenId: tid };
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

function safeEqualStr(a, b) {
  const ba = Buffer.from(String(a));
  const bb = Buffer.from(String(b));
  if (ba.length !== bb.length) return false;
  return timingSafeEqual(ba, bb);
}

export function getGdiShareCapabilitySecret() {
  const secret = process.env.GDI_SHARE_CAPABILITY_SECRET || "";
  if (secret && secret.length >= 32) return secret;
  if (
    process.env.GDI_SHARE_CAPABILITY_ALLOW_DEV_SECRET === "1" ||
    process.env.GDI_SHARE_CAPABILITY_ALLOW_DEV_SECRET === "true"
  ) {
    return "gdi-share-dev-secret-do-not-use-in-production-32b";
  }
  return null;
}

/**
 * Current signing secret + optional previous verification keys.
 * Env: GDI_SHARE_CAPABILITY_SECRET_PREVIOUS=secret1,secret2 (min 32 each)
 * Never generated at startup.
 */
export function getGdiShareVerificationSecrets() {
  const secrets = [];
  const current = getGdiShareCapabilitySecret();
  if (current) secrets.push(current);
  const prev = String(process.env.GDI_SHARE_CAPABILITY_SECRET_PREVIOUS || "").trim();
  if (prev) {
    for (const part of prev.split(",")) {
      const s = part.trim();
      if (s.length >= 32 && !secrets.includes(s)) secrets.push(s);
    }
  }
  return secrets;
}

export function isProductionLikeRuntime() {
  const nodeEnv = String(process.env.NODE_ENV || "").toLowerCase();
  if (nodeEnv === "production") return true;
  if (String(process.env.RAILWAY_ENVIRONMENT || "").trim()) return true;
  if (String(process.env.RAILWAY_ENVIRONMENT_NAME || "").toLowerCase() === "production") {
    return true;
  }
  return false;
}

/**
 * Fail hard in production-like runtimes if share secret is absent.
 * Call at server boot. Does not generate secrets.
 */
export function assertGdiShareProductionConfig() {
  if (!isProductionLikeRuntime()) {
    return { ok: true, skipped: true };
  }
  if (
    process.env.GDI_SHARE_CAPABILITY_ALLOW_DEV_SECRET === "1" ||
    process.env.GDI_SHARE_CAPABILITY_ALLOW_DEV_SECRET === "true"
  ) {
    const err = new Error(
      "GDI_SHARE_CAPABILITY_ALLOW_DEV_SECRET must not be enabled in production-like runtimes."
    );
    err.code = "GDI_SHARE_DEV_SECRET_IN_PRODUCTION";
    throw err;
  }
  const secret = process.env.GDI_SHARE_CAPABILITY_SECRET || "";
  if (!secret || secret.length < 32) {
    const err = new Error(
      "GDI_SHARE_CAPABILITY_SECRET missing or too short (min 32). Production share URLs cannot start without a durable signing secret."
    );
    err.code = "GDI_SHARE_SECRET_REQUIRED";
    throw err;
  }
  return { ok: true, secretConfigured: true };
}

export function gdiShareEnforcementEnabled() {
  if (process.env.GDI_SHARE_CAPABILITY_ENFORCE === "0") return false;
  if (process.env.GDI_SHARE_CAPABILITY_ENFORCE === "false") return false;
  return true;
}

/**
 * When a valid HMAC share token has no registry row (common after Railway
 * redeploys wipe ephemeral disk), auto-reseed the ACTIVE row so client URLs
 * keep working. Explicit durable REVOKED ids are never resurrected.
 * Set GDI_SHARE_SIGNATURE_SELF_HEAL=0 to disable.
 */
export function gdiShareSignatureSelfHealEnabled() {
  if (process.env.GDI_SHARE_SIGNATURE_SELF_HEAL === "0") return false;
  if (process.env.GDI_SHARE_SIGNATURE_SELF_HEAL === "false") return false;
  return true;
}

export function readGdiShareRegistry() {
  return loadRegistry();
}

export function customerMessageForShareCode(code) {
  const key = String(code || "INTERNAL_ERROR");
  return GDI_SHARE_CUSTOMER_MESSAGES[key] || GDI_SHARE_CUSTOMER_MESSAGES.INTERNAL_ERROR;
}

/**
 * Derive explicit capabilities from token payload and optional registry overlay.
 *
 * Precedence:
 * 1. ACTIVE registry row `capabilities` (server-side grant by tokenId — no URL change)
 * 2. Explicit capabilities embedded in signed payload
 * 3. Derived from surfaces (legacy)
 *
 * Legacy tokens with mode=read_only + opportunity_detail surface → CAN_VALIDATE
 * (historical Rad/Francesca share behavior — validation writes were intentional).
 */
export function resolveGdiShareCapabilities(payload, registryRow = null) {
  const fromRegistry = Array.isArray(registryRow?.capabilities)
    ? registryRow.capabilities.filter(Boolean).map(String)
    : [];
  if (fromRegistry.length) {
    return [...new Set(fromRegistry)];
  }
  const surfaces = Array.isArray(payload?.surfaces) ? payload.surfaces : [];
  const explicit = Array.isArray(payload?.capabilities) ? payload.capabilities : null;
  if (explicit && explicit.length) {
    return [...new Set(explicit.map(String))];
  }
  const caps = [];
  if (surfaces.includes("brief")) caps.push(GDI_SHARE_CAPABILITY.CAN_READ_BRIEF);
  if (surfaces.includes("opportunities")) caps.push(GDI_SHARE_CAPABILITY.CAN_READ_OPPORTUNITIES);
  if (surfaces.includes("opportunity_detail")) {
    caps.push(GDI_SHARE_CAPABILITY.CAN_READ_OPPORTUNITY_DETAIL);
    // Legacy: opportunity_detail share links were issued for customer validation.
    caps.push(GDI_SHARE_CAPABILITY.CAN_VALIDATE);
  }
  if (surfaces.includes("summary")) caps.push(GDI_SHARE_CAPABILITY.CAN_READ_SUMMARY);
  return caps;
}

/**
 * Server-side capability grant by tokenId — does NOT change the signed URL/token.
 * Only ACTIVE registry rows may be upgraded.
 */
export function grantGdiShareCapabilitiesByTokenId(
  tokenId,
  capabilities,
  { merge = true, reason = "capability_grant" } = {}
) {
  const tid = String(tokenId || "").trim();
  if (!tid) return { ok: false, error: "tokenId_required" };
  const wanted = [...new Set((capabilities || []).map(String).filter(Boolean))];
  if (!wanted.length) return { ok: false, error: "capabilities_required" };

  const reg = loadRegistry();
  const row = reg.tokens[tid];
  if (!row) return { ok: false, error: "token_unknown" };
  if (row.status !== "ACTIVE") return { ok: false, error: "token_not_active" };

  const nextCaps = merge
    ? [...new Set([...(row.capabilities || []), ...wanted])]
    : wanted;

  reg.tokens[tid] = {
    ...row,
    capabilities: nextCaps,
    capabilitiesUpdatedAt: new Date().toISOString(),
    capabilitiesUpdateReason: reason,
  };
  saveRegistry(reg);
  return { ok: true, tokenId: tid, meta: reg.tokens[tid], capabilities: nextCaps };
}

export function shareHasCapability(payloadOrCaps, capability) {
  const caps = Array.isArray(payloadOrCaps)
    ? payloadOrCaps
    : resolveGdiShareCapabilities(payloadOrCaps);
  return caps.includes(capability);
}

/**
 * Upsert an ACTIVE registry row from a verified share payload.
 * Never overwrites an existing REVOKED row. Never heals durable revoked ids.
 */
export function upsertGdiShareRegistryFromClaims(payload, { reason = "signature_self_heal" } = {}) {
  if (!payload?.tid || !payload?.hotelId) {
    return { ok: false, error: "incomplete_payload" };
  }
  if (isTokenIdDurablyRevoked(payload.tid)) {
    return { ok: false, error: "token_revoked", healed: false };
  }
  const reg = loadRegistry();
  const existing = reg.tokens[payload.tid];
  if (existing && existing.status === "REVOKED") {
    markTokenIdDurablyRevoked(payload.tid, "registry_revoked_observed");
    return { ok: false, error: "token_revoked", meta: existing, healed: false };
  }
  if (existing && existing.status === "ACTIVE") {
    return { ok: true, meta: existing, healed: false };
  }
  const row = {
    tokenId: payload.tid,
    hotelId: payload.hotelId,
    label: (existing && existing.label) || payload.hotelId,
    surfaces: Array.isArray(payload.surfaces)
      ? [...payload.surfaces]
      : [...DEFAULT_GDI_SHARE_SURFACES],
    mode: payload.mode || "read_only",
    capabilities: resolveGdiShareCapabilities(payload),
    status: "ACTIVE",
    issuedAt: payload.iat
      ? new Date(Number(payload.iat) * 1000).toISOString()
      : new Date().toISOString(),
    expiresAt: payload.exp
      ? new Date(Number(payload.exp) * 1000).toISOString().slice(0, 10)
      : null,
    revokedAt: null,
    restoredAt: new Date().toISOString(),
    restoreReason: reason,
  };
  reg.tokens[payload.tid] = row;
  saveRegistry(reg);
  return { ok: true, meta: row, healed: true };
}

/**
 * Restore / re-register a share URL from its token string (same URL kept).
 * Requires valid HMAC under current or previous GDI share secrets.
 */
export function restoreGdiShareCapabilityFromToken(token, { reason = "manual_restore" } = {}) {
  const verified = verifyGdiShareCapability(token, { allowSelfHeal: true, restoreReason: reason });
  if (!verified.ok) return verified;
  return {
    ok: true,
    tokenId: verified.claims.tid,
    hotelId: verified.claims.hotelId,
    claims: verified.claims,
    meta: verified.meta,
    healed: verified.healed === true,
  };
}

function defaultCapabilitiesForIssue(surfaces) {
  return resolveGdiShareCapabilities({ surfaces });
}

export function issueGdiShareCapability({
  hotelId,
  label = null,
  surfaces = DEFAULT_GDI_SHARE_SURFACES,
  capabilities = null,
  expiresAt = null,
  tokenId = null,
} = {}) {
  const secret = getGdiShareCapabilitySecret();
  if (!secret) {
    throw new Error(
      "GDI_SHARE_CAPABILITY_SECRET missing (min 32 chars). Set env or GDI_SHARE_CAPABILITY_ALLOW_DEV_SECRET=1 for local only."
    );
  }
  if (!hotelId) throw new Error("hotelId required");
  const tid = tokenId || `gdisht_${randomBytes(12).toString("hex")}`;
  const iat = Math.floor(Date.now() / 1000);
  const surfaceList = [...surfaces];
  const caps =
    Array.isArray(capabilities) && capabilities.length
      ? [...new Set(capabilities.map(String))]
      : defaultCapabilitiesForIssue(surfaceList);
  const payload = {
    v: GDI_SHARE_TOKEN_VERSION,
    tid,
    hotelId,
    surfaces: surfaceList,
    capabilities: caps,
    mode: "read_only",
    iat,
    exp: expiresAt ? Math.floor(new Date(expiresAt).getTime() / 1000) : null,
  };
  const body = b64urlJson(payload);
  const sig = b64url(createHmac("sha256", secret).update(body).digest());
  const token = `${GDI_SHARE_TOKEN_PREFIX}${body}.${sig}`;

  const reg = loadRegistry();
  reg.tokens[tid] = {
    tokenId: tid,
    hotelId,
    label: label || hotelId,
    surfaces: payload.surfaces,
    capabilities: caps,
    mode: "read_only",
    status: "ACTIVE",
    issuedAt: new Date(iat * 1000).toISOString(),
    expiresAt: expiresAt || null,
    revokedAt: null,
  };
  saveRegistry(reg);

  return {
    token,
    tokenId: tid,
    hotelId,
    sharePath: `/group-demand-intelligence-share.html?share=${encodeURIComponent(token)}`,
    meta: reg.tokens[tid],
  };
}

export function revokeGdiShareCapability(tokenId, reason = "revoked") {
  const reg = loadRegistry();
  const row = reg.tokens[tokenId];
  markTokenIdDurablyRevoked(tokenId, reason);
  if (!row) {
    return { ok: true, tokenId, meta: null, durableRevokeOnly: true };
  }
  row.status = "REVOKED";
  row.revokedAt = new Date().toISOString();
  row.revokeReason = reason;
  saveRegistry(reg);
  return { ok: true, tokenId, meta: row };
}

function verifySignatureWithSecrets(body, sig, secrets) {
  for (const secret of secrets) {
    const expectedSig = b64url(createHmac("sha256", secret).update(body).digest());
    if (safeEqualStr(sig, expectedSig)) return true;
  }
  return false;
}

export function verifyGdiShareCapability(
  token,
  {
    expectedHotelId = null,
    requiredSurface = null,
    requiredCapability = null,
    allowSelfHeal = null,
    restoreReason = "signature_self_heal",
  } = {}
) {
  const secrets = getGdiShareVerificationSecrets();
  if (!secrets.length) {
    return {
      ok: false,
      error: "share_secret_not_configured",
      code: "SHARE_SECRET_MISSING",
      customerMessage: customerMessageForShareCode("SHARE_SECRET_MISSING"),
    };
  }
  if (!token || typeof token !== "string") {
    return {
      ok: false,
      error: "share_capability_required",
      code: "SHARE_MISSING",
      customerMessage: customerMessageForShareCode("SHARE_MISSING"),
    };
  }
  let raw = token.trim();
  if (raw.startsWith(GDI_SHARE_TOKEN_PREFIX)) {
    raw = raw.slice(GDI_SHARE_TOKEN_PREFIX.length);
  }
  const parts = raw.split(".");
  if (parts.length !== 2) {
    return {
      ok: false,
      error: "malformed_share_capability",
      code: "SHARE_MALFORMED",
      customerMessage: customerMessageForShareCode("SHARE_MALFORMED"),
    };
  }
  const [body, sig] = parts;
  if (!verifySignatureWithSecrets(body, sig, secrets)) {
    return {
      ok: false,
      error: "invalid_share_signature",
      code: "SHARE_BAD_SIGNATURE",
      customerMessage: customerMessageForShareCode("SHARE_BAD_SIGNATURE"),
    };
  }
  let payload;
  try {
    payload = JSON.parse(fromB64url(body).toString("utf8"));
  } catch {
    return {
      ok: false,
      error: "malformed_share_payload",
      code: "SHARE_MALFORMED",
      customerMessage: customerMessageForShareCode("SHARE_MALFORMED"),
    };
  }
  if (payload.v !== GDI_SHARE_TOKEN_VERSION) {
    return {
      ok: false,
      error: "unsupported_share_version",
      code: "SHARE_VERSION",
      customerMessage: customerMessageForShareCode("SHARE_VERSION"),
    };
  }
  if (!payload.tid || !payload.hotelId) {
    return {
      ok: false,
      error: "incomplete_share_payload",
      code: "SHARE_MALFORMED",
      customerMessage: customerMessageForShareCode("SHARE_MALFORMED"),
    };
  }
  if (payload.exp != null && Number(payload.exp) < Math.floor(Date.now() / 1000)) {
    return {
      ok: false,
      error: "share_capability_expired",
      code: "SHARE_EXPIRED",
      customerMessage: customerMessageForShareCode("SHARE_EXPIRED"),
    };
  }
  if (isTokenIdDurablyRevoked(payload.tid)) {
    return {
      ok: false,
      error: "share_capability_revoked",
      code: "SHARE_REVOKED",
      customerMessage: customerMessageForShareCode("SHARE_REVOKED"),
    };
  }
  const reg = loadRegistry();
  let row = reg.tokens[payload.tid];
  let healed = false;
  const selfHeal =
    allowSelfHeal == null ? gdiShareSignatureSelfHealEnabled() : !!allowSelfHeal;
  if (!row) {
    if (!selfHeal) {
      return {
        ok: false,
        error: "share_capability_unknown",
        code: "SHARE_UNKNOWN",
        customerMessage: customerMessageForShareCode("SHARE_UNKNOWN"),
      };
    }
    const upsert = upsertGdiShareRegistryFromClaims(payload, { reason: restoreReason });
    if (!upsert.ok) {
      const code = upsert.error === "token_revoked" ? "SHARE_REVOKED" : "SHARE_UNKNOWN";
      return {
        ok: false,
        error:
          upsert.error === "token_revoked"
            ? "share_capability_revoked"
            : "share_capability_unknown",
        code,
        customerMessage: customerMessageForShareCode(code),
      };
    }
    row = upsert.meta;
    healed = upsert.healed === true;
  }
  if (row.status !== "ACTIVE") {
    markTokenIdDurablyRevoked(payload.tid, row.revokeReason || "registry_not_active");
    return {
      ok: false,
      error: "share_capability_revoked",
      code: "SHARE_REVOKED",
      customerMessage: customerMessageForShareCode("SHARE_REVOKED"),
    };
  }
  if (row.hotelId !== payload.hotelId) {
    return {
      ok: false,
      error: "share_registry_mismatch",
      code: "SHARE_REGISTRY_MISMATCH",
      customerMessage: customerMessageForShareCode("SHARE_REGISTRY_MISMATCH"),
    };
  }
  if (expectedHotelId && expectedHotelId !== payload.hotelId) {
    return {
      ok: false,
      error: "share_hotel_scope_mismatch",
      code: "SHARE_HOTEL_SCOPE",
      tokenHotelId: payload.hotelId,
      requestedHotelId: expectedHotelId,
      customerMessage: customerMessageForShareCode("SHARE_HOTEL_SCOPE"),
    };
  }
  if (requiredSurface && !(payload.surfaces || []).includes(requiredSurface)) {
    return {
      ok: false,
      error: "share_surface_not_allowed",
      code: "SHARE_SURFACE",
      customerMessage: customerMessageForShareCode("SHARE_SURFACE"),
    };
  }
  const capabilities = resolveGdiShareCapabilities(payload, row);
  if (requiredCapability && !capabilities.includes(requiredCapability)) {
    return {
      ok: false,
      error: "share_capability_not_allowed",
      code: "SHARE_SURFACE",
      customerMessage: customerMessageForShareCode("SHARE_SURFACE"),
    };
  }
  return {
    ok: true,
    claims: { ...payload, capabilities },
    capabilities,
    meta: row,
    healed,
    gate: GDI_SIGNED_SHARE_CAPABILITY,
  };
}

export function extractGdiShareCapabilityFromRequest(req) {
  const q = req?.query || {};
  if (typeof q.share === "string" && q.share) return q.share;
  const h = req?.headers || {};
  if (typeof h["x-gdi-share-capability"] === "string" && h["x-gdi-share-capability"]) {
    return h["x-gdi-share-capability"];
  }
  const auth = h.authorization || h.Authorization;
  if (typeof auth === "string" && auth.toLowerCase().startsWith("bearer ")) {
    const t = auth.slice(7).trim();
    if (t.startsWith(GDI_SHARE_TOKEN_PREFIX)) return t;
  }
  return null;
}

/**
 * Strip internal/admin fields for external share payloads.
 */
export function sanitizeOpportunityForShare(opp) {
  if (!opp) return null;
  const evidence = (opp.evidence || [])
    .filter((e) => e.claimKind === "FACT" || e.sourceAuthority === "Tier_A" || e.sourceAuthority === "Tier_B")
    .slice(0, 8)
    .map((e) => ({
      field: e.field,
      value: e.value,
      claimKind: e.claimKind,
      sourceTitle: e.sourceTitle,
      sourceUrl: e.sourceUrl,
      sourceDomain: e.sourceDomain,
      confidence: e.confidence,
    }));
  return {
    id: opp.id,
    title: opp.title,
    organizationName: opp.organizationName,
    segment: opp.segment,
    demandType: opp.demandType,
    demandStatus: opp.demandStatus,
    priority: opp.priority,
    eventStartDate: opp.eventStartDate,
    eventEndDate: opp.eventEndDate,
    destinationStatus: opp.destinationStatus,
    venueStatus: opp.venueStatus,
    estimatedAttendance: opp.estimatedAttendance,
    estimatedAttendanceClaimKind: opp.estimatedAttendanceClaimKind,
    estimatedPeakRooms: opp.estimatedPeakRooms,
    estimatedPeakRoomsClaimKind: opp.estimatedPeakRoomsClaimKind,
    estimatedNights: opp.estimatedNights,
    estimatedNightsClaimKind: opp.estimatedNightsClaimKind,
    hotelFitScore: opp.hotelFitScore,
    physicalFitScore: opp.physicalFitScore,
    geographyFitScore: opp.geographyFitScore,
    timingScore: opp.timingScore,
    commercialValueScore: opp.commercialValueScore,
    historicalFitScore: opp.historicalFitScore,
    competitiveAccessibilityScore: opp.competitiveAccessibilityScore,
    contactabilityScore: opp.contactabilityScore,
    evidenceConfidence: opp.evidenceConfidence,
    evidenceConfidenceExplanation: opp.evidenceConfidenceExplanation || null,
    evidenceConfidenceTooltip: opp.evidenceConfidenceTooltip || null,
    hotelFitComponentLabels: opp.hotelFitComponentLabels || null,
    bookingWindowStatus: opp.bookingWindowStatus,
    bookingWindowLabel: opp.bookingWindowLabel || null,
    demandTerritoryFit: opp.demandTerritoryFit || null,
    demandTerritoryFitLabel: opp.demandTerritoryFitLabel || null,
    demandTerritoryRationale: opp.demandTerritoryRationale || null,
    bethesdaWinThesis: opp.bethesdaWinThesis || null,
    hotelOpportunityThesis: opp.hotelOpportunityThesis || opp.bethesdaWinThesis || null,
    opportunityType: opp.opportunityType || null,
    opportunityTypeLabel: opp.opportunityTypeLabel || null,
    venueSourcingStatus: opp.venueSourcingStatus || null,
    venueSourcingStatusLabel: opp.venueSourcingStatusLabel || null,
    venueSourcingRationale: opp.venueSourcingRationale || null,
    eventLocationStatus: opp.eventLocationStatus || null,
    eventLocationStatusLabel: opp.eventLocationStatusLabel || null,
    eventLocationSummary: opp.eventLocationSummary || null,
    roomDemandStatus: opp.roomDemandStatus || null,
    roomDemandStatusLabel: opp.roomDemandStatusLabel || null,
    roomDemandRationale: opp.roomDemandRationale || null,
    roomDemandConfidence: opp.roomDemandConfidence ?? null,
    publishedAttendance: opp.publishedAttendance ?? null,
    publishedPeakRooms: opp.publishedPeakRooms ?? null,
    opportunityQualification: opp.opportunityQualification || null,
    opportunityQualificationLabel: opp.opportunityQualificationLabel || null,
    contactQuality: opp.contactQuality || null,
    contactQualityLabel: opp.contactQualityLabel || null,
    contactRole: opp.contactRole || null,
    relationshipToEvent: opp.relationshipToEvent || null,
    relationshipConfidence: opp.relationshipConfidence || null,
    reactivationSignal: opp.reactivationSignal || null,
    reactivationSignalLabel: opp.reactivationSignalLabel || null,
    reactivationThesis: opp.reactivationThesis || null,
    marketCompetitors: (opp.marketCompetitors || []).slice(0, 8),
    sourcingStatus: opp.sourcingStatus || "UNKNOWN",
    sourcingStatusLabel: opp.sourcingStatusLabel || "Unknown — requires hotel validation",
    alreadySourcedToHotelDisplay:
      opp.alreadySourcedToHotelDisplay || "Unknown — requires hotel validation",
    incrementalValueStatus: opp.incrementalValueStatus || "UNKNOWN",
    incrementalValueStatusLabel: opp.incrementalValueStatusLabel || null,
    whyNow: opp.whyNow,
    fitExplanation: opp.fitExplanation,
    summaryWhat: opp.summaryWhat,
    summaryWhyMatters: opp.summaryWhyMatters,
    summaryWhyHotel: opp.summaryWhyHotel,
    recommendedAction: opp.recommendedAction,
    likelyCompetitor: opp.likelyCompetitor,
    likelyStrCompetitor: opp.likelyStrCompetitor || null,
    likelyGroupCompetitor: opp.likelyGroupCompetitor || null,
    competitorRationale: opp.competitorRationale,
    sources: (opp.sources || []).slice(0, 8).map((s) => ({
      name: s.name,
      sourceType: s.sourceType,
      date: s.date,
      url: s.url,
      supportsFact: s.supportsFact,
      claimKind: s.claimKind,
    })),
    knownVsEstimated: opp.knownVsEstimated
      ? {
          verified: (opp.knownVsEstimated.verified || []).slice(0, 8),
          estimated: (opp.knownVsEstimated.estimated || []).slice(0, 8),
          inferred: (opp.knownVsEstimated.inferred || []).slice(0, 8),
        }
      : null,
    competitors: (opp.competitors || []).slice(0, 8).map((c) => ({
      name: c.name,
      class: c.class,
      classLabel: c.classLabel,
      reason: c.reason,
    })),
    contactGrade: opp.contactGrade || null,
    contactGradeLabel: opp.contactGradeLabel || null,
    contactConfidence: opp.contactConfidence ?? null,
    contactRoleMatch: opp.contactRoleMatch || null,
    contactRoleMatchLabel: opp.contactRoleMatchLabel || null,
    whyThisContact: opp.whyThisContact || null,
    primaryContact: opp.primaryContact
      ? {
          name: opp.primaryContact.name,
          role: opp.primaryContact.role,
          title: opp.primaryContact.title || opp.primaryContact.role,
          organization: opp.primaryContact.organization,
          email: opp.primaryContact.email,
          emailType: opp.primaryContact.emailType || null,
          emailTypeLabel: opp.primaryContact.emailTypeLabel || null,
          emailVerificationStatus: opp.primaryContact.emailVerificationStatus || null,
          emailVerificationStatusLabel:
            opp.primaryContact.emailVerificationStatusLabel || null,
          phone: opp.primaryContact.phone || null,
          phoneType: opp.primaryContact.phoneType || null,
          phoneTypeLabel: opp.primaryContact.phoneTypeLabel || null,
          contactGrade: opp.primaryContact.contactGrade || opp.contactGrade || null,
          contactGradeLabel:
            opp.primaryContact.contactGradeLabel || opp.contactGradeLabel || null,
          contactConfidence:
            opp.primaryContact.contactConfidence ?? opp.contactConfidence ?? null,
          targetRoleMatch: opp.primaryContact.targetRoleMatch || null,
          targetRoleMatchLabel: opp.primaryContact.targetRoleMatchLabel || null,
          relationshipToEvent: opp.primaryContact.relationshipToEvent || null,
          whyThisContact: opp.primaryContact.whyThisContact || opp.whyThisContact || null,
          lastVerifiedAt: opp.primaryContact.lastVerifiedAt || null,
          claimKind: opp.primaryContact.claimKind,
        }
      : null,
    backupContacts: (opp.backupContacts || []).slice(0, 3).map((c) => ({
      name: c.name,
      role: c.role,
      organization: c.organization,
      email: c.email,
      phone: c.phone || null,
      phoneTypeLabel: c.phoneTypeLabel || null,
      emailVerificationStatusLabel: c.emailVerificationStatusLabel || null,
      contactGrade: c.contactGrade || null,
      whyThisContact: c.whyThisContact || null,
    })),
    meetingHistory: (opp.meetingHistory || []).map((h) => ({
      year: h.year,
      city: h.city,
      venue: h.venue,
      claimKind: h.claimKind,
    })),
    evidence,
    plannerConsideration: opp.plannerConsideration
      ? {
          experimental: true,
          label: "EXPERIMENTAL",
          prompt: opp.plannerConsideration.prompt,
          bethesdaAppeared: opp.plannerConsideration.bethesdaAppeared,
          bethesdaRank: opp.plannerConsideration.bethesdaRank,
          observations: opp.plannerConsideration.observations,
        }
      : null,
  };
}
