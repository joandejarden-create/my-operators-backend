/**
 * BAI signed share capabilities:
 * - BAI_BRAND_SHARE (baishare.v1.) — one brand (legacy / internal)
 * - BAI_PARENT_COMPANY_SHARE (baiparent.v1.) — one parent company, N brands
 *
 * Security boundary is server verify + entitlement scope.
 * Selector UX is not the authorization boundary.
 */
import { createHmac, randomBytes, timingSafeEqual } from "crypto";
import { existsSync, mkdirSync, readFileSync, writeFileSync } from "fs";
import { join } from "path";
import {
  getShowcaseBrandDisplay,
  getShowcaseCompany,
  getShowcasePortfolioBrandIds,
  isShowcaseMonitoringBrand,
  listShowcaseCompanyKeys,
  loadShowcaseCompaniesConfig,
} from "../brand-ai-showcase-companies.js";
import { buildFixtureEntitlementGraph } from "../entitlements.js";
import {
  peerSetBrandNamesById,
  PEER_SET_ID_V2,
  resolvePeerSetMembership,
} from "../peer-sets.js";

export const BAI_SHARE_TOKEN_VERSION = 1;
export const BAI_SHARE_TOKEN_PREFIX = "baishare.v1.";
export const BAI_PARENT_SHARE_TOKEN_PREFIX = "baiparent.v1.";
export const BAI_BRAND_SHARE = "BAI_BRAND_SHARE";
export const BAI_PARENT_COMPANY_SHARE = "BAI_PARENT_COMPANY_SHARE";
export const EXTERNAL_BAI_SHARE_BRAND_SELECTOR_DISABLED =
  "EXTERNAL_BAI_SHARE_BRAND_SELECTOR_DISABLED";
export const EXTERNAL_BAI_SHARE_PARENT_SCOPED_SELECTOR =
  "EXTERNAL_BAI_SHARE_PARENT_SCOPED_SELECTOR";
export const BAI_PARENT_SHARE_BRAND_SCOPE_AUTHORIZATION =
  "BAI_PARENT_SHARE_BRAND_SCOPE_AUTHORIZATION";
export const BAI_PARENT_SHARE_DOES_NOT_ENUMERATE_OTHER_PARENTS =
  "BAI_PARENT_SHARE_DOES_NOT_ENUMERATE_OTHER_PARENTS";
export const BAI_PARENT_SHARE_TAB_ISOLATION = "BAI_PARENT_SHARE_TAB_ISOLATION";
export const BAI_PARENT_SHARE_PUBLICATION_BOUNDARY =
  "BAI_PARENT_SHARE_PUBLICATION_BOUNDARY";
export const SHARE_CAPABILITY_CANNOT_BYPASS_PUBLICATION_STATE =
  "SHARE_CAPABILITY_CANNOT_BYPASS_PUBLICATION_STATE";

export const DEFAULT_BAI_SHARE_SURFACES = Object.freeze([
  "report",
  "evidence",
  "portfolio",
  "executive_summary",
]);

export const CURRENT_CUSTOMER_PUBLISHED_SCOPE = "current_published";

const REGISTRY_DIR =
  (process.env.BAI_SHARE_REGISTRY_DIR && String(process.env.BAI_SHARE_REGISTRY_DIR).trim()) ||
  join(process.cwd(), "config/client-share/bai-share-registry");
const REGISTRY_PATH = join(REGISTRY_DIR, "active-tokens.json");
const LEGACY_REGISTRY_PATH = join(
  process.cwd(),
  "data/ai-visibility/share-registry/active-tokens.json"
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

function safeEqualStr(a, b) {
  const ba = Buffer.from(String(a));
  const bb = Buffer.from(String(b));
  if (ba.length !== bb.length) return false;
  return timingSafeEqual(ba, bb);
}

export function getBaiShareCapabilitySecret() {
  const secret = process.env.BAI_SHARE_CAPABILITY_SECRET || "";
  if (secret && secret.length >= 32) return secret;
  if (
    process.env.BAI_SHARE_CAPABILITY_ALLOW_DEV_SECRET === "1" ||
    process.env.BAI_SHARE_CAPABILITY_ALLOW_DEV_SECRET === "true"
  ) {
    return "bai-share-dev-secret-do-not-use-in-production-32b";
  }
  return null;
}

export function baiShareEnforcementEnabled() {
  if (process.env.BAI_SHARE_CAPABILITY_ENFORCE === "0") return false;
  if (process.env.BAI_SHARE_CAPABILITY_ENFORCE === "false") return false;
  return true;
}

function loadRegistry() {
  const path = resolveRegistryPath();
  if (!existsSync(path)) {
    return { version: "bai_share_registry_v1", tokens: {} };
  }
  return JSON.parse(readFileSync(path, "utf8"));
}

function saveRegistry(reg) {
  mkdirSync(REGISTRY_DIR, { recursive: true });
  writeFileSync(REGISTRY_PATH, JSON.stringify(reg, null, 2) + "\n");
}

export function readBaiShareRegistry() {
  return loadRegistry();
}

/**
 * Governed parent groupings from showcase cohort (server-side only).
 */
export function listGovernedBaiParentCompanies(config) {
  const cfg = config || loadShowcaseCompaniesConfig();
  return (cfg.companies || []).map((c) => {
    const brandIds = [...(c.brandIds || [])].map(String);
    const brands = (c.brands || [])
      .filter((b) => b?.brandId)
      .map((b) => ({
        brandId: String(b.brandId),
        brandName: String(b.brandName || b.brandId),
      }));
    // Prefer brandIds order; fill names from brands[] when present
    const byId = new Map(brands.map((b) => [b.brandId, b]));
    const ordered = brandIds.map((id) => byId.get(id) || getShowcaseBrandDisplay(id) || {
      brandId: id,
      brandName: id,
    });
    return {
      parentCompanyId: String(c.companyKey),
      companyKey: String(c.companyKey),
      canonicalCompanyName: String(c.canonicalCompanyName || c.companyKey),
      brandIds,
      brands: ordered,
      defaultBrandId: brandIds[0] || null,
      ambiguous: false,
    };
  });
}

export function resolveGovernedBaiParentCompany(parentCompanyId, config) {
  const key = String(parentCompanyId || "")
    .trim()
    .toLowerCase();
  if (!key) return { ok: false, error: "parent_company_required" };
  const hit = listGovernedBaiParentCompanies(config).find(
    (p) => p.parentCompanyId === key || p.companyKey === key
  );
  if (!hit) return { ok: false, error: `parent_company_not_found:${parentCompanyId}` };
  // Server-derived allowlist — never trust client brandIds
  const portfolio = getShowcasePortfolioBrandIds(hit.companyKey, config);
  if (!portfolio.ok) return portfolio;
  return {
    ok: true,
    ...hit,
    brandIds: [...portfolio.brandIds],
    allowedBrandIds: [...portfolio.brandIds],
    defaultBrandId: portfolio.brandIds[0] || hit.defaultBrandId,
  };
}

export function getAllowedBrandIdsForParentShare(parentCompanyId) {
  const parent = resolveGovernedBaiParentCompany(parentCompanyId);
  if (!parent.ok) return [];
  return parent.allowedBrandIds;
}

function assertCurrentPublishedScope(reportScope) {
  if (reportScope && reportScope !== CURRENT_CUSTOMER_PUBLISHED_SCOPE) {
    throw new Error("share_scope_not_allowed:" + reportScope);
  }
}

export function issueBaiShareCapability({
  brandId,
  label = null,
  surfaces = DEFAULT_BAI_SHARE_SURFACES,
  expiresAt = null,
  reportScope = CURRENT_CUSTOMER_PUBLISHED_SCOPE,
  tokenId = null,
} = {}) {
  const secret = getBaiShareCapabilitySecret();
  if (!secret) {
    throw new Error(
      "BAI_SHARE_CAPABILITY_SECRET missing (min 32 chars). Set env or BAI_SHARE_CAPABILITY_ALLOW_DEV_SECRET=1 for local only."
    );
  }
  assertCurrentPublishedScope(reportScope);
  if (!brandId) throw new Error("brandId required");
  if (!isShowcaseMonitoringBrand(brandId)) {
    throw new Error("brand_not_share_eligible:" + brandId);
  }
  const display = getShowcaseBrandDisplay(brandId);
  const tid = tokenId || `sht_bai_${randomBytes(12).toString("hex")}`;
  const iat = Math.floor(Date.now() / 1000);
  const payload = {
    v: BAI_SHARE_TOKEN_VERSION,
    kind: BAI_BRAND_SHARE,
    tid,
    brandId,
    surfaces: [...surfaces],
    reportScope: CURRENT_CUSTOMER_PUBLISHED_SCOPE,
    iat,
    exp: expiresAt ? Math.floor(new Date(expiresAt).getTime() / 1000) : null,
  };
  const body = b64urlJson(payload);
  const sig = b64url(createHmac("sha256", secret).update(body).digest());
  const token = `${BAI_SHARE_TOKEN_PREFIX}${body}.${sig}`;

  const reg = loadRegistry();
  reg.tokens[tid] = {
    tokenId: tid,
    kind: BAI_BRAND_SHARE,
    brandId,
    label: label || display?.brandName || brandId,
    surfaces: payload.surfaces,
    reportScope: payload.reportScope,
    status: "ACTIVE",
    issuedAt: new Date(iat * 1000).toISOString(),
    expiresAt: expiresAt || null,
    revokedAt: null,
  };
  saveRegistry(reg);

  return {
    token,
    tokenId: tid,
    kind: BAI_BRAND_SHARE,
    brandId,
    brandName: display?.brandName || brandId,
    sharePath: `/brand-ai-visibility-share.html?share=${encodeURIComponent(token)}`,
    meta: reg.tokens[tid],
  };
}

/**
 * Issue BAI_PARENT_COMPANY_SHARE — allowedBrandIds derived server-side only.
 */
export function issueBaiParentShareCapability({
  parentCompanyId,
  label = null,
  surfaces = DEFAULT_BAI_SHARE_SURFACES,
  expiresAt = null,
  reportScope = CURRENT_CUSTOMER_PUBLISHED_SCOPE,
  tokenId = null,
} = {}) {
  const secret = getBaiShareCapabilitySecret();
  if (!secret) {
    throw new Error(
      "BAI_SHARE_CAPABILITY_SECRET missing (min 32 chars). Set env or BAI_SHARE_CAPABILITY_ALLOW_DEV_SECRET=1 for local only."
    );
  }
  assertCurrentPublishedScope(reportScope);
  const parent = resolveGovernedBaiParentCompany(parentCompanyId);
  if (!parent.ok) throw new Error(parent.error || "parent_company_not_found");
  if (!parent.allowedBrandIds?.length) {
    throw new Error("parent_company_has_no_brands:" + parentCompanyId);
  }

  const tid = tokenId || `sht_baip_${randomBytes(12).toString("hex")}`;
  const iat = Math.floor(Date.now() / 1000);
  // Payload binds parent only — allowlist re-derived on every verify
  const payload = {
    v: BAI_SHARE_TOKEN_VERSION,
    kind: BAI_PARENT_COMPANY_SHARE,
    tid,
    parentCompanyId: parent.parentCompanyId,
    surfaces: [...surfaces],
    reportScope: CURRENT_CUSTOMER_PUBLISHED_SCOPE,
    iat,
    exp: expiresAt ? Math.floor(new Date(expiresAt).getTime() / 1000) : null,
  };
  const body = b64urlJson(payload);
  const sig = b64url(createHmac("sha256", secret).update(body).digest());
  const token = `${BAI_PARENT_SHARE_TOKEN_PREFIX}${body}.${sig}`;

  const reg = loadRegistry();
  reg.tokens[tid] = {
    tokenId: tid,
    kind: BAI_PARENT_COMPANY_SHARE,
    parentCompanyId: parent.parentCompanyId,
    label: label || parent.canonicalCompanyName,
    surfaces: payload.surfaces,
    reportScope: payload.reportScope,
    status: "ACTIVE",
    issuedAt: new Date(iat * 1000).toISOString(),
    expiresAt: expiresAt || null,
    revokedAt: null,
    allowedBrandIdsAtIssue: [...parent.allowedBrandIds],
  };
  saveRegistry(reg);

  return {
    token,
    tokenId: tid,
    kind: BAI_PARENT_COMPANY_SHARE,
    parentCompanyId: parent.parentCompanyId,
    parentCompanyName: parent.canonicalCompanyName,
    allowedBrandIds: [...parent.allowedBrandIds],
    brands: parent.brands,
    defaultBrandId: parent.defaultBrandId,
    sharePath: `/brand-ai-visibility-share.html?share=${encodeURIComponent(token)}`,
    meta: reg.tokens[tid],
    gate: BAI_PARENT_COMPANY_SHARE,
  };
}

export function revokeBaiShareCapability(tokenId, reason = "revoked") {
  const reg = loadRegistry();
  const row = reg.tokens[tokenId];
  if (!row) return { ok: false, error: "token_not_found" };
  row.status = "REVOKED";
  row.revokedAt = new Date().toISOString();
  row.revokeReason = reason;
  saveRegistry(reg);
  return { ok: true, tokenId, meta: row };
}

export function rotateBaiShareCapability(tokenId, opts = {}) {
  const reg = loadRegistry();
  const row = reg.tokens[tokenId];
  if (!row) throw new Error("token_not_found");
  revokeBaiShareCapability(tokenId, "rotated");
  if (row.kind === BAI_PARENT_COMPANY_SHARE || row.parentCompanyId) {
    return issueBaiParentShareCapability({
      parentCompanyId: row.parentCompanyId,
      label: opts.label || row.label,
      surfaces: opts.surfaces || row.surfaces,
      expiresAt: opts.expiresAt || row.expiresAt,
      reportScope: opts.reportScope || row.reportScope,
    });
  }
  return issueBaiShareCapability({
    brandId: row.brandId,
    label: opts.label || row.label,
    surfaces: opts.surfaces || row.surfaces,
    expiresAt: opts.expiresAt || row.expiresAt,
    reportScope: opts.reportScope || row.reportScope,
  });
}

export function rotateBaiParentShareCapability(tokenId, opts = {}) {
  return rotateBaiShareCapability(tokenId, opts);
}

function verifyCommon(token, prefix) {
  const secret = getBaiShareCapabilitySecret();
  if (!secret) {
    return { ok: false, error: "share_secret_not_configured", code: "SHARE_SECRET_MISSING" };
  }
  if (!token || typeof token !== "string") {
    return { ok: false, error: "share_capability_required", code: "SHARE_MISSING" };
  }
  let raw = token.trim();
  if (raw.startsWith(prefix)) {
    raw = raw.slice(prefix.length);
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
  if (payload.v !== BAI_SHARE_TOKEN_VERSION) {
    return { ok: false, error: "unsupported_share_version", code: "SHARE_VERSION" };
  }
  if (!payload.tid) {
    return { ok: false, error: "incomplete_share_payload", code: "SHARE_MALFORMED" };
  }
  if (payload.exp != null && Number(payload.exp) < Math.floor(Date.now() / 1000)) {
    return { ok: false, error: "share_capability_expired", code: "SHARE_EXPIRED" };
  }
  if (payload.reportScope && payload.reportScope !== CURRENT_CUSTOMER_PUBLISHED_SCOPE) {
    return {
      ok: false,
      error: "share_scope_not_allowed",
      code: "SHARE_SCOPE",
      gate: SHARE_CAPABILITY_CANNOT_BYPASS_PUBLICATION_STATE,
    };
  }
  const reg = loadRegistry();
  const row = reg.tokens[payload.tid];
  if (!row) {
    return { ok: false, error: "share_capability_unknown", code: "SHARE_UNKNOWN" };
  }
  if (row.status !== "ACTIVE") {
    return { ok: false, error: "share_capability_revoked", code: "SHARE_REVOKED" };
  }
  return { ok: true, payload, row, secret };
}

export function verifyBaiShareCapability(
  token,
  { expectedBrandId = null, requiredSurface = null } = {}
) {
  const trimmed = String(token || "").trim();
  if (trimmed.startsWith(BAI_PARENT_SHARE_TOKEN_PREFIX)) {
    return verifyBaiParentShareCapability(trimmed, {
      expectedBrandId,
      requiredSurface,
    });
  }

  const common = verifyCommon(trimmed, BAI_SHARE_TOKEN_PREFIX);
  if (!common.ok) return common;
  const { payload, row } = common;
  if (!payload.brandId) {
    return { ok: false, error: "incomplete_share_payload", code: "SHARE_MALFORMED" };
  }
  if (row.brandId && row.brandId !== payload.brandId) {
    return { ok: false, error: "share_registry_mismatch", code: "SHARE_REGISTRY_MISMATCH" };
  }
  if (expectedBrandId && expectedBrandId !== payload.brandId) {
    return {
      ok: false,
      error: "share_brand_scope_mismatch",
      code: "SHARE_BRAND_SCOPE",
      tokenBrandId: payload.brandId,
      requestedBrandId: expectedBrandId,
    };
  }
  if (requiredSurface && !(payload.surfaces || []).includes(requiredSurface)) {
    return { ok: false, error: "share_surface_not_allowed", code: "SHARE_SURFACE" };
  }
  return {
    ok: true,
    claims: { ...payload, kind: BAI_BRAND_SHARE },
    meta: row,
    gate: BAI_BRAND_SHARE,
    allowedBrandIds: [payload.brandId],
  };
}

export function verifyBaiParentShareCapability(
  token,
  { expectedBrandId = null, requiredSurface = null } = {}
) {
  const common = verifyCommon(token, BAI_PARENT_SHARE_TOKEN_PREFIX);
  if (!common.ok) return common;
  const { payload, row } = common;
  if (!payload.parentCompanyId) {
    return { ok: false, error: "incomplete_share_payload", code: "SHARE_MALFORMED" };
  }
  if (row.parentCompanyId && row.parentCompanyId !== payload.parentCompanyId) {
    return { ok: false, error: "share_registry_mismatch", code: "SHARE_REGISTRY_MISMATCH" };
  }
  // Re-derive allowlist server-side — never trust client or stale token brand lists
  const parent = resolveGovernedBaiParentCompany(payload.parentCompanyId);
  if (!parent.ok) {
    return { ok: false, error: parent.error, code: "SHARE_PARENT_UNKNOWN" };
  }
  const allowedBrandIds = parent.allowedBrandIds;
  if (expectedBrandId && !allowedBrandIds.includes(expectedBrandId)) {
    return {
      ok: false,
      error: "share_brand_scope_mismatch",
      code: "SHARE_BRAND_SCOPE",
      parentCompanyId: payload.parentCompanyId,
      allowedBrandIds,
      requestedBrandId: expectedBrandId,
      gate: BAI_PARENT_SHARE_BRAND_SCOPE_AUTHORIZATION,
    };
  }
  if (requiredSurface && !(payload.surfaces || []).includes(requiredSurface)) {
    return { ok: false, error: "share_surface_not_allowed", code: "SHARE_SURFACE" };
  }
  return {
    ok: true,
    claims: {
      ...payload,
      kind: BAI_PARENT_COMPANY_SHARE,
      allowedBrandIds,
      defaultBrandId: parent.defaultBrandId,
      parentCompanyName: parent.canonicalCompanyName,
    },
    meta: row,
    parent,
    allowedBrandIds,
    gate: BAI_PARENT_COMPANY_SHARE,
  };
}

export function extractBaiShareCapabilityFromRequest(req) {
  const q = req?.query || {};
  if (typeof q.share === "string" && q.share) {
    const s = String(q.share);
    if (
      s.startsWith(BAI_SHARE_TOKEN_PREFIX) ||
      s.startsWith(BAI_PARENT_SHARE_TOKEN_PREFIX)
    ) {
      return s;
    }
  }
  const h = req?.headers || {};
  if (typeof h["x-bai-share-capability"] === "string" && h["x-bai-share-capability"]) {
    const s = h["x-bai-share-capability"];
    if (
      s.startsWith(BAI_SHARE_TOKEN_PREFIX) ||
      s.startsWith(BAI_PARENT_SHARE_TOKEN_PREFIX)
    ) {
      return s;
    }
  }
  const auth = h.authorization || h.Authorization;
  if (typeof auth === "string" && auth.toLowerCase().startsWith("bearer ")) {
    const t = auth.slice(7).trim();
    if (
      t.startsWith(BAI_SHARE_TOKEN_PREFIX) ||
      t.startsWith(BAI_PARENT_SHARE_TOKEN_PREFIX)
    ) {
      return t;
    }
  }
  return null;
}

export function resolveBaiShareBrandDisplay(brandId) {
  const display = getShowcaseBrandDisplay(brandId);
  return {
    brandId,
    brandName: display?.brandName || brandId,
    companyKey: display?.companyKey || null,
  };
}

export function buildBaiShareEntitlement(brandIdOrIds) {
  const ids = Array.isArray(brandIdOrIds)
    ? brandIdOrIds.map(String)
    : [String(brandIdOrIds)];
  const membership = resolvePeerSetMembership({
    peerSetId: PEER_SET_ID_V2,
    commercialRegion: "CALA",
  });
  const peerNames = peerSetBrandNamesById(PEER_SET_ID_V2);
  const brandNamesById = { ...peerNames };
  for (const id of ids) {
    const d = resolveBaiShareBrandDisplay(id);
    brandNamesById[id] = d.brandName;
  }
  return {
    entitlementGraph: buildFixtureEntitlementGraph({
      entitledBrandIds: ids,
      peerBrandIds: membership.entityIds || [],
      source: "bai_share_capability",
    }),
    brandNamesById,
    brandDisplay: ids.length === 1 ? resolveBaiShareBrandDisplay(ids[0]) : null,
  };
}

export function buildBaiShareViewer(brandIdOrParent) {
  const isParent =
    brandIdOrParent &&
    typeof brandIdOrParent === "object" &&
    brandIdOrParent.parentCompanyId;
  if (isParent) {
    return {
      email: "share-capability@dealality.invalid",
      memberstackId: null,
      role: "share_capability",
      isAdmin: false,
      isDemo: false,
      isOwner: false,
      isBrand: true,
      isOperator: false,
      isShareCapability: true,
      canAccessBrandWorkspace: true,
      workspaceAccess: ["Brand"],
      activeWorkspace: "Brand",
      shareBoundParentCompanyId: brandIdOrParent.parentCompanyId,
      shareBoundParentCompanyName: brandIdOrParent.parentCompanyName || null,
      shareAllowedBrandIds: brandIdOrParent.allowedBrandIds || [],
    };
  }
  const display = resolveBaiShareBrandDisplay(brandIdOrParent);
  return {
    email: "share-capability@dealality.invalid",
    memberstackId: null,
    role: "share_capability",
    isAdmin: false,
    isDemo: false,
    isOwner: false,
    isBrand: true,
    isOperator: false,
    isShareCapability: true,
    canAccessBrandWorkspace: true,
    workspaceAccess: ["Brand"],
    activeWorkspace: "Brand",
    shareBoundBrandId: brandIdOrParent,
    shareBoundBrandName: display.brandName,
  };
}

export function listPublishedParentCompanyIds() {
  return listShowcaseCompanyKeys();
}
