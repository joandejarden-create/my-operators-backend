/**
 * Admin external client links for ADP + GDI reports.
 * Reconstructs stable share URLs from existing registries.
 * NEVER rotates Bethesda contract token gdisht_47c25d74c79216021fb36150.
 */

import { createHmac, createHash, randomBytes } from "crypto";
import { existsSync, readFileSync, readdirSync } from "fs";
import { join } from "path";
import {
  readShareRegistry,
  ADP_SHARE_TOKEN_PREFIX,
  ADP_SHARE_TOKEN_VERSION,
  getShareCapabilitySecret,
  issueShareCapability,
  DEFAULT_SHARE_SURFACES,
  isAdpTokenIdDurablyRevoked,
  verifyShareCapability,
} from "../ai-demand-positioning/share/adp-signed-share-capability-v1.js";
import {
  readGdiShareRegistry,
  GDI_SHARE_TOKEN_PREFIX,
  GDI_SHARE_TOKEN_VERSION,
  getGdiShareCapabilitySecret,
  issueGdiShareCapability,
  DEFAULT_GDI_SHARE_SURFACES,
  isTokenIdDurablyRevoked,
  verifyGdiShareCapability,
  isProductionLikeRuntime,
} from "../group-demand-intelligence/share/gdi-signed-share-capability-v1.js";
import {
  loadCensusLinkRegistry,
  getCensusRecordIdForAdpProperty,
} from "../ai-demand-positioning/census-link-registry.js";

export const BETHESDA_GDI_CONTRACT_TOKEN_ID = "gdisht_47c25d74c79216021fb36150";
export const BETHESDA_ADP_CONTRACT_TOKEN_ID = "sht_24ff4ada4a622db62a228d3f";
export const BETHESDA_ADP_PROPERTY_ID = "adp_bethesda_marriott";
export const BETHESDA_HPC = "recLuxvwwxID7U2B8";

const ROOT = process.cwd();
const CONTRACT_PATH = join(
  ROOT,
  "config/client-share/production-share-contract-tokens.json"
);
const SEALED_LOCAL_PATH = join(
  ROOT,
  "config/client-share/production-share-sealed.local.json"
);

export const CLIENT_LINK_UNAVAILABLE =
  "Client link unavailable in this environment.";
const PRODUCTION_SHARE_TOKEN_ENV_BY_TOKEN_ID = {
  "gdisht_47c25d74c79216021fb36150": "GDI_BETHESDA_PRODUCTION_SHARE_TOKEN",
  "sht_24ff4ada4a622db62a228d3f": "ADP_BETHESDA_PRODUCTION_SHARE_TOKEN",
  "sht_2fa10cb1596f38204b39fd61": "ADP_HILTON_TIMES_SQUARE_PRODUCTION_SHARE_TOKEN",
  "sht_55e39d4e0724786bc1baf097": "ADP_AC_CORUNA_PRODUCTION_SHARE_TOKEN",
  "sht_9f9b35057ffc4bd879f9eee1": "ADP_RADISSON_SANTO_DOMINGO_PRODUCTION_SHARE_TOKEN",
};

function hydrateProductionShareToken(row) {
  if (!row) return null;

  const envName = PRODUCTION_SHARE_TOKEN_ENV_BY_TOKEN_ID[row.tokenId];
  const token = envName ? String(process.env[envName] || "").trim() : "";

  return token ? { ...row, token } : row;
}

/**
 * Local Admin Open/Copy should hydrate sealed production client URLs without
 * minting tokens. Opt-in explicitly, or auto-enable outside production-like runtimes.
 */
export function preferProductionShareCatalog() {
  const flag = String(process.env.USE_PRODUCTION_SHARE_CATALOG || "")
    .trim()
    .toLowerCase();
  if (flag === "1" || flag === "true" || flag === "yes") return true;
  if (flag === "0" || flag === "false" || flag === "no") return false;
  // Default: local/dev Admin uses sealed production contract URLs when present.
  return !isProductionLikeRuntime();
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

const PRODUCTION_PUBLIC_BASE =
  "https://my-operators-backend-production.up.railway.app";

export function resolvePublicBaseUrl(req = null) {
  const envBase =
    process.env.DEALALITY_PUBLIC_BASE_URL ||
    process.env.PUBLIC_BASE_URL ||
    process.env.ADP_PUBLIC_BASE_URL ||
    process.env.GDI_PUBLIC_BASE_URL ||
    process.env.RAILWAY_PUBLIC_DOMAIN ||
    "";
  let base = String(envBase || "").trim().replace(/\/$/, "");
  if (base && !/^https?:\/\//i.test(base)) base = `https://${base}`;

  const isProdLike = isProductionLikeRuntime() || /railway\.app/i.test(base);

  if (base) {
    // Never emit localhost client URLs from production-like runtimes.
    if (isProdLike && /localhost|127\.0\.0\.1/i.test(base)) {
      return PRODUCTION_PUBLIC_BASE;
    }
    return base;
  }

  if (req) {
    const host = String(req.get?.("x-forwarded-host") || req.get?.("host") || "").trim();
    const proto = String(req.get?.("x-forwarded-proto") || req.protocol || "https").split(",")[0].trim();
    if (host) {
      if (isProdLike && /localhost|127\.0\.0\.1/i.test(host)) {
        return PRODUCTION_PUBLIC_BASE;
      }
      // Prefer canonical production host when request is against Railway production.
      if (/my-operators-backend-production\.up\.railway\.app/i.test(host)) {
        return PRODUCTION_PUBLIC_BASE;
      }
      return `${proto}://${host}`.replace(/\/$/, "");
    }
  }

  if (isProdLike) return PRODUCTION_PUBLIC_BASE;
  return "http://localhost:3000";
}

/**
 * Admin must not hand out a URL that this runtime cannot verify.
 * Bethesda contract token is preserved; we only gate availability.
 */
function assertShareTokenVerifiable(token) {
  if (!token) {
    return { ok: false, code: "SHARE_MISSING", reason: "Share token missing." };
  }
  const secret = getGdiShareCapabilitySecret();
  if (!secret) {
    return {
      ok: false,
      code: "SHARE_SECRET_MISSING",
      reason:
        "GDI share secret not configured in this environment â€” Open/Copy would show 'Access link unavailable' to clients.",
    };
  }
  const verified = verifyGdiShareCapability(token);
  if (!verified?.ok) {
    return {
      ok: false,
      code: verified?.code || "SHARE_BAD_SIGNATURE",
      reason:
        verified?.customerMessage ||
        verified?.reason ||
        "Share token failed verification in this environment.",
    };
  }
  return { ok: true, hotelId: verified.claims?.hotelId || null };
}

function readGdiHotelConfigAdpAlias(hpcHotelId) {
  const id = String(hpcHotelId || "").trim();
  if (!id || !id.startsWith("rec")) return null;
  const cfgPath = join(
    ROOT,
    "config/group-demand-intelligence/hotels",
    `${id}.json`
  );
  if (!existsSync(cfgPath)) return null;
  try {
    const cfg = JSON.parse(readFileSync(cfgPath, "utf8"));
    const alias = String(cfg?.aliases?.adpPropertyId || cfg?.adpPropertyId || "").trim();
    return alias.startsWith("adp_") ? alias : null;
  } catch {
    return null;
  }
}

function readGdiHotelConfigHpcForAdp(adpPropertyId) {
  const want = String(adpPropertyId || "").trim();
  if (!want.startsWith("adp_")) return null;
  const dir = join(ROOT, "config/group-demand-intelligence/hotels");
  if (!existsSync(dir)) return null;
  try {
    for (const name of readdirSync(dir)) {
      if (!name.endsWith(".json")) continue;
      const cfg = JSON.parse(readFileSync(join(dir, name), "utf8"));
      const alias = String(
        cfg?.aliases?.adpPropertyId || cfg?.adpPropertyId || ""
      ).trim();
      if (alias === want) {
        const hotelId = String(
          cfg?.hotelId || name.replace(/\.json$/, "")
        ).trim();
        if (hotelId.startsWith("rec")) return hotelId;
      }
    }
  } catch {
    return null;
  }
  return null;
}

export function getAdpPropertyIdForHpc(hpcHotelId) {
  const id = String(hpcHotelId || "").trim();
  if (!id) return null;
  if (id.startsWith("adp_")) return id;
  const reg = loadCensusLinkRegistry();
  for (const [propertyId, entry] of Object.entries(reg.links || {})) {
    if (String(entry?.censusRecordId || "").trim() === id) return propertyId;
  }
  return readGdiHotelConfigAdpAlias(id);
}

export function getHpcForAdpProperty(propertyId) {
  return (
    getCensusRecordIdForAdpProperty(propertyId) ||
    readGdiHotelConfigHpcForAdp(propertyId)
  );
}

function reconstructAdpToken(row, secret) {
  const iat = Math.floor(new Date(row.issuedAt).getTime() / 1000);
  const payload = {
    v: ADP_SHARE_TOKEN_VERSION,
    tid: row.tokenId,
    propertyId: row.propertyId,
    surfaces: [...(row.surfaces || DEFAULT_SHARE_SURFACES)],
    reportScope: row.reportScope || "current_published",
    iat,
    exp: row.expiresAt ? Math.floor(new Date(row.expiresAt).getTime() / 1000) : null,
  };
  const body = b64urlJson(payload);
  const sig = b64url(createHmac("sha256", secret).update(body).digest());
  return `${ADP_SHARE_TOKEN_PREFIX}${body}.${sig}`;
}

function reconstructGdiToken(row, secret) {
  const iat = Math.floor(new Date(row.issuedAt).getTime() / 1000);
  const payload = {
    v: GDI_SHARE_TOKEN_VERSION,
    tid: row.tokenId,
    hotelId: row.hotelId,
    surfaces: [...(row.surfaces || DEFAULT_GDI_SHARE_SURFACES)],
    capabilities: Array.isArray(row.capabilities) ? [...row.capabilities] : undefined,
    mode: row.mode || "read_only",
    iat,
    exp: row.expiresAt ? Math.floor(new Date(row.expiresAt).getTime() / 1000) : null,
  };
  if (!payload.capabilities) delete payload.capabilities;
  const body = b64urlJson(payload);
  const sig = b64url(createHmac("sha256", secret).update(body).digest());
  return `${GDI_SHARE_TOKEN_PREFIX}${body}.${sig}`;
}

function loadContractDoc() {
  if (!existsSync(CONTRACT_PATH)) return null;
  try {
    return JSON.parse(readFileSync(CONTRACT_PATH, "utf8"));
  } catch {
    return null;
  }
}

function loadBethesdaContractToken() {
  const doc = loadContractDoc();
  if (!doc) return null;
  const row = (doc.tokens || []).find(
    (t) =>
      t.product === "GDI" &&
      t.tokenId === BETHESDA_GDI_CONTRACT_TOKEN_ID &&
      t.hotelId === BETHESDA_HPC
  );
  return hydrateProductionShareToken(row)?.token || null;
}

function loadAdpContractToken(propertyId) {
  const want = String(propertyId || "").trim();
  if (!want) return null;

  const doc = loadContractDoc();
  if (!doc) return null;

  const row = (doc.tokens || []).find(
    (t) =>
      t.product === "ADP" &&
      String(t.propertyId || "").trim() === want
  );

  const hydrated = hydrateProductionShareToken(row);

  if (
    !hydrated?.token ||
    !hydrated.token.startsWith(ADP_SHARE_TOKEN_PREFIX)
  ) {
    return hydrated || null;
  }

  return hydrated;
}

function fingerprintAdpContractToken(propertyId = BETHESDA_ADP_PROPERTY_ID) {
  const row = loadAdpContractToken(propertyId);
  if (!row?.token) {
    return {
      present: false,
      propertyId,
      tokenId: row?.tokenId || BETHESDA_ADP_CONTRACT_TOKEN_ID,
      sha12: null,
    };
  }
  return {
    present: true,
    propertyId,
    tokenId: row.tokenId || null,
    sha12: createHash("sha256").update(row.token).digest("hex").slice(0, 12),
    length: row.token.length,
  };
}

function sealedLocalCatalogEnabled() {
  const flag = String(process.env.DEALALITY_SHARE_SEALED_LOCAL || "")
    .trim()
    .toLowerCase();
  return flag !== "0" && flag !== "false" && flag !== "no";
}

function loadSealedLocalDoc() {
  if (!sealedLocalCatalogEnabled() || !existsSync(SEALED_LOCAL_PATH)) return null;
  try {
    return JSON.parse(readFileSync(SEALED_LOCAL_PATH, "utf8"));
  } catch {
    return null;
  }
}

function decodeSharePayload(token, prefix) {
  const raw = String(token || "").trim();
  if (!raw.startsWith(prefix)) return null;
  const body = raw.slice(prefix.length).split(".")[0];
  if (!body) return null;
  try {
    const pad = body.length % 4 === 0 ? "" : "=".repeat(4 - (body.length % 4));
    const json = Buffer.from(
      body.replace(/-/g, "+").replace(/_/g, "/") + pad,
      "base64"
    ).toString("utf8");
    const payload = JSON.parse(json);
    return payload && typeof payload === "object" ? payload : null;
  } catch {
    return null;
  }
}

function expiryEpochSeconds(value) {
  if (value == null || value === "") return null;
  if (typeof value === "number" && Number.isFinite(value)) {
    return value > 1e12 ? Math.floor(value / 1000) : Math.floor(value);
  }
  const parsed = Date.parse(String(value));
  return Number.isFinite(parsed) ? Math.floor(parsed / 1000) : null;
}

function registryTokens(product, injectedRegistry) {
  if (injectedRegistry && typeof injectedRegistry === "object") {
    return injectedRegistry.tokens || {};
  }
  if (product === "ADP") return readShareRegistry().tokens || {};
  return readGdiShareRegistry().tokens || {};
}

/**
 * Claim, registry, expiry, revocation, and scope checks for a sealed bearer.
 * A token id alone is never enough. Signature success is reported separately.
 */
export function assessSealedShareToken(token, opts = {}) {
  const product = opts.product === "GDI" ? "GDI" : "ADP";
  const prefix = product === "GDI" ? GDI_SHARE_TOKEN_PREFIX : ADP_SHARE_TOKEN_PREFIX;
  const nowSec = Math.floor((opts.now instanceof Date ? opts.now.getTime() : Number(opts.now) || Date.now()) / 1000);
  const payload = decodeSharePayload(token, prefix);
  if (!payload) {
    return {
      ok: false,
      code: "SHARE_MALFORMED",
      reason: `${CLIENT_LINK_UNAVAILABLE} Sealed ${product} token could not be read.`,
    };
  }

  const tokenId = String(payload.tid || "").trim();
  if (opts.tokenId && tokenId !== String(opts.tokenId).trim()) {
    return {
      ok: false,
      code: "SHARE_TOKEN_ID",
      reason: `${CLIENT_LINK_UNAVAILABLE} Sealed ${product} token id does not match the catalog row.`,
    };
  }

  if (product === "ADP") {
    if (!opts.propertyId || payload.propertyId !== opts.propertyId) {
      return {
        ok: false,
        code: "SHARE_PROPERTY_SCOPE",
        reason: `${CLIENT_LINK_UNAVAILABLE} Sealed ADP token is for a different property.`,
      };
    }
    const scope = payload.reportScope || null;
    if (scope !== "current_published") {
      return {
        ok: false,
        code: "SHARE_SCOPE",
        reason: `${CLIENT_LINK_UNAVAILABLE} Sealed ADP token is not the current published client report.`,
      };
    }
  } else if (!opts.hotelId || payload.hotelId !== opts.hotelId) {
    return {
      ok: false,
      code: "SHARE_PROPERTY_SCOPE",
      reason: `${CLIENT_LINK_UNAVAILABLE} Sealed GDI token is for a different hotel.`,
    };
  } else {
    const surfaces = Array.isArray(payload.surfaces) ? payload.surfaces : [];
    const mode = payload.mode || "read_only";
    if (mode !== "read_only" || !surfaces.includes("opportunities")) {
      return {
        ok: false,
        code: "SHARE_SCOPE",
        reason: `${CLIENT_LINK_UNAVAILABLE} Sealed GDI token is not a read-only client opportunities link.`,
      };
    }
  }

  const payloadExp = expiryEpochSeconds(payload.exp);
  if (payloadExp != null && payloadExp < nowSec) {
    return {
      ok: false,
      code: "SHARE_EXPIRED",
      reason: `${CLIENT_LINK_UNAVAILABLE} Sealed ${product} token is expired.`,
    };
  }

  const revoked =
    typeof opts.revoked === "boolean"
      ? opts.revoked
      : product === "ADP"
        ? isAdpTokenIdDurablyRevoked(tokenId)
        : isTokenIdDurablyRevoked(tokenId);
  if (revoked) {
    return {
      ok: false,
      code: "SHARE_REVOKED",
      reason: `${CLIENT_LINK_UNAVAILABLE} Sealed ${product} token is revoked.`,
    };
  }

  const row = registryTokens(product, opts.registry)[tokenId] || null;
  if (!row || row.status !== "ACTIVE" || row.revokedAt) {
    return {
      ok: false,
      code: "SHARE_REVOKED",
      reason: row
        ? `${CLIENT_LINK_UNAVAILABLE} Sealed ${product} token is not an active client share.`
        : `${CLIENT_LINK_UNAVAILABLE} Sealed ${product} token has no active registry row.`,
    };
  }
  if (product === "ADP" && row.propertyId !== payload.propertyId) {
    return {
      ok: false,
      code: "SHARE_PROPERTY_SCOPE",
      reason: `${CLIENT_LINK_UNAVAILABLE} Sealed ADP token does not match the registry property.`,
    };
  }
  if (product === "GDI" && row.hotelId !== payload.hotelId) {
    return {
      ok: false,
      code: "SHARE_PROPERTY_SCOPE",
      reason: `${CLIENT_LINK_UNAVAILABLE} Sealed GDI token does not match the registry hotel.`,
    };
  }
  const rowExp = expiryEpochSeconds(row.expiresAt);
  if (rowExp != null && rowExp < nowSec) {
    return {
      ok: false,
      code: "SHARE_EXPIRED",
      reason: `${CLIENT_LINK_UNAVAILABLE} Sealed ${product} registry row is expired.`,
    };
  }
  if (product === "ADP" && row.reportScope && row.reportScope !== "current_published") {
    return {
      ok: false,
      code: "SHARE_SCOPE",
      reason: `${CLIENT_LINK_UNAVAILABLE} Sealed ADP registry row is not the current published scope.`,
    };
  }

  return { ok: true, tokenId, payload, code: null, reason: null };
}

function contractRows(product) {
  const docs = [loadContractDoc(), loadSealedLocalDoc()].filter(Boolean);
  const rows = [];
  for (const doc of docs) {
    for (const row of doc.tokens || []) {
      if (row?.product === product) rows.push(row);
    }
  }
  return rows;
}

function acceptSealedAdp(propertyId) {
  const want = String(propertyId || "").trim();
  const row = contractRows("ADP").find((t) => String(t.propertyId || "").trim() === want);
  const hydrated = hydrateProductionShareToken(row);
  if (!hydrated?.token || !String(hydrated.token).startsWith(ADP_SHARE_TOKEN_PREFIX)) {
    return { use: false, token: null, tokenId: hydrated?.tokenId || null, signatureVerified: false };
  }
  const assessed = assessSealedShareToken(hydrated.token, {
    product: "ADP",
    propertyId: want,
    tokenId: hydrated.tokenId || null,
  });
  if (!assessed.ok) {
    return {
      use: false,
      token: null,
      tokenId: hydrated.tokenId || null,
      signatureVerified: false,
      reason: assessed.reason,
      code: assessed.code,
    };
  }
  let signatureVerified = false;
  const secret = getShareCapabilitySecret();
  if (secret) {
    const verified = verifyShareCapability(hydrated.token, { expectedPropertyId: want });
    signatureVerified = verified?.ok === true;
  }
  return {
    use: true,
    token: hydrated.token,
    tokenId: assessed.tokenId,
    signatureVerified,
    reason: null,
  };
}

function acceptSealedGdi(hotelId) {
  const want = String(hotelId || "").trim();
  const matches = contractRows("GDI").filter((t) => String(t.hotelId || "").trim() === want);
  let rejection = null;
  for (const row of matches) {
    const hydrated = hydrateProductionShareToken(row);
    if (!hydrated?.token || !String(hydrated.token).startsWith(GDI_SHARE_TOKEN_PREFIX)) {
      rejection = {
        code: "SEALED_URL_ABSENT",
        reason: `${CLIENT_LINK_UNAVAILABLE} The share catalog names this hotel, but it does not contain a sealed production URL.`,
      };
      continue;
    }
    const assessed = assessSealedShareToken(hydrated.token, {
      product: "GDI",
      hotelId: want,
      tokenId: hydrated.tokenId || null,
    });
    if (!assessed.ok) {
      rejection = assessed;
      continue;
    }
    let signatureVerified = false;
    if (getGdiShareCapabilitySecret()) {
      const verified = verifyGdiShareCapability(hydrated.token, {
        expectedHotelId: want,
        allowSelfHeal: false,
      });
      signatureVerified = verified?.ok === true;
    }
    return {
      use: true,
      token: hydrated.token,
      tokenId: assessed.tokenId,
      signatureVerified,
      reason: null,
    };
  }
  return {
    use: false,
    token: null,
    tokenId: rejection?.tokenId || null,
    signatureVerified: false,
    reason: rejection?.reason || null,
    code: rejection?.code || null,
  };
}

export function probeGdiClientShareState(hotelId) {
  const id = String(hotelId || "").trim();
  if (!id) {
    return {
      available: false,
      tokenId: null,
      signatureVerified: false,
      reason: `${CLIENT_LINK_UNAVAILABLE} hotelId is required.`,
    };
  }
  try {
    const sealed = acceptSealedGdi(id);
    if (sealed.use && sealed.token) {
      return {
        available: true,
        tokenId: sealed.tokenId,
        signatureVerified: sealed.signatureVerified === true,
        reason: null,
      };
    }
    const secret = getGdiShareCapabilitySecret();
    const preferred = preferGdiRow(id, Object.values(readGdiShareRegistry().tokens || {}));
    if (secret && preferred && !sealed.reason) {
      return {
        available: true,
        tokenId: preferred.tokenId || null,
        signatureVerified: false,
        reason: null,
      };
    }
    return {
      available: false,
      tokenId: preferred?.tokenId || sealed.tokenId || null,
      signatureVerified: false,
      reason: gdiUnavailableReason(id, sealed.reason),
    };
  } catch (err) {
    return {
      available: false,
      tokenId: null,
      signatureVerified: false,
      reason: `${CLIENT_LINK_UNAVAILABLE} ${String(err?.message || err).slice(0, 160)}`,
    };
  }
}

function gdiUnavailableReason(hotelId, sealedRejection) {
  if (sealedRejection) return sealedRejection;
  const preferred = preferGdiRow(hotelId, Object.values(readGdiShareRegistry().tokens || {}));
  if (preferred?.tokenId) {
    return `${CLIENT_LINK_UNAVAILABLE} Active share ${preferred.tokenId} is issued, but the sealed production URL is not in the share contract catalog and this environment has no signing secret, so the link cannot be reconstructed.`;
  }
  return `${CLIENT_LINK_UNAVAILABLE} No active GDI client share is issued for this hotel.`;
}

function newestByIssuedAt(rows) {
  return [...rows].sort((a, b) =>
    String(b.issuedAt || "").localeCompare(String(a.issuedAt || ""))
  )[0] || null;
}

function preferAdpRow(rows) {
  const active = rows.filter((t) => t.status === "ACTIVE" && !t.revokedAt);
  // Bethesda pilot: keep the known sealed production client URL (founder-review / publication-14).
  const bethesdaPinned = active.find(
    (t) => t.tokenId === "sht_24ff4ada4a622db62a228d3f"
  );
  if (bethesdaPinned) return bethesdaPinned;
  const prod = active.filter((t) => String(t.label || "").startsWith("production-distribution:"));
  const founder = active.filter((t) => String(t.label || "").startsWith("founder-review:"));
  const clientReady = active.filter((t) => String(t.label || "").startsWith("client-ready:"));
  return (
    newestByIssuedAt(prod) ||
    newestByIssuedAt(founder) ||
    newestByIssuedAt(clientReady) ||
    newestByIssuedAt(active) ||
    null
  );
}

/**
 * Cheap availability probe for AI Demand Reviews table hydration.
 * Does NOT mint/rotate tokens and does NOT return share URLs.
 * @param {string} hotelKey â€” adp_* property id or HPC recâ€¦
 * @returns {{ available: boolean, propertyId: string|null, tokenId: string|null, reason: string|null }}
 */
export function probeAdpClientShareAvailable(hotelKey) {
  const key = String(hotelKey || "").trim();
  const adpPropertyId = key.startsWith("adp_") ? key : getAdpPropertyIdForHpc(key);
  if (!adpPropertyId) {
    return {
      available: false,
      propertyId: null,
      tokenId: null,
      reason: "No ADP property mapping for this hotel.",
    };
  }
  try {
    const sealedAdp = acceptSealedAdp(adpPropertyId);
    // A sealed contract bearer is the client URL only after identity, expiry,
    // revocation, and scope checks. Do not re-sign an older registry row over it.
    if (sealedAdp.use && sealedAdp.token) {
      return {
        available: true,
        propertyId: adpPropertyId,
        tokenId: sealedAdp.tokenId || null,
        signatureVerified: sealedAdp.signatureVerified === true,
        reason: null,
      };
    }
    if (sealedAdp.reason) {
      return {
        available: false,
        propertyId: adpPropertyId,
        tokenId: sealedAdp.tokenId || null,
        signatureVerified: false,
        reason: sealedAdp.reason,
      };
    }
    const secret = getShareCapabilitySecret();
    const reg = readShareRegistry();
    const rows = Object.values(reg.tokens || {}).filter(
      (t) => t.propertyId === adpPropertyId
    );
    const preferred = preferAdpRow(rows);
    if (preferred && secret) {
      return {
        available: true,
        propertyId: adpPropertyId,
        tokenId: preferred.tokenId || null,
        reason: null,
      };
    }
    if (!secret) {
      return {
        available: false,
        propertyId: adpPropertyId,
        tokenId: preferred?.tokenId || null,
        reason: "ADP share secret not configured in this environment.",
      };
    }
    return {
      available: false,
      propertyId: adpPropertyId,
      tokenId: null,
      reason: "No active ADP client link is available for this hotel.",
    };
  } catch (err) {
    return {
      available: false,
      propertyId: adpPropertyId,
      tokenId: null,
      reason: String(err?.message || err).slice(0, 160),
    };
  }
}

function gdiShareRowScore(row) {
  const label = String(row?.label || "");
  let score = 0;
  if (/production-distribution/i.test(label)) score += 100;
  if (/client share/i.test(label)) score += 50;
  if (/pilot|production|client/i.test(label)) score += 10;
  return score;
}

function rankGdiShareRows(rows) {
  return rows.slice().sort((a, b) => {
    const score = gdiShareRowScore(b) - gdiShareRowScore(a);
    if (score) return score;
    return String(b.issuedAt || "").localeCompare(String(a.issuedAt || ""));
  });
}

function preferGdiRow(hotelId, rows) {
  const active = rows.filter(
    (t) =>
      t.hotelId === hotelId &&
      t.status === "ACTIVE" &&
      !t.revokedAt &&
      !isTokenIdDurablyRevoked(t.tokenId)
  );
  if (hotelId === BETHESDA_HPC) {
    const contract = active.find((t) => t.tokenId === BETHESDA_GDI_CONTRACT_TOKEN_ID);
    if (contract) return contract;
  }
  return rankGdiShareRows(active)[0] || null;
}

/**
 * Resolve ADP + GDI external client links for a hotel.
 * @param {string} hotelKey â€” HPC recâ€¦ or adp_* property id
 * @param {{ req?: object, createIfMissing?: boolean }} [opts]
 */
export function resolveExternalClientLinks(hotelKey, opts = {}) {
  const key = String(hotelKey || "").trim();
  const base = resolvePublicBaseUrl(opts.req || null);
  const createIfMissing = opts.createIfMissing === true;

  let hpcId = key.startsWith("rec") ? key : getHpcForAdpProperty(key);
  let adpPropertyId = key.startsWith("adp_") ? key : getAdpPropertyIdForHpc(key);

  const out = {
    ok: true,
    hotelKey: key,
    hpcId: hpcId || null,
    adpPropertyId: adpPropertyId || null,
    publicBase: base,
    adp: {
      available: false,
      reportType: "ADP",
      reason: null,
      tokenId: null,
      path: null,
      url: null,
      created: false,
    },
    gdi: {
      available: false,
      reportType: "GDI",
      reason: null,
      tokenId: null,
      path: null,
      url: null,
      created: false,
      bethesdaContractPreserved: null,
    },
  };

  // --- ADP ---
  if (adpPropertyId) {
    try {
      const secret = getShareCapabilitySecret();
      const reg = readShareRegistry();
      const rows = Object.values(reg.tokens || {}).filter((t) => t.propertyId === adpPropertyId);
      let preferred = preferAdpRow(rows);
      const sealedAdp = acceptSealedAdp(adpPropertyId);
      // Sealed contract bearer wins over registry reconstruction, including when
      // a signing secret is present. Reject expired, revoked, or wrong-scope tokens.
      if (sealedAdp.use && sealedAdp.token) {
        const path = `/owner-ai-demand-share.html?share=${encodeURIComponent(sealedAdp.token)}`;
        out.adp.available = true;
        out.adp.tokenId = sealedAdp.tokenId || preferred?.tokenId || null;
        out.adp.path = path;
        out.adp.url = `${PRODUCTION_PUBLIC_BASE}${path}`;
        out.adp.created = false;
        out.adp.servedFromProductionHost = true;
        out.adp.signatureVerified = sealedAdp.signatureVerified === true;
        out.adp.reason = null;
      } else if (sealedAdp.reason) {
        out.adp.reason = sealedAdp.reason;
        out.adp.tokenId = sealedAdp.tokenId || null;
        out.adp.signatureVerified = false;
      } else if (!preferred && createIfMissing && secret) {
        const issued = issueShareCapability({
          propertyId: adpPropertyId,
          label: `admin-external-client:${adpPropertyId}`,
          surfaces: DEFAULT_SHARE_SURFACES,
          reportScope: "current_published",
        });
        preferred = issued.meta;
        out.adp.created = true;
        out.adp.tokenId = issued.tokenId;
        out.adp.path = issued.sharePath;
        out.adp.url = `${base}${issued.sharePath}`;
        out.adp.available = true;
      } else if (preferred && secret) {
        const token = reconstructAdpToken(preferred, secret);
        const path = `/owner-ai-demand-share.html?share=${encodeURIComponent(token)}`;
        out.adp.available = true;
        out.adp.tokenId = preferred.tokenId;
        out.adp.path = path;
        out.adp.url = `${base}${path}`;
      } else if (!secret) {
        out.adp.reason = "ADP share secret not configured in this environment.";
      } else {
        out.adp.reason = "No active ADP client link is available for this hotel.";
      }
    } catch (err) {
      out.adp.reason = String(err?.message || err).slice(0, 160);
    }
  } else {
    out.adp.reason = "No ADP property mapping for this hotel.";
  }

  // --- GDI ---
  if (hpcId) {
    try {
      const beforeBethesda = loadBethesdaContractToken();
      const secret = getGdiShareCapabilitySecret();
      const reg = readGdiShareRegistry();
      let preferred = preferGdiRow(hpcId, Object.values(reg.tokens || {}));

      function commitGdiLink(token, tokenId, { created = false, contract = false } = {}) {
        const path = `/group-demand-intelligence-share.html?share=${encodeURIComponent(token)}`;
        const gate = assertShareTokenVerifiable(token);

        // Bethesda contract: if this runtime cannot verify the sealed envelope
        // (missing secret OR local DEV/wrong secret â†’ BAD_SIGNATURE), still return
        // the PRODUCTION client URL â€” never rotate the token.
        const sealedClaimsOk =
          !gate.ok &&
          contract &&
          tokenId === BETHESDA_GDI_CONTRACT_TOKEN_ID &&
          assessSealedShareToken(token, {
            product: "GDI",
            hotelId: BETHESDA_HPC,
            tokenId,
          }).ok;
        if (sealedClaimsOk) {
          out.gdi.available = true;
          out.gdi.tokenId = tokenId;
          out.gdi.path = path;
          out.gdi.url = `${PRODUCTION_PUBLIC_BASE}${path}`;
          out.gdi.created = false;
          out.gdi.servedFromProductionHost = true;
          out.gdi.signatureVerified = false;
          out.gdi.bethesdaContractPreserved = true;
          out.gdi.verifyCode = gate.code;
          out.gdi.reason = null;
          return true;
        }

        if (!gate.ok) {
          out.gdi.available = false;
          out.gdi.signatureVerified = false;
          out.gdi.reason = String(gate.reason || "").startsWith(CLIENT_LINK_UNAVAILABLE)
            ? gate.reason
            : `${CLIENT_LINK_UNAVAILABLE} ${gate.reason || "Share token failed verification in this environment."}`;
          out.gdi.tokenId = tokenId || null;
          out.gdi.path = null;
          out.gdi.url = null;
          out.gdi.verifyCode = gate.code;
          return false;
        }

        // Prefer production host for Bethesda contract even when local verify works,
        // so Admin Open/Copy always yields the canonical client destination.
        const linkBase =
          contract && tokenId === BETHESDA_GDI_CONTRACT_TOKEN_ID
            ? PRODUCTION_PUBLIC_BASE
            : base;

        out.gdi.available = true;
        out.gdi.signatureVerified = true;
        out.gdi.tokenId = tokenId;
        out.gdi.path = path;
        out.gdi.url = `${linkBase}${path}`;
        out.gdi.created = created;
        if (hpcId === BETHESDA_HPC) {
          out.gdi.bethesdaContractPreserved =
            contract || tokenId === BETHESDA_GDI_CONTRACT_TOKEN_ID;
        }
        return true;
      }

      const sealedGdi = acceptSealedGdi(hpcId);
      if (sealedGdi.use && sealedGdi.token) {
        const path = `/group-demand-intelligence-share.html?share=${encodeURIComponent(sealedGdi.token)}`;
        out.gdi.available = true;
        out.gdi.tokenId = sealedGdi.tokenId;
        out.gdi.path = path;
        out.gdi.url = `${PRODUCTION_PUBLIC_BASE}${path}`;
        out.gdi.created = false;
        out.gdi.servedFromProductionHost = true;
        out.gdi.signatureVerified = sealedGdi.signatureVerified === true;
        out.gdi.reason = null;
        if (
          hpcId === BETHESDA_HPC &&
          sealedGdi.tokenId === BETHESDA_GDI_CONTRACT_TOKEN_ID
        ) {
          out.gdi.bethesdaContractPreserved = true;
        }
      } else if (secret) {
        const ranked = rankGdiShareRows(
          Object.values(reg.tokens || {}).filter(
            (t) =>
              t.hotelId === hpcId &&
              t.status === "ACTIVE" &&
              !t.revokedAt &&
              !isTokenIdDurablyRevoked(t.tokenId)
          )
        );
        let signed = false;
        for (const row of ranked) {
          const token = reconstructGdiToken(row, secret);
          signed = commitGdiLink(token, row.tokenId, {
            contract: row.tokenId === BETHESDA_GDI_CONTRACT_TOKEN_ID,
          });
          if (signed) break;
        }
        if (!signed && createIfMissing) {
          if (hpcId === BETHESDA_HPC && beforeBethesda) {
            commitGdiLink(beforeBethesda, BETHESDA_GDI_CONTRACT_TOKEN_ID, {
              contract: true,
            });
          } else if (!ranked.length) {
            const issued = issueGdiShareCapability({
              hotelId: hpcId,
              label: `admin-external-client:${hpcId}`,
              tokenId:
                hpcId === BETHESDA_HPC
                  ? BETHESDA_GDI_CONTRACT_TOKEN_ID
                  : `gdisht_${randomBytes(12).toString("hex")}`,
            });
            commitGdiLink(issued.token, issued.tokenId, {
              created: true,
              contract: issued.tokenId === BETHESDA_GDI_CONTRACT_TOKEN_ID,
            });
          }
        }
        if (!out.gdi.available && !out.gdi.reason) {
          out.gdi.reason = ranked.length
            ? `${CLIENT_LINK_UNAVAILABLE} Active GDI share records could not be verified in this environment.`
            : gdiUnavailableReason(hpcId, sealedGdi.reason);
          out.gdi.signatureVerified = false;
        }
      } else {
        out.gdi.available = false;
        out.gdi.signatureVerified = false;
        out.gdi.url = null;
        out.gdi.path = null;
        out.gdi.tokenId = preferred?.tokenId || sealedGdi.tokenId || null;
        out.gdi.reason = gdiUnavailableReason(hpcId, sealedGdi.reason);
      }

      // Hard proof: Bethesda contract token string unchanged on disk
      if (hpcId === BETHESDA_HPC) {
        const after = loadBethesdaContractToken();
        const preserved =
          Boolean(beforeBethesda) && beforeBethesda === after;
        out.gdi.bethesdaContractPreserved =
          out.gdi.bethesdaContractPreserved == null
            ? preserved
            : out.gdi.bethesdaContractPreserved && preserved;
        if (beforeBethesda && after && beforeBethesda !== after) {
          throw new Error("BETHESDA_GDI_SHARE_TOKEN_MUTATED");
        }
      }
    } catch (err) {
      out.gdi.reason = String(err?.message || err).slice(0, 160);
      if (String(err?.message || "").includes("BETHESDA_GDI_SHARE_TOKEN_MUTATED")) {
        out.ok = false;
      }
    }
  } else {
    out.gdi.reason = "No GDI hotel id for this selection.";
  }

  return out;
}

export function fingerprintBethesdaGdiContractToken() {
  const token = loadBethesdaContractToken();
  if (!token) return { present: false, tokenId: BETHESDA_GDI_CONTRACT_TOKEN_ID, sha12: null };
  return {
    present: true,
    tokenId: BETHESDA_GDI_CONTRACT_TOKEN_ID,
    sha12: createHash("sha256").update(token).digest("hex").slice(0, 12),
    length: token.length,
  };
}

/**
 * Startup / local-dev diagnostic â€” names + presence only (never print secrets).
 */
export function logAdminShareHydrationStatus() {
  const adpSecret = Boolean(getShareCapabilitySecret());
  const gdiSecret = Boolean(getGdiShareCapabilitySecret());
  const gdiContract = fingerprintBethesdaGdiContractToken();
  const adpContract = fingerprintAdpContractToken(BETHESDA_ADP_PROPERTY_ID);
  const regAdp = existsSync(
    join(ROOT, "config/client-share/adp-share-registry/active-tokens.json")
  );
  const regGdi = existsSync(
    join(ROOT, "config/client-share/gdi-share-registry/active-tokens.json")
  );
  const mode = preferProductionShareCatalog()
    ? "production_share_catalog"
    : "local_reconstruct";

  console.log(
    `[share-hydration] ADP_SHARE_CAPABILITY_SECRET: ${adpSecret ? "configured" : "missing"}`
  );
  console.log(
    `[share-hydration] GDI_SHARE_CAPABILITY_SECRET: ${gdiSecret ? "configured" : "missing"}`
  );
  console.log(
    `[share-hydration] SHARE_REGISTRY: ${
      regAdp && regGdi ? "configured" : "partial_or_missing"
    } (adp=${regAdp ? "yes" : "no"} gdi=${regGdi ? "yes" : "no"})`
  );
  console.log(
    `[share-hydration] BETHESDA_GDI_CONTRACT: ${gdiContract.present ? "present" : "MISSING"}`
  );
  console.log(
    `[share-hydration] BETHESDA_ADP_CONTRACT: ${adpContract.present ? "present" : "MISSING"}`
  );
  console.log(`[share-hydration] ADMIN_LINK_MODE: ${mode}`);

  if (!adpSecret && !adpContract.present) {
    console.warn(
      "[share-hydration] ADP Open/Copy disabled â€” missing ADP secret and sealed ADP contract token"
    );
  }
  if (!gdiSecret && !gdiContract.present) {
    console.warn(
      "[share-hydration] GDI Open/Copy disabled â€” missing GDI secret and sealed GDI contract token"
    );
  }
}

export { fingerprintAdpContractToken };
