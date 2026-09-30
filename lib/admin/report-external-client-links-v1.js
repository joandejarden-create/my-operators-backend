/**
 * Admin external client links for ADP + GDI reports.
 * Reconstructs stable share URLs from existing registries.
 * NEVER rotates Bethesda contract token gdisht_47c25d74c79216021fb36150.
 */

import { createHmac, createHash, randomBytes } from "crypto";
import { existsSync, readFileSync } from "fs";
import { join } from "path";
import {
  readShareRegistry,
  ADP_SHARE_TOKEN_PREFIX,
  ADP_SHARE_TOKEN_VERSION,
  getShareCapabilitySecret,
  issueShareCapability,
  DEFAULT_SHARE_SURFACES,
} from "../ai-demand-positioning/share/adp-signed-share-capability-v1.js";
import {
  readGdiShareRegistry,
  GDI_SHARE_TOKEN_PREFIX,
  GDI_SHARE_TOKEN_VERSION,
  getGdiShareCapabilitySecret,
  issueGdiShareCapability,
  DEFAULT_GDI_SHARE_SURFACES,
  isTokenIdDurablyRevoked,
} from "../group-demand-intelligence/share/gdi-signed-share-capability-v1.js";
import {
  loadCensusLinkRegistry,
  getCensusRecordIdForAdpProperty,
} from "../ai-demand-positioning/census-link-registry.js";

export const BETHESDA_GDI_CONTRACT_TOKEN_ID = "gdisht_47c25d74c79216021fb36150";
export const BETHESDA_HPC = "recLuxvwwxID7U2B8";

const ROOT = process.cwd();
const CONTRACT_PATH = join(
  ROOT,
  "config/client-share/production-share-contract-tokens.json"
);

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

  const isProdLike =
    process.env.NODE_ENV === "production" ||
    process.env.RAILWAY_ENVIRONMENT === "production" ||
    /railway\.app/i.test(base);

  if (base) {
    if (isProdLike && /localhost|127\.0\.0\.1/i.test(base)) {
      return "https://my-operators-backend-production.up.railway.app";
    }
    return base;
  }

  if (req) {
    const host = String(req.get?.("x-forwarded-host") || req.get?.("host") || "").trim();
    const proto = String(req.get?.("x-forwarded-proto") || req.protocol || "https").split(",")[0].trim();
    if (host) {
      if (isProdLike && /localhost|127\.0\.0\.1/i.test(host)) {
        return "https://my-operators-backend-production.up.railway.app";
      }
      return `${proto}://${host}`.replace(/\/$/, "");
    }
  }

  if (isProdLike) return "https://my-operators-backend-production.up.railway.app";
  return "http://localhost:3000";
}

export function getAdpPropertyIdForHpc(hpcHotelId) {
  const id = String(hpcHotelId || "").trim();
  if (!id) return null;
  if (id.startsWith("adp_")) return id;
  const reg = loadCensusLinkRegistry();
  for (const [propertyId, entry] of Object.entries(reg.links || {})) {
    if (String(entry?.censusRecordId || "").trim() === id) return propertyId;
  }
  return null;
}

export function getHpcForAdpProperty(propertyId) {
  return getCensusRecordIdForAdpProperty(propertyId);
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

function loadBethesdaContractToken() {
  if (!existsSync(CONTRACT_PATH)) return null;
  try {
    const doc = JSON.parse(readFileSync(CONTRACT_PATH, "utf8"));
    const row = (doc.tokens || []).find(
      (t) =>
        t.product === "GDI" &&
        t.tokenId === BETHESDA_GDI_CONTRACT_TOKEN_ID &&
        t.hotelId === BETHESDA_HPC
    );
    return row?.token || null;
  } catch {
    return null;
  }
}

function preferAdpRow(rows) {
  const active = rows.filter((t) => t.status === "ACTIVE" && !t.revokedAt);
  const prod = active.filter((t) => String(t.label || "").startsWith("production-distribution:"));
  const founder = active.filter((t) => String(t.label || "").startsWith("founder-review:"));
  return prod[0] || founder[0] || active.sort((a, b) => String(b.issuedAt).localeCompare(String(a.issuedAt)))[0] || null;
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
  const labeled = active.filter((t) => /pilot|client|production|rad/i.test(String(t.label || "")));
  return (
    labeled[0] ||
    active.sort((a, b) => String(a.issuedAt || "").localeCompare(String(b.issuedAt || "")))[0] ||
    null
  );
}

/**
 * Resolve ADP + GDI external client links for a hotel.
 * @param {string} hotelKey — HPC rec… or adp_* property id
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
      if (!preferred && createIfMissing && secret) {
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

      if (hpcId === BETHESDA_HPC && beforeBethesda) {
        const path = `/group-demand-intelligence-share.html?share=${encodeURIComponent(beforeBethesda)}`;
        out.gdi.available = true;
        out.gdi.tokenId = BETHESDA_GDI_CONTRACT_TOKEN_ID;
        out.gdi.path = path;
        out.gdi.url = `${base}${path}`;
        out.gdi.bethesdaContractPreserved = true;
      } else if (preferred && secret) {
        const token = reconstructGdiToken(preferred, secret);
        const path = `/group-demand-intelligence-share.html?share=${encodeURIComponent(token)}`;
        out.gdi.available = true;
        out.gdi.tokenId = preferred.tokenId;
        out.gdi.path = path;
        out.gdi.url = `${base}${path}`;
        if (hpcId === BETHESDA_HPC) {
          out.gdi.bethesdaContractPreserved =
            preferred.tokenId === BETHESDA_GDI_CONTRACT_TOKEN_ID;
        }
      } else if (!preferred && createIfMissing && secret) {
        // Never create a replacement for Bethesda contract — only if somehow missing entirely
        if (hpcId === BETHESDA_HPC && beforeBethesda) {
          const path = `/group-demand-intelligence-share.html?share=${encodeURIComponent(beforeBethesda)}`;
          out.gdi.available = true;
          out.gdi.tokenId = BETHESDA_GDI_CONTRACT_TOKEN_ID;
          out.gdi.path = path;
          out.gdi.url = `${base}${path}`;
          out.gdi.bethesdaContractPreserved = true;
        } else {
          const issued = issueGdiShareCapability({
            hotelId: hpcId,
            label: `admin-external-client:${hpcId}`,
            tokenId:
              hpcId === BETHESDA_HPC
                ? BETHESDA_GDI_CONTRACT_TOKEN_ID
                : `gdisht_${randomBytes(12).toString("hex")}`,
          });
          out.gdi.created = true;
          out.gdi.available = true;
          out.gdi.tokenId = issued.tokenId;
          out.gdi.path = issued.sharePath;
          out.gdi.url = `${base}${issued.sharePath}`;
          if (hpcId === BETHESDA_HPC) {
            out.gdi.bethesdaContractPreserved =
              issued.tokenId === BETHESDA_GDI_CONTRACT_TOKEN_ID;
          }
        }
      } else if (!secret) {
        out.gdi.reason = "GDI share secret not configured in this environment.";
      } else {
        out.gdi.reason = "No active GDI client link is available for this hotel.";
      }

      // Hard proof: Bethesda contract token string unchanged on disk
      if (hpcId === BETHESDA_HPC) {
        const after = loadBethesdaContractToken();
        out.gdi.bethesdaContractPreserved =
          Boolean(beforeBethesda) && beforeBethesda === after;
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
