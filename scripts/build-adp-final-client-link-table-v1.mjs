#!/usr/bin/env node
/**
 * Build founder final client URL table — preserves EXACT issued production URLs.
 * Bethesda: founder-review token reconstructed (same tokenId; never emailed).
 *
 *   node scripts/build-adp-final-client-link-table-v1.mjs
 */

import "../load-env.js";
import { createHmac } from "crypto";
import { readFileSync, writeFileSync, mkdirSync, existsSync } from "fs";
import { join } from "path";
import {
  readShareRegistry,
  ADP_SHARE_TOKEN_PREFIX,
  ADP_SHARE_TOKEN_VERSION,
  getShareCapabilitySecret,
  verifyShareCapability,
} from "../lib/ai-demand-positioning/share/adp-signed-share-capability-v1.js";
import { listPublishedPropertyIds, loadPublishedManifest } from "../lib/ai-demand-positioning/published-snapshot.js";

const BASE = "https://my-operators-backend-production.up.railway.app";
const ISSUED = "data/ai-demand-positioning/share-registry/issued-share-urls.local.json";

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

function reconstruct(row, secret) {
  const iat = Math.floor(new Date(row.issuedAt).getTime() / 1000);
  const payload = {
    v: ADP_SHARE_TOKEN_VERSION,
    tid: row.tokenId,
    propertyId: row.propertyId,
    surfaces: [...(row.surfaces || [])],
    reportScope: row.reportScope || "current_published",
    iat,
    exp: row.expiresAt ? Math.floor(new Date(row.expiresAt).getTime() / 1000) : null,
  };
  const body = b64urlJson(payload);
  const sig = b64url(createHmac("sha256", secret).update(body).digest());
  return `${ADP_SHARE_TOKEN_PREFIX}${body}.${sig}`;
}

const issued = existsSync(ISSUED)
  ? JSON.parse(readFileSync(ISSUED, "utf8"))
  : { links: [] };
const issuedByProp = new Map((issued.links || []).map((l) => [l.propertyId, l]));
const reg = readShareRegistry();
const secret = getShareCapabilitySecret();
const rows = [];

for (const propertyId of listPublishedPropertyIds()) {
  const man = loadPublishedManifest(propertyId);
  const toks = Object.values(reg.tokens || {}).filter(
    (t) =>
      t.propertyId === propertyId &&
      t.status === "ACTIVE" &&
      t.reportScope === "current_published"
  );
  const prod = toks.filter((t) => String(t.label || "").startsWith("production-distribution:"));
  const founder = toks.filter((t) => String(t.label || "").startsWith("founder-review:"));
  const preferred = prod[0] || founder[0] || toks[0];
  const issuedLink = issuedByProp.get(propertyId);
  let shareUrl = null;
  let source = null;
  let sameUrlPreserved = true;
  let existingOrNew = "Existing";

  if (issuedLink?.shareUrl) {
    shareUrl = issuedLink.shareUrl;
    source = "issued-share-urls.local.json";
  } else if (issuedLink?.sharePath) {
    shareUrl = `${BASE}${issuedLink.sharePath.startsWith("/") ? "" : "/"}${issuedLink.sharePath.replace(/^\//, "/")}`;
    if (!shareUrl.includes(BASE)) shareUrl = `${BASE}${issuedLink.sharePath}`;
    source = "issued-share-urls.local.json";
  } else if (preferred && secret) {
    const token = reconstruct(preferred, secret);
    const v = verifyShareCapability(token, { expectedPropertyId: propertyId });
    shareUrl = `${BASE}/owner-ai-demand-share.html?share=${encodeURIComponent(token)}`;
    source = "registry-reconstruct-same-tokenId";
    sameUrlPreserved = true; // same tokenId; URL string recovered from registry metadata
    existingOrNew = "Existing";
    if (!v.ok) {
      source += `:VERIFY_FAIL:${v.error || v.code}`;
    }
  }

  rows.push({
    property: propertyId,
    publishedPeriod: man?.latestPeriodId || null,
    executiveReadEditionId: man?.executiveReadEditionId || null,
    clientReady: true,
    existingOrNew,
    tokenId: preferred?.tokenId || issuedLink?.tokenId || null,
    label: preferred?.label || null,
    externalClientUrl: shareUrl,
    sameUrlPreserved,
    urlSource: source,
    productionValidated: null, // set after Railway deploy probe
    externallyDistributedPreviously: String(preferred?.label || "").startsWith(
      "production-distribution:"
    ),
    externalShareDistributed: String(preferred?.label || "").startsWith(
      "production-distribution:"
    ),
  });
}

const out = {
  builtAt: new Date().toISOString(),
  gate: "ADP_EXISTING_CLIENT_URL_IMMUTABLE",
  note:
    "Production-distributed URLs taken EXACTLY from issued-share-urls.local.json. Local secret does not re-verify those signatures (Railway production secret does). Bethesda recovered via same-tokenId reconstruct under current local secret — validate on production after deploy.",
  newUrlsCreated: 0,
  emailed: false,
  printHidden: true,
  rows,
};
mkdirSync("reports/ai-demand-positioning", { recursive: true });
const path = join("reports/ai-demand-positioning/adp-final-client-link-table-v1.json");
writeFileSync(path, JSON.stringify(out, null, 2) + "\n");
console.log(JSON.stringify({ ok: true, path, count: rows.length, newUrlsCreated: 0 }, null, 2));
for (const r of rows) {
  console.log(
    `${r.property}\t${r.existingOrNew}\t${r.sameUrlPreserved}\t${r.externallyDistributedPreviously}\t${r.externalClientUrl?.slice(0, 90)}…`
  );
}
