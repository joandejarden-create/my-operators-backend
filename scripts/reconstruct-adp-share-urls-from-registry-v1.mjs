#!/usr/bin/env node
/**
 * Reconstruct existing ADP share URLs from registry metadata (same tokenId + iat).
 * Does NOT issue new tokens. Does NOT rotate.
 *
 *   node scripts/reconstruct-adp-share-urls-from-registry-v1.mjs
 *   node scripts/reconstruct-adp-share-urls-from-registry-v1.mjs --property=adp_bethesda_marriott
 */

import "../load-env.js";
import { createHmac } from "crypto";
import { writeFileSync, mkdirSync } from "fs";
import { join } from "path";
import {
  readShareRegistry,
  ADP_SHARE_TOKEN_PREFIX,
  ADP_SHARE_TOKEN_VERSION,
  getShareCapabilitySecret,
  verifyShareCapability,
} from "../lib/ai-demand-positioning/share/adp-signed-share-capability-v1.js";
import { listPublishedPropertyIds, loadPublishedManifest } from "../lib/ai-demand-positioning/published-snapshot.js";

const PUBLIC_BASE =
  process.env.ADP_PUBLIC_BASE_URL ||
  "https://my-operators-backend-production.up.railway.app";
const propArg = process.argv.find((a) => a.startsWith("--property="))?.slice("--property=".length);

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

function reconstructToken(row, secret) {
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

const secret = getShareCapabilitySecret();
if (!secret) {
  console.error("ADP_SHARE_CAPABILITY_SECRET missing");
  process.exit(1);
}

const reg = readShareRegistry();
const published = propArg ? [propArg] : listPublishedPropertyIds();
const rows = [];

for (const propertyId of published) {
  const man = loadPublishedManifest(propertyId);
  const toks = Object.values(reg.tokens || {}).filter(
    (t) =>
      t.propertyId === propertyId &&
      t.status === "ACTIVE" &&
      t.reportScope === "current_published"
  );
  const prod = toks.filter((t) => String(t.label || "").startsWith("production-distribution:"));
  const founder = toks.filter((t) => String(t.label || "").startsWith("founder-review:"));
  const preferred = prod[0] || founder[0] || toks[0] || null;
  if (!preferred) {
    rows.push({ propertyId, ok: false, reason: "NO_ACTIVE_TOKEN" });
    continue;
  }
  const token = reconstructToken(preferred, secret);
  const verified = verifyShareCapability(token, {
    expectedPropertyId: propertyId,
    requiredSurface: "report",
  });
  const sharePath = `/owner-ai-demand-share.html?share=${encodeURIComponent(token)}`;
  const shareUrl = `${String(PUBLIC_BASE).replace(/\/$/, "")}${sharePath}`;
  rows.push({
    propertyId,
    ok: verified.ok === true,
    tokenId: preferred.tokenId,
    label: preferred.label,
    reportScope: preferred.reportScope,
    periodId: man?.latestPeriodId || null,
    executiveReadEditionId: man?.executiveReadEditionId || null,
    externallyDistributed: String(preferred.label || "").startsWith("production-distribution:"),
    existingOrNew: "Existing",
    sameUrlPreserved: true,
    sharePath,
    shareUrl,
    verifyError: verified.ok ? null : verified.error || verified.code,
  });
}

const out = {
  reconstructedAt: new Date().toISOString(),
  publicBase: PUBLIC_BASE,
  methodologyChanged: false,
  tokenRotation: false,
  rows,
  fail: rows.filter((r) => !r.ok),
};
mkdirSync(join(process.cwd(), "reports/ai-demand-positioning"), { recursive: true });
const outPath = join(
  process.cwd(),
  "reports/ai-demand-positioning/adp-reconstructed-client-share-urls-v1.json"
);
writeFileSync(outPath, JSON.stringify(out, null, 2) + "\n");
console.log(JSON.stringify({ ok: out.fail.length === 0, outPath, count: rows.length, fail: out.fail.length }, null, 2));
for (const r of rows) {
  console.log(`${r.propertyId}\t${r.ok ? "OK" : "FAIL"}\t${r.tokenId}\t${r.shareUrl || r.reason}`);
}
if (out.fail.length) process.exit(1);
