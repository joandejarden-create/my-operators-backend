#!/usr/bin/env node
/**
 * GDI frontend asset identity — local tree vs production served bytes.
 *   node scripts/gdi-frontend-asset-identity-v1.mjs
 *   GDI_PARITY_PRODUCTION_URL=... node scripts/gdi-frontend-asset-identity-v1.mjs
 */
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const outDir = path.join(
  ROOT,
  "reports/gdi-complete-application-release-parity-v1",
);
fs.mkdirSync(outDir, { recursive: true });

const base = (
  process.env.GDI_PARITY_PRODUCTION_URL ||
  "https://my-operators-backend-production.up.railway.app"
).replace(/\/$/, "");

/** Customer-facing share closure + shared modules used by share HTML. */
const ASSETS = [
  "public/group-demand-intelligence-share.html",
  "public/group-demand-intelligence.html",
  "public/css/group-demand-intelligence.css",
  "public/css/deal-workspace-shell.css",
  "public/js/ai-visibility/ai-visibility-shared.css",
  "public/js/ai-demand-positioning/adp-guided-report-tour.css",
  "public/js/group-demand-intelligence/dealality-gdi-ui.js",
  "public/js/group-demand-intelligence/share-app.js",
  "public/js/group-demand-intelligence/app.js",
  "public/js/group-demand-intelligence/gdi-universal-presentation-controller.js",
  "public/js/group-demand-intelligence/gdi-guided-report-tour.js",
  "public/js/dealality-guided-tour/dealality-guided-tour-engine.js",
];

function sha256(buf) {
  return crypto.createHash("sha256").update(buf).digest("hex");
}

/** Normalize EOL so Windows CRLF checkouts compare equal to LF production trees. */
function normalizeEol(buf) {
  return Buffer.from(
    Buffer.from(buf).toString("utf8").replace(/\r\n/g, "\n").replace(/\r/g, "\n"),
  );
}

function toUrlPath(rel) {
  return "/" + rel.replace(/^public\//, "").split(path.sep).join("/");
}

async function fetchBuf(url) {
  const res = await fetch(url, {
    headers: { "Cache-Control": "no-cache", Pragma: "no-cache" },
  });
  const ab = await res.arrayBuffer();
  return { status: res.status, buf: Buffer.from(ab), headers: res.headers };
}

const rows = [];
for (const rel of ASSETS) {
  const localPath = path.join(ROOT, rel);
  const local = fs.existsSync(localPath) ? fs.readFileSync(localPath) : null;
  const urlPath = toUrlPath(rel);
  let remote = { status: 0, buf: Buffer.alloc(0) };
  try {
    remote = await fetchBuf(`${base}${urlPath}?cb=${Date.now()}`);
  } catch (e) {
    remote = { status: -1, buf: Buffer.from(String(e.message || e)) };
  }
  const localNorm = local ? normalizeEol(local) : null;
  const remoteNorm =
    remote.status === 200 ? normalizeEol(remote.buf) : null;
  const localSha = localNorm ? sha256(localNorm) : null;
  const remoteSha = remoteNorm ? sha256(remoteNorm) : null;
  rows.push({
    rel,
    urlPath,
    localBytes: local?.length ?? null,
    localSha256: localSha,
    remoteStatus: remote.status,
    remoteBytes: remote.buf.length,
    remoteSha256: remoteSha,
    identical: Boolean(localSha && remoteSha && localSha === remoteSha),
    note:
      local &&
      remote.status === 200 &&
      sha256(local) !== sha256(remote.buf) &&
      localSha === remoteSha
        ? "raw_bytes_differ_eol_only"
        : null,
  });
}

const manifest = {
  generatedAt: new Date().toISOString(),
  productionBase: base,
  assetCount: rows.length,
  identicalCount: rows.filter((r) => r.identical).length,
  mismatchCount: rows.filter((r) => !r.identical).length,
  mismatches: rows.filter((r) => !r.identical),
  rows,
};

fs.writeFileSync(
  path.join(outDir, "02-FRONTEND-ASSET-MANIFEST.json"),
  JSON.stringify(manifest, null, 2),
);
fs.writeFileSync(
  path.join(outDir, "03-LOCAL-PRODUCTION-ASSET-DIFF.json"),
  JSON.stringify(
    {
      ok: manifest.mismatchCount === 0,
      mismatchCount: manifest.mismatchCount,
      mismatches: manifest.mismatches.map((m) => ({
        rel: m.rel,
        localSha256: m.localSha256,
        remoteSha256: m.remoteSha256,
        remoteStatus: m.remoteStatus,
        localBytes: m.localBytes,
        remoteBytes: m.remoteBytes,
      })),
    },
    null,
    2,
  ),
);

console.log(
  JSON.stringify(
    {
      ok: manifest.mismatchCount === 0,
      identicalCount: manifest.identicalCount,
      mismatchCount: manifest.mismatchCount,
      mismatches: manifest.mismatches.map((m) => m.rel),
    },
    null,
    2,
  ),
);
process.exit(manifest.mismatchCount === 0 ? 0 : 2);
