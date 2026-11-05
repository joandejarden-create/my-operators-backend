/**
 * AVA staged PDF archive under durable storage root.
 * Layout: {AVA_STORAGE_ROOT}/pdfs/{propertyId}/current.pdf
 *          {AVA_STORAGE_ROOT}/pdfs/{propertyId}/{versionId}.pdf
 *          {AVA_STORAGE_ROOT}/pdfs/{propertyId}/checksums.json
 *
 * Storage-relative path returned: pdfs/{propertyId}/current.pdf
 */
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import {
  assertAvaDurableWritable,
  isAvaDurableStorageEnabled,
  resolveAvaPdfAbsolute,
  repoRoot,
} from "./resolve-ava-storage-root-v1.js";
import { AVA_PRODUCTION_ARCHIVE_ROOT } from "./certified-baseline-paths-v1.js";

function sha256File(abs) {
  return crypto.createHash("sha256").update(fs.readFileSync(abs)).digest("hex");
}

function atomicCopy(src, dest) {
  fs.mkdirSync(path.dirname(dest), { recursive: true });
  const tmp = `${dest}.${process.pid}.tmp`;
  fs.copyFileSync(src, tmp);
  fs.renameSync(tmp, dest);
}

function atomicWriteJson(filePath, obj) {
  fs.mkdirSync(path.dirname(filePath), { recursive: true });
  const tmp = `${filePath}.${process.pid}.tmp`;
  fs.writeFileSync(tmp, `${JSON.stringify(obj, null, 2)}\n`, "utf8");
  fs.renameSync(tmp, filePath);
}

export function resolveAvaPdfOutDir(propertyId, env = process.env) {
  if (isAvaDurableStorageEnabled(env)) {
    const state = assertAvaDurableWritable(env);
    return path.join(state.layout.pdfs, propertyId);
  }
  return path.join(AVA_PRODUCTION_ARCHIVE_ROOT, propertyId);
}

/**
 * Persist a generated PDF into the archive. Returns storage-relative path
 * suitable for AUDIT_META.pdfRelativePath.
 */
export function stageAvaPdf({
  propertyId,
  generatedAbsPath,
  versionId,
  filename,
  env = process.env,
}) {
  const outDir = resolveAvaPdfOutDir(propertyId, env);
  fs.mkdirSync(outDir, { recursive: true });

  const versionName = `${versionId || "staged"}.pdf`;
  const versionAbs = path.join(outDir, versionName);
  const draftAbs = path.join(outDir, "draft.pdf");
  // current.pdf remains a draft alias for legacy readers — NEVER overwrite published.pdf
  const currentAbs = path.join(outDir, "current.pdf");
  const publishedAbs = path.join(outDir, "published.pdf");
  const namedAbs = path.join(outDir, filename || path.basename(generatedAbsPath));

  atomicCopy(generatedAbsPath, versionAbs);
  atomicCopy(generatedAbsPath, draftAbs);
  atomicCopy(generatedAbsPath, currentAbs);
  if (filename) atomicCopy(generatedAbsPath, namedAbs);

  // Explicit safeguard: staging a draft must not replace a frozen published artifact.
  if (fs.existsSync(publishedAbs)) {
    const publishedSha = sha256File(publishedAbs);
    const draftSha = sha256File(draftAbs);
    if (publishedSha === draftSha) {
      // Unusual but legal if regenerate matched published bytes.
    }
  }

  const sha = sha256File(versionAbs);
  const checksumsPath = path.join(outDir, "checksums.json");
  let checksums = { propertyId, versions: {} };
  if (fs.existsSync(checksumsPath)) {
    try {
      checksums = JSON.parse(fs.readFileSync(checksumsPath, "utf8"));
    } catch (_err) {
      checksums = { propertyId, versions: {} };
    }
  }
  checksums.versions = checksums.versions || {};
  checksums.versions[versionId || "current"] = {
    sha256: sha,
    file: versionName,
    stagedAt: new Date().toISOString(),
    channel: "draft",
  };
  checksums.draftSha256 = sha;
  checksums.draftVersionId = versionId || null;
  checksums.currentSha256 = sha;
  if (fs.existsSync(publishedAbs)) {
    checksums.publishedSha256 = sha256File(publishedAbs);
  }
  checksums.updatedAt = new Date().toISOString();
  atomicWriteJson(checksumsPath, checksums);

  if (isAvaDurableStorageEnabled(env)) {
    return {
      // Prefer immutable version path for draft registration (not current.pdf alias).
      pdfRelativePath: `pdfs/${propertyId}/${versionName}`,
      draftPdfRelativePath: `pdfs/${propertyId}/${versionName}`,
      currentAbs: draftAbs,
      versionAbs,
      sha256: sha,
      durable: true,
      channel: "draft",
      publishedUntouched: true,
    };
  }
  const rel = path
    .relative(repoRoot(), versionAbs)
    .replace(/\\/g, "/");
  return {
    pdfRelativePath: rel,
    draftPdfRelativePath: rel,
    currentAbs: draftAbs,
    versionAbs,
    sha256: sha,
    durable: false,
    channel: "draft",
    publishedUntouched: true,
  };
}

export function resolveStagedAvaPdfAbs(pdfRelativePath, env = process.env) {
  return resolveAvaPdfAbsolute(pdfRelativePath, env);
}

export function readAvaPdfChecksums(propertyId, env = process.env) {
  const dir = resolveAvaPdfOutDir(propertyId, env);
  const p = path.join(dir, "checksums.json");
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

export function assertSafeAvaPropertyId(propertyId) {
  const id = String(propertyId || "");
  if (!/^adp_[a-z0-9_]+$/i.test(id)) {
    const err = new Error("INVALID_PROPERTY_ID");
    err.code = "INVALID_PROPERTY_ID";
    throw err;
  }
  return id;
}

export function assertSafeAvaVersionId(versionId) {
  const id = String(versionId || "");
  if (!id || !/^[a-zA-Z0-9][a-zA-Z0-9._-]{0,180}$/.test(id) || id.includes("..")) {
    const err = new Error("INVALID_VERSION_ID");
    err.code = "INVALID_VERSION_ID";
    throw err;
  }
  return id;
}

function isPathInside(parent, candidate) {
  const root = path.resolve(parent);
  const target = path.resolve(candidate);
  const rel = path.relative(root, target);
  return rel === "" || (!rel.startsWith("..") && !path.isAbsolute(rel));
}

/**
 * Resolve PDF bytes for latest or a specific version.
 * Never follows arbitrary client paths — only store-relative locations.
 */
export function resolveAvaPdfFileForDelivery({
  propertyId,
  versionId = null,
  pdfRelativePath = null,
  env = process.env,
} = {}) {
  const safePropertyId = assertSafeAvaPropertyId(propertyId);
  const outDir = resolveAvaPdfOutDir(safePropertyId, env);
  const allowedRoots = [
    outDir,
    path.join(AVA_PRODUCTION_ARCHIVE_ROOT, safePropertyId),
    path.join(repoRoot(), "reports/ai-visibility-audit-action-plan/archive", safePropertyId),
    path.join(
      repoRoot(),
      "reports/dealality-ai-visibility-audit-action-plan-pilot-v1"
    ),
  ];

  const candidates = [];
  if (versionId) {
    const safeVersion = assertSafeAvaVersionId(versionId);
    candidates.push(path.join(outDir, `${safeVersion}.pdf`));
  }
  if (pdfRelativePath) {
    const abs = resolveAvaPdfAbsolute(pdfRelativePath, env);
    if (abs) candidates.push(abs);
  }
  candidates.push(path.join(outDir, "current.pdf"));
  candidates.push(
    path.join(AVA_PRODUCTION_ARCHIVE_ROOT, safePropertyId, "current.pdf")
  );

  for (const candidate of candidates) {
    if (!candidate || !fs.existsSync(candidate)) continue;
    const resolved = path.resolve(candidate);
    const allowed = allowedRoots.some((root) => {
      try {
        return isPathInside(root, resolved) || isPathInside(path.dirname(root), resolved);
      } catch {
        return false;
      }
    });
    // Also allow if under repo reports/ or data/ava-data/pdfs/
    const underRepoReports = isPathInside(
      path.join(repoRoot(), "reports"),
      resolved
    );
    const underAvaData = isPathInside(
      path.join(repoRoot(), "data/ava-data/pdfs"),
      resolved
    );
    const underEnvRoot =
      env.AVA_STORAGE_ROOT &&
      isPathInside(path.join(path.resolve(env.AVA_STORAGE_ROOT), "pdfs"), resolved);
    if (!allowed && !underRepoReports && !underAvaData && !underEnvRoot) {
      continue;
    }
    if (!resolved.toLowerCase().endsWith(".pdf")) continue;
    return {
      abs: resolved,
      sha256: sha256File(resolved),
      propertyId: safePropertyId,
      versionId: versionId || null,
    };
  }
  return null;
}
