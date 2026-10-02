/**
 * GDI PDF store V1 — durable filesystem SoT.
 *
 * Default: reports/group-demand-intelligence/pdf/
 * Override for Railway volume / persistent disk:
 *   DEALALITY_REPORT_PDF_ROOT or GDI_PDF_DATA_DIR
 *
 * Current pointer: {root}/{hotelId}/report.pdf + meta.json
 * Versioned immutable copies: {root}/{hotelId}/archive/v{N}/report.pdf + snapshot.json + meta.json
 */

import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { buildGdiPdfFilename } from "./gdi-pdf-report-data-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const REPO_ROOT = path.resolve(__dirname, "../../..");

export function resolveGdiPdfRoot(env = process.env) {
  const override = String(
    env.DEALALITY_REPORT_PDF_ROOT || env.GDI_PDF_DATA_DIR || ""
  ).trim();
  if (override) return path.resolve(override, "group-demand-intelligence/pdf");
  return path.join(REPO_ROOT, "reports/group-demand-intelligence/pdf");
}

export const GDI_PDF_ROOT = resolveGdiPdfRoot();

export function gdiPdfHotelDir(hotelId, env = process.env) {
  return path.join(resolveGdiPdfRoot(env), String(hotelId || "").trim());
}

export function gdiPdfPaths(hotelId, env = process.env) {
  const dir = gdiPdfHotelDir(hotelId, env);
  return {
    dir,
    pdfPath: path.join(dir, "report.pdf"),
    metaPath: path.join(dir, "meta.json"),
    archiveDir: path.join(dir, "archive"),
  };
}

export function hasGdiReportPdf(hotelId, env = process.env) {
  const { pdfPath } = gdiPdfPaths(hotelId, env);
  return fs.existsSync(pdfPath) && fs.statSync(pdfPath).size > 100;
}

function sha256Buffer(buf) {
  return crypto.createHash("sha256").update(buf).digest("hex");
}

function nextArchiveVersion(archiveDir) {
  if (!fs.existsSync(archiveDir)) return 1;
  const versions = fs
    .readdirSync(archiveDir)
    .map((n) => /^v(\d+)$/.exec(n))
    .filter(Boolean)
    .map((m) => Number(m[1]));
  return versions.length ? Math.max(...versions) + 1 : 1;
}

/**
 * Write current PDF pointer + immutable versioned archive copy.
 * Never overwrites prior archive versions.
 */
export function writeGdiReportPdf(hotelId, buffer, meta = {}, env = process.env) {
  const { dir, pdfPath, metaPath, archiveDir } = gdiPdfPaths(hotelId, env);
  fs.mkdirSync(dir, { recursive: true });
  if (/\.tmp$/i.test(pdfPath)) {
    throw new Error("GDI_PDF_TEMP_FILE_NAME_NEVER_USER_VISIBLE");
  }
  fs.writeFileSync(pdfPath, buffer);
  const checksum = sha256Buffer(buffer);
  const filename =
    meta.filename ||
    buildGdiPdfFilename(meta.hotelName || hotelId, meta.reportDate);
  const version = nextArchiveVersion(archiveDir);
  const versionLabel = `v${version}`;
  const versionDir = path.join(archiveDir, versionLabel);
  fs.mkdirSync(versionDir, { recursive: true });
  const versionPdf = path.join(versionDir, "report.pdf");
  fs.writeFileSync(versionPdf, buffer);

  const payload = {
    hotelId,
    filename,
    generatedAt: new Date().toISOString(),
    byteLength: buffer.length,
    checksumSha256: checksum,
    version: versionLabel,
    versionNumber: version,
    durableRoot: resolveGdiPdfRoot(env),
    ...meta,
    filename,
    checksumSha256: checksum,
    version: versionLabel,
    versionNumber: version,
  };
  fs.writeFileSync(metaPath, JSON.stringify(payload, null, 2), "utf8");
  fs.writeFileSync(
    path.join(versionDir, "meta.json"),
    JSON.stringify(payload, null, 2),
    "utf8"
  );
  if (meta.reportSnapshot) {
    const snapPath = path.join(versionDir, "snapshot.json");
    const snapBody = JSON.stringify(meta.reportSnapshot, null, 2);
    fs.writeFileSync(snapPath, snapBody, "utf8");
    fs.writeFileSync(
      path.join(versionDir, "snapshot.sha256"),
      crypto.createHash("sha256").update(snapBody).digest("hex") + "\n",
      "utf8"
    );
  }
  return { pdfPath, metaPath, meta: payload, versionDir, versionLabel };
}

export function readGdiReportPdf(hotelId, env = process.env) {
  const { pdfPath, metaPath } = gdiPdfPaths(hotelId, env);
  if (!fs.existsSync(pdfPath)) return null;
  const buffer = fs.readFileSync(pdfPath);
  let meta = {};
  if (fs.existsSync(metaPath)) {
    try {
      meta = JSON.parse(fs.readFileSync(metaPath, "utf8"));
    } catch {
      meta = {};
    }
  }
  const checksum = sha256Buffer(buffer);
  return {
    buffer,
    meta,
    pdfPath,
    checksumSha256: checksum,
    checksumMatches:
      !meta.checksumSha256 || meta.checksumSha256 === checksum,
  };
}

export function listGdiPdfArchiveVersions(hotelId, env = process.env) {
  const { archiveDir } = gdiPdfPaths(hotelId, env);
  if (!fs.existsSync(archiveDir)) return [];
  return fs
    .readdirSync(archiveDir)
    .filter((n) => /^v\d+$/.test(n))
    .map((n) => {
      const versionDir = path.join(archiveDir, n);
      const metaPath = path.join(versionDir, "meta.json");
      let meta = {};
      if (fs.existsSync(metaPath)) {
        try {
          meta = JSON.parse(fs.readFileSync(metaPath, "utf8"));
        } catch {
          meta = {};
        }
      }
      return {
        version: n,
        versionNumber: Number(n.slice(1)),
        versionDir,
        pdfPath: path.join(versionDir, "report.pdf"),
        snapshotPath: path.join(versionDir, "snapshot.json"),
        meta,
        hasPdf: fs.existsSync(path.join(versionDir, "report.pdf")),
        hasSnapshot: fs.existsSync(path.join(versionDir, "snapshot.json")),
      };
    })
    .sort((a, b) => a.versionNumber - b.versionNumber);
}

export function readGdiPdfArchiveVersion(hotelId, version, env = process.env) {
  const label = String(version || "").startsWith("v")
    ? String(version)
    : `v${version}`;
  const versionDir = path.join(gdiPdfPaths(hotelId, env).archiveDir, label);
  const pdfPath = path.join(versionDir, "report.pdf");
  if (!fs.existsSync(pdfPath)) return null;
  const buffer = fs.readFileSync(pdfPath);
  let meta = {};
  const metaPath = path.join(versionDir, "meta.json");
  if (fs.existsSync(metaPath)) {
    try {
      meta = JSON.parse(fs.readFileSync(metaPath, "utf8"));
    } catch {
      meta = {};
    }
  }
  let snapshot = null;
  const snapPath = path.join(versionDir, "snapshot.json");
  if (fs.existsSync(snapPath)) {
    snapshot = JSON.parse(fs.readFileSync(snapPath, "utf8"));
  }
  return {
    version: label,
    buffer,
    meta,
    snapshot,
    checksumSha256: sha256Buffer(buffer),
    pdfPath,
  };
}
