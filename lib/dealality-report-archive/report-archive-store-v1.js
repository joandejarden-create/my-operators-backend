/**
 * Dealality Report Archive V1 — immutable published ADP/GDI report ledger.
 *
 * Root (override with DEALALITY_REPORT_ARCHIVE_ROOT):
 *   data/dealality-report-archive/
 *
 * Layout:
 *   {root}/index.json
 *   {root}/{hotelKey}/{reportType}/{archiveId}/
 *     meta.json
 *     snapshot.json
 *     snapshot.sha256
 *     report.pdf (optional)
 *     report.pdf.sha256 (optional)
 */

import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
// lib/dealality-report-archive → repo root (two levels up)
const REPO_ROOT = path.resolve(__dirname, "../..");

export const REPORT_ARCHIVE_VERSION = "dealality_report_archive_v1";

export function resolveReportArchiveRoot(env = process.env) {
  const override = String(env.DEALALITY_REPORT_ARCHIVE_ROOT || "").trim();
  if (override) return path.resolve(override);
  const pdfRoot = String(env.DEALALITY_REPORT_PDF_ROOT || "").trim();
  if (pdfRoot) return path.resolve(pdfRoot, "report-archive");
  return path.join(REPO_ROOT, "data/dealality-report-archive");
}

function ensureDir(dir) {
  fs.mkdirSync(dir, { recursive: true });
}

function sha256(value) {
  const buf = Buffer.isBuffer(value) ? value : Buffer.from(String(value), "utf8");
  return crypto.createHash("sha256").update(buf).digest("hex");
}

function indexPath(env) {
  return path.join(resolveReportArchiveRoot(env), "index.json");
}

function readIndex(env = process.env) {
  const p = indexPath(env);
  if (!fs.existsSync(p)) {
    return { version: REPORT_ARCHIVE_VERSION, entries: [] };
  }
  try {
    return JSON.parse(fs.readFileSync(p, "utf8"));
  } catch {
    return { version: REPORT_ARCHIVE_VERSION, entries: [] };
  }
}

function writeIndex(index, env = process.env) {
  const root = resolveReportArchiveRoot(env);
  ensureDir(root);
  fs.writeFileSync(indexPath(env), JSON.stringify(index, null, 2) + "\n", "utf8");
}

function hotelKeyOf(entry) {
  return String(entry.hotelId || entry.propertyId || entry.hotelKey || "unknown").trim();
}

/**
 * Publish an immutable archive entry. Never overwrites an existing archiveId.
 */
export function publishReportArchiveEntry(input = {}, env = process.env) {
  const hotelKey = hotelKeyOf(input);
  const reportType = String(input.reportType || "").toUpperCase(); // ADP | GDI
  if (!hotelKey || !["ADP", "GDI"].includes(reportType)) {
    throw new Error("archive_entry_requires_hotel_and_type");
  }
  const reportClass = String(input.reportClass || "CURRENT").toUpperCase();
  const reportDate = String(input.reportDate || "").slice(0, 10);
  const versionLabel = String(input.versionLabel || input.version || "v1");
  const archiveId =
    input.archiveId ||
    `${reportType.toLowerCase()}_${reportClass.toLowerCase()}_${reportDate || "undated"}_${versionLabel}`;

  const root = resolveReportArchiveRoot(env);
  const entryDir = path.join(root, hotelKey, reportType, archiveId);
  if (fs.existsSync(path.join(entryDir, "meta.json")) && !input.allowReplace) {
    throw new Error(`ARCHIVE_IMMUTABLE: ${archiveId} already exists`);
  }
  ensureDir(entryDir);

  const snapshot = input.snapshot || null;
  let snapshotChecksum = null;
  if (snapshot) {
    const snapBody = `${JSON.stringify(snapshot, null, 2)}\n`;
    fs.writeFileSync(path.join(entryDir, "snapshot.json"), snapBody, "utf8");
    snapshotChecksum = sha256(snapBody);
    fs.writeFileSync(path.join(entryDir, "snapshot.sha256"), snapshotChecksum + "\n", "utf8");
  }

  let pdfChecksum = null;
  let pdfByteLength = null;
  if (input.pdfBuffer && Buffer.isBuffer(input.pdfBuffer)) {
    fs.writeFileSync(path.join(entryDir, "report.pdf"), input.pdfBuffer);
    pdfChecksum = sha256(input.pdfBuffer);
    pdfByteLength = input.pdfBuffer.length;
    fs.writeFileSync(path.join(entryDir, "report.pdf.sha256"), pdfChecksum + "\n", "utf8");
  } else if (input.pdfPath && fs.existsSync(input.pdfPath)) {
    const buf = fs.readFileSync(input.pdfPath);
    fs.writeFileSync(path.join(entryDir, "report.pdf"), buf);
    pdfChecksum = sha256(buf);
    pdfByteLength = buf.length;
    fs.writeFileSync(path.join(entryDir, "report.pdf.sha256"), pdfChecksum + "\n", "utf8");
  }

  const meta = {
    archiveId,
    hotelKey,
    hotelId: input.hotelId || null,
    propertyId: input.propertyId || null,
    hotelName: input.hotelName || null,
    reportType,
    reportClass,
    reportDate: reportDate || null,
    snapshotDate: input.snapshotDate || reportDate || null,
    version: versionLabel,
    generatedAt: input.generatedAt || new Date().toISOString(),
    archivedAt: new Date().toISOString(),
    immutable: true,
    supersedes: input.supersedes || null,
    pdfStorageRef: pdfChecksum
      ? path.relative(REPO_ROOT, path.join(entryDir, "report.pdf")).replace(/\\/g, "/")
      : input.pdfStorageRef || null,
    pdfMissingReason: pdfChecksum ? null : input.pdfMissingReason || null,
    pdfChecksumSha256: pdfChecksum,
    pdfByteLength,
    snapshotChecksumSha256: snapshotChecksum,
    codeSha: input.codeSha || process.env.RAILWAY_GIT_COMMIT_SHA || null,
    methodologyVersion: input.methodologyVersion || null,
    querySetId: input.querySetId || null,
    baselineId: input.baselineId || null,
    externalShareTokenId: input.externalShareTokenId || null,
    notes: input.notes || null,
  };
  fs.writeFileSync(path.join(entryDir, "meta.json"), JSON.stringify(meta, null, 2) + "\n", "utf8");

  const index = readIndex(env);
  index.entries = (index.entries || []).filter((e) => e.archiveId !== archiveId);
  index.entries.push({
    archiveId,
    hotelKey,
    hotelId: meta.hotelId,
    propertyId: meta.propertyId,
    hotelName: meta.hotelName,
    reportType,
    reportClass,
    reportDate: meta.reportDate,
    snapshotDate: meta.snapshotDate,
    version: versionLabel,
    generatedAt: meta.generatedAt,
    archivedAt: meta.archivedAt,
    hasPdf: Boolean(pdfChecksum),
    hasSnapshot: Boolean(snapshotChecksum),
    pdfMissingReason: meta.pdfMissingReason || null,
    pdfChecksumSha256: pdfChecksum,
    snapshotChecksumSha256: snapshotChecksum,
    supersedes: meta.supersedes,
    entryDir,
  });
  index.entries.sort((a, b) =>
    String(b.reportDate || "").localeCompare(String(a.reportDate || "")) ||
    String(b.archivedAt || "").localeCompare(String(a.archivedAt || ""))
  );
  writeIndex(index, env);
  return meta;
}

export function listReportArchiveEntries(filter = {}, env = process.env) {
  const index = readIndex(env);
  let rows = index.entries || [];
  const hotel =
    filter.hotelId || filter.propertyId || filter.hotelKey || null;
  if (hotel) {
    const h = String(hotel);
    rows = rows.filter(
      (e) => e.hotelKey === h || e.hotelId === h || e.propertyId === h
    );
  }
  if (filter.reportType && filter.reportType !== "ALL") {
    rows = rows.filter((e) => e.reportType === String(filter.reportType).toUpperCase());
  }
  if (filter.reportClass && filter.reportClass !== "ALL") {
    const cls = String(filter.reportClass).toUpperCase();
    rows = rows.filter((e) => e.reportClass === cls);
  }
  return rows.map((e) => ({
    ...e,
    entryDirRel: path.relative(REPO_ROOT, e.entryDir).replace(/\\/g, "/"),
  }));
}

export function loadReportArchiveEntry(archiveId, env = process.env) {
  const index = readIndex(env);
  const hit = (index.entries || []).find((e) => e.archiveId === archiveId);
  if (!hit) return null;
  const root = resolveReportArchiveRoot(env);
  // Prefer canonical path under current root (index may carry stale absolute entryDir
  // from prior mis-rooted deploys / migrations).
  const entryDir = path.join(
    root,
    String(hit.hotelKey || hit.hotelId || "unknown"),
    String(hit.reportType || "UNKNOWN"),
    String(archiveId)
  );
  if (!fs.existsSync(path.join(entryDir, "meta.json"))) {
    // Fallback to recorded absolute path if present and readable
    if (hit.entryDir && fs.existsSync(path.join(hit.entryDir, "meta.json"))) {
      return loadReportArchiveEntryFromDir(hit.entryDir);
    }
    return null;
  }
  return loadReportArchiveEntryFromDir(entryDir);
}

function loadReportArchiveEntryFromDir(entryDir) {
  const meta = JSON.parse(fs.readFileSync(path.join(entryDir, "meta.json"), "utf8"));
  let snapshot = null;
  const snapPath = path.join(entryDir, "snapshot.json");
  let snapshotChecksumOk = true;
  if (fs.existsSync(snapPath)) {
    const raw = fs.readFileSync(snapPath);
    snapshot = JSON.parse(raw.toString("utf8"));
    if (meta.snapshotChecksumSha256) {
      snapshotChecksumOk = sha256(raw) === meta.snapshotChecksumSha256;
    }
  }
  const pdfPath = path.join(entryDir, "report.pdf");
  let pdf = null;
  if (fs.existsSync(pdfPath)) {
    const buffer = fs.readFileSync(pdfPath);
    const sum = sha256(buffer);
    pdf = {
      buffer,
      checksumSha256: sum,
      checksumMatches: !meta.pdfChecksumSha256 || meta.pdfChecksumSha256 === sum,
    };
  }
  return { meta, snapshot, pdf, entryDir, snapshotChecksumOk };
}
