/**
 * Immutable local AVA audit-run archive.
 * Layout:
 *   data/.../audit-runs/{propertyId}/{auditRunId}/
 *     input-manifest.json
 *     execution-identity.json
 *     payload.json
 *     certification.json
 *     compare-ready-summary.json
 *     report.pdf (optional)
 *     OUTPUT_MANIFEST.json
 *   index.json — portfolio run index
 */
import fs from "node:fs";
import path from "node:path";
import {
  getAvaAuditRunsRoot,
  sha256File,
  sha256Json,
  AVA_AUDIT_EXECUTION_CONTRACT,
} from "./ava-audit-execution-contract-v1.js";

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

function atomicWriteJson(filePath, obj) {
  ensureDir(path.dirname(filePath));
  const tmp = `${filePath}.${process.pid}.tmp`;
  fs.writeFileSync(tmp, `${JSON.stringify(obj, null, 2)}\n`, "utf8");
  fs.renameSync(tmp, filePath);
}

export function auditRunDir(propertyId, auditRunId, env = process.env) {
  return path.join(getAvaAuditRunsRoot(env), propertyId, auditRunId);
}

export function readAuditRunIndex(env = process.env) {
  const root = getAvaAuditRunsRoot(env);
  const idxPath = path.join(root, "index.json");
  if (!fs.existsSync(idxPath)) {
    return { contract: AVA_AUDIT_EXECUTION_CONTRACT, runs: [] };
  }
  return JSON.parse(fs.readFileSync(idxPath, "utf8"));
}

function writeAuditRunIndex(index, env = process.env) {
  const root = getAvaAuditRunsRoot(env);
  ensureDir(root);
  atomicWriteJson(path.join(root, "index.json"), index);
}

export function listAuditRunsForProperty(propertyId, env = process.env) {
  const index = readAuditRunIndex(env);
  return (index.runs || [])
    .filter((r) => r.propertyId === propertyId)
    .sort((a, b) => String(b.createdAt || "").localeCompare(String(a.createdAt || "")));
}

export function loadAuditRun(auditRunId, env = process.env) {
  const index = readAuditRunIndex(env);
  const hit = (index.runs || []).find((r) => r.auditRunId === auditRunId);
  if (!hit) return { ok: false, error: "AUDIT_RUN_NOT_FOUND", auditRunId };
  const dir = auditRunDir(hit.propertyId, auditRunId, env);
  if (!fs.existsSync(dir)) {
    return { ok: false, error: "AUDIT_RUN_DIR_MISSING", auditRunId, dir };
  }
  const read = (name) => {
    const abs = path.join(dir, name);
    return fs.existsSync(abs) ? JSON.parse(fs.readFileSync(abs, "utf8")) : null;
  };
  return {
    ok: true,
    auditRunId,
    propertyId: hit.propertyId,
    dir,
    indexEntry: hit,
    identity: read("execution-identity.json"),
    inputManifest: read("input-manifest.json"),
    payload: read("payload.json"),
    certification: read("certification.json"),
    outputManifest: read("OUTPUT_MANIFEST.json"),
    summary: read("compare-ready-summary.json"),
    pdfPath: fs.existsSync(path.join(dir, "report.pdf"))
      ? path.join(dir, "report.pdf")
      : null,
  };
}

export function saveImmutableAuditRun({
  identity,
  inputManifest,
  payload,
  certification,
  summary,
  pdfAbsPath = null,
  previousVersionRef = null,
  env = process.env,
}) {
  const propertyId = identity.propertyId;
  const auditRunId = identity.auditRunId;
  const dir = auditRunDir(propertyId, auditRunId, env);
  if (fs.existsSync(dir) && fs.existsSync(path.join(dir, "OUTPUT_MANIFEST.json"))) {
    const err = new Error("AUDIT_RUN_ALREADY_EXISTS");
    err.code = "AUDIT_RUN_ALREADY_EXISTS";
    err.dir = dir;
    throw err;
  }
  ensureDir(dir);

  const contentSha256 = sha256Json(payload);
  identity.contentSha256 = contentSha256;
  identity.previousVersionRef = previousVersionRef || identity.previousVersionRef || null;

  atomicWriteJson(path.join(dir, "execution-identity.json"), identity);
  atomicWriteJson(path.join(dir, "input-manifest.json"), inputManifest);
  atomicWriteJson(path.join(dir, "payload.json"), payload);
  atomicWriteJson(path.join(dir, "certification.json"), certification || {});
  atomicWriteJson(path.join(dir, "compare-ready-summary.json"), summary || {});

  let pdfSha256 = null;
  let pdfRelative = null;
  if (pdfAbsPath && fs.existsSync(pdfAbsPath)) {
    const dest = path.join(dir, "report.pdf");
    fs.copyFileSync(pdfAbsPath, dest);
    pdfSha256 = sha256File(dest);
    pdfRelative = path.relative(getAvaAuditRunsRoot(env), dest).replace(/\\/g, "/");
    identity.pdfSha256 = pdfSha256;
    atomicWriteJson(path.join(dir, "execution-identity.json"), identity);
  }

  const outputManifest = {
    contract: AVA_AUDIT_EXECUTION_CONTRACT,
    auditRunId,
    propertyId,
    createdAt: identity.createdAt,
    contentSha256,
    pdfSha256,
    pdfRelative,
    identityFingerprintSha256: identity.identityFingerprintSha256,
    certificationStatus: identity.certificationStatus,
    executionStatus: identity.executionStatus,
    crossReportStatus: identity.crossReportStatus || null,
    jointPublicationAllowed: identity.jointPublicationAllowed === true,
    files: fs.readdirSync(dir),
  };
  atomicWriteJson(path.join(dir, "OUTPUT_MANIFEST.json"), outputManifest);

  const index = readAuditRunIndex(env);
  // Clear previous "latest" flags for this hotel
  for (const r of index.runs || []) {
    if (r.propertyId === propertyId) r.isLatest = false;
  }
  index.runs = index.runs || [];
  index.runs.push({
    auditRunId,
    propertyId,
    hotelName: identity.hotelName,
    airtableRecordId: identity.airtableRecordId,
    createdAt: identity.createdAt,
    adpPeriodId: identity.adpPeriodId,
    adpPackageId: identity.adpPackageId,
    certificationStatus: identity.certificationStatus,
    executionStatus: identity.executionStatus,
    crossReportStatus: identity.crossReportStatus,
    contentSha256,
    pdfSha256,
    previousVersionRef: identity.previousVersionRef,
    isLatest: true,
    dir: path.relative(getAvaAuditRunsRoot(env), dir).replace(/\\/g, "/"),
  });
  index.updatedAt = new Date().toISOString();
  writeAuditRunIndex(index, env);

  return { dir, outputManifest, identity };
}
