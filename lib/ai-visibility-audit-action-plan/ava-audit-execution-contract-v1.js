/**
 * AVA_AUDIT_EXECUTION_CONTRACT_V1
 *
 * Versioned contract for permanent, on-demand AVA audit execution.
 * Reuses certified AVA V4 Full E2E + ADP↔AVA permanent cross-report certification.
 * Does not redefine ADP measurement methodology.
 */
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { AVA_FULL_E2E_PIPELINE_CONTRACT } from "./ava-full-e2e-pipeline-v1.js";
import { ADP_AVA_PERMANENT_CROSS_REPORT_CERTIFICATION } from "./adp-ava-permanent-cross-report-certification-v1.js";
import { ADP_AVA_REFERENCE_CONTRACT } from "./adp-ava-reference-contract-v1.js";
import { AVA_CUSTOMER_REPORT_QUALITY_STANDARD_V1 } from "./customer-report-quality-standard-v1.js";

export const AVA_AUDIT_EXECUTION_CONTRACT = "AVA_AUDIT_EXECUTION_CONTRACT_V1";
export const AVA_AUDIT_EXECUTION_CONTRACT_VERSION = "1.0.0";

export const AVA_AUDIT_RUN_STATUSES = Object.freeze({
  PENDING: "PENDING",
  RUNNING: "RUNNING",
  CERTIFIED: "AUDIT_CERTIFIED",
  FAILED_CERTIFICATION: "FAILED_CERTIFICATION",
  ARCHIVE_COMPLETE: "ARCHIVE_COMPLETE",
  ARCHIVE_PENDING_AIRTABLE: "ARCHIVE_PENDING_AIRTABLE",
  REPRODUCED: "REPRODUCED",
  DRY_RUN: "DRY_RUN",
});

export const AVA_AUDIT_CERTIFICATION_STATES = Object.freeze({
  AUDIT_CERTIFIED: "AUDIT_CERTIFIED",
  ARCHIVE_COMPLETE: "ARCHIVE_COMPLETE",
  FOUNDER_APPROVED: "FOUNDER_APPROVED",
  CUSTOMER_PUBLISHED: "CUSTOMER_PUBLISHED",
  SUPERSEDED: "SUPERSEDED",
  FAILED_CERTIFICATION: "FAILED_CERTIFICATION",
  DRAFT: "DRAFT",
  HISTORICAL_REFERENCE: "HISTORICAL_REFERENCE",
});

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../..");

export function sha256Text(text) {
  return crypto.createHash("sha256").update(String(text ?? ""), "utf8").digest("hex");
}

export function sha256File(abs) {
  return crypto.createHash("sha256").update(fs.readFileSync(abs)).digest("hex");
}

export function sha256Json(value) {
  return sha256Text(stableStringify(value));
}

export function stableStringify(value) {
  if (value == null || typeof value !== "object") return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(stableStringify).join(",")}]`;
  const keys = Object.keys(value).sort();
  return `{${keys.map((k) => `${JSON.stringify(k)}:${stableStringify(value[k])}`).join(",")}}`;
}

export function newAuditRunId(propertyId, when = new Date()) {
  const ts = when.toISOString().replace(/[-:]/g, "").replace(/\.\d+Z$/, "Z");
  const short = sha256Text(`${propertyId}|${ts}|${Math.random()}`).slice(0, 8);
  return `AVA-RUN-${String(propertyId || "hotel").replace(/^adp_/, "").toUpperCase().slice(0, 24)}-${ts}-${short}`;
}

export function resolveGitRevision(cwd = ROOT) {
  try {
    const head = path.join(cwd, ".git", "HEAD");
    if (!fs.existsSync(head)) return null;
    const raw = fs.readFileSync(head, "utf8").trim();
    if (raw.startsWith("ref:")) {
      const ref = raw.slice(4).trim();
      const refPath = path.join(cwd, ".git", ref);
      if (fs.existsSync(refPath)) return fs.readFileSync(refPath, "utf8").trim().slice(0, 40);
      return ref;
    }
    return raw.slice(0, 40);
  } catch {
    return null;
  }
}

/**
 * Build the immutable execution identity recorded for every run.
 */
export function buildAuditExecutionIdentity({
  propertyId,
  hotelAirtableRecordId = null,
  hotelName = null,
  auditRunId,
  auditDate = new Date().toISOString(),
  reportingPeriodLabel = null,
  adpPackageId = null,
  adpPeriodId = null,
  adpMethodologyVersion = null,
  adpCertificationFingerprint = null,
  avaMethodologyVersion = AVA_FULL_E2E_PIPELINE_CONTRACT,
  avaTemplateVersion = "AI_VISIBILITY_AUDIT_PDF_TEMPLATE_V4",
  canonicalActionRegisterVersion = "AVA_CANONICAL_ACTION_REGISTER_V1",
  sourceDataSnapshotRefs = null,
  researchProvenance = null,
  codeRevision = null,
  executionStatus = AVA_AUDIT_RUN_STATUSES.PENDING,
  certificationStatus = AVA_AUDIT_CERTIFICATION_STATES.DRAFT,
  pdfSha256 = null,
  contentSha256 = null,
  previousVersionRef = null,
  crossReportStatus = null,
  jointPublicationAllowed = false,
} = {}) {
  const createdAt = new Date().toISOString();
  const identity = {
    contract: AVA_AUDIT_EXECUTION_CONTRACT,
    contractVersion: AVA_AUDIT_EXECUTION_CONTRACT_VERSION,
    hotelAirtableRecordId: hotelAirtableRecordId || null,
    airtableRecordId: hotelAirtableRecordId || null,
    propertyId: propertyId || null,
    hotelName: hotelName || null,
    auditRunId: auditRunId || null,
    auditDate,
    reportingPeriodLabel: reportingPeriodLabel || null,
    avaMethodologyVersion,
    avaTemplateVersion,
    canonicalActionRegisterVersion,
    customerReportQualityStandard: AVA_CUSTOMER_REPORT_QUALITY_STANDARD_V1,
    adpReferenceContract: ADP_AVA_REFERENCE_CONTRACT,
    adpCrossReportContract: ADP_AVA_PERMANENT_CROSS_REPORT_CERTIFICATION,
    adpPackageId,
    adpPeriodId,
    adpMethodologyVersion,
    adpCertificationFingerprint,
    sourceDataSnapshotRefs: sourceDataSnapshotRefs || [],
    researchProvenance: researchProvenance || {
      paidResearchAuthorized: false,
      mode: "CERTIFIED_PAYLOAD_REUSE_OR_LOCAL_PIPELINE",
    },
    codeRevision: codeRevision || resolveGitRevision(),
    executionStatus,
    certificationStatus,
    crossReportStatus,
    jointPublicationAllowed: jointPublicationAllowed === true,
    pdfSha256,
    contentSha256,
    previousVersionRef,
    createdAt,
  };
  identity.identityFingerprintSha256 = sha256Json({
    propertyId: identity.propertyId,
    auditRunId: identity.auditRunId,
    adpPeriodId: identity.adpPeriodId,
    adpPackageId: identity.adpPackageId,
    adpCertificationFingerprint: identity.adpCertificationFingerprint,
    avaMethodologyVersion: identity.avaMethodologyVersion,
    avaTemplateVersion: identity.avaTemplateVersion,
    codeRevision: identity.codeRevision,
  });
  return identity;
}

export function getAvaAuditRunsRoot(env = process.env) {
  if (env.AVA_AUDIT_RUNS_ROOT) return path.resolve(env.AVA_AUDIT_RUNS_ROOT);
  return path.join(ROOT, "data/ai-visibility-audit-action-plan/audit-runs");
}

export function getAvaAirtableMirrorRoot(env = process.env) {
  if (env.AVA_AIRTABLE_MIRROR_ROOT) return path.resolve(env.AVA_AIRTABLE_MIRROR_ROOT);
  return path.join(ROOT, "data/ai-visibility-audit-action-plan/airtable-history-mirror");
}
