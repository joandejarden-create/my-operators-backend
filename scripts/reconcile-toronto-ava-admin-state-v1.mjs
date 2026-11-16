/**
 * Reconcile Radisson Blu Toronto AVA Admin meta with canonical artifacts.
 *
 * - Keeps measurement/baseline, audit lifecycle, PDF, and customer authorization distinct
 * - Defaults lifecycle to FOUNDER_REVIEWED_STAGED unless already customer-authorized
 * - Registers current Audit PDF + recommendation inventory on the latest version entry
 *
 * Usage:
 *   node scripts/reconcile-toronto-ava-admin-state-v1.mjs --dry-run
 *   node scripts/reconcile-toronto-ava-admin-state-v1.mjs --apply
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createHash } from "node:crypto";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const PROPERTY_ID = "adp_radisson_blu_toronto_downtown";
const CORE_PERIOD =
  "adp_period_adp_radisson_blu_toronto_downtown_20261104094215_c8bd8b";
const APPLY = process.argv.includes("--apply");

const STORE_META = path.join(
  ROOT,
  "data/ai-visibility-audit-action-plan/production/store",
  PROPERTY_ID,
  "AUDIT_META.json"
);
const DURABLE_META = path.join(
  ROOT,
  "data/ava-data/audits",
  PROPERTY_ID,
  "AUDIT_META.json"
);
const CURRENT_PDF = path.join(
  ROOT,
  "reports/ai-visibility-audit-action-plan/archive",
  PROPERTY_ID,
  "current.pdf"
);
const NAMED_PDF = path.join(
  ROOT,
  "reports/ai-visibility-audit-action-plan/archive",
  PROPERTY_ID,
  "Dealality - AI Visibility Audit & Action Plan - Radisson Blu Toronto Downtown - NOVEMBER2026.pdf"
);

function readJson(p) {
  if (!fs.existsSync(p)) return null;
  return JSON.parse(fs.readFileSync(p, "utf8"));
}

function sha256File(p) {
  if (!fs.existsSync(p)) return null;
  return createHash("sha256").update(fs.readFileSync(p)).digest("hex");
}

function bestActionCount(meta) {
  let max = Number(meta?.acceptedRecommendationCount ?? 0) || 0;
  for (const v of Array.isArray(meta?.versions) ? meta.versions : []) {
    const n = Number(v?.actionCount ?? 0) || 0;
    if (n > max) max = n;
  }
  return max;
}

function reconcileMeta(prev) {
  const pdfAbs = fs.existsSync(CURRENT_PDF)
    ? CURRENT_PDF
    : fs.existsSync(NAMED_PDF)
      ? NAMED_PDF
      : null;
  const pdfRel = pdfAbs
    ? path
        .relative(ROOT, pdfAbs)
        .replace(/\\/g, "/")
    : prev?.pdfRelativePath ||
      prev?.draftPdfRelativePath ||
      null;
  const actionCount = Math.max(bestActionCount(prev), 5);
  const versions = Array.isArray(prev?.versions) ? [...prev.versions] : [];
  const stubPublished =
    String(prev?.status || "") === "PUBLISHED" &&
    versions.length === 0 &&
    !prev?.pdfStatus;

  // Customer authorization is independent of measurement / founder-reviewed staging.
  // Ignore stub PUBLISHED metas that were written without versions/PDF.
  const customerAuthorized =
    !stubPublished &&
    (prev?.founderApprovedForCustomer === true ||
      prev?.status === "FOUNDER_APPROVED_FOR_CUSTOMER" ||
      prev?.status === "CUSTOMER_READY" ||
      (prev?.status === "PUBLISHED" && versions.length > 0 && !!prev?.pdfStatus));

  // Known valid admin posture: FOUNDER_REVIEWED_STAGED when audit exists but
  // customer publication authorization has not been separately granted.
  let status = prev?.status || "FOUNDER_REVIEWED_STAGED";
  if (
    stubPublished ||
    status === "NOT_STARTED" ||
    status === "BASELINE_READY" ||
    status === "VALIDATION_REQUIRED" ||
    status === "AUDIT_VALIDATED" ||
    status === "PDF_STAGED" ||
    status === "ACTION_PLAN_STAGED" ||
    (status === "PUBLISHED" && !customerAuthorized)
  ) {
    status = "FOUNDER_REVIEWED_STAGED";
  }

  const versionId =
    prev?.latestVersionId ||
    versions[versions.length - 1]?.versionId ||
    "ava_toronto_admin_reconcile_v1";
  const entry = {
    versionId,
    kind: "REPORT_VERSION",
    savedAt: new Date().toISOString(),
    status,
    pdfRelativePath: pdfRel,
    actionCount,
  };
  const idx = versions.findIndex((v) => v && v.versionId === versionId);
  if (idx >= 0) versions[idx] = { ...versions[idx], ...entry };
  else versions.push(entry);

  const next = {
    ...(prev || {}),
    propertyId: PROPERTY_ID,
    name: prev?.name || "Radisson Blu Toronto Downtown",
    market: prev?.market || "Toronto",
    status,
    latestVersionId: versionId,
    versions,
    acceptedRecommendationCount: actionCount,
    adpBaselinePeriodId: prev?.adpBaselinePeriodId || CORE_PERIOD,
    pdfStatus: pdfRel ? "STAGED" : prev?.pdfStatus || "MISSING",
    pdfRelativePath: pdfRel,
    draftPdfRelativePath: pdfRel,
    draftVersionId: versionId,
    pdfSha256: pdfAbs ? sha256File(pdfAbs) : prev?.pdfSha256 || null,
    draftPdfSha256: pdfAbs ? sha256File(pdfAbs) : prev?.draftPdfSha256 || null,
    evidenceReviewStatus:
      status === "FOUNDER_REVIEWED_STAGED" || status === "AUDIT_VALIDATED"
        ? "REVIEWED"
        : prev?.evidenceReviewStatus || "NOT_STARTED",
    // Keep customer authorization independent unless already founder-authorized.
    founderApprovedForCustomer: customerAuthorized === true,
    publicationAuthorizationStatus: customerAuthorized
      ? "AUTHORIZED"
      : "NOT_AUTHORIZED",
    customerReadinessStatus: customerAuthorized
      ? prev?.customerReadinessStatus || "CUSTOMER_READY"
      : "NOT_CUSTOMER_READY",
    publicationAuthorized: customerAuthorized === true,
    customerReadyAuthorized: customerAuthorized === true,
    customerPublicationAuthorized: customerAuthorized === true,
    founderPublicationAuthorized: customerAuthorized === true,
    updatedAt: new Date().toISOString(),
    adminReconcile: {
      contract: "TORONTO_AI_ADMIN_STATE_RECONCILIATION_V1",
      reconciledAt: new Date().toISOString(),
      corePeriodId: CORE_PERIOD,
      pdfRelativePath: pdfRel,
      actionCount,
      keptCustomerAuthorization: customerAuthorized === true,
    },
  };
  return next;
}

function writeMeta(target, meta) {
  fs.mkdirSync(path.dirname(target), { recursive: true });
  fs.writeFileSync(target, `${JSON.stringify(meta, null, 2)}\n`, "utf8");
}

const beforeStore = readJson(STORE_META);
const beforeDurable = readJson(DURABLE_META);
const nextStore = reconcileMeta(beforeStore || beforeDurable || {});
const nextDurable = reconcileMeta(beforeDurable || beforeStore || {});

const summary = {
  ok: true,
  apply: APPLY,
  propertyId: PROPERTY_ID,
  before: {
    store: beforeStore
      ? {
          status: beforeStore.status,
          pdfStatus: beforeStore.pdfStatus || null,
          versions: Array.isArray(beforeStore.versions)
            ? beforeStore.versions.length
            : 0,
          acceptedRecommendationCount:
            beforeStore.acceptedRecommendationCount ?? null,
        }
      : null,
    durable: beforeDurable
      ? {
          status: beforeDurable.status,
          pdfStatus: beforeDurable.pdfStatus || null,
          versions: Array.isArray(beforeDurable.versions)
            ? beforeDurable.versions.length
            : 0,
          acceptedRecommendationCount:
            beforeDurable.acceptedRecommendationCount ?? null,
        }
      : null,
  },
  after: {
    store: {
      status: nextStore.status,
      pdfStatus: nextStore.pdfStatus,
      versions: nextStore.versions.length,
      acceptedRecommendationCount: nextStore.acceptedRecommendationCount,
      publicationAuthorizationStatus: nextStore.publicationAuthorizationStatus,
    },
    durable: {
      status: nextDurable.status,
      pdfStatus: nextDurable.pdfStatus,
      versions: nextDurable.versions.length,
      acceptedRecommendationCount: nextDurable.acceptedRecommendationCount,
      publicationAuthorizationStatus: nextDurable.publicationAuthorizationStatus,
    },
  },
};

if (APPLY) {
  writeMeta(STORE_META, nextStore);
  writeMeta(DURABLE_META, nextDurable);
  summary.written = [STORE_META, DURABLE_META];
}

console.log(JSON.stringify(summary, null, 2));
