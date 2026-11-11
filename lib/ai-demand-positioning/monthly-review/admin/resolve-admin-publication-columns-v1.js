/**
 * ADP_ADMIN_PUBLICATION_COLUMNS_AUTHORITY_V1
 *
 * Authoritative helpers for Admin publication dashboard columns.
 * Published period + Consideration come from durable published-current when MODE=prefer|required.
 * Never hardcode statuses. Never claim PDF Ready unless retrievable.
 * Never claim Customer Access Verified without independent verification.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  getPublishedCurrentEntry,
  resolveAuthoritativePublishedCurrent,
} from "../../published-current-registry-v1.js";
import { loadPublishedManifest, loadPublishedReport } from "../../published-snapshot.js";
import {
  resolveFullAdpE2ePdfArtifact,
  fullAdpE2ePdfPaths,
  sha256File,
} from "../../full-adp-e2e-pdf/adp-full-e2e-pdf-artifact-store-v1.js";
import { listPublishedPdfHistoryForAdmin } from "../../full-adp-e2e-pdf/adp-published-pdf-history-v1.js";
import { EXTERNAL_SHARE_ACCESS_STATUS } from "../../share/adp-external-share-access-gate-v1.js";

export const ADP_ADMIN_PUBLICATION_COLUMNS_AUTHORITY_V1 =
  "ADP_ADMIN_PUBLICATION_COLUMNS_AUTHORITY_V1";

export const ADMIN_PDF_STATUS = Object.freeze({
  READY: "READY",
  MISSING: "MISSING",
  INTEGRITY_MISMATCH: "INTEGRITY_MISMATCH",
  VERIFICATION_REQUIRED: "VERIFICATION_REQUIRED",
  STALE: "STALE",
});

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../../..");

const MONTH_NAMES = [
  "January",
  "February",
  "March",
  "April",
  "May",
  "June",
  "July",
  "August",
  "September",
  "October",
  "November",
  "December",
];

/**
 * Measurement month/year from certified measurementPeriodId only.
 * Never use this for Admin "Published" — that must come from latestPublishedAt.
 */
export function monthYearFromPeriodId(periodId) {
  const m = String(periodId || "").match(
    /_(\d{4})(\d{2})(\d{2})\d{6}_[a-f0-9]{6}$/i
  );
  if (!m) return null;
  const monthIdx = Number(m[2]) - 1;
  if (monthIdx < 0 || monthIdx > 11) return null;
  return `${MONTH_NAMES[monthIdx]} ${Number(m[1])}`;
}

/**
 * Publication month/year from an authoritative ISO publication timestamp.
 * Does not accept measurementPeriodId, deploy IDs, or filesystem mtimes.
 */
export function monthYearFromPublicationTimestamp(iso) {
  const raw = String(iso || "").trim();
  if (!raw) return null;
  const d = new Date(raw);
  if (Number.isNaN(d.getTime())) return null;
  return `${MONTH_NAMES[d.getUTCMonth()]} ${d.getUTCFullYear()}`;
}

export function considerationFromReport(report) {
  const rate =
    report?.executiveMetrics?.considerationRate?.rate ??
    report?.payload?.executiveMetrics?.considerationRate?.rate ??
    report?.executiveMetrics?.considerationRate?.percentage ??
    null;
  return rate == null || rate === "" ? null : Number(rate);
}

/**
 * Authoritative published surface for Admin columns.
 * Durable registry wins under prefer|required; report bytes must match fail-closed reads.
 *
 * Distinct timestamps (never conflate):
 *   - publishedAt / publishedMonthYear ← durable latestPublishedAt (publication event)
 *   - measurementPeriodId / measuredMonthYear ← certified measurement period id
 * Do not infer Published from measurementPeriodId, deploy dates, or file mtimes.
 */
export function resolveAuthoritativePublishedColumn(propertyId, env = process.env) {
  const id = String(propertyId || "").trim();
  const authority = resolveAuthoritativePublishedCurrent(id, env);
  const entry = authority.entry || getPublishedCurrentEntry(id, env) || null;

  let manifest = null;
  let report = null;
  let integrityError = null;
  try {
    manifest = loadPublishedManifest(id);
  } catch (err) {
    integrityError = {
      code: err?.code || "PUBLISHED_MANIFEST_LOAD_FAILED",
      message: err?.message || String(err),
    };
  }
  try {
    report = loadPublishedReport(id);
  } catch (err) {
    if (!integrityError) {
      integrityError = {
        code: err?.code || "PUBLISHED_REPORT_LOAD_FAILED",
        message: err?.message || String(err),
      };
    }
  }

  const periodId =
    entry?.periodId ||
    entry?.latestPeriodId ||
    manifest?.latestPeriodId ||
    report?.period?.periodId ||
    null;
  const publishedAt =
    entry?.latestPublishedAt || manifest?.latestPublishedAt || null;
  const publishedMonthYear = monthYearFromPublicationTimestamp(publishedAt);
  const measuredMonthYear = monthYearFromPeriodId(periodId);
  const reportCons = considerationFromReport(report);
  const registryCons =
    entry?.considerationRate == null || entry?.considerationRate === ""
      ? null
      : Number(entry.considerationRate);
  const consideration = Number.isFinite(reportCons)
    ? reportCons
    : Number.isFinite(registryCons)
      ? registryCons
      : null;

  return {
    contract: ADP_ADMIN_PUBLICATION_COLUMNS_AUTHORITY_V1,
    propertyId: id,
    authoritySource: authority.source || null,
    inPublishedUniverse: Boolean(periodId),
    measurementPeriodId: periodId,
    measuredMonthYear,
    // Back-compat alias used by existing coverage callers
    publishedPeriodId: periodId,
    publishedAt,
    publishedMonthYear,
    publicationTimestampMissing: Boolean(periodId) && !publishedAt,
    publishedConsideration: consideration,
    publishedReportSha256:
      entry?.reportSha256 ||
      manifest?.reportSha256 ||
      manifest?.latestReportSha256 ||
      null,
    sourceStagedVersionId:
      entry?.sourceStagedVersionId ||
      manifest?.sourceStagedVersionId ||
      null,
    methodologyFingerprintSha256:
      entry?.methodologyFingerprintSha256 ||
      report?.methodology?.fingerprintSha256 ||
      report?.certification?.methodologyFingerprintSha256 ||
      null,
    publicationManifestSha256: entry?.publicationManifestSha256 || null,
    pdfArtifactSha256: entry?.pdfArtifactSha256 || null,
    integrityError,
    reportLoaded: Boolean(report),
    staticFallbackOnly:
      authority.source === "static_deploy_pack_registry_fallback",
  };
}

function findArtifactMatchingPeriod(propertyId, periodId) {
  const id = String(propertyId || "").trim();
  const want = String(periodId || "").trim();
  if (!id || !want) return null;
  const rootCandidates = [
    path.join(
      String(process.env.ADP_DURABLE_PUBLISHED_ROOT || "").trim() ||
        path.join(ROOT, "data/adp-durable-published"),
      "full-adp-e2e-pdf",
      id
    ),
    path.join(ROOT, "data/ai-demand-positioning/full-adp-e2e-pdf", id),
  ];
  const root = rootCandidates.find((p) => fs.existsSync(p)) || null;
  if (!root) return null;
  for (const ent of fs.readdirSync(root, { withFileTypes: true })) {
    if (!ent.isDirectory() || !ent.name.startsWith("staged_")) continue;
    const { metaJson, reportPdf } = fullAdpE2ePdfPaths(id, ent.name);
    if (!fs.existsSync(metaJson) || !fs.existsSync(reportPdf)) continue;
    let meta;
    try {
      meta = JSON.parse(fs.readFileSync(metaJson, "utf8"));
    } catch {
      continue;
    }
    if (String(meta.periodId || "") !== want) continue;
    const resolved = resolveFullAdpE2ePdfArtifact(id, {
      expectedStagedVersionId: meta.stagedVersionId || ent.name,
    });
    if (resolved.available) return resolved;
  }
  return null;
}

/**
 * Resolve Admin PDF column against the published package.
 * Do not mark Ready from a local sealed PDF that is not registered/retrievable.
 */
export function resolveAdminPublishedPdfColumn({
  propertyId,
  publishedPeriodId = null,
  expectedStagedVersionId = null,
  expectedPdfSha256 = null,
} = {}) {
  const id = String(propertyId || "").trim();
  const periodId = String(publishedPeriodId || "").trim() || null;
  const expectedSha =
    String(expectedPdfSha256 || "").trim().toLowerCase() || null;

  if (!id || !periodId) {
    return {
      status: ADMIN_PDF_STATUS.MISSING,
      label: "Missing",
      kind: "warn",
      available: false,
      reason: "published_period_missing",
      pdfSha256: null,
      stagedVersionId: null,
      viewUrl: null,
      downloadUrl: null,
      previewMatchesDownload: null,
    };
  }

  let historyCurrent = null;
  try {
    const hist = listPublishedPdfHistoryForAdmin(id);
    historyCurrent = (hist.entries || []).find(
      (e) =>
        e.status === "CURRENT" &&
        String(e.periodId || "") === periodId &&
        e.unavailable !== true &&
        e.pdfSha256
    );
  } catch {
    historyCurrent = null;
  }

  let artifact = null;
  if (expectedStagedVersionId) {
    artifact = resolveFullAdpE2ePdfArtifact(id, {
      expectedStagedVersionId,
    });
    if (
      artifact?.available &&
      artifact.periodId &&
      String(artifact.periodId) !== periodId
    ) {
      artifact = {
        ...artifact,
        available: false,
        reason: "artifact_period_mismatch_vs_published",
      };
    }
  }
  if (!artifact?.available) {
    artifact = findArtifactMatchingPeriod(id, periodId);
  }

  const candidate =
    (historyCurrent && {
      available: true,
      pdfSha256: historyCurrent.pdfSha256,
      stagedVersionId:
        historyCurrent.stagedVersionId || expectedStagedVersionId,
      periodId,
      viewUrl: historyCurrent.viewUrl || null,
      downloadUrl: historyCurrent.downloadUrl || null,
      source: historyCurrent.source || "published_pdf_history",
      absolutePath: historyCurrent.absolutePath || null,
    }) ||
    (artifact?.available
      ? {
          available: true,
          pdfSha256: artifact.pdfSha256,
          stagedVersionId: artifact.stagedVersionId,
          periodId: artifact.periodId || periodId,
          viewUrl: artifact.viewUrl || null,
          downloadUrl: artifact.downloadUrl || null,
          source: "full_adp_e2e_pdf_artifact_store",
          absolutePath: artifact.pdfAbsolutePath || null,
        }
      : null);

  if (!candidate?.available) {
    return {
      status: ADMIN_PDF_STATUS.MISSING,
      label: "Missing",
      kind: "warn",
      available: false,
      reason:
        artifact?.reason ||
        "published_pdf_not_registered_or_not_retrievable",
      pdfSha256: null,
      stagedVersionId: expectedStagedVersionId || null,
      viewUrl: null,
      downloadUrl: null,
      previewMatchesDownload: null,
      expectedPdfSha256: expectedSha,
    };
  }

  const liveSha =
    candidate.absolutePath && fs.existsSync(candidate.absolutePath)
      ? sha256File(candidate.absolutePath)
      : candidate.pdfSha256;

  if (expectedSha && liveSha && String(liveSha).toLowerCase() !== expectedSha) {
    return {
      status: ADMIN_PDF_STATUS.INTEGRITY_MISMATCH,
      label: "Integrity mismatch",
      kind: "danger",
      available: false,
      reason: "pdf_sha_mismatch_vs_approved_package",
      pdfSha256: liveSha,
      expectedPdfSha256: expectedSha,
      stagedVersionId: candidate.stagedVersionId || null,
      viewUrl: null,
      downloadUrl: null,
      previewMatchesDownload: false,
    };
  }

  if (!candidate.viewUrl || !candidate.downloadUrl) {
    return {
      status: ADMIN_PDF_STATUS.VERIFICATION_REQUIRED,
      label: "Verification required",
      kind: "warn",
      available: false,
      reason: "pdf_urls_incomplete",
      pdfSha256: liveSha,
      stagedVersionId: candidate.stagedVersionId || null,
      viewUrl: candidate.viewUrl || null,
      downloadUrl: candidate.downloadUrl || null,
      previewMatchesDownload: null,
    };
  }

  return {
    status: ADMIN_PDF_STATUS.READY,
    label: "Ready",
    kind: "ok",
    available: true,
    reason: null,
    pdfSha256: liveSha,
    expectedPdfSha256: expectedSha,
    stagedVersionId: candidate.stagedVersionId || null,
    periodId,
    viewUrl: candidate.viewUrl,
    downloadUrl: candidate.downloadUrl,
    previewMatchesDownload: true,
    source: candidate.source,
  };
}

export function resolveCustomerAccessColumn({
  inPublishedUniverse = false,
  shareProbe = null,
  liveExternalShareVerify = null,
  publicationIntegrity = null,
} = {}) {
  if (!inPublishedUniverse) {
    return {
      status: EXTERNAL_SHARE_ACCESS_STATUS.NOT_YET_PUBLISHED,
      label: "No customer link",
      kind: "muted",
      lastVerifiedAt: null,
      inventoryClass: shareProbe?.inventoryClass || null,
    };
  }

  const lastVerifiedAt =
    liveExternalShareVerify?.verifiedAt ||
    publicationIntegrity?.liveExternalShareVerify?.verifiedAt ||
    shareProbe?.lastVerifiedAt ||
    null;

  const inventoryClass = String(shareProbe?.inventoryClass || "");
  const productionShareExists =
    shareProbe?.hasProductionEligibleShare === true ||
    shareProbe?.available === true ||
    inventoryClass === "SEALED_PRODUCTION_BEARER" ||
    inventoryClass === "PRODUCTION_REGISTRY_WITH_SECRET" ||
    inventoryClass === "PRODUCTION_REGISTRY_SECRET_MISSING";
  const localSecretMissing =
    shareProbe?.localSecretMissing === true ||
    inventoryClass === "PRODUCTION_REGISTRY_SECRET_MISSING" ||
    /share secret not configured|secret not configured/i.test(
      String(shareProbe?.reason || "")
    );
  const tokenPresent = Boolean(
    shareProbe?.tokenId ||
      publicationIntegrity?.externalShareTokenId ||
      productionShareExists
  );

  // Live verify wins — never invent Verified without evidence.
  if (
    liveExternalShareVerify?.ok === true ||
    publicationIntegrity?.customerAccessStatus ===
      EXTERNAL_SHARE_ACCESS_STATUS.EXTERNAL_ACCESS_VERIFIED ||
    publicationIntegrity?.externalShareAccessStatus ===
      EXTERNAL_SHARE_ACCESS_STATUS.EXTERNAL_ACCESS_VERIFIED
  ) {
    return {
      status: EXTERNAL_SHARE_ACCESS_STATUS.EXTERNAL_ACCESS_VERIFIED,
      label: "Verified",
      kind: "ok",
      lastVerifiedAt,
      inventoryClass: inventoryClass || null,
    };
  }

  if (liveExternalShareVerify && liveExternalShareVerify.ok === false) {
    return {
      status: EXTERNAL_SHARE_ACCESS_STATUS.EXTERNAL_SHARE_ACCESS_BLOCKED,
      label: "Error",
      kind: "danger",
      lastVerifiedAt,
      note: liveExternalShareVerify.reason || "external_share_verification_failed",
      inventoryClass: inventoryClass || null,
    };
  }

  // Production share exists (registry or sealed) but not yet live-verified.
  // Missing local secret ≠ missing production token.
  if (productionShareExists || tokenPresent || localSecretMissing) {
    return {
      status: EXTERNAL_SHARE_ACCESS_STATUS.VERIFICATION_PENDING,
      label: "Verification required",
      kind: "warn",
      lastVerifiedAt,
      note: localSecretMissing
        ? "Production share token exists; local share secret missing — not a missing customer link"
        : "Production share token exists; live HTTP verification not yet recorded",
      inventoryClass: inventoryClass || null,
    };
  }

  if (
    inventoryClass === "REGISTRY_TEST_OR_EXPIRED_ONLY" ||
    inventoryClass === "NO_CUSTOMER_LINK" ||
    inventoryClass === "SEALED_BEARER_UNUSABLE"
  ) {
    return {
      status: EXTERNAL_SHARE_ACCESS_STATUS.SHARE_NOT_CREATED,
      label: "No customer link",
      kind: "warn",
      lastVerifiedAt,
      note:
        inventoryClass === "REGISTRY_TEST_OR_EXPIRED_ONLY"
          ? "Only expired/LOCAL_TEST_ONLY/neg-legacy registry rows — no production client share"
          : null,
      inventoryClass,
    };
  }

  return {
    status: EXTERNAL_SHARE_ACCESS_STATUS.SHARE_NOT_CREATED,
    label: "No customer link",
    kind: "warn",
    lastVerifiedAt,
    inventoryClass: inventoryClass || "NO_CUSTOMER_LINK",
  };
}

export function buildAdminActionsAgenda({
  inPublishedUniverse = false,
  publishAction = null,
  pdfAvailable = false,
  pdfReason = null,
  shareAvailable = false,
  pdfViewUrl = null,
  pdfDownloadUrl = null,
} = {}) {
  return {
    viewPublishedReport: {
      enabled: inPublishedUniverse === true,
      reason: inPublishedUniverse ? null : "not_published",
    },
    previewPdf: {
      enabled: pdfAvailable === true,
      reason: pdfAvailable ? null : pdfReason || "pdf_not_ready",
      url: pdfViewUrl || null,
    },
    downloadPdf: {
      enabled: pdfAvailable === true,
      reason: pdfAvailable ? null : pdfReason || "pdf_not_ready",
      url: pdfDownloadUrl || null,
    },
    copyCustomerLink: {
      enabled: shareAvailable === true,
      reason: shareAvailable ? null : "no_customer_share",
    },
    viewHistoricalReports: { enabled: true, reason: null },
    reviewAndPublish: {
      enabled: publishAction?.enabled === true,
      label: publishAction?.label || "Review & Publish",
      reason:
        publishAction?.enabled === true
          ? null
          : publishAction?.blocker || publishAction?.title || null,
    },
  };
}
