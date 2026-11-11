/**
 * ADP_AI_ADMIN_AUTHORIZED_PDF_DELIVERY_V1
 *
 * Permanent Full E2E gate: stored PDF + manifest is insufficient.
 * Verifies protected route delivery identity and separates DEV vs Memberstack auth.
 */

import fs from "node:fs";
import crypto from "node:crypto";
import {
  resolveFullAdpE2ePdfArtifact,
  ADP_ARTIFACT_TYPE_FULL_ADP_REPORT_PDF,
} from "./adp-full-e2e-pdf-artifact-store-v1.js";
import {
  ADP_EXECUTIVE_PDF_FOUNDER_DESIGN_ACCEPTANCE_V1,
  PDF_FOUNDER_DESIGN_ACCEPTANCE_PENDING,
} from "./adp-executive-pdf-founder-design-acceptance-v1.js";

export const ADP_AI_ADMIN_AUTHORIZED_PDF_DELIVERY_V1 =
  "ADP_AI_ADMIN_AUTHORIZED_PDF_DELIVERY_V1";

export const DEV_PDF_DELIVERY_PASS = "DEV_PDF_DELIVERY_PASS";
export const MEMBERSTACK_AUTHENTICATED_PDF_PASS =
  "MEMBERSTACK_AUTHENTICATED_PDF_PASS";
export const AUTHENTICATED_PDF_QA_PENDING = "AUTHENTICATED_PDF_QA_PENDING";

function shaBuf(buf) {
  return crypto.createHash("sha256").update(buf).digest("hex");
}

/**
 * @param {{
 *   propertyId: string,
 *   expectedStagedVersionId?: string|null,
 *   expectedPdfSha256Prefix?: string|null,
 *   baseUrl?: string,
 *   authMode?: 'none'|'dev_bypass'|'memberstack',
 *   bearerToken?: string|null,
 *   browserOpened?: boolean,
 *   viewerVerified?: boolean,
 * }} opts
 */
export async function assessAiAdminAuthorizedPdfDeliveryV1(opts = {}) {
  const propertyId = String(opts.propertyId || "").trim();
  const failures = [];
  const warnings = [];
  const baseUrl = String(opts.baseUrl || "http://127.0.0.1:8080").replace(
    /\/$/,
    ""
  );
  const artifact = resolveFullAdpE2ePdfArtifact(propertyId, {
    expectedStagedVersionId: opts.expectedStagedVersionId || null,
  });

  if (!artifact.available) {
    failures.push(`ARTIFACT_NOT_FOUND:${artifact.reason || "missing"}`);
  }
  if (artifact.artifactType !== ADP_ARTIFACT_TYPE_FULL_ADP_REPORT_PDF) {
    failures.push("WRONG_ARTIFACT_TYPE");
  }
  if (
    opts.expectedStagedVersionId &&
    artifact.stagedVersionId &&
    artifact.stagedVersionId !== opts.expectedStagedVersionId
  ) {
    failures.push("ARTIFACT_VERSION_MISMATCH");
  }
  if (
    opts.expectedPdfSha256Prefix &&
    artifact.pdfSha256 &&
    !String(artifact.pdfSha256).startsWith(opts.expectedPdfSha256Prefix)
  ) {
    failures.push("PDF_SHA_PREFIX_MISMATCH");
  }

  const url = `${baseUrl}/api/admin/ai-demand-positioning/full-adp-report-pdf/${encodeURIComponent(
    propertyId
  )}${
    opts.expectedStagedVersionId || artifact.stagedVersionId
      ? `?stagedVersionId=${encodeURIComponent(
          opts.expectedStagedVersionId || artifact.stagedVersionId
        )}`
      : ""
  }`;

  const headers = {};
  if (opts.authMode === "memberstack" && opts.bearerToken) {
    headers.Authorization = `Bearer ${opts.bearerToken}`;
  }

  let http = {
    status: null,
    contentType: null,
    disposition: null,
    fingerprint: null,
    staged: null,
    artifactType: null,
    filename: null,
    bytes: null,
    sha256: null,
    classification: null,
    magic: null,
  };

  try {
    const res = await fetch(url, { headers });
    const buf = Buffer.from(await res.arrayBuffer());
    http = {
      status: res.status,
      contentType: res.headers.get("content-type"),
      disposition: res.headers.get("content-disposition"),
      fingerprint: res.headers.get("x-adp-pdf-fingerprint"),
      staged: res.headers.get("x-adp-staged-version-id"),
      artifactType: res.headers.get("x-adp-pdf-artifact-type"),
      filename: res.headers.get("x-adp-pdf-filename"),
      bytes: buf.length,
      sha256: buf.length > 4 ? shaBuf(buf) : null,
      magic: buf.slice(0, 5).toString("utf8"),
      classification: null,
    };
    if (!res.ok) {
      try {
        const j = JSON.parse(buf.toString("utf8"));
        http.classification =
          j.classification ||
          (res.status === 401
            ? "AUTHENTICATION_REQUIRED"
            : res.status === 403
              ? "AUTHORIZATION_DENIED"
              : "ARTIFACT_NOT_FOUND");
        http.error = j.error || null;
      } catch {
        http.classification =
          res.status === 401
            ? "AUTHENTICATION_REQUIRED"
            : res.status === 403
              ? "AUTHORIZATION_DENIED"
              : "HTTP_ERROR";
      }
      failures.push(`HTTP_${res.status}:${http.classification || http.error}`);
    } else {
      if (!String(http.contentType || "").includes("application/pdf")) {
        failures.push("CONTENT_TYPE_NOT_PDF");
      }
      if (http.magic !== "%PDF-") {
        failures.push("PDF_MAGIC_INVALID");
      }
      if (artifact.pdfSha256 && http.sha256 !== artifact.pdfSha256) {
        failures.push("SERVED_SHA_MISMATCH_STORE");
      }
      if (http.fingerprint && artifact.pdfSha256 && http.fingerprint !== artifact.pdfSha256) {
        failures.push("FINGERPRINT_MISMATCH");
      }
      if (
        http.artifactType &&
        http.artifactType !== ADP_ARTIFACT_TYPE_FULL_ADP_REPORT_PDF
      ) {
        failures.push("WRONG_SERVED_ARTIFACT_TYPE");
      }
    }
  } catch (e) {
    failures.push(`FETCH_FAILED:${e.message}`);
  }

  const authMode = opts.authMode || "none";
  let authAcceptance = AUTHENTICATED_PDF_QA_PENDING;
  if (authMode === "memberstack" && failures.length === 0) {
    authAcceptance = MEMBERSTACK_AUTHENTICATED_PDF_PASS;
  } else if (authMode === "dev_bypass" && failures.length === 0) {
    authAcceptance = DEV_PDF_DELIVERY_PASS;
    warnings.push(
      "DEV_AUTH_BYPASS exercised — do not claim MEMBERSTACK_AUTHENTICATED_PDF_PASS"
    );
  } else if (authMode === "none") {
    warnings.push("No auth mode for delivery probe");
  }

  if (opts.browserOpened === false) {
    failures.push("BROWSER_VIEWER_DID_NOT_OPEN");
  }
  if (opts.viewerVerified === false) {
    warnings.push("VIEWER_CONTENT_NOT_VERIFIED");
  }

  const ok = failures.length === 0;
  return {
    ok,
    gate: ADP_AI_ADMIN_AUTHORIZED_PDF_DELIVERY_V1,
    propertyId,
    stagedVersionId: artifact.stagedVersionId || opts.expectedStagedVersionId || null,
    storeSha256: artifact.pdfSha256 || null,
    requestUrl: url,
    http,
    authMode,
    authAcceptance,
    founderDesignAcceptanceGate: ADP_EXECUTIVE_PDF_FOUNDER_DESIGN_ACCEPTANCE_V1,
    founderDesignAcceptanceStatus: PDF_FOUNDER_DESIGN_ACCEPTANCE_PENDING,
    failures,
    warnings,
    blocksCustomerDeliveryCertification: true,
  };
}

export async function ensureAiAdminAuthorizedPdfDeliveryV1(opts = {}) {
  return assessAiAdminAuthorizedPdfDeliveryV1(opts);
}
