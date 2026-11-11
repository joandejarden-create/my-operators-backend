/**
 * ADP_FULL_E2E_PDF_ARTIFACT_STORE_V1
 *
 * Immutable Full ADP E2E customer report PDFs, keyed by propertyId + stagedVersionId.
 * Distinct from Monthly Executive Review / Performance Review archive PDFs.
 */

import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import {
  loadStagedManifest,
  loadStagedOwnerPayload,
} from "../review/adp-staged-report-store-v1.js";

export const ADP_FULL_E2E_PDF_ARTIFACT_STORE_V1 =
  "ADP_FULL_E2E_PDF_ARTIFACT_STORE_V1";
export const ADP_ARTIFACT_TYPE_FULL_ADP_REPORT_PDF = "FULL_ADP_REPORT_PDF";
export const ADP_ARTIFACT_TYPE_MONTHLY_EXECUTIVE_REVIEW_PDF =
  "MONTHLY_EXECUTIVE_REVIEW_PDF";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

function resolveFullAdpE2ePdfRoot(env = process.env) {
  const override = String(env.ADP_FULL_E2E_PDF_ROOT || "").trim();
  if (override) return path.resolve(override);
  const durableRoot = String(
    env.ADP_DURABLE_PUBLISHED_ROOT || ""
  ).trim();
  const mode = String(env.ADP_DURABLE_PUBLISHED_MODE || "")
    .trim()
    .toLowerCase();
  if (
    durableRoot &&
    (mode === "prefer" || mode === "required" || mode === "dual")
  ) {
    const durablePdf = path.join(durableRoot, "full-adp-e2e-pdf");
    if (fs.existsSync(durablePdf)) return durablePdf;
  }
  return path.join(ROOT, "data/ai-demand-positioning/full-adp-e2e-pdf");
}

/** Resolved at call-time so durable prefer can serve PDFs without ephemeral image trees. */
export function getFullAdpE2ePdfRoot(env = process.env) {
  return resolveFullAdpE2ePdfRoot(env);
}

/** @deprecated Prefer getFullAdpE2ePdfRoot() — kept for existing imports. */
export const FULL_ADP_E2E_PDF_ROOT = path.join(
  ROOT,
  "data/ai-demand-positioning/full-adp-e2e-pdf"
);

export function fullAdpE2ePdfPaths(propertyId, stagedVersionId, env = process.env) {
  const id = String(propertyId || "").trim();
  const ver = String(stagedVersionId || "latest").trim();
  const root = resolveFullAdpE2ePdfRoot(env);
  const dir = path.join(root, id, ver);
  return {
    dir,
    reportPdf: path.join(dir, "report.pdf"),
    metaJson: path.join(dir, "meta.json"),
    latestPointer: path.join(root, id, "LATEST.json"),
  };
}

export function sha256File(filePath) {
  return crypto.createHash("sha256").update(fs.readFileSync(filePath)).digest("hex");
}

export function loadFullAdpE2ePdfMeta(propertyId, stagedVersionId = null) {
  const id = String(propertyId || "").trim();
  if (!id) return null;
  let ver = stagedVersionId;
  if (!ver) {
    const pointer = path.join(resolveFullAdpE2ePdfRoot(), id, "LATEST.json");
    if (fs.existsSync(pointer)) {
      try {
        ver = JSON.parse(fs.readFileSync(pointer, "utf8")).stagedVersionId;
      } catch {
        ver = null;
      }
    }
  }
  if (!ver) {
    const staged = loadStagedOwnerPayload(id);
    ver = staged?.versionId || loadStagedManifest(id)?.latestStagedVersionId || null;
  }
  if (!ver) return null;
  const { metaJson, reportPdf } = fullAdpE2ePdfPaths(id, ver);
  if (!fs.existsSync(metaJson) || !fs.existsSync(reportPdf)) return null;
  try {
    return JSON.parse(fs.readFileSync(metaJson, "utf8"));
  } catch {
    return null;
  }
}

export function resolveFullAdpE2ePdfArtifact(propertyId, opts = {}) {
  const id = String(propertyId || "").trim();
  const expected = opts.expectedStagedVersionId || null;
  const staged = loadStagedOwnerPayload(id);
  const stagedVersionId =
    expected ||
    staged?.versionId ||
    loadStagedManifest(id)?.latestStagedVersionId ||
    null;
  if (!stagedVersionId) {
    return {
      ok: false,
      available: false,
      artifactType: ADP_ARTIFACT_TYPE_FULL_ADP_REPORT_PDF,
      reason: "staged_version_missing",
      propertyId: id,
    };
  }
  const paths = fullAdpE2ePdfPaths(id, stagedVersionId);
  if (!fs.existsSync(paths.reportPdf) || !fs.existsSync(paths.metaJson)) {
    return {
      ok: false,
      available: false,
      artifactType: ADP_ARTIFACT_TYPE_FULL_ADP_REPORT_PDF,
      reason: "full_adp_pdf_missing",
      propertyId: id,
      stagedVersionId,
      paths,
    };
  }
  const meta = JSON.parse(fs.readFileSync(paths.metaJson, "utf8"));
  if (expected && meta.stagedVersionId !== expected) {
    return {
      ok: false,
      available: false,
      artifactType: ADP_ARTIFACT_TYPE_FULL_ADP_REPORT_PDF,
      reason: "staged_version_mismatch",
      propertyId: id,
      stagedVersionId: meta.stagedVersionId,
      expected,
      paths,
      meta,
    };
  }
  const liveSha = sha256File(paths.reportPdf);
  if (meta.pdfSha256 && meta.pdfSha256 !== liveSha) {
    return {
      ok: false,
      available: false,
      artifactType: ADP_ARTIFACT_TYPE_FULL_ADP_REPORT_PDF,
      reason: "pdf_hash_drift",
      propertyId: id,
      stagedVersionId,
      expectedSha256: meta.pdfSha256,
      actualSha256: liveSha,
      paths,
      meta,
    };
  }
  return {
    ok: true,
    available: true,
    artifactType: ADP_ARTIFACT_TYPE_FULL_ADP_REPORT_PDF,
    propertyId: id,
    stagedVersionId: meta.stagedVersionId || stagedVersionId,
    periodId: meta.periodId || null,
    pdfAbsolutePath: paths.reportPdf,
    pdfSha256: liveSha,
    byteLength: fs.statSync(paths.reportPdf).size,
    viewUrl: `/api/admin/ai-demand-positioning/full-adp-report-pdf/${encodeURIComponent(id)}?stagedVersionId=${encodeURIComponent(stagedVersionId)}`,
    downloadUrl: `/api/admin/ai-demand-positioning/full-adp-report-pdf/${encodeURIComponent(id)}?stagedVersionId=${encodeURIComponent(stagedVersionId)}&download=1`,
    meta,
    publicationStatus: meta.publicationStatus || "NOT_AUTHORIZED",
  };
}

/**
 * Register immutable Full ADP PDF for a staged version.
 * Never overwrites an existing version directory (immutable).
 */
export function registerFullAdpE2ePdfArtifact({
  propertyId,
  stagedVersionId,
  pdfAbsolutePath,
  periodId = null,
  generatedBy = "adp_full_e2e_pdf_artifact_store_v1",
  sourceNote = null,
  publicationStatus = "NOT_AUTHORIZED",
  replaceWithHistory = false,
  templateId = null,
  pageCount = null,
  renderer = null,
  visualQaStatus = null,
} = {}) {
  const id = String(propertyId || "").trim();
  const ver = String(stagedVersionId || "").trim();
  if (!id || !ver) throw new Error("propertyId_and_stagedVersionId_required");
  if (!pdfAbsolutePath || !fs.existsSync(pdfAbsolutePath)) {
    throw new Error("pdf_source_missing");
  }
  const buf = fs.readFileSync(pdfAbsolutePath);
  if (!buf.length || buf.slice(0, 4).toString("utf8") !== "%PDF") {
    throw new Error("pdf_magic_invalid");
  }
  const paths = fullAdpE2ePdfPaths(id, ver);
  if (fs.existsSync(paths.reportPdf) && fs.existsSync(paths.metaJson)) {
    const existing = JSON.parse(fs.readFileSync(paths.metaJson, "utf8"));
    const liveSha = sha256File(paths.reportPdf);
    const attemptedSha = sha256File(pdfAbsolutePath);
    if (existing.pdfSha256 === liveSha && liveSha === attemptedSha) {
      // Idempotent same-bytes register — refresh LATEST pointer only.
      fs.writeFileSync(
        paths.latestPointer,
        `${JSON.stringify(
          {
            propertyId: id,
            stagedVersionId: ver,
            pdfSha256: liveSha,
            updatedAt: new Date().toISOString(),
          },
          null,
          2
        )}\n`
      );
      return {
        ok: true,
        alreadyRegistered: true,
        ...resolveFullAdpE2ePdfArtifact(id, { expectedStagedVersionId: ver }),
      };
    }
    // Visual/fidelity correction for the same staged version: preserve prior bytes.
    if (replaceWithHistory === true && liveSha !== attemptedSha) {
      const histDir = path.join(paths.dir, "history");
      fs.mkdirSync(histDir, { recursive: true });
      const histPdf = path.join(histDir, `report-${liveSha.slice(0, 16)}.pdf`);
      const histMeta = path.join(histDir, `meta-${liveSha.slice(0, 16)}.json`);
      if (!fs.existsSync(histPdf)) fs.copyFileSync(paths.reportPdf, histPdf);
      if (!fs.existsSync(histMeta)) {
        fs.writeFileSync(histMeta, `${JSON.stringify(existing, null, 2)}\n`);
      }
      fs.copyFileSync(pdfAbsolutePath, paths.reportPdf);
      const pdfSha256 = sha256File(paths.reportPdf);
      const meta = {
        ...existing,
        pdfSha256,
        pdfBytes: fs.statSync(paths.reportPdf).size,
        generatedAt: new Date().toISOString(),
        generatedBy: generatedBy || existing.generatedBy,
        previousPdfSha256: liveSha,
        visualCorrection: true,
        templateId: templateId || existing.templateId || null,
        pageCount: pageCount ?? existing.pageCount ?? null,
        renderer: renderer || existing.renderer || null,
        visualQaStatus: visualQaStatus || existing.visualQaStatus || null,
        sourceNote:
          sourceNote ||
          "Visual/fidelity correction; prior PDF preserved under history/.",
      };
      fs.writeFileSync(paths.metaJson, `${JSON.stringify(meta, null, 2)}\n`);
      fs.writeFileSync(
        paths.latestPointer,
        `${JSON.stringify(
          {
            propertyId: id,
            stagedVersionId: ver,
            pdfSha256,
            updatedAt: meta.generatedAt,
            previousPdfSha256: liveSha,
          },
          null,
          2
        )}\n`
      );
      return {
        ok: true,
        alreadyRegistered: false,
        replacedWithHistory: true,
        previousPdfSha256: liveSha,
        ...resolveFullAdpE2ePdfArtifact(id, { expectedStagedVersionId: ver }),
      };
    }
    throw Object.assign(new Error("full_adp_pdf_version_immutable"), {
      code: "ADP_FULL_E2E_PDF_VERSION_IMMUTABILITY",
      stagedVersionId: ver,
      existingSha256: existing.pdfSha256,
      attemptedSha256: attemptedSha,
    });
  }

  fs.mkdirSync(paths.dir, { recursive: true });
  fs.copyFileSync(pdfAbsolutePath, paths.reportPdf);
  const pdfSha256 = sha256File(paths.reportPdf);
  const staged = loadStagedOwnerPayload(id);
  const meta = {
    contract: ADP_FULL_E2E_PDF_ARTIFACT_STORE_V1,
    artifactType: ADP_ARTIFACT_TYPE_FULL_ADP_REPORT_PDF,
    propertyId: id,
    hotelName: staged?.payload?.property?.name || id,
    stagedVersionId: ver,
    periodId:
      periodId ||
      staged?.periodId ||
      staged?.payload?.period?.periodId ||
      null,
    pdfSha256,
    pdfBytes: fs.statSync(paths.reportPdf).size,
    generatedAt: new Date().toISOString(),
    generatedBy,
    templateId,
    pageCount,
    renderer,
    visualQaStatus,
    sourceNote:
      sourceNote ||
      "Full ADP E2E Bethesda-master executive PDF — not a dashboard print or Monthly Executive Review substitute.",
    publicationStatus,
    published: false,
  };
  fs.writeFileSync(paths.metaJson, `${JSON.stringify(meta, null, 2)}\n`);
  fs.writeFileSync(
    paths.latestPointer,
    `${JSON.stringify(
      {
        propertyId: id,
        stagedVersionId: ver,
        pdfSha256,
        updatedAt: meta.generatedAt,
      },
      null,
      2
    )}\n`
  );
  return {
    ok: true,
    alreadyRegistered: false,
    ...resolveFullAdpE2ePdfArtifact(id, { expectedStagedVersionId: ver }),
  };
}
