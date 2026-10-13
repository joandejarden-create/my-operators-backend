/**
 * Auto-archive published / client-facing ADP + GDI reports into the
 * immutable Dealality Report Archive ledger.
 *
 * Never overwrites existing archiveIds. Failures are non-fatal to publish/PDF
 * write paths (logged + returned).
 */

import fs from "node:fs";
import {
  listReportArchiveEntries,
  publishReportArchiveEntry,
} from "./report-archive-store-v1.js";

export const PDF_NOT_PREVIOUSLY_ARCHIVED = "PDF_NOT_PREVIOUSLY_ARCHIVED";

function nextVersionLabel(hotelKey, reportType, reportClass, reportDate) {
  const rows = listReportArchiveEntries({
    hotelId: hotelKey,
    reportType,
    reportClass,
  }).filter((e) => String(e.reportDate || "") === String(reportDate || ""));
  const n = rows.length + 1;
  return `v${n}`;
}

/**
 * Archive after ADP current PDF write (preferred — has PDF bytes).
 */
export function autoArchiveAdpCurrentPdf({
  propertyId,
  hotelId = null,
  hotelName = null,
  report = null,
  manifest = null,
  pdfPath = null,
  pdfBuffer = null,
  reportClass = null,
  generatedAt = null,
  codeSha = null,
} = {}) {
  try {
    const pid = String(propertyId || "").trim();
    if (!pid) return { ok: false, reason: "propertyId_required" };
    const period = report?.period || {};
    const periodId =
      period.periodId || manifest?.latestPeriodId || null;
    const status = String(period.status || "").toUpperCase();
    const cls = String(
      reportClass ||
        (status === "OFFICIAL_BASELINE"
          ? "OFFICIAL_BASELINE"
          : status === "BASELINE"
            ? "BASELINE"
            : "CURRENT")
    ).toUpperCase();
    const reportDate = String(
      period.executionDate ||
        period.measurementDate ||
        generatedAt ||
        new Date().toISOString()
    ).slice(0, 10);
    const hotelKey = String(hotelId || report?.property?.censusRecordId || pid).trim();
    const version = nextVersionLabel(hotelKey, "ADP", cls, reportDate);
    const archiveId = `adp_${cls.toLowerCase()}_${reportDate}_${pid}_${version}`;

    const existing = listReportArchiveEntries({ hotelId: hotelKey }).find(
      (e) => e.archiveId === archiveId
    );
    if (existing) {
      return { ok: true, skipped: true, reason: "already_archived", archiveId };
    }

    const snapshot = {
      schema: "adp_report_archive_snapshot_v1",
      propertyId: pid,
      periodId,
      reportDate,
      reportClass: cls,
      report: report || null,
      manifest: manifest
        ? {
            propertyId: manifest.propertyId,
            latestPeriodId: manifest.latestPeriodId,
            latestPublishedAt: manifest.latestPublishedAt,
            publishStatus: manifest.publishStatus,
          }
        : null,
    };

    const meta = publishReportArchiveEntry({
      archiveId,
      hotelId: hotelKey,
      propertyId: pid,
      hotelName: hotelName || report?.property?.name || pid,
      reportType: "ADP",
      reportClass: cls,
      reportDate,
      snapshotDate: reportDate,
      versionLabel: version,
      generatedAt: generatedAt || new Date().toISOString(),
      snapshot,
      pdfPath: pdfPath || null,
      pdfBuffer: Buffer.isBuffer(pdfBuffer) ? pdfBuffer : null,
      pdfMissingReason:
        pdfPath || Buffer.isBuffer(pdfBuffer)
          ? null
          : PDF_NOT_PREVIOUSLY_ARCHIVED,
      methodologyVersion:
        report?.measurementContractVersion ||
        report?.methodologyVersion ||
        "ADP_MEASUREMENT_CONTRACT_V1",
      querySetId: report?.querySetId || report?.controlQuerySetId || null,
      baselineId: report?.pilot?.baselineId || null,
      codeSha: codeSha || process.env.RAILWAY_GIT_COMMIT_SHA || null,
      notes: `Auto-archived ADP ${cls} for ${pid}`,
    });
    return { ok: true, archiveId: meta.archiveId, meta };
  } catch (err) {
    console.error(
      "[report-archive] autoArchiveAdpCurrentPdf failed:",
      err?.message || err
    );
    return { ok: false, reason: String(err?.message || err) };
  }
}

/**
 * Archive after ADP publish (structured snapshot; PDF may arrive later via current PDF write).
 */
export function autoArchiveAdpPublishedSnapshot({
  propertyId,
  hotelId = null,
  hotelName = null,
  report = null,
  manifest = null,
  reportClass = "CURRENT",
} = {}) {
  return autoArchiveAdpCurrentPdf({
    propertyId,
    hotelId,
    hotelName,
    report,
    manifest,
    reportClass,
    pdfPath: null,
    pdfBuffer: null,
  });
}

/**
 * Archive after GDI client-facing PDF persist.
 */
export function autoArchiveGdiReportPdf({
  hotelId,
  hotelName = null,
  propertyId = null,
  reportDate = null,
  reportClass = "CURRENT",
  versionLabel = null,
  pdfBuffer = null,
  pdfPath = null,
  reportSnapshot = null,
  externalShareTokenId = null,
  codeSha = null,
  skipPreview = true,
} = {}) {
  try {
    const hid = String(hotelId || "").trim();
    if (!hid) return { ok: false, reason: "hotelId_required" };
    if (skipPreview && String(reportClass).toUpperCase() === "PREVIEW") {
      return { ok: true, skipped: true, reason: "preview_not_archived" };
    }
    const cls = String(reportClass || "CURRENT").toUpperCase();
    const date = String(reportDate || new Date().toISOString()).slice(0, 10);
    const finalVersion =
      versionLabel || nextVersionLabel(hid, "GDI", cls, date);
    const finalId = `gdi_${cls.toLowerCase()}_${date}_${finalVersion}`;

    const existing = listReportArchiveEntries({ hotelId: hid }).find(
      (e) => e.archiveId === finalId
    );
    if (existing) {
      return { ok: true, skipped: true, reason: "already_archived", archiveId: finalId };
    }

    const snapshot = reportSnapshot || {
      schema: "gdi_report_archive_snapshot_v1",
      hotelId: hid,
      reportDate: date,
    };

    const meta = publishReportArchiveEntry({
      archiveId: finalId,
      hotelId: hid,
      propertyId: propertyId || null,
      hotelName: hotelName || hid,
      reportType: "GDI",
      reportClass: cls,
      reportDate: date,
      snapshotDate: date,
      versionLabel: finalVersion,
      generatedAt: new Date().toISOString(),
      snapshot,
      pdfBuffer: Buffer.isBuffer(pdfBuffer) ? pdfBuffer : null,
      pdfPath: pdfPath || null,
      pdfMissingReason:
        pdfBuffer || pdfPath ? null : PDF_NOT_PREVIOUSLY_ARCHIVED,
      externalShareTokenId: externalShareTokenId || null,
      codeSha: codeSha || process.env.RAILWAY_GIT_COMMIT_SHA || null,
      notes: `Auto-archived GDI ${cls} for ${hid}`,
    });
    return { ok: true, archiveId: meta.archiveId, meta };
  } catch (err) {
    console.error(
      "[report-archive] autoArchiveGdiReportPdf failed:",
      err?.message || err
    );
    return { ok: false, reason: String(err?.message || err) };
  }
}
