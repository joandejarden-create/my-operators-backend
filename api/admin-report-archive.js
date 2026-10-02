/**
 * Admin Report Archive API — list / view data / download PDF for immutable published reports.
 */

import {
  listReportArchiveEntries,
  loadReportArchiveEntry,
} from "../lib/dealality-report-archive/report-archive-store-v1.js";

export async function getAdminReportArchiveCatalog(req, res) {
  try {
    const hotelId = String(req.query.hotelId || req.query.propertyId || "").trim() || null;
    const reportType = String(req.query.reportType || "ALL").trim().toUpperCase();
    const reportClass = String(req.query.reportClass || "ALL").trim().toUpperCase();
    const entries = listReportArchiveEntries({
      hotelId,
      reportType,
      reportClass,
    });
    res.json({ ok: true, count: entries.length, entries });
  } catch (err) {
    console.error("[admin-report-archive] catalog", err);
    res.status(500).json({ ok: false, error: "catalog_failed" });
  }
}

export async function getAdminReportArchiveEntry(req, res) {
  try {
    const archiveId = String(req.params.archiveId || "").trim();
    if (!archiveId) return res.status(400).json({ ok: false, error: "archiveId_required" });
    const pack = loadReportArchiveEntry(archiveId);
    if (!pack) return res.status(404).json({ ok: false, error: "not_found" });
    res.json({
      ok: true,
      meta: pack.meta,
      snapshot: pack.snapshot,
      hasPdf: Boolean(pack.pdf),
      snapshotChecksumOk: pack.snapshotChecksumOk,
      pdfChecksumOk: pack.pdf ? pack.pdf.checksumMatches : null,
    });
  } catch (err) {
    console.error("[admin-report-archive] entry", err);
    res.status(500).json({ ok: false, error: "entry_failed" });
  }
}

export async function getAdminReportArchivePdf(req, res) {
  try {
    const archiveId = String(req.params.archiveId || "").trim();
    if (!archiveId) return res.status(400).json({ ok: false, error: "archiveId_required" });
    const pack = loadReportArchiveEntry(archiveId);
    if (!pack?.pdf?.buffer) {
      return res.status(404).json({
        ok: false,
        error: "pdf_missing",
        reason: pack?.meta?.pdfMissingReason || "no_pdf_in_archive",
      });
    }
    if (pack.pdf.buffer.slice(0, 5).toString() !== "%PDF-") {
      return res.status(500).json({ ok: false, error: "pdf_invalid" });
    }
    const download = String(req.query.download || "") === "1";
    const filename = `${pack.meta.hotelName || pack.meta.hotelKey}_${pack.meta.reportType}_${pack.meta.reportDate || "report"}_${pack.meta.version}.pdf`
      .replace(/[^\w.\-]+/g, "_");
    res.setHeader("Content-Type", "application/pdf");
    res.setHeader(
      "Content-Disposition",
      `${download ? "attachment" : "inline"}; filename="${filename}"`
    );
    res.setHeader("X-Archive-Id", pack.meta.archiveId);
    res.setHeader("X-PDF-Checksum", pack.pdf.checksumSha256);
    res.setHeader("Cache-Control", "private, no-store");
    return res.send(pack.pdf.buffer);
  } catch (err) {
    console.error("[admin-report-archive] pdf", err);
    res.status(500).json({ ok: false, error: "pdf_failed" });
  }
}
