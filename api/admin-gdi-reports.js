/**
 * Admin GDI PDF report API — separate from ADP monthly reviews.
 * Catalog returns a row-ready summary payload (counts + per-hotel operating fields).
 */

import {
  getGdiPdfReportData,
  buildGdiPdfFilename,
} from "../lib/group-demand-intelligence/reports/gdi-pdf-report-data-v1.js";
import { generateGdiReportPdfV1 } from "../lib/group-demand-intelligence/reports/generate-gdi-report-pdf-v1.mjs";
import {
  hasGdiReportPdf,
  readGdiReportPdf,
} from "../lib/group-demand-intelligence/reports/gdi-pdf-store-v1.js";
import { listGdiSelectableHotels } from "../lib/group-demand-intelligence/index.js";
import { resolveExternalClientLinks } from "../lib/admin/report-external-client-links-v1.js";

/**
 * Canonical report-status labels for Admin row chips.
 * Derived from existing GDI availability + opportunity counts — not a second state model.
 */
export function deriveGdiReportStatus(summary) {
  if (!summary?.available) {
    const reason = String(summary?.reason || "").toLowerCase();
    if (reason.includes("not configured") || reason.includes("needs build")) {
      return "NEEDS_BUILD";
    }
    return "BLOCKED";
  }
  const ready = Number(summary.ready || 0);
  const watch = Number(summary.watch || 0);
  if (ready > 0) return "READY";
  if (watch > 0) return "WATCH_ONLY";
  return "NO_READY_OPPORTUNITIES";
}

function derivePdfStatus(pdfReady) {
  return pdfReady ? "READY" : "MISSING";
}

export async function getAdminGdiReportCatalog(req, res) {
  try {
    const hotels = listGdiSelectableHotels?.() || [];
    const rows = [];
    for (const h of hotels) {
      const hotelId = h.hotelId || h.id;
      if (!hotelId) continue;
      let summary = {
        available: false,
        reason: "unavailable",
        ready: 0,
        watch: 0,
        actionSet: 0,
        hotelName: h.displayName || hotelId,
        market: h.locationLine || h.city || h.market || null,
      };
      try {
        const data = await getGdiPdfReportData(hotelId);
        if (data?.available) {
          summary = {
            available: true,
            ready: data.executiveSummary?.customerReadyCount ?? 0,
            watch: data.executiveSummary?.futureWatchCount ?? 0,
            actionSet: data.executiveSummary?.actionSetCount ?? 0,
            hotelName: data.hotel?.name || h.displayName || hotelId,
            market: data.hotel?.market || h.locationLine || h.city || null,
          };
        } else {
          summary = {
            available: false,
            reason: data?.reason || "unavailable",
            ready: 0,
            watch: 0,
            actionSet: 0,
            hotelName: h.displayName || hotelId,
            market: h.locationLine || h.city || null,
          };
        }
      } catch (err) {
        summary = {
          available: false,
          reason: String(err?.message || err).slice(0, 120),
          ready: 0,
          watch: 0,
          actionSet: 0,
          hotelName: h.displayName || hotelId,
          market: h.locationLine || h.city || null,
        };
      }

      const pdfReady = hasGdiReportPdf(hotelId);
      let lastGeneratedAt = null;
      if (pdfReady) {
        try {
          const pack = readGdiReportPdf(hotelId);
          lastGeneratedAt = pack?.meta?.generatedAt || null;
        } catch {
          lastGeneratedAt = null;
        }
      }

      // Share availability flags only — no signed URLs in catalog payload.
      let gdiShareAvailable = false;
      let adpShareAvailable = false;
      try {
        const links = resolveExternalClientLinks(hotelId, {
          req,
          createIfMissing: false,
        });
        gdiShareAvailable = !!links?.gdi?.available;
        adpShareAvailable = !!links?.adp?.available;
      } catch {
        gdiShareAvailable = false;
        adpShareAvailable = false;
      }

      const reportStatus = deriveGdiReportStatus(summary);
      const pdfStatus = derivePdfStatus(pdfReady);

      rows.push({
        hotelId,
        displayName: h.displayName || summary.hotelName || hotelId,
        hotelName: summary.hotelName,
        market: summary.market || null,
        available: !!summary.available,
        reason: summary.reason || null,
        ready: summary.ready || 0,
        actionSet: summary.actionSet || 0,
        watch: summary.watch || 0,
        reportStatus,
        pdfReady,
        pdfStatus,
        lastGeneratedAt,
        gdiShareAvailable,
        adpShareAvailable,
      });
    }

    const counts = {
      hotels: rows.length,
      reportReady: rows.filter((r) => r.reportStatus === "READY").length,
      pdfReady: rows.filter((r) => r.pdfReady).length,
      needsPdf: rows.filter((r) => r.available && !r.pdfReady).length,
      blocked: rows.filter(
        (r) => r.reportStatus === "BLOCKED" || r.reportStatus === "NEEDS_BUILD"
      ).length,
      watchOnly: rows.filter((r) => r.reportStatus === "WATCH_ONLY").length,
    };

    res.json({ ok: true, counts, hotels: rows });
  } catch (err) {
    console.error("[admin-gdi-reports] catalog", err);
    res.status(500).json({ ok: false, error: "catalog_failed" });
  }
}

export async function getAdminGdiReportData(req, res) {
  try {
    const hotelId = String(req.params.hotelId || "").trim();
    if (!hotelId) return res.status(400).json({ ok: false, error: "hotelId_required" });
    const data = await getGdiPdfReportData(hotelId);
    if (!data?.available) {
      return res.status(404).json({
        ok: false,
        available: false,
        error: "gdi_report_unavailable",
        reason: data?.reason || "unavailable",
      });
    }
    // Strip any residual internal fields from response used by render
    const safe = { ...data };
    delete safe.hotelId;
    res.json({ ok: true, available: true, data: safe });
  } catch (err) {
    console.error("[admin-gdi-reports] data", err);
    res.status(500).json({ ok: false, error: "data_failed" });
  }
}

export async function postAdminGdiReportGenerate(req, res) {
  try {
    const hotelId = String(req.params.hotelId || "").trim();
    if (!hotelId) return res.status(400).json({ ok: false, error: "hotelId_required" });
    const result = await generateGdiReportPdfV1({ hotelId, persist: true });
    if (!result.ok || !result.available) {
      return res.status(422).json({
        ok: false,
        available: false,
        error: "gdi_report_unavailable",
        reason: result.reason || "unavailable",
      });
    }
    res.json({
      ok: true,
      hotelId,
      hotelName: result.hotelName,
      filename: result.filename,
      byteLength: result.byteLength,
      pageCountHint: result.pageCountHint,
      executiveSummary: result.data?.executiveSummary || null,
      generatedAt: new Date().toISOString(),
    });
  } catch (err) {
    console.error("[admin-gdi-reports] generate", err);
    res.status(500).json({
      ok: false,
      error: "generate_failed",
      message: String(err?.message || err).slice(0, 200),
    });
  }
}

export async function getAdminGdiReportPdf(req, res) {
  try {
    const hotelId = String(req.params.hotelId || "").trim();
    if (!hotelId) return res.status(400).json({ ok: false, error: "hotelId_required" });
    let pack = readGdiReportPdf(hotelId);
    if (!pack && String(req.query.generate || "") === "1") {
      const result = await generateGdiReportPdfV1({ hotelId, persist: true });
      if (!result.ok) {
        return res.status(422).json({
          ok: false,
          error: "gdi_report_unavailable",
          reason: result.reason,
        });
      }
      pack = readGdiReportPdf(hotelId);
    }
    if (!pack?.buffer) {
      return res.status(404).json({ ok: false, error: "pdf_missing" });
    }
    if (pack.buffer.slice(0, 5).toString() !== "%PDF-") {
      return res.status(500).json({ ok: false, error: "pdf_magic_invalid" });
    }
    const filename =
      pack.meta?.filename ||
      buildGdiPdfFilename(pack.meta?.hotelName || hotelId, pack.meta?.reportDate);
    if (/\.tmp$/i.test(filename)) {
      return res.status(500).json({ ok: false, error: "invalid_pdf_filename" });
    }
    const download = String(req.query.download || "") === "1";
    res.setHeader("Content-Type", "application/pdf");
    res.setHeader(
      "Content-Disposition",
      `${download ? "attachment" : "inline"}; filename="${filename.replace(/"/g, "")}"`
    );
    res.setHeader("X-GDI-PDF-Filename", filename);
    res.send(pack.buffer);
  } catch (err) {
    console.error("[admin-gdi-reports] pdf", err);
    res.status(500).json({ ok: false, error: "pdf_failed" });
  }
}
