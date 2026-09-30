/**
 * Admin GDI PDF report API — separate from ADP monthly reviews.
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

function requireAdmin(req, res) {
  // Reuse support admin pattern already applied by route registration.
  if (req.gdiAdminOk === false) {
    res.status(403).json({ ok: false, error: "forbidden" });
    return false;
  }
  return true;
}

export async function getAdminGdiReportCatalog(req, res) {
  try {
    const hotels = listGdiSelectableHotels?.() || [];
    const rows = [];
    for (const h of hotels) {
      const hotelId = h.hotelId || h.id;
      if (!hotelId) continue;
      let summary = null;
      try {
        const data = await getGdiPdfReportData(hotelId);
        summary = data?.available
          ? {
              available: true,
              ready: data.executiveSummary?.customerReadyCount ?? 0,
              watch: data.executiveSummary?.futureWatchCount ?? 0,
              actionSet: data.executiveSummary?.actionSetCount ?? 0,
              hotelName: data.hotel?.name,
            }
          : { available: false, reason: data?.reason || "unavailable", hotelName: h.displayName || hotelId };
      } catch (err) {
        summary = { available: false, reason: String(err?.message || err).slice(0, 120) };
      }
      rows.push({
        hotelId,
        displayName: h.displayName || summary.hotelName || hotelId,
        pdfReady: hasGdiReportPdf(hotelId),
        ...summary,
      });
    }
    res.json({ ok: true, hotels: rows });
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
