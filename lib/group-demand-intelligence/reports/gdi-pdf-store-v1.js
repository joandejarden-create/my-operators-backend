/**
 * GDI PDF store V1 — filesystem SoT under reports/group-demand-intelligence/pdf/
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { buildGdiPdfFilename } from "./gdi-pdf-report-data-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");
export const GDI_PDF_ROOT = path.join(ROOT, "reports/group-demand-intelligence/pdf");

export function gdiPdfHotelDir(hotelId) {
  return path.join(GDI_PDF_ROOT, String(hotelId || "").trim());
}

export function gdiPdfPaths(hotelId) {
  const dir = gdiPdfHotelDir(hotelId);
  return {
    dir,
    pdfPath: path.join(dir, "report.pdf"),
    metaPath: path.join(dir, "meta.json"),
  };
}

export function hasGdiReportPdf(hotelId) {
  const { pdfPath } = gdiPdfPaths(hotelId);
  return fs.existsSync(pdfPath) && fs.statSync(pdfPath).size > 100;
}

export function writeGdiReportPdf(hotelId, buffer, meta = {}) {
  const { dir, pdfPath, metaPath } = gdiPdfPaths(hotelId);
  fs.mkdirSync(dir, { recursive: true });
  if (/\.tmp$/i.test(pdfPath)) {
    throw new Error("GDI_PDF_TEMP_FILE_NAME_NEVER_USER_VISIBLE");
  }
  fs.writeFileSync(pdfPath, buffer);
  const filename =
    meta.filename ||
    buildGdiPdfFilename(meta.hotelName || hotelId, meta.reportDate);
  const payload = {
    hotelId,
    filename,
    generatedAt: new Date().toISOString(),
    byteLength: buffer.length,
    ...meta,
    filename,
  };
  fs.writeFileSync(metaPath, JSON.stringify(payload, null, 2), "utf8");
  return { pdfPath, metaPath, meta: payload };
}

export function readGdiReportPdf(hotelId) {
  const { pdfPath, metaPath } = gdiPdfPaths(hotelId);
  if (!fs.existsSync(pdfPath)) return null;
  const buffer = fs.readFileSync(pdfPath);
  let meta = {};
  if (fs.existsSync(metaPath)) {
    try {
      meta = JSON.parse(fs.readFileSync(metaPath, "utf8"));
    } catch {
      meta = {};
    }
  }
  return { buffer, meta, pdfPath };
}
