/**
 * Generate Group & Demand Intelligence PDF V1 (Playwright / Chromium).
 * Reuses DealalityReportPrintChrome margins + footer.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  getGdiPdfReportData,
  buildGdiPdfFilename,
} from "./gdi-pdf-report-data-v1.js";
import { buildGdiPdfDocumentHtml } from "./gdi-pdf-report-html-v1.js";
import { GDI_PDF_EXTRA_CSS } from "./gdi-pdf-extra-css-v1.js";
import { writeGdiReportPdf } from "./gdi-pdf-store-v1.js";
import { loadDealalityPdfReportCssBundle } from "../../dealality-report-family/dealality-pdf-report-css-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

function loadPrintChrome() {
  const code = fs.readFileSync(
    path.join(ROOT, "public/js/dealality-report-print-chrome.js"),
    "utf8"
  );
  const sandbox = { window: {}, module: { exports: {} }, globalThis: {} };
  const fn = new Function(
    "window",
    "module",
    "globalThis",
    code + "\nreturn window.DealalityReportPrintChrome || module.exports;"
  );
  return fn(sandbox.window, sandbox.module, sandbox.window);
}

function loadCssBundle() {
  return loadDealalityPdfReportCssBundle(GDI_PDF_EXTRA_CSS);
}

/**
 * @param {object} opts
 * @param {string} opts.hotelId
 * @param {string} [opts.outPath]
 * @param {boolean} [opts.persist=true]
 * @param {string} [opts.pagesDir] rasterize pages if set
 * @param {string} [opts.nowDate]
 */
export async function generateGdiReportPdfV1(opts = {}) {
  const hotelId = String(opts.hotelId || "").trim();
  if (!hotelId) throw new Error("hotelId_required");

  const data = await getGdiPdfReportData(hotelId, { nowDate: opts.nowDate });
  if (!data?.available) {
    return {
      ok: false,
      available: false,
      reason: data?.reason || "unavailable",
      hotelId,
    };
  }

  const cssText = loadCssBundle();
  const html = buildGdiPdfDocumentHtml(data, { cssText });
  const Chrome = loadPrintChrome();
  const headerHtml = Chrome.playwrightHeaderTemplate();
  const footerHtml = Chrome.playwrightFooterTemplate("Group & Demand Intelligence");
  const margins = Chrome.pdfMargins();

  const { chromium } = await import("playwright");
  const browser = await chromium.launch({ headless: true });
  let buffer;
  let pageCountHint = null;
  try {
    const page = await browser.newPage({ viewport: { width: 1200, height: 1600 } });
    await page.setContent(html, { waitUntil: "load", timeout: 60000 });
    await page.waitForSelector("#gdi-pdf-host[data-gdi-pdf-ready='1']", {
      timeout: 30000,
    });
    await page.evaluate(async () => {
      const imgs = [...document.images];
      await Promise.all(
        imgs.map(
          (img) =>
            img.complete
              ? Promise.resolve()
              : new Promise((resolve) => {
                  img.addEventListener("load", resolve, { once: true });
                  img.addEventListener("error", resolve, { once: true });
                })
        )
      );
    });
    await page.waitForTimeout(200);

    const gate = await page.evaluate(() => {
      const host = document.getElementById("gdi-pdf-host");
      const text = host?.innerText || "";
      return {
        hotelName: host?.getAttribute("data-gdi-hotel-name") || "",
        reportType: host?.getAttribute("data-gdi-report-type") || "",
        hasCover: !!host?.querySelector("[data-gdi-pdf-section='cover']"),
        hasExecutive: !!host?.querySelector("[data-gdi-pdf-section='executive']"),
        hasMarkdown: /(^|\n)\s*#{1,3}\s|```|\*\*\*/.test(text),
        hasInternalId: /\b(gdi_opp_|gdi_mkt_|rec[A-Za-z0-9]{10,}|eventSeriesId|tbl[A-Za-z0-9]+)\b/.test(
          text
        ),
        textSample: text.slice(0, 200),
      };
    });

    if (!gate.hasCover || !gate.hasExecutive) {
      throw new Error("GDI_PDF_STRUCTURE_INVALID: missing cover or executive section");
    }
    if (gate.hasMarkdown) {
      throw new Error("GDI_PDF_MARKDOWN_LEAK");
    }
    if (gate.hasInternalId) {
      throw new Error("GDI_PDF_INTERNAL_ID_LEAK");
    }

    const filename = buildGdiPdfFilename(
      data.hotel.name,
      data.reportMetadata.reportDate
    );
    const outPath = opts.outPath || undefined;
    if (outPath && /\.tmp$/i.test(outPath)) {
      throw new Error("GDI_PDF_TEMP_FILE_NAME_NEVER_USER_VISIBLE");
    }

    buffer = await page.pdf({
      path: outPath,
      format: "A4",
      printBackground: true,
      preferCSSPageSize: false,
      displayHeaderFooter: true,
      headerTemplate: headerHtml,
      footerTemplate: footerHtml,
      margin: margins,
    });

    if (!Buffer.isBuffer(buffer)) buffer = Buffer.from(buffer);
    if (buffer.length < 500 || buffer.slice(0, 5).toString() !== "%PDF-") {
      throw new Error("GDI_PDF_MAGIC_INVALID");
    }

    // Approximate page count via PDF page object markers
    const asText = buffer.toString("latin1");
    const pageMatches = asText.match(/\/Type\s*\/Page[^s]/g);
    pageCountHint = pageMatches ? pageMatches.length : null;

    if (opts.pagesDir) {
      fs.mkdirSync(opts.pagesDir, { recursive: true });
      // Full-page screenshots via PDF isn't trivial without pdf.js; write HTML snapshot instead
      fs.writeFileSync(
        path.join(opts.pagesDir, "render.html"),
        html,
        "utf8"
      );
      fs.writeFileSync(
        path.join(opts.pagesDir, "gate.json"),
        JSON.stringify({ ...gate, pageCountHint, byteLength: buffer.length }, null, 2),
        "utf8"
      );
    }

    let persisted = null;
    if (opts.persist !== false) {
      // Strip large internal-only blobs from archived snapshot; keep customer contract.
      const reportSnapshot = JSON.parse(JSON.stringify(data));
      if (reportSnapshot.reportMetadata) {
        delete reportSnapshot.reportMetadata.hotelId;
      }
      persisted = writeGdiReportPdf(hotelId, buffer, {
        hotelName: data.hotel.name,
        reportDate: data.reportMetadata.reportDate,
        filename,
        reportVersion: data.reportMetadata.reportVersion,
        reportClass: opts.reportClass || "CURRENT",
        reportType: "GDI",
        pageCountHint,
        executiveSummary: data.executiveSummary,
        reportSnapshot,
        codeSha: opts.codeSha || process.env.RAILWAY_GIT_COMMIT_SHA || null,
      });
    }

    return {
      ok: true,
      available: true,
      hotelId,
      hotelName: data.hotel.name,
      filename,
      buffer,
      byteLength: buffer.length,
      pageCountHint,
      data,
      gate,
      persisted,
    };
  } finally {
    await browser.close();
  }
}
