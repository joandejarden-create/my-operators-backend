/**
 * Packet 2.6B-R9 — Canonical Hotel Intelligence Dossier PDF generator.
 * SAME engine for founder Download PDF and automated QA.
 * Playwright/Chromium only — never window.print().
 * Packet 2.6C publishing hotfix: report-specific filenames via shared builder.
 */
import path from "node:path";
import { fileURLToPath } from "node:url";
import { createRequire } from "node:module";
import fs from "node:fs";
import { buildReportDownloadFilename } from "./report-filename.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");
const require = createRequire(import.meta.url);

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

/**
 * @param {object} opts
 * @param {string} opts.baseUrl - Absolute origin, e.g. http://127.0.0.1:8080
 * @param {string} [opts.dossierId]
 * @param {string} [opts.recordId]
 * @param {string} [opts.outPath] - Optional write path
 * @param {string} [opts.filename] - Preferred customer filename (from shared builder)
 * @param {object} [opts.dossier] - Enriched dossier for filename fallback
 * @returns {Promise<{ buffer: Buffer, filename: string, pageCountHint: number|null }>}
 */
export async function generateCanonicalDossierPdf(opts = {}) {
  const baseUrl = String(opts.baseUrl || "").replace(/\/$/, "");
  if (!baseUrl) throw new Error("baseUrl_required");

  const Chrome = loadPrintChrome();
  const headerHtml = Chrome.playwrightHeaderTemplate();
  const idHint = String(opts.dossierId || opts.dossier?.dossier_id || "");
  const isAddendum =
    /^addendum_/i.test(idHint) ||
    String(opts.dossier?.dossier_type || "").toUpperCase() === "RESEARCH_ADDENDUM";
  const footerProduct = isAddendum
    ? "Research Addendum"
    : "Full Hotel Intelligence Investigation";
  const footerHtml = Chrome.playwrightFooterTemplate(footerProduct);
  const margins = Chrome.pdfMargins();

  const { chromium } = await import("playwright");
  const browser = await chromium.launch({ headless: true });
  try {
    const page = await browser.newPage({
      viewport: { width: 1440, height: 900 },
    });

    const qs = new URLSearchParams();
    if (opts.dossierId) qs.set("dossierId", opts.dossierId);
    if (opts.recordId) qs.set("recordId", opts.recordId);
    qs.set("_cb", "r9pdf");
    const renderUrl = `${baseUrl}/hotel-intelligence-dossier-pdf-render.html?${qs.toString()}`;

    await page.goto(renderUrl, { waitUntil: "networkidle", timeout: 180000 });
    await page.waitForSelector("#hid-print-host[data-hid-pdf-ready='1']", {
      timeout: 120000,
    });
    await page.waitForTimeout(600);

    const meta = await page.evaluate(() => {
      const host = document.getElementById("hid-print-host");
      return {
        hotel:
          host?.getAttribute("data-hid-hotel-name") ||
          document.title ||
          "hotel-intelligence-dossier",
        dossierId: host?.getAttribute("data-hid-dossier-id") || "",
        reportType: host?.getAttribute("data-hid-report-type") || "",
        templateId: host?.getAttribute("data-hid-template-id") || "",
        completedAt: host?.getAttribute("data-hid-completed-at") || "",
      };
    });

    const filename =
      opts.filename ||
      buildReportDownloadFilename(
        opts.dossier || {
          hotel_name: meta.hotel,
          dossier_id: meta.dossierId || opts.dossierId,
          dossier_type: meta.reportType || (isAddendum ? "RESEARCH_ADDENDUM" : "FULL_HOTEL_INTELLIGENCE_INVESTIGATION"),
          template_id: meta.templateId || null,
          completed_at: meta.completedAt || opts.dossier?.completed_at || null,
        }
      );

    const buffer = await page.pdf({
      path: opts.outPath || undefined,
      format: "A4",
      printBackground: true,
      preferCSSPageSize: false,
      displayHeaderFooter: true,
      headerTemplate: headerHtml,
      footerTemplate: footerHtml,
      margin: margins,
    });

    return {
      buffer: Buffer.from(buffer),
      filename,
      pageCountHint: null,
      dossierId: meta.dossierId || opts.dossierId || null,
      hotelName: meta.hotel,
    };
  } finally {
    await browser.close();
  }
}

export function getCanonicalPdfChromeConfig() {
  const Chrome = loadPrintChrome();
  return {
    headerTemplate: Chrome.playwrightHeaderTemplate(),
    footerTemplate: Chrome.playwrightFooterTemplate(
      "Full Hotel Intelligence Investigation"
    ),
    margins: Chrome.pdfMargins(),
  };
}
