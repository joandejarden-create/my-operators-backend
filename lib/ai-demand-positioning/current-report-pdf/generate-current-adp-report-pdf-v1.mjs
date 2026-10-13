/**
 * Playwright PDF for the CURRENT published ADP report (not Monthly Review).
 * Doctrine: ADP_ADMIN_PDF_USES_CURRENT_REPORT_PRINT_RENDERER
 *           ADP_PDF_IS_A_PRINT_REPRESENTATION_OF_THE_CURRENT_PUBLISHED_ADP_REPORT
 *
 * Headless render fulfills report APIs from published SoT — no Memberstack,
 * no provider calls, no independent analytical composer.
 */
import path from "node:path";
import { fileURLToPath } from "node:url";
import fs from "node:fs";
import { getPublishedOwnerReport } from "../published-read-service.js";
import { loadPropertyProfile } from "../data-model.js";
import { resolveBrandPortfolioPosition } from "../../../api/ai-demand-positioning.js";
import {
  ADP_ADMIN_PDF_USES_CURRENT_REPORT_PRINT_RENDERER,
  ADP_PDF_IS_A_PRINT_REPRESENTATION_OF_THE_CURRENT_PUBLISHED_ADP_REPORT,
  writeCurrentReportPdf,
  customerPdfFilename,
} from "./current-report-pdf-store-v1.js";

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

async function buildReportApiBody(propertyId) {
  const result = await getPublishedOwnerReport(propertyId);
  if (!result?.ok) {
    throw new Error(result?.error || "published_report_unavailable");
  }
  const profile = loadPropertyProfile(propertyId);
  if (!profile) throw new Error("property_not_found");
  const brandPortfolioPosition = resolveBrandPortfolioPosition(
    propertyId,
    profile,
    { query: {} }
  );
  const { brandPortfolioPosition: _stale, ...corePayload } = result.payload || {};
  return {
    ...corePayload,
    ok: true,
    propertyId,
    property: {
      ...(corePayload.property || {}),
      propertyId,
    },
    brandPortfolioPosition,
    liveProviderCalls: 0,
    _adpPdfRender: true,
    _adpPdfGate: ADP_PDF_IS_A_PRINT_REPRESENTATION_OF_THE_CURRENT_PUBLISHED_ADP_REPORT,
  };
}

/**
 * @param {object} opts
 * @param {string} opts.baseUrl
 * @param {string} opts.propertyId
 * @param {string} [opts.outPath]
 * @param {string} [opts.generatedBy]
 */
export async function generateCurrentAdpReportPdfV1(opts = {}) {
  const baseUrl = String(opts.baseUrl || "").replace(/\/$/, "");
  if (!baseUrl) throw new Error("baseUrl_required");
  const propertyId = String(opts.propertyId || "").trim();
  if (!propertyId) throw new Error("propertyId_required");

  const reportBody = await buildReportApiBody(propertyId);
  const Chrome = loadPrintChrome();
  const headerHtml = Chrome.playwrightHeaderTemplate();
  const footerHtml = Chrome.playwrightFooterTemplate("AI Demand Positioning");
  const margins = Chrome.pdfMargins();

  const { chromium } = await import("playwright");
  const browser = await chromium.launch({ headless: true });
  try {
    const page = await browser.newPage({
      viewport: { width: 1440, height: 900 },
    });

    await page.route("**/api/ai-demand-positioning/**", async (route) => {
      const url = route.request().url();
      if (url.includes("/properties")) {
        await route.fulfill({
          status: 200,
          contentType: "application/json",
          body: JSON.stringify({
            ok: true,
            properties: [
              {
                propertyId,
                name: reportBody.property?.name || propertyId,
                city: reportBody.property?.city || "",
                state: reportBody.property?.state || "",
              },
            ],
          }),
        });
        return;
      }
      if (url.includes("/publication-meta") || url.includes("/publication")) {
        await route.fulfill({
          status: 200,
          contentType: "application/json",
          body: JSON.stringify({
            ok: true,
            product: "ai_demand_positioning",
            bpp: { customerPublished: true },
          }),
        });
        return;
      }
      if (url.includes(`/property/${encodeURIComponent(propertyId)}/report`) ||
          url.includes(`/property/${propertyId}/report`)) {
        await route.fulfill({
          status: 200,
          contentType: "application/json",
          body: JSON.stringify(reportBody),
        });
        return;
      }
      // Evidence / secondary APIs — empty safe stubs (print shows baked payload)
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify({ ok: true, items: [], rows: [] }),
      });
    });

    const qs = new URLSearchParams({
      propertyId,
      pdfRender: "1",
      _cb: "adppdf1",
    });
    const renderUrl = `${baseUrl}/adp-current-report-pdf-render.html?${qs.toString()}`;
    await page.goto(renderUrl, { waitUntil: "networkidle", timeout: 180000 });
    await page.waitForSelector("#adp-pdf-ready-host[data-adp-pdf-ready='1']", {
      timeout: 120000,
      state: "attached",
    });
    await page.waitForFunction(() => {
      const host = document.getElementById("adp-pdf-ready-host");
      return host && host.getAttribute("data-adp-pdf-ready") === "1";
    }, { timeout: 120000 });
    await page.waitForTimeout(800);

    const meta = await page.evaluate(() => {
      const host = document.getElementById("adp-pdf-ready-host");
      return {
        propertyName: host?.getAttribute("data-adp-property-name") || "",
        propertyId: host?.getAttribute("data-adp-property-id") || "",
        periodId: host?.getAttribute("data-adp-period-id") || "",
        actionCount: Number(host?.getAttribute("data-adp-action-count") || 0),
        hasExecutiveRead: host?.getAttribute("data-adp-has-er") === "1",
        hasMonthlyLegacy: !!document.body.innerText?.includes("MONTHLY EXECUTIVE REVIEW"),
      };
    });

    if (meta.hasMonthlyLegacy) {
      throw new Error("legacy_monthly_review_detected_in_current_pdf");
    }

    const filename = customerPdfFilename(propertyId);
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

    let stored = null;
    if (opts.attach !== false) {
      const tmp =
        opts.outPath ||
        path.join(
          ROOT,
          "reports/ai-demand-positioning/current-report-pdf/_tmp",
          `${propertyId}.pdf`
        );
      if (!opts.outPath) {
        fs.mkdirSync(path.dirname(tmp), { recursive: true });
        fs.writeFileSync(tmp, buffer);
      }
      stored = writeCurrentReportPdf(propertyId, opts.outPath || tmp, {
        generatedBy: opts.generatedBy || "generate-current-adp-report-pdf-v1",
        periodId: meta.periodId || reportBody.period?.periodId,
        propertyName: meta.propertyName || reportBody.property?.name,
      });
    }

    return {
      buffer: Buffer.from(buffer),
      filename,
      propertyId,
      propertyName: meta.propertyName || reportBody.property?.name,
      periodId: meta.periodId || reportBody.period?.periodId,
      actionCount: meta.actionCount,
      actions: Array.isArray(reportBody.actions) ? reportBody.actions : [],
      structural: meta,
      stored,
      gate: ADP_ADMIN_PDF_USES_CURRENT_REPORT_PRINT_RENDERER,
      liveProviderCalls: 0,
    };
  } finally {
    await browser.close();
  }
}
