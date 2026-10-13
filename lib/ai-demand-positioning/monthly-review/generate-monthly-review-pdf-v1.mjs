/**
 * Canonical Monthly Executive Review PDF generator (report-family parity).
 * Playwright/Chromium only — same chrome as Full Hotel Intelligence Investigation.
 *
 * Headless PDF must not depend on Airtable Users / Memberstack session.
 * When source=archive (+ reviewId), fulfill the review API from the local
 * archive SoT so presentation artifacts can be generated without live auth.
 * Doctrine: AI_DEMAND_REVIEW_USES_CANONICAL_PUBLISHED_ADP (archive payload).
 */
import path from "node:path";
import { fileURLToPath } from "node:url";
import fs from "node:fs";
import { loadReviewPayload } from "./admin/archive-store-v1.js";
import { buildMonthlyExecutiveReviewV1 } from "./build-monthly-executive-review-v1.js";
import { buildAiDemandPerformanceReviewFilename } from "./ai-demand-performance-review-filename-v1.js";
import {
  AI_DEMAND_PERFORMANCE_REVIEW_CURRENT_TEMPLATE_V1,
  CANONICAL_MONTHLY_REVIEW_CSS,
  CANONICAL_PDF_RENDER_HTML,
  CANONICAL_PDF_RENDERER_JS,
  CANONICAL_REPORT_FAMILY,
  CANONICAL_REPORT_SYSTEM_CSS,
  SINGLE_CANONICAL_AI_DEMAND_PERFORMANCE_REVIEW_RENDERER,
  assertCanonicalPerformanceReviewHtml,
} from "./ai-demand-performance-review-template-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../../..");

function assertCanonicalCssOnDisk() {
  const reportSystem = path.join(ROOT, "public/css/dealality-report-system-v1.css");
  const monthly = path.join(ROOT, "public/css/adp-monthly-review-report-v1.css");
  if (!fs.existsSync(reportSystem)) {
    throw new Error(
      "CSS_NOT_LOADED: missing public/css/dealality-report-system-v1.css (AI_DEMAND_REVIEW_KPI_GRID_COMPONENT_REQUIRED)"
    );
  }
  if (!fs.existsSync(monthly)) {
    throw new Error("CSS_NOT_LOADED: missing public/css/adp-monthly-review-report-v1.css");
  }
  const css = fs.readFileSync(reportSystem, "utf8");
  if (!css.includes(".drs-kpi-band") || !css.includes(".drs-kpi")) {
    throw new Error("CSS_VERSION_MISMATCH: dealality-report-system-v1.css missing KPI grid rules");
  }
}

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
 * Resolve review JSON for headless fulfill (no live provider calls).
 * @param {{ propertyId: string, source?: string, reviewId?: string }} opts
 */
function resolveReviewForPdfRender(opts) {
  const propertyId = String(opts.propertyId || "").trim();
  const source = String(opts.source || "golden").trim();
  const reviewId = String(opts.reviewId || "").trim();

  if (source === "archive" && reviewId) {
    const pack = loadReviewPayload(reviewId);
    if (!pack) {
      throw new Error(`review_not_found:${reviewId}`);
    }
    if (pack.meta?.propertyId && pack.meta.propertyId !== propertyId) {
      throw new Error("review_property_mismatch");
    }
    return {
      ok: true,
      source: "archive",
      reviewId,
      review: pack.review,
      meta: pack.meta,
      liveProviderCalls: 0,
    };
  }

  const review = buildMonthlyExecutiveReviewV1(propertyId);
  return {
    ok: true,
    source: source === "live" ? "live" : "golden",
    review,
    liveProviderCalls: 0,
  };
}

/**
 * @param {object} opts
 * @param {string} opts.baseUrl
 * @param {string} opts.propertyId
 * @param {string} [opts.source]
 * @param {string} [opts.reviewId]
 * @param {string} [opts.outPath]
 * @param {string} [opts.pagesDir] - if set, rasterize each PDF page to PNG
 */
export async function generateMonthlyReviewPdfV1(opts = {}) {
  const baseUrl = String(opts.baseUrl || "").replace(/\/$/, "");
  if (!baseUrl) throw new Error("baseUrl_required");
  const propertyId = String(opts.propertyId || "").trim();
  if (!propertyId) throw new Error("propertyId_required");

  assertCanonicalCssOnDisk();

  const Chrome = loadPrintChrome();
  const headerHtml = Chrome.playwrightHeaderTemplate();
  const footerHtml = Chrome.playwrightFooterTemplate(
    "AI Demand Performance Review"
  );
  const margins = Chrome.pdfMargins();

  const apiBody = resolveReviewForPdfRender({
    propertyId,
    source: opts.source,
    reviewId: opts.reviewId,
  });

  const { chromium } = await import("playwright");
  const browser = await chromium.launch({ headless: true });
  try {
    const page = await browser.newPage({
      viewport: { width: 1440, height: 900 },
    });

    // Bypass Memberstack/Airtable auth for headless PDF — fulfill from archive SoT.
    await page.route("**/api/ai-demand-positioning/monthly-review/**", async (route) => {
      await route.fulfill({
        status: 200,
        contentType: "application/json",
        body: JSON.stringify(apiBody),
      });
    });

    const qs = new URLSearchParams();
    qs.set("propertyId", propertyId);
    qs.set("source", opts.source || "golden");
    if (opts.reviewId) qs.set("reviewId", opts.reviewId);
    qs.set("_cb", "mrpdf1");
    qs.set("template", AI_DEMAND_PERFORMANCE_REVIEW_CURRENT_TEMPLATE_V1);
    const renderUrl = `${baseUrl}${CANONICAL_PDF_RENDER_HTML}?${qs.toString()}`;

    await page.goto(renderUrl, { waitUntil: "networkidle", timeout: 120000 });
    await page.waitForSelector("#adp-mr-pdf-host[data-adp-mr-pdf-ready='1']", {
      timeout: 90000,
    });
    await page.waitForTimeout(500);

    const meta = await page.evaluate(() => {
      const host = document.getElementById("adp-mr-pdf-host");
      const band = host?.querySelector(".drs-kpi-band");
      const bandStyle = band ? getComputedStyle(band) : null;
      const reportSystemHref = [
        ...document.querySelectorAll('link[rel="stylesheet"]'),
      ]
        .map((l) => l.getAttribute("href") || "")
        .find((h) => h.includes("dealality-report-system-v1.css"));
      return {
        propertyName:
          host?.getAttribute("data-adp-mr-property-name") ||
          document.title ||
          "Property",
        propertyId: host?.getAttribute("data-adp-mr-property-id") || "",
        reportFamily: host?.getAttribute("data-adp-mr-report-family") || "",
        templateId: host?.getAttribute("data-adp-mr-template-id") || "",
        renderer: host?.getAttribute("data-adp-mr-renderer") || "",
        html: host?.innerHTML || "",
        hasCover: !!host?.querySelector(".bas-cover-page"),
        hasGeometric: !!host?.querySelector(".bas-cover-geometric"),
        sectionCount: host?.querySelectorAll("[data-adp-mr-section]")?.length || 0,
        legacyKpiCount:
          host?.querySelectorAll(".adp-mr-kpi, .adp-mr-kpi-row")?.length || 0,
        drsKpiCount: host?.querySelectorAll(".drs-kpi")?.length || 0,
        kpiBandDisplay: bandStyle?.display || "",
        kpiBandColumns: bandStyle?.gridTemplateColumns || "",
        reportSystemCssHref: reportSystemHref || "",
        fontFamily: getComputedStyle(host?.querySelector(".drs-report") || host)
          .fontFamily,
      };
    });

    if (!meta.reportSystemCssHref) {
      throw new Error("CSS_NOT_LOADED: dealality-report-system-v1.css link missing in render DOM");
    }
    if (meta.legacyKpiCount > 0) {
      throw new Error(
        "LEGACY_TEMPLATE_SELECTED: PDF DOM contains .adp-mr-kpi markup (forbidden for current reviews)"
      );
    }
    const htmlGate = assertCanonicalPerformanceReviewHtml(meta.html);
    if (!htmlGate.ok) {
      throw new Error(
        `FALLBACK_RENDERER_SELECTED_OR_INVALID_MARKUP: ${htmlGate.errors.join(",")}`
      );
    }
    if (meta.drsKpiCount < 1) {
      throw new Error("AI_DEMAND_REVIEW_KPI_GRID_COMPONENT_REQUIRED: zero .drs-kpi cards");
    }
    if (!/grid/i.test(meta.kpiBandDisplay) && meta.kpiBandDisplay !== "flex") {
      // Soft signal — Chromium may report grid; fail hard only when clearly block/raw.
      if (meta.kpiBandDisplay === "block" || meta.kpiBandDisplay === "") {
        throw new Error(
          `CSS_NOT_LOADED: .drs-kpi-band display=${meta.kpiBandDisplay || "empty"} (expected grid)`
        );
      }
    }

    const reviewDate =
      apiBody.review?.reporting?.currentMonitoringDate ||
      apiBody.meta?.currentMonitoringDate ||
      apiBody.meta?.generatedAt ||
      new Date().toISOString();
    const filename = buildAiDemandPerformanceReviewFilename({
      hotelName: meta.propertyName,
      date: reviewDate,
      reportingMonthKey:
        apiBody.review?.reporting?.reportingMonthKey ||
        apiBody.meta?.reportingMonthKey,
    });

    // Never write Playwright temp names into customer-facing paths.
    const outPath = opts.outPath || undefined;
    if (outPath && /\.tmp$/i.test(outPath)) {
      throw new Error("AI_DEMAND_PDF_TEMP_FILE_NAME_NEVER_USER_VISIBLE");
    }

    const buffer = await page.pdf({
      path: outPath,
      format: "A4",
      printBackground: true,
      preferCSSPageSize: false,
      displayHeaderFooter: true,
      headerTemplate: headerHtml,
      footerTemplate: footerHtml,
      margin: margins,
    });

    let pagePngs = [];
    if (opts.pagesDir) {
      fs.mkdirSync(opts.pagesDir, { recursive: true });
      const sections = [
        "cover",
        "executive",
        "movement",
        "evidence",
        "actions",
        "accountability",
      ];
      for (const key of sections) {
        const el = await page.$(`[data-adp-mr-section="${key}"]`);
        if (!el) continue;
        const outPng = path.join(opts.pagesDir, `${key}.png`);
        await el.screenshot({ path: outPng });
        pagePngs.push(outPng);
      }
    }

    return {
      buffer: Buffer.from(buffer),
      filename,
      propertyId: meta.propertyId || propertyId,
      propertyName: meta.propertyName,
      reportFamily: meta.reportFamily || CANONICAL_REPORT_FAMILY,
      templateId:
        meta.templateId || AI_DEMAND_PERFORMANCE_REVIEW_CURRENT_TEMPLATE_V1,
      renderer:
        meta.renderer || SINGLE_CANONICAL_AI_DEMAND_PERFORMANCE_REVIEW_RENDERER,
      css: {
        reportSystem: CANONICAL_REPORT_SYSTEM_CSS,
        monthlyReview: CANONICAL_MONTHLY_REVIEW_CSS,
        rendererJs: CANONICAL_PDF_RENDERER_JS,
      },
      structural: meta,
      pagePngs,
      margins,
      gate: AI_DEMAND_PERFORMANCE_REVIEW_CURRENT_TEMPLATE_V1,
    };
  } finally {
    await browser.close();
  }
}

export function getMonthlyReviewPdfChromeConfig() {
  const Chrome = loadPrintChrome();
  return {
    headerTemplate: Chrome.playwrightHeaderTemplate(),
    footerTemplate: Chrome.playwrightFooterTemplate(
      "AI Demand Performance Review"
    ),
    margins: Chrome.pdfMargins(),
    format: "A4",
  };
}
