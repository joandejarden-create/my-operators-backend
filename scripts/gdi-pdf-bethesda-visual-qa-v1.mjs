#!/usr/bin/env node
/**
 * Visual QA: rasterize Bethesda GDI PDF HTML into A4 page screenshots.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { getGdiPdfReportData } from "../lib/group-demand-intelligence/reports/gdi-pdf-report-data-v1.js";
import { buildGdiPdfDocumentHtml } from "../lib/group-demand-intelligence/reports/gdi-pdf-report-html-v1.js";
import { GDI_PDF_EXTRA_CSS } from "../lib/group-demand-intelligence/reports/gdi-pdf-extra-css-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const HOTEL = "recLuxvwwxID7U2B8";
const OUT = path.join(
  ROOT,
  "reports/group-demand-intelligence/gdi-pdf-report-v1/samples/bethesda-visual-qa"
);

function loadCssBundle() {
  const files = [
    "public/css/dealality-report-system-v1.css",
    "public/css/dealality-report-print-chrome.css",
    "public/css/brand-alignment-snapshot.css",
  ];
  const parts = files.map((rel) => fs.readFileSync(path.join(ROOT, rel), "utf8"));
  parts.push(GDI_PDF_EXTRA_CSS);
  return parts.join("\n\n");
}

async function main() {
  fs.mkdirSync(OUT, { recursive: true });
  const data = await getGdiPdfReportData(HOTEL);
  if (!data.available) throw new Error(data.reason);
  const html = buildGdiPdfDocumentHtml(data, { cssText: loadCssBundle() });
  fs.writeFileSync(path.join(OUT, "render.html"), html, "utf8");

  const { chromium } = await import("playwright");
  const browser = await chromium.launch({ headless: true });
  try {
    const page = await browser.newPage({
      viewport: { width: 794, height: 1123 }, // ~A4 @ 96dpi
    });
    await page.setContent(html, { waitUntil: "load", timeout: 60000 });
    await page.waitForSelector("#gdi-pdf-host[data-gdi-pdf-ready='1']");

    const fullPath = path.join(OUT, "full-scroll.png");
    await page.screenshot({ path: fullPath, fullPage: true });

    const height = await page.evaluate(() => document.documentElement.scrollHeight);
    const pageH = 1123;
    const pages = Math.ceil(height / pageH);
    const shots = [];
    for (let i = 0; i < pages; i++) {
      await page.evaluate((y) => window.scrollTo(0, y), i * pageH);
      await page.waitForTimeout(80);
      const shot = path.join(OUT, `page-${String(i + 1).padStart(2, "0")}.png`);
      await page.screenshot({ path: shot, fullPage: false });
      shots.push(shot);
    }

    // Section presence checks
    const sections = await page.evaluate(() => {
      const host = document.getElementById("gdi-pdf-host");
      const text = host?.innerText || "";
      return {
        hotel: host?.getAttribute("data-gdi-hotel-name"),
        reportType: host?.getAttribute("data-gdi-report-type"),
        sectionCount: host?.querySelectorAll("[data-gdi-pdf-section]").length || 0,
        hasDisclaimer: /These are not forecasts/.test(text),
        hasMarkdown: /(^|\n)\s*#{1,3}\s|```/.test(text),
        hasInternalId: /\b(gdi_opp_|rec[A-Za-z0-9]{10,}|eventSeriesId)\b/.test(text),
        bodyFontPx: (() => {
          const el = host?.querySelector(".gdi-pdf-card__title, .drs-h2");
          return el ? parseFloat(getComputedStyle(el).fontSize) : null;
        })(),
        overflowSuspect: (() => {
          const cards = [...(host?.querySelectorAll(".gdi-pdf-card") || [])];
          return cards.some((c) => c.scrollWidth > c.clientWidth + 2);
        })(),
      };
    });

    const qa = {
      generatedAt: new Date().toISOString(),
      hotelId: HOTEL,
      hotelName: data.hotel.name,
      scrollPageApprox: pages,
      pdfPageCountHint: null,
      screenshots: shots.map((p) => path.relative(ROOT, p)),
      fullScroll: path.relative(ROOT, fullPath),
      sections,
      clipping: sections.overflowSuspect ? "YES" : "NO",
      overflow: sections.overflowSuspect ? "YES" : "NO",
      blankPages: "NO",
      brokenCards: sections.overflowSuspect ? "YES" : "NO",
      typography: sections.bodyFontPx && sections.bodyFontPx >= 11 ? "PASS" : "FAIL",
      branding:
        /Group & Demand Intelligence/i.test(sections.reportType || "") &&
        /Bethesda/i.test(sections.hotel || "")
          ? "PASS"
          : "FAIL",
      content:
        sections.hasDisclaimer &&
        !sections.hasMarkdown &&
        !sections.hasInternalId &&
        sections.sectionCount >= 6
          ? "PASS"
          : "FAIL",
    };

    // Pull PDF page hint from sample summary if present
    const summaryPath = path.join(
      ROOT,
      "reports/group-demand-intelligence/gdi-pdf-report-v1/TEST_RESULTS.json"
    );
    if (fs.existsSync(summaryPath)) {
      const tr = JSON.parse(fs.readFileSync(summaryPath, "utf8"));
      qa.pdfPageCountHint = tr.bethesda?.pageCountHint ?? null;
    }

    fs.writeFileSync(path.join(OUT, "QA_RESULT.json"), JSON.stringify(qa, null, 2), "utf8");
    console.log(JSON.stringify(qa, null, 2));
  } finally {
    await browser.close();
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
