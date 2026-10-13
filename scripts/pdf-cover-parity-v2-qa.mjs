#!/usr/bin/env node
/**
 * PDF cover parity v2 — ADP vs GDI page-1 geometry comparison.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { chromium } from "playwright";
import { generateGdiReportPdfV1 } from "../lib/group-demand-intelligence/reports/generate-gdi-report-pdf-v1.mjs";
import { buildMonthlyExecutiveReviewV1 } from "../lib/ai-demand-positioning/monthly-review/build-monthly-executive-review-v1.js";
import { loadDealalityPdfReportCssBundle } from "../lib/dealality-report-family/dealality-pdf-report-css-v1.js";
const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(ROOT, "reports/pdf-cover-parity-v2");
fs.mkdirSync(OUT, { recursive: true });

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

function loadAdpPdfBuilder() {
  const code = fs.readFileSync(
    path.join(ROOT, "public/js/ai-demand-positioning/adp-monthly-review-report-pdf-v1.js"),
    "utf8"
  );
  const sandbox = { window: {}, module: { exports: {} }, globalThis: {} };
  sandbox.globalThis.DealalityReportPrintChrome = loadPrintChrome();
  const fn = new Function(
    "window",
    "module",
    "globalThis",
    code + "\nreturn window.AdpMonthlyReviewReportPdfV1;"
  );
  return fn(sandbox.window, sandbox.module, sandbox.globalThis);
}

async function pdfToPng(pdfPath, pngPath) {
  const buf = fs.readFileSync(pdfPath);
  const b64 = buf.toString("base64");
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({ viewport: { width: 1200, height: 1600 } });
  await page.setContent(
    `<!doctype html><html><body style="margin:0"><canvas id="c"></canvas>
<script type="module">
import * as pdfjs from "https://cdnjs.cloudflare.com/ajax/libs/pdf.js/4.4.168/pdf.min.mjs";
pdfjs.GlobalWorkerOptions.workerSrc = "https://cdnjs.cloudflare.com/ajax/libs/pdf.js/4.4.168/pdf.worker.min.mjs";
const data = Uint8Array.from(atob(${JSON.stringify(b64)}), (c) => c.charCodeAt(0));
window.__doc = await pdfjs.getDocument({ data }).promise;
window.__ready = true;
</script></body></html>`,
    { waitUntil: "networkidle", timeout: 180000 }
  );
  await page.waitForFunction(() => window.__ready === true, { timeout: 180000 });
  await page.evaluate(async () => {
    const pdfPage = await window.__doc.getPage(1);
    const viewport = pdfPage.getViewport({ scale: 2 });
    const canvas = document.getElementById("c");
    canvas.width = viewport.width;
    canvas.height = viewport.height;
    await pdfPage.render({
      canvasContext: canvas.getContext("2d"),
      viewport,
    }).promise;
  });
  await page.locator("#c").screenshot({ path: pngPath });
  await browser.close();
}

async function measureGeometry(pngPath) {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage();
  const b64 = fs.readFileSync(pngPath).toString("base64");
  await page.setContent(`<img id="i" src="data:image/png;base64,${b64}" />`, {
    waitUntil: "load",
  });
  await page.waitForFunction(() => {
    const i = document.getElementById("i");
    return i && i.complete && i.naturalWidth > 0;
  });
  const stats = await page.evaluate(() => {
    const img = document.getElementById("i");
    const c = document.createElement("canvas");
    c.width = img.naturalWidth;
    c.height = img.naturalHeight;
    const ctx = c.getContext("2d");
    ctx.drawImage(img, 0, 0);
    const w = c.width;
    const h = c.height;
    const isDark = (d) => d[0] < 50 && d[1] < 60 && d[2] < 90;
    let firstDark = -1;
    let lastDark = -1;
    for (let y = 0; y < h; y++) {
      const d = ctx.getImageData(Math.floor(w * 0.5), y, 1, 1).data;
      if (isDark(d)) {
        if (firstDark < 0) firstDark = y;
        lastDark = y;
      }
    }
    const midY = Math.floor((firstDark + lastDark) / 2);
    let firstDarkX = -1;
    let lastDarkX = -1;
    for (let x = 0; x < w; x++) {
      const d = ctx.getImageData(x, midY, 1, 1).data;
      if (isDark(d)) {
        if (firstDarkX < 0) firstDarkX = x;
        lastDarkX = x;
      }
    }
    return {
      w,
      h,
      firstDark,
      lastDark,
      panelHeightPx: lastDark - firstDark + 1,
      firstDarkPct: +(firstDark / h).toFixed(4),
      lastDarkPct: +(lastDark / h).toFixed(4),
      panelHeightPct: +((lastDark - firstDark + 1) / h).toFixed(4),
      sideInsetL: firstDarkX,
      sideInsetR: w - 1 - lastDarkX,
      bottomWhiteBandPx: h - 1 - lastDark,
    };
  });
  await browser.close();
  return stats;
}

async function makeOverlay(adpPng, gdiPng, outPath) {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({ viewport: { width: 1400, height: 900 } });
  const adpB64 = fs.readFileSync(adpPng).toString("base64");
  const gdiB64 = fs.readFileSync(gdiPng).toString("base64");
  await page.setContent(
    `<!doctype html><html><body style="margin:0;background:#222;display:flex;gap:12px;padding:12px">
<style>
  .stack { position:relative; width:595px; height:842px; }
  .stack img { position:absolute; inset:0; width:100%; height:100%; object-fit:contain; }
  .gdi { opacity:0.5; }
</style>
<div class="stack"><img src="data:image/png;base64,${adpB64}" alt="adp" /><img class="gdi" src="data:image/png;base64,${gdiB64}" alt="gdi" /></div>
</body></html>`,
    { waitUntil: "load" }
  );
  await page.locator(".stack").screenshot({ path: outPath });
  await browser.close();
}

async function makeSideBySide(adpPng, gdiPng, outPath) {
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({ viewport: { width: 1400, height: 900 } });
  const adpB64 = fs.readFileSync(adpPng).toString("base64");
  const gdiB64 = fs.readFileSync(gdiPng).toString("base64");
  await page.setContent(
    `<!doctype html><html><body style="margin:0;display:flex;gap:12px;padding:12px;background:#eee">
<img src="data:image/png;base64,${adpB64}" style="height:820px" />
<img src="data:image/png;base64,${gdiB64}" style="height:820px" />
</body></html>`,
    { waitUntil: "load" }
  );
  await page.screenshot({ path: outPath, fullPage: true });
  await browser.close();
}

async function generateAdpPdf(outPath) {
  const Chrome = loadPrintChrome();
  const review = buildMonthlyExecutiveReviewV1("adp_bethesda_marriott");
  const AdpPdf = loadAdpPdfBuilder();
  const bodyInner = AdpPdf.buildReportHtml(review);
  const css = loadDealalityPdfReportCssBundle("");
  const html = `<!DOCTYPE html><html><head><meta charset="UTF-8"><style>${css}</style></head>
<body style="margin:0;padding:0;background:#fff">
<div id="adp-mr-pdf-host" class="adp-mr-pdf-host brand-alignment-snapshot" data-adp-mr-pdf-ready="1"
  data-adp-mr-property-name="${review.property?.name || "Property"}"
  data-adp-mr-property-id="adp_bethesda_marriott">
${bodyInner}
</div>
</body></html>`;
  fs.writeFileSync(path.join(OUT, "adp-render.html"), html, "utf8");

  const { chromium: pw } = await import("playwright");
  const browser = await pw.launch({ headless: true });
  const page = await browser.newPage({ viewport: { width: 1200, height: 1600 } });
  await page.setContent(html, { waitUntil: "load", timeout: 60000 });
  await page.waitForSelector("#adp-mr-pdf-host[data-adp-mr-pdf-ready='1']");
  await page.waitForTimeout(300);
  await page.pdf({
    path: outPath,
    format: "A4",
    printBackground: true,
    preferCSSPageSize: false,
    displayHeaderFooter: true,
    headerTemplate: Chrome.playwrightHeaderTemplate(),
    footerTemplate: Chrome.playwrightFooterTemplate("AI Demand Performance Review"),
    margin: Chrome.pdfMargins(),
  });
  await browser.close();
}

const adpPdf = path.join(OUT, "bethesda-adp-reference.pdf");
const gdiPdf = path.join(OUT, "bethesda-gdi-fixed.pdf");

console.log("Generating ADP reference PDF…");
await generateAdpPdf(adpPdf);

console.log("Generating GDI PDF…");
const gdiResult = await generateGdiReportPdfV1({
  hotelId: "recLuxvwwxID7U2B8",
  persist: false,
  nowDate: "2026-10-02",
  outPath: gdiPdf,
});
console.log("GDI ok", gdiResult.ok, gdiResult.byteLength);

const adpPng = path.join(OUT, "adp-page-1.png");
const gdiPng = path.join(OUT, "gdi-page-1.png");
await pdfToPng(adpPdf, adpPng);
await pdfToPng(gdiPdf, gdiPng);

const adpGeo = await measureGeometry(adpPng);
const gdiGeo = await measureGeometry(gdiPng);

const geometry = {
  adp: adpGeo,
  gdi: gdiGeo,
  delta: {
    firstDarkPx: Math.abs(adpGeo.firstDark - gdiGeo.firstDark),
    lastDarkPx: Math.abs(adpGeo.lastDark - gdiGeo.lastDark),
    panelHeightPx: Math.abs(adpGeo.panelHeightPx - gdiGeo.panelHeightPx),
    sideInsetLPx: Math.abs(adpGeo.sideInsetL - gdiGeo.sideInsetL),
    sideInsetRPx: Math.abs(adpGeo.sideInsetR - gdiGeo.sideInsetR),
    bottomWhiteBandPx: Math.abs(adpGeo.bottomWhiteBandPx - gdiGeo.bottomWhiteBandPx),
  },
  pass: {
    pageSize: adpGeo.w === gdiGeo.w && adpGeo.h === gdiGeo.h,
    panelHeight:
      Math.abs(adpGeo.panelHeightPx - gdiGeo.panelHeightPx) <= 2 &&
      Math.abs(adpGeo.firstDark - gdiGeo.firstDark) <= 3 &&
      Math.abs(adpGeo.lastDark - gdiGeo.lastDark) <= 3,
    sideInsets:
      Math.abs(adpGeo.sideInsetL - gdiGeo.sideInsetL) <= 3 &&
      Math.abs(adpGeo.sideInsetR - gdiGeo.sideInsetR) <= 3,
    bottomBand: Math.abs(adpGeo.bottomWhiteBandPx - gdiGeo.bottomWhiteBandPx) <= 3,
  },
};

fs.writeFileSync(path.join(OUT, "COVER_GEOMETRY.json"), JSON.stringify(geometry, null, 2));

await makeSideBySide(adpPng, gdiPng, path.join(OUT, "ADP_GDI_COVER_SIDE_BY_SIDE.png"));
await makeOverlay(adpPng, gdiPng, path.join(OUT, "ADP_GDI_COVER_OVERLAY.png"));

console.log(JSON.stringify(geometry, null, 2));
