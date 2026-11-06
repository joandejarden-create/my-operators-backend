#!/usr/bin/env node
/**
 * Real PDF page rasterization + takeaway pagination QA for AVA V4 portfolio.
 *
 * Uses Playwright + pdf.js canvas rendering (not HTML section screenshots).
 *
 *   node scripts/ava-v4-final-pdf-visual-qa-v1.mjs
 *   node scripts/ava-v4-final-pdf-visual-qa-v1.mjs --manifest <path> --six-only
 */
import crypto from "node:crypto";
import fs from "node:fs";
import http from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const REPORT = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1"
);
const DEFAULT_MANIFEST = path.join(
  REPORT,
  "AVA_38_HOTEL_V4_LOCAL_REGENERATION_MANIFEST.json"
);
const OUT_DIR = path.join(REPORT, "AVA_V4_FINAL_PDF_VISUAL_QA");

const SIX = [
  /waterstone/i,
  /a coru[nñ]a|ac-hotel-a-coruna/i,
  /castillo|son vida/i,
  /cambridge beaches/i,
  /hilton.*times square|hilton-new-york/i,
  /spice island/i,
];

function arg(flag) {
  const i = process.argv.indexOf(flag);
  return i >= 0 ? process.argv[i + 1] : null;
}

function sha256File(p) {
  return crypto.createHash("sha256").update(fs.readFileSync(p)).digest("hex");
}

function detectTakeawayDefects(pageTexts, closing = "") {
  const defects = [];
  let labelPage = -1;
  for (let i = 0; i < pageTexts.length; i++) {
    if (/Executive\s+Takeaway/i.test(pageTexts[i])) {
      labelPage = i;
      break;
    }
  }
  if (labelPage < 0) {
    defects.push({ type: "MISSING_TAKEAWAY_LABEL", page: null });
    return defects;
  }
  const page = pageTexts[labelPage];
  const m = page.match(/Executive\s+Takeaway/i);
  const after = page.slice((m?.index || 0) + (m?.[0]?.length || 0)).trim();
  if (after.replace(/\s+/g, " ").length < 60) {
    defects.push({ type: "EMPTY_TAKEAWAY_BOX", page: labelPage + 1 });
  }
  const closingNorm = String(closing || "").replace(/\s+/g, " ").trim();
  if (closingNorm.length >= 40) {
    const head = closingNorm.slice(0, 32);
    const tail = closingNorm.slice(-32);
    const onLabel = page.replace(/\s+/g, " ").includes(head);
    for (let i = 0; i < pageTexts.length; i++) {
      if (i === labelPage) continue;
      const p = pageTexts[i].replace(/\s+/g, " ");
      if (p.includes(head) && !onLabel) {
        defects.push({ type: "TAKEAWAY_NARRATIVE_SPLIT", page: i + 1 });
      } else if (onLabel && tail.length >= 20 && p.includes(tail) && !page.replace(/\s+/g, " ").includes(tail)) {
        defects.push({ type: "TAKEAWAY_NARRATIVE_SPLIT", page: i + 1 });
      }
    }
  }
  return defects;
}

async function extractPages(pdfPath) {
  const { getDocument } = await import("pdfjs-dist/legacy/build/pdf.mjs");
  const data = new Uint8Array(fs.readFileSync(pdfPath));
  const doc = await getDocument({ data, useSystemFonts: true }).promise;
  const pageTexts = [];
  for (let i = 1; i <= doc.numPages; i++) {
    const page = await doc.getPage(i);
    const content = await page.getTextContent();
    pageTexts.push(content.items.map((it) => it.str).join(" "));
  }
  return { pageCount: doc.numPages, pageTexts };
}

/**
 * Rasterize every page of a PDF via Playwright + pdf.js (real PDF pages).
 */
async function rasterizeAllPages(pdfPath, outDir) {
  const { chromium } = await import("playwright");
  const pdfjsRoot = path.resolve(ROOT, "node_modules/pdfjs-dist/legacy/build");
  fs.mkdirSync(outDir, { recursive: true });
  const reportCopy = path.join(outDir, "_qa-report.pdf");
  fs.copyFileSync(pdfPath, reportCopy);
  const viewer = path.join(outDir, "_viewer.html");
  fs.writeFileSync(
    viewer,
    `<!doctype html><html><body style="margin:0;background:#fff">
<canvas id="c"></canvas>
<script type="module">
import * as pdfjs from "/pdfjs/pdf.mjs";
pdfjs.GlobalWorkerOptions.workerSrc = "/pdfjs/pdf.worker.mjs";
const res = await fetch("/_qa-report.pdf");
const data = new Uint8Array(await res.arrayBuffer());
const doc = await pdfjs.getDocument({ data }).promise;
window.__doc = doc;
window.__pages = doc.numPages;
window.__ready = true;
</script></body></html>`
  );

  const server = http.createServer((req, res) => {
    const urlPath = decodeURIComponent((req.url || "/").split("?")[0]);
    let filePath;
    if (urlPath.startsWith("/pdfjs/")) {
      filePath = path.join(pdfjsRoot, urlPath.replace("/pdfjs/", ""));
    } else if (urlPath === "/" || urlPath === "/_viewer.html") {
      filePath = viewer;
    } else {
      filePath = path.join(outDir, urlPath.replace(/^\//, ""));
    }
    if (!fs.existsSync(filePath)) {
      res.writeHead(404);
      res.end("missing");
      return;
    }
    const ext = path.extname(filePath);
    const type =
      ext === ".mjs" || ext === ".js"
        ? "text/javascript"
        : ext === ".pdf"
          ? "application/pdf"
          : ext === ".html"
            ? "text/html"
            : "application/octet-stream";
    res.writeHead(200, { "Content-Type": type });
    fs.createReadStream(filePath).pipe(res);
  });
  await new Promise((r) => server.listen(0, "127.0.0.1", r));
  const { port } = server.address();
  const browser = await chromium.launch({ headless: true });
  const written = [];
  try {
    const page = await browser.newPage({ viewport: { width: 920, height: 1300 } });
    await page.goto(`http://127.0.0.1:${port}/_viewer.html`, {
      waitUntil: "networkidle",
      timeout: 120000,
    });
    await page.waitForFunction(() => window.__ready === true, null, {
      timeout: 120000,
    });
    const pageCount = await page.evaluate(() => window.__pages);
    for (let n = 1; n <= pageCount; n++) {
      const png = path.join(outDir, `page-${String(n).padStart(2, "0")}.png`);
      await page.evaluate(async (pageNum) => {
        const doc = window.__doc;
        const pdfPage = await doc.getPage(pageNum);
        const viewport = pdfPage.getViewport({ scale: 1.4 });
        const canvas = document.getElementById("c");
        canvas.width = viewport.width;
        canvas.height = viewport.height;
        const ctx = canvas.getContext("2d");
        ctx.fillStyle = "#fff";
        ctx.fillRect(0, 0, canvas.width, canvas.height);
        await pdfPage.render({ canvasContext: ctx, viewport }).promise;
      }, n);
      await page.locator("#c").screenshot({ path: png });
      written.push(path.relative(ROOT, png).replace(/\\/g, "/"));
    }
    return { pageCount, written };
  } finally {
    await browser.close();
    server.close();
  }
}

/**
 * Heuristic pixel QA: flag pages that are almost blank or have large empty bands
 * near the top after a section heading (empty takeaway box proxy via PNG size +
 * text-layer defects already caught separately).
 */
function flagRasterHeuristics(pngPath, pageText) {
  const flags = [];
  const st = fs.statSync(pngPath);
  // Very small PNG → nearly blank page
  if (st.size < 18000) flags.push("NEAR_EMPTY_RASTER");
  if (/Executive\s+Takeaway/i.test(pageText || "")) {
    const after = (pageText || "").split(/Executive\s+Takeaway/i)[1] || "";
    if (after.replace(/\s+/g, " ").trim().length < 60) {
      flags.push("TAKEAWAY_LABEL_SPARSE_TEXT");
    }
  }
  return flags;
}

async function main() {
  const manifestPath = path.resolve(arg("--manifest") || DEFAULT_MANIFEST);
  const sixOnly = process.argv.includes("--six-only");
  if (!fs.existsSync(manifestPath)) {
    console.error("Manifest missing:", manifestPath);
    process.exit(1);
  }
  const manifest = JSON.parse(fs.readFileSync(manifestPath, "utf8"));
  const hotels = (manifest.hotels || manifest.entries || []).filter((h) => h.pdfPath || h.pdfSha256);
  const selected = sixOnly
    ? hotels.filter((h) =>
        SIX.some((re) => re.test(h.hotelName || "") || re.test(h.file || "") || re.test(h.pdfPath || ""))
      )
    : hotels;

  fs.mkdirSync(OUT_DIR, { recursive: true });
  const results = [];
  let i = 0;
  for (const h of selected) {
    i += 1;
    const pdfAbs = path.isAbsolute(h.pdfPath)
      ? h.pdfPath
      : path.join(ROOT, h.pdfPath);
    const slug =
      String(h.file || h.hotelName || `hotel-${i}`)
        .replace(/-v1\.json$/i, "")
        .replace(/[^a-z0-9]+/gi, "-")
        .toLowerCase() || `hotel-${i}`;
    process.stdout.write(`[${i}/${selected.length}] ${h.hotelName || slug}... `);
    if (!fs.existsSync(pdfAbs)) {
      console.log("MISSING_PDF");
      results.push({
        hotelName: h.hotelName,
        file: h.file,
        error: "MISSING_PDF",
        pdfPath: h.pdfPath,
      });
      continue;
    }
    const hash = sha256File(pdfAbs);
    const hotelOut = path.join(OUT_DIR, slug);
    fs.mkdirSync(hotelOut, { recursive: true });
    try {
      const { pageCount, pageTexts } = await extractPages(pdfAbs);
      const takeawayDefects = detectTakeawayDefects(
        pageTexts,
        h.closing || ""
      );
      // Load closing from payload when available
      let closing = "";
      if (h.file) {
        const payloadPath = path.join(
          ROOT,
          "lib/ai-visibility-audit-action-plan/payloads",
          h.file
        );
        if (fs.existsSync(payloadPath)) {
          const p = JSON.parse(fs.readFileSync(payloadPath, "utf8"));
          closing = p.executiveSummary?.closing || "";
        }
      }
      const takeaway2 = detectTakeawayDefects(pageTexts, closing);
      const raster = await rasterizeAllPages(pdfAbs, hotelOut);
      const pageFlags = [];
      for (let n = 1; n <= raster.pageCount; n++) {
        const png = path.join(hotelOut, `page-${String(n).padStart(2, "0")}.png`);
        const flags = flagRasterHeuristics(png, pageTexts[n - 1] || "");
        if (flags.length) pageFlags.push({ page: n, flags });
      }
      const nearEmptyFinal =
        pageTexts.length > 0 &&
        (pageTexts[pageTexts.length - 1] || "").replace(/\s+/g, "").length < 400;
      const allDefects = [
        ...takeaway2.map((d) => d.type),
        ...(nearEmptyFinal ? ["NEAR_EMPTY_FINAL_PAGE"] : []),
        ...pageFlags.flatMap((p) => p.flags.map((f) => `${f}@${p.page}`)),
      ];
      const isSix = SIX.some(
        (re) => re.test(h.hotelName || "") || re.test(h.file || "")
      );
      results.push({
        hotelName: h.hotelName,
        file: h.file,
        propertyId: h.propertyId,
        pdfPath: path.relative(ROOT, pdfAbs).replace(/\\/g, "/"),
        pdfSha256: hash,
        pageCount,
        rasterPages: raster.written.length,
        takeawayDefects: takeaway2,
        pageFlags,
        allDefects,
        sixHotelFullInspect: isSix,
        pass: allDefects.length === 0 && raster.written.length === pageCount,
      });
      console.log(
        `pages=${pageCount} raster=${raster.written.length} defects=${allDefects.length || 0} sha=${hash.slice(0, 10)}`
      );
    } catch (err) {
      console.log("FAIL", err.message);
      results.push({
        hotelName: h.hotelName,
        file: h.file,
        error: err.message,
        pass: false,
      });
    }
  }

  const summary = {
    generatedAt: new Date().toISOString(),
    method:
      "playwright_pdfjs_canvas_raster_every_page + pdfjs_text_takeaway_pagination",
    hotelCount: results.length,
    passed: results.filter((r) => r.pass).length,
    failed: results.filter((r) => !r.pass).length,
    sixHotelResults: results.filter((r) => r.sixHotelFullInspect),
    results,
  };
  const outJson = path.join(OUT_DIR, "AVA_V4_FINAL_PDF_VISUAL_QA_REPORT.json");
  fs.writeFileSync(outJson, `${JSON.stringify(summary, null, 2)}\n`);
  console.log("\nWrote", path.relative(ROOT, outJson).replace(/\\/g, "/"));
  console.log(`Visual QA: ${summary.passed}/${summary.hotelCount} pass`);
  if (summary.failed) process.exit(2);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
