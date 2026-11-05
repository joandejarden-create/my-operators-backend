/**
 * Click-test the NIH Patient Hotel Shuttles hyperlink annotation inside the
 * regenerated Bethesda Marriott PDF via pdf.js + Playwright navigation.
 *
 *   node scripts/ava-click-test-bethesda-nih-link-v1.mjs
 */
import fs from "node:fs";
import http from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const PDF = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1/bethesda-marriott/pdf-staged/Dealality - AI Visibility Audit & Action Plan - Bethesda Marriott - NOVEMBER2026.pdf"
);
const EXPECTED =
  "https://ors.od.nih.gov/pes/dats/PatientServices/PatientShuttles/Pages/hotel_shuttles.aspx";
const BROKEN = "https://www.cc.nih.gov/about/visitor/hotels";
const OUT = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1/AVA_BETHESDA_PDF_NIH_LINK_CLICK_TEST.json"
);

async function main() {
  const { chromium } = await import("playwright");
  const pdfjsRoot = path.resolve(ROOT, "node_modules/pdfjs-dist/legacy/build");
  const workDir = path.join(
    ROOT,
    "reports/dealality-ai-visibility-audit-action-plan-pilot-v1/bethesda-marriott/pdf-staged/page-previews-nih-correction"
  );
  fs.mkdirSync(workDir, { recursive: true });
  const reportCopy = path.join(workDir, "_click-report.pdf");
  fs.copyFileSync(PDF, reportCopy);
  const viewer = path.join(workDir, "_click-viewer.html");
  fs.writeFileSync(
    viewer,
    `<!doctype html><html><body style="margin:0">
<canvas id="c"></canvas>
<script type="module">
import * as pdfjs from "/pdfjs/pdf.mjs";
pdfjs.GlobalWorkerOptions.workerSrc = "/pdfjs/pdf.worker.mjs";
const res = await fetch("/_click-report.pdf");
const data = new Uint8Array(await res.arrayBuffer());
const doc = await pdfjs.getDocument({ data }).promise;
const urls = [];
for (let i = 1; i <= doc.numPages; i++) {
  const page = await doc.getPage(i);
  const ann = await page.getAnnotations();
  for (const a of ann) {
    if (a.url || a.unsafeUrl) urls.push({ page: i, url: a.url || a.unsafeUrl, subtype: a.subtype });
  }
}
window.__urls = urls;
window.__ready = true;
</script></body></html>`
  );

  const server = http.createServer((req, res) => {
    const urlPath = decodeURIComponent((req.url || "/").split("?")[0]);
    let filePath;
    if (urlPath.startsWith("/pdfjs/")) {
      filePath = path.join(pdfjsRoot, urlPath.replace("/pdfjs/", ""));
    } else if (urlPath === "/" || urlPath === "/_click-viewer.html") {
      filePath = viewer;
    } else {
      filePath = path.join(workDir, urlPath.replace(/^\//, ""));
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
  const page = await browser.newPage();
  await page.goto(`http://127.0.0.1:${port}/_click-viewer.html`, {
    waitUntil: "networkidle",
  });
  await page.waitForFunction(() => window.__ready === true, null, {
    timeout: 30000,
  });
  const urls = await page.evaluate(() => window.__urls);
  const nihLinks = urls.filter((u) =>
    String(u.url || "").includes("ors.od.nih.gov/pes/dats/PatientServices/PatientShuttles")
  );
  const brokenLinks = urls.filter((u) =>
    String(u.url || "").includes("cc.nih.gov/about/visitor/hotels")
  );

  let navigation = null;
  if (nihLinks.length) {
    const target = nihLinks[0].url;
    const resp = await page.goto(target, {
      waitUntil: "domcontentloaded",
      timeout: 30000,
    });
    const bodyText = await page.evaluate(() => document.body?.innerText || "");
    navigation = {
      requestedUrl: target,
      finalUrl: page.url(),
      status: resp?.status() || null,
      ok: (resp?.status() || 0) >= 200 && (resp?.status() || 0) < 400,
      titleHasPatientHotelShuttles: /Patient\s+Hotel\s+Shuttles/i.test(bodyText),
      listsBethesdaMarriottOrMarriottPooks:
        /Bethesda\s+Marriott|Marriott\s+Pooks/i.test(bodyText),
      listsHyattRegency: /Hyatt\s+Regency/i.test(bodyText),
    };
  }

  await browser.close();
  server.close();

  const report = {
    checkedAt: new Date().toISOString(),
    pdf: path.relative(ROOT, PDF).replace(/\\/g, "/"),
    annotationCount: urls.length,
    annotations: urls,
    brokenAnnotationCount: brokenLinks.length,
    nihReplacementAnnotationCount: nihLinks.length,
    expectedReplacement: EXPECTED,
    brokenUrl: BROKEN,
    clickNavigation: navigation,
    ok:
      brokenLinks.length === 0 &&
      nihLinks.length > 0 &&
      navigation?.ok === true &&
      navigation?.listsBethesdaMarriottOrMarriottPooks !== true,
  };
  fs.writeFileSync(OUT, `${JSON.stringify(report, null, 2)}\n`);
  console.log(
    JSON.stringify(
      {
        ok: report.ok,
        annotationCount: urls.length,
        brokenAnnotationCount: brokenLinks.length,
        nihReplacementAnnotationCount: nihLinks.length,
        clickStatus: navigation?.status || null,
        listsMarriott: navigation?.listsBethesdaMarriottOrMarriottPooks === true,
        out: path.relative(ROOT, OUT).replace(/\\/g, "/"),
      },
      null,
      2
    )
  );
  process.exit(report.ok ? 0 : 1);
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
