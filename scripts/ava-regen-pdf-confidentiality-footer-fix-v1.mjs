/**
 * Minor template cleanup: regenerate six staged AVA PDFs after removing
 * the duplicate body confidentiality line (footer remains authoritative).
 *
 * Preserves prior PDF bytes under pdf-staged/pre-confidentiality-footer-fix/.
 * Does not change findings, approvals, or publication permissions.
 *
 *   node scripts/ava-regen-pdf-confidentiality-footer-fix-v1.mjs
 */
import fs from "node:fs";
import http from "node:http";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { generateAiVisibilityAuditPdfV1 } from "../lib/ai-visibility-audit-action-plan/generate-audit-pdf-v1.mjs";
import { buildAiVisibilityAuditPdfHtml } from "../lib/ai-visibility-audit-action-plan/build-audit-pdf-html-v1.js";
import {
  getAuditMeta,
  saveAuditVersion,
  upsertAuditMeta,
} from "../lib/ai-visibility-audit-action-plan/audit-store-v1.js";
import { stageAvaPdf } from "../lib/ai-visibility-audit-action-plan/ava-pdf-archive-v1.js";
import { buildAiVisibilityAuditPdfFilename } from "../lib/ai-visibility-audit-action-plan/ava-pdf-filename-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

const HOTELS = [
  {
    payload: "lib/ai-visibility-audit-action-plan/payloads/spice-island-beach-resort-v1.json",
    slug: "spice-island-beach-resort",
  },
  {
    payload: "lib/ai-visibility-audit-action-plan/payloads/wyndham-san-diego-bayside-v1.json",
    slug: "wyndham-san-diego-bayside",
  },
  {
    payload: "lib/ai-visibility-audit-action-plan/payloads/waterstone-boca-raton-v1.json",
    slug: "waterstone-boca-raton",
  },
  {
    payload: "lib/ai-visibility-audit-action-plan/payloads/bethesda-marriott-v1.json",
    slug: "bethesda-marriott",
  },
  {
    payload: "lib/ai-visibility-audit-action-plan/payloads/castillo-hotel-son-vida-v1.json",
    slug: "castillo-hotel-son-vida",
  },
  {
    payload: "lib/ai-visibility-audit-action-plan/payloads/sheraton-mallorca-arabella-golf-v1.json",
    slug: "sheraton-mallorca-arabella-golf",
  },
];

const CONF_RE = /Confidential\s*[·•]\s*For recipient only\s*[·•]\s*©\s*2026\s*Dealality/gi;

function countMatches(text) {
  const m = String(text || "").match(CONF_RE);
  return m ? m.length : 0;
}

async function extractPdfPages(pdfPath) {
  const pdfjs = await import("pdfjs-dist/legacy/build/pdf.mjs");
  const data = new Uint8Array(fs.readFileSync(pdfPath));
  const doc = await pdfjs.getDocument({ data }).promise;
  const pages = [];
  for (let i = 1; i <= doc.numPages; i += 1) {
    const page = await doc.getPage(i);
    const c = await page.getTextContent();
    pages.push(c.items.map((x) => x.str).join(" "));
  }
  return { pageCount: doc.numPages, pages };
}

/**
 * Rasterize the last PDF page via Playwright + pdf.js HTTP viewer (ADP QA pattern).
 */
async function rasterizeLastPage(pdfPath, outPng) {
  const { chromium } = await import("playwright");
  const pdfjsRoot = path.resolve(ROOT, "node_modules/pdfjs-dist/legacy/build");
  const workDir = path.dirname(outPng);
  fs.mkdirSync(workDir, { recursive: true });
  const reportCopy = path.join(workDir, "_qa-report.pdf");
  fs.copyFileSync(pdfPath, reportCopy);
  const viewer = path.join(workDir, "_viewer.html");
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
  await new Promise((resolve) => server.listen(0, "127.0.0.1", resolve));
  const { port } = server.address();
  const browser = await chromium.launch({ headless: true });
  try {
    const page = await browser.newPage({ viewport: { width: 900, height: 1200 } });
    await page.goto(`http://127.0.0.1:${port}/_viewer.html`, {
      waitUntil: "domcontentloaded",
      timeout: 120000,
    });
    await page.waitForFunction(() => window.__ready === true, {
      timeout: 120000,
    });
    const pageCount = await page.evaluate(() => window.__pages);
    await page.evaluate(async (n) => {
      const pg = await window.__doc.getPage(n);
      const viewport = pg.getViewport({ scale: 1.5 });
      const canvas = document.getElementById("c");
      canvas.width = viewport.width;
      canvas.height = viewport.height;
      await pg.render({
        canvasContext: canvas.getContext("2d"),
        viewport,
      }).promise;
    }, pageCount);
    await page.locator("#c").screenshot({ path: outPng });
    return { pageCount, outPng };
  } finally {
    await browser.close();
    await new Promise((r) => server.close(r));
  }
}

function backupExistingPdfs(outDir, priorDir) {
  fs.mkdirSync(priorDir, { recursive: true });
  const copied = [];
  if (!fs.existsSync(outDir)) return copied;
  for (const f of fs.readdirSync(outDir)) {
    if (!/\.pdf$/i.test(f)) continue;
    const from = path.join(outDir, f);
    const to = path.join(priorDir, f.replace(/\.pdf$/i, "-pre-footer-fix.pdf"));
    fs.copyFileSync(from, to);
    copied.push(path.relative(ROOT, to).replace(/\\/g, "/"));
  }
  return copied;
}

async function main() {
  const results = [];
  for (const h of HOTELS) {
    const audit = JSON.parse(fs.readFileSync(path.join(ROOT, h.payload), "utf8"));
    const propertyId = audit.property.propertyId;
    const priorMeta = getAuditMeta(propertyId) || {};
    const preserveStatus = priorMeta.status || audit.status || "PDF_STAGED";

    const built = buildAiVisibilityAuditPdfHtml(audit);
    if (!built.ok) {
      throw new Error(`HTML_BUILD_FAILED ${propertyId}: ${(built.errors || []).join("; ")}`);
    }
    const bodyConfCount = countMatches(built.html);
    if (bodyConfCount !== 0) {
      throw new Error(
        `BODY_STILL_HAS_CONFIDENTIALITY ${propertyId} count=${bodyConfCount}`
      );
    }

    const outDir = path.join(
      ROOT,
      "reports/dealality-ai-visibility-audit-action-plan-pilot-v1",
      h.slug,
      "pdf-staged"
    );
    fs.mkdirSync(outDir, { recursive: true });
    const priorDir = path.join(outDir, "pre-confidentiality-footer-fix");
    const prior = backupExistingPdfs(outDir, priorDir);

    const filename = buildAiVisibilityAuditPdfFilename({
      hotelName: audit.property.name,
      reportMonthLabel: audit.reporting?.reportMonthLabel,
      date: audit.reporting?.generatedAt,
    });
    const outPath = path.join(outDir, filename);
    const pagesDir = path.join(outDir, "page-previews-footer-fix");
    console.log("Generating", propertyId, "...");
    const generated = await generateAiVisibilityAuditPdfV1({
      payload: audit,
      outPath,
      pagesDir,
      bridgeOutPath: path.join(outDir, "proposed-monthly-action-register-bridge.json"),
    });

    // Staged archive / durable current without destroying prior version files in pdf-staged backup.
    const versionId = `ava_v1_footer_fix_${h.slug.replace(/-/g, "_")}_${Date.now().toString(36)}`;
    const staged = stageAvaPdf({
      propertyId,
      generatedAbsPath: outPath,
      versionId,
      filename,
    });

    saveAuditVersion(propertyId, versionId, audit, {
      status: preserveStatus,
      metaStatus: preserveStatus,
      forceStatus: true,
      kind: "REPORT_VERSION",
      pdfRelativePath: staged.pdfRelativePath,
      researchCostUsd: priorMeta.researchCostUsd || 0,
      evidenceReviewStatus:
        priorMeta.evidenceReviewStatus ||
        (preserveStatus === "AUDIT_VALIDATED" ? "AUDIT_VALIDATED" : "STAGED"),
      validation: {
        contractOk: true,
        templateFix: "confidentiality-footer-only-v1",
        generatedAt: new Date().toISOString(),
        priorStatusPreserved: preserveStatus,
        founderApprovedForCustomer: priorMeta.founderApprovedForCustomer === true,
        customerReadyBlocked: true,
        pdfSha256: staged.sha256,
      },
    });
    upsertAuditMeta(propertyId, {
      pdfRelativePath: staged.pdfRelativePath,
      pdfSha256: staged.sha256,
      // Preserve founder / publication gates
      founderApprovedForCustomer: priorMeta.founderApprovedForCustomer === true,
      founderApprovedForCustomerAt: priorMeta.founderApprovedForCustomerAt || null,
      customerReadinessStatus:
        priorMeta.customerReadinessStatus || "NOT_CUSTOMER_READY",
      publicationAuthorizationStatus:
        priorMeta.publicationAuthorizationStatus || "NOT_AUTHORIZED",
      forceStatus: true,
    });

    // Visual / text QA on real PDF pages (footer once per page; last page = 1).
    const extracted = await extractPdfPages(outPath);
    const lastPageText = extracted.pages[extracted.pages.length - 1] || "";
    const lastPageConf = countMatches(lastPageText);
    const perPageConf = extracted.pages.map((t) => countMatches(t));
    const lastPng = path.join(pagesDir, "last-page.png");
    await rasterizeLastPage(outPath, lastPng);
    const methodologyPng = path.join(pagesDir, "methodology.png");

    const lastPagePass =
      lastPageConf === 1 &&
      /Suggested Next Step/i.test(lastPageText) &&
      perPageConf.every((n) => n === 1);

    results.push({
      propertyId,
      name: audit.property.name,
      filename,
      pdf: path.relative(ROOT, outPath).replace(/\\/g, "/"),
      stagedPath: staged.pdfRelativePath,
      sha256: staged.sha256,
      priorBackups: prior,
      preservedStatus: preserveStatus,
      bodyConfidentialityCount: bodyConfCount,
      pageCount: extracted.pageCount,
      confidentialityPerPage: perPageConf,
      lastPageConfidentialityCount: lastPageConf,
      lastPagePreview: path.relative(ROOT, lastPng).replace(/\\/g, "/"),
      methodologyPreview: fs.existsSync(methodologyPng)
        ? path.relative(ROOT, methodologyPng).replace(/\\/g, "/")
        : null,
      footerOnlyPass: bodyConfCount === 0 && lastPagePass,
      generatedTemplate: generated.templateId || null,
    });
    console.log(
      "OK",
      filename,
      "pages",
      extracted.pageCount,
      "lastPageConf",
      lastPageConf,
      "perPage",
      perPageConf.join(",")
    );
  }

  const ok = results.every((r) => r.footerOnlyPass && r.bodyConfidentialityCount === 0);
  const summary = {
    ok,
    fix: "AVA_PDF_CONFIDENTIALITY_FOOTER_ONLY_V1",
    generatedAt: new Date().toISOString(),
    publicationAuthorized: false,
    paidResearchUsd: 0,
    note: "Body confidentiality after Suggested Next Step removed; shared print footer retained.",
    results,
  };
  const outJson = path.join(
    ROOT,
    "reports/dealality-ai-visibility-audit-action-plan-pilot-v1/AVA_PDF_CONFIDENTIALITY_FOOTER_FIX.json"
  );
  fs.writeFileSync(outJson, `${JSON.stringify(summary, null, 2)}\n`);
  console.log(JSON.stringify({ ok, count: results.length, outJson }, null, 2));
  process.exit(ok ? 0 : 1);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
