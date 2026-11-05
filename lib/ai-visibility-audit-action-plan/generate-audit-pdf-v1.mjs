/**
 * AI Visibility Audit & Action Plan — staged PDF generator.
 * Reuses Dealality report-system chrome (same family as AI Demand Performance Review).
 * Does not publish, email, or modify production ADP.
 */
import path from "node:path";
import { fileURLToPath } from "node:url";
import fs from "node:fs";
import http from "node:http";
import { buildAiVisibilityAuditPdfHtml } from "./build-audit-pdf-html-v1.js";
import {
  AI_VISIBILITY_AUDIT_PRODUCT_TITLE,
  AI_VISIBILITY_AUDIT_PDF_TEMPLATE_V1,
  AI_VISIBILITY_AUDIT_RENDERER,
  AI_VISIBILITY_AUDIT_REPORT_FAMILY,
} from "./audit-pdf-contract-v1.js";
import { proposeMonthlyRegisterImportFromAudit } from "./audit-to-monthly-action-register-bridge-v1.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "../..");
const RENDER_HTML = "/adp-ai-visibility-audit-pdf-render.html";

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

function assertCssOnDisk() {
  const required = [
    "public/css/dealality-report-system-v1.css",
    "public/css/adp-monthly-review-report-v1.css",
    "public/css/adp-ai-visibility-audit-report-v1.css",
    "public/css/brand-alignment-snapshot.css",
    "public/css/dealality-report-print-chrome.css",
  ];
  for (const rel of required) {
    const p = path.join(ROOT, rel);
    if (!fs.existsSync(p)) throw new Error(`CSS_NOT_LOADED: missing ${rel}`);
  }
}

function contentTypeFor(filePath) {
  const ext = path.extname(filePath).toLowerCase();
  if (ext === ".css") return "text/css; charset=utf-8";
  if (ext === ".js") return "application/javascript; charset=utf-8";
  if (ext === ".html") return "text/html; charset=utf-8";
  if (ext === ".png") return "image/png";
  if (ext === ".svg") return "image/svg+xml";
  if (ext === ".woff2") return "font/woff2";
  return "application/octet-stream";
}

function startStaticServer(payloadPack) {
  return new Promise((resolve, reject) => {
    const server = http.createServer((req, res) => {
      try {
        const url = new URL(req.url || "/", "http://127.0.0.1");
        if (url.pathname === "/__ava_audit_pdf_payload__") {
          res.writeHead(200, { "Content-Type": "application/json; charset=utf-8" });
          res.end(JSON.stringify(payloadPack));
          return;
        }
        let rel = url.pathname;
        if (rel === "/" || rel === RENDER_HTML) {
          rel = "/adp-ai-visibility-audit-pdf-render.html";
        }
        const filePath = path.join(ROOT, "public", rel.replace(/^\//, ""));
        if (!filePath.startsWith(path.join(ROOT, "public")) || !fs.existsSync(filePath)) {
          res.writeHead(404);
          res.end("not_found");
          return;
        }
        res.writeHead(200, { "Content-Type": contentTypeFor(filePath) });
        fs.createReadStream(filePath).pipe(res);
      } catch (err) {
        res.writeHead(500);
        res.end(String(err && err.message ? err.message : err));
      }
    });
    server.listen(0, "127.0.0.1", () => {
      const addr = server.address();
      resolve({
        server,
        baseUrl: `http://127.0.0.1:${addr.port}`,
      });
    });
    server.on("error", reject);
  });
}

function buildFilename(audit) {
  const hotel = String(audit.property?.name || "Property")
    .replace(/[^\w\s-]+/g, "")
    .trim()
    .replace(/\s+/g, " ");
  const month = String(audit.reporting?.reportMonthLabel || "REPORT")
    .replace(/\s+/g, "");
  return `Dealality - AI Visibility Audit & Action Plan - ${hotel} - ${month}.pdf`;
}

/**
 * @param {object} opts
 * @param {object|string} opts.payload - audit object or path to JSON
 * @param {string} [opts.outPath]
 * @param {string} [opts.pagesDir]
 * @param {string} [opts.bridgeOutPath]
 */
export async function generateAiVisibilityAuditPdfV1(opts = {}) {
  assertCssOnDisk();

  let audit = opts.payload;
  if (typeof audit === "string") {
    audit = JSON.parse(fs.readFileSync(audit, "utf8"));
  }
  if (!audit || typeof audit !== "object") {
    throw new Error("payload_required");
  }

  const buildHtml = opts.buildHtml || buildAiVisibilityAuditPdfHtml;
  const built = buildHtml(audit);
  if (!built.ok) {
    throw new Error(`AUDIT_PDF_PAYLOAD_INVALID: ${(built.errors || []).join(", ")}`);
  }
  const expectedEvidenceUrls = built.evidenceUrls || [];
  const minSections = opts.minSections ?? 6;

  const payloadPack = {
    html: built.html,
    propertyId: audit.property?.propertyId || "",
    propertyName: audit.property?.name || "",
    templateId: built.templateId || AI_VISIBILITY_AUDIT_PDF_TEMPLATE_V1,
    renderer: AI_VISIBILITY_AUDIT_RENDERER,
    reportFamily: AI_VISIBILITY_AUDIT_REPORT_FAMILY,
    status: audit.status || "STAGED_NOT_PUBLISHED",
  };

  const Chrome = loadPrintChrome();
  const headerHtml = Chrome.playwrightHeaderTemplate();
  const footerHtml = Chrome.playwrightFooterTemplate(AI_VISIBILITY_AUDIT_PRODUCT_TITLE);
  const margins = Chrome.pdfMargins();

  const { server, baseUrl } = await startStaticServer(payloadPack);
  const { chromium } = await import("playwright");
  const browser = await chromium.launch({ headless: true });

  try {
    const page = await browser.newPage({ viewport: { width: 1440, height: 900 } });
    const renderUrl = `${baseUrl}${RENDER_HTML}?_cb=ava1`;
    await page.goto(renderUrl, { waitUntil: "networkidle", timeout: 120000 });
    await page.waitForSelector("#ava-pdf-host[data-ava-pdf-ready='1']", {
      timeout: 90000,
    });
    await page.waitForTimeout(600);

    const meta = await page.evaluate(() => {
      const host = document.getElementById("ava-pdf-host");
      const band = host?.querySelector(".drs-kpi-band");
      const bandStyle = band ? getComputedStyle(band) : null;
      const cssHrefs = [...document.querySelectorAll('link[rel="stylesheet"]')].map(
        (l) => l.getAttribute("href") || ""
      );
      return {
        propertyName: host?.getAttribute("data-ava-property-name") || "",
        propertyId: host?.getAttribute("data-ava-property-id") || "",
        reportFamily: host?.getAttribute("data-ava-report-family") || "",
        templateId: host?.getAttribute("data-ava-template-id") || "",
        renderer: host?.getAttribute("data-ava-renderer") || "",
        hasCover: !!host?.querySelector(".bas-cover-page"),
        hasGeometric: !!host?.querySelector(".bas-cover-geometric"),
        sectionCount: host?.querySelectorAll("[data-ava-section]")?.length || 0,
        actionCount: host?.querySelectorAll("[data-ava-action]")?.length || 0,
        drsKpiCount: host?.querySelectorAll(".drs-kpi")?.length || 0,
        kpiBandDisplay: bandStyle?.display || "",
        cssHrefs,
        fontFamily: getComputedStyle(host?.querySelector(".drs-report") || host)
          .fontFamily,
      };
    });

    if (!meta.hasCover || !meta.hasGeometric) {
      throw new Error("COVER_CHROME_MISSING");
    }
    if (meta.drsKpiCount < 1) {
      throw new Error("KPI_GRID_MISSING");
    }
    if (!meta.cssHrefs.some((h) => h.includes("dealality-report-system-v1.css"))) {
      throw new Error("CSS_NOT_LOADED: report-system");
    }
    if (meta.sectionCount < minSections) {
      throw new Error(`SECTION_COUNT_LOW: ${meta.sectionCount}`);
    }

    const domLinks = await page.evaluate(() =>
      [...document.querySelectorAll("a.ava-evidence-link[href^='http']")].map(
        (a) => a.getAttribute("href")
      )
    );

    const filename = buildFilename(audit);
    const outPath = opts.outPath || undefined;
    if (outPath && /\.tmp$/i.test(outPath)) {
      throw new Error("TEMP_FILE_NAME_FORBIDDEN");
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
      // Full-page PDF screenshots via pdf.js-style: use Chromium PDF page raster
      // through printing each section element as visual QA proxies.
      const sections = [
        "cover",
        "executive",
        "overview",
        "decisions",
        "findings",
        "actions",
        "methodology",
      ];
      for (const key of sections) {
        const el = await page.$(`[data-ava-section="${key}"]`);
        if (!el) continue;
        const outPng = path.join(opts.pagesDir, `${key}.png`);
        await el.screenshot({ path: outPng });
        pagePngs.push(outPng);
      }
    }

    let bridgeProposal = null;
    if (opts.bridgeOutPath) {
      bridgeProposal = proposeMonthlyRegisterImportFromAudit(audit);
      fs.mkdirSync(path.dirname(opts.bridgeOutPath), { recursive: true });
      fs.writeFileSync(
        opts.bridgeOutPath,
        JSON.stringify(bridgeProposal, null, 2),
        "utf8"
      );
    }

    return {
      buffer: Buffer.from(buffer),
      filename,
      outPath: outPath || null,
      propertyId: meta.propertyId,
      propertyName: meta.propertyName,
      reportFamily: meta.reportFamily || AI_VISIBILITY_AUDIT_REPORT_FAMILY,
      templateId: meta.templateId || AI_VISIBILITY_AUDIT_PDF_TEMPLATE_V1,
      renderer: meta.renderer || AI_VISIBILITY_AUDIT_RENDERER,
      structural: meta,
      pagePngs,
      margins,
      bridgeProposal,
      expectedEvidenceUrls,
      domEvidenceLinks: [...new Set(domLinks)],
      status: "STAGED_NOT_PUBLISHED",
      publicationAuthorized: false,
    };
  } finally {
    await browser.close();
    await new Promise((r) => server.close(r));
  }
}

async function main(argv = process.argv.slice(2)) {
  const get = (flag) => {
    const i = argv.indexOf(flag);
    return i >= 0 ? argv[i + 1] : null;
  };
  const payload =
    get("--payload") ||
    path.join(
      ROOT,
      "lib/ai-visibility-audit-action-plan/payloads/spice-island-beach-resort-v1.json"
    );
  const outDir =
    get("--out-dir") ||
    path.join(
      ROOT,
      "reports/dealality-ai-visibility-audit-action-plan-pilot-v1/spice-island-beach-resort/pdf-staged"
    );
  fs.mkdirSync(outDir, { recursive: true });

  const audit = JSON.parse(fs.readFileSync(payload, "utf8"));
  const filename = buildFilename(audit);
  const outPath = path.join(outDir, filename);
  const pagesDir = path.join(outDir, "page-previews");
  const bridgeOutPath = path.join(outDir, "proposed-monthly-action-register-bridge.json");

  const result = await generateAiVisibilityAuditPdfV1({
    payload: audit,
    outPath,
    pagesDir,
    bridgeOutPath,
  });

  const manifest = {
    contract: AI_VISIBILITY_AUDIT_PDF_TEMPLATE_V1,
    status: "STAGED_NOT_PUBLISHED",
    publicationAuthorized: false,
    productionAdpModified: false,
    generatedAt: new Date().toISOString(),
    pdf: outPath,
    filename: result.filename,
    pagePreviews: result.pagePngs,
    bridgeProposal: bridgeOutPath,
    structural: result.structural,
    property: audit.property,
    reporting: audit.reporting,
  };
  fs.writeFileSync(
    path.join(outDir, "STAGED_MANIFEST.json"),
    JSON.stringify(manifest, null, 2),
    "utf8"
  );

  console.log(
    JSON.stringify(
      {
        ok: true,
        status: "STAGED_NOT_PUBLISHED",
        pdf: outPath,
        filename: result.filename,
        pagePreviewCount: result.pagePngs.length,
        bridgeProposal: bridgeOutPath,
        sections: result.structural.sectionCount,
        actions: result.structural.actionCount,
      },
      null,
      2
    )
  );
}

const isDirect =
  process.argv[1] &&
  path.resolve(process.argv[1]) === fileURLToPath(import.meta.url);

if (isDirect) {
  main().catch((err) => {
    console.error(err);
    process.exit(1);
  });
}
