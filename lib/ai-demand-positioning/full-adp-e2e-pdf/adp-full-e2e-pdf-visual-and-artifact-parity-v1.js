/**
 * ADP_FULL_E2E_PDF_VISUAL_AND_ARTIFACT_PARITY_V1
 *
 * Mandatory Full E2E gate: correct Full ADP PDF artifact (not Monthly Review),
 * staged-version match, metric content, and page-render visual inspection.
 */

import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  ADP_ARTIFACT_TYPE_FULL_ADP_REPORT_PDF,
  ADP_ARTIFACT_TYPE_MONTHLY_EXECUTIVE_REVIEW_PDF,
  resolveFullAdpE2ePdfArtifact,
} from "./adp-full-e2e-pdf-artifact-store-v1.js";
import { loadStagedOwnerPayload } from "../review/adp-staged-report-store-v1.js";
import {
  ADP_EXECUTIVE_PDF_BETHESDA_MASTER_FIDELITY_V1,
  ADP_EXECUTIVE_REPORT_PDF_BETHESDA_MASTER_V1,
  assessBethesdaMasterFidelityV1,
} from "./adp-executive-report-pdf-bethesda-master-v1.js";
import {
  ADP_EXECUTIVE_PDF_FOUNDER_DESIGN_ACCEPTANCE_V1,
  PDF_FOUNDER_DESIGN_ACCEPTANCE_PENDING,
  FOUNDER_DESIGN_ACCEPTED,
  assessExecutivePdfFounderDesignAcceptanceV1,
  assertAutomationDidNotClaimFounderAcceptance,
} from "./adp-executive-pdf-founder-design-acceptance-v1.js";
import { getFounderExecutivePdfDesignAcceptance } from "./adp-executive-pdf-founder-design-acceptance-ledger-v1.js";

export const ADP_FULL_E2E_PDF_VISUAL_AND_ARTIFACT_PARITY_V1 =
  "ADP_FULL_E2E_PDF_VISUAL_AND_ARTIFACT_PARITY_V1";
export {
  ADP_EXECUTIVE_PDF_BETHESDA_MASTER_FIDELITY_V1,
  ADP_EXECUTIVE_REPORT_PDF_BETHESDA_MASTER_V1,
};

async function extractPdfText(pdfPath) {
  const pdfjs = await import("pdfjs-dist/legacy/build/pdf.mjs");
  const data = new Uint8Array(fs.readFileSync(pdfPath));
  const doc = await pdfjs.getDocument({ data }).promise;
  let text = "";
  for (let i = 1; i <= doc.numPages; i += 1) {
    const page = await doc.getPage(i);
    const content = await page.getTextContent();
    text += `\n===PAGE ${i}===\n${content.items.map((x) => x.str).join(" ")}`;
  }
  return { pages: doc.numPages, text };
}

function kpiFromStaged(payload) {
  const bpp = payload?.brandPortfolioPosition;
  const kpis = Array.isArray(bpp?.kpis) ? bpp.kpis : [];
  const byId = Object.fromEntries(kpis.map((k) => [k.id, k]));
  const rg =
    payload?.realityGap?.gapScore ??
    payload?.realityGap?.v2?.gapScore ??
    payload?.realityGap?.score ??
    null;
  return {
    realityGapV2: rg,
    bppPresence: byId.portfolioAiPresence?.value || null,
    bppRank: byId.portfolioRank?.value || null,
    numberOne: byId.numberOneAppearance?.value || null,
    top3: byId.top3Appearance?.value || null,
    hotelName: payload?.property?.name || null,
  };
}

/**
 * @param {{
 *   propertyId: string,
 *   expectedStagedVersionId?: string|null,
 *   expectedMetrics?: object,
 *   pageRenderDir?: string|null,
 *   baseUrl?: string,
 * }} opts
 */
export async function assessFullAdpE2ePdfVisualAndArtifactParity(opts = {}) {
  const propertyId = String(opts.propertyId || "").trim();
  const failures = [];
  const warnings = [];
  const staged = loadStagedOwnerPayload(propertyId);
  const expectedStaged =
    opts.expectedStagedVersionId || staged?.versionId || null;
  const artifact = resolveFullAdpE2ePdfArtifact(propertyId, {
    expectedStagedVersionId: expectedStaged,
  });

  if (!artifact.available) {
    failures.push(`FULL_ADP_PDF_UNAVAILABLE:${artifact.reason || "missing"}`);
  }
  if (artifact.artifactType !== ADP_ARTIFACT_TYPE_FULL_ADP_REPORT_PDF) {
    failures.push("WRONG_ARTIFACT_TYPE");
  }
  if (
    expectedStaged &&
    artifact.stagedVersionId &&
    artifact.stagedVersionId !== expectedStaged
  ) {
    failures.push("STAGED_VERSION_MISMATCH");
  }

  let textPack = null;
  let bethesdaFidelity = null;
  if (artifact.pdfAbsolutePath && fs.existsSync(artifact.pdfAbsolutePath)) {
    textPack = await extractPdfText(artifact.pdfAbsolutePath);
    // Full ADP E2E PDFs must match the original portfolio executive cover
    // (MONTHLY EXECUTIVE REVIEW + AI Demand Performance Review) — not a
    // parallel FULL ADP cover subtitle and not a dashboard print.
    if (/Load Report|Staged review requested but not authorized/i.test(textPack.text)) {
      failures.push("WEB_DASHBOARD_PRINTED_INSTEAD_OF_EXECUTIVE_REPORT");
    }
    if (/AI Demand Positioning/i.test(textPack.text) && !/AI Demand Performance Review/i.test(textPack.text)) {
      failures.push("DASHBOARD_SHELL_LABELED_AI_DEMAND_POSITIONING");
    }
    if (!/AI Demand Performance Review|Executive Performance/i.test(textPack.text)) {
      failures.push("MISSING_BETHESDA_EXECUTIVE_IDENTITY");
    }
    // Cover subtitle uses letter-spacing in CSS; PDF text extraction often inserts
    // spaces between glyphs — collapse whitespace before product-framing checks.
    // Prefer page-1 (cover) so methodology disclaimers cannot false-trigger.
    const coverPageText = String(textPack.text || "").split(/===PAGE\s+2===/i)[0] || "";
    const collapsedCoverText = coverPageText.replace(/\s+/g, "");
    const hasFullAdpCover = /FULLADPEXECUTIVEREPORT/i.test(collapsedCoverText);
    const hasMonthlyCover = /MONTHLYEXECUTIVEREVIEW/i.test(collapsedCoverText);
    if (hasFullAdpCover) {
      failures.push("NON_PORTFOLIO_FULL_ADP_COVER_SUBTITLE");
    }
    if (!hasMonthlyCover) {
      failures.push("MISSING_PORTFOLIO_MONTHLY_EXECUTIVE_REVIEW_COVER");
    }
    if (/Still developing/i.test(textPack.text) && /#1 of\s+\d+/i.test(textPack.text)) {
      warnings.push(
        "STRENGTH_STILL_DEVELOPING_DESPITE_BPP_RANK_ONE — content mapping should prefer certified positives"
      );
    }
    bethesdaFidelity = assessBethesdaMasterFidelityV1({
      text: textPack.text,
      pages: textPack.pages,
      hasCover: /CONFIDENTIAL|AI DEMAND PERFORMANCE REVIEW/i.test(textPack.text),
      hasDrsKpi: /AI Consideration|Reality Gap|Portfolio Rank|Brand Portfolio/i.test(
        textPack.text
      ),
      hasInteractiveControls: /Load Report|Save hotel feedback/i.test(textPack.text),
    });
    if (!bethesdaFidelity.ok) {
      for (const f of bethesdaFidelity.failures) failures.push(`BETHESDA_${f}`);
    }
    if (
      artifact.meta?.templateId &&
      artifact.meta.templateId !== ADP_EXECUTIVE_REPORT_PDF_BETHESDA_MASTER_V1 &&
      artifact.meta.templateId !== "AI_DEMAND_PERFORMANCE_REVIEW_CURRENT_TEMPLATE_V1"
    ) {
      failures.push(`WRONG_PDF_TEMPLATE:${artifact.meta.templateId}`);
    }
    const expected = {
      ...kpiFromStaged(staged?.payload),
      ...(opts.expectedMetrics || {}),
    };
    if (expected.hotelName && !textPack.text.includes(expected.hotelName)) {
      failures.push("HOTEL_NAME_MISSING_IN_PDF");
    }
    if (
      expected.realityGapV2 != null &&
      !String(textPack.text).includes(String(expected.realityGapV2))
    ) {
      // soft: narrative may format differently
      warnings.push(`RG_VALUE_NOT_LITERAL:${expected.realityGapV2}`);
    }
    if (expected.bppRank) {
      const rankNeedle = String(expected.bppRank).replace(/^#/, "").trim();
      if (
        !textPack.text.includes(String(expected.bppRank)) &&
        !textPack.text.includes(rankNeedle)
      ) {
        failures.push(`BPP_RANK_MISSING:${expected.bppRank}`);
      }
    }
    if (
      expected.bppPresence &&
      !textPack.text.includes(String(expected.bppPresence).replace(/%$/, ""))
    ) {
      warnings.push(`BPP_PRESENCE_LITERAL_SOFT:${expected.bppPresence}`);
    }
    if (textPack.pages < 3) failures.push("PDF_TOO_FEW_PAGES");
  }

  // Visual page renders (actual inspection evidence) — local pdf.js over HTTP.
  const pageRenders = [];
  if (opts.pageRenderDir && artifact.pdfAbsolutePath) {
    fs.mkdirSync(opts.pageRenderDir, { recursive: true });
    try {
      const http = await import("node:http");
      const pdfjsRoot = path.resolve(
        path.dirname(fileURLToPath(import.meta.url)),
        "../../../node_modules/pdfjs-dist/legacy/build"
      );
      fs.copyFileSync(
        artifact.pdfAbsolutePath,
        path.join(opts.pageRenderDir, "report.pdf")
      );
      const viewer = path.join(opts.pageRenderDir, "_viewer.html");
      fs.writeFileSync(
        viewer,
        `<!doctype html><html><body style="margin:0;background:#ddd">
<canvas id="c"></canvas>
<script type="module">
import * as pdfjs from "/pdfjs/pdf.mjs";
pdfjs.GlobalWorkerOptions.workerSrc = "/pdfjs/pdf.worker.mjs";
const res = await fetch("/report.pdf");
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
          filePath = path.join(opts.pageRenderDir, urlPath.replace(/^\//, ""));
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
      const { chromium } = await import("playwright");
      const browser = await chromium.launch({ headless: true });
      const page = await browser.newPage({ viewport: { width: 920, height: 1200 } });
      try {
        await page.goto(`http://127.0.0.1:${port}/_viewer.html`, {
          waitUntil: "domcontentloaded",
          timeout: 120000,
        });
        await page.waitForFunction(() => window.__ready === true, {
          timeout: 120000,
        });
        const pages = await page.evaluate(() => window.__pages);
        const maxPages = Math.min(pages, opts.maxPages || 6);
        for (let i = 1; i <= maxPages; i += 1) {
          await page.evaluate(async (n) => {
            const pg = await window.__doc.getPage(n);
            const viewport = pg.getViewport({ scale: 1.35 });
            const canvas = document.getElementById("c");
            canvas.width = viewport.width;
            canvas.height = viewport.height;
            await pg.render({
              canvasContext: canvas.getContext("2d"),
              viewport,
            }).promise;
          }, i);
          const out = path.join(
            opts.pageRenderDir,
            `page-${String(i).padStart(2, "0")}.png`
          );
          await page.locator("#c").screenshot({ path: out });
          const stats = await page.evaluate(() => {
            const canvas = document.getElementById("c");
            const ctx = canvas.getContext("2d");
            const { width, height } = canvas;
            const img = ctx.getImageData(0, 0, width, height).data;
            let sum = 0;
            let dark = 0;
            let light = 0;
            const step = 16;
            let n = 0;
            for (let y = 0; y < height; y += step) {
              for (let x = 0; x < width; x += step) {
                const i = (y * width + x) * 4;
                const l = (img[i] + img[i + 1] + img[i + 2]) / 3;
                sum += l;
                if (l < 25) dark += 1;
                if (l > 245) light += 1;
                n += 1;
              }
            }
            return {
              meanLuminance: sum / n,
              darkShare: dark / n,
              lightShare: light / n,
            };
          });
          // Page 1 is the Bethesda branded cover (intentionally dark navy).
          const isCover = i === 1;
          const visualFail = isCover
            ? false
            : stats.meanLuminance < 40 ||
              (stats.darkShare > 0.85 && stats.lightShare < 0.05) ||
              (stats.lightShare > 0.995 &&
                stats.darkShare < 0.001 &&
                stats.meanLuminance > 252);
          if (visualFail) {
            failures.push(`VISUAL_PAGE_${i}_UNREADABLE`);
          }
          pageRenders.push({
            page: i,
            path: out,
            ...stats,
            visualFail,
            isCover,
          });
        }
      } finally {
        await browser.close();
        await new Promise((resolve) => server.close(resolve));
      }
    } catch (e) {
      failures.push(`PAGE_RENDER_FAILED:${e.message}`);
    }
  }

  const ok = failures.length === 0;
  const ledgerAcceptance = getFounderExecutivePdfDesignAcceptance(
    propertyId,
    artifact.stagedVersionId || expectedStaged
  );
  const ledgerShaMatches =
    Boolean(ledgerAcceptance?.pdfSha256) &&
    Boolean(artifact.pdfSha256) &&
    String(ledgerAcceptance.pdfSha256).toLowerCase() ===
      String(artifact.pdfSha256).toLowerCase();
  const founderAcceptedFromLedger =
    ledgerAcceptance?.status === FOUNDER_DESIGN_ACCEPTED && ledgerShaMatches;

  const founderAcceptance = assessExecutivePdfFounderDesignAcceptanceV1({
    technicallyValid: ok && Boolean(artifact.pdfSha256),
    contentParityPass:
      ok &&
      !failures.some((f) =>
        /METRIC|RG_|BPP_|MISSING_|STRENGTH|COVER_STILL|NON_PORTFOLIO/i.test(f)
      ),
    visualQaPass: ok && !failures.some((f) => /VISUAL_|PAGE_RENDER/i.test(f)),
    bethesdaDesignReviewReady:
      ok && bethesdaFidelity?.ok === true && !failures.length,
    founderAccepted: founderAcceptedFromLedger,
    founderAcceptedBy: founderAcceptedFromLedger
      ? ledgerAcceptance.founderAcceptedBy
      : null,
    founderAcceptedAt: founderAcceptedFromLedger
      ? ledgerAcceptance.founderAcceptedAt
      : null,
    founderAcceptedNote: founderAcceptedFromLedger
      ? ledgerAcceptance.founderAcceptedNote
      : null,
  });
  // Automation must not invent acceptance; ledger-backed acceptance is allowed.
  if (!founderAcceptedFromLedger) {
    assertAutomationDidNotClaimFounderAcceptance(founderAcceptance.status);
  }

  const designAccepted =
    founderAcceptance.status === FOUNDER_DESIGN_ACCEPTED;

  return {
    ok,
    gate: ADP_FULL_E2E_PDF_VISUAL_AND_ARTIFACT_PARITY_V1,
    bethesdaFidelityGate: ADP_EXECUTIVE_PDF_BETHESDA_MASTER_FIDELITY_V1,
    founderDesignAcceptanceGate: ADP_EXECUTIVE_PDF_FOUNDER_DESIGN_ACCEPTANCE_V1,
    founderDesignAcceptance: founderAcceptance,
    founderDesignAcceptanceStatus: designAccepted
      ? FOUNDER_DESIGN_ACCEPTED
      : PDF_FOUNDER_DESIGN_ACCEPTANCE_PENDING,
    templateId: ADP_EXECUTIVE_REPORT_PDF_BETHESDA_MASTER_V1,
    propertyId,
    artifactType: ADP_ARTIFACT_TYPE_FULL_ADP_REPORT_PDF,
    notArtifactType: ADP_ARTIFACT_TYPE_MONTHLY_EXECUTIVE_REVIEW_PDF,
    stagedVersionId: artifact.stagedVersionId || expectedStaged,
    pdfSha256: artifact.pdfSha256 || null,
    pdfAbsolutePath: artifact.pdfAbsolutePath || null,
    pages: textPack?.pages || null,
    pageRenders,
    bethesdaFidelity,
    failures,
    warnings,
    // Design acceptance unblocks design gate only — publication still requires R1+.
    blocksCustomerDeliveryCertification: !designAccepted,
    publicationBlockedByFounderDesignAcceptance: !designAccepted,
    publicationStillRequiresSeparateFounderAuthorization: true,
    published: false,
    memberstackAuthenticatedQa: "AUTHENTICATED_QA_PENDING",
  };
}

export async function ensureFullAdpE2ePdfVisualAndArtifactParity(opts = {}) {
  return assessFullAdpE2ePdfVisualAndArtifactParity(opts);
}
