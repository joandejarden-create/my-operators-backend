#!/usr/bin/env node
/**
 * Regenerate representative AVA V4 PDFs with the new cover title and
 * rasterize cover pages for visual verification.
 *
 * Does not initiate paid research. Does not overwrite historical audit-run archives.
 *
 *   node scripts/ava-v4-cover-title-regen-verify-v1.mjs
 */
import fs from "node:fs";
import path from "node:path";
import crypto from "node:crypto";
import { fileURLToPath } from "node:url";
import { generateAiVisibilityAuditPdfV1 } from "../lib/ai-visibility-audit-action-plan/generate-audit-pdf-v1.mjs";
import {
  AVA_COVER_REPORT_TITLE,
  AVA_COVER_REPORT_SUBTITLE,
  renderAiVisibilityAuditCover,
} from "../lib/dealality-report-family/dealality-report-cover-v1.js";
import { certifyAvaAgainstCustomerFacingAdp } from "../lib/ai-visibility-audit-action-plan/adp-ava-permanent-cross-report-certification-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const OUT = path.join(
  ROOT,
  "reports/ava-permanent-audit-runner-v1/cover-title-regen-v1"
);
const HOTELS = [
  {
    slug: "spice-island-beach-resort",
    propertyId: "adp_spice_island_beach_resort",
    payload: "spice-island-beach-resort-v1.json",
  },
  {
    slug: "ac-hotel-a-coruna",
    propertyId: "adp_ac_hotel_a_coruna",
    payload: "ac-hotel-a-coruna-v1.json",
  },
  {
    slug: "cambridge-beaches-bermuda",
    propertyId: "adp_cambridge_beaches_bermuda",
    payload: "cambridge-beaches-bermuda-v1.json",
  },
  {
    slug: "castillo-hotel-son-vida",
    propertyId: "adp_castillo_hotel_son_vida",
    payload: "castillo-hotel-son-vida-v1.json",
  },
];

function sha256File(p) {
  return crypto.createHash("sha256").update(fs.readFileSync(p)).digest("hex");
}

function ensureDir(p) {
  fs.mkdirSync(p, { recursive: true });
}

async function rasterizeCover(pdfPath, pngPath) {
  const { chromium } = await import("playwright");
  const browser = await chromium.launch({ headless: true });
  try {
    const page = await browser.newPage({
      viewport: { width: 900, height: 1200 },
    });
    const pdfData = fs.readFileSync(pdfPath).toString("base64");
    await page.setContent(
      `<!doctype html><html><body style="margin:0;background:#111">
      <canvas id="c"></canvas>
      <script src="https://cdnjs.cloudflare.com/ajax/libs/pdf.js/3.11.174/pdf.min.js"></script>
      <script>
        const b64=${JSON.stringify(pdfData)};
        const raw=atob(b64); const arr=new Uint8Array(raw.length);
        for (let i=0;i<raw.length;i++) arr[i]=raw.charCodeAt(i);
        pdfjsLib.GlobalWorkerOptions.workerSrc='https://cdnjs.cloudflare.com/ajax/libs/pdf.js/3.11.174/pdf.worker.min.js';
        pdfjsLib.getDocument({data:arr}).promise.then(async (pdf)=>{
          const pg=await pdf.getPage(1);
          const vp=pg.getViewport({scale:2});
          const c=document.getElementById('c');
          c.width=vp.width; c.height=vp.height;
          await pg.render({canvasContext:c.getContext('2d'), viewport:vp}).promise;
          document.title='READY';
        });
      </script></body></html>`,
      { waitUntil: "networkidle" }
    );
    await page.waitForFunction(() => document.title === "READY", null, {
      timeout: 60000,
    });
    const canvas = await page.$("#c");
    await canvas.screenshot({ path: pngPath, type: "png" });
    const text = await page.evaluate(async () => {
      // re-extract via pdf.js text content for page 1
      return document.title;
    });
    // Extract page-1 text properly
    const pageText = await page.evaluate(async () => {
      const b64 = document.querySelector("script")?.textContent || "";
      return null;
    });
    void pageText;
    void text;
    // Separate text extract
    const textPage = await browser.newPage();
    await textPage.setContent(
      `<!doctype html><html><body><pre id="out"></pre>
      <script src="https://cdnjs.cloudflare.com/ajax/libs/pdf.js/3.11.174/pdf.min.js"></script>
      <script>
        const b64=${JSON.stringify(pdfData)};
        const raw=atob(b64); const arr=new Uint8Array(raw.length);
        for (let i=0;i<raw.length;i++) arr[i]=raw.charCodeAt(i);
        pdfjsLib.GlobalWorkerOptions.workerSrc='https://cdnjs.cloudflare.com/ajax/libs/pdf.js/3.11.174/pdf.worker.min.js';
        pdfjsLib.getDocument({data:arr}).promise.then(async (pdf)=>{
          const pg=await pdf.getPage(1);
          const tc=await pg.getTextContent();
          document.getElementById('out').textContent=tc.items.map(i=>i.str).join(' ');
          document.title='TEXT_READY';
        });
      </script></body></html>`,
      { waitUntil: "networkidle" }
    );
    await textPage.waitForFunction(() => document.title === "TEXT_READY", null, {
      timeout: 60000,
    });
    const coverText = await textPage.$eval("#out", (el) => el.textContent || "");
    await textPage.close();
    return { coverText };
  } finally {
    await browser.close();
  }
}

async function main() {
  ensureDir(OUT);
  const results = [];

  for (const h of HOTELS) {
    const payloadPath = path.join(
      ROOT,
      "lib/ai-visibility-audit-action-plan/payloads",
      h.payload
    );
    const audit = JSON.parse(fs.readFileSync(payloadPath, "utf8"));
    const coverHtml = renderAiVisibilityAuditCover(audit);
    const htmlOk =
      coverHtml.includes("AI VISIBILITY &amp; DIGITAL PRESENCE ASSESSMENT") &&
      coverHtml.includes("AUDIT &amp; ACTION PLAN") &&
      !coverHtml.includes("INITIAL DIGITAL PRESENCE ASSESSMENT");

    const hotelDir = path.join(OUT, h.slug);
    ensureDir(hotelDir);
    const pdfPath = path.join(
      hotelDir,
      `Dealality - AVA V4 Cover Title - ${h.slug} - NOVEMBER2026.pdf`
    );

    const gen = await generateAiVisibilityAuditPdfV1({
      payload: audit,
      outPath: pdfPath,
      propertyId: h.propertyId,
    });

    let cert = null;
    try {
      cert = await certifyAvaAgainstCustomerFacingAdp(audit, {});
    } catch (e) {
      cert = { ok: false, error: String(e?.message || e) };
    }

    const { coverText } = await rasterizeCover(
      pdfPath,
      path.join(hotelDir, "cover.png")
    );
    // PDF letter-spacing often extracts as "A I V I S I B I L I T Y"; collapse for match.
    const coverNorm = String(coverText || "")
      .replace(/\s+/g, "")
      .toUpperCase();
    const titleOk = coverNorm.includes(
      "AIVISIBILITY&DIGITALPRESENCEASSESSMENT"
    );
    const subOk = coverNorm.includes("AUDIT&ACTIONPLAN");
    const legacyAbsent = !coverNorm.includes(
      "INITIALDIGITALPRESENCEASSESSMENT"
    );

    const row = {
      propertyId: h.propertyId,
      slug: h.slug,
      hotelName: audit.property?.name || null,
      period: audit.reporting?.reportMonthLabel || null,
      htmlOk,
      titleOk,
      subOk,
      legacyAbsent,
      pdfPath: path.relative(ROOT, pdfPath).replace(/\\/g, "/"),
      pdfSha256: sha256File(pdfPath),
      coverPng: path.relative(ROOT, path.join(hotelDir, "cover.png")).replace(/\\/g, "/"),
      coverTextSample: coverText.slice(0, 280),
      generation: {
        ok: gen?.ok !== false,
        pages: gen?.pageCount || gen?.pages || null,
      },
      certification: {
        status: cert?.status || cert?.crossReportStatus || null,
        ok: cert?.ok !== false,
        jointPublicationAllowed: cert?.jointPublicationAllowed,
      },
      expectedTitle: AVA_COVER_REPORT_TITLE,
      expectedSubtitle: AVA_COVER_REPORT_SUBTITLE,
    };
    results.push(row);
    console.log(
      JSON.stringify({
        slug: h.slug,
        htmlOk,
        titleOk,
        subOk,
        legacyAbsent,
        cert: row.certification.status,
      })
    );
  }

  const summary = {
    contract: "AVA_V4_COVER_TITLE_REGEN_VERIFY_V1",
    generatedAt: new Date().toISOString(),
    expectedTitle: AVA_COVER_REPORT_TITLE,
    expectedSubtitle: AVA_COVER_REPORT_SUBTITLE,
    allHtmlOk: results.every((r) => r.htmlOk),
    allTitleOk: results.every((r) => r.titleOk),
    allSubOk: results.every((r) => r.subOk),
    allLegacyAbsent: results.every((r) => r.legacyAbsent),
    hotels: results,
  };
  const outPath = path.join(OUT, "COVER_TITLE_REGEN_VERIFY.json");
  fs.writeFileSync(outPath, `${JSON.stringify(summary, null, 2)}\n`);
  console.log(
    JSON.stringify(
      {
        outPath,
        allOk:
          summary.allHtmlOk &&
          summary.allTitleOk &&
          summary.allSubOk &&
          summary.allLegacyAbsent,
      },
      null,
      2
    )
  );
  if (
    !(
      summary.allHtmlOk &&
      summary.allTitleOk &&
      summary.allSubOk &&
      summary.allLegacyAbsent
    )
  ) {
    process.exit(2);
  }
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
