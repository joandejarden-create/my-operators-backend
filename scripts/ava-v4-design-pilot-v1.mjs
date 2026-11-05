/**
 * AVA Customer Report V4 design pilot (3 hotels).
 * Preserves published + V3 draft pointers; stages V4 as new draft with prior V3 SHA retained.
 *
 *   AVA_STORAGE_MODE=local node scripts/ava-v4-design-pilot-v1.mjs
 */
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { buildAiVisibilityAuditPdfHtmlV4 } from "../lib/ai-visibility-audit-action-plan/build-audit-pdf-html-v4.js";
import { generateAiVisibilityAuditPdfV1 } from "../lib/ai-visibility-audit-action-plan/generate-audit-pdf-v1.mjs";
import { buildAiVisibilityAuditPdfFilename } from "../lib/ai-visibility-audit-action-plan/ava-pdf-filename-v1.js";
import { stageAvaPdf } from "../lib/ai-visibility-audit-action-plan/ava-pdf-archive-v1.js";
import {
  getAuditMeta,
  upsertAuditMeta,
  saveAuditVersion,
} from "../lib/ai-visibility-audit-action-plan/audit-store-v1.js";
import { assessCustomerQuality } from "../lib/ai-visibility-audit-action-plan/customer-quality-assurance-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const REPORT = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1"
);
const PAYLOAD_DIR = path.join(ROOT, "lib/ai-visibility-audit-action-plan/payloads");

const COHORT = [
  "waterstone-boca-raton-v1.json",
  "ac-hotel-a-coruna-v1.json",
  "castillo-hotel-son-vida-v1.json",
];

function sha256(b) {
  return crypto.createHash("sha256").update(b).digest("hex");
}

async function pdfMeta(buf) {
  const { getDocument } = await import("pdfjs-dist/legacy/build/pdf.mjs");
  const doc = await getDocument({
    data: new Uint8Array(buf),
    useSystemFonts: true,
  }).promise;
  let text = "";
  let linkCount = 0;
  const hosts = new Set();
  for (let i = 1; i <= doc.numPages; i++) {
    const page = await doc.getPage(i);
    const content = await page.getTextContent();
    text += content.items.map((it) => it.str).join(" ") + "\n";
    const annots = await page.getAnnotations();
    for (const a of annots) {
      if (a.subtype === "Link" && (a.url || a.unsafeUrl)) {
        linkCount += 1;
        try {
          hosts.add(new URL(a.url || a.unsafeUrl).hostname.replace(/^www\./, ""));
        } catch {
          /* ignore */
        }
      }
    }
  }
  return { pages: doc.numPages, text, linkCount, hosts: [...hosts].sort() };
}

async function processOne(file) {
  const slug = file.replace(/-v1\.json$/i, "");
  const payloadPath = path.join(PAYLOAD_DIR, file);
  const payload = JSON.parse(fs.readFileSync(payloadPath, "utf8"));
  const propertyId = payload.property?.propertyId;
  const priorMeta = getAuditMeta(propertyId) || {};

  const outDir = path.join(REPORT, slug, "pdf-staged-v4");
  fs.mkdirSync(outDir, { recursive: true });
  const filename = buildAiVisibilityAuditPdfFilename({
    hotelName: `${payload.property?.name || slug} V4`,
    reportMonthLabel: payload.reporting?.reportMonthLabel || "NOVEMBER2026",
    date: payload.reporting?.generatedAt,
  }).replace(" V4", "");
  // Keep distinct V4 filename
  const v4Name = filename.replace(
    "AI Visibility Audit & Action Plan",
    "AI Visibility Audit & Action Plan V4"
  );
  const outPath = path.join(outDir, v4Name);

  await generateAiVisibilityAuditPdfV1({
    payload,
    outPath,
    pagesDir: path.join(outDir, "page-previews"),
    buildHtml: buildAiVisibilityAuditPdfHtmlV4,
    minSections: 6,
  });
  const pdfBuf = fs.readFileSync(outPath);
  const metaPdf = await pdfMeta(pdfBuf);
  const cqa = assessCustomerQuality(payload, { pdfBuffer: pdfBuf });

  const versionId = `ava_v4_design_${slug.replace(/-/g, "_")}_${Date.now().toString(36)}`;
  const staged = stageAvaPdf({
    propertyId,
    generatedAbsPath: outPath,
    versionId,
    filename: v4Name,
  });

  // Preserve V3 draft identity for Admin comparison
  upsertAuditMeta(propertyId, {
    v3DraftPdfSha256: priorMeta.draftPdfSha256 || priorMeta.v3DraftPdfSha256 || null,
    v3DraftPdfRelativePath:
      priorMeta.draftPdfRelativePath || priorMeta.v3DraftPdfRelativePath || null,
    v3DraftVersionId: priorMeta.draftVersionId || priorMeta.v3DraftVersionId || null,
    draftVersionId: versionId,
    draftPdfRelativePath: staged.draftPdfRelativePath || staged.pdfRelativePath,
    draftPdfSha256: staged.sha256,
    draftUpdatedAt: new Date().toISOString(),
    draftTemplate: "V4",
    forceStatus: true,
  });
  saveAuditVersion(propertyId, versionId, payload, {
    status: "AUDIT_VALIDATED",
    metaStatus: "AUDIT_VALIDATED",
    forceStatus: true,
    kind: "REPORT_VERSION_V4",
    pdfRelativePath: staged.draftPdfRelativePath || staged.pdfRelativePath,
    researchCostUsd: 0,
    channel: "draft",
    template: "V4",
  });

  const hasExec = /Executive Audit Summary/i.test(metaPdf.text);
  const hasPriority = /Priority Findings/i.test(metaPdf.text);
  const hasRoadmap = /Implementation Roadmap|Prioritized Implementation/i.test(
    metaPdf.text
  );

  return {
    slug,
    hotel: payload.property?.name,
    propertyId,
    pages: metaPdf.pages,
    linkAnnotations: metaPdf.linkCount,
    distinctHosts: metaPdf.hosts.length,
    sha256: sha256(pdfBuf),
    cqaPass: cqa.overallPass === true,
    hasExec,
    hasPriority,
    hasRoadmap,
    priorV3DraftSha: priorMeta.draftPdfSha256 || null,
    publishedSha: priorMeta.publishedPdfSha256 || null,
    filename: v4Name,
    afterPdf: path.relative(ROOT, outPath).replace(/\\/g, "/"),
    draftPdfRelativePath: staged.draftPdfRelativePath || staged.pdfRelativePath,
  };
}

async function main() {
  process.env.AVA_STORAGE_MODE = process.env.AVA_STORAGE_MODE || "local";
  const results = [];
  console.log("=== AVA V4 design pilot (3 hotels) ===");
  for (const file of COHORT) {
    console.log("V4", file);
    try {
      const row = await processOne(file);
      results.push(row);
      console.log(
        `  ${row.hotel} pages=${row.pages} links=${row.linkAnnotations} sha=${row.sha256.slice(0, 12)} exec=${row.hasExec} CQA=${row.cqaPass}`
      );
    } catch (err) {
      console.error(err);
      results.push({ slug: file, ok: false, error: String(err?.stack || err) });
    }
  }
  const out = {
    contract: "AVA_V4_DESIGN_PILOT_V1",
    generatedAt: new Date().toISOString(),
    paidResearchUsd: 0,
    publicationAuthorized: false,
    deploymentAuthorized: false,
    adpMeasurementsChanged: false,
    phaseAPrerequisite: "AVA_CUSTOMER_RESEARCH_READINESS_CERTIFICATION design-pilot PASS",
    results,
  };
  fs.writeFileSync(
    path.join(REPORT, "AVA_V4_DESIGN_PILOT_RESULTS.json"),
    `${JSON.stringify(out, null, 2)}\n`
  );
  console.log("\nWrote AVA_V4_DESIGN_PILOT_RESULTS.json");
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
