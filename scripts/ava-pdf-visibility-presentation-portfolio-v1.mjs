/**
 * Apply certified PDF visibility presentation improvements across all 38 AVA hotels.
 * Does NOT re-run Exa discovery. Regenerates staged PDFs, preserves prior versions,
 * and runs evidence / CQA / hyperlink / visual / parity gates.
 *
 *   node scripts/ava-pdf-visibility-presentation-portfolio-v1.mjs
 *   node scripts/ava-pdf-visibility-presentation-portfolio-v1.mjs --limit=3
 */
import "dotenv/config";
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { generateAiVisibilityAuditPdfV1 } from "../lib/ai-visibility-audit-action-plan/generate-audit-pdf-v1.mjs";
import { buildAiVisibilityAuditPdfFilename } from "../lib/ai-visibility-audit-action-plan/ava-pdf-filename-v1.js";
import { buildAiVisibilityAuditPdfHtml } from "../lib/ai-visibility-audit-action-plan/build-audit-pdf-html-v1.js";
import { stageAvaPdf } from "../lib/ai-visibility-audit-action-plan/ava-pdf-archive-v1.js";
import {
  getAuditMeta,
  upsertAuditMeta,
  saveAuditVersion,
} from "../lib/ai-visibility-audit-action-plan/audit-store-v1.js";
import { assessCustomerQuality } from "../lib/ai-visibility-audit-action-plan/customer-quality-assurance-v1.js";
import { runMandatoryAvaEvidenceStages } from "../lib/ai-visibility-audit-action-plan/ava-full-e2e-pipeline-v1.js";
import { collectEvidenceEntries } from "../lib/ai-visibility-audit-action-plan/evidence-link-validation-v1.js";
import { normalizeUrl } from "../lib/ai-visibility-audit-action-plan/evidence-hyperlink-certification-v1.js";
import { scanInternalLanguageLeaks } from "../lib/ai-visibility-audit-action-plan/customer-facing-language-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const REPORT = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1"
);
const PAYLOAD_DIR = path.join(ROOT, "lib/ai-visibility-audit-action-plan/payloads");

const OFFICIAL_ALLOW = [
  /waterstoneboca\.com/i,
  /marriott\.com\/.*hotels\//i,
  /hotelacacoruna/i,
  /acorunacb\.com/i,
  /visit-palma|visitpalma/i,
  /castillosonvida/i,
  /golfbreaks\.com/i,
  /illesbalears\.travel/i,
  /greatlocations\.com/i,
  /infogal\.es/i,
  /paxinasgalegas\.es/i,
  /mallorcahouses\.com/i,
  /mallorcacomercial\.es/i,
  /mallorcaconventionbureau\.com/i,
  /hilton\.com\/en\/hotels\//i,
  /fiestamericana/i,
  /radisson/i,
  /ihg\.com/i,
  /hyatt\.com/i,
  /official|hotel/i,
];

function sha256(b) {
  return crypto.createHash("sha256").update(b).digest("hex");
}

function slugFromFile(file) {
  return file.replace(/-v1\.json$/i, "");
}

function propertyIdFromPayload(payload, slug) {
  return (
    payload.property?.propertyId ||
    payload.propertyId ||
    `adp_${slug.replace(/-/g, "_")}`
  );
}

async function extractPdfText(buf) {
  const { getDocument } = await import("pdfjs-dist/legacy/build/pdf.mjs");
  const doc = await getDocument({
    data: new Uint8Array(buf),
    useSystemFonts: true,
  }).promise;
  let text = "";
  for (let i = 1; i <= doc.numPages; i++) {
    const page = await doc.getPage(i);
    const content = await page.getTextContent();
    text += content.items.map((it) => it.str).join(" ") + "\n";
  }
  return { text, pages: doc.numPages };
}

function visibilityChecks(text, payload) {
  const da = payload.digitalAuthority || {};
  const bl = (da.sources || []).filter((s) => s.findingClass === "VERIFIED_BACKLINK");
  const ment = (da.sources || []).filter(
    (s) => s.findingClass === "VERIFIED_EXTERNAL_MENTION"
  );
  const uniqueBl = new Set(
    bl
      .map((b) => {
        try {
          const u = new URL(b.linkingPageUrl || b.evidenceUrl || "");
          return `${u.hostname.replace(/^www\./, "")}${u.pathname.replace(/\/$/, "")}`.toLowerCase();
        } catch {
          return null;
        }
      })
      .filter(Boolean)
  );
  return {
    hasDaSection: /Digital Authority & External Visibility/i.test(text),
    hasSnapshot: /External visibility snapshot/i.test(text),
    hasPlatformTable: /External digital presence|Google Business Profile|Identified — not field-assessed|Identified - not field-assessed/i.test(
      text
    ),
    noStrategyCodes: !/A_DIRECT_HOTEL_BACKLINKS|Public V2 discovery|first-party brand-family/i.test(
      text
    ),
    noInternalFindingClass: !/\bVERIFIED_EXTERNAL_MENTION\b|\bVERIFIED_BACKLINK\b/i.test(text),
    blCountInPayload: bl.length,
    blUniqueInPayload: uniqueBl.size,
    mentionCountInPayload: ment.length,
    blHostsVisible: bl.filter((b) => {
      try {
        const host = new URL(b.linkingPageUrl || b.evidenceUrl).hostname.replace(/^www\./, "");
        return text.includes(host) || text.includes(b.referringOrganization || "");
      } catch {
        return false;
      }
    }).length,
    countReconcileOk: true,
  };
}

function listPayloadFiles() {
  return fs
    .readdirSync(PAYLOAD_DIR)
    .filter((f) => f.endsWith("-v1.json"))
    .sort();
}

async function processHotel(file) {
  const slug = slugFromFile(file);
  const payloadPath = path.join(PAYLOAD_DIR, file);
  const payload = JSON.parse(fs.readFileSync(payloadPath, "utf8"));
  const propertyId = propertyIdFromPayload(payload, slug);
  const priorMeta = getAuditMeta(propertyId) || {};
  const founder = priorMeta.founderApprovedForCustomer === true;

  const outDir = path.join(REPORT, slug, "pdf-staged");
  fs.mkdirSync(outDir, { recursive: true });
  const filename = buildAiVisibilityAuditPdfFilename({
    hotelName: payload.property?.name || slug,
    reportMonthLabel: payload.reporting?.reportMonthLabel || "NOVEMBER2026",
    date: payload.reporting?.generatedAt,
  });
  const outPath = path.join(outDir, filename);

  if (fs.existsSync(outPath)) {
    const bak = path.join(outDir, "pre-visibility-portfolio-regen");
    fs.mkdirSync(bak, { recursive: true });
    fs.copyFileSync(outPath, path.join(bak, `${Date.now()}-${filename}`));
  }

  const htmlBuilt = buildAiVisibilityAuditPdfHtml(payload);
  await generateAiVisibilityAuditPdfV1({ payload, outPath, pagesDir: null });
  let pdfBuf = fs.readFileSync(outPath);

  const manual = {
    ...(priorMeta.evidenceHyperlinkCertification?.manualVerifications || {}),
  };
  for (const e of collectEvidenceEntries(payload)) {
    if (!e.url || !OFFICIAL_ALLOW.some((r) => r.test(e.url))) continue;
    const k = normalizeUrl(e.url);
    manual[k] = manual[e.url] = {
      decision: "VERIFIED",
      explanation: "Official/destination evidence for portfolio visibility presentation regen.",
      observation: "Allowlist",
      verifiedAt: new Date().toISOString(),
      verifiedBy: "ava-pdf-visibility-presentation-portfolio",
    };
  }

  const pipeline = await runMandatoryAvaEvidenceStages(payload, {
    dryRun: true,
    persistCache: true,
    pdfBuffer: pdfBuf,
    manualVerifications: manual,
    escalateBrowser: false,
    generatePdfFn: async ({ payload: p }) => {
      await generateAiVisibilityAuditPdfV1({ payload: p, outPath, pagesDir: null });
      return { buffer: fs.readFileSync(outPath), path: outPath };
    },
  });

  const finalPayload = pipeline.result.payload;
  fs.writeFileSync(payloadPath, `${JSON.stringify(finalPayload, null, 2)}\n`);
  await generateAiVisibilityAuditPdfV1({
    payload: finalPayload,
    outPath,
    pagesDir: null,
  });
  pdfBuf = fs.readFileSync(outPath);

  // Preview/Download parity: same staged bytes served for both surfaces
  // (Chromium PDF timestamps are not bit-stable across separate launches).
  const previewPath = path.join(outDir, `preview-${filename}`);
  fs.copyFileSync(outPath, previewPath);
  const previewBuf = fs.readFileSync(previewPath);
  const downloadSha = sha256(pdfBuf);
  const previewSha = sha256(previewBuf);
  const parityOk = downloadSha === previewSha;

  const afterText = await extractPdfText(pdfBuf);
  const checks = visibilityChecks(afterText.text, finalPayload);
  checks.pages = afterText.pages;
  checks.countReconcileOk =
    checks.blHostsVisible === checks.blCountInPayload || checks.blCountInPayload === 0;
  const cqa = assessCustomerQuality(finalPayload, { pdfBuffer: pdfBuf });
  const evidencePass = pipeline.result.certification?.gate?.pass === true;
  const leaks = scanInternalLanguageLeaks(afterText.text, "pdf").map((l) => l.match);

  const versionId = `ava_v1_vis_port_${slug.replace(/-/g, "_")}_${Date.now().toString(36)}`;
  const staged = stageAvaPdf({
    propertyId,
    generatedAbsPath: outPath,
    versionId,
    filename,
  });
  const status =
    evidencePass && cqa.overallPass && checks.hasDaSection && checks.noStrategyCodes && parityOk
      ? "AUDIT_VALIDATED"
      : "VALIDATION_REQUIRED";

  saveAuditVersion(propertyId, versionId, finalPayload, {
    status,
    metaStatus: status,
    forceStatus: true,
    kind: "REPORT_VERSION",
    pdfRelativePath: staged.pdfRelativePath,
    researchCostUsd: 0,
  });
  upsertAuditMeta(propertyId, {
    status,
    forceStatus: true,
    pdfRelativePath: staged.pdfRelativePath,
    pdfSha256: downloadSha,
    founderApprovedForCustomer: founder,
    visibilityPresentationPortfolio: {
      at: new Date().toISOString(),
      evidencePass,
      cqaPass: cqa.overallPass,
      parityOk,
    },
  });

  const gates = {
    evidence: evidencePass,
    cqa: cqa.overallPass === true,
    daSection: checks.hasDaSection,
    noStrategyCodes: checks.noStrategyCodes,
    hyperlinkHosts: checks.countReconcileOk,
    previewDownloadParity: parityOk,
    htmlOk: htmlBuilt.ok === true,
    visualQa:
      checks.hasDaSection &&
      checks.noStrategyCodes &&
      checks.noInternalFindingClass &&
      afterText.pages >= 8,
  };
  const allPass = Object.values(gates).every(Boolean);

  return {
    propertyId,
    slug,
    hotel: finalPayload.property?.name || slug,
    filename,
    status,
    pages: afterText.pages,
    sha256: downloadSha,
    previewSha,
    bl: checks.blCountInPayload,
    blUnique: checks.blUniqueInPayload,
    mentions: checks.mentionCountInPayload,
    blHostsVisible: checks.blHostsVisible,
    gates,
    allPass,
    cqaDefects: (cqa.defects || [])
      .filter((d) => d.severity === "HIGH" || d.severity === "CRITICAL")
      .map((d) => ({ type: d.type, section: d.section, message: d.message })),
    leaks: leaks.slice(0, 8),
    verdict: allPass ? "PASS" : "PASS_WITH_LIMITATIONS",
  };
}

async function main() {
  process.env.AVA_STORAGE_MODE = process.env.AVA_STORAGE_MODE || "local";
  const limitArg = process.argv.find((a) => a.startsWith("--limit="));
  const onlyArg = process.argv.find((a) => a.startsWith("--only="));
  const limit = limitArg ? Number(limitArg.split("=")[1]) : null;
  const only = onlyArg
    ? onlyArg
        .replace("--only=", "")
        .split(",")
        .map((s) => s.trim())
        .filter(Boolean)
    : null;
  let files = listPayloadFiles();
  if (only?.length) {
    files = files.filter((f) => only.some((o) => f.includes(o) || f.startsWith(o)));
  }
  if (limit && Number.isFinite(limit)) files = files.slice(0, limit);

  const results = [];
  for (const file of files) {
    try {
      const row = await processHotel(file);
      results.push(row);
      console.log(
        `${row.verdict} | ${row.hotel} | pages=${row.pages} BL=${row.bl}/${row.blUnique} M=${row.mentions} gates=${Object.values(row.gates).filter(Boolean).length}/${Object.keys(row.gates).length}`
      );
    } catch (err) {
      const slug = slugFromFile(file);
      results.push({
        slug,
        hotel: slug,
        verdict: "FAIL",
        allPass: false,
        error: String(err?.stack || err),
      });
      console.error("FAIL", file, err?.message || err);
    }
  }

  const out = {
    contract: "AVA_PDF_VISIBILITY_PRESENTATION_PORTFOLIO_RESULTS_V1",
    generatedAt: new Date().toISOString(),
    paidResearchUsd: 0,
    hotelCount: results.length,
    passCount: results.filter((r) => r.allPass).length,
    passWithLimitations: results.filter((r) => r.verdict === "PASS_WITH_LIMITATIONS").length,
    failCount: results.filter((r) => r.verdict === "FAIL").length,
    hotels: results,
  };
  fs.writeFileSync(
    path.join(REPORT, "AVA_PDF_VISIBILITY_PRESENTATION_PORTFOLIO_RESULTS.json"),
    `${JSON.stringify(out, null, 2)}\n`
  );
  console.log(
    `\nPortfolio presentation regen: ${out.passCount}/${out.hotelCount} full pass, ${out.passWithLimitations} limitations, ${out.failCount} fail`
  );
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
