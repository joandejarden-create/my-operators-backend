/**
 * Customer Quality Assurance — six priority hotels + regression trio
 *   node scripts/ava-customer-quality-six-certify-v1.mjs
 */
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { runCustomerQualityAssuranceLoop } from "../lib/ai-visibility-audit-action-plan/customer-quality-remediation-v1.js";
import { assessCustomerQuality } from "../lib/ai-visibility-audit-action-plan/customer-quality-assurance-v1.js";
import { generateAiVisibilityAuditPdfV1 } from "../lib/ai-visibility-audit-action-plan/generate-audit-pdf-v1.mjs";
import { buildAiVisibilityAuditPdfFilename } from "../lib/ai-visibility-audit-action-plan/ava-pdf-filename-v1.js";
import { stageAvaPdf } from "../lib/ai-visibility-audit-action-plan/ava-pdf-archive-v1.js";
import {
  getAuditMeta,
  upsertAuditMeta,
  saveAuditVersion,
} from "../lib/ai-visibility-audit-action-plan/audit-store-v1.js";
import { runMandatoryAvaEvidenceStages } from "../lib/ai-visibility-audit-action-plan/ava-full-e2e-pipeline-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const REPORT = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1"
);

const PILOT = [
  { propertyId: "adp_waterstone_boca_raton", slug: "waterstone-boca-raton", payload: "waterstone-boca-raton-v1.json" },
  { propertyId: "adp_castillo_hotel_son_vida", slug: "castillo-hotel-son-vida", payload: "castillo-hotel-son-vida-v1.json" },
  { propertyId: "adp_sheraton_mallorca_arabella_golf", slug: "sheraton-mallorca-arabella-golf", payload: "sheraton-mallorca-arabella-golf-v1.json" },
  { propertyId: "adp_ac_hotel_a_coruna", slug: "ac-hotel-a-coruna", payload: "ac-hotel-a-coruna-v1.json" },
  { propertyId: "adp_grand_fiesta_americana_chapultepec", slug: "grand-fiesta-americana-chapultepec", payload: "grand-fiesta-americana-chapultepec-v1.json" },
  { propertyId: "adp_fiesta_americana_condesa_cancun_ai", slug: "fiesta-americana-condesa-cancun-ai", payload: "fiesta-americana-condesa-cancun-ai-v1.json" },
];

const REGRESSION = [
  { propertyId: "adp_bethesda_marriott", slug: "bethesda-marriott", payload: "bethesda-marriott-v1.json" },
  { propertyId: "adp_wyndham_san_diego_bayside", slug: "wyndham-san-diego-bayside", payload: "wyndham-san-diego-bayside-v1.json" },
  { propertyId: "adp_spice_island_beach_resort", slug: "spice-island-beach-resort", payload: "spice-island-beach-resort-v1.json" },
];

function sha256(buf) {
  return crypto.createHash("sha256").update(buf).digest("hex");
}

function sleepSync(ms) {
  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, ms);
}

function writeJson(file, data) {
  const body = `${JSON.stringify(data, null, 2)}\n`;
  let lastErr;
  for (let i = 0; i < 10; i++) {
    try {
      fs.writeFileSync(file, body);
      return;
    } catch (err) {
      lastErr = err;
      sleepSync(200 * (i + 1));
    }
  }
  throw lastErr;
}

async function processHotel(hotel, { mode }) {
  const payloadPath = path.join(
    ROOT,
    "lib/ai-visibility-audit-action-plan/payloads",
    hotel.payload
  );
  let payload = JSON.parse(fs.readFileSync(payloadPath, "utf8"));
  const priorMeta = getAuditMeta(hotel.propertyId) || {};
  const founder = priorMeta.founderApprovedForCustomer === true;

  const before = assessCustomerQuality(payload);
  const outDir = path.join(REPORT, hotel.slug, "pdf-staged");
  fs.mkdirSync(outDir, { recursive: true });
  const filename = buildAiVisibilityAuditPdfFilename({
    hotelName: payload.property.name,
    reportMonthLabel: payload.reporting?.reportMonthLabel || "NOVEMBER2026",
    date: payload.reporting?.generatedAt,
  });
  const outPath = path.join(outDir, filename);

  if (fs.existsSync(outPath)) {
    const bak = path.join(outDir, "pre-cqa");
    fs.mkdirSync(bak, { recursive: true });
    fs.copyFileSync(outPath, path.join(bak, `${Date.now()}-${filename}`));
  }

  const cqa = await runCustomerQualityAssuranceLoop(payload, {
    limits: { maxIterations: 3, maxPdfRegenerations: 2 },
    regeneratePdfFn: async (p) => {
      try {
        await generateAiVisibilityAuditPdfV1({
          payload: p,
          outPath,
          pagesDir: null,
        });
      } catch (err) {
        console.warn("PDF regen skipped:", err.message);
      }
    },
  });
  payload = cqa.payload;
  writeJson(payloadPath, payload);

  // Evidence + CQA via mandatory pipeline (CQA already applied; pipeline re-checks)
  const pipeline = await runMandatoryAvaEvidenceStages(payload, {
    dryRun: true,
    persistCache: true,
    forcePdfRegenerate: false,
    pdfBuffer: fs.existsSync(outPath) ? fs.readFileSync(outPath) : null,
    manualVerifications:
      priorMeta.evidenceHyperlinkCertification?.manualVerifications || {},
    generatePdfFn: async ({ payload: p }) => {
      await generateAiVisibilityAuditPdfV1({ payload: p, outPath, pagesDir: null });
      return { buffer: fs.readFileSync(outPath), path: outPath };
    },
  });
  payload = pipeline.result.payload;
  writeJson(payloadPath, payload);

  if (!fs.existsSync(outPath) || cqa.totalRepairs > 0) {
    await generateAiVisibilityAuditPdfV1({ payload, outPath, pagesDir: null });
  }

  const pdfBuf = fs.readFileSync(outPath);
  const finalAssess = assessCustomerQuality(payload, { pdfBuffer: pdfBuf });
  const evidencePass = pipeline.result.certification?.gate?.pass === true;
  const cqaPass = finalAssess.overallPass === true;
  const overall = evidencePass && cqaPass;

  const versionId = `ava_v1_cqa_${hotel.slug.replace(/-/g, "_")}_${Date.now().toString(36)}`;
  const staged = stageAvaPdf({
    propertyId: hotel.propertyId,
    generatedAbsPath: outPath,
    versionId,
    filename,
  });
  saveAuditVersion(hotel.propertyId, versionId, payload, {
    status: overall ? "AUDIT_VALIDATED" : "VALIDATION_REQUIRED",
    metaStatus: overall ? "AUDIT_VALIDATED" : "VALIDATION_REQUIRED",
    forceStatus: true,
    kind: "REPORT_VERSION",
    pdfRelativePath: staged.pdfRelativePath,
    researchCostUsd: 0,
    validation: {
      enhancement: "customer-quality-assurance-v1",
      evidenceHyperlinkGatePass: evidencePass,
      customerQualityCertified: cqaPass,
      founderApprovedForCustomer: founder,
      customerReadyBlocked: true,
      pdfSha256: staged.sha256,
    },
  });
  upsertAuditMeta(hotel.propertyId, {
    status: overall ? "AUDIT_VALIDATED" : "VALIDATION_REQUIRED",
    forceStatus: true,
    pdfRelativePath: staged.pdfRelativePath,
    pdfSha256: sha256(pdfBuf),
    founderApprovedForCustomer: founder,
    founderApprovedForCustomerAt: priorMeta.founderApprovedForCustomerAt || null,
    customerQualityAssurance: {
      contract: "AVA_CUSTOMER_QUALITY_ASSURANCE_V1",
      customerQualityCertified: cqaPass,
      assessedAt: finalAssess.assessedAt,
      totalRepairs: cqa.totalRepairs,
      gates: {
        researchQuality: finalAssess.gates.researchQuality.pass,
        evidenceQuality: finalAssess.gates.evidenceQuality.pass,
        communicationQuality: finalAssess.gates.communicationQuality.pass,
        pdfPresentationQuality: finalAssess.gates.pdfPresentationQuality.pass,
      },
      unresolvedMaterialIssues: finalAssess.defects.filter(
        (d) => d.severity === "CRITICAL" || d.severity === "HIGH"
      ),
      qaLog: (cqa.history || []).flatMap((h) => h.log || []).slice(-50),
      beforeDefectCount: before.defectCount,
      founderApprovedForCustomer: false,
      publicationAuthorized: false,
    },
  });

  return {
    propertyId: hotel.propertyId,
    name: payload.property.name,
    mode,
    beforeDefects: before.defectCount,
    repairs: cqa.totalRepairs,
    researchQuality: finalAssess.gates.researchQuality.pass ? "PASS" : "FAIL",
    evidenceQuality: finalAssess.gates.evidenceQuality.pass ? "PASS" : "FAIL",
    communicationQuality: finalAssess.gates.communicationQuality.pass
      ? "PASS"
      : "FAIL",
    pdfQuality: finalAssess.gates.pdfPresentationQuality.pass ? "PASS" : "FAIL",
    customerQualityCertified: cqaPass,
    evidenceGatePass: evidencePass,
    overall: overall ? "PASS" : "FAIL",
    remainingBlockers: overall
      ? []
      : finalAssess.defects
          .filter((d) => d.severity === "CRITICAL" || d.severity === "HIGH")
          .map((d) => d.message)
          .slice(0, 8),
    pdfSha256: sha256(pdfBuf),
    filename,
    founderApprovedForCustomer: founder,
  };
}

async function main() {
  process.env.AVA_STORAGE_MODE = process.env.AVA_STORAGE_MODE || "local";
  const rows = [];
  for (const h of PILOT) {
    console.log("CQA", h.propertyId, "...");
    const row = await processHotel(h, { mode: "PILOT" });
    rows.push(row);
    console.log(" →", row.overall, "repairs", row.repairs, "before", row.beforeDefects);
  }
  for (const h of REGRESSION) {
    console.log("CQA regression", h.propertyId, "...");
    const row = await processHotel(h, { mode: "REGRESSION" });
    rows.push(row);
    console.log(" →", row.overall, "repairs", row.repairs);
  }

  const report = {
    contract: "AVA_CUSTOMER_QUALITY_ASSURANCE_RESULTS_V1",
    generatedAt: new Date().toISOString(),
    paidResearchUsd: 0,
    publicationAuthorized: false,
    hotels: rows,
    totals: {
      pilot: rows.filter((r) => r.mode === "PILOT").length,
      pilotPass: rows.filter((r) => r.mode === "PILOT" && r.overall === "PASS")
        .length,
      regression: rows.filter((r) => r.mode === "REGRESSION").length,
      regressionPass: rows.filter(
        (r) => r.mode === "REGRESSION" && r.overall === "PASS"
      ).length,
    },
  };
  fs.mkdirSync(REPORT, { recursive: true });
  writeJson(
    path.join(REPORT, "AVA_CUSTOMER_QUALITY_ASSURANCE_RESULTS.json"),
    report
  );
  console.log(JSON.stringify(report.totals, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
