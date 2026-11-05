/**
 * Apply permanent autonomous evidence stage to six existing AVA audits.
 * Bethesda = regression only (no content mutation expected).
 *
 *   node scripts/ava-autonomous-evidence-six-hotel-certify-v1.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { runMandatoryAvaEvidenceStages } from "../lib/ai-visibility-audit-action-plan/ava-full-e2e-pipeline-v1.js";
import {
  getAuditMeta,
  upsertAuditMeta,
  saveAuditVersion,
} from "../lib/ai-visibility-audit-action-plan/audit-store-v1.js";
import { generateAiVisibilityAuditPdfV1 } from "../lib/ai-visibility-audit-action-plan/generate-audit-pdf-v1.mjs";
import { stageAvaPdf } from "../lib/ai-visibility-audit-action-plan/ava-pdf-archive-v1.js";
import { buildAiVisibilityAuditPdfFilename } from "../lib/ai-visibility-audit-action-plan/ava-pdf-filename-v1.js";
import { summarizeCertificationForAdmin } from "../lib/ai-visibility-audit-action-plan/evidence-hyperlink-certification-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const REPORT_DIR = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1"
);

const HOTELS = [
  {
    propertyId: "adp_waterstone_boca_raton",
    slug: "waterstone-boca-raton",
    payload: "lib/ai-visibility-audit-action-plan/payloads/waterstone-boca-raton-v1.json",
    mode: "remediate",
  },
  {
    propertyId: "adp_castillo_hotel_son_vida",
    slug: "castillo-hotel-son-vida",
    payload: "lib/ai-visibility-audit-action-plan/payloads/castillo-hotel-son-vida-v1.json",
    mode: "remediate",
  },
  {
    propertyId: "adp_sheraton_mallorca_arabella_golf",
    slug: "sheraton-mallorca-arabella-golf",
    payload:
      "lib/ai-visibility-audit-action-plan/payloads/sheraton-mallorca-arabella-golf-v1.json",
    mode: "remediate",
  },
  {
    propertyId: "adp_wyndham_san_diego_bayside",
    slug: "wyndham-san-diego-bayside",
    payload:
      "lib/ai-visibility-audit-action-plan/payloads/wyndham-san-diego-bayside-v1.json",
    mode: "remediate",
  },
  {
    propertyId: "adp_spice_island_beach_resort",
    slug: "spice-island-beach-resort",
    payload:
      "lib/ai-visibility-audit-action-plan/payloads/spice-island-beach-resort-v1.json",
    mode: "remediate",
  },
  {
    propertyId: "adp_bethesda_marriott",
    slug: "bethesda-marriott",
    payload: "lib/ai-visibility-audit-action-plan/payloads/bethesda-marriott-v1.json",
    mode: "regression",
  },
];

async function processHotel(hotel) {
  const payloadPath = path.join(ROOT, hotel.payload);
  const beforePayload = JSON.parse(fs.readFileSync(payloadPath, "utf8"));
  const priorMeta = getAuditMeta(hotel.propertyId) || {};
  const beforeStatus = priorMeta.status || beforePayload.status;
  const founder = priorMeta.founderApprovedForCustomer === true;

  const outDir = path.join(REPORT_DIR, hotel.slug, "pdf-staged");
  fs.mkdirSync(outDir, { recursive: true });
  const filename = buildAiVisibilityAuditPdfFilename({
    hotelName: beforePayload.property.name,
    reportMonthLabel: beforePayload.reporting?.reportMonthLabel,
    date: beforePayload.reporting?.generatedAt,
  });
  const outPath = path.join(outDir, filename);

  const existingPdf = fs.existsSync(outPath) ? fs.readFileSync(outPath) : null;

  const pipeline = await runMandatoryAvaEvidenceStages(beforePayload, {
    pdfBuffer: existingPdf,
    dryRun: true,
    persistCache: true,
    manualVerifications:
      priorMeta.evidenceHyperlinkCertification?.manualVerifications || {},
    generatePdfFn: async ({ payload }) => {
      await generateAiVisibilityAuditPdfV1({
        payload,
        outPath,
        pagesDir: null,
        bridgeOutPath: path.join(outDir, "proposed-monthly-action-register-bridge.json"),
      });
      return { buffer: fs.readFileSync(outPath), path: outPath };
    },
  });

  const remediation = pipeline.result;
  const certification = remediation.certification;
  const repaired =
    (remediation.urlsRepaired || []).length > 0 ||
    (remediation.claimsRevised || []).length > 0 ||
    (remediation.claimsRemoved || []).length > 0;

  // Regression hotel: do not persist content mutations
  if (hotel.mode === "regression" && repaired) {
    console.warn(
      "REGRESSION_MUTATION_UNEXPECTED",
      hotel.propertyId,
      remediation.urlsRepaired,
      remediation.claimsRevised?.length
    );
  }

  if (repaired && hotel.mode !== "regression") {
    fs.writeFileSync(
      payloadPath,
      `${JSON.stringify(remediation.payload, null, 2)}\n`
    );
  }

  let pdfSha = priorMeta.pdfSha256 || null;
  let pdfRel = priorMeta.pdfRelativePath || null;
  let versionCreated = false;

  if (repaired && hotel.mode !== "regression") {
    versionCreated = true;
    const versionId = `ava_v1_autonomous_evidence_${Date.now().toString(36)}`;
    if (!fs.existsSync(outPath)) {
      await generateAiVisibilityAuditPdfV1({
        payload: remediation.payload,
        outPath,
        pagesDir: null,
      });
    }
    const staged = stageAvaPdf({
      propertyId: hotel.propertyId,
      generatedAbsPath: outPath,
      versionId,
      filename,
    });
    pdfSha = staged.sha256;
    pdfRel = staged.pdfRelativePath;
    saveAuditVersion(hotel.propertyId, versionId, remediation.payload, {
      status: certification.gate.pass ? "AUDIT_VALIDATED" : "VALIDATION_REQUIRED",
      metaStatus: certification.gate.pass ? "AUDIT_VALIDATED" : "VALIDATION_REQUIRED",
      forceStatus: true,
      kind: "REPORT_VERSION",
      pdfRelativePath: pdfRel,
      researchCostUsd: 0,
      validation: {
        enhancement: "autonomous-evidence-resolution-v1",
        evidenceHyperlinkGatePass: certification.gate.pass,
        founderApprovedForCustomer: founder,
        customerReadyBlocked: true,
        pdfSha256: pdfSha,
      },
    });
  }

  const nextStatus = certification.gate.pass
    ? beforeStatus === "VALIDATION_REQUIRED"
      ? "AUDIT_VALIDATED"
      : beforeStatus === "AUDIT_VALIDATED" ||
          beforeStatus === "FOUNDER_REVIEWED_STAGED" ||
          beforeStatus === "FOUNDER_APPROVED" ||
          beforeStatus === "FOUNDER_APPROVED_FOR_CUSTOMER"
        ? beforeStatus
        : "AUDIT_VALIDATED"
    : "VALIDATION_REQUIRED";

  // Never change founder flags
  upsertAuditMeta(hotel.propertyId, {
    status: nextStatus,
    forceStatus: true,
    evidencePipelineStatus: remediation.pipelineStatus,
    pdfRelativePath: pdfRel || priorMeta.pdfRelativePath,
    pdfSha256: pdfSha || priorMeta.pdfSha256,
    founderApprovedForCustomer: founder,
    founderApprovedForCustomerAt: priorMeta.founderApprovedForCustomerAt || null,
    evidenceHyperlinkCertification: {
      ...(priorMeta.evidenceHyperlinkCertification || {}),
      lastRun: summarizeCertificationForAdmin(certification),
      lastFullReport: {
        checkedAt: certification.checkedAt,
        gate: certification.gate,
        summary: certification.summary,
        evidence: certification.evidence,
      },
    },
    autonomousEvidenceResolution: {
      lastRun: {
        finishedAt: remediation.finishedAt,
        summary: remediation.summary,
        resolutionLog: remediation.resolutionLog,
        urlsRepaired: remediation.urlsRepaired,
        claimsRevised: remediation.claimsRevised,
        claimsRemoved: remediation.claimsRemoved,
        founderReviewDiffs: remediation.founderReviewDiffs,
        mode: hotel.mode,
      },
    },
  });

  return {
    propertyId: hotel.propertyId,
    name: beforePayload.property.name,
    mode: hotel.mode,
    beforeStatus,
    status: nextStatus,
    gatePass: certification.gate.pass,
    pipelineStatus: remediation.pipelineStatus,
    automaticallyRepaired: remediation.summary.automaticallyRepaired,
    claimsRevisedOrRemoved: remediation.summary.claimsRevisedOrRemoved,
    remainingUnresolved: remediation.summary.remainingUnresolved,
    resolutionLogCount: (remediation.resolutionLog || []).length,
    versionCreated,
    founderApprovedForCustomer: founder,
    evidenceSummary: certification.summary,
  };
}

async function main() {
  process.env.AVA_STORAGE_MODE = process.env.AVA_STORAGE_MODE || "local";
  const rows = [];
  for (const h of HOTELS) {
    console.log("Certifying", h.propertyId, h.mode, "...");
    const row = await processHotel(h);
    rows.push(row);
    console.log(
      " →",
      row.gatePass ? "PASS" : "FAIL",
      row.status,
      "repaired",
      row.automaticallyRepaired
    );
  }

  const report = {
    contract: "AVA_AUTONOMOUS_EVIDENCE_SIX_HOTEL_CERT_V1",
    generatedAt: new Date().toISOString(),
    paidResearchUsd: 0,
    hotels: rows,
    totals: {
      hotels: rows.length,
      gatePass: rows.filter((r) => r.gatePass).length,
      gateFail: rows.filter((r) => !r.gatePass).length,
      repaired: rows.reduce((n, r) => n + (r.automaticallyRepaired || 0), 0),
      versionsCreated: rows.filter((r) => r.versionCreated).length,
    },
  };

  fs.mkdirSync(REPORT_DIR, { recursive: true });
  const outJson = path.join(
    REPORT_DIR,
    "AVA_AUTONOMOUS_EVIDENCE_SIX_HOTEL_RESULTS.json"
  );
  fs.writeFileSync(outJson, `${JSON.stringify(report, null, 2)}\n`);
  console.log(JSON.stringify(report.totals, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
