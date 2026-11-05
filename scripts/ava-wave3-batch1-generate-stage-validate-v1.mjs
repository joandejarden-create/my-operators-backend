/**
 * Wave 3 Batch 1 AVA: generate PDFs, stage, and set AUDIT_VALIDATED.
 * Zero paid research. No publication or customer-ready approval.
 *
 *   node scripts/ava-wave3-batch1-generate-stage-validate-v1.mjs
 */
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { generateAiVisibilityAuditPdfV1 } from "../lib/ai-visibility-audit-action-plan/generate-audit-pdf-v1.mjs";
import { assertAuditPdfPayload } from "../lib/ai-visibility-audit-action-plan/audit-pdf-contract-v1.js";
import {
  getAuditMeta,
  saveAuditVersion,
  upsertAuditMeta,
} from "../lib/ai-visibility-audit-action-plan/audit-store-v1.js";
import { stageAvaPdf } from "../lib/ai-visibility-audit-action-plan/ava-pdf-archive-v1.js";
import { buildAiVisibilityAuditPdfFilename } from "../lib/ai-visibility-audit-action-plan/ava-pdf-filename-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const QUALITY_PATH = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1/AVA_WAVE3_BATCH1_QUALITY_NOTES.json"
);
const QUALITY = JSON.parse(fs.readFileSync(QUALITY_PATH, "utf8"));

const HOTELS = [
  {
    payload: "lib/ai-visibility-audit-action-plan/payloads/jw-marriott-santo-domingo-v1.json",
    slug: "jw-marriott-santo-domingo",
  },
  {
    payload: "lib/ai-visibility-audit-action-plan/payloads/umusic-hotel-madrid-v1.json",
    slug: "umusic-hotel-madrid",
  },
  {
    payload: "lib/ai-visibility-audit-action-plan/payloads/st-regis-cap-cana-v1.json",
    slug: "st-regis-cap-cana",
  },
];

function sha256File(abs) {
  return crypto.createHash("sha256").update(fs.readFileSync(abs)).digest("hex");
}

async function main() {
  const results = [];
  for (const h of HOTELS) {
    const audit = JSON.parse(fs.readFileSync(path.join(ROOT, h.payload), "utf8"));
    const gate = assertAuditPdfPayload(audit);
    if (!gate.ok) throw new Error(`${h.slug} INVALID ${gate.errors.join("; ")}`);

    const propertyId = audit.property.propertyId;
    const quality = QUALITY.hotels.find((row) => row.propertyId === propertyId);
    if (!quality || quality.score < 9) {
      throw new Error(`${h.slug} QUALITY_NOTE_MISSING_OR_BELOW_9`);
    }

    const outDir = path.join(
      ROOT,
      "reports/dealality-ai-visibility-audit-action-plan-pilot-v1",
      h.slug,
      "pdf-staged"
    );
    fs.mkdirSync(outDir, { recursive: true });
    const filename = buildAiVisibilityAuditPdfFilename({
      hotelName: audit.property.name,
      reportMonthLabel: audit.reporting.reportMonthLabel,
      date: audit.reporting.generatedAt,
    });
    const outPath = path.join(outDir, filename);
    console.log("Generating", propertyId, "...");
    await generateAiVisibilityAuditPdfV1({
      payload: audit,
      outPath,
      pagesDir: path.join(outDir, "page-previews-wave3-batch1"),
      bridgeOutPath: path.join(outDir, "proposed-monthly-action-register-bridge.json"),
    });

    const versionId = `ava_v1_wave3_batch1_${h.slug.replace(/-/g, "_")}`;
    const staged = stageAvaPdf({
      propertyId,
      generatedAbsPath: outPath,
      versionId,
      filename,
    });

    const legacyDir = path.join(
      ROOT,
      "reports/ai-visibility-audit-action-plan/archive",
      propertyId
    );
    fs.mkdirSync(legacyDir, { recursive: true });
    const legacyCurrent = path.join(legacyDir, "current.pdf");
    if (fs.existsSync(legacyCurrent)) {
      const priorSha = sha256File(legacyCurrent);
      if (priorSha !== staged.sha256) {
        fs.copyFileSync(
          legacyCurrent,
          path.join(legacyDir, `pre_wave3_batch1_${priorSha.slice(0, 12)}.pdf`)
        );
      }
    }
    fs.copyFileSync(outPath, legacyCurrent);
    fs.copyFileSync(outPath, path.join(legacyDir, `${versionId}.pdf`));
    fs.copyFileSync(outPath, path.join(legacyDir, filename));

    saveAuditVersion(propertyId, versionId, audit, {
      status: "AUDIT_VALIDATED",
      metaStatus: "AUDIT_VALIDATED",
      forceStatus: true,
      kind: "REPORT_VERSION",
      pdfRelativePath: staged.pdfRelativePath,
      researchCostUsd: 0,
      evidenceReviewStatus: "AUDIT_VALIDATED",
      validation: {
        contractOk: true,
        wave: "WAVE_3_BATCH_1",
        phase: "ava-full-portfolio-production-v1",
        auditValidated: true,
        qualityOverall: quality.score,
        strongestFindings: quality.strongestFindings,
        founderApprovedForCustomer: false,
        customerReadyBlocked: true,
        paidResearchUsd: 0,
        generatedAt: new Date().toISOString(),
        pdfSha256: staged.sha256,
      },
    });
    upsertAuditMeta(propertyId, {
      pdfRelativePath: staged.pdfRelativePath,
      pdfSha256: staged.sha256,
      auditValidatedAt: new Date().toISOString(),
      founderApprovedForCustomer: false,
      customerReadinessStatus: "NOT_CUSTOMER_READY",
      publicationAuthorizationStatus: "NOT_AUTHORIZED",
      researchCostUsd: 0,
      forceStatus: true,
    });

    const meta = getAuditMeta(propertyId);
    if (meta?.forceStatus !== undefined) {
      upsertAuditMeta(propertyId, { forceStatus: undefined });
    }

    results.push({
      propertyId,
      name: audit.property.name,
      wave: 3,
      batch: 1,
      status: "AUDIT_VALIDATED",
      founderApprovedForCustomer: false,
      actionCount: audit.actions.length,
      qualityOverall: quality.score,
      strongestFindings: quality.strongestFindings,
      filename,
      pdf: path.relative(ROOT, outPath).replace(/\\/g, "/"),
      stagedPath: staged.pdfRelativePath,
      sha256: staged.sha256,
      versionId,
      researchCostUsd: 0,
    });
    console.log("OK", filename, staged.sha256.slice(0, 12));
  }

  const summary = {
    ok: true,
    contract: "AVA_WAVE3_BATCH1_PRODUCTION_V1",
    generatedAt: new Date().toISOString(),
    publicationAuthorized: false,
    paidResearchUsd: 0,
    results,
  };
  const outJson = path.join(
    ROOT,
    "reports/dealality-ai-visibility-audit-action-plan-pilot-v1/AVA_WAVE3_BATCH1_CHECKPOINT.json"
  );
  fs.writeFileSync(outJson, `${JSON.stringify(summary, null, 2)}\n`);
  console.log(JSON.stringify({ ok: true, count: results.length, outJson }, null, 2));
}

main().catch((error) => {
  console.error(error);
  process.exit(1);
});
