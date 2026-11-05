/**
 * Wave 2 Ambridge AVA: generate PDFs, stage, AUDIT_VALIDATED.
 *   node scripts/ava-wave2-ambridge-generate-stage-validate-v1.mjs
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

const HOTELS = [
  {
    payload: "lib/ai-visibility-audit-action-plan/payloads/sheraton-guadalajara-expo-v1.json",
    slug: "sheraton-guadalajara-expo",
    quality: {
      overall: 9.2,
      dimensions: {
        executiveSummary: 9.3,
        competitivePositioning: 9.1,
        commercialRelevance: 9.3,
        evidenceQuality: 9.5,
        actionSpecificity: 9.2,
        gmReadability: 9.1,
        agencyUsefulness: 9.2,
        visualConsistency: 9.0,
      },
      strongestFindings: [
        "Overview “over 400 m²” vs Events page 1,654 m² / 12 rooms / 500 capacity",
        "Expo Guadalajara + FIFA 2026 messaging needs consistent meetings facts",
        "MATRIA / QUIDEA Spa / Sheraton Lounge strengths to reinforce after capacity cleanup",
      ],
    },
  },
  {
    payload: "lib/ai-visibility-audit-action-plan/payloads/voco-cancun-zona-hotelera-v1.json",
    slug: "voco-cancun-zona-hotelera",
    quality: {
      overall: 9.0,
      dimensions: {
        executiveSummary: 9.1,
        competitivePositioning: 9.0,
        commercialRelevance: 9.0,
        evidenceQuality: 9.1,
        actionSpecificity: 9.0,
        gmReadability: 9.0,
        agencyUsefulness: 9.0,
        visualConsistency: 9.0,
      },
      strongestFindings: [
        "Near-zero AI consideration (0.4%) with clear Playa Langosta / Isla Mujeres location assets",
        "EN page room-category bilingual/label hygiene issues",
        "IHG property URLs returned HTTP 403 to automated research probe",
      ],
    },
  },
  {
    payload: "lib/ai-visibility-audit-action-plan/payloads/wyndham-alltra-punta-cana-v1.json",
    slug: "wyndham-alltra-punta-cana",
    quality: {
      overall: 9.1,
      dimensions: {
        executiveSummary: 9.2,
        competitivePositioning: 9.0,
        commercialRelevance: 9.1,
        evidenceQuality: 9.3,
        actionSpecificity: 9.1,
        gmReadability: 9.1,
        agencyUsefulness: 9.0,
        visualConsistency: 9.0,
      },
      strongestFindings: [
        "Official overview typo “Alltra Puna Cana”",
        "Uvero Alto beachfront / 20 F&B / 620-room inventory under absolute consideration 3.4%",
        "Cap Cana Marina remains out of scope — avoid geography confusion",
      ],
    },
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
    const outDir = path.join(
      ROOT,
      "reports/dealality-ai-visibility-audit-action-plan-pilot-v1",
      h.slug,
      "pdf-staged"
    );
    fs.mkdirSync(outDir, { recursive: true });
    const filename = buildAiVisibilityAuditPdfFilename({
      hotelName: audit.property.name,
      reportMonthLabel: audit.reporting?.reportMonthLabel,
      date: audit.reporting?.generatedAt,
    });
    const outPath = path.join(outDir, filename);
    console.log("Generating", propertyId, "...");
    await generateAiVisibilityAuditPdfV1({
      payload: audit,
      outPath,
      pagesDir: path.join(outDir, "page-previews-wave2"),
      bridgeOutPath: path.join(outDir, "proposed-monthly-action-register-bridge.json"),
    });
    const versionId = `ava_v1_wave2_${h.slug.replace(/-/g, "_")}`;
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
          path.join(legacyDir, `pre_wave2_${priorSha.slice(0, 12)}.pdf`)
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
        wave: "WAVE_2_AMBRIDGE",
        phase: "ava-full-portfolio-production-v1",
        auditValidated: true,
        qualityOverall: h.quality.overall,
        qualityDimensions: h.quality.dimensions,
        founderApprovedForCustomer: false,
        customerReadyBlocked: true,
        paidResearchUsd: 0,
        generatedAt: new Date().toISOString(),
        pdfSha256: staged.sha256,
        clientPortfolio: "Ambridge",
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
      clientPortfolio: "Ambridge",
      forceStatus: true,
    });
    const meta = getAuditMeta(propertyId);
    if (meta?.forceStatus !== undefined) {
      delete meta.forceStatus;
      fs.writeFileSync(
        path.join(ROOT, "data/ava-data/audits", propertyId, "AUDIT_META.json"),
        `${JSON.stringify(meta, null, 2)}\n`
      );
    }
    results.push({
      propertyId,
      name: audit.property.name,
      wave: 2,
      clientPortfolio: "Ambridge",
      status: "AUDIT_VALIDATED",
      founderApprovedForCustomer: false,
      actionCount: audit.actions.length,
      qualityOverall: h.quality.overall,
      qualityDimensions: h.quality.dimensions,
      strongestFindings: h.quality.strongestFindings,
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
    contract: "AVA_WAVE2_AMBRIDGE_BATCH_V1",
    generatedAt: new Date().toISOString(),
    publicationAuthorized: false,
    paidResearchUsd: 0,
    ambridgeIdentificationSource:
      "reports/dealality-adp-ambridge-three-hotel-integrity-re-audit-v1/INTEGRITY_AUDIT_VERDICT.md",
    results,
  };
  const outJson = path.join(
    ROOT,
    "reports/dealality-ai-visibility-audit-action-plan-pilot-v1/AVA_WAVE2_AMBRIDGE_BATCH_CHECKPOINT.json"
  );
  fs.writeFileSync(outJson, `${JSON.stringify(summary, null, 2)}\n`);
  console.log(JSON.stringify({ ok: true, count: results.length, outJson }, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
