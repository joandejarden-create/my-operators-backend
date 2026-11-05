/**
 * Wave 1 AVA: generate PDFs, stage archive, set AUDIT_VALIDATED (not founder customer approval).
 * Zero paid research. No publication.
 *
 *   node scripts/ava-wave1-generate-stage-validate-v1.mjs
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
    payload: "lib/ai-visibility-audit-action-plan/payloads/ac-hotel-a-coruna-v1.json",
    slug: "ac-hotel-a-coruna",
    quality: {
      overall: 9.2,
      dimensions: {
        executiveSummary: 9.3,
        competitivePositioning: 9.1,
        commercialRelevance: 9.3,
        evidenceQuality: 9.4,
        actionSpecificity: 9.2,
        gmReadability: 9.2,
        agencyUsefulness: 9.1,
        visualConsistency: 9.0,
      },
      strongestFindings: [
        "Meetings room-count / capacity / address contradictions across Marriott and CVB",
        "AC Lounge exists while ADP on-site restaurant recognition was 0%",
        "Spanish Marriott URL returned 403 for automated research probe",
      ],
    },
  },
  {
    payload:
      "lib/ai-visibility-audit-action-plan/payloads/grand-fiesta-americana-chapultepec-v1.json",
    slug: "grand-fiesta-americana-chapultepec",
    quality: {
      overall: 9.1,
      dimensions: {
        executiveSummary: 9.2,
        competitivePositioning: 9.0,
        commercialRelevance: 9.2,
        evidenceQuality: 9.3,
        actionSpecificity: 9.1,
        gmReadability: 9.1,
        agencyUsefulness: 9.0,
        visualConsistency: 9.0,
      },
      strongestFindings: [
        "Low consideration (4.7%) despite recognized spa/restaurant assets",
        "Meetings capacity language conflict (220 vs Forbes 300 reference)",
        "Forbes Travel Guide listing exists but official award copy is vague",
      ],
    },
  },
  {
    payload:
      "lib/ai-visibility-audit-action-plan/payloads/fiesta-americana-condesa-cancun-ai-v1.json",
    slug: "fiesta-americana-condesa-cancun-ai",
    quality: {
      overall: 9.1,
      dimensions: {
        executiveSummary: 9.2,
        competitivePositioning: 9.2,
        commercialRelevance: 9.1,
        evidenceQuality: 9.2,
        actionSpecificity: 9.1,
        gmReadability: 9.1,
        agencyUsefulness: 9.0,
        visualConsistency: 9.0,
      },
      strongestFindings: [
        "#1 of 4 Fiesta Americana AI peers but only 2.8% absolute consideration",
        "Official “10 restaurants and bars” vs Booking “15 restaurants” language",
        "Under-recognized fitness / watersports / honeymoon signals vs strong AI/beach/spa recognition",
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
      pagesDir: path.join(outDir, "page-previews-wave1"),
      bridgeOutPath: path.join(outDir, "proposed-monthly-action-register-bridge.json"),
    });

    const versionId = `ava_v1_wave1_${h.slug.replace(/-/g, "_")}`;
    const staged = stageAvaPdf({
      propertyId,
      generatedAbsPath: outPath,
      versionId,
      filename,
    });

    // Also sync legacy archive current without deleting prior version files
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
          path.join(legacyDir, `pre_wave1_${priorSha.slice(0, 12)}.pdf`)
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
        wave: "WAVE_1",
        phase: "ava-full-portfolio-production-v1",
        auditValidated: true,
        qualityOverall: h.quality.overall,
        qualityDimensions: h.quality.dimensions,
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
    // strip forceStatus pollution
    const meta = getAuditMeta(propertyId);
    if (meta?.forceStatus !== undefined) {
      delete meta.forceStatus;
      fs.writeFileSync(
        path.join(
          ROOT,
          "data/ava-data/audits",
          propertyId,
          "AUDIT_META.json"
        ),
        `${JSON.stringify(meta, null, 2)}\n`
      );
    }

    results.push({
      propertyId,
      name: audit.property.name,
      wave: 1,
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
    contract: "AVA_WAVE1_PRODUCTION_BATCH_V1",
    generatedAt: new Date().toISOString(),
    publicationAuthorized: false,
    paidResearchUsd: 0,
    results,
  };
  const outJson = path.join(
    ROOT,
    "reports/dealality-ai-visibility-audit-action-plan-pilot-v1/AVA_WAVE1_BATCH_CHECKPOINT.json"
  );
  fs.writeFileSync(outJson, `${JSON.stringify(summary, null, 2)}\n`);
  console.log(JSON.stringify({ ok: true, count: results.length, outJson }, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
