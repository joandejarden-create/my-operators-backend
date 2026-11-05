/**
 * Restage Wave 3 Batch 1 PDFs into durable AVA store + legacy archive.
 *   node scripts/ava-restage-wave3-batch1-durable-v1.mjs
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import {
  getAuditMeta,
  saveAuditVersion,
  upsertAuditMeta,
} from "../lib/ai-visibility-audit-action-plan/audit-store-v1.js";
import { stageAvaPdf } from "../lib/ai-visibility-audit-action-plan/ava-pdf-archive-v1.js";
import { assertAuditPdfPayload } from "../lib/ai-visibility-audit-action-plan/audit-pdf-contract-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const CP = JSON.parse(
  fs.readFileSync(
    path.join(
      ROOT,
      "reports/dealality-ai-visibility-audit-action-plan-pilot-v1/AVA_WAVE3_BATCH1_CHECKPOINT.json"
    ),
    "utf8"
  )
);

const QUALITY = {
  adp_jw_marriott_santo_domingo: 9.3,
  adp_umusic_hotel_madrid: 9.2,
  adp_st_regis_cap_cana: 9.4,
};

async function main() {
  const results = [];
  for (const row of CP.results) {
    const payloadRel = {
      adp_jw_marriott_santo_domingo:
        "lib/ai-visibility-audit-action-plan/payloads/jw-marriott-santo-domingo-v1.json",
      adp_umusic_hotel_madrid:
        "lib/ai-visibility-audit-action-plan/payloads/umusic-hotel-madrid-v1.json",
      adp_st_regis_cap_cana:
        "lib/ai-visibility-audit-action-plan/payloads/st-regis-cap-cana-v1.json",
    }[row.propertyId];
    const audit = JSON.parse(fs.readFileSync(path.join(ROOT, payloadRel), "utf8"));
    const gate = assertAuditPdfPayload(audit);
    if (!gate.ok) throw new Error(`${row.propertyId} ${gate.errors.join(";")}`);
    const pdfAbs = path.join(ROOT, row.pdf);
    if (!fs.existsSync(pdfAbs)) throw new Error(`MISSING_PDF ${pdfAbs}`);

    const staged = stageAvaPdf({
      propertyId: row.propertyId,
      generatedAbsPath: pdfAbs,
      versionId: row.versionId,
      filename: row.filename,
    });

    saveAuditVersion(row.propertyId, row.versionId, audit, {
      status: "AUDIT_VALIDATED",
      metaStatus: "AUDIT_VALIDATED",
      forceStatus: true,
      kind: "REPORT_VERSION",
      pdfRelativePath: staged.pdfRelativePath,
      researchCostUsd: 0,
      evidenceReviewStatus: "AUDIT_VALIDATED",
      validation: {
        contractOk: true,
        wave: "WAVE_3",
        batch: 1,
        auditValidated: true,
        qualityOverall: QUALITY[row.propertyId],
        founderApprovedForCustomer: false,
        customerReadyBlocked: true,
        paidResearchUsd: 0,
        pdfSha256: staged.sha256,
        generatedAt: new Date().toISOString(),
      },
    });
    upsertAuditMeta(row.propertyId, {
      pdfRelativePath: staged.pdfRelativePath,
      pdfSha256: staged.sha256,
      auditValidatedAt: new Date().toISOString(),
      founderApprovedForCustomer: false,
      customerReadinessStatus: "NOT_CUSTOMER_READY",
      publicationAuthorizationStatus: "NOT_AUTHORIZED",
      researchCostUsd: 0,
      forceStatus: true,
    });
    const meta = getAuditMeta(row.propertyId);
    if (meta?.forceStatus !== undefined) {
      delete meta.forceStatus;
      fs.writeFileSync(
        path.join(ROOT, "data/ava-data/audits", row.propertyId, "AUDIT_META.json"),
        `${JSON.stringify(meta, null, 2)}\n`
      );
    }
    results.push({
      propertyId: row.propertyId,
      status: "AUDIT_VALIDATED",
      stagedPath: staged.pdfRelativePath,
      sha256: staged.sha256,
      durable: staged.durable === true,
    });
    console.log("RESTORED", row.propertyId, staged.pdfRelativePath, staged.sha256.slice(0, 12));
  }
  console.log(JSON.stringify({ ok: true, results }, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
