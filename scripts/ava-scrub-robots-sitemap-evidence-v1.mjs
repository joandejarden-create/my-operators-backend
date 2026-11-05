/**
 * Remove robots.txt / sitemap.xml from customer-facing AVA evidence registers
 * (they are technical probes, not customer hyperlinks) and recertify listed hotels.
 *
 *   node scripts/ava-scrub-robots-sitemap-evidence-v1.mjs
 */
import crypto from "node:crypto";
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { runMandatoryAvaEvidenceStages } from "../lib/ai-visibility-audit-action-plan/ava-full-e2e-pipeline-v1.js";
import { generateAiVisibilityAuditPdfV1 } from "../lib/ai-visibility-audit-action-plan/generate-audit-pdf-v1.mjs";
import { buildAiVisibilityAuditPdfFilename } from "../lib/ai-visibility-audit-action-plan/ava-pdf-filename-v1.js";
import { stageAvaPdf } from "../lib/ai-visibility-audit-action-plan/ava-pdf-archive-v1.js";
import {
  getAuditMeta,
  upsertAuditMeta,
  saveAuditVersion,
} from "../lib/ai-visibility-audit-action-plan/audit-store-v1.js";
import { summarizeCertificationForAdmin } from "../lib/ai-visibility-audit-action-plan/evidence-hyperlink-certification-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");
const REPORT = path.join(
  ROOT,
  "reports/dealality-ai-visibility-audit-action-plan-pilot-v1"
);

const HOTELS = [
  {
    propertyId: "adp_cambridge_beaches_bermuda",
    file: "cambridge-beaches-bermuda-v1.json",
    slug: "cambridge-beaches-bermuda",
  },
  {
    propertyId: "adp_hilton_times_square",
    file: "hilton-times-square-v1.json",
    slug: "hilton-times-square",
  },
  {
    propertyId: "adp_radisson_blu_toronto_downtown",
    file: "radisson-blu-toronto-downtown-v1.json",
    slug: "radisson-blu-toronto-downtown",
  },
];

const DROP_RE = /robots\.txt|sitemap\.xml/i;

function scrub(value) {
  if (typeof value === "string") {
    return DROP_RE.test(value) ? null : value;
  }
  if (Array.isArray(value)) {
    return value.map(scrub).filter((v) => v != null && v !== "");
  }
  if (value && typeof value === "object") {
    if (typeof value.url === "string" && DROP_RE.test(value.url)) return null;
    const out = {};
    for (const [k, v] of Object.entries(value)) {
      const next = scrub(v);
      if (next != null) out[k] = next;
    }
    return out;
  }
  return value;
}

async function main() {
  process.env.AVA_STORAGE_MODE = process.env.AVA_STORAGE_MODE || "local";
  const results = [];
  for (const h of HOTELS) {
    const payloadPath = path.join(
      ROOT,
      "lib/ai-visibility-audit-action-plan/payloads",
      h.file
    );
    let payload = scrub(JSON.parse(fs.readFileSync(payloadPath, "utf8")));
    fs.writeFileSync(payloadPath, `${JSON.stringify(payload, null, 2)}\n`);
    const outDir = path.join(REPORT, h.slug, "pdf-staged");
    fs.mkdirSync(outDir, { recursive: true });
    const filename = buildAiVisibilityAuditPdfFilename({
      hotelName: payload.property.name,
      reportMonthLabel: payload.reporting?.reportMonthLabel || "NOVEMBER2026",
    });
    const outPath = path.join(outDir, filename);
    const prior = getAuditMeta(h.propertyId) || {};
    const pipeline = await runMandatoryAvaEvidenceStages(payload, {
      dryRun: true,
      persistCache: true,
      forcePdfRegenerate: true,
      generatePdfFn: async ({ payload: p }) => {
        await generateAiVisibilityAuditPdfV1({ payload: p, outPath, pagesDir: null });
        return { buffer: fs.readFileSync(outPath), path: outPath };
      },
    });
    payload = pipeline.result.payload;
    fs.writeFileSync(payloadPath, `${JSON.stringify(payload, null, 2)}\n`);
    const pdfSha = crypto
      .createHash("sha256")
      .update(fs.readFileSync(outPath))
      .digest("hex");
    const gatePass = pipeline.result.certification?.gate?.pass === true;
    const versionId = `ava_v1_scrub_robots_${Date.now().toString(36)}`;
    const staged = stageAvaPdf({
      propertyId: h.propertyId,
      generatedAbsPath: outPath,
      versionId,
      filename,
    });
    saveAuditVersion(h.propertyId, versionId, payload, {
      status: gatePass ? "AUDIT_VALIDATED" : "VALIDATION_REQUIRED",
      metaStatus: gatePass ? "AUDIT_VALIDATED" : "VALIDATION_REQUIRED",
      forceStatus: true,
      kind: "REPORT_VERSION",
      pdfRelativePath: staged.pdfRelativePath,
      researchCostUsd: 0,
      validation: {
        enhancement: "scrub-robots-sitemap-evidence-v1",
        evidenceHyperlinkGatePass: gatePass,
        founderApprovedForCustomer: prior.founderApprovedForCustomer === true,
        customerReadyBlocked: true,
        pdfSha256: staged.sha256,
      },
    });
    upsertAuditMeta(h.propertyId, {
      status: gatePass ? "AUDIT_VALIDATED" : "VALIDATION_REQUIRED",
      forceStatus: true,
      pdfRelativePath: staged.pdfRelativePath,
      pdfSha256: pdfSha,
      founderApprovedForCustomer: prior.founderApprovedForCustomer === true,
      evidenceHyperlinkCertification: {
        ...(prior.evidenceHyperlinkCertification || {}),
        lastRun: summarizeCertificationForAdmin(pipeline.result.certification),
      },
    });
    results.push({
      propertyId: h.propertyId,
      gatePass,
      reasons: pipeline.result.certification?.gate?.reasons || [],
    });
    console.log(h.propertyId, gatePass ? "PASS" : "FAIL");
  }
  console.log(JSON.stringify(results, null, 2));
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
