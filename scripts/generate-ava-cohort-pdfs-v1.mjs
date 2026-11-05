/**
 * Generate staged AI Visibility Audit PDFs for the validation cohort.
 * No publish, no paid research, no Airtable writes.
 */
import fs from "node:fs";
import path from "node:path";
import { fileURLToPath } from "node:url";
import { generateAiVisibilityAuditPdfV1 } from "../lib/ai-visibility-audit-action-plan/generate-audit-pdf-v1.mjs";
import {
  saveAuditVersion,
  upsertAuditMeta,
} from "../lib/ai-visibility-audit-action-plan/audit-store-v1.js";

const ROOT = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

const HOTELS = [
  {
    payload: "lib/ai-visibility-audit-action-plan/payloads/wyndham-san-diego-bayside-v1.json",
    slug: "wyndham-san-diego-bayside",
  },
  {
    payload: "lib/ai-visibility-audit-action-plan/payloads/waterstone-boca-raton-v1.json",
    slug: "waterstone-boca-raton",
  },
  {
    payload: "lib/ai-visibility-audit-action-plan/payloads/bethesda-marriott-v1.json",
    slug: "bethesda-marriott",
  },
  {
    payload: "lib/ai-visibility-audit-action-plan/payloads/castillo-hotel-son-vida-v1.json",
    slug: "castillo-hotel-son-vida",
  },
  {
    payload: "lib/ai-visibility-audit-action-plan/payloads/sheraton-mallorca-arabella-golf-v1.json",
    slug: "sheraton-mallorca-arabella-golf",
  },
];

function safeName(name) {
  return String(name || "Property")
    .replace(/[^\w\s-]+/g, "")
    .trim()
    .replace(/\s+/g, " ");
}

async function main() {
  const results = [];
  for (const h of HOTELS) {
    const audit = JSON.parse(fs.readFileSync(path.join(ROOT, h.payload), "utf8"));
    const outDir = path.join(
      ROOT,
      "reports/dealality-ai-visibility-audit-action-plan-pilot-v1",
      h.slug,
      "pdf-staged"
    );
    fs.mkdirSync(outDir, { recursive: true });
    const filename = `Dealality - AI Visibility Audit & Action Plan - ${safeName(
      audit.property.name
    )} - NOVEMBER2026.pdf`;
    const outPath = path.join(outDir, filename);
    const pagesDir = path.join(outDir, "page-previews");
    console.log("Generating", audit.property.propertyId, "...");
    const result = await generateAiVisibilityAuditPdfV1({
      payload: audit,
      outPath,
      pagesDir,
      bridgeOutPath: path.join(outDir, "proposed-monthly-action-register-bridge.json"),
    });
    const relPdf = path
      .relative(ROOT, outPath)
      .replace(/\\/g, "/");
    const versionId = `ava_v1_20261105_${h.slug.replace(/-/g, "_")}`;
    saveAuditVersion(audit.property.propertyId, versionId, audit, {
      status: "PDF_STAGED",
      metaStatus: "PDF_STAGED",
      forceStatus: true,
      pdfRelativePath: relPdf,
      researchCostUsd: 0,
      evidenceReviewStatus: "STAGED",
      validation: { contractOk: true, generatedAt: new Date().toISOString() },
    });
    results.push({
      propertyId: audit.property.propertyId,
      name: audit.property.name,
      pdf: relPdf,
      actions: audit.actions.length,
      filename: result.filename,
    });
    console.log("OK", result.filename);
  }

  const spice = JSON.parse(
    fs.readFileSync(
      path.join(
        ROOT,
        "lib/ai-visibility-audit-action-plan/payloads/spice-island-beach-resort-v1.json"
      ),
      "utf8"
    )
  );
  const spicePdf =
    "reports/dealality-ai-visibility-audit-action-plan-pilot-v1/spice-island-beach-resort/pdf-staged/Dealality - AI Visibility Audit & Action Plan - Spice Island Beach Resort - NOVEMBER2026-final.pdf";
  saveAuditVersion(
    spice.property.propertyId,
    "ava_v1_20261105_spice_island_founder_reviewed",
    spice,
    {
      status: "FOUNDER_REVIEWED_STAGED",
      metaStatus: "FOUNDER_REVIEWED_STAGED",
      forceStatus: true,
      pdfRelativePath: spicePdf,
      researchCostUsd: 0,
      evidenceReviewStatus: "FOUNDER_REVIEWED",
      validation: {
        contractOk: true,
        editorial: "FOUNDER_REVIEWED",
        generatedAt: new Date().toISOString(),
      },
    }
  );
  upsertAuditMeta(spice.property.propertyId, {
    customerReadinessStatus: "NOT_CUSTOMER_READY",
    publicationAuthorizationStatus: "NOT_AUTHORIZED",
  });

  const summaryPath = path.join(
    ROOT,
    "reports/dealality-ai-visibility-audit-action-plan-pilot-v1/COHORT_PDF_GENERATION_SUMMARY.json"
  );
  fs.writeFileSync(
    summaryPath,
    JSON.stringify(
      {
        ok: true,
        generatedAt: new Date().toISOString(),
        publicationAuthorized: false,
        paidResearchUsd: 0,
        results,
        spiceIsland: {
          status: "FOUNDER_REVIEWED_STAGED",
          pdf: spicePdf,
        },
      },
      null,
      2
    )
  );
  console.log(JSON.stringify({ ok: true, count: results.length, summaryPath }, null, 2));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
