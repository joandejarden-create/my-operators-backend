#!/usr/bin/env node
/**
 * Generate corrected PDFs for Group B packages (local only, register:false).
 * Does not mutate production durable registry or Airtable.
 */
import "../load-env.js";
import { existsSync, mkdirSync, readFileSync, writeFileSync, readdirSync } from "fs";
import { join } from "path";
import { saveStagedAdpReport } from "../lib/ai-demand-positioning/review/adp-staged-report-store-v1.js";
import { generateFullAdpE2eReportPdfV1 } from "../lib/ai-demand-positioning/full-adp-e2e-pdf/generate-full-adp-e2e-report-pdf-v1.mjs";
import { createHash } from "crypto";

const PKG_DIR =
  "reports/dealality-adp-permanent-full-e2e-certification-v1/prepared-group-b-packages";
const PDF_DIR =
  "reports/dealality-adp-permanent-full-e2e-certification-v1/prepared-group-b-pdfs";
const OUT =
  "reports/dealality-adp-final-full-e2e-completion-v1/02b-group-b-pdf-generation.json";
const BASE =
  process.argv.find((a) => a.startsWith("--base="))?.slice("--base=".length) ||
  "http://127.0.0.1:8080";
const ONLY = process.argv.find((a) => a.startsWith("--only="))?.slice("--only=".length);

mkdirSync(PDF_DIR, { recursive: true });

function sha256File(fp) {
  return createHash("sha256").update(readFileSync(fp)).digest("hex");
}

const files = readdirSync(PKG_DIR)
  .filter((f) => f.endsWith(".corrected-package.json"))
  .filter((f) => !ONLY || f.startsWith(ONLY));

const rows = [];
for (const file of files) {
  const pkgPath = join(PKG_DIR, file);
  const prepared = JSON.parse(readFileSync(pkgPath, "utf8"));
  const propertyId = prepared.propertyId;
  const outPdf = join(PDF_DIR, `${propertyId}.pdf`);
  if (existsSync(outPdf) && prepared.newPdfArtifactSha256) {
    rows.push({
      propertyId,
      skipped: true,
      sha256: prepared.newPdfArtifactSha256,
      path: outPdf,
    });
    console.log(`SKIP existing ${propertyId}`);
    continue;
  }
  try {
    console.log(`PDF ${propertyId}...`);
    const staged = saveStagedAdpReport({
      propertyId,
      periodId: prepared.periodId,
      payload: prepared.payload,
      note: "group-b-corrected-pdf-offline",
      source: "generate-adp-group-b-corrected-pdfs-v1",
    });
    if (!staged.ok) throw new Error(staged.error || "stage_failed");
    const generated = await generateFullAdpE2eReportPdfV1({
      baseUrl: BASE.replace(/\/$/, ""),
      propertyId,
      stagedVersionId: staged.versionId,
      outPath: outPdf,
      register: false,
      generatedBy: "generate_adp_group_b_corrected_pdfs_v1",
    });
    const pdfSha = generated.pdfSha256 || sha256File(outPdf);
    prepared.newPdfArtifactSha256 = pdfSha;
    prepared.pdfPath = outPdf.replace(/\\/g, "/");
    prepared.payload.pdfArtifactSha256 = pdfSha;
    writeFileSync(pkgPath, `${JSON.stringify(prepared, null, 2)}\n`);
    writeFileSync(
      join(PDF_DIR, `${propertyId}.pdf-manifest.json`),
      `${JSON.stringify(
        {
          propertyId,
          sha256: pdfSha,
          path: prepared.pdfPath,
          stagedVersionId: staged.versionId,
          generatedAt: new Date().toISOString(),
        },
        null,
        2
      )}\n`
    );
    rows.push({ propertyId, ok: true, sha256: pdfSha, path: prepared.pdfPath });
    console.log(`OK ${propertyId} ${pdfSha.slice(0, 12)}`);
  } catch (e) {
    rows.push({ propertyId, ok: false, error: String(e.message || e) });
    console.error(`FAIL ${propertyId}: ${e.message || e}`);
  }
}

const summary = {
  total: rows.length,
  ok: rows.filter((r) => r.ok || r.skipped).length,
  failed: rows.filter((r) => r.ok === false).length,
  rows,
};
mkdirSync("reports/dealality-adp-final-full-e2e-completion-v1", { recursive: true });
writeFileSync(OUT, `${JSON.stringify(summary, null, 2)}\n`);
console.log(JSON.stringify({ ok: summary.failed === 0, ...summary }, null, 2));
process.exit(summary.failed ? 1 : 0);
