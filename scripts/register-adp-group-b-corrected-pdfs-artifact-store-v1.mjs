#!/usr/bin/env node
/**
 * Idempotent batch registration for Group B corrected PDFs into the Full ADP
 * E2E PDF artifact store.
 *
 * AUTHORIZED locally / staging only by default.
 * Production registration requires --authorize-production-register=1 AND
 * founder publication authorization context — never run against prod blindly.
 *
 * Verifies for each hotel:
 *  - PDF exists and is valid %PDF
 *  - SHA matches package newPdfArtifactSha256 / payload.pdfArtifactSha256
 *  - periodId matches prepared package
 *  - propertyId matches
 *
 * Does NOT publish reports, rotate shares, or write Airtable.
 */
import "../load-env.js";
import { createHash } from "crypto";
import {
  existsSync,
  mkdirSync,
  readFileSync,
  readdirSync,
  writeFileSync,
} from "fs";
import { join } from "path";
import { registerFullAdpE2ePdfArtifact } from "../lib/ai-demand-positioning/full-adp-e2e-pdf/adp-full-e2e-pdf-artifact-store-v1.js";

const PKG_DIR =
  "reports/dealality-adp-permanent-full-e2e-certification-v1/prepared-group-b-packages";
const PDF_DIR =
  "reports/dealality-adp-permanent-full-e2e-certification-v1/prepared-group-b-pdfs";
const OUT_DIR = "reports/dealality-adp-clean-main-release-v1";
const DRY_RUN = !process.argv.includes("--apply");
const AUTHORIZE_PROD = process.argv.includes(
  "--authorize-production-register=1"
);
const ROOT_OVERRIDE = process.argv
  .find((a) => a.startsWith("--pdf-root="))
  ?.slice("--pdf-root=".length);

if (ROOT_OVERRIDE) {
  process.env.ADP_FULL_E2E_PDF_ROOT = ROOT_OVERRIDE;
}

function sha256File(fp) {
  return createHash("sha256").update(readFileSync(fp)).digest("hex");
}

mkdirSync(OUT_DIR, { recursive: true });

const packages = readdirSync(PKG_DIR).filter((f) =>
  f.endsWith(".corrected-package.json")
);
const rows = [];

for (const file of packages) {
  const pkgPath = join(PKG_DIR, file);
  const prepared = JSON.parse(readFileSync(pkgPath, "utf8"));
  const propertyId = prepared.propertyId;
  const periodId = prepared.periodId;
  const pdfPath = join(PDF_DIR, `${propertyId}.pdf`);
  const expectedSha =
    prepared.newPdfArtifactSha256 ||
    prepared.payload?.pdfArtifactSha256 ||
    null;

  const row = {
    propertyId,
    periodId,
    pdfPath,
    expectedSha,
    ok: false,
    registered: false,
  };

  if (!propertyId || !periodId) {
    row.error = "MISSING_PROPERTY_OR_PERIOD";
    rows.push(row);
    continue;
  }
  if (!existsSync(pdfPath)) {
    row.error = "PDF_MISSING";
    rows.push(row);
    continue;
  }
  const buf = readFileSync(pdfPath);
  if (buf.slice(0, 4).toString("utf8") !== "%PDF") {
    row.error = "PDF_MAGIC_INVALID";
    rows.push(row);
    continue;
  }
  const actualSha = sha256File(pdfPath);
  row.actualSha = actualSha;
  if (!expectedSha) {
    row.error = "PACKAGE_SHA_MISSING";
    rows.push(row);
    continue;
  }
  if (actualSha !== expectedSha) {
    row.error = "SHA_MISMATCH";
    rows.push(row);
    continue;
  }
  if (prepared.payload?.period?.periodId && prepared.payload.period.periodId !== periodId) {
    row.error = "PERIOD_MISMATCH_IN_PAYLOAD";
    rows.push(row);
    continue;
  }

  row.prechecksPass = true;

  if (DRY_RUN) {
    row.ok = true;
    row.dryRun = true;
    rows.push(row);
    continue;
  }

  if (
    String(process.env.ADP_FULL_E2E_PDF_ROOT || "").includes("adp-durable") &&
    !AUTHORIZE_PROD
  ) {
    row.error = "PRODUCTION_REGISTER_NOT_AUTHORIZED";
    rows.push(row);
    continue;
  }

  try {
    const stagedVersionId = `group_b_corrected_pdf_${periodId}`;
    const result = registerFullAdpE2ePdfArtifact({
      propertyId,
      stagedVersionId,
      pdfAbsolutePath: pdfPath,
      periodId,
      generatedBy: "register_adp_group_b_corrected_pdfs_artifact_store_v1",
      sourceNote:
        "Group B corrected offline PDF — SHA/period verified; publicationAuthorized=false unless separately approved",
      publicationStatus: "NOT_AUTHORIZED",
      replaceWithHistory: true,
    });
    row.ok = result.ok === true;
    row.registered = true;
    row.alreadyRegistered = result.alreadyRegistered === true;
    row.pdfSha256 = result.pdfSha256 || actualSha;
    rows.push(row);
  } catch (e) {
    row.error = String(e.message || e);
    row.code = e.code || null;
    rows.push(row);
  }
}

const summary = {
  contract: "ADP_GROUP_B_CORRECTED_PDF_BATCH_REGISTER_V1",
  dryRun: DRY_RUN,
  authorizeProductionRegister: AUTHORIZE_PROD,
  total: rows.length,
  ok: rows.filter((r) => r.ok).length,
  failed: rows.filter((r) => !r.ok).length,
  registered: rows.filter((r) => r.registered).length,
  rows,
};

const outPath = join(OUT_DIR, "03-group-b-pdf-register-dry-run.json");
writeFileSync(outPath, `${JSON.stringify(summary, null, 2)}\n`);
console.log(
  JSON.stringify(
    {
      ok: summary.failed === 0,
      dryRun: DRY_RUN,
      total: summary.total,
      passed: summary.ok,
      failed: summary.failed,
      outPath,
    },
    null,
    2
  )
);
process.exit(summary.failed ? 1 : 0);
