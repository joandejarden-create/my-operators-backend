#!/usr/bin/env node
/**
 * CLI: generate Full ADP E2E report PDF from certified staged payload.
 * Usage:
 *   node lib/.../run-generate-full-adp-e2e-report-pdf-v1.mjs --property <id> [--staged <ver>] [--replace-with-history]
 */
import { generateFullAdpE2eReportPdfV1 } from "./generate-full-adp-e2e-report-pdf-v1.mjs";

function arg(name, fallback = null) {
  const i = process.argv.indexOf(name);
  if (i === -1) return fallback;
  return process.argv[i + 1] || fallback;
}

const propertyId = arg("--property") || arg("--hotel");
if (!propertyId) {
  console.error("Usage: --property <adp_property_id> [--staged <versionId>] [--replace-with-history] [--base http://127.0.0.1:8080]");
  process.exit(1);
}

const result = await generateFullAdpE2eReportPdfV1({
  propertyId,
  stagedVersionId: arg("--staged"),
  baseUrl: arg("--base", process.env.VERIFY_BASE || "http://127.0.0.1:8080"),
  replaceWithHistory: process.argv.includes("--replace-with-history"),
  generatedBy: "cli_generate_full_adp_e2e_report_pdf_v1",
});
console.log(JSON.stringify(result, null, 2));
