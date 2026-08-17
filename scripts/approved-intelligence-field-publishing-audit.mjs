#!/usr/bin/env node
/**
 * Approved Intelligence → Platform Field Publishing v1 — read-only audit.
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  auditReportFileNames,
  buildFieldPublishingAuditMarkdown,
  rejectFieldPublishingApplyFlags,
} from "../lib/partner-intelligence/approved-intelligence-field-publishing.js";
import { loadFieldPublishingAuditForEntity } from "../lib/partner-intelligence/field-publishing-entity-loader.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");

function argValue(flag) {
  const idx = process.argv.indexOf(flag);
  if (idx === -1) return "";
  return String(process.argv[idx + 1] || "").trim();
}

const ENTITY_TYPE = argValue("--entity-type");
const TARGET_REC_ID = argValue("--target-rec-id");

function writeAuditReports(audit) {
  const paths = auditReportFileNames(audit.targetRecId);
  const reportsDir = join(ROOT, "reports");
  mkdirSync(reportsDir, { recursive: true });
  const json = JSON.stringify(audit, null, 2);
  const md = buildFieldPublishingAuditMarkdown(audit);
  const files = [
    join(reportsDir, paths.latestJson),
    join(reportsDir, paths.latestMd),
    join(reportsDir, paths.perEntityJson),
    join(reportsDir, paths.perEntityMd),
  ];
  writeFileSync(files[0], json, "utf8");
  writeFileSync(files[1], md, "utf8");
  writeFileSync(files[2], json, "utf8");
  writeFileSync(files[3], md, "utf8");
  return files;
}

async function main() {
  const applyReject = rejectFieldPublishingApplyFlags();
  if (applyReject.rejected) {
    console.error(applyReject.message);
    process.exit(1);
  }

  if (!ENTITY_TYPE || !TARGET_REC_ID) {
    console.error(
      "Usage: npm run approved-intelligence-field-publishing-audit -- --entity-type brand|operator --target-rec-id rec..."
    );
    process.exit(1);
  }

  if (!["brand", "operator"].includes(ENTITY_TYPE)) {
    console.error(`Unsupported --entity-type "${ENTITY_TYPE}"`);
    process.exit(1);
  }

  const { audit } = await loadFieldPublishingAuditForEntity(ENTITY_TYPE, TARGET_REC_ID);
  const files = writeAuditReports(audit);

  console.log(
    `[approved-intelligence-field-publishing-audit] entity=${audit.entityName} approved=${audit.summary.approvedFacts} suggested=${audit.summary.suggestedUpdate} controlled=${audit.summary.controlledPublishCandidate}`
  );
  for (const f of files) console.log(`Wrote ${f}`);
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(err.code === "NOT_FOUND" ? 1 : 1);
});
