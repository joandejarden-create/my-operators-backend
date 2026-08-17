#!/usr/bin/env node
/**
 * Approved Intelligence Field Suggestions v1 — read-only Mode B review queue.
 *
 * Usage:
 *   npm run approved-intelligence-field-suggestions -- --entity-type operator --target-rec-id rec...
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import { loadFieldPublishingAuditForEntity } from "../lib/partner-intelligence/field-publishing-entity-loader.js";
import {
  buildFieldSuggestionsFromAudit,
  buildFieldSuggestionsMarkdown,
  rejectSuggestionsApplyFlags,
  suggestionsReportFileNames,
} from "../lib/partner-intelligence/approved-intelligence-field-suggestions.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");

function argValue(flag) {
  const idx = process.argv.indexOf(flag);
  if (idx === -1) return "";
  return String(process.argv[idx + 1] || "").trim();
}

const ENTITY_TYPE = argValue("--entity-type");
const TARGET_REC_ID = argValue("--target-rec-id");

function writeSuggestionReports(report) {
  const paths = suggestionsReportFileNames(report.targetRecId);
  const reportsDir = join(ROOT, "reports");
  mkdirSync(reportsDir, { recursive: true });
  const json = JSON.stringify(report, null, 2);
  const md = buildFieldSuggestionsMarkdown(report);
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
  const applyReject = rejectSuggestionsApplyFlags();
  if (applyReject.rejected) {
    console.error(applyReject.message);
    process.exit(1);
  }

  if (!ENTITY_TYPE || !TARGET_REC_ID) {
    console.error(
      "Usage: npm run approved-intelligence-field-suggestions -- --entity-type brand|operator --target-rec-id rec..."
    );
    process.exit(1);
  }

  if (!["brand", "operator"].includes(ENTITY_TYPE)) {
    console.error(`Unsupported --entity-type "${ENTITY_TYPE}"`);
    process.exit(1);
  }

  const { audit, sources, facts } = await loadFieldPublishingAuditForEntity(
    ENTITY_TYPE,
    TARGET_REC_ID
  );
  const report = buildFieldSuggestionsFromAudit(audit, sources, facts);
  const files = writeSuggestionReports(report);

  console.log(
    `[approved-intelligence-field-suggestions] entity=${report.entityName} suggestions=${report.summary.totalSuggestions} controlled=${report.summary.controlledPublishCandidates} suggested_only=${report.summary.suggestedOnlyUpdates}`
  );
  for (const f of files) console.log(`Wrote ${f}`);
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
