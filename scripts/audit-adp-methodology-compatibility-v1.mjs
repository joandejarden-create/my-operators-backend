#!/usr/bin/env node
/**
 * Read-only ADP methodology compatibility audit → reports matrix.
 * Usage: node scripts/audit-adp-methodology-compatibility-v1.mjs
 */
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  buildAdpMethodologyCompatibilityAudit,
  renderMethodologyMatrixMarkdown,
} from "../lib/ai-demand-positioning/methodology/adp-methodology-compatibility-audit-v1.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const root = join(__dirname, "..");
const outDir = join(root, "reports/ai-demand-positioning/methodology");

const audit = buildAdpMethodologyCompatibilityAudit({ cwd: root });
mkdirSync(outDir, { recursive: true });

const jsonPath = join(outDir, "adp-methodology-compatibility-matrix-v1.json");
const mdPath = join(outDir, "adp-methodology-compatibility-matrix-v1.md");
const summaryPath = join(outDir, "FOUNDER_METHODOLOGY_AUDIT_SUMMARY_V1.md");

writeFileSync(jsonPath, JSON.stringify(audit, null, 2));
writeFileSync(mdPath, renderMethodologyMatrixMarkdown(audit));

const summary = [
  "# ADP Methodology Consistency — Founder Audit Summary",
  "",
  `Generated: ${audit.generatedAt}`,
  "",
  "## Authoritative methodology",
  "",
  `- Gate: \`${audit.gate}\``,
  `- Appearance: \`${audit.canonicalFingerprint.appearanceMethodologyId}\``,
  `- Presence Index: \`${audit.canonicalFingerprint.presenceIndexMethodologyId}\``,
  `- Reality Gap (current): \`${audit.canonicalFingerprint.realityGapMethodologyId}\``,
  `- Reality Gap (historical immutable): \`${audit.canonicalFingerprint.realityGapHistoricalImmutableId}\``,
  `- Executive composition: \`${audit.canonicalFingerprint.executiveCompositionVersion}\``,
  `- Measurement contract: \`${audit.canonicalFingerprint.measurementContractVersion}\``,
  `- Performance Review builder/template/PDF: \`${audit.canonicalFingerprint.performanceReviewBuilder}\` / \`${audit.canonicalFingerprint.performanceReviewTemplate}\` / \`${audit.canonicalFingerprint.performanceReviewPdf}\``,
  "",
  "## Portfolio classification",
  "",
  `| Class | Count |`,
  `|---|---:|`,
  `| CURRENT_COMPARABLE | ${audit.comparableCount} |`,
  `| RECOMPUTABLE | ${audit.recomputableCount} |`,
  `| PARTIALLY_COMPARABLE | ${audit.partiallyComparableCount} |`,
  `| HISTORICAL_ONLY | ${audit.historicalOnlyCount} |`,
  `| EVIDENCE_INSUFFICIENT | ${audit.evidenceInsufficientCount} |`,
  `| Live inventory | ${audit.inventoryCount} |`,
  `| Legitimate comparison set | ${audit.legitimateCount} |`,
  "",
  "## Notes",
  "",
  ...audit.notes.map((n) => `- ${n}`),
  "",
  `Full matrix: \`${mdPath.replace(/\\/g, "/")}\``,
  "",
].join("\n");

writeFileSync(summaryPath, summary);

console.log(
  JSON.stringify(
    {
      ok: true,
      inventoryCount: audit.inventoryCount,
      legitimateCount: audit.legitimateCount,
      byCompatibility: audit.byCompatibility,
      jsonPath,
      mdPath,
      summaryPath,
    },
    null,
    2
  )
);
