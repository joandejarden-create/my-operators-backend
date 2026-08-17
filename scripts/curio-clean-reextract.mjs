#!/usr/bin/env node
/**
 * Curio narrow clean re-extraction — dry-run preview by default.
 * Allowlisted sources only; does not approve sources/facts or touch Setup governance.
 *
 * Usage:
 *   npm run curio-clean-reextract -- --dry-run
 *   npm run curio-clean-reextract -- --dry-run --source-ids "recy2pyEahF9UUsEk"
 *   npm run curio-clean-reextract -- --apply --approve-curio-clean-reextract
 *
 * Requires: AIRTABLE_API_KEY, AIRTABLE_BASE_ID
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import { getPartnerSourceById } from "../lib/partner-intelligence/airtable-source.js";
import { listPartnerFacts } from "../lib/partner-intelligence/airtable-facts.js";
import {
  CURIO_BRAND_ID,
  PRIMARY_CONTAMINATED_SOURCE_ID,
} from "../lib/partner-intelligence/curio-fact-contamination.js";
import {
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  ALLOWLISTED_SOURCE_IDS,
  DEFAULT_TARGET_FACT_KEYS,
  parseIdList,
  parseFactKeyList,
  resolveSourceIds,
  resolveTargetFactKeys,
  buildTargetKeyPlan,
  summarizeExistingFacts,
  previewCurioCleanSource,
  buildWouldWritePlan,
  applyCurioCleanReextract,
} from "../lib/partner-intelligence/curio-clean-reextract.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", REPORT_JSON_NAME);
const REPORT_MD = join(ROOT, "reports", REPORT_MD_NAME);

const APPLY = process.argv.includes("--apply");
const DRY_RUN = process.argv.includes("--dry-run") || !APPLY;
const APPROVE = process.argv.includes("--approve-curio-clean-reextract");

function argValue(flag) {
  const idx = process.argv.indexOf(flag);
  if (idx === -1) return "";
  return String(process.argv[idx + 1] || "").trim();
}

const SOURCE_IDS = resolveSourceIds(parseIdList(argValue("--source-ids")));
const TARGET_KEYS = resolveTargetFactKeys(parseFactKeyList(argValue("--fact-keys")));
const LIMIT_FACTS = Math.max(0, Number(argValue("--limit-facts") || "0") || 0) || null;

function validateCli() {
  if (APPLY && !APPROVE) {
    console.error("Apply requires both --apply and --approve-curio-clean-reextract.");
    process.exit(1);
  }
}

async function fetchCurioFacts() {
  const all = [];
  let offset = null;
  do {
    const page = await listPartnerFacts({ brandId: CURIO_BRAND_ID, limit: 100, offset });
    all.push(...(page.facts || []));
    offset = page.offset;
  } while (offset);
  return all;
}

function buildMarkdown(report) {
  const lines = [
    "# Curio Clean Re-Extraction",
    "",
    `Generated: ${report.generatedAt}`,
    `Mode: **${report.mode}**`,
    `Brand: Curio Collection by Hilton — \`${report.brandId}\``,
    "",
    "## Summary",
    "",
    `- Allowed source IDs: ${report.sourceIds.map((id) => `\`${id}\``).join(", ")}`,
    `- Target fact keys: ${report.targetFactKeys.length}`,
    `- Registry-supported keys: ${report.keyPlan.filter((k) => k.registrySupported).length}`,
    `- Registry-unsupported keys: ${report.keyPlan.filter((k) => !k.registrySupported).length}`,
    `- True extraction preview: **${report.trueExtractionPreviewPossible ? "yes" : "no"}**`,
    `- Existing fact contamination warnings: ${report.existingFacts.contaminatedCount}`,
    `- Facts that would be created on apply: ${report.wouldWrite.factsWouldCreateCount}`,
    "",
    report.recommendationNote,
    "",
    "## Target Fact Keys",
    "",
    "| Priority | Field key | Registry supported | Display label |",
    "|----------|-----------|-------------------|---------------|",
  ];

  for (const row of report.keyPlan) {
    lines.push(
      `| ${row.priority} | \`${row.fieldKey}\` | ${row.registrySupported ? "yes" : "**no**"} | ${row.displayLabel || "—"} |`
    );
  }
  lines.push("");

  if (report.existingFacts.rows.length) {
    lines.push("## Existing Facts (by source / key)", "");
    for (const row of report.existingFacts.rows) {
      lines.push(
        `- \`${row.sourceId}\` · \`${row.fieldKey}\` — ${row.existingCount} fact(s) ${row.factIds.map((id) => `\`${id}\``).join(", ")}`
      );
      for (const w of row.contaminationWarnings) {
        lines.push(
          `  - ⚠ \`${w.factId}\` (${w.humanReviewStatus}): ${w.reasons.join("; ")} — "${w.extractedValuePreview}"`
        );
      }
    }
    lines.push("");
  }

  for (const preview of report.sourcePreviews) {
    lines.push(`## Source: ${preview.sourceTitle} (\`${preview.sourceId}\`)`, "");
    lines.push(`- Validation: ${preview.validation.ok ? "pass" : `**blocked** — ${preview.validation.reasons.join("; ")}`}`);
    if (preview.previewAvailable) {
      lines.push(`- Document kind: ${preview.documentKind || "—"}`);
      lines.push(`- Classification role: ${preview.classificationRole || "—"}`);
      lines.push(`- Raw candidates from extractor: ${preview.rawCandidateCount}`);
      lines.push("");
      lines.push("### Preview candidates", "");
      if (!preview.previewCandidates.length) {
        lines.push("_No candidates for supported target keys._", "");
      } else {
        for (const c of preview.previewCandidates) {
          lines.push(`#### \`${c.fieldKey}\` (${c.priority})`, "");
          lines.push(`- Extracted preview: ${c.extractedValuePreview || "—"}`);
          lines.push(`- Evidence preview: ${c.evidencePreview || "—"}`);
          lines.push(`- Extraction: ${c.extractionType || "—"} · Confidence: ${c.confidenceLevel || "—"}`);
          if (c.contaminationWarnings?.length) {
            lines.push(`- **Contamination warning:** ${c.contaminationWarnings.join("; ")}`);
          }
          lines.push("");
        }
      }
    } else {
      lines.push(`- Preview skipped: ${preview.previewSkippedReason}`, "");
    }
  }

  lines.push("## Would Write On Apply", "");
  lines.push(`- Fact rows (clean): ${report.wouldWrite.factsWouldCreateCount}`);
  lines.push(`- Fact rows blocked by contamination preview: ${report.wouldWrite.factRowsBlockedByContamination.length}`);
  if (report.wouldWrite.unsupportedRegistryKeys.length) {
    lines.push(`- Unsupported registry keys (skipped): ${report.wouldWrite.unsupportedRegistryKeys.map((k) => `\`${k}\``).join(", ")}`);
  }
  lines.push("");
  lines.push("**Does not write:**");
  for (const item of report.wouldWrite.doesNotWrite) lines.push(`- ${item}`);
  lines.push("");

  if (report.applyResult) {
    lines.push("## Apply Result", "");
    lines.push(`- Run ID: \`${report.applyResult.runId}\``);
    lines.push(`- Sources patched: ${report.applyResult.sourcesPatched.length}`);
    lines.push(`- Facts created: ${report.applyResult.factsCreated.length}`);
    lines.push(`- Skipped: ${report.applyResult.skipped.length}`);
    lines.push("");
  }

  lines.push("## Post-Apply Recommendation", "");
  lines.push("```bash");
  lines.push(report.postApplyCommand);
  lines.push("```", "");
  lines.push("_Manually review extracted facts before any approval._", "");

  return lines.join("\n");
}

async function main() {
  validateCli();

  const apiKey = process.env.AIRTABLE_API_KEY;
  const baseId = process.env.AIRTABLE_BASE_ID;
  if (!apiKey || !baseId) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  console.log(
    `[curio-clean-reextract] mode=${DRY_RUN ? "dry-run" : "apply"} sources=${SOURCE_IDS.join(",")}`
  );

  const sources = [];
  for (const sourceId of SOURCE_IDS) {
    const source = await getPartnerSourceById(sourceId);
    if (!source) {
      console.error(`Source not found: ${sourceId}`);
      process.exit(1);
    }
    sources.push(source);
  }

  const allFacts = await fetchCurioFacts();
  const existingFacts = summarizeExistingFacts(allFacts, SOURCE_IDS, TARGET_KEYS);
  const keyPlan = buildTargetKeyPlan(TARGET_KEYS);

  const sourcePreviews = [];
  for (const source of sources) {
    const preview = await previewCurioCleanSource(source, TARGET_KEYS, {
      limitFacts: LIMIT_FACTS,
    });
    sourcePreviews.push(preview);
    console.log(
      `[curio-clean-reextract] ${source.id} preview=${preview.previewAvailable ? "ok" : "skipped"} candidates=${preview.previewCandidates?.length || 0}`
    );
  }

  const wouldWrite = buildWouldWritePlan(sourcePreviews, TARGET_KEYS);
  const trueExtractionPreviewPossible = sourcePreviews.some((p) => p.previewAvailable);

  let applyResult = null;
  if (APPLY && APPROVE) {
    applyResult = await applyCurioCleanReextract({
      sources,
      targetKeys: TARGET_KEYS,
      limitFacts: LIMIT_FACTS,
    });
    console.log(
      `[curio-clean-reextract] apply complete runId=${applyResult.runId} facts=${applyResult.factsCreated.length}`
    );
  }

  const report = {
    generatedAt: new Date().toISOString(),
    mode: DRY_RUN ? "dry-run" : "apply",
    brandId: CURIO_BRAND_ID,
    brandName: "Curio Collection by Hilton",
    sourceIds: SOURCE_IDS,
    allowlistedSourceIds: [...ALLOWLISTED_SOURCE_IDS],
    blockedSourceIds: [PRIMARY_CONTAMINATED_SOURCE_ID],
    targetFactKeys: TARGET_KEYS,
    defaultTargetFactKeys: DEFAULT_TARGET_FACT_KEYS,
    keyPlan,
    limitFacts: LIMIT_FACTS,
    trueExtractionPreviewPossible,
    recommendationNote: trueExtractionPreviewPossible
      ? "Dry-run uses read-only document extraction preview (`extractFromBrandSourceDocument`) — no Airtable fact writes in dry-run."
      : "Extraction preview unavailable for one or more sources — see per-source skip reasons. Report includes extraction plan only.",
    existingFacts,
    sourcePreviews,
    wouldWrite,
    applyResult,
    postApplyCommand:
      "npm run steward-partner-intelligence -- --entity-type brand --target-rec-id receQkxgjlezsc1xg --dry-run",
    refuses: [
      "Sources outside hard-coded allowlist",
      "Contaminated Mexico FDD source recIH5lyY8MASnfrp",
      "Broad extraction from all Curio brand sources",
      "Loyalty / Hilton Honors field keys",
      "Approving sources or facts",
      "Brand Basics profile governance updates",
      "Company Validated / Company Validation Date / Show Trust Label",
      "Apply without --approve-curio-clean-reextract",
    ],
  };

  mkdirSync(join(ROOT, "reports"), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, buildMarkdown(report), "utf8");
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${REPORT_MD}`);
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
