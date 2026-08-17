#!/usr/bin/env node
/**
 * GHL Hoteles narrow operator extraction — dry-run preview by default.
 * Allowlisted English sources only; does not approve sources/facts or touch Setup governance.
 *
 * Usage:
 *   npm run ghl-hoteles-extract -- --dry-run
 *   npm run ghl-hoteles-extract -- --apply --approve-ghl-hoteles-extract
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
  GHL_OPERATOR_ID,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  ALLOWLISTED_SOURCE_IDS,
  EXCLUDED_SOURCE_ID,
  DEFAULT_TARGET_FACT_KEYS,
  PRIMARY_TARGET_FACT_KEYS,
  OPTIONAL_TARGET_FACT_KEYS,
  parseIdList,
  parseFactKeyList,
  resolveSourceIds,
  resolveTargetFactKeys,
  buildTargetKeyPlan,
  summarizeExistingFacts,
  previewGhlHotelesSource,
  buildWouldWritePlan,
  applyGhlHotelesExtract,
  assessExtractionQuality,
} from "../lib/partner-intelligence/ghl-hoteles-extract.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", REPORT_JSON_NAME);
const REPORT_MD = join(ROOT, "reports", REPORT_MD_NAME);

const APPLY = process.argv.includes("--apply");
const DRY_RUN = process.argv.includes("--dry-run") || !APPLY;
const APPROVE = process.argv.includes("--approve-ghl-hoteles-extract");

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
    console.error("Apply requires both --apply and --approve-ghl-hoteles-extract.");
    process.exit(1);
  }
}

async function fetchOperatorFacts() {
  const all = [];
  let offset = null;
  do {
    const page = await listPartnerFacts({ operatorId: GHL_OPERATOR_ID, limit: 100, offset });
    all.push(...(page.facts || []));
    offset = page.offset;
  } while (offset);
  return all;
}

function buildMarkdown(report) {
  const lines = [
    "# GHL Hoteles Narrow Extraction",
    "",
    `Generated: ${report.generatedAt}`,
    `Mode: **${report.mode}**`,
    `Operator: GHL Hoteles (GHL Holding) — \`${report.operatorId}\``,
    "",
    "## Summary",
    "",
    `- Sources in scope: ${report.sourceIds.map((id) => `\`${id}\``).join(", ")}`,
    `- Sources excluded: \`${report.excludedSourceId}\` (Spanish home — not in allowlist)`,
    `- Target fact keys requested: ${report.targetFactKeys.length}`,
    `- Registry-supported keys: ${report.keyPlan.filter((k) => k.registrySupported).length}`,
    `- Registry-unsupported keys: ${report.keyPlan.filter((k) => !k.registrySupported).length}`,
    `- HTML text clean (all sources): **${report.allHtmlTextClean ? "yes" : "see per-source"}**`,
    `- Existing facts (allowlisted sources): ${report.existingFacts.existingCount}`,
    `- Clean facts that would be created on apply: **${report.wouldWrite.factsWouldCreateCount}**`,
    `- Skipped candidates: ${report.wouldWrite.factRowsSkipped.length}`,
    `- Duplicate warnings: ${report.wouldWrite.duplicateWarnings?.length || 0}`,
    "",
  ];

  if (report.extractionQualityAssessment) {
    const eq = report.extractionQualityAssessment;
    lines.push(
      "## Extraction Quality Assessment",
      "",
      `- Overall: **${eq.overall}**`,
      `- Readable sources: ${eq.readableSourceCount}`,
      `- Substantive facts: ${eq.substantiveFactCount}`,
      `- Has company name: ${eq.hasCompanyName ? "yes" : "no"}`,
      `- Has regions: ${eq.hasRegions ? "yes" : "no"}`,
      `- Has brand families: ${eq.hasBrands ? "yes" : "no"}`,
      `- Has offered services: ${eq.hasOfferedServices ? "yes" : "no"}`,
      `- Apply recommended: **${eq.applyRecommended ? "yes — pending founder review" : "no — review report first"}**`,
      `- Governance publish still blocked: **${eq.governancePublishStillBlocked ? "yes" : "no"}** — ${eq.governanceBlockReason}`,
      ""
    );
  }

  lines.push(report.recommendationNote, "");

  lines.push(
    "## Target Fact Keys",
    "",
    "| Priority | Field key | Registry supported | Display label |",
    "|----------|-----------|-------------------|---------------|"
  );

  for (const row of report.keyPlan) {
    lines.push(
      `| ${row.priority} | \`${row.fieldKey}\` | ${row.registrySupported ? "yes" : "**no**"} | ${row.displayLabel || "—"} |`
    );
  }
  lines.push("");

  if (report.wouldWrite.unsupportedRegistryKeys.length) {
    lines.push(
      "## Unsupported Registry Keys",
      "",
      "_These keys were requested but are not in the Operator Explorer registry — no writes._",
      "",
      report.wouldWrite.unsupportedRegistryKeys.map((k) => `- \`${k}\``).join("\n"),
      ""
    );
  }

  if (report.wouldWrite.duplicateWarnings?.length) {
    lines.push("## Duplicate warnings", "");
    for (const w of report.wouldWrite.duplicateWarnings) {
      lines.push(`- \`${w.fieldKey}\` from \`${w.sourceId}\`: ${w.reasons?.join(", ")}`);
    }
    lines.push("");
  }

  for (const preview of report.sourcePreviews) {
    lines.push(`## Source: ${preview.sourceTitle} (\`${preview.sourceId}\`)`, "");
    lines.push(
      `- Validation: ${preview.validation.ok ? "pass" : `**blocked** — ${preview.validation.reasons.join("; ")}`}`
    );
    if (preview.extractionQuality) {
      lines.push(
        `- Extraction quality: ${preview.extractionQuality.note} (${preview.extractionQuality.textLength || 0} chars)`
      );
    }
    lines.push(`- HTML text clean: ${preview.htmlTextClean ? "yes" : "**no**"}`);
    if (preview.previewAvailable) {
      lines.push(`- Document kind: ${preview.documentKind || "—"} (${preview.textLength || 0} chars)`);
      lines.push(`- Raw candidates (target keys): ${preview.rawCandidateCount}`);
      lines.push(`- Clean candidates (this source): ${preview.previewCandidates.length}`);
      lines.push(`- Skipped (this source): ${preview.skippedCandidates.length}`);
      lines.push("");
      if (preview.previewCandidates.length) {
        lines.push("### Clean candidates", "");
        for (const c of preview.previewCandidates) {
          lines.push(`#### \`${c.fieldKey}\` (${c.priority})`, "");
          lines.push(`- Value: ${c.extractedValuePreview || "—"}`);
          lines.push(`- Evidence: ${c.evidencePreview || "—"}`);
          lines.push(`- Extraction: ${c.extractionType || "—"} · Confidence: ${c.confidenceLevel || "—"}`);
          if (c.enriched) lines.push("- Enriched by GHL narrow script");
          lines.push("");
        }
      }
      if (preview.skippedCandidates.length) {
        lines.push("### Skipped candidates", "");
        for (const s of preview.skippedCandidates.slice(0, 20)) {
          lines.push(`- \`${s.fieldKey}\`: ${s.reasons.join(", ")} — "${s.extractedValuePreview || ""}"`);
        }
        lines.push("");
      }
    } else {
      lines.push(`- Preview skipped: ${preview.previewSkippedReason}`, "");
    }
  }

  lines.push("## Proposed Facts (global, after dedupe)", "");
  if (!report.wouldWrite.factRowsWouldCreate.length) {
    lines.push("_No clean proposed facts._", "");
  } else {
    lines.push("| Source | Field key | Value preview |", "|--------|-----------|---------------|");
    for (const row of report.wouldWrite.factRowsWouldCreate) {
      lines.push(
        `| ${row.sourceTitle} | \`${row.fieldKey}\` | ${String(row.extractedValuePreview || "").slice(0, 100)} |`
      );
    }
    lines.push("");
  }

  lines.push("## Would Write On Apply", "");
  lines.push(`- Pending fact rows: ${report.wouldWrite.factsWouldCreateCount}`);
  lines.push(`- Sources patched (Status → Extracted): ${report.wouldWrite.sourcesWouldPatch.length}`);
  lines.push("");
  lines.push("**Does not write:**");
  for (const item of report.wouldWrite.doesNotWrite) lines.push(`- ${item}`);
  lines.push("");

  if (report.applyResult) {
    lines.push("## Apply Result", "");
    lines.push(`- Run ID: \`${report.applyResult.runId}\``);
    lines.push(`- Sources patched: ${report.applyResult.sourcesPatched.length}`);
    lines.push(`- Facts created: ${report.applyResult.factsCreated.length}`);
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
    `[ghl-hoteles-extract] mode=${DRY_RUN ? "dry-run" : "apply"} sources=${SOURCE_IDS.join(",")}`
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

  const allFacts = await fetchOperatorFacts();
  const existingFacts = summarizeExistingFacts(allFacts, SOURCE_IDS, TARGET_KEYS);
  const keyPlan = buildTargetKeyPlan(TARGET_KEYS);

  const sourcePreviews = [];
  for (const source of sources) {
    const preview = await previewGhlHotelesSource(source, TARGET_KEYS, {
      limitFacts: LIMIT_FACTS,
    });
    sourcePreviews.push(preview);
    console.log(
      `[ghl-hoteles-extract] ${source.id} clean=${preview.previewCandidates?.length || 0} skipped=${preview.skippedCandidates?.length || 0}`
    );
  }

  const wouldWrite = buildWouldWritePlan(sourcePreviews, TARGET_KEYS, { limitFacts: LIMIT_FACTS });
  const allHtmlTextClean = sourcePreviews.every((p) => p.htmlTextClean !== false);
  const extractionQualityAssessment = assessExtractionQuality(sourcePreviews, wouldWrite);

  let applyResult = null;
  if (APPLY && APPROVE) {
    applyResult = await applyGhlHotelesExtract({
      sources,
      targetKeys: TARGET_KEYS,
      limitFacts: LIMIT_FACTS,
      sourcePreviews,
    });
    console.log(
      `[ghl-hoteles-extract] apply complete runId=${applyResult.runId} facts=${applyResult.factsCreated.length}`
    );
  }

  const report = {
    generatedAt: new Date().toISOString(),
    mode: DRY_RUN ? "dry-run" : "apply",
    operatorId: GHL_OPERATOR_ID,
    operatorName: "GHL Hoteles (GHL Holding)",
    sourceIds: SOURCE_IDS,
    allowlistedSourceIds: ALLOWLISTED_SOURCE_IDS,
    excludedSourceId: EXCLUDED_SOURCE_ID,
    targetFactKeys: TARGET_KEYS,
    primaryTargetFactKeys: PRIMARY_TARGET_FACT_KEYS,
    optionalTargetFactKeys: OPTIONAL_TARGET_FACT_KEYS,
    keyPlan,
    limitFacts: LIMIT_FACTS,
    allHtmlTextClean,
    extractionQualityAssessment,
    recommendationNote: DRY_RUN
      ? "Dry-run uses read-only extraction preview — no Airtable writes unless --apply --approve-ghl-hoteles-extract."
      : "Apply completed — facts remain Pending; steward review required.",
    existingFacts,
    sourcePreviews: sourcePreviews.map((p) => ({
      ...p,
      previewCandidates: (p.previewCandidates || []).map(({ _candidate, ...rest }) => rest),
    })),
    wouldWrite: {
      ...wouldWrite,
      proposedCandidates: undefined,
    },
    applyResult,
    postApplyCommand:
      "npm run steward-partner-intelligence -- --entity-type operator --target-rec-id reciI2tYQBfMoMK9G --dry-run --recompute",
    refuses: [
      "Sources outside hard-coded allowlist",
      "Spanish home recFqJpw4wJbMmVSF",
      "Sources not linked to reciI2tYQBfMoMK9G",
      "Gap facts / Not confirmed placeholders",
      "Approving sources or facts",
      "Operator Setup profile governance updates",
      "Company Validated / Company Validation Date / Show Trust Label",
      "Governance publish",
      "Apply without --approve-ghl-hoteles-extract",
    ],
  };

  mkdirSync(join(ROOT, "reports"), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, buildMarkdown(report), "utf8");
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${REPORT_MD}`);
  console.log(
    `[ghl-hoteles-extract] proposed_facts=${wouldWrite.factsWouldCreateCount} unsupported_keys=${wouldWrite.unsupportedRegistryKeys.length}`
  );
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
