#!/usr/bin/env node
/**
 * Radisson Blu by Choice narrow brand extraction — dry-run preview by default.
 * Allowlisted Choice/Americas sources only.
 *
 *   npm run radisson-blu-extract -- --dry-run
 *   npm run radisson-blu-extract -- --apply --approve-radisson-blu-extract
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import { getPartnerSourceById } from "../lib/partner-intelligence/airtable-source.js";
import { listPartnerFacts } from "../lib/partner-intelligence/airtable-facts.js";
import {
  RADISSON_BLU_BRAND_ID,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  ALLOWLISTED_SOURCE_IDS,
  PRIMARY_TARGET_FACT_KEYS,
  SECONDARY_TARGET_FACT_KEYS,
  DEFAULT_TARGET_FACT_KEYS,
  USER_REQUESTED_KEY_MAPPING,
  parseIdList,
  parseFactKeyList,
  resolveSourceIds,
  resolveTargetFactKeys,
  buildTargetKeyPlan,
  summarizeExistingFacts,
  previewRadissonBluSource,
  buildWouldWritePlan,
  applyRadissonBluExtract,
  assessExtractionQuality,
  getRegionOwnershipCaveats,
} from "../lib/partner-intelligence/radisson-blu-extract.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", REPORT_JSON_NAME);
const REPORT_MD = join(ROOT, "reports", REPORT_MD_NAME);

const APPLY = process.argv.includes("--apply");
const DRY_RUN = process.argv.includes("--dry-run") || !APPLY;
const APPROVE = process.argv.includes("--approve-radisson-blu-extract");

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
    console.error("Apply requires both --apply and --approve-radisson-blu-extract.");
    process.exit(1);
  }
}

async function fetchBrandFacts() {
  const all = [];
  let offset = null;
  do {
    const page = await listPartnerFacts({ brandId: RADISSON_BLU_BRAND_ID, limit: 100, offset });
    all.push(...(page.facts || []));
    offset = page.offset;
  } while (offset);
  return all;
}

function buildMarkdown(report) {
  const lines = [
    "# Radisson Blu by Choice — Narrow Extraction",
    "",
    `Generated: ${report.generatedAt}`,
    `Mode: **${report.mode}**`,
    `Brand: Radisson Blu by Choice — \`${report.brandId}\``,
    "",
    "## Summary",
    "",
    `- Sources in scope: ${report.sourceIds.map((id) => `\`${id}\``).join(", ")}`,
    `- Target fact keys: ${report.targetFactKeys.length}`,
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
      `- Has brand name: ${eq.hasBrandName ? "yes" : "no"}`,
      `- Has parent company (Choice): ${eq.hasParentCompany ? "yes" : "no"}`,
      `- Has Americas footprint: ${eq.hasAmericasFootprint ? "yes" : "no"}`,
      `- Has ownership caveat: ${eq.hasOwnershipCaveat ? "yes" : "no"}`,
      `- Apply recommended: **${eq.applyRecommended ? "yes — pending founder review" : "no — review report first"}**`,
      `- Governance publish still blocked: **${eq.governancePublishStillBlocked ? "yes" : "no"}** — ${eq.governanceBlockReason}`,
      ""
    );
    lines.push("### Platform intelligence (read paths)", "");
    for (const note of eq.platformIntelligenceNotes || []) {
      lines.push(`- ${note}`);
    }
    lines.push("");
  }

  lines.push("## Region / Ownership Caveats", "");
  for (const c of report.regionOwnershipCaveats || []) {
    lines.push(`- ${c}`);
  }
  lines.push("", report.recommendationNote, "");

  lines.push("## User-Requested Key Mapping", "");
  lines.push("| Requested key | Registry key | Supported | Note |");
  lines.push("|---------------|--------------|-----------|------|");
  for (const row of report.userRequestedKeyMapping || []) {
    lines.push(
      `| \`${row.requested}\` | ${row.registryKey ? `\`${row.registryKey}\`` : "—"} | ${row.supported ? "yes" : "no"} | ${row.note || "—"} |`
    );
  }
  lines.push("");

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
      report.wouldWrite.unsupportedRegistryKeys.map((k) => `- \`${k}\``).join("\n"),
      ""
    );
  }

  if (report.wouldWrite.duplicateWarnings?.length) {
    lines.push("## Duplicate Warnings", "");
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
    lines.push(`- HTML text clean: ${preview.htmlTextClean ? "yes" : "**no**"}`);
    if (preview.previewAvailable) {
      lines.push(`- Document kind: ${preview.documentKind || "—"} (${preview.textLength || 0} chars)`);
      lines.push(`- Raw candidates (target keys): ${preview.rawCandidateCount}`);
      lines.push(`- Clean candidates: ${preview.previewCandidates.length}`);
      lines.push(`- Skipped: ${preview.skippedCandidates.length}`);
      lines.push("");
      if (preview.previewCandidates.length) {
        lines.push("### Clean candidates", "");
        for (const c of preview.previewCandidates) {
          lines.push(`#### \`${c.fieldKey}\` (${c.priority})`, "");
          lines.push(`- Value: ${c.extractedValuePreview || "—"}`);
          lines.push(`- Evidence: ${c.evidencePreview || "—"}`);
          lines.push(`- Extraction: ${c.extractionType || "—"} · ${c.confidenceLevel || "—"}`);
          if (c.enriched) lines.push("- Enriched by Radisson Blu narrow script");
          lines.push("");
        }
      }
      if (preview.skippedCandidates.length) {
        lines.push("### Skipped candidates", "");
        for (const s of preview.skippedCandidates.slice(0, 25)) {
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
    lines.push(`- Facts created: ${report.applyResult.factsCreated.length}`);
    lines.push("");
  }

  lines.push("## Post-Apply Recommendation", "");
  lines.push("```bash");
  lines.push(report.postApplyCommand);
  lines.push("```", "");
  lines.push("_Manually review and approve facts before governance publish._", "");

  return lines.join("\n");
}

async function main() {
  validateCli();

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  console.log(
    `[radisson-blu-extract] mode=${DRY_RUN ? "dry-run" : "apply"} sources=${SOURCE_IDS.join(",")}`
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

  const allFacts = await fetchBrandFacts();
  const existingFacts = summarizeExistingFacts(allFacts, SOURCE_IDS, TARGET_KEYS);
  const keyPlan = buildTargetKeyPlan(TARGET_KEYS);

  const sourcePreviews = [];
  for (const source of sources) {
    const preview = await previewRadissonBluSource(source, TARGET_KEYS, {
      limitFacts: LIMIT_FACTS,
    });
    sourcePreviews.push(preview);
    console.log(
      `[radisson-blu-extract] ${source.id} clean=${preview.previewCandidates?.length || 0} skipped=${preview.skippedCandidates?.length || 0}`
    );
  }

  const wouldWrite = buildWouldWritePlan(sourcePreviews, TARGET_KEYS, { limitFacts: LIMIT_FACTS });
  const allHtmlTextClean = sourcePreviews.every((p) => p.htmlTextClean !== false);
  const extractionQualityAssessment = assessExtractionQuality(sourcePreviews, wouldWrite);

  let applyResult = null;
  if (APPLY && APPROVE) {
    applyResult = await applyRadissonBluExtract({
      sources,
      targetKeys: TARGET_KEYS,
      limitFacts: LIMIT_FACTS,
      sourcePreviews,
    });
    console.log(
      `[radisson-blu-extract] apply complete runId=${applyResult.runId} facts=${applyResult.factsCreated.length}`
    );
  }

  const report = {
    generatedAt: new Date().toISOString(),
    mode: DRY_RUN ? "dry-run" : "apply",
    brandId: RADISSON_BLU_BRAND_ID,
    brandName: "Radisson Blu by Choice",
    sourceIds: SOURCE_IDS,
    allowlistedSourceIds: ALLOWLISTED_SOURCE_IDS,
    targetFactKeys: TARGET_KEYS,
    primaryTargetFactKeys: PRIMARY_TARGET_FACT_KEYS,
    secondaryTargetFactKeys: SECONDARY_TARGET_FACT_KEYS,
    defaultTargetFactKeys: DEFAULT_TARGET_FACT_KEYS,
    userRequestedKeyMapping: USER_REQUESTED_KEY_MAPPING,
    keyPlan,
    limitFacts: LIMIT_FACTS,
    allHtmlTextClean,
    regionOwnershipCaveats: getRegionOwnershipCaveats(),
    extractionQualityAssessment,
    recommendationNote: DRY_RUN
      ? "Dry-run only — no Airtable writes unless --apply --approve-radisson-blu-extract."
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
      "npm run steward-partner-intelligence -- --entity-type brand --target-rec-id recWPEvxBQxVVzSq3 --dry-run --recompute",
    refuses: [
      "Sources outside allowlist",
      "RHG-global footprint without Americas Choice evidence",
      "Approving sources or facts",
      "Brand Setup governance / Company Validated",
      "Governance publish / platform field publishing",
      "Apply without --approve-radisson-blu-extract",
    ],
  };

  mkdirSync(join(ROOT, "reports"), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, buildMarkdown(report), "utf8");
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${REPORT_MD}`);
  console.log(
    `[radisson-blu-extract] proposed_facts=${wouldWrite.factsWouldCreateCount} skipped=${wouldWrite.factRowsSkipped.length}`
  );
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
