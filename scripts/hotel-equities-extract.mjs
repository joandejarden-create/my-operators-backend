#!/usr/bin/env node
/**
 * Hotel Equities narrow operator extraction — dry-run preview by default.
 * Allowlisted sources only; does not approve sources/facts or touch Setup governance.
 *
 * Usage:
 *   npm run hotel-equities-extract -- --dry-run
 *   npm run hotel-equities-extract -- --dry-run --source-group pdf
 *   npm run hotel-equities-extract -- --dry-run --limit-facts 12
 *   npm run hotel-equities-extract -- --apply --approve-hotel-equities-extract
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
  HE_OPERATOR_ID,
  REPORT_JSON_NAME,
  REPORT_MD_NAME,
  ALLOWLISTED_SOURCE_IDS,
  WEBSITE_SOURCE_IDS,
  PDF_SOURCE_IDS,
  SOURCE_GROUPS,
  DEFAULT_TARGET_FACT_KEYS,
  PDF_ENRICHMENT_TARGET_FACT_KEYS,
  parseIdList,
  parseFactKeyList,
  resolveSourceGroup,
  resolveSourceIds,
  resolveTargetFactKeys,
  buildTargetKeyPlan,
  summarizeExistingFacts,
  listApprovedWebsiteFacts,
  previewHotelEquitiesSource,
  buildWouldWritePlan,
  applyHotelEquitiesExtract,
} from "../lib/partner-intelligence/hotel-equities-extract.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");
const REPORT_JSON = join(ROOT, "reports", REPORT_JSON_NAME);
const REPORT_MD = join(ROOT, "reports", REPORT_MD_NAME);

const APPLY = process.argv.includes("--apply");
const DRY_RUN = process.argv.includes("--dry-run") || !APPLY;
const APPROVE = process.argv.includes("--approve-hotel-equities-extract");

function argValue(flag) {
  const idx = process.argv.indexOf(flag);
  if (idx === -1) return "";
  return String(process.argv[idx + 1] || "").trim();
}

const SOURCE_GROUP = resolveSourceGroup(argValue("--source-group") || "website");
const SOURCE_IDS = resolveSourceIds(parseIdList(argValue("--source-ids")), SOURCE_GROUP);
const TARGET_KEYS = resolveTargetFactKeys(parseFactKeyList(argValue("--fact-keys")), SOURCE_GROUP);
const LIMIT_FACTS = Math.max(0, Number(argValue("--limit-facts") || "0") || 0) || null;

function validateCli() {
  if (APPLY && !APPROVE) {
    console.error("Apply requires both --apply and --approve-hotel-equities-extract.");
    process.exit(1);
  }
}

async function fetchOperatorFacts() {
  const all = [];
  let offset = null;
  do {
    const page = await listPartnerFacts({ operatorId: HE_OPERATOR_ID, limit: 100, offset });
    all.push(...(page.facts || []));
    offset = page.offset;
  } while (offset);
  return all;
}

function buildMarkdown(report) {
  const lines = [
    "# Hotel Equities Narrow Extraction",
    "",
    `Generated: ${report.generatedAt}`,
    `Mode: **${report.mode}**`,
    `Source group: **${report.sourceGroup}**`,
    `Operator: Hotel Equities (CALA) — \`${report.operatorId}\``,
    "",
    "## Summary",
    "",
    `- Source group: **${report.sourceGroup}** (${report.sourceGroupDescription})`,
    `- Sources in scope: ${report.sourceIds.map((id) => `\`${id}\``).join(", ")}`,
    `- Target fact keys requested: ${report.targetFactKeys.length}`,
    `- Registry-supported keys: ${report.keyPlan.filter((k) => k.registrySupported).length}`,
    `- Registry-unsupported keys: ${report.keyPlan.filter((k) => !k.registrySupported).length}`,
    `- HTML text clean (all sources): **${report.allHtmlTextClean ? "yes" : "see per-source"}**`,
    `- Existing facts (allowlisted sources): ${report.existingFacts.existingCount}`,
    `- Clean facts that would be created on apply: **${report.wouldWrite.factsWouldCreateCount}**`,
    `- Skipped candidates: ${report.wouldWrite.factRowsSkipped.length}`,
    `- Duplicate warnings vs approved website facts: ${report.wouldWrite.duplicateWarningsAgainstApproved?.length || 0}`,
    "",
  ];

  if (report.wouldWrite.publishScopeStrength) {
    const ps = report.wouldWrite.publishScopeStrength;
    lines.push(
      "## Publish scope strength (later)",
      "",
      `- Substantive proposed facts: **${ps.substantiveCount}**`,
      `- Has \`op.platform.offeredServices\`: **${ps.hasOfferedServices ? "yes" : "no"}**`,
      `- Strong enough to add to publish scope later: **${ps.strongEnoughForPublishScopeLater ? "yes — pending steward review" : "no — needs stronger candidates"}**`,
      `- Note: ${ps.notes}`,
      ""
    );
  }

  lines.push(
    report.recommendationNote,
    "",
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

  if (report.wouldWrite.duplicateWarningsAgainstApproved?.length) {
    lines.push("## Duplicate warnings (approved website facts)", "");
    for (const w of report.wouldWrite.duplicateWarningsAgainstApproved) {
      lines.push(
        `- \`${w.fieldKey}\` from \`${w.sourceId}\`: ${w.reasons.join(", ")} — "${w.extractedValuePreview || ""}"`
      );
    }
    lines.push("");
  }

  if (report.approvedWebsiteFactCount != null) {
    lines.push(`_Approved website facts used for duplicate check: ${report.approvedWebsiteFactCount}_`, "");
  }

  for (const preview of report.sourcePreviews) {
    lines.push(`## Source: ${preview.sourceTitle} (\`${preview.sourceId}\`)`, "");
    lines.push(`- Validation: ${preview.validation.ok ? "pass" : `**blocked** — ${preview.validation.reasons.join("; ")}`}`);
    if (preview.extractionQuality) {
      lines.push(
        `- Extraction quality: ${preview.extractionQuality.note} (${preview.extractionQuality.textLength || 0} chars, kind=${preview.extractionQuality.kind})`
      );
    }
    lines.push(`- HTML text clean: ${preview.htmlTextClean ? "yes" : preview.documentKind === "pdf" ? "n/a (pdf)" : "**no**"}`);
    if (preview.previewAvailable) {
      lines.push(`- Document kind: ${preview.documentKind || "—"} (${preview.textLength || 0} chars)`);
      lines.push(`- Classifier role: ${preview.classificationRole || "—"}`);
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
          if (c.enriched) lines.push("- Enriched by HE narrow script (title/identity)");
          lines.push("");
        }
      }
      if (preview.skippedCandidates.length) {
        lines.push("### Skipped candidates", "");
        for (const s of preview.skippedCandidates.slice(0, 20)) {
          lines.push(`- \`${s.fieldKey}\`: ${s.reasons.join(", ")} — "${s.extractedValuePreview || ""}"`);
        }
        if (preview.skippedCandidates.length > 20) {
          lines.push(`- …and ${preview.skippedCandidates.length - 20} more`);
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
    `[hotel-equities-extract] mode=${DRY_RUN ? "dry-run" : "apply"} source-group=${SOURCE_GROUP} sources=${SOURCE_IDS.join(",")}`
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
  const approvedWebsiteFacts =
    SOURCE_GROUP === "pdf" || SOURCE_GROUP === "all"
      ? listApprovedWebsiteFacts(allFacts)
      : [];
  const keyPlan = buildTargetKeyPlan(TARGET_KEYS);

  const sourcePreviews = [];
  for (const source of sources) {
    const preview = await previewHotelEquitiesSource(source, TARGET_KEYS, {
      sourceGroup: SOURCE_GROUP,
      approvedWebsiteFacts,
      limitFacts: LIMIT_FACTS,
    });
    sourcePreviews.push(preview);
    console.log(
      `[hotel-equities-extract] ${source.id} clean=${preview.previewCandidates?.length || 0} skipped=${preview.skippedCandidates?.length || 0}`
    );
  }

  const wouldWrite = buildWouldWritePlan(sourcePreviews, TARGET_KEYS, { limitFacts: LIMIT_FACTS });
  const allHtmlTextClean = sourcePreviews.every((p) => p.htmlTextClean !== false);

  let applyResult = null;
  if (APPLY && APPROVE) {
    applyResult = await applyHotelEquitiesExtract({
      sources,
      targetKeys: TARGET_KEYS,
      limitFacts: LIMIT_FACTS,
      sourcePreviews,
    });
    console.log(
      `[hotel-equities-extract] apply complete runId=${applyResult.runId} facts=${applyResult.factsCreated.length}`
    );
  }

  const report = {
    generatedAt: new Date().toISOString(),
    mode: DRY_RUN ? "dry-run" : "apply",
    sourceGroup: SOURCE_GROUP,
    sourceGroupDescription:
      SOURCE_GROUP === "pdf"
        ? "PDF enrichment — recxdPFckVzA3ckmN + rectqBTiGkq3hUlXa"
        : SOURCE_GROUP === "website"
          ? "Website HTML captures only"
          : "All allowlisted website + PDF sources",
    operatorId: HE_OPERATOR_ID,
    operatorName: "Hotel Equities (CALA)",
    sourceIds: SOURCE_IDS,
    websiteSourceIds: WEBSITE_SOURCE_IDS,
    pdfSourceIds: PDF_SOURCE_IDS,
    allowlistedSourceIds: [...ALLOWLISTED_SOURCE_IDS],
    targetFactKeys: TARGET_KEYS,
    defaultTargetFactKeys:
      SOURCE_GROUP === "pdf" ? PDF_ENRICHMENT_TARGET_FACT_KEYS : DEFAULT_TARGET_FACT_KEYS,
    approvedWebsiteFactCount: approvedWebsiteFacts.length,
    keyPlan,
    limitFacts: LIMIT_FACTS,
    allHtmlTextClean,
    recommendationNote: DRY_RUN
      ? "Dry-run uses read-only extraction preview — no Airtable writes unless --apply --approve-hotel-equities-extract."
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
      "npm run steward-partner-intelligence -- --entity-type operator --target-rec-id recWPKu5laVZxsvpn --dry-run --recompute",
    refuses: [
      "Sources outside hard-coded allowlist",
      "Sources not linked to recWPKu5laVZxsvpn",
      "Broad extraction from all operator PI sources",
      "Gap facts / Not confirmed placeholders",
      "Markup-like or weak-evidence candidates",
      "Approving sources or facts",
      "Operator Setup profile governance updates",
      "Company Validated / Company Validation Date / Show Trust Label",
      "Apply without --approve-hotel-equities-extract",
      "Force mode",
    ],
  };

  mkdirSync(join(ROOT, "reports"), { recursive: true });
  writeFileSync(REPORT_JSON, JSON.stringify(report, null, 2), "utf8");
  writeFileSync(REPORT_MD, buildMarkdown(report), "utf8");
  console.log(`Wrote ${REPORT_JSON}`);
  console.log(`Wrote ${REPORT_MD}`);
  console.log(
    `[hotel-equities-extract] proposed_facts=${wouldWrite.factsWouldCreateCount} unsupported_keys=${wouldWrite.unsupportedRegistryKeys.length}`
  );
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
