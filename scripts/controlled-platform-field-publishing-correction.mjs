#!/usr/bin/env node
/**
 * Generate steward correction plan + correction dry-run for GHL specificMarkets.
 * Read-only — does not apply writes.
 */
import "../load-env.js";
import { mkdirSync, writeFileSync } from "fs";
import { dirname, join } from "path";
import { fileURLToPath } from "url";
import {
  GHL_SPECIFIC_MARKETS_CORRECTION,
  buildGhlSpecificMarketsCorrectionMarkdown,
  buildGhlSpecificMarketsCorrectionPlan,
} from "../lib/partner-intelligence/controlled-platform-field-publishing-correction-plans.js";
import {
  buildControlledPublishCorrectionMarkdown,
  controlledPublishCorrectionReportFileNames,
  runControlledPlatformFieldCorrection,
} from "../lib/partner-intelligence/controlled-platform-field-publishing.js";

const __dirname = dirname(fileURLToPath(import.meta.url));
const ROOT = join(__dirname, "..");

async function main() {
  if (process.argv.includes("--apply")) {
    console.error(
      "[controlled-platform-field-publishing-correction] --apply not allowed on this report script. Use controlled-platform-field-publishing with correction flags after steward approval."
    );
    process.exit(1);
  }

  if (!process.env.AIRTABLE_API_KEY || !process.env.AIRTABLE_BASE_ID) {
    console.error("AIRTABLE_API_KEY and AIRTABLE_BASE_ID are required.");
    process.exit(1);
  }

  const plan = GHL_SPECIFIC_MARKETS_CORRECTION;
  const correctionRun = await runControlledPlatformFieldCorrection({
    entityType: plan.entityType,
    targetRecId: plan.targetRecId,
    destinationFieldKey: plan.destinationFieldKey,
    correctValue: plan.recommendedCorrectedValue,
    reason: plan.correctionReason,
    apply: false,
    approvalPresent: false,
    stewardContext: plan,
  });

  const liveValue = correctionRun.liveDestination?.previousValue ?? null;
  const planReport = buildGhlSpecificMarketsCorrectionPlan(liveValue);

  const dir = join(ROOT, "reports");
  mkdirSync(dir, { recursive: true });
  const names = controlledPublishCorrectionReportFileNames(plan.targetRecId);
  const jsonPath = join(dir, names.perEntityJson);
  const mdPath = join(dir, names.perEntityMd);

  planReport.correctionDryRun = {
    ok: correctionRun.validation?.ok ?? false,
    failures: correctionRun.validation?.failures ?? [],
    mode: correctionRun.mode,
    plan: correctionRun.plan || null,
    runReportJson: names.runJson,
    runReportMd: names.runMd,
  };

  const md = buildGhlSpecificMarketsCorrectionMarkdown(planReport);
  const mdWithDryRun = [
    md,
    "",
    "## Correction dry-run result",
    "",
    `- Eligible: **${correctionRun.validation?.ok ? "yes" : "no"}**`,
    correctionRun.validation?.ok
      ? `- Planned: \`${planReport.values.currentLive}\` → **${plan.recommendedCorrectedValue}**`
      : `- Failures: ${(correctionRun.validation?.failures || []).join(", ") || "—"}`,
    "",
    "---",
    "",
    buildControlledPublishCorrectionMarkdown(correctionRun),
  ].join("\n");

  writeFileSync(jsonPath, JSON.stringify(planReport, null, 2), "utf8");
  writeFileSync(mdPath, mdWithDryRun, "utf8");

  const runJson = JSON.stringify(correctionRun, null, 2);
  const runMd = buildControlledPublishCorrectionMarkdown(correctionRun);
  writeFileSync(join(dir, names.runJson), runJson, "utf8");
  writeFileSync(join(dir, names.runMd), runMd, "utf8");

  console.log(
    `[controlled-platform-field-publishing-correction] entity=${plan.entityName} dry-run=${correctionRun.validation?.ok ? "eligible" : "blocked"}`
  );
  console.log(`  current="${liveValue}"`);
  console.log(`  corrected="${plan.recommendedCorrectedValue}"`);
  console.log(`Wrote ${jsonPath}`);
  console.log(`Wrote ${mdPath}`);

  if (!correctionRun.validation?.ok) process.exit(2);
}

main().catch((err) => {
  console.error(err.message || err);
  process.exit(1);
});
