#!/usr/bin/env node
/**
 * Re-detect subject mentions from stored raw responses (no new LLM calls).
 *
 *   node scripts/reparse-adp-subject-mentions-v1.mjs --property adp_hotel_phillips_kansas_city --dry-run
 *   node scripts/reparse-adp-subject-mentions-v1.mjs --property adp_hotel_phillips_kansas_city --apply
 */
import "../load-env.js";
import { readFileSync, writeFileSync, existsSync } from "fs";
import { join } from "path";
import { loadAllPeriods, loadPropertyProfile } from "../lib/ai-demand-positioning/data-model.js";
import { selectLatestCertifiedOfficialPeriod } from "../lib/ai-demand-positioning/period-eligibility-v1.js";
import { detectPropertyMention } from "../lib/ai-demand-positioning/execution/response-parser.js";
import { buildScenarioUniverse } from "../lib/ai-demand-positioning/prompt-universe/scenario-registry.js";
import { buildOwnerPayload } from "../lib/ai-demand-positioning/customer/owner-payload.js";

const args = process.argv.slice(2);
const propertyId = args.find((a, i) => args[i - 1] === "--property");
const apply = args.includes("--apply");
const dryRun = !apply;

if (!propertyId) {
  console.error("Usage: --property <id> [--dry-run|--apply]");
  process.exit(1);
}

function findRuntimePeriodPath(periodId) {
  const runtimeDir = join(process.cwd(), "data/ai-demand-positioning/runtime");
  const candidate = join(runtimeDir, `${periodId}.json`);
  if (existsSync(candidate)) return candidate;
  return null;
}

function main() {
  const profile = loadPropertyProfile(propertyId);
  if (!profile) {
    console.error("Profile not found");
    process.exit(1);
  }
  const period = selectLatestCertifiedOfficialPeriod(loadAllPeriods(propertyId));
  if (!period) {
    console.error("No certified period");
    process.exit(1);
  }

  const scenarios = buildScenarioUniverse(profile);
  const beforePayload = buildOwnerPayload(period, scenarios, profile);

  const flips = [];
  let openaiBefore = 0;
  let openaiAfter = 0;
  for (const obs of period.observations || []) {
    if (!obs.parsed && !obs.rawResponse) continue;
    const isOpenai = String(obs.provider).toLowerCase() === "openai";
    if (isOpenai && obs.mentioned) openaiBefore += 1;
    const det = detectPropertyMention(obs.rawResponse || "", profile);
    if (isOpenai && det.mentioned) openaiAfter += 1;
    if (!!obs.mentioned !== !!det.mentioned) {
      flips.push({
        provider: obs.provider,
        scenarioId: obs.scenarioId,
        from: !!obs.mentioned,
        to: !!det.mentioned,
        matchedVariant: det.matchedVariant || null,
      });
      obs.mentioned = det.mentioned;
      if (det.position != null) obs.position = det.position;
      if (det.context) obs.mentionContext = det.context;
      obs.subjectMatchVersion = "adp_subject_match_v2_normalized_aliases";
    }
  }

  const afterPayload = buildOwnerPayload(period, scenarios, profile);
  const report = {
    propertyId,
    periodId: period.periodId,
    mode: dryRun ? "DRY_RUN" : "APPLY",
    flips,
    openaiMentionedBefore: openaiBefore,
    openaiMentionedAfter: openaiAfter,
    metricsOld: {
      demandCapture: beforePayload.demandCapture?.overallRate,
      consideration: beforePayload.executiveMetrics?.considerationRate?.rate,
      scenarioPresence: beforePayload.executiveMetrics?.scenarioPresence?.rate,
    },
    metricsNew: {
      demandCapture: afterPayload.demandCapture?.overallRate,
      consideration: afterPayload.executiveMetrics?.considerationRate?.rate,
      scenarioPresence: afterPayload.executiveMetrics?.scenarioPresence?.rate,
    },
  };

  console.log(JSON.stringify(report, null, 2));

  if (dryRun) {
    console.log("\n[DRY RUN] No files written.");
    return;
  }

  const path = findRuntimePeriodPath(period.periodId);
  if (!path) {
    console.error("Runtime period file not found for", period.periodId);
    process.exit(1);
  }
  const disk = JSON.parse(readFileSync(path, "utf8"));
  const byKey = new Map(
    (disk.observations || []).map((o) => [`${o.provider}|${o.scenarioId}`, o])
  );
  for (const flip of flips) {
    const o = byKey.get(`${flip.provider}|${flip.scenarioId}`);
    if (!o) continue;
    o.mentioned = flip.to;
    o.subjectMatchVersion = "adp_subject_match_v2_normalized_aliases";
  }
  writeFileSync(path, JSON.stringify(disk, null, 2));
  console.log("\nUpdated", path);
}

main();
