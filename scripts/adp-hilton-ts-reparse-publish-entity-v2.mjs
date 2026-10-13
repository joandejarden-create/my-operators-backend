#!/usr/bin/env node
/**
 * Reparse a Hilton TS runtime period with entity_v2 aliases and republish.
 * Does not re-call providers. Does not change thresholds.
 *
 *   node scripts/adp-hilton-ts-reparse-publish-entity-v2.mjs --period=<periodId> --certify
 */
import "../load-env.js";
import { readFileSync, existsSync, writeFileSync, mkdirSync } from "fs";
import { join } from "path";
import { loadPropertyProfile, savePeriod } from "../lib/ai-demand-positioning/data-model.js";
import { parseObservation } from "../lib/ai-demand-positioning/execution/response-parser.js";
import {
  buildPublishedSnapshotBundle,
  savePublishedSnapshotBundle,
} from "../lib/ai-demand-positioning/published-snapshot.js";
import { HILTON_TS_ENTITY_VERSION } from "../lib/ai-demand-positioning/execution/hilton-times-square-baseline-period-001-v1.js";

const args = process.argv.slice(2);
const periodArg = args.find((a) => a.startsWith("--period="));
const certify = args.includes("--certify");
if (!periodArg) {
  console.error("Usage: --period=<periodId> [--certify]");
  process.exit(2);
}
const periodId = periodArg.split("=")[1];
const path = join(process.cwd(), "data/ai-demand-positioning/runtime", `${periodId}.json`);
if (!existsSync(path)) {
  console.error("Missing runtime period", path);
  process.exit(2);
}

const period = JSON.parse(readFileSync(path, "utf8"));
const profile = loadPropertyProfile("adp_hilton_times_square");
const before = (period.observations || []).filter((o) => o.mentioned).length;
// Force re-parse even when obs.parsed=true (parsePeriodObservations skips those).
const reparsed = {
  ...period,
  observations: (period.observations || []).map((obs) => parseObservation(obs, profile)),
  status: "PARSED",
  entityResolutionVersion: HILTON_TS_ENTITY_VERSION,
};
if (certify) reparsed.certified = true;
const after = (reparsed.observations || []).filter((o) => o.mentioned).length;

savePeriod(reparsed);
const bundle = buildPublishedSnapshotBundle({ period: reparsed, profile });
if (!bundle.ok) {
  console.error("PUBLISH_BUNDLE_FAILED", bundle.message || bundle);
  process.exit(1);
}
const published = savePublishedSnapshotBundle(bundle, { seed: false });

const outDir = join(process.cwd(), "reports/adp/hilton-times-square-controlled-rerun");
mkdirSync(outDir, { recursive: true });
writeFileSync(
  join(outDir, "LIVE_REPARSE_RESULT.json"),
  JSON.stringify(
    {
      periodId,
      entityResolutionVersion: HILTON_TS_ENTITY_VERSION,
      mentionedBefore: before,
      mentionedAfter: after,
      observationCount: (reparsed.observations || []).length,
      scenarioCount: reparsed.scenarioCount,
      certified: !!reparsed.certified,
      published: published?.reportPath || published,
      considerationAfter: +((after / (reparsed.observations || []).length) * 100).toFixed(1),
    },
    null,
    2
  )
);

console.log(
  JSON.stringify(
    {
      ok: true,
      periodId,
      mentionedBefore: before,
      mentionedAfter: after,
      entityResolutionVersion: HILTON_TS_ENTITY_VERSION,
      publishedOk: true,
    },
    null,
    2
  )
);
