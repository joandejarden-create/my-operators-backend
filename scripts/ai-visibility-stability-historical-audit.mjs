#!/usr/bin/env node
/**
 * Read-only historical observation audit for repeated testing.
 * PROVIDER_CALLS = 0. DATAFORSEO_CALLS = 0.
 */
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import { auditStoredStabilityHistory, lookupValidationCohortHistory } from "../lib/ai-visibility/stability-historical-audit.js";
import { estimateValidationCost, VALIDATION_COHORT } from "../lib/ai-visibility/stability-policy.js";

const root = path.join(path.dirname(fileURLToPath(import.meta.url)), "..");
const outDir = path.join(root, "reports", "ai-visibility");
const outPath = path.join(outDir, "repeated-testing-historical-audit-v1.json");

const audit = await auditStoredStabilityHistory();
const lookup = await lookupValidationCohortHistory();
const cost = estimateValidationCost(VALIDATION_COHORT);

fs.mkdirSync(outDir, { recursive: true });
const report = {
  PROVIDER_CALLS: 0,
  DATAFORSEO_CALLS: 0,
  STAGE_B: "NOT_RUN",
  PROMPT_PROVIDER_PAIRS: audit.PROMPT_PROVIDER_PAIRS,
  PAIRS_WITH_1_OBSERVATION: audit.PAIRS_WITH_1_OBSERVATION,
  PAIRS_WITH_2_OBSERVATIONS: audit.PAIRS_WITH_2_OBSERVATIONS,
  PAIRS_WITH_3_PLUS_OBSERVATIONS: audit.PAIRS_WITH_3_PLUS_OBSERVATIONS,
  STRICT_GRAIN_KEYS: audit.STRICT_GRAIN_KEYS,
  GRAINS_WITH_1: audit.GRAINS_WITH_1,
  GRAINS_WITH_2: audit.GRAINS_WITH_2,
  GRAINS_WITH_3_PLUS: audit.GRAINS_WITH_3_PLUS,
  DATE_SPAN: audit.DATE_SPAN,
  evidenceRows: audit.evidenceRows,
  crossProviderExamples: audit.crossProviderExamples,
  validationCost: cost,
  FULL_COHORT_LOOKUP: lookup.FULL_COHORT_LOOKUP,
  cohortLookup: lookup.rows.map((r) => ({
    PROMPT_ID: r.PROMPT_ID,
    HISTORICAL_OBSERVATIONS_BY_PROVIDER: r.HISTORICAL_OBSERVATIONS_BY_PROVIDER,
    EXACT_REPEAT_COUNT: r.EXACT_REPEAT_COUNT,
    FIRST_OBSERVED_AT: r.FIRST_OBSERVED_AT,
    LAST_OBSERVED_AT: r.LAST_OBSERVED_AT,
    CURRENT_RECURRENCE_STATE: r.CURRENT_RECURRENCE_STATE,
    CURRENT_STABILITY_STATE: r.CURRENT_STABILITY_STATE,
  })),
  grainSeriesCount: audit.grainSeries.length,
  grainSeriesSample: audit.grainSeries.slice(0, 40),
};
fs.writeFileSync(outPath, JSON.stringify(report, null, 2));
console.log(JSON.stringify({ outPath, ...report, grainSeriesSample: undefined }, null, 2));
