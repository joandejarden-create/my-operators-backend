#!/usr/bin/env node
/**
 * Presence Holdout v3 — fresh unseen candidate generation.
 *
 *   node scripts/ai-intelligence-presence-holdout-v3-fresh-generate.mjs --plan-only
 *   node scripts/ai-intelligence-presence-holdout-v3-fresh-generate.mjs --execute
 *
 * HOLDOUT_V3_SELECTION=0 FREEZE=0 SCORING=0 · no resolver/alias changes
 */
import "../load-env.js";
import fs from "fs";
import path from "path";
import { fileURLToPath } from "url";
import {
  buildPresenceHoldoutV3Plan,
  generatePresenceHoldoutV3Responses,
  presenceHoldoutV3Paths,
  classifyNegativeControlCategories,
  HOLDOUT_V3_BATCH_ID,
} from "../lib/ai-visibility/validation/presence-holdout-v3-fresh-candidates.js";
import {
  loadPresenceValidationCandidates,
  presenceValidationPaths,
} from "../lib/ai-visibility/validation/presence-validation-candidates.js";
import { uniqueResponseIds } from "../lib/ai-visibility/validation/presence-validation-pool-governance.js";

const __dirname = path.dirname(fileURLToPath(import.meta.url));
const ROOT = path.resolve(__dirname, "..");
const PLAN_ONLY = process.argv.includes("--plan-only");
const EXECUTE = process.argv.includes("--execute");

function countBy(rows, key) {
  const out = {};
  for (const r of rows || []) {
    const k = r[key] || "unspecified";
    out[k] = (out[k] || 0) + 1;
  }
  return out;
}

function preferV3PrimaryQueue() {
  const paths = presenceValidationPaths();
  const doc = loadPresenceValidationCandidates();
  if (!doc?.cases) return null;
  for (const c of doc.cases) {
    c.primaryReviewQueue = c.batchId === HOLDOUT_V3_BATCH_ID;
  }
  const primaryN = doc.cases.filter((c) => c.primaryReviewQueue).length;
  doc.PRIMARY_REVIEW_QUEUE = primaryN;
  doc.activeReviewBatch = HOLDOUT_V3_BATCH_ID;
  doc.HOLDOUT_V3_SELECTION_ALLOWED = false;
  doc.HOLDOUT_V3_FREEZE_ALLOWED = false;
  doc.HOLDOUT_V3_SCORING_ALLOWED = false;
  fs.writeFileSync(paths.candidatesPath, JSON.stringify(doc, null, 2) + "\n");
  return primaryN;
}

async function main() {
  const plan = buildPresenceHoldoutV3Plan();
  const paths = presenceHoldoutV3Paths();
  fs.mkdirSync(path.dirname(paths.plan), { recursive: true });
  const planOut = { ...plan };
  delete planOut.slots;
  fs.writeFileSync(paths.plan, JSON.stringify(planOut, null, 2) + "\n");

  console.log("PRESENCE_HOLDOUT_V3_GENERATION_PLAN_READY");
  console.log(JSON.stringify(planOut, null, 2));

  if (PLAN_ONLY || !EXECUTE) {
    console.log("\nPass --execute to run live generation (requires AI_VISIBILITY_LIVE_TEST=true).");
    return;
  }

  if (plan.READY_TO_RUN !== "YES") {
    console.error("READY_TO_RUN=NO — stopping before provider calls");
    process.exit(2);
  }

  process.env.AI_VISIBILITY_LIVE_TEST = process.env.AI_VISIBILITY_LIVE_TEST || "true";

  const gen = await generatePresenceHoldoutV3Responses({
    resume: true,
    requireLiveFlag: true,
  });
  if (!gen.ok) {
    console.error(JSON.stringify(gen, null, 2));
    process.exit(2);
  }

  const primaryN = preferV3PrimaryQueue();
  const cases = gen.candidates.cases || [];
  const report = {
    phase: "PRESENCE_HOLDOUT_V3_FRESH_CANDIDATES_READY",
    status: "PRESENCE_HOLDOUT_V3_FRESH_VALIDATION_POOL_READY",
    batchId: HOLDOUT_V3_BATCH_ID,
    NEW_RESPONSES: gen.manifest.COMPLETED_OK,
    TOTAL_CANDIDATES: cases.length,
    PRIMARY_REVIEW_QUEUE: primaryN ?? gen.candidates.PRIMARY_REVIEW_QUEUE,
    POTENTIAL_PRESENT: gen.candidates.POTENTIAL_TRUE,
    POTENTIAL_NOT_PRESENT: gen.candidates.POTENTIAL_FALSE,
    UNIQUE_RESPONSE_N: uniqueResponseIds(cases).size,
    PROVIDER_COVERAGE: countBy(cases, "provider"),
    LANGUAGE_COVERAGE: countBy(cases, "language"),
    GEOGRAPHY_COVERAGE: countBy(cases, "geography"),
    NEGATIVE_CONTROL_CATEGORIES: classifyNegativeControlCategories(cases),
    DUPLICATES_REJECTED: gen.manifest.DUPLICATES_REJECTED,
    LEAKAGE_REJECTED: gen.manifest.LEAKAGE_REJECTED,
    ACTUAL_MODELS_USED: gen.manifest.ACTUAL_MODELS_USED,
    ESTIMATED_SPENT_USD: gen.manifest.ESTIMATED_SPENT_USD,
    ERRORS: gen.manifest.ERRORS,
    REJECTED: gen.manifest.REJECTED,
    HUMAN_REVIEW_ROUTE: "/ai-intelligence-presence-validation-review",
    EXPORT_READY: cases.length > 0 ? "YES" : "NO",
    HUMAN_REVIEW_REQUIRED: "YES",
    HOLDOUT_V3_SELECTION_ALLOWED: "NO",
    HOLDOUT_V3_FREEZE_ALLOWED: "NO",
    HOLDOUT_V3_SCORING_ALLOWED: "NO",
    NEXT_ACTION: "EXPORT_FRESH_HOLDOUT_V3_PRESENCE_REVIEW_BATCH",
    FINAL_TARGET_REMINDER: {
      PAIR_N: 100,
      PRESENT: 60,
      NOT_PRESENT: 40,
      UNIQUE_RESPONSES: ">=80",
    },
    hardGuards: {
      HOLDOUT_V2_CHANGES: 0,
      ENTITY_RESOLVER_CHANGES: 0,
      ALIAS_CHANGES: 0,
      HOLDOUT_V3_SELECTION: 0,
      HOLDOUT_V3_FREEZE: 0,
      HOLDOUT_V3_SCORING: 0,
      REGIONALIZATION_EXECUTION: 0,
      AIRTABLE_WRITES: 0,
      DEPLOYS: 0,
    },
  };

  fs.writeFileSync(paths.readyReport, JSON.stringify(report, null, 2) + "\n");
  console.log("\nPRESENCE_HOLDOUT_V3_FRESH_CANDIDATES_READY");
  console.log(JSON.stringify(report, null, 2));
  console.log(`\nWrote ${path.relative(ROOT, paths.readyReport)}`);
  console.log("STOP for human review — do not select/freeze/score Holdout v3.");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
