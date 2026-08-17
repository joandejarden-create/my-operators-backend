#!/usr/bin/env node
/**
 * Promote CONFIRMED/CORRECTED human reviews into Golden Set v2.
 * Default: dry-run. Pass --apply to write.
 * Never overwrites v1. Never auto-approves.
 */
import { promoteGoldenSetV2, getReviewProgress } from "../lib/ai-visibility/validation/golden-set-human-review.js";

const apply = process.argv.includes("--apply");
const progress = getReviewProgress({});
console.log("Review progress:", progress);
const result = promoteGoldenSetV2({ apply });
console.log(JSON.stringify({
  apply: result.apply,
  written: result.written,
  version: result.version,
  caseCount: result.caseCount,
  casesFromV1: result.casesFromV1,
  casesPromotedFromReview: result.casesPromotedFromReview,
  humanLabelled: result.humanLabelled,
  llmLabelledAsGroundTruth: result.llmLabelledAsGroundTruth,
  note: result.note,
  path: result.path,
}, null, 2));
if (!apply) {
  console.log("\nDry-run only. Re-run with --apply after enough CONFIRMED/CORRECTED reviews (≥85 for n≥150 with v1).");
}
